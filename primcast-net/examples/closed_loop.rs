use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use tokio::sync::Mutex;
use tokio::sync::Semaphore;

use bytes::Bytes;

use hdrhistogram::Histogram;

use itertools::Itertools;

use serde::Deserialize;
use serde::Serialize;

use clap::CommandFactory;
use clap::Parser;

use rand::prelude::*;

use primcast_core::config::Config;
use primcast_core::types::*;
use primcast_net::PrimcastReplica;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// group id
    #[clap(long, short)]
    gid: u8,

    /// process id
    #[clap(long, short)]
    pid: u32,

    /// outstanding messages
    #[clap(long, short, default_value_t = 1)]
    outstanding: usize,

    /// config file
    #[clap(long, short)]
    cfg: String,

    /// percentage of global commands
    #[clap(long, default_value_t = 0.0)]
    globals: f64,

    /// number of destinations for global commands
    #[clap(long, default_value_t = 2)]
    global_dests: u8,

    /// print stats to stdout every N seconds
    #[clap(long)]
    stats: Option<u64>,

    /// print debug info every N seconds
    #[clap(long)]
    debug: Option<u64>,

    /// use multi-threaded executor with N threads
    #[clap(long)]
    threads: Option<usize>,

    #[clap(long)]
    /// use hybrid logical clocks
    hybrid: bool,

    /// write deliveries to stdout
    #[clap(long)]
    check: bool,
}

#[derive(Serialize, Deserialize)]
struct Payload {
    amcast_at: Duration,
    sender: (Gid, Pid),
}

/// Periodically print stats
async fn print_stats(
    secs: u64,
    all: Arc<Mutex<Histogram<u64>>>,
    locals: Arc<Mutex<Histogram<u64>>>,
    globals: Arc<Mutex<Histogram<u64>>>,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(secs));
    let mut hists = [(all, "ALL"), (locals, "LOCALS"), (globals, "GLOBALS")];
    println!("--------------------------------");
    loop {
        interval.tick().await;
        for (h, hs) in &mut hists {
            let mut hist = h.lock().await;
            println!(
                "{} - count: {} mean_us: {} min_us: {} max_us: {} 50_us: {} 95_us: {} 99_us: {} 999_us: {}",
                hs,
                hist.len(),
                hist.mean() as usize,
                hist.min() as usize,
                hist.max() as usize,
                hist.value_at_quantile(0.5) as usize,
                hist.value_at_quantile(0.95) as usize,
                hist.value_at_quantile(0.99) as usize,
                hist.value_at_quantile(0.999) as usize,
            );
            hist.reset();
        }
    }
}

fn main() {
    let args = Args::parse();
    let cfg = Config::load(&args.cfg).unwrap();
    let mut cmd = Args::command(); // just to call .error()

    if args.globals < 0.0 || args.globals > 1.0 {
        cmd.error(clap::ErrorKind::InvalidValue, "globals percentage must be a value between 0.0 and 1.0")
            .exit()
    }
    if args.global_dests < 1 || args.global_dests as usize > cfg.groups.len() {
        cmd.error(clap::ErrorKind::InvalidValue, "global-dests larger than the number of groups in the configuration")
            .exit()
    }
    if let Some(secs) = args.stats {
        if secs == 0 {
            cmd.error(clap::ErrorKind::InvalidValue, "stats can't be 0").exit()
        }
    }

    let gid = Gid(args.gid);
    let pid = Pid(args.pid);

    // GidSet for local commands
    let local_dest: GidSet = [gid].into_iter().collect();
    // possible GidSets for globals of chosen size which include local gid
    let global_dests: Vec<_> = cfg
        .groups
        .iter()
        .cloned()
        .map(|g| g.gid)
        .combinations(args.global_dests as usize)
        .filter_map(|dest| {
            if dest.iter().find(|g| **g == gid).is_some() {
                Some(GidSet::from_iter(dest))
            } else {
                None
            }
        })
        .collect();

    let rt = if let Some(n) = args.threads {
        eprintln!("running multi-threaded executor with {} worker threads", n);
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(n)
            .enable_all()
            .build()
            .unwrap()
    } else {
        eprintln!("running single-threaded executor");
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    };

    rt.block_on(async {
        let mut handle = PrimcastReplica::start(Gid(args.gid), Pid(args.pid), cfg, args.hybrid, args.debug);
        let mut delivery_rx = handle.take_delivery_rx().unwrap();

        let hist = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
        let hist_locals = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));
        let hist_globals = Arc::new(Mutex::new(Histogram::<u64>::new(3).unwrap()));

        tokio::time::sleep(Duration::from_secs(10)).await; // wait for things to settle...

        // print stats
        if let Some(secs) = args.stats {
            tokio::spawn(print_stats(secs, hist.clone(), hist_locals.clone(), hist_globals.clone()));
        }

        // can't serialize Instant, so we use Duration from start
        let start = Instant::now();

        // release outstanding msgs
        let outstanding = Arc::new(Semaphore::new(args.outstanding));

        // proposal sender
        let outstanding_consumer = outstanding.clone();
        tokio::spawn(async move {
            let mut rng = StdRng::from_entropy();
            while let Ok(permit) = outstanding_consumer.acquire().await {
                permit.forget(); // delivery will create more permits
                let id: MsgId = rng.gen();
                let dest: GidSet = if rng.gen_bool(args.globals) {
                    // global msg
                    global_dests.choose(&mut rng).cloned().unwrap()
                } else {
                    // local msg
                    local_dest.clone()
                };
                let payload = Payload {
                    amcast_at: Instant::now() - start,
                    sender: (gid, pid),
                };
                let msg: Bytes = bincode::serialize(&payload).unwrap().into();
                handle.propose(id, msg.into(), dest).await.unwrap();
            }
        });

        // handle deliveries
        while let Some((ts, id, msg, dest)) = delivery_rx.recv().await {
            if args.check {
                println!("{ts} {id} {dest:?} DELIVERY");
            }
            let payload: Payload = bincode::deserialize(&msg).unwrap();
            // if our msg, record latency and release another proposal
            if (gid, pid) == payload.sender {
                let now = Instant::now() - start;
                let lat = now - payload.amcast_at;
                let lat_usec = u64::try_from(lat.as_micros()).unwrap();
                hist.lock().await.record(lat_usec).unwrap();
                if dest.len() == 1 {
                    hist_locals.lock().await.record(lat_usec).unwrap();
                } else {
                    hist_globals.lock().await.record(lat_usec).unwrap();
                }
                // allow next proposal
                outstanding.add_permits(1);
            }
        }
    })
}
