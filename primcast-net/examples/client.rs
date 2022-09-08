use std::sync::Arc;
use std::time::Instant;

use tokio::net::TcpStream;
use tokio::sync::Semaphore;

use serde::Deserialize;
use serde::Serialize;

use bincode;

use clap::CommandFactory;
use clap::Parser;

use itertools::Itertools;

use bytes::Bytes;
use bytes::BytesMut;

use rand::prelude::*;

use primcast_core::config::Config;
use primcast_core::types::*;
use primcast_net::codec::*;

use futures::prelude::*;

mod shared;
use shared::*;

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

    /// message size in bytes
    #[clap(long, short)]
    size: u32,

    /// config file
    #[clap(long, short)]
    cfg: String,

    /// percentage of global commands
    #[clap(long, default_value_t = 0.0)]
    globals: f64,

    /// number of destinations for global commands
    #[clap(long, default_value_t = 2)]
    global_dests: u8,
}

#[derive(Debug, Serialize, Deserialize)]
struct Payload {
    sent_at_us: u64,
    data: Bytes,
}

fn main() {
    let args = Args::parse();
    let cfg = Config::load(&args.cfg).unwrap();
    let mut cmd = Args::command(); // just to call .error()

    let gid = Gid(args.gid);
    let pid = Pid(args.pid);

    if args.globals < 0.0 || args.globals > 1.0 {
        cmd.error(clap::ErrorKind::InvalidValue, "globals percentage must be a value between 0.0 and 1.0")
            .exit();
    }
    if args.global_dests < 1 || args.global_dests as usize > cfg.groups.len() {
        cmd.error(clap::ErrorKind::InvalidValue, "global-dests larger than the number of groups in the configuration")
            .exit();
    }

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

    let mut rng = StdRng::from_entropy();

    if args.size < 8 {
        cmd.error(clap::ErrorKind::InvalidValue, "msg size must be at least 8 bytes")
            .exit();
    }

    let data_size = args.size - 8; // we add an extra u64 timestamp
    let mut data = BytesMut::with_capacity(data_size as usize);
    unsafe { data.set_len(data_size as usize) };
    rng.fill(&mut data[..]);
    let data = data.freeze();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        // connect to replica
        let addr = cfg.peer(gid, pid).expect("gid/pid not in config").addr_for_client();
        let sock = match TcpStream::connect(addr).await {
            Ok(sock) => sock,
            Err(err) => {
                eprintln!("error connecting to server at {addr}: {err}");
                return;
            }
        };
        eprintln!("connected to {gid:?}:{pid:?} at {addr}");
        let (mut rx, mut tx) = bincode_split::<Reply, Request, _>(sock);

        // keep outstanding requests
        let outstanding = Arc::new(Semaphore::new(args.outstanding));
        let outstanding_consumer = outstanding.clone();

        let start = Instant::now();

        // send requests
        tokio::spawn(async move {
            while let Ok(permit) = outstanding_consumer.acquire().await {
                permit.forget(); // create permits when receiving replies
                let dest: GidSet = if rng.gen_bool(args.globals) {
                    // global msg
                    global_dests.choose(&mut rng).cloned().unwrap()
                } else {
                    // local msg
                    local_dest.clone()
                };
                let payload = Payload {
                    sent_at_us: u64::try_from((Instant::now() - start).as_micros()).unwrap(),
                    data: data.clone(),
                };
                let msg = bincode::serialize(&payload).expect("could not serialize payload");
                tx.send(Request { dest, msg: msg.into() })
                    .await
                    .expect("error sending request");
            }
        });

        // read replies and allow more requests.
        // Then, print stats upon exit.
        let mut stats = Vec::with_capacity(10_000_000);
        let ctrl_c = tokio::signal::ctrl_c();
        tokio::pin!(ctrl_c);
        loop {
            tokio::select! {
                some_res = rx.next() => {
                    match some_res {
                        Some(Ok(Reply { msg, dest, .. })) => {
                            let payload: Payload = bincode::deserialize(&msg).expect("error: deserializing reply");
                            let now_us = u64::try_from((Instant::now() - start).as_micros()).unwrap();
                            let latency_us = now_us - payload.sent_at_us;
                            let n_dest = dest.len();
                            // allow next request
                            outstanding.add_permits(1);
                            // latency / deliver time from start / number of dests
                            stats.push((latency_us, now_us, n_dest));
                        }
                        _ => {
                            break;
                        }
                    }
                }
                _ = &mut ctrl_c => {
                    break;
                }
            }
        }
        let mut count = 0;
        eprintln!("printing stats...");
        println!("# ORDER\tLATENCY\tABS\tDLEN");
        for (latency_us, time_from_start_us, n_dest) in stats {
            count += 1;
            println!("{count}\t{latency_us}\t{time_from_start_us}\t{n_dest}");
        }
    })
}
