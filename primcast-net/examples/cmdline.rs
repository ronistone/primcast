use std::time::Duration;

use primcast_core::config::Config;
use primcast_core::types::*;
use primcast_net::PrimcastReplica;

use serde::Deserialize;
use serde::Serialize;

use clap::Parser;

use rand::Rng;
use std::time::Instant;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// group id
    #[clap(long)]
    gid: u8,

    /// process id
    #[clap(long)]
    pid: u32,

    /// config file
    #[clap(long, short)]
    cfg: String,
}

#[derive(Serialize, Deserialize)]
struct Payload {
    amcast_at: Duration,
    sender: (Gid, Pid),
    msg: String,
}

fn main() {
    let args = Args::parse();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let cfg = Config::load(&args.cfg).unwrap();

    let mut rng = rand::thread_rng();
    let start = Instant::now();

    let (proposal_tx, mut proposal_rx) = tokio::sync::mpsc::unbounded_channel();

    // read proposals from stdin
    std::thread::spawn(move || {
        for line in std::io::stdin().lines() {
            if let Err(_) = line {
                return;
            }
            let line = line.unwrap();
            let parts: Vec<_> = line.split(":").collect();
            if parts.len() < 2 {
                continue;
            }
            let dests: GidSet = parts[0].split_whitespace().map(|g| Gid(g.parse().unwrap())).collect();
            let msg: String = parts[1..].join(":");
            proposal_tx.send((dests, msg)).unwrap();
        }
    });

    rt.block_on(async {
        let mut handle = PrimcastReplica::start(Gid(args.gid), Pid(args.pid), cfg, false, None);
        tokio::time::sleep(Duration::from_secs(2)).await; // wait for things to settle...

        let mut rx = handle.take_delivery_rx().unwrap();
        tokio::spawn(async move {
            while let Some((ts, id, msg, dest)) = rx.recv().await {
                let payload: Payload = bincode::deserialize(&msg).unwrap();
                if (Gid(args.gid), Pid(args.pid)) == payload.sender {
                    let now = Instant::now() - start;
                    let lat = now - payload.amcast_at;
                    println!(
                        "DELIVERY: \"{}\" - final_ts:{:?} dest:{:?} msg_id:{:?} after {}usec",
                        payload.msg,
                        ts,
                        dest,
                        id,
                        lat.as_micros()
                    );
                } else {
                    println!("DELIVERY: \"{}\" - final_ts:{:?} dest:{:?} msg_id:{:?}", payload.msg, ts, dest, id);
                }
            }
        });

        while let Some((dest, msg)) = proposal_rx.recv().await {
            // tokio::time::sleep(Duration::from_millis(1)).await;
            println!("sending multicast to {:?}", dest);
            let id: MsgId = rng.gen();
            let payload = Payload {
                amcast_at: Instant::now() - start,
                sender: (Gid(args.gid), Pid(args.pid)),
                msg,
            };
            handle
                .propose(id, bincode::serialize(&payload).unwrap().into(), dest)
                .await
                .unwrap();
            tokio::task::yield_now().await;
        }
    });
}
