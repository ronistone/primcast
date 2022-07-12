use std::time::Duration;

use primcast_core::config::Config;
use primcast_core::types::*;
use primcast_net::PrimcastReplica;

use serde::{Serialize, Deserialize};
use tokio::runtime;

use clap::Parser;

use rand::seq::SliceRandom;
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
}

fn main() {
    let args = Args::parse();

    let rt = runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let cfg = Config::load(&args.cfg).unwrap();

    let mut rng = rand::thread_rng();
    let start = Instant::now();

    rt.block_on(async {
        let mut handle = PrimcastReplica::start(Gid(args.gid), Pid(args.pid), cfg);
        let dest0: GidSet = [Gid(0)].into_iter().collect();
        let dest1: GidSet = [Gid(1)].into_iter().collect();
        let dest0_1: GidSet = [Gid(0), Gid(1)].into_iter().collect();
        tokio::time::sleep(Duration::from_secs(2)).await; // wait for things to settle...

        // if (args.gid,args.pid) == (0, 0) {
        //     loop {
        //         // tokio::time::sleep(Duration::from_millis(1)).await;
        //         let id: MsgId = rand::random();
        //         handle.propose(id, format!("{:?}", id).into_bytes(), dest.clone()).await.unwrap();
        //     }
        // } else {
        //     futures::future::pending::<()>().await;
        // }

        // println!("DELIVERY - final_ts:{:?} dest:{:?} msg_id:{:?}", d.final_ts.unwrap(), &d.dest, d.msg_id);
        let mut rx = handle.take_delivery_rx().unwrap();
        tokio::spawn(async move {
            while let Some((ts, id, msg, dest)) = rx.recv().await {
                let payload: Payload = bincode::deserialize(&msg).unwrap();
                if (Gid(args.gid), Pid(args.pid)) == payload.sender {
                    let now = Instant::now() - start;
                    let lat = now - payload.amcast_at;
                    println!("DELIVERY - final_ts:{:?} dest:{:?} msg_id:{:?} after {}usec", ts, dest, id, lat.as_micros());
                } else {
                    println!("DELIVERY - final_ts:{:?} dest:{:?} msg_id:{:?}", ts, dest, id);
                }
            }
        });

        loop {
            // tokio::time::sleep(Duration::from_millis(1)).await;
            let id: MsgId = rng.gen();
            let dest = *[&dest0_1, &dest0, &dest1].choose(&mut rng).unwrap();
            let payload = Payload {
                amcast_at: Instant::now() - start,
                sender: (Gid(args.gid), Pid(args.pid)),
            };
            handle
                .propose(id, bincode::serialize(&payload).unwrap().into(), dest.clone())
                .await
                .unwrap();
            tokio::task::yield_now().await;
        }
    });
}
