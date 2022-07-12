use std::time::Duration;

use primcast_core::config::Config;
use primcast_core::types::*;
use primcast_net::PrimcastReplica;

use tokio::runtime;

use clap::Parser;

use rand::seq::SliceRandom;
use rand::Rng;

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// group id
    #[clap(long)]
    gid: u8,

    /// process id
    #[clap(long)]
    pid: u32,
}

fn main() {
    let args = Args::parse();

    let rt = runtime::Builder::new_current_thread().enable_all().build().unwrap();

    let cfg = Config::load("../example.yaml").unwrap();

    let mut rng = rand::thread_rng();

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

        loop {
            // tokio::time::sleep(Duration::from_millis(1)).await;
            let id: MsgId = rng.gen();
            let dest = *[&dest0_1, &dest0, &dest1].choose(&mut rng).unwrap();
            handle
                .propose(id, bincode::serialize(&id).unwrap(), dest.clone())
                .await
                .unwrap();
            tokio::task::yield_now().await;
        }
    });
}
