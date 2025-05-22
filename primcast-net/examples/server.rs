use std::sync::Arc;

use bytes::Bytes;

use rustc_hash::FxHashMap as HashMap;

use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

use clap::CommandFactory;
use clap::Parser;

use rand::prelude::*;

use primcast_core::config::Config;
use primcast_core::types::*;
use primcast_net::codec::*;
use primcast_net::PrimcastHandle;
use primcast_net::PrimcastReplica;

use futures::prelude::*;

mod shared;
use shared::*;

type ReplyTx = mpsc::UnboundedSender<(Clock, Bytes, GidSet)>;
type ReplyRx = mpsc::UnboundedReceiver<(Clock, Bytes, GidSet)>;
type RequestMap = Arc<Mutex<HashMap<MsgId, ReplyTx>>>;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// group id
    #[clap(long, short)]
    gid: u8,

    /// process id
    #[clap(long, short)]
    pid: u32,

    /// config file
    #[clap(long, short)]
    cfg: String,

    /// print debug info every N seconds
    #[clap(long)]
    debug: Option<u64>,

    /// print stats to stdout every N seconds
    #[clap(long)]
    stats: Option<u64>,

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

async fn listen_for_clients(
    gid: Gid,
    pid: Pid,
    cfg: Config,
    req_tx: mpsc::UnboundedSender<(Bytes, GidSet, ReplyTx)>,
) -> std::io::Result<()> {
    let addr = cfg.peer(gid, pid).expect("gid/pid not in config").addr_for_client();
    let listener = TcpListener::bind(addr).await?;
    while let Ok((sock, _addr)) = listener.accept().await {
        sock.set_nodelay(true).unwrap();
        let (rx, tx) = bincode_split(sock);
        let (reply_tx, reply_rx) = mpsc::unbounded_channel();
        tokio::spawn(client_recv(gid, rx, req_tx.clone(), reply_tx));
        tokio::spawn(client_send(tx, reply_rx));
    }
    Ok(())
}

async fn client_recv(
    gid: Gid,
    mut rx: BincodeReader<Request, TcpStream>,
    req_tx: mpsc::UnboundedSender<(Bytes, GidSet, ReplyTx)>,
    reply_tx: ReplyTx,
) {
    while let Some(Ok(req)) = rx.next().await {
        assert!(req.dest.contains(gid), "dest must include the local group");
        if let Err(_) = req_tx.send((req.msg, req.dest, reply_tx.clone())) {
            break;
        }
    }
}

async fn client_send(mut tx: BincodeWriter<Reply, TcpStream>, mut reply_rx: ReplyRx) -> std::io::Result<()> {
    while let Some((ts, msg, dest)) = reply_rx.recv().await {
        // write back to client
        tx.send(Reply { ts, msg, dest }).await?;
    }
    Ok(())
}

/// register and propose client requests
async fn propose(
    request_map: RequestMap,
    mut handle: PrimcastHandle,
    mut req_rx: mpsc::UnboundedReceiver<(Bytes, GidSet, ReplyTx)>,
) -> Result<(), ()> {
    let mut rng = StdRng::from_entropy();
    while let Some((msg, dest, reply_tx)) = req_rx.recv().await {
        let mid = rng.gen();
        let mut request_map = request_map.lock().await;
        use std::collections::hash_map::Entry;
        match request_map.entry(mid) {
            Entry::Occupied(_) => panic!("conflicting msg id generated"),
            Entry::Vacant(e) => {
                e.insert(reply_tx);
            }
        }
        handle.propose(mid, msg, dest).await.unwrap();
    }
    todo!()
}

/// deliver msgs and send back reply to originating client
async fn deliver(
    request_map: RequestMap,
    mut delivery_rx: mpsc::Receiver<(Clock, MsgId, Bytes, GidSet)>,
    check: bool,
) -> Result<(), ()> {
    while let Some((ts, id, data, dest)) = delivery_rx.recv().await {
        if check {
            println!("{ts} {id} {dest:?} DELIVERY");
        }

        let mut request_map = request_map.lock().await;
        if let Some(reply_tx) = request_map.remove(&id) {
            if let Err(_) = reply_tx.send((ts, data, dest)) {
                eprintln!("client disconnected before reply");
            }
        }
    }
    todo!()
}

fn main() {
    let args = Args::parse();
    let cfg = Config::load(&args.cfg).unwrap();
    let mut cmd = Args::command(); // just to call .error()

    if let Some(secs) = args.stats {
        if secs == 0 {
            cmd.error(clap::ErrorKind::InvalidValue, "stats can't be 0").exit()
        }
    }

    let gid = Gid(args.gid);
    let pid = Pid(args.pid);

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
        let mut handle = PrimcastReplica::start(Gid(args.gid), Pid(args.pid), cfg.clone(), args.hybrid, args.debug).await;
        let delivery_rx = handle.take_delivery_rx().unwrap();

        let (req_tx, req_rx) = mpsc::unbounded_channel();

        let request_map = Arc::new(Mutex::new(HashMap::default()));

        tokio::spawn(propose(request_map.clone(), handle, req_rx));
        tokio::spawn(deliver(request_map, delivery_rx, args.check));

        listen_for_clients(gid, pid, cfg, req_tx).await.unwrap()
    })
}
