use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;

use futures::future;
use futures::stream::FuturesUnordered;
use futures::Future;
use futures::FutureExt;
use futures::SinkExt;
use futures::StreamExt;

use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::watch;
use tokio::sync::RwLock;

use messages::Message;
use primcast_core::config;
use primcast_core::config::PeerConfig;
use primcast_core::types::*;
use primcast_core::GroupReplica;
use primcast_core::ReplicaState;

use rustc_hash::FxHashMap;

mod codec;
mod conn;
mod messages;

use conn::Conn;

const RETRY_TIMEOUT: Duration = Duration::from_secs(2);
const PROPOSAL_QUEUE: usize = 1000;
const DELIVERY_QUEUE: usize = 1000;
const BATCH_SIZE_YIELD: usize = 10;

pub struct ShutdownHandle(oneshot::Sender<()>);
#[derive(Clone)]
pub struct Shutdown(future::Shared<oneshot::Receiver<()>>);

impl Future for Shutdown {
    type Output = ();

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.0.poll_unpin(cx).map(|_| ())
    }
}

impl Shutdown {
    pub fn new() -> (Self, ShutdownHandle) {
        let (tx, rx) = oneshot::channel();
        (Shutdown(rx.shared()), ShutdownHandle(tx))
    }
}

async fn with_shutdown<T>(shutdown: Shutdown, future: T) -> Option<T::Output>
where
    T: Future + Send + 'static,
    T::Output: Send + 'static,
{
    tokio::select! {
        _ = shutdown => {
            None
        }
        res = future => {
            Some(res)
        }
    }
}

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Core(primcast_core::Error),
    HandshakeFail,
    ReplicaShutdown,
    NotLeader(Epoch),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<primcast_core::Error> for Error {
    fn from(err: primcast_core::Error) -> Error {
        Error::Core(err)
    }
}

impl From<watch::error::RecvError> for Error {
    fn from(_err: watch::error::RecvError) -> Error {
        Error::ReplicaShutdown
    }
}

impl<T> From<watch::error::SendError<T>> for Error {
    fn from(_err: watch::error::SendError<T>) -> Error {
        Error::ReplicaShutdown
    }
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(_err: mpsc::error::SendError<T>) -> Error {
        Error::ReplicaShutdown
    }
}

pub struct PrimcastReplica {
    gid: Gid,
    pid: Pid,
    cfg: config::Config,
    shared: Arc<RwLock<Shared>>,
    ev_rx: mpsc::UnboundedReceiver<Event>,
    proposal_rx: mpsc::Receiver<(MsgId, Bytes, GidSet)>,
}

pub struct Shared {
    core: GroupReplica,
    update_tx: watch::Sender<()>,
    update_rx: watch::Receiver<()>,
    ack_tx: watch::Sender<(Epoch, u64, Clock)>,
    ack_rx: watch::Receiver<(Epoch, u64, Clock)>,
    ev_tx: mpsc::UnboundedSender<Event>,
    proposal_tx: mpsc::Sender<(MsgId, Bytes, GidSet)>,
    shutdown: Shutdown,
}

enum Event {
    PeriodicChecks(Instant),
    Follow(Conn, Epoch),
}

pub struct PrimcastHandle {
    _shutdown: ShutdownHandle,
    gid_proposal_tx: FxHashMap<Gid, mpsc::Sender<(MsgId, Bytes, GidSet)>>,
    delivery_rx: Option<mpsc::Receiver<(Clock, MsgId, Bytes, GidSet)>>,
}

impl PrimcastHandle {
    pub async fn propose(&mut self, msg_id: MsgId, msg: Bytes, dest: GidSet) -> Result<(), Error> {
        for gid in dest.iter() {
            self.gid_proposal_tx
                .get_mut(gid)
                .unwrap()
                .send((msg_id, msg.clone(), dest.clone()))
                .await?;
        }
        Ok(())
    }

    pub fn take_delivery_rx(&mut self) -> Option<mpsc::Receiver<(Clock, MsgId, Bytes, GidSet)>> {
        self.delivery_rx.take()
    }

    pub async fn deliver(&mut self) -> Option<(Clock, MsgId, Bytes, GidSet)> {
        if let Some(ref mut rx) = self.delivery_rx {
            rx.recv().await
        } else {
            None
        }
    }

    pub async fn shutdown(self) {}
}

impl PrimcastReplica {
    pub fn start(gid: Gid, pid: Pid, cfg: config::Config) -> PrimcastHandle {
        let core = GroupReplica::new(gid, pid, cfg.clone());
        let (log_epoch, log_len) = core.log_status();
        let clock = core.clock();

        let (ack_tx, ack_rx) = watch::channel((log_epoch, log_len, clock));
        let (update_tx, update_rx) = watch::channel(());
        let (ev_tx, ev_rx) = mpsc::unbounded_channel();
        let (proposal_tx, proposal_rx) = mpsc::channel(PROPOSAL_QUEUE);
        let (delivery_tx, delivery_rx) = mpsc::channel(DELIVERY_QUEUE);
        let (shutdown, shutdown_handle) = Shutdown::new();

        let shared = Shared {
            core,
            update_tx,
            update_rx,
            ack_tx,
            ack_rx,
            ev_tx,
            proposal_tx: proposal_tx.clone(),
            shutdown,
        };

        let s = Self {
            gid,
            pid,
            cfg: cfg.clone(),
            shared: Arc::new(RwLock::new(shared)),
            proposal_rx,
            ev_rx,
        };

        // remote proposers
        let mut remote_proposal_tx = FxHashMap::default();
        for g in s.cfg.groups.iter() {
            let (tx, rx) = mpsc::channel(PROPOSAL_QUEUE);
            tokio::spawn(proposal_sender((gid, pid), g.gid, cfg.clone(), rx));
            remote_proposal_tx.insert(g.gid, tx);
        }

        tokio::spawn(s.run(delivery_tx));
        PrimcastHandle {
            _shutdown: shutdown_handle,
            delivery_rx: Some(delivery_rx),
            gid_proposal_tx: remote_proposal_tx,
        }
    }

    async fn run(mut self, delivery_tx: mpsc::Sender<(Clock, MsgId, Bytes, GidSet)>) -> Result<(), Error> {
        println!("starting replica {:?}:{:?}", self.gid, self.pid);

        // start acceptor task
        let addr = self.cfg.peer(self.gid, self.pid).expect("gid/pid not in config").addr();
        println!("accepting connections in {:?}", addr);
        let listener = tokio::net::TcpListener::bind(addr).await?;

        let mut shutdown = self.shared.read().await.shutdown.clone();

        tokio::spawn(with_shutdown(shutdown.clone(), acceptor_task(listener, self.shared.clone())));

        // start delivery task
        tokio::spawn(with_shutdown(shutdown.clone(), deliver_task(delivery_tx, self.shared.clone())));

        // {
        //     // debug printing
        //     let s = self.shared.clone();
        //     tokio::spawn(with_shutdown(shutdown.clone(), async move {
        //         loop {
        //             tokio::time::sleep(Duration::from_secs(2)).await;
        //             s.write().await.core.print_debug_info();
        //         }
        //     }));
        // }

        // request remote log/acks
        for g in &self.cfg.groups {
            let gid = g.gid;
            if gid == self.gid {
                continue;
            }
            for p in g.peers.iter().cloned() {
                let pid = p.pid;
                if p.pid == self.pid {
                    let shared = self.shared.clone();
                    tokio::spawn(with_shutdown(shutdown.clone(), async move {
                        loop {
                            if let Err(err) = remote_log_fetch(gid, p.clone(), shared.clone()).await {
                                eprintln!("error fetching remote logs from {:?}{:?}: {:?}", gid, pid, err);
                                tokio::time::sleep(RETRY_TIMEOUT).await;
                            }
                        }
                    }));
                } else {
                    let shared = self.shared.clone();
                    tokio::spawn(with_shutdown(shutdown.clone(), async move {
                        loop {
                            if let Err(err) = remote_acks_fetch(gid, p.clone(), shared.clone()).await {
                                eprintln!("error fetching remote acks from {:?}{:?}: {:?}", gid, pid, err);
                                tokio::time::sleep(RETRY_TIMEOUT).await;
                            }
                        }
                    }));
                }
            }
        }

        let (e, mut state) = self.shared.read().await.core.state();
        let mut fut: Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>;

        match state {
            ReplicaState::Promised | ReplicaState::Follower => {
                // wait for incoming connection
                fut = Box::pin(shutdown.clone().map(|_| Ok(())));
            }
            ReplicaState::Candidate => {
                fut = Box::pin(run_candidate(e, self.shared.clone()));
            }
            ReplicaState::Primary => {
                fut = Box::pin(run_primary(e, self.shared.clone()));
            }
        }

        let mut proposals = vec![];

        'main: loop {
            loop {
                tokio::select! {
                    // biased;
                    _ = &mut shutdown => break 'main,
                    Some(p) = self.proposal_rx.recv() => {

                        // try to batch a few proposals before adding to replica state
                        proposals.push(p);
                        loop {
                            tokio::select! {
                                biased;
                                Some(p) = self.proposal_rx.recv(), if proposals.len() < BATCH_SIZE_YIELD => {
                                    proposals.push(p);
                                }
                                _ = future::ready(()) => break,
                            }
                        }
                        assert!(proposals.len() <= BATCH_SIZE_YIELD);
                        {
                            let mut s = self.shared.write().await;
                            for (msg_id, msg, dest) in proposals.drain(..) {
                                match s.core.add_proposal(msg_id, msg, dest) {
                                    Ok(_) => (),
                                    Err(err) => eprintln!("error queuing proposal {:?}: {:?}", msg_id, err),
                                }
                            }
                            if let Err(err) = s.core.propose() {
                                eprintln!("error proposing: {:?}", err);
                            }
                            let (log_epoch, log_len) = s.core.log_status();
                            let clock = s.core.clock();
                            s.ack_tx.send((log_epoch, log_len, clock))?;
                        }
                    }
                    _ = &mut fut => {
                        // idle state: waiting for incoming leader connection or a timeout to become leader
                        (_, state) = self.shared.read().await.core.state();
                        assert!(state == ReplicaState::Promised || state == ReplicaState::Follower);
                        fut = Box::pin(shutdown.clone().map(|_| Ok(())));
                    },
                    Some(ev) = self.ev_rx.recv() => {
                        match ev {
                            Event::PeriodicChecks(now) => {
                                // TODO: leadership check
                            }
                            Event::Follow(conn, epoch) => {
                                (_, state) = self.shared.read().await.core.state();
                                assert!(state == ReplicaState::Promised || state == ReplicaState::Follower);
                                fut = Box::pin(run_follower(conn, epoch, self.shared.clone()));
                            }
                        }
                    }
                }
            }
        }

        println!("replica shutting down...");

        Ok(())
    }
}

async fn run_candidate(e: Epoch, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    println!("== CANDIDATE FOR {:?} ==", e);
    let cfg;
    let gid;
    let pid;
    {
        let s = s.read().await;
        cfg = s.core.config.clone();
        pid = s.core.pid;
        gid = s.core.gid;
    }

    let mut promise_reqs = FuturesUnordered::new();

    for p in &cfg.group(gid).unwrap().peers {
        // connect to higher pids only (lower pid, higher leadership priority)
        if p.pid > pid {
            let p = p.clone();
            let s = s.clone();
            promise_reqs.push(async move {
                loop {
                    match get_promise(p.clone(), e, s.clone()).await {
                        Err(err) => {
                            eprintln!("error getting promise from {:?}:{:?}: {:?}", gid, p.pid, err);
                            tokio::time::sleep(RETRY_TIMEOUT).await;
                        }
                        Ok(p) => break p,
                    }
                }
            });
        }
    }

    // wait for quorum promises
    let mut promise_result = None;

    while let Some(res) = promise_reqs.next().await {
        let mut s = s.write().await;
        match res {
            Ok((pid, log_epoch, log_len, clock)) => {
                if let Some(quorum_promise) = s.core.add_promise(e, pid, clock, log_epoch, log_len)? {
                    promise_result = Some(quorum_promise);
                    break;
                }
            }
            Err(higher_epoch) => {
                s.core.new_epoch_proposal(higher_epoch).unwrap();
                return Ok(());
            }
        }
    }

    let (high_pid, high_log_epoch, high_log_len, high_clock) = promise_result.unwrap();

    // sync with highest follower
    // TODO: retry?
    sync_with(cfg.peer(gid, high_pid).unwrap(), e, &s).await?;

    // TODO: sync a quorum of followers

    unimplemented!();
}

async fn run_primary(e: Epoch, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    println!("== PRIMARY OF {:?} ==", e);
    let cfg;
    let gid;
    let pid;
    {
        let s = s.read().await;
        cfg = s.core.config.clone();
        pid = s.core.pid;
        gid = s.core.gid;
    }

    let mut sync_tasks = FuturesUnordered::new();

    for p in &cfg.group(gid).unwrap().peers {
        // connect to higher pids only (lower pid, higher leadership priority)
        if p.pid > pid {
            // sync them to our log
            let p = p.clone();
            let s = s.clone();
            sync_tasks.push(async move {
                loop {
                    if let Err(err) = sync_follower(p.clone(), e, s.clone()).await {
                        eprintln!("error syncing follower {:?}:{:?}: {:?}", gid, p.pid, err);
                        tokio::time::sleep(RETRY_TIMEOUT).await;
                    }
                }
            });
        }
    }

    // a `sync_follower` task completing without error means some higher epoch was seen
    sync_tasks.next().await.unwrap()
}

async fn run_follower(conn: Conn, e: Epoch, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    println!("== FOLLOWER OF {:?} ==", e);
    let cfg;
    let gid;
    let pid;
    {
        let s = s.read().await;
        cfg = s.core.config.clone();
        pid = s.core.pid;
        gid = s.core.gid;
    }

    let (mut conn_tx, mut conn_rx) = conn.split();

    // send ack back to primary
    let send_acks_task = {
        let mut ack_rx = s.read().await.ack_rx.clone();
        async move {
            println!("sending acks to {:?}:{:?}", conn_tx.gid(), conn_tx.pid());
            loop {
                ack_rx.changed().await?;
                let (log_epoch, log_len, clock) = *ack_rx.borrow_and_update();
                conn_tx
                    .send(Message::Ack {
                        log_epoch,
                        log_len,
                        clock,
                    })
                    .await?;
            }
        }
    };
    tokio::pin!(send_acks_task);

    // fetch acks from other replicas in group
    let mut fetch_ack_tasks = FuturesUnordered::new();
    for p in &cfg.group(gid).unwrap().peers {
        if p.pid != pid {
            let p = p.clone();
            let s = s.clone();
            fetch_ack_tasks.push(async move {
                loop {
                    if let Err(err) = ack_fetch(p.clone(), s.clone()).await {
                        eprintln!("fetching acks from {:?}:{:?}: {:?}", gid, p.pid, err);
                        tokio::time::sleep(RETRY_TIMEOUT).await;
                    }
                }
            });
        }
    }

    let mut msgs = vec![];
    let err = 'recv: loop {
        // the following weird logic is used to batch and prioritize sending log entries
        tokio::select! {
            biased; // first branch always checked first
            res = conn_rx.recv() => {
                match res {
                    Ok(Message::LogAppend { idx, entry_epoch, entry }) => {
                        msgs.push((idx, entry_epoch, entry));
                        if msgs.len() < BATCH_SIZE_YIELD {
                            continue 'recv; // read more
                        }
                    }
                    Ok(Message::StartEpochAccept { epoch, prev_entry, clock }) => {
                        assert_eq!(e, epoch);
                        let mut s = s.write().await;
                        for (idx, entry_epoch, entry) in msgs.drain(..) {
                            if entry_epoch < e {
                                s.core.start_epoch_append(e, idx, entry_epoch, entry)?;
                            }
                        }
                        s.core.start_epoch_accept(e, prev_entry, clock)?;
                        let (epoch, len) = s.core.log_status();
                        s.ack_tx.send((epoch, len, s.core.clock()))?;
                        s.update_tx.send(())?;
                        continue 'recv; // read more
                    }
                    Ok(m) => panic!("unexpected message {:?}", m),
                    Err(err) => break 'recv err,
                }
            }
            res = &mut send_acks_task => return res,
            _ = &mut fetch_ack_tasks.next() => return Ok(()),
            // this default branch is only enabled when at least 1 msg is buffered
            _default = future::ready(()), if msgs.len() > 0 => {}
        }

        assert!(msgs.len() <= BATCH_SIZE_YIELD);
        {
            let mut s = s.write().await;
            for (idx, entry_epoch, entry) in msgs.drain(..) {
                if entry_epoch < e {
                    s.core.start_epoch_append(e, idx, entry_epoch, entry)?;
                } else {
                    s.core.append(e, idx, entry)?;
                }
            }
            let (epoch, len) = s.core.log_status();
            s.ack_tx.send((epoch, len, s.core.clock()))?;
            s.update_tx.send(())?;
        }
        tokio::task::yield_now().await;
    };

    // append remaining buffered msgs
    let mut s = s.write().await;
    for (idx, entry_epoch, entry) in msgs.drain(..) {
        if entry_epoch < e {
            s.core.start_epoch_append(e, idx, entry_epoch, entry)?;
        } else {
            s.core.append(e, idx, entry)?;
        }
    }
    let (epoch, len) = s.core.log_status();
    s.ack_tx.send((epoch, len, s.core.clock()))?;
    s.update_tx.send(())?;

    Err(err.into())
}

/// Accept connections and spawn handler tasks
async fn acceptor_task(listener: tokio::net::TcpListener, s: Arc<RwLock<Shared>>) {
    let gid;
    let pid;
    let shutdown;
    {
        let s = s.read().await;
        gid = s.core.gid;
        pid = s.core.pid;
        shutdown = s.shutdown.clone();
    }
    loop {
        let (sock, addr) = listener.accept().await.expect("error accepting connections");
        println!("new connection from {:?}", addr);
        sock.set_nodelay(true).unwrap();
        tokio::spawn(with_shutdown(shutdown.clone(), handle_connection((gid, pid), sock, s.clone())));
    }
}

/// Wait for the first request msg to properly handle the connection
async fn handle_connection(
    self_id: (Gid, Pid),
    sock: tokio::net::TcpStream,
    s: Arc<RwLock<Shared>>,
) -> Result<(), Error> {
    let mut conn = Conn::incoming(self_id, sock).await?;
    let shutdown = s.read().await.shutdown.clone();
    match conn.recv().await? {
        Message::AckRequest => {
            tokio::spawn(with_shutdown(shutdown.clone(), ack_send(conn, s.clone())));
        }
        Message::RemoteAckRequest => {
            tokio::spawn(with_shutdown(shutdown.clone(), ack_send(conn, s.clone())));
        }
        Message::RemoteLogRequest {
            dest,
            log_epoch,
            next_idx,
        } => {
            tokio::spawn(with_shutdown(shutdown.clone(), remote_log_send(conn, dest, log_epoch, next_idx, s.clone())));
        }
        Message::NewEpoch { epoch } => {
            let res = s.write().await.core.new_epoch_proposal(epoch);
            match res {
                Ok((log_epoch, log_len, clock)) => {
                    conn.send(Message::Promise {
                        log_epoch,
                        log_len,
                        clock,
                    })
                    .await?;
                }
                Err(primcast_core::Error::EpochTooOld { promised, .. }) => {
                    conn.send(Message::NewEpoch { epoch: promised }).await?;
                }
                _ => unreachable!(),
            }
            // TODO: interrupt main loop?
        }
        Message::StartEpochCheck { epoch, log_epochs } => {
            let res = s.write().await.core.start_epoch_check(epoch, log_epochs);
            match res {
                Ok((log_epoch, log_len)) => {
                    conn.send(Message::Following { log_epoch, log_len }).await?;
                    let ev_tx = s.read().await.ev_tx.clone();
                    ev_tx
                        .send(Event::Follow(conn, epoch))
                        .map_err(|_| Error::ReplicaShutdown)?;
                }
                Err(primcast_core::Error::EpochTooOld { promised, .. }) => {
                    conn.send(Message::NewEpoch { epoch: promised }).await?;
                }
                _ => unreachable!(),
            }
        }
        Message::ProposalStart => {
            let proposal_tx = s.read().await.proposal_tx.clone();
            let (epoch, state) = s.read().await.core.state();
            match state {
                ReplicaState::Primary | ReplicaState::Candidate => {
                    conn.send(Message::ProposalStart).await?;
                }
                _ => {
                    conn.send(Message::NewEpoch { epoch }).await?;
                    return Ok(());
                }
            }
            let mut count = 0;
            loop {
                match conn.recv().await? {
                    Message::Proposal { msg_id, msg, dest } => {
                        proposal_tx.send((msg_id, msg, dest)).await?;
                        count += 1;
                        if count >= BATCH_SIZE_YIELD {
                            count = 0;
                            tokio::task::yield_now().await;
                        }
                    }
                    m => panic!("unexpected message: {:?}", m),
                }
            }
        }

        m => panic!("unexpected message: {:?}", m),
    }
    Ok(())
}

async fn get_promise(
    peer: PeerConfig,
    e: Epoch,
    s: Arc<RwLock<Shared>>,
) -> Result<Result<(Pid, Epoch, u64, Clock), Epoch>, Error> {
    use Message::*;
    let self_gid;
    let self_pid;
    {
        let s = s.read().await;
        self_gid = s.core.gid;
        self_pid = s.core.pid;
    }
    let req = NewEpoch { epoch: e };
    let mut conn = Conn::request((self_gid, self_pid), (self_gid, peer.pid), peer.addr(), req).await?;
    match conn.recv().await? {
        NewEpoch { epoch } => Ok(Err(epoch)),
        Promise {
            log_epoch,
            log_len,
            clock,
        } => Ok(Ok((conn.pid(), log_epoch, log_len, clock))),
        m => panic!("unexpected msg {:?}", m),
    }
}

async fn deliver_task(mut delivery_tx: mpsc::Sender<(Clock, MsgId, Bytes, GidSet)>, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    let mut update_rx = s.read().await.update_rx.clone();
    let mut deliveries = vec![];
    loop {
        update_rx.changed().await?;
        let mut s = s.write().await;
        s.core.update();
        while let Some(d) = s.core.next_delivery() {
            deliveries.push(d);
        }
        drop(s); // must not await with Shared locked
        for d in deliveries.drain(..) {
            delivery_tx.send((d.final_ts.unwrap(), d.msg_id, d.msg, d.dest)).await?;
        }
    }
}

async fn sync_with(peer: &PeerConfig, e: Epoch, s: &Arc<RwLock<Shared>>) -> Result<(), Error> {
    unimplemented!()
}

async fn sync_follower(peer: PeerConfig, e: Epoch, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    use Message::*;
    let self_gid;
    let self_pid;
    let sync_log_epoch;
    {
        let s = s.read().await;
        self_gid = s.core.gid;
        self_pid = s.core.pid;
        sync_log_epoch = s.core.log_status().0;
    }
    assert_eq!(sync_log_epoch, e);
    println!("syncing follower {:?}:{:?}", self_gid, peer.pid);

    // ensure remote is promised and compatible with our log epoch
    let mut conn = {
        let promised_epoch;
        let log_epochs;
        {
            let s = s.read().await;
            (promised_epoch, _) = s.core.state();
            if promised_epoch != e {
                return Ok(()); // replica accepted higher epoch
            }
            log_epochs = s.core.log_epochs().clone();
        }
        let req = StartEpochCheck {
            epoch: promised_epoch,
            log_epochs,
        };
        Conn::request((self_gid, self_pid), (self_gid, peer.pid), peer.addr(), req).await?
    };

    let (mut follower_log_epoch, mut follower_log_len) = match conn.recv().await? {
        NewEpoch { epoch: higher_epoch } => {
            s.write().await.core.new_epoch_proposal(higher_epoch)?;
            return Ok(());
        }
        Following { log_epoch, log_len } => {
            assert!(log_epoch <= e);
            (log_epoch, log_len)
        }
        // Ack { log_epoch, log_len, clock: _ } => {
        //     assert!(log_epoch <= e);
        //     (log_epoch, log_len)
        // }
        m => panic!("unexpected msg {:?}", m),
    };

    // send log entries as they become available
    let mut ack_rx = s.read().await.ack_rx.clone();
    let mut to_send = vec![];
    loop {
        {
            let s = s.read().await;
            let (promised_epoch, _) = s.core.state();
            let (log_epoch, log_len) = s.core.log_status();
            if promised_epoch != e || log_epoch != sync_log_epoch {
                // replica changed epochs, stop task
                return Ok(());
            }
            while follower_log_len < log_len {
                let (epoch, entry) = s.core.log_entry(follower_log_len).unwrap();
                if follower_log_epoch < epoch && epoch == e {
                    // follower synced up to e
                    let clock = s.core.clock();
                    to_send.push(StartEpochAccept {
                        epoch,
                        prev_entry: (follower_log_epoch, follower_log_len),
                        clock,
                    });
                }
                to_send.push(LogAppend {
                    idx: follower_log_len,
                    entry_epoch: epoch,
                    entry: entry.clone(),
                });
                follower_log_epoch = epoch;
                follower_log_len += 1;
                if to_send.len() >= BATCH_SIZE_YIELD {
                    break;
                }
            }
        };

        assert!(to_send.len() <= BATCH_SIZE_YIELD);

        for msg in to_send.drain(..) {
            conn.feed(msg).await?;
        }
        conn.flush().await?;
        tokio::task::yield_now().await;

        'wait: loop {
            tokio::select! {
                // check if there are more log entries to send
                _ = ack_rx.changed() => {
                    let (new_log_epoch, new_log_len, _) = *ack_rx.borrow_and_update();
                    if sync_log_epoch != new_log_epoch {
                        return Ok(())
                    }
                    if new_log_len > follower_log_len {
                        // entries to send out
                        break 'wait;
                    }
                }
                // read acks from the follower
                res = conn.recv() => {
                    match res? {
                        NewEpoch { epoch } => {
                            if epoch > sync_log_epoch {
                                return Ok(());
                            }
                        }
                        Ack { log_epoch, log_len, clock } => {
                            let mut s = s.write().await;
                            s.core.add_ack(conn.pid(), log_epoch, log_len, clock)?;
                            s.ack_tx.send_modify(|(_, _, clock)| *clock = s.core.clock());
                            s.update_tx.send(())?;
                        }
                        m => panic!("unexpected message: {:?}", m),
                    }
                }
            }
        }
    }
}

async fn remote_log_fetch(remote_gid: Gid, remote_peer: PeerConfig, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    println!("fetching remote log from {:?}:{:?}", remote_gid, remote_peer.pid);
    let gid;
    let pid;
    let epoch;
    let next_idx;
    {
        let s = s.read().await;
        gid = s.core.gid;
        pid = s.core.pid;
        (epoch, next_idx) = s.core.remote_expected_entry(remote_gid);
    }
    use Message::*;
    let req = RemoteLogRequest {
        dest: gid,
        log_epoch: epoch,
        next_idx,
    };
    let mut conn = Conn::request((gid, pid), (remote_gid, remote_peer.pid), remote_peer.addr(), req).await?;

    let mut count = 0;
    loop {
        match conn.recv().await? {
            RemoteLogEpoch { log_epoch } => {
                // possibly truncate log, return to be retried
                s.write().await.core.remote_update_log_epoch(remote_gid, log_epoch)?;
                return Ok(());
            }
            RemoteLogAppend(entry) => {
                // batch?
                let mut s = s.write().await;
                s.core
                    .remote_add_ack(remote_gid, remote_peer.pid, epoch, entry.idx, entry.ts)?;
                s.core.remote_append(remote_gid, entry)?;
                s.ack_tx.send_modify(|(_, _, clock)| *clock = s.core.clock());
                s.update_tx.send(())?;
            }
            _ => panic!("unexpected message"),
        }
        count += 1;
        if count >= BATCH_SIZE_YIELD {
            tokio::task::yield_now().await;
            count = 0;
        }
    }
}

async fn ack_fetch(peer: PeerConfig, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    let self_gid = s.read().await.core.gid;
    let self_pid = s.read().await.core.pid;
    println!("fetching acks from {:?}:{:?}", self_gid, peer.pid);

    use Message::*;
    let req = RemoteAckRequest;
    let mut conn = Conn::request((self_gid, self_pid), (self_gid, peer.pid), peer.addr(), req).await?;

    // TODO: batch recvs?
    let mut count = 0;
    loop {
        match conn.recv().await? {
            Ack {
                log_epoch,
                log_len,
                clock,
            } => {
                let mut s = s.write().await;
                s.core.add_ack(conn.pid(), log_epoch, log_len, clock)?;
                s.ack_tx.send_modify(|(_, _, clock)| *clock = s.core.clock());
                s.update_tx.send(())?;
            }
            m => panic!("unexpected message: {:?}", m),
        }
        count += 1;
        if count >= BATCH_SIZE_YIELD {
            tokio::task::yield_now().await;
            count = 0;
        }
    }
}

async fn remote_acks_fetch(remote_gid: Gid, remote_peer: PeerConfig, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    let self_gid = s.read().await.core.gid;
    let self_pid = s.read().await.core.pid;
    println!("fetching remote acks from {:?}:{:?}", remote_gid, remote_peer.pid);

    use Message::*;
    let req = RemoteAckRequest;
    let mut conn = Conn::request((self_gid, self_pid), (remote_gid, remote_peer.pid), remote_peer.addr(), req).await?;

    // TODO: batch recvs?
    let mut count = 0;
    loop {
        match conn.recv().await? {
            Ack {
                log_epoch,
                log_len,
                clock,
            } => {
                let mut s = s.write().await;
                s.core
                    .remote_add_ack(conn.gid(), conn.pid(), log_epoch, log_len, clock)?;
                s.ack_tx.send_modify(|(_, _, clock)| *clock = s.core.clock());
                s.update_tx.send(())?;
            }
            m => panic!("unexpected message: {:?}", m),
        }
        count += 1;
        if count >= BATCH_SIZE_YIELD {
            tokio::task::yield_now().await;
            count = 0;
        }
    }
}

async fn ack_send(mut conn: Conn, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    println!("sending acks to {:?}:{:?}", conn.gid(), conn.pid());
    let mut ack_rx;
    {
        let s = s.read().await;
        ack_rx = s.ack_rx.clone();
    }
    use Message::*;
    loop {
        let (log_epoch, log_len, clock) = *ack_rx.borrow_and_update();
        conn.send(Ack {
            log_epoch,
            log_len,
            clock,
        })
        .await?;
        ack_rx.changed().await?;
    }
}

async fn remote_log_send(
    mut conn: Conn,
    dest: Gid,
    epoch: Epoch,
    mut next_idx: u64,
    s: Arc<RwLock<Shared>>,
) -> Result<(), Error> {
    println!("sending remote logs for {:?} starting at {:?}", dest, next_idx);
    let mut ack_rx = s.read().await.ack_rx.clone();

    use Message::*;
    let mut to_send = vec![];
    loop {
        // check that we can provide next entry
        let (log_epoch, log_len, _) = *ack_rx.borrow_and_update();
        if log_epoch != epoch {
            conn.send(RemoteLogEpoch { log_epoch }).await?;
            return Ok(());
        }
        if log_len <= next_idx {
            ack_rx.changed().await?;
            continue;
        }

        {
            let sr = s.read().await;
            let (log_epoch, log_len) = sr.core.log_status();
            // we need to double check the epoch, as it may have changed in the meanwhile
            if log_epoch != epoch {
                drop(sr); // release lock before sending msg
                conn.send(RemoteLogEpoch { log_epoch }).await?;
                return Ok(());
            }

            while next_idx < log_len && to_send.len() < BATCH_SIZE_YIELD {
                let e = sr.core.log_entry_for_remote(next_idx).unwrap();
                if e.dest.contains(dest) {
                    to_send.push(e);
                }
                next_idx += 1;
            }
        }

        assert!(to_send.len() <= BATCH_SIZE_YIELD);

        for entry in to_send.drain(..) {
            conn.feed(RemoteLogAppend(entry)).await?;
        }
        conn.flush().await?;
        tokio::task::yield_now().await;
    }
}

async fn proposal_sender(
    from: (Gid, Pid),
    to_gid: Gid,
    cfg: config::Config,
    mut rx: mpsc::Receiver<(MsgId, Bytes, GidSet)>,
) -> Result<(), Error> {
    let peers = cfg.peers(to_gid).unwrap();
    'connect: loop {
        // connect to everyone and see who is the leader
        // TODO: better remote leader selection?
        let mut connect_futs = FuturesUnordered::new();
        for p in peers {
            let opid = p.pid;
            let fut = connect_to_leader(from, (to_gid, opid), p.clone()).map(move |res| (opid, res));
            connect_futs.push(fut);
        }
        let mut conn = loop {
            match connect_futs.next().await {
                Some((opid, Ok(conn))) => {
                    println!("connected to leader {:?}:{:?}", to_gid, opid);
                    break conn;
                }
                Some((_opid, Err(Error::NotLeader(_epoch)))) => {
                    continue;
                }
                Some((opid, Err(err))) => {
                    eprintln!("error connecting to {:?}:{:?}: {:?}", to_gid, opid, err);
                    continue;
                }
                None => {
                    tokio::time::sleep(RETRY_TIMEOUT).await;
                    continue 'connect;
                }
            }
        };

        println!("forwarding proposals to {:?}:{:?}", conn.gid(), conn.pid());

        // send proposals
        loop {
            // TODO: yield more?
            let (msg_id, msg, dest) = rx.recv().await.ok_or(Error::ReplicaShutdown)?;
            if let Err(err) = conn.feed(Message::Proposal { msg_id, msg, dest }).await {
                eprintln!("error forwarding proposals to {:?}: {:?}", to_gid, err);
                break;
            }
            while let Ok((msg_id, msg, dest)) = rx.try_recv() {
                if let Err(err) = conn.feed(Message::Proposal { msg_id, msg, dest }).await {
                    eprintln!("error forwarding proposals to {:?}: {:?}", to_gid, err);
                    break;
                }
            }
            if let Err(err) = conn.flush().await {
                eprintln!("error forwarding proposals to {:?}: {:?}", to_gid, err);
                break;
            }
            tokio::task::yield_now().await;
        }
    }
}

async fn connect_to_leader(from: (Gid, Pid), to: (Gid, Pid), p: PeerConfig) -> Result<Conn, Error> {
    let req = Message::ProposalStart;
    let mut conn = Conn::request(from, to, p.addr(), req).await?;
    match conn.recv().await? {
        Message::NewEpoch { epoch } => Err(Error::NotLeader(epoch)),
        Message::ProposalStart => Ok(conn),
        m => panic!("unexpected message {:?}", m),
    }
}

#[cfg(test)]
pub mod tests {}
