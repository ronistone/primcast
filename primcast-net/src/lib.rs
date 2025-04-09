use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use bytes::Bytes;

use futures::stream::FuturesUnordered;
use futures::Future;
use futures::FutureExt;
use futures::SinkExt;
use futures::StreamExt;

use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::sync::RwLock;

use tokio_stream::wrappers::ReceiverStream;

use rustc_hash::FxHashMap as HashMap;
use tokio::time::sleep;
use primcast_core::config;
use primcast_core::config::PeerConfig;
use primcast_core::types::*;
use primcast_core::GroupReplica;
use primcast_core::ReplicaState;

pub mod codec;
pub mod conn;
mod messages;
pub mod util;
pub mod leader_election;

use conn::Conn;
use messages::Message;
use util::AbortHandle;
use util::RoundRobinStreams;
use util::Shutdown;
use util::ShutdownHandle;
use util::StreamExt2;
use crate::leader_election::LeaderElection;

const RETRY_TIMEOUT: Duration = Duration::from_secs(3);
const PROPOSAL_QUEUE: usize = 100_000;
const DELIVERY_QUEUE: usize = 100_000;
const BATCH_SIZE_YIELD: usize = 50;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Core(primcast_core::Error),
    HandshakeFail,
    ReplicaShutdown,
    NotLeader(Epoch),
    InvalidGid(Gid),
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
}

pub struct Shared {
    core: GroupReplica,
    update_tx: watch::Sender<()>,
    update_rx: watch::Receiver<()>,
    ack_tx: HashMap<Gid, watch::Sender<(Epoch, u64, Clock)>>,
    ack_rx: HashMap<Gid, watch::Receiver<(Epoch, u64, Clock)>>,
    ev_tx: mpsc::UnboundedSender<Event>,
    proposal_tx: mpsc::Sender<(MsgId, Bytes, GidSet)>,
    shutdown: Shutdown,
}

enum Event {
    PeriodicChecks(Instant),
    Follow(Conn, Epoch),
    InitiateEpoch(Epoch),
    ProposalIn(mpsc::Receiver<(MsgId, Bytes, GidSet)>),
}

pub struct PrimcastHandle {
    _shutdown: ShutdownHandle,
    gid_proposal_tx: HashMap<Gid, mpsc::Sender<(MsgId, Bytes, GidSet)>>,
    delivery_rx: Option<mpsc::Receiver<(Clock, MsgId, Bytes, GidSet)>>,
}

impl PrimcastHandle {
    pub async fn propose(&mut self, msg_id: MsgId, msg: Bytes, dest: GidSet) -> Result<(), Error> {
        for gid in dest.iter() {
            self.gid_proposal_tx
                .get_mut(gid)
                .ok_or(Error::InvalidGid(*gid))?
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
    pub async fn start(gid: Gid, pid: Pid, cfg: config::Config, hybrid_clock: bool, debug: Option<u64>) -> PrimcastHandle {
        let mut leader_election = LeaderElection::new(gid, pid, cfg.clone());
        let (ev_tx, mut election_rcv) = mpsc::unbounded_channel();
        leader_election.subscribe(ev_tx);
        tokio::spawn(leader_election.run());

        let mut actual_epoch = Epoch::initial();
        match election_rcv.recv().await {
            Some(Event::InitiateEpoch(epoch)) => {
                println!("Initiating epoch {:?}", epoch);
                actual_epoch = epoch;
            }
            _ => unreachable!(),
        }

        let core = GroupReplica::new(gid, pid, actual_epoch, cfg.clone(), hybrid_clock);
        let (log_epoch, log_len) = core.log_status();
        let clock = core.clock();

        let mut ack_tx = HashMap::default();
        let mut ack_rx = HashMap::default();
        for gid in cfg.groups.iter().map(|g| g.gid) {
            let (tx, rx) = watch::channel((log_epoch, log_len, clock));
            ack_tx.insert(gid, tx);
            ack_rx.insert(gid, rx);
        }

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
            proposal_tx,
            shutdown,
        };

        let s = Self {
            gid,
            pid,
            cfg: cfg.clone(),
            shared: Arc::new(RwLock::new(shared)),
            ev_rx,
        };

        let leader_election_shared = s.shared.clone();
        tokio::spawn(async move {
            let self_pid = pid.clone();
            loop {
                tokio::select! {
                    Some(ev) = election_rcv.recv() => {
                        match ev {
                            Event::InitiateEpoch(epoch) => {
                                println!("Initiating epoch {:?}", epoch);
                                let mut lock = leader_election_shared.write().await;
                                if epoch.1 == self_pid {
                                    lock.core.propose_new_epoch(Some(epoch)).unwrap();
                                }
                            }
                            _ => unreachable!(),
                        }
                    }
                    else => {}
                }
            }
        });

        let mut gid_proposal_tx = HashMap::default();
        // create a task forwarding proposals to leader of each group
        for g in s.cfg.groups.iter() {
            let (tx, rx) = mpsc::channel(PROPOSAL_QUEUE);
            tokio::spawn(proposal_sender((gid, pid), g.gid, cfg.clone(), rx, s.shared.clone()));
            gid_proposal_tx.insert(g.gid, tx);
        }

        tokio::spawn(s.run(proposal_rx, delivery_tx, debug));
        PrimcastHandle {
            _shutdown: shutdown_handle,
            delivery_rx: Some(delivery_rx),
            gid_proposal_tx,
        }
    }

    async fn run(
        mut self,
        proposal_rx: mpsc::Receiver<(MsgId, Bytes, GidSet)>,
        delivery_tx: mpsc::Sender<(Clock, MsgId, Bytes, GidSet)>,
        debug: Option<u64>,
    ) -> Result<(), Error> {
        eprintln!("starting replica {:?}:{:?}", self.gid, self.pid);

        // start acceptor task
        let addr = self.cfg.peer(self.gid, self.pid).expect("gid/pid not in config").addr();
        let listener = tokio::net::TcpListener::bind(addr).await?;

        let mut abort_handles = vec![];

        if let Some(secs) = debug {
            // debug printing
            let s = self.shared.clone();
            abort_handles.push(AbortHandle::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(secs)).await;
                    s.write().await.core.print_debug_info();
                }
            }));
        }

        // task accepting connections
        abort_handles.push(AbortHandle::spawn(acceptor_task(listener, self.shared.clone())));
        // task a-delivering msgs
        abort_handles.push(AbortHandle::spawn(deliver_task(delivery_tx, self.shared.clone())));

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
                    abort_handles.push(AbortHandle::spawn(async move {
                        loop {
                            if let Err(err) = remote_log_fetch(gid, p.clone(), shared.clone()).await {
                                eprintln!("error fetching remote logs from {gid:?}{pid:?}: {err:?}");
                                tokio::time::sleep(RETRY_TIMEOUT).await;
                            }
                        }
                    }));
                } else {
                    let shared = self.shared.clone();
                    abort_handles.push(AbortHandle::spawn(async move {
                        loop {
                            if let Err(err) = remote_acks_fetch(gid, p.clone(), shared.clone()).await {
                                eprintln!("error fetching remote acks from {gid:?}{pid:?}: {err:?}");
                                tokio::time::sleep(RETRY_TIMEOUT).await;
                            }
                        }
                    }));
                }
            }
        }

        // Main replica future, driving the different states a replica may be
        // in. Whenever this future completes, we replace it with a new one
        // based on the replicas current state.
        let mut fut: Pin<Box<dyn Future<Output = Result<(), Error>> + Send>>;
        fut = Box::pin(futures::future::ready(Ok(())));

        // Main loop consists in:
        // - driving the main replica future (run_follower/candidate/primary)
        // - passing proposals to the primcast core state
        // - handling Events
        // The main loop stops when shutdown resolves
        let mut shutdown = self.shared.read().await.shutdown.clone();
        eprintln!("starting main loop...");
        let mut proposals: Vec<(MsgId, Bytes, GidSet)> = vec![];
        let mut rr_proposals_rx = RoundRobinStreams::new();
        rr_proposals_rx.push(ReceiverStream::new(proposal_rx));
        'main: loop {
            // for tracking to which dests an ack should be sent
            let mut ack_dests = GidSet::new();
            loop {
                tokio::select! {
                    // biased;
                    _ = &mut fut => {
                        // when the main task resolves, become idle
                        let (e, state) = self.shared.read().await.core.state();
                        match state {
                            ReplicaState::Promised | ReplicaState::Follower => {
                                fut = Box::pin(run_idle());
                            }
                            ReplicaState::Candidate => {
                                fut = Box::pin(run_candidate(e, self.shared.clone()));
                            }
                            ReplicaState::Primary => {
                                fut = Box::pin(run_primary(e, self.shared.clone()));
                            }
                        }
                    },
                    Some(ev) = self.ev_rx.recv() => {
                        match ev {
                            Event::Follow(conn, epoch) => {
                                // new connection from a leader
                                let (e, state) = self.shared.read().await.core.state();
                                if e == epoch && (state == ReplicaState::Promised || state == ReplicaState::Follower) {
                                    fut = Box::pin(run_follower(conn, epoch, self.shared.clone()));
                                } else {
                                    eprintln!("=================== [ignoring follow event for epoch {:?} in state {:?}] ========================", epoch, state);
                                    fut = Box::pin(run_idle());
                                }
                            }
                            Event::ProposalIn(rx) => {
                                rr_proposals_rx.push(ReceiverStream::new(rx));
                            }
                            Event::PeriodicChecks(_now) => {
                                // TODO:
                            }
                            Event::InitiateEpoch(epoch) => {
                                // new epoch proposal
                            }
                        }
                    }
                    n = rr_proposals_rx.next_ready_chunk(BATCH_SIZE_YIELD, &mut proposals) => {
                        if n == 0 {
                            break;
                        }
                        // handle next batch of proposals

                        let mut s = self.shared.write().await;
                        for (msg_id, msg, dest) in proposals.drain(..) {
                            ack_dests.merge(&dest);
                            match s.core.add_proposal(msg_id, msg, dest) {
                                Ok(_) => (),
                                Err(err) => eprintln!("error queuing proposal {msg_id}: {err:?}"),
                            }
                        }
                        if let Err(err) = s.core.propose() {
                            eprintln!("error proposing: {err:?}");
                        }
                        let (log_epoch, log_len) = s.core.log_status();
                        let clock = s.core.clock();
                        for gid in ack_dests.0.drain(..) {
                            s.ack_tx[&gid].send((log_epoch, log_len, clock))?;
                        }
                    }
                    _ = &mut shutdown => break 'main,
                }
            }
        }

        eprintln!("replica shutting down...");

        Ok(())
    }
}

/// Replica is waiting for a connection from the leader.
async fn run_idle() -> Result<(), Error> {
    eprintln!("== IDLE ==");
    // TODO: become candidate on timeout here?

    sleep(Duration::from_secs(1)).await;
    eprintln!("Exiting idle...");

    Ok(())
}

/// Run candidate logic:
/// - Get a quorum of promises from the group (including itself)
/// - Sync up with most up-to-date in quorum
/// - Get a quorum of replicas synchronized
async fn run_candidate(e: Epoch, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    eprintln!("== CANDIDATE FOR {e:?} ==");
    let cfg;
    let self_gid;
    let self_pid;
    {
        let s = s.read().await;
        cfg = s.core.config.clone();
        self_pid = s.core.pid;
        self_gid = s.core.gid;
    }

    let mut promise_reqs = FuturesUnordered::new();

    for p in &cfg.group(self_gid).unwrap().peers {
        // connect to higher pids only (lower pid, higher leadership priority)
        if p.pid != self_pid {
            let p = p.clone();
            let s = s.clone();
            promise_reqs.push(AbortHandle::spawn(async move {
                loop {
                    match get_promise(p.clone(), e, s.clone()).await {
                        Err(err) => {
                            eprintln!("error getting promise from {:?}:{:?}: {:?}", self_gid, p.pid, err);
                            tokio::time::sleep(RETRY_TIMEOUT).await;
                        }
                        Ok(p) => break p
                    }
                }
            }));
        }
    }

    // wait for quorum promises
    let mut promise_result = None;

    while let Some(join_result) = promise_reqs.next().await {
        match join_result {
            Ok(res) => {
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
            Err(err) => {
                assert!(!err.is_panic());
                std::panic::resume_unwind(err.into_panic());
            }
        }

    }

    let (high_pid, _high_log_epoch, _high_log_len, _high_clock) = promise_result.unwrap();

    // sync with highest follower
    // TODO: retry?
    sync_with(cfg.peer(self_gid, high_pid).unwrap(), e, &s).await?;

    // TODO: sync a quorum of followers --> The sync_followers() in run_primary do that?
    let mut state = s.write().await;
    state.core.start_new_epoch(e).unwrap();
    //TODO Roni now is take error to sync followers


    // unimplemented!();
    Ok(())
}

/// Run primary logic. Connect to higher pids (lower pid => leadership priority) and keep them synced with our log.
/// Returns upon discovering a higher epoch.
async fn run_primary(e: Epoch, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    eprintln!("== PRIMARY OF {e:?} ==");
    let cfg;
    let self_gid;
    let self_pid;
    {
        let s = s.read().await;
        cfg = s.core.config.clone();
        self_pid = s.core.pid;
        self_gid = s.core.gid;
    }

    let mut sync_tasks = FuturesUnordered::new();
    for p in &cfg.group(self_gid).unwrap().peers {
        // connect to higher pids only (lower pid, higher leadership priority)
        if p.pid != self_pid {
            // sync them to our log
            let p = p.clone();
            let s = s.clone();
            let task = async move {
                loop {
                    if let Err(err) = sync_follower(p.clone(), e, s.clone()).await {
                        eprintln!("error syncing follower {:?}:{:?}: {:?}", self_gid, p.pid, err);
                        tokio::time::sleep(RETRY_TIMEOUT).await;
                    }
                }
            };
            let abort_handle = AbortHandle::spawn(task);
            sync_tasks.push(abort_handle);
        }
    }

    // a `sync_follower` task completing without error means some higher epoch was seen
    if let Err(err) = sync_tasks.next().await.unwrap() {
        assert!(!err.is_panic());
        std::panic::resume_unwind(err.into_panic());
    }
    Ok(())
}

/// Run follower logic, handling msgs from the leader connection.
/// Returns when the connection to the leader is lost or we get promised for a higher epoch.
async fn run_follower(conn: Conn, e: Epoch, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    eprintln!("== FOLLOWER OF {e:?} ==");
    let cfg;
    let self_gid;
    let self_pid;
    {
        let s = s.read().await;
        cfg = s.core.config.clone();
        self_pid = s.core.pid;
        self_gid = s.core.gid;
    }

    let (mut conn_tx, mut conn_rx) = conn.split();

    // send ack back to primary
    let mut ack_rx = s.read().await.ack_rx[&self_gid].clone();
    let send_acks_task = {
        async move {
            eprintln!("sending acks to {:?}:{:?}", conn_tx.gid(), conn_tx.pid());
            loop {
                ack_rx.changed().await?;
                let (log_epoch, log_len, clock) = *ack_rx.borrow_and_update();
                let ack = Message::Ack {
                    log_epoch,
                    log_len,
                    clock,
                };
                conn_tx.send(ack).await?;
            }
        }
    };
    tokio::pin!(send_acks_task);

    // fetch acks from other replicas in group (except from the primary, which
    // uses its own connection to ensure FIFO between appends/acks/bumps).
    let mut fetch_acks_tasks = FuturesUnordered::new();
    for p in &cfg.group(self_gid).unwrap().peers {
        if p.pid != self_pid && p.pid != conn_rx.pid() {
            let p = p.clone();
            let s = s.clone();
            let task = async move {
                loop {
                    if let Err(err) = acks_fetch(p.clone(), s.clone()).await {
                        eprintln!("error fetching acks from {:?}:{:?}: {:?}", self_gid, p.pid, err);
                        tokio::time::sleep(RETRY_TIMEOUT).await;
                    }
                }
            };
            let abort_handle = AbortHandle::spawn(task);
            fetch_acks_tasks.push(abort_handle);
        }
    }

    let mut msgs = vec![];
    let mut ack_dests = GidSet::new();
    let err = 'recv: loop {
        tokio::select! {
            // biased;
            n = conn_rx.next_ready_chunk(BATCH_SIZE_YIELD, &mut msgs) => {
                debug_assert!(n == msgs.len());
                debug_assert!(msgs.len() <= BATCH_SIZE_YIELD);
            }
            res = &mut send_acks_task => {
                // could not write back to the primary
                return res;
            }
            _ = &mut fetch_acks_tasks.next() => {
                // any of the tasks failing means a higher epoch was seen from the peer
                return Ok(());
            }
        }

        if msgs.len() == 0 {
            // connection to leader lost
            eprintln!("====== connection to leader lost ======");
            break 'recv std::io::ErrorKind::ConnectionReset.into();
        }

        // process msg batch
        {
            let mut s = s.write().await;
            for msg in msgs.drain(..) {
                match msg {
                    Ok(Message::LogAppend {
                        idx,
                        entry_epoch,
                        entry,
                    }) => {
                        eprintln!("appending entry {:?} to log {:?}", idx, entry_epoch);
                        ack_dests.merge(&entry.dest);
                        if entry_epoch < e {
                            s.core.start_epoch_append(e, idx, entry_epoch, entry)?;
                        } else {
                            s.core.append(e, idx, entry)?;
                        }
                    }
                    Ok(Message::StartEpochAccept {
                        epoch,
                        prev_entry,
                        clock,
                    }) => {
                        s.core.start_epoch_accept(epoch, prev_entry, clock)?;
                        ack_dests.insert(self_gid);
                    }
                    Ok(Message::Ack {
                        log_epoch,
                        log_len,
                        clock,
                    }) => {
                        let old_clock = s.core.clock();
                        s.core.add_ack(conn_rx.pid(), log_epoch, log_len, clock)?;
                        if clock > old_clock {
                            ack_dests.insert(self_gid);
                        }
                    }
                    Ok(m) => panic!("unexpected message {:?}", m),
                    Err(err) => break 'recv err,
                }
            }

            let (epoch, len) = s.core.log_status();
            let clock = s.core.clock();
            for gid in ack_dests.0.drain(..) {
                s.ack_tx[&gid].send((epoch, len, clock))?;
            }
            s.update_tx.send(())?;
        }
        tokio::task::yield_now().await;
    };

    Err(err.into())
}

/// Accept connections and spawn handler tasks
async fn acceptor_task(listener: tokio::net::TcpListener, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    let self_gid;
    let self_pid;
    let mut handles = FuturesUnordered::new();
    {
        let s = s.read().await;
        self_gid = s.core.gid;
        self_pid = s.core.pid;
    }
    eprintln!("accepting connections at {:?}", listener.local_addr());
    loop {
        tokio::select! {
            Ok((sock, _addr)) = listener.accept() => {
                sock.set_nodelay(true).unwrap();
                handles.push(AbortHandle::spawn(handle_connection((self_gid, self_pid), sock, s.clone())));
            }
            Err(err) = listener.accept() => {
                panic!("error accepting new connections: {}", err);
            }
            // remove connection tasks they complete
            Some(_) = handles.next() => {}
        }
    }
}

/// Wait for the first request msg and then handle the connection as appropriate.
async fn handle_connection(
    self_id: (Gid, Pid),
    sock: tokio::net::TcpStream,
    s: Arc<RwLock<Shared>>,
) -> Result<(), Error> {
    let mut conn = Conn::incoming(self_id, sock).await?;
    match conn.recv().await? {
        Message::AckRequest => {
            ack_send(conn, s.clone(), true).await?;
        }
        Message::RemoteAckRequest => {
            ack_send(conn, s.clone(), false).await?;
        }
        Message::RemoteLogRequest {
            dest,
            log_epoch,
            next_idx,
        } => {
            remote_log_send(conn, dest, log_epoch, next_idx, s.clone()).await?;
        }
        Message::NewEpoch { epoch } => {
            // Incoming connection from the candidate of `epoch`
            eprintln!("new epoch request from {:?}:{:?} =  {:?}", conn.gid(), conn.pid(), epoch);
            let res = s.write().await.core.new_epoch_proposal(epoch);
            match res {
                Ok((log_epoch, log_len, clock)) => {
                    eprintln!("===============promised epoch: {:?}============", epoch);
                    conn.send(Message::Promise {
                        log_epoch,
                        log_len,
                        clock,
                    })
                    .await?;
                }
                Err(primcast_core::Error::EpochTooOld { promised, .. }) => {
                    eprintln!("===============promised epoch too old: {:?}============", promised);
                    conn.send(Message::NewEpoch { epoch: promised }).await?;
                }
                _ => unreachable!(),
            }
            // TODO: interrupt main loop?
        }
        Message::StartEpochCheck { epoch, log_epochs } => {
            // Incoming connection from the primary of `epoch`
            let res = s.write().await.core.start_epoch_check(epoch, log_epochs);
            match res {
                Ok((log_epoch, log_len)) => {
                    eprintln!("=== Starting Epoch Check and sending Follow ===");
                    conn.send(Message::Following { log_epoch, log_len }).await?;
                    let ev_tx = s.read().await.ev_tx.clone();
                    ev_tx
                        .send(Event::Follow(conn, epoch))
                        .map_err(|_| Error::ReplicaShutdown)?;
                }
                Err(primcast_core::Error::EpochTooOld { promised, .. }) => {
                    eprintln!("=== [ERROR] Fail to starting Epoch Check and sending NewEpoch ===");
                    conn.send(Message::NewEpoch { epoch: promised }).await?;
                }
                _ => unreachable!(),
            }
        }
        Message::ProposalStart => {
            // Incoming proposals from a process that thinks we're the primary
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
            // forward proposals to main loop
            eprintln!("receiving proposals from {:?}:{:?}", conn.gid(), conn.pid());
            let ev_tx = s.read().await.ev_tx.clone();
            let (tx, rx) = mpsc::channel(PROPOSAL_QUEUE);
            ev_tx.send(Event::ProposalIn(rx))?;
            let mut msgs = vec![];
            while 0 < conn.next_ready_chunk(BATCH_SIZE_YIELD, &mut msgs).await {
                for msg in msgs.drain(..) {
                    match msg? {
                        Message::Proposal { msg_id, msg, dest } => {
                            tx.send((msg_id, msg, dest)).await?;
                        }
                        m => panic!("unexpected message: {:?}", m),
                    }
                }
                tokio::task::yield_now().await;
            }
        }

        m => panic!("unexpected message: {:?}", m),
    }
    Ok(())
}

/// Request promise from the given Pid
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
    eprintln!("requesting promise from {:?}:{:?} to {:?} for {:?}", self_gid, self_pid, peer.pid, e);
    let mut conn = Conn::request((self_gid, self_pid), (self_gid, peer.pid), peer.addr(), req).await?;
    match conn.recv().await? {
        NewEpoch { epoch } => {
            eprintln!("New Epoch from {:?}:{:?} to {:?} for {:?}", self_gid, peer.pid, self_pid, e);
            Ok(Err(epoch))
        },
        Promise {
            log_epoch,
            log_len,
            clock,
        } => {
            eprintln!("promise from {:?}:{:?} to {:?} for {:?}", self_gid, peer.pid, self_pid, e);
            Ok(Ok((conn.pid(), log_epoch, log_len, clock)))
        },
        m => panic!("unexpected msg {:?}", m),
    }
}

/// Loops checking if new messages can be delivered.
async fn deliver_task(
    delivery_tx: mpsc::Sender<(Clock, MsgId, Bytes, GidSet)>,
    s: Arc<RwLock<Shared>>,
) -> Result<(), Error> {
    let mut update_rx = s.read().await.update_rx.clone();
    let mut deliveries = vec![];
    let mut last_delivery = (0, 0);
    loop {
        update_rx.changed().await?;
        let mut s = s.write().await;
        s.core.update();
        while let Some(d) = s.core.next_delivery() {
            let final_ts = d.final_ts.unwrap();
            assert!(last_delivery < (final_ts, d.msg_id)); // sanity check!
            last_delivery = (final_ts, d.msg_id);
            deliveries.push((final_ts, d.msg_id, d.msg.clone(), d.dest.clone()));
        }
        drop(s); // must not await with Shared locked
        for d in deliveries.drain(..) {
            delivery_tx.send(d).await?;
        }
    }
}

/// Sync state up with the given peer. Used by the candidate to sync up with the highest promised peer.
async fn sync_with(_peer: &PeerConfig, _e: Epoch, _s: &Arc<RwLock<Shared>>) -> Result<(), Error> {
    // unimplemented!()
    Ok(())
}

/// Keeps the given peer (follower) synchronized with our state (primary).
/// First, ensure that the peer is promised and that its log is truncated up to a compatible prefix.
/// Then, watch the log and send entries as they become available.
/// The task returns if the connection is lost or a higher epoch is seen.
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
    eprintln!("syncing follower {:?}:{:?}", self_gid, peer.pid);

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
        m => panic!("unexpected msg {:?}", m),
    };

    // send log entries as they become available
    let mut ack_rx = s.read().await.ack_rx[&self_gid].clone();
    let mut to_send = vec![];
    let mut last_clock_sent = 0;
    loop {
        {
            let s = s.read().await;
            let (promised_epoch, _) = s.core.state();
            let (log_epoch, log_len) = s.core.log_status();
            if promised_epoch != e || log_epoch != sync_log_epoch {
                // replica changed epochs, stop task
                return Ok(());
            }
            // gather entries to be sent (up to BATCH_SIZE_YIELD)
            while follower_log_len < log_len {
                let (epoch, entry) = s.core.log_entry(follower_log_len).unwrap();
                if follower_log_epoch < epoch && epoch == e {
                    // follower synced up to primary epoch e
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
                last_clock_sent = std::cmp::max(last_clock_sent, entry.local_ts);
                if to_send.len() >= BATCH_SIZE_YIELD {
                    break;
                }
            }
            // Send clock bump ack if needed (and safe to do). Acks and Appends
            // from a primary *must be sent in ts order*. A follower should see
            // its primary clock update ONLY IF it has received all log entries
            // with smaller clock value.
            if follower_log_len == log_len && s.core.clock() > last_clock_sent {
                last_clock_sent = s.core.clock();
                to_send.push(Ack {
                    log_epoch,
                    log_len,
                    clock: last_clock_sent,
                });
            }
        };

        // send gathered msgs
        for msg in to_send.drain(..) {
            conn.feed(msg).await?;
        }
        conn.flush().await?;
        tokio::task::yield_now().await;

        // are more entries already available? then send them
        let (new_log_epoch, new_log_len, new_clock) = *ack_rx.borrow_and_update();
        if sync_log_epoch != new_log_epoch {
            return Ok(());
        }
        if new_log_len > follower_log_len || new_clock > last_clock_sent {
            continue;
        }

        // otherwise, wait for more entries to become available and read acks from follower
        'wait: loop {
            tokio::select! {
                // biased;
                // check if there are more log entries to send
                _ = ack_rx.changed() => {
                    let (new_log_epoch, new_log_len, new_clock) = *ack_rx.borrow_and_update();
                    if sync_log_epoch != new_log_epoch {
                        return Ok(())
                    }
                    if new_log_len > follower_log_len || new_clock > last_clock_sent {
                        // entries or ack to send out
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
                            let old_clock = s.core.clock();
                            s.core.add_ack(conn.pid(), log_epoch, log_len, clock)?;
                            if clock > old_clock {
                                s.ack_tx[&self_gid].send_modify(|(_, _, clock)| *clock = s.core.clock());
                            }
                            s.update_tx.send(())?;
                        }
                        m => panic!("unexpected message: {:?}", m),
                    }
                }
            }
        }
    }
}

/// Fetch remote log entries from a replica in another group.
async fn remote_log_fetch(remote_gid: Gid, remote_peer: PeerConfig, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    eprintln!("fetching remote log from {:?}:{:?}", remote_gid, remote_peer.pid);
    let self_gid;
    let self_pid;
    let epoch;
    let next_idx;
    {
        let s = s.read().await;
        self_gid = s.core.gid;
        self_pid = s.core.pid;
        (epoch, next_idx) = s.core.remote_expected_entry(remote_gid);
    }
    use Message::*;
    let req = RemoteLogRequest {
        dest: self_gid,
        log_epoch: epoch,
        next_idx,
    };
    let mut conn = Conn::request((self_gid, self_pid), (remote_gid, remote_peer.pid), remote_peer.addr(), req).await?;

    let mut msgs = vec![];
    while 0 < conn.next_ready_chunk(BATCH_SIZE_YIELD, &mut msgs).await {
        {
            let mut s = s.write().await;
            let old_clock = s.core.clock();
            for msg in msgs.drain(..) {
                match msg? {
                    RemoteLogEpoch { log_epoch } => {
                        // will possibly truncate the log. We return so the
                        // function is retried and a new connection/request is
                        // established
                        s.core.remote_update_log_epoch(remote_gid, log_epoch)?;
                        return Ok(());
                    }
                    RemoteLogAppend(entry) => {
                        let log_len = entry.idx + 1;
                        let ts = entry.ts;
                        s.core.remote_append(remote_gid, entry)?;
                        s.core.remote_add_ack(remote_gid, remote_peer.pid, epoch, log_len, ts)?;
                    }
                    _ => panic!("unexpected message"),
                }
            }
            let new_clock = s.core.clock();
            if new_clock > old_clock {
                s.ack_tx[&self_gid].send_modify(|(_, _, clock)| *clock = new_clock);
            }
            s.update_tx.send(())?;
        }
        tokio::task::yield_now().await;
    }
    Ok(())
}

/// Fetch acks from another replica in our group
async fn acks_fetch(peer: PeerConfig, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    let self_gid = s.read().await.core.gid;
    let self_pid = s.read().await.core.pid;
    eprintln!("fetching acks from {:?}:{:?}", self_gid, peer.pid);

    use Message::*;
    let req = AckRequest;
    let mut conn = Conn::request((self_gid, self_pid), (self_gid, peer.pid), peer.addr(), req).await?;

    let mut msgs = vec![];
    while 0 < conn.next_ready_chunk(BATCH_SIZE_YIELD, &mut msgs).await {
        {
            let mut s = s.write().await;
            let old_clock = s.core.clock();
            for msg in msgs.drain(..) {
                match msg? {
                    Ack {
                        log_epoch,
                        log_len,
                        clock,
                    } => {
                        s.core.add_ack(conn.pid(), log_epoch, log_len, clock)?;
                    }
                    m => panic!("unexpected message: {:?}", m),
                }
            }
            if s.core.clock() > old_clock {
                s.ack_tx[&self_gid].send_modify(|(_, _, clock)| *clock = s.core.clock());
            }
            s.update_tx.send(())?;
        }
        tokio::task::yield_now().await;
    }
    Ok(())
}

/// Fetch remote acks from a replica in another group
async fn remote_acks_fetch(remote_gid: Gid, remote_peer: PeerConfig, s: Arc<RwLock<Shared>>) -> Result<(), Error> {
    let self_gid = s.read().await.core.gid;
    let self_pid = s.read().await.core.pid;
    eprintln!("fetching remote acks from {:?}:{:?}", remote_gid, remote_peer.pid);

    use Message::*;
    let req = RemoteAckRequest;
    let mut conn = Conn::request((self_gid, self_pid), (remote_gid, remote_peer.pid), remote_peer.addr(), req).await?;

    let mut msgs = vec![];
    while 0 < conn.next_ready_chunk(BATCH_SIZE_YIELD, &mut msgs).await {
        {
            let mut s = s.write().await;
            let old_clock = s.core.clock();
            for msg in msgs.drain(..) {
                match msg? {
                    Ack {
                        log_epoch,
                        log_len,
                        clock,
                    } => {
                        s.core
                            .remote_add_ack(conn.gid(), conn.pid(), log_epoch, log_len, clock)?;
                    }
                    m => panic!("unexpected message: {:?}", m),
                }
            }
            if s.core.clock() > old_clock {
                s.ack_tx[&self_gid].send_modify(|(_, _, clock)| *clock = s.core.clock());
            }
            s.update_tx.send(())?;
        }
    }
    Ok(())
}

/// Send acks to the incoming connection.
/// If send_bump is false, don't send ack on "clock only" updates.
async fn ack_send(mut conn: Conn, s: Arc<RwLock<Shared>>, send_bump: bool) -> Result<(), Error> {
    let mut ack_rx = s.read().await.ack_rx[&conn.gid()].clone();
    use Message::*;
    let mut last_ack = None;
    let mut last_clock = 0;
    loop {
        let (log_epoch, log_len, clock) = *ack_rx.borrow_and_update();
        if Some((log_epoch, log_len)) > last_ack || (send_bump && clock > last_clock) {
            last_ack = Some((log_epoch, log_len));
            last_clock = clock;
            conn.send(Ack {
                log_epoch,
                log_len,
                clock,
            })
            .await?;
        }
        ack_rx.changed().await?;
    }
}

/// Send relevant log entries to a replica in another group.
/// The task tracks the log and sends entries as they become available.
/// It returns if the connection is lost or if the log's epoch changes.
async fn remote_log_send(
    mut conn: Conn,
    dest: Gid,
    epoch: Epoch,
    mut next_idx: u64,
    s: Arc<RwLock<Shared>>,
) -> Result<(), Error> {
    eprintln!("sending remote logs for {dest:?} starting at {next_idx:?}");
    let mut ack_rx = s.read().await.ack_rx[&dest].clone();
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

/// Task which maintains a connection to the leader and forwards proposals to it.
async fn proposal_sender(
    from: (Gid, Pid),
    to_gid: Gid,
    cfg: config::Config,
    rx: mpsc::Receiver<(MsgId, Bytes, GidSet)>,
    s: Arc<RwLock<Shared>>,
) -> Result<(), Error> {
    let peers = cfg.peers(to_gid).unwrap();
    let mut rx = ReceiverStream::new(rx);
    'connect: loop {
        // TODO: better remote leader selection?
        // connect to everyone and see who is the leader
        let mut connect_futs = FuturesUnordered::new();
        for p in peers {
            let to_pid = p.pid;
            let fut = request_send_proposals(from, (to_gid, to_pid), p.clone()).map(move |res| (to_pid, res));
            connect_futs.push(fut);
        }
        let mut conn = loop {
            match connect_futs.next().await {
                Some((to_pid, Ok(conn))) => {
                    eprintln!("connected to leader {to_gid:?}:{to_pid:?}");
                    break conn;
                }
                Some((_to_pid, Err(Error::NotLeader(_epoch)))) => {
                    continue;
                }
                Some((to_pid, Err(err))) => {
                    eprintln!("error connecting to {to_gid:?}:{to_pid:?}: {err:?}");
                    continue;
                }
                None => {
                    tokio::time::sleep(RETRY_TIMEOUT).await;
                    continue 'connect;
                }
            }
        };

        // if we're the leader of the group, send directly to main loop
        if (conn.gid(), conn.pid()) == from {
            eprintln!("forwarding proposals to itself");
            let tx = s.read().await.proposal_tx.clone();
            let mut proposals = vec![];
            loop {
                let (_, state) = s.read().await.core.state();
                if state != ReplicaState::Primary && state != ReplicaState::Candidate {
                    // not group leader anymore, try connect to leader
                    continue 'connect;
                }
                if 0 == rx.next_ready_chunk(BATCH_SIZE_YIELD, &mut proposals).await {
                    // input channel closed
                    return Ok(());
                }
                for (msg_id, msg, dest) in proposals.drain(..) {
                    tx.send((msg_id, msg, dest)).await?;
                }
                tokio::task::yield_now().await;
            }
        }

        // send it to the leader of the given group
        eprintln!("forwarding proposals to {:?}:{:?}", conn.gid(), conn.pid());

        let mut proposals = vec![];
        loop {
            if 0 == rx.next_ready_chunk(BATCH_SIZE_YIELD, &mut proposals).await {
                // input channel closed
                return Ok(());
            }
            for (msg_id, msg, dest) in proposals.drain(..) {
                if let Err(err) = conn.feed(Message::Proposal { msg_id, msg, dest }).await {
                    eprintln!("error forwarding proposals to {to_gid:?}: {err:?}");
                    break;
                }
            }
            if let Err(err) = conn.flush().await {
                eprintln!("error forwarding proposals to {to_gid:?}: {err:?}");
                break;
            }
            tokio::task::yield_now().await;
        }
    }
}

/// Make a request for sending proposals to the leader.
/// If successful, returns the connection on which proposals can be sent.
async fn request_send_proposals(from: (Gid, Pid), to: (Gid, Pid), p: PeerConfig) -> Result<Conn, Error> {
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
