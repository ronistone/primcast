use bytes::Bytes;

use std::time::Instant;
use std::time::{SystemTime, UNIX_EPOCH};

use rustc_hash::FxHashMap as HashMap;
use rustc_hash::FxHashSet as HashSet;

pub use remote_learner::RemoteEntry;
use remote_learner::RemoteLearner;
use serde::Deserialize;
use serde::Serialize;

pub mod clock;
pub mod config;
mod pending;
pub mod remote_learner;
pub mod types;
pub mod persistence;

use clock::LogicalClock;
use config::Config;
use pending::PendingSet;
use types::*;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::persistence::{LMDBPersistence, PersistenceLayer, RocksDBPersistence, SledPersistence};

// Add this global toggle
static TIMED_PRINT_ENABLED: AtomicBool = AtomicBool::new(true);

// Add helper functions to control the toggle
pub fn enable_timed_print() {
    TIMED_PRINT_ENABLED.store(true, Ordering::Relaxed);
}

pub fn disable_timed_print() {
    TIMED_PRINT_ENABLED.store(false, Ordering::Relaxed);
}

pub fn is_timed_print_enabled() -> bool {
    TIMED_PRINT_ENABLED.load(Ordering::Relaxed)
}



#[macro_export]
macro_rules! timed_print {
    ($($arg:tt)*) => {
        if $crate::is_timed_print_enabled() {
            let now = SystemTime::now();
            let since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");
            let millis = since_epoch.as_millis();

            // Format: HH:MM:SS.mmm
            let secs = millis / 1000;
            let hours = (secs / 3600) % 24;
            let minutes = (secs / 60) % 60;
            let seconds = secs % 60;
            let ms = millis % 1000;

            let timestamp = format!("{:02}:{:02}:{:02}.{:03}", hours, minutes, seconds, ms);
            eprintln!("[{}] {}", timestamp, format!($($arg)*))
        }
    }
}
/// Split msgid set into multiple hashsets to prevent large reallocations
const MSGID_LOW_MASK: MsgId = 0xff;

// TODO: how to avoid MsgId conflicts? Right now we just assume random u128 won't collide.
// The id must be picked by the proposer. If we assume only replicas are proposers, we could use gid+pid+sequence.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Hash)]
pub enum ReplicaState {
    Primary,
    Candidate,
    // TODO: maybe the `Promised` state is not needed, as we don't consider ack
    // info coming from epochs different than current_epoch. As such, I think
    // there is no need to check for `Primary|Follower` when delivering. We
    // still need to check for a majority of epoch `accepts` before sending
    // remote acks though, so maybe it's simpler to just keep it.
    Promised,
    Follower,
    Recovering,  // Node is recovering from persistent storage
}

/// State loaded from persistent storage for recovery
#[derive(Clone, Serialize, Deserialize)]
pub struct PersistedState {
    pub promised_epoch: Epoch,
    pub log: Vec<LogEntry>,
    pub log_epochs: Vec<(Epoch, u64)>,
    pub clock_value: Clock,
    pub safe_len: u64,
    pub delivery_watermark: (Clock, MsgId),
}

#[derive(Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub local_ts: Clock,
    pub msg_id: MsgId,
    pub msg: Bytes,
    pub dest: GidSet,
    pub final_ts: Option<Clock>,
}

impl std::fmt::Debug for LogEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LogEntry")
            .field("ts", &self.local_ts)
            .field("msg_id", &self.msg_id)
            .field("msg_len", &self.msg.len())
            .field("dest", &self.dest)
            .field("final_ts", &self.final_ts)
            .finish()
    }
}

pub struct GroupReplica {
    pub gid: Gid,
    pub pid: Pid,
    pub config: Config,
    // Derived from config
    group_size: usize,
    quorum_size: usize,

    clock: LogicalClock,
    state: ReplicaState,
    promised_epoch: Epoch,

    /// The replica log - cached entries indexed by log position
    log: HashMap<u64, LogEntry>,
    /// Current log length (highest index + 1)
    log_len_cached: u64,
    /// Epochs stored in the log and the size of the log for each.
    log_epochs: Vec<(Epoch, u64)>,
    /// Log length acknowledged by group replicas in the current_epoch only (i.e., last epoch in log_epochs).
    current_epoch_acks: Vec<(u64, Pid)>,
    /// Safe prefix of the log (acknowledged by a quorum)
    safe_len: u64,
    /// All MsgId present in the log
    msgid: HashMap<u16, HashSet<MsgId>>,
    /// Msgs which we know about that have not yet been delivered.
    /// We don't keep an explicit set of delivered msgs: the set of delivered msgs is (msgid - pending).
    pending: PendingSet,

    remote_learners: HashMap<Gid, RemoteLearner>,

    // Epoch change related state ---
    leader_last_seen: Instant,
    proposals: Vec<LogEntry>,
    proposals_max: usize,
    promises: HashMap<Pid, (Epoch, u64, Clock)>,
    accepts: HashSet<Pid>,

    persistence: Box<dyn PersistenceLayer>,
}

#[derive(Debug)]
pub enum Error {
    EpochTooOld { promised: Epoch, current: Epoch },
    UnexpectedPromise { state: ReplicaState, promised: Epoch },
    WrongEpoch { expected: Epoch },
    InvalidIndex { len: u64 },
    InvalidReplicaState,
    NotLeader { leader: Pid },
    NotPrimary,
    NotPromised,
    NotFollower,
    IdAlreadyUsed,
    GroupNotInDest,
    RemoteLearner(remote_learner::Error),
    InvalidPersistence,
}

impl From<remote_learner::Error> for Error {
    fn from(err: remote_learner::Error) -> Self {
        Error::RemoteLearner(err)
    }
}

impl GroupReplica {
    /// New replica state for the given (gid,pid).
    /// TODO: we don't currently use persistent storage.
    /// Every replica starts at epoch Epoch::initial() with an empty log.
    pub fn new(gid: Gid, pid: Pid, epoch: Epoch, config: Config, hybrid_clock: bool) -> Self {
        // let state = if epoch.owner() == pid {
        //     ReplicaState::Primary
        // } else {
        //     ReplicaState::Follower
        // };

        let remote_learners = config
            .groups
            .iter()
            .filter_map(|g| {
                if g.gid == gid {
                    return None;
                }
                let pids = g.peers.iter().map(|p| p.pid);
                let remote_quorum_size = config.quorum_size(g.gid).unwrap();
                Some((g.gid, RemoteLearner::new(g.gid, pids, 0, remote_quorum_size)))
            })
            .collect();

        let promised_epoch = epoch;
        let current_epoch = epoch;
        let current_epoch_acks = config
            .group_pids(gid)
            .unwrap()
            .iter()
            .map(|pid| (0, *pid))
            .collect::<Vec<_>>();

        let group_size = current_epoch_acks.len();
        let quorum_size = config.quorum_size(gid).unwrap();

        let mut msgid = HashMap::default();
        msgid.reserve(MSGID_LOW_MASK as usize);
        let persistence_path = config.peer(gid, pid).unwrap().persistence_database.clone();
        let persistence = get_persistence(&config.persistence_backend, &persistence_path).unwrap();

        let log_persisted = persistence.list_log_entries().unwrap();

        let metadate_persisted = persistence.get_metadata().unwrap();
        timed_print!("Loaded {} log entries from persistence", log_persisted.len());
        timed_print!("Loaded metadata from persistence: {:?}", metadate_persisted);
        
        // Always start in Promised state. Recovery (catch-up) happens via the normal
        // leader sync process (sync_with/sync_follower), not as a separate pre-startup phase.
        let state = ReplicaState::Promised;
        
        let (promised_epoch, log_epochs, safe_len, _) = if let Some(m) = metadate_persisted {
            (m.promised_epoch, m.log_epochs, m.safe_len, m.clock)
            // (promised_epoch, vec![(epoch, 0)], 0, 0)
        } else {
            (promised_epoch, vec![(epoch, 0)], 0, 0)
        };

        let mut result = GroupReplica {
            gid,
            pid,
            clock: LogicalClock::new(pid, current_epoch, config.group_pids(gid).unwrap(), hybrid_clock),
            config,
            group_size,
            quorum_size,

            state,
            promised_epoch,
            log: HashMap::default(),
            log_len_cached: 0,
            log_epochs: log_epochs,
            current_epoch_acks,
            safe_len: safe_len,
            msgid,
            pending: PendingSet::new(gid),

            leader_last_seen: Instant::now(),
            proposals: Default::default(),
            proposals_max: 0,
            promises: Default::default(),
            accepts: Default::default(),
            remote_learners,
            persistence,
        };

        // Reset log_epochs and log_len_cached to safe_len so the loading loop
        // can re-append unsafe entries (idx >= safe_len) via the normal append path.
        // Metadata stores the full persisted count; we rebuild in-memory state from safe_len.
        let truncate_at = result.log_epochs.iter()
            .position(|&(_, count)| count > safe_len)
            .unwrap_or_else(|| result.log_epochs.len().saturating_sub(1));
        result.log_epochs.truncate(truncate_at + 1);
        if let Some(last) = result.log_epochs.last_mut() {
            last.1 = safe_len;
        }
        result.log_len_cached = safe_len;

        log_persisted.iter().for_each(|e| { // This code load all log in memory
            let (actual_epoch, _) = result.log_status();
            if e.epoch < actual_epoch || e.idx < safe_len {
                return; // skip entries from old epochs
            }
            result.append(e.epoch, e.idx, e.entry.clone()).unwrap();
        });

        return result
    }

    fn get_log(&self, idx: u64) -> Result<LogEntry, Error> {
        
        // Try cache first
        if let Some(entry) = self.log.get(&idx) {
            return Ok(entry.clone());
        }

        let result = self.persistence.as_ref().get_log_entry(idx);
        if let Ok(Some((_, entry))) = &result {
            return Ok(entry.clone());
        }

        Err(Error::InvalidIndex { len: self.log_actual_len() })
    }

    // === Log Access Abstraction Methods ===
    // These methods provide controlled access to the log for future persistence integration.
    // All direct self.log access should go through these methods.
    
    /// Get log entry by index (returns a reference)
    // fn get_log_ref(&self, idx: u64) -> Result<LogEntry, Error> {
    //     if idx >= self.log_actual_len() {
    //         return Err(Error::InvalidIndex { len: self.log_actual_len() });
    //     }
        
    //     // Try cache first
    //     if let Some(entry) = self.log.get(&idx) {
    //         return Ok(entry);
    //     }

    //     let result = self.persistence.as_ref().get_log_entry(idx);
    //     if let Ok(Some((_, entry))) = &result {
    //         return Ok(entry.clone());
    //     }
        
    //     Err(Error::InvalidIndex { len: self.log_actual_len() })
    // }

    fn update_log_ts(&mut self, idx: u64, final_ts: Clock) -> Result<(), Error> {
        let mut entry = self.persistence.as_mut().get_log_entry(idx);
        if let Ok(e) = &mut entry {
            if e.is_none() {
                return Err(Error::InvalidIndex { len: self.log_actual_len() });
            }
            let (epoch, log) = e.as_mut().unwrap();
            log.final_ts = Some(final_ts);
            self.persistence.as_mut().put_log_entry(*epoch, idx, log).unwrap();
            self.log.insert(idx, log.clone());
        }

        
        Ok(())
    }

    /// Get actual log length (different from log_len which uses log_epochs)
    fn log_actual_len(&self) -> u64 {
        self.log_len_cached
    }
    
    /// Push entry to log
    fn push_log(&mut self, idx: u64, entry: LogEntry) {
        self.log.insert(idx, entry);
        self.log_len_cached += 1;
    }
    
    /// Pop last entry from log
    fn pop_log(&mut self) -> Option<LogEntry> {
        if self.log_len_cached == 0 {
            return None;
        }
        self.log_len_cached -= 1;
        self.log.remove(&self.log_len_cached)
    }
    
    /// Get last log entry reference
    fn last_log(&self) -> Option<&LogEntry> {
        if self.log_len_cached == 0 {
            return None;
        }
        self.log.get(&(self.log_len_cached - 1))
    }
    
    /// Find log entry in range for destination
    fn find_log_for_dest(&self, start_idx: u64, gid: Gid) -> Option<&LogEntry> {
        for idx in start_idx..self.log_len_cached {
            if let Some(entry) = self.log.get(&idx) {
                if entry.dest.contains(gid) {
                    return Some(entry);
                }
            }
            // TODO: If not in cache, check persistence
        }
        None
    }
    
    /// Clear all log entries (for testing/reset purposes)
    #[allow(dead_code)]
    fn clear_log(&mut self) {
        self.log.clear();
        self.log_len_cached = 0;
    }

    /// helper for getting the entry for pid in current_epoch_acks
    fn get_ack_mut(&mut self, pid: Pid) -> &mut u64 {
        self.current_epoch_acks
            .iter_mut()
            .find(|(_, p)| *p == pid)
            .map(|(len, _)| len)
            .expect("pid should be present")
    }

    pub fn state(&self) -> (Epoch, ReplicaState) {
        (self.promised_epoch, self.state)
    }

    pub fn print_debug_info(&mut self) {
        let pending = self.pending.stats();
        timed_print!("=================");
        timed_print!("proposals: {} (max: {})", self.proposals.len(), self.proposals_max);
        timed_print!(
            "pending: {} (max: {}) with local ts: {} (max: {})",
            pending.all, pending.all_max, pending.with_local_ts, pending.with_local_ts_max,
        );
        timed_print!("log_len: {} safe_len: {}", self.log_len(), self.safe_len);
        timed_print!(
            "clock: {} min_clock_leader: {:?} quorum_clock: {:?}",
            self.clock(),
            self.min_clock_leader(),
            self.min_new_epoch_ts()
        );
        timed_print!("acks: {:?} epoch: {:?}", self.current_epoch_acks, self.current_epoch());
        timed_print!("remote learners:");
        for (gid, l) in &self.remote_learners {
            timed_print!(
                "    {:?} - safe_idx:{:?} next_entry:{:?} acks:{:?}",
                gid,
                l.safe_idx(),
                l.next_expected_log_entry(),
                Vec::from_iter(l.remote_info()),
            );
        }

        let mut count = 0;
        for _ in self.persistence.as_ref().list_log_entries().unwrap() {
            count += 1;
        }
        timed_print!(
            "persistence: {} entries in log",
            count
        );
        timed_print!("=================");
    }

    pub fn current_epoch(&self) -> Epoch {
        self.log_epochs.last().unwrap().0
    }

    pub fn log_status(&self) -> (Epoch, u64) {
        *self.log_epochs.last().unwrap()
    }

    pub fn log_epochs(&self) -> &Vec<(Epoch, u64)> {
        &self.log_epochs
    }

    pub fn clock(&self) -> Clock {
        self.clock.local()
    }

    pub fn accepts_len(&self) -> usize {
        self.accepts.len()
    }

    pub fn log_len(&self) -> u64 {
        self.log_epochs.last().unwrap().1
    }
    /// Append a batch of log entries during recovery.
    /// Entries MUST be sorted by idx (ascending) and contiguous starting from self.log.len().
    /// The replica must be in Recovering state.
    pub fn recovery_append_batch(
        &mut self,
        entries: Vec<(u64, Epoch, LogEntry)>,
    ) -> Result<(), Error> {
        if self.state != ReplicaState::Recovering {
            return Err(Error::InvalidReplicaState);
        }

        for (idx, entry_epoch, entry) in entries {
            // Validate contiguity
            let log_len = self.log_actual_len();
            if idx != log_len {
                return Err(Error::InvalidIndex { len: log_len });
            }

            // Use the existing append_inner which handles:
            // - msgid index update
            // - pending.add_entry_ts
            // - log_epochs tracking
            // - log.push
            // - own ack update
            self.append_inner(idx, entry_epoch, entry)?;
        }

        Ok(())
    }

    /// Returns the replica's current status for the recovery protocol.
    pub fn recovery_status(&self) -> (Epoch, Epoch, u64, Clock, Vec<(Epoch, u64)>) {
        let (log_epoch, log_len) = self.log_status();
        (
            self.promised_epoch,
            log_epoch,
            log_len,
            self.clock.local(),
            self.log_epochs.clone(),
        )
    }

    /// Finalize recovery: transition from Recovering to Promised.
    /// After this, the node can join the normal epoch flow.
    pub fn finalize_recovery(&mut self, epoch: Epoch, clock: Clock) -> Result<(), Error> {
        if self.state != ReplicaState::Recovering {
            return Err(Error::InvalidReplicaState);
        }

        self.promised_epoch = epoch;
        self.state = ReplicaState::Promised;
        self.clock.advance_epoch(epoch);
        self.clock.update(self.pid, epoch, clock);

        // Reset ack tracking for the new epoch
        let log_len = self.log.len() as u64;
        for (len, pid) in self.current_epoch_acks.iter_mut() {
            *len = if *pid == self.pid { log_len } else { 0 };
        }

        Ok(())
    }

    pub fn become_candidate(&mut self) {
        self.state = ReplicaState::Candidate;

    }
    /// Move to a higher epoch for which we are leader
    pub fn propose_new_epoch(&mut self, higher_than: Option<Epoch>) -> Result<Epoch, Error> {
        let epoch = if let Some(higher) = higher_than {
            std::cmp::max(higher, self.promised_epoch).next_for(self.pid)
        } else {
            self.promised_epoch.next_for(self.pid)
        };
        self.promised_epoch = epoch;
        self.state = ReplicaState::Candidate;
        self.accepts.clear();
        // add self promise
        let (current_epoch, log_len) = self.log_status();
        self.add_promise(epoch, self.pid, self.clock.local(), current_epoch, log_len)
            .unwrap();
        Ok(epoch)
    }

    /// New epoch proposal from another replica.
    /// If higher than our current promise, the replica becomes promised to it.
    pub fn new_epoch_proposal(&mut self, epoch: Epoch) -> Result<(Epoch, u64, Clock), Error> {
        assert!(epoch.owner() != self.pid, "epoch proposal for an epoch we own");

        if epoch >= self.promised_epoch {
            self.leader_last_seen = Instant::now();
            self.promised_epoch = epoch;
            self.state = ReplicaState::Promised;
            self.proposals.clear();
            self.accepts.clear();
            let (log_epoch, log_len) = self.log_status();
            Ok((log_epoch, log_len, self.clock()))
        } else {
            Err(Error::EpochTooOld {
                promised: self.promised_epoch,
                current: self.current_epoch(),
            })
        }
    }

    /// Add promise for a given epoch from some replica in the group.
    /// If a quorum is reached, returns the most up-to-date promise.
    pub fn add_promise(
        &mut self,
        epoch: Epoch,
        from: Pid,
        ts: Clock,
        current_epoch: Epoch,
        current_len: u64,
    ) -> Result<Option<(Pid, Epoch, u64, Clock)>, Error> {
        if matches!(self.state, ReplicaState::Candidate) && epoch == self.promised_epoch {
            if let Some((old_e, old_len, old_ts)) = self.promises.get(&from) {
                // TODO: only way this should happen if the candidate updated
                // the promised node, then requested same promise again.
                // Maybe we don't allow this?
                assert!(current_epoch >= *old_e || (current_epoch == *old_e && current_len >= *old_len));
                assert!(ts >= *old_ts);
            }
            self.promises.insert(from, (current_epoch, current_len, ts));
            if self.promises.len() >= self.quorum_size {
                let (high_epoch, high_len, high_pid) =
                    self.promises.iter().map(|(p, (e, l, _))| (*e, *l, *p)).max().unwrap();
                let max_ts = self.promises.values().map(|(_, _, ts)| *ts).max().unwrap();
                Ok(Some((high_pid, high_epoch, high_len, max_ts)))
            } else {
                Ok(None)
            }
        } else {
            Err(Error::UnexpectedPromise {
                state: self.state,
                promised: self.promised_epoch,
            })
        }
    }

    /// Check before accepting the new epoch. Will truncate the replica's log if
    /// needed. Returns the log status after possibly truncating.
    pub fn start_epoch_check(&mut self, epoch: Epoch, log_epochs: Vec<(Epoch, u64)>) -> Result<(Epoch, u64), Error> {
        if self.promised_epoch <= epoch {
            if epoch == self.current_epoch() {
                // log already synced to the epoch
                return Ok(self.log_status());
            }

            self.new_epoch_proposal(epoch).unwrap();
            self.leader_last_seen = Instant::now();

            // find matching log prefix
            let mut idx = 0;
            let mut prefix_len = 0;
            timed_print!("checking log prefix for {:?} with {:?}", self.log_epochs, log_epochs);
            for (our, leader) in self.log_epochs.iter().zip(log_epochs.iter()) {
                if our.0 == leader.0 {
                    // same epoch
                    idx += 1;
                    prefix_len = std::cmp::min(our.1, leader.1);
                } else {
                    break;
                }
            }

            // truncate the log and remove invalid msgid mappings
            self.log_epochs.truncate(idx + 1);
            {
                let last_entry = self.log_epochs.last_mut().unwrap();
                last_entry.1 = prefix_len;
            }
            
            // TODO: This needs to be abstracted when persistence is fully implemented
            // Collect entries to remove first to avoid multiple mutable borrows
            let mut entries_to_remove = Vec::new();
            while self.log_len_cached > prefix_len {
                if let Some(entry) = self.pop_log() {
                    entries_to_remove.push(entry);
                } else {
                    break;
                }
            }
            
            // Process removed entries
            for entry in entries_to_remove {
                let id_low = (entry.msg_id & MSGID_LOW_MASK) as u16;
                let id_set = self.msgid.get_mut(&id_low).expect("msgid should be present");
                assert!(id_set.remove(&entry.msg_id), "msgid should be present");
                self.pending.remove_entry_ts(entry.msg_id);
            }

            assert!(self.log_epochs.last().unwrap() <= log_epochs.last().unwrap());
            Ok(self.log_status())
        } else {
            Err(Error::EpochTooOld {
                promised: self.promised_epoch,
                current: self.current_epoch(),
            })
        }
    }

    /// Append entry for recovery before accepting a new epoch.
    /// Returns the new entry idx in the log.
    pub fn start_epoch_append(
        &mut self,
        epoch: Epoch,
        idx: u64,
        entry_epoch: Epoch,
        entry: LogEntry,
    ) -> Result<u64, Error> {
        if self.promised_epoch != epoch {
            timed_print!("FAIL TO START EPOCH APPEND: {:?} != {:?}", self.promised_epoch, epoch);
            return Err(Error::WrongEpoch {
                expected: self.promised_epoch,
            });
        }
        if self.state != ReplicaState::Promised && self.state != ReplicaState::Follower {
            timed_print!("FAIL TO START EPOCH APPEND: {:?} != {:?}", self.state, ReplicaState::Promised);
            return Err(Error::NotPromised);
        }

        timed_print!("start epoch append for {:?} with log len {} accept_len={}", entry_epoch, self.log_actual_len(), self.accepts.len());
        self.leader_last_seen = Instant::now();
        self.append_inner(idx, entry_epoch, entry)
    }

    /// Returns Ok if the replica's state is up-to-date for accepting the given epoch, and moves the current epoch to it.
    pub fn start_epoch_accept(&mut self, epoch: Epoch, last_entry: (Epoch, u64), clock: Clock) -> Result<(), Error> {
        if self.promised_epoch != epoch {
            timed_print!("FAIL TO START EPOCH ACCEPT promised epoch is different: {:?} != {:?}", self.promised_epoch, epoch);
            return Err(Error::WrongEpoch {
                expected: self.promised_epoch,
            });
        }
        if self.log_status() != last_entry {
            timed_print!("FAIL TO START EPOCH ACCEPT: {:?} != {:?}", self.log_status(), last_entry);
            return Err(Error::InvalidReplicaState);
        }

        if self.current_epoch() == epoch {
            timed_print!("EPOCH IS ALREADY RUNNING: {:?} == {:?}", self.current_epoch(), epoch);
            return Ok(());
        }

        let (_current_epoch, current_len) = self.log_status();

        // move current_epoch forward
        timed_print!("start epoch accept for {:?} with log len {} accept_len={}", epoch, current_len, self.accepts.len());
        self.log_epochs.push((epoch, current_len));
        // update clock info
        self.clock.advance_epoch(epoch);
        self.clock.update(self.pid, epoch, clock);
        self.clock.update(epoch.owner(), epoch, clock);
        // reset ack info for new epoch
        for (len, pid) in self.current_epoch_acks.iter_mut() {
            *len = if *pid == self.pid || *pid == epoch.owner() {
                current_len
            } else {
                0 // for others we wait for ack from current epoch
            };
        }

        self.append_accept(self.pid, epoch);
        self.append_accept(epoch.owner(), epoch); // we also know leader is up-to-date
        timed_print!("start epoch accept for {:?} with log len {} accept_len={}", epoch, current_len, self.accepts.len());

        Ok(())
    }

    /// Add a client proposal to be proposed when primary.
    pub fn add_proposal<I>(&mut self, msg_id: MsgId, msg: Bytes, dest: I) -> Result<(), Error>
    where
        I: IntoIterator<Item = Gid>,
    {
        if self.promised_epoch.owner() != self.pid {
            return Err(Error::NotLeader {
                leader: self.promised_epoch.owner(),
            });
        }

        let dest = dest.into_iter().collect::<GidSet>();
        if !dest.contains(self.gid) {
            return Err(Error::GroupNotInDest);
        }

        // check id not already used
        let id_low = (msg_id & MSGID_LOW_MASK) as u16;
        if self.msgid.get(&id_low).map_or(false, |s| s.contains(&msg_id))
            || self.proposals.iter().find(|e| e.msg_id == msg_id).is_some()
        {
            return Err(Error::IdAlreadyUsed);
        }

        self.proposals.push(LogEntry {
            local_ts: 0,
            msg_id,
            msg,
            dest,
            final_ts: None,
        });
        self.proposals_max = std::cmp::max(self.proposals_max, self.proposals.len());
        Ok(())
    }

    /// Assign a timestamp to queued messages and append the entry to the log. Must be primary.
    /// Returns true if any new entries were appended.
    pub fn propose(&mut self) -> Result<bool, Error> {
        if self.state != ReplicaState::Primary {
            return Err(Error::NotPrimary);
        }
        let appended = !self.proposals.is_empty();
        let mut proposals = std::mem::take(&mut self.proposals);
        for mut log_entry in proposals.drain(..) {
            let (log_epoch, log_len) = self.log_status();
            let ts = self.clock.tick();
            log_entry.local_ts = ts;
            self.append_inner(log_len, log_epoch, log_entry).unwrap();
        }
        self.proposals = proposals;
        Ok(appended)
    }

    /// Get the entry at a given log position.
    pub fn log_entry(&self, idx: u64) -> Option<(Epoch, LogEntry)> {
        let e = self.get_log(idx).expect("out of range log idx");
        // derive entry epoch from the log_epochs array
        let mut epoch = None;
        // timed_print!("get log entry {:?} {:?}", idx, self.log_epochs);
        for &(e, len) in &self.log_epochs {
            // TODO: store Epoch in LogEntry instead?
            if len > idx {
                epoch = Some(e);
                break;
            }
        }
        Some((epoch.unwrap(), e))
    }

    pub fn log_entry_for_remote(&self, idx: u64) -> Option<RemoteEntry> {
        let (epoch, entry) = self.log_entry(idx)?;
        Some(RemoteEntry {
            epoch,
            idx,
            msg_id: entry.msg_id,
            ts: entry.local_ts,
            dest: entry.dest.clone(),
        })
    }

    /// Get the next log entry destined for to a given Gid, starting at idx.
    /// Needed by replicas from remote groups to fetch relevant log entries.
    pub fn next_log_entry_for_dest(&self, start_idx: u64, gid: Gid) -> Option<&LogEntry> {
        self.find_log_for_dest(start_idx, gid)
    }

    /// Helper method for properly appending to the log
    fn append_inner(&mut self, idx: u64, entry_epoch: Epoch, entry: LogEntry) -> Result<u64, Error> {
        self.append_inner_internal(idx, entry_epoch, entry, false)
    }

    /// Helper method for properly appending to the log
    fn append_inner_internal(&mut self, idx: u64, entry_epoch: Epoch, entry: LogEntry, from_storage: bool) -> Result<u64, Error> {
        // timed_print!("append_inner: {:?} {:?} {:?}", entry_epoch, idx, entry);
        let (log_epoch, log_len) = self.log_status();

        if log_len != idx {
            return Err(Error::InvalidIndex { len: log_len });
        }

        // add msg_id mapping
        use std::collections::hash_map::Entry;
        let id_low = (entry.msg_id & MSGID_LOW_MASK) as u16;
        match self.msgid.entry(id_low) {
            Entry::Occupied(mut e) => {
                assert!(e.get_mut().insert(entry.msg_id), "msg_id should not be present");
            }
            Entry::Vacant(e) => {
                let s = e.insert(Default::default());
                s.insert(entry.msg_id);
            }
        }

        if !from_storage {
            self.pending
                .add_entry_ts(entry.msg_id, &entry.dest, entry.local_ts, self.log_len() as u64);
        }

        // add to log_epochs
        if log_epoch == entry_epoch {
            self.log_epochs.last_mut().unwrap().1 += 1;
        } else {
            timed_print!("append_inner: invalid epoch {:?} to log_epoch={:?}, entry={:?}", entry_epoch, log_epoch, entry);
            self.log_epochs.push((entry_epoch, log_len + 1));
            // don't think the following ever does anything, but its safe to do.
            self.clock.advance_epoch(entry_epoch);
            self.promised_epoch = entry_epoch;
            self.persistence.put_metadata(&persistence::ReplicaMetadata { 
                gid: self.gid, 
                pid: self.pid,
                promised_epoch: self.promised_epoch.clone(),
                log_epochs: self.log_epochs.clone(),
                safe_len: self.safe_len.clone(),
                clock: self.clock.get(self.pid),
            }).expect("failed to update metadata in persistence");
        }

        assert!(
            self.last_log().is_none() || self.last_log().unwrap().local_ts < entry.local_ts,
            "log append out of ts order"
        );
        self.persistence
        .put_log_entry(entry_epoch, idx, &entry)
        .expect("failed to append log entry to persistence");
        self.push_log(idx, entry);

        // update own ack
        let len = self.log_len() as u64;
        let ack = self.get_ack_mut(self.pid);
        *ack = std::cmp::max(*ack, len);

        Ok(len - 1)
    }

    /// Append log entry from the leader. Returns the entry idx the log.
    pub fn append(&mut self, epoch: Epoch, idx: u64, entry: LogEntry) -> Result<u64, Error> {
        let (log_epoch, _) = self.log_status();
        if (self.promised_epoch > epoch || self.current_epoch() > epoch) && log_epoch > epoch {
            timed_print!("FAIL TO APPEND: {:?} > {:?} or {:?} > {:?}", self.promised_epoch, epoch, self.current_epoch(), epoch);
            return Err(Error::EpochTooOld {
                promised: self.promised_epoch,
                current: self.promised_epoch,
            });
        }
        // if self.current_epoch() > epoch {
        //     timed_print!("FAIL TO APPEND: {:?} < {:?}", self.current_epoch(), epoch);
        //     return Err(Error::WrongEpoch {
        //         expected: self.current_epoch(),
        //     });
        // }
        // if self.state != ReplicaState::Follower {
        //     timed_print!("FAIL TO APPEND: {:?} != {:?}", self.state, ReplicaState::Follower);
        //     return Err(Error::NotFollower);
        // }

        self.leader_last_seen = Instant::now();
        let entry_ts = entry.local_ts;
        let res = self.append_inner(idx, epoch, entry)?;
        // assert!(entry_ts > self.min_clock_leader(), "info from leader out of ts order: entry_ts = {}, min_clock_leader = {}", entry_ts, self.min_clock_leader());
        // append is an ack from leader
        self.add_ack(epoch.owner(), epoch, idx + 1, entry_ts).unwrap();

        self.clock.update(epoch.owner(), epoch, entry_ts);
        Ok(res)
    }

        /// Append log entry from the leader. Returns the entry idx the log.
    pub fn append_load_storage(&mut self, epoch: Epoch, idx: u64, entry: LogEntry) -> Result<u64, Error> {
        let (log_epoch, _) = self.log_status();
        if (self.promised_epoch > epoch || self.current_epoch() > epoch) && log_epoch > epoch {
            timed_print!("FAIL TO APPEND: {:?} > {:?} or {:?} > {:?}", self.promised_epoch, epoch, self.current_epoch(), epoch);
            return Err(Error::EpochTooOld {
                promised: self.promised_epoch,
                current: self.promised_epoch,
            });
        }

        self.leader_last_seen = Instant::now();
        let entry_ts = entry.local_ts;
        let res = self.append_inner_internal(idx, epoch, entry, true)?;

        // append is an ack from leader
        self.add_ack(epoch.owner(), epoch, idx + 1, entry_ts).unwrap();

        self.clock.update(epoch.owner(), epoch, entry_ts);
        Ok(res)
    }

    pub fn append_accept(&mut self, pid: Pid, epoch: Epoch) {
        use ReplicaState::*;
        timed_print!("{:?}: {:?}: received ack from {:?} for epoch {}, adding to accepts len={}", self.gid, self.pid, pid, epoch, self.accepts.len());
        self.accepts.insert(pid);
        if self.accepts.len() >= self.quorum_size {
            self.state = if self.state == Candidate { Primary } else { Follower };
            timed_print!(">==== {:?}: {:?}: quorum reached for epoch {}, moving to state {:?}", self.gid, self.pid, epoch, self.state);
            self.accepts.clear();
        }
    }

    /// Ack from a replica from our group.
    /// Also servers as a bump message and heartbeat, as replicas in a group keep exchanging this info.
    pub fn add_ack(&mut self, pid: Pid, epoch: Epoch, log_len: u64, clock: Clock) -> Result<(), Error> {
        use ReplicaState::*;

        if self.promised_epoch == epoch {
            if self.state == Candidate || self.state == Promised {
                timed_print!("{:?}: {:?}: received ack from {:?} for epoch {}, adding to accepts len={}", self.gid, self.pid, pid, epoch, self.accepts.len());
                self.accepts.insert(pid);
                if self.accepts.len() >= self.quorum_size {
                    self.state = if self.state == Candidate { Primary } else { Follower };
                    timed_print!(">==== {:?}: {:?}: quorum reached for epoch {}, moving to state {:?}", self.gid, self.pid, epoch, self.state);
                    self.accepts.clear();
                }
                // TODO: check replica goes to primary/follower
            }
        }

        if pid == self.promised_epoch.owner() {
            self.leader_last_seen = Instant::now();
        }

        // ignore acks from epochs different from our log's current_epoch
        if epoch != self.current_epoch() {
            return Ok(());
        }

        // update acked len
        let ack = self.get_ack_mut(pid);
        *ack = std::cmp::max(*ack, log_len);
        self.clock.update(pid, epoch, clock);
        // we use promised_epoch here because acks may be accepted when promised_epoch > current_epoch
        self.clock.update(self.pid, self.promised_epoch, clock);

        Ok(())
    }

    /// Set the log epoch the remote learner is following.
    pub fn remote_update_log_epoch(&mut self, gid: Gid, epoch: Epoch) -> Result<(), Error> {
        let learner = self.remote_learners.get_mut(&gid).unwrap();
        Ok(learner.update_log_epoch(epoch)?)
    }

    /// Return the log epoch the remote learner is following, and what is the
    /// next expected log idx.
    pub fn remote_expected_entry(&self, gid: Gid) -> (Epoch, u64) {
        let learner = self.remote_learners.get(&gid).unwrap();
        learner.next_expected_log_entry()
    }

    /// Append the next relevant entry from the remote replica
    pub fn remote_append(&mut self, gid: Gid, entry: RemoteEntry) -> Result<(), Error> {
        // we use promised_epoch here since the received epoch has no relation to our group
        self.clock.update(self.pid, self.promised_epoch, entry.ts);
        let learner = self.remote_learners.get_mut(&gid).unwrap();
        Ok(learner.append(gid, entry)?)
    }

    /// Add information about the given remote replica
    pub fn remote_add_ack(&mut self, gid: Gid, pid: Pid, epoch: Epoch, log_len: u64, clock: u64) -> Result<(), Error> {
        // we use promised_epoch here since the received epoch has no relation to our group
        self.clock.update(self.pid, self.promised_epoch, clock);
        let learner = self.remote_learners.get_mut(&gid).unwrap();
        Ok(learner.add_remote_ack(gid, pid, epoch, log_len)?)
    }

    /// Update calculated replica state from received info.
    /// Update safe_len, update pending msg state from local group and remote learners
    pub fn update(&mut self) {
        // update safe len
        self.current_epoch_acks.sort(); // sort by acked log len
        let safe_len_from_acks = self.current_epoch_acks[self.group_size - self.quorum_size].0;
        let safe_len = std::cmp::min(std::cmp::max(self.safe_len, safe_len_from_acks), self.log_len() as u64);
        for idx in self.safe_len as usize..safe_len as usize {
            let entry = self.get_log(idx as u64).unwrap();
            // timed_print!("index: {}, to safe_len {}", idx, safe_len);
            self.pending
                .add_group_ts(entry.msg_id, &entry.dest, self.gid, entry.local_ts);
        }
        self.safe_len = safe_len;

        // update from remote learners
        for (gid, l) in &mut self.remote_learners {
            l.update();
            while let Some((msg_id, dest, ts)) = l.next_delivery() {
                timed_print!("msg_id: {}, to timestamp: {}", msg_id, ts);
                self.pending.add_group_ts(msg_id, &dest, *gid, ts);
            }
        }
        self.persistence.put_metadata(&persistence::ReplicaMetadata { 
                gid: self.gid, 
                pid: self.pid,
                promised_epoch: self.promised_epoch.clone(),
                log_epochs: self.log_epochs.clone(),
                safe_len: self.safe_len.clone(),
                clock: self.clock.get(self.pid),
            }).expect("failed to update metadata in persistence");
    }

    /// Returns the list of messages with some decided remote timestamp but not proposed locally yet.
    pub fn missing_local_ts(&mut self) -> Vec<(MsgId, GidSet)> {
        self.pending.missing_entry_ts()
    }

    pub fn min_clock_leader(&self) -> Clock {
        self.clock.get(self.current_epoch().owner())
    }

    /// Minimum clock value for epochs higher than current_epoch(). When the log
    /// is truncated, current epoch goes backward (node needs to recover state
    /// from a peer) and we can't really use quorum clock information until the
    /// node catches up.
    pub fn min_new_epoch_ts(&mut self) -> Option<Clock> {
        self.clock.quorum(self.quorum_size, self.current_epoch()).map(|c| c + 1)
    }

    /// Returns the next delivery (if any) in final timestamp order
    pub fn next_delivery(&mut self) -> Option<LogEntry> {
        let min_new_epoch_ts = self.min_new_epoch_ts();
        let min_clock_leader = self.min_clock_leader();
        let min_new_proposal = std::cmp::min(min_new_epoch_ts, Some(min_clock_leader + 1))?;
        let (final_ts, id, idx) = self.pending.pop_next_smallest(min_new_proposal)?;
        let mut log_entry = self.get_log(idx).unwrap();
        debug_assert_eq!(id, log_entry.msg_id);
        debug_assert!(log_entry.local_ts <= final_ts);
        self.update_log_ts(idx, final_ts).unwrap();
        log_entry.final_ts = Some(final_ts);
        return Some(log_entry);
    }
}


fn get_persistence(backend: &str, database: &str) -> Result<Box<dyn PersistenceLayer>, Error> {
    match backend {
        "lmdb" => {
            LMDBPersistence::new(&database)
                .map(|p| Box::new(p) as Box<dyn PersistenceLayer>)
                .map_err(|_| Error::InvalidPersistence)
        }
        "rocksdb" => {
            RocksDBPersistence::new(&database)
                .map(|p| Box::new(p) as Box<dyn PersistenceLayer>)
                .map_err(|_| Error::InvalidPersistence)
        }
        "sled" => {
            SledPersistence::new(&database)
                .map(|p| Box::new(p) as Box<dyn PersistenceLayer>)
                .map_err(|_| Error::InvalidPersistence)
        }
        _ => Err(Error::InvalidPersistence),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ReplicaState::*;

    pub struct IdGen(pub MsgId);
    impl IdGen {
        pub fn next(&mut self) -> MsgId {
            self.0 += 1;
            self.0
        }
    }

    #[test]
    fn group_replica_basics() {
        let config = Config::new_for_test();
        let mut idgen = IdGen(0);

        let mut r0 = GroupReplica::new(Gid(0), Pid(0), Epoch::initial(), config.clone(), false);
        let mut r1 = GroupReplica::new(Gid(0), Pid(1), Epoch::initial(), config.clone(), false);
        let mut r2 = GroupReplica::new(Gid(0), Pid(2), Epoch::initial(), config.clone(), false);

        assert_eq!(r0.state, Primary);
        assert_eq!(r1.state, Follower);
        assert_eq!(r2.state, Follower);

        // propose on primary
        assert!(r0.add_proposal(idgen.next(), "a1".into(), [Gid(0)]).is_ok());
        assert!(r0.add_proposal(idgen.next(), "a2".into(), [Gid(0)]).is_ok());
        assert!(r0.add_proposal(idgen.next(), "a3".into(), [Gid(0)]).is_ok());
        assert!(r0.propose().is_ok());
        assert_eq!(r0.log.len(), 3);

        // can't propose on followers
        assert!(r1.add_proposal(idgen.next(), "foo".into(), [Gid(0)]).is_err());
        assert!(r2.add_proposal(idgen.next(), "foo".into(), [Gid(0)]).is_err());
        // can't reuse msg id
        assert!(r0
            .add_proposal(r0.log_entry(0).unwrap().1.msg_id, "foo".into(), [Gid(0)])
            .is_err());
        // group must be a destination
        assert!(r0.add_proposal(idgen.next(), "foo".into(), [Gid(1)]).is_err());

        // append to followers
        let (epoch, entry) = r0.log_entry(0).unwrap();
        assert!(r1.append(epoch, 0, entry.clone()).is_ok());
        assert!(r2.append(epoch, 0, entry.clone()).is_ok());
        let (epoch, entry) = r0.log_entry(1).unwrap();
        assert!(r1.append(epoch, 1, entry.clone()).is_ok());
        assert!(r2.append(epoch, 1, entry.clone()).is_ok());

        // invalid append (wrong idx)
        let (epoch, entry) = r0.log_entry(0).unwrap();
        assert!(r1.append(epoch, 0, entry.clone()).is_err());
        assert!(r1.append(epoch, 4, entry.clone()).is_err());

        // ack info from followers
        assert!(r0
            .add_ack(r1.pid, r1.current_epoch(), r1.log.len() as u64, r1.clock.local())
            .is_ok());
        assert!(r0
            .add_ack(r2.pid, r2.current_epoch(), r2.log.len() as u64, r2.clock.local())
            .is_ok());

        r0.update();

        assert_eq!(r0.log.len(), 3);
        assert_eq!(r0.safe_len, 2);
        assert_eq!(r0.clock.quorum(r0.quorum_size, epoch), Some(2));
    }

    #[test]
    fn group_replica_global_msg() {
        let config = Config::new_for_test();
        let mut idgen = IdGen(0);

        let mut r0_0 = GroupReplica::new(Gid(0), Pid(0), Epoch::initial(), config.clone(), false);
        let mut r0_1 = GroupReplica::new(Gid(0), Pid(1), Epoch::initial(), config.clone(), false);
        let mut r1_0 = GroupReplica::new(Gid(1), Pid(0), Epoch::initial(), config.clone(), false);
        let mut r1_1 = GroupReplica::new(Gid(1), Pid(1), Epoch::initial(), config.clone(), false);

        // ----- STEP 1 ------

        // propose on group primaries
        let id = idgen.next();
        assert!(r0_0.add_proposal(id, "m".into(), [Gid(0), Gid(1)]).is_ok());
        assert!(r0_0.propose().is_ok());

        // we make tick the clock on the primary of Gid(1) to have 2 different local timestamps
        r1_0.clock.tick();
        r1_0.clock.tick();
        r1_0.clock.tick();
        assert!(r1_0.add_proposal(id, "m".into(), [Gid(0), Gid(1)]).is_ok());
        assert!(r1_0.propose().is_ok());

        // ----- STEP 2 ------

        // append from primary to followers
        let (epoch, entry) = r0_0.log_entry(0).unwrap();
        assert!(r0_1.append(epoch, 0, entry.clone()).is_ok());
        let (epoch, entry) = r1_0.log_entry(0).unwrap();
        assert!(r1_1.append(epoch, 0, entry.clone()).is_ok());

        // append/ack from primaries to remote groups ("matchings Pid's" in each
        // group follow each other, only acks from other remotes)

        assert!(r0_0
            .remote_append(Gid(1), r1_0.log_entry_for_remote(0).unwrap())
            .is_ok());
        assert!(r0_0
            .remote_add_ack(Gid(1), r1_0.pid, r1_0.current_epoch(), r1_0.log.len() as u64, r1_0.clock.local())
            .is_ok());
        assert!(r0_1
            .remote_add_ack(Gid(1), r1_0.pid, r1_0.current_epoch(), r1_0.log.len() as u64, r1_0.clock.local())
            .is_ok());

        assert!(r1_0
            .remote_append(Gid(0), r0_0.log_entry_for_remote(0).unwrap())
            .is_ok());
        assert!(r1_0
            .remote_add_ack(Gid(0), r0_0.pid, r0_0.current_epoch(), r0_0.log.len() as u64, r0_0.clock.local())
            .is_ok());
        assert!(r1_1
            .remote_add_ack(Gid(0), r0_0.pid, r0_0.current_epoch(), r0_0.log.len() as u64, r0_0.clock.local())
            .is_ok());

        // ----- STEP 3 ------

        // follower acks back to leaders/each other (the ack here also works as the <bump> message in the protocol)
        assert!(r0_0
            .add_ack(r0_1.pid, r0_1.current_epoch(), r0_1.log.len() as u64, r0_1.clock.local())
            .is_ok());
        assert!(r0_1
            .add_ack(r0_0.pid, r0_0.current_epoch(), r0_0.log.len() as u64, r0_0.clock.local())
            .is_ok());

        assert!(r1_0
            .add_ack(r1_1.pid, r1_1.current_epoch(), r1_1.log.len() as u64, r1_1.clock.local())
            .is_ok());
        assert!(r1_1
            .add_ack(r1_0.pid, r1_0.current_epoch(), r1_0.log.len() as u64, r1_0.clock.local())
            .is_ok());

        // follower append/ack to remote learners
        assert!(r0_1
            .remote_append(Gid(1), r1_1.log_entry_for_remote(0).unwrap())
            .is_ok());
        assert!(r0_0
            .remote_add_ack(Gid(1), r1_1.pid, r1_1.current_epoch(), r1_1.log.len() as u64, r1_1.clock.local())
            .is_ok());
        assert!(r0_1
            .remote_add_ack(Gid(1), r1_1.pid, r1_1.current_epoch(), r1_1.log.len() as u64, r1_1.clock.local())
            .is_ok());

        assert!(r1_1
            .remote_append(Gid(0), r0_1.log_entry_for_remote(0).unwrap())
            .is_ok());
        assert!(r1_0
            .remote_add_ack(Gid(0), r0_1.pid, r0_1.current_epoch(), r0_1.log.len() as u64, r0_1.clock.local())
            .is_ok());
        assert!(r1_1
            .remote_add_ack(Gid(0), r0_1.pid, r0_1.current_epoch(), r0_1.log.len() as u64, r0_1.clock.local())
            .is_ok());

        // ----- CHECKS ------

        // check in every replica msg is deliverable
        for r in &mut [r0_0, r0_1, r1_0, r1_1] {
            r.update();

            // local group timestamp is quorum safe
            assert_eq!(r.safe_len, 1);
            // check that final_ts is learned
            let (pending_ts, pending_id) = r.pending.peek_next_smallest().unwrap();
            assert_eq!(pending_id, id);
            assert_eq!(pending_ts, 4);
            // check that final_ts is safe for delivery
            assert_eq!(r.min_clock_leader(), 4);
            assert!(r.min_new_epoch_ts() > Some(4));

            // thus, msg should be deliverable
            let entry = r.next_delivery().unwrap();
            assert_eq!(entry.final_ts, Some(4));
            assert_eq!(entry.msg_id, id);
            assert!(r.next_delivery().is_none());
        }
    }

    #[test]
    fn recovery_append_batch_basic() {
        let config = Config::new_for_test();

        // Create source log entries to be recovered
        let mut source_entries = Vec::new();
        source_entries.push(LogEntry {
            local_ts: 1,
            msg_id: 1,
            msg: "a".into(),
            dest: {
                let mut s = GidSet::new();
                s.insert(Gid(0));
                s
            },
            final_ts: None,
        });
        source_entries.push(LogEntry {
            local_ts: 2,
            msg_id: 2,
            msg: "b".into(),
            dest: {
                let mut s = GidSet::new();
                s.insert(Gid(0));
                s
            },
            final_ts: None,
        });
        source_entries.push(LogEntry {
            local_ts: 3,
            msg_id: 3,
            msg: "c".into(),
            dest: {
                let mut s = GidSet::new();
                s.insert(Gid(0));
                s
            },
            final_ts: None,
        });

        // Create entries to pass to recovery_append_batch
        let entries = vec![
            (0u64, Epoch::initial(), source_entries[0].clone()),
            (1u64, Epoch::initial(), source_entries[1].clone()),
            (2u64, Epoch::initial(), source_entries[2].clone()),
        ];

        // Create a fresh replica
        let mut replica = GroupReplica::new(Gid(0), Pid(1), Epoch::initial(), config.clone(), false);
        // Must be in Recovering state to use recovery_append_batch
        replica.state = ReplicaState::Recovering;

        // Apply batch of entries
        assert!(replica.recovery_append_batch(entries).is_ok());
        assert_eq!(replica.log.len(), 3);

        // Verify entries were applied
        assert_eq!(replica.log_entry(0).unwrap().1.msg_id, 1);
        assert_eq!(replica.log_entry(1).unwrap().1.msg_id, 2);
        assert_eq!(replica.log_entry(2).unwrap().1.msg_id, 3);
    }

    #[test]
    fn recovery_status_and_finalize() {
        let config = Config::new_for_test();

        // Create replica in Recovering state
        let mut replica = GroupReplica::new(Gid(0), Pid(0), Epoch::initial(), config.clone(), false);
        replica.state = ReplicaState::Recovering;

        // Check recovery status - returns (promised_epoch, log_epoch, log_len, clock, log_epochs)
        let (promised_epoch, log_epoch, log_len, _clock, log_epochs) = replica.recovery_status();
        assert_eq!(promised_epoch, Epoch::initial());
        assert_eq!(log_epoch, Epoch::initial());
        assert_eq!(log_len, 0);
        assert_eq!(log_epochs.len(), 1); // At least the initial epoch

        // Create log entries manually
        let mut dest = GidSet::new();
        dest.insert(Gid(0));
        
        replica.log.insert(0, LogEntry {
            local_ts: 1,
            msg_id: 1,
            msg: "x".into(),
            dest: dest.clone(),
            final_ts: None,
        });
        replica.log.insert(1, LogEntry {
            local_ts: 2,
            msg_id: 2,
            msg: "y".into(),
            dest: dest.clone(),
            final_ts: None,
        });

        // Finalize recovery with an epoch and clock
        let new_epoch = Epoch(1, Pid(0));
        let new_clock = 5;
        assert!(replica.finalize_recovery(new_epoch, new_clock).is_ok());
        assert_eq!(replica.state, ReplicaState::Promised);

        // After finalization, log should be preserved
        assert_eq!(replica.log.len(), 2);
    }

    #[test]
    fn restart_with_unsafe_log_entries_does_not_panic() {
        // Regression: when safe_len < log_len on restart, GroupReplica::new
        // panicked with InvalidIndex because log_epochs from metadata reflected
        // the full log length while append_inner expected log_len == idx.
        let db_path = format!("/tmp/primcast_restart_test_{}", std::process::id());
        let _ = std::fs::remove_dir_all(&db_path);

        let initial_epoch = Epoch::initial();
        let real_epoch = Epoch(1, Pid(0));
        let gid = Gid(0);
        let pid = Pid(0);
        const TOTAL: u64 = 5;
        const SAFE: u64 = 3;

        // Build persistence state: 5 log entries, safe_len=3 (entries 3,4 are unsafe).
        // This mirrors what update() persists after a quorum ack for only the first 3 entries.
        {
            let mut db = persistence::LMDBPersistence::new(&db_path).unwrap();
            for idx in 0..TOTAL {
                let entry = LogEntry {
                    local_ts: idx + 1,
                    msg_id: (idx + 1) as MsgId,
                    msg: Bytes::from("x"),
                    dest: [gid].into_iter().collect(),
                    final_ts: None,
                };
                db.put_log_entry(real_epoch, idx, &entry).unwrap();
            }
            db.put_metadata(&persistence::ReplicaMetadata {
                gid,
                pid,
                promised_epoch: real_epoch,
                log_epochs: vec![(initial_epoch, 0), (real_epoch, TOTAL)],
                safe_len: SAFE,
                clock: TOTAL,
            })
            .unwrap();
        }

        let mut config = Config::new_for_test();
        config.groups[0].peers[0].persistence_database = db_path.clone();

        // Before fix: panics with InvalidIndex { len: 5 }
        // After fix: loads without panic
        let replica = GroupReplica::new(gid, pid, initial_epoch, config, false);

        assert_eq!(replica.log_len(), TOTAL);
        assert_eq!(replica.safe_len, SAFE);

        let _ = std::fs::remove_dir_all(&db_path);
    }
}
