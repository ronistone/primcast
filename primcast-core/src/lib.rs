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

use clock::LogicalClock;
use config::Config;
use pending::PendingSet;
use types::*;

#[macro_export]
macro_rules! timed_print {
    ($($arg:tt)*) => {
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
/// Allocate space for this amount of log entries at the start, to avoid
/// reallocations. The way we keep entries in memory (just a Vec), large
/// reallocations may cause pauses due to the amount of copying.
// TODO: handle this issue
const INITIAL_CAP: usize = 50_000_000;

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

    /// The replica log
    log: Vec<LogEntry>,
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
        let state = if epoch.owner() == pid {
            ReplicaState::Primary
        } else {
            ReplicaState::Follower
        };

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

        let mut log = Vec::new();
        log.reserve(INITIAL_CAP);
        let mut msgid = HashMap::default();
        msgid.reserve(MSGID_LOW_MASK as usize);

        GroupReplica {
            gid,
            pid,
            clock: LogicalClock::new(pid, current_epoch, config.group_pids(gid).unwrap(), hybrid_clock),
            config,
            group_size,
            quorum_size,

            state,
            promised_epoch,
            log,
            log_epochs: vec![(epoch, 0)],
            current_epoch_acks,
            safe_len: 0,
            msgid,
            pending: PendingSet::new(gid),

            leader_last_seen: Instant::now(),
            proposals: Default::default(),
            proposals_max: 0,
            promises: Default::default(),
            accepts: Default::default(),
            remote_learners,
        }
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
        timed_print!("log_len: {} safe_len: {}", self.log.len(), self.safe_len);
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
            let last_entry = self.log_epochs.last_mut().unwrap();
            last_entry.1 = prefix_len;
            while u64::try_from(self.log.len()).unwrap() > prefix_len {
                let entry = self.log.pop().unwrap();
                let id_low = (entry.msg_id & MSGID_LOW_MASK) as u16;
                let id_set = self.msgid.get_mut(&id_low).expect("msgid should be present");
                assert!(id_set.remove(&entry.msg_id), "msgid should be present");
                self.pending.remove_entry_ts(entry.msg_id);
            }

            assert!(*last_entry <= *log_epochs.last().unwrap());
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

        timed_print!("start epoch append for {:?} with log len {} accept_len={}", entry_epoch, self.log.len(), self.accepts.len());
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
    pub fn log_entry(&self, idx: u64) -> Option<(Epoch, &LogEntry)> {
        let e = self.log.get(usize::try_from(idx).expect("out of range log idx"))?;
        // derive entry epoch from the log_epochs array
        let mut epoch = None;
        timed_print!("get log entry {:?} {:?}", idx, self.log_epochs);
        for &(e, len) in &self.log_epochs {
            // TODO: store Epoch in LogEntry instead?
            if len > idx {
                epoch = Some(e);
                break;
            }
        }
        Some((epoch.unwrap(), e))
    }

    fn log_entry_mut(&mut self, idx: u64) -> Option<&mut LogEntry> {
        self.log.get_mut(usize::try_from(idx).expect("out of range log idx"))
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
        let idx = usize::try_from(start_idx).expect("out of range log idx");
        self.log[idx..].iter().find(|it| it.dest.contains(gid))
    }

    /// Helper method for properly appending to the log
    fn append_inner(&mut self, idx: u64, entry_epoch: Epoch, entry: LogEntry) -> Result<u64, Error> {
        timed_print!("append_inner: {:?} {:?} {:?}", entry_epoch, idx, entry);
        let (log_epoch, log_len) = self.log_status();
        assert_eq!(log_len, self.log.len() as u64);

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
        self.pending
            .add_entry_ts(entry.msg_id, &entry.dest, entry.local_ts, self.log.len() as u64);

        // add to log_epochs
        if log_epoch == entry_epoch {
            self.log_epochs.last_mut().unwrap().1 += 1;
        } else {
            timed_print!("append_inner: invalid epoch {:?} to log_epoch={:?}, entry={:?}", entry_epoch, log_epoch, entry);
            self.log_epochs.push((entry_epoch, log_len + 1));
            // don't think the following ever does anything, but its safe to do.
            self.clock.advance_epoch(entry_epoch);
        }

        assert!(
            self.log.last().is_none() || self.log.last().unwrap().local_ts < entry.local_ts,
            "log append out of ts order"
        );
        self.log.push(entry);

        // update own ack
        let len = self.log.len() as u64;
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
        let safe_len = std::cmp::min(std::cmp::max(self.safe_len, safe_len_from_acks), self.log.len() as u64);
        for entry in &mut self.log[self.safe_len as usize..safe_len as usize] {
            self.pending
                .add_group_ts(entry.msg_id, &entry.dest, self.gid, entry.local_ts);
        }
        self.safe_len = safe_len;

        // update from remote learners
        for (gid, l) in &mut self.remote_learners {
            l.update();
            while let Some((msg_id, dest, ts)) = l.next_delivery() {
                self.pending.add_group_ts(msg_id, &dest, *gid, ts);
            }
        }
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
    pub fn next_delivery(&mut self) -> Option<&LogEntry> {
        let min_new_epoch_ts = self.min_new_epoch_ts();
        let min_clock_leader = self.min_clock_leader();
        let min_new_proposal = std::cmp::min(min_new_epoch_ts, Some(min_clock_leader + 1))?;
        let (final_ts, id, idx) = self.pending.pop_next_smallest(min_new_proposal)?;
        let log_entry = self.log_entry_mut(idx).unwrap();
        debug_assert_eq!(id, log_entry.msg_id);
        debug_assert!(log_entry.local_ts <= final_ts);
        log_entry.final_ts = Some(final_ts);
        return Some(log_entry);
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
}
