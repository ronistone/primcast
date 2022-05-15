pub mod clock;
pub mod config;
pub mod remote_learner;
pub mod types;

use crate::clock::LogicalClock;
use crate::config::Config;
use crate::types::*;

use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::time::Instant;

use fnv::FnvHashMap;
use fnv::FnvHashSet;

use remote_learner::RemoteLearner;
pub use remote_learner::RemoteEntry;
use serde::Deserialize;
use serde::Serialize;

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

#[derive(Clone)]
pub struct LogEntry {
    pub ts: Clock,
    pub msg_id: MsgId,
    pub msg: Vec<u8>,
    pub dest: GidSet,
    pub final_ts: Option<Clock>,
}

#[derive(Clone)]
/// Pending message information needed to track delivery
struct PendingMsg {
    id: MsgId,
    dest: GidSet,
    /// current minimum final ts.
    min_ts: Clock,
    /// group ts still to be decided
    missing_group_ts: GidSet,
}

impl PendingMsg {
    fn final_ts(&self) -> Option<Clock> {
        if self.missing_group_ts.is_empty() {
            Some(self.min_ts)
        } else {
            None
        }
    }
}

pub struct GroupReplica {
    gid: Gid,
    pid: Pid,
    config: Config,
    // Derived from config
    group_size: usize,
    quorum_size: usize,

    clock: LogicalClock,
    state: ReplicaState,
    promised_epoch: Epoch,

    /// The replica log
    log: Vec<LogEntry>,
    /// Index by epoch into the log. Sequence of (epoch, number of epoch entries)
    log_epochs: Vec<(Epoch, u64)>,
    /// Epoch of the log. The tuple (current_epoch, log.len()) determines which replica is most up to date
    current_epoch: Epoch,
    /// Log length acknowledged by group replicas in the !current_epoch! only.
    current_epoch_acks: Vec<(u64, Pid)>,
    /// Safe prefix of the log (acknowledged by a quorum)
    safe_len: u64,
    /// Msgid to log idx.
    /// TODO: The mapping grows proportionally to the log. Should we do it differently?
    msgid_to_idx: FnvHashMap<MsgId, u64>,

    /// Msgs not yet delivered which we know of. Delivered is (msgid_to_idx / pending).
    /// Best to do it like this then having an ever growing "delivered" set.
    pending: FnvHashMap<MsgId, PendingMsg>,
    /// Id of pending messsages which have been proposed in the local group
    /// (i.e., present in the log), in min_ts order.
    /// Only msgs present here can block deliveries.
    pending_ts: BTreeSet<(Clock, MsgId)>,

    remote_learners: FnvHashMap<Gid, RemoteLearner>,

    // Epoch change related state ---

    leader_last_seen: Instant,
    proposals: VecDeque<LogEntry>,
    promises: FnvHashMap<Pid, (Epoch, u64, Clock)>,
    accepts: FnvHashSet<Pid>,
}

#[derive(Debug)]
pub enum Error {
    EpochTooOld {
        promised: Epoch,
        current: Epoch,
    },
    UnexpectedPromise {
        state: ReplicaState,
        promised: Epoch,
    },
    NeedsRecovery {
        from: u64,
    },
    WrongEpoch {
        expected: Epoch,
    },
    InvalidIndex {
        len: u64,
    },
    InvalidReplicaState,
    NotLeader {
        leader: Pid,
    },
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
    pub fn new(gid: Gid, pid: Pid, config: Config) -> Self {
        let state = if Epoch::initial().owner() == pid {
            ReplicaState::Primary
        } else {
            ReplicaState::Follower
        };

        let remote_learners = config.groups.iter()
            .map(|g| {
                let pids = g.peers.iter().map(|p| p.pid);
                (g.gid, RemoteLearner::new(g.gid, pids, 0))
            })
            .collect();

        let promised_epoch = Epoch::initial();
        let current_epoch = Epoch::initial();
        let current_epoch_acks = config.group_pids(gid).unwrap()
            .iter()
            .map(|pid| (0, *pid))
            .collect::<Vec<_>>();

        let group_size = current_epoch_acks.len();
        let quorum_size = config.quorum_size(gid).unwrap();

        GroupReplica {
            gid,
            pid,
            clock: LogicalClock::new(pid, current_epoch, config.group_pids(gid).unwrap()),
            config,
            group_size,
            quorum_size,

            state,
            promised_epoch,
            log: vec![],
            log_epochs: vec![(Epoch::initial(), 0)],
            current_epoch,
            current_epoch_acks,
            safe_len: 0,
            msgid_to_idx: Default::default(),
            pending: Default::default(),
            pending_ts: Default::default(),

            leader_last_seen: Instant::now(),
            proposals: Default::default(),
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

    /// Move to a higher epoch for which we are leader
    pub fn propose_new_epoch(&mut self) -> Result<Epoch, Error> {
        let epoch = self.promised_epoch.next_for(self.pid);
        self.promised_epoch = epoch;
        self.state = ReplicaState::Candidate;
        self.accepts.clear();
        // add self promise
        self.add_promise(epoch, self.pid, self.clock.local(), self.current_epoch, self.log.len() as u64).unwrap();
        Ok(epoch)
    }

    /// New epoch proposal from another replica.
    /// If higher than our current promise, the replica becomes promised to it.
    pub fn new_epoch_proposal(&mut self, epoch: Epoch) -> Result<(Epoch, u64), Error> {
        assert!(epoch.owner() != self.pid, "epoch proposal for an epoch we own");

        if epoch >= self.promised_epoch {
            self.promised_epoch = epoch;
            self.state = ReplicaState::Promised;
            self.proposals.clear();
            self.accepts.clear();
            Ok((self.current_epoch, self.log.len() as u64))
        } else {
            Err(Error::EpochTooOld {
                promised: self.promised_epoch,
                current: self.current_epoch,
            })
        }
    }

    /// Add promise for a given epoch from some replica in the group.
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
                assert!(
                    current_epoch >= *old_e || (current_epoch == *old_e && current_len >= *old_len)
                );
                assert!(ts >= *old_ts);
            }
            self.promises.insert(from, (current_epoch, current_len, ts));
            if self.promises.len() == self.quorum_size {
                let (high_epoch, high_len, high_pid) = self
                    .promises
                    .iter()
                    .map(|(p, (e, l, _))| (*e, *l, *p))
                    .max()
                    .unwrap();
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
    /// needed. If there is need to recover entries from previous epochs, the
    /// `NeedsRecovery` error is returned.
    pub fn start_epoch_check(
        &mut self,
        epoch: Epoch,
        log_epochs: Vec<(Epoch, u64)>,
    ) -> Result<(), Error> {
        if self.promised_epoch <= epoch {
            self.new_epoch_proposal(epoch).unwrap();
            // find matching prefix
            let mut idx = 0;
            let mut last_len = 0;
            let mut total_len = 0;
            for (our, leader) in self.log_epochs.iter().zip(log_epochs.iter()) {
                if our.0 == leader.0 {
                    // same epoch
                    idx += 1;
                    last_len = std::cmp::min(our.1, leader.1);
                    total_len += last_len;
                } else {
                    break;
                }
            }

            // truncate the log and remove invalid msgid mappings
            self.log_epochs.truncate(idx + 1);
            let last_entry = self.log_epochs.last_mut().unwrap();
            last_entry.1 = last_len;
            while self.log.len() as u64 > total_len {
                let entry = self.log.pop().unwrap();
                self.msgid_to_idx.remove(&entry.msg_id).expect("mapping should exist");
                let min_ts = self.pending.get(&entry.msg_id).expect("should be pending").min_ts;
                // no more current local ts, remote from pending_ts so it doesn't prevent other deliveries
                assert!(self.pending_ts.remove(&(min_ts, entry.msg_id)));
            }

            if *last_entry < *log_epochs.last().unwrap() {
                Err(Error::NeedsRecovery { from: total_len })
            } else {
                assert!(*last_entry == *log_epochs.last().unwrap());
                Ok(())
            }
        } else {
            Err(Error::EpochTooOld {
                promised: self.promised_epoch,
                current: self.current_epoch,
            })
        }
    }

    /// Append entry for recovery before accepting a new epoch
    pub fn start_epoch_append(
        &mut self,
        epoch: Epoch,
        idx: u64,
        entry_epoch: Epoch,
        entry: LogEntry,
    ) -> Result<&LogEntry, Error> {
        if self.promised_epoch != epoch {
            return Err(Error::WrongEpoch {
                expected: self.promised_epoch,
            });
        }
        if self.state != ReplicaState::Promised {
            return Err(Error::NotPromised);
        }
        self.append_inner(idx, entry_epoch, entry)
    }

    /// Returns Ok if the replica's state is up-to-date for accepting the given epoch, and moves the current epoch to it.
    pub fn start_epoch_accept(
        &mut self,
        epoch: Epoch,
        last_entry: (Epoch, u64),
    ) -> Result<(), Error> {
        if self.promised_epoch != epoch {
            return Err(Error::WrongEpoch {
                expected: self.promised_epoch,
            });
        }
        if *self.log_epochs.last().unwrap() != last_entry {
            return Err(Error::InvalidReplicaState);
        }
        assert!(self.current_epoch <= epoch);

        // move current_epoch forward
        self.current_epoch = epoch;
        self.log_epochs.push((epoch, 0));
        // reset ack info for new epoch
        for (len, pid) in self.current_epoch_acks.iter_mut() {
            *len = if *pid == self.pid || *pid == epoch.owner() {
                self.log.len() as u64 // we know about ourselves and leader
            } else {
                0 // for others we wait for ack from current epoch
            };
        }

        self.accepts.insert(self.pid);
        self.accepts.insert(epoch.owner()); // we also know leader is up-to-date
        Ok(())
    }

    /// Add a client proposal to be proposed when primary.
    pub fn add_proposal<I>(&mut self, msg_id: MsgId, msg: Vec<u8>, dest: I) -> Result<(), Error>
    where
        I: IntoIterator<Item=Gid>
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
        if self.msgid_to_idx.contains_key(&msg_id) ||
            self.proposals.iter().find(|e| e.msg_id == msg_id).is_some()
        {
            return Err(Error::IdAlreadyUsed);
        }

        self.proposals.push_back(LogEntry {
            ts: 0,
            msg_id,
            msg,
            dest,
            final_ts: None,
        });
        Ok(())
    }

    /// Assign a timestamp to queued messages and append the entry to the log. Must be primary.
    pub fn propose(&mut self) -> Result<(), Error> {
        if self.state != ReplicaState::Primary {
            return Err(Error::NotPrimary);
        }
        while let Some(mut log_entry) = self.proposals.pop_front() {
            let ts = self.clock.tick();
            log_entry.ts = ts;
            self.append_inner(self.log.len() as u64, self.current_epoch, log_entry).unwrap();
        }
        Ok(())
    }

    /// Get the entry at a given log position.
    pub fn log_entry(&self, idx: u64) -> Option<&LogEntry> {
        self.log.get(usize::try_from(idx).expect("out of range log idx"))
    }

    /// Get the entry at a given log position, mutably
    fn log_entry_mut(&mut self, idx: u64) -> Option<&mut LogEntry> {
        self.log.get_mut(usize::try_from(idx).expect("out of range log idx"))
    }

    pub fn log_entry_for_remote(&self, idx: u64) -> Option<RemoteEntry> {
        if let Some(entry) = self.log.get(usize::try_from(idx).expect("out of range log idx")) {
            // derive entry epoch from the log_epochs array
            let mut epoch = None;
            let mut total_len = 0;
            for &(e, len) in &self.log_epochs {
                total_len += len;
                if total_len > idx {
                    epoch = Some(e);
                    break;
                }
            }

            return Some(RemoteEntry {
                epoch: epoch.unwrap(),
                idx,
                msg_id: entry.msg_id,
                ts: entry.ts,
                dest: entry.dest.clone(),
            });
        }
        None
    }

    /// Get log entry by msg_id
    pub fn log_entry_by_id(&self, msg_id: MsgId) -> Option<&LogEntry> {
        if let Some(idx) = self.msgid_to_idx.get(&msg_id) {
            let e = self.log_entry(*idx).expect("should be present");
            assert_eq!(msg_id, e.msg_id, "mapping from msg_id to log idx should be valid");
            return Some(e);
        }
        None
    }

    /// Get the next log entry destined for to a given Gid, starting at idx.
    /// Needed by replicas from remote groups to fetch relevant log entries.
    pub fn next_log_entry_for_dest(&self, start_idx: u64, gid: Gid) -> Option<&LogEntry> {
        let idx = usize::try_from(start_idx).expect("out of range log idx");
        self.log[idx..].iter().find(|it| {
            it.dest.contains(gid)
        })
    }

    /// Helper method for properly appending to the log
    fn append_inner(&mut self, idx: u64, entry_epoch: Epoch, entry: LogEntry) -> Result<&LogEntry, Error> {
        use std::collections::hash_map::Entry;

        if self.log.len() as u64 != idx {
            return Err(Error::InvalidIndex { len: self.log.len() as u64 });
        }

        // add msg_id mapping
        match self.msgid_to_idx.entry(entry.msg_id) {
            Entry::Vacant(e) => {
                e.insert(self.log.len() as u64);
                match self.pending.entry(entry.msg_id) {
                    Entry::Occupied(mut e) => {
                        let m = e.get_mut();
                        debug_assert!(self.pending_ts.iter().find(|(_,id)| *id == m.id).is_none());
                        // debug_assert!(m.missing_group_ts.len() < m.dest.len());
                        m.min_ts = std::cmp::max(entry.ts, m.min_ts);
                        self.pending_ts.insert((m.min_ts, m.id));
                    }
                    Entry::Vacant(e) => {
                        // first time we see the msg
                        e.insert(PendingMsg { id: entry.msg_id, dest: entry.dest.clone(), min_ts: entry.ts, missing_group_ts: entry.dest.clone() });
                        self.pending_ts.insert((entry.ts, entry.msg_id));
                    }
                }
            }
            Entry::Occupied(_) => {
                panic!("unexpected msg_id present");
            }
        }

        // add to log_epochs
        if matches!(self.log_epochs.last(), Some((last_epoch, _)) if *last_epoch == entry_epoch) {
            self.log_epochs.last_mut().unwrap().1 += 1;
        } else {
            self.log_epochs.push((entry_epoch, 1));
        }

        // add to log
        assert!(entry.ts <= self.clock.local());
        assert!(self.log.last().is_none() || self.log.last().unwrap().ts < entry.ts);
        self.log.push(entry);

        // update own ack
        let len = self.log.len() as u64;
        let ack = self.get_ack_mut(self.pid);
        *ack = std::cmp::max(*ack, len);

        Ok(self.log.last().unwrap())
    }

    /// Append log entry from the leader
    pub fn append(&mut self, epoch: Epoch, idx: u64, entry: LogEntry) -> Result<&LogEntry, Error> {
        if self.promised_epoch > epoch || self.current_epoch > epoch {
            return Err(Error::EpochTooOld {
                promised: self.promised_epoch,
                current: self.promised_epoch,
            });
        }
        if self.current_epoch != epoch {
            return Err(Error::WrongEpoch { expected: self.current_epoch });
        }
        if self.state != ReplicaState::Follower {
            return Err(Error::NotFollower);
        }

        self.clock.update(epoch.owner(), epoch, entry.ts);
        self.append_inner(idx, epoch, entry)
    }

    /// Ack from a replica from our group.
    /// Also servers as a bump message, as replicas in a group keep exchanging this info.
    pub fn add_ack(&mut self, pid: Pid, epoch: Epoch, log_len: u64, clock: Clock) -> Result<(), Error> {
        use ReplicaState::*;
        if self.promised_epoch == epoch {
            if self.state == Candidate || self.state == Promised {
                self.accepts.insert(pid);
                // TODO: check replica goes to primary/follower
            }
        }

        // ignore acks from epochs different from our log's current_epoch
        if epoch != self.current_epoch {
            return Ok(());
        }

        // update acked len
        let ack = self.get_ack_mut(pid);
        *ack = std::cmp::max(*ack, log_len);
        // update clock
        self.clock.update(pid, epoch, clock);

        Ok(())
    }

    /// Set the log epoch the remote learner is following.
    pub fn remote_update_log_epoch(&mut self, gid: Gid, epoch: Epoch) -> Result<(), Error> {
        let learner = self.remote_learners.get_mut(&gid).unwrap();
        Ok(learner.update_log_epoch(epoch)?)
    }

    /// Return the log epoch the remote learner is following, and what is the
    /// next expected log idx.
    pub fn remote_expected_entry(&mut self, gid: Gid) -> (Epoch, u64) {
        let learner = self.remote_learners.get_mut(&gid).unwrap();
        learner.next_expected_log_entry()
    }

    /// Append the next relevant entry from the remote replica
    pub fn remote_append(&mut self, gid: Gid, entry: RemoteEntry) -> Result<(), Error> {
        let learner = self.remote_learners.get_mut(&gid).unwrap();
        Ok(learner.append_log_entry(gid, entry)?)

    }

    /// Add information about the given remote replica
    pub fn remote_add_ack(&mut self, gid: Gid, pid: Pid, epoch: Epoch, log_len: u64, clock: u64) -> Result<(), Error> {
        self.clock.update(pid, epoch, clock);
        let learner = self.remote_learners.get_mut(&gid).unwrap();
        Ok(learner.add_remote_ack(gid, pid, epoch, log_len)?)
    }

    /// Update calculated replica state from received info.
    /// Update safe_len, update pending msg state from local group and remote learners
    pub fn update(&mut self) {
        // update safe len
        self.current_epoch_acks.sort(); // sort by acked log len
        let safe_len_from_acks = self.current_epoch_acks[self.group_size - self.quorum_size].0;
        for entry in &mut self.log[self.safe_len as usize .. safe_len_from_acks as usize] {
            let p = self.pending.get_mut(&entry.msg_id).unwrap();
            assert!(p.missing_group_ts.remove(self.gid));
        }
        self.safe_len = std::cmp::max(self.safe_len, safe_len_from_acks);

        // update from remote learners
        for (gid, l) in &mut self.remote_learners {
            l.update();
            while let Some((id, dest, ts)) = l.next_delivery() {
                use std::collections::hash_map::Entry;
                match self.pending.entry(id) {
                    Entry::Occupied(mut e) => {
                        // update existing pending state
                        let e = e.get_mut();
                        assert!(e.missing_group_ts.remove(*gid));
                        let old_min_ts = e.min_ts;
                        e.min_ts = std::cmp::max(e.min_ts, ts);
                        // update ordered ts if present
                        if self.pending_ts.remove(&(old_min_ts, e.id)) {
                            self.pending_ts.insert((e.min_ts, e.id));
                        }
                    }
                    Entry::Vacant(e) => {
                        // create new pending state
                        let e = e.insert(PendingMsg { id, dest: dest.clone(), min_ts: ts, missing_group_ts: dest });
                        e.missing_group_ts.remove(*gid);
                    }
                }
            }
        }
    }

    pub fn min_clock_leader(&self) -> Clock {
        self.clock.get(self.current_epoch.owner())
    }

    pub fn min_new_epoch_ts(&mut self) -> Clock {
        1 + self.clock.quorum(self.quorum_size)
    }

    pub fn next_delivery(&mut self) -> Option<&LogEntry> {
        let (ts, id) = self.pending_ts.iter().cloned().next()?;
        use std::collections::hash_map::Entry;
        let p = match self.pending.entry(id) {
            Entry::Occupied(mut e) => {
                if e.get_mut().missing_group_ts.is_empty() {
                    self.pending_ts.remove(&(ts, id));
                    assert_eq!(e.get().min_ts, ts);
                    e.remove()
                } else {
                    return None;
                }
            }
            Entry::Vacant(e) => {
                return None;
            }
        };
        let e = self.log_entry_mut(self.msgid_to_idx[&id]).unwrap();
        e.final_ts = Some(ts);
        Some(e)
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

        let mut r0 = GroupReplica::new(Gid(0), Pid(0), config.clone());
        let mut r1 = GroupReplica::new(Gid(0), Pid(1), config.clone());
        let mut r2 = GroupReplica::new(Gid(0), Pid(2), config.clone());

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
        assert!(r0.add_proposal(r0.log_entry(0).unwrap().msg_id, "foo".into(), [Gid(0)]).is_err());
        // group must be a destination
        assert!(r0.add_proposal(idgen.next(), "foo".into(), [Gid(1)]).is_err());

        // append to followers
        assert!(r1.append(r0.current_epoch, 0, r0.log_entry(0).cloned().unwrap()).is_ok());
        assert!(r2.append(r0.current_epoch, 0, r0.log_entry(0).cloned().unwrap()).is_ok());
        assert!(r1.append(r0.current_epoch, 1, r0.log_entry(1).cloned().unwrap()).is_ok());
        assert!(r2.append(r0.current_epoch, 1, r0.log_entry(1).cloned().unwrap()).is_ok());

        // invalid appends
        assert!(r1.append(r0.current_epoch, 0, r0.log_entry(0).cloned().unwrap()).is_err());
        assert!(r1.append(r0.current_epoch, 4, r0.log_entry(0).cloned().unwrap()).is_err());

        // ack info from followers
        assert!(r0.add_ack(r1.pid, r1.current_epoch, r1.log.len() as u64, r1.clock.local()).is_ok());
        assert!(r0.add_ack(r2.pid, r2.current_epoch, r2.log.len() as u64, r2.clock.local()).is_ok());

        r0.update();

        assert_eq!(r0.log.len(), 3);
        assert_eq!(r0.safe_len, 2);
        assert_eq!(r0.clock.quorum(r0.quorum_size), 2);
    }

    #[test]
    fn group_replica_global_msg() {
        let config = Config::new_for_test();
        let mut idgen = IdGen(0);

        let mut r0_0 = GroupReplica::new(Gid(0), Pid(0), config.clone());
        let mut r0_1 = GroupReplica::new(Gid(0), Pid(1), config.clone());
        let mut r1_0 = GroupReplica::new(Gid(1), Pid(0), config.clone());
        let mut r1_1 = GroupReplica::new(Gid(1), Pid(1), config.clone());

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
        assert!(r0_1.append(r0_0.current_epoch, 0, r0_0.log_entry(0).cloned().unwrap()).is_ok());
        assert!(r1_1.append(r1_0.current_epoch, 0, r1_0.log_entry(0).cloned().unwrap()).is_ok());

        // append/ack from primaries to remote groups ("matchings Pid's" in each
        // group follow each other, only acks from other remotes)

        assert!(r0_0.remote_append(Gid(1), r1_0.log_entry_for_remote(0).unwrap()).is_ok());
        assert!(r0_0.remote_add_ack(Gid(1), r1_0.pid, r1_0.current_epoch, r1_0.log.len() as u64, r1_0.clock.local()).is_ok());
        assert!(r0_1.remote_add_ack(Gid(1), r1_0.pid, r1_0.current_epoch, r1_0.log.len() as u64, r1_0.clock.local()).is_ok());

        assert!(r1_0.remote_append(Gid(0), r0_0.log_entry_for_remote(0).unwrap()).is_ok());
        assert!(r1_0.remote_add_ack(Gid(0), r0_0.pid, r0_0.current_epoch, r0_0.log.len() as u64, r0_0.clock.local()).is_ok());
        assert!(r1_1.remote_add_ack(Gid(0), r0_0.pid, r0_0.current_epoch, r0_0.log.len() as u64, r0_0.clock.local()).is_ok());

        // ----- STEP 3 ------

        // follower acks back to leaders/each other (the ack here also works as the <bump> message in the protocol)
        assert!(r0_0.add_ack(r0_1.pid, r0_1.current_epoch, r0_1.log.len() as u64, r0_1.clock.local()).is_ok());
        assert!(r0_1.add_ack(r0_0.pid, r0_0.current_epoch, r0_0.log.len() as u64, r0_0.clock.local()).is_ok());

        assert!(r1_0.add_ack(r1_1.pid, r1_1.current_epoch, r1_1.log.len() as u64, r1_1.clock.local()).is_ok());
        assert!(r1_1.add_ack(r1_0.pid, r1_0.current_epoch, r1_0.log.len() as u64, r1_0.clock.local()).is_ok());

        // follower append/ack to remote learners
        assert!(r0_1.remote_append(Gid(1), r1_1.log_entry_for_remote(0).unwrap()).is_ok());
        assert!(r0_0.remote_add_ack(Gid(1), r1_1.pid, r1_1.current_epoch, r1_1.log.len() as u64, r1_1.clock.local()).is_ok());
        assert!(r0_1.remote_add_ack(Gid(1), r1_1.pid, r1_1.current_epoch, r1_1.log.len() as u64, r1_1.clock.local()).is_ok());

        assert!(r1_1.remote_append(Gid(0), r0_1.log_entry_for_remote(0).unwrap()).is_ok());
        assert!(r1_0.remote_add_ack(Gid(0), r0_1.pid, r0_1.current_epoch, r0_1.log.len() as u64, r0_1.clock.local()).is_ok());
        assert!(r1_1.remote_add_ack(Gid(0), r0_1.pid, r0_1.current_epoch, r0_1.log.len() as u64, r0_1.clock.local()).is_ok());

        // ----- CHECKS ------

        // check in every replica msg is deliverable
        for r in &mut [r0_0, r0_1, r1_0, r1_1] {
            r.update();

            // local group timestamp is quorum safe
            assert_eq!(r.safe_len, 1);
            // check that final_ts is learned
            let (pending_ts, pending_id) = r.pending_ts.iter().next().unwrap();
            assert_eq!(*pending_id, id);
            assert_eq!(*pending_ts, 4);
            let p = r.pending.get(pending_id).unwrap();
            assert_eq!(p.min_ts, 4);
            assert!(p.missing_group_ts.is_empty());
            // check that msgs is deliverable
            assert_eq!(r.min_clock_leader(), 4);
            assert_eq!(r.clock.quorum(r.quorum_size), 4);

            // thus, msg should be deliverable
            let entry = r.next_delivery().unwrap();
            assert_eq!(entry.final_ts, Some(4));
            assert_eq!(entry.msg_id, id);
            assert!(r.next_delivery().is_none());
        }
    }

    fn group_replica_epoch_change() {
        unimplemented!()
    }
}
