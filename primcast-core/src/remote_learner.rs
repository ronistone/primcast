use crate::types::*;

use rustc_hash::FxHashMap as HashMap;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

#[derive(Debug, Clone)]
/// Information kept for each replica in the remote group
pub struct RemoteInfo {
    epoch: Epoch,
    log_len: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteEntry {
    pub epoch: Epoch,
    pub idx: u64,
    pub msg_id: MsgId,
    pub ts: Clock,
    pub dest: GidSet,
}

#[derive(Debug)]
pub enum Error {
    EpochTooOld { current: Epoch },
    OutOfOrderAppend,
}

/// Learner for remote group timestamps.
/// N.B. if group replicas keep their full log, no part of the remote learner state needs to be persisted.
#[derive(Debug)]
pub struct RemoteLearner {
    remote_gid: Gid,
    remote_info: HashMap<Pid, RemoteInfo>,
    remote_log_epoch: Epoch,
    remote_log: VecDeque<RemoteEntry>,
    next_idx: u64,
    safe_idx: Option<u64>,
    quorum_size: usize,
}

impl RemoteLearner {
    pub fn new<I: IntoIterator<Item = Pid>>(
        remote_gid: Gid,
        remote_pids: I,
        next_idx: u64,
        quorum_size: usize,
    ) -> Self {
        RemoteLearner {
            remote_log_epoch: Epoch(0, Pid(0)),
            remote_gid,
            remote_info: remote_pids
                .into_iter()
                .map(|p| {
                    (
                        p,
                        RemoteInfo {
                            epoch: Epoch::initial(),
                            log_len: 0,
                        },
                    )
                })
                .collect(),
            remote_log: Default::default(),
            safe_idx: None,
            next_idx,
            quorum_size,
        }
    }

    pub fn remote_gid(&self) -> Gid {
        self.remote_gid
    }

    /// Next expected log entry from the remote group.
    /// The learner follows the log from a specific epoch (see `update_log_epoch`).
    pub fn next_expected_log_entry(&self) -> (Epoch, u64) {
        match self.remote_log.back() {
            Some(entry) => (self.remote_log_epoch, entry.idx + 1),
            None => (self.remote_log_epoch, self.next_idx),
        }
    }

    /// Append info about the next log entry.
    /// The learner expects to know about remote entries _in log order_, but only those entries destined to the local group.
    pub fn append(&mut self, gid: Gid, entry: RemoteEntry) -> Result<(), Error> {
        assert_eq!(self.remote_gid, gid);
        // the entry must come from an epoch earlier than our current knowledge of that group's epoch.
        if entry.epoch > self.remote_log_epoch {
            return Err(Error::OutOfOrderAppend);
        }
        // entry can't be older than our next expected delivery idx
        if entry.idx < self.next_idx {
            return Err(Error::OutOfOrderAppend);
        }
        // ensure entry is more recent than our previous entry in the log
        if let Some(prev) = self.remote_log.back() {
            if prev.epoch > entry.epoch || prev.idx >= entry.idx || prev.ts >= entry.ts {
                return Err(Error::OutOfOrderAppend);
            }
        }
        self.remote_log.push_back(entry);
        Ok(())
    }

    /// Update remote replica info, for tracking safe log entries.
    pub fn add_remote_ack(&mut self, gid: Gid, pid: Pid, log_epoch: Epoch, log_len: u64) -> Result<(), Error> {
        assert_eq!(self.remote_gid, gid);
        let mut info = self.remote_info.get_mut(&pid).expect("pid should be present");
        if (log_epoch, log_len) > (info.epoch, info.log_len) {
            info.epoch = log_epoch;
            info.log_len = log_len;
        }
        Ok(())
    }

    /// The remote learner follows a remote replica in a specific epoch.
    /// If the remote changes epochs, the logs may not be compatible anymore, so we truncate it to quorum safe state.
    pub fn update_log_epoch(&mut self, e: Epoch) -> Result<(), Error> {
        if e < self.remote_log_epoch {
            return Err(Error::EpochTooOld {
                current: self.remote_log_epoch,
            });
        }
        self.update(); // update the current safe idx before changing our epoch avoid truncating unnecessarily
        self.remote_log_epoch = e;
        if let Some(safe_idx) = self.safe_idx {
            while let Some(entry) = self.remote_log.pop_back() {
                if entry.idx <= safe_idx {
                    self.remote_log.push_back(entry);
                    break;
                }
            }
        } else {
            // no quorum safe entries
            self.remote_log.clear();
        }
        Ok(())
    }

    /// Based on information know from remote replicas we know when a given
    /// log prefix is safe. Any two remote group replicas in the same Epoch will
    /// have compatible logs (i.e., one is a prefix of the other). Thus, if a
    /// quorum of processes are in the same Epoch (and so is our learner log),
    /// we can use the min of the log lenghts to track the safe prefix of the
    /// log.
    ///
    /// If in remote info we keep there's no quorum of replicas in the same
    /// epoch, we can't derive a safe idx, and must rely on a previously
    /// calculated value `self.safe_idx`.
    fn calculate_safe_idx_from_remote_info(&self) -> Option<u64> {
        let mut same_epoch_log_sizes: Vec<_> = self
            .remote_info
            .values()
            .filter_map(|info| {
                if info.epoch == self.remote_log_epoch {
                    Some(info.log_len)
                } else {
                    None
                }
            })
            .collect();
        let q_diff = same_epoch_log_sizes.len() as isize - self.quorum_size as isize;
        if q_diff >= 0 {
            same_epoch_log_sizes.sort_unstable();
            let safe_len = same_epoch_log_sizes[q_diff as usize];
            if safe_len > 0 {
                Some(safe_len - 1)
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Update self.safe_idx from received remote info.
    pub fn update(&mut self) {
        self.safe_idx = std::cmp::max(self.safe_idx, self.calculate_safe_idx_from_remote_info());
    }

    pub(crate) fn safe_idx(&self) -> Option<u64> {
        self.safe_idx
    }

    pub(crate) fn remote_info(&self) -> impl Iterator<Item = (Pid, Epoch, u64)> + '_ {
        self.remote_info
            .iter()
            .map(|(pid, info)| (*pid, info.epoch, info.log_len))
    }

    /// Return the next learned timetamp, if any.
    /// `update` should be called once before making calls to this.
    pub fn next_delivery(&mut self) -> Option<(MsgId, GidSet, Clock)> {
        if let Some(safe_idx) = self.safe_idx {
            if let Some(entry) = self.remote_log.pop_front() {
                if entry.idx <= safe_idx {
                    self.next_idx = entry.idx + 1;
                    return Some((entry.msg_id, entry.dest, entry.ts));
                } else {
                    self.remote_log.push_front(entry);
                }
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_learner_basics() {
        let gid = Gid(1);
        let quorum_size = 2;
        let mut r = RemoteLearner::new(gid, [Pid(1), Pid(2), Pid(3)], 2, quorum_size);
        assert_eq!(r.next_expected_log_entry(), (Epoch::initial(), 2));

        let mut epoch = Epoch::initial();

        let dest = GidSet::from_iter([Gid(0), Gid(1)].into_iter());

        assert!(r
            .append(
                gid,
                RemoteEntry {
                    idx: 2,
                    epoch,
                    msg_id: 2,
                    ts: 2,
                    dest: dest.clone(),
                },
            )
            .is_ok());
        assert!(r
            .append(
                gid,
                RemoteEntry {
                    idx: 3,
                    epoch,
                    msg_id: 3,
                    ts: 3,
                    dest: dest.clone(),
                },
            )
            .is_ok());
        assert!(r
            .append(
                gid,
                RemoteEntry {
                    idx: 5,
                    epoch,
                    msg_id: 5,
                    ts: 5,
                    dest: dest.clone(),
                },
            )
            .is_ok());
        assert!(r
            .append(
                gid,
                RemoteEntry {
                    idx: 7,
                    epoch,
                    msg_id: 7,
                    ts: 7,
                    dest: dest.clone(),
                },
            )
            .is_ok());

        // quorum replicas ack for log length 3, thus log idx 2 should be safe
        assert!(r.add_remote_ack(gid, Pid(1), Epoch::initial(), 3).is_ok());
        assert!(r.add_remote_ack(gid, Pid(2), Epoch::initial(), 3).is_ok());

        // only log idx 2 can be delivered
        let mut d = vec![];
        r.update();
        while let Some((id, _, ts)) = r.next_delivery() {
            d.push((id, ts));
        }
        assert_eq!(d.len(), 1);
        assert_eq!(d[0], (2, 2));

        // quorum replicas ack for log length 4, thus log idx 3 should be safe
        assert!(r.add_remote_ack(gid, Pid(1), Epoch::initial(), 4).is_ok());
        assert!(r.add_remote_ack(gid, Pid(2), Epoch::initial(), 4).is_ok());

        // idx 3 should be deliverable
        r.update();
        while let Some((id, _, ts)) = r.next_delivery() {
            d.push((id, ts));
        }

        assert_eq!(d.len(), 2);
        assert_eq!(d[1], (3, 3));

        // move to a higher epoch, which truncates the log to what we know to be safe
        epoch = epoch.next_for(Pid(0));
        assert!(r.update_log_epoch(epoch).is_ok());

        // append some more entries
        assert!(r
            .append(
                gid,
                RemoteEntry {
                    idx: 5,
                    epoch,
                    msg_id: 5,
                    ts: 5,
                    dest: dest.clone(),
                },
            )
            .is_ok());
        assert!(r
            .append(
                gid,
                RemoteEntry {
                    idx: 6,
                    epoch,
                    msg_id: 6,
                    ts: 6,
                    dest: dest.clone(),
                },
            )
            .is_ok());
        // old epoch info should be ignored
        assert!(r.add_remote_ack(gid, Pid(2), Epoch::initial(), 5).is_ok());
        assert!(r.add_remote_ack(gid, Pid(3), Epoch::initial(), 5).is_ok());

        assert!(r.add_remote_ack(gid, Pid(2), epoch, 7).is_ok());
        assert!(r.add_remote_ack(gid, Pid(3), epoch, 7).is_ok());

        // idx 5 and 6 should be deliverable
        r.update();
        while let Some((id, _, ts)) = r.next_delivery() {
            d.push((id, ts));
        }
        assert_eq!(d.len(), 4);
        assert_eq!(d[2], (5, 5));
        assert_eq!(d[3], (6, 6));
    }
}
