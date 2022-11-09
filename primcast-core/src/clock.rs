use crate::types::*;
use rustc_hash::FxHashMap as HashMap;

/// Tracks info about logical clock values for a process and its group. For
/// safety, clock values from higher epochs coming from other processes must be
/// ignored until the local epoch catches up.
#[derive(Debug, Clone)]
pub struct LogicalClock {
    pid: Pid,
    epoch: Epoch,
    sorted: bool,
    clocks: Vec<(Pid, Clock)>,
    /// store acks from the "future" to be applied later when moving the epoch of the clock
    future_epochs: HashMap<Epoch, HashMap<Pid, Clock>>,
    hybrid: bool,
}

impl LogicalClock {
    pub fn new<I: IntoIterator<Item = Pid>>(pid: Pid, epoch: Epoch, group: I, hybrid: bool) -> Self {
        let clocks = group.into_iter().map(|pid| (pid, 0)).collect::<Vec<_>>();
        clocks.iter().find(|(p, _)| *p == pid).expect("pid not found");
        LogicalClock {
            pid,
            epoch,
            sorted: false,
            clocks,
            future_epochs: Default::default(),
            hybrid,
        }
    }

    /// Value of the clock for given pid
    pub fn get(&self, pid: Pid) -> Clock {
        self.clocks
            .iter()
            .find_map(|(p, c)| if pid == *p { Some(*c) } else { None })
            .expect("pid not found")
    }

    fn get_mut(&mut self, pid: Pid) -> &mut Clock {
        self.clocks
            .iter_mut()
            .find_map(|(p, c)| if pid == *p { Some(c) } else { None })
            .expect("pid not found")
    }

    /// Value of the local clock
    pub fn local(&self) -> Clock {
        self.get(self.pid)
    }

    /// Current epoch being considered
    pub fn current_epoch(&self) -> Epoch {
        self.epoch
    }

    /// Value for which a quorum of clocks are equal/higher, in the given epoch
    pub fn quorum(&mut self, quorum_size: usize, epoch: Epoch) -> Option<Clock> {
        debug_assert!(quorum_size > self.clocks.len() / 2);
        debug_assert!(quorum_size <= self.clocks.len());
        if !self.sorted {
            self.clocks.sort_unstable_by_key(|(_, c)| std::cmp::Reverse(*c));
            self.sorted = true;
        }
        if epoch >= self.epoch {
            Some(self.clocks[quorum_size - 1].1)
        } else {
            None
        }
    }

    /// Update clock for the given pid, if larger
    pub fn update(&mut self, pid: Pid, epoch: Epoch, clock: Clock) {
        if epoch > self.epoch {
            // store future epoch clock values
            use std::collections::hash_map::Entry;
            match self.future_epochs.entry(epoch) {
                Entry::Occupied(mut e) => match e.get_mut().entry(pid) {
                    Entry::Occupied(mut e) => {
                        *e.get_mut() = std::cmp::max(clock, *e.get_mut());
                    }
                    Entry::Vacant(e) => {
                        e.insert(clock);
                    }
                },
                Entry::Vacant(e) => {
                    let e = e.insert(HashMap::default());
                    e.insert(pid, clock);
                }
            }
        } else {
            // update pid clock and invalidate cached majority if needed
            let c = self.get_mut(pid);
            if *c < clock {
                *c = clock;
                self.sorted = false;
            }
        }
    }

    pub fn advance_epoch(&mut self, new_epoch: Epoch) -> bool {
        if new_epoch <= self.epoch {
            return false;
        }
        self.epoch = new_epoch;
        // apply clocks received before we moved epochs
        let tmp = std::mem::take(&mut self.future_epochs);
        let (le, gt) = tmp.into_iter().partition(|(e, _)| *e <= new_epoch);
        self.future_epochs = gt;
        for (_, clocks) in le {
            for (pid, clock) in clocks {
                self.update(pid, new_epoch, clock);
            }
        }
        self.sorted = false;
        true
    }

    /// Increment clock and return its value
    pub fn tick(&mut self) -> Clock {
        self.sorted = false;
        let hybrid = self.hybrid;
        let c = self.get_mut(self.pid);
        if hybrid {
            // micros from UNIX_EPOCH
            let now = (chrono::Utc::now().timestamp_nanos() / 1000) as u64;
            *c = std::cmp::max(now, *c + 1);
        } else {
            *c += 1;
        }
        *c
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn clock_group_missing_pid() {
        let _ = LogicalClock::new(Pid(0), Epoch::initial(), [Pid(1), Pid(2), Pid(3)], false);
    }

    #[test]
    #[should_panic]
    fn clock_group_missing_pid_update() {
        let mut c = LogicalClock::new(Pid(1), Epoch::initial(), [Pid(1), Pid(2), Pid(3)], false);
        c.update(Pid(0), Epoch::initial(), 1);
    }

    #[test]
    fn clock_basics() {
        let e1 = Epoch::initial();
        let e2 = e1.next_for(Pid(1));
        let e3 = e2.next_for(Pid(1));
        let q = 3;
        let mut c = LogicalClock::new(Pid(1), e1, [Pid(1), Pid(2), Pid(3), Pid(4)], false);
        assert_eq!(c.current_epoch(), e1);
        assert_eq!(c.local(), 0);
        assert_eq!(c.quorum(q, e1), Some(0));

        // higher epoch values get stored for when epoch is advanced
        c.update(Pid(1), e2, 2);
        c.update(Pid(3), e2, 2);
        assert_eq!(c.current_epoch(), e1);
        assert_eq!(c.local(), 0);
        assert_eq!(c.quorum(q, e1), Some(0));

        assert!(c.advance_epoch(e2)); // 2,0,2,0
        assert!(!c.advance_epoch(e1));
        assert_eq!(c.local(), 2);
        assert_eq!(c.quorum(q, e2), Some(0));

        // quorum for lower epochs are lost
        assert!(c.quorum(q, e1).is_none());

        c.update(Pid(2), e2, 1); // 2,1,0,0
        assert_eq!(c.local(), 2);
        assert_eq!(c.quorum(q, e2), Some(1));

        c.update(Pid(3), e2, 2); // 2,1,2,0
        assert_eq!(c.local(), 2);
        assert_eq!(c.quorum(q, e2), Some(1));

        c.update(Pid(3), e2, 3); // 2,1,3,0
        assert_eq!(c.local(), 2);
        assert_eq!(c.quorum(q, e2), Some(1));

        c.update(Pid(2), e2, 2); // 2,2,3,0
        assert_eq!(c.local(), 2);
        assert_eq!(c.quorum(q, e2), Some(2));

        // quorum for current epoch is lower bound for higher epochs
        assert_eq!(c.quorum(q, e3), Some(2));

        // old epoch values can still update clock
        c.update(Pid(2), e1, 4); // 2,4,3,0
        assert_eq!(c.quorum(q, e2), Some(2));
        c.update(Pid(3), e1, 4); // 2,4,4,0
        assert_eq!(c.quorum(q, e2), Some(2));
        c.update(Pid(4), e1, 4); // 2,4,4,4
        assert_eq!(c.current_epoch(), e2);
        assert_eq!(c.local(), 2);
        assert_eq!(c.quorum(q, e2), Some(4));

        assert_eq!(c.tick(), 3); // 3,4,4,4
        assert_eq!(c.local(), 3);
        assert_eq!(c.quorum(q, e2), Some(4));
        assert_eq!(c.tick(), 4); // 4,4,4,4
        assert_eq!(c.local(), 4);
        assert_eq!(c.quorum(q, e2), Some(4));
    }
}
