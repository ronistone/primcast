use crate::types::*;
use std::cmp::max;

/// Tracks info about logical clock values for a process and its group. For
/// safety, clock values from higher epochs coming from other processes must be
/// ignored until the local epoch catches up.
#[derive(Debug, Clone)]
pub struct LogicalClock {
    pid: Pid,
    epoch: Epoch,
    sorted: bool,
    clocks: Vec<(Pid, Clock)>,
}

impl LogicalClock {
    pub fn new<I: IntoIterator<Item = Pid>>(pid: Pid, epoch: Epoch, group: I) -> Self {
        let clocks = group.into_iter().map(|pid| (pid, 0)).collect::<Vec<_>>();
        clocks.iter().find(|(p, _)| *p == pid).expect("pid not found");
        LogicalClock {
            pid,
            epoch,
            sorted: false,
            clocks,
        }
    }

    /// Value of the clock for given pid
    pub fn get(&self, pid: Pid) -> Clock {
        self.clocks
            .iter()
            .find(|(p, _)| pid == *p)
            .map(|(_, c)| *c)
            .expect("pid not found")
    }

    fn get_mut(&mut self, pid: Pid) -> &mut Clock {
        self.clocks
            .iter_mut()
            .find(|(p, _)| pid == *p)
            .map(|(_, c)| c)
            .expect("pid not found")
    }

    /// Value of the local clock
    pub fn local(&self) -> Clock {
        self.get(self.pid)
    }

    pub fn current_epoch(&self) -> Epoch {
        self.epoch
    }

    /// Value for which a quorum of clocks are equal/higher
    pub fn quorum(&mut self, quorum_size: usize) -> Clock {
        debug_assert!(quorum_size > self.clocks.len() / 2);
        debug_assert!(quorum_size <= self.clocks.len());
        if !self.sorted {
            // self.clocks.sort_unstable_by(|a,b| a.1.cmp(&b.1));
            self.clocks.sort_unstable_by_key(|(_, c)| *c);
            self.sorted = true;
        }
        self.clocks[(quorum_size - 1)].1
    }

    /// Update clock for the given pid, if larger
    pub fn update(&mut self, pid: Pid, epoch: Epoch, clock: Clock) {
        if epoch < self.epoch {
            // ignore old epoch values
            return;
        }

        if epoch > self.epoch {
            if pid == self.pid {
                self.epoch = epoch;
            } else {
                // ignore higher epoch values from other replicas
                return;
            }
        }

        // update clock and invalidate cached majority if needed
        let c = self.get_mut(pid);
        if *c < clock {
            *c = clock;
            self.sorted = false;

            if pid != self.pid {
                // ensure bump of local clock
                let c = self.get_mut(self.pid);
                *c = max(*c, clock);
            }
        }
    }

    /// Increment clock and return its value
    pub fn tick(&mut self) -> Clock {
        let c = self.get_mut(self.pid);
        *c += 1;
        *c
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn clock_group_missing_pid() {
        let _ = LogicalClock::new(Pid(0), Epoch::initial(), [Pid(1), Pid(2), Pid(3)]);
    }

    #[test]
    #[should_panic]
    fn clock_group_missing_pid_update() {
        let mut c = LogicalClock::new(Pid(1), Epoch::initial(), [Pid(1), Pid(2), Pid(3)]);
        c.update(Pid(0), Epoch::initial(), 1);
    }

    #[test]
    fn clock_basics() {
        let e1 = Epoch::initial();
        let e2 = e1.next_for(Pid(1));
        let e3 = e2.next_for(Pid(1));
        let q = 3;
        let mut c = LogicalClock::new(Pid(1), e1, [Pid(1), Pid(2), Pid(3), Pid(4)]);
        assert_eq!(c.current_epoch(), e1);
        assert_eq!(c.local(), 0);
        assert_eq!(c.quorum(q), 0);

        c.update(Pid(1), e2, 0);
        assert_eq!(c.current_epoch(), e2);
        assert_eq!(c.local(), 0);
        assert_eq!(c.quorum(q), 0);

        c.update(Pid(1), e2, 2); // 2,0,0,0
        assert_eq!(c.local(), 2);
        assert_eq!(c.quorum(q), 0);

        c.update(Pid(2), e2, 1); // 2,1,0,0
        assert_eq!(c.local(), 2);
        assert_eq!(c.quorum(q), 1);

        c.update(Pid(3), e2, 2); // 2,1,2,0
        assert_eq!(c.local(), 2);
        assert_eq!(c.quorum(q), 2);

        // local always gets updated
        c.update(Pid(3), e2, 3); // 3,1,3,0
        assert_eq!(c.local(), 3);
        assert_eq!(c.quorum(q), 3);

        // ignore old epoch values
        c.update(Pid(2), e1, 99); // 3,1,3,0
        assert_eq!(c.current_epoch(), e2);
        assert_eq!(c.local(), 3);
        assert_eq!(c.quorum(q), 3);

        // ignore higher epoch values from other nodes
        c.update(Pid(2), e3, 99); // 3,1,3,0
        assert_eq!(c.current_epoch(), e2);
        assert_eq!(c.local(), 3);
        assert_eq!(c.quorum(q), 3);

        assert_eq!(c.tick(), 4); // 4,1,3,0
        assert_eq!(c.quorum(q), 3);
        assert_eq!(c.tick(), 5); // 5,1,3,0
        assert_eq!(c.quorum(q), 3);
    }
}
