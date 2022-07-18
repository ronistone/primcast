use priority_queue::PriorityQueue;
use std::cmp::Reverse;
use std::hash::BuildHasherDefault;
use std::time::Instant;

use rustc_hash::FxHashMap as HashMap;
use rustc_hash::FxHasher;

use crate::types::*;

/// Pending message information needed to track delivery
struct PendingMsg {
    dest: GidSet,
    /// timestamp assigned to the msg in the log.
    entry_ts: Option<Clock>,
    /// maximum of decided group ts
    max_group_ts: Option<Clock>,
    /// group ts still to be decided
    missing_group_ts: GidSet,
    last_modified: Instant,
    log_idx: Option<u64>,
}

impl PendingMsg {
    fn final_ts(&self) -> Option<Clock> {
        if self.missing_group_ts.is_empty() {
            Some(self.max_group_ts.unwrap())
        } else {
            None
        }
    }
}

pub struct Stats {
    pub all: usize,
    pub all_max: usize,
    pub with_local_ts: usize,
    pub with_local_ts_max: usize,
}

/// Tracks the timestamp of messages not yet delivered
pub struct PendingSet {
    /// Replica's group
    gid: Gid,
    /// All pending msgs
    all: HashMap<MsgId, PendingMsg>,
    all_max: usize,
    /// Messages with a local timestamp in the local group, sorted by "possible" final timestamp
    // TODO: kind of a waste of space to keep MsgId twice there... but should be fine
    ts_order: PriorityQueue<MsgId, Reverse<(Clock, MsgId)>, BuildHasherDefault<FxHasher>>,
    ts_order_max: usize,
    /// sanity check: items should be popped in final_ts order
    last_popped: (Clock, MsgId),
}

impl PendingSet {
    pub fn new(gid: Gid) -> Self {
        Self {
            gid,
            all: Default::default(),
            all_max: 0,
            ts_order: Default::default(),
            ts_order_max: 0,
            last_popped: (0, 0),
        }
    }

    pub fn stats(&self) -> Stats {
        Stats {
            all: self.all.len(),
            all_max: self.all_max,
            with_local_ts: self.ts_order.len(),
            with_local_ts_max: self.ts_order_max,
        }
    }

    pub fn add_entry_ts(&mut self, msg_id: MsgId, dest: &GidSet, entry_ts: Clock, log_idx: u64) {
        use std::collections::hash_map::Entry;
        let ts;
        debug_assert!((entry_ts, msg_id) > self.last_popped);
        match self.all.entry(msg_id) {
            Entry::Occupied(mut e) => {
                let m = e.get_mut();
                // we've already seen the message through some remote group ts
                assert!(m.missing_group_ts.len() < m.dest.len());
                assert!(m.missing_group_ts.contains(self.gid));
                m.last_modified = Instant::now();
                // msg should not be present in ts_order
                debug_assert!(self.ts_order.get_priority(&msg_id).is_none());
                assert!(m.entry_ts.is_none());
                m.entry_ts = Some(entry_ts);
                m.log_idx = Some(log_idx);
                ts = std::cmp::max(m.entry_ts, m.max_group_ts).unwrap();
            }
            Entry::Vacant(e) => {
                // first time we see the msg
                e.insert(PendingMsg {
                    dest: dest.clone(),
                    entry_ts: Some(entry_ts),
                    max_group_ts: None,
                    missing_group_ts: dest.clone(),
                    last_modified: Instant::now(),
                    log_idx: Some(log_idx),
                });
                ts = entry_ts;
            }
        }
        self.ts_order.push(msg_id, Reverse((ts, msg_id)));
        self.all_max = std::cmp::max(self.all_max, self.all.len());
        self.ts_order_max = std::cmp::max(self.ts_order_max, self.ts_order.len());
    }

    pub fn add_group_ts(&mut self, msg_id: MsgId, dest: &GidSet, gid: Gid, group_ts: Clock) {
        use std::collections::hash_map::Entry;
        match self.all.entry(msg_id) {
            Entry::Occupied(mut e) => {
                let m = e.get_mut();
                let prev_ts = std::cmp::max(m.entry_ts, m.max_group_ts);
                assert!(m.missing_group_ts.remove(gid));
                m.max_group_ts = std::cmp::max(m.max_group_ts, Some(group_ts));
                m.last_modified = Instant::now();
                if m.entry_ts.is_some() {
                    // entry should already be present in ts_order
                    debug_assert!(self.ts_order.get_priority(&msg_id).is_some());
                    if m.max_group_ts > prev_ts {
                        // update priority
                        let Reverse((ts, _)) = self
                            .ts_order
                            .change_priority(&msg_id, Reverse((m.max_group_ts.unwrap(), msg_id)))
                            .unwrap();
                        assert_eq!(Some(ts), prev_ts);
                    }
                }
            }
            Entry::Vacant(e) => {
                // first time we see the msg
                debug_assert!(self.ts_order.get_priority(&msg_id).is_none());
                // must not be from our group, would have been inserted with add_entry_ts
                assert!(gid != self.gid);

                let e = e.insert(PendingMsg {
                    dest: dest.clone(),
                    entry_ts: None,
                    max_group_ts: Some(group_ts),
                    missing_group_ts: dest.clone(),
                    last_modified: Instant::now(),
                    log_idx: None,
                });
                assert!(e.missing_group_ts.remove(gid));
            }
        }
        self.all_max = std::cmp::max(self.all_max, self.all.len());
        self.ts_order_max = std::cmp::max(self.ts_order_max, self.ts_order.len());
    }

    /// Remove information about a msg's log entry ts (happens when the log gets truncated)
    pub fn remove_entry_ts(&mut self, msg_id: MsgId) {
        let p = self.all.get_mut(&msg_id).unwrap();
        p.entry_ts = None;
        p.log_idx = None;
        // sanity check that the group timestamp is not decided
        debug_assert!(p.missing_group_ts.contains(self.gid));
        // remove from ts_order
        self.ts_order.remove(&msg_id).unwrap();
    }

    /// Returns the list of messages not yet proposed in the local group
    pub fn missing_entry_ts(&mut self) -> Vec<(MsgId, GidSet)> {
        self.all
            .iter()
            .filter_map(|(msg_id, p)| {
                if p.entry_ts.is_none() {
                    Some((*msg_id, p.dest.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Return next smallest pending ts, but only if it is the final timestamp.
    #[cfg(test)]
    pub fn peek_next_smallest(&mut self) -> Option<(Clock, MsgId)> {
        let (_, Reverse((ts, msg_id))) = self.ts_order.peek()?;
        let final_ts = self.all.get(&msg_id).unwrap().final_ts()?;
        assert_eq!(*ts, final_ts);
        return Some((*ts, *msg_id));
    }

    /// Remove and return the pending msg with the smallest ts, but only if it
    /// is the final timestamp and it's smaller than min_new_proposal.
    pub fn pop_next_smallest(&mut self, min_new_proposal: Clock) -> Option<(Clock, MsgId, u64)> {
        let (_, Reverse((ts, msg_id))) = self.ts_order.peek()?;
        if *ts >= min_new_proposal {
            return None;
        }
        let final_ts = self.all.get(&msg_id).unwrap().final_ts()?;
        assert_eq!(*ts, final_ts);
        let p = self.all.remove(&msg_id).unwrap();
        let (_, Reverse((ts, msg_id))) = self.ts_order.pop().unwrap();
        debug_assert!((ts, msg_id) > self.last_popped);
        self.last_popped = (ts, msg_id);
        return Some((ts, msg_id, p.log_idx.unwrap()));
    }
}
