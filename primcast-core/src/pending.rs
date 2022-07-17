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

/// Tracks the timestamp of messages not yet delivered
pub struct PendingSet {
    /// Replica's group
    gid: Gid,
    /// All pending msgs
    all: HashMap<MsgId, PendingMsg>,
    /// Messages with a local timestamp in the local group, sorted by "possible" final timestamp
    // TODO: kind of a waste of space to keep MsgId twice there... but should be fine
    ts_order: PriorityQueue<MsgId, Reverse<(Clock, MsgId)>, BuildHasherDefault<FxHasher>>,
    /// sanity check: items should be popped in final_ts order
    last_popped: (Clock, MsgId),
}

impl PendingSet {
    pub fn new(gid: Gid) -> Self {
        Self {
            gid,
            all: Default::default(),
            ts_order: Default::default(),
            last_popped: (0, 0),
        }
    }

    pub fn add_entry_ts(&mut self, msg_id: MsgId, dest: &GidSet, entry_ts: Clock) {
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
                });
                ts = entry_ts;
            }
        }
        self.ts_order.push(msg_id, Reverse((ts, msg_id)));
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
                });
                assert!(e.missing_group_ts.remove(gid));
            }
        }
    }

    /// Remove information about a msg's log entry ts (happens when the log gets truncated)
    pub fn remove_entry_ts(&mut self, msg_id: MsgId) {
        let p = self.all.get_mut(&msg_id).unwrap();
        p.entry_ts = None;
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
    pub fn pop_next_smallest(&mut self, min_new_proposal: Clock) -> Option<(Clock, MsgId)> {
        let (_, Reverse((ts, msg_id))) = self.ts_order.peek()?;
        if *ts >= min_new_proposal {
            return None;
        }
        let final_ts = self.all.get(&msg_id).unwrap().final_ts()?;
        assert_eq!(*ts, final_ts);
        self.all.remove(&msg_id);
        let (_, Reverse((ts, msg_id))) = self.ts_order.pop().unwrap();
        debug_assert!((ts, msg_id) > self.last_popped);
        self.last_popped = (ts, msg_id);
        return Some((ts, msg_id));
    }
}
