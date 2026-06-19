//! Multi-group collaborative recovery (isolated module).
//!
//! A message addressed to several groups is proposed to *all* of them, so each
//! destination group stores a full copy (payload included). When a node recovers
//! it can therefore pull the heavy *payloads* of multi-destination messages from
//! the other destination groups instead of overloading its own group's peers.
//!
//! The split:
//!   * SKELETON  — small, authoritative ordering metadata (idx, epoch, ts,
//!                 final_ts, msg_id, dest). Only the recovering node's own group
//!                 can provide it. Fetched in phase A.
//!   * PAYLOAD   — heavy bytes, byte-identical in every destination group.
//!                 Offloaded to co-destination groups in phase C.
//!
//! The routing decision (which source serves which payload) lives behind the
//! [`MultiGroupRecoveryPlanner`] trait so the algorithm is easy to swap. Nothing
//! else in the crate needs to change to try a different strategy.

use std::collections::HashMap;
use std::sync::Arc;

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::sync::RwLock;

use bytes::Bytes;
use primcast_core::config::Config;
use primcast_core::types::*;
use primcast_core::{LogEntry, LogSkeletonEntry};

use crate::conn::Conn;
use crate::messages::Message;
use crate::{Error, Shared};

/// Where a payload will be fetched from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PayloadSource {
    /// A peer in the recovering node's own group.
    OwnPeer(Pid),
    /// A co-destination group (any of its peers can serve the payload).
    CoDestGroup(Gid),
}

/// The pluggable routing algorithm.
///
/// Given the authoritative skeleton range and which sources are available,
/// decide who serves each message's payload. Swap the implementation to change
/// the multi-group recovery strategy (load-aware, locality-aware, ...).
pub trait MultiGroupRecoveryPlanner: Send + Sync {
    fn plan(
        &self,
        self_gid: Gid,
        skeletons: &[LogSkeletonEntry],
        reachable_groups: &GidSet,
        own_peers: &[Pid],
    ) -> Vec<(MsgId, PayloadSource)>;
}

/// Default strategy.
///
/// Route every multi-destination payload to a reachable co-destination group,
/// round-robin across the eligible co-dest groups to spread load. Single-dest
/// messages (and multi-dest messages with no reachable co-dest group) fall back
/// to own-group peers, round-robin across them.
pub struct CoDestinationPlanner;

impl MultiGroupRecoveryPlanner for CoDestinationPlanner {
    fn plan(
        &self,
        self_gid: Gid,
        skeletons: &[LogSkeletonEntry],
        reachable_groups: &GidSet,
        own_peers: &[Pid],
    ) -> Vec<(MsgId, PayloadSource)> {
        let mut out = Vec::with_capacity(skeletons.len());
        let mut group_rr = 0usize;
        let mut peer_rr = 0usize;

        for skel in skeletons {
            // co-destination groups for this entry that we believe are reachable
            let codest: Vec<Gid> = skel
                .dest
                .iter()
                .copied()
                .filter(|g| *g != self_gid && reachable_groups.contains(*g))
                .collect();

            if !codest.is_empty() {
                let g = codest[group_rr % codest.len()];
                group_rr += 1;
                out.push((skel.msg_id, PayloadSource::CoDestGroup(g)));
            } else if !own_peers.is_empty() {
                let p = own_peers[peer_rr % own_peers.len()];
                peer_rr += 1;
                out.push((skel.msg_id, PayloadSource::OwnPeer(p)));
            }
            // else: unroutable (no own peers and no co-dest) — orchestrator errors
        }
        out
    }
}

/// Run multi-group collaborative recovery for the log range `[from_idx, to_idx)`.
///
/// Drop-in sibling of `recovery::run_follower_collaborative_recovery`: same apply
/// semantics, but payloads of multi-destination messages are pulled from co-dest
/// groups according to `planner`.
pub async fn run_multi_group_recovery(
    from_idx: u64,
    to_idx: u64,
    epoch: Epoch,
    cfg: &Config,
    self_gid: Gid,
    self_pid: Pid,
    planner: &dyn MultiGroupRecoveryPlanner,
    s: &Arc<RwLock<Shared>>,
) -> Result<(), Error> {
    if to_idx <= from_idx {
        return Ok(());
    }

    let group = cfg.group(self_gid).ok_or_else(|| {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "group not found",
        ))
    })?;
    let own_peers: Vec<Pid> = group
        .peers
        .iter()
        .filter(|p| p.pid != self_pid)
        .map(|p| p.pid)
        .collect();

    if own_peers.is_empty() {
        return Ok(());
    }

    eprintln!(
        "[MGRecovery] recovering [{}, {}) — own peers {:?}",
        from_idx, to_idx, own_peers
    );

    // --- Phase A: authoritative skeleton from own group ---
    let skeletons =
        fetch_skeleton(from_idx, to_idx, self_gid, self_pid, &own_peers, cfg, s).await?;
    if skeletons.len() as u64 != to_idx - from_idx {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "incomplete skeleton",
        )));
    }

    // --- Phase B: plan payload sources ---
    let mut reachable_groups = GidSet::new();
    for skel in &skeletons {
        for g in skel.dest.iter().copied() {
            if g != self_gid && cfg.group(g).is_some() {
                reachable_groups.insert(g);
            }
        }
    }
    let assignments = planner.plan(self_gid, &skeletons, &reachable_groups, &own_peers);
    let assignment_map: HashMap<MsgId, PayloadSource> = assignments.into_iter().collect();

    // group the msg_ids by source endpoint
    let mut by_group: HashMap<Gid, Vec<MsgId>> = HashMap::new();
    let mut by_peer: HashMap<Pid, Vec<MsgId>> = HashMap::new();
    for skel in &skeletons {
        match assignment_map.get(&skel.msg_id) {
            Some(PayloadSource::CoDestGroup(g)) => by_group.entry(*g).or_default().push(skel.msg_id),
            Some(PayloadSource::OwnPeer(p)) => by_peer.entry(*p).or_default().push(skel.msg_id),
            None => {
                // planner declined to route it — fall back to first own peer
                by_peer.entry(own_peers[0]).or_default().push(skel.msg_id);
            }
        }
    }

    // --- Phase C: fetch payloads in parallel ---
    let mut payloads: HashMap<MsgId, Bytes> = HashMap::new();
    let mut fallback_ids: Vec<MsgId> = Vec::new();

    let mut fetches: FuturesUnordered<
        std::pin::Pin<Box<dyn std::future::Future<Output = PayloadResult> + Send>>,
    > = FuturesUnordered::new();
    for (g, ids) in by_group {
        let cfg = cfg.clone();
        fetches.push(Box::pin(fetch_payloads_from_group(
            g, ids, self_gid, self_pid, cfg,
        )));
    }
    for (p, ids) in by_peer {
        let cfg = cfg.clone();
        fetches.push(Box::pin(fetch_payloads_from_peer(
            self_gid, p, ids, self_gid, self_pid, cfg,
        )));
    }

    while let Some(res) = fetches.next().await {
        match res {
            Ok((found, missing)) => {
                for (id, bytes) in found {
                    payloads.insert(id, bytes);
                }
                fallback_ids.extend(missing);
            }
            Err((ids, e)) => {
                // whole endpoint failed — retry those ids against an own peer
                eprintln!("[MGRecovery] source failed ({:?}), falling back", e);
                fallback_ids.extend(ids);
            }
        }
    }

    // --- Phase C fallback: any missing/failed ids come from own peers ---
    fallback_ids.retain(|id| !payloads.contains_key(id));
    if !fallback_ids.is_empty() {
        eprintln!(
            "[MGRecovery] {} payloads falling back to own group",
            fallback_ids.len()
        );
        let (found, still_missing) = fetch_payloads_with_own_peers(
            fallback_ids.clone(),
            &own_peers,
            self_gid,
            self_pid,
            cfg,
        )
        .await;
        for (id, bytes) in found {
            payloads.insert(id, bytes);
        }
        if !still_missing.is_empty() {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("{} payloads unrecoverable", still_missing.len()),
            )));
        }
    }

    // --- Phase D: reassemble full entries and apply ---
    let mut all_entries: Vec<(u64, Epoch, LogEntry)> = Vec::with_capacity(skeletons.len());
    for skel in skeletons {
        let msg = payloads.get(&skel.msg_id).ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "missing payload after fetch",
            ))
        })?;
        let entry = LogEntry {
            local_ts: skel.local_ts,
            msg_id: skel.msg_id,
            msg: msg.clone(),
            dest: skel.dest.clone(),
            final_ts: skel.final_ts,
        };
        all_entries.push((skel.idx, skel.entry_epoch, entry));
    }

    all_entries.sort_by_key(|(idx, _, _)| *idx);
    for (i, (idx, _, _)) in all_entries.iter().enumerate() {
        let expected = from_idx + i as u64;
        if *idx != expected {
            return Err(Error::Core(primcast_core::Error::InvalidIndex { len: expected }));
        }
    }

    eprintln!("[MGRecovery] applying {} entries", all_entries.len());
    {
        let mut shared = s.write().await;
        for (idx, entry_epoch, entry) in all_entries {
            let (actual_epoch, _) = shared.core.log_status();
            if entry_epoch > actual_epoch {
                shared.core.start_epoch_append(epoch, idx, entry_epoch, entry)?;
            } else {
                shared.core.append(entry_epoch, idx, entry)?;
            }
        }
        shared.update_tx.send(()).ok();
    }

    eprintln!("[MGRecovery] done");
    Ok(())
}

/// Phase A — fetch the contiguous skeleton `[from, to)` from own-group peers.
/// Tries peers in order; the data is small, so a single peer suffices.
async fn fetch_skeleton(
    from_idx: u64,
    to_idx: u64,
    self_gid: Gid,
    self_pid: Pid,
    own_peers: &[Pid],
    cfg: &Config,
    _s: &Arc<RwLock<Shared>>,
) -> Result<Vec<LogSkeletonEntry>, Error> {
    let mut last_err: Option<Error> = None;
    for &pid in own_peers {
        let addr = match cfg.peer(self_gid, pid) {
            Some(p) => p.addr(),
            None => continue,
        };
        let req = Message::RecoverySkeletonRequest { from_idx, to_idx };
        let conn = match Conn::request((self_gid, self_pid), (self_gid, pid), addr, req).await {
            Ok(c) => c,
            Err(e) => {
                last_err = Some(e.into());
                continue;
            }
        };
        match collect_skeleton(conn).await {
            Ok(entries) => return Ok(entries),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::NotConnected,
            "no own peer served the skeleton",
        ))
    }))
}

async fn collect_skeleton(mut conn: Conn) -> Result<Vec<LogSkeletonEntry>, Error> {
    let mut entries = Vec::new();
    loop {
        match conn.recv().await? {
            Message::RecoverySkeletonChunk {
                entries: chunk,
                is_last,
            } => {
                entries.extend(chunk);
                if is_last {
                    break;
                }
            }
            m => {
                return Err(Error::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("unexpected message during skeleton fetch: {:?}", m),
                )))
            }
        }
    }
    entries.sort_by_key(|e| e.idx);
    Ok(entries)
}

type PayloadResult = Result<(Vec<(MsgId, Bytes)>, Vec<MsgId>), (Vec<MsgId>, Error)>;

/// Fetch payloads from any reachable peer of a co-destination group.
async fn fetch_payloads_from_group(
    target_gid: Gid,
    ids: Vec<MsgId>,
    self_gid: Gid,
    self_pid: Pid,
    cfg: Config,
) -> PayloadResult {
    let peers: Vec<Pid> = match cfg.group(target_gid) {
        Some(g) => g.peers.iter().map(|p| p.pid).collect(),
        None => return Err((ids, Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "co-dest group not found",
        )))),
    };
    let mut last_err: Option<Error> = None;
    for pid in peers {
        match request_payloads(target_gid, pid, &ids, self_gid, self_pid, &cfg).await {
            Ok(r) => return Ok(r),
            Err(e) => last_err = Some(e),
        }
    }
    Err((ids, last_err.unwrap_or_else(|| {
        Error::Io(std::io::Error::new(std::io::ErrorKind::NotConnected, "co-dest group unreachable"))
    })))
}

/// Fetch payloads from a specific own-group peer.
async fn fetch_payloads_from_peer(
    target_gid: Gid,
    target_pid: Pid,
    ids: Vec<MsgId>,
    self_gid: Gid,
    self_pid: Pid,
    cfg: Config,
) -> PayloadResult {
    match request_payloads(target_gid, target_pid, &ids, self_gid, self_pid, &cfg).await {
        Ok(r) => Ok(r),
        Err(e) => Err((ids, e)),
    }
}

/// Sequentially try own peers to resolve a set of msg_ids. Returns (found, missing).
async fn fetch_payloads_with_own_peers(
    mut ids: Vec<MsgId>,
    own_peers: &[Pid],
    self_gid: Gid,
    self_pid: Pid,
    cfg: &Config,
) -> (Vec<(MsgId, Bytes)>, Vec<MsgId>) {
    let mut found = Vec::new();
    for &pid in own_peers {
        if ids.is_empty() {
            break;
        }
        match request_payloads(self_gid, pid, &ids, self_gid, self_pid, cfg).await {
            Ok((got, missing)) => {
                for (id, bytes) in got {
                    found.push((id, bytes));
                }
                ids = missing;
            }
            Err(_) => continue,
        }
    }
    (found, ids)
}

/// One request/response round to fetch payloads by msg_id from (gid, pid).
async fn request_payloads(
    target_gid: Gid,
    target_pid: Pid,
    ids: &[MsgId],
    self_gid: Gid,
    self_pid: Pid,
    cfg: &Config,
) -> Result<(Vec<(MsgId, Bytes)>, Vec<MsgId>), Error> {
    let addr = cfg
        .peer(target_gid, target_pid)
        .ok_or_else(|| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "target peer not found",
            ))
        })?
        .addr();
    let req = Message::CrossGroupPayloadRequest {
        gid: self_gid,
        msg_ids: ids.to_vec(),
    };
    let mut conn = Conn::request((self_gid, self_pid), (target_gid, target_pid), addr, req).await?;
    match conn.recv().await? {
        Message::CrossGroupPayloadResponse { payloads, missing } => Ok((payloads, missing)),
        m => Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("unexpected payload response: {:?}", m),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn skel(idx: u64, msg_id: MsgId, dests: &[u8]) -> LogSkeletonEntry {
        let mut dest = GidSet::new();
        for d in dests {
            dest.insert(Gid(*d));
        }
        LogSkeletonEntry {
            idx,
            entry_epoch: Epoch::initial(),
            local_ts: idx as Clock,
            final_ts: None,
            msg_id,
            dest,
        }
    }

    fn gidset(gids: &[u8]) -> GidSet {
        let mut s = GidSet::new();
        for g in gids {
            s.insert(Gid(*g));
        }
        s
    }

    #[test]
    fn single_dest_routes_to_own_peers() {
        let p = CoDestinationPlanner;
        let skels = vec![skel(0, 10, &[0]), skel(1, 11, &[0])];
        let plan = p.plan(Gid(0), &skels, &gidset(&[1, 2]), &[Pid(1), Pid(2)]);
        // both single-dest -> own peers, round-robin
        assert_eq!(plan[0], (10, PayloadSource::OwnPeer(Pid(1))));
        assert_eq!(plan[1], (11, PayloadSource::OwnPeer(Pid(2))));
    }

    #[test]
    fn multi_dest_offloads_to_codest_groups() {
        let p = CoDestinationPlanner;
        let skels = vec![
            skel(0, 10, &[0, 1]),
            skel(1, 11, &[0, 2]),
            skel(2, 12, &[0, 1, 2]),
        ];
        let plan = p.plan(Gid(0), &skels, &gidset(&[1, 2]), &[Pid(1)]);
        assert_eq!(plan[0], (10, PayloadSource::CoDestGroup(Gid(1))));
        assert_eq!(plan[1], (11, PayloadSource::CoDestGroup(Gid(2))));
        // entry 2 has co-dests {1,2}; round-robin picks index group_rr=2 -> 2 % 2 = 0 -> Gid(1)
        assert_eq!(plan[2], (12, PayloadSource::CoDestGroup(Gid(1))));
    }

    #[test]
    fn unreachable_codest_falls_back_to_own_peer() {
        let p = CoDestinationPlanner;
        // dest {0,1} but group 1 not reachable -> own peer
        let skels = vec![skel(0, 10, &[0, 1])];
        let plan = p.plan(Gid(0), &skels, &gidset(&[2]), &[Pid(1)]);
        assert_eq!(plan[0], (10, PayloadSource::OwnPeer(Pid(1))));
    }

    #[test]
    fn no_route_when_no_sources() {
        let p = CoDestinationPlanner;
        let skels = vec![skel(0, 10, &[0])];
        let plan = p.plan(Gid(0), &skels, &gidset(&[]), &[]);
        assert!(plan.is_empty());
    }
}
