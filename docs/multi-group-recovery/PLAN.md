# Multi-Group Collaborative Recovery — Implementation Plan

## 1. Goal

Today, when a node in group `A` rejoins and is missing a chunk of its log, it
fetches **every** missing entry from its own group peers (`recovery.rs`,
`run_recovery` / `run_follower_collaborative_recovery`). The load — especially
the **message payload bytes** — falls entirely on group `A`'s surviving
replicas, which are also the ones still serving live traffic.

**Idea:** a message addressed to a `GidSet` of several groups is proposed to
**all** of those groups (see `PrimcastHandle::propose`, `lib.rs:125-126`:
`for gid in dest.iter() { ... }`). Each destination group therefore stores a
**full copy** (payload included) of every multi-destination message in its own
log. So a recovering `A` node can pull the heavy payloads of multi-destination
messages from the **other destination groups** instead of from its own peers,
spreading the recovery cost across the whole cluster and relieving group `A`.

## 2. What each group actually holds

| Data | Group A log | Co-dest group B log | B's `RemoteLearner` for A |
|------|:-----------:|:-------------------:|:-------------------------:|
| `msg_id` | yes | yes | yes |
| payload `msg: Bytes` | yes | **yes** | no |
| `dest: GidSet` | yes | yes | yes |
| `final_ts` (global order) | yes | yes (same value) | tracks |
| A's local `idx` | yes | **no** (B has its own idx) | no |
| A's `entry_epoch` / `local_ts` | yes | **no** | no |

**Key consequence.** Group `B` can supply the **payload** (which is byte-identical
across all destination groups — it is the same proposal) and `msg_id`, but it
**cannot** supply group `A`'s authoritative ordering metadata (`idx`,
`entry_epoch`, `local_ts`). That metadata is small and must always come from
`A`'s own peers.

This is the core design split:

- **Skeleton** (small, authoritative) — always from own-group peers.
- **Payload** (large, identical everywhere) — offloadable to co-dest groups.

## 3. Algorithm (4 phases)

```
Recovering A-node needs entries [from_idx, to_idx)

Phase A  SKELETON FETCH  (own group, cheap)
   ask own peers for skeletons: {idx, entry_epoch, local_ts, final_ts, msg_id, dest}
   -> authoritative, contiguous, no payload bytes

Phase B  PLAN  (pluggable strategy)
   for each skeleton entry:
     if dest is multi-group AND a co-dest group is reachable
        -> assign payload fetch to a co-dest group   (offload)
     else
        -> assign payload fetch to an own-group peer (local, single-dest msgs)
   strategy decides *which* co-dest group and balances load

Phase C  PAYLOAD FETCH  (parallel, load-distributed)
   cross-group: CrossGroupPayloadRequest { gid: A, msg_ids: [...] }
                -> CrossGroupPayloadResponse { payloads, missing }
   own-group:   reuse existing range/payload fetch
   any `missing` from a co-dest group  -> fall back to own-group peer

Phase D  REASSEMBLE + APPLY
   full LogEntry = skeleton (idx, epoch, ts, dest, final_ts) + payload bytes
   verify payload.msg_id == skeleton.msg_id
   sort by idx, check contiguity, recovery_append_batch(), finalize
```

## 4. Isolation & pluggability (hard requirement)

All new logic lives in **one new module** so the multi-group algorithm is easy to
swap or disable.

### New module: `primcast-net/src/multi_group_recovery.rs`

```rust
/// Authoritative per-entry metadata, fetched from own-group peers. No payload.
pub struct EntrySkeleton {
    pub idx: u64,
    pub entry_epoch: Epoch,
    pub local_ts: Clock,
    pub final_ts: Option<Clock>,
    pub msg_id: MsgId,
    pub dest: GidSet,
}

/// Where a payload will be fetched from.
pub enum PayloadSource {
    OwnPeer(Pid),
    CoDestGroup(Gid),
}

/// THE pluggable algorithm. Swap this to change cross-group routing.
pub trait MultiGroupRecoveryPlanner: Send + Sync {
    /// Given the skeleton range and which groups/peers are reachable,
    /// decide who serves each payload.
    fn plan(
        &self,
        self_gid: Gid,
        skeletons: &[EntrySkeleton],
        reachable_groups: &GidSet,
        own_peers: &[Pid],
    ) -> Vec<(MsgId, PayloadSource)>;
}

/// Default: route every multi-dest payload to a reachable co-dest group,
/// round-robin across co-dest groups for balance; single-dest -> own peer.
pub struct CoDestinationPlanner;

/// Orchestrator. The ONLY entry point the rest of the net crate calls.
pub async fn run_multi_group_recovery(
    from_idx: u64,
    to_idx: u64,
    epoch: Epoch,
    planner: &dyn MultiGroupRecoveryPlanner,
    s: &Arc<RwLock<Shared>>,
) -> Result<(), Error> { /* phases A-D */ }
```

### Why this is isolated

- The **routing policy** is behind `MultiGroupRecoveryPlanner`. Want
  load-aware, locality-aware, or bandwidth-aware routing? Add a new struct, no
  other code changes.
- The **orchestrator** `run_multi_group_recovery` is a drop-in sibling of the
  existing `run_follower_collaborative_recovery`. Existing single-group recovery
  stays untouched and remains the fallback.
- A single **config flag** (`multi_group_recovery: bool`, default `false`)
  selects between the existing path and the new module at the one call site in
  `lib.rs`. Off = today's behavior, byte-for-byte.

### Touch list (minimal, additive)

| File | Change |
|------|--------|
| `primcast-net/src/multi_group_recovery.rs` | **new** — all logic + trait |
| `primcast-net/src/messages.rs` | **add** 4 message variants (skeleton req/chunk, cross-group payload req/resp) |
| `primcast-net/src/lib.rs` | dispatch 2 new server handlers in `handle_connection`; 1 guarded call-site swap |
| `primcast-core/src/config.rs` | **add** `multi_group_recovery: bool` flag |
| `primcast-core/src/lib.rs` | **add** read-only helpers: `log_skeleton_range()`, `payloads_by_msg_id()` (msg_id→payload lookup) |

No existing function signatures change. No protocol change when the flag is off.

## 5. New wire messages (`messages.rs`)

```rust
// own-group skeleton (metadata only, no payload)
RecoverySkeletonRequest { from_idx: u64, to_idx: u64 },
RecoverySkeletonChunk   { entries: Vec<EntrySkeleton>, is_last: bool },

// cross-group payload by msg_id
CrossGroupPayloadRequest  { gid: Gid, msg_ids: Vec<MsgId> },
CrossGroupPayloadResponse { payloads: Vec<(MsgId, Bytes)>, missing: Vec<MsgId> },
```

Server side slots into the existing `handle_connection` match
(`lib.rs:857`) — same pattern as `RecoveryRequest` / `LogRangeRequest`. The
cross-group payload handler reads `gid` from the request (the requesting group)
and looks up payloads in the **local** log by `msg_id`.

## 6. Correctness notes

1. **Ordering is always authoritative-local.** Co-dest groups never supply A's
   `idx`/`epoch`/`local_ts`; only opaque payload bytes keyed by `msg_id`. The
   global delivery order is unaffected.
2. **Payload identity.** The payload of a given `msg_id` is byte-identical in
   every destination group (same `Proposal`). Reassembly asserts
   `payload.msg_id == skeleton.msg_id`; optional length/hash check.
3. **Only safe multi-dest entries are offloadable.** An entry is present in a
   co-dest group only once it was proposed/safe there. The skeleton's `dest`
   and `final_ts` (from A's own quorum) tell the planner which entries are
   eligible; in-flight or single-dest entries stay on own-group peers.
4. **Completeness fallback.** `CrossGroupPayloadResponse.missing` lists msg_ids
   a co-dest group could not serve; those are re-requested from own-group peers.
   Recovery never fails just because a co-dest group lagged.
5. **msg_id → payload lookup.** Co-dest server needs to map `msg_id` to its
   local log payload. `pending.rs` already keys undelivered msgs by `msg_id`;
   for delivered entries add a `msg_id -> idx` index (or scan a bounded window).
   Captured as an implementation task.

## 7. Expected benefit

- Group `A` peers serve only the **skeleton** (tens of bytes/entry) for
  multi-dest messages instead of full payloads (`MSG_SIZE`, configurable up to
  KB). For a global-heavy workload (`--globals`, `--global-dests N`) most
  payload bytes move off group `A`.
- Payload fetch parallelizes across `N` co-dest groups instead of serializing on
  group `A`'s quorum → lower recovery wall-clock and less interference with live
  traffic.

## 8. Validation

- Unit: `CoDestinationPlanner::plan` routing + fallback (table-driven).
- Integration: existing `scripts/check_consistency.sh` after a kill/recover
  cycle with `--globals 0.5 --check`; recovered log must match peers and
  cross-group delivery order must stay consistent.
- A/B: recovery wall-clock + bytes-served-by-group-A, flag off vs on, under
  `tmux-open_loop.sh --globals 0.7`.

## 9. Rollout

1. Land module + messages + flag (default off) — zero behavior change.
2. Enable in local 3×3 cluster, run consistency check.
3. Benchmark; tune planner.
4. Flip default once stable.
