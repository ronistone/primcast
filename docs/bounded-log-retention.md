# Bounded log retention — implementation plan

Status: planned, not started. Branch `multi-group-recovery-impl`.
Decision record (rationale, rejected alternatives): https://claude.ai/code/artifact/b799abb6-bfea-4bec-9031-a9da970ff1ca

## Problem

Nothing ever trims the log. `GroupReplica.log`, `msgid`, `msgid_idx` and the LMDB log db all grow
without bound, so a 3×3 local cluster exhausts a 31 GB box and collapses.

Measured 2026-09-10 (3×3 local, 3 clients × 10k msg/s, 50% globals, 3 dests, ~20k appends/s per replica):

| | |
|---|---|
| delivered rate | 10k/s flat until t≈195 s, then decays to 3.2k/s |
| backlog / p50 latency at t=250 s | 165,461 msgs / 25 s |
| server RSS, 9 procs | 1.8 GB @5 s → 11.3 GB @120 s → plateau ~19 GB @160 s (**+83 MB/s**) |
| LMDB dir | 577 MB @5 s → 10.4 GB @220 s (**+42 MB/s**) |
| MemAvailable | 16.7 GB → 1.8 GB |
| swap | saturates at 8.2 GB by t≈195 s |
| server major faults | 17.8k → 27.8k |

Per-entry cost: ~460 B resident, ~233 B on disk. The split below is derived from struct sizes, not
measured (±30%): `log` 265 B, `msgid_idx` 34 B, `msgid` 28 B, LMDB resident 138 B.

**Key discriminator:** LMDB pages are file-backed mmap — the kernel reclaims them from page cache and
they never swap. Only anonymous heap swaps. The 8.2 GB of swap is the three HashMaps, not the database.
LMDB growth caused the gradual slope; the HashMaps caused the collapse.

This was hidden because `scripts/plot_client_log.py:74` buckets by `SEND_AT`, so the throughput panel
charts *offered* load and stays flat regardless of what delivery does.

## Design

Two windows, not one. The expensive structure needs only a short window; the structure that needs a
long window is cheap.

- **`log`** (payload-bearing, 265 B/entry) → **short window**. Safe to trim aggressively because
  `get_log` (`primcast-core/src/lib.rs:338`) already falls back to `persistence.get_log_entry`, and we
  are *not* truncating LMDB. A miss costs an LMDB read, never a wrong answer. The floor is therefore a
  **performance** choice, not a correctness one.
- **`msgid` + `msgid_idx`** (id-keyed metadata, 28–34 B/entry) → **long window**. `msgid` is the dedup
  check at `:822` and cannot fall back to persistence (there is no msg_id-keyed db), so its window must
  cover the re-proposal horizon. `msgid_idx` is cheap and a longer window lets multi-group recovery
  serve older payloads, so it shares that window.

Re-proposal horizon — the only things that re-propose an existing id, all second-scale:

- `repair_missing_proposals` (`primcast-net/src/lib.rs:606`), `MISSING_PROPOSAL_AGE = 1s`
- leadership-change re-injection via `overflow_proposals` (`primcast-net/src/lib.rs:568`)
- client reconnect, `reconnect_timeout_secs: 2`

`MsgId` is a random u128, so accidental collision is not a concern (birthday bound ≈ 2^64 messages).

Trim floor: `min(safe_len, pending.min_log_idx()) − margin`.

Use `safe_len` — the **quorum** ack already computed in `update()` at `:1139` — and *not* the minimum
ack. A dead follower's ack never advances; with min-ack it would pin the log forever at ~5.3 MB/s. A
replica left behind the quorum recovers from LMDB or a peer, which is what recovery is for. In the
measured dumps `Pid(2)` is consistently the laggard, so this is not hypothetical.

Measured steady-state distances: min ack lags `log_len` by 2–8 entries, `safe_len` by 0–3, entries
pinned by pending by 0–3. The watermark sits on the tail.

### Expected result

Per replica over a 15-minute run:

| | today | after |
|---|---|---|
| `log` | 4.8 GB | 26 MB (100k window) |
| `msgid_idx` | 0.6 GB | 41 MB (1.2M window) |
| `msgid` | 0.4 GB | 34 MB (1.2M window) |
| **× 9 replicas** | **~74 GB** | **~0.9 GB** |

## Changes

Order matters: 1 makes 2 cheap, 2 makes 3 safe.

### 1. Replace the log HashMap with a deque

`primcast-core/src/lib.rs:164`

```rust
log: VecDeque<LogEntry>,
log_base: u64,          // log idx of log[0]
log_len_cached: u64,    // unchanged: one past the highest idx
```

The log is dense and contiguous, so hashing buys nothing. The decisive reason is that **hashbrown never
shrinks on remove** — evicting from a HashMap frees the payload allocations but leaves the table at its
high-water mark. `pop_front` actually frees.

Accessors to update (all in `primcast-core/src/lib.rs`):

- `get_log:338` — `if idx >= self.log_base { self.log.get((idx - self.log_base) as usize) }`, then the
  existing persistence fallback unchanged.
- `push_log:409` — `debug_assert_eq!(idx, self.log_base + self.log.len() as u64); self.log.push_back(entry);`
- `pop_log:415` — guard `if self.log_len_cached <= self.log_base { return None; }`, then decrement and
  `pop_back()`.
- `last_log:427` — `self.log.back()`.
- `update_log_ts:376` — index with `idx - self.log_base`; if `idx < self.log_base` skip the in-memory
  write (persistence still gets it). Should not happen while the floor holds, but handle it.
- `clear_log:446` — also reset `log_base`.

**Initialisation.** In `new()`, alongside the existing `result.log_len_cached = safe_len;` (`:325`) set
`result.log_base = safe_len;` and leave the deque empty. The loader at `:327` only re-inserts entries
with `idx >= safe_len`, so entries below `safe_len` already live only in LMDB today — `log_base` just
makes that explicit.

**Rollback** (`:712-723`) pops from the tail and only ever touches entries with `idx >= safe_len`, which
the floor never trims. Add a `debug_assert!(self.log_len_cached > self.log_base)` there.

### 2. Track the pending floor

`primcast-core/src/pending.rs`

`ts_order.peek()` is **not** usable as the minimum log idx: its priority is
`max(entry_ts, max_group_ts)`, so a remote group ts can push an early entry deep into the queue. Keep an
explicit set.

```rust
pending_log_idx: BTreeSet<u64>,

pub fn min_log_idx(&self) -> Option<u64> { self.pending_log_idx.first().copied() }
```

- `add_entry_ts` — insert `log_idx` in both the Occupied and Vacant branches.
- `remove_entry_ts` — remove `p.log_idx` before clearing it.
- `pop_next_smallest` — remove `p.log_idx.unwrap()` after `self.all.remove(&msg_id)`.

### 3. Trim on both windows

New in `GroupReplica`, called at the end of `update()` (`:1136`) — already `&mut self`, already runs once
per delivery round under the write lock.

```rust
fn trim_retained(&mut self) {
    // short window: payload-bearing log + its index
    let floor = std::cmp::min(self.safe_len, self.pending.min_log_idx().unwrap_or(self.safe_len));
    let new_base = floor.saturating_sub(self.config.log_retention_entries);
    while self.log_base < new_base {
        let entry = self.log.pop_front().expect("log_base below deque");
        self.msgid_idx.remove(&entry.msg_id);   // NOTE: see below
        self.log_base += 1;
    }

    // long window: dedup set
    while self.msgid_order.len() as u64 > self.config.msgid_retention_entries {
        let id = self.msgid_order.pop_front().unwrap();
        let id_low = (id & MSGID_LOW_MASK) as u16;
        if let Some(s) = self.msgid.get_mut(&id_low) { s.remove(&id); }
    }
}
```

`msgid_idx` shares the **long** window, so it must not be removed in the log loop above — move its
removal into the `msgid_order` loop instead and keep the two id-keyed structures in lockstep. (Written
as two separate loops above only to show the two windows; collapse the id-keyed removals into one.)

`msgid_order: VecDeque<MsgId>` is new: `push_back` in `append_inner_internal` (`:950`, next to the
existing `msgid` insert) and `pop_back` in the rollback loop (`:719-723`), which pops from the tail in
the same order.

The rollback path at `:719-723` already removes from `msgid`, `msgid_idx` and `pending` together — front
eviction is its mirror image, so follow that code's shape.

**Config** (`primcast-core/src/config.rs`), top-level, with defaults:

- `log_retention_entries: u64` — default `100_000` (≈ 5 s at 20k appends/s)
- `msgid_retention_entries: u64` — default `1_200_000` (≈ 60 s)

Size the msgid window in **seconds**, not entries: the entry count is only meaningful against the append
rate, and the whole point is to cover the re-proposal horizon.

Because the floor includes `pending.min_log_idx()`, the window **cannot evict a still-pending id**.

### 4. Fix the ack length on epoch promotion

`primcast-core/src/lib.rs:584`

```rust
let log_len = self.log.len() as u64;   // WRONG
let log_len = self.log_actual_len();   // fix
```

This is **already wrong today**, independent of this work: the loader at `:327` only re-inserts entries
with `idx >= safe_len`, so after any restart the map is shorter than the log and the replica
under-reports its own ack on promotion. Eviction makes it worse. Fix regardless.

### 5. Delete the in-memory-only log scan

`primcast-core/src/lib.rs:432` `find_log_for_dest` reads only the map, with a
`// TODO: If not in cache, check persistence` that was never done. Its sole caller
`next_log_entry_for_dest` (`:929`) has no callers. Delete both before eviction gives it a wrong answer.

### 6. Bucket the throughput plot by delivery time

`scripts/plot_client_log.py:74` — bucket index from `send_at + latency`, not `send_at`. As written the
panel cannot show a capacity collapse.

## Verification

1. `cargo build --release --examples`.
2. `cargo test -p primcast-core` — note five GroupReplica/config/remote_learner tests fail pre-existing
   on multi-group branches (always-Promised init + shared `db/` paths). Don't chase them; compare
   against the pre-change failure set.
3. Consistency: `./scripts/tmux-open_loop.sh --globals 0.5 -m 1000 --release --check`, kill, then
   `./scripts/check_consistency.sh`. Must be clean — this is the guard against duplicate or misordered
   delivery.
4. Memory: run the 3×3 cluster with 3 clients at 10k msg/s and sample
   `ps -eo rss,args | grep examples/server` every 5 s. **RSS must plateau**, not climb linearly. Target
   ≲ 1 GB total across 9 replicas after the first minute; today it is +83 MB/s without bound.
5. Throughput: with the fixed plot script, delivered rate must stay flat for the whole run instead of
   decaying after ~195 s.

## Watch for

**If the window is ever too short**, two outcomes. If the first copy is still pending, `add_entry_ts`
trips `assert!(m.entry_ts.is_none())` and panics — loud and safe. If the first copy was already
delivered, the vacant branch is taken and the message is **delivered twice** — silent, and a violation
of the multicast contract. Flooring at `pending.min_log_idx()` makes the second case unreachable inside
the window; the margin covers re-proposals older than the window itself.

**Both feed loops `.unwrap()` the log read** — `log_entry_for_remote(next_idx).unwrap()`
(`primcast-net/src/lib.rs:2166`) and `log_entry(follower_log_len).unwrap()` (`:1441`). They survive
in-memory trimming only because LMDB still has everything. They become panics the day LMDB truncation
lands. Both also hold a **read lock** while the read happens, so a far-behind peer turns every fetch
into a blocking LMDB read under the lock — that is the performance reason for the log margin.

## Deferred

- **LMDB prefix truncation.** 15-minute runs write ~4.6 GB per replica, under the map, and 41 GB against
  303 GB free. Truncation would not shrink the file anyway — LMDB reuses freed pages via its free list.
  **But there is a hard ceiling:** `set_map_size` is 10 GB (`persistence/lmdb_impl.rs:29`; the `// 1GB`
  comment is wrong), and exceeding it returns `MDB_MAP_FULL`, which hits `.expect` on the append path
  (`primcast-core/src/lib.rs:991`) and panics. At the measured 5.1 MB/s per replica that is **~34
  minutes**. Any run longer than that needs this first.
- **Stop double-writing every entry.** `update_log_ts:376` re-serializes the whole `LogEntry`, payload
  included, once per delivery just to stamp `final_ts`. Doubles disk write volume, and because the
  delivery index trails the append index it copy-on-writes pages already evicted from page cache — the
  mechanism behind the gradual slope. Halves LMDB growth, no recovery impact.
- **Config byte budget as a cap.** A RAM budget is a *trigger*, not a victim policy; it composes with the
  watermark rather than replacing it. The watermark needs only 30–75 MB per replica, so a 512 MB budget
  would never fire. Worth adding later purely as a backstop for a stalled watermark (a wedged remote
  learner), which is a failure mode we have hit before.
- **Collapse the msgid sharding.** `HashMap<u16, HashSet<MsgId>>` with `MSGID_LOW_MASK = 0xff` is 256
  buckets and buys nothing over a single `HashSet<MsgId>`. Memory-neutral, just simpler. Fold in while
  touching the eviction path.

## Rejected

Recorded so they are not revisited by accident.

- **LRU for `log`** — reorders on read, forcing `&mut self`. `get_log:338` and every caller are `&self`
  and several run under read locks, so this converts read paths into write paths on `RwLock<Shared>` and
  worsens delivery contention we already fixed once. The access pattern is a sliding window, so LRU
  degenerates to FIFO anyway; lowest-index eviction is FIFO without the bookkeeping.
- **LRU or size-triggered eviction for `msgid`** — both pick arbitrary victims, and an arbitrary victim
  in a dedup set is a silently accepted duplicate.
- **Bloom filter for `msgid`** — wrong error direction. A false positive means "id already used" for a
  brand-new message, and `primcast-net/src/lib.rs:570` swallows that case on the assumption the message
  is already in the log, so the message is never delivered and the client is never told. Plain Bloom also
  cannot delete, so its false-positive rate degrades monotonically over 18M inserts.
- **Disk fallback for `msgid`** — inverts the cost. The common case is *absence*, and absence is exactly
  what forces the disk read: 20k lookups/s to prove a key is not there. There is also no `msg_id`-keyed
  database. Moot regardless: a windowed exact set is ~34 MB per replica.
