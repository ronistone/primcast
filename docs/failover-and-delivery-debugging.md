# Failover and delivery: problems found and fixed

Everything here comes from one debugging campaign on branch `multi-group-recovery-impl`
(commit `10445d8`). It documents seven distinct bugs — six that stopped delivery after a
leader failover, one that starved delivery even without a failover — plus the
instrumentation added along the way and how to read it next time.

## Test setup used throughout

Three groups of three replicas, all on one host, plus one open-loop client per group:

```bash
# per replica (gid 0..2, pid 0..2)
./target/release/examples/server --gid G --pid P --cfg example_3_groups.yaml --threads 4 --debug 1

# per group, one client
./target/release/examples/client_open_loop --gid G --pid 0 --cfg example_3_groups.yaml \
    --global-dests 3 --globals 0.5 --size 64 -m 5000
```

A failover is `kill -9` on the group's primary. `example_3_groups.yaml` has
`log_enabled: true`, `persistence_backend: "lmdb"`, `multi_group_recovery: true`, quorum 2.

Two practical notes:

- With `log_enabled: true` each replica writes on the order of 100 MB/min. Write those logs
  to disk, never to a tmpfs such as `/tmp` — nine replicas will exhaust RAM.
- Deliveries are gated head-of-line by `PendingSet::pop_next_smallest`: the smallest-ts
  pending message blocks every message behind it until *all* its destination groups have
  given it a timestamp. Almost every symptom below is that gate reacting to something else
  being broken.

---

## 1. New leader froze on the shared lock (self-deadlock)

**Symptom.** After the primary was killed, the replica that should take over stopped
completely: no state transition, `[STALL?] remote_log_send(...)` repeating, all tokio worker
threads parked, the process alive but doing nothing.

**Diagnosis.** The lock tracker (see *Instrumentation*) printed:

```
[LOCKPROBE] write HELD, read HELD/BLOCKED
  writer=none readers=0
  acquires=23232 releases=23232          <- balanced: no leaked guard
  waiting=15
    waiter lib.rs:597 kind=write task=Some(Id(11)) waiting_for=42.19s polls=1 wakes=1
    waiter lib.rs:497 kind=write task=Some(Id(11)) waiting_for=42.19s polls=4 wakes=1
    ... 13 more, wakes=0
```

The same task id appears twice, and the head waiter was woken but never polled again.

**Root cause.** `tokio::sync::RwLock` is FIFO-fair: when the lock frees, the permits are
handed to the queued head waiter and are only released again once that waiter's future is
polled. `PrimcastReplica::run`'s main `select!` had two pending acquisitions of the same lock
in one task — `fut` (`run_candidate` / `run_primary` / `run_follower`, all of which take the
lock internally) and a `.write().await` inside the *body* of the proposals branch. Awaiting
inside a branch body suspends the whole select, so `fut` stops being polled; when the permits
went to `fut`'s pending acquisition, nothing could ever proceed.

**Fix.** The main loop now buffers work (`pending_proposals`, `pending_follow`, `fut_done`)
and touches the lock in exactly one place: a select branch whose *future is the acquisition*,

```rust
mut s = self.shared.write(crate::loc!()), if want_lock => { /* no .await in this body */ }
```

so `fut`'s acquisition and the loop's acquisition are always polled together and whichever
gets the permits makes progress.

**Rule this establishes.** Never `.await` a lock inside a `select!` branch body when another
branch's future can hold a pending acquisition of the same lock. The other selects in the
codebase (`run_follower`, `sync_follower`, `sync_with`) were checked and are safe: their
non-winning branches never touch the lock.

---

## 2. A primary dropped its followers' acks

**Symptom.** The new primary appended fine but never delivered. Debug dump:

```
log_len: 730968 safe_len: 46193
acks: [(0, Pid(0)), (0, Pid(2)), (730968, Pid(1))] epoch: Epoch(2, Pid(1))
pending: 812314
```

Both followers stuck at 0 acked entries while the log grew.

**Root cause.** A primary learns follower progress *only* over the `sync_follower`
connection — `acks_fetch` runs inside `run_follower`, so a primary has no other ack source.
Two branches consumed the ack for local bookkeeping and returned before reaching
`core.add_ack`:

- the collaborative-recovery wait loop (`follower_log_len = ack_ll; break;`)
- the `'wait` select's `log_len > follower_log_len` case

With both followers at 0, the quorum never advanced, `safe_len` froze, and nothing was ever
delivered.

**Fix.** `apply_follower_ack()` in `primcast-net/src/lib.rs` — every ack from a follower goes
through it (`core.add_ack` + clock bump + `update_tx`). Additionally, `sync_follower` drains
already-arrived acks non-blockingly after each send batch, because the "more entries
available → `continue`" path could otherwise never reach the select that reads acks while the
log kept growing.

---

## 3. A primary shipping a backlog never acked its own log length

**Symptom.** After failover the *follower* stopped delivering: `safe_len` frozen while its
own log grew, and the other groups' remote learners froze behind it.

**Root cause.** The primary's clock-bump `Ack` was sent only when
`follower_log_len == log_len`. A primary streaming a post-failover backlog is never caught
up, so the follower's entry for the primary never advanced, and its `safe_len`
(median of the acked lengths) stayed put.

**Fix.** `sync_follower` now also acks while behind, sending `Ack { log_len, clock:
last_clock_sent }` where the clock is the ts of the last entry actually fed to that follower —
never the primary's current clock. That preserves the ordering invariant (a follower must not
learn a leader clock ahead of the entries it holds) while unfreezing the quorum.

---

## 4. Messages in flight when the leader died were never re-proposed

**Symptom.** One group's log line repeating forever for the same message, `last_popped` stuck
far behind the head, and on the other groups a `blocked_head` that never changed.

**Diagnosis.** The new debug line showed the group that owed the timestamp:

```
blocked_head: Some((85874, 53152863..., GidSet([Gid(0)]), true)) missing_local_ts: 0
```

and on G0 itself `missing_local_ts: 15` — fifteen messages known only through a remote
timestamp, never proposed locally.

**Root cause.** Global messages that reached the dying leader but never made it into the
group's log are known to the other destination groups (they timestamped them) but not to this
one. Every group then blocks head-of-line forever. `GroupReplica::missing_local_ts()` existed
in core for exactly this case but nothing ever called it.

**Fix.** `repair_missing_proposals` (`primcast-net/src/lib.rs`), a task that on the primary,
every `MISSING_PROPOSAL_INTERVAL`:

1. lists messages missing a local ts for at least `MISSING_PROPOSAL_AGE`
   (`core.missing_local_ts_older_than`, so in-flight messages are not touched),
2. fetches their payloads by `msg_id` from a co-destination group
   (`CrossGroupPayloadRequest`, reused from multi-group recovery — the payload is not kept in
   the pending set),
3. re-proposes them through the normal `proposal_tx` path.

Duplicates are harmless: `add_proposal` rejects an id already in the log or in the proposal
buffer with `IdAlreadyUsed`, which the main loop swallows.

---

## 5. Collaborative recovery livelock: followers stuck in `IDLE`

**Symptom.** Steady stream of `remote ts from Gid(2)` and never `Gid(1)`; the blocked group's
delivery pointer frozen while its log grew into the millions.

**Diagnosis.** The learner dump separates "not receiving" from "receiving but not safe":

```
Gid(1) - safe_idx:Some(655602) next_entry:911010 buffered:191625 acks:[(P0,655603),(P1,483270),(P2,911010)]
Gid(2) - safe_idx:Some(904213) next_entry:904213 buffered:0      acks:[(P0,904214),(P1,645055),(P2,904214)]
```

Entries from G1 were arriving (`buffered` growing), but `safe_idx` — the median of the remote
group's acked log lengths — was pinned, because two of G1's three replicas were frozen. Those
two turned out to be sitting in `== IDLE ==`.

**Root cause, in order.** The primary told a follower to run collaborative recovery →
`fetch_skeleton` asked own peers *in config order*, hitting the other (equally lagging)
follower first and accepting its short answer → `"incomplete skeleton"` → the `?` in
`run_follower`'s recovery branch ended the whole follower session → the main loop discarded
that error silently, so nothing was logged → the dropped connection made the primary
reconnect, see the same gap, and ask for the same range again. The follower never appended
again, its acks froze, and every other group's remote learner froze behind it.

**Fix.**

- `fetch_skeleton` asks the epoch owner (the primary that ordered the recovery) first and
  rejects a short skeleton, moving on to the next peer instead of failing the round.
- A failed recovery round no longer tears down the follower session: it is logged and the
  follower stays a follower, letting the primary re-drive.
- The primary gives up on recovery for that session (`use_collab_recovery = false`) and
  streams entries normally if a recovery round does not advance the follower, so progress
  never depends on recovery working.
- The main loop logs errors returned by the replica future instead of silently going idle.

---

## 6. A peer could panic a node by asking past the end of the log

**Symptom.** A replica stopped logging, kept ~70% CPU, and stayed alive but useless. Its log
contained:

```
thread 'tokio-runtime-worker' panicked at primcast-core/src/lib.rs:820:
out of range log idx: InvalidIndex { len: 1166088 }
...
panicked at primcast-net/src/lib.rs: assertion failed: !err.is_panic()
panicked at primcast-net/examples/server.rs: not yet implemented
```

**Root cause.** `GroupReplica::log_entry` returns `Option`, but its body did
`self.get_log(idx).expect("out of range log idx")`. Callers written against the `Option`
(`recovery_send_range`, `log_skeleton_range`, the follower-sync paths) could never see `None`
— they died instead, and the panic then cascaded through `resume_unwind` into a `todo!()`.

**Fix.** `log_entry` returns `None` for an index this replica does not hold, matching its
signature. Call sites that index a follower-reported length also clamp to the local log
length first.

---

## 7. Delivery lag with no failover at all: lock + LMDB contention

**Symptom.** In steady state, messages enqueued but the delivered pointer trailed the append
timestamp by ~1M, with `pending` in the hundreds of thousands.

**Diagnosis.** The `[LOCKTIME]` report (cumulative hold time per call site, every 10 s):

```
[LOCKTIME] lock time total=25.576s          <- of ~50s wall: the lock is held ~50% of the time
    deliver_task        14.28s over 108301 acquires (avg 132µs)
    append/propose      10.60s over  25728 acquires (avg 412µs)
    ...everything else < 0.25s
```

**Root cause.** The single `RwLock<Shared>` serializes the whole replica, and the two hot
paths were doing one LMDB transaction *per entry* while holding it:

- `append_inner` → `put_log_entry`, one rw txn per appended entry;
- `update_log_ts`, once per *delivered* message: an LMDB read txn (to fetch an entry that was
  already in the in-memory log) plus a write txn;
- `GroupReplica::update` → `put_metadata`, one rw txn on every delivery round.

**Fix.**

| Change | Where |
| --- | --- |
| Write-behind buffer, `WRITE_BUF_LIMIT` entries per txn; reads consult the buffer, truncate/flush/close/clear commit it first | `persistence/lmdb_impl.rs` |
| `update_log_ts` reads the entry from the in-memory log instead of the store | `primcast-core/src/lib.rs` |
| Metadata persisted on epoch change or at most every `METADATA_FLUSH_INTERVAL` | `GroupReplica::update` |
| `deliver_task` caps its drain at `BATCH_SIZE_YIELD` per acquisition and re-loops instead of holding the lock for a whole backlog | `primcast-net/src/lib.rs` |
| Hot per-message traces sampled 1-in-`LOG_SAMPLE` | `sampled_print!` |

The env already runs with `EnvironmentFlags::NO_SYNC`, so buffering writes does not weaken
durability beyond what the configuration already accepts.

**Result** at 5000 msg/s × 3 clients on a 16-core host:

| | before | after |
| --- | --- | --- |
| lock held (per 50 s) | 24.7 s | 8.9 s |
| `deliver_task` hold per acquisition | 132 µs | 20 µs |
| `pending`, steady state | ~360 000, growing | ~50 |
| delivered pointer vs log head | ~1.1 M ts behind | ~50 ts behind |

---

## Instrumentation reference

### `util::RwLock` — who holds the replica lock

A tracking wrapper around `tokio::sync::RwLock`. Every `read`/`write` passes its call site via
the `loc!()` macro (`file:line`), and the tracker records holders, waiters, tokio task ids,
poll and wake counts, and cumulative hold time.

`[LOCKPROBE]`, once a second, only when the lock is contended:

```
[LOCKPROBE] write HELD, read HELD/BLOCKED
  writer=<loc> task=<id> held_for=<d> readers=N
  acquires=N releases=N last_release=<loc> <d> ago
  waiting=N
    waiter <loc> kind=write task=<id> waiting_for=<d> polls=N wakes=N
```

How to read it:

- `acquires == releases` and `writer=none` with waiters piling up ⇒ nobody holds the lock;
  the permits were handed to a waiter that is not being polled. Look for the same task id
  appearing twice (problem 1).
- `wakes=0` on the head waiter ⇒ the lock was never handed over (a holder is stuck).
- One `writer=<loc>` with a large `held_for` ⇒ that call site is doing slow work under the
  lock.

`[LOCKDEADLOCK]` is printed immediately if a task acquires the lock while already holding it.

`[LOCKTIME]`, every 10 s, is the contention view: cumulative hold time and acquisition count
per call site, biggest first. Use it whenever throughput — not liveness — is the question.

### `print_debug_info` (`--debug SECS`)

Two additions:

```
blocked_head: Some((ts, msg_id, missing_groups, has_local_ts)) missing_local_ts: N
    Gid(1) - safe_idx:.. next_entry:.. buffered:N following_epoch:E acks:[..]
```

- **`blocked_head`** — the head-of-line message and the groups whose timestamp it still
  lacks. A head that *changes* between dumps means backlog; a head that stays *identical*
  while the log grows means a genuine stall, and `missing_groups` names who owes the
  timestamp.
- **`missing_local_ts`** — messages known only through a remote timestamp that this group
  still has to propose. Non-zero and persistent on a primary means the repair path is not
  keeping up; non-zero on a follower is normal (only the primary can propose).
- **`buffered` / `following_epoch`** on each remote learner — `buffered` growing while
  `safe_idx` is frozen means entries arrive fine but no quorum of that remote group's acks is
  being seen: the problem is in *that* group's replicas (look for `== IDLE ==`), not in the
  fetch path.

Remote timestamp traces now name their source group (`remote ts from Gid(2): ...`), which is
what makes "we only ever hear from one group" visible.

### Log sampling

`sampled_print!(LOG_SAMPLE, ...)` prints one in `LOG_SAMPLE` calls per call site. Applied to
the three per-message traces (`ADD_ENTRY_TS`, `add_entry_ts: ... already present`,
`remote ts from`). Change `LOG_SAMPLE` in `primcast-core/src/lib.rs`, or set
`log_enabled: false` in the config for clean throughput measurements.

## Tunables introduced

| Constant | Value | Where | Meaning |
| --- | --- | --- | --- |
| `LOG_SAMPLE` | 10 | `primcast-core/src/lib.rs` | 1-in-N sampling for hot traces |
| `METADATA_FLUSH_INTERVAL` | 100 ms | `primcast-core/src/lib.rs` | max rate for metadata persistence |
| `WRITE_BUF_LIMIT` | 256 | `persistence/lmdb_impl.rs` | log entries per LMDB transaction |
| `MISSING_PROPOSAL_INTERVAL` | 500 ms | `primcast-net/src/lib.rs` | how often the primary looks for lost proposals |
| `MISSING_PROPOSAL_AGE` | 1 s | `primcast-net/src/lib.rs` | how long a message waits before being re-proposed |
| `MISSING_PROPOSAL_BATCH` | 1000 | `primcast-net/src/lib.rs` | max messages repaired per round |

## Verification performed

Repeated `kill -9` of a group's primary (G0 and G1, several runs, 1000–5000 msg/s per client):

- new primary elected in 3–5 s;
- all surviving replicas stay `PRIMARY`/`FOLLOWER` — no `IDLE` flapping, no panics;
- `acks` advance for every live replica, `safe_len` tracks `log_len`;
- `blocked_head` keeps moving and `missing_local_ts` returns to 0;
- stopping the clients drains every replica to `pending: 0`.

## Known remaining issues

- `multi_group_recovery.rs` applies a whole recovered batch under one write-lock acquisition;
  holds of ~5 s were observed on a recovering follower.
- The `--debug` printer takes the *write* lock and was seen holding it for 1–2 s
  (`print_debug_info` also counts log entries in the store).
- The append path still writes each entry individually into the write-behind buffer
  (serialization and a `HashMap` insert per entry); a batched `put_log_entries` on the
  persistence trait would remove the remaining per-entry overhead.
- Throughput is still bounded by the single replica-wide lock: everything the replica does is
  serialized through it, so the long-term fix is either finer-grained state or a single owner
  task for `GroupReplica`.
- `.cargo/config.toml` now enables `--cfg tokio_unstable` for every build in this repo (it
  pairs with the optional `console-subscriber` setup in `examples/server.rs`, activated by
  `TOKIO_CONSOLE_PORT`).
