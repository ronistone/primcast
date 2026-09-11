# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build / Run / Test

Cargo workspace (`primcast-core`, `primcast-net`). Edition 2021.

```bash
# Build everything (library + examples)
cargo build --examples
cargo build --release --examples

# Run a single replica (see scripts/ for full tmux launchers)
./target/debug/examples/server --gid G --pid P --cfg example_3_groups.yaml [--threads N] [--hybrid] [--debug SECS] [--check]

# Load generators (two variants under primcast-net/examples)
./target/release/examples/closed_loop  --gid G --pid P --cfg FILE -o OUTSTANDING   [--globals F] [--global-dests N] [--stats S] [--threads N]
./target/release/examples/open_loop    --gid G --pid P --cfg FILE -m MSGS_PER_SEC [--globals F] [--global-dests N] [--stats S] [--threads N]

# Full local cluster in tmux (3x3 by default, uses example_3_groups.yaml)
./scripts/tmux-servers.sh     [--release] [--threads N] [--hybrid] [--check] [--debug N]
./scripts/tmux-open_loop.sh   [same flags] -m MSGS_PER_SEC --globals F
./scripts/tmux-closed_loop.sh [same flags] -o OUTSTANDING  --globals F

# Single-workload test of consistency (open_loop ex.)
./scripts/tmux-open_loop.sh --globals 0.5 -m 1000 --release --check
# ...kill, then:
./scripts/check_consistency.sh   # grep DELIVERY out of out_GID_PID.txt, diff across replicas/groups

# Single test filter
cargo test -p primcast-core <name>
cargo test -p primcast-net  <name>
```

Release profile has `debug = true` (for `cargo flamegraph`). No `[features]` gate persistence backends — all three (lmdb/rocksdb/sled) always compile.

Requires ZooKeeper reachable at URL(s) in the yaml config (`zookeeper_url` per group). For local: `docker-compose up` (`docker-compose.yaml` runs a single ZK node). For the 3-server cluster see `CLUSTER_README.md` and `scripts/cluster/`.

## Architecture

Primcast = atomic multicast protocol. Replicas grouped by `Gid`; each group has `Pid`s. Messages addressed to a `GidSet` (one or more groups). Guarantees total order of delivery across all destination groups.

### Crates

- **primcast-core** — pure protocol state machine. No network, no IO on the hot path. `GroupReplica` in `lib.rs` (~58KB) owns the in-memory log, epochs, pending-set, clock, and remote-learner state. Feeds proposals in, emits acks/deliveries out via method calls. Persistence is a pluggable `PersistenceLayer` trait with LMDB/RocksDB/Sled impls under `src/persistence/`.
- **primcast-net** — tokio runtime wrapping core. `PrimcastReplica` in `lib.rs` (~74KB) spawns per-peer connection tasks, leader election (ZooKeeper-backed), recovery, and exposes `PrimcastHandle` for app-level proposal/delivery. Wire format is length-delimited bincode via `codec.rs` / `messages.rs`. Recovery logic (log gap repair, collaborative recovery) is `recovery.rs`.

### Key types (primcast-core::types)

`Gid`, `Pid`, `Clock` (logical or hybrid — see `clock.rs`), `MsgId` (random u128 picked by proposer), `Epoch`, `LogEntry`, `GidSet`, `ReplicaState` (`Primary | Candidate | Promised | Follower | Recovering`).

### Protocol flow

1. Client → primary of some group → `propose(msg, dests)`.
2. Primary assigns local ts, appends to log, replicates within group, collects quorum acks → local safe.
3. For multi-dest msgs, groups exchange remote acks (via `RemoteLearner`) to agree on final delivery ts.
4. `PendingSet` (`pending.rs`) tracks undelivered msgs, orders deliveries by `(final_ts, MsgId)`.

### Leader election / recovery

- `primcast-net/src/leader_election.rs` — ZooKeeper ephemeral-znode election per `(gid)`. `pid 0` is primary by default when no election is active (see README notes).
- `primcast-net/src/recovery.rs` — log-gap repair. `COLLAB_RECOVERY_GAP_THRESHOLD = 1000` in `lib.rs` gates collaborative (multi-peer) recovery vs single-source fetch.

### Persistence

Trait: `PersistenceLayer` in `primcast-core/src/persistence/mod.rs`. Operations: log put/get/truncate/list, metadata put/get, flush/close/clear. `PersistedState` (lib.rs) is loaded on restart into `ReplicaState::Recovering`, replayed, then transitions to Follower/Primary. DB data lives in `db/primcast.{00,01,02}/` locally.

### Config

YAML (`example_{1,2,3}_groups.yaml`, `cluster_config.yaml`). Loaded by `primcast-core/src/config.rs`. Top-level: `leader_timeout_secs`, `reconnect_timeout_secs`, `log_enabled`, `groups[]`. Per group: `gid`, `quorum`, `zookeeper_url`, `peers[{pid, ip, port, client_port}]`. Peer port = intra-cluster, client_port = client-facing.

## Notes from README

- Leader election + recovery were previously "not implemented/tested" — they now exist (`leader_election.rs`, `recovery.rs`); when touching, verify against `scripts/check_consistency.sh`.
- >2 threads rarely helps benchmarks; pin primary to NUMA-close CPUs: `taskset 0x5 closed_loop ... --threads 2`.
- Consistency check workflow: run cluster with `--check` → each proc emits `out_GID_PID.txt` with `DELIVERY` lines → `scripts/check_consistency.sh` diffs intra-group and cross-group delivery orders.

## Cluster deploy

See `CLUSTER_README.md` for the 3-node deployment (servers 192.168.2.17-19). Entry points: `scripts/cluster/deploy-to-cluster.sh`, `start-all-zookeeper.sh`, `start-all-primcast.sh`, `check-cluster-status.sh`, `stop-all.sh`. Per-server env in `~/.primcast_env` (`THREADS`, `GLOBALS`, `GLOBAL_DESTS`, `MSG_SIZE`, `DEBUG`).

## MCP Tools: code-review-graph

**IMPORTANT: This project has a knowledge graph. ALWAYS use the
code-review-graph MCP tools BEFORE using Grep/Glob/Read to explore
the codebase.** The graph is faster, cheaper (fewer tokens), and gives
you structural context (callers, dependents, test coverage) that file
scanning cannot.

### When to use graph tools FIRST

- **Exploring code**: `semantic_search_nodes` or `query_graph` instead of Grep
- **Understanding impact**: `get_impact_radius` instead of manually tracing imports
- **Code review**: `detect_changes` + `get_review_context` instead of reading entire files
- **Finding relationships**: `query_graph` with callers_of/callees_of/imports_of/tests_for
- **Architecture questions**: `get_architecture_overview` + `list_communities`

Fall back to Grep/Glob/Read **only** when the graph doesn't cover what you need.

### Key Tools

| Tool | Use when |
|------|----------|
| `detect_changes` | Reviewing code changes — gives risk-scored analysis |
| `get_review_context` | Need source snippets for review — token-efficient |
| `get_impact_radius` | Understanding blast radius of a change |
| `get_affected_flows` | Finding which execution paths are impacted |
| `query_graph` | Tracing callers, callees, imports, tests, dependencies |
| `semantic_search_nodes` | Finding functions/classes by name or keyword |
| `get_architecture_overview` | Understanding high-level codebase structure |
| `refactor_tool` | Planning renames, finding dead code |

### Workflow

1. The graph auto-updates on file changes (via hooks).
2. Use `detect_changes` for code review.
3. Use `get_affected_flows` to understand impact.
4. Use `query_graph` pattern="tests_for" to check coverage.
