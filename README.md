# RaftKV

A three-node, crash-recoverable key/value store built to make Raft's failure modes visible. The consensus core is Java 17; committed commands are applied to a checksummed, fsync-backed C write-ahead log through JNI.

[![CI](https://github.com/EesherJ39/raft-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/EesherJ39/raft-kv/actions/workflows/ci.yml)

## Why this exists

RaftKV is a correctness project, not a wrapper around a consensus library. It implements and tests the parts that usually expose incomplete Raft implementations:

- persisted terms, votes, logs, commit indexes, and snapshots;
- log matching and conflict-term/index hints for suffix repair;
- current-term majority commit rules and committed-only state application;
- leader-only, quorum-checked linearizable reads;
- snapshot compaction and `InstallSnapshot` catch-up;
- CRC32 WAL records, fsync-before-apply, and torn-tail recovery;
- bounded binary RPC decoding, request limits, non-root containers, and no runtime dependencies.

The root URL serves a live protocol trace showing elections, replication, commits, snapshots, and leadership changes.

```mermaid
flowchart LR
    C[HTTP client] -->|GET / PUT| L[Raft leader]
    L -->|AppendEntries| F1[Follower]
    L -->|AppendEntries| F2[Follower]
    L -->|majority acknowledged| M[Commit + apply]
    M --> J[Java state mirror]
    M -->|JNI| W[C WAL: CRC32 + fsync]
    L -. lag exceeds retained log .->|InstallSnapshot| F2
```

## Evidence, not assertions

| Verification | What it exercises | Latest local result |
|---|---|---|
| Core protocol suite | persisted vote, election, quorum loss, stale suffix repair, exact match acknowledgements, snapshots, corrupt-state detection, RPC round trips | 8/8 passed |
| Deterministic fault harness | 1,000 seeded scenarios with partitions, crash/restart, minority leaders, duplicate delivery, healing, and convergence | 21,621 committed writes; 36,992 blocked RPCs; over 17,000 duplicate deliveries |
| History checker | backtracking register linearizability with real-time precedence, decomposed by key | 5/5 unit tests passed |
| Live Linux cluster | concurrent client history, leader termination, replacement election, restart/catch-up, native backend assertion | 60/60 operations completed and linearizable; 186 ms observed failover |

The live timings are one WSL2 run on September 3, 2026, not a production performance claim. Run the scripts below to reproduce the checks on your machine.

## Run it

### Docker Compose

```bash
docker compose up --build -d

# Open the live trace UI (use start on Windows or xdg-open on Linux).
open http://localhost:8081

# Find the leader.
curl http://localhost:8081/v1/status
curl http://localhost:8082/v1/status
curl http://localhost:8083/v1/status

# A follower returns 307 with the leader location; -L preserves the PUT.
curl -i -L -X PUT --data 'world' http://localhost:8082/v1/kv/hello
curl -L http://localhost:8081/v1/kv/hello
```

### Build and test without Docker

Requirements: Java 17, Python 3.10+, CMake, and a C11 compiler.

```bash
bash scripts/test.sh
bash scripts/build_all.sh
bash scripts/live_fault_test.sh
```

The Windows protocol/checker suite is available as `scripts/test.ps1`; the native build targets Linux/macOS.

## Public API

| Method | Path | Result |
|---|---|---|
| `GET` | `/` | live Raft trace viewer |
| `GET` | `/healthz` | process liveness |
| `GET` | `/v1/status` | role, term, leader, commit/apply/log/snapshot indexes |
| `GET` | `/v1/trace` | bounded structured event history |
| `PUT` | `/v1/kv/{key}` | replicate and commit a UTF-8 value; `307` on a follower, `503` without quorum |
| `GET` | `/v1/kv/{key}` | quorum-checked linearizable read; `307` on a follower |

Internal Raft endpoints use a bounded, versioned binary codec instead of reflection or object deserialization.

## Repository map

- `java/src/main/.../RaftNode.java` — consensus state machine and replication logic
- `java/src/main/.../FileRaftStorage.java` — atomic, checksummed consensus-state persistence
- `c/kvstore.c` — JNI state-machine WAL and crash-tail repair
- `java/src/test/...` — deterministic cluster, protocol regression suite, and chaos harness
- `python/linearizability.py` — real-time history checker
- `scripts/live_fault_test.sh` — live three-process fault and recovery verification

Read [DESIGN.md](docs/DESIGN.md), [TESTING.md](docs/TESTING.md), and [LIMITATIONS.md](docs/LIMITATIONS.md) before treating this as anything beyond an educational systems implementation.

## Design stance

The code favors explicit invariants and inspectability over headline throughput. Consensus state is synchronously persisted, client writes return only after majority commit, failed-quorum writes are never exposed by the state machine, and a leader read must first confirm that it still reaches a majority. See [LIMITATIONS.md](docs/LIMITATIONS.md) for the intentionally unfinished production work.

## References

- Diego Ongaro and John Ousterhout, [In Search of an Understandable Consensus Algorithm](https://raft.github.io/raft.pdf)
- Martin Kleppmann, [Designing a linearizability checker](https://martin.kleppmann.com/2020/04/17/linearizability-checker.html)
