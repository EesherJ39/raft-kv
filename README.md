<div align="center">

# RaftKV

**A crash-recoverable key/value store that makes Raft's failure modes visible.**

[![CI](https://github.com/EesherJ39/raft-kv/actions/workflows/ci.yml/badge.svg)](https://github.com/EesherJ39/raft-kv/actions/workflows/ci.yml)
[![Java](https://img.shields.io/badge/Consensus-Java_17-ED8B00?logo=openjdk&logoColor=white)](java/)
[![C11](https://img.shields.io/badge/Durability-C11%2FJNI-A8B9CC?logo=c&logoColor=111827)](c/)
[![Fault scenarios](https://img.shields.io/badge/Fault_scenarios-1%2C000-2EA44F)](docs/TESTING.md)
[![Case study](https://img.shields.io/badge/Case_study-eesherj.com-2455E6)](https://eesherj.com/projects/raftkv)

</div>

RaftKV is a three-node distributed store with a Java consensus core and a checksummed, fsync-backed C write-ahead log connected through JNI. It implements Raft directly—no consensus library—and pairs the protocol with deterministic fault injection, a linearizability checker, and a live trace viewer.

## At a glance

| Property | Implementation |
|---|---|
| Consistency | majority-committed writes and quorum-checked linearizable reads |
| Recovery | persisted term/vote/log state, snapshots, `InstallSnapshot`, suffix repair |
| Durability | CRC32 WAL records, fsync-before-apply, torn-tail recovery |
| Fault model | partitions, crash/restart, stale leaders, duplicate delivery, healing |
| Observability | structured election/replication/commit traces served from the root URL |
| Safety surface | bounded binary RPC decoding, request limits, non-root container |

## Architecture

```mermaid
flowchart LR
    C[HTTP client] -->|GET / PUT| L[Raft leader]
    L -->|AppendEntries| F1[Follower]
    L -->|AppendEntries| F2[Follower]
    L -->|majority acknowledged| M[Commit + apply]
    M --> J[Java state mirror]
    M -->|JNI| W[C WAL<br/>CRC32 + fsync]
    L -. retained log is too new .->|InstallSnapshot| F2
```

The implementation includes log-matching checks, conflict-term/index hints, current-term commit rules, committed-only state application, snapshot compaction, and atomic consensus-state persistence. Followers redirect clients to the known leader; a leader rejects operations when it cannot confirm a quorum.

## Reproducible evidence

| Verification | What it exercises | Recorded local result |
|---|---|---|
| Core protocol suite | persisted votes, election, quorum loss, stale suffix repair, exact-match acknowledgements, snapshots, corrupt-state detection, RPC round trips | **8/8 passed** |
| Deterministic chaos harness | 1,000 seeded scenarios with partitions, restart, minority leaders, duplicate delivery, healing, and convergence | **21,621 committed writes**, 36,992 blocked RPCs, 17,000+ duplicate deliveries |
| History checker | backtracking register linearizability with real-time precedence, decomposed by key | **5/5 unit tests passed** |
| Live Linux cluster | concurrent history, leader termination, replacement election, restart/catch-up, native-backend assertion | **60/60 operations linearizable; 186 ms observed failover** |

The live timing is one WSL2 run recorded on September 3, 2026—not a production latency claim. [`docs/TESTING.md`](docs/TESTING.md) documents the workloads and commands needed to reproduce the checks.

## Run the cluster

### Docker Compose

```bash
git clone https://github.com/EesherJ39/raft-kv.git
cd raft-kv
docker compose up --build -d
```

Open `http://localhost:8081` for the live trace viewer, then inspect the cluster:

```bash
curl http://localhost:8081/v1/status
curl http://localhost:8082/v1/status
curl http://localhost:8083/v1/status

# A follower returns 307; -L follows the leader redirect.
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

Windows can run the protocol/checker suite through `scripts/test.ps1`; the native build targets Linux and macOS.

## Public API

| Method | Path | Behavior |
|---|---|---|
| `GET` | `/` | live Raft protocol trace |
| `GET` | `/healthz` | process liveness |
| `GET` | `/v1/status` | role, term, leader, commit/apply/log/snapshot indexes |
| `GET` | `/v1/trace` | bounded structured event history |
| `PUT` | `/v1/kv/{key}` | replicate and commit UTF-8 data; `307` on follower, `503` without quorum |
| `GET` | `/v1/kv/{key}` | quorum-checked linearizable read; `307` on follower |

Internal Raft endpoints use a bounded, versioned binary codec rather than reflection or object deserialization.

## Repository map

| Path | Responsibility |
|---|---|
| `java/.../RaftNode.java` | consensus state machine and replication logic |
| `java/.../FileRaftStorage.java` | atomic, checksummed consensus-state persistence |
| `c/kvstore.c` | JNI state-machine WAL and crash-tail repair |
| `java/src/test/` | deterministic cluster, protocol suite, and chaos harness |
| `python/linearizability.py` | real-time history checker |
| `scripts/live_fault_test.sh` | live three-process failover and recovery verification |

## Design stance

RaftKV favors explicit invariants and inspectability over headline throughput. Consensus state is synchronously persisted, a write returns only after majority commit, failed-quorum writes are never exposed by the state machine, and leader reads first confirm majority reachability.

Read [`docs/DESIGN.md`](docs/DESIGN.md), [`docs/TESTING.md`](docs/TESTING.md), and [`docs/LIMITATIONS.md`](docs/LIMITATIONS.md) before treating this as more than an educational systems implementation. The limitations document covers membership changes, multi-key transactions, production transport/security, and other intentionally unfinished work.

## References

- Diego Ongaro and John Ousterhout, [*In Search of an Understandable Consensus Algorithm*](https://raft.github.io/raft.pdf)
- Martin Kleppmann, [*Designing a linearizability checker*](https://martin.kleppmann.com/2020/04/17/linearizability-checker.html)
