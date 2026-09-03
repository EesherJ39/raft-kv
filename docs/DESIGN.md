# Design notes

## Safety model

Each node serializes protocol state behind one mutex. Disk state contains `currentTerm`, `votedFor`, `commitIndex`, the latest snapshot, and the retained suffix. Term/vote changes and accepted log mutations are persisted before a successful RPC response leaves the node.

The implementation enforces these invariants:

1. A node casts at most one vote per term, including across restart.
2. A candidate wins only with a strict majority and an up-to-date log.
3. A follower accepts entries only after its `prevLogIndex` and `prevLogTerm` match.
4. A leader advances `commitIndex` through majority replication only for an entry from its current term.
5. The state machine applies only indexes at or below the persisted commit index.
6. A successful read is served only by the leader after a current-term entry is committed and a fresh quorum round succeeds.

These are the project's safety goals. The tests provide evidence for them; they are not a formal proof.

## Replication and repair

Leaders track `nextIndex` and `matchIndex` per follower. A rejection includes the follower's conflicting term and the first index of that term, allowing the leader to skip a mismatched range instead of backing up one entry at a time.

A success response acknowledges only the request's matched prefix—not any unrelated suffix already held by the follower. That distinction prevents a stale follower tail from being counted toward the leader's majority.

Network calls happen outside the protocol mutex. Responses are rechecked against the active role and term before they can update leader state.

## Persistence layers

There are two deliberately separate persistence concerns:

- `FileRaftStorage` stores consensus metadata, snapshots, and retained log entries in one versioned CRC32 envelope. It writes a sibling temporary file, forces it to disk, then atomically replaces the prior state where the filesystem supports it.
- `Storage` applies committed key/value commands to a Java mirror and to `c/kvstore.c` through JNI. The C backend writes bounded little-endian WAL records with CRC32, flushes and calls `fsync`, then updates its in-memory index. Startup replay truncates an incomplete or corrupt tail to the last valid record.

Raft consensus storage is authoritative for reconstructing committed state. The native WAL demonstrates a durability boundary and cross-language integration rather than acting as a second source of consensus truth.

## Snapshots

When committed entries since the last snapshot reach the configured threshold, the node captures the state-machine map, records the included index/term, and releases the covered log prefix. A leader sends `InstallSnapshot` when a follower's `nextIndex` is behind the retained log. A compatible local suffix is preserved; otherwise it is discarded.

## HTTP and observability

The public API redirects followers to the known leader. Reads and writes return `503` when a majority cannot be reached. Internal RPCs use length-bounded binary payloads; client values are capped at 1 MiB. Every node keeps a bounded ring of trace events for the browser viewer and `/v1/trace` endpoint.

## Threading

One daemon scheduler drives elections and heartbeats. A fixed HTTP worker pool handles public and internal requests. The simple synchronous peer client is easy to reason about but creates a throughput ceiling; parallel/asynchronous replication is listed as future work.
