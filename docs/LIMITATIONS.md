# Limitations and production roadmap

RaftKV is an educational implementation with unusually strong executable evidence for its size. It is not a production database.

## Known limitations

- Membership is static and requires a non-empty odd-sized cluster. Joint consensus is not implemented.
- Replication is synchronous and peers are contacted sequentially. Slow peers therefore affect tail latency.
- Consensus persistence rewrites a checksummed state image for clarity. It does not use a segmented or group-committed Raft log.
- The key/value model supports PUT and GET only—no delete, compare-and-swap, transactions, TTLs, or watches.
- Snapshots are sent as one bounded RPC rather than streamed in chunks.
- Client requests have no session IDs or deduplication table, so a timed-out retry can append the same logical write twice.
- Inter-node traffic has no TLS, authentication, or authorization. Run only on a trusted network.
- The trace ring is local and in-memory; metrics, structured logs, and distributed tracing are not exported.
- Fault tests simulate partitions and duplicate delivery, and the live test kills processes, but there is no formal model check, disk-fault injection, clock-skew campaign, or multi-hour soak test.

## Highest-value next steps

1. Segment the consensus log and batch/group-commit fsyncs without weakening the acknowledgement boundary.
2. Replicate concurrently and add adaptive deadlines, connection reuse metrics, and backpressure.
3. Add client operation IDs and a replicated deduplication table for retry-safe writes.
4. Stream snapshots with checksummed chunks and resumable installation.
5. Add mTLS node identity, client authentication, and an authorization layer.
6. Model the protocol in TLA+ or PlusCal and compare traces against implementation invariants.
7. Add Jepsen-style network/disk nemeses and long-running invariant checks.

Keeping this list explicit is part of the project: credible systems work distinguishes demonstrated guarantees from future claims.
