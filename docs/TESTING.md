# Testing strategy

## Fast regression suite

`bash scripts/test.sh` compiles Java 17 with all lint warnings promoted to errors, runs the protocol tests, executes 1,000 seeded fault scenarios, checks the Python history checker, and builds the C11 JNI library with `-Wall -Wextra -Wpedantic -Werror`.

The protocol cases cover:

- persisted one-vote-per-term behavior;
- election, majority replication, and linearizable reads;
- read/write rejection when the leader loses quorum;
- overwrite of an uncommitted stale suffix after leadership changes;
- prevention of overstated match acknowledgements;
- snapshot installation into a restarted lagging follower;
- consensus-state recovery and checksum corruption detection;
- bounded binary RPC encode/decode round trips.

The chaos harness seeds every scenario and checks final state-machine equality after:

1. initial election and writes;
2. follower crash and majority progress;
3. follower restart and catch-up;
4. old-leader isolation and rejected minority writes;
5. replacement election and writes;
6. duplicate RPC delivery, network healing, and convergence.

The exact duplicate-delivery counter can vary with map iteration order, so documentation reports a conservative lower bound. Scenario seeds and assertions are deterministic.

## Linearizability checker

`python/linearizability.py` searches legal sequential register histories while respecting real-time precedence: when operation A finishes before B starts, A must precede B. Histories are decomposed by key, then explored with memoized backtracking. Failed operations have no state-machine effect.

The checker has positive and negative unit histories, including a stale read that must be rejected. It is intentionally scoped to read/write registers rather than arbitrary transactional models.

## Live process test

After `bash scripts/build_all.sh`, run:

```bash
bash scripts/live_fault_test.sh
```

The script creates disposable data directories, launches three real JVM processes, asserts that JNI storage loaded, sends concurrent operations through all three endpoints, validates the captured history, terminates the active leader, times replacement election, commits and reads a post-failover value, restarts the old leader, and waits for its commit index to catch up.

The script binds ports `18081`–`18083` and removes its temporary processes/data on exit.

## Performance probe

`python/benchmark.py` reports throughput plus median/p95/p99 client latency for a stable leader. It exists to make tradeoffs measurable, not to imply production performance. Run it only after a cluster is healthy and record the machine, configuration, operation count, concurrency, and value size with any result.
