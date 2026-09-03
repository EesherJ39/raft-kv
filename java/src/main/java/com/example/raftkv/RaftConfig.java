package com.example.raftkv;

/** Tunable protocol limits. Small values can be used by deterministic tests. */
public record RaftConfig(
        long electionMinMs,
        long electionMaxMs,
        long heartbeatMs,
        int appendBatchSize,
        int snapshotThreshold,
        int traceCapacity) {

    public RaftConfig {
        if (electionMinMs <= heartbeatMs) throw new IllegalArgumentException("election timeout must exceed heartbeat");
        if (electionMaxMs < electionMinMs) throw new IllegalArgumentException("invalid election range");
        if (appendBatchSize < 1) throw new IllegalArgumentException("appendBatchSize must be positive");
        if (snapshotThreshold < 0) throw new IllegalArgumentException("snapshotThreshold cannot be negative");
        if (traceCapacity < 16) throw new IllegalArgumentException("traceCapacity too small");
    }

    public static RaftConfig production() {
        return new RaftConfig(450, 900, 125, 128, 256, 2_048);
    }

    public static RaftConfig deterministicTests(int snapshotThreshold) {
        return new RaftConfig(40, 80, 10, 32, snapshotThreshold, 512);
    }
}
