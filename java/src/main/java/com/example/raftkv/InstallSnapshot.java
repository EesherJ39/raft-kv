package com.example.raftkv;

import java.util.Map;

/** InstallSnapshot RPC used when a follower is behind the compacted log prefix. */
public final class InstallSnapshot {
    private InstallSnapshot() {}

    public record Request(
            int term,
            String leaderId,
            long lastIncludedIndex,
            int lastIncludedTerm,
            Map<String, String> values,
            long leaderCommit) {
        public Request {
            values = Map.copyOf(values);
        }
    }

    public record Response(int term, boolean accepted, long matchIndex) {}
}
