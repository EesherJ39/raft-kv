package com.example.raftkv;

import java.util.List;

/** AppendEntries RPC, including conflict hints for efficient log repair. */
public final class AppendEntries {
    private AppendEntries() {}

    public record Request(
            int term,
            String leaderId,
            long prevLogIndex,
            int prevLogTerm,
            List<LogEntry> entries,
            long leaderCommit) {
        public Request {
            entries = List.copyOf(entries);
        }
    }

    public record Response(
            int term,
            boolean success,
            long matchIndex,
            long conflictIndex,
            int conflictTerm) {}
}
