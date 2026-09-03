package com.example.raftkv;

import java.util.Objects;

/** A command in the replicated Raft log. Indices are absolute and zero-based. */
public record LogEntry(long index, int term, Command command, String key, String value) {
    public enum Command { NOOP, PUT }

    public LogEntry {
        if (index < 0) throw new IllegalArgumentException("index must be non-negative");
        if (term < 0) throw new IllegalArgumentException("term must be non-negative");
        Objects.requireNonNull(command, "command");
        if (command == Command.PUT) {
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(value, "value");
        }
    }

    public static LogEntry noop(long index, int term) {
        return new LogEntry(index, term, Command.NOOP, "", "");
    }

    public static LogEntry put(long index, int term, String key, String value) {
        return new LogEntry(index, term, Command.PUT, key, value);
    }
}
