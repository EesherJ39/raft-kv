package com.example.raftkv;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Durable consensus state. Implementations must make each save atomic. */
public interface RaftStorage {
    record Snapshot(long lastIncludedIndex, int lastIncludedTerm, Map<String, String> values) {
        public Snapshot {
            values = Map.copyOf(values);
        }

        public static Snapshot empty() {
            return new Snapshot(-1, -1, Map.of());
        }
    }

    record StoredState(
            int currentTerm,
            String votedFor,
            long commitIndex,
            Snapshot snapshot,
            List<LogEntry> log) {
        public StoredState {
            if (currentTerm < 0) throw new IllegalArgumentException("negative term");
            snapshot = snapshot == null ? Snapshot.empty() : snapshot;
            log = List.copyOf(log);
        }

        public static StoredState empty() {
            return new StoredState(0, null, -1, Snapshot.empty(), List.of());
        }
    }

    StoredState load() throws IOException;
    void save(StoredState state) throws IOException;
}
