package com.example.raftkv;

import java.util.LinkedHashMap;

/** Durable-across-node-restarts test storage, intentionally lost across process restarts. */
public final class InMemoryRaftStorage implements RaftStorage {
    private StoredState state = StoredState.empty();

    @Override
    public synchronized StoredState load() {
        return copy(state);
    }

    @Override
    public synchronized void save(StoredState state) {
        this.state = copy(state);
    }

    private static StoredState copy(StoredState state) {
        Snapshot snapshot = new Snapshot(
                state.snapshot().lastIncludedIndex(),
                state.snapshot().lastIncludedTerm(),
                new LinkedHashMap<>(state.snapshot().values()));
        return new StoredState(state.currentTerm(), state.votedFor(), state.commitIndex(), snapshot, state.log());
    }
}
