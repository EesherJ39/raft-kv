package com.example.raftkv;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Pure-Java state machine used by protocol tests and benchmarks. */
public final class InMemoryStateMachine implements KeyValueStateMachine {
    private final ConcurrentHashMap<String, String> values = new ConcurrentHashMap<>();

    @Override
    public void put(String key, String value) {
        values.put(key, value);
    }

    @Override
    public String get(String key) {
        return values.get(key);
    }

    @Override
    public Map<String, String> snapshot() {
        return new LinkedHashMap<>(values);
    }

    @Override
    public void restore(Map<String, String> restored) {
        values.clear();
        values.putAll(restored);
    }
}
