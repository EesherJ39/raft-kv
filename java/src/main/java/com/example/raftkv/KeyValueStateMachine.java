package com.example.raftkv;

import java.util.Map;

/** State-machine boundary kept separate from the Raft log and hard state. */
public interface KeyValueStateMachine {
    void put(String key, String value);
    String get(String key);
    Map<String, String> snapshot();
    void restore(Map<String, String> values);
}
