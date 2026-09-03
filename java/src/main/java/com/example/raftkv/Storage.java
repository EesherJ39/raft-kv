package com.example.raftkv;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Replicated state machine with an optional C/JNI write-ahead-log backend.
 * A Java mirror enables deterministic snapshots and keeps local development
 * functional when the native library is unavailable.
 */
public final class Storage implements KeyValueStateMachine, AutoCloseable {
    private final ConcurrentHashMap<String, String> values = new ConcurrentHashMap<>();
    private final boolean nativeEnabled;

    private static native boolean nativeInit(String dataDir);
    private static native boolean nativePut(byte[] key, byte[] value);
    private static native byte[] nativeGet(byte[] key);
    private static native void nativeClose();

    public Storage(String dataDir) {
        boolean enabled = false;
        try {
            Files.createDirectories(Path.of(dataDir));
            System.loadLibrary("kvstore");
            enabled = nativeInit(dataDir);
        } catch (Throwable error) {
            System.err.println("storage: native backend unavailable; using Java memory ("
                    + error.getClass().getSimpleName() + ")");
        }
        nativeEnabled = enabled;
        if (nativeEnabled) System.out.println("storage: C/JNI WAL enabled at " + dataDir);
    }

    @Override
    public void put(String key, String value) {
        if (nativeEnabled) {
            if (!nativePut(key.getBytes(StandardCharsets.UTF_8), value.getBytes(StandardCharsets.UTF_8))) {
                throw new IllegalStateException("native WAL did not durably record the update");
            }
        }
        values.put(key, value);
    }

    @Override
    public String get(String key) {
        String value = values.get(key);
        if (value != null || !nativeEnabled) return value;
        byte[] nativeValue = nativeGet(key.getBytes(StandardCharsets.UTF_8));
        if (nativeValue == null) return null;
        value = new String(nativeValue, StandardCharsets.UTF_8);
        values.putIfAbsent(key, value);
        return value;
    }

    @Override
    public Map<String, String> snapshot() {
        return new LinkedHashMap<>(values);
    }

    @Override
    public void restore(Map<String, String> restored) {
        values.clear();
        values.putAll(restored);
        if (nativeEnabled) {
            for (Map.Entry<String, String> entry : restored.entrySet()) {
                if (!nativePut(
                        entry.getKey().getBytes(StandardCharsets.UTF_8),
                        entry.getValue().getBytes(StandardCharsets.UTF_8))) {
                    throw new IllegalStateException("native WAL did not durably restore the snapshot");
                }
            }
        }
    }

    @Override
    public void close() {
        if (nativeEnabled) nativeClose();
    }
}
