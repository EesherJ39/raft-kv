package com.example.raftkv;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;

/**
 * KV storage with JNI fast-path (libkvstore.so) and an in-memory fallback so the app
 * still runs if the native lib can't be loaded in your local dev environment.
 */
public class Storage implements RaftNode.KVStore {
    private final String dataDir;
    private final ConcurrentHashMap<String, byte[]> mem = new ConcurrentHashMap<>();
    private boolean useNative = false;

    // Native JNI hooks (implemented by the C code in libkvstore)
    private static native void nativeInit(String dataDir);
    private static native void nativePut(byte[] key, byte[] value);
    private static native byte[] nativeGet(byte[] key);

    public Storage(String dataDir) {
        this.dataDir = dataDir;
        try {
            // The runtime container sets -Djava.library.path to /opt/raftkv/native
            System.loadLibrary("kvstore");
            nativeInit(dataDir);
            useNative = true;
            System.out.println("Storage: using JNI backend at " + dataDir);
        } catch (Throwable t) {
            useNative = false;
            System.out.println("Storage: JNI not available, using in-memory fallback (" + t.getClass().getSimpleName() + ")");
        }
    }

    @Override
    public void put(byte[] key, byte[] value) {
        if (useNative) {
            nativePut(key, value);
        } else {
            mem.put(new String(key, StandardCharsets.UTF_8), value);
        }
    }

    @Override
    public byte[] get(byte[] key) {
        if (useNative) {
            return nativeGet(key);
        } else {
            return mem.get(new String(key, StandardCharsets.UTF_8));
        }
    }
}