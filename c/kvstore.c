// c/kvstore.c
// Minimal JNI-backed KV with an append-only WAL at <DATA_DIR>/kv.wal.
// - nativeInit(String dataDir): load WAL (if any) + prepare for appends
// - nativePut(byte[] key, byte[] value): append to WAL + update in-mem map
// - nativeGet(byte[] key): return last value or null
//
// This is demo-grade: simple linear search map + pthread mutex.
// Good enough for functional testing in the container image.

#include <jni.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <sys/stat.h>
#include <errno.h>
#include <limits.h>

#ifndef PATH_MAX
#define PATH_MAX 4096
#endif

// ---------------- In-memory store ----------------

typedef struct {
    uint8_t *key;
    uint32_t klen;
    uint8_t *val;
    uint32_t vlen;
} entry_t;

static entry_t *entries = NULL;
static size_t    n_entries = 0;
static size_t    cap_entries = 0;

static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;

static void* xmalloc(size_t n) {
    void* p = malloc(n);
    if (!p) { fprintf(stderr, "kvstore: OOM\n"); abort(); }
    return p;
}

static void store_put_mem(const uint8_t* key, uint32_t klen,
                          const uint8_t* val, uint32_t vlen) {
    // Replace if key exists (linear search)
    for (size_t i = 0; i < n_entries; i++) {
        if (entries[i].klen == klen && memcmp(entries[i].key, key, klen) == 0) {
            // replace value
            free(entries[i].val);
            entries[i].val = (uint8_t*)xmalloc(vlen);
            memcpy(entries[i].val, val, vlen);
            entries[i].vlen = vlen;
            return;
        }
    }
    // else append new
    if (n_entries == cap_entries) {
        cap_entries = cap_entries ? cap_entries * 2 : 64;
        entries = (entry_t*)realloc(entries, cap_entries * sizeof(entry_t));
        if (!entries) { fprintf(stderr, "kvstore: OOM\n"); abort(); }
    }
    entries[n_entries].key  = (uint8_t*)xmalloc(klen);
    entries[n_entries].val  = (uint8_t*)xmalloc(vlen);
    memcpy(entries[n_entries].key, key, klen);
    memcpy(entries[n_entries].val, val, vlen);
    entries[n_entries].klen = klen;
    entries[n_entries].vlen = vlen;
    n_entries++;
}

static int store_get_mem(const uint8_t* key, uint32_t klen,
                         const uint8_t** out_val, uint32_t* out_vlen) {
    // Search from the end (slightly faster for recently written keys)
    for (ssize_t i = (ssize_t)n_entries - 1; i >= 0; i--) {
        if (entries[i].klen == klen && memcmp(entries[i].key, key, klen) == 0) {
            *out_val = entries[i].val;
            *out_vlen = entries[i].vlen;
            return 1;
        }
    }
    return 0;
}

// ---------------- WAL handling ----------------

static char wal_path[PATH_MAX] = {0};
static FILE* wal_fp = NULL;

static void ensure_dir(const char* dir) {
    // mkdir -p like behaviour (single level; docker volume already exists)
    if (mkdir(dir, 0777) != 0 && errno != EEXIST) {
        // not fatal in container if volume exists
    }
}

static int replay_wal(FILE* fp) {
    // Record format: [u32 klen][u32 vlen][klen bytes][vlen bytes]
    // Native endianness; fine inside one container image.
    uint32_t klen = 0, vlen = 0;
    while (fread(&klen, sizeof(uint32_t), 1, fp) == 1) {
        if (fread(&vlen, sizeof(uint32_t), 1, fp) != 1) break;
        uint8_t* k = (uint8_t*)xmalloc(klen);
        uint8_t* v = (uint8_t*)xmalloc(vlen);
        if (fread(k, 1, klen, fp) != klen) { free(k); free(v); break; }
        if (fread(v, 1, vlen, fp) != vlen) { free(k); free(v); break; }
        store_put_mem(k, klen, v, vlen);
        free(k);
        free(v);
    }
    // Move to end for subsequent appends
    fseek(fp, 0, SEEK_END);
    return 0;
}

static int wal_append(const uint8_t* key, uint32_t klen,
                      const uint8_t* val, uint32_t vlen) {
    if (!wal_fp) return -1;
    if (fwrite(&klen, sizeof(uint32_t), 1, wal_fp) != 1) return -1;
    if (fwrite(&vlen, sizeof(uint32_t), 1, wal_fp) != 1) return -1;
    if (fwrite(key, 1, klen, wal_fp) != klen) return -1;
    if (fwrite(val, 1, vlen, wal_fp) != vlen) return -1;
    if (fflush(wal_fp) != 0) return -1;
    return 0;
}

// ---------------- JNI bridge ----------------

JNIEXPORT void JNICALL
Java_com_example_raftkv_Storage_nativeInit(JNIEnv* env, jclass cls, jstring jdir) {
    (void)cls;
    const char* dir = (*env)->GetStringUTFChars(env, jdir, NULL);
    if (!dir) return;

    pthread_mutex_lock(&mu);

    // Compose WAL path
    snprintf(wal_path, sizeof(wal_path), "%s/kv.wal", dir);
    ensure_dir(dir);

    wal_fp = fopen(wal_path, "ab+");  // create if not exists, read+append
    if (!wal_fp) {
        pthread_mutex_unlock(&mu);
        (*env)->ReleaseStringUTFChars(env, jdir, dir);
        return;
    }

    // Rewind and replay
    fseek(wal_fp, 0, SEEK_SET);
    replay_wal(wal_fp);

    pthread_mutex_unlock(&mu);
    (*env)->ReleaseStringUTFChars(env, jdir, dir);
}

JNIEXPORT void JNICALL
Java_com_example_raftkv_Storage_nativePut(JNIEnv* env, jclass cls,
                                          jbyteArray jkey, jbyteArray jval) {
    (void)cls;
    if (!jkey || !jval) return;

    jsize klen = (*env)->GetArrayLength(env, jkey);
    jsize vlen = (*env)->GetArrayLength(env, jval);
    jbyte* k = (*env)->GetByteArrayElements(env, jkey, NULL);
    jbyte* v = (*env)->GetByteArrayElements(env, jval, NULL);

    pthread_mutex_lock(&mu);
    // Append to WAL first for durability
    wal_append((const uint8_t*)k, (uint32_t)klen, (const uint8_t*)v, (uint32_t)vlen);
    // Update in-memory map
    store_put_mem((const uint8_t*)k, (uint32_t)klen, (const uint8_t*)v, (uint32_t)vlen);
    pthread_mutex_unlock(&mu);

    (*env)->ReleaseByteArrayElements(env, jkey, k, JNI_ABORT);
    (*env)->ReleaseByteArrayElements(env, jval, v, JNI_ABORT);
}

JNIEXPORT jbyteArray JNICALL
Java_com_example_raftkv_Storage_nativeGet(JNIEnv* env, jclass cls, jbyteArray jkey) {
    (void)cls;
    if (!jkey) return NULL;

    jsize klen = (*env)->GetArrayLength(env, jkey);
    jbyte* k   = (*env)->GetByteArrayElements(env, jkey, NULL);

    const uint8_t* val = NULL;
    uint32_t vlen = 0;

    pthread_mutex_lock(&mu);
    int found = store_get_mem((const uint8_t*)k, (uint32_t)klen, &val, &vlen);
    pthread_mutex_unlock(&mu);

    (*env)->ReleaseByteArrayElements(env, jkey, k, JNI_ABORT);

    if (!found) return NULL;

    jbyteArray out = (*env)->NewByteArray(env, (jsize)vlen);
    if (!out) return NULL;
    (*env)->SetByteArrayRegion(env, out, 0, (jsize)vlen, (const jbyte*)val);
    return out;
}