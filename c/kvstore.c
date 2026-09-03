#define _POSIX_C_SOURCE 200809L

/*
 * JNI-backed key/value state machine.
 *
 * WAL record (little endian): magic, key length, value length, CRC32, payload.
 * A torn or corrupt tail is discarded during replay. Each acknowledged call
 * flushes and fsyncs the record before updating the in-memory map.
 */

#include <jni.h>
#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#define RECORD_MAGIC 0x574B5652u /* RVKW on disk */
#define MAX_KEY_BYTES (4u * 1024u)
#define MAX_VALUE_BYTES (4u * 1024u * 1024u)

typedef struct {
    uint8_t *key;
    uint32_t key_len;
    uint8_t *value;
    uint32_t value_len;
} kv_entry;

static kv_entry *entries;
static size_t entry_count;
static size_t entry_capacity;
static FILE *wal;
static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

static void *checked_malloc(size_t size) {
    void *value = malloc(size == 0 ? 1 : size);
    if (value == NULL) {
        fprintf(stderr, "raftkv native storage: out of memory\n");
        abort();
    }
    return value;
}

static uint32_t crc32_update(uint32_t crc, const uint8_t *data, size_t length) {
    crc = ~crc;
    for (size_t i = 0; i < length; i++) {
        crc ^= data[i];
        for (int bit = 0; bit < 8; bit++) {
            uint32_t mask = (uint32_t)-(int32_t)(crc & 1u);
            crc = (crc >> 1u) ^ (0xEDB88320u & mask);
        }
    }
    return ~crc;
}

static uint32_t record_crc(const uint8_t *key, uint32_t key_len, const uint8_t *value, uint32_t value_len) {
    uint32_t crc = crc32_update(0, key, key_len);
    return crc32_update(crc, value, value_len);
}

static void write_u32_le(uint8_t out[4], uint32_t value) {
    out[0] = (uint8_t)value;
    out[1] = (uint8_t)(value >> 8u);
    out[2] = (uint8_t)(value >> 16u);
    out[3] = (uint8_t)(value >> 24u);
}

static uint32_t read_u32_le(const uint8_t in[4]) {
    return (uint32_t)in[0]
            | ((uint32_t)in[1] << 8u)
            | ((uint32_t)in[2] << 16u)
            | ((uint32_t)in[3] << 24u);
}

static void clear_entries(void) {
    for (size_t i = 0; i < entry_count; i++) {
        free(entries[i].key);
        free(entries[i].value);
    }
    free(entries);
    entries = NULL;
    entry_count = 0;
    entry_capacity = 0;
}

static void put_memory(const uint8_t *key, uint32_t key_len, const uint8_t *value, uint32_t value_len) {
    for (size_t i = 0; i < entry_count; i++) {
        if (entries[i].key_len == key_len && memcmp(entries[i].key, key, key_len) == 0) {
            uint8_t *replacement = checked_malloc(value_len);
            memcpy(replacement, value, value_len);
            free(entries[i].value);
            entries[i].value = replacement;
            entries[i].value_len = value_len;
            return;
        }
    }

    if (entry_count == entry_capacity) {
        size_t next_capacity = entry_capacity == 0 ? 64 : entry_capacity * 2;
        kv_entry *resized = realloc(entries, next_capacity * sizeof(*entries));
        if (resized == NULL) {
            fprintf(stderr, "raftkv native storage: out of memory\n");
            abort();
        }
        entries = resized;
        entry_capacity = next_capacity;
    }
    kv_entry *entry = &entries[entry_count++];
    entry->key = checked_malloc(key_len);
    entry->value = checked_malloc(value_len);
    memcpy(entry->key, key, key_len);
    memcpy(entry->value, value, value_len);
    entry->key_len = key_len;
    entry->value_len = value_len;
}

static int append_record(const uint8_t *key, uint32_t key_len, const uint8_t *value, uint32_t value_len) {
    uint8_t header[16];
    write_u32_le(header, RECORD_MAGIC);
    write_u32_le(header + 4, key_len);
    write_u32_le(header + 8, value_len);
    write_u32_le(header + 12, record_crc(key, key_len, value, value_len));
    if (fwrite(header, sizeof(header), 1, wal) != 1) return -1;
    if (key_len > 0 && fwrite(key, key_len, 1, wal) != 1) return -1;
    if (value_len > 0 && fwrite(value, value_len, 1, wal) != 1) return -1;
    if (fflush(wal) != 0) return -1;
    return fsync(fileno(wal));
}

static int replay_and_repair_tail(FILE *file) {
    if (fseek(file, 0, SEEK_SET) != 0) return -1;
    off_t valid_end = 0;
    for (;;) {
        uint8_t header[16];
        size_t header_bytes = fread(header, 1, sizeof(header), file);
        if (header_bytes == 0 && feof(file)) break;
        if (header_bytes != sizeof(header)) break;

        uint32_t magic = read_u32_le(header);
        uint32_t key_len = read_u32_le(header + 4);
        uint32_t value_len = read_u32_le(header + 8);
        uint32_t expected_crc = read_u32_le(header + 12);
        if (magic != RECORD_MAGIC || key_len > MAX_KEY_BYTES || value_len > MAX_VALUE_BYTES) break;

        uint8_t *key = checked_malloc(key_len);
        uint8_t *value = checked_malloc(value_len);
        int complete = (key_len == 0 || fread(key, key_len, 1, file) == 1)
                && (value_len == 0 || fread(value, value_len, 1, file) == 1);
        if (!complete || record_crc(key, key_len, value, value_len) != expected_crc) {
            free(key);
            free(value);
            break;
        }
        put_memory(key, key_len, value, value_len);
        free(key);
        free(value);
        valid_end = ftello(file);
    }

    clearerr(file);
    if (ftruncate(fileno(file), valid_end) != 0) return -1;
    return fseeko(file, 0, SEEK_END);
}

JNIEXPORT jboolean JNICALL
Java_com_example_raftkv_Storage_nativeInit(JNIEnv *env, jclass type, jstring directory) {
    (void)type;
    const char *path = (*env)->GetStringUTFChars(env, directory, NULL);
    if (path == NULL) return JNI_FALSE;

    pthread_mutex_lock(&mutex);
    if (wal != NULL) {
        fclose(wal);
        wal = NULL;
    }
    clear_entries();
    if (mkdir(path, 0770) != 0 && errno != EEXIST) {
        pthread_mutex_unlock(&mutex);
        (*env)->ReleaseStringUTFChars(env, directory, path);
        return JNI_FALSE;
    }

    size_t length = strlen(path) + sizeof("/kv.wal");
    char *wal_path = checked_malloc(length);
    snprintf(wal_path, length, "%s/kv.wal", path);
    wal = fopen(wal_path, "a+b");
    free(wal_path);
    int ok = wal != NULL && replay_and_repair_tail(wal) == 0;
    if (!ok && wal != NULL) {
        fclose(wal);
        wal = NULL;
    }
    pthread_mutex_unlock(&mutex);
    (*env)->ReleaseStringUTFChars(env, directory, path);
    return ok ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jboolean JNICALL
Java_com_example_raftkv_Storage_nativePut(
        JNIEnv *env, jclass type, jbyteArray key_array, jbyteArray value_array) {
    (void)type;
    if (key_array == NULL || value_array == NULL) return JNI_FALSE;
    jsize key_len = (*env)->GetArrayLength(env, key_array);
    jsize value_len = (*env)->GetArrayLength(env, value_array);
    if (key_len <= 0 || (uint32_t)key_len > MAX_KEY_BYTES || (uint32_t)value_len > MAX_VALUE_BYTES) {
        return JNI_FALSE;
    }
    jbyte *key = (*env)->GetByteArrayElements(env, key_array, NULL);
    jbyte *value = (*env)->GetByteArrayElements(env, value_array, NULL);
    if (key == NULL || value == NULL) {
        if (key != NULL) (*env)->ReleaseByteArrayElements(env, key_array, key, JNI_ABORT);
        if (value != NULL) (*env)->ReleaseByteArrayElements(env, value_array, value, JNI_ABORT);
        return JNI_FALSE;
    }

    pthread_mutex_lock(&mutex);
    int persisted = wal != NULL
            && append_record((uint8_t *)key, (uint32_t)key_len, (uint8_t *)value, (uint32_t)value_len) == 0;
    if (persisted) {
        put_memory((uint8_t *)key, (uint32_t)key_len, (uint8_t *)value, (uint32_t)value_len);
    }
    pthread_mutex_unlock(&mutex);
    (*env)->ReleaseByteArrayElements(env, key_array, key, JNI_ABORT);
    (*env)->ReleaseByteArrayElements(env, value_array, value, JNI_ABORT);
    return persisted ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT jbyteArray JNICALL
Java_com_example_raftkv_Storage_nativeGet(JNIEnv *env, jclass type, jbyteArray key_array) {
    (void)type;
    if (key_array == NULL) return NULL;
    jsize key_len = (*env)->GetArrayLength(env, key_array);
    jbyte *key = (*env)->GetByteArrayElements(env, key_array, NULL);
    if (key == NULL) return NULL;

    jbyteArray result = NULL;
    pthread_mutex_lock(&mutex);
    for (size_t i = 0; i < entry_count; i++) {
        if (entries[i].key_len == (uint32_t)key_len && memcmp(entries[i].key, key, (size_t)key_len) == 0) {
            result = (*env)->NewByteArray(env, (jsize)entries[i].value_len);
            if (result != NULL) {
                (*env)->SetByteArrayRegion(
                        env, result, 0, (jsize)entries[i].value_len, (const jbyte *)entries[i].value);
            }
            break;
        }
    }
    pthread_mutex_unlock(&mutex);
    (*env)->ReleaseByteArrayElements(env, key_array, key, JNI_ABORT);
    return result;
}

JNIEXPORT void JNICALL
Java_com_example_raftkv_Storage_nativeClose(JNIEnv *env, jclass type) {
    (void)env;
    (void)type;
    pthread_mutex_lock(&mutex);
    if (wal != NULL) {
        fflush(wal);
        fsync(fileno(wal));
        fclose(wal);
        wal = NULL;
    }
    clear_entries();
    pthread_mutex_unlock(&mutex);
}
