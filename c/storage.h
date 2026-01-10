#ifndef STORAGE_H
#define STORAGE_H


#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <pthread.h>
#include "msqueue.h"


// Simple hashmap + append-only WAL; single background worker applies ops.


typedef struct kv_pair {
uint64_t klen, vlen;
unsigned char* key;
unsigned char* val;
} kv_pair;


typedef struct bucket {
struct bucket* next;
uint64_t hash;
kv_pair pair;
} bucket;


typedef struct hashmap {
bucket** bins;
size_t cap;
} hashmap;


typedef enum { OP_PUT } op_kind;


typedef struct op {
op_kind kind;
kv_pair pair;
} op;


typedef struct storage {
FILE* wal;
char* wal_path;
hashmap map;
msqueue_t q; // lock-free ingest queue
pthread_t worker; // single applier thread
_Atomic int running; // worker loop flag
} storage;


int storage_open(storage* s, const char* wal_path);
void storage_close(storage* s);
void storage_put_async(storage* s,
const unsigned char* k, uint64_t klen,
const unsigned char* v, uint64_t vlen);
int storage_get(storage* s,
const unsigned char* k, uint64_t klen,
unsigned char** out, uint64_t* outlen);


#endif // STORAGE_H