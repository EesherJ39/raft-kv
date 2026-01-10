#include "storage.h"
// ---- Background worker ------------------------------------------------------
static void* worker_loop(void* arg) {
storage* s = (storage*)arg;
while (atomic_load(&s->running)) {
op* o = (op*)msq_dequeue(&s->q);
if (!o) {
struct timespec ts = {0, 5 * 1000 * 1000}; // 5ms
nanosleep(&ts, NULL);
continue;
}
if (o->kind == OP_PUT) {
hm_put(&s->map, o->pair.key, o->pair.klen, o->pair.val, o->pair.vlen);
wal_write(s->wal, o->pair.key, o->pair.klen, o->pair.val, o->pair.vlen);
}
free(o->pair.key);
free(o->pair.val);
free(o);
}
return NULL;
}


// ---- Public API -------------------------------------------------------------
int storage_open(storage* s, const char* wal_path) {
memset(s, 0, sizeof(*s));
s->wal_path = strdup(wal_path);
s->wal = fopen(wal_path, "ab+");
if (!s->wal) return -1;
fseek(s->wal, 0, SEEK_SET);


hm_init(&s->map, 4096);
wal_replay(s->wal, &s->map);


msq_init(&s->q);
atomic_store(&s->running, 1);
if (pthread_create(&s->worker, NULL, worker_loop, s) != 0) return -2;
return 0;
}


void storage_close(storage* s) {
atomic_store(&s->running, 0);
pthread_join(s->worker, NULL);
fclose(s->wal);
free(s->wal_path);
msq_destroy(&s->q);
hm_free(&s->map);
}


void storage_put_async(storage* s,
const unsigned char* k, uint64_t klen,
const unsigned char* v, uint64_t vlen) {
op* o = (op*)calloc(1, sizeof(op));
o->kind = OP_PUT;
o->pair.klen = klen;
o->pair.vlen = vlen;
o->pair.key = (unsigned char*)malloc(klen);
memcpy(o->pair.key, k, klen);
o->pair.val = (unsigned char*)malloc(vlen);
memcpy(o->pair.val, v, vlen);
msq_enqueue(&s->q, o);
}


int storage_get(storage* s,
const unsigned char* k, uint64_t klen,
unsigned char** out, uint64_t* outlen) {
return hm_get(&s->map, k, klen, out, outlen);
}