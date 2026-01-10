#include "msqueue.h"
#include <stdlib.h>


static msq_node_t* node_new(void* v) {
msq_node_t* n = (msq_node_t*)calloc(1, sizeof(msq_node_t));
n->value = v;
atomic_store(&n->next, NULL);
return n;
}


void msq_init(msqueue_t* q) {
msq_node_t* dummy = node_new(NULL);
atomic_store(&q->head, dummy);
atomic_store(&q->tail, dummy);
}


void msq_destroy(msqueue_t* q) {
msq_node_t* h = atomic_load(&q->head);
free(h); // only the dummy node
}


void msq_enqueue(msqueue_t* q, void* val) {
msq_node_t* n = node_new(val);
for (;;) {
msq_node_t* tail = atomic_load(&q->tail);
msq_node_t* next = atomic_load(&tail->next);
if (next != NULL) {
atomic_compare_exchange_weak(&q->tail, &tail, next);
continue;
}
if (atomic_compare_exchange_weak(&tail->next, &next, n)) {
atomic_compare_exchange_weak(&q->tail, &tail, n);
return;
}
}
}


void* msq_dequeue(msqueue_t* q) {
for (;;) {
msq_node_t* head = atomic_load(&q->head);
msq_node_t* tail = atomic_load(&q->tail);
msq_node_t* next = atomic_load(&head->next);
if (next == NULL) return NULL; // empty
if (head == tail) {
atomic_compare_exchange_weak(&q->tail, &tail, next);
continue;
}
if (atomic_compare_exchange_weak(&q->head, &head, next)) {
void* val = next->value;
free(head); // free old dummy
return val;
}
}
}