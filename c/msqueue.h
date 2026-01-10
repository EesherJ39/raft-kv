#ifndef MSQUEUE_H
#define MSQUEUE_H


#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>


// Michael–Scott lock-free queue (MPMC).
// We use it for the write ingest path: Java enqueues, one C worker thread dequeues.


typedef struct msq_node {
_Atomic(struct msq_node*) next;
void* value;
} msq_node_t;


typedef struct {
_Atomic(msq_node_t*) head;
_Atomic(msq_node_t*) tail;
} msqueue_t;


void msq_init(msqueue_t* q);
void msq_destroy(msqueue_t* q); // frees dummy only
void msq_enqueue(msqueue_t* q, void* val);
void* msq_dequeue(msqueue_t* q); // returns NULL if empty


#endif // MSQUEUE_H