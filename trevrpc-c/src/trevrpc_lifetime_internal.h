#ifndef TREVRPC_LIFETIME_INTERNAL_H
#define TREVRPC_LIFETIME_INTERNAL_H

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>

typedef struct trevrpc_lifetime_gate {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    size_t entered;
    bool closed;
} trevrpc_lifetime_gate;

int trevrpc_lifetime_gate_init(trevrpc_lifetime_gate* gate);
void trevrpc_lifetime_gate_destroy(trevrpc_lifetime_gate* gate);
int trevrpc_lifetime_gate_enter(trevrpc_lifetime_gate* gate);
void trevrpc_lifetime_gate_leave(trevrpc_lifetime_gate* gate);
void trevrpc_lifetime_gate_open(trevrpc_lifetime_gate* gate);
void trevrpc_lifetime_gate_close(trevrpc_lifetime_gate* gate);
void trevrpc_lifetime_gate_wait_closed(trevrpc_lifetime_gate* gate);
void trevrpc_lifetime_gate_close_and_wait(trevrpc_lifetime_gate* gate);

#endif
