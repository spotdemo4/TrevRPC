#include "trevrpc_lifetime_internal.h"

#include <errno.h> // IWYU pragma: keep

int trevrpc_lifetime_gate_init(trevrpc_lifetime_gate* gate) {
    if (gate == NULL) {
        return -EINVAL;
    }
    int err = pthread_mutex_init(&gate->mutex, NULL);
    if (err != 0) {
        return -err;
    }
    err = pthread_cond_init(&gate->cond, NULL);
    if (err != 0) {
        pthread_mutex_destroy(&gate->mutex);
        return -err;
    }
    gate->entered = 0;
    gate->closed = false;
    return 0;
}

void trevrpc_lifetime_gate_destroy(trevrpc_lifetime_gate* gate) {
    if (gate == NULL) {
        return;
    }
    pthread_cond_destroy(&gate->cond);
    pthread_mutex_destroy(&gate->mutex);
}

int trevrpc_lifetime_gate_enter(trevrpc_lifetime_gate* gate) {
    if (gate == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&gate->mutex);
    if (gate->closed) {
        pthread_mutex_unlock(&gate->mutex);
        return -EPIPE;
    }
    gate->entered++;
    pthread_mutex_unlock(&gate->mutex);
    return 0;
}

void trevrpc_lifetime_gate_leave(trevrpc_lifetime_gate* gate) {
    if (gate == NULL) {
        return;
    }
    pthread_mutex_lock(&gate->mutex);
    if (gate->entered > 0) {
        gate->entered--;
    }
    if (gate->closed && gate->entered == 0) {
        pthread_cond_broadcast(&gate->cond);
    }
    pthread_mutex_unlock(&gate->mutex);
}

void trevrpc_lifetime_gate_open(trevrpc_lifetime_gate* gate) {
    if (gate == NULL) {
        return;
    }
    pthread_mutex_lock(&gate->mutex);
    if (gate->entered == 0) {
        gate->closed = false;
    }
    pthread_mutex_unlock(&gate->mutex);
}

void trevrpc_lifetime_gate_close(trevrpc_lifetime_gate* gate) {
    if (gate == NULL) {
        return;
    }
    pthread_mutex_lock(&gate->mutex);
    gate->closed = true;
    pthread_mutex_unlock(&gate->mutex);
}

void trevrpc_lifetime_gate_wait_closed(trevrpc_lifetime_gate* gate) {
    if (gate == NULL) {
        return;
    }
    pthread_mutex_lock(&gate->mutex);
    while (gate->entered > 0) {
        pthread_cond_wait(&gate->cond, &gate->mutex);
    }
    pthread_mutex_unlock(&gate->mutex);
}

void trevrpc_lifetime_gate_close_and_wait(trevrpc_lifetime_gate* gate) {
    trevrpc_lifetime_gate_close(gate);
    trevrpc_lifetime_gate_wait_closed(gate);
}
