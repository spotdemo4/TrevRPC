#ifndef TREVRPC_OWNED_BYTES_INTERNAL_H
#define TREVRPC_OWNED_BYTES_INTERNAL_H

#include "trevrpc_values_internal.h"

static inline void trevrpc_owned_bytes_init(trevrpc_owned_bytes* bytes) {
    if (bytes == NULL) {
        return;
    }
    bytes->data = NULL;
    bytes->len = 0;
    bytes->owner = NULL;
    bytes->release = NULL;
    bytes->release_context = NULL;
}

static inline void trevrpc_owned_bytes_reset(trevrpc_owned_bytes* bytes) {
    if (bytes == NULL) {
        return;
    }
    if (bytes->release != NULL) {
        bytes->release(bytes->owner, bytes->release_context);
    }
    trevrpc_owned_bytes_init(bytes);
}

static inline void trevrpc_owned_bytes_move(trevrpc_owned_bytes* destination, trevrpc_owned_bytes* source) {
    if (destination == NULL || source == NULL || destination == source) {
        return;
    }
    *destination = *source;
    trevrpc_owned_bytes_init(source);
}

#endif
