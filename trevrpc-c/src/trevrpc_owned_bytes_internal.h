#ifndef TREVRPC_OWNED_BYTES_INTERNAL_H
#define TREVRPC_OWNED_BYTES_INTERNAL_H

#include "trevrpc.h"

#include <stddef.h>
#include <stdint.h>

typedef void (*trevrpc_owned_bytes_release_fn)(void* owner, void* context);

typedef struct trevrpc_owned_bytes {
    const uint8_t* data;
    size_t len;
    void* owner;
    trevrpc_owned_bytes_release_fn release;
    void* release_context;
} trevrpc_owned_bytes;

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

typedef struct trevrpc_wire_response_values {
    uint32_t status;
    char* message;
    size_t message_len;
    trevrpc_owned_bytes body;
    trevrpc_metadata metadata;
} trevrpc_wire_response_values;

typedef struct trevrpc_wire_stream_frame_values {
    uint32_t kind;
    uint32_t status;
    char* message;
    size_t message_len;
    trevrpc_owned_bytes body;
    trevrpc_metadata metadata;
} trevrpc_wire_stream_frame_values;

int trevrpc_inbound_response_create(trevrpc_wire_response_values* values, trevrpc_inbound_response** inbound);
int trevrpc_inbound_stream_frame_create(
    trevrpc_wire_stream_frame_values* values, trevrpc_inbound_stream_frame** inbound);

#endif
