#ifndef TREVRPC_OWNED_BYTES_INTERNAL_H
#define TREVRPC_OWNED_BYTES_INTERNAL_H

#include "trevrpc_values_internal.h"

#ifndef TREVRPC_OWNERSHIP_TYPES_DEFINED
#define TREVRPC_OWNERSHIP_TYPES_DEFINED

typedef struct trevrpc_body_owner trevrpc_body_owner;

typedef struct trevrpc_bytes_view {
    const uint8_t* data;
    size_t len;
} trevrpc_bytes_view;

#endif

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

#ifndef TREVRPC_OWNERSHIP_INTERNAL_FUNCTIONS_DEFINED
#define TREVRPC_OWNERSHIP_INTERNAL_FUNCTIONS_DEFINED

int trevrpc_inbound_response_create(trevrpc_wire_response_values* values, trevrpc_inbound_response** inbound);
int trevrpc_inbound_stream_frame_create(
    trevrpc_wire_stream_frame_values* values, trevrpc_inbound_stream_frame** inbound);

#endif

#ifndef TREVRPC_OWNERSHIP_FUNCTIONS_DEFINED
#define TREVRPC_OWNERSHIP_FUNCTIONS_DEFINED

int trevrpc_inbound_response_get_status(const trevrpc_inbound_response* response, uint32_t* status);
int trevrpc_inbound_response_get_message(const trevrpc_inbound_response* response, trevrpc_bytes_view* message);
int trevrpc_inbound_response_get_body(const trevrpc_inbound_response* response, trevrpc_bytes_view* body);
size_t trevrpc_inbound_response_metadata_count(const trevrpc_inbound_response* response);
int trevrpc_inbound_response_metadata_at(
    const trevrpc_inbound_response* response, size_t index, trevrpc_bytes_view* key, trevrpc_bytes_view* value);
int trevrpc_inbound_response_take_body(trevrpc_inbound_response* response, trevrpc_body_owner** owner);
void trevrpc_inbound_response_release(trevrpc_inbound_response* response);

int trevrpc_inbound_stream_frame_get_kind(const trevrpc_inbound_stream_frame* frame, uint32_t* kind);
int trevrpc_inbound_stream_frame_get_status(const trevrpc_inbound_stream_frame* frame, uint32_t* status);
int trevrpc_inbound_stream_frame_get_message(const trevrpc_inbound_stream_frame* frame, trevrpc_bytes_view* message);
int trevrpc_inbound_stream_frame_get_body(const trevrpc_inbound_stream_frame* frame, trevrpc_bytes_view* body);
size_t trevrpc_inbound_stream_frame_metadata_count(const trevrpc_inbound_stream_frame* frame);
int trevrpc_inbound_stream_frame_metadata_at(
    const trevrpc_inbound_stream_frame* frame, size_t index, trevrpc_bytes_view* key, trevrpc_bytes_view* value);
int trevrpc_inbound_stream_frame_take_body(trevrpc_inbound_stream_frame* frame, trevrpc_body_owner** owner);
void trevrpc_inbound_stream_frame_release(trevrpc_inbound_stream_frame* frame);

int trevrpc_body_owner_get_view(const trevrpc_body_owner* owner, trevrpc_bytes_view* body);
void trevrpc_body_owner_release(trevrpc_body_owner* owner);

#endif

#endif
