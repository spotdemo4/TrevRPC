#ifndef TREVRPC_OWNERSHIP_ABI6_INTERNAL_H
#define TREVRPC_OWNERSHIP_ABI6_INTERNAL_H

#include "trevrpc_owned_bytes_internal.h"

#ifndef TREVRPC_INBOUND_VALUE_TYPES_DEFINED
#define TREVRPC_INBOUND_VALUE_TYPES_DEFINED
typedef struct trevrpc_inbound_response trevrpc_inbound_response;
typedef struct trevrpc_inbound_stream_frame trevrpc_inbound_stream_frame;
#endif

#ifndef TREVRPC_OWNERSHIP_TYPES_DEFINED
#define TREVRPC_OWNERSHIP_TYPES_DEFINED
typedef struct trevrpc_body_owner trevrpc_body_owner;
typedef struct trevrpc_bytes_view {
    const uint8_t* data;
    size_t len;
} trevrpc_bytes_view;
#endif

int trevrpc_inbound_response_create(trevrpc_wire_response_values* values, trevrpc_inbound_response** inbound);
int trevrpc_inbound_stream_frame_create(
    trevrpc_wire_stream_frame_values* values, trevrpc_inbound_stream_frame** inbound);

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
