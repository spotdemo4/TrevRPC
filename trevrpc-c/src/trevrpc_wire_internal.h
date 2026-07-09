#ifndef TREVRPC_WIRE_INTERNAL_H
#define TREVRPC_WIRE_INTERNAL_H

#include "trevrpc.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef struct trevrpc_wire_frame_parts {
    uint8_t* prefix;
    size_t prefix_len;
    const uint8_t* body;
    size_t body_len;
    uint8_t* suffix;
    size_t suffix_len;
    size_t frame_body_len;
} trevrpc_wire_frame_parts;

int trevrpc_wire_encode_request(const char* service,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len,
    const trevrpc_metadata* metadata,
    uint64_t timeout_nanos,
    size_t max_frame_size,
    uint8_t** frame,
    size_t* frame_len);
int trevrpc_wire_encode_request_view(const char* service,
    size_t service_len,
    const char* method,
    size_t method_len,
    uint32_t kind,
    uint32_t version,
    const uint8_t* body,
    size_t body_len,
    const trevrpc_metadata* metadata,
    uint64_t timeout_nanos,
    size_t max_frame_size,
    uint8_t** frame,
    size_t* frame_len);
int trevrpc_wire_encode_response(
    const trevrpc_response* response, size_t max_frame_size, uint8_t** frame, size_t* frame_len);
int trevrpc_wire_encode_response_parts(
    const trevrpc_response* response, size_t max_frame_size, trevrpc_wire_frame_parts* parts);
int trevrpc_wire_encode_stream_frame(uint32_t kind,
    uint32_t status,
    const char* message,
    size_t message_len,
    const uint8_t* body,
    size_t body_len,
    const trevrpc_metadata* metadata,
    size_t max_frame_size,
    uint8_t** frame,
    size_t* frame_len);
int trevrpc_wire_encode_stream_status_parts(uint32_t status,
    const char* message,
    size_t message_len,
    const trevrpc_metadata* metadata,
    size_t max_frame_size,
    trevrpc_wire_frame_parts* parts);
int trevrpc_wire_encode_stream_message_parts(
    const uint8_t* body, size_t body_len, size_t max_frame_size, trevrpc_wire_frame_parts* parts);
void trevrpc_wire_frame_parts_reset(trevrpc_wire_frame_parts* parts);

int trevrpc_wire_decode_request(const uint8_t* data, size_t len, trevrpc_request* request);
int trevrpc_wire_decode_response(const uint8_t* data, size_t len, trevrpc_response** out_response);
int trevrpc_wire_decode_response_take(uint8_t* data, size_t len, trevrpc_response** out_response, bool* took_body);
int trevrpc_wire_decode_stream_frame(const uint8_t* data, size_t len, trevrpc_stream_frame** out_frame);
int trevrpc_wire_decode_stream_frame_take(uint8_t* data, size_t len, trevrpc_stream_frame** out_frame, bool* took_body);

#endif
