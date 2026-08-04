#ifndef TREVRPC_WIRE_INTERNAL_H
#define TREVRPC_WIRE_INTERNAL_H

#include "trevrpc.h"
#include "trevrpc_owned_bytes_internal.h"

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

typedef enum trevrpc_wire_diagnostic_reason {
    TREVRPC_WIRE_DIAGNOSTIC_NONE = 0,
    TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF = 1,
    TREVRPC_WIRE_DIAGNOSTIC_WRONG_WIRE_TYPE = 2,
    TREVRPC_WIRE_DIAGNOSTIC_INVALID_UTF8 = 3,
    TREVRPC_WIRE_DIAGNOSTIC_UINT32_OVERFLOW = 4,
    TREVRPC_WIRE_DIAGNOSTIC_INVALID_METADATA = 5,
    TREVRPC_WIRE_DIAGNOSTIC_UNSUPPORTED_FRAME_KIND = 6,
    TREVRPC_WIRE_DIAGNOSTIC_ALLOCATION_FAILURE = 7,
} trevrpc_wire_diagnostic_reason;

typedef struct trevrpc_wire_request_diagnostic {
    uint32_t response_kind;
    trevrpc_wire_diagnostic_reason reason;
} trevrpc_wire_request_diagnostic;

typedef struct trevrpc_wire_diagnostic {
    trevrpc_wire_diagnostic_reason reason;
} trevrpc_wire_diagnostic;

#define TREVRPC_WIRE_REQUEST_KIND_UNKNOWN UINT32_MAX

int trevrpc_internal_response_set_message(
    trevrpc_wire_response_values* response, const char* message, size_t message_len);
int trevrpc_internal_response_set_body(trevrpc_wire_response_values* response, const uint8_t* body, size_t body_len);
int trevrpc_internal_response_set_status(trevrpc_wire_response_values* response, trevrpc_status status);
void trevrpc_internal_response_reset(trevrpc_wire_response_values* response);
void trevrpc_internal_response_free(trevrpc_wire_response_values* response);
int trevrpc_internal_stream_frame_set_message(
    trevrpc_wire_stream_frame_values* frame, const char* message, size_t message_len);
int trevrpc_internal_stream_frame_set_body(
    trevrpc_wire_stream_frame_values* frame, const uint8_t* body, size_t body_len);
int trevrpc_internal_stream_frame_set_status(trevrpc_wire_stream_frame_values* frame, trevrpc_status status);
void trevrpc_internal_stream_frame_reset(trevrpc_wire_stream_frame_values* frame);
void trevrpc_internal_stream_frame_free(trevrpc_wire_stream_frame_values* frame);

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
int trevrpc_wire_encode_request_parts(
    const trevrpc_request* request, size_t max_frame_size, trevrpc_wire_frame_parts* parts);
int trevrpc_wire_encode_response(
    const trevrpc_wire_response_values* response, size_t max_frame_size, uint8_t** frame, size_t* frame_len);
int trevrpc_wire_encode_response_parts(
    const trevrpc_wire_response_values* response, size_t max_frame_size, trevrpc_wire_frame_parts* parts);
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

int trevrpc_wire_decode_request_diagnostic(
    const uint8_t* data, size_t len, trevrpc_request* request, trevrpc_wire_request_diagnostic* diagnostic);
int trevrpc_wire_decode_request(const uint8_t* data, size_t len, trevrpc_request* request);
int trevrpc_wire_decode_response_diagnostic(
    const uint8_t* data, size_t len, trevrpc_wire_response_values** out_response, trevrpc_wire_diagnostic* diagnostic);
int trevrpc_wire_decode_response(const uint8_t* data, size_t len, trevrpc_wire_response_values** out_response);
int trevrpc_wire_decode_response_take(
    uint8_t* data, size_t len, trevrpc_wire_response_values** out_response, bool* took_body);
int trevrpc_wire_decode_response_owned(trevrpc_owned_bytes* data, trevrpc_inbound_response** out_response);
int trevrpc_wire_decode_stream_frame_diagnostic(
    const uint8_t* data, size_t len, trevrpc_wire_stream_frame_values** out_frame, trevrpc_wire_diagnostic* diagnostic);
int trevrpc_wire_decode_stream_frame(const uint8_t* data, size_t len, trevrpc_wire_stream_frame_values** out_frame);
int trevrpc_wire_decode_stream_frame_take(
    uint8_t* data, size_t len, trevrpc_wire_stream_frame_values** out_frame, bool* took_body);
int trevrpc_wire_decode_stream_frame_owned(trevrpc_owned_bytes* data, trevrpc_inbound_stream_frame** out_frame);
int trevrpc_wire_decode_stream_frame_owned_diagnostic(
    trevrpc_owned_bytes* data, trevrpc_inbound_stream_frame** out_frame, trevrpc_wire_diagnostic* diagnostic);
int trevrpc_wire_canonicalize_bytes_field(
    const uint8_t* data, size_t len, uint32_t field_number, uint8_t** canonical, size_t* canonical_len);

#endif
