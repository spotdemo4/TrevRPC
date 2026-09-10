#ifndef TREVRPC_VALUES_INTERNAL_H
#define TREVRPC_VALUES_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

#define TREVRPC_WIRE_VERSION 1u

#define TREVRPC_MAX_METADATA_ENTRIES 64u
#define TREVRPC_MAX_METADATA_KEY_LEN 128u
#define TREVRPC_MAX_METADATA_VALUE_LEN (8u * 1024u)
#define TREVRPC_MAX_METADATA_TOTAL_SIZE (64u * 1024u)
#define TREVRPC_RESERVED_METADATA_PREFIX "trevrpc-"

#define TREVRPC_WIRE_STATUS_OK 0u

#define TREVRPC_RPC_KIND_UNARY 0u
#define TREVRPC_RPC_KIND_CLIENT_STREAMING 1u
#define TREVRPC_RPC_KIND_SERVER_STREAMING 2u
#define TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING 3u

#define TREVRPC_STREAM_FRAME_KIND_MESSAGE 0u
#define TREVRPC_STREAM_FRAME_KIND_STATUS 1u

#define TREVRPC_ERR_INVALID_FRAME -2001
#define TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION -2002
#define TREVRPC_ERR_UNSUPPORTED_RPC_KIND -2003
#define TREVRPC_ERR_FRAME_TOO_LARGE -2005

typedef struct trevrpc_metadata_entry {
    char* key;
    size_t key_len;
    uint8_t* value;
    size_t value_len;
} trevrpc_metadata_entry;

typedef struct trevrpc_metadata {
    trevrpc_metadata_entry* entries;
    size_t entries_len;
} trevrpc_metadata;

typedef struct trevrpc_wire_request_values {
    const char* service;
    size_t service_len;
    const char* method;
    size_t method_len;
    const uint8_t* body;
    size_t body_len;
    trevrpc_metadata metadata;
    uint32_t kind;
    uint32_t version;
    uint64_t timeout_nanos;
} trevrpc_wire_request_values;

typedef void (*trevrpc_owned_bytes_release_fn)(void* owner, void* context);

typedef struct trevrpc_owned_bytes {
    const uint8_t* data;
    size_t len;
    void* owner;
    trevrpc_owned_bytes_release_fn release;
    void* release_context;
} trevrpc_owned_bytes;

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

int trevrpc_internal_metadata_set(
    trevrpc_metadata* metadata, const char* key, size_t key_len, const uint8_t* value, size_t value_len);
int trevrpc_internal_metadata_validate(const trevrpc_metadata* metadata);
void trevrpc_internal_metadata_reset(trevrpc_metadata* metadata);
void trevrpc_internal_request_reset(trevrpc_wire_request_values* request);
int trevrpc_internal_response_set_message(
    trevrpc_wire_response_values* response, const char* message, size_t message_len);
int trevrpc_internal_response_set_body(trevrpc_wire_response_values* response, const uint8_t* body, size_t body_len);
int trevrpc_internal_stream_frame_set_message(
    trevrpc_wire_stream_frame_values* frame, const char* message, size_t message_len);
int trevrpc_internal_stream_frame_set_body(
    trevrpc_wire_stream_frame_values* frame, const uint8_t* body, size_t body_len);

#endif
