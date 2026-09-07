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

#define TREVRPC_STATUS_OK 0u
#define TREVRPC_STATUS_CANCELLED 1u
#define TREVRPC_STATUS_UNKNOWN 2u
#define TREVRPC_STATUS_INVALID_ARGUMENT 3u
#define TREVRPC_STATUS_DEADLINE_EXCEEDED 4u
#define TREVRPC_STATUS_NOT_FOUND 5u
#define TREVRPC_STATUS_ALREADY_EXISTS 6u
#define TREVRPC_STATUS_PERMISSION_DENIED 7u
#define TREVRPC_STATUS_RESOURCE_EXHAUSTED 8u
#define TREVRPC_STATUS_FAILED_PRECONDITION 9u
#define TREVRPC_STATUS_ABORTED 10u
#define TREVRPC_STATUS_OUT_OF_RANGE 11u
#define TREVRPC_STATUS_UNIMPLEMENTED 12u
#define TREVRPC_STATUS_INTERNAL 13u
#define TREVRPC_STATUS_UNAVAILABLE 14u
#define TREVRPC_STATUS_DATA_LOSS 15u
#define TREVRPC_STATUS_UNAUTHENTICATED 16u

#define TREVRPC_RPC_KIND_UNARY 0u
#define TREVRPC_RPC_KIND_CLIENT_STREAMING 1u
#define TREVRPC_RPC_KIND_SERVER_STREAMING 2u
#define TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING 3u

#define TREVRPC_STREAM_FRAME_KIND_MESSAGE 0u
#define TREVRPC_STREAM_FRAME_KIND_STATUS 1u

#define TREVRPC_ERR_INVALID_FRAME -2001
#define TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION -2002
#define TREVRPC_ERR_UNSUPPORTED_RPC_KIND -2003
#define TREVRPC_ERR_HANDLER_FAILED -2004
#define TREVRPC_ERR_FRAME_TOO_LARGE -2005
#define TREVRPC_ERR_STREAM_LIMIT_EXCEEDED -2006
#define TREVRPC_ERR_STREAM_IDLE_TIMEOUT -2007

#ifndef TREVRPC_VALUE_TYPES_DEFINED
#define TREVRPC_VALUE_TYPES_DEFINED

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

typedef struct trevrpc_status {
    uint32_t code;
    const char* message;
    size_t message_len;
} trevrpc_status;

typedef struct trevrpc_request {
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
} trevrpc_request;

#endif

#ifndef TREVRPC_INBOUND_VALUE_TYPES_DEFINED
#define TREVRPC_INBOUND_VALUE_TYPES_DEFINED
typedef struct trevrpc_inbound_response trevrpc_inbound_response;
typedef struct trevrpc_inbound_stream_frame trevrpc_inbound_stream_frame;
#endif

#ifndef TREVRPC_CALL_CONTEXT_TYPE_DEFINED
#define TREVRPC_CALL_CONTEXT_TYPE_DEFINED
typedef struct trevrpc_call_context trevrpc_call_context;
#endif

#ifndef TREVRPC_AUTHORIZER_TYPES_DEFINED
#define TREVRPC_AUTHORIZER_TYPES_DEFINED

typedef struct trevrpc_metadata_value_authorizer {
    const char* key;
    size_t key_len;
    const uint8_t* value;
    size_t value_len;
} trevrpc_metadata_value_authorizer;

typedef struct trevrpc_bearer_authorizer {
    const char* token;
    size_t token_len;
} trevrpc_bearer_authorizer;

#endif

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

int trevrpc_metadata_set(
    trevrpc_metadata* metadata, const char* key, size_t key_len, const uint8_t* value, size_t value_len);
int trevrpc_metadata_validate(const trevrpc_metadata* metadata);
void trevrpc_metadata_reset(trevrpc_metadata* metadata);
void trevrpc_request_reset(trevrpc_request* request);

#endif
