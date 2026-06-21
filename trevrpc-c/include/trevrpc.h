#ifndef TREVRPC_H
#define TREVRPC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TREVRPC_ALPN "trevrpc/1"
#define TREVRPC_WIRE_VERSION 1u
#define TREVRPC_DEFAULT_MAX_FRAME_SIZE (4u * 1024u * 1024u)

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

typedef struct trevrpc_client trevrpc_client;
typedef struct trevrpc_server trevrpc_server;
typedef struct trevrpc_stream trevrpc_stream;

typedef struct trevrpc_config {
    const char* cert_file;
    const char* key_file;
    uint64_t max_idle_timeout_ms;
    uint32_t keep_alive_ms;
    uint16_t peer_bidi_stream_count;
    uint32_t max_stateless_operations;
    uint16_t max_binding_stateless_operations;
    size_t max_frame_size;
} trevrpc_config;

typedef struct trevrpc_request {
    const char* service;
    size_t service_len;
    const char* method;
    size_t method_len;
    const uint8_t* body;
    size_t body_len;
    uint32_t kind;
    uint32_t version;
    uint64_t timeout_nanos;
} trevrpc_request;

typedef struct trevrpc_response {
    uint32_t status;
    char* message;
    size_t message_len;
    uint8_t* body;
    size_t body_len;
} trevrpc_response;

typedef struct trevrpc_stream_frame {
    uint32_t kind;
    uint32_t status;
    char* message;
    size_t message_len;
    uint8_t* body;
    size_t body_len;
} trevrpc_stream_frame;

typedef int (*trevrpc_unary_handler)(void* user_data, const trevrpc_request* request, trevrpc_response* response);
typedef int (*trevrpc_stream_handler)(void* user_data, const trevrpc_request* request, trevrpc_stream* stream);

trevrpc_config trevrpc_default_config(void);

int trevrpc_client_connect(const char* host, uint16_t port, const trevrpc_config* config, trevrpc_client** client);
int trevrpc_client_call_unary(trevrpc_client* client,
    const char* service,
    const char* method,
    const uint8_t* body,
    size_t body_len,
    trevrpc_response** response);
int trevrpc_client_start_stream(trevrpc_client* client,
    const char* service,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len,
    trevrpc_stream** stream);
void trevrpc_client_close(trevrpc_client* client);

int trevrpc_server_listen(const char* host, uint16_t port, const trevrpc_config* config, trevrpc_server** server);
int trevrpc_server_register_unary(
    trevrpc_server* server, const char* service, const char* method, trevrpc_unary_handler handler, void* user_data);
int trevrpc_server_register_streaming(trevrpc_server* server,
    const char* service,
    const char* method,
    uint32_t kind,
    trevrpc_stream_handler handler,
    void* user_data);
int trevrpc_server_serve(trevrpc_server* server);
void trevrpc_server_shutdown(trevrpc_server* server);
void trevrpc_server_close(trevrpc_server* server);

int trevrpc_response_set_message(trevrpc_response* response, const char* message, size_t message_len);
int trevrpc_response_set_body(trevrpc_response* response, const uint8_t* body, size_t body_len);
void trevrpc_response_reset(trevrpc_response* response);
void trevrpc_response_free(trevrpc_response* response);

int trevrpc_stream_send_message(trevrpc_stream* stream, const uint8_t* body, size_t body_len);
int trevrpc_stream_send_status(trevrpc_stream* stream, uint32_t status, const char* message, size_t message_len);
int trevrpc_stream_recv(trevrpc_stream* stream, trevrpc_stream_frame** frame);
int trevrpc_stream_finish_send(trevrpc_stream* stream);
void trevrpc_stream_close(trevrpc_stream* stream);

int trevrpc_stream_frame_set_message(trevrpc_stream_frame* frame, const char* message, size_t message_len);
int trevrpc_stream_frame_set_body(trevrpc_stream_frame* frame, const uint8_t* body, size_t body_len);
void trevrpc_stream_frame_reset(trevrpc_stream_frame* frame);
void trevrpc_stream_frame_free(trevrpc_stream_frame* frame);

const char* trevrpc_error(int code);

#ifdef __cplusplus
}
#endif

#endif
