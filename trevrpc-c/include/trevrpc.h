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

#define TREVRPC_TRANSPORT_KIND_MSQUIC 0u
#define TREVRPC_TRANSPORT_KIND_WEBTRANSPORT 1u

#define TREVRPC_TRANSPORT_EVENT_LISTENER_OPEN 0u
#define TREVRPC_TRANSPORT_EVENT_LISTENER_CLOSE 1u
#define TREVRPC_TRANSPORT_EVENT_LISTENER_ERROR 2u
#define TREVRPC_TRANSPORT_EVENT_CONNECTION_OPEN 3u
#define TREVRPC_TRANSPORT_EVENT_CONNECTION_CLOSE 4u
#define TREVRPC_TRANSPORT_EVENT_CONNECTION_ERROR 5u
#define TREVRPC_TRANSPORT_EVENT_STREAM_OPEN 6u
#define TREVRPC_TRANSPORT_EVENT_STREAM_CLOSE 7u
#define TREVRPC_TRANSPORT_EVENT_STREAM_ERROR 8u

#define TREVRPC_LOG_LEVEL_DEBUG 0u
#define TREVRPC_LOG_LEVEL_INFO 1u
#define TREVRPC_LOG_LEVEL_WARN 2u
#define TREVRPC_LOG_LEVEL_ERROR 3u

#define TREVRPC_ERR_INVALID_FRAME -2001
#define TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION -2002
#define TREVRPC_ERR_UNSUPPORTED_RPC_KIND -2003
#define TREVRPC_ERR_HANDLER_FAILED -2004
#define TREVRPC_ERR_FRAME_TOO_LARGE -2005
#define TREVRPC_ERR_STREAM_LIMIT_EXCEEDED -2006
#define TREVRPC_ERR_STREAM_IDLE_TIMEOUT -2007

typedef struct trevrpc_client trevrpc_client;
typedef struct trevrpc_server trevrpc_server;
typedef struct trevrpc_stream trevrpc_stream;
typedef struct trevrpc_call trevrpc_call;
typedef struct trevrpc_call_context trevrpc_call_context;
typedef struct trevrpc_wt_config trevrpc_wt_config;

#define TREVRPC_CALL_DEFERRED 1

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

typedef struct trevrpc_server_config {
    const char* host;
    uint16_t port;
    const char* cert_file;
    const char* key_file;
    const char* webtransport_path;
    const char* webtransport_origin;
    uint64_t max_idle_timeout_ms;
    uint32_t keep_alive_ms;
    uint16_t peer_bidi_stream_count;
    uint32_t max_stateless_operations;
    uint16_t max_binding_stateless_operations;
    uint32_t max_sessions_per_connection;
    uint32_t max_streams_per_session;
    uint64_t max_data_per_session;
    uint32_t stream_recv_window;
    uint32_t conn_flow_control_window;
    uint32_t handshake_timeout_ms;
    size_t max_frame_size;
} trevrpc_server_config;

typedef struct trevrpc_server_options {
    int64_t max_concurrent_connections;
    int64_t max_concurrent_streams_per_connection;
    int64_t max_concurrent_requests;
    uint64_t graceful_shutdown_timeout_nanos;
    uint64_t initial_request_timeout_nanos;
    int64_t max_stream_messages;
    int64_t max_stream_body_size;
    uint64_t stream_idle_timeout_nanos;
} trevrpc_server_options;

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

typedef struct trevrpc_response {
    uint32_t status;
    char* message;
    size_t message_len;
    uint8_t* body;
    size_t body_len;
    trevrpc_metadata metadata;
} trevrpc_response;

typedef struct trevrpc_stream_frame {
    uint32_t kind;
    uint32_t status;
    char* message;
    size_t message_len;
    uint8_t* body;
    size_t body_len;
    trevrpc_metadata metadata;
} trevrpc_stream_frame;

typedef int (*trevrpc_unary_handler)(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_response* response);
typedef int (*trevrpc_stream_handler)(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_stream* stream);
/*
 * Binding-oriented handler surface. Handlers must complete the call with
 * trevrpc_call_respond, trevrpc_call_finish_stream, or trevrpc_call_close.
 * Return TREVRPC_CALL_DEFERRED or call trevrpc_call_defer before handing the
 * call to another runtime thread.
 */
typedef int (*trevrpc_call_handler)(void* user_data, trevrpc_call* call);
typedef int (*trevrpc_authorizer)(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_status* status);

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

typedef struct trevrpc_rpc_started_event {
    const char* service;
    size_t service_len;
    const char* method;
    size_t method_len;
    size_t request_body_len;
} trevrpc_rpc_started_event;

typedef struct trevrpc_rpc_finished_event {
    const char* service;
    size_t service_len;
    const char* method;
    size_t method_len;
    size_t request_body_len;
    size_t response_body_len;
    uint32_t status;
    uint64_t elapsed_nanos;
} trevrpc_rpc_finished_event;

typedef void (*trevrpc_rpc_started_callback)(void* user_data, const trevrpc_rpc_started_event* event);
typedef void (*trevrpc_rpc_finished_callback)(void* user_data, const trevrpc_rpc_finished_event* event);

typedef struct trevrpc_metrics {
    trevrpc_rpc_started_callback rpc_started;
    trevrpc_rpc_finished_callback rpc_finished;
    void* user_data;
} trevrpc_metrics;

typedef struct trevrpc_transport_event {
    uint32_t kind;
    uint32_t transport;
    int error_code;
    const char* message;
    size_t message_len;
} trevrpc_transport_event;

typedef void (*trevrpc_transport_event_callback)(void* user_data, const trevrpc_transport_event* event);

typedef struct trevrpc_transport_observer {
    trevrpc_transport_event_callback transport_event;
    void* user_data;
} trevrpc_transport_observer;

typedef struct trevrpc_log_event {
    uint32_t level;
    const char* event;
    size_t event_len;
    const char* message;
    size_t message_len;
    const char* service;
    size_t service_len;
    const char* method;
    size_t method_len;
    int error_code;
} trevrpc_log_event;

typedef void (*trevrpc_log_callback)(void* user_data, const trevrpc_log_event* event);

typedef struct trevrpc_logger {
    trevrpc_log_callback log;
    void* user_data;
} trevrpc_logger;

trevrpc_config trevrpc_default_config(void);
trevrpc_server_config trevrpc_default_server_config(void);
trevrpc_server_options trevrpc_default_server_options(void);

int trevrpc_call_context_has_deadline(const trevrpc_call_context* context);
int trevrpc_call_context_deadline_expired(const trevrpc_call_context* context);
int trevrpc_call_context_cancelled(const trevrpc_call_context* context);
int trevrpc_call_context_time_remaining_nanos(const trevrpc_call_context* context, uint64_t* remaining_nanos);

uint32_t trevrpc_status_code_from_uint32(uint32_t code);
const char* trevrpc_status_code_string(uint32_t code);
trevrpc_status trevrpc_status_new(uint32_t code, const char* message, size_t message_len);
trevrpc_status trevrpc_status_ok(void);
trevrpc_status trevrpc_status_cancelled(const char* message, size_t message_len);
trevrpc_status trevrpc_status_unknown(const char* message, size_t message_len);
trevrpc_status trevrpc_status_invalid_argument(const char* message, size_t message_len);
trevrpc_status trevrpc_status_deadline_exceeded(const char* message, size_t message_len);
trevrpc_status trevrpc_status_not_found(const char* message, size_t message_len);
trevrpc_status trevrpc_status_already_exists(const char* message, size_t message_len);
trevrpc_status trevrpc_status_permission_denied(const char* message, size_t message_len);
trevrpc_status trevrpc_status_resource_exhausted(const char* message, size_t message_len);
trevrpc_status trevrpc_status_failed_precondition(const char* message, size_t message_len);
trevrpc_status trevrpc_status_aborted(const char* message, size_t message_len);
trevrpc_status trevrpc_status_out_of_range(const char* message, size_t message_len);
trevrpc_status trevrpc_status_unimplemented(const char* message, size_t message_len);
trevrpc_status trevrpc_status_internal(const char* message, size_t message_len);
trevrpc_status trevrpc_status_unavailable(const char* message, size_t message_len);
trevrpc_status trevrpc_status_data_loss(const char* message, size_t message_len);
trevrpc_status trevrpc_status_unauthenticated(const char* message, size_t message_len);

int trevrpc_metadata_set(
    trevrpc_metadata* metadata, const char* key, size_t key_len, const uint8_t* value, size_t value_len);
int trevrpc_metadata_validate(const trevrpc_metadata* metadata);
void trevrpc_metadata_reset(trevrpc_metadata* metadata);
int trevrpc_authorize_metadata_value(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_status* status);
int trevrpc_authorize_bearer_token(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_status* status);

int trevrpc_client_connect(const char* host, uint16_t port, const trevrpc_config* config, trevrpc_client** client);
int trevrpc_client_connect_webtransport(
    const trevrpc_wt_config* wt_config, const trevrpc_config* config, trevrpc_client** client);
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
void trevrpc_client_shutdown(trevrpc_client* client);
void trevrpc_client_close(trevrpc_client* client);

void trevrpc_request_reset(trevrpc_request* request);

int trevrpc_server_listen(const trevrpc_server_config* config, trevrpc_server** server);
int trevrpc_server_port(trevrpc_server* server, uint16_t* port);
int trevrpc_server_set_options(trevrpc_server* server, const trevrpc_server_options* options);
int trevrpc_server_get_options(trevrpc_server* server, trevrpc_server_options* options);
int trevrpc_server_set_authorizer(trevrpc_server* server, trevrpc_authorizer authorizer, void* user_data);
void trevrpc_server_clear_authorizer(trevrpc_server* server);
int trevrpc_server_set_metrics(trevrpc_server* server, const trevrpc_metrics* metrics);
void trevrpc_server_clear_metrics(trevrpc_server* server);
int trevrpc_server_set_transport_observer(trevrpc_server* server, const trevrpc_transport_observer* observer);
void trevrpc_server_clear_transport_observer(trevrpc_server* server);
int trevrpc_server_set_logger(trevrpc_server* server, const trevrpc_logger* logger);
void trevrpc_server_clear_logger(trevrpc_server* server);
int trevrpc_server_register_unary(
    trevrpc_server* server, const char* service, const char* method, trevrpc_unary_handler handler, void* user_data);
int trevrpc_server_register_streaming(trevrpc_server* server,
    const char* service,
    const char* method,
    uint32_t kind,
    trevrpc_stream_handler handler,
    void* user_data);
int trevrpc_server_register_call(trevrpc_server* server,
    const char* service,
    const char* method,
    uint32_t kind,
    trevrpc_call_handler handler,
    void* user_data);
int trevrpc_server_serve(trevrpc_server* server);
void trevrpc_server_shutdown(trevrpc_server* server);
void trevrpc_server_close(trevrpc_server* server);

const trevrpc_request* trevrpc_call_request(const trevrpc_call* call);
const trevrpc_call_context* trevrpc_call_get_context(const trevrpc_call* call);
trevrpc_stream* trevrpc_call_stream(trevrpc_call* call);
/* Keeps call alive after the handler returns; completion APIs release it. */
int trevrpc_call_defer(trevrpc_call* call);
int trevrpc_call_respond(trevrpc_call* call, trevrpc_response* response);
int trevrpc_call_finish_stream(trevrpc_call* call, uint32_t status, const char* message, size_t message_len);
void trevrpc_call_close(trevrpc_call* call);

int trevrpc_response_set_message(trevrpc_response* response, const char* message, size_t message_len);
int trevrpc_response_set_body(trevrpc_response* response, const uint8_t* body, size_t body_len);
int trevrpc_response_set_status(trevrpc_response* response, trevrpc_status status);
void trevrpc_response_reset(trevrpc_response* response);
void trevrpc_response_free(trevrpc_response* response);

int trevrpc_stream_send_message(trevrpc_stream* stream, const uint8_t* body, size_t body_len);
int trevrpc_stream_send_messages(trevrpc_stream* stream, const uint8_t* bodies, const size_t* body_lens, size_t count);
int trevrpc_stream_send_status(trevrpc_stream* stream, uint32_t status, const char* message, size_t message_len);
int trevrpc_stream_recv(trevrpc_stream* stream, trevrpc_stream_frame** frame);
int trevrpc_stream_recv_ready(trevrpc_stream* stream, trevrpc_stream_frame** frame, int* ready);
int trevrpc_stream_finish_send(trevrpc_stream* stream);
void trevrpc_stream_cancel(trevrpc_stream* stream);
void trevrpc_stream_close(trevrpc_stream* stream);

int trevrpc_stream_frame_set_message(trevrpc_stream_frame* frame, const char* message, size_t message_len);
int trevrpc_stream_frame_set_body(trevrpc_stream_frame* frame, const uint8_t* body, size_t body_len);
int trevrpc_stream_frame_set_status(trevrpc_stream_frame* frame, trevrpc_status status);
void trevrpc_stream_frame_reset(trevrpc_stream_frame* frame);
void trevrpc_stream_frame_free(trevrpc_stream_frame* frame);

const char* trevrpc_error(int code);

#ifdef __cplusplus
}
#endif

#endif
