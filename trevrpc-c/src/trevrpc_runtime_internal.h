#ifndef TREVRPC_RUNTIME_INTERNAL_H
#define TREVRPC_RUNTIME_INTERNAL_H

#include "trevrpc.h"
#include "trevrpc_binding.h"
#include "trevrpc_raw.h"
#include "trevrpc_webtransport.h"

#include "trevrpc_msquic_internal.h"
#include "trevrpc_wire_internal.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct trevrpc_client_config_internal {
    const char* cert_file;
    const char* key_file;
    const char* ca_cert_file;
    int skip_certificate_validation;
    uint64_t max_idle_timeout_ms;
    uint32_t keep_alive_ms;
    uint16_t peer_bidi_stream_count;
    uint32_t max_stateless_operations;
    uint16_t max_binding_stateless_operations;
    size_t max_pending_send_bytes;
    size_t max_pending_send_count;
    size_t max_frame_size;
    uint32_t stream_recv_window;
    uint32_t conn_flow_control_window;
    trevrpc_msquic_execution_profile msquic_execution_profile;
    int msquic_send_buffering_enabled;
} trevrpc_client_config_internal;

typedef struct trevrpc_server_config_internal {
    const char* host;
    uint16_t port;
    const char* cert_file;
    const char* key_file;
    const char* webtransport_path;
    const char* webtransport_origin;
    trevrpc_webtransport_admission webtransport_admission;
    void* webtransport_admission_user_data;
    int enable_http3;
    const char* http3_path;
    trevrpc_http3_admission http3_admission;
    void* http3_admission_user_data;
    uint64_t max_idle_timeout_ms;
    uint32_t keep_alive_ms;
    uint16_t peer_bidi_stream_count;
    uint32_t max_stateless_operations;
    uint16_t max_binding_stateless_operations;
    size_t max_pending_send_bytes;
    size_t max_pending_send_count;
    uint32_t max_sessions_per_connection;
    uint32_t max_streams_per_session;
    uint32_t stream_recv_window;
    uint32_t conn_flow_control_window;
    size_t max_frame_size;
    trevrpc_msquic_execution_profile msquic_execution_profile;
    int msquic_send_buffering_enabled;
} trevrpc_server_config_internal;

typedef struct trevrpc_server_options_internal {
    int64_t max_concurrent_connections;
    int64_t max_concurrent_streams_per_connection;
    int64_t max_concurrent_requests;
    int64_t worker_count;
    int64_t worker_queue_capacity;
    uint64_t graceful_shutdown_timeout_nanos;
    uint64_t initial_request_timeout_nanos;
    int64_t max_stream_messages;
    int64_t max_stream_body_size;
    uint64_t stream_idle_timeout_nanos;
} trevrpc_server_options_internal;

typedef struct trevrpc_call_options_internal {
    const trevrpc_metadata* metadata;
    uint64_t timeout_nanos;
    trevrpc_cancellation* cancellation;
    int64_t max_response_body_size;
    int64_t max_response_messages;
    int64_t max_response_stream_body_size;
    uint64_t response_idle_timeout_nanos;
} trevrpc_call_options_internal;

typedef int (*trevrpc_unary_handler_internal)(void* user_data,
    const trevrpc_call_context* context,
    const trevrpc_request* request,
    trevrpc_wire_response_values* response);

trevrpc_client_config_internal trevrpc_internal_default_config(void);
trevrpc_server_config_internal trevrpc_internal_default_server_config(void);
trevrpc_server_options_internal trevrpc_internal_default_server_options(void);
trevrpc_call_options_internal trevrpc_internal_default_call_options(void);

int trevrpc_internal_channel_connect(const char* host,
    uint16_t port,
    const trevrpc_client_config_internal* config,
    const trevrpc_channel_options* options,
    uint64_t timeout_nanos,
    trevrpc_cancellation* cancellation,
    trevrpc_channel** channel);
int trevrpc_internal_raw_client_connect(
    const char* host, uint16_t port, const trevrpc_client_config_internal* config, trevrpc_raw_client** client);
int trevrpc_internal_raw_client_connect_cancellable(const char* host,
    uint16_t port,
    const trevrpc_client_config_internal* config,
    trevrpc_cancellation* cancellation,
    trevrpc_raw_client** client);
int trevrpc_internal_raw_client_connect_webtransport(
    const trevrpc_wt_config* wt_config, const trevrpc_client_config_internal* config, trevrpc_raw_client** client);
int trevrpc_internal_raw_client_connect_cancellable_with_shutdown_callback(const char* host,
    uint16_t port,
    const trevrpc_client_config_internal* config,
    trevrpc_cancellation* cancellation,
    trevrpc_raw_client_shutdown_callback callback,
    void* user_data,
    trevrpc_raw_client** client);
int trevrpc_internal_raw_client_call_unary(trevrpc_raw_client* client,
    const char* service,
    const char* method,
    const uint8_t* body,
    size_t body_len,
    trevrpc_wire_response_values** response);
int trevrpc_internal_raw_client_call_unary_with_options(trevrpc_raw_client* client,
    const char* service,
    const char* method,
    const uint8_t* body,
    size_t body_len,
    const trevrpc_call_options_internal* options,
    trevrpc_wire_response_values** response);
int trevrpc_internal_raw_client_call_request(
    trevrpc_raw_client* client, const trevrpc_request* request, trevrpc_wire_response_values** response);
int trevrpc_internal_raw_client_call_request_cancellable(trevrpc_raw_client* client,
    const trevrpc_request* request,
    trevrpc_cancellation* cancellation,
    trevrpc_wire_response_values** response);
int trevrpc_internal_raw_client_call_request_borrowed_cancellable(trevrpc_raw_client* client,
    const trevrpc_request* request,
    trevrpc_cancellation* cancellation,
    trevrpc_wire_response_values** response);
int trevrpc_internal_raw_client_call_request_with_options(trevrpc_raw_client* client,
    const trevrpc_request* request,
    const trevrpc_call_options_internal* options,
    trevrpc_wire_response_values** response);
int trevrpc_internal_raw_client_start_stream(trevrpc_raw_client* client,
    const char* service,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len,
    trevrpc_stream** stream);
int trevrpc_internal_raw_client_start_stream_with_options(trevrpc_raw_client* client,
    const char* service,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len,
    const trevrpc_call_options_internal* options,
    trevrpc_stream** stream);
int trevrpc_internal_raw_client_start_stream_request(
    trevrpc_raw_client* client, const trevrpc_request* request, trevrpc_stream** stream);
int trevrpc_internal_raw_client_start_stream_request_cancellable(trevrpc_raw_client* client,
    const trevrpc_request* request,
    trevrpc_cancellation* cancellation,
    trevrpc_stream** stream);
int trevrpc_internal_raw_client_start_stream_request_borrowed_cancellable(trevrpc_raw_client* client,
    const trevrpc_request* request,
    trevrpc_cancellation* cancellation,
    trevrpc_stream** stream);
int trevrpc_internal_raw_client_start_stream_request_with_options(trevrpc_raw_client* client,
    const trevrpc_request* request,
    const trevrpc_call_options_internal* options,
    trevrpc_stream** stream);
int trevrpc_internal_server_listen(const trevrpc_server_config_internal* config, trevrpc_server** server);
int trevrpc_internal_server_set_options(trevrpc_server* server, const trevrpc_server_options_internal* options);
int trevrpc_internal_server_get_options(trevrpc_server* server, trevrpc_server_options_internal* options);
int trevrpc_internal_server_register_unary(trevrpc_server* server,
    const char* service,
    const char* method,
    trevrpc_unary_handler_internal handler,
    void* user_data);
int trevrpc_internal_call_respond(trevrpc_call* call, trevrpc_wire_response_values* response);
void trevrpc_internal_server_shutdown(trevrpc_server* server);
void trevrpc_internal_server_close(trevrpc_server* server);
int trevrpc_internal_stream_recv(trevrpc_stream* stream, trevrpc_wire_stream_frame_values** frame);
int trevrpc_internal_stream_recv_ready(trevrpc_stream* stream, trevrpc_wire_stream_frame_values** frame, int* ready);
int trevrpc_internal_stream_recv_ready_since(
    trevrpc_stream* stream, trevrpc_wire_stream_frame_values** frame, int* ready, uint64_t wait_started_nanos);
int trevrpc_internal_stream_recv_batch(
    trevrpc_stream* stream, trevrpc_wire_stream_frame_values** frames, size_t capacity, size_t* count, int* eof);

int trevrpc_internal_raw_client_connect_observed(const char* host,
    uint16_t port,
    const trevrpc_client_config_internal* config,
    trevrpc_msquic_cancelled_fn cancelled,
    void* cancellation_context,
    const uint8_t* resumption_ticket,
    size_t resumption_ticket_len,
    trevrpc_msquic_conn_observer observer,
    void* observer_context,
    trevrpc_raw_client** client);
void trevrpc_internal_raw_client_clear_observer(trevrpc_raw_client* client);
int trevrpc_raw_client_call_request_versioned(trevrpc_raw_client* client,
    const trevrpc_request* request,
    const trevrpc_call_options_internal* options,
    bool borrow_request_body,
    trevrpc_inbound_response** response);
int trevrpc_raw_client_start_stream_request_versioned(trevrpc_raw_client* client,
    const trevrpc_request* request,
    const trevrpc_call_options_internal* options,
    bool borrow_request_body,
    trevrpc_stream** stream);
int trevrpc_channel_call_request_versioned(trevrpc_channel* channel,
    const trevrpc_request* request,
    const trevrpc_call_options_internal* options,
    bool borrow_request_body,
    trevrpc_inbound_response** response);
int trevrpc_channel_start_stream_request_versioned(trevrpc_channel* channel,
    const trevrpc_request* request,
    const trevrpc_call_options_internal* options,
    bool borrow_request_body,
    trevrpc_stream** stream);
void trevrpc_stream_set_release(trevrpc_stream* stream, void (*release)(void* context), void* context);
int trevrpc_stream_recv_inbound_versioned(trevrpc_stream* stream, trevrpc_inbound_stream_frame** out_frame);
int trevrpc_stream_recv_inbound_ready_versioned(
    trevrpc_stream* stream, trevrpc_inbound_stream_frame** out_frame, int* ready);
int trevrpc_stream_recv_inbound_ready_since_versioned(
    trevrpc_stream* stream, trevrpc_inbound_stream_frame** out_frame, int* ready, uint64_t wait_started_nanos);
int trevrpc_stream_recv_inbound_batch_versioned(
    trevrpc_stream* stream, trevrpc_inbound_stream_frame** frames, size_t capacity, size_t* count, int* eof);
void trevrpc_server_enable_strict_lifecycle(trevrpc_server* server);
typedef void (*trevrpc_versioned_test_connect_hook)(void* context);
void trevrpc_versioned_test_set_connect_hook(trevrpc_versioned_test_connect_hook hook, void* context);
size_t trevrpc_test_cancellation_ref_count(trevrpc_cancellation* cancellation);
#define TREVRPC_TEST_STREAM_SEND_LOCK_WAIT 1u
#define TREVRPC_TEST_STREAM_STATUS_CLAIMED 2u
typedef void (*trevrpc_test_stream_send_hook)(uint32_t event, void* context);
void trevrpc_test_set_stream_send_hook(trevrpc_test_stream_send_hook hook, void* context);
typedef void (*trevrpc_test_call_insert_hook)(void* context);
void trevrpc_test_set_call_insert_hook(trevrpc_test_call_insert_hook hook, void* context);
intptr_t trevrpc_wt_stream_read_frame_owned(trevrpc_wt_stream* stream, trevrpc_owned_bytes* body, size_t max_len);
intptr_t trevrpc_wt_stream_read_frame_owned_timeout(
    trevrpc_wt_stream* stream, trevrpc_owned_bytes* body, size_t max_len, uint64_t timeout_nanos);
intptr_t trevrpc_wt_stream_read_frame_owned_ready(trevrpc_wt_stream* stream, trevrpc_owned_bytes* body, size_t max_len);
int trevrpc_wt_stream_abort_receive(trevrpc_wt_stream* stream);
intptr_t trevrpc_h3_stream_read_frame_owned(trevrpc_h3_stream* stream, trevrpc_owned_bytes* body, size_t max_len);
intptr_t trevrpc_h3_stream_read_frame_owned_timeout(
    trevrpc_h3_stream* stream, trevrpc_owned_bytes* body, size_t max_len, uint64_t timeout_nanos);
intptr_t trevrpc_h3_stream_read_frame_owned_ready(trevrpc_h3_stream* stream, trevrpc_owned_bytes* body, size_t max_len);
int trevrpc_h3_stream_abort_receive(trevrpc_h3_stream* stream);

typedef struct trevrpc_scripted_frame_body {
    const uint8_t* body;
    size_t body_len;
} trevrpc_scripted_frame_body;

typedef struct trevrpc_scripted_stream_source trevrpc_scripted_stream_source;

typedef enum trevrpc_response_state_failure {
    TREVRPC_RESPONSE_STATE_FAILURE_NONE = 0,
    TREVRPC_RESPONSE_STATE_FAILURE_FRAME_TOO_LARGE = 1,
    TREVRPC_RESPONSE_STATE_FAILURE_INVALID_METADATA = 2,
    TREVRPC_RESPONSE_STATE_FAILURE_MALFORMED_PROTOBUF = 3,
    TREVRPC_RESPONSE_STATE_FAILURE_MISSING_TERMINAL_STATUS = 4,
    TREVRPC_RESPONSE_STATE_FAILURE_NATIVE = 5,
    TREVRPC_RESPONSE_STATE_FAILURE_REMOTE_STATUS = 6,
    TREVRPC_RESPONSE_STATE_FAILURE_RESPONSE_CARDINALITY = 7,
    TREVRPC_RESPONSE_STATE_FAILURE_TRAILING_FRAME = 8,
    TREVRPC_RESPONSE_STATE_FAILURE_UNSUPPORTED_FRAME_KIND = 9,
} trevrpc_response_state_failure;

typedef int (*trevrpc_response_decode_fn)(
    const uint8_t* body, size_t body_len, uint8_t** decoded, size_t* decoded_len, void* context);

typedef struct trevrpc_response_state {
    trevrpc_stream* stream;
    trevrpc_response_decode_fn decode;
    void* decode_context;
    trevrpc_inbound_stream_frame* terminal;
    size_t response_count;
    trevrpc_response_state_failure failure;
    uint32_t failure_status;
    int native_error;
    bool done;
} trevrpc_response_state;

void trevrpc_response_state_init(
    trevrpc_response_state* state, trevrpc_stream* stream, trevrpc_response_decode_fn decode, void* decode_context);
int trevrpc_response_state_next(trevrpc_response_state* state, uint8_t** decoded, size_t* decoded_len, int* eof);
int trevrpc_response_state_read_exactly_one(trevrpc_response_state* state, uint8_t** decoded, size_t* decoded_len);
const trevrpc_inbound_stream_frame* trevrpc_response_state_terminal(const trevrpc_response_state* state);
void trevrpc_response_state_reset(trevrpc_response_state* state);

int trevrpc_scripted_stream_new(const trevrpc_scripted_frame_body* frames,
    size_t frame_count,
    int source_error,
    size_t max_frame_size,
    trevrpc_stream** out_stream,
    trevrpc_scripted_stream_source** out_source);
size_t trevrpc_scripted_stream_close_count(const trevrpc_scripted_stream_source* source);
void trevrpc_scripted_stream_source_free(trevrpc_scripted_stream_source* source);
trevrpc_wire_diagnostic_reason trevrpc_stream_last_wire_diagnostic(const trevrpc_stream* stream);

#ifdef __cplusplus
}
#endif

#endif
