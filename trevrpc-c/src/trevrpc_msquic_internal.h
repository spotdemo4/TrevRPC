#ifndef TREVRPC_MSQUIC_INTERNAL_H
#define TREVRPC_MSQUIC_INTERNAL_H

#include "trevrpc_msquic_types_internal.h"
#include "trevrpc_msquic_accept_internal.h"
#include "trevrpc_frame_internal.h"
#include "trevrpc_msquic_features_internal.h"
#include "trevrpc_owned_bytes_internal.h"

#include <errno.h>
#include <stdbool.h>

/* Private declarations for the callback-native MsQuic implementation. */
int trevrpc_msquic_listen(
    const char* host, uint16_t port, const trevrpc_msquic_config* config, trevrpc_msquic_listener** listener);
int trevrpc_msquic_listen_alpns(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    const trevrpc_msquic_alpn* alpns,
    size_t alpns_len,
    trevrpc_msquic_listener** listener);
int trevrpc_msquic_listener_port(trevrpc_msquic_listener* listener, uint16_t* port);
int trevrpc_msquic_listener_accept(trevrpc_msquic_listener* listener, trevrpc_msquic_conn** conn);
void trevrpc_msquic_listener_shutdown(trevrpc_msquic_listener* listener);
void trevrpc_msquic_listener_close(trevrpc_msquic_listener* listener);
int trevrpc_msquic_dial(
    const char* host, uint16_t port, const trevrpc_msquic_config* config, trevrpc_msquic_conn** conn);
int trevrpc_msquic_dial_cancellable(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    trevrpc_msquic_cancelled_fn cancelled,
    void* cancellation_context,
    trevrpc_msquic_conn** conn);
int trevrpc_msquic_conn_negotiated_alpn(trevrpc_msquic_conn* conn, const uint8_t** alpn, size_t* alpn_len);
int trevrpc_msquic_conn_peer_close_error(trevrpc_msquic_conn* conn, uint64_t* error_code);
int trevrpc_msquic_conn_accept_stream(trevrpc_msquic_conn* conn, trevrpc_msquic_stream** stream);
int trevrpc_msquic_conn_open_stream(trevrpc_msquic_conn* conn, trevrpc_msquic_stream** stream);
int trevrpc_msquic_conn_open_uni_stream(trevrpc_msquic_conn* conn, trevrpc_msquic_stream** stream);
void trevrpc_msquic_conn_shutdown(trevrpc_msquic_conn* conn);
void trevrpc_msquic_conn_shutdown_error(trevrpc_msquic_conn* conn, uint64_t error_code);
void trevrpc_msquic_conn_close(trevrpc_msquic_conn* conn);
int trevrpc_msquic_stream_id(trevrpc_msquic_stream* stream, uint64_t* stream_id);
intptr_t trevrpc_msquic_stream_read(trevrpc_msquic_stream* stream, uint8_t* data, size_t len);
intptr_t trevrpc_msquic_stream_read_timeout(
    trevrpc_msquic_stream* stream, uint8_t* data, size_t len, uint64_t timeout_nanos);
intptr_t trevrpc_msquic_stream_read_ready(trevrpc_msquic_stream* stream, uint8_t* data, size_t len);
intptr_t trevrpc_msquic_stream_read_frame(trevrpc_msquic_stream* stream, uint8_t** body, size_t* len, size_t max_len);
intptr_t trevrpc_msquic_stream_read_frame_timeout(
    trevrpc_msquic_stream* stream, uint8_t** body, size_t* len, size_t max_len, uint64_t timeout_nanos);
intptr_t trevrpc_msquic_stream_read_frame_ready(
    trevrpc_msquic_stream* stream, uint8_t** body, size_t* len, size_t max_len);
intptr_t trevrpc_msquic_stream_write(trevrpc_msquic_stream* stream, const uint8_t* data, size_t len);
intptr_t trevrpc_msquic_stream_write_fin(trevrpc_msquic_stream* stream, const uint8_t* data, size_t len);
intptr_t trevrpc_msquic_stream_write_frame_parts(
    trevrpc_msquic_stream* stream, const trevrpc_msquic_frame_part* parts, size_t parts_len, size_t max_len);
intptr_t trevrpc_msquic_stream_write_frame_parts_fin(
    trevrpc_msquic_stream* stream, const trevrpc_msquic_frame_part* parts, size_t parts_len, size_t max_len);
intptr_t trevrpc_msquic_stream_write_frame_parts_with_completion(trevrpc_msquic_stream* stream,
    const trevrpc_msquic_frame_part* parts,
    size_t parts_len,
    size_t max_len,
    trevrpc_msquic_send_completion** completion);
intptr_t trevrpc_msquic_stream_write_frame_parts_fin_with_completion(trevrpc_msquic_stream* stream,
    const trevrpc_msquic_frame_part* parts,
    size_t parts_len,
    size_t max_len,
    trevrpc_msquic_send_completion** completion);
intptr_t trevrpc_msquic_stream_write_message_frame(
    trevrpc_msquic_stream* stream, const uint8_t* body, size_t body_len, size_t max_len);
intptr_t trevrpc_msquic_stream_write_message_frame_wait_capacity(
    trevrpc_msquic_stream* stream, const uint8_t* body, size_t body_len, size_t max_len);
intptr_t trevrpc_msquic_stream_write_message_frames(
    trevrpc_msquic_stream* stream, const uint8_t* bodies, const size_t* body_lens, size_t count, size_t max_len);
intptr_t trevrpc_msquic_stream_write_message_frames_borrowed(trevrpc_msquic_stream* stream,
    const uint8_t* const* bodies,
    const size_t* body_lens,
    size_t count,
    size_t max_len,
    trevrpc_msquic_send_completion** completion);
int trevrpc_msquic_send_completion_wait(trevrpc_msquic_send_completion* completion);
void trevrpc_msquic_send_completion_free(trevrpc_msquic_send_completion* completion);
int trevrpc_msquic_stream_wait_pending_sends(trevrpc_msquic_stream* stream);
int trevrpc_msquic_stream_shutdown_send(trevrpc_msquic_stream* stream);
int trevrpc_msquic_stream_abort(trevrpc_msquic_stream* stream);
int trevrpc_msquic_stream_abort_receive(trevrpc_msquic_stream* stream);
void trevrpc_msquic_stream_close(trevrpc_msquic_stream* stream);
void trevrpc_msquic_free(void* ptr);
const char* trevrpc_msquic_error(int code);

#define TREV_MSQUIC_CONN_EVENT_CONNECTED 0u
#define TREV_MSQUIC_CONN_EVENT_SHUTDOWN_COMPLETE 1u
#define TREV_MSQUIC_CONN_EVENT_LOCAL_ADDRESS_CHANGED 2u
#define TREV_MSQUIC_CONN_EVENT_PEER_ADDRESS_CHANGED 3u
#define TREV_MSQUIC_CONN_EVENT_RESUMPTION_TICKET_RECEIVED 4u
#define TREV_MSQUIC_CONN_EVENT_PEER_STREAM_AVAILABLE 5u

typedef struct trevrpc_msquic_conn_event {
    uint32_t kind;
    int error_code;
    int session_resumed;
    const uint8_t* resumption_ticket;
    size_t resumption_ticket_len;
} trevrpc_msquic_conn_event;

typedef void (*trevrpc_msquic_conn_observer)(void* context, const trevrpc_msquic_conn_event* event);
typedef void (*trevrpc_msquic_wake_observer)(void* context);
typedef trevrpc_msquic_wake_observer trevrpc_msquic_listener_observer;
typedef trevrpc_msquic_wake_observer trevrpc_msquic_stream_start_observer;
typedef struct trevrpc_msquic_finalizer_scope trevrpc_msquic_finalizer_scope;

typedef struct trevrpc_msquic_receive_policy {
    size_t max_stream_owned_bytes;
    size_t max_stream_owned_count;
    size_t max_connection_owned_bytes;
    size_t max_connection_owned_count;
    size_t max_undecided_owned_bytes;
    size_t max_undecided_owned_count;
} trevrpc_msquic_receive_policy;

size_t trevrpc_msquic_receive_minimum_raw_bytes(void);

int trevrpc_msquic_dial_observed(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    trevrpc_msquic_cancelled_fn cancelled,
    void* cancellation_context,
    const uint8_t* resumption_ticket,
    size_t resumption_ticket_len,
    trevrpc_msquic_conn_observer observer,
    void* observer_context,
    trevrpc_msquic_conn** conn);
int trevrpc_msquic_dial_observed_features(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    const trevrpc_msquic_feature_request* features,
    trevrpc_msquic_cancelled_fn cancelled,
    void* cancellation_context,
    const uint8_t* resumption_ticket,
    size_t resumption_ticket_len,
    trevrpc_msquic_conn_observer observer,
    void* observer_context,
    trevrpc_msquic_conn** conn);
int trevrpc_msquic_dial_start_observed_features_with_receive_policy(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    const char* server_name,
    const trevrpc_msquic_feature_request* features,
    trevrpc_msquic_cancelled_fn cancelled,
    void* cancellation_context,
    const uint8_t* resumption_ticket,
    size_t resumption_ticket_len,
    trevrpc_msquic_conn_observer observer,
    void* observer_context,
    const trevrpc_msquic_receive_policy* receive_policy,
    trevrpc_msquic_conn** conn);
int trevrpc_msquic_dial_observed_features_with_receive_policy(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    const trevrpc_msquic_feature_request* features,
    trevrpc_msquic_cancelled_fn cancelled,
    void* cancellation_context,
    const uint8_t* resumption_ticket,
    size_t resumption_ticket_len,
    trevrpc_msquic_conn_observer observer,
    void* observer_context,
    const trevrpc_msquic_receive_policy* receive_policy,
    trevrpc_msquic_conn** conn);
trevrpc_msquic_feature_request trevrpc_msquic_default_h3_features(void);
int trevrpc_msquic_listen_alpns_features(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    const trevrpc_msquic_alpn* alpns,
    size_t alpns_len,
    const trevrpc_msquic_feature_request* features,
    trevrpc_msquic_listener** out_listener);
int trevrpc_msquic_listen_alpns_features_with_receive_policy(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    const trevrpc_msquic_alpn* alpns,
    size_t alpns_len,
    const trevrpc_msquic_feature_request* features,
    const trevrpc_msquic_receive_policy* receive_policy,
    trevrpc_msquic_listener** out_listener);
int trevrpc_msquic_listen_alpns_features_with_dispatch(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    const trevrpc_msquic_alpn* alpns,
    size_t alpns_len,
    const trevrpc_msquic_feature_request* features,
    const trevrpc_msquic_receive_policy* receive_policy,
    trevrpc_msquic_accept_dispatch dispatch,
    void* dispatch_context,
    trevrpc_msquic_context_destroy dispatch_context_destroy,
    trevrpc_msquic_listener** out_listener);
trevrpc_msquic_provider_features trevrpc_msquic_provider_features_current(void);
int trevrpc_msquic_conn_feature_snapshot(trevrpc_msquic_conn* conn, trevrpc_msquic_feature_snapshot* snapshot);
int trevrpc_msquic_listener_set_observer(
    trevrpc_msquic_listener* listener, trevrpc_msquic_listener_observer observer, void* context);
void trevrpc_msquic_listener_clear_observer(trevrpc_msquic_listener* listener);
void trevrpc_msquic_listener_drain_observer(trevrpc_msquic_listener* listener);
int trevrpc_msquic_listener_accept_ready(trevrpc_msquic_listener* listener, trevrpc_msquic_conn** out_conn);
int trevrpc_msquic_conn_set_observer(trevrpc_msquic_conn* conn, trevrpc_msquic_conn_observer observer, void* context);
void trevrpc_msquic_conn_clear_observer(trevrpc_msquic_conn* conn);
void trevrpc_msquic_conn_drain_observer(trevrpc_msquic_conn* conn);
int trevrpc_msquic_conn_ready(trevrpc_msquic_conn* conn);
int trevrpc_msquic_conn_accept_stream_ready(trevrpc_msquic_conn* conn, trevrpc_msquic_stream** out_stream);

enum {
    TREV_MSQUIC_STREAM_OBSERVER_READABLE = 1u << 0,
    TREV_MSQUIC_STREAM_OBSERVER_TERMINAL = 1u << 1,
    TREV_MSQUIC_STREAM_OBSERVER_SEND_COMPLETE = 1u << 2,
};

typedef void (*trevrpc_msquic_stream_observer)(void* context, uint32_t flags);

/* Observer callbacks are wake-only, run without the stream mutex held, and are never called by install. */
int trevrpc_msquic_stream_set_observer(
    trevrpc_msquic_stream* stream, trevrpc_msquic_stream_observer observer, void* context);
/* Clear is nonblocking and safe from the observer callback itself. */
void trevrpc_msquic_stream_clear_observer(trevrpc_msquic_stream* stream);
/* Drain must not be called from an observer callback. */
void trevrpc_msquic_stream_drain_observer(trevrpc_msquic_stream* stream);
int trevrpc_msquic_stream_set_start_observer(
    trevrpc_msquic_stream* stream, trevrpc_msquic_stream_start_observer observer, void* context);
void trevrpc_msquic_stream_clear_start_observer(trevrpc_msquic_stream* stream);
void trevrpc_msquic_stream_drain_start_observer(trevrpc_msquic_stream* stream);
int trevrpc_msquic_stream_start_status(trevrpc_msquic_stream* stream, uint64_t* out_stream_id);
typedef struct trevrpc_msquic_stream_state_snapshot {
    bool recv_fin;
    bool closed;
    bool shutdown_complete;
    int error_code;
    bool peer_send_aborted;
    uint64_t peer_send_error;
    bool peer_receive_aborted;
    uint64_t peer_receive_error;
} trevrpc_msquic_stream_state_snapshot;
int trevrpc_msquic_stream_state(trevrpc_msquic_stream* stream, trevrpc_msquic_stream_state_snapshot* out_state);

typedef int (*trevrpc_msquic_send_encode_fn)(uint8_t* data, size_t len, void* context);
intptr_t trevrpc_msquic_stream_write_raw_encoded_with_completion(trevrpc_msquic_stream* stream,
    size_t len,
    bool fin,
    trevrpc_msquic_send_encode_fn encode,
    void* encode_context,
    trevrpc_msquic_send_completion** completion);
intptr_t trevrpc_msquic_stream_write_raw_with_completion(trevrpc_msquic_stream* stream,
    const uint8_t* data,
    size_t len,
    bool fin,
    trevrpc_msquic_send_completion** completion);
int trevrpc_msquic_send_completion_status(trevrpc_msquic_send_completion* completion);
int trevrpc_msquic_stream_abort_with_error(trevrpc_msquic_stream* stream, uint64_t error_code);
int trevrpc_msquic_stream_abort_receive_with_error(trevrpc_msquic_stream* stream, uint64_t error_code);
int trevrpc_msquic_stream_abort_send_with_error(trevrpc_msquic_stream* stream, uint64_t error_code);
void trevrpc_msquic_stream_close_immediate(trevrpc_msquic_stream* stream);
/* These ownership-transfer helpers return immediately after scheduling native teardown. */
void trevrpc_msquic_stream_close_deferred_owned(trevrpc_msquic_stream* stream, bool immediate);
void trevrpc_msquic_conn_close_deferred_owned(trevrpc_msquic_conn* conn);
/* Shutdown is non-owning; close transfers the caller's final listener reference. */
void trevrpc_msquic_listener_shutdown_deferred(trevrpc_msquic_listener* listener);
void trevrpc_msquic_listener_close_deferred_owned(trevrpc_msquic_listener* listener);
/* Waits for all native finalizer work queued before and during the wait. */
void trevrpc_msquic_finalizer_drain(void);
int trevrpc_msquic_finalizer_scope_init(trevrpc_msquic_finalizer_scope* scope);
void trevrpc_msquic_finalizer_scope_drain(trevrpc_msquic_finalizer_scope* scope);
void trevrpc_msquic_finalizer_scope_destroy(trevrpc_msquic_finalizer_scope* scope);
void trevrpc_msquic_stream_set_finalizer_scope(
    trevrpc_msquic_stream* stream, trevrpc_msquic_finalizer_scope* scope);
void trevrpc_msquic_conn_set_finalizer_scope(
    trevrpc_msquic_conn* conn, trevrpc_msquic_finalizer_scope* scope);
void trevrpc_msquic_listener_set_finalizer_scope(
    trevrpc_msquic_listener* listener, trevrpc_msquic_finalizer_scope* scope);
intptr_t trevrpc_msquic_stream_read_protocol(trevrpc_msquic_stream* stream, uint8_t* data, size_t len);
intptr_t trevrpc_msquic_stream_read_protocol_timeout(
    trevrpc_msquic_stream* stream, uint8_t* data, size_t len, uint64_t timeout_nanos);
intptr_t trevrpc_msquic_stream_read_protocol_ready(trevrpc_msquic_stream* stream, uint8_t* data, size_t len);
int trevrpc_msquic_stream_select_protocol_bytes(trevrpc_msquic_stream* stream);
intptr_t trevrpc_msquic_stream_read_frame_owned(
    trevrpc_msquic_stream* stream, trevrpc_owned_bytes* body, size_t max_len);
intptr_t trevrpc_msquic_stream_read_frame_owned_timeout(
    trevrpc_msquic_stream* stream, trevrpc_owned_bytes* body, size_t max_len, uint64_t timeout_nanos);
intptr_t trevrpc_msquic_stream_read_frame_owned_ready(
    trevrpc_msquic_stream* stream, trevrpc_owned_bytes* body, size_t max_len);

typedef enum trevrpc_msquic_test_stream_event {
    TREV_MSQUIC_TEST_STREAM_SEND_PREPARE = 0,
    TREV_MSQUIC_TEST_STREAM_SEND_RESERVED = 1,
    TREV_MSQUIC_TEST_STREAM_SEND_CAPACITY_WAIT = 2,
    TREV_MSQUIC_TEST_STREAM_CLOSE_STARTED = 3,
    TREV_MSQUIC_TEST_STREAM_SEND_TERMINAL = 4,
    TREV_MSQUIC_TEST_STREAM_SHUTDOWN_GRACEFUL = 5,
    TREV_MSQUIC_TEST_STREAM_SHUTDOWN_ABORT_RECEIVE = 6,
    TREV_MSQUIC_TEST_STREAM_SHUTDOWN_ABORT = 7,
    TREV_MSQUIC_TEST_STREAM_CLOSE_COMPLETED = 8,
    TREV_MSQUIC_TEST_STREAM_RECV_RESUME_PINNED = 9,
    TREV_MSQUIC_TEST_STREAM_DRAIN_OBSERVER_PINNED = 10,
    TREV_MSQUIC_TEST_STREAM_WAIT_SENDS_PINNED = 11,
    TREV_MSQUIC_TEST_STREAM_SEND_COMPLETE_ENTERED = 12,
    TREV_MSQUIC_TEST_STREAM_SHUTDOWN_ABORT_SEND = 13,
    TREV_MSQUIC_TEST_STREAM_CLOSE_IMMEDIATE = 14,
    TREV_MSQUIC_TEST_STREAM_EVENT_COUNT = 15,
} trevrpc_msquic_test_stream_event;

typedef void (*trevrpc_msquic_test_stream_hook)(trevrpc_msquic_test_stream_event event, void* context);

typedef struct trevrpc_msquic_test_receive_fixture trevrpc_msquic_test_receive_fixture;
typedef struct trevrpc_msquic_test_receive_snapshot {
    size_t stream_owned_bytes;
    size_t stream_owned_count;
    size_t connection_owned_bytes;
    size_t connection_owned_count;
    size_t buffered_bytes;
    size_t queued_frames;
    uint64_t resume_scan_count;
    size_t active_resume_pins;
    bool paused;
    bool receive_disabled;
    bool recv_fin;
    bool pending_frame;
    int err;
} trevrpc_msquic_test_receive_snapshot;

size_t trevrpc_msquic_test_receive_minimum_bytes(size_t max_frame_size);
int trevrpc_msquic_test_receive_fixture_create(const trevrpc_msquic_receive_policy* policy,
    size_t max_frame_size,
    size_t stream_count,
    trevrpc_msquic_test_receive_fixture** out_fixture);
trevrpc_msquic_stream* trevrpc_msquic_test_receive_fixture_stream(
    trevrpc_msquic_test_receive_fixture* fixture, size_t index);
trevrpc_msquic_stream* trevrpc_msquic_test_receive_fixture_take_stream(
    trevrpc_msquic_test_receive_fixture* fixture, size_t index);
int trevrpc_msquic_test_receive_inject(trevrpc_msquic_stream* stream,
    const uint8_t* const* buffers,
    const size_t* lengths,
    size_t buffer_count,
    bool fin,
    size_t* accepted);
void trevrpc_msquic_test_receive_snapshot_get(
    trevrpc_msquic_stream* stream, trevrpc_msquic_test_receive_snapshot* snapshot);
void trevrpc_msquic_test_receive_fixture_drop_connection(trevrpc_msquic_test_receive_fixture* fixture);
void trevrpc_msquic_test_receive_fixture_set_handle_present(trevrpc_msquic_stream* stream, bool present);
void trevrpc_msquic_test_receive_fixture_prepare_graceful_close(trevrpc_msquic_stream* stream);
void trevrpc_msquic_test_receive_fixture_complete_shutdown(trevrpc_msquic_stream* stream);
void trevrpc_msquic_test_receive_simulate_handle_loss(trevrpc_msquic_stream* stream);
void trevrpc_msquic_test_receive_fixture_destroy(trevrpc_msquic_test_receive_fixture* fixture);

void trevrpc_msquic_test_set_stream_hook(trevrpc_msquic_test_stream_hook hook, void* context);
void trevrpc_msquic_test_fail_next_stream_send(void);
void trevrpc_msquic_test_fail_next_graceful_shutdown(void);
void trevrpc_msquic_test_fail_next_receive_raw_allocation(void);
void trevrpc_msquic_test_fail_next_receive_parser_allocation(void);
void trevrpc_msquic_test_fail_next_receive_frame_allocation(void);
size_t trevrpc_msquic_test_api_leases(void);
int trevrpc_msquic_test_reliable_reset_negotiated(trevrpc_msquic_conn* conn);
void trevrpc_msquic_test_wait_stream_shutdown_complete(trevrpc_msquic_stream* stream);
int trevrpc_msquic_test_inject_peer_send_aborted(trevrpc_msquic_stream* stream, uint64_t error_code);
int trevrpc_msquic_test_inject_peer_receive_aborted(trevrpc_msquic_stream* stream, uint64_t error_code);
int trevrpc_msquic_test_stream_last_abort_error(trevrpc_msquic_stream* stream, uint64_t* out_error);
void trevrpc_msquic_test_stream_clear_abort_error(trevrpc_msquic_stream* stream);
int trevrpc_msquic_test_parse_frame_owned(const uint8_t* data,
    size_t len,
    size_t max_len,
    trevrpc_frame_alloc_fn alloc,
    trevrpc_frame_free_fn dealloc,
    void* allocator_context,
    trevrpc_owned_bytes* body);

#endif
