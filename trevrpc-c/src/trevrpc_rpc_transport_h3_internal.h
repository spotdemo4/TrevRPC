#ifndef TREVRPC_RPC_TRANSPORT_H3_INTERNAL_H
#define TREVRPC_RPC_TRANSPORT_H3_INTERNAL_H

#include "trevrpc_msquic_internal.h"
#include "trevrpc_rpc_transport_internal.h"

#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
#include <stdbool.h>
#endif

/* Direct callback-native HTTP/3 source. This is private and is not installed. */
int trevrpc_rpc_transport_h3_create(const trevrpc_rpc_transport_config* config, trevrpc_rpc_transport** out_transport);
int trevrpc_rpc_transport_h3_listen_shared(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_endpoint_config* config,
    trevrpc_msquic_accept_dispatch dispatch,
    void* dispatch_context,
    trevrpc_msquic_context_destroy dispatch_context_destroy,
    trevrpc_rpc_transport_handle* out_listener);

#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
int trevrpc_rpc_transport_h3_test_make_connection(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle* out_connection);
int trevrpc_rpc_transport_h3_test_make_server_connection(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle* out_listener,
    trevrpc_rpc_transport_handle* out_connection);
int trevrpc_rpc_transport_h3_test_set_deferred_admission(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, bool enabled);
int trevrpc_rpc_transport_h3_test_set_connection_protocol(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, uint32_t protocol);
int trevrpc_rpc_transport_h3_test_set_multiplexed_webtransport(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, bool enabled);
int trevrpc_rpc_transport_h3_test_set_peer_settings_ready(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, bool ready);
int trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle* out_connection);
int trevrpc_rpc_transport_h3_test_adopt_peer_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    trevrpc_msquic_stream* object,
    bool unidirectional,
    trevrpc_rpc_transport_handle* out_stream);
int trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    trevrpc_msquic_stream* object,
    trevrpc_rpc_transport_handle* out_stream);
int trevrpc_rpc_transport_h3_test_set_unresolved_limits(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    uint32_t stream_count,
    uint64_t timeout_ms);
void trevrpc_rpc_transport_h3_test_set_now_nanos(trevrpc_rpc_transport* transport, uint64_t now_nanos);
int trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, uint32_t profile);
int trevrpc_rpc_transport_h3_test_resolve_webtransport_session(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, uint64_t session_id);
int trevrpc_rpc_transport_h3_test_inject_data(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle stream,
    const uint8_t* data,
    size_t data_len,
    bool fin);
int trevrpc_rpc_transport_h3_test_retry_buffered_stream(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream);
int trevrpc_rpc_transport_h3_test_emit(trevrpc_rpc_transport* transport,
    uint32_t kind,
    uint32_t flags,
    int32_t status,
    trevrpc_rpc_transport_handle subject,
    trevrpc_rpc_transport_handle parent,
    uint64_t operation_id);
int trevrpc_rpc_transport_h3_test_make_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    trevrpc_rpc_transport_handle* out_stream);
int trevrpc_rpc_transport_h3_test_make_peer_request_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    const char* configured_path,
    size_t configured_path_len,
    trevrpc_rpc_transport_handle* out_stream);
int trevrpc_rpc_transport_h3_test_attach_stream_object(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, trevrpc_msquic_stream* object);
int trevrpc_rpc_transport_h3_test_make_control_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    trevrpc_rpc_transport_handle* out_stream);
int trevrpc_rpc_transport_h3_test_make_qpack_encoder_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    trevrpc_rpc_transport_handle* out_stream);
int trevrpc_rpc_transport_h3_test_make_connect_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    trevrpc_rpc_transport_handle* out_stream);
int trevrpc_rpc_transport_h3_test_mark_connect_accepted(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, trevrpc_rpc_transport_handle stream);
int trevrpc_rpc_transport_h3_test_fail_connection(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection);
size_t trevrpc_rpc_transport_h3_test_pending_events(trevrpc_rpc_transport* transport);
void trevrpc_rpc_transport_h3_test_fail_event_alloc_after(
    trevrpc_rpc_transport* transport, size_t successful_allocations);
void trevrpc_rpc_transport_h3_test_capture_acceptances(trevrpc_rpc_transport* transport, bool enabled);
size_t trevrpc_rpc_transport_h3_test_acceptance_count(trevrpc_rpc_transport* transport);
void trevrpc_rpc_transport_h3_test_capture_rejections(trevrpc_rpc_transport* transport, bool enabled);
int trevrpc_rpc_transport_h3_test_last_rejection(
    trevrpc_rpc_transport* transport, uint16_t* out_status, size_t* out_count);
int trevrpc_rpc_transport_h3_test_stage_pending_send(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle stream,
    uint64_t operation_id,
    size_t bytes,
    trevrpc_msquic_send_completion** out_completion);
void trevrpc_rpc_transport_h3_test_signal_send_completion(trevrpc_msquic_send_completion* completion, int status);
int trevrpc_rpc_transport_h3_test_complete_pending_send(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, int status);
int trevrpc_rpc_transport_h3_test_stream_send_fin(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, bool* out_send_fin);
#endif

#endif
