#include "trevrpc_binding.h"

#include <stddef.h>
#include <stdint.h>

#define LAYOUT(type, expected_size, expected_align)                                                                    \
    _Static_assert(sizeof(type) == (expected_size), #type " size");                                                    \
    _Static_assert(_Alignof(type) == (expected_align), #type " align")

LAYOUT(trevrpc_bytes_view, 16, 8);
_Static_assert(offsetof(trevrpc_bytes_view, data) == 0, "bytes data");
_Static_assert(offsetof(trevrpc_bytes_view, len) == 8, "bytes len");

LAYOUT(trevrpc_client_config_v1, 104, 8);
_Static_assert(offsetof(trevrpc_client_config_v1, struct_size) == 0, "client size");
_Static_assert(offsetof(trevrpc_client_config_v1, struct_version) == 4, "client version");
_Static_assert(offsetof(trevrpc_client_config_v1, cert_file) == 8, "client cert");
_Static_assert(offsetof(trevrpc_client_config_v1, key_file) == 16, "client key");
_Static_assert(offsetof(trevrpc_client_config_v1, ca_cert_file) == 24, "client ca");
_Static_assert(offsetof(trevrpc_client_config_v1, skip_certificate_validation) == 32, "client skip");
_Static_assert(offsetof(trevrpc_client_config_v1, max_idle_timeout_ms) == 40, "client idle");
_Static_assert(offsetof(trevrpc_client_config_v1, keep_alive_ms) == 48, "client keepalive");
_Static_assert(offsetof(trevrpc_client_config_v1, peer_bidi_stream_count) == 52, "client streams");
_Static_assert(offsetof(trevrpc_client_config_v1, max_stateless_operations) == 56, "client stateless");
_Static_assert(offsetof(trevrpc_client_config_v1, max_binding_stateless_operations) == 60, "client binding");
_Static_assert(offsetof(trevrpc_client_config_v1, max_pending_send_bytes) == 64, "client send bytes");
_Static_assert(offsetof(trevrpc_client_config_v1, max_pending_send_count) == 72, "client send count");
_Static_assert(offsetof(trevrpc_client_config_v1, max_frame_size) == 80, "client frame");
_Static_assert(offsetof(trevrpc_client_config_v1, stream_recv_window) == 88, "client stream window");
_Static_assert(offsetof(trevrpc_client_config_v1, conn_flow_control_window) == 92, "client conn window");
_Static_assert(offsetof(trevrpc_client_config_v1, msquic_execution_profile) == 96, "client profile");
_Static_assert(offsetof(trevrpc_client_config_v1, msquic_send_buffering_enabled) == 100, "client buffering");

LAYOUT(trevrpc_server_config_v1, 184, 8);
_Static_assert(offsetof(trevrpc_server_config_v1, struct_size) == 0, "server size");
_Static_assert(offsetof(trevrpc_server_config_v1, struct_version) == 4, "server version");
_Static_assert(offsetof(trevrpc_server_config_v1, host) == 8, "server host");
_Static_assert(offsetof(trevrpc_server_config_v1, port) == 16, "server port");
_Static_assert(offsetof(trevrpc_server_config_v1, cert_file) == 24, "server cert");
_Static_assert(offsetof(trevrpc_server_config_v1, key_file) == 32, "server key");
_Static_assert(offsetof(trevrpc_server_config_v1, webtransport_path) == 40, "server wt path");
_Static_assert(offsetof(trevrpc_server_config_v1, webtransport_origin) == 48, "server wt origin");
_Static_assert(offsetof(trevrpc_server_config_v1, webtransport_admission) == 56, "server wt admission");
_Static_assert(offsetof(trevrpc_server_config_v1, webtransport_admission_user_data) == 64, "server wt data");
_Static_assert(offsetof(trevrpc_server_config_v1, enable_http3) == 72, "server h3 enable");
_Static_assert(offsetof(trevrpc_server_config_v1, http3_path) == 80, "server h3 path");
_Static_assert(offsetof(trevrpc_server_config_v1, http3_admission) == 88, "server h3 admission");
_Static_assert(offsetof(trevrpc_server_config_v1, http3_admission_user_data) == 96, "server h3 data");
_Static_assert(offsetof(trevrpc_server_config_v1, enable_native) == 104, "server native enable");
_Static_assert(offsetof(trevrpc_server_config_v1, max_idle_timeout_ms) == 112, "server idle");
_Static_assert(offsetof(trevrpc_server_config_v1, keep_alive_ms) == 120, "server keepalive");
_Static_assert(offsetof(trevrpc_server_config_v1, peer_bidi_stream_count) == 124, "server streams");
_Static_assert(offsetof(trevrpc_server_config_v1, max_stateless_operations) == 128, "server stateless");
_Static_assert(offsetof(trevrpc_server_config_v1, max_binding_stateless_operations) == 132, "server binding");
_Static_assert(offsetof(trevrpc_server_config_v1, max_pending_send_bytes) == 136, "server send bytes");
_Static_assert(offsetof(trevrpc_server_config_v1, max_pending_send_count) == 144, "server send count");
_Static_assert(offsetof(trevrpc_server_config_v1, max_sessions_per_connection) == 152, "server sessions");
_Static_assert(offsetof(trevrpc_server_config_v1, max_streams_per_session) == 156, "server session streams");
_Static_assert(offsetof(trevrpc_server_config_v1, stream_recv_window) == 160, "server stream window");
_Static_assert(offsetof(trevrpc_server_config_v1, conn_flow_control_window) == 164, "server conn window");
_Static_assert(offsetof(trevrpc_server_config_v1, max_frame_size) == 168, "server frame");
_Static_assert(offsetof(trevrpc_server_config_v1, msquic_execution_profile) == 176, "server profile");
_Static_assert(offsetof(trevrpc_server_config_v1, msquic_send_buffering_enabled) == 180, "server buffering");

LAYOUT(trevrpc_server_options_v1, 88, 8);
_Static_assert(offsetof(trevrpc_server_options_v1, struct_size) == 0, "server options size");
_Static_assert(offsetof(trevrpc_server_options_v1, struct_version) == 4, "server options version");
_Static_assert(offsetof(trevrpc_server_options_v1, max_concurrent_connections) == 8, "server options connections");
_Static_assert(offsetof(trevrpc_server_options_v1, max_concurrent_streams_per_connection) == 16,
    "server options connection streams");
_Static_assert(offsetof(trevrpc_server_options_v1, max_concurrent_requests) == 24, "server options requests");
_Static_assert(offsetof(trevrpc_server_options_v1, worker_count) == 32, "server options workers");
_Static_assert(offsetof(trevrpc_server_options_v1, worker_queue_capacity) == 40, "server options queue");
_Static_assert(
    offsetof(trevrpc_server_options_v1, graceful_shutdown_timeout_nanos) == 48, "server options graceful timeout");
_Static_assert(
    offsetof(trevrpc_server_options_v1, initial_request_timeout_nanos) == 56, "server options request timeout");
_Static_assert(offsetof(trevrpc_server_options_v1, max_stream_messages) == 64, "server options stream messages");
_Static_assert(offsetof(trevrpc_server_options_v1, max_stream_body_size) == 72, "server options stream body");
_Static_assert(offsetof(trevrpc_server_options_v1, stream_idle_timeout_nanos) == 80, "server options stream idle");

LAYOUT(trevrpc_call_options_v1, 72, 8);
_Static_assert(offsetof(trevrpc_call_options_v1, struct_size) == 0, "call options size");
_Static_assert(offsetof(trevrpc_call_options_v1, struct_version) == 4, "call options version");
_Static_assert(offsetof(trevrpc_call_options_v1, metadata) == 8, "call options metadata");
_Static_assert(offsetof(trevrpc_call_options_v1, timeout_nanos) == 16, "call options timeout");
_Static_assert(offsetof(trevrpc_call_options_v1, cancellation) == 24, "call options cancellation");
_Static_assert(offsetof(trevrpc_call_options_v1, max_response_body_size) == 32, "call options response body");
_Static_assert(offsetof(trevrpc_call_options_v1, max_response_messages) == 40, "call options response messages");
_Static_assert(
    offsetof(trevrpc_call_options_v1, max_response_stream_body_size) == 48, "call options response stream body");
_Static_assert(offsetof(trevrpc_call_options_v1, response_idle_timeout_nanos) == 56, "call options response idle");
_Static_assert(offsetof(trevrpc_call_options_v1, request_body_lifetime) == 64, "call options body lifetime");
_Static_assert(offsetof(trevrpc_call_options_v1, reserved0) == 68, "call options reserved");

LAYOUT(trevrpc_response_view_v1, 56, 8);
_Static_assert(offsetof(trevrpc_response_view_v1, struct_size) == 0, "response view size");
_Static_assert(offsetof(trevrpc_response_view_v1, struct_version) == 4, "response view version");
_Static_assert(offsetof(trevrpc_response_view_v1, status) == 8, "response view status");
_Static_assert(offsetof(trevrpc_response_view_v1, reserved0) == 12, "response view reserved");
_Static_assert(offsetof(trevrpc_response_view_v1, message) == 16, "response view message");
_Static_assert(offsetof(trevrpc_response_view_v1, message_len) == 24, "response view message len");
_Static_assert(offsetof(trevrpc_response_view_v1, body) == 32, "response view body");
_Static_assert(offsetof(trevrpc_response_view_v1, body_len) == 40, "response view body len");
_Static_assert(offsetof(trevrpc_response_view_v1, metadata) == 48, "response view metadata");

LAYOUT(trevrpc_status_view_v1, 40, 8);
_Static_assert(offsetof(trevrpc_status_view_v1, struct_size) == 0, "status view size");
_Static_assert(offsetof(trevrpc_status_view_v1, struct_version) == 4, "status view version");
_Static_assert(offsetof(trevrpc_status_view_v1, status) == 8, "status view status");
_Static_assert(offsetof(trevrpc_status_view_v1, reserved0) == 12, "status view reserved");
_Static_assert(offsetof(trevrpc_status_view_v1, message) == 16, "status view message");
_Static_assert(offsetof(trevrpc_status_view_v1, message_len) == 24, "status view message len");
_Static_assert(offsetof(trevrpc_status_view_v1, metadata) == 32, "status view metadata");

static int check_signatures(void) {
    int (*channel_connect)(const char*,
        uint16_t,
        const trevrpc_client_config_v1*,
        const trevrpc_channel_options*,
        uint64_t,
        trevrpc_cancellation*,
        trevrpc_channel**) = trevrpc_channel_connect_v1;
    int (*channel_call)(
        trevrpc_channel*, const trevrpc_request*, const trevrpc_call_options_v1*, trevrpc_inbound_response**) =
        trevrpc_channel_call_request_inbound_v1;
    int (*raw_connect)(
        const char*, uint16_t, const trevrpc_client_config_v1*, trevrpc_cancellation*, trevrpc_raw_client**) =
        trevrpc_raw_client_connect_v1;
    int (*binding_connect)(const char*,
        uint16_t,
        const trevrpc_client_config_v1*,
        trevrpc_cancellation*,
        trevrpc_raw_client_shutdown_callback,
        void*,
        trevrpc_raw_client**) = trevrpc_raw_client_connect_v1_with_shutdown_callback;
    int (*raw_call)(
        trevrpc_raw_client*, const trevrpc_request*, const trevrpc_call_options_v1*, trevrpc_inbound_response**) =
        trevrpc_raw_client_call_request_inbound_v1;
    int (*recv_frame)(trevrpc_stream*, trevrpc_inbound_stream_frame**) = trevrpc_stream_recv_inbound;
    return channel_connect == NULL || channel_call == NULL || raw_connect == NULL || binding_connect == NULL ||
           raw_call == NULL || recv_frame == NULL;
}

int main(void) {
    return TREVRPC_C_ABI_VERSION != 6u || trevrpc_c_abi_version() != 6u || check_signatures() != 0;
}
