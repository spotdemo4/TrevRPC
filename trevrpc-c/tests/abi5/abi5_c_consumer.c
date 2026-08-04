#include "trevrpc.h"
#include "trevrpc_binding.h"      // IWYU pragma: keep
#include "trevrpc_raw.h"          // IWYU pragma: keep
#include "trevrpc_webtransport.h" // IWYU pragma: keep

#include <stddef.h>
#include <stdint.h> // IWYU pragma: keep

_Static_assert(TREVRPC_C_ABI_VERSION == 5u, "ABI-5 fixture must remain version 5");

#define ABI5_LAYOUT(type, expected_size)                                                                               \
    _Static_assert(sizeof(type) == (expected_size), #type " size changed");                                            \
    _Static_assert(_Alignof(type) == 8, #type " alignment changed")
#define ABI5_OFFSET(type, field, expected_offset)                                                                      \
    _Static_assert(offsetof(type, field) == (expected_offset), #type "." #field " offset changed")

ABI5_LAYOUT(trevrpc_config, 96);
ABI5_OFFSET(trevrpc_config, cert_file, 0);
ABI5_OFFSET(trevrpc_config, key_file, 8);
ABI5_OFFSET(trevrpc_config, ca_cert_file, 16);
ABI5_OFFSET(trevrpc_config, skip_certificate_validation, 24);
ABI5_OFFSET(trevrpc_config, max_idle_timeout_ms, 32);
ABI5_OFFSET(trevrpc_config, keep_alive_ms, 40);
ABI5_OFFSET(trevrpc_config, peer_bidi_stream_count, 44);
ABI5_OFFSET(trevrpc_config, max_stateless_operations, 48);
ABI5_OFFSET(trevrpc_config, max_binding_stateless_operations, 52);
ABI5_OFFSET(trevrpc_config, max_pending_send_bytes, 56);
ABI5_OFFSET(trevrpc_config, max_pending_send_count, 64);
ABI5_OFFSET(trevrpc_config, max_frame_size, 72);
ABI5_OFFSET(trevrpc_config, stream_recv_window, 80);
ABI5_OFFSET(trevrpc_config, conn_flow_control_window, 84);
ABI5_OFFSET(trevrpc_config, msquic_execution_profile, 88);
ABI5_OFFSET(trevrpc_config, msquic_send_buffering_enabled, 92);

ABI5_LAYOUT(trevrpc_server_config, 168);
ABI5_OFFSET(trevrpc_server_config, host, 0);
ABI5_OFFSET(trevrpc_server_config, port, 8);
ABI5_OFFSET(trevrpc_server_config, cert_file, 16);
ABI5_OFFSET(trevrpc_server_config, key_file, 24);
ABI5_OFFSET(trevrpc_server_config, webtransport_path, 32);
ABI5_OFFSET(trevrpc_server_config, webtransport_origin, 40);
ABI5_OFFSET(trevrpc_server_config, webtransport_admission, 48);
ABI5_OFFSET(trevrpc_server_config, webtransport_admission_user_data, 56);
ABI5_OFFSET(trevrpc_server_config, enable_http3, 64);
ABI5_OFFSET(trevrpc_server_config, http3_path, 72);
ABI5_OFFSET(trevrpc_server_config, http3_admission, 80);
ABI5_OFFSET(trevrpc_server_config, http3_admission_user_data, 88);
ABI5_OFFSET(trevrpc_server_config, max_idle_timeout_ms, 96);
ABI5_OFFSET(trevrpc_server_config, keep_alive_ms, 104);
ABI5_OFFSET(trevrpc_server_config, peer_bidi_stream_count, 108);
ABI5_OFFSET(trevrpc_server_config, max_stateless_operations, 112);
ABI5_OFFSET(trevrpc_server_config, max_binding_stateless_operations, 116);
ABI5_OFFSET(trevrpc_server_config, max_pending_send_bytes, 120);
ABI5_OFFSET(trevrpc_server_config, max_pending_send_count, 128);
ABI5_OFFSET(trevrpc_server_config, max_sessions_per_connection, 136);
ABI5_OFFSET(trevrpc_server_config, max_streams_per_session, 140);
ABI5_OFFSET(trevrpc_server_config, stream_recv_window, 144);
ABI5_OFFSET(trevrpc_server_config, conn_flow_control_window, 148);
ABI5_OFFSET(trevrpc_server_config, max_frame_size, 152);
ABI5_OFFSET(trevrpc_server_config, msquic_execution_profile, 160);
ABI5_OFFSET(trevrpc_server_config, msquic_send_buffering_enabled, 164);

ABI5_LAYOUT(trevrpc_server_options, 80);
ABI5_OFFSET(trevrpc_server_options, max_concurrent_connections, 0);
ABI5_OFFSET(trevrpc_server_options, max_concurrent_streams_per_connection, 8);
ABI5_OFFSET(trevrpc_server_options, max_concurrent_requests, 16);
ABI5_OFFSET(trevrpc_server_options, worker_count, 24);
ABI5_OFFSET(trevrpc_server_options, worker_queue_capacity, 32);
ABI5_OFFSET(trevrpc_server_options, graceful_shutdown_timeout_nanos, 40);
ABI5_OFFSET(trevrpc_server_options, initial_request_timeout_nanos, 48);
ABI5_OFFSET(trevrpc_server_options, max_stream_messages, 56);
ABI5_OFFSET(trevrpc_server_options, max_stream_body_size, 64);
ABI5_OFFSET(trevrpc_server_options, stream_idle_timeout_nanos, 72);

ABI5_LAYOUT(trevrpc_metadata_entry, 32);
ABI5_OFFSET(trevrpc_metadata_entry, key, 0);
ABI5_OFFSET(trevrpc_metadata_entry, key_len, 8);
ABI5_OFFSET(trevrpc_metadata_entry, value, 16);
ABI5_OFFSET(trevrpc_metadata_entry, value_len, 24);
ABI5_LAYOUT(trevrpc_metadata, 16);
ABI5_OFFSET(trevrpc_metadata, entries, 0);
ABI5_OFFSET(trevrpc_metadata, entries_len, 8);
ABI5_LAYOUT(trevrpc_status, 24);
ABI5_OFFSET(trevrpc_status, code, 0);
ABI5_OFFSET(trevrpc_status, message, 8);
ABI5_OFFSET(trevrpc_status, message_len, 16);

ABI5_LAYOUT(trevrpc_request, 80);
ABI5_OFFSET(trevrpc_request, service, 0);
ABI5_OFFSET(trevrpc_request, service_len, 8);
ABI5_OFFSET(trevrpc_request, method, 16);
ABI5_OFFSET(trevrpc_request, method_len, 24);
ABI5_OFFSET(trevrpc_request, body, 32);
ABI5_OFFSET(trevrpc_request, body_len, 40);
ABI5_OFFSET(trevrpc_request, metadata, 48);
ABI5_OFFSET(trevrpc_request, kind, 64);
ABI5_OFFSET(trevrpc_request, version, 68);
ABI5_OFFSET(trevrpc_request, timeout_nanos, 72);
ABI5_LAYOUT(trevrpc_call_options, 56);
ABI5_OFFSET(trevrpc_call_options, metadata, 0);
ABI5_OFFSET(trevrpc_call_options, timeout_nanos, 8);
ABI5_OFFSET(trevrpc_call_options, cancellation, 16);
ABI5_OFFSET(trevrpc_call_options, max_response_body_size, 24);
ABI5_OFFSET(trevrpc_call_options, max_response_messages, 32);
ABI5_OFFSET(trevrpc_call_options, max_response_stream_body_size, 40);
ABI5_OFFSET(trevrpc_call_options, response_idle_timeout_nanos, 48);

ABI5_LAYOUT(trevrpc_response, 64);
ABI5_OFFSET(trevrpc_response, status, 0);
ABI5_OFFSET(trevrpc_response, message, 8);
ABI5_OFFSET(trevrpc_response, message_len, 16);
ABI5_OFFSET(trevrpc_response, body, 24);
ABI5_OFFSET(trevrpc_response, body_len, 32);
ABI5_OFFSET(trevrpc_response, metadata, 40);
ABI5_OFFSET(trevrpc_response, _body_owner, 56);
ABI5_LAYOUT(trevrpc_stream_frame, 64);
ABI5_OFFSET(trevrpc_stream_frame, kind, 0);
ABI5_OFFSET(trevrpc_stream_frame, status, 4);
ABI5_OFFSET(trevrpc_stream_frame, message, 8);
ABI5_OFFSET(trevrpc_stream_frame, message_len, 16);
ABI5_OFFSET(trevrpc_stream_frame, body, 24);
ABI5_OFFSET(trevrpc_stream_frame, body_len, 32);
ABI5_OFFSET(trevrpc_stream_frame, metadata, 40);
ABI5_OFFSET(trevrpc_stream_frame, _body_owner, 56);

static trevrpc_config (*default_config_fn)(void) = trevrpc_default_config;
static trevrpc_server_config (*default_server_config_fn)(void) = trevrpc_default_server_config;
static trevrpc_server_options (*default_server_options_fn)(void) = trevrpc_default_server_options;
static trevrpc_call_options (*default_call_options_fn)(void) = trevrpc_default_call_options;
static int (*server_listen_fn)(const trevrpc_server_config*, trevrpc_server**) = trevrpc_server_listen;
static void (*server_close_fn)(trevrpc_server*) = trevrpc_server_close;
static int (*call_respond_fn)(trevrpc_call*, trevrpc_response*) = trevrpc_call_respond;
static int (*stream_recv_fn)(trevrpc_stream*, trevrpc_stream_frame**) = trevrpc_stream_recv;
static void (*response_reset_fn)(trevrpc_response*) = trevrpc_response_reset;
static void (*stream_frame_reset_fn)(trevrpc_stream_frame*) = trevrpc_stream_frame_reset;
static int (*raw_connect_fn)(
    const char*, uint16_t, const trevrpc_config*, trevrpc_raw_client**) = trevrpc_raw_client_connect;
static int (*raw_call_request_fn)(
    trevrpc_raw_client*, const trevrpc_request*, trevrpc_response**) = trevrpc_raw_client_call_request;
static void (*raw_close_fn)(trevrpc_raw_client*) = trevrpc_raw_client_close;
static int (*server_set_options_fn)(trevrpc_server*, const trevrpc_server_options*) = trevrpc_server_set_options;
static int (*server_get_options_fn)(trevrpc_server*, trevrpc_server_options*) = trevrpc_server_get_options;
static int (*call_retain_fn)(trevrpc_call*) = trevrpc_call_retain;
static void (*call_release_fn)(trevrpc_call*) = trevrpc_call_release;
static int (*call_defer_fn)(trevrpc_call*) = trevrpc_call_defer;
static int (*response_set_status_fn)(trevrpc_response*, trevrpc_status) = trevrpc_response_set_status;
static int (*stream_send_status_fn)(trevrpc_stream*, uint32_t, const char*, size_t) = trevrpc_stream_send_status;
static void (*stream_close_fn)(trevrpc_stream*) = trevrpc_stream_close;

int main(void) {
    trevrpc_config config = default_config_fn();
    trevrpc_server_config server_config = default_server_config_fn();
    trevrpc_server_options server_options = default_server_options_fn();
    trevrpc_call_options call_options = default_call_options_fn();
    trevrpc_response response = {0};
    trevrpc_stream_frame frame = {0};

    response_reset_fn(&response);
    stream_frame_reset_fn(&frame);
    if (trevrpc_c_abi_version() != 5u || config.max_frame_size == 0 || server_config.max_frame_size == 0 ||
        server_options.worker_count <= 0 || call_options.max_response_messages <= 0) {
        return 1;
    }

    return server_listen_fn == NULL || server_close_fn == NULL || call_respond_fn == NULL || stream_recv_fn == NULL ||
           raw_connect_fn == NULL || raw_call_request_fn == NULL || raw_close_fn == NULL ||
           server_set_options_fn == NULL || server_get_options_fn == NULL || call_retain_fn == NULL ||
           call_release_fn == NULL || call_defer_fn == NULL || response_set_status_fn == NULL ||
           stream_send_status_fn == NULL || stream_close_fn == NULL;
}
