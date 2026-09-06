#include "trevrpc_rpc.h"
#ifdef TREVRPC_RPC_TEST_MSQUIC
#include "trevrpc_rpc_msquic.h"
#endif

#include <stddef.h>
#include <stdint.h>

#define LAYOUT(type, expected_size, expected_align)                                                                    \
    _Static_assert(sizeof(type) == (expected_size), #type " size");                                                    \
    _Static_assert(_Alignof(type) == (expected_align), #type " align")
#define OFFSET(type, field, expected) _Static_assert(offsetof(type, field) == (expected), #type "." #field)
#define SIGNATURE(name, return_type, ...)                                                                              \
    do {                                                                                                               \
        return_type (*function)(__VA_ARGS__) = name;                                                                   \
        if (function == NULL)                                                                                          \
            return 1;                                                                                                  \
    } while (0)

_Static_assert(TREVRPC_RPC_EVENT_HTTP3_ADMISSION == 18u, "HTTP/3 admission event");
_Static_assert(TREVRPC_RPC_EVENT_WEBTRANSPORT_ADMISSION == 19u, "WebTransport admission event");
_Static_assert(TREVRPC_RPC_CALL_CONTEXT_HAS_DEADLINE == 1u, "call context deadline flag");
_Static_assert(TREVRPC_RPC_CALL_CONTEXT_DEADLINE_EXPIRED == 2u, "call context expired flag");
_Static_assert(TREVRPC_RPC_CALL_CONTEXT_CANCELLED == 4u, "call context cancelled flag");
_Static_assert(TREVRPC_RPC_ADMISSION_PROTOCOL_HTTP3 == 1u, "HTTP/3 admission protocol");
_Static_assert(TREVRPC_RPC_ADMISSION_PROTOCOL_WEBTRANSPORT == 2u, "WebTransport admission protocol");
_Static_assert(TREVRPC_RPC_ADMISSION_FLAG_SECURE == 1u, "secure admission flag");

LAYOUT(trevrpc_rpc_endpoint_v1, 16, 8);
LAYOUT(trevrpc_rpc_call_v1, 16, 8);
LAYOUT(trevrpc_rpc_stream_v1, 16, 8);
LAYOUT(trevrpc_rpc_cancellation_v1, 16, 8);
LAYOUT(trevrpc_rpc_runtime_config_v1, 128, 8);
LAYOUT(trevrpc_rpc_wake_source_v1, 48, 8);
LAYOUT(trevrpc_rpc_metadata_entry_v1, 32, 8);
LAYOUT(trevrpc_rpc_call_config_v1, 152, 8);
LAYOUT(trevrpc_rpc_call_context_info_v1, 56, 8);
LAYOUT(trevrpc_rpc_admission_info_v1, 112, 8);
LAYOUT(trevrpc_rpc_status_v1, 72, 8);
LAYOUT(trevrpc_rpc_event_info_v1, 192, 8);
LAYOUT(trevrpc_rpc_receive_info_v1, 96, 8);
LAYOUT(trevrpc_rpc_diagnostics_v1, 208, 8);
#ifdef TREVRPC_RPC_TEST_MSQUIC
_Static_assert(TREVRPC_RPC_MSQUIC_ENABLE_ADMISSION_EVENTS == 0x00000004u, "MsQuic admission events flag");
LAYOUT(trevrpc_rpc_msquic_config_v1, 64, 8);
LAYOUT(trevrpc_rpc_msquic_endpoint_config_v1, 296, 8);
#endif

#define HANDLE_OFFSETS(type)                                                                                           \
    OFFSET(type, owner, 0);                                                                                            \
    OFFSET(type, slot, 8);                                                                                             \
    OFFSET(type, generation, 12)
HANDLE_OFFSETS(trevrpc_rpc_endpoint_v1);
HANDLE_OFFSETS(trevrpc_rpc_call_v1);
HANDLE_OFFSETS(trevrpc_rpc_stream_v1);
HANDLE_OFFSETS(trevrpc_rpc_cancellation_v1);

OFFSET(trevrpc_rpc_runtime_config_v1, struct_size, 0);
OFFSET(trevrpc_rpc_runtime_config_v1, struct_version, 4);
OFFSET(trevrpc_rpc_runtime_config_v1, event_capacity, 8);
OFFSET(trevrpc_rpc_runtime_config_v1, endpoint_capacity, 12);
OFFSET(trevrpc_rpc_runtime_config_v1, call_capacity, 16);
OFFSET(trevrpc_rpc_runtime_config_v1, stream_capacity, 20);
OFFSET(trevrpc_rpc_runtime_config_v1, max_receive_owned_count, 24);
OFFSET(trevrpc_rpc_runtime_config_v1, flags, 28);
OFFSET(trevrpc_rpc_runtime_config_v1, max_metadata_count, 32);
OFFSET(trevrpc_rpc_runtime_config_v1, max_status_message_size, 36);
OFFSET(trevrpc_rpc_runtime_config_v1, max_receive_owned_bytes, 40);
OFFSET(trevrpc_rpc_runtime_config_v1, max_message_size, 48);
OFFSET(trevrpc_rpc_runtime_config_v1, max_metadata_bytes, 56);
OFFSET(trevrpc_rpc_runtime_config_v1, initial_request_timeout_nanos, 64);
OFFSET(trevrpc_rpc_runtime_config_v1, max_stream_messages, 72);
OFFSET(trevrpc_rpc_runtime_config_v1, max_stream_body_size, 80);
OFFSET(trevrpc_rpc_runtime_config_v1, stream_idle_timeout_nanos, 88);
OFFSET(trevrpc_rpc_runtime_config_v1, reserved, 96);

OFFSET(trevrpc_rpc_wake_source_v1, struct_size, 0);
OFFSET(trevrpc_rpc_wake_source_v1, struct_version, 4);
OFFSET(trevrpc_rpc_wake_source_v1, kind, 8);
OFFSET(trevrpc_rpc_wake_source_v1, flags, 12);
OFFSET(trevrpc_rpc_wake_source_v1, native_handle, 16);
OFFSET(trevrpc_rpc_wake_source_v1, reserved, 24);

OFFSET(trevrpc_rpc_metadata_entry_v1, key, 0);
OFFSET(trevrpc_rpc_metadata_entry_v1, key_len, 8);
OFFSET(trevrpc_rpc_metadata_entry_v1, reserved0, 12);
OFFSET(trevrpc_rpc_metadata_entry_v1, value, 16);
OFFSET(trevrpc_rpc_metadata_entry_v1, value_len, 24);

OFFSET(trevrpc_rpc_call_config_v1, struct_size, 0);
OFFSET(trevrpc_rpc_call_config_v1, struct_version, 4);
OFFSET(trevrpc_rpc_call_config_v1, kind, 8);
OFFSET(trevrpc_rpc_call_config_v1, flags, 12);
OFFSET(trevrpc_rpc_call_config_v1, service, 16);
OFFSET(trevrpc_rpc_call_config_v1, service_len, 24);
OFFSET(trevrpc_rpc_call_config_v1, reserved0, 28);
OFFSET(trevrpc_rpc_call_config_v1, method, 32);
OFFSET(trevrpc_rpc_call_config_v1, method_len, 40);
OFFSET(trevrpc_rpc_call_config_v1, metadata_count, 44);
OFFSET(trevrpc_rpc_call_config_v1, metadata, 48);
OFFSET(trevrpc_rpc_call_config_v1, timeout_nanos, 56);
OFFSET(trevrpc_rpc_call_config_v1, cancellation, 64);
OFFSET(trevrpc_rpc_call_config_v1, initial_message, 80);
OFFSET(trevrpc_rpc_call_config_v1, initial_message_len, 88);
OFFSET(trevrpc_rpc_call_config_v1, max_response_body_size, 96);
OFFSET(trevrpc_rpc_call_config_v1, max_response_messages, 104);
OFFSET(trevrpc_rpc_call_config_v1, max_response_stream_body_size, 112);
OFFSET(trevrpc_rpc_call_config_v1, response_idle_timeout_nanos, 120);
OFFSET(trevrpc_rpc_call_config_v1, reserved, 128);

OFFSET(trevrpc_rpc_call_context_info_v1, struct_size, 0);
OFFSET(trevrpc_rpc_call_context_info_v1, struct_version, 4);
OFFSET(trevrpc_rpc_call_context_info_v1, flags, 8);
OFFSET(trevrpc_rpc_call_context_info_v1, reserved0, 12);
OFFSET(trevrpc_rpc_call_context_info_v1, time_remaining_nanos, 16);
OFFSET(trevrpc_rpc_call_context_info_v1, reserved, 24);

OFFSET(trevrpc_rpc_admission_info_v1, struct_size, 0);
OFFSET(trevrpc_rpc_admission_info_v1, struct_version, 4);
OFFSET(trevrpc_rpc_admission_info_v1, protocol, 8);
OFFSET(trevrpc_rpc_admission_info_v1, flags, 12);
OFFSET(trevrpc_rpc_admission_info_v1, listener, 16);
OFFSET(trevrpc_rpc_admission_info_v1, path, 32);
OFFSET(trevrpc_rpc_admission_info_v1, path_len, 40);
OFFSET(trevrpc_rpc_admission_info_v1, authority, 48);
OFFSET(trevrpc_rpc_admission_info_v1, authority_len, 56);
OFFSET(trevrpc_rpc_admission_info_v1, origin, 64);
OFFSET(trevrpc_rpc_admission_info_v1, origin_len, 72);
OFFSET(trevrpc_rpc_admission_info_v1, reserved, 80);

OFFSET(trevrpc_rpc_status_v1, struct_size, 0);
OFFSET(trevrpc_rpc_status_v1, struct_version, 4);
OFFSET(trevrpc_rpc_status_v1, code, 8);
OFFSET(trevrpc_rpc_status_v1, flags, 12);
OFFSET(trevrpc_rpc_status_v1, message, 16);
OFFSET(trevrpc_rpc_status_v1, message_len, 24);
OFFSET(trevrpc_rpc_status_v1, metadata_count, 28);
OFFSET(trevrpc_rpc_status_v1, metadata, 32);
OFFSET(trevrpc_rpc_status_v1, reserved, 40);

OFFSET(trevrpc_rpc_event_info_v1, struct_size, 0);
OFFSET(trevrpc_rpc_event_info_v1, struct_version, 4);
OFFSET(trevrpc_rpc_event_info_v1, kind, 8);
OFFSET(trevrpc_rpc_event_info_v1, flags, 12);
OFFSET(trevrpc_rpc_event_info_v1, status, 16);
OFFSET(trevrpc_rpc_event_info_v1, subject_kind, 20);
OFFSET(trevrpc_rpc_event_info_v1, sequence, 24);
OFFSET(trevrpc_rpc_event_info_v1, endpoint, 32);
OFFSET(trevrpc_rpc_event_info_v1, call, 48);
OFFSET(trevrpc_rpc_event_info_v1, stream, 64);
OFFSET(trevrpc_rpc_event_info_v1, cancellation, 80);
OFFSET(trevrpc_rpc_event_info_v1, operation_id, 96);
OFFSET(trevrpc_rpc_event_info_v1, rpc_status, 104);
OFFSET(trevrpc_rpc_event_info_v1, rpc_kind, 108);
OFFSET(trevrpc_rpc_event_info_v1, service, 112);
OFFSET(trevrpc_rpc_event_info_v1, service_len, 120);
OFFSET(trevrpc_rpc_event_info_v1, reserved0, 124);
OFFSET(trevrpc_rpc_event_info_v1, method, 128);
OFFSET(trevrpc_rpc_event_info_v1, method_len, 136);
OFFSET(trevrpc_rpc_event_info_v1, reserved1, 140);
OFFSET(trevrpc_rpc_event_info_v1, application_error_code, 144);
OFFSET(trevrpc_rpc_event_info_v1, provider_error_code, 152);
OFFSET(trevrpc_rpc_event_info_v1, reserved, 160);

OFFSET(trevrpc_rpc_receive_info_v1, struct_size, 0);
OFFSET(trevrpc_rpc_receive_info_v1, struct_version, 4);
OFFSET(trevrpc_rpc_receive_info_v1, kind, 8);
OFFSET(trevrpc_rpc_receive_info_v1, flags, 12);
OFFSET(trevrpc_rpc_receive_info_v1, rpc_status, 16);
OFFSET(trevrpc_rpc_receive_info_v1, metadata_count, 20);
OFFSET(trevrpc_rpc_receive_info_v1, data, 24);
OFFSET(trevrpc_rpc_receive_info_v1, data_len, 32);
OFFSET(trevrpc_rpc_receive_info_v1, message, 40);
OFFSET(trevrpc_rpc_receive_info_v1, message_len, 48);
OFFSET(trevrpc_rpc_receive_info_v1, reserved0, 52);
OFFSET(trevrpc_rpc_receive_info_v1, metadata, 56);
OFFSET(trevrpc_rpc_receive_info_v1, reserved, 64);

OFFSET(trevrpc_rpc_diagnostics_v1, struct_size, 0);
OFFSET(trevrpc_rpc_diagnostics_v1, struct_version, 4);
OFFSET(trevrpc_rpc_diagnostics_v1, rpc_abi_version, 8);
OFFSET(trevrpc_rpc_diagnostics_v1, state, 12);
OFFSET(trevrpc_rpc_diagnostics_v1, terminal_status, 16);
OFFSET(trevrpc_rpc_diagnostics_v1, event_capacity, 20);
OFFSET(trevrpc_rpc_diagnostics_v1, queue_depth, 24);
OFFSET(trevrpc_rpc_diagnostics_v1, ordinary_queue_depth, 28);
OFFSET(trevrpc_rpc_diagnostics_v1, events_enqueued, 32);
OFFSET(trevrpc_rpc_diagnostics_v1, events_dequeued, 40);
OFFSET(trevrpc_rpc_diagnostics_v1, events_rejected, 48);
OFFSET(trevrpc_rpc_diagnostics_v1, receive_owned_count, 56);
OFFSET(trevrpc_rpc_diagnostics_v1, peak_receive_owned_count, 64);
OFFSET(trevrpc_rpc_diagnostics_v1, receive_owned_bytes, 72);
OFFSET(trevrpc_rpc_diagnostics_v1, peak_receive_owned_bytes, 80);
OFFSET(trevrpc_rpc_diagnostics_v1, pending_send_bytes, 88);
OFFSET(trevrpc_rpc_diagnostics_v1, pending_send_count, 96);
OFFSET(trevrpc_rpc_diagnostics_v1, live_endpoints, 104);
OFFSET(trevrpc_rpc_diagnostics_v1, live_calls, 112);
OFFSET(trevrpc_rpc_diagnostics_v1, live_streams, 120);
OFFSET(trevrpc_rpc_diagnostics_v1, active_api_calls, 128);
OFFSET(trevrpc_rpc_diagnostics_v1, active_callbacks, 136);
OFFSET(trevrpc_rpc_diagnostics_v1, wake_signals, 144);
OFFSET(trevrpc_rpc_diagnostics_v1, wake_write_eagain, 152);
OFFSET(trevrpc_rpc_diagnostics_v1, wake_failures, 160);
OFFSET(trevrpc_rpc_diagnostics_v1, provider_error_code, 168);
OFFSET(trevrpc_rpc_diagnostics_v1, mandatory_reservations, 176);
OFFSET(trevrpc_rpc_diagnostics_v1, reserved, 184);

#ifdef TREVRPC_RPC_TEST_MSQUIC
OFFSET(trevrpc_rpc_msquic_config_v1, struct_size, 0);
OFFSET(trevrpc_rpc_msquic_config_v1, struct_version, 4);
OFFSET(trevrpc_rpc_msquic_config_v1, flags, 8);
OFFSET(trevrpc_rpc_msquic_config_v1, reserved0, 12);
OFFSET(trevrpc_rpc_msquic_config_v1, reserved, 16);

OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, struct_size, 0);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, struct_version, 4);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, mode, 8);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, transport, 12);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, host, 16);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, host_len, 24);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, port, 28);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, peer_bidi_stream_count, 30);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, server_name, 32);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, server_name_len, 40);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, flags, 44);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, cert_file, 48);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, cert_file_len, 56);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, reserved0, 60);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, key_file, 64);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, key_file_len, 72);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, reserved1, 76);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, ca_cert_file, 80);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, ca_cert_file_len, 88);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, reserved2, 92);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, cert_data, 96);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, cert_data_len, 104);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, key_data, 112);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, key_data_len, 120);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, ca_cert_data, 128);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, ca_cert_data_len, 136);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, path, 144);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, path_len, 152);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, webtransport_profiles, 156);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, origin, 160);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, origin_len, 168);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, max_sessions, 172);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, max_frame_size, 176);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, max_field_section_size, 184);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, max_pending_send_bytes, 192);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, max_pending_receive_bytes, 200);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, max_idle_timeout_ms, 208);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, max_pending_send_count, 216);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, max_pending_receive_count, 220);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, keep_alive_ms, 224);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, stream_recv_window, 228);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, conn_flow_control_window, 232);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, unresolved_stream_count, 236);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, unresolved_stream_bytes, 240);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, unresolved_stream_timeout_ms, 248);
OFFSET(trevrpc_rpc_msquic_endpoint_config_v1, reserved, 256);
#endif

static int check_signatures(void) {
    SIGNATURE(trevrpc_rpc_abi_version, uint32_t, void);
    SIGNATURE(trevrpc_rpc_abi_1_anchor, void, void);
    SIGNATURE(trevrpc_rpc_runtime_config_v1_init, int, trevrpc_rpc_runtime_config_v1*, size_t);
    SIGNATURE(trevrpc_rpc_wake_source_v1_init, int, trevrpc_rpc_wake_source_v1*, size_t);
    SIGNATURE(trevrpc_rpc_call_config_v1_init, int, trevrpc_rpc_call_config_v1*, size_t);
    SIGNATURE(trevrpc_rpc_call_context_info_v1_init, int, trevrpc_rpc_call_context_info_v1*, size_t);
    SIGNATURE(trevrpc_rpc_admission_info_v1_init, int, trevrpc_rpc_admission_info_v1*, size_t);
    SIGNATURE(trevrpc_rpc_status_v1_init, int, trevrpc_rpc_status_v1*, size_t);
    SIGNATURE(trevrpc_rpc_event_info_v1_init, int, trevrpc_rpc_event_info_v1*, size_t);
    SIGNATURE(trevrpc_rpc_receive_info_v1_init, int, trevrpc_rpc_receive_info_v1*, size_t);
    SIGNATURE(trevrpc_rpc_diagnostics_v1_init, int, trevrpc_rpc_diagnostics_v1*, size_t);
    SIGNATURE(trevrpc_rpc_runtime_get_wake_source_v1, int, trevrpc_rpc_runtime*, trevrpc_rpc_wake_source_v1*);
    SIGNATURE(trevrpc_rpc_runtime_next_event, int, trevrpc_rpc_runtime*, trevrpc_rpc_event**);
    SIGNATURE(trevrpc_rpc_event_get_info_v1, int, const trevrpc_rpc_event*, trevrpc_rpc_event_info_v1*);
    SIGNATURE(trevrpc_rpc_event_get_admission_info_v1, int, const trevrpc_rpc_event*, trevrpc_rpc_admission_info_v1*);
    SIGNATURE(trevrpc_rpc_admission_respond_v1, int, const trevrpc_rpc_event*, uint16_t);
    SIGNATURE(trevrpc_rpc_event_take_incoming_call,
        int,
        trevrpc_rpc_event*,
        trevrpc_rpc_call_v1*,
        trevrpc_rpc_stream_v1*,
        trevrpc_rpc_receive**);
    SIGNATURE(trevrpc_rpc_event_release, void, trevrpc_rpc_event*);
    SIGNATURE(trevrpc_rpc_receive_get_info_v1, int, const trevrpc_rpc_receive*, trevrpc_rpc_receive_info_v1*);
    SIGNATURE(trevrpc_rpc_receive_release, void, trevrpc_rpc_receive*);
    SIGNATURE(trevrpc_rpc_runtime_get_diagnostics_v1, int, trevrpc_rpc_runtime*, trevrpc_rpc_diagnostics_v1*);
    SIGNATURE(trevrpc_rpc_call_get_context_v1,
        int,
        trevrpc_rpc_runtime*,
        trevrpc_rpc_call_v1,
        trevrpc_rpc_call_context_info_v1*);
    SIGNATURE(trevrpc_rpc_endpoint_get_port_v1, int, trevrpc_rpc_runtime*, trevrpc_rpc_endpoint_v1, uint16_t*);
    SIGNATURE(trevrpc_rpc_call_open_v1,
        int,
        trevrpc_rpc_runtime*,
        trevrpc_rpc_endpoint_v1,
        const trevrpc_rpc_call_config_v1*,
        uint64_t,
        trevrpc_rpc_call_v1*,
        trevrpc_rpc_stream_v1*);
    SIGNATURE(trevrpc_rpc_call_accept, int, trevrpc_rpc_runtime*, trevrpc_rpc_call_v1, uint64_t);
    SIGNATURE(trevrpc_rpc_stream_send_copy_v1,
        int,
        trevrpc_rpc_runtime*,
        trevrpc_rpc_stream_v1,
        uint64_t,
        const uint8_t*,
        size_t,
        uint32_t);
    SIGNATURE(trevrpc_rpc_stream_receive, int, trevrpc_rpc_runtime*, trevrpc_rpc_stream_v1, trevrpc_rpc_receive**);
    SIGNATURE(trevrpc_rpc_stream_finish_send, int, trevrpc_rpc_runtime*, trevrpc_rpc_stream_v1, uint64_t);
    SIGNATURE(trevrpc_rpc_call_respond_copy_v1,
        int,
        trevrpc_rpc_runtime*,
        trevrpc_rpc_call_v1,
        uint64_t,
        const trevrpc_rpc_status_v1*,
        const uint8_t*,
        size_t);
    SIGNATURE(trevrpc_rpc_call_finish_v1,
        int,
        trevrpc_rpc_runtime*,
        trevrpc_rpc_call_v1,
        uint64_t,
        const trevrpc_rpc_status_v1*);
    SIGNATURE(trevrpc_rpc_call_cancel, int, trevrpc_rpc_runtime*, trevrpc_rpc_call_v1, uint64_t, uint64_t);
    SIGNATURE(trevrpc_rpc_stream_close, int, trevrpc_rpc_runtime*, trevrpc_rpc_stream_v1, uint64_t, uint32_t, uint64_t);
    SIGNATURE(trevrpc_rpc_call_close, int, trevrpc_rpc_runtime*, trevrpc_rpc_call_v1, uint64_t, uint32_t, uint64_t);
    SIGNATURE(trevrpc_rpc_endpoint_close, int, trevrpc_rpc_runtime*, trevrpc_rpc_endpoint_v1, uint64_t);
    SIGNATURE(trevrpc_rpc_stream_release, int, trevrpc_rpc_runtime*, trevrpc_rpc_stream_v1);
    SIGNATURE(trevrpc_rpc_call_release, int, trevrpc_rpc_runtime*, trevrpc_rpc_call_v1);
    SIGNATURE(trevrpc_rpc_endpoint_release, int, trevrpc_rpc_runtime*, trevrpc_rpc_endpoint_v1);
    SIGNATURE(trevrpc_rpc_cancellation_create, int, trevrpc_rpc_runtime*, trevrpc_rpc_cancellation_v1*);
    SIGNATURE(trevrpc_rpc_cancellation_cancel, int, trevrpc_rpc_runtime*, trevrpc_rpc_cancellation_v1, uint64_t);
    SIGNATURE(trevrpc_rpc_cancellation_release, int, trevrpc_rpc_runtime*, trevrpc_rpc_cancellation_v1);
    SIGNATURE(trevrpc_rpc_runtime_close, int, trevrpc_rpc_runtime*, uint64_t);
    SIGNATURE(trevrpc_rpc_runtime_drain, int, trevrpc_rpc_runtime*);
    SIGNATURE(trevrpc_rpc_runtime_release, int, trevrpc_rpc_runtime*);
#ifdef TREVRPC_RPC_TEST_MSQUIC
    SIGNATURE(trevrpc_rpc_msquic_abi_version, uint32_t, void);
    SIGNATURE(trevrpc_rpc_msquic_abi_1_anchor, void, void);
    SIGNATURE(trevrpc_rpc_msquic_config_v1_init, int, trevrpc_rpc_msquic_config_v1*, size_t);
    SIGNATURE(trevrpc_rpc_msquic_endpoint_config_v1_init, int, trevrpc_rpc_msquic_endpoint_config_v1*, size_t);
    SIGNATURE(trevrpc_rpc_msquic_create_v1,
        int,
        const trevrpc_rpc_runtime_config_v1*,
        const trevrpc_rpc_msquic_config_v1*,
        trevrpc_rpc_runtime**);
    SIGNATURE(trevrpc_rpc_msquic_endpoint_start_v1,
        int,
        trevrpc_rpc_runtime*,
        const trevrpc_rpc_msquic_endpoint_config_v1*,
        uint64_t,
        trevrpc_rpc_endpoint_v1*);
#endif
    return 0;
}

int main(void) {
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_wake_source_v1 wake_source;
    trevrpc_rpc_call_config_v1 call_config;
    trevrpc_rpc_call_context_info_v1 call_context_info;
    trevrpc_rpc_admission_info_v1 admission_info;
    trevrpc_rpc_status_v1 status;
    trevrpc_rpc_event_info_v1 event_info;
    trevrpc_rpc_receive_info_v1 receive_info;
    trevrpc_rpc_diagnostics_v1 diagnostics;
#ifdef TREVRPC_RPC_TEST_MSQUIC
    trevrpc_rpc_msquic_config_v1 provider_config;
    trevrpc_rpc_msquic_endpoint_config_v1 endpoint_config;
#endif

    if (TREVRPC_RPC_ABI_VERSION != 1u || check_signatures() != 0 ||
        trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) != 0 ||
        trevrpc_rpc_wake_source_v1_init(&wake_source, sizeof(wake_source)) != 0 ||
        trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) != 0 ||
        trevrpc_rpc_call_context_info_v1_init(&call_context_info, sizeof(call_context_info)) != 0 ||
        trevrpc_rpc_admission_info_v1_init(&admission_info, sizeof(admission_info)) != 0 ||
        trevrpc_rpc_status_v1_init(&status, sizeof(status)) != 0 ||
        trevrpc_rpc_event_info_v1_init(&event_info, sizeof(event_info)) != 0 ||
        trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) != 0 ||
        trevrpc_rpc_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) != 0
#ifdef TREVRPC_RPC_TEST_MSQUIC
        || TREVRPC_RPC_MSQUIC_ABI_VERSION != 1u ||
        trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config)) != 0 ||
        trevrpc_rpc_msquic_endpoint_config_v1_init(&endpoint_config, sizeof(endpoint_config)) != 0
#endif
    ) {
        return 1;
    }

    return runtime_config.max_metadata_count == TREVRPC_RPC_DEFAULT_MAX_METADATA_COUNT &&
                   runtime_config.max_metadata_bytes == TREVRPC_RPC_DEFAULT_MAX_METADATA_BYTES &&
                   runtime_config.max_status_message_size == TREVRPC_RPC_DEFAULT_MAX_STATUS_MESSAGE_SIZE &&
                   runtime_config.initial_request_timeout_nanos == 0 && runtime_config.max_stream_messages == -1 &&
                   runtime_config.max_stream_body_size == -1 && runtime_config.stream_idle_timeout_nanos == 0 &&
                   call_config.timeout_nanos == TREVRPC_RPC_DEADLINE_INFINITE &&
                   call_config.max_response_body_size == -1 && call_config.max_response_messages == -1 &&
                   call_config.max_response_stream_body_size == -1 && call_config.response_idle_timeout_nanos == 0 &&
                   status.code == TREVRPC_RPC_STATUS_OK
#ifdef TREVRPC_RPC_TEST_MSQUIC
                   && endpoint_config.webtransport_profiles == TREVRPC_RPC_MSQUIC_PROFILE_ALL_SUPPORTED
#endif
               ? 0
               : 1;
}
