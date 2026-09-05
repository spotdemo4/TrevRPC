#include "trevrpc_rpc.h"
#ifdef TREVRPC_RPC_TEST_MSQUIC
#include "trevrpc_rpc_msquic.h"
#endif

#include <cstddef>
#include <cstdint>
#include <type_traits>

#define LAYOUT(type, expected_size, expected_align)                                                                    \
    static_assert(sizeof(type) == (expected_size), #type " size");                                                     \
    static_assert(alignof(type) == (expected_align), #type " align");                                                  \
    static_assert(std::is_standard_layout_v<type>, #type " standard layout")
#define OFFSET(type, field, expected) static_assert(offsetof(type, field) == (expected), #type "." #field)
#define SIGNATURE(name, ...) static_assert(std::is_same_v<decltype(&name), __VA_ARGS__>, #name " signature")

LAYOUT(trevrpc_rpc_endpoint_v1, 16, 8);
LAYOUT(trevrpc_rpc_call_v1, 16, 8);
LAYOUT(trevrpc_rpc_stream_v1, 16, 8);
LAYOUT(trevrpc_rpc_cancellation_v1, 16, 8);
LAYOUT(trevrpc_rpc_runtime_config_v1, 96, 8);
LAYOUT(trevrpc_rpc_wake_source_v1, 48, 8);
LAYOUT(trevrpc_rpc_metadata_entry_v1, 32, 8);
LAYOUT(trevrpc_rpc_call_config_v1, 120, 8);
LAYOUT(trevrpc_rpc_status_v1, 72, 8);
LAYOUT(trevrpc_rpc_event_info_v1, 192, 8);
LAYOUT(trevrpc_rpc_receive_info_v1, 96, 8);
LAYOUT(trevrpc_rpc_diagnostics_v1, 208, 8);
#ifdef TREVRPC_RPC_TEST_MSQUIC
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
OFFSET(trevrpc_rpc_runtime_config_v1, reserved, 64);

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
OFFSET(trevrpc_rpc_call_config_v1, reserved, 96);

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

SIGNATURE(trevrpc_rpc_abi_version, std::uint32_t (*)(void));
SIGNATURE(trevrpc_rpc_abi_1_anchor, void (*)(void));
SIGNATURE(trevrpc_rpc_runtime_config_v1_init, int (*)(trevrpc_rpc_runtime_config_v1*, std::size_t));
SIGNATURE(trevrpc_rpc_wake_source_v1_init, int (*)(trevrpc_rpc_wake_source_v1*, std::size_t));
SIGNATURE(trevrpc_rpc_call_config_v1_init, int (*)(trevrpc_rpc_call_config_v1*, std::size_t));
SIGNATURE(trevrpc_rpc_status_v1_init, int (*)(trevrpc_rpc_status_v1*, std::size_t));
SIGNATURE(trevrpc_rpc_event_info_v1_init, int (*)(trevrpc_rpc_event_info_v1*, std::size_t));
SIGNATURE(trevrpc_rpc_receive_info_v1_init, int (*)(trevrpc_rpc_receive_info_v1*, std::size_t));
SIGNATURE(trevrpc_rpc_diagnostics_v1_init, int (*)(trevrpc_rpc_diagnostics_v1*, std::size_t));
SIGNATURE(trevrpc_rpc_runtime_get_wake_source_v1, int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_wake_source_v1*));
SIGNATURE(trevrpc_rpc_runtime_next_event, int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_event**));
SIGNATURE(trevrpc_rpc_event_get_info_v1, int (*)(const trevrpc_rpc_event*, trevrpc_rpc_event_info_v1*));
SIGNATURE(trevrpc_rpc_event_take_incoming_call,
    int (*)(trevrpc_rpc_event*, trevrpc_rpc_call_v1*, trevrpc_rpc_stream_v1*, trevrpc_rpc_receive**));
SIGNATURE(trevrpc_rpc_event_release, void (*)(trevrpc_rpc_event*));
SIGNATURE(trevrpc_rpc_receive_get_info_v1, int (*)(const trevrpc_rpc_receive*, trevrpc_rpc_receive_info_v1*));
SIGNATURE(trevrpc_rpc_receive_release, void (*)(trevrpc_rpc_receive*));
SIGNATURE(trevrpc_rpc_runtime_get_diagnostics_v1, int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_diagnostics_v1*));
SIGNATURE(trevrpc_rpc_endpoint_get_port_v1, int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_endpoint_v1, std::uint16_t*));
SIGNATURE(trevrpc_rpc_call_open_v1,
    int (*)(trevrpc_rpc_runtime*,
        trevrpc_rpc_endpoint_v1,
        const trevrpc_rpc_call_config_v1*,
        std::uint64_t,
        trevrpc_rpc_call_v1*,
        trevrpc_rpc_stream_v1*));
SIGNATURE(trevrpc_rpc_call_accept, int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_call_v1, std::uint64_t));
SIGNATURE(trevrpc_rpc_stream_send_copy_v1,
    int (*)(
        trevrpc_rpc_runtime*, trevrpc_rpc_stream_v1, std::uint64_t, const std::uint8_t*, std::size_t, std::uint32_t));
SIGNATURE(trevrpc_rpc_stream_receive, int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_stream_v1, trevrpc_rpc_receive**));
SIGNATURE(trevrpc_rpc_stream_finish_send, int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_stream_v1, std::uint64_t));
SIGNATURE(trevrpc_rpc_call_respond_copy_v1,
    int (*)(trevrpc_rpc_runtime*,
        trevrpc_rpc_call_v1,
        std::uint64_t,
        const trevrpc_rpc_status_v1*,
        const std::uint8_t*,
        std::size_t));
SIGNATURE(trevrpc_rpc_call_finish_v1,
    int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_call_v1, std::uint64_t, const trevrpc_rpc_status_v1*));
SIGNATURE(trevrpc_rpc_call_cancel, int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_call_v1, std::uint64_t, std::uint64_t));
SIGNATURE(trevrpc_rpc_stream_close,
    int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_stream_v1, std::uint64_t, std::uint32_t, std::uint64_t));
SIGNATURE(trevrpc_rpc_call_close,
    int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_call_v1, std::uint64_t, std::uint32_t, std::uint64_t));
SIGNATURE(trevrpc_rpc_endpoint_close, int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_endpoint_v1, std::uint64_t));
SIGNATURE(trevrpc_rpc_stream_release, int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_stream_v1));
SIGNATURE(trevrpc_rpc_call_release, int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_call_v1));
SIGNATURE(trevrpc_rpc_endpoint_release, int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_endpoint_v1));
SIGNATURE(trevrpc_rpc_cancellation_create, int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_cancellation_v1*));
SIGNATURE(trevrpc_rpc_cancellation_cancel, int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_cancellation_v1, std::uint64_t));
SIGNATURE(trevrpc_rpc_cancellation_release, int (*)(trevrpc_rpc_runtime*, trevrpc_rpc_cancellation_v1));
SIGNATURE(trevrpc_rpc_runtime_close, int (*)(trevrpc_rpc_runtime*, std::uint64_t));
SIGNATURE(trevrpc_rpc_runtime_drain, int (*)(trevrpc_rpc_runtime*));
SIGNATURE(trevrpc_rpc_runtime_release, int (*)(trevrpc_rpc_runtime*));
#ifdef TREVRPC_RPC_TEST_MSQUIC
SIGNATURE(trevrpc_rpc_msquic_abi_version, std::uint32_t (*)(void));
SIGNATURE(trevrpc_rpc_msquic_abi_1_anchor, void (*)(void));
SIGNATURE(trevrpc_rpc_msquic_config_v1_init, int (*)(trevrpc_rpc_msquic_config_v1*, std::size_t));
SIGNATURE(trevrpc_rpc_msquic_endpoint_config_v1_init, int (*)(trevrpc_rpc_msquic_endpoint_config_v1*, std::size_t));
SIGNATURE(trevrpc_rpc_msquic_create_v1,
    int (*)(const trevrpc_rpc_runtime_config_v1*, const trevrpc_rpc_msquic_config_v1*, trevrpc_rpc_runtime**));
SIGNATURE(trevrpc_rpc_msquic_endpoint_start_v1,
    int (*)(
        trevrpc_rpc_runtime*, const trevrpc_rpc_msquic_endpoint_config_v1*, std::uint64_t, trevrpc_rpc_endpoint_v1*));
#endif

int main() {
    trevrpc_rpc_runtime_config_v1 config;
    return trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config));
}
