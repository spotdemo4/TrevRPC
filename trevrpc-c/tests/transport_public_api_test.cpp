#include "trevrpc_transport_provider.h"

#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <type_traits>

#define LAYOUT(type, expected_size, expected_align)                                                                    \
    static_assert(sizeof(type) == (expected_size), #type " size");                                                     \
    static_assert(alignof(type) == (expected_align), #type " align");                                                  \
    static_assert(std::is_standard_layout_v<type>, #type " standard layout")
#define OFFSET(type, field, expected) static_assert(offsetof(type, field) == (expected), #type "." #field)
#define SIGNATURE(name, ...) static_assert(std::is_same_v<decltype(&name), __VA_ARGS__>, #name " signature")

LAYOUT(trevrpc_transport_handle_v1, 16, 8);
LAYOUT(trevrpc_transport_config_v1, 80, 8);
LAYOUT(trevrpc_transport_endpoint_config_v1, 312, 8);
LAYOUT(trevrpc_transport_wake_source_v1, 48, 8);
LAYOUT(trevrpc_transport_event_info_v1, 136, 8);
LAYOUT(trevrpc_transport_receive_info_v1, 64, 8);
LAYOUT(trevrpc_transport_header_field_v1, 32, 8);
LAYOUT(trevrpc_transport_admission_info_v1, 144, 8);
LAYOUT(trevrpc_transport_event_protocol_info_v1, 48, 8);
LAYOUT(trevrpc_transport_diagnostics_v1, 208, 8);

OFFSET(trevrpc_transport_handle_v1, owner, 0);
OFFSET(trevrpc_transport_handle_v1, slot, 8);
OFFSET(trevrpc_transport_handle_v1, generation, 12);
OFFSET(trevrpc_transport_config_v1, max_receive_owned_bytes, 32);
OFFSET(trevrpc_transport_endpoint_config_v1, protocol, 8);
OFFSET(trevrpc_transport_endpoint_config_v1, host, 16);
OFFSET(trevrpc_transport_endpoint_config_v1, cert_data, 112);
OFFSET(trevrpc_transport_endpoint_config_v1, path, 160);
OFFSET(trevrpc_transport_endpoint_config_v1, max_frame_size, 192);
OFFSET(trevrpc_transport_endpoint_config_v1, unresolved_stream_bytes, 256);
OFFSET(trevrpc_transport_wake_source_v1, native_handle, 16);
OFFSET(trevrpc_transport_event_info_v1, sequence, 24);
OFFSET(trevrpc_transport_event_info_v1, subject, 32);
OFFSET(trevrpc_transport_event_info_v1, operation_id, 64);
OFFSET(trevrpc_transport_event_info_v1, data, 88);
OFFSET(trevrpc_transport_receive_info_v1, data, 16);
OFFSET(trevrpc_transport_header_field_v1, name, 0);
OFFSET(trevrpc_transport_header_field_v1, name_len, 8);
OFFSET(trevrpc_transport_header_field_v1, value, 16);
OFFSET(trevrpc_transport_header_field_v1, value_len, 24);
OFFSET(trevrpc_transport_admission_info_v1, protocol, 8);
OFFSET(trevrpc_transport_admission_info_v1, listener, 16);
OFFSET(trevrpc_transport_admission_info_v1, headers, 32);
OFFSET(trevrpc_transport_admission_info_v1, header_count, 40);
OFFSET(trevrpc_transport_admission_info_v1, method, 48);
OFFSET(trevrpc_transport_admission_info_v1, path, 64);
OFFSET(trevrpc_transport_admission_info_v1, authority, 80);
OFFSET(trevrpc_transport_admission_info_v1, origin, 96);
OFFSET(trevrpc_transport_event_protocol_info_v1, protocol, 8);
OFFSET(trevrpc_transport_diagnostics_v1, provider_error_code, 168);

SIGNATURE(trevrpc_transport_abi_version, std::uint32_t (*)(void));
SIGNATURE(trevrpc_transport_abi_1_anchor, void (*)(void));
SIGNATURE(trevrpc_transport_provider_abi_version, std::uint32_t (*)(void));
SIGNATURE(trevrpc_transport_provider_abi_1_anchor, void (*)(void));
SIGNATURE(
    trevrpc_transport_provider_descriptor_v1_init, int (*)(trevrpc_transport_provider_descriptor_v1*, std::size_t));
SIGNATURE(
    trevrpc_transport_provider_adopt_v1, int (*)(const trevrpc_transport_provider_descriptor_v1*, trevrpc_transport**));
SIGNATURE(trevrpc_transport_config_v1_init, int (*)(trevrpc_transport_config_v1*, std::size_t));
SIGNATURE(trevrpc_transport_endpoint_config_v1_init, int (*)(trevrpc_transport_endpoint_config_v1*, std::size_t));
SIGNATURE(trevrpc_transport_wake_source_v1_init, int (*)(trevrpc_transport_wake_source_v1*, std::size_t));
SIGNATURE(trevrpc_transport_event_info_v1_init, int (*)(trevrpc_transport_event_info_v1*, std::size_t));
SIGNATURE(trevrpc_transport_receive_info_v1_init, int (*)(trevrpc_transport_receive_info_v1*, std::size_t));
SIGNATURE(trevrpc_transport_admission_info_v1_init, int (*)(trevrpc_transport_admission_info_v1*, std::size_t));
SIGNATURE(
    trevrpc_transport_event_protocol_info_v1_init, int (*)(trevrpc_transport_event_protocol_info_v1*, std::size_t));
SIGNATURE(trevrpc_transport_diagnostics_v1_init, int (*)(trevrpc_transport_diagnostics_v1*, std::size_t));
SIGNATURE(trevrpc_transport_get_wake_sources_v1,
    int (*)(trevrpc_transport*, trevrpc_transport_wake_source_v1*, std::size_t, std::size_t*));
SIGNATURE(trevrpc_transport_poll_timeout_ms, int (*)(trevrpc_transport*));
SIGNATURE(trevrpc_transport_next_event, int (*)(trevrpc_transport*, trevrpc_transport_event_v1**));
SIGNATURE(trevrpc_transport_event_get_info_v1,
    int (*)(trevrpc_transport*, const trevrpc_transport_event_v1*, trevrpc_transport_event_info_v1*));
SIGNATURE(trevrpc_transport_event_get_admission_info_v1,
    int (*)(trevrpc_transport*, const trevrpc_transport_event_v1*, trevrpc_transport_admission_info_v1*));
SIGNATURE(trevrpc_transport_event_get_protocol_info_v1,
    int (*)(trevrpc_transport*, const trevrpc_transport_event_v1*, trevrpc_transport_event_protocol_info_v1*));
SIGNATURE(trevrpc_transport_admission_respond_v1,
    int (*)(trevrpc_transport*, const trevrpc_transport_event_v1*, std::uint16_t));
SIGNATURE(trevrpc_transport_event_release, void (*)(trevrpc_transport*, trevrpc_transport_event_v1*));
SIGNATURE(trevrpc_transport_receive_get_info_v1,
    int (*)(trevrpc_transport*, const trevrpc_transport_receive_v1*, trevrpc_transport_receive_info_v1*));
SIGNATURE(trevrpc_transport_receive_release, void (*)(trevrpc_transport*, trevrpc_transport_receive_v1*));
SIGNATURE(trevrpc_transport_get_diagnostics_v1, int (*)(trevrpc_transport*, trevrpc_transport_diagnostics_v1*));
SIGNATURE(trevrpc_transport_listen_v1,
    int (*)(trevrpc_transport*, const trevrpc_transport_endpoint_config_v1*, trevrpc_transport_handle_v1*));
SIGNATURE(
    trevrpc_transport_listener_get_port_v1, int (*)(trevrpc_transport*, trevrpc_transport_handle_v1, std::uint16_t*));
SIGNATURE(trevrpc_transport_dial_v1,
    int (*)(
        trevrpc_transport*, const trevrpc_transport_endpoint_config_v1*, std::uint64_t, trevrpc_transport_handle_v1*));
SIGNATURE(trevrpc_transport_dial_cancel, int (*)(trevrpc_transport*, trevrpc_transport_handle_v1));
SIGNATURE(trevrpc_transport_connection_open_bidi_stream_v1,
    int (*)(trevrpc_transport*, trevrpc_transport_handle_v1, std::uint64_t, trevrpc_transport_handle_v1*));
SIGNATURE(trevrpc_transport_stream_send_v1,
    int (*)(trevrpc_transport*, trevrpc_transport_handle_v1, std::uint64_t, const std::uint8_t*, std::size_t));
SIGNATURE(trevrpc_transport_stream_receive,
    int (*)(trevrpc_transport*, trevrpc_transport_handle_v1, trevrpc_transport_receive_v1**));
SIGNATURE(trevrpc_transport_stream_finish_send, int (*)(trevrpc_transport*, trevrpc_transport_handle_v1));
SIGNATURE(
    trevrpc_transport_stream_abort_receive, int (*)(trevrpc_transport*, trevrpc_transport_handle_v1, std::uint64_t));
SIGNATURE(trevrpc_transport_stream_abort_send, int (*)(trevrpc_transport*, trevrpc_transport_handle_v1, std::uint64_t));
SIGNATURE(trevrpc_transport_stream_abort, int (*)(trevrpc_transport*, trevrpc_transport_handle_v1, std::uint64_t));
SIGNATURE(trevrpc_transport_stream_close, int (*)(trevrpc_transport*, trevrpc_transport_handle_v1));
SIGNATURE(trevrpc_transport_connection_close, int (*)(trevrpc_transport*, trevrpc_transport_handle_v1, std::uint64_t));
SIGNATURE(trevrpc_transport_listener_close, int (*)(trevrpc_transport*, trevrpc_transport_handle_v1));
SIGNATURE(trevrpc_transport_release_handle, int (*)(trevrpc_transport*, trevrpc_transport_handle_v1, std::uint32_t));
SIGNATURE(trevrpc_transport_close, int (*)(trevrpc_transport*));
SIGNATURE(trevrpc_transport_drain, int (*)(trevrpc_transport*));
SIGNATURE(trevrpc_transport_release, int (*)(trevrpc_transport*));

int main() {
    trevrpc_transport_config_v1 config{};
    trevrpc_transport_endpoint_config_v1 endpoint{};
    trevrpc_transport_admission_info_v1 admission{};
    trevrpc_transport_event_protocol_info_v1 protocol{};
    trevrpc_transport_abi_1_anchor();
    if (TREVRPC_TRANSPORT_ABI_VERSION != 1u || trevrpc_transport_abi_version() != 1u ||
        trevrpc_transport_config_v1_init(&config, sizeof(config)) != 0 ||
        trevrpc_transport_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) != 0 ||
        trevrpc_transport_admission_info_v1_init(&admission, sizeof(admission)) != 0 ||
        trevrpc_transport_event_protocol_info_v1_init(&protocol, sizeof(protocol)) != 0 ||
        config.event_capacity != TREVRPC_TRANSPORT_DEFAULT_EVENT_CAPACITY ||
        config.listener_capacity != TREVRPC_TRANSPORT_DEFAULT_LISTENER_CAPACITY ||
        config.connection_capacity != TREVRPC_TRANSPORT_DEFAULT_CONNECTION_CAPACITY ||
        config.stream_capacity != TREVRPC_TRANSPORT_DEFAULT_STREAM_CAPACITY ||
        config.max_receive_owned_count != TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_COUNT ||
        config.max_receive_owned_bytes != TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_BYTES ||
        endpoint.max_frame_size != TREVRPC_TRANSPORT_DEFAULT_MAX_FRAME_SIZE ||
        endpoint.max_field_section_size != TREVRPC_TRANSPORT_DEFAULT_MAX_FIELD_SECTION_SIZE ||
        trevrpc_transport_config_v1_init(&config, sizeof(config) - 1) != -EINVAL) {
        return 1;
    }
    return 0;
}
