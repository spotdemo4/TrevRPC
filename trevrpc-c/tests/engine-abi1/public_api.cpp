#include "trevrpc_engine.h"

#include <cerrno>
#include <cstddef>
#include <type_traits>

static_assert(TREVRPC_ENGINE_ABI_VERSION == 1u);
static_assert(sizeof(trevrpc_engine_handle_v1) == 16);
static_assert(sizeof(trevrpc_engine_config_v1) == 80);
static_assert(sizeof(trevrpc_engine_wake_source_v1) == 48);
static_assert(sizeof(trevrpc_engine_endpoint_config_v1) == 160);
static_assert(sizeof(trevrpc_engine_event_info_v1) == 136);
static_assert(sizeof(trevrpc_engine_receive_info_v1) == 64);
static_assert(sizeof(trevrpc_engine_diagnostics_v1) == 208);
static_assert(alignof(trevrpc_engine_config_v1) == 8);
static_assert(offsetof(trevrpc_engine_handle_v1, generation) == 12);
static_assert(offsetof(trevrpc_engine_endpoint_config_v1, max_pending_send_bytes) == 88);
static_assert(offsetof(trevrpc_engine_event_info_v1, operation_id) == 64);
static_assert(offsetof(trevrpc_engine_receive_info_v1, data) == 16);
static_assert(offsetof(trevrpc_engine_diagnostics_v1, provider_error_code) == 168);
static_assert(std::is_standard_layout_v<trevrpc_engine_handle_v1>);
static_assert(std::is_standard_layout_v<trevrpc_engine_config_v1>);
static_assert(std::is_standard_layout_v<trevrpc_engine_endpoint_config_v1>);
static_assert(std::is_standard_layout_v<trevrpc_engine_event_info_v1>);
static_assert(std::is_standard_layout_v<trevrpc_engine_receive_info_v1>);
static_assert(std::is_standard_layout_v<trevrpc_engine_diagnostics_v1>);

int main() {
    auto* abi_version = &trevrpc_engine_abi_version;
    auto* anchor = &trevrpc_engine_abi_1_anchor;
    auto* listen = &trevrpc_engine_listen_v1;
    auto* dial = &trevrpc_engine_dial_v1;
    auto* open_stream = &trevrpc_engine_connection_open_bidi_stream_v1;
    auto* send = &trevrpc_engine_stream_send_frame_v1;
    auto* receive = &trevrpc_engine_stream_receive_frame;
    auto* next = &trevrpc_engine_next_event;
    auto* release = &trevrpc_engine_release;
    trevrpc_engine_config_v1 config{};
    trevrpc_engine_endpoint_config_v1 endpoint{};
    anchor();
    if (abi_version() != 1u || listen == nullptr || dial == nullptr || open_stream == nullptr || send == nullptr ||
        receive == nullptr || next == nullptr || release == nullptr ||
        trevrpc_engine_config_v1_init(&config, sizeof(config)) != 0 ||
        trevrpc_engine_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) != 0 ||
        config.event_capacity != TREVRPC_ENGINE_DEFAULT_EVENT_CAPACITY ||
        config.listener_capacity != TREVRPC_ENGINE_DEFAULT_LISTENER_CAPACITY ||
        config.connection_capacity != TREVRPC_ENGINE_DEFAULT_CONNECTION_CAPACITY ||
        config.stream_capacity != TREVRPC_ENGINE_DEFAULT_STREAM_CAPACITY ||
        config.max_receive_owned_count != TREVRPC_ENGINE_DEFAULT_MAX_RECEIVE_OWNED_COUNT ||
        config.max_receive_owned_bytes != TREVRPC_ENGINE_DEFAULT_RECEIVE_OWNED_BYTES ||
        endpoint.max_pending_send_count != TREVRPC_ENGINE_DEFAULT_MAX_PENDING_SEND_COUNT ||
        endpoint.max_pending_send_bytes != TREVRPC_ENGINE_DEFAULT_MAX_PENDING_SEND_BYTES ||
        endpoint.max_frame_size != TREVRPC_ENGINE_DEFAULT_MAX_FRAME_SIZE ||
        trevrpc_engine_config_v1_init(&config, sizeof(config) - 1) != -EINVAL) {
        return 1;
    }
    return 0;
}
