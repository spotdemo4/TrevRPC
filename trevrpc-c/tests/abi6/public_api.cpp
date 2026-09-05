#include "trevrpc_binding.h"

#include <cstddef>
#include <cstdint>
#include <type_traits>

static_assert(TREVRPC_C_ABI_VERSION == 6u);
static_assert(sizeof(trevrpc_bytes_view) == 16);
static_assert(alignof(trevrpc_bytes_view) == 8);
static_assert(sizeof(trevrpc_client_config_v1) == 104);
static_assert(sizeof(trevrpc_server_config_v1) == 184);
static_assert(sizeof(trevrpc_server_options_v1) == 88);
static_assert(sizeof(trevrpc_call_options_v1) == 72);
static_assert(sizeof(trevrpc_response_view_v1) == 56);
static_assert(sizeof(trevrpc_status_view_v1) == 40);
static_assert(std::is_standard_layout_v<trevrpc_client_config_v1>);
static_assert(std::is_standard_layout_v<trevrpc_server_config_v1>);

int main() {
    auto* abi_version = &trevrpc_c_abi_version;
    auto* abi_anchor = &trevrpc_c_abi_6_anchor;
    auto* channel_connect = &trevrpc_channel_connect_v1;
    auto* raw_connect = &trevrpc_raw_client_connect_v1_with_shutdown_callback;
    auto* receive = &trevrpc_stream_recv_inbound;
    abi_anchor();
    return abi_version() != 6u || channel_connect == nullptr || raw_connect == nullptr || receive == nullptr;
}
