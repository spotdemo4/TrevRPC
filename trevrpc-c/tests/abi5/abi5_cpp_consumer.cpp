#include "trevrpc.h"
#include "trevrpc_binding.h"
#include "trevrpc_raw.h"
#include "trevrpc_webtransport.h"

#include <cstddef>
#include <cstdint>
#include <type_traits>

static_assert(TREVRPC_C_ABI_VERSION == 5u);

#define ABI5_CPP_LAYOUT(type, expected_size)                                                                           \
    static_assert(std::is_standard_layout_v<type>);                                                                    \
    static_assert(sizeof(type) == (expected_size));                                                                    \
    static_assert(alignof(type) == 8)
#define ABI5_CPP_OFFSET(type, field, expected_offset) static_assert(offsetof(type, field) == (expected_offset))

ABI5_CPP_LAYOUT(trevrpc_config, 96);
ABI5_CPP_LAYOUT(trevrpc_server_config, 168);
ABI5_CPP_LAYOUT(trevrpc_server_options, 80);
ABI5_CPP_LAYOUT(trevrpc_metadata_entry, 32);
ABI5_CPP_LAYOUT(trevrpc_metadata, 16);
ABI5_CPP_LAYOUT(trevrpc_status, 24);
ABI5_CPP_LAYOUT(trevrpc_request, 80);
ABI5_CPP_LAYOUT(trevrpc_call_options, 56);
ABI5_CPP_LAYOUT(trevrpc_response, 64);
ABI5_CPP_LAYOUT(trevrpc_stream_frame, 64);

ABI5_CPP_OFFSET(trevrpc_response, status, 0);
ABI5_CPP_OFFSET(trevrpc_response, message, 8);
ABI5_CPP_OFFSET(trevrpc_response, message_len, 16);
ABI5_CPP_OFFSET(trevrpc_response, body, 24);
ABI5_CPP_OFFSET(trevrpc_response, body_len, 32);
ABI5_CPP_OFFSET(trevrpc_response, metadata, 40);
ABI5_CPP_OFFSET(trevrpc_response, _body_owner, 56);
ABI5_CPP_OFFSET(trevrpc_stream_frame, kind, 0);
ABI5_CPP_OFFSET(trevrpc_stream_frame, status, 4);
ABI5_CPP_OFFSET(trevrpc_stream_frame, message, 8);
ABI5_CPP_OFFSET(trevrpc_stream_frame, message_len, 16);
ABI5_CPP_OFFSET(trevrpc_stream_frame, body, 24);
ABI5_CPP_OFFSET(trevrpc_stream_frame, body_len, 32);
ABI5_CPP_OFFSET(trevrpc_stream_frame, metadata, 40);
ABI5_CPP_OFFSET(trevrpc_stream_frame, _body_owner, 56);

using default_config_type = trevrpc_config (*)(void);
using server_close_type = void (*)(trevrpc_server*);
using response_set_body_type = int (*)(trevrpc_response*, const std::uint8_t*, std::size_t);
using stream_send_type = int (*)(trevrpc_stream*, const std::uint8_t*, std::size_t);
using raw_connect_type = int (*)(const char*, std::uint16_t, const trevrpc_config*, trevrpc_raw_client**);
using raw_call_type = int (*)(trevrpc_raw_client*, const trevrpc_request*, trevrpc_response**);
using server_options_type = int (*)(trevrpc_server*, const trevrpc_server_options*);
using call_respond_type = int (*)(trevrpc_call*, trevrpc_response*);
using stream_status_type = int (*)(trevrpc_stream*, std::uint32_t, const char*, std::size_t);

static_assert(std::is_same_v<decltype(&trevrpc_default_config), default_config_type>);
static_assert(std::is_same_v<decltype(&trevrpc_server_close), server_close_type>);
static_assert(std::is_same_v<decltype(&trevrpc_response_set_body), response_set_body_type>);
static_assert(std::is_same_v<decltype(&trevrpc_stream_send_message), stream_send_type>);
static_assert(std::is_same_v<decltype(&trevrpc_raw_client_connect), raw_connect_type>);
static_assert(std::is_same_v<decltype(&trevrpc_raw_client_call_request), raw_call_type>);
static_assert(std::is_same_v<decltype(&trevrpc_server_set_options), server_options_type>);
static_assert(std::is_same_v<decltype(&trevrpc_call_respond), call_respond_type>);
static_assert(std::is_same_v<decltype(&trevrpc_stream_send_status), stream_status_type>);

int main() {
    const auto config = trevrpc_default_config();
    const auto server_config = trevrpc_default_server_config();
    return trevrpc_c_abi_version() != 5u || config.max_frame_size == 0 || server_config.max_frame_size == 0;
}
