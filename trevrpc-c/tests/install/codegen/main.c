#include "greeter.trevrpc.h"

#include <stddef.h>

int main(void) {
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_call_config_v1 unary_config;
    trevrpc_rpc_call_config_v1 server_streaming_config;
    trevrpc_rpc_call_config_v1 client_streaming_config;
    trevrpc_rpc_call_config_v1 bidi_config;
    trevrpc_rpc_status_v1 status;
    trevrpc_rpc_event_info_v1 event_info;
    trevrpc_rpc_receive_info_v1 receive_info;

    if (TREVRPC_RPC_ABI_VERSION != 1u || trevrpc_rpc_abi_version() != 1u ||
        trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) != 0 ||
        installed_v1_greeter_say_hello_call_config_init(&unary_config, sizeof(unary_config)) != 0 ||
        installed_v1_greeter_lots_of_replies_call_config_init(
            &server_streaming_config, sizeof(server_streaming_config)) != 0 ||
        installed_v1_greeter_lots_of_greetings_call_config_init(
            &client_streaming_config, sizeof(client_streaming_config)) != 0 ||
        installed_v1_greeter_bidi_hello_call_config_init(&bidi_config, sizeof(bidi_config)) != 0 ||
        trevrpc_rpc_status_v1_init(&status, sizeof(status)) != 0 ||
        trevrpc_rpc_event_info_v1_init(&event_info, sizeof(event_info)) != 0 ||
        trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) != 0 ||
        unary_config.kind != TREVRPC_RPC_KIND_UNARY ||
        server_streaming_config.kind != TREVRPC_RPC_KIND_SERVER_STREAMING ||
        client_streaming_config.kind != TREVRPC_RPC_KIND_CLIENT_STREAMING ||
        bidi_config.kind != TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        return 1;
    }

    trevrpc_rpc_abi_1_anchor();
    return 0;
}
