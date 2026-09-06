#include "trevrpc_rpc.h"

int main(void) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_call_config_v1 call_config;
    trevrpc_rpc_call_context_info_v1 context_info;
    trevrpc_rpc_admission_info_v1 admission_info;
    trevrpc_rpc_abi_1_anchor();
    (void)&trevrpc_rpc_call_get_context_v1;
    (void)&trevrpc_rpc_event_get_admission_info_v1;
    (void)&trevrpc_rpc_admission_respond_v1;
    return trevrpc_rpc_abi_version() == TREVRPC_RPC_ABI_VERSION &&
                   trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0 &&
                   trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0 &&
                   trevrpc_rpc_call_context_info_v1_init(&context_info, sizeof(context_info)) == 0 &&
                   trevrpc_rpc_admission_info_v1_init(&admission_info, sizeof(admission_info)) == 0
               ? 0
               : 1;
}
