#include "trevrpc_rpc.h"

int main(void) {
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_abi_1_anchor();
    return trevrpc_rpc_abi_version() == TREVRPC_RPC_ABI_VERSION &&
                   trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) == 0
               ? 0
               : 1;
}
