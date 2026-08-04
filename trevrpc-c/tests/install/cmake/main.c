#include <trevrpc.h>

#include <stddef.h>

int main(void) {
    trevrpc_client_config_v1 config;
    if (TREVRPC_C_ABI_VERSION != 6u || trevrpc_c_abi_version() != 6u ||
        trevrpc_client_config_v1_init(&config, sizeof(config)) != 0 || config.max_frame_size == 0) {
        return 1;
    }
    return 0;
}
