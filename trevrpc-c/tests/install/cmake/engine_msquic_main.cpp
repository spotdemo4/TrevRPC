#include "trevrpc_engine.h"
#include "trevrpc_engine_msquic.h"

int main() {
    trevrpc_engine_config_v1 engine_config;
    trevrpc_engine_msquic_config_v1 provider_config;
    trevrpc_engine* engine = nullptr;

    trevrpc_engine_abi_1_anchor();
    trevrpc_engine_msquic_abi_1_anchor();
    if (TREVRPC_ENGINE_ABI_VERSION != 1u || TREVRPC_ENGINE_MSQUIC_ABI_VERSION != 1u ||
        trevrpc_engine_abi_version() != 1u || trevrpc_engine_msquic_abi_version() != 1u ||
        trevrpc_engine_config_v1_init(&engine_config, sizeof(engine_config)) != 0 ||
        trevrpc_engine_msquic_config_v1_init(&provider_config, sizeof(provider_config)) != 0 ||
        trevrpc_engine_msquic_create_v1(&engine_config, &provider_config, &engine) != 0) {
        return 1;
    }
    return trevrpc_engine_release(engine) == 0 ? 0 : 1;
}
