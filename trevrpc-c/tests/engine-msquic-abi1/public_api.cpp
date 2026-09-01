#include <trevrpc_engine_msquic.h>

#include <cstddef>

static_assert(sizeof(trevrpc_engine_msquic_config_v1) == 64);
static_assert(alignof(trevrpc_engine_msquic_config_v1) == 8);
static_assert(offsetof(trevrpc_engine_msquic_config_v1, struct_size) == 0);
static_assert(offsetof(trevrpc_engine_msquic_config_v1, struct_version) == 4);
static_assert(offsetof(trevrpc_engine_msquic_config_v1, flags) == 8);
static_assert(offsetof(trevrpc_engine_msquic_config_v1, reserved0) == 12);
static_assert(offsetof(trevrpc_engine_msquic_config_v1, reserved) == 16);

int main() {
    static_assert(TREVRPC_ENGINE_MSQUIC_ABI_VERSION == 1u);
    static_assert(TREVRPC_ENGINE_MSQUIC_STRUCT_VERSION_1 == 1u);
    trevrpc_engine_msquic_abi_1_anchor();
    return trevrpc_engine_msquic_abi_version() == 1u ? 0 : 1;
}
