#ifndef TREVRPC_ENGINE_MSQUIC_H
#define TREVRPC_ENGINE_MSQUIC_H

#include "trevrpc_engine.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TREVRPC_ENGINE_MSQUIC_ABI_VERSION 1u
#define TREVRPC_ENGINE_MSQUIC_STRUCT_VERSION_1 1u

typedef struct trevrpc_engine_msquic_config_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t flags;
    uint32_t reserved0;
    uint64_t reserved[6];
} trevrpc_engine_msquic_config_v1;

uint32_t trevrpc_engine_msquic_abi_version(void);
void trevrpc_engine_msquic_abi_1_anchor(void);

int trevrpc_engine_msquic_config_v1_init(trevrpc_engine_msquic_config_v1* config, size_t struct_size);
int trevrpc_engine_msquic_create_v1(const trevrpc_engine_config_v1* engine_config,
    const trevrpc_engine_msquic_config_v1* provider_config,
    trevrpc_engine** out_engine);

#ifdef __cplusplus
}
#endif

#endif
