#ifndef TREVRPC_TRANSPORT_MSQUIC_H
#define TREVRPC_TRANSPORT_MSQUIC_H

#include "trevrpc_transport.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TREVRPC_TRANSPORT_MSQUIC_ABI_VERSION 1u
#define TREVRPC_TRANSPORT_MSQUIC_STRUCT_VERSION_1 1u

typedef struct trevrpc_transport_msquic_config_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t flags;
    uint32_t reserved0;
    uint64_t reserved[6];
} trevrpc_transport_msquic_config_v1;

uint32_t trevrpc_transport_msquic_abi_version(void);
void trevrpc_transport_msquic_abi_1_anchor(void);

int trevrpc_transport_msquic_config_v1_init(trevrpc_transport_msquic_config_v1* config, size_t struct_size);
int trevrpc_transport_msquic_create_v1(const trevrpc_transport_config_v1* transport_config,
    const trevrpc_transport_msquic_config_v1* provider_config,
    trevrpc_transport** out_transport);

#ifdef __cplusplus
}
#endif

#endif
