#ifndef TREVRPC_GO_PROVIDER_SPIKE_ABI_H
#define TREVRPC_GO_PROVIDER_SPIKE_ABI_H

#include <stddef.h>
#include <stdint.h>

#define TREVRPC_GO_PROVIDER_SPIKE_ABI_VERSION 1u
#define TREVRPC_GO_PROVIDER_SPIKE_STRUCT_VERSION_1 1u
#define TREVRPC_GO_PROVIDER_SPIKE_ENGINE 1u
#define TREVRPC_GO_PROVIDER_SPIKE_TRANSPORT 2u

typedef struct trevrpc_go_provider_spike_ops_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint64_t (*identity)(void* context);
    void (*destroy)(void* context);
    uint64_t reserved[4];
} trevrpc_go_provider_spike_ops_v1;

typedef struct trevrpc_go_provider_spike_descriptor_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t kind;
    uint32_t reserved0;
    const trevrpc_go_provider_spike_ops_v1* operations;
    void* context;
    uint64_t reserved[4];
} trevrpc_go_provider_spike_descriptor_v1;

typedef struct trevrpc_go_provider_spike_runtime trevrpc_go_provider_spike_runtime;

int trevrpc_go_provider_spike_descriptor_v1_init(
    trevrpc_go_provider_spike_descriptor_v1* descriptor, size_t struct_size);
int trevrpc_go_provider_spike_adopt_v1(const trevrpc_go_provider_spike_descriptor_v1* descriptor,
    uint32_t expected_kind,
    trevrpc_go_provider_spike_runtime** out_runtime);
uint64_t trevrpc_go_provider_spike_identity(const trevrpc_go_provider_spike_runtime* runtime);
void trevrpc_go_provider_spike_release(trevrpc_go_provider_spike_runtime* runtime);

#endif
