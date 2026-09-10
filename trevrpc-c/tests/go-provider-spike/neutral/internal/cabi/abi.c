#include "abi.h"

#include <errno.h> // IWYU pragma: keep
#include <stdlib.h>
#include <string.h>

struct trevrpc_go_provider_spike_runtime {
    const trevrpc_go_provider_spike_ops_v1* operations;
    void* context;
    uint32_t kind;
};

static int fields_are_zero(const uint64_t* fields, size_t count) {
    size_t index;
    for (index = 0; index < count; ++index) {
        if (fields[index] != 0) {
            return 0;
        }
    }
    return 1;
}

int trevrpc_go_provider_spike_descriptor_v1_init(
    trevrpc_go_provider_spike_descriptor_v1* descriptor, size_t struct_size) {
    if (descriptor == NULL || struct_size < sizeof(*descriptor) || struct_size > UINT32_MAX) {
        return -EINVAL;
    }
    memset(descriptor, 0, struct_size);
    descriptor->struct_size = (uint32_t)struct_size;
    descriptor->struct_version = TREVRPC_GO_PROVIDER_SPIKE_STRUCT_VERSION_1;
    return 0;
}

int trevrpc_go_provider_spike_adopt_v1(const trevrpc_go_provider_spike_descriptor_v1* descriptor,
    uint32_t expected_kind,
    trevrpc_go_provider_spike_runtime** out_runtime) {
    trevrpc_go_provider_spike_runtime* runtime;
    const trevrpc_go_provider_spike_ops_v1* operations;
    if (out_runtime == NULL) {
        return -EINVAL;
    }
    *out_runtime = NULL;
    if (descriptor == NULL || descriptor->struct_size < sizeof(*descriptor)) {
        return -EINVAL;
    }
    if (descriptor->struct_version != TREVRPC_GO_PROVIDER_SPIKE_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (descriptor->kind != expected_kind ||
        (expected_kind != TREVRPC_GO_PROVIDER_SPIKE_ENGINE && expected_kind != TREVRPC_GO_PROVIDER_SPIKE_TRANSPORT) ||
        descriptor->reserved0 != 0 ||
        !fields_are_zero(descriptor->reserved, sizeof(descriptor->reserved) / sizeof(descriptor->reserved[0]))) {
        return -EINVAL;
    }
    operations = descriptor->operations;
    if (operations == NULL || operations->struct_size < sizeof(*operations)) {
        return -EINVAL;
    }
    if (operations->struct_version != TREVRPC_GO_PROVIDER_SPIKE_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (operations->identity == NULL || operations->destroy == NULL ||
        !fields_are_zero(operations->reserved, sizeof(operations->reserved) / sizeof(operations->reserved[0]))) {
        return -EINVAL;
    }
    runtime = calloc(1, sizeof(*runtime));
    if (runtime == NULL) {
        return -ENOMEM;
    }
    runtime->operations = operations;
    runtime->context = descriptor->context;
    runtime->kind = descriptor->kind;
    *out_runtime = runtime;
    return 0;
}

uint64_t trevrpc_go_provider_spike_identity(const trevrpc_go_provider_spike_runtime* runtime) {
    return runtime == NULL ? 0 : runtime->operations->identity(runtime->context);
}

void trevrpc_go_provider_spike_release(trevrpc_go_provider_spike_runtime* runtime) {
    if (runtime == NULL) {
        return;
    }
    runtime->operations->destroy(runtime->context);
    free(runtime);
}
