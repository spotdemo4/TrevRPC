#include "../include/abi.h"

#include <errno.h> // IWYU pragma: keep
#include <stdlib.h>
#include <string.h>

typedef struct spike_provider_state {
    uint64_t identity;
    trevrpc_go_provider_spike_ops_v1* mutable_operations;
} spike_provider_state;

static uint64_t provider_identity(void* context) {
    spike_provider_state* state = context;
    return state == NULL ? 0 : state->identity;
}

static void provider_destroy(void* context) {
    spike_provider_state* state = context;
    if (state == NULL) {
        return;
    }
    free(state->mutable_operations);
    free(state);
}

static const trevrpc_go_provider_spike_ops_v1 provider_operations = {
    .struct_size = sizeof(trevrpc_go_provider_spike_ops_v1),
    .struct_version = TREVRPC_GO_PROVIDER_SPIKE_STRUCT_VERSION_1,
    .identity = provider_identity,
    .destroy = provider_destroy,
};

int trevrpc_go_provider_spike_make_descriptor(
    uint32_t kind, uint64_t identity, uint32_t mutation, trevrpc_go_provider_spike_descriptor_v1* descriptor) {
    spike_provider_state* state;
    trevrpc_go_provider_spike_ops_v1* mutable_operations = NULL;
    if (descriptor == NULL) {
        return -EINVAL;
    }
    memset(descriptor, 0, sizeof(*descriptor));
    state = calloc(1, sizeof(*state));
    if (state == NULL) {
        return -ENOMEM;
    }
    state->identity = identity;
    descriptor->struct_size = sizeof(*descriptor);
    descriptor->struct_version = TREVRPC_GO_PROVIDER_SPIKE_STRUCT_VERSION_1;
    descriptor->kind = kind;
    descriptor->operations = &provider_operations;
    descriptor->context = state;
    if (mutation == 1) {
        descriptor->struct_size = 1;
    } else if (mutation == 2) {
        descriptor->struct_version = 2;
    } else if (mutation == 3 || mutation == 4) {
        mutable_operations = malloc(sizeof(*mutable_operations));
        if (mutable_operations == NULL) {
            free(state);
            memset(descriptor, 0, sizeof(*descriptor));
            return -ENOMEM;
        }
        *mutable_operations = provider_operations;
        mutable_operations->struct_size = mutation == 3 ? 1 : sizeof(*mutable_operations);
        mutable_operations->struct_version = mutation == 4 ? 2 : TREVRPC_GO_PROVIDER_SPIKE_STRUCT_VERSION_1;
        state->mutable_operations = mutable_operations;
        descriptor->operations = mutable_operations;
    }
    return 0;
}

void trevrpc_go_provider_spike_dispose_descriptor(trevrpc_go_provider_spike_descriptor_v1* descriptor) {
    if (descriptor == NULL) {
        return;
    }
    provider_destroy(descriptor->context);
    memset(descriptor, 0, sizeof(*descriptor));
}
