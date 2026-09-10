#include "go-provider-spike/neutral/internal/cabi/abi.h"

#include <assert.h>
#include <errno.h> // IWYU pragma: keep
#include <stdint.h>

int trevrpc_go_provider_spike_make_descriptor(
    uint32_t kind, uint64_t identity, uint32_t mutation, trevrpc_go_provider_spike_descriptor_v1* descriptor);
void trevrpc_go_provider_spike_dispose_descriptor(trevrpc_go_provider_spike_descriptor_v1* descriptor);

static void rejects_invalid_descriptors(void) {
    trevrpc_go_provider_spike_descriptor_v1 descriptor;
    trevrpc_go_provider_spike_runtime* runtime = NULL;
    assert(trevrpc_go_provider_spike_make_descriptor(TREVRPC_GO_PROVIDER_SPIKE_ENGINE, 1, 1, &descriptor) == 0);
    assert(trevrpc_go_provider_spike_adopt_v1(&descriptor, TREVRPC_GO_PROVIDER_SPIKE_ENGINE, &runtime) == -EINVAL);
    assert(runtime == NULL);
    trevrpc_go_provider_spike_dispose_descriptor(&descriptor);

    assert(trevrpc_go_provider_spike_make_descriptor(TREVRPC_GO_PROVIDER_SPIKE_ENGINE, 1, 2, &descriptor) == 0);
    assert(trevrpc_go_provider_spike_adopt_v1(&descriptor, TREVRPC_GO_PROVIDER_SPIKE_ENGINE, &runtime) == -ENOTSUP);
    assert(runtime == NULL);
    trevrpc_go_provider_spike_dispose_descriptor(&descriptor);
}

int main(void) {
    trevrpc_go_provider_spike_descriptor_v1 engine_descriptor;
    trevrpc_go_provider_spike_descriptor_v1 transport_descriptor;
    trevrpc_go_provider_spike_runtime* engine = NULL;
    trevrpc_go_provider_spike_runtime* transport = NULL;

    rejects_invalid_descriptors();
    assert(trevrpc_go_provider_spike_make_descriptor(
               TREVRPC_GO_PROVIDER_SPIKE_ENGINE, UINT64_C(11), 0, &engine_descriptor) == 0);
    assert(trevrpc_go_provider_spike_make_descriptor(
               TREVRPC_GO_PROVIDER_SPIKE_TRANSPORT, UINT64_C(22), 0, &transport_descriptor) == 0);
    assert(trevrpc_go_provider_spike_adopt_v1(&engine_descriptor, TREVRPC_GO_PROVIDER_SPIKE_ENGINE, &engine) == 0);
    assert(trevrpc_go_provider_spike_adopt_v1(&transport_descriptor, TREVRPC_GO_PROVIDER_SPIKE_TRANSPORT, &transport) ==
           0);
    assert(trevrpc_go_provider_spike_identity(engine) == UINT64_C(11));
    assert(trevrpc_go_provider_spike_identity(transport) == UINT64_C(22));
    trevrpc_go_provider_spike_release(engine);
    trevrpc_go_provider_spike_release(transport);
    return 0;
}
