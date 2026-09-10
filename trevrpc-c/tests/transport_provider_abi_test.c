#include "../internal/cabi/testprovider/provider.h"

#include <errno.h> // IWYU pragma: keep
#include <stdint.h>

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            return __LINE__;                                                                                           \
        }                                                                                                              \
    } while (0)

static int make_descriptor(trevrpc_transport_provider_descriptor_v1* descriptor) {
    const trevrpc_transport_provider_ops_v1* operations;
    void* context;
    int result = trevrpc_go_test_transport_provider_create(&operations, &context);
    if (result != 0) {
        return result;
    }
    result = trevrpc_transport_provider_descriptor_v1_init(descriptor, sizeof(*descriptor));
    if (result != 0) {
        trevrpc_go_test_transport_provider_dispose(context);
        return result;
    }
    descriptor->operations = operations;
    descriptor->context = context;
    return 0;
}

static int test_invalid_descriptors(void) {
    trevrpc_transport_provider_descriptor_v1 descriptor;
    trevrpc_transport* transport = NULL;
    uint64_t destroyed = trevrpc_go_test_transport_destroy_count();

    CHECK(make_descriptor(&descriptor) == 0);
    descriptor.struct_size = sizeof(descriptor) - 1;
    transport = (trevrpc_transport*)(uintptr_t)1;
    CHECK(trevrpc_transport_provider_adopt_v1(&descriptor, &transport) == -EINVAL);
    CHECK(transport == NULL);
    CHECK(trevrpc_go_test_transport_destroy_count() == destroyed);
    trevrpc_go_test_transport_provider_dispose(descriptor.context);
    ++destroyed;

    CHECK(make_descriptor(&descriptor) == 0);
    descriptor.struct_version = 2;
    CHECK(trevrpc_transport_provider_adopt_v1(&descriptor, &transport) == -ENOTSUP);
    CHECK(trevrpc_go_test_transport_destroy_count() == destroyed);
    trevrpc_go_test_transport_provider_dispose(descriptor.context);
    ++destroyed;

    CHECK(make_descriptor(&descriptor) == 0);
    descriptor.reserved[0] = 1;
    CHECK(trevrpc_transport_provider_adopt_v1(&descriptor, &transport) == -EINVAL);
    CHECK(trevrpc_go_test_transport_destroy_count() == destroyed);
    trevrpc_go_test_transport_provider_dispose(descriptor.context);
    ++destroyed;

    CHECK(make_descriptor(&descriptor) == 0);
    trevrpc_go_test_transport_ops_set_size(descriptor.operations, sizeof(*descriptor.operations) - 1);
    CHECK(trevrpc_transport_provider_adopt_v1(&descriptor, &transport) == -EINVAL);
    CHECK(trevrpc_go_test_transport_destroy_count() == destroyed);
    trevrpc_go_test_transport_provider_dispose(descriptor.context);
    ++destroyed;

    CHECK(make_descriptor(&descriptor) == 0);
    trevrpc_go_test_transport_ops_set_version(descriptor.operations, 2);
    CHECK(trevrpc_transport_provider_adopt_v1(&descriptor, &transport) == -ENOTSUP);
    CHECK(trevrpc_go_test_transport_destroy_count() == destroyed);
    trevrpc_go_test_transport_provider_dispose(descriptor.context);
    ++destroyed;

    CHECK(make_descriptor(&descriptor) == 0);
    trevrpc_go_test_transport_ops_set_reserved(descriptor.operations, 1);
    CHECK(trevrpc_transport_provider_adopt_v1(&descriptor, &transport) == -EINVAL);
    CHECK(trevrpc_go_test_transport_destroy_count() == destroyed);
    trevrpc_go_test_transport_provider_dispose(descriptor.context);

    return 0;
}

int main(void) {
    trevrpc_transport_provider_descriptor_v1 descriptor;
    trevrpc_transport_endpoint_config_v1 endpoint;
    trevrpc_transport_handle_v1 listener = {0};
    trevrpc_transport_handle_v1 connection = {0};
    trevrpc_transport_handle_v1 stream = {0};
    trevrpc_transport* transport = NULL;
    uint16_t port = 0;
    uint64_t destroyed;

    CHECK(trevrpc_transport_provider_abi_version() == TREVRPC_TRANSPORT_PROVIDER_ABI_VERSION);
    trevrpc_transport_provider_abi_1_anchor();
    CHECK(trevrpc_transport_provider_descriptor_v1_init(&descriptor, sizeof(descriptor) - 1) == -EINVAL);
    CHECK(test_invalid_descriptors() == 0);

    destroyed = trevrpc_go_test_transport_destroy_count();
    CHECK(make_descriptor(&descriptor) == 0);
    CHECK(trevrpc_transport_provider_adopt_v1(&descriptor, &transport) == 0);
    CHECK(transport != NULL);
    CHECK(trevrpc_transport_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) == 0);
    CHECK(trevrpc_transport_listen_v1(transport, &endpoint, &listener) == 0);
    CHECK(trevrpc_transport_listener_get_port_v1(transport, listener, &port) == 0);
    CHECK(port == 4242);
    CHECK(trevrpc_transport_dial_v1(transport, &endpoint, 1, &connection) == 0);
    CHECK(trevrpc_transport_connection_open_bidi_stream_v1(transport, connection, 2, &stream) == 0);
    CHECK(trevrpc_transport_stream_send_v1(transport, stream, 3, (const uint8_t*)"x", 1) == 0);
    CHECK(trevrpc_transport_stream_finish_send(transport, stream) == 0);
    CHECK(trevrpc_transport_stream_abort_receive(transport, stream, 4) == 0);
    CHECK(trevrpc_transport_stream_abort_send(transport, stream, 5) == 0);
    CHECK(trevrpc_transport_stream_abort(transport, stream, 6) == 0);
    CHECK(trevrpc_transport_stream_close(transport, stream) == 0);
    CHECK(trevrpc_transport_connection_close(transport, connection, 7) == 0);
    CHECK(trevrpc_transport_listener_close(transport, listener) == 0);
    CHECK(trevrpc_transport_close(transport) == 0);
    CHECK(trevrpc_transport_drain(transport) == 0);
    CHECK(trevrpc_transport_release(transport) == 0);
    CHECK(trevrpc_go_test_transport_destroy_count() == destroyed + 1);
    CHECK(trevrpc_go_test_transport_destroy_before_drain_count() == 0);

    return 0;
}
