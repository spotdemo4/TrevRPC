#include "trevrpc_engine_provider.h"

#include <errno.h> // IWYU pragma: keep
#include <stdint.h>
#include <stdlib.h>

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            return __LINE__;                                                                                           \
        }                                                                                                              \
    } while (0)

struct test_provider {
    const trevrpc_engine_provider_host_v1* host;
    trevrpc_engine* engine;
    unsigned* destroy_count;
};

static int test_attach(void* context, trevrpc_engine* engine) {
    struct test_provider* provider = context;
    provider->engine = engine;
    return 0;
}

static int test_close(void* context) {
    struct test_provider* provider = context;
    provider->host->stopped(provider->engine, 0, 0);
    return 0;
}

static void test_destroy(void* context) {
    struct test_provider* provider = context;
    ++*provider->destroy_count;
    free(provider);
}

static struct test_provider* test_provider_create(unsigned* destroy_count) {
    struct test_provider* provider = calloc(1, sizeof(*provider));
    if (provider != NULL) {
        provider->host = trevrpc_engine_provider_host_v1_get();
        provider->destroy_count = destroy_count;
    }
    return provider;
}

static trevrpc_engine_provider_ops_v1 test_operations(void) {
    trevrpc_engine_provider_ops_v1 operations = {0};
    operations.struct_size = sizeof(operations);
    operations.struct_version = TREVRPC_ENGINE_PROVIDER_STRUCT_VERSION_1;
    operations.attach = test_attach;
    operations.close = test_close;
    operations.destroy = test_destroy;
    return operations;
}

static trevrpc_engine_provider_descriptor_v1 test_descriptor(
    const trevrpc_engine_provider_ops_v1* operations, struct test_provider* provider) {
    trevrpc_engine_provider_descriptor_v1 descriptor;
    if (trevrpc_engine_provider_descriptor_v1_init(&descriptor, sizeof(descriptor)) != 0) {
        abort();
    }
    descriptor.operations = operations;
    descriptor.context = provider;
    descriptor.owner_cookie = UINT64_C(0x70726f7669646572);
    return descriptor;
}

int main(void) {
    const trevrpc_engine_provider_host_v1* host = trevrpc_engine_provider_host_v1_get();
    trevrpc_engine_provider_descriptor_v1 descriptor;
    trevrpc_engine_provider_ops_v1 operations;
    trevrpc_engine_config_v1 config;
    struct test_provider* provider;
    trevrpc_engine* engine = NULL;
    unsigned destroy_count = 0;

    CHECK(trevrpc_engine_provider_abi_version() == TREVRPC_ENGINE_PROVIDER_ABI_VERSION);
    trevrpc_engine_provider_abi_1_anchor();
    CHECK(host != NULL);
    CHECK(host->struct_size >= sizeof(*host));
    CHECK(host->struct_version == TREVRPC_ENGINE_PROVIDER_STRUCT_VERSION_1);
    CHECK(host->callback_enter != NULL);
    CHECK(host->stopped != NULL);
    CHECK(trevrpc_engine_provider_descriptor_v1_init(&descriptor, sizeof(descriptor) - 1) == -EINVAL);
    CHECK(trevrpc_engine_provider_descriptor_v1_init(&descriptor, sizeof(descriptor)) == 0);
    CHECK(descriptor.struct_size == sizeof(descriptor));
    CHECK(descriptor.struct_version == TREVRPC_ENGINE_PROVIDER_STRUCT_VERSION_1);

    CHECK(trevrpc_engine_config_v1_init(&config, sizeof(config)) == 0);
    operations = test_operations();

    provider = test_provider_create(&destroy_count);
    CHECK(provider != NULL);
    descriptor = test_descriptor(&operations, provider);
    descriptor.struct_size = sizeof(descriptor) - 1;
    engine = (trevrpc_engine*)(uintptr_t)1;
    CHECK(trevrpc_engine_provider_adopt_v1(&config, &descriptor, &engine) == -EINVAL);
    CHECK(engine == NULL);
    CHECK(destroy_count == 0);
    free(provider);

    provider = test_provider_create(&destroy_count);
    CHECK(provider != NULL);
    descriptor = test_descriptor(&operations, provider);
    descriptor.struct_version = 2;
    CHECK(trevrpc_engine_provider_adopt_v1(&config, &descriptor, &engine) == -ENOTSUP);
    CHECK(destroy_count == 0);
    free(provider);

    provider = test_provider_create(&destroy_count);
    CHECK(provider != NULL);
    descriptor = test_descriptor(&operations, provider);
    descriptor.reserved[0] = 1;
    CHECK(trevrpc_engine_provider_adopt_v1(&config, &descriptor, &engine) == -EINVAL);
    CHECK(destroy_count == 0);
    free(provider);

    provider = test_provider_create(&destroy_count);
    CHECK(provider != NULL);
    operations = test_operations();
    operations.struct_size = sizeof(operations) - 1;
    descriptor = test_descriptor(&operations, provider);
    CHECK(trevrpc_engine_provider_adopt_v1(&config, &descriptor, &engine) == -EINVAL);
    CHECK(destroy_count == 0);
    free(provider);

    provider = test_provider_create(&destroy_count);
    CHECK(provider != NULL);
    operations = test_operations();
    operations.struct_version = 2;
    descriptor = test_descriptor(&operations, provider);
    CHECK(trevrpc_engine_provider_adopt_v1(&config, &descriptor, &engine) == -ENOTSUP);
    CHECK(destroy_count == 0);
    free(provider);

    provider = test_provider_create(&destroy_count);
    CHECK(provider != NULL);
    operations = test_operations();
    operations.reserved[0] = 1;
    descriptor = test_descriptor(&operations, provider);
    CHECK(trevrpc_engine_provider_adopt_v1(&config, &descriptor, &engine) == -EINVAL);
    CHECK(destroy_count == 0);
    free(provider);

    provider = test_provider_create(&destroy_count);
    CHECK(provider != NULL);
    operations = test_operations();
    descriptor = test_descriptor(&operations, provider);
    config.event_capacity = 0;
    CHECK(trevrpc_engine_provider_adopt_v1(&config, &descriptor, &engine) == -EINVAL);
    CHECK(destroy_count == 1);
    CHECK(engine == NULL);

    CHECK(trevrpc_engine_config_v1_init(&config, sizeof(config)) == 0);
    provider = test_provider_create(&destroy_count);
    CHECK(provider != NULL);
    operations = test_operations();
    descriptor = test_descriptor(&operations, provider);
    CHECK(trevrpc_engine_provider_adopt_v1(&config, &descriptor, &engine) == 0);
    CHECK(engine != NULL);
    CHECK(trevrpc_engine_close(engine) == 0);
    CHECK(trevrpc_engine_release(engine) == 0);
    CHECK(destroy_count == 2);

    return 0;
}
