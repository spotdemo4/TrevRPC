#include "trevrpc_engine_internal.h"
#include "trevrpc_engine_testing_internal.h"

#include <errno.h> // NOLINT(misc-include-cleaner)
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

struct trevrpc_engine_test_provider {
    trevrpc_engine* engine;
};

static int trevrpc_engine_test_attach(void* context, trevrpc_engine* engine) {
    struct trevrpc_engine_test_provider* provider = context;
    provider->engine = engine;
    return 0;
}

static int trevrpc_engine_test_close(void* context) {
    struct trevrpc_engine_test_provider* provider = context;
    trevrpc_engine_provider_stopped(provider->engine, 0, 0);
    return 0;
}

static void trevrpc_engine_test_destroy(void* context) {
    free(context);
}

int trevrpc_engine_testing_create(const trevrpc_engine_config_v1* config, trevrpc_engine** out_engine) {
    static const trevrpc_engine_provider_ops operations = {
        .attach = trevrpc_engine_test_attach,
        .close = trevrpc_engine_test_close,
        .destroy = trevrpc_engine_test_destroy,
    };
    struct trevrpc_engine_test_provider* provider;
    int result;
    if (out_engine == NULL) {
        return -EINVAL;
    }
    provider = calloc(1, sizeof(*provider));
    if (provider == NULL) {
        return -ENOMEM;
    }
    result = trevrpc_engine_provider_create_v1(config, &operations, provider, UINT64_C(0x746573742d656e67), out_engine);
    return result;
}

int trevrpc_engine_testing_enqueue_diagnostic_copy(trevrpc_engine* engine, const void* data, size_t data_len) {
    trevrpc_engine_event_spec spec;
    bool held;
    if (engine == NULL || (data == NULL && data_len != 0)) {
        return -EINVAL;
    }
    held = trevrpc_engine_provider_context(engine) != NULL;
    if (!held) {
        return -EINVAL;
    }
    memset(&spec, 0, sizeof(spec));
    spec.kind = TREVRPC_ENGINE_EVENT_DIAGNOSTIC;
    spec.data = data;
    spec.data_len = data_len;
    return trevrpc_engine_provider_publish_event(engine, &spec);
}

int trevrpc_engine_testing_producer_acquire(trevrpc_engine* engine) {
    return trevrpc_engine_internal_test_producer_acquire(engine);
}

int trevrpc_engine_testing_producer_release(trevrpc_engine* engine) {
    if (engine == NULL) {
        return -EINVAL;
    }
    trevrpc_engine_internal_test_producer_release(engine);
    return 0;
}

void trevrpc_engine_testing_pause_last_event_drain(trevrpc_engine* engine) {
    trevrpc_engine_internal_test_pause_last_event_drain(engine);
}

void trevrpc_engine_testing_resume_last_event_drain(trevrpc_engine* engine) {
    trevrpc_engine_internal_test_resume_last_event_drain(engine);
}

int trevrpc_engine_testing_get_write_fd(trevrpc_engine* engine) {
    return trevrpc_engine_internal_test_get_write_fd(engine);
}

int trevrpc_engine_testing_force_wake_failure(trevrpc_engine* engine, uint32_t operation, uint64_t counter_seed) {
    return trevrpc_engine_internal_test_force_wake_failure(engine, operation, counter_seed);
}

void trevrpc_engine_testing_pause_api_enter(trevrpc_engine* engine) {
    trevrpc_engine_internal_test_pause_api_enter(engine);
}

void trevrpc_engine_testing_wait_for_api_enter(trevrpc_engine* engine) {
    trevrpc_engine_internal_test_wait_for_api_enter(engine);
}

void trevrpc_engine_testing_resume_api_enter(trevrpc_engine* engine) {
    trevrpc_engine_internal_test_resume_api_enter(engine);
}

void trevrpc_engine_testing_pause_api_leave(trevrpc_engine* engine) {
    trevrpc_engine_internal_test_pause_api_leave(engine);
}

void trevrpc_engine_testing_wait_for_api_leave(trevrpc_engine* engine) {
    trevrpc_engine_internal_test_wait_for_api_leave(engine);
}

void trevrpc_engine_testing_resume_api_leave(trevrpc_engine* engine) {
    trevrpc_engine_internal_test_resume_api_leave(engine);
}

int trevrpc_engine_testing_release_is_waiting(const trevrpc_engine* engine) {
    return trevrpc_engine_internal_test_release_is_waiting(engine);
}
