#include "trevrpc_engine.h"
#include "trevrpc_engine_internal.h"
#include "trevrpc_engine_testing_internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(expression)                                                                                              \
    do {                                                                                                               \
        if (!(expression)) {                                                                                           \
            fprintf(stderr, "check failed at %s:%d: %s\n", __FILE__, __LINE__, #expression);                           \
            return 1;                                                                                                  \
        }                                                                                                              \
    } while (0)

static int create_engine(uint32_t capacity, uint32_t payload_limit, trevrpc_engine** engine) {
    trevrpc_engine_config_v1 config;
    int result = trevrpc_engine_config_v1_init(&config, sizeof(config));
    (void)payload_limit;
    if (result != 0) {
        return result;
    }
    config.event_capacity = capacity;
    return trevrpc_engine_testing_create(&config, engine);
}

static int pop_info(trevrpc_engine* engine, trevrpc_engine_event** event, trevrpc_engine_event_info_v1* info) {
    int result = trevrpc_engine_next_event(engine, event);
    if (result != 0) {
        return result;
    }
    result = trevrpc_engine_event_info_v1_init(info, sizeof(*info));
    if (result == 0) {
        result = trevrpc_engine_event_get_info_v1(*event, info);
    }
    return result;
}

static int test_configuration_validation(void) {
    trevrpc_engine_config_v1 config;
    trevrpc_engine* sentinel = (trevrpc_engine*)(uintptr_t)0x1234u;
    trevrpc_engine* output = sentinel;
    CHECK(trevrpc_engine_config_v1_init(&config, sizeof(config)) == 0);
    CHECK(trevrpc_engine_testing_create(NULL, &output) == -EINVAL);
    CHECK(output == sentinel);
    CHECK(trevrpc_engine_testing_create(&config, NULL) == -EINVAL);
    config.struct_size = sizeof(config) - 1;
    CHECK(trevrpc_engine_testing_create(&config, &output) == -EINVAL);
    CHECK(output == sentinel);
    CHECK(trevrpc_engine_config_v1_init(&config, sizeof(config)) == 0);
    config.struct_version = 2;
    CHECK(trevrpc_engine_testing_create(&config, &output) == -ENOTSUP);
    CHECK(output == sentinel);
    CHECK(trevrpc_engine_config_v1_init(&config, sizeof(config)) == 0);
    config.reserved[2] = 1;
    CHECK(trevrpc_engine_testing_create(&config, &output) == -EINVAL);
    CHECK(trevrpc_engine_config_v1_init(&config, sizeof(config)) == 0);
    config.event_capacity = 0;
    CHECK(trevrpc_engine_testing_create(&config, &output) == -EINVAL);
    CHECK(trevrpc_engine_config_v1_init(&config, sizeof(config)) == 0);
    config.event_capacity = TREVRPC_ENGINE_MAX_EVENT_CAPACITY + 1u;
    CHECK(trevrpc_engine_testing_create(&config, &output) == -EINVAL);
    return 0;
}

static int test_empty_and_output_guarantees(void) {
    trevrpc_engine* engine = NULL;
    trevrpc_engine_event* sentinel = (trevrpc_engine_event*)(uintptr_t)0x4321u;
    trevrpc_engine_event* output = sentinel;
    CHECK(create_engine(1, 16, &engine) == 0);
    CHECK(trevrpc_engine_next_event(engine, &output) == -EAGAIN);
    CHECK(output == sentinel);
    CHECK(trevrpc_engine_next_event(engine, NULL) == -EINVAL);
    CHECK(trevrpc_engine_release(engine) == 0);
    return 0;
}

static int test_fifo_wraparound_and_copy(void) {
    trevrpc_engine* engine = NULL;
    trevrpc_engine_event* event = NULL;
    trevrpc_engine_event_info_v1 info;
    uint32_t value;
    uint32_t expected;
    CHECK(create_engine(3, sizeof(value), &engine) == 0);
    for (expected = 0; expected < 200; ++expected) {
        value = expected;
        CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, &value, sizeof(value)) == 0);
        value = UINT32_MAX;
        CHECK(pop_info(engine, &event, &info) == 0);
        CHECK(info.kind == TREVRPC_ENGINE_EVENT_DIAGNOSTIC);
        CHECK(info.data_len == sizeof(expected));
        CHECK(memcmp(info.data, &expected, sizeof(expected)) == 0);
        CHECK(info.sequence == (uint64_t)expected + 1u);
        trevrpc_engine_event_release(event);
        event = NULL;
    }
    for (value = 10; value < 13; ++value) {
        CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, &value, sizeof(value)) == 0);
    }
    for (expected = 10; expected < 13; ++expected) {
        CHECK(pop_info(engine, &event, &info) == 0);
        CHECK(memcmp(info.data, &expected, sizeof(expected)) == 0);
        trevrpc_engine_event_release(event);
    }
    value = 99;
    CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, &value, sizeof(value)) == 0);
    CHECK(pop_info(engine, &event, &info) == 0);
    CHECK(memcmp(info.data, &value, sizeof(value)) == 0);
    trevrpc_engine_event_release(event);
    CHECK(trevrpc_engine_release(engine) == 0);
    return 0;
}

static int test_overflow_terminal_sequence(void) {
    trevrpc_engine* engine = NULL;
    trevrpc_engine_event* event = NULL;
    trevrpc_engine_event_info_v1 info;
    trevrpc_engine_diagnostics_v1 diagnostics;
    trevrpc_engine_event_spec rejected_spec;
    uint8_t first = 7;
    uint8_t rejected = 8;
    uint64_t sequence = 0;
    CHECK(create_engine(1, 1, &engine) == 0);
    CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, &first, sizeof(first)) == 0);
    memset(&rejected_spec, 0, sizeof(rejected_spec));
    rejected_spec.kind = TREVRPC_ENGINE_EVENT_DIAGNOSTIC;
    rejected_spec.data = &rejected;
    rejected_spec.data_len = SIZE_MAX;
    CHECK(trevrpc_engine_provider_publish_event(engine, &rejected_spec) == -ENOSPC);
    CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, &rejected, sizeof(rejected)) == -EPIPE);

    CHECK(pop_info(engine, &event, &info) == 0);
    CHECK(info.kind == TREVRPC_ENGINE_EVENT_DIAGNOSTIC && info.flags == 0);
    CHECK(info.data_len == 1 && info.data[0] == first);
    sequence = info.sequence;
    trevrpc_engine_event_release(event);

    CHECK(pop_info(engine, &event, &info) == 0);
    CHECK(info.kind == TREVRPC_ENGINE_EVENT_DIAGNOSTIC);
    CHECK((info.flags & TREVRPC_ENGINE_EVENT_FLAG_FATAL) != 0);
    CHECK(info.status == -ENOSPC && info.sequence > sequence);
    sequence = info.sequence;
    trevrpc_engine_event_release(event);

    CHECK(pop_info(engine, &event, &info) == 0);
    CHECK(info.kind == TREVRPC_ENGINE_EVENT_STOPPED);
    CHECK((info.flags & (TREVRPC_ENGINE_EVENT_FLAG_FATAL | TREVRPC_ENGINE_EVENT_FLAG_TERMINAL)) ==
          (TREVRPC_ENGINE_EVENT_FLAG_FATAL | TREVRPC_ENGINE_EVENT_FLAG_TERMINAL));
    CHECK(info.status == -ENOSPC && info.sequence > sequence);
    trevrpc_engine_event_release(event);
    event = (trevrpc_engine_event*)(uintptr_t)0x55u;
    CHECK(trevrpc_engine_next_event(engine, &event) == -EAGAIN);
    CHECK(event == (trevrpc_engine_event*)(uintptr_t)0x55u);

    CHECK(trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
    CHECK(trevrpc_engine_get_diagnostics_v1(engine, &diagnostics) == 0);
    CHECK(diagnostics.state == TREVRPC_ENGINE_STATE_STOPPED);
    CHECK(diagnostics.terminal_status == -ENOSPC);
    CHECK(diagnostics.events_enqueued == 3);
    CHECK(diagnostics.events_dequeued == 3);
    CHECK(diagnostics.events_rejected == 2);
    CHECK(trevrpc_engine_close(engine) == 0);
    CHECK(trevrpc_engine_close(engine) == 0);
    CHECK(trevrpc_engine_release(engine) == 0);
    return 0;
}

static void count_dequeue(void* provider_context, void* hook_context) {
    unsigned* count = hook_context;
    if (provider_context != NULL) {
        ++*count;
    }
}

static void count_drop(void* provider_context, void* hook_context) {
    unsigned* count = hook_context;
    if (provider_context != NULL) {
        *count += 100;
    }
}

static int test_event_metadata_and_detachment(void) {
    trevrpc_engine* engine = NULL;
    trevrpc_engine_event* event = NULL;
    trevrpc_engine_event_info_v1 info;
    trevrpc_engine_event_spec spec;
    const uint8_t payload[] = {1, 2, 3};
    unsigned hooks = 0;
    CHECK(create_engine(2, sizeof(payload), &engine) == 0);
    memset(&spec, 0, sizeof(spec));
    spec.kind = TREVRPC_ENGINE_EVENT_SEND_COMPLETE;
    spec.flags = TREVRPC_ENGINE_EVENT_FLAG_CLIENT;
    spec.status = -ECANCELED;
    spec.subject_kind = TREVRPC_ENGINE_OBJECT_STREAM;
    spec.subject.owner = UINT64_C(0x746573742d656e67);
    spec.subject.slot = 7;
    spec.subject.generation = 3;
    spec.parent.owner = spec.subject.owner;
    spec.parent.slot = 2;
    spec.parent.generation = 4;
    spec.operation_id = 99;
    spec.application_error_code = 101;
    spec.provider_error_code = 202;
    spec.data = payload;
    spec.data_len = sizeof(payload);
    spec.dequeue_hook = count_dequeue;
    spec.drop_hook = count_drop;
    spec.hook_context = &hooks;
    CHECK(trevrpc_engine_provider_publish_event(engine, &spec) == 0);
    CHECK(pop_info(engine, &event, &info) == 0);
    CHECK(hooks == 1);
    CHECK(info.kind == spec.kind && info.flags == spec.flags && info.status == spec.status);
    CHECK(info.subject_kind == spec.subject_kind);
    CHECK(memcmp(&info.subject, &spec.subject, sizeof(info.subject)) == 0);
    CHECK(memcmp(&info.parent, &spec.parent, sizeof(info.parent)) == 0);
    CHECK(info.operation_id == spec.operation_id);
    CHECK(info.application_error_code == spec.application_error_code);
    CHECK(info.provider_error_code == spec.provider_error_code);
    CHECK(info.data_len == sizeof(payload) && memcmp(info.data, payload, sizeof(payload)) == 0);
    CHECK(trevrpc_engine_release(engine) == 0);
    CHECK(trevrpc_engine_event_get_info_v1(event, &info) == 0);
    trevrpc_engine_event_release(event);
    CHECK(hooks == 1);
    return 0;
}

static int test_event_survives_engine_release(void) {
    trevrpc_engine* engine = NULL;
    trevrpc_engine_event* event = NULL;
    trevrpc_engine_event_info_v1 info;
    const char source[] = "owned payload";
    CHECK(create_engine(2, sizeof(source), &engine) == 0);
    CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, source, sizeof(source)) == 0);
    CHECK(trevrpc_engine_next_event(engine, &event) == 0);
    CHECK(trevrpc_engine_release(engine) == 0);
    CHECK(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
    CHECK(trevrpc_engine_event_get_info_v1(event, &info) == 0);
    CHECK(info.data_len == sizeof(source));
    CHECK(memcmp(info.data, source, sizeof(source)) == 0);
    trevrpc_engine_event_release(event);
    return 0;
}

static void release_owned_receive(void* owner, void* release_context) {
    unsigned* releases = release_context;
    free(owner);
    ++*releases;
}

static int test_receive_detachment(void) {
    trevrpc_engine* engine = NULL;
    trevrpc_engine_receive* copied = NULL;
    trevrpc_engine_receive* owned = NULL;
    trevrpc_engine_receive_info_v1 info;
    const uint8_t source[] = {4, 5, 6};
    uint8_t* allocation;
    unsigned releases = 0;
    CHECK(create_engine(1, 0, &engine) == 0);
    CHECK(trevrpc_engine_receive_create_copy(source, sizeof(source), TREVRPC_ENGINE_RECEIVE_FLAG_NONE, &copied) == 0);
    allocation = malloc(sizeof(source));
    CHECK(allocation != NULL);
    memcpy(allocation, source, sizeof(source));
    CHECK(trevrpc_engine_receive_create_owned(allocation,
              sizeof(source),
              TREVRPC_ENGINE_RECEIVE_FLAG_NONE,
              allocation,
              release_owned_receive,
              &releases,
              &owned) == 0);
    CHECK(trevrpc_engine_release(engine) == 0);
    CHECK(trevrpc_engine_receive_info_v1_init(&info, sizeof(info)) == 0);
    CHECK(trevrpc_engine_receive_get_info_v1(copied, &info) == 0);
    CHECK(info.data_len == sizeof(source) && memcmp(info.data, source, sizeof(source)) == 0);
    trevrpc_engine_receive_release(copied);
    CHECK(trevrpc_engine_receive_get_info_v1(owned, &info) == 0);
    CHECK(info.data_len == sizeof(source) && memcmp(info.data, source, sizeof(source)) == 0);
    trevrpc_engine_receive_release(owned);
    CHECK(releases == 1);
    return 0;
}

static int test_structure_failures_leave_outputs(void) {
    trevrpc_engine* engine = NULL;
    trevrpc_engine_wake_source_v1 wake;
    trevrpc_engine_wake_source_v1 before;
    CHECK(create_engine(1, 0, &engine) == 0);
    memset(&wake, 0, sizeof(wake));
    wake.struct_size = sizeof(wake);
    wake.struct_version = 2;
    before = wake;
    CHECK(trevrpc_engine_get_wake_source_v1(engine, &wake) == -ENOTSUP);
    CHECK(memcmp(&wake, &before, sizeof(wake)) == 0);
    wake.struct_version = 1;
    wake.reserved[0] = 1;
    before = wake;
    CHECK(trevrpc_engine_get_wake_source_v1(engine, &wake) == -EINVAL);
    CHECK(memcmp(&wake, &before, sizeof(wake)) == 0);
    CHECK(trevrpc_engine_release(engine) == 0);
    return 0;
}

int main(void) {
    CHECK(test_configuration_validation() == 0);
    CHECK(test_empty_and_output_guarantees() == 0);
    CHECK(test_fifo_wraparound_and_copy() == 0);
    CHECK(test_overflow_terminal_sequence() == 0);
    CHECK(test_event_metadata_and_detachment() == 0);
    CHECK(test_event_survives_engine_release() == 0);
    CHECK(test_receive_detachment() == 0);
    CHECK(test_structure_failures_leave_outputs() == 0);
    return 0;
}
