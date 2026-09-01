#include "trevrpc_engine.h"
#include "trevrpc_engine_testing_internal.h"

#include <errno.h> // IWYU pragma: keep
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
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

static int create_engine(uint32_t capacity, trevrpc_engine** engine) {
    trevrpc_engine_config_v1 config;
    int result = trevrpc_engine_config_v1_init(&config, sizeof(config));
    if (result != 0) {
        return result;
    }
    config.event_capacity = capacity;
    return trevrpc_engine_testing_create(&config, engine);
}

static int get_diagnostics(trevrpc_engine* engine, trevrpc_engine_diagnostics_v1* diagnostics) {
    int result = trevrpc_engine_diagnostics_v1_init(diagnostics, sizeof(*diagnostics));
    if (result != 0) {
        return result;
    }
    return trevrpc_engine_get_diagnostics_v1(engine, diagnostics);
}

struct call_context {
    trevrpc_engine* engine;
    atomic_bool started;
    atomic_bool done;
    int result;
};

static void* close_thread(void* argument) {
    struct call_context* context = argument;
    atomic_store_explicit(&context->started, true, memory_order_release);
    context->result = trevrpc_engine_close(context->engine);
    atomic_store_explicit(&context->done, true, memory_order_release);
    return NULL;
}

static void* drain_thread(void* argument) {
    struct call_context* context = argument;
    atomic_store_explicit(&context->started, true, memory_order_release);
    context->result = trevrpc_engine_drain(context->engine);
    atomic_store_explicit(&context->done, true, memory_order_release);
    return NULL;
}

static void* release_thread(void* argument) {
    struct call_context* context = argument;
    atomic_store_explicit(&context->started, true, memory_order_release);
    context->result = trevrpc_engine_release(context->engine);
    atomic_store_explicit(&context->done, true, memory_order_release);
    return NULL;
}

static void* producer_release_thread(void* argument) {
    struct call_context* context = argument;
    atomic_store_explicit(&context->started, true, memory_order_release);
    context->result = trevrpc_engine_testing_producer_release(context->engine);
    atomic_store_explicit(&context->done, true, memory_order_release);
    return NULL;
}

static void* diagnostics_call_thread(void* argument) {
    struct call_context* context = argument;
    trevrpc_engine_diagnostics_v1 diagnostics;
    atomic_store_explicit(&context->started, true, memory_order_release);
    context->result = get_diagnostics(context->engine, &diagnostics);
    atomic_store_explicit(&context->done, true, memory_order_release);
    return NULL;
}

static void initialize_context(struct call_context* context, trevrpc_engine* engine) {
    context->engine = engine;
    atomic_init(&context->started, false);
    atomic_init(&context->done, false);
    context->result = -1;
}

static int test_close_and_drain(void) {
    enum { THREAD_COUNT = 8 };
    trevrpc_engine* engine = NULL;
    trevrpc_engine_diagnostics_v1 diagnostics;
    struct call_context contexts[THREAD_COUNT];
    pthread_t threads[THREAD_COUNT];
    trevrpc_engine_event* event = NULL;
    trevrpc_engine_event_info_v1 info;
    unsigned index;
    unsigned stopped_count = 0;
    CHECK(create_engine(4, &engine) == 0);
    CHECK(get_diagnostics(engine, &diagnostics) == 0);
    CHECK(diagnostics.state == TREVRPC_ENGINE_STATE_RUNNING);
    for (index = 0; index < THREAD_COUNT; ++index) {
        initialize_context(&contexts[index], engine);
        CHECK(pthread_create(&threads[index], NULL, close_thread, &contexts[index]) == 0);
    }
    for (index = 0; index < THREAD_COUNT; ++index) {
        CHECK(pthread_join(threads[index], NULL) == 0);
        CHECK(contexts[index].result == 0);
    }
    CHECK(get_diagnostics(engine, &diagnostics) == 0);
    CHECK(diagnostics.state == TREVRPC_ENGINE_STATE_STOPPED);
    CHECK(diagnostics.terminal_status == 0);
    for (;;) {
        int result = trevrpc_engine_next_event(engine, &event);
        if (result == -EAGAIN) {
            break;
        }
        CHECK(result == 0);
        CHECK(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
        CHECK(trevrpc_engine_event_get_info_v1(event, &info) == 0);
        if (info.kind == TREVRPC_ENGINE_EVENT_STOPPED) {
            ++stopped_count;
        }
        trevrpc_engine_event_release(event);
    }
    CHECK(stopped_count == 1);
    CHECK(trevrpc_engine_drain(engine) == 0);
    CHECK(trevrpc_engine_release(engine) == 0);
    CHECK(trevrpc_engine_release(NULL) == 0);
    return 0;
}

static int test_held_producer_ordering(void) {
    trevrpc_engine* engine = NULL;
    trevrpc_engine_diagnostics_v1 diagnostics;
    struct call_context drain_context;
    pthread_t thread;
    trevrpc_engine_event* event = NULL;
    trevrpc_engine_event_info_v1 info;
    uint32_t first = 1;
    uint32_t second = 2;
    uint64_t previous_sequence = 0;
    unsigned ordinary_count = 0;
    unsigned stopped_count = 0;
    CHECK(create_engine(4, &engine) == 0);
    CHECK(trevrpc_engine_testing_producer_acquire(engine) == 0);
    CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, &first, sizeof(first)) == 0);
    initialize_context(&drain_context, engine);
    CHECK(pthread_create(&thread, NULL, drain_thread, &drain_context) == 0);
    while (!atomic_load_explicit(&drain_context.started, memory_order_acquire)) {
        sched_yield();
    }
    for (;;) {
        CHECK(get_diagnostics(engine, &diagnostics) == 0);
        if (diagnostics.state == TREVRPC_ENGINE_STATE_STOPPING) {
            break;
        }
        sched_yield();
    }
    CHECK(!atomic_load_explicit(&drain_context.done, memory_order_acquire));
    CHECK(trevrpc_engine_testing_producer_acquire(engine) == -EPIPE);
    CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, &second, sizeof(second)) == 0);
    CHECK(trevrpc_engine_testing_producer_release(engine) == 0);
    CHECK(pthread_join(thread, NULL) == 0);
    CHECK(drain_context.result == 0);
    CHECK(get_diagnostics(engine, &diagnostics) == 0);
    CHECK(diagnostics.state == TREVRPC_ENGINE_STATE_STOPPED);
    for (;;) {
        int result = trevrpc_engine_next_event(engine, &event);
        if (result == -EAGAIN) {
            break;
        }
        CHECK(result == 0);
        CHECK(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
        CHECK(trevrpc_engine_event_get_info_v1(event, &info) == 0);
        CHECK(info.sequence > previous_sequence);
        previous_sequence = info.sequence;
        if (info.kind == TREVRPC_ENGINE_EVENT_DIAGNOSTIC) {
            uint32_t expected = ordinary_count == 0 ? first : second;
            CHECK(info.data_len == sizeof(expected));
            CHECK(memcmp(info.data, &expected, sizeof(expected)) == 0);
            ++ordinary_count;
        } else if (info.kind == TREVRPC_ENGINE_EVENT_STOPPED) {
            ++stopped_count;
            CHECK(ordinary_count == 2);
        }
        trevrpc_engine_event_release(event);
    }
    CHECK(ordinary_count == 2);
    CHECK(stopped_count == 1);
    CHECK(trevrpc_engine_release(engine) == 0);
    return 0;
}

struct paused_pop_context {
    trevrpc_engine* engine;
    trevrpc_engine_event* event;
    int result;
};

static void* paused_pop_thread(void* argument) {
    struct paused_pop_context* context = argument;
    context->result = trevrpc_engine_next_event(context->engine, &context->event);
    return NULL;
}

static int test_release_waits_for_pre_mutex_admission(void) {
    trevrpc_engine* engine = NULL;
    struct call_context diagnostics_context;
    struct call_context release_context;
    pthread_t caller;
    pthread_t releaser;
    CHECK(create_engine(2, &engine) == 0);
    trevrpc_engine_testing_pause_api_enter(engine);
    initialize_context(&diagnostics_context, engine);
    CHECK(pthread_create(&caller, NULL, diagnostics_call_thread, &diagnostics_context) == 0);
    trevrpc_engine_testing_wait_for_api_enter(engine);
    initialize_context(&release_context, engine);
    CHECK(pthread_create(&releaser, NULL, release_thread, &release_context) == 0);
    while (!trevrpc_engine_testing_release_is_waiting(engine)) {
        sched_yield();
    }
    CHECK(!atomic_load_explicit(&release_context.done, memory_order_acquire));
    trevrpc_engine_testing_resume_api_enter(engine);
    CHECK(pthread_join(caller, NULL) == 0);
    CHECK(diagnostics_context.result == 0);
    CHECK(pthread_join(releaser, NULL) == 0);
    CHECK(release_context.result == 0);
    return 0;
}

static int test_release_admission_gate(void) {
    trevrpc_engine* engine = NULL;
    struct paused_pop_context pop_context;
    struct call_context release_context;
    trevrpc_engine_diagnostics_v1 diagnostics;
    trevrpc_engine_diagnostics_v1 before;
    pthread_t consumer;
    pthread_t releaser;
    uint32_t value = 1;
    CHECK(create_engine(2, &engine) == 0);
    CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, &value, sizeof(value)) == 0);
    trevrpc_engine_testing_pause_api_leave(engine);
    pop_context.engine = engine;
    pop_context.event = NULL;
    pop_context.result = -1;
    CHECK(pthread_create(&consumer, NULL, paused_pop_thread, &pop_context) == 0);
    trevrpc_engine_testing_wait_for_api_leave(engine);
    initialize_context(&release_context, engine);
    CHECK(pthread_create(&releaser, NULL, release_thread, &release_context) == 0);
    while (!trevrpc_engine_testing_release_is_waiting(engine)) {
        sched_yield();
    }
    CHECK(!atomic_load_explicit(&release_context.done, memory_order_acquire));
    CHECK(trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
    before = diagnostics;
    CHECK(trevrpc_engine_get_diagnostics_v1(engine, &diagnostics) == -EPIPE);
    CHECK(memcmp(&diagnostics, &before, sizeof(diagnostics)) == 0);
    trevrpc_engine_testing_resume_api_leave(engine);
    CHECK(pthread_join(consumer, NULL) == 0);
    CHECK(pop_context.result == 0);
    CHECK(pthread_join(releaser, NULL) == 0);
    CHECK(release_context.result == 0);
    trevrpc_engine_event_release(pop_context.event);
    return 0;
}

static int test_cross_thread_operation_unpin(void) {
    trevrpc_engine* engine = NULL;
    struct call_context unpin_context;
    pthread_t unpin_thread;
    CHECK(create_engine(1, &engine) == 0);
    CHECK(trevrpc_engine_testing_producer_acquire(engine) == 0);
    initialize_context(&unpin_context, engine);
    CHECK(pthread_create(&unpin_thread, NULL, producer_release_thread, &unpin_context) == 0);
    CHECK(pthread_join(unpin_thread, NULL) == 0);
    CHECK(unpin_context.result == 0);
    CHECK(trevrpc_engine_release(engine) == 0);
    return 0;
}

static int test_concurrent_drain_and_release(void) {
    enum { ITERATIONS = 200 };
    unsigned iteration;
    for (iteration = 0; iteration < ITERATIONS; ++iteration) {
        trevrpc_engine* engine = NULL;
        trevrpc_engine_diagnostics_v1 diagnostics;
        struct call_context drain_context;
        struct call_context release_context;
        pthread_t drainer;
        pthread_t releaser;
        CHECK(create_engine(1, &engine) == 0);
        CHECK(trevrpc_engine_testing_producer_acquire(engine) == 0);
        initialize_context(&drain_context, engine);
        CHECK(pthread_create(&drainer, NULL, drain_thread, &drain_context) == 0);
        while (!atomic_load_explicit(&drain_context.started, memory_order_acquire)) {
            sched_yield();
        }
        for (;;) {
            CHECK(get_diagnostics(engine, &diagnostics) == 0);
            if (diagnostics.state == TREVRPC_ENGINE_STATE_STOPPING) {
                break;
            }
            sched_yield();
        }
        initialize_context(&release_context, engine);
        CHECK(pthread_create(&releaser, NULL, release_thread, &release_context) == 0);
        while (!trevrpc_engine_testing_release_is_waiting(engine)) {
            sched_yield();
        }
        CHECK(trevrpc_engine_testing_producer_release(engine) == 0);
        CHECK(pthread_join(drainer, NULL) == 0);
        CHECK(drain_context.result == 0);
        CHECK(pthread_join(releaser, NULL) == 0);
        CHECK(release_context.result == 0);
    }
    return 0;
}

#define STRESS_PRODUCERS 4u
#define STRESS_CONSUMERS 4u
#define STRESS_PER_PRODUCER 1000u
#define STRESS_TOTAL (STRESS_PRODUCERS * STRESS_PER_PRODUCER)

struct stress_payload {
    uint32_t id;
    uint32_t checksum;
};

struct stress_context {
    trevrpc_engine* engine;
    atomic_uchar* seen;
    atomic_uint consumed;
    atomic_uint errors;
    atomic_bool stopped_seen;
    atomic_bool diagnostics_stop;
};

struct stress_producer_context {
    struct stress_context* shared;
    uint32_t producer;
};

static void* stress_producer(void* argument) {
    struct stress_producer_context* context = argument;
    uint32_t index;
    for (index = 0; index < STRESS_PER_PRODUCER; ++index) {
        struct stress_payload payload;
        payload.id = context->producer * STRESS_PER_PRODUCER + index;
        payload.checksum = payload.id ^ 0xa5a55a5au;
        if (trevrpc_engine_testing_enqueue_diagnostic_copy(context->shared->engine, &payload, sizeof(payload)) != 0) {
            atomic_fetch_add_explicit(&context->shared->errors, 1, memory_order_relaxed);
        }
    }
    return NULL;
}

static void* stress_consumer(void* argument) {
    struct stress_context* context = argument;
    for (;;) {
        trevrpc_engine_event* event = NULL;
        trevrpc_engine_event_info_v1 info;
        int result = trevrpc_engine_next_event(context->engine, &event);
        if (result == -EAGAIN) {
            if (atomic_load_explicit(&context->stopped_seen, memory_order_acquire)) {
                return NULL;
            }
            sched_yield();
            continue;
        }
        if (result != 0 || trevrpc_engine_event_info_v1_init(&info, sizeof(info)) != 0 ||
            trevrpc_engine_event_get_info_v1(event, &info) != 0) {
            atomic_fetch_add_explicit(&context->errors, 1, memory_order_relaxed);
            trevrpc_engine_event_release(event);
            return NULL;
        }
        if (info.kind == TREVRPC_ENGINE_EVENT_STOPPED) {
            atomic_store_explicit(&context->stopped_seen, true, memory_order_release);
            trevrpc_engine_event_release(event);
            return NULL;
        }
        if (info.kind != TREVRPC_ENGINE_EVENT_DIAGNOSTIC || info.flags != 0 ||
            info.data_len != sizeof(struct stress_payload)) {
            atomic_fetch_add_explicit(&context->errors, 1, memory_order_relaxed);
        } else {
            struct stress_payload payload;
            memcpy(&payload, info.data, sizeof(payload));
            if (payload.id >= STRESS_TOTAL || payload.checksum != (payload.id ^ 0xa5a55a5au) ||
                atomic_exchange_explicit(&context->seen[payload.id], 1, memory_order_relaxed) != 0) {
                atomic_fetch_add_explicit(&context->errors, 1, memory_order_relaxed);
            } else {
                atomic_fetch_add_explicit(&context->consumed, 1, memory_order_relaxed);
            }
        }
        trevrpc_engine_event_release(event);
    }
}

static void* stress_diagnostics(void* argument) {
    struct stress_context* context = argument;
    uint32_t previous_state = TREVRPC_ENGINE_STATE_RUNNING;
    while (!atomic_load_explicit(&context->diagnostics_stop, memory_order_acquire)) {
        trevrpc_engine_diagnostics_v1 diagnostics;
        if (get_diagnostics(context->engine, &diagnostics) != 0 || diagnostics.engine_abi_version != 1u ||
            diagnostics.state < previous_state) {
            atomic_fetch_add_explicit(&context->errors, 1, memory_order_relaxed);
            return NULL;
        }
        previous_state = diagnostics.state;
        sched_yield();
    }
    return NULL;
}

static int test_concurrent_stress(void) {
    trevrpc_engine* engine = NULL;
    struct stress_context context;
    struct stress_producer_context producer_contexts[STRESS_PRODUCERS];
    pthread_t producers[STRESS_PRODUCERS];
    pthread_t consumers[STRESS_CONSUMERS];
    pthread_t diagnostics_thread;
    trevrpc_engine_diagnostics_v1 diagnostics;
    unsigned index;
    CHECK(create_engine(8192, &engine) == 0);
    context.engine = engine;
    context.seen = calloc(STRESS_TOTAL, sizeof(*context.seen));
    CHECK(context.seen != NULL);
    atomic_init(&context.consumed, 0);
    atomic_init(&context.errors, 0);
    atomic_init(&context.stopped_seen, false);
    atomic_init(&context.diagnostics_stop, false);
    CHECK(pthread_create(&diagnostics_thread, NULL, stress_diagnostics, &context) == 0);
    for (index = 0; index < STRESS_CONSUMERS; ++index) {
        CHECK(pthread_create(&consumers[index], NULL, stress_consumer, &context) == 0);
    }
    for (index = 0; index < STRESS_PRODUCERS; ++index) {
        producer_contexts[index].shared = &context;
        producer_contexts[index].producer = index;
        CHECK(pthread_create(&producers[index], NULL, stress_producer, &producer_contexts[index]) == 0);
    }
    for (index = 0; index < STRESS_PRODUCERS; ++index) {
        CHECK(pthread_join(producers[index], NULL) == 0);
    }
    CHECK(trevrpc_engine_close(engine) == 0);
    for (index = 0; index < STRESS_CONSUMERS; ++index) {
        CHECK(pthread_join(consumers[index], NULL) == 0);
    }
    atomic_store_explicit(&context.diagnostics_stop, true, memory_order_release);
    CHECK(pthread_join(diagnostics_thread, NULL) == 0);
    CHECK(atomic_load_explicit(&context.errors, memory_order_relaxed) == 0);
    CHECK(atomic_load_explicit(&context.consumed, memory_order_relaxed) == STRESS_TOTAL);
    for (index = 0; index < STRESS_TOTAL; ++index) {
        CHECK(atomic_load_explicit(&context.seen[index], memory_order_relaxed) == 1);
    }
    CHECK(get_diagnostics(engine, &diagnostics) == 0);
    CHECK(diagnostics.state == TREVRPC_ENGINE_STATE_STOPPED);
    CHECK(diagnostics.events_rejected == 0);
    CHECK(diagnostics.events_enqueued == STRESS_TOTAL + 1u);
    CHECK(diagnostics.events_dequeued == STRESS_TOTAL + 1u);
    free(context.seen);
    CHECK(trevrpc_engine_release(engine) == 0);
    return 0;
}

int main(void) {
    CHECK(test_close_and_drain() == 0);
    CHECK(test_held_producer_ordering() == 0);
    CHECK(test_release_waits_for_pre_mutex_admission() == 0);
    CHECK(test_release_admission_gate() == 0);
    CHECK(test_cross_thread_operation_unpin() == 0);
    CHECK(test_concurrent_drain_and_release() == 0);
    CHECK(test_concurrent_stress() == 0);
    return 0;
}
