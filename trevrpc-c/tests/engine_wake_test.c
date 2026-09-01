#include "trevrpc_engine.h"
#include "trevrpc_engine_testing_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

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

static int get_read_fd(trevrpc_engine* engine) {
    trevrpc_engine_wake_source_v1 wake;
    if (trevrpc_engine_wake_source_v1_init(&wake, sizeof(wake)) != 0 ||
        trevrpc_engine_get_wake_source_v1(engine, &wake) != 0 ||
        trevrpc_engine_get_wake_source_v1(engine, &wake) != 0) {
        return -1;
    }
    return (int)wake.native_handle;
}

static int readable(int descriptor, int timeout_ms) {
    struct pollfd poll_descriptor = {
        .fd = descriptor,
        .events = POLLIN,
        .revents = 0,
    };
    int result;
    do {
        result = poll(&poll_descriptor, 1, timeout_ms);
    } while (result < 0 && errno == EINTR);
    if (result <= 0) {
        return result;
    }
    return (poll_descriptor.revents & (POLLIN | POLLHUP | POLLERR | POLLNVAL)) != 0;
}

static int pop_release(trevrpc_engine* engine) {
    trevrpc_engine_event* event = NULL;
    int result = trevrpc_engine_next_event(engine, &event);
    if (result == 0) {
        trevrpc_engine_event_release(event);
    }
    return result;
}

static int test_level_triggered_queue(void) {
    trevrpc_engine* engine = NULL;
    trevrpc_engine_diagnostics_v1 diagnostics;
    uint32_t value;
    int descriptor;
    CHECK(create_engine(4, &engine) == 0);
    descriptor = get_read_fd(engine);
    CHECK(descriptor >= 0);
    value = 1;
    CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, &value, sizeof(value)) == 0);
    CHECK(readable(descriptor, 1000) == 1);
    for (value = 2; value <= 4; ++value) {
        CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, &value, sizeof(value)) == 0);
    }
    CHECK(trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
    CHECK(trevrpc_engine_get_diagnostics_v1(engine, &diagnostics) == 0);
    CHECK(diagnostics.wake_signals == 1);
    CHECK(pop_release(engine) == 0);
    CHECK(readable(descriptor, 0) == 1);
    CHECK(pop_release(engine) == 0);
    CHECK(pop_release(engine) == 0);
    CHECK(readable(descriptor, 0) == 1);
    CHECK(pop_release(engine) == 0);
    CHECK(readable(descriptor, 0) == 0);
    value = 5;
    CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, &value, sizeof(value)) == 0);
    CHECK(readable(descriptor, 1000) == 1);
    CHECK(pop_release(engine) == 0);
    CHECK(readable(descriptor, 0) == 0);
    CHECK(trevrpc_engine_release(engine) == 0);
    return 0;
}

struct pop_thread_context {
    trevrpc_engine* engine;
    int result;
};

static void* pop_thread(void* argument) {
    struct pop_thread_context* context = argument;
    context->result = pop_release(context->engine);
    return NULL;
}

struct enqueue_thread_context {
    trevrpc_engine* engine;
    atomic_bool started;
    int result;
};

static void* enqueue_thread(void* argument) {
    struct enqueue_thread_context* context = argument;
    uint32_t value = 2;
    atomic_store_explicit(&context->started, true, memory_order_release);
    context->result = trevrpc_engine_testing_enqueue_diagnostic_copy(context->engine, &value, sizeof(value));
    return NULL;
}

static int test_last_pop_drain_window(void) {
    trevrpc_engine* engine = NULL;
    struct pop_thread_context pop_context;
    struct enqueue_thread_context enqueue_context;
    pthread_t consumer;
    pthread_t producer;
    uint32_t value = 1;
    int descriptor;
    CHECK(create_engine(2, &engine) == 0);
    descriptor = get_read_fd(engine);
    CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, &value, sizeof(value)) == 0);
    trevrpc_engine_testing_pause_last_event_drain(engine);
    pop_context.engine = engine;
    pop_context.result = -1;
    CHECK(pthread_create(&consumer, NULL, pop_thread, &pop_context) == 0);
    trevrpc_engine_testing_pause_last_event_drain(engine);
    enqueue_context.engine = engine;
    atomic_init(&enqueue_context.started, false);
    enqueue_context.result = -1;
    CHECK(pthread_create(&producer, NULL, enqueue_thread, &enqueue_context) == 0);
    while (!atomic_load_explicit(&enqueue_context.started, memory_order_acquire)) {
        sched_yield();
    }
    trevrpc_engine_testing_resume_last_event_drain(engine);
    CHECK(pthread_join(consumer, NULL) == 0);
    CHECK(pthread_join(producer, NULL) == 0);
    CHECK(pop_context.result == 0);
    CHECK(enqueue_context.result == 0);
    CHECK(readable(descriptor, 1000) == 1);
    CHECK(pop_release(engine) == 0);
    CHECK(readable(descriptor, 0) == 0);
    CHECK(trevrpc_engine_release(engine) == 0);
    return 0;
}

static int test_pipe_full_eagain(void) {
    trevrpc_engine* engine = NULL;
    trevrpc_engine_diagnostics_v1 diagnostics;
    uint8_t byte = 0;
    uint32_t value = 1;
    int write_descriptor;
    int read_descriptor;
    ssize_t result;
    CHECK(create_engine(1, &engine) == 0);
    read_descriptor = get_read_fd(engine);
    write_descriptor = trevrpc_engine_testing_get_write_fd(engine);
    CHECK(write_descriptor >= 0);
    do {
        result = write(write_descriptor, &byte, sizeof(byte));
    } while (result == 1);
    CHECK(result < 0 && errno == EAGAIN);
    CHECK(readable(read_descriptor, 0) == 1);
    CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, &value, sizeof(value)) == 0);
    CHECK(trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
    CHECK(trevrpc_engine_get_diagnostics_v1(engine, &diagnostics) == 0);
    CHECK(diagnostics.wake_write_eagain == 1);
    CHECK(diagnostics.wake_signals == 1);
    CHECK(pop_release(engine) == 0);
    CHECK(readable(read_descriptor, 0) == 0);
    CHECK(trevrpc_engine_release(engine) == 0);
    return 0;
}

static int test_wake_failures(void) {
    trevrpc_engine* engine = NULL;
    trevrpc_engine_diagnostics_v1 diagnostics;
    trevrpc_engine_event* event = NULL;
    trevrpc_engine_event_info_v1 info;
    uint32_t value = 1;
    unsigned events = 0;
    CHECK(create_engine(1, &engine) == 0);
    CHECK(trevrpc_engine_testing_force_wake_failure(engine, TREVRPC_ENGINE_TEST_WAKE_FAILURE_WRITE, UINT64_MAX - 1u) ==
          0);
    CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, &value, sizeof(value)) == 0);
    for (;;) {
        int result = trevrpc_engine_next_event(engine, &event);
        if (result == -EAGAIN) {
            break;
        }
        CHECK(result == 0);
        CHECK(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
        CHECK(trevrpc_engine_event_get_info_v1(event, &info) == 0);
        ++events;
        trevrpc_engine_event_release(event);
    }
    CHECK(events == 3);
    CHECK(trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
    CHECK(trevrpc_engine_get_diagnostics_v1(engine, &diagnostics) == 0);
    CHECK(diagnostics.terminal_status == -EBADF);
    CHECK(diagnostics.wake_failures == UINT64_MAX);
    CHECK(trevrpc_engine_release(engine) == 0);

    CHECK(create_engine(1, &engine) == 0);
    CHECK(trevrpc_engine_testing_enqueue_diagnostic_copy(engine, &value, sizeof(value)) == 0);
    CHECK(trevrpc_engine_testing_force_wake_failure(engine, TREVRPC_ENGINE_TEST_WAKE_FAILURE_READ, 0) == 0);
    CHECK(pop_release(engine) == 0);
    CHECK(trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
    CHECK(trevrpc_engine_get_diagnostics_v1(engine, &diagnostics) == 0);
    CHECK(diagnostics.terminal_status == -EBADF);
    CHECK(diagnostics.wake_failures == 1);
    CHECK(trevrpc_engine_release(engine) == 0);
    return 0;
}

static int descriptor_is_closed(const char* descriptor_text) {
    char* end = NULL;
    long descriptor;
    errno = 0;
    descriptor = strtol(descriptor_text, &end, 10);
    if (errno != 0 || end == descriptor_text || *end != '\0' || descriptor < 0 || descriptor > INT32_MAX) {
        return 0;
    }
    errno = 0;
    return fcntl((int)descriptor, F_GETFD) == -1 && errno == EBADF;
}

static int test_descriptor_flags_and_exec(const char* executable) {
    trevrpc_engine* engine = NULL;
    int read_descriptor;
    int write_descriptor;
    int descriptor_flags;
    int status;
    unsigned iteration;
    char read_text[32];
    char write_text[32];
    CHECK(create_engine(1, &engine) == 0);
    read_descriptor = get_read_fd(engine);
    write_descriptor = trevrpc_engine_testing_get_write_fd(engine);
    CHECK(read_descriptor >= 0);
    CHECK(write_descriptor >= 0);

    descriptor_flags = fcntl(read_descriptor, F_GETFL);
    CHECK(descriptor_flags >= 0);
    CHECK((descriptor_flags & O_NONBLOCK) != 0);
    descriptor_flags = fcntl(write_descriptor, F_GETFL);
    CHECK(descriptor_flags >= 0);
    CHECK((descriptor_flags & O_NONBLOCK) != 0);
    descriptor_flags = fcntl(read_descriptor, F_GETFD);
    CHECK(descriptor_flags >= 0);
    CHECK((descriptor_flags & FD_CLOEXEC) != 0);
    descriptor_flags = fcntl(write_descriptor, F_GETFD);
    CHECK(descriptor_flags >= 0);
    CHECK((descriptor_flags & FD_CLOEXEC) != 0);

    CHECK(snprintf(read_text, sizeof(read_text), "%d", read_descriptor) > 0);
    CHECK(snprintf(write_text, sizeof(write_text), "%d", write_descriptor) > 0);
    for (iteration = 0; iteration < 16; ++iteration) {
        pid_t child = fork();
        CHECK(child >= 0);
        if (child == 0) {
            execl(executable, executable, "--check-closed-descriptors", read_text, write_text, (char*)NULL);
            _exit(127);
        }
        pid_t waited;
        do {
            waited = waitpid(child, &status, 0);
        } while (waited < 0 && errno == EINTR);
        CHECK(waited == child);
        CHECK(WIFEXITED(status));
        CHECK(WEXITSTATUS(status) == 0);
    }
    CHECK(trevrpc_engine_release(engine) == 0);
    return 0;
}

static int test_terminal_drain_failure_ordering(void) {
    trevrpc_engine* engine = NULL;
    trevrpc_engine_diagnostics_v1 diagnostics;
    trevrpc_engine_event* event = NULL;
    trevrpc_engine_event_info_v1 info;
    uint64_t fatal_sequence;
    CHECK(create_engine(1, &engine) == 0);
    CHECK(trevrpc_engine_close(engine) == 0);
    CHECK(trevrpc_engine_testing_force_wake_failure(engine, TREVRPC_ENGINE_TEST_WAKE_FAILURE_READ, 0) == 0);

    CHECK(trevrpc_engine_next_event(engine, &event) == 0);
    CHECK(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
    CHECK(trevrpc_engine_event_get_info_v1(event, &info) == 0);
    CHECK(info.kind == TREVRPC_ENGINE_EVENT_DIAGNOSTIC);
    CHECK(info.flags == TREVRPC_ENGINE_EVENT_FLAG_FATAL);
    CHECK(info.status == -EBADF);
    fatal_sequence = info.sequence;
    trevrpc_engine_event_release(event);
    event = NULL;

    CHECK(trevrpc_engine_next_event(engine, &event) == 0);
    CHECK(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
    CHECK(trevrpc_engine_event_get_info_v1(event, &info) == 0);
    CHECK(info.kind == TREVRPC_ENGINE_EVENT_STOPPED);
    CHECK(info.flags == (TREVRPC_ENGINE_EVENT_FLAG_FATAL | TREVRPC_ENGINE_EVENT_FLAG_TERMINAL));
    CHECK(info.status == -EBADF);
    CHECK(info.sequence > fatal_sequence);
    trevrpc_engine_event_release(event);
    event = NULL;
    CHECK(trevrpc_engine_next_event(engine, &event) == -EAGAIN);
    CHECK(event == NULL);

    CHECK(trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
    CHECK(trevrpc_engine_get_diagnostics_v1(engine, &diagnostics) == 0);
    CHECK(diagnostics.state == TREVRPC_ENGINE_STATE_STOPPED);
    CHECK(diagnostics.terminal_status == -EBADF);
    CHECK(diagnostics.queue_depth == 0);
    CHECK(diagnostics.events_enqueued == 2);
    CHECK(diagnostics.events_dequeued == 2);
    CHECK(diagnostics.wake_failures == 1);
    CHECK(trevrpc_engine_release(engine) == 0);
    return 0;
}

static int test_close_terminal_and_descriptor_lifetime(void) {
    trevrpc_engine* engine = NULL;
    trevrpc_engine_event* event = NULL;
    trevrpc_engine_event_info_v1 info;
    int descriptor;
    CHECK(create_engine(1, &engine) == 0);
    descriptor = get_read_fd(engine);
    CHECK(trevrpc_engine_close(engine) == 0);
    CHECK(readable(descriptor, 1000) == 1);
    CHECK(trevrpc_engine_next_event(engine, &event) == 0);
    CHECK(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
    CHECK(trevrpc_engine_event_get_info_v1(event, &info) == 0);
    CHECK(info.kind == TREVRPC_ENGINE_EVENT_STOPPED);
    CHECK((info.flags & TREVRPC_ENGINE_EVENT_FLAG_TERMINAL) != 0);
    trevrpc_engine_event_release(event);
    CHECK(readable(descriptor, 0) == 0);
    CHECK(trevrpc_engine_release(engine) == 0);
    errno = 0;
    CHECK(fcntl(descriptor, F_GETFD) == -1);
    CHECK(errno == EBADF);
    return 0;
}

int main(int argc, char** argv) {
    if (argc == 4 && strcmp(argv[1], "--check-closed-descriptors") == 0) {
        return descriptor_is_closed(argv[2]) && descriptor_is_closed(argv[3]) ? 0 : 1;
    }
    CHECK(argc >= 1);
    CHECK(test_level_triggered_queue() == 0);
    CHECK(test_last_pop_drain_window() == 0);
    CHECK(test_pipe_full_eagain() == 0);
    CHECK(test_wake_failures() == 0);
    CHECK(test_descriptor_flags_and_exec(argv[0]) == 0);
    CHECK(test_terminal_drain_failure_ordering() == 0);
    CHECK(test_close_terminal_and_descriptor_lifetime() == 0);
    return 0;
}
