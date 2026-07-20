#define _POSIX_C_SOURCE 200809L

#include "trevrpc_raw.h"

#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#ifndef TREVRPC_MSQUIC_TEST_CERT
#define TREVRPC_MSQUIC_TEST_CERT ""
#endif

#ifndef TREVRPC_MSQUIC_TEST_KEY
#define TREVRPC_MSQUIC_TEST_KEY ""
#endif

#define CHECK_GOTO(condition)                                                                                          \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            result = 1;                                                                                                \
            goto cleanup;                                                                                              \
        }                                                                                                              \
    } while (0)

typedef struct test_blocker {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool entered;
    bool released;
} test_blocker;

typedef struct test_server {
    trevrpc_server* server;
    pthread_t thread;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    atomic_int* calls;
    test_blocker* blocker;
    uint16_t port;
    bool thread_started;
    bool sync_initialized;
    bool observer_set;
    bool ready;
    bool serve_done;
    int serve_result;
} test_server;

typedef struct lifecycle_counts {
    atomic_int shutdowns;
    atomic_int tickets;
    atomic_int ready_states;
} lifecycle_counts;

typedef struct call_args {
    trevrpc_channel* channel;
    int result;
} call_args;

typedef struct channel_wait_args {
    trevrpc_channel* channel;
    int result;
} channel_wait_args;

typedef struct channel_race_args {
    trevrpc_channel* channel;
    atomic_bool* stop;
    atomic_int* failures;
} channel_race_args;

typedef struct reentrant_lifecycle {
    _Atomic(trevrpc_channel*) channel;
    atomic_int ready_callbacks;
    atomic_int close_callbacks;
    atomic_int failures;
} reentrant_lifecycle;

static uint64_t test_monotonic_nanos(void) {
    struct timespec now = {0};
    (void)clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec;
}

static void test_sleep_ms(uint64_t milliseconds) {
    struct timespec duration = {
        .tv_sec = (time_t)(milliseconds / 1000),
        .tv_nsec = (long)((milliseconds % 1000) * 1000000),
    };
    while (nanosleep(&duration, &duration) != 0 && errno == EINTR) {
    }
}

static int test_unary_handler(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_response* response) {
    (void)context;
    (void)request;
    atomic_int* calls = user_data;
    atomic_fetch_add(calls, 1);
    static const uint8_t body[] = {'o', 'k'};
    return trevrpc_response_set_body(response, body, sizeof(body));
}

static int test_blocking_handler(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_response* response) {
    (void)context;
    (void)request;
    test_blocker* blocker = user_data;
    pthread_mutex_lock(&blocker->mutex);
    blocker->entered = true;
    pthread_cond_broadcast(&blocker->cond);
    while (!blocker->released) {
        pthread_cond_wait(&blocker->cond, &blocker->mutex);
    }
    pthread_mutex_unlock(&blocker->mutex);
    static const uint8_t body[] = {'l', 'a', 't', 'e'};
    return trevrpc_response_set_body(response, body, sizeof(body));
}

static int test_stream_handler(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    (void)request;
    trevrpc_stream_frame* frame = NULL;
    int err = trevrpc_stream_recv(stream, &frame);
    trevrpc_stream_frame_free(frame);
    return err;
}

static void* test_serve(void* context) {
    test_server* fixture = context;
    int result = trevrpc_server_serve(fixture->server);
    pthread_mutex_lock(&fixture->mutex);
    fixture->serve_result = result;
    fixture->serve_done = true;
    pthread_cond_broadcast(&fixture->cond);
    pthread_mutex_unlock(&fixture->mutex);
    return NULL;
}

static void test_server_transport_event(void* context, const trevrpc_transport_event* event) {
    if (event->kind != TREVRPC_TRANSPORT_EVENT_LISTENER_OPEN) {
        return;
    }
    test_server* fixture = context;
    pthread_mutex_lock(&fixture->mutex);
    fixture->ready = true;
    pthread_cond_broadcast(&fixture->cond);
    pthread_mutex_unlock(&fixture->mutex);
}

static void test_server_destroy_sync(test_server* fixture) {
    if (!fixture->sync_initialized) {
        return;
    }
    pthread_cond_destroy(&fixture->cond);
    pthread_mutex_destroy(&fixture->mutex);
    fixture->sync_initialized = false;
}

static int test_server_start(test_server* fixture, uint16_t port, atomic_int* calls, test_blocker* blocker) {
    memset(fixture, 0, sizeof(*fixture));
    fixture->calls = calls;
    fixture->blocker = blocker;
    int err = pthread_mutex_init(&fixture->mutex, NULL);
    if (err != 0) {
        return -err;
    }
    err = pthread_cond_init(&fixture->cond, NULL);
    if (err != 0) {
        pthread_mutex_destroy(&fixture->mutex);
        return -err;
    }
    fixture->sync_initialized = true;
    trevrpc_server_config config = trevrpc_default_server_config();
    config.host = "127.0.0.1";
    config.port = port;
    config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    config.key_file = TREVRPC_MSQUIC_TEST_KEY;
    config.peer_bidi_stream_count = 16;
    err = trevrpc_server_listen(&config, &fixture->server);
    if (err == 0) {
        err = trevrpc_server_register_unary(fixture->server, "test.Service", "Unary", test_unary_handler, calls);
    }
    if (err == 0) {
        err = trevrpc_server_register_unary(fixture->server, "test.Service", "Block", test_blocking_handler, blocker);
    }
    if (err == 0) {
        err = trevrpc_server_register_streaming(fixture->server,
            "test.Service",
            "Stream",
            TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING,
            test_stream_handler,
            NULL);
    }
    if (err == 0) {
        err = trevrpc_server_port(fixture->server, &fixture->port);
    }
    if (err == 0) {
        trevrpc_transport_observer observer = {
            .transport_event = test_server_transport_event,
            .user_data = fixture,
        };
        err = trevrpc_server_set_transport_observer(fixture->server, &observer);
        fixture->observer_set = err == 0;
    }
    if (err == 0) {
        err = pthread_create(&fixture->thread, NULL, test_serve, fixture);
        if (err == 0) {
            fixture->thread_started = true;
        } else {
            err = -err;
        }
    }
    if (err == 0) {
        pthread_mutex_lock(&fixture->mutex);
        while (!fixture->ready && !fixture->serve_done && err == 0) {
            int wait_err = pthread_cond_wait(&fixture->cond, &fixture->mutex);
            if (wait_err != 0) {
                err = -wait_err;
            }
        }
        if (err == 0 && !fixture->ready) {
            err = fixture->serve_result == 0 ? -ECANCELED : fixture->serve_result;
        }
        pthread_mutex_unlock(&fixture->mutex);
    }
    if (err != 0) {
        trevrpc_server_shutdown(fixture->server);
        if (fixture->thread_started) {
            (void)pthread_join(fixture->thread, NULL);
            fixture->thread_started = false;
        }
        if (fixture->observer_set) {
            trevrpc_server_clear_transport_observer(fixture->server);
            fixture->observer_set = false;
        }
        trevrpc_server_close(fixture->server);
        fixture->server = NULL;
        test_server_destroy_sync(fixture);
    }
    return err;
}

static int test_server_stop(test_server* fixture) {
    if (fixture->server == NULL) {
        return 0;
    }
    trevrpc_server_shutdown(fixture->server);
    int result = 0;
    if (fixture->thread_started) {
        int err = pthread_join(fixture->thread, NULL);
        fixture->thread_started = false;
        if (err != 0) {
            result = -err;
        } else if (fixture->serve_result != 0) {
            fprintf(stderr, "server serve failed: %d\n", fixture->serve_result);
            result = fixture->serve_result;
        }
    }
    if (fixture->observer_set) {
        trevrpc_server_clear_transport_observer(fixture->server);
        fixture->observer_set = false;
    }
    trevrpc_server_close(fixture->server);
    fixture->server = NULL;
    test_server_destroy_sync(fixture);
    return result;
}

static void test_lifecycle(void* user_data, const trevrpc_channel_event* event) {
    lifecycle_counts* counts = user_data;
    if (event->kind == TREVRPC_CHANNEL_EVENT_CONNECTION_SHUTDOWN) {
        atomic_fetch_add(&counts->shutdowns, 1);
    } else if (event->kind == TREVRPC_CHANNEL_EVENT_RESUMPTION_TICKET_RECEIVED) {
        atomic_fetch_add(&counts->tickets, 1);
    } else if (event->kind == TREVRPC_CHANNEL_EVENT_STATE_CHANGED && event->state == TREVRPC_CHANNEL_READY) {
        atomic_fetch_add(&counts->ready_states, 1);
    }
}

static int test_wait_state(trevrpc_channel* channel, uint32_t expected, uint64_t timeout_ms) {
    uint64_t deadline = test_monotonic_nanos() + timeout_ms * 1000000ull;
    while (test_monotonic_nanos() < deadline) {
        uint32_t state = 0;
        uint64_t generation = 0;
        int err = trevrpc_channel_get_state(channel, &state, &generation);
        if (err != 0) {
            return err;
        }
        if (state == expected) {
            return 0;
        }
        test_sleep_ms(5);
    }
    return TREV_MSQUIC_ERR_TIMEOUT;
}

static int test_wait_lifecycle(lifecycle_counts* counts,
    int expected_shutdowns,
    int expected_tickets,
    int expected_ready_states,
    uint64_t timeout_ms) {
    uint64_t deadline = test_monotonic_nanos() + timeout_ms * 1000000ull;
    while (test_monotonic_nanos() < deadline) {
        if (atomic_load(&counts->shutdowns) >= expected_shutdowns &&
            atomic_load(&counts->tickets) >= expected_tickets &&
            atomic_load(&counts->ready_states) >= expected_ready_states) {
            return 0;
        }
        test_sleep_ms(5);
    }
    return TREV_MSQUIC_ERR_TIMEOUT;
}

static int test_call(trevrpc_channel* channel, const char* method) {
    trevrpc_response* response = NULL;
    int err = trevrpc_channel_call_unary(channel, "test.Service", method, NULL, 0, &response);
    if (err == 0 && (response == NULL || response->status != TREVRPC_STATUS_OK)) {
        err = -EIO;
    }
    trevrpc_response_free(response);
    return err;
}

static void* test_call_thread(void* context) {
    call_args* args = context;
    args->result = test_call(args->channel, "Block");
    return NULL;
}

static void* test_wait_thread(void* context) {
    channel_wait_args* args = context;
    args->result = trevrpc_channel_wait_ready(args->channel, 0, NULL, NULL);
    return NULL;
}

static void* test_api_race_thread(void* context) {
    channel_race_args* args = context;
    while (!atomic_load(args->stop)) {
        uint32_t state = 0;
        uint64_t generation = 0;
        if (trevrpc_channel_get_state(args->channel, &state, &generation) != 0) {
            atomic_fetch_add(args->failures, 1);
            break;
        }

        trevrpc_response* response = NULL;
        int err = trevrpc_channel_call_unary(args->channel, "test.Service", "Unary", NULL, 0, &response);
        if (err != TREV_MSQUIC_ERR_CLOSED) {
            atomic_fetch_add(args->failures, 1);
        }
        trevrpc_response_free(response);

        trevrpc_stream* stream = NULL;
        err = trevrpc_channel_start_stream(
            args->channel, "test.Service", "Stream", TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, NULL, 0, &stream);
        if (err != TREV_MSQUIC_ERR_CLOSED) {
            atomic_fetch_add(args->failures, 1);
        }
        trevrpc_stream_close(stream);
    }
    return NULL;
}

static void test_reentrant_lifecycle(void* user_data, const trevrpc_channel_event* event) {
    reentrant_lifecycle* lifecycle = user_data;
    if (event->kind != TREVRPC_CHANNEL_EVENT_STATE_CHANGED || event->state != TREVRPC_CHANNEL_READY) {
        return;
    }
    if (atomic_fetch_add(&lifecycle->ready_callbacks, 1) != 0) {
        return;
    }
    trevrpc_channel* channel = NULL;
    while ((channel = atomic_load(&lifecycle->channel)) == NULL) {
        sched_yield();
    }
    uint32_t state = 0;
    uint64_t generation = 0;
    if (trevrpc_channel_get_state(channel, &state, &generation) != 0 || generation == 0) {
        atomic_fetch_add(&lifecycle->failures, 1);
    }
    trevrpc_channel_close(channel);
    atomic_fetch_add(&lifecycle->close_callbacks, 1);
}

static int test_channel_reconnect_and_ownership(void) {
    int result = 1;
    atomic_int calls;
    atomic_init(&calls, 0);
    test_blocker blocker = {0};
    pthread_mutex_init(&blocker.mutex, NULL);
    pthread_cond_init(&blocker.cond, NULL);
    lifecycle_counts lifecycle = {0};
    test_server server = {0};
    trevrpc_channel_options* options = NULL;
    trevrpc_channel* channel = NULL;
    trevrpc_stream* stream = NULL;
    trevrpc_cancellation* cancellation = NULL;
    pthread_t call_thread = {0};
    bool call_thread_started = false;
    uint64_t first_generation = 0;
    uint16_t port = 0;

    CHECK_GOTO(test_server_start(&server, 0, &calls, &blocker) == 0);
    port = server.port;
    options = trevrpc_channel_options_new();
    CHECK_GOTO(options != NULL);
    CHECK_GOTO(trevrpc_channel_options_set_backoff(options, 20, 100, 0) == 0);
    CHECK_GOTO(trevrpc_channel_options_set_lifecycle_callback(options, test_lifecycle, &lifecycle) == 0);
    trevrpc_config config = trevrpc_default_config();
    config.skip_certificate_validation = 1;
    char host[] = "127.0.0.1";
    CHECK_GOTO(trevrpc_channel_connect(host, port, &config, options, 5000000000ull, NULL, &channel) == 0);
    memset(host, 'x', sizeof(host) - 1);
    uint32_t initial_state = 0;
    CHECK_GOTO(trevrpc_channel_get_state(channel, &initial_state, &first_generation) == 0);
    CHECK_GOTO(initial_state == TREVRPC_CHANNEL_READY);
    CHECK_GOTO(first_generation == 1);
    CHECK_GOTO(test_call(channel, "Unary") == 0);
    CHECK_GOTO(atomic_load(&calls) == 1);

    CHECK_GOTO(test_server_stop(&server) == 0);
    CHECK_GOTO(test_wait_state(channel, TREVRPC_CHANNEL_RECONNECTING, 2000) == 0);
    uint64_t fail_started = test_monotonic_nanos();
    CHECK_GOTO(test_call(channel, "Unary") == TREV_MSQUIC_ERR_CLOSED);
    CHECK_GOTO(test_monotonic_nanos() - fail_started < 100000000ull);
    CHECK_GOTO(atomic_load(&calls) == 1);

    cancellation = trevrpc_cancellation_new();
    CHECK_GOTO(cancellation != NULL);
    trevrpc_cancellation_cancel(cancellation);
    CHECK_GOTO(trevrpc_channel_wait_ready(channel, 5000000000ull, cancellation, NULL) == -ECANCELED);
    trevrpc_cancellation_free(cancellation);
    cancellation = NULL;

    CHECK_GOTO(test_server_start(&server, port, &calls, &blocker) == 0);
    uint64_t recovered_generation = 0;
    CHECK_GOTO(trevrpc_channel_wait_ready(channel, 5000000000ull, NULL, &recovered_generation) == 0);
    CHECK_GOTO(recovered_generation > first_generation);
    CHECK_GOTO(test_wait_lifecycle(&lifecycle, 1, 1, 2, 5000) == 0);
    CHECK_GOTO(atomic_load(&calls) == 1);
    CHECK_GOTO(test_call(channel, "Unary") == 0);
    CHECK_GOTO(atomic_load(&calls) == 2);

    CHECK_GOTO(trevrpc_channel_start_stream(
                   channel, "test.Service", "Stream", TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, NULL, 0, &stream) == 0);

    call_args args = {.channel = channel};
    CHECK_GOTO(pthread_create(&call_thread, NULL, test_call_thread, &args) == 0);
    call_thread_started = true;
    pthread_mutex_lock(&blocker.mutex);
    while (!blocker.entered) {
        pthread_cond_wait(&blocker.cond, &blocker.mutex);
    }
    pthread_mutex_unlock(&blocker.mutex);

    uint64_t close_started = test_monotonic_nanos();
    trevrpc_channel_close(channel);
    CHECK_GOTO(test_monotonic_nanos() - close_started < 2000000000ull);
    trevrpc_stream_close(stream);
    stream = NULL;

    pthread_mutex_lock(&blocker.mutex);
    blocker.released = true;
    pthread_cond_broadcast(&blocker.cond);
    pthread_mutex_unlock(&blocker.mutex);
    CHECK_GOTO(pthread_join(call_thread, NULL) == 0);
    call_thread_started = false;
    CHECK_GOTO(args.result != 0);
    trevrpc_channel_release(channel);
    channel = NULL;

    result = 0;

cleanup:
    if (call_thread_started) {
        pthread_mutex_lock(&blocker.mutex);
        blocker.released = true;
        pthread_cond_broadcast(&blocker.cond);
        pthread_mutex_unlock(&blocker.mutex);
        (void)pthread_join(call_thread, NULL);
    }
    trevrpc_stream_close(stream);
    trevrpc_channel_release(channel);
    trevrpc_cancellation_free(cancellation);
    trevrpc_channel_options_free(options);
    (void)test_server_stop(&server);
    pthread_cond_destroy(&blocker.cond);
    pthread_mutex_destroy(&blocker.mutex);
    return result;
}

static int test_initial_connect_failure_releases_channel(void) {
    int result = 1;
    atomic_int calls;
    atomic_init(&calls, 0);
    test_blocker blocker = {0};
    pthread_mutex_init(&blocker.mutex, NULL);
    pthread_cond_init(&blocker.cond, NULL);
    test_server server = {0};
    trevrpc_channel_options* options = NULL;
    trevrpc_channel* channel = NULL;
    trevrpc_cancellation* cancellation = NULL;

    CHECK_GOTO(test_server_start(&server, 0, &calls, &blocker) == 0);
    uint16_t port = server.port;
    cancellation = trevrpc_cancellation_new();
    CHECK_GOTO(cancellation != NULL);
    trevrpc_cancellation_cancel(cancellation);
    uint64_t cancelled_started = test_monotonic_nanos();
    trevrpc_config config = trevrpc_default_config();
    config.skip_certificate_validation = 1;
    CHECK_GOTO(
        trevrpc_channel_connect("127.0.0.1", port, &config, NULL, 5000000000ull, cancellation, &channel) == -ECANCELED);
    CHECK_GOTO(channel == NULL);
    CHECK_GOTO(test_monotonic_nanos() - cancelled_started < 1000000000ull);
    trevrpc_cancellation_free(cancellation);
    cancellation = NULL;

    (void)test_server_stop(&server);
    options = trevrpc_channel_options_new();
    CHECK_GOTO(options != NULL);
    CHECK_GOTO(trevrpc_channel_options_set_backoff(options, 5000, 5000, 0) == 0);
    cancellation = trevrpc_cancellation_new();
    CHECK_GOTO(cancellation != NULL);
    trevrpc_cancellation_cancel(cancellation);
    cancelled_started = test_monotonic_nanos();
    CHECK_GOTO(trevrpc_channel_connect("127.0.0.1", port, &config, options, 5000000000ull, cancellation, &channel) ==
               -ECANCELED);
    CHECK_GOTO(channel == NULL);
    CHECK_GOTO(test_monotonic_nanos() - cancelled_started < 1000000000ull);
    trevrpc_cancellation_free(cancellation);
    cancellation = NULL;

    uint64_t connect_started = test_monotonic_nanos();
    CHECK_GOTO(trevrpc_channel_connect("127.0.0.1", port, &config, options, 50000000ull, NULL, &channel) ==
               TREV_MSQUIC_ERR_TIMEOUT);
    CHECK_GOTO(channel == NULL);
    CHECK_GOTO(test_monotonic_nanos() - connect_started < 1000000000ull);
    result = 0;

cleanup:
    trevrpc_channel_release(channel);
    trevrpc_cancellation_free(cancellation);
    trevrpc_channel_options_free(options);
    (void)test_server_stop(&server);
    pthread_cond_destroy(&blocker.cond);
    pthread_mutex_destroy(&blocker.mutex);
    return result;
}

static int test_reconnect_without_lifecycle_callback(void) {
    int result = 1;
    atomic_int calls;
    atomic_init(&calls, 0);
    test_blocker blocker = {0};
    pthread_mutex_init(&blocker.mutex, NULL);
    pthread_cond_init(&blocker.cond, NULL);
    test_server server = {0};
    trevrpc_channel_options* options = NULL;
    trevrpc_channel* channel = NULL;

    CHECK_GOTO(test_server_start(&server, 0, &calls, &blocker) == 0);
    uint16_t port = server.port;
    options = trevrpc_channel_options_new();
    CHECK_GOTO(options != NULL);
    CHECK_GOTO(trevrpc_channel_options_set_backoff(options, 20, 100, 0) == 0);
    trevrpc_config config = trevrpc_default_config();
    config.skip_certificate_validation = 1;
    CHECK_GOTO(trevrpc_channel_connect("127.0.0.1", port, &config, options, 5000000000ull, NULL, &channel) == 0);

    uint64_t first_generation = 0;
    uint32_t first_state = 0;
    CHECK_GOTO(trevrpc_channel_get_state(channel, &first_state, &first_generation) == 0);
    CHECK_GOTO(first_state == TREVRPC_CHANNEL_READY);
    CHECK_GOTO(test_server_stop(&server) == 0);
    CHECK_GOTO(test_wait_state(channel, TREVRPC_CHANNEL_RECONNECTING, 2000) == 0);
    CHECK_GOTO(test_server_start(&server, port, &calls, &blocker) == 0);

    uint64_t recovered_generation = 0;
    CHECK_GOTO(trevrpc_channel_wait_ready(channel, 5000000000ull, NULL, &recovered_generation) == 0);
    CHECK_GOTO(recovered_generation > first_generation);
    result = 0;

cleanup:
    trevrpc_channel_close(channel);
    trevrpc_channel_release(channel);
    trevrpc_channel_options_free(options);
    (void)test_server_stop(&server);
    pthread_cond_destroy(&blocker.cond);
    pthread_mutex_destroy(&blocker.mutex);
    return result;
}

static int test_close_races_public_entry(void) {
    int result = 1;
    atomic_int calls;
    atomic_init(&calls, 0);
    test_blocker blocker = {0};
    pthread_mutex_init(&blocker.mutex, NULL);
    pthread_cond_init(&blocker.cond, NULL);
    test_server server = {0};
    trevrpc_channel* channel = NULL;
    pthread_t waiter = {0};
    bool waiter_started = false;
    enum { RACE_THREAD_COUNT = 4 };
    pthread_t race_threads[RACE_THREAD_COUNT] = {0};
    size_t race_threads_started = 0;
    atomic_bool stop;
    atomic_init(&stop, false);
    atomic_int failures;
    atomic_init(&failures, 0);

    CHECK_GOTO(test_server_start(&server, 0, &calls, &blocker) == 0);
    uint16_t port = server.port;
    trevrpc_config config = trevrpc_default_config();
    config.skip_certificate_validation = 1;
    CHECK_GOTO(trevrpc_channel_connect("127.0.0.1", port, &config, NULL, 5000000000ull, NULL, &channel) == 0);
    CHECK_GOTO(test_server_stop(&server) == 0);
    CHECK_GOTO(test_wait_state(channel, TREVRPC_CHANNEL_RECONNECTING, 2000) == 0);

    channel_wait_args wait_args = {.channel = channel};
    CHECK_GOTO(pthread_create(&waiter, NULL, test_wait_thread, &wait_args) == 0);
    waiter_started = true;
    channel_race_args race_args = {
        .channel = channel,
        .stop = &stop,
        .failures = &failures,
    };
    for (; race_threads_started < RACE_THREAD_COUNT; race_threads_started++) {
        CHECK_GOTO(pthread_create(&race_threads[race_threads_started], NULL, test_api_race_thread, &race_args) == 0);
    }
    test_sleep_ms(25);
    trevrpc_channel_close(channel);
    CHECK_GOTO(pthread_join(waiter, NULL) == 0);
    waiter_started = false;
    CHECK_GOTO(wait_args.result == TREV_MSQUIC_ERR_CLOSED);
    test_sleep_ms(25);
    atomic_store(&stop, true);
    for (size_t i = 0; i < race_threads_started; i++) {
        CHECK_GOTO(pthread_join(race_threads[i], NULL) == 0);
    }
    race_threads_started = 0;
    CHECK_GOTO(atomic_load(&failures) == 0);
    trevrpc_channel_release(channel);
    channel = NULL;
    result = 0;

cleanup:
    trevrpc_channel_close(channel);
    atomic_store(&stop, true);
    if (waiter_started) {
        (void)pthread_join(waiter, NULL);
    }
    for (size_t i = 0; i < race_threads_started; i++) {
        (void)pthread_join(race_threads[i], NULL);
    }
    trevrpc_channel_release(channel);
    (void)test_server_stop(&server);
    pthread_cond_destroy(&blocker.cond);
    pthread_mutex_destroy(&blocker.mutex);
    return result;
}

static int test_callback_close_and_reentry(void) {
    int result = 1;
    atomic_int calls;
    atomic_init(&calls, 0);
    test_blocker blocker = {0};
    pthread_mutex_init(&blocker.mutex, NULL);
    pthread_cond_init(&blocker.cond, NULL);
    test_server server = {0};
    trevrpc_channel_options* options = NULL;
    trevrpc_channel* channel = NULL;
    reentrant_lifecycle lifecycle = {0};

    CHECK_GOTO(test_server_start(&server, 0, &calls, &blocker) == 0);
    options = trevrpc_channel_options_new();
    CHECK_GOTO(options != NULL);
    CHECK_GOTO(trevrpc_channel_options_set_lifecycle_callback(options, test_reentrant_lifecycle, &lifecycle) == 0);
    trevrpc_config config = trevrpc_default_config();
    config.skip_certificate_validation = 1;
    CHECK_GOTO(trevrpc_channel_connect("127.0.0.1", server.port, &config, options, 5000000000ull, NULL, &channel) == 0);
    atomic_store(&lifecycle.channel, channel);
    uint64_t deadline = test_monotonic_nanos() + 5000000000ull;
    while (atomic_load(&lifecycle.close_callbacks) == 0 && test_monotonic_nanos() < deadline) {
        test_sleep_ms(5);
    }
    CHECK_GOTO(atomic_load(&lifecycle.close_callbacks) == 1);
    CHECK_GOTO(atomic_load(&lifecycle.failures) == 0);
    CHECK_GOTO(test_wait_state(channel, TREVRPC_CHANNEL_CLOSED, 2000) == 0);
    trevrpc_channel_release(channel);
    channel = NULL;
    result = 0;

cleanup:
    trevrpc_channel_close(channel);
    trevrpc_channel_release(channel);
    trevrpc_channel_options_free(options);
    (void)test_server_stop(&server);
    pthread_cond_destroy(&blocker.cond);
    pthread_mutex_destroy(&blocker.mutex);
    return result;
}

int main(void) {
    if (test_channel_reconnect_and_ownership() != 0) {
        return 1;
    }
    if (test_initial_connect_failure_releases_channel() != 0) {
        return 1;
    }
    if (test_reconnect_without_lifecycle_callback() != 0) {
        return 1;
    }
    if (test_close_races_public_entry() != 0) {
        return 1;
    }
    return test_callback_close_and_reentry();
}
