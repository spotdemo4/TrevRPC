#define _POSIX_C_SOURCE 200809L

#include "trevrpc_rpc_internal.h"
#include "trevrpc_rpc_transport_internal.h"
#include "trevrpc_rpc_transport_msquic_internal.h"
#include "trevrpc_wire_internal.h"

#include <assert.h>
#include <stdbool.h>
#include <errno.h> // IWYU pragma: keep
#include <stdint.h>
#include <stdio.h>
#include <poll.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "rpc_fake_transport_support.h"

static bool handle_equal(trevrpc_rpc_transport_handle left, trevrpc_rpc_transport_handle right) {
    return left.owner == right.owner && left.slot == right.slot && left.generation == right.generation;
}

static trevrpc_rpc_transport_config test_config(void) {
    trevrpc_rpc_transport_config config = {0};
    config.event_capacity = 8;
    config.listener_capacity = 1;
    config.connection_capacity = 1;
    config.stream_capacity = 1;
    return config;
}

static trevrpc_rpc_transport_handle test_handle(uint32_t slot) {
    trevrpc_rpc_transport_handle handle = {UINT64_C(0x9000), slot, 1};
    return handle;
}

static trevrpc_rpc_transport_handle next_subject(trevrpc_rpc_transport* transport, uint32_t expected_kind) {
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    assert(trevrpc_rpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(transport, event, &info) == 0);
    assert(info.kind == expected_kind);
    trevrpc_rpc_transport_event_release(transport, event);
    return info.subject;
}

static trevrpc_rpc_event* wait_rpc_event(trevrpc_rpc_runtime* runtime, const trevrpc_rpc_wake_source_v1* wake) {
    int attempt;
    for (attempt = 0; attempt < 1000; ++attempt) {
        trevrpc_rpc_event* event = NULL;
        int result = trevrpc_rpc_runtime_next_event(runtime, &event);
        if (result == 0)
            return event;
        assert(result == -EAGAIN);
        {
            struct pollfd descriptor = {.fd = (int)wake->native_handle, .events = POLLIN, .revents = 0};
            assert(poll(&descriptor, 1, 10) >= 0);
        }
    }
    assert(false);
    return NULL;
}

static trevrpc_rpc_event_info_v1 rpc_event_info(trevrpc_rpc_event* event) {
    trevrpc_rpc_event_info_v1 info;
    assert(trevrpc_rpc_event_info_v1_init(&info, sizeof(info)) == 0);
    assert(trevrpc_rpc_event_get_info_v1(event, &info) == 0);
    return info;
}

static void push_rpc_stream_frame(
    fake_transport* fake, uint32_t kind, uint32_t status, const uint8_t* body, size_t body_len) {
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    assert(trevrpc_wire_encode_stream_frame(
               kind, status, NULL, 0, body, body_len, NULL, 1024u * 1024u, &frame, &frame_len) == 0);
    assert(frame_len >= 4);
    assert(fake_push_receive(fake, frame + 4, frame_len - 4) == 0);
    free(frame);
}

typedef struct endpoint_dial_args {
    trevrpc_rpc_transport* transport;
    trevrpc_rpc_transport_endpoint_config config;
    trevrpc_rpc_transport_handle connection;
    int result;
} endpoint_dial_args;

typedef struct next_event_args {
    trevrpc_rpc_transport* transport;
    trevrpc_rpc_transport_event* event;
    int result;
} next_event_args;

typedef struct stream_receive_args {
    trevrpc_rpc_transport* transport;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_receive* receive;
    int result;
} stream_receive_args;

typedef struct release_handle_args {
    trevrpc_rpc_transport* transport;
    trevrpc_rpc_transport_handle handle;
    uint32_t kind;
    int result;
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    bool entered;
} release_handle_args;

static void* endpoint_dial_thread(void* context) {
    endpoint_dial_args* args = context;
    args->result = trevrpc_rpc_transport_endpoint_dial(args->transport, &args->config, 1, &args->connection);
    return NULL;
}

static void* next_event_thread(void* context) {
    next_event_args* args = context;
    args->result = trevrpc_rpc_transport_next_event(args->transport, &args->event);
    return NULL;
}

static void* stream_receive_thread(void* context) {
    stream_receive_args* args = context;
    args->result = trevrpc_rpc_transport_stream_receive(args->transport, args->stream, &args->receive);
    return NULL;
}

static void* release_handle_thread(void* context) {
    release_handle_args* args = context;
    pthread_mutex_lock(&args->mutex);
    args->entered = true;
    pthread_cond_broadcast(&args->condition);
    pthread_mutex_unlock(&args->mutex);
    args->result = trevrpc_rpc_transport_release_handle(args->transport, args->handle, args->kind);
    return NULL;
}

static void add_milliseconds(struct timespec* time, long milliseconds) {
    time->tv_nsec += milliseconds * 1000000L;
    time->tv_sec += time->tv_nsec / 1000000000L;
    time->tv_nsec %= 1000000000L;
}

static bool wait_for_next_event_attempt(fake_transport* source) {
    struct timespec deadline;
    int result;
    bool attempted;

    pthread_mutex_lock(&source->next_event_probe_mutex);
    assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
    add_milliseconds(&deadline, 1000);
    while (!source->next_event_attempted) {
        result =
            pthread_cond_timedwait(&source->next_event_probe_condition, &source->next_event_probe_mutex, &deadline);
        if (result == ETIMEDOUT)
            break;
        assert(result == 0);
    }
    attempted = source->next_event_attempted;
    pthread_mutex_unlock(&source->next_event_probe_mutex);
    return attempted;
}

static void test_registration_precedes_ready_dequeue(void) {
    fake_transport* source = fake_create();
    fake_transport* other = fake_create();
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_config config = test_config();
    endpoint_dial_args dial = {0};
    next_event_args next = {0};
    pthread_t dial_thread;
    pthread_t next_thread;
    trevrpc_rpc_transport_event_info info;
    bool next_event_entered;

    assert(source != NULL && other != NULL);
    assert(trevrpc_rpc_transport_msquic_adopt(&source->base, &other->base, &config, &composite) == 0);
    pthread_mutex_lock(&source->mutex);
    source->dial_blocked = true;
    pthread_mutex_unlock(&source->mutex);

    dial.transport = composite;
    assert(pthread_create(&dial_thread, NULL, endpoint_dial_thread, &dial) == 0);
    pthread_mutex_lock(&source->mutex);
    while (!source->dial_entered)
        pthread_cond_wait(&source->condition, &source->mutex);
    pthread_mutex_unlock(&source->mutex);

    assert(fake_push_event(source,
               TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
               0,
               fake_connection_handle,
               (trevrpc_rpc_transport_handle){0}) == 0);
    pthread_mutex_lock(&source->mutex);
    source->next_event_blocked = true;
    pthread_mutex_unlock(&source->mutex);

    next.transport = composite;
    assert(pthread_create(&next_thread, NULL, next_event_thread, &next) == 0);

    /* The probe is published at the child dequeue boundary, before the fake
     * source mutex is acquired.  A one-second wait is only a bounded
     * deadlock guard: the fixed implementation must remain blocked on the
     * composite mutex while admission is paused, whereas the old
     * implementation reaches this probe and is rejected below. */
    next_event_entered = wait_for_next_event_attempt(source);
    assert(!next_event_entered);

    pthread_mutex_lock(&source->mutex);
    assert(source->event_count == 1);
    source->dial_release = true;
    pthread_cond_broadcast(&source->condition);
    pthread_mutex_unlock(&source->mutex);

    assert(pthread_join(dial_thread, NULL) == 0);
    pthread_mutex_lock(&source->mutex);
    source->next_event_release = true;
    pthread_cond_broadcast(&source->condition);
    pthread_mutex_unlock(&source->mutex);
    assert(pthread_join(next_thread, NULL) == 0);
    assert(dial.result == 0);
    assert(next.result == 0);
    assert(trevrpc_rpc_transport_event_get_info(composite, next.event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY);
    assert(info.subject.owner == dial.connection.owner);
    assert(info.subject.slot == dial.connection.slot);
    assert(info.subject.generation == dial.connection.generation);
    trevrpc_rpc_transport_event_release(composite, next.event);
    trevrpc_rpc_transport_release_handle(composite, dial.connection, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION);
    trevrpc_rpc_transport_destroy(composite);
}

static void test_release_waits_for_receive_lifetime(void) {
    fake_transport* source = fake_create();
    fake_transport* other = fake_create();
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_receive* receive = NULL;
    stream_receive_args receive_call = {0};
    release_handle_args release_call = {0};
    pthread_t receive_thread;
    pthread_t release_thread;
    const uint8_t payload[] = {'o', 'k'};
    struct timespec pause = {0, 20 * 1000 * 1000L};

    assert(source != NULL && other != NULL);
    assert(trevrpc_rpc_transport_msquic_adopt(&source->base, &other->base, &config, &composite) == 0);
    assert(fake_push_event(
               source, TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY, 0, fake_stream_handle, fake_connection_handle) == 0);
    stream = next_subject(composite, TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY);
    assert(fake_push_receive(source, payload, sizeof(payload)) == 0);
    pthread_mutex_lock(&source->mutex);
    source->receive_blocked = true;
    pthread_mutex_unlock(&source->mutex);

    receive_call.transport = composite;
    receive_call.stream = stream;
    assert(pthread_create(&receive_thread, NULL, stream_receive_thread, &receive_call) == 0);
    pthread_mutex_lock(&source->mutex);
    while (!source->receive_entered)
        pthread_cond_wait(&source->condition, &source->mutex);
    pthread_mutex_unlock(&source->mutex);

    release_call.transport = composite;
    release_call.handle = stream;
    release_call.kind = TREVRPC_RPC_TRANSPORT_OBJECT_STREAM;
    assert(pthread_mutex_init(&release_call.mutex, NULL) == 0);
    assert(pthread_cond_init(&release_call.condition, NULL) == 0);
    assert(pthread_create(&release_thread, NULL, release_handle_thread, &release_call) == 0);
    pthread_mutex_lock(&release_call.mutex);
    while (!release_call.entered)
        pthread_cond_wait(&release_call.condition, &release_call.mutex);
    pthread_mutex_unlock(&release_call.mutex);

    /* The child release must not run while the source receive is admitted. */
    nanosleep(&pause, NULL);
    assert(fake_release_handle_count(source) == 0);
    pthread_mutex_lock(&source->mutex);
    source->receive_release = true;
    pthread_cond_broadcast(&source->condition);
    pthread_mutex_unlock(&source->mutex);
    assert(pthread_join(receive_thread, NULL) == 0);
    assert(receive_call.result == 0);
    receive = receive_call.receive;
    assert(receive != NULL);
    nanosleep(&pause, NULL);
    assert(fake_release_handle_count(source) == 0);

    trevrpc_rpc_transport_receive_release(composite, receive);
    assert(pthread_join(release_thread, NULL) == 0);
    assert(release_call.result == 0);
    assert(fake_release_handle_count(source) == 1);
    pthread_cond_destroy(&release_call.condition);
    pthread_mutex_destroy(&release_call.mutex);
    trevrpc_rpc_transport_destroy(composite);
}

static void test_translation_retry_preserves_event(void) {
    fake_transport* source = fake_create();
    fake_transport* other = fake_create();
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport_handle retired = test_handle(1);
    trevrpc_rpc_transport_handle public_retired;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_wake wakes[TREVRPC_RPC_TRANSPORT_MAX_WAKE_SOURCES];
    struct pollfd descriptors[TREVRPC_RPC_TRANSPORT_MAX_WAKE_SOURCES];
    size_t wake_count;
    size_t wake_index;
    int result;
    uint32_t slot;

    assert(source != NULL && other != NULL);
    assert(trevrpc_rpc_transport_msquic_adopt(&source->base, &other->base, &config, &composite) == 0);
    assert(fake_push_event(source,
               TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL,
               retired,
               (trevrpc_rpc_transport_handle){0}) == 0);
    for (slot = 2; slot <= 7; ++slot) {
        assert(fake_push_event(source,
                   TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
                   0,
                   test_handle(slot),
                   (trevrpc_rpc_transport_handle){0}) == 0);
    }

    public_retired = next_subject(composite, TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED);
    for (slot = 2; slot <= 6; ++slot)
        assert(next_subject(composite, TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY).slot == slot);

    assert(trevrpc_rpc_transport_next_event(composite, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_transport_get_wake_sources(
               composite, wakes, TREVRPC_RPC_TRANSPORT_MAX_WAKE_SOURCES, &wake_count) == 0);
    assert(wake_count >= 3);
    for (wake_index = 0; wake_index < wake_count; ++wake_index) {
        descriptors[wake_index].fd = (int)wakes[wake_index].native_handle;
        descriptors[wake_index].events = POLLIN;
        descriptors[wake_index].revents = 0;
    }
    assert(poll(descriptors, (nfds_t)wake_count, 20) == 0);

    result = trevrpc_rpc_transport_release_handle(composite, public_retired, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION);
    assert(result == 0);
    assert(fake_release_handle_count(source) == 1);
    assert(poll(descriptors, (nfds_t)wake_count, 1000) > 0);
    for (wake_index = 0; wake_index + 1 < wake_count; ++wake_index)
        assert((descriptors[wake_index].revents & POLLIN) == 0);
    assert((descriptors[wake_count - 1].revents & POLLIN) != 0);

    assert(trevrpc_rpc_transport_next_event(composite, &event) == 0);
    assert(poll(descriptors, (nfds_t)wake_count, 0) == 0);
    assert(trevrpc_rpc_transport_event_get_info(composite, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY);
    assert(info.subject.owner != 0);
    assert(!handle_equal(info.subject, public_retired));
    trevrpc_rpc_transport_event_release(composite, event);
    trevrpc_rpc_transport_destroy(composite);
}

static void test_retained_event_signals_retry_wake(void) {
    fake_transport* source = fake_create();
    fake_transport* other = fake_create();
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_wake wakes[TREVRPC_RPC_TRANSPORT_MAX_WAKE_SOURCES];
    struct pollfd descriptors[TREVRPC_RPC_TRANSPORT_MAX_WAKE_SOURCES];
    size_t wake_count;
    size_t wake_index;

    assert(source != NULL && other != NULL);
    assert(trevrpc_rpc_transport_msquic_adopt(&source->base, &other->base, &config, &composite) == 0);
    assert(fake_push_event(source,
               TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
               0,
               fake_connection_handle,
               (trevrpc_rpc_transport_handle){0}) == 0);
    atomic_store_explicit(&source->event_get_info_result, -EAGAIN, memory_order_release);

    assert(trevrpc_rpc_transport_next_event(composite, &event) == -EAGAIN);
    assert(event == NULL);
    assert(trevrpc_rpc_transport_get_wake_sources(
               composite, wakes, TREVRPC_RPC_TRANSPORT_MAX_WAKE_SOURCES, &wake_count) == 0);
    assert(wake_count >= 3);
    for (wake_index = 0; wake_index < wake_count; ++wake_index) {
        descriptors[wake_index].fd = (int)wakes[wake_index].native_handle;
        descriptors[wake_index].events = POLLIN;
        descriptors[wake_index].revents = 0;
    }
    assert(poll(descriptors, (nfds_t)wake_count, 1000) > 0);
    for (wake_index = 0; wake_index + 1 < wake_count; ++wake_index)
        assert((descriptors[wake_index].revents & POLLIN) == 0);
    assert((descriptors[wake_count - 1].revents & POLLIN) != 0);

    assert(trevrpc_rpc_transport_next_event(composite, &event) == 0);
    assert(event != NULL);
    assert(poll(descriptors, (nfds_t)wake_count, 0) == 0);
    assert(trevrpc_rpc_transport_event_get_info(composite, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY);
    trevrpc_rpc_transport_event_release(composite, event);
    event = NULL;
    assert(trevrpc_rpc_transport_next_event(composite, &event) == -EAGAIN);
    assert(event == NULL);
    trevrpc_rpc_transport_destroy(composite);
}

static void test_retired_parent_is_not_resurrected(void) {
    fake_transport* source = fake_create();
    fake_transport* other = fake_create();
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport_handle parent_local = test_handle(20);
    trevrpc_rpc_transport_handle child_local = test_handle(21);
    trevrpc_rpc_transport_handle parent_external;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    int result;

    assert(source != NULL && other != NULL);
    assert(trevrpc_rpc_transport_msquic_adopt(&source->base, &other->base, &config, &composite) == 0);
    assert(fake_push_event(source,
               TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL,
               parent_local,
               (trevrpc_rpc_transport_handle){0}) == 0);
    parent_external = next_subject(composite, TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED);
    result = trevrpc_rpc_transport_release_handle(composite, parent_external, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION);
    assert(result == 0);

    assert(fake_push_event(source,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL,
               child_local,
               parent_local) == 0);
    assert(trevrpc_rpc_transport_next_event(composite, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(composite, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED);
    assert(info.parent.owner == 0 && info.parent.slot == 0 && info.parent.generation == 0);
    assert(info.subject.owner != 0);
    trevrpc_rpc_transport_event_release(composite, event);
    trevrpc_rpc_transport_destroy(composite);
}

static void test_parent_registration_rolls_back_on_subject_capacity(void) {
    fake_transport* source = fake_create();
    fake_transport* other = fake_create();
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport_handle existing[5];
    trevrpc_rpc_transport_handle parent_local = test_handle(30);
    trevrpc_rpc_transport_handle child_local = test_handle(31);
    trevrpc_rpc_transport_handle released_external;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    int result;
    uint32_t slot;

    assert(source != NULL && other != NULL);
    assert(trevrpc_rpc_transport_msquic_adopt(&source->base, &other->base, &config, &composite) == 0);
    for (slot = 1; slot <= 5; ++slot) {
        existing[slot - 1] = test_handle(40 + slot);
        assert(fake_push_event(source,
                   TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED,
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL,
                   existing[slot - 1],
                   (trevrpc_rpc_transport_handle){0}) == 0);
        existing[slot - 1] = next_subject(composite, TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED);
    }
    assert(fake_push_event(source, TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY, 0, child_local, parent_local) == 0);
    assert(trevrpc_rpc_transport_next_event(composite, &event) == -EAGAIN);
    assert(event == NULL);

    released_external = existing[0];
    result =
        trevrpc_rpc_transport_release_handle(composite, released_external, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION);
    assert(result == 0);
    assert(trevrpc_rpc_transport_next_event(composite, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(composite, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY);
    assert(info.parent.owner == released_external.owner);
    assert(info.parent.slot == released_external.slot);
    assert(info.parent.generation != released_external.generation);
    trevrpc_rpc_transport_event_release(composite, event);
    trevrpc_rpc_transport_destroy(composite);
}

static void test_listener_close_linearizes_before_terminal_dequeue(void) {
    fake_transport* source = fake_create();
    fake_transport* other = fake_create();
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_transport_handle listener;
    trevrpc_rpc_transport_event* event = NULL;
    trevrpc_rpc_transport_event_info info;
    uint16_t port = 0;

    assert(source != NULL && other != NULL);
    assert(trevrpc_rpc_transport_msquic_adopt(&source->base, &other->base, &config, &composite) == 0);
    assert(trevrpc_rpc_transport_endpoint_listen(composite, &endpoint_config, &listener) == 0);
    assert(trevrpc_rpc_transport_listener_close(composite, listener) == 0);

    /* The close call is the admission linearization point.  Do not permit a
     * new operation while the child terminal event is still queued. */
    assert(trevrpc_rpc_transport_endpoint_get_port(composite, listener, &port) == -ESTALE);
    assert(trevrpc_rpc_transport_next_event(composite, &event) == 0);
    assert(trevrpc_rpc_transport_event_get_info(composite, event, &info) == 0);
    assert(info.kind == TREVRPC_RPC_TRANSPORT_EVENT_LISTENER_STOPPED);
    assert(handle_equal(info.subject, listener));
    trevrpc_rpc_transport_event_release(composite, event);
    assert(trevrpc_rpc_transport_release_handle(composite, listener, TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER) == 0);
    trevrpc_rpc_transport_destroy(composite);
}

static void test_endpoint_admission_rejects_before_child_creation_when_mapping_full(void) {
    fake_transport* source = fake_create();
    fake_transport* other = fake_create();
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_transport_handle handle;
    uint32_t slot;

    assert(source != NULL && other != NULL);
    assert(trevrpc_rpc_transport_msquic_adopt(&source->base, &other->base, &config, &composite) == 0);
    for (slot = 1; slot <= 6; ++slot) {
        assert(fake_push_event(source,
                   TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED,
                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL,
                   test_handle(60 + slot),
                   (trevrpc_rpc_transport_handle){0}) == 0);
        (void)next_subject(composite, TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED);
    }

    assert(trevrpc_rpc_transport_endpoint_dial(composite, &endpoint_config, 1, &handle) == -EAGAIN);
    assert(!source->connection_closed);
    assert(trevrpc_rpc_transport_endpoint_listen(composite, &endpoint_config, &handle) == -EAGAIN);
    assert(!source->listener_closed);

    trevrpc_rpc_transport_destroy(composite);
}

static void test_rpc_terminal_releases_restored_composite_receive(void) {
    fake_transport* source = fake_create();
    fake_transport* other = fake_create();
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_config transport_config = test_config();
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
    trevrpc_rpc_call_config_v1 call_config;
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
    uint64_t transport_operation_id;
    const uint8_t response[] = "retained";
    int attempt;

    assert(source != NULL && other != NULL);
    assert(trevrpc_rpc_transport_msquic_adopt(&source->base, &other->base, &transport_config, &composite) == 0);
    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&runtime_config, composite, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(trevrpc_rpc_runtime_start_transport_endpoint_v1(
               runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_CLIENT, 1, &endpoint) == 0);
    assert(fake_push_event(source,
               TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_connection_handle,
               (trevrpc_rpc_transport_handle){0}) == 0);
    event = wait_rpc_event(runtime, &wake);
    info = rpc_event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_ENDPOINT_READY);
    assert(info.operation_id == 1);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0);
    call_config.kind = TREVRPC_RPC_KIND_CLIENT_STREAMING;
    call_config.service = "fake.Service";
    call_config.service_len = (uint32_t)strlen(call_config.service);
    call_config.method = "Test";
    call_config.method_len = (uint32_t)strlen(call_config.method);
    call_config.initial_message = (const uint8_t*)"request";
    call_config.initial_message_len = strlen("request");
    assert(trevrpc_rpc_call_open_v1(runtime, endpoint, &call_config, 2, &call, &stream) == 0);
    assert(fake_push_event(source,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    for (attempt = 0; attempt < 1000; ++attempt) {
        if (atomic_load_explicit(&source->stream_send_calls, memory_order_acquire) != 0)
            break;
        assert(poll(NULL, 0, 1) >= 0);
    }
    assert(attempt != 1000);
    transport_operation_id = atomic_load_explicit(&source->last_send_operation_id, memory_order_acquire);
    assert(transport_operation_id != 0);
    assert(fake_push_operation_event(source,
               TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle,
               transport_operation_id) == 0);
    event = wait_rpc_event(runtime, &wake);
    info = rpc_event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_READY);
    assert(info.operation_id == 2);
    trevrpc_rpc_event_release(event);

    push_rpc_stream_frame(
        source, TREVRPC_STREAM_FRAME_KIND_MESSAGE, TREVRPC_RPC_STATUS_OK, response, sizeof(response) - 1);
    assert(fake_push_event(source,
               TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
               TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
               fake_stream_handle,
               fake_connection_handle) == 0);
    event = wait_rpc_event(runtime, &wake);
    assert(rpc_event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_event_release(event);

    trevrpc_rpc_internal_test_fail_next_receive_copy(runtime);
    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -ENOMEM);
    assert(receive == NULL);
    event = wait_rpc_event(runtime, &wake);
    assert(rpc_event_info(event).kind == TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_event_release(event);

    assert(trevrpc_rpc_call_close(runtime, call, 3, TREVRPC_RPC_CLOSE_FLAG_ABORT, 17) == 0);
    event = wait_rpc_event(runtime, &wake);
    info = rpc_event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED);
    trevrpc_rpc_event_release(event);
    event = wait_rpc_event(runtime, &wake);
    info = rpc_event_info(event);
    assert(info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED);
    assert(info.operation_id == 3);
    trevrpc_rpc_event_release(event);

    assert(fake_release_handle_count(source) == 0);
    assert(trevrpc_rpc_stream_release(runtime, stream) == 0);
    assert(fake_release_handle_count(source) == 1);
    assert(trevrpc_rpc_call_release(runtime, call) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, endpoint, 4) == 0);
    event = wait_rpc_event(runtime, &wake);
    assert(rpc_event_info(event).kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_endpoint_release(runtime, endpoint) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 5) == 0);
    event = wait_rpc_event(runtime, &wake);
    assert(rpc_event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void test_runtime_driver_observes_composite_timeout(void) {
    fake_transport* source = fake_create();
    fake_transport* other = fake_create();
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_config transport_config = test_config();
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_runtime* runtime = NULL;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_event* event;
    int attempt;

    assert(source != NULL && other != NULL);
    atomic_store_explicit(&other->poll_timeout_ms, 20, memory_order_release);
    assert(trevrpc_rpc_transport_msquic_adopt(&source->base, &other->base, &transport_config, &composite) == 0);
    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    assert(trevrpc_rpc_runtime_adopt_transport_v1(&runtime_config, composite, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    for (attempt = 0; attempt < 1000; ++attempt) {
        if (atomic_load_explicit(&other->poll_timeout_fires, memory_order_acquire) != 0)
            break;
        assert(poll(NULL, 0, 1) >= 0);
    }
    assert(attempt != 1000);
    assert(atomic_load_explicit(&source->poll_timeout_fires, memory_order_acquire) == 0);
    assert(atomic_load_explicit(&other->poll_timeout_fires, memory_order_acquire) == 1);

    assert(trevrpc_rpc_runtime_close(runtime, 1) == 0);
    event = wait_rpc_event(runtime, &wake);
    assert(rpc_event_info(event).kind == TREVRPC_RPC_EVENT_STOPPED);
    trevrpc_rpc_event_release(event);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
}

static void test_directional_stream_abort_routes_without_terminal(void) {
    fake_transport* source = fake_create();
    fake_transport* other = fake_create();
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_event* event = NULL;

    assert(source != NULL && other != NULL);
    assert(trevrpc_rpc_transport_msquic_adopt(&source->base, &other->base, &config, &composite) == 0);
    assert(fake_push_event(
               source, TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY, 0, fake_stream_handle, fake_connection_handle) == 0);
    stream = next_subject(composite, TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY);

    assert(trevrpc_rpc_transport_stream_abort_receive(composite, stream, 11) == 0);
    assert(atomic_load_explicit(&source->stream_abort_receive_calls, memory_order_acquire) == 1);
    assert(atomic_load_explicit(&source->stream_abort_send_calls, memory_order_acquire) == 0);
    assert(atomic_load_explicit(&source->stream_abort_calls, memory_order_acquire) == 0);
    assert(atomic_load_explicit(&source->last_abort_error, memory_order_acquire) == 11);
    assert(trevrpc_rpc_transport_next_event(composite, &event) == -EAGAIN);
    assert(event == NULL);

    assert(trevrpc_rpc_transport_stream_abort_send(composite, stream, 12) == 0);
    assert(atomic_load_explicit(&source->stream_abort_receive_calls, memory_order_acquire) == 1);
    assert(atomic_load_explicit(&source->stream_abort_send_calls, memory_order_acquire) == 1);
    assert(atomic_load_explicit(&source->stream_abort_calls, memory_order_acquire) == 0);
    assert(atomic_load_explicit(&source->last_abort_error, memory_order_acquire) == 12);
    assert(trevrpc_rpc_transport_next_event(composite, &event) == -EAGAIN);
    assert(event == NULL);

    assert(trevrpc_rpc_transport_release_handle(composite, stream, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM) == 0);
    trevrpc_rpc_transport_destroy(composite);
}

static void test_release_retries_after_downstream_failure(void) {
    fake_transport* source = fake_create();
    fake_transport* other = fake_create();
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_config config = test_config();
    trevrpc_rpc_transport_handle local = test_handle(1);
    trevrpc_rpc_transport_handle external;
    int result;

    assert(source != NULL && other != NULL);
    assert(trevrpc_rpc_transport_msquic_adopt(&source->base, &other->base, &config, &composite) == 0);
    assert(fake_push_event(
               source, TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY, 0, local, (trevrpc_rpc_transport_handle){0}) == 0);
    external = next_subject(composite, TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY);

    source->release_handle_result = -EIO;
    result = trevrpc_rpc_transport_release_handle(composite, external, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION);
    assert(result == -EIO);
    assert(fake_release_handle_count(source) == 1);
    source->release_handle_result = 0;
    result = trevrpc_rpc_transport_release_handle(composite, external, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION);
    assert(result == 0);
    assert(fake_release_handle_count(source) == 2);
    result = trevrpc_rpc_transport_release_handle(composite, external, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION);
    assert(result == 0);
    assert(fake_release_handle_count(source) == 2);
    trevrpc_rpc_transport_destroy(composite);
}

int main(void) {
    test_registration_precedes_ready_dequeue();
    test_release_waits_for_receive_lifetime();
    test_translation_retry_preserves_event();
    test_retained_event_signals_retry_wake();
    test_retired_parent_is_not_resurrected();
    test_parent_registration_rolls_back_on_subject_capacity();
    test_listener_close_linearizes_before_terminal_dequeue();
    test_endpoint_admission_rejects_before_child_creation_when_mapping_full();
    test_rpc_terminal_releases_restored_composite_receive();
    test_runtime_driver_observes_composite_timeout();
    test_directional_stream_abort_routes_without_terminal();
    test_release_retries_after_downstream_failure();
    puts("rpc_transport_msquic_lifecycle_test: ok");
    return 0;
}
