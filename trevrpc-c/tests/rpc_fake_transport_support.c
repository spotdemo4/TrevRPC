#define _POSIX_C_SOURCE 200809L

#include "rpc_fake_transport_support.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

const trevrpc_rpc_transport_handle fake_listener_handle = {UINT64_C(0x1111), 1u, 1u};
const trevrpc_rpc_transport_handle fake_connection_handle = {UINT64_C(0x2222), 2u, 1u};
const trevrpc_rpc_transport_handle fake_stream_handle = {UINT64_C(0x3333), 3u, 1u};
const trevrpc_rpc_transport_handle fake_second_stream_handle = {UINT64_C(0x3333), 4u, 1u};

struct fake_event {
    fake_transport* transport;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_admission_info admission;
    uint8_t* admission_path;
    uint8_t* admission_authority;
    uint8_t* admission_origin;
    bool admission_decided;
};

struct fake_receive {
    fake_receive* next;
    fake_transport* transport;
    uint8_t* data;
    size_t data_len;
};

fake_transport* fake_from_base(trevrpc_rpc_transport* transport) {
    return (fake_transport*)transport;
}

bool fake_handle_equal(trevrpc_rpc_transport_handle left, trevrpc_rpc_transport_handle right) {
    return left.owner == right.owner && left.slot == right.slot && left.generation == right.generation;
}

static bool fake_stream_handle_valid(trevrpc_rpc_transport_handle stream) {
    return fake_handle_equal(stream, fake_stream_handle) || fake_handle_equal(stream, fake_second_stream_handle);
}

void fake_drain_wake_locked(fake_transport* transport) {
    uint8_t bytes[64];
    /* Both pipe read ends are permanently nonblocking. */
    // NOLINTBEGIN(clang-analyzer-unix.BlockInCriticalSection)
    while (read(transport->wake_read_fd, bytes, sizeof(bytes)) > 0) {
    }
    while (read(transport->alternate_wake_read_fd, bytes, sizeof(bytes)) > 0) {
    }
    // NOLINTEND(clang-analyzer-unix.BlockInCriticalSection)
}

int fake_enqueue_event(fake_transport* transport, const trevrpc_rpc_transport_event_info* info) {
    fake_event* event = calloc(1, sizeof(*event));
    if (event == NULL) {
        return -ENOMEM;
    }
    event->transport = transport;
    event->info = *info;
    pthread_mutex_lock(&transport->mutex);
    if (transport->event_count == FAKE_EVENT_CAPACITY) {
        pthread_mutex_unlock(&transport->mutex);
        free(event);
        return -EAGAIN;
    }
    transport->events[transport->event_tail] = event;
    transport->event_tail = (transport->event_tail + 1u) % FAKE_EVENT_CAPACITY;
    ++transport->event_count;
    {
        uint8_t byte = 1;
        int wake_fd = transport->signal_alternate_wake ? transport->alternate_wake_write_fd : transport->wake_write_fd;
        ssize_t written = write(wake_fd, &byte, sizeof(byte));
        (void)written;
    }
    pthread_mutex_unlock(&transport->mutex);
    return 0;
}

int fake_push_event(fake_transport* transport,
    uint32_t kind,
    uint32_t flags,
    trevrpc_rpc_transport_handle subject,
    trevrpc_rpc_transport_handle parent) {
    trevrpc_rpc_transport_event_info info = {0};
    info.kind = kind;
    info.flags = flags;
    info.subject_kind =
        kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY || kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE ||
                kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN || kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED
            ? TREVRPC_RPC_TRANSPORT_OBJECT_STREAM
        : kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY ||
                kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_FAILED ||
                kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED
            ? TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION
            : TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER;
    info.subject = subject;
    info.parent = parent;
    return fake_enqueue_event(transport, &info);
}

int fake_push_status_event(fake_transport* transport,
    uint32_t kind,
    uint32_t flags,
    trevrpc_rpc_transport_handle subject,
    trevrpc_rpc_transport_handle parent,
    int32_t status) {
    trevrpc_rpc_transport_event_info info = {0};
    info.kind = kind;
    info.flags = flags;
    info.subject_kind = kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY ||
                                kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_FAILED ||
                                kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED
                            ? TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION
                            : TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER;
    info.subject = subject;
    info.parent = parent;
    info.status = status;
    return fake_enqueue_event(transport, &info);
}

int fake_push_operation_status_event(fake_transport* transport,
    uint32_t kind,
    uint32_t flags,
    trevrpc_rpc_transport_handle subject,
    trevrpc_rpc_transport_handle parent,
    uint64_t operation_id,
    int32_t status) {
    trevrpc_rpc_transport_event_info info = {0};
    info.kind = kind;
    info.flags = flags;
    info.subject_kind = TREVRPC_RPC_TRANSPORT_OBJECT_STREAM;
    info.subject = subject;
    info.parent = parent;
    info.operation_id = operation_id;
    info.status = status;
    return fake_enqueue_event(transport, &info);
}

int fake_push_operation_event(fake_transport* transport,
    uint32_t kind,
    uint32_t flags,
    trevrpc_rpc_transport_handle subject,
    trevrpc_rpc_transport_handle parent,
    uint64_t operation_id) {
    return fake_push_operation_status_event(transport, kind, flags, subject, parent, operation_id, 0);
}

static int fake_copy_bytes(const uint8_t* source, size_t source_len, uint8_t** out_copy) {
    *out_copy = NULL;
    if (source_len == 0) {
        return 0;
    }
    if (source == NULL) {
        return -EINVAL;
    }
    *out_copy = malloc(source_len);
    if (*out_copy == NULL) {
        return -ENOMEM;
    }
    memcpy(*out_copy, source, source_len);
    return 0;
}

int fake_push_admission_event(fake_transport* transport,
    uint32_t kind,
    const uint8_t* path,
    size_t path_len,
    const uint8_t* authority,
    size_t authority_len,
    const uint8_t* origin,
    size_t origin_len) {
    fake_event* event;
    int result;
    if (kind != TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION &&
        kind != TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION) {
        return -EINVAL;
    }
    event = calloc(1, sizeof(*event));
    if (event == NULL) {
        return -ENOMEM;
    }
    result = fake_copy_bytes(path, path_len, &event->admission_path);
    if (result == 0) {
        result = fake_copy_bytes(authority, authority_len, &event->admission_authority);
    }
    if (result == 0) {
        result = fake_copy_bytes(origin, origin_len, &event->admission_origin);
    }
    if (result != 0) {
        free(event->admission_path);
        free(event->admission_authority);
        free(event->admission_origin);
        free(event);
        return result;
    }
    event->transport = transport;
    event->info.kind = kind;
    event->info.flags = TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER;
    event->info.subject_kind = TREVRPC_RPC_TRANSPORT_OBJECT_STREAM;
    event->info.subject = fake_stream_handle;
    event->info.parent = fake_listener_handle;
    event->admission.protocol = kind == TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION
                                    ? TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3
                                    : TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT;
    event->admission.listener = fake_listener_handle;
    event->admission.path = event->admission_path;
    event->admission.path_len = path_len;
    event->admission.authority = event->admission_authority;
    event->admission.authority_len = authority_len;
    event->admission.origin = event->admission_origin;
    event->admission.origin_len = origin_len;
    pthread_mutex_lock(&transport->mutex);
    if (transport->event_count == FAKE_EVENT_CAPACITY) {
        pthread_mutex_unlock(&transport->mutex);
        free(event->admission_path);
        free(event->admission_authority);
        free(event->admission_origin);
        free(event);
        return -EAGAIN;
    }
    transport->events[transport->event_tail] = event;
    transport->event_tail = (transport->event_tail + 1u) % FAKE_EVENT_CAPACITY;
    ++transport->event_count;
    {
        uint8_t byte = 1;
        ssize_t written = write(transport->wake_write_fd, &byte, sizeof(byte));
        (void)written;
    }
    pthread_mutex_unlock(&transport->mutex);
    return 0;
}

static fake_receive* fake_receive_create(fake_transport* transport, const uint8_t* data, size_t data_len) {
    fake_receive* receive = calloc(1, sizeof(*receive));
    if (receive == NULL) {
        return NULL;
    }
    if (data_len != 0) {
        receive->data = malloc(data_len);
        if (receive->data == NULL) {
            free(receive);
            return NULL;
        }
        memcpy(receive->data, data, data_len);
    }
    receive->transport = transport;
    receive->data_len = data_len;
    return receive;
}

static void fake_receive_enqueue_locked(fake_transport* transport, fake_receive* receive) {
    if (transport->stream_receive_tail != NULL) {
        transport->stream_receive_tail->next = receive;
    } else {
        transport->stream_receive_head = receive;
    }
    transport->stream_receive_tail = receive;
}

int fake_push_receive(fake_transport* transport, const uint8_t* data, size_t data_len) {
    fake_receive* receive;
    if (transport == NULL || (data == NULL && data_len != 0)) {
        return -EINVAL;
    }
    receive = fake_receive_create(transport, data, data_len);
    if (receive == NULL) {
        return -ENOMEM;
    }
    pthread_mutex_lock(&transport->mutex);
    fake_receive_enqueue_locked(transport, receive);
    pthread_mutex_unlock(&transport->mutex);
    return 0;
}

int fake_push_incoming_stream_for_handle(
    fake_transport* transport, trevrpc_rpc_transport_handle stream, const uint8_t* data, size_t data_len) {
    fake_receive* receive;
    fake_event* ready;
    fake_event* readable;
    if (transport == NULL || !fake_stream_handle_valid(stream) || (data == NULL && data_len != 0)) {
        return -EINVAL;
    }
    receive = fake_receive_create(transport, data, data_len);
    ready = calloc(1, sizeof(*ready));
    readable = calloc(1, sizeof(*readable));
    if (receive == NULL || ready == NULL || readable == NULL) {
        if (receive != NULL) {
            free(receive->data);
            free(receive);
        }
        free(ready);
        free(readable);
        return -ENOMEM;
    }

    ready->transport = transport;
    ready->info.kind = TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY;
    ready->info.flags = TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER;
    ready->info.subject_kind = TREVRPC_RPC_TRANSPORT_OBJECT_STREAM;
    ready->info.subject = stream;
    ready->info.parent = fake_listener_handle;
    readable->transport = transport;
    readable->info.kind = TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE;
    readable->info.flags = TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER;
    readable->info.subject_kind = TREVRPC_RPC_TRANSPORT_OBJECT_STREAM;
    readable->info.subject = stream;
    readable->info.parent = fake_listener_handle;

    pthread_mutex_lock(&transport->mutex);
    if (transport->event_count > FAKE_EVENT_CAPACITY - 2u) {
        pthread_mutex_unlock(&transport->mutex);
        free(receive->data);
        free(receive);
        free(ready);
        free(readable);
        return -EAGAIN;
    }
    if (fake_handle_equal(stream, fake_second_stream_handle)) {
        transport->second_stream_created = true;
    }
    fake_receive_enqueue_locked(transport, receive);
    transport->events[transport->event_tail] = ready;
    transport->event_tail = (transport->event_tail + 1u) % FAKE_EVENT_CAPACITY;
    transport->events[transport->event_tail] = readable;
    transport->event_tail = (transport->event_tail + 1u) % FAKE_EVENT_CAPACITY;
    transport->event_count += 2u;
    {
        uint8_t byte = 1;
        int wake_fd = transport->signal_alternate_wake ? transport->alternate_wake_write_fd : transport->wake_write_fd;
        ssize_t written = write(wake_fd, &byte, sizeof(byte));
        (void)written;
    }
    pthread_mutex_unlock(&transport->mutex);
    return 0;
}

int fake_push_incoming_stream(fake_transport* transport, const uint8_t* data, size_t data_len) {
    return fake_push_incoming_stream_for_handle(transport, fake_stream_handle, data, data_len);
}

int fake_get_wake_source(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_wake* wake) {
    fake_transport* fake = fake_from_base(transport);
    wake->kind = fake->wake_source_kind != 0 ? fake->wake_source_kind : TREVRPC_RPC_TRANSPORT_WAKE_SOURCE_POSIX_FD;
    wake->flags = TREVRPC_RPC_TRANSPORT_WAKE_FLAG_BORROWED | TREVRPC_RPC_TRANSPORT_WAKE_FLAG_LEVEL_TRIGGERED;
    wake->native_handle = fake->wake_read_fd;
    return 0;
}

int fake_get_wake_sources(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_wake* wakes, size_t capacity, size_t* out_count) {
    fake_transport* fake = fake_from_base(transport);
    if (fake->wake_sources_result != 0) {
        return fake->wake_sources_result;
    }
    if (capacity < 2) {
        return -ENOSPC;
    }
    wakes[0].kind = fake->wake_source_kind != 0 ? fake->wake_source_kind : TREVRPC_RPC_TRANSPORT_WAKE_SOURCE_POSIX_FD;
    wakes[0].flags = TREVRPC_RPC_TRANSPORT_WAKE_FLAG_BORROWED | TREVRPC_RPC_TRANSPORT_WAKE_FLAG_LEVEL_TRIGGERED;
    wakes[0].native_handle = fake->wake_read_fd;
    wakes[1] = wakes[0];
    wakes[1].native_handle = fake->alternate_wake_read_fd;
    *out_count = 2;
    return 0;
}

static uint64_t fake_monotonic_nanos(void) {
    struct timespec now = {0};
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
        return 0;
    return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}

static int fake_poll_timeout_ms(trevrpc_rpc_transport* transport) {
    fake_transport* fake = fake_from_base(transport);
    int timeout = atomic_load_explicit(&fake->poll_timeout_ms, memory_order_acquire);
    uint64_t now;
    uint64_t deadline;
    uint64_t remaining;
    if (timeout < 0)
        return -1;
    now = fake_monotonic_nanos();
    deadline = atomic_load_explicit(&fake->poll_deadline_nanos, memory_order_acquire);
    if (deadline == 0) {
        uint64_t desired = now + (uint64_t)timeout * UINT64_C(1000000);
        if (atomic_compare_exchange_strong_explicit(
                &fake->poll_deadline_nanos, &deadline, desired, memory_order_acq_rel, memory_order_acquire))
            deadline = desired;
    }
    if (deadline <= now)
        return 0;
    remaining = deadline - now;
    return (int)(remaining / UINT64_C(1000000) + (remaining % UINT64_C(1000000) != 0));
}

int fake_next_event(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_event** out_event) {
    fake_transport* fake = fake_from_base(transport);
    fake_event* event;
    int next_event_result = atomic_load_explicit(&fake->next_event_result, memory_order_acquire);
    if (next_event_result != 0) {
        return next_event_result;
    }
    /* Test-only probe: publish entry before taking the source mutex.  This
     * makes an old composite implementation that calls us while endpoint
     * admission is blocked observable without relying on thread scheduling. */
    pthread_mutex_lock(&fake->next_event_probe_mutex);
    fake->next_event_attempted = true;
    pthread_cond_broadcast(&fake->next_event_probe_condition);
    pthread_mutex_unlock(&fake->next_event_probe_mutex);
    pthread_mutex_lock(&fake->mutex);
    if (fake->next_event_blocked) {
        fake->next_event_entered = true;
        pthread_cond_broadcast(&fake->condition);
        while (!fake->next_event_release)
            pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    if (fake->event_count == 0) {
        uint64_t deadline = atomic_load_explicit(&fake->poll_deadline_nanos, memory_order_acquire);
        if (deadline != 0 && fake_monotonic_nanos() >= deadline &&
            atomic_compare_exchange_strong_explicit(
                &fake->poll_deadline_nanos, &deadline, 0, memory_order_acq_rel, memory_order_acquire)) {
            atomic_store_explicit(&fake->poll_timeout_ms, -1, memory_order_release);
            atomic_fetch_add_explicit(&fake->poll_timeout_fires, 1, memory_order_release);
        }
        pthread_mutex_unlock(&fake->mutex);
        return -EAGAIN;
    }
    event = fake->events[fake->event_head];
    fake->events[fake->event_head] = NULL;
    fake->event_head = (fake->event_head + 1u) % FAKE_EVENT_CAPACITY;
    --fake->event_count;
    if (fake->event_count == 0) {
        fake_drain_wake_locked(fake);
    }
    pthread_mutex_unlock(&fake->mutex);
    *out_event = (trevrpc_rpc_transport_event*)event;
    return 0;
}

int fake_event_get_info(const trevrpc_rpc_transport_event* transport_event, trevrpc_rpc_transport_event_info* info) {
    const fake_event* event = (const fake_event*)transport_event;
    int result = atomic_exchange_explicit(&event->transport->event_get_info_result, 0, memory_order_acq_rel);
    if (result != 0) {
        return result;
    }
    *info = event->info;
    if (event->info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE) {
        pthread_mutex_lock(&event->transport->mutex);
        atomic_fetch_add_explicit(&event->transport->readable_info_calls, 1, memory_order_release);
        pthread_cond_broadcast(&event->transport->condition);
        pthread_mutex_unlock(&event->transport->mutex);
    }
    return 0;
}

static int fake_event_get_admission_info(
    const trevrpc_rpc_transport_event* transport_event, trevrpc_rpc_transport_admission_info* info) {
    const fake_event* event = (const fake_event*)transport_event;
    fake_transport* transport = event->transport;
    int result;
    if (event->info.kind != TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION &&
        event->info.kind != TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION) {
        return -ENOTSUP;
    }
    pthread_mutex_lock(&transport->mutex);
    if (transport->admission_info_blocked) {
        transport->admission_info_entered = true;
        pthread_cond_broadcast(&transport->condition);
        while (!transport->admission_info_release) {
            pthread_cond_wait(&transport->condition, &transport->mutex);
        }
    }
    pthread_mutex_unlock(&transport->mutex);
    result = atomic_exchange_explicit(&transport->admission_get_info_result, 0, memory_order_acq_rel);
    if (result != 0) {
        return result;
    }
    *info = event->admission;
    return 0;
}

static int fake_admission_respond(const trevrpc_rpc_transport_event* transport_event, uint16_t status) {
    fake_event* event = (fake_event*)transport_event;
    fake_transport* transport;
    int result;
    if (event == NULL || (status != 200 && (status < 400 || status > 599))) {
        return -EINVAL;
    }
    if (event->info.kind != TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION &&
        event->info.kind != TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION) {
        return -ENOTSUP;
    }
    transport = event->transport;
    pthread_mutex_lock(&transport->mutex);
    if (event->admission_decided) {
        pthread_mutex_unlock(&transport->mutex);
        return -EALREADY;
    }
    event->admission_decided = true;
    atomic_store_explicit(&transport->admission_last_status, status, memory_order_relaxed);
    atomic_fetch_add_explicit(&transport->admission_response_calls, 1, memory_order_release);
    result = atomic_exchange_explicit(&transport->admission_respond_result, 0, memory_order_acq_rel);
    pthread_cond_broadcast(&transport->condition);
    pthread_mutex_unlock(&transport->mutex);
    return result;
}

void fake_event_release(trevrpc_rpc_transport_event* transport_event) {
    fake_event* event = (fake_event*)transport_event;
    if (event == NULL) {
        return;
    }
    if (!event->admission_decided && (event->info.kind == TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION ||
                                         event->info.kind == TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION)) {
        atomic_fetch_add_explicit(&event->transport->admission_undecided_release_calls, 1, memory_order_release);
        (void)fake_admission_respond(transport_event, 500);
    }
    pthread_mutex_lock(&event->transport->mutex);
    if (event->info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE) {
        atomic_fetch_add_explicit(&event->transport->readable_release_calls, 1, memory_order_release);
    } else if (event->info.kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN) {
        atomic_fetch_add_explicit(&event->transport->receive_fin_release_calls, 1, memory_order_release);
    } else if (event->info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED ||
               event->info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED) {
        atomic_fetch_add_explicit(&event->transport->stream_terminal_release_calls, 1, memory_order_release);
    }
    pthread_cond_broadcast(&event->transport->condition);
    pthread_mutex_unlock(&event->transport->mutex);
    free(event->admission_path);
    free(event->admission_authority);
    free(event->admission_origin);
    free(event);
}

int fake_receive_get_info(
    const trevrpc_rpc_transport_receive* transport_receive, trevrpc_rpc_transport_receive_info* info) {
    const fake_receive* receive = (const fake_receive*)transport_receive;
    int result = atomic_exchange_explicit(&receive->transport->receive_get_info_result, 0, memory_order_acq_rel);
    if (result != 0) {
        return result;
    }
    info->flags = 0;
    info->data = receive->data;
    info->data_len = receive->data_len;
    return 0;
}

void fake_receive_release(trevrpc_rpc_transport_receive* transport_receive) {
    fake_receive* receive = (fake_receive*)transport_receive;
    if (receive == NULL) {
        return;
    }
    atomic_fetch_add_explicit(&receive->transport->receive_release_calls, 1, memory_order_release);
    free(receive->data);
    free(receive);
}

int fake_get_diagnostics(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_diagnostics* diagnostics) {
    fake_transport* fake = fake_from_base(transport);
    pthread_mutex_lock(&fake->mutex);
    while (fake->diagnostics_blocked && !fake->diagnostics_release) {
        fake->diagnostics_entered = true;
        pthread_cond_broadcast(&fake->condition);
        pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    diagnostics->state =
        fake->close_requested ? TREVRPC_RPC_TRANSPORT_STATE_STOPPED : TREVRPC_RPC_TRANSPORT_STATE_RUNNING;
    diagnostics->event_capacity = FAKE_EVENT_CAPACITY;
    diagnostics->queue_depth = (uint32_t)fake->event_count;
    diagnostics->ordinary_queue_depth = 0;
    diagnostics->live_listeners = fake->listener_closed ? 0 : 1;
    diagnostics->live_connections = fake->connection_closed ? 0 : 1;
    diagnostics->live_streams =
        (fake->stream_closed ? 0 : 1) + (fake->second_stream_created && !fake->second_stream_closed ? 1 : 0);
    pthread_mutex_unlock(&fake->mutex);
    return 0;
}

int fake_endpoint_listen(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_endpoint_config* config,
    trevrpc_rpc_transport_handle* listener) {
    fake_transport* fake = fake_from_base(transport);
    (void)config;
    pthread_mutex_lock(&fake->mutex);
    *listener = fake_listener_handle;
    pthread_mutex_unlock(&fake->mutex);
    return 0;
}

int fake_endpoint_get_port(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle listener, uint16_t* port) {
    (void)transport;
    (void)listener;
    *port = 4242;
    return 0;
}

int fake_endpoint_dial(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_endpoint_config* config,
    uint64_t operation_id,
    trevrpc_rpc_transport_handle* connection) {
    (void)config;
    (void)operation_id;
    fake_transport* fake = fake_from_base(transport);
    pthread_mutex_lock(&fake->mutex);
    if (fake->dial_blocked) {
        fake->dial_entered = true;
        pthread_cond_broadcast(&fake->condition);
        while (!fake->dial_release)
            pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    *connection = fake_connection_handle;
    pthread_mutex_unlock(&fake->mutex);
    return 0;
}

int fake_dial_cancel(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection) {
    (void)transport;
    (void)connection;
    return -ENOTSUP;
}

int fake_stream_open(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    uint64_t operation_id,
    trevrpc_rpc_transport_handle* stream) {
    fake_transport* fake = fake_from_base(transport);
    (void)operation_id;
    if (!fake_handle_equal(connection, fake_connection_handle)) {
        return -ESTALE;
    }
    *stream = fake_stream_handle;
    atomic_fetch_add_explicit(&fake->stream_open_calls, 1, memory_order_release);
    pthread_mutex_lock(&fake->mutex);
    pthread_cond_broadcast(&fake->condition);
    pthread_mutex_unlock(&fake->mutex);
    return 0;
}

int fake_stream_send(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle stream,
    uint64_t operation_id,
    const uint8_t* body,
    size_t body_len) {
    fake_transport* fake = fake_from_base(transport);
    uint8_t* body_copy = NULL;
    int result;
    if (!fake_stream_handle_valid(stream)) {
        return -ESTALE;
    }
    result = atomic_exchange_explicit(&fake->stream_send_result, 0, memory_order_acq_rel);
    if (result == 0 && body_len != 0) {
        body_copy = malloc(body_len);
        if (body_copy == NULL) {
            result = -ENOMEM;
        } else {
            memcpy(body_copy, body, body_len);
        }
    }
    pthread_mutex_lock(&fake->mutex);
    if (fake->send_blocked) {
        fake->send_entered = true;
        pthread_cond_broadcast(&fake->condition);
        while (!fake->send_release) {
            pthread_cond_wait(&fake->condition, &fake->mutex);
        }
    }
    if (result == 0) {
        free(fake->last_send_body);
        fake->last_send_body = body_copy;
        fake->last_send_body_len = body_len;
        atomic_store_explicit(&fake->last_send_operation_id, operation_id, memory_order_release);
    } else {
        free(body_copy);
    }
    atomic_fetch_add_explicit(&fake->stream_send_calls, 1, memory_order_release);
    pthread_cond_broadcast(&fake->condition);
    pthread_mutex_unlock(&fake->mutex);
    return result;
}

int fake_stream_receive(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle stream,
    trevrpc_rpc_transport_receive** out_receive) {
    fake_transport* fake = fake_from_base(transport);
    fake_receive* receive;
    int result;
    if (!fake_stream_handle_valid(stream)) {
        return -ESTALE;
    }
    result = atomic_exchange_explicit(&fake->stream_receive_result, 0, memory_order_acq_rel);
    if (result != 0) {
        return result;
    }
    pthread_mutex_lock(&fake->mutex);
    if (fake->receive_blocked) {
        fake->receive_entered = true;
        pthread_cond_broadcast(&fake->condition);
        while (!fake->receive_release)
            pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    receive = fake->stream_receive_head;
    if (receive != NULL) {
        fake->stream_receive_head = receive->next;
        if (fake->stream_receive_head == NULL) {
            fake->stream_receive_tail = NULL;
        }
        receive->next = NULL;
    }
    pthread_mutex_unlock(&fake->mutex);
    if (receive == NULL) {
        return -EAGAIN;
    }
    *out_receive = (trevrpc_rpc_transport_receive*)receive;
    return 0;
}

int fake_stream_finish_send(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream) {
    fake_transport* fake = fake_from_base(transport);
    if (!fake_handle_equal(stream, fake_stream_handle)) {
        return -ESTALE;
    }
    atomic_fetch_add_explicit(&fake->stream_finish_send_calls, 1, memory_order_release);
    pthread_mutex_lock(&fake->mutex);
    pthread_cond_broadcast(&fake->condition);
    pthread_mutex_unlock(&fake->mutex);
    return 0;
}

static int fake_stream_abort_half(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, uint64_t error_code, atomic_uint* calls) {
    fake_transport* fake = fake_from_base(transport);
    if (!fake_stream_handle_valid(stream)) {
        return -ESTALE;
    }
    if (fake->stream_abort_result != 0) {
        return fake->stream_abort_result;
    }
    atomic_fetch_add_explicit(calls, 1, memory_order_release);
    atomic_store_explicit(&fake->last_abort_error, error_code, memory_order_release);
    pthread_mutex_lock(&fake->mutex);
    pthread_cond_broadcast(&fake->condition);
    pthread_mutex_unlock(&fake->mutex);
    return 0;
}

static int fake_stream_abort_receive(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, uint64_t error_code) {
    fake_transport* fake = fake_from_base(transport);
    return fake_stream_abort_half(transport, stream, error_code, &fake->stream_abort_receive_calls);
}

static int fake_stream_abort_send(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, uint64_t error_code) {
    fake_transport* fake = fake_from_base(transport);
    return fake_stream_abort_half(transport, stream, error_code, &fake->stream_abort_send_calls);
}

int fake_stream_abort(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, uint64_t error_code) {
    fake_transport* fake = fake_from_base(transport);
    bool* stream_closed;
    if (!fake_stream_handle_valid(stream)) {
        return -ESTALE;
    }
    if (fake->stream_abort_result != 0) {
        return fake->stream_abort_result;
    }
    pthread_mutex_lock(&fake->mutex);
    stream_closed =
        fake_handle_equal(stream, fake_second_stream_handle) ? &fake->second_stream_closed : &fake->stream_closed;
    if (*stream_closed) {
        pthread_mutex_unlock(&fake->mutex);
        return -EALREADY;
    }
    *stream_closed = true;
    atomic_fetch_add_explicit(&fake->stream_abort_calls, 1, memory_order_release);
    atomic_store_explicit(&fake->last_abort_error, error_code, memory_order_release);
    pthread_cond_broadcast(&fake->condition);
    pthread_mutex_unlock(&fake->mutex);
    return fake_push_event(fake,
        TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL,
        stream,
        fake_connection_handle);
}

int fake_stream_close(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream) {
    fake_transport* fake = fake_from_base(transport);
    int result;
    if (!fake_handle_equal(stream, fake_stream_handle)) {
        return -ESTALE;
    }
    pthread_mutex_lock(&fake->mutex);
    if (fake->stream_closed) {
        pthread_mutex_unlock(&fake->mutex);
        return -EALREADY;
    }
    fake->stream_closed = true;
    atomic_fetch_add_explicit(&fake->stream_close_calls, 1, memory_order_release);
    pthread_mutex_unlock(&fake->mutex);
    result = fake_push_event(fake,
        TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
        fake_stream_handle,
        fake_listener_handle);
    return result;
}

int fake_connection_close(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, uint64_t error_code) {
    fake_transport* fake = fake_from_base(transport);
    int result;
    (void)error_code;
    if (!fake_handle_equal(connection, fake_connection_handle)) {
        return -ESTALE;
    }
    if (atomic_load_explicit(&fake->connection_close_result_persistent, memory_order_acquire)) {
        result = atomic_load_explicit(&fake->connection_close_result, memory_order_acquire);
    } else {
        result = atomic_exchange_explicit(&fake->connection_close_result, 0, memory_order_acq_rel);
    }
    if (result != 0) {
        return result;
    }
    pthread_mutex_lock(&fake->mutex);
    if (fake->connection_closed) {
        pthread_mutex_unlock(&fake->mutex);
        return -EALREADY;
    }
    fake->connection_closed = true;
    pthread_mutex_unlock(&fake->mutex);
    return fake_push_event(fake,
        TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL |
            TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
        fake_connection_handle,
        (trevrpc_rpc_transport_handle){0});
}

int fake_listener_close(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle listener) {
    fake_transport* fake = fake_from_base(transport);
    int result;
    if (!fake_handle_equal(listener, fake_listener_handle)) {
        return -ESTALE;
    }
    pthread_mutex_lock(&fake->mutex);
    if (fake->listener_close_blocked) {
        fake->listener_close_entered = true;
        pthread_cond_broadcast(&fake->condition);
        while (!fake->listener_close_release) {
            pthread_cond_wait(&fake->condition, &fake->mutex);
        }
    }
    if (fake->listener_closed) {
        pthread_mutex_unlock(&fake->mutex);
        return -EALREADY;
    }
    fake->listener_closed = true;
    pthread_mutex_unlock(&fake->mutex);
    result = fake_push_event(fake,
        TREVRPC_RPC_TRANSPORT_EVENT_LISTENER_STOPPED,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL |
            TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
        fake_listener_handle,
        (trevrpc_rpc_transport_handle){0});
    return result;
}

int fake_close(trevrpc_rpc_transport* transport) {
    fake_transport* fake = fake_from_base(transport);
    int result;
    pthread_mutex_lock(&fake->mutex);
    if (fake->close_blocked) {
        fake->close_entered = true;
        pthread_cond_broadcast(&fake->condition);
        while (!fake->close_release)
            pthread_cond_wait(&fake->condition, &fake->mutex);
    }
    if (atomic_load_explicit(&fake->close_result_persistent, memory_order_acquire)) {
        result = atomic_load_explicit(&fake->close_result, memory_order_acquire);
    } else {
        result = atomic_exchange_explicit(&fake->close_result, 0, memory_order_acq_rel);
    }
    if (result != 0) {
        pthread_mutex_unlock(&fake->mutex);
        return result;
    }
    if (fake->close_requested) {
        pthread_mutex_unlock(&fake->mutex);
        return -EALREADY;
    }
    fake->close_requested = true;
    pthread_mutex_unlock(&fake->mutex);
    result = fake_push_status_event(fake,
        TREVRPC_RPC_TRANSPORT_EVENT_STOPPED,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL,
        (trevrpc_rpc_transport_handle){0},
        (trevrpc_rpc_transport_handle){0},
        atomic_load_explicit(&fake->close_status, memory_order_acquire));
    return result;
}

static int fake_release_handle(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle, uint32_t kind) {
    fake_transport* fake = fake_from_base(transport);
    int result;
    atomic_int* result_slot = kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM ? &fake->stream_release_handle_result
                                                                          : &fake->call_release_handle_result;
    atomic_bool* persistent_slot = kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM
                                       ? &fake->stream_release_handle_result_persistent
                                       : &fake->call_release_handle_result_persistent;
    if (atomic_load_explicit(persistent_slot, memory_order_acquire)) {
        result = atomic_load_explicit(result_slot, memory_order_acquire);
    } else {
        result = atomic_exchange_explicit(result_slot, 0, memory_order_acq_rel);
    }
    (void)handle;
    atomic_fetch_add_explicit(&fake->release_handle_calls, 1, memory_order_release);
    pthread_mutex_lock(&fake->mutex);
    pthread_cond_broadcast(&fake->condition);
    pthread_mutex_unlock(&fake->mutex);
    return result;
}

int fake_drain(trevrpc_rpc_transport* transport) {
    (void)transport;
    return 0;
}

void fake_destroy(trevrpc_rpc_transport* transport) {
    fake_transport* fake = fake_from_base(transport);
    size_t index;
    pthread_mutex_lock(&fake->mutex);
    for (index = 0; index < FAKE_EVENT_CAPACITY; ++index) {
        if (fake->events[index] != NULL) {
            free(fake->events[index]->admission_path);
            free(fake->events[index]->admission_authority);
            free(fake->events[index]->admission_origin);
            free(fake->events[index]);
            fake->events[index] = NULL;
        }
    }
    while (fake->stream_receive_head != NULL) {
        fake_receive* receive = fake->stream_receive_head;
        fake->stream_receive_head = receive->next;
        free(receive->data);
        free(receive);
    }
    fake->stream_receive_tail = NULL;
    free(fake->last_send_body);
    fake->last_send_body = NULL;
    fake->last_send_body_len = 0;
    pthread_mutex_unlock(&fake->mutex);
    close(fake->wake_read_fd);
    close(fake->wake_write_fd);
    close(fake->alternate_wake_read_fd);
    close(fake->alternate_wake_write_fd);
    pthread_cond_destroy(&fake->next_event_probe_condition);
    pthread_mutex_destroy(&fake->next_event_probe_mutex);
    pthread_cond_destroy(&fake->condition);
    pthread_mutex_destroy(&fake->mutex);
    free(fake);
}

static const trevrpc_rpc_transport_ops fake_ops = {
    .get_wake_source = fake_get_wake_source,
    .next_event = fake_next_event,
    .event_get_info = fake_event_get_info,
    .event_get_admission_info = fake_event_get_admission_info,
    .admission_respond = fake_admission_respond,
    .event_release = fake_event_release,
    .receive_get_info = fake_receive_get_info,
    .receive_release = fake_receive_release,
    .get_diagnostics = fake_get_diagnostics,
    .poll_timeout_ms = fake_poll_timeout_ms,
    .endpoint_listen = fake_endpoint_listen,
    .endpoint_get_port = fake_endpoint_get_port,
    .endpoint_dial = fake_endpoint_dial,
    .dial_cancel = fake_dial_cancel,
    .stream_open = fake_stream_open,
    .stream_send = fake_stream_send,
    .stream_receive = fake_stream_receive,
    .stream_finish_send = fake_stream_finish_send,
    .stream_abort_receive = fake_stream_abort_receive,
    .stream_abort_send = fake_stream_abort_send,
    .stream_abort = fake_stream_abort,
    .stream_close = fake_stream_close,
    .connection_close = fake_connection_close,
    .listener_close = fake_listener_close,
    .release_handle = fake_release_handle,
    .close = fake_close,
    .drain = fake_drain,
    .destroy = fake_destroy,
    .get_wake_sources = fake_get_wake_sources,
};

size_t fake_close_count(const fake_transport* fake) {
    return (size_t)atomic_load_explicit(&fake->stream_abort_calls, memory_order_acquire) +
           (size_t)atomic_load_explicit(&fake->stream_close_calls, memory_order_acquire);
}

size_t fake_release_handle_count(const fake_transport* fake) {
    return (size_t)atomic_load_explicit(&fake->release_handle_calls, memory_order_acquire);
}

fake_transport* fake_create(void) {
    fake_transport* fake = calloc(1, sizeof(*fake));
    int descriptors[2];
    int alternate_descriptors[2];
    if (fake == NULL)
        return NULL;
    if (pipe(descriptors) != 0) {
        free(fake);
        return NULL;
    }
    if (pipe(alternate_descriptors) != 0) {
        close(descriptors[0]);
        close(descriptors[1]);
        free(fake);
        return NULL;
    }
    fake->wake_read_fd = descriptors[0];
    fake->wake_write_fd = descriptors[1];
    fake->alternate_wake_read_fd = alternate_descriptors[0];
    fake->alternate_wake_write_fd = alternate_descriptors[1];
    if (fcntl(fake->wake_read_fd, F_SETFL, O_NONBLOCK) != 0 || fcntl(fake->wake_write_fd, F_SETFL, O_NONBLOCK) != 0 ||
        fcntl(fake->alternate_wake_read_fd, F_SETFL, O_NONBLOCK) != 0 ||
        fcntl(fake->alternate_wake_write_fd, F_SETFL, O_NONBLOCK) != 0 || pthread_mutex_init(&fake->mutex, NULL) != 0) {
        close(fake->wake_read_fd);
        close(fake->wake_write_fd);
        close(fake->alternate_wake_read_fd);
        close(fake->alternate_wake_write_fd);
        free(fake);
        return NULL;
    }
    if (pthread_cond_init(&fake->condition, NULL) != 0) {
        pthread_mutex_destroy(&fake->mutex);
        close(fake->wake_read_fd);
        close(fake->wake_write_fd);
        close(fake->alternate_wake_read_fd);
        close(fake->alternate_wake_write_fd);
        free(fake);
        return NULL;
    }
    if (pthread_mutex_init(&fake->next_event_probe_mutex, NULL) != 0) {
        pthread_cond_destroy(&fake->condition);
        pthread_mutex_destroy(&fake->mutex);
        close(fake->wake_read_fd);
        close(fake->wake_write_fd);
        close(fake->alternate_wake_read_fd);
        close(fake->alternate_wake_write_fd);
        free(fake);
        return NULL;
    }
    if (pthread_cond_init(&fake->next_event_probe_condition, NULL) != 0) {
        pthread_mutex_destroy(&fake->next_event_probe_mutex);
        pthread_cond_destroy(&fake->condition);
        pthread_mutex_destroy(&fake->mutex);
        close(fake->wake_read_fd);
        close(fake->wake_write_fd);
        close(fake->alternate_wake_read_fd);
        close(fake->alternate_wake_write_fd);
        free(fake);
        return NULL;
    }
    atomic_init(&fake->readable_info_calls, 0);
    atomic_init(&fake->readable_release_calls, 0);
    atomic_init(&fake->receive_release_calls, 0);
    atomic_init(&fake->stream_abort_receive_calls, 0);
    atomic_init(&fake->stream_abort_send_calls, 0);
    atomic_init(&fake->stream_abort_calls, 0);
    atomic_init(&fake->stream_close_calls, 0);
    atomic_init(&fake->stream_open_calls, 0);
    atomic_init(&fake->stream_send_calls, 0);
    atomic_init(&fake->stream_finish_send_calls, 0);
    atomic_init(&fake->admission_response_calls, 0);
    atomic_init(&fake->admission_undecided_release_calls, 0);
    atomic_init(&fake->admission_last_status, 0);
    atomic_init(&fake->next_event_result, 0);
    atomic_init(&fake->close_result, 0);
    atomic_init(&fake->close_result_persistent, false);
    atomic_init(&fake->close_status, 0);
    atomic_init(&fake->connection_close_result, 0);
    atomic_init(&fake->connection_close_result_persistent, false);
    atomic_init(&fake->poll_timeout_ms, -1);
    atomic_init(&fake->poll_deadline_nanos, 0);
    atomic_init(&fake->poll_timeout_fires, 0);
    atomic_init(&fake->stream_receive_result, 0);
    atomic_init(&fake->event_get_info_result, 0);
    atomic_init(&fake->admission_get_info_result, 0);
    atomic_init(&fake->admission_respond_result, 0);
    atomic_init(&fake->receive_get_info_result, 0);
    atomic_init(&fake->stream_send_result, 0);
    atomic_init(&fake->release_handle_result, 0);
    atomic_init(&fake->stream_release_handle_result, 0);
    atomic_init(&fake->stream_release_handle_result_persistent, false);
    atomic_init(&fake->call_release_handle_result, 0);
    atomic_init(&fake->call_release_handle_result_persistent, false);
    atomic_init(&fake->last_abort_error, 0);
    atomic_init(&fake->last_send_operation_id, 0);
    fake->base.ops = &fake_ops;
    return fake;
}
