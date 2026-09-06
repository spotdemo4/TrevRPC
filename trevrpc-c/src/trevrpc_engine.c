#if defined(TREVRPC_ENGINE_HAVE_PIPE2) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif

#include "trevrpc_engine_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define TREVRPC_ENGINE_GATE_CLOSED 1u
#define TREVRPC_ENGINE_GATE_REFERENCE 2u
#define TREVRPC_ENGINE_EVENT_CLASS_ORDINARY 0u
#define TREVRPC_ENGINE_EVENT_CLASS_MANDATORY 1u
#define TREVRPC_ENGINE_EVENT_CLASS_FATAL 2u
#define TREVRPC_ENGINE_EVENT_CLASS_STOPPED 3u

struct trevrpc_engine_event {
    struct trevrpc_engine_event* next;
    uint32_t kind;
    uint32_t flags;
    int32_t status;
    uint32_t subject_kind;
    uint32_t event_class;
    uint64_t sequence;
    trevrpc_engine_handle_v1 subject;
    trevrpc_engine_handle_v1 parent;
    uint64_t operation_id;
    uint64_t application_error_code;
    uint64_t provider_error_code;
    const uint8_t* data;
    uint64_t data_len;
    void* provider_context;
    trevrpc_engine_detach_hook dequeue_hook;
    trevrpc_engine_detach_hook drop_hook;
    void* hook_context;
    uint8_t copied_data[];
};

struct trevrpc_engine_receive {
    uint32_t flags;
    const uint8_t* data;
    uint64_t data_len;
    void* owner;
    trevrpc_engine_owned_release release;
    void* release_context;
    uint8_t copied_data[];
};

struct trevrpc_engine_reservation {
    atomic_uint references;
    atomic_bool consumed;
    trevrpc_engine_event* event;
};

struct trevrpc_engine {
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    pthread_mutex_t lifetime_mutex;
    pthread_cond_t lifetime_condition;
    pthread_mutex_t test_mutex;
    pthread_cond_t test_condition;
    atomic_uint_fast64_t api_gate;
    atomic_uint_fast64_t lifetime_gate;
    atomic_bool release_waiting;
    uint32_t state;
    int32_t terminal_status;
    uint64_t provider_error_code;
    uint32_t event_capacity;
    uint32_t queue_depth;
    uint32_t ordinary_queue_depth;
    uint32_t ordinary_event_reservations;
    uint64_t next_sequence;
    uint64_t events_enqueued;
    uint64_t events_dequeued;
    uint64_t events_rejected;
    uint64_t wake_signals;
    uint64_t wake_write_eagain;
    uint64_t wake_failures;
    uint64_t active_callbacks;
    uint64_t active_operations;
    uint64_t mandatory_reservations;
    uint64_t provider_close_in_flight;
    bool wake_armed;
    bool provider_close_started;
    bool provider_stop_reported;
    int wake_read_fd;
    int wake_write_fd;
    trevrpc_engine_event* event_head;
    trevrpc_engine_event* event_tail;
    trevrpc_engine_event* fatal_event;
    trevrpc_engine_event* stopped_event;
    trevrpc_engine_provider_ops provider_ops;
    void* provider_context;
    uint64_t owner_cookie;
    atomic_bool test_pause_last_drain_active;
    atomic_bool test_pause_api_enter_active;
    atomic_bool test_pause_api_leave_active;
    bool test_pause_last_drain;
    bool test_in_last_drain;
    bool test_pause_api_enter;
    bool test_in_api_enter;
    bool test_pause_api_leave;
    bool test_in_api_leave;
};

static _Thread_local trevrpc_engine* trevrpc_engine_callback_context;
static _Thread_local uint32_t trevrpc_engine_callback_depth;

static void trevrpc_engine_counter_increment(uint64_t* counter) {
    if (*counter != UINT64_MAX) {
        ++*counter;
    }
}

static int trevrpc_engine_initialize_structure(void* structure, size_t struct_size, size_t required_size) {
    uint32_t size_field;
    uint32_t version_field = TREVRPC_ENGINE_STRUCT_VERSION_1;
    uint8_t* bytes = structure;
    if (structure == NULL || struct_size < required_size) {
        return -EINVAL;
    }
    if (struct_size > UINT32_MAX) {
        return -EOVERFLOW;
    }
    size_field = (uint32_t)struct_size;
    memset(structure, 0, struct_size);
    memcpy(bytes, &size_field, sizeof(size_field));
    memcpy(bytes + sizeof(size_field), &version_field, sizeof(version_field));
    return 0;
}

static bool trevrpc_engine_u64_fields_zero(const uint64_t* fields, size_t count) {
    size_t index;
    for (index = 0; index < count; ++index) {
        if (fields[index] != 0) {
            return false;
        }
    }
    return true;
}

static int trevrpc_engine_validate_config(const trevrpc_engine_config_v1* config) {
    if (config == NULL || config->struct_size < sizeof(*config)) {
        return -EINVAL;
    }
    if (config->struct_version != TREVRPC_ENGINE_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (config->flags != 0 || !trevrpc_engine_u64_fields_zero(config->reserved, 5)) {
        return -EINVAL;
    }
    if (config->event_capacity == 0 || config->event_capacity > TREVRPC_ENGINE_MAX_EVENT_CAPACITY ||
        config->listener_capacity == 0 || config->connection_capacity == 0 || config->stream_capacity == 0 ||
        config->max_receive_owned_count == 0 || config->max_receive_owned_bytes == 0) {
        return -EINVAL;
    }
    return 0;
}

static int trevrpc_engine_validate_endpoint_config(const trevrpc_engine_endpoint_config_v1* config) {
    if (config == NULL || config->struct_size < sizeof(*config)) {
        return -EINVAL;
    }
    if (config->struct_version != TREVRPC_ENGINE_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (!trevrpc_engine_u64_fields_zero(config->reserved, 4) || config->reserved0 != 0 || config->reserved1 != 0 ||
        config->reserved2 != 0 || (config->host == NULL && config->host_len != 0) ||
        (config->alpn == NULL && config->alpn_len != 0) || (config->cert_file == NULL && config->cert_file_len != 0) ||
        (config->key_file == NULL && config->key_file_len != 0) ||
        (config->ca_cert_file == NULL && config->ca_cert_file_len != 0)) {
        return -EINVAL;
    }
    return 0;
}

static int trevrpc_engine_validate_wake_output(const trevrpc_engine_wake_source_v1* wake_source) {
    if (wake_source == NULL || wake_source->struct_size < sizeof(*wake_source)) {
        return -EINVAL;
    }
    if (wake_source->struct_version != TREVRPC_ENGINE_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (!trevrpc_engine_u64_fields_zero(wake_source->reserved, 3)) {
        return -EINVAL;
    }
    return 0;
}

static int trevrpc_engine_validate_event_info_output(const trevrpc_engine_event_info_v1* info) {
    if (info == NULL || info->struct_size < sizeof(*info)) {
        return -EINVAL;
    }
    if (info->struct_version != TREVRPC_ENGINE_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (!trevrpc_engine_u64_fields_zero(info->reserved, 4)) {
        return -EINVAL;
    }
    return 0;
}

static int trevrpc_engine_validate_receive_info_output(const trevrpc_engine_receive_info_v1* info) {
    if (info == NULL || info->struct_size < sizeof(*info)) {
        return -EINVAL;
    }
    if (info->struct_version != TREVRPC_ENGINE_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (info->reserved0 != 0 || !trevrpc_engine_u64_fields_zero(info->reserved, 4)) {
        return -EINVAL;
    }
    return 0;
}

static int trevrpc_engine_validate_diagnostics_output(const trevrpc_engine_diagnostics_v1* diagnostics) {
    if (diagnostics == NULL || diagnostics->struct_size < sizeof(*diagnostics)) {
        return -EINVAL;
    }
    if (diagnostics->struct_version != TREVRPC_ENGINE_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (!trevrpc_engine_u64_fields_zero(diagnostics->reserved, 3)) {
        return -EINVAL;
    }
    return 0;
}

static trevrpc_engine_event* trevrpc_engine_event_allocate(size_t data_len) {
    trevrpc_engine_event* event;
    if (data_len > SIZE_MAX - sizeof(*event)) {
        return NULL;
    }
    event = calloc(1, sizeof(*event) + data_len);
    if (event != NULL && data_len != 0) {
        event->data = event->copied_data;
        event->data_len = data_len;
    }
    return event;
}

static trevrpc_engine_event* trevrpc_engine_event_from_spec(
    const trevrpc_engine_event_spec* spec, uint32_t event_class) {
    trevrpc_engine_event* event;
    if (spec == NULL || (spec->data == NULL && spec->data_len != 0) || spec->data_len > UINT64_MAX) {
        return NULL;
    }
    event = trevrpc_engine_event_allocate(spec->data_len);
    if (event == NULL) {
        return NULL;
    }
    event->kind = spec->kind;
    event->flags = spec->flags;
    event->status = spec->status;
    event->subject_kind = spec->subject_kind;
    event->event_class = event_class;
    event->subject = spec->subject;
    event->parent = spec->parent;
    event->operation_id = spec->operation_id;
    event->application_error_code = spec->application_error_code;
    event->provider_error_code = spec->provider_error_code;
    event->dequeue_hook = spec->dequeue_hook;
    event->drop_hook = spec->drop_hook;
    event->hook_context = spec->hook_context;
    if (spec->data_len != 0) {
        memcpy(event->copied_data, spec->data, spec->data_len);
    }
    return event;
}

static void trevrpc_engine_event_destroy(trevrpc_engine_event* event) {
    if (event == NULL) {
        return;
    }
    if (event->drop_hook != NULL) {
        event->drop_hook(event->provider_context, event->hook_context);
    }
    free(event);
}

#ifndef TREVRPC_ENGINE_HAVE_PIPE2
static int trevrpc_engine_set_fd_flags(int descriptor) {
    int flags = fcntl(descriptor, F_GETFL);
    if (flags < 0 || fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) < 0) {
        return -errno;
    }
    flags = fcntl(descriptor, F_GETFD);
    if (flags < 0 || fcntl(descriptor, F_SETFD, flags | FD_CLOEXEC) < 0) {
        return -errno;
    }
    return 0;
}
#endif

static int trevrpc_engine_create_pipe(int descriptors[2]) {
#ifdef TREVRPC_ENGINE_HAVE_PIPE2
    if (pipe2(descriptors, O_NONBLOCK | O_CLOEXEC) != 0) {
        return -errno;
    }
#else
    int result;
    if (pipe(descriptors) != 0) {
        return -errno;
    }
    result = trevrpc_engine_set_fd_flags(descriptors[0]);
    if (result == 0) {
        result = trevrpc_engine_set_fd_flags(descriptors[1]);
    }
    if (result != 0) {
        close(descriptors[0]);
        close(descriptors[1]);
        return result;
    }
#endif
    return 0;
}

static int trevrpc_engine_gate_enter(atomic_uint_fast64_t* gate) {
    uint_fast64_t current = atomic_load_explicit(gate, memory_order_acquire);
    for (;;) {
        if ((current & TREVRPC_ENGINE_GATE_CLOSED) != 0) {
            return -EPIPE;
        }
        if (current > UINT_FAST64_MAX - TREVRPC_ENGINE_GATE_REFERENCE) {
            return -EOVERFLOW;
        }
        if (atomic_compare_exchange_weak_explicit(
                gate, &current, current + TREVRPC_ENGINE_GATE_REFERENCE, memory_order_acq_rel, memory_order_acquire)) {
            return 0;
        }
    }
}

static void trevrpc_engine_gate_leave(trevrpc_engine* engine, atomic_uint_fast64_t* gate, bool allow_test_pause) {
    uint_fast64_t current = atomic_load_explicit(gate, memory_order_acquire);
    if (allow_test_pause && atomic_load_explicit(&engine->test_pause_api_leave_active, memory_order_acquire)) {
        pthread_mutex_lock(&engine->test_mutex);
        if (engine->test_pause_api_leave) {
            engine->test_in_api_leave = true;
            pthread_cond_broadcast(&engine->test_condition);
            while (engine->test_pause_api_leave) {
                pthread_cond_wait(&engine->test_condition, &engine->test_mutex);
            }
            engine->test_in_api_leave = false;
        }
        pthread_mutex_unlock(&engine->test_mutex);
    }
    for (;;) {
        if ((current & TREVRPC_ENGINE_GATE_CLOSED) != 0) {
            pthread_mutex_lock(&engine->lifetime_mutex);
            atomic_fetch_sub_explicit(gate, TREVRPC_ENGINE_GATE_REFERENCE, memory_order_acq_rel);
            pthread_cond_broadcast(&engine->lifetime_condition);
            pthread_mutex_unlock(&engine->lifetime_mutex);
            return;
        }
        if (atomic_compare_exchange_weak_explicit(
                gate, &current, current - TREVRPC_ENGINE_GATE_REFERENCE, memory_order_acq_rel, memory_order_acquire)) {
            return;
        }
    }
}

static int trevrpc_engine_api_enter(trevrpc_engine* engine) {
    int result;
    if (engine == NULL) {
        return -EINVAL;
    }
    result = trevrpc_engine_gate_enter(&engine->api_gate);
    if (result != 0) {
        return result;
    }
    if (atomic_load_explicit(&engine->test_pause_api_enter_active, memory_order_acquire)) {
        pthread_mutex_lock(&engine->test_mutex);
        if (engine->test_pause_api_enter) {
            engine->test_in_api_enter = true;
            pthread_cond_broadcast(&engine->test_condition);
            while (engine->test_pause_api_enter) {
                pthread_cond_wait(&engine->test_condition, &engine->test_mutex);
            }
            engine->test_in_api_enter = false;
        }
        pthread_mutex_unlock(&engine->test_mutex);
    }
    pthread_mutex_lock(&engine->mutex);
    return 0;
}

static void trevrpc_engine_api_leave_unlocked(trevrpc_engine* engine) {
    trevrpc_engine_gate_leave(engine, &engine->api_gate, true);
}

static void trevrpc_engine_api_leave(trevrpc_engine* engine) {
    pthread_mutex_unlock(&engine->mutex);
    trevrpc_engine_api_leave_unlocked(engine);
}

static void trevrpc_engine_queue_push_locked(trevrpc_engine* engine, trevrpc_engine_event* event) {
    event->sequence = engine->next_sequence;
    if (engine->next_sequence != UINT64_MAX) {
        ++engine->next_sequence;
    }
    event->provider_context = engine->provider_context;
    if (engine->event_tail == NULL) {
        engine->event_head = event;
    } else {
        engine->event_tail->next = event;
    }
    engine->event_tail = event;
    ++engine->queue_depth;
    if (event->event_class == TREVRPC_ENGINE_EVENT_CLASS_ORDINARY) {
        ++engine->ordinary_queue_depth;
    }
    trevrpc_engine_counter_increment(&engine->events_enqueued);
}

static int trevrpc_engine_arm_wake_locked(trevrpc_engine* engine) {
    uint8_t byte = 1;
    ssize_t result;
    if (engine->wake_armed) {
        return 0;
    }
    if (engine->wake_read_fd < 0) {
        return -EBADF;
    }
    if (engine->wake_write_fd < 0) {
        trevrpc_engine_counter_increment(&engine->wake_failures);
        return -EBADF;
    }
    for (;;) {
        result = write(engine->wake_write_fd, &byte, sizeof(byte));
        if (result == (ssize_t)sizeof(byte)) {
            engine->wake_armed = true;
            trevrpc_engine_counter_increment(&engine->wake_signals);
            return 0;
        }
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result < 0 && errno == EAGAIN) {
            engine->wake_armed = true;
            trevrpc_engine_counter_increment(&engine->wake_signals);
            trevrpc_engine_counter_increment(&engine->wake_write_eagain);
            return 0;
        }
        trevrpc_engine_counter_increment(&engine->wake_failures);
        return result < 0 ? -errno : -EIO;
    }
}

static int trevrpc_engine_queue_event_locked(trevrpc_engine* engine, trevrpc_engine_event* event) {
    int result = 0;
    bool was_empty = engine->queue_depth == 0;
    trevrpc_engine_queue_push_locked(engine, event);
    if (was_empty) {
        result = trevrpc_engine_arm_wake_locked(engine);
    }
    return result;
}

static bool trevrpc_engine_record_failure_locked(trevrpc_engine* engine, int32_t status, uint64_t provider_error_code) {
    int wake_result;
    if (engine->state >= TREVRPC_ENGINE_STATE_STOPPED) {
        return false;
    }
    if (status >= 0) {
        status = -EIO;
    }
    if (engine->terminal_status == 0) {
        engine->terminal_status = status;
        engine->provider_error_code = provider_error_code;
        if (engine->fatal_event != NULL) {
            trevrpc_engine_event* fatal = engine->fatal_event;
            engine->fatal_event = NULL;
            fatal->kind = TREVRPC_ENGINE_EVENT_DIAGNOSTIC;
            fatal->flags = TREVRPC_ENGINE_EVENT_FLAG_FATAL;
            fatal->status = status;
            fatal->provider_error_code = provider_error_code;
            wake_result = trevrpc_engine_queue_event_locked(engine, fatal);
            if (wake_result != 0 && engine->terminal_status == 0) {
                engine->terminal_status = wake_result;
            }
        }
    }
    if (engine->state == TREVRPC_ENGINE_STATE_RUNNING) {
        engine->state = TREVRPC_ENGINE_STATE_STOPPING;
    }
    if (!engine->provider_close_started) {
        engine->provider_close_started = true;
        engine->provider_close_in_flight++;
        return true;
    }
    return false;
}

static void trevrpc_engine_maybe_publish_stopped_locked(trevrpc_engine* engine) {
    trevrpc_engine_event* stopped;
    int wake_result;
    if (engine->state != TREVRPC_ENGINE_STATE_STOPPING || !engine->provider_stop_reported ||
        engine->active_callbacks != 0 || engine->active_operations != 0 || engine->mandatory_reservations != 0 ||
        engine->provider_close_in_flight != 0 || engine->stopped_event == NULL) {
        return;
    }
    stopped = engine->stopped_event;
    engine->stopped_event = NULL;
    stopped->kind = TREVRPC_ENGINE_EVENT_STOPPED;
    stopped->flags = TREVRPC_ENGINE_EVENT_FLAG_TERMINAL;
    stopped->status = engine->terminal_status;
    stopped->provider_error_code = engine->provider_error_code;
    if (engine->terminal_status != 0) {
        stopped->flags |= TREVRPC_ENGINE_EVENT_FLAG_FATAL;
    }
    wake_result = trevrpc_engine_queue_event_locked(engine, stopped);
    if (wake_result != 0 && engine->terminal_status == 0) {
        engine->terminal_status = wake_result;
        stopped->status = wake_result;
        stopped->flags |= TREVRPC_ENGINE_EVENT_FLAG_FATAL;
    }
    engine->state = TREVRPC_ENGINE_STATE_STOPPED;
    pthread_cond_broadcast(&engine->condition);
}

static int trevrpc_engine_normalize_provider_result(int result) {
    return result > 0 ? -EIO : result;
}

static int trevrpc_engine_start_close(trevrpc_engine* engine, bool* call_provider) {
    int result = 0;
    *call_provider = false;
    pthread_mutex_lock(&engine->mutex);
    if (engine->state == TREVRPC_ENGINE_STATE_RUNNING) {
        engine->state = TREVRPC_ENGINE_STATE_STOPPING;
    }
    if (engine->state == TREVRPC_ENGINE_STATE_STOPPING && !engine->provider_close_started) {
        engine->provider_close_started = true;
        engine->provider_close_in_flight++;
        *call_provider = true;
    } else if (engine->state == TREVRPC_ENGINE_STATE_RELEASING) {
        result = -EPIPE;
    }
    pthread_mutex_unlock(&engine->mutex);
    return result;
}

static int trevrpc_engine_call_provider_close(trevrpc_engine* engine) {
    int result = trevrpc_engine_normalize_provider_result(engine->provider_ops.close(engine->provider_context));

    pthread_mutex_lock(&engine->mutex);
    if (result != 0) {
        (void)trevrpc_engine_record_failure_locked(engine, result, 0);
    }
    if (engine->provider_close_in_flight != 0) {
        engine->provider_close_in_flight--;
    }
    trevrpc_engine_maybe_publish_stopped_locked(engine);
    pthread_cond_broadcast(&engine->condition);
    pthread_mutex_unlock(&engine->mutex);
    return result;
}

static int trevrpc_engine_request_close(trevrpc_engine* engine) {
    bool call_provider;
    int result = trevrpc_engine_start_close(engine, &call_provider);
    if (result == 0 && call_provider) {
        result = trevrpc_engine_call_provider_close(engine);
    }
    return result;
}

static int trevrpc_engine_drain_wake_locked(trevrpc_engine* engine) {
    uint8_t buffer[128];
    ssize_t result;
    for (;;) {
        result = read(engine->wake_read_fd, buffer, sizeof(buffer));
        if (result > 0) {
            continue;
        }
        if (result < 0 && errno == EINTR) {
            continue;
        }
        if (result < 0 && errno == EAGAIN) {
            return 0;
        }
        trevrpc_engine_counter_increment(&engine->wake_failures);
        return result < 0 ? -errno : -EIO;
    }
}

static int trevrpc_engine_validate_handle(trevrpc_engine* engine, trevrpc_engine_handle_v1 handle) {
    if (handle.owner == 0 || handle.slot == 0 || handle.generation == 0) {
        return -EINVAL;
    }
    return handle.owner == engine->owner_cookie ? 0 : -ESTALE;
}

static int trevrpc_engine_validate_provider_handle(trevrpc_engine* engine, trevrpc_engine_handle_v1 handle) {
    return trevrpc_engine_validate_handle(engine, handle) == 0 ? 0 : -EIO;
}

static trevrpc_engine_reservation* trevrpc_engine_reserve_locked(trevrpc_engine* engine) {
    trevrpc_engine_reservation* reservation = calloc(1, sizeof(*reservation));
    if (reservation == NULL) {
        return NULL;
    }
    atomic_init(&reservation->references, 1u);
    atomic_init(&reservation->consumed, false);
    reservation->event = trevrpc_engine_event_allocate(0);
    if (reservation->event == NULL) {
        free(reservation);
        return NULL;
    }
    reservation->event->event_class = TREVRPC_ENGINE_EVENT_CLASS_MANDATORY;
    trevrpc_engine_counter_increment(&engine->mandatory_reservations);
    return reservation;
}

static void trevrpc_engine_reservation_retain(trevrpc_engine_reservation* reservation) {
    if (reservation != NULL) {
        atomic_fetch_add_explicit(&reservation->references, 1u, memory_order_relaxed);
    }
}

static void trevrpc_engine_reservation_release(trevrpc_engine_reservation* reservation) {
    if (reservation != NULL && atomic_fetch_sub_explicit(&reservation->references, 1u, memory_order_acq_rel) == 1u) {
        free(reservation);
    }
}

static bool trevrpc_engine_reservation_consume(trevrpc_engine_reservation* reservation) {
    return reservation != NULL && !atomic_exchange_explicit(&reservation->consumed, true, memory_order_acq_rel);
}

static void trevrpc_engine_destroy(trevrpc_engine* engine) {
    trevrpc_engine_event* event;
    while ((event = engine->event_head) != NULL) {
        engine->event_head = event->next;
        event->next = NULL;
        trevrpc_engine_event_destroy(event);
    }
    trevrpc_engine_event_destroy(engine->fatal_event);
    trevrpc_engine_event_destroy(engine->stopped_event);
    if (engine->provider_ops.destroy != NULL) {
        engine->provider_ops.destroy(engine->provider_context);
    }
    if (engine->wake_read_fd >= 0) {
        close(engine->wake_read_fd);
    }
    if (engine->wake_write_fd >= 0) {
        close(engine->wake_write_fd);
    }
    pthread_cond_destroy(&engine->test_condition);
    pthread_mutex_destroy(&engine->test_mutex);
    pthread_cond_destroy(&engine->lifetime_condition);
    pthread_mutex_destroy(&engine->lifetime_mutex);
    pthread_cond_destroy(&engine->condition);
    pthread_mutex_destroy(&engine->mutex);
    free(engine);
}

uint32_t trevrpc_engine_abi_version(void) {
    return TREVRPC_ENGINE_ABI_VERSION;
}

void trevrpc_engine_abi_1_anchor(void) {
}

int trevrpc_engine_config_v1_init(trevrpc_engine_config_v1* config, size_t struct_size) {
    int result = trevrpc_engine_initialize_structure(config, struct_size, sizeof(*config));
    if (result == 0) {
        config->event_capacity = TREVRPC_ENGINE_DEFAULT_EVENT_CAPACITY;
        config->listener_capacity = TREVRPC_ENGINE_DEFAULT_LISTENER_CAPACITY;
        config->connection_capacity = TREVRPC_ENGINE_DEFAULT_CONNECTION_CAPACITY;
        config->stream_capacity = TREVRPC_ENGINE_DEFAULT_STREAM_CAPACITY;
        config->max_receive_owned_count = TREVRPC_ENGINE_DEFAULT_MAX_RECEIVE_OWNED_COUNT;
        config->max_receive_owned_bytes = TREVRPC_ENGINE_DEFAULT_RECEIVE_OWNED_BYTES;
    }
    return result;
}

int trevrpc_engine_wake_source_v1_init(trevrpc_engine_wake_source_v1* wake_source, size_t struct_size) {
    return trevrpc_engine_initialize_structure(wake_source, struct_size, sizeof(*wake_source));
}

int trevrpc_engine_endpoint_config_v1_init(trevrpc_engine_endpoint_config_v1* config, size_t struct_size) {
    int result = trevrpc_engine_initialize_structure(config, struct_size, sizeof(*config));
    if (result == 0) {
        config->peer_bidi_stream_count = 100;
        config->max_pending_send_count = TREVRPC_ENGINE_DEFAULT_MAX_PENDING_SEND_COUNT;
        config->max_pending_send_bytes = TREVRPC_ENGINE_DEFAULT_MAX_PENDING_SEND_BYTES;
        config->max_frame_size = TREVRPC_ENGINE_DEFAULT_MAX_FRAME_SIZE;
    }
    return result;
}

int trevrpc_engine_event_info_v1_init(trevrpc_engine_event_info_v1* info, size_t struct_size) {
    return trevrpc_engine_initialize_structure(info, struct_size, sizeof(*info));
}

int trevrpc_engine_receive_info_v1_init(trevrpc_engine_receive_info_v1* info, size_t struct_size) {
    return trevrpc_engine_initialize_structure(info, struct_size, sizeof(*info));
}

int trevrpc_engine_diagnostics_v1_init(trevrpc_engine_diagnostics_v1* diagnostics, size_t struct_size) {
    return trevrpc_engine_initialize_structure(diagnostics, struct_size, sizeof(*diagnostics));
}

int trevrpc_engine_provider_create_v1(const trevrpc_engine_config_v1* config,
    const trevrpc_engine_provider_ops* provider_ops,
    void* provider_context,
    uint64_t owner_cookie,
    trevrpc_engine** out_engine) {
    trevrpc_engine* engine;
    int descriptors[2] = {-1, -1};
    int result;
    int pthread_result;
    bool mutex_initialized = false;
    bool condition_initialized = false;
    bool lifetime_mutex_initialized = false;
    bool lifetime_condition_initialized = false;
    bool test_mutex_initialized = false;
    bool test_condition_initialized = false;
    if (out_engine == NULL || provider_ops == NULL || provider_ops->close == NULL || provider_ops->destroy == NULL ||
        owner_cookie == 0) {
        return -EINVAL;
    }
    result = trevrpc_engine_validate_config(config);
    if (result != 0) {
        provider_ops->destroy(provider_context);
        return result;
    }
    engine = calloc(1, sizeof(*engine));
    if (engine == NULL) {
        provider_ops->destroy(provider_context);
        return -ENOMEM;
    }
    engine->wake_read_fd = -1;
    engine->wake_write_fd = -1;
    engine->fatal_event = trevrpc_engine_event_allocate(0);
    engine->stopped_event = trevrpc_engine_event_allocate(0);
    if (engine->fatal_event == NULL || engine->stopped_event == NULL) {
        result = -ENOMEM;
        goto fail;
    }
    engine->fatal_event->event_class = TREVRPC_ENGINE_EVENT_CLASS_FATAL;
    engine->stopped_event->event_class = TREVRPC_ENGINE_EVENT_CLASS_STOPPED;
    pthread_result = pthread_mutex_init(&engine->mutex, NULL);
    if (pthread_result != 0) {
        result = -pthread_result;
        goto fail;
    }
    mutex_initialized = true;
    pthread_result = pthread_cond_init(&engine->condition, NULL);
    if (pthread_result != 0) {
        result = -pthread_result;
        goto fail;
    }
    condition_initialized = true;
    pthread_result = pthread_mutex_init(&engine->lifetime_mutex, NULL);
    if (pthread_result != 0) {
        result = -pthread_result;
        goto fail;
    }
    lifetime_mutex_initialized = true;
    pthread_result = pthread_cond_init(&engine->lifetime_condition, NULL);
    if (pthread_result != 0) {
        result = -pthread_result;
        goto fail;
    }
    lifetime_condition_initialized = true;
    pthread_result = pthread_mutex_init(&engine->test_mutex, NULL);
    if (pthread_result != 0) {
        result = -pthread_result;
        goto fail;
    }
    test_mutex_initialized = true;
    pthread_result = pthread_cond_init(&engine->test_condition, NULL);
    if (pthread_result != 0) {
        result = -pthread_result;
        goto fail;
    }
    test_condition_initialized = true;
    result = trevrpc_engine_create_pipe(descriptors);
    if (result != 0) {
        goto fail;
    }
    engine->wake_read_fd = descriptors[0];
    engine->wake_write_fd = descriptors[1];
    descriptors[0] = -1;
    descriptors[1] = -1;
    engine->state = TREVRPC_ENGINE_STATE_RUNNING;
    engine->event_capacity = config->event_capacity;
    engine->next_sequence = 1;
    engine->provider_ops = *provider_ops;
    engine->provider_context = provider_context;
    engine->owner_cookie = owner_cookie;
    atomic_init(&engine->api_gate, 0);
    atomic_init(&engine->lifetime_gate, 0);
    atomic_init(&engine->release_waiting, false);
    atomic_init(&engine->test_pause_last_drain_active, false);
    atomic_init(&engine->test_pause_api_enter_active, false);
    atomic_init(&engine->test_pause_api_leave_active, false);
    if (engine->provider_ops.attach != NULL) {
        result = trevrpc_engine_normalize_provider_result(engine->provider_ops.attach(provider_context, engine));
        if (result != 0) {
            goto fail;
        }
    }
    *out_engine = engine;
    return 0;

fail:
    if (descriptors[0] >= 0) {
        close(descriptors[0]);
    }
    if (descriptors[1] >= 0) {
        close(descriptors[1]);
    }
    if (engine->wake_read_fd >= 0) {
        close(engine->wake_read_fd);
    }
    if (engine->wake_write_fd >= 0) {
        close(engine->wake_write_fd);
    }
    if (provider_ops->destroy != NULL) {
        provider_ops->destroy(provider_context);
    }
    if (test_condition_initialized) {
        pthread_cond_destroy(&engine->test_condition);
    }
    if (test_mutex_initialized) {
        pthread_mutex_destroy(&engine->test_mutex);
    }
    if (lifetime_condition_initialized) {
        pthread_cond_destroy(&engine->lifetime_condition);
    }
    if (lifetime_mutex_initialized) {
        pthread_mutex_destroy(&engine->lifetime_mutex);
    }
    if (condition_initialized) {
        pthread_cond_destroy(&engine->condition);
    }
    if (mutex_initialized) {
        pthread_mutex_destroy(&engine->mutex);
    }
    trevrpc_engine_event_destroy(engine->fatal_event);
    trevrpc_engine_event_destroy(engine->stopped_event);
    free(engine);
    return result;
}

int trevrpc_engine_provider_callback_enter(trevrpc_engine* engine) {
    int result;
    if (engine == NULL) {
        return -EINVAL;
    }
    result = trevrpc_engine_gate_enter(&engine->lifetime_gate);
    if (result != 0) {
        return result;
    }
    pthread_mutex_lock(&engine->mutex);
    if (engine->state >= TREVRPC_ENGINE_STATE_STOPPED) {
        pthread_mutex_unlock(&engine->mutex);
        trevrpc_engine_gate_leave(engine, &engine->lifetime_gate, false);
        return -EPIPE;
    }
    trevrpc_engine_counter_increment(&engine->active_callbacks);
    pthread_mutex_unlock(&engine->mutex);
    if (trevrpc_engine_callback_depth != 0 && trevrpc_engine_callback_context != engine) {
        pthread_mutex_lock(&engine->mutex);
        --engine->active_callbacks;
        trevrpc_engine_maybe_publish_stopped_locked(engine);
        pthread_cond_broadcast(&engine->condition);
        pthread_mutex_unlock(&engine->mutex);
        trevrpc_engine_gate_leave(engine, &engine->lifetime_gate, false);
        return -EDEADLK;
    }
    trevrpc_engine_callback_context = engine;
    ++trevrpc_engine_callback_depth;
    return 0;
}

void trevrpc_engine_provider_callback_leave(trevrpc_engine* engine) {
    if (engine == NULL || trevrpc_engine_callback_context != engine || trevrpc_engine_callback_depth == 0) {
        return;
    }
    --trevrpc_engine_callback_depth;
    if (trevrpc_engine_callback_depth == 0) {
        trevrpc_engine_callback_context = NULL;
    }
    pthread_mutex_lock(&engine->mutex);
    if (engine->active_callbacks != 0) {
        --engine->active_callbacks;
    }
    trevrpc_engine_maybe_publish_stopped_locked(engine);
    pthread_cond_broadcast(&engine->condition);
    pthread_mutex_unlock(&engine->mutex);
    trevrpc_engine_gate_leave(engine, &engine->lifetime_gate, false);
}

int trevrpc_engine_provider_operation_pin(trevrpc_engine* engine) {
    int result;
    if (engine == NULL) {
        return -EINVAL;
    }
    result = trevrpc_engine_gate_enter(&engine->lifetime_gate);
    if (result != 0) {
        return result;
    }
    pthread_mutex_lock(&engine->mutex);
    if (engine->state != TREVRPC_ENGINE_STATE_RUNNING) {
        pthread_mutex_unlock(&engine->mutex);
        trevrpc_engine_gate_leave(engine, &engine->lifetime_gate, false);
        return -EPIPE;
    }
    trevrpc_engine_counter_increment(&engine->active_operations);
    pthread_mutex_unlock(&engine->mutex);
    return 0;
}

void trevrpc_engine_provider_operation_unpin(trevrpc_engine* engine) {
    if (engine == NULL) {
        return;
    }
    pthread_mutex_lock(&engine->mutex);
    if (engine->active_operations != 0) {
        --engine->active_operations;
    }
    trevrpc_engine_maybe_publish_stopped_locked(engine);
    pthread_cond_broadcast(&engine->condition);
    pthread_mutex_unlock(&engine->mutex);
    trevrpc_engine_gate_leave(engine, &engine->lifetime_gate, false);
}

int trevrpc_engine_provider_reserve_mandatory(trevrpc_engine* engine, trevrpc_engine_reservation** out_reservation) {
    trevrpc_engine_reservation* reservation;
    if (engine == NULL || out_reservation == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&engine->mutex);
    if (engine->state >= TREVRPC_ENGINE_STATE_STOPPED) {
        pthread_mutex_unlock(&engine->mutex);
        return -EPIPE;
    }
    reservation = trevrpc_engine_reserve_locked(engine);
    pthread_mutex_unlock(&engine->mutex);
    if (reservation == NULL) {
        return -ENOMEM;
    }
    *out_reservation = reservation;
    return 0;
}

int trevrpc_engine_provider_publish_reserved(
    trevrpc_engine* engine, trevrpc_engine_reservation* reservation, const trevrpc_engine_event_spec* spec) {
    trevrpc_engine_event* event;
    bool abort_publication = false;
    bool call_provider = false;
    int result = 0;
    int wake_result;
    if (engine == NULL || reservation == NULL || spec == NULL) {
        if (spec != NULL && spec->mandatory_abort_hook != NULL) {
            spec->mandatory_abort_hook(engine != NULL ? engine->provider_context : NULL, spec->mandatory_hook_context);
        }
        if (engine != NULL && reservation != NULL) {
            trevrpc_engine_provider_cancel_reservation(engine, reservation);
        }
        return -EINVAL;
    }
    if (!trevrpc_engine_reservation_consume(reservation)) {
        if (spec->mandatory_abort_hook != NULL) {
            spec->mandatory_abort_hook(engine->provider_context, spec->mandatory_hook_context);
        }
        return -EALREADY;
    }
    event = reservation->event;
    reservation->event = NULL;
    if (spec->data_len != 0 || spec->data != NULL) {
        result = -EMSGSIZE;
    } else {
        event->kind = spec->kind;
        event->flags = spec->flags;
        event->status = spec->status;
        event->subject_kind = spec->subject_kind;
        event->subject = spec->subject;
        event->parent = spec->parent;
        event->operation_id = spec->operation_id;
        event->application_error_code = spec->application_error_code;
        event->provider_error_code = spec->provider_error_code;
        event->dequeue_hook = spec->dequeue_hook;
        event->drop_hook = spec->drop_hook;
        event->provider_context = engine->provider_context;
        event->hook_context = spec->hook_context;
    }
    pthread_mutex_lock(&engine->mutex);
    if (engine->mandatory_reservations != 0) {
        --engine->mandatory_reservations;
    }
    if (result == 0 && engine->state < TREVRPC_ENGINE_STATE_STOPPED) {
        wake_result = trevrpc_engine_queue_event_locked(engine, event);
        event = NULL;
        if (spec->mandatory_commit_hook != NULL) {
            spec->mandatory_commit_hook(engine->provider_context, spec->mandatory_hook_context);
        }
        if (wake_result != 0) {
            call_provider = trevrpc_engine_record_failure_locked(engine, wake_result, spec->provider_error_code);
        }
    } else {
        if (result == 0) {
            result = -EPIPE;
        }
        abort_publication = true;
    }
    trevrpc_engine_maybe_publish_stopped_locked(engine);
    pthread_cond_broadcast(&engine->condition);
    pthread_mutex_unlock(&engine->mutex);
    if (abort_publication && spec->mandatory_abort_hook != NULL) {
        spec->mandatory_abort_hook(engine->provider_context, spec->mandatory_hook_context);
    }
    trevrpc_engine_event_destroy(event);
    trevrpc_engine_reservation_release(reservation);
    if (call_provider) {
        (void)trevrpc_engine_call_provider_close(engine);
    }
    return result;
}

void trevrpc_engine_provider_cancel_reservation(trevrpc_engine* engine, trevrpc_engine_reservation* reservation) {
    trevrpc_engine_event* event;
    if (engine == NULL || reservation == NULL || !trevrpc_engine_reservation_consume(reservation)) {
        return;
    }
    event = reservation->event;
    reservation->event = NULL;
    pthread_mutex_lock(&engine->mutex);
    if (engine->mandatory_reservations != 0) {
        --engine->mandatory_reservations;
    }
    trevrpc_engine_maybe_publish_stopped_locked(engine);
    pthread_cond_broadcast(&engine->condition);
    pthread_mutex_unlock(&engine->mutex);
    trevrpc_engine_event_destroy(event);
    trevrpc_engine_reservation_release(reservation);
}

int trevrpc_engine_provider_publish_event(trevrpc_engine* engine, const trevrpc_engine_event_spec* spec) {
    trevrpc_engine_event* event;
    bool call_provider = false;
    int result = 0;
    int wake_result;
    if (engine == NULL || spec == NULL || (spec->data == NULL && spec->data_len != 0)) {
        return -EINVAL;
    }
    pthread_mutex_lock(&engine->mutex);
    if (engine->state >= TREVRPC_ENGINE_STATE_STOPPED) {
        result = -EPIPE;
        trevrpc_engine_counter_increment(&engine->events_rejected);
    } else if (engine->ordinary_queue_depth + engine->ordinary_event_reservations >= engine->event_capacity) {
        result = -ENOSPC;
        trevrpc_engine_counter_increment(&engine->events_rejected);
        call_provider = trevrpc_engine_record_failure_locked(engine, result, spec->provider_error_code);
    } else {
        engine->ordinary_event_reservations++;
    }
    pthread_mutex_unlock(&engine->mutex);
    if (result != 0) {
        if (spec->drop_hook != NULL) {
            spec->drop_hook(engine->provider_context, spec->hook_context);
        }
        if (call_provider) {
            (void)trevrpc_engine_call_provider_close(engine);
        }
        return result;
    }

    event = trevrpc_engine_event_from_spec(spec, TREVRPC_ENGINE_EVENT_CLASS_ORDINARY);
    pthread_mutex_lock(&engine->mutex);
    engine->ordinary_event_reservations--;
    if (event == NULL) {
        result = -ENOMEM;
    } else if (engine->state >= TREVRPC_ENGINE_STATE_STOPPED) {
        result = -EPIPE;
        trevrpc_engine_counter_increment(&engine->events_rejected);
    } else {
        wake_result = trevrpc_engine_queue_event_locked(engine, event);
        event = NULL;
        if (wake_result != 0) {
            call_provider = trevrpc_engine_record_failure_locked(engine, wake_result, spec->provider_error_code);
        }
    }
    pthread_mutex_unlock(&engine->mutex);
    if (event == NULL && result == -ENOMEM && spec->drop_hook != NULL) {
        spec->drop_hook(engine->provider_context, spec->hook_context);
    }
    trevrpc_engine_event_destroy(event);
    if (call_provider) {
        (void)trevrpc_engine_call_provider_close(engine);
    }
    return result;
}

void trevrpc_engine_provider_fail(trevrpc_engine* engine, int32_t status, uint64_t provider_error_code) {
    bool call_provider;
    if (engine == NULL) {
        return;
    }
    pthread_mutex_lock(&engine->mutex);
    call_provider = trevrpc_engine_record_failure_locked(engine, status, provider_error_code);
    pthread_mutex_unlock(&engine->mutex);
    if (call_provider) {
        (void)trevrpc_engine_call_provider_close(engine);
    }
}

void trevrpc_engine_provider_stopped(trevrpc_engine* engine, int32_t status, uint64_t provider_error_code) {
    if (engine == NULL) {
        return;
    }
    pthread_mutex_lock(&engine->mutex);
    engine->provider_close_started = true;
    if (status != 0) {
        (void)trevrpc_engine_record_failure_locked(engine, status, provider_error_code);
    } else if (engine->state == TREVRPC_ENGINE_STATE_RUNNING) {
        engine->state = TREVRPC_ENGINE_STATE_STOPPING;
    }
    engine->provider_stop_reported = true;
    trevrpc_engine_maybe_publish_stopped_locked(engine);
    pthread_cond_broadcast(&engine->condition);
    pthread_mutex_unlock(&engine->mutex);
}

int trevrpc_engine_receive_create_copy(
    const void* data, size_t data_len, uint32_t flags, trevrpc_engine_receive** out_receive) {
    trevrpc_engine_receive* receive;
    if (out_receive == NULL || (data == NULL && data_len != 0) || data_len > SIZE_MAX - sizeof(*receive)) {
        return -EINVAL;
    }
    receive = calloc(1, sizeof(*receive) + data_len);
    if (receive == NULL) {
        return -ENOMEM;
    }
    receive->flags = flags;
    receive->data_len = data_len;
    if (data_len != 0) {
        memcpy(receive->copied_data, data, data_len);
        receive->data = receive->copied_data;
    }
    *out_receive = receive;
    return 0;
}

int trevrpc_engine_receive_create_owned(void* data,
    size_t data_len,
    uint32_t flags,
    void* owner,
    trevrpc_engine_owned_release release,
    void* release_context,
    trevrpc_engine_receive** out_receive) {
    trevrpc_engine_receive* receive;
    if (out_receive == NULL || (data == NULL && data_len != 0) || release == NULL) {
        return -EINVAL;
    }
    receive = calloc(1, sizeof(*receive));
    if (receive == NULL) {
        return -ENOMEM;
    }
    receive->flags = flags;
    receive->data = data;
    receive->data_len = data_len;
    receive->owner = owner;
    receive->release = release;
    receive->release_context = release_context;
    *out_receive = receive;
    return 0;
}

uint64_t trevrpc_engine_provider_owner_cookie(const trevrpc_engine* engine) {
    return engine == NULL ? 0 : engine->owner_cookie;
}

void* trevrpc_engine_provider_context(const trevrpc_engine* engine) {
    return engine == NULL ? NULL : engine->provider_context;
}

int trevrpc_engine_get_wake_source_v1(trevrpc_engine* engine, trevrpc_engine_wake_source_v1* wake_source) {
    trevrpc_engine_wake_source_v1 output;
    uint32_t supplied_size;
    int result = trevrpc_engine_validate_wake_output(wake_source);
    if (result != 0) {
        return result;
    }
    supplied_size = wake_source->struct_size;
    result = trevrpc_engine_api_enter(engine);
    if (result != 0) {
        return result;
    }
    memset(&output, 0, sizeof(output));
    output.struct_size = supplied_size;
    output.struct_version = TREVRPC_ENGINE_STRUCT_VERSION_1;
    output.kind = TREVRPC_ENGINE_WAKE_SOURCE_POSIX_FD;
    output.flags = TREVRPC_ENGINE_WAKE_FLAG_BORROWED | TREVRPC_ENGINE_WAKE_FLAG_LEVEL_TRIGGERED;
    output.native_handle = engine->wake_read_fd;
    memcpy(wake_source, &output, sizeof(output));
    trevrpc_engine_api_leave(engine);
    return 0;
}

int trevrpc_engine_next_event(trevrpc_engine* engine, trevrpc_engine_event** out_event) {
    trevrpc_engine_event* event;
    int result;
    int drain_result = 0;
    bool call_provider = false;
    if (out_event == NULL) {
        return -EINVAL;
    }
    result = trevrpc_engine_api_enter(engine);
    if (result != 0) {
        return result;
    }
    if (engine->event_head == NULL) {
        trevrpc_engine_api_leave(engine);
        return -EAGAIN;
    }
    event = engine->event_head;
    engine->event_head = event->next;
    event->next = NULL;
    if (engine->event_head == NULL) {
        engine->event_tail = NULL;
    }
    --engine->queue_depth;
    if (event->event_class == TREVRPC_ENGINE_EVENT_CLASS_ORDINARY) {
        --engine->ordinary_queue_depth;
    }
    if (engine->queue_depth == 0 && engine->wake_armed) {
        if (atomic_load_explicit(&engine->test_pause_last_drain_active, memory_order_acquire)) {
            pthread_mutex_lock(&engine->test_mutex);
            if (engine->test_pause_last_drain) {
                engine->test_in_last_drain = true;
                pthread_cond_broadcast(&engine->test_condition);
                while (engine->test_pause_last_drain) {
                    pthread_cond_wait(&engine->test_condition, &engine->test_mutex);
                }
                engine->test_in_last_drain = false;
            }
            pthread_mutex_unlock(&engine->test_mutex);
        }
        drain_result = trevrpc_engine_drain_wake_locked(engine);
        engine->wake_armed = false;
        if (drain_result != 0) {
            if (event->event_class == TREVRPC_ENGINE_EVENT_CLASS_STOPPED && engine->fatal_event != NULL) {
                trevrpc_engine_event* stopped = event;
                trevrpc_engine_event* fatal = engine->fatal_event;
                engine->fatal_event = NULL;
                engine->terminal_status = drain_result;
                fatal->kind = TREVRPC_ENGINE_EVENT_DIAGNOSTIC;
                fatal->flags = TREVRPC_ENGINE_EVENT_FLAG_FATAL;
                fatal->status = drain_result;
                fatal->sequence = stopped->sequence;
                stopped->flags |= TREVRPC_ENGINE_EVENT_FLAG_FATAL;
                stopped->status = drain_result;
                stopped->sequence = engine->next_sequence++;
                trevrpc_engine_counter_increment(&engine->events_enqueued);
                engine->event_head = stopped;
                engine->event_tail = stopped;
                engine->queue_depth = 1;
                event = fatal;
            } else {
                call_provider = trevrpc_engine_record_failure_locked(engine, drain_result, 0);
            }
        }
    }
    trevrpc_engine_counter_increment(&engine->events_dequeued);
    pthread_mutex_unlock(&engine->mutex);
    if (event->dequeue_hook != NULL) {
        event->dequeue_hook(event->provider_context, event->hook_context);
    }
    event->dequeue_hook = NULL;
    event->drop_hook = NULL;
    event->provider_context = NULL;
    event->hook_context = NULL;
    *out_event = event;
    if (call_provider) {
        (void)trevrpc_engine_call_provider_close(engine);
    }
    trevrpc_engine_api_leave_unlocked(engine);
    return 0;
}

int trevrpc_engine_event_get_info_v1(const trevrpc_engine_event* event, trevrpc_engine_event_info_v1* info) {
    trevrpc_engine_event_info_v1 output;
    uint32_t supplied_size;
    int result = trevrpc_engine_validate_event_info_output(info);
    if (result != 0) {
        return result;
    }
    if (event == NULL) {
        return -EINVAL;
    }
    supplied_size = info->struct_size;
    memset(&output, 0, sizeof(output));
    output.struct_size = supplied_size;
    output.struct_version = TREVRPC_ENGINE_STRUCT_VERSION_1;
    output.kind = event->kind;
    output.flags = event->flags;
    output.status = event->status;
    output.subject_kind = event->subject_kind;
    output.sequence = event->sequence;
    output.subject = event->subject;
    output.parent = event->parent;
    output.operation_id = event->operation_id;
    output.application_error_code = event->application_error_code;
    output.provider_error_code = event->provider_error_code;
    output.data = event->data;
    output.data_len = event->data_len;
    memcpy(info, &output, sizeof(output));
    return 0;
}

void trevrpc_engine_event_release(trevrpc_engine_event* event) {
    trevrpc_engine_event_destroy(event);
}

int trevrpc_engine_receive_get_info_v1(const trevrpc_engine_receive* receive, trevrpc_engine_receive_info_v1* info) {
    trevrpc_engine_receive_info_v1 output;
    uint32_t supplied_size;
    int result = trevrpc_engine_validate_receive_info_output(info);
    if (result != 0) {
        return result;
    }
    if (receive == NULL) {
        return -EINVAL;
    }
    supplied_size = info->struct_size;
    memset(&output, 0, sizeof(output));
    output.struct_size = supplied_size;
    output.struct_version = TREVRPC_ENGINE_STRUCT_VERSION_1;
    output.flags = receive->flags;
    output.data = receive->data;
    output.data_len = receive->data_len;
    memcpy(info, &output, sizeof(output));
    return 0;
}

void trevrpc_engine_receive_release(trevrpc_engine_receive* receive) {
    if (receive == NULL) {
        return;
    }
    if (receive->release != NULL) {
        receive->release(receive->owner, receive->release_context);
    }
    free(receive);
}

int trevrpc_engine_get_diagnostics_v1(trevrpc_engine* engine, trevrpc_engine_diagnostics_v1* diagnostics) {
    trevrpc_engine_diagnostics_v1 output;
    trevrpc_engine_provider_diagnostics provider = {0};
    uint32_t supplied_size;
    int result = trevrpc_engine_validate_diagnostics_output(diagnostics);
    if (result != 0) {
        return result;
    }
    supplied_size = diagnostics->struct_size;
    result = trevrpc_engine_api_enter(engine);
    if (result != 0) {
        return result;
    }
    memset(&output, 0, sizeof(output));
    output.struct_size = supplied_size;
    output.struct_version = TREVRPC_ENGINE_STRUCT_VERSION_1;
    output.engine_abi_version = TREVRPC_ENGINE_ABI_VERSION;
    output.state = engine->state;
    output.terminal_status = engine->terminal_status;
    output.event_capacity = engine->event_capacity;
    output.queue_depth = engine->queue_depth;
    output.ordinary_queue_depth = engine->ordinary_queue_depth;
    output.events_enqueued = engine->events_enqueued;
    output.events_dequeued = engine->events_dequeued;
    output.events_rejected = engine->events_rejected;
    output.active_callbacks = engine->active_callbacks;
    output.active_api_calls = atomic_load_explicit(&engine->api_gate, memory_order_acquire) >> 1u;
    output.wake_signals = engine->wake_signals;
    output.wake_write_eagain = engine->wake_write_eagain;
    output.wake_failures = engine->wake_failures;
    output.provider_error_code = engine->provider_error_code;
    output.mandatory_reservations = engine->mandatory_reservations;
    pthread_mutex_unlock(&engine->mutex);
    if (engine->provider_ops.get_diagnostics != NULL) {
        engine->provider_ops.get_diagnostics(engine->provider_context, &provider);
    }
    output.receive_owned_count = provider.receive_owned_count;
    output.peak_receive_owned_count = provider.peak_receive_owned_count;
    output.receive_owned_bytes = provider.receive_owned_bytes;
    output.peak_receive_owned_bytes = provider.peak_receive_owned_bytes;
    output.pending_send_bytes = provider.pending_send_bytes;
    output.pending_send_count = provider.pending_send_count;
    output.live_listeners = provider.live_listeners;
    output.live_connections = provider.live_connections;
    output.live_streams = provider.live_streams;
    memcpy(diagnostics, &output, sizeof(output));
    trevrpc_engine_api_leave_unlocked(engine);
    return 0;
}

static int trevrpc_engine_prepare_command(trevrpc_engine* engine) {
    int result = trevrpc_engine_api_enter(engine);
    if (result != 0) {
        return result;
    }
    if (engine->state != TREVRPC_ENGINE_STATE_RUNNING) {
        trevrpc_engine_api_leave(engine);
        return -EPIPE;
    }
    pthread_mutex_unlock(&engine->mutex);
    return 0;
}

int trevrpc_engine_listen_v1(
    trevrpc_engine* engine, const trevrpc_engine_endpoint_config_v1* config, trevrpc_engine_handle_v1* out_listener) {
    trevrpc_engine_handle_v1 output = {0};
    trevrpc_engine_reservation* terminal = NULL;
    int (*operation)(
        void*, const trevrpc_engine_endpoint_config_v1*, trevrpc_engine_reservation*, trevrpc_engine_handle_v1*);
    int result = trevrpc_engine_validate_endpoint_config(config);
    if (result != 0 || out_listener == NULL) {
        return result != 0 ? result : -EINVAL;
    }
    result = trevrpc_engine_prepare_command(engine);
    if (result != 0) {
        return result;
    }
    operation = engine->provider_ops.listen;
    if (operation == NULL) {
        trevrpc_engine_api_leave_unlocked(engine);
        return -ENOTSUP;
    }
    result = trevrpc_engine_provider_reserve_mandatory(engine, &terminal);
    if (result == 0) {
        trevrpc_engine_reservation_retain(terminal);
        result =
            trevrpc_engine_normalize_provider_result(operation(engine->provider_context, config, terminal, &output));
        if (result != 0) {
            trevrpc_engine_provider_cancel_reservation(engine, terminal);
        } else {
            result = trevrpc_engine_validate_provider_handle(engine, output);
            if (result == 0) {
                *out_listener = output;
            } else {
                trevrpc_engine_provider_cancel_reservation(engine, terminal);
                trevrpc_engine_provider_fail(engine, result, 0);
            }
        }
        trevrpc_engine_reservation_release(terminal);
    }
    trevrpc_engine_api_leave_unlocked(engine);
    return result;
}

int trevrpc_engine_listener_get_port_v1(trevrpc_engine* engine, trevrpc_engine_handle_v1 listener, uint16_t* out_port) {
    int (*operation)(void*, trevrpc_engine_handle_v1, uint16_t*);
    uint16_t output = 0;
    int result;
    if (out_port == NULL) {
        return -EINVAL;
    }
    result = trevrpc_engine_prepare_command(engine);
    if (result != 0) {
        return result;
    }
    operation = engine->provider_ops.listener_get_port;
    if (operation == NULL) {
        trevrpc_engine_api_leave_unlocked(engine);
        return -ENOTSUP;
    }
    result = trevrpc_engine_validate_handle(engine, listener);
    if (result == 0) {
        result = trevrpc_engine_normalize_provider_result(operation(engine->provider_context, listener, &output));
    }
    if (result == 0) {
        *out_port = output;
    }
    trevrpc_engine_api_leave_unlocked(engine);
    return result;
}

int trevrpc_engine_dial_v1(trevrpc_engine* engine,
    const trevrpc_engine_endpoint_config_v1* config,
    uint64_t operation_id,
    trevrpc_engine_handle_v1* out_connection) {
    trevrpc_engine_handle_v1 output = {0};
    trevrpc_engine_reservation* completion = NULL;
    trevrpc_engine_reservation* terminal = NULL;
    int (*operation)(void*,
        const trevrpc_engine_endpoint_config_v1*,
        uint64_t,
        trevrpc_engine_reservation*,
        trevrpc_engine_reservation*,
        trevrpc_engine_handle_v1*);
    int result = trevrpc_engine_validate_endpoint_config(config);
    if (result != 0 || operation_id == 0 || out_connection == NULL) {
        return result != 0 ? result : -EINVAL;
    }
    result = trevrpc_engine_prepare_command(engine);
    if (result != 0) {
        return result;
    }
    operation = engine->provider_ops.dial;
    if (operation == NULL) {
        trevrpc_engine_api_leave_unlocked(engine);
        return -ENOTSUP;
    }
    result = trevrpc_engine_provider_reserve_mandatory(engine, &completion);
    if (result == 0) {
        result = trevrpc_engine_provider_reserve_mandatory(engine, &terminal);
    }
    if (result == 0) {
        trevrpc_engine_reservation_retain(completion);
        trevrpc_engine_reservation_retain(terminal);
        result = trevrpc_engine_normalize_provider_result(
            operation(engine->provider_context, config, operation_id, completion, terminal, &output));
        if (result != 0) {
            trevrpc_engine_provider_cancel_reservation(engine, completion);
            trevrpc_engine_provider_cancel_reservation(engine, terminal);
        } else {
            result = trevrpc_engine_validate_provider_handle(engine, output);
            if (result == 0) {
                *out_connection = output;
            } else {
                trevrpc_engine_provider_cancel_reservation(engine, completion);
                trevrpc_engine_provider_cancel_reservation(engine, terminal);
                trevrpc_engine_provider_fail(engine, result, 0);
            }
        }
        trevrpc_engine_reservation_release(completion);
        trevrpc_engine_reservation_release(terminal);
    } else {
        trevrpc_engine_provider_cancel_reservation(engine, completion);
        trevrpc_engine_provider_cancel_reservation(engine, terminal);
    }
    trevrpc_engine_api_leave_unlocked(engine);
    return result;
}

#define TREVRPC_ENGINE_SIMPLE_HANDLE_COMMAND(name, member)                                                             \
    int name(trevrpc_engine* engine, trevrpc_engine_handle_v1 handle) {                                                \
        int (*operation)(void*, trevrpc_engine_handle_v1);                                                             \
        int result = trevrpc_engine_prepare_command(engine);                                                           \
        if (result == 0) {                                                                                             \
            operation = engine->provider_ops.member;                                                                   \
            if (operation == NULL) {                                                                                   \
                trevrpc_engine_api_leave_unlocked(engine);                                                             \
                return -ENOTSUP;                                                                                       \
            }                                                                                                          \
            result = trevrpc_engine_validate_handle(engine, handle);                                                   \
            if (result == 0) {                                                                                         \
                result = trevrpc_engine_normalize_provider_result(operation(engine->provider_context, handle));        \
            }                                                                                                          \
            trevrpc_engine_api_leave_unlocked(engine);                                                                 \
        }                                                                                                              \
        return result;                                                                                                 \
    }

TREVRPC_ENGINE_SIMPLE_HANDLE_COMMAND(trevrpc_engine_dial_cancel, dial_cancel)
TREVRPC_ENGINE_SIMPLE_HANDLE_COMMAND(trevrpc_engine_stream_finish_send, stream_finish_send)
TREVRPC_ENGINE_SIMPLE_HANDLE_COMMAND(trevrpc_engine_stream_close, stream_close)
TREVRPC_ENGINE_SIMPLE_HANDLE_COMMAND(trevrpc_engine_listener_close, listener_close)

int trevrpc_engine_connection_open_bidi_stream_v1(trevrpc_engine* engine,
    trevrpc_engine_handle_v1 connection,
    uint64_t operation_id,
    trevrpc_engine_handle_v1* out_stream) {
    trevrpc_engine_handle_v1 output = {0};
    trevrpc_engine_reservation* completion = NULL;
    trevrpc_engine_reservation* terminal = NULL;
    int (*operation)(void*,
        trevrpc_engine_handle_v1,
        uint64_t,
        trevrpc_engine_reservation*,
        trevrpc_engine_reservation*,
        trevrpc_engine_handle_v1*);
    int result;
    if (operation_id == 0 || out_stream == NULL) {
        return -EINVAL;
    }
    result = trevrpc_engine_prepare_command(engine);
    if (result != 0) {
        return result;
    }
    operation = engine->provider_ops.connection_open_bidi_stream;
    if (operation == NULL) {
        trevrpc_engine_api_leave_unlocked(engine);
        return -ENOTSUP;
    }
    result = trevrpc_engine_validate_handle(engine, connection);
    if (result == 0) {
        result = trevrpc_engine_provider_reserve_mandatory(engine, &completion);
    }
    if (result == 0) {
        result = trevrpc_engine_provider_reserve_mandatory(engine, &terminal);
    }
    if (result == 0) {
        trevrpc_engine_reservation_retain(completion);
        trevrpc_engine_reservation_retain(terminal);
        result = trevrpc_engine_normalize_provider_result(
            operation(engine->provider_context, connection, operation_id, completion, terminal, &output));
        if (result != 0) {
            trevrpc_engine_provider_cancel_reservation(engine, completion);
            trevrpc_engine_provider_cancel_reservation(engine, terminal);
        } else {
            result = trevrpc_engine_validate_provider_handle(engine, output);
            if (result == 0) {
                *out_stream = output;
            } else {
                trevrpc_engine_provider_cancel_reservation(engine, completion);
                trevrpc_engine_provider_cancel_reservation(engine, terminal);
                trevrpc_engine_provider_fail(engine, result, 0);
            }
        }
        trevrpc_engine_reservation_release(completion);
        trevrpc_engine_reservation_release(terminal);
    } else {
        trevrpc_engine_provider_cancel_reservation(engine, completion);
        trevrpc_engine_provider_cancel_reservation(engine, terminal);
    }
    trevrpc_engine_api_leave_unlocked(engine);
    return result;
}

int trevrpc_engine_stream_send_frame_v1(trevrpc_engine* engine,
    trevrpc_engine_handle_v1 stream,
    uint64_t operation_id,
    const uint8_t* body,
    size_t body_len) {
    trevrpc_engine_reservation* completion = NULL;
    int (*operation)(void*, trevrpc_engine_handle_v1, uint64_t, const uint8_t*, size_t, trevrpc_engine_reservation*);
    int result;
    if (operation_id == 0 || (body == NULL && body_len != 0) || body_len > UINT32_MAX - 4u) {
        return -EINVAL;
    }
    result = trevrpc_engine_prepare_command(engine);
    if (result != 0) {
        return result;
    }
    operation = engine->provider_ops.stream_send_frame;
    if (operation == NULL) {
        trevrpc_engine_api_leave_unlocked(engine);
        return -ENOTSUP;
    }
    result = trevrpc_engine_validate_handle(engine, stream);
    if (result == 0) {
        result = trevrpc_engine_provider_reserve_mandatory(engine, &completion);
    }
    if (result == 0) {
        trevrpc_engine_reservation_retain(completion);
        result = trevrpc_engine_normalize_provider_result(
            operation(engine->provider_context, stream, operation_id, body, body_len, completion));
        if (result != 0) {
            trevrpc_engine_provider_cancel_reservation(engine, completion);
        }
        trevrpc_engine_reservation_release(completion);
    } else {
        trevrpc_engine_provider_cancel_reservation(engine, completion);
    }
    trevrpc_engine_api_leave_unlocked(engine);
    return result;
}

int trevrpc_engine_stream_receive_frame(
    trevrpc_engine* engine, trevrpc_engine_handle_v1 stream, trevrpc_engine_receive** out_receive) {
    int (*operation)(void*, trevrpc_engine_handle_v1, trevrpc_engine_receive**);
    trevrpc_engine_receive* output = NULL;
    int result;
    if (out_receive == NULL) {
        return -EINVAL;
    }
    result = trevrpc_engine_prepare_command(engine);
    if (result != 0) {
        return result;
    }
    operation = engine->provider_ops.stream_receive_frame;
    if (operation == NULL) {
        trevrpc_engine_api_leave_unlocked(engine);
        return -ENOTSUP;
    }
    result = trevrpc_engine_validate_handle(engine, stream);
    if (result == 0) {
        result = trevrpc_engine_normalize_provider_result(operation(engine->provider_context, stream, &output));
    }
    if (result == 0 && output == NULL) {
        result = -EIO;
        trevrpc_engine_provider_fail(engine, result, 0);
    }
    if (result == 0) {
        *out_receive = output;
    }
    trevrpc_engine_api_leave_unlocked(engine);
    return result;
}

#define TREVRPC_ENGINE_ERROR_HANDLE_COMMAND(name, member)                                                              \
    int name(trevrpc_engine* engine, trevrpc_engine_handle_v1 handle, uint64_t application_error_code) {               \
        int (*operation)(void*, trevrpc_engine_handle_v1, uint64_t);                                                   \
        int result = trevrpc_engine_prepare_command(engine);                                                           \
        if (result == 0) {                                                                                             \
            operation = engine->provider_ops.member;                                                                   \
            if (operation == NULL) {                                                                                   \
                trevrpc_engine_api_leave_unlocked(engine);                                                             \
                return -ENOTSUP;                                                                                       \
            }                                                                                                          \
            result = trevrpc_engine_validate_handle(engine, handle);                                                   \
            if (result == 0) {                                                                                         \
                result = trevrpc_engine_normalize_provider_result(                                                     \
                    operation(engine->provider_context, handle, application_error_code));                              \
            }                                                                                                          \
            trevrpc_engine_api_leave_unlocked(engine);                                                                 \
        }                                                                                                              \
        return result;                                                                                                 \
    }

TREVRPC_ENGINE_ERROR_HANDLE_COMMAND(trevrpc_engine_stream_abort_receive, stream_abort_receive)
TREVRPC_ENGINE_ERROR_HANDLE_COMMAND(trevrpc_engine_stream_abort_send, stream_abort_send)
TREVRPC_ENGINE_ERROR_HANDLE_COMMAND(trevrpc_engine_stream_abort, stream_abort)

int trevrpc_engine_connection_close(
    trevrpc_engine* engine, trevrpc_engine_handle_v1 connection, uint64_t application_error_code) {
    int (*operation)(void*, trevrpc_engine_handle_v1, uint64_t);
    int result = trevrpc_engine_prepare_command(engine);
    if (result == 0) {
        operation = engine->provider_ops.connection_close;
        if (operation == NULL) {
            trevrpc_engine_api_leave_unlocked(engine);
            return -ENOTSUP;
        }
        result = trevrpc_engine_validate_handle(engine, connection);
        if (result == 0) {
            result = trevrpc_engine_normalize_provider_result(
                operation(engine->provider_context, connection, application_error_code));
        }
        trevrpc_engine_api_leave_unlocked(engine);
    }
    return result;
}

int trevrpc_engine_close(trevrpc_engine* engine) {
    int result = trevrpc_engine_api_enter(engine);
    if (result != 0) {
        return result;
    }
    pthread_mutex_unlock(&engine->mutex);
    result = trevrpc_engine_request_close(engine);
    trevrpc_engine_api_leave_unlocked(engine);
    return result;
}

int trevrpc_engine_drain(trevrpc_engine* engine) {
    int result;
    if (engine != NULL && trevrpc_engine_callback_context == engine && trevrpc_engine_callback_depth != 0) {
        return -EDEADLK;
    }
    result = trevrpc_engine_api_enter(engine);
    if (result != 0) {
        return result;
    }
    pthread_mutex_unlock(&engine->mutex);
    result = trevrpc_engine_request_close(engine);
    pthread_mutex_lock(&engine->mutex);
    while (result == 0 && engine->state < TREVRPC_ENGINE_STATE_STOPPED) {
        pthread_cond_wait(&engine->condition, &engine->mutex);
    }
    pthread_mutex_unlock(&engine->mutex);
    trevrpc_engine_api_leave_unlocked(engine);
    return result;
}

int trevrpc_engine_release(trevrpc_engine* engine) {
    uint_fast64_t gate;
    int result;
    if (engine == NULL) {
        return 0;
    }
    if (trevrpc_engine_callback_context == engine && trevrpc_engine_callback_depth != 0) {
        return -EDEADLK;
    }
    gate = atomic_fetch_or_explicit(&engine->api_gate, TREVRPC_ENGINE_GATE_CLOSED, memory_order_acq_rel);
    if ((gate & TREVRPC_ENGINE_GATE_CLOSED) != 0) {
        return -EPIPE;
    }
    atomic_store_explicit(&engine->release_waiting, true, memory_order_release);
    result = trevrpc_engine_request_close(engine);
    pthread_mutex_lock(&engine->mutex);
    while (engine->state < TREVRPC_ENGINE_STATE_STOPPED) {
        pthread_cond_wait(&engine->condition, &engine->mutex);
    }
    engine->state = TREVRPC_ENGINE_STATE_RELEASING;
    pthread_mutex_unlock(&engine->mutex);
    atomic_fetch_or_explicit(&engine->lifetime_gate, TREVRPC_ENGINE_GATE_CLOSED, memory_order_acq_rel);
    pthread_mutex_lock(&engine->lifetime_mutex);
    while ((atomic_load_explicit(&engine->api_gate, memory_order_acquire) >> 1u) != 0 ||
           (atomic_load_explicit(&engine->lifetime_gate, memory_order_acquire) >> 1u) != 0) {
        pthread_cond_wait(&engine->lifetime_condition, &engine->lifetime_mutex);
    }
    pthread_mutex_unlock(&engine->lifetime_mutex);
    atomic_store_explicit(&engine->release_waiting, false, memory_order_release);
    trevrpc_engine_destroy(engine);
    return result;
}

int trevrpc_engine_internal_test_producer_acquire(trevrpc_engine* engine) {
    return trevrpc_engine_provider_operation_pin(engine);
}

void trevrpc_engine_internal_test_producer_release(trevrpc_engine* engine) {
    trevrpc_engine_provider_operation_unpin(engine);
}

void trevrpc_engine_internal_test_pause_last_event_drain(trevrpc_engine* engine) {
    if (engine == NULL) {
        return;
    }
    pthread_mutex_lock(&engine->test_mutex);
    if (!engine->test_pause_last_drain) {
        engine->test_pause_last_drain = true;
        atomic_store_explicit(&engine->test_pause_last_drain_active, true, memory_order_release);
    } else {
        while (!engine->test_in_last_drain) {
            pthread_cond_wait(&engine->test_condition, &engine->test_mutex);
        }
    }
    pthread_mutex_unlock(&engine->test_mutex);
}

void trevrpc_engine_internal_test_resume_last_event_drain(trevrpc_engine* engine) {
    if (engine == NULL) {
        return;
    }
    pthread_mutex_lock(&engine->test_mutex);
    while (engine->test_pause_last_drain && !engine->test_in_last_drain) {
        pthread_cond_wait(&engine->test_condition, &engine->test_mutex);
    }
    engine->test_pause_last_drain = false;
    atomic_store_explicit(&engine->test_pause_last_drain_active, false, memory_order_release);
    pthread_cond_broadcast(&engine->test_condition);
    pthread_mutex_unlock(&engine->test_mutex);
}

int trevrpc_engine_internal_test_get_write_fd(trevrpc_engine* engine) {
    return engine == NULL ? -EINVAL : engine->wake_write_fd;
}

int trevrpc_engine_internal_test_force_wake_failure(trevrpc_engine* engine, uint32_t operation, uint64_t counter_seed) {
    int descriptor;
    if (engine == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&engine->mutex);
    engine->wake_failures = counter_seed;
    if (operation == 1u) {
        descriptor = engine->wake_read_fd;
        engine->wake_read_fd = -1;
    } else if (operation == 2u) {
        descriptor = engine->wake_write_fd;
        engine->wake_write_fd = -1;
    } else {
        pthread_mutex_unlock(&engine->mutex);
        return -EINVAL;
    }
    if (descriptor >= 0) {
        close(descriptor);
    }
    pthread_mutex_unlock(&engine->mutex);
    return 0;
}

void trevrpc_engine_internal_test_pause_api_enter(trevrpc_engine* engine) {
    if (engine != NULL) {
        pthread_mutex_lock(&engine->test_mutex);
        engine->test_pause_api_enter = true;
        atomic_store_explicit(&engine->test_pause_api_enter_active, true, memory_order_release);
        pthread_mutex_unlock(&engine->test_mutex);
    }
}

void trevrpc_engine_internal_test_wait_for_api_enter(trevrpc_engine* engine) {
    if (engine != NULL) {
        pthread_mutex_lock(&engine->test_mutex);
        while (!engine->test_in_api_enter) {
            pthread_cond_wait(&engine->test_condition, &engine->test_mutex);
        }
        pthread_mutex_unlock(&engine->test_mutex);
    }
}

void trevrpc_engine_internal_test_resume_api_enter(trevrpc_engine* engine) {
    if (engine != NULL) {
        pthread_mutex_lock(&engine->test_mutex);
        engine->test_pause_api_enter = false;
        atomic_store_explicit(&engine->test_pause_api_enter_active, false, memory_order_release);
        pthread_cond_broadcast(&engine->test_condition);
        pthread_mutex_unlock(&engine->test_mutex);
    }
}

void trevrpc_engine_internal_test_pause_api_leave(trevrpc_engine* engine) {
    if (engine != NULL) {
        pthread_mutex_lock(&engine->test_mutex);
        engine->test_pause_api_leave = true;
        atomic_store_explicit(&engine->test_pause_api_leave_active, true, memory_order_release);
        pthread_mutex_unlock(&engine->test_mutex);
    }
}

void trevrpc_engine_internal_test_wait_for_api_leave(trevrpc_engine* engine) {
    if (engine != NULL) {
        pthread_mutex_lock(&engine->test_mutex);
        while (!engine->test_in_api_leave) {
            pthread_cond_wait(&engine->test_condition, &engine->test_mutex);
        }
        pthread_mutex_unlock(&engine->test_mutex);
    }
}

void trevrpc_engine_internal_test_resume_api_leave(trevrpc_engine* engine) {
    if (engine != NULL) {
        pthread_mutex_lock(&engine->test_mutex);
        engine->test_pause_api_leave = false;
        atomic_store_explicit(&engine->test_pause_api_leave_active, false, memory_order_release);
        pthread_cond_broadcast(&engine->test_condition);
        pthread_mutex_unlock(&engine->test_mutex);
    }
}

int trevrpc_engine_internal_test_release_is_waiting(const trevrpc_engine* engine) {
    return engine != NULL && atomic_load_explicit(&engine->release_waiting, memory_order_acquire) ? 1 : 0;
}
