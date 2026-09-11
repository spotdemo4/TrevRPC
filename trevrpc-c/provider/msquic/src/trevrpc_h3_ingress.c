#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
#define _DARWIN_C_SOURCE
#endif
#define _POSIX_C_SOURCE 200809L

#include "trevrpc_h3_ingress_internal.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define TREV_H3_INGRESS_ACTION_QUEUE_COUNT 5u
#define TREV_H3_INGRESS_UNCLASSIFIED_BIDI_QUEUE TREV_H3_INGRESS_ACTION_QUEUE_COUNT
#define TREV_H3_INGRESS_QUEUE_COUNT (TREV_H3_INGRESS_ACTION_QUEUE_COUNT + 1u)
#define TREV_H3_INGRESS_CLASSIFIER_READ_SIZE 1024u

typedef enum trevrpc_h3_ingress_entry_state {
    TREV_H3_INGRESS_ENTRY_FREE = 0,
    TREV_H3_INGRESS_ENTRY_LIVE,
    TREV_H3_INGRESS_ENTRY_CLASSIFIED,
    TREV_H3_INGRESS_ENTRY_ACCEPTED,
    TREV_H3_INGRESS_ENTRY_HANDOFF,
    TREV_H3_INGRESS_ENTRY_CLOSING,
} trevrpc_h3_ingress_entry_state;

typedef struct trevrpc_h3_ingress_runtime trevrpc_h3_ingress_runtime;

typedef struct trevrpc_h3_ingress_entry {
    trevrpc_h3_ingress_runtime* runtime;
    void* stream;
    uint64_t stream_id;
    uint64_t accepted_at_nanos;
    uint64_t ready_sequence;
    trevrpc_h3_demux_direction direction;
    trevrpc_h3_demux_stream classifier;
    trevrpc_h3_demux_result result;
    trevrpc_h3_ingress_entry_state state;
    uint32_t pending_flags;
    uint64_t wake_generation;
    bool install_pending;
    bool observer_installed;
    bool processing;
    bool terminal_seen;
} trevrpc_h3_ingress_entry;

typedef struct trevrpc_h3_ingress_queue {
    size_t* entries;
    size_t head;
    size_t count;
} trevrpc_h3_ingress_queue;

struct trevrpc_h3_ingress {
    struct trevrpc_h3_ingress* registry_next;
    trevrpc_h3_ingress_runtime* runtime;
    trevrpc_h3_ingress_runtime* retired_runtime;
    size_t api_references;
    bool retired_reclaim_deferred;
#ifdef TREVRPC_H3_INGRESS_TESTING
    pthread_mutex_t test_admission_mutex;
    pthread_cond_t test_admission_cond;
    bool test_pause_before_reference;
    bool test_before_reference_paused;
    bool test_pause_after_admission;
    bool test_admission_paused;
    size_t test_runtime_reap_count;
#endif
};

struct trevrpc_h3_ingress_runtime {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    pthread_cond_t pump_cond;
    trevrpc_h3_ingress* handle;
    void* conn;
    trevrpc_h3_ingress_transport_ops ops;
    trevrpc_h3_ingress_entry* entries;
    size_t max_live_entries;
    size_t live_entries;
    size_t max_queue_entries;
    size_t* queue_storage;
    trevrpc_h3_ingress_queue queues[TREV_H3_INGRESS_QUEUE_COUNT];
    trevrpc_h3_ingress_bidi_mode bidi_mode;
    trevrpc_h3_demux_profile_capabilities profile;
#ifdef TREVRPC_H3_INGRESS_TESTING
    trevrpc_h3_ingress_clock_gettime clock_gettime;
    void* clock_context;
#endif
    uint64_t next_ready_sequence;
    trevrpc_h3_demux_roles roles;
    pthread_t accept_thread;
    pthread_t pump_thread;
    bool accept_thread_started;
    bool pump_thread_started;
    bool start_attempted;
    bool start_in_progress;
    bool stopping;
    bool fatal;
    uint64_t fatal_error;
    int settings_status;
    uint64_t settings_error;
    bool teardown_started;
    bool teardown_complete;
    bool connection_shutdown_sent;
    size_t connection_shutdown_calls_in_progress;
    size_t active_waiters;
    size_t teardown_waiters;
    size_t teardown_calls_active;
    size_t handoffs_in_progress;
};

/*
 * Public handles are process-lifetime identities. Release closes a handle by
 * detaching and reclaiming its implementation, but retains the tombstone so an
 * already-started call can never alias a later allocation at the same address.
 * The registry lock serializes implementation acquisition with detachment.
 * A released tombstone moves to a graveyard list: admission walks only live
 * handles, while the tombstone memory stays allocated (never freed, never
 * reused) so a stale raw pointer can only fail the membership check.
 */
static pthread_mutex_t trevrpc_h3_ingress_registry_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t trevrpc_h3_ingress_registry_cond = PTHREAD_COND_INITIALIZER;
static trevrpc_h3_ingress* trevrpc_h3_ingress_registry_head;
static trevrpc_h3_ingress* trevrpc_h3_ingress_graveyard_head;
static _Thread_local trevrpc_h3_ingress_runtime* trevrpc_h3_ingress_shutdown_callback_runtime;
#ifdef TREVRPC_H3_INGRESS_TESTING
#define TREV_H3_INGRESS_TEST_WAIT_SECONDS 10
static size_t trevrpc_h3_ingress_test_released_count;
static bool trevrpc_h3_ingress_test_recycle_runtime;
static trevrpc_h3_ingress_runtime* trevrpc_h3_ingress_test_recycled_runtime;
static pthread_mutex_t trevrpc_h3_ingress_test_timed_wait_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t trevrpc_h3_ingress_test_timed_wait_cond = PTHREAD_COND_INITIALIZER;
static bool trevrpc_h3_ingress_test_force_timed_wait_timeout;
static bool trevrpc_h3_ingress_test_timed_wait_entered;
#endif

static void trevrpc_h3_ingress_shutdown_internal(trevrpc_h3_ingress_runtime* runtime);
static void trevrpc_h3_ingress_runtime_free(trevrpc_h3_ingress_runtime* runtime);
static void trevrpc_h3_ingress_reclaim_runtime(trevrpc_h3_ingress_runtime* runtime);

static int trevrpc_h3_ingress_action_queue_index(trevrpc_h3_demux_action action) {
    switch (action) {
    case TREV_H3_DEMUX_ACTION_CONTROL:
        return 0;
    case TREV_H3_DEMUX_ACTION_QPACK_ENCODER:
        return 1;
    case TREV_H3_DEMUX_ACTION_QPACK_DECODER:
        return 2;
    case TREV_H3_DEMUX_ACTION_REQUEST:
        return 3;
    case TREV_H3_DEMUX_ACTION_WEBTRANSPORT:
        return 4;
    case TREV_H3_DEMUX_ACTION_NONE:
    case TREV_H3_DEMUX_ACTION_UNKNOWN_UNIDIRECTIONAL:
    case TREV_H3_DEMUX_ACTION_RECLAIM_UNCLASSIFIED:
        return -1;
    }
    return -1;
}

static bool trevrpc_h3_ingress_ops_valid(const trevrpc_h3_ingress_transport_ops* ops) {
    return ops != NULL && ops->conn_accept_stream != NULL && ops->conn_shutdown != NULL &&
           ops->conn_shutdown_error != NULL && ops->stream_id != NULL && ops->stream_set_observer != NULL &&
           ops->stream_clear_observer != NULL && ops->stream_drain_observer != NULL &&
           ops->stream_read_protocol_ready != NULL && ops->stream_abort_with_error != NULL && ops->stream_close != NULL;
}

static int trevrpc_h3_ingress_clock(
    trevrpc_h3_ingress_runtime* runtime, clockid_t clock_id, struct timespec* out_time) {
#ifdef TREVRPC_H3_INGRESS_TESTING
    if (runtime->clock_gettime != NULL) {
        return runtime->clock_gettime(runtime->clock_context, clock_id, out_time);
    }
#else
    (void)runtime;
#endif
    return clock_gettime(clock_id, out_time);
}

static int trevrpc_h3_ingress_monotonic_nanos(trevrpc_h3_ingress_runtime* runtime, uint64_t* out_nanos) {
    struct timespec now;
    if (trevrpc_h3_ingress_clock(runtime, CLOCK_MONOTONIC, &now) != 0) {
        return -errno;
    }
    if (now.tv_sec < 0 || (uint64_t)now.tv_sec > UINT64_MAX / UINT64_C(1000000000)) {
        return -EOVERFLOW;
    }
    uint64_t seconds = (uint64_t)now.tv_sec * UINT64_C(1000000000);
    if ((uint64_t)now.tv_nsec > UINT64_MAX - seconds) {
        return -EOVERFLOW;
    }
    *out_nanos = seconds + (uint64_t)now.tv_nsec;
    return 0;
}

static int trevrpc_h3_ingress_deadline(
    trevrpc_h3_ingress_runtime* runtime, uint64_t timeout_nanos, struct timespec* out_deadline) {
    if (trevrpc_h3_ingress_clock(runtime, CLOCK_MONOTONIC, out_deadline) != 0) {
        return -errno;
    }
    uint64_t seconds = timeout_nanos / 1000000000u;
    uint64_t nanos = timeout_nanos % 1000000000u;
    if (seconds > (uint64_t)INT64_MAX - (uint64_t)out_deadline->tv_sec) {
        return -EOVERFLOW;
    }
    out_deadline->tv_sec += (time_t)seconds;
    out_deadline->tv_nsec += (long)nanos;
    if (out_deadline->tv_nsec >= 1000000000L) {
        out_deadline->tv_sec++;
        out_deadline->tv_nsec -= 1000000000L;
    }
    return 0;
}

static int trevrpc_h3_ingress_condition_init(pthread_cond_t* cond) {
#if defined(__APPLE__)
    return pthread_cond_init(cond, NULL);
#else
    pthread_condattr_t attributes;
    int err = pthread_condattr_init(&attributes);
    if (err != 0) {
        return err;
    }
    err = pthread_condattr_setclock(&attributes, CLOCK_MONOTONIC);
    if (err == 0) {
        err = pthread_cond_init(cond, &attributes);
    }
    pthread_condattr_destroy(&attributes);
    return err;
#endif
}

static int trevrpc_h3_ingress_platform_cond_timedwait(trevrpc_h3_ingress_runtime* runtime,
    pthread_cond_t* cond,
    pthread_mutex_t* mutex,
    const struct timespec* deadline) {
#if defined(__APPLE__)
    struct timespec now;
    struct timespec remaining;
    if (trevrpc_h3_ingress_clock(runtime, CLOCK_MONOTONIC, &now) != 0) {
        return errno;
    }
    if (now.tv_sec > deadline->tv_sec || (now.tv_sec == deadline->tv_sec && now.tv_nsec >= deadline->tv_nsec)) {
        return ETIMEDOUT;
    }
    remaining.tv_sec = deadline->tv_sec - now.tv_sec;
    if (deadline->tv_nsec < now.tv_nsec) {
        remaining.tv_sec--;
        remaining.tv_nsec = 1000000000L + deadline->tv_nsec - now.tv_nsec;
    } else {
        remaining.tv_nsec = deadline->tv_nsec - now.tv_nsec;
    }
    return pthread_cond_timedwait_relative_np(cond, mutex, &remaining);
#else
    (void)runtime;
    return pthread_cond_timedwait(cond, mutex, deadline);
#endif
}

#ifdef TREVRPC_H3_INGRESS_TESTING
typedef bool (*trevrpc_h3_ingress_test_wait_ready)(void* context);

static int trevrpc_h3_ingress_test_wait_until(
    pthread_cond_t* cond, pthread_mutex_t* mutex, trevrpc_h3_ingress_test_wait_ready ready, void* context) {
    struct timespec deadline;
    int err = clock_gettime(CLOCK_REALTIME, &deadline);
    if (err != 0) {
        return -errno;
    }
    if (deadline.tv_sec < 0 || deadline.tv_sec > INT64_MAX - TREV_H3_INGRESS_TEST_WAIT_SECONDS) {
        return -EOVERFLOW;
    }
    deadline.tv_sec += TREV_H3_INGRESS_TEST_WAIT_SECONDS;

    pthread_mutex_lock(mutex);
    while (!ready(context)) {
        err = pthread_cond_timedwait(cond, mutex, &deadline);
        if (err != 0) {
            pthread_mutex_unlock(mutex);
            return err == ETIMEDOUT ? -ETIMEDOUT : -err;
        }
    }
    pthread_mutex_unlock(mutex);
    return 0;
}

static bool trevrpc_h3_ingress_test_timed_wait_is_entered(void* context) {
    (void)context;
    return trevrpc_h3_ingress_test_timed_wait_entered;
}

void trevrpc_h3_ingress_test_force_next_timed_wait_timeout(void) {
    pthread_mutex_lock(&trevrpc_h3_ingress_test_timed_wait_mutex);
    trevrpc_h3_ingress_test_force_timed_wait_timeout = true;
    trevrpc_h3_ingress_test_timed_wait_entered = false;
    pthread_mutex_unlock(&trevrpc_h3_ingress_test_timed_wait_mutex);
}

int trevrpc_h3_ingress_test_wait_timed_wait_entered(void) {
    int result = trevrpc_h3_ingress_test_wait_until(&trevrpc_h3_ingress_test_timed_wait_cond,
        &trevrpc_h3_ingress_test_timed_wait_mutex,
        trevrpc_h3_ingress_test_timed_wait_is_entered,
        NULL);
    if (result != 0) {
        pthread_mutex_lock(&trevrpc_h3_ingress_test_timed_wait_mutex);
        trevrpc_h3_ingress_test_force_timed_wait_timeout = false;
        pthread_mutex_unlock(&trevrpc_h3_ingress_test_timed_wait_mutex);
    }
    return result;
}

static int trevrpc_h3_ingress_cond_timedwait(trevrpc_h3_ingress_runtime* runtime,
    pthread_cond_t* cond,
    pthread_mutex_t* mutex,
    const struct timespec* deadline) {
    pthread_mutex_lock(&trevrpc_h3_ingress_test_timed_wait_mutex);
    bool force_timeout = trevrpc_h3_ingress_test_force_timed_wait_timeout;
    trevrpc_h3_ingress_test_force_timed_wait_timeout = false;
    if (force_timeout) {
        trevrpc_h3_ingress_test_timed_wait_entered = true;
        pthread_cond_broadcast(&trevrpc_h3_ingress_test_timed_wait_cond);
    }
    pthread_mutex_unlock(&trevrpc_h3_ingress_test_timed_wait_mutex);

    int err = trevrpc_h3_ingress_platform_cond_timedwait(runtime, cond, mutex, deadline);
    return force_timeout ? ETIMEDOUT : err;
}
#else
static int trevrpc_h3_ingress_cond_timedwait(trevrpc_h3_ingress_runtime* runtime,
    pthread_cond_t* cond,
    pthread_mutex_t* mutex,
    const struct timespec* deadline) {
    return trevrpc_h3_ingress_platform_cond_timedwait(runtime, cond, mutex, deadline);
}
#endif

static trevrpc_h3_ingress** trevrpc_h3_ingress_registry_find_slot(trevrpc_h3_ingress* handle) {
    trevrpc_h3_ingress** slot = &trevrpc_h3_ingress_registry_head;
    while (*slot != NULL && *slot != handle) {
        slot = &(*slot)->registry_next;
    }
    return slot;
}

static trevrpc_h3_ingress_runtime* trevrpc_h3_ingress_claim_deferred_reclaim_locked(trevrpc_h3_ingress* handle) {
    if (handle->api_references != 0 || !handle->retired_reclaim_deferred || handle->retired_runtime == NULL) {
        return NULL;
    }
    trevrpc_h3_ingress_runtime* runtime = handle->retired_runtime;
    handle->retired_runtime = NULL;
    handle->retired_reclaim_deferred = false;
    return runtime;
}

static void trevrpc_h3_ingress_schedule_deferred_reclaim(trevrpc_h3_ingress* handle) {
    pthread_mutex_lock(&trevrpc_h3_ingress_registry_mutex);
    trevrpc_h3_ingress_runtime* runtime = trevrpc_h3_ingress_claim_deferred_reclaim_locked(handle);
    pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);
    if (runtime != NULL) {
        trevrpc_h3_ingress_reclaim_runtime(runtime);
    }
}

static int trevrpc_h3_ingress_api_enter(trevrpc_h3_ingress* handle, trevrpc_h3_ingress_runtime** out_runtime) {
    if (handle == NULL || out_runtime == NULL) {
        return -EINVAL;
    }

#ifdef TREVRPC_H3_INGRESS_TESTING
    pthread_mutex_lock(&handle->test_admission_mutex);
    if (handle->test_pause_before_reference) {
        handle->test_before_reference_paused = true;
        pthread_cond_broadcast(&handle->test_admission_cond);
        while (handle->test_pause_before_reference) {
            pthread_cond_wait(&handle->test_admission_cond, &handle->test_admission_mutex);
        }
        handle->test_before_reference_paused = false;
        pthread_cond_broadcast(&handle->test_admission_cond);
    }
    pthread_mutex_unlock(&handle->test_admission_mutex);
#endif

    pthread_mutex_lock(&trevrpc_h3_ingress_registry_mutex);
    trevrpc_h3_ingress** slot = trevrpc_h3_ingress_registry_find_slot(handle);
    if (*slot == NULL || handle->runtime == NULL) {
        pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);
        return -EPIPE;
    }
    if (handle->api_references == SIZE_MAX) {
        pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);
        return -EOVERFLOW;
    }
    handle->api_references++;
    *out_runtime = handle->runtime;
    pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);

#ifdef TREVRPC_H3_INGRESS_TESTING
    pthread_mutex_lock(&handle->test_admission_mutex);
    if (handle->test_pause_after_admission) {
        handle->test_admission_paused = true;
        pthread_cond_broadcast(&handle->test_admission_cond);
        while (handle->test_pause_after_admission) {
            pthread_cond_wait(&handle->test_admission_cond, &handle->test_admission_mutex);
        }
        handle->test_admission_paused = false;
        pthread_cond_broadcast(&handle->test_admission_cond);
    }
    pthread_mutex_unlock(&handle->test_admission_mutex);
#endif
    return 0;
}

static void trevrpc_h3_ingress_api_leave(trevrpc_h3_ingress* handle) {
    trevrpc_h3_ingress_runtime* reclaim = NULL;
    pthread_mutex_lock(&trevrpc_h3_ingress_registry_mutex);
    handle->api_references--;
    if (handle->api_references == 0) {
        pthread_cond_broadcast(&trevrpc_h3_ingress_registry_cond);
        if (handle->retired_runtime != trevrpc_h3_ingress_shutdown_callback_runtime) {
            reclaim = trevrpc_h3_ingress_claim_deferred_reclaim_locked(handle);
        }
    }
    pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);
    if (reclaim != NULL) {
        trevrpc_h3_ingress_reclaim_runtime(reclaim);
    }
}

static void trevrpc_h3_ingress_observer_wake(void* context, uint32_t flags) {
    trevrpc_h3_ingress_entry* entry = context;
    trevrpc_h3_ingress_runtime* runtime = entry->runtime;
    pthread_mutex_lock(&runtime->mutex);
    if (entry->state == TREV_H3_INGRESS_ENTRY_LIVE) {
        entry->pending_flags |= flags;
        entry->wake_generation++;
        pthread_cond_signal(&runtime->pump_cond);
    }
    pthread_mutex_unlock(&runtime->mutex);
}

static void trevrpc_h3_ingress_finish_shutdown_call(trevrpc_h3_ingress_runtime* runtime) {
    pthread_mutex_lock(&runtime->mutex);
    runtime->connection_shutdown_calls_in_progress--;
    pthread_cond_broadcast(&runtime->cond);
    pthread_mutex_unlock(&runtime->mutex);
    trevrpc_h3_ingress_schedule_deferred_reclaim(runtime->handle);
}

static void trevrpc_h3_ingress_invoke_shutdown(
    trevrpc_h3_ingress_runtime* runtime, bool fatal, uint64_t application_error) {
    trevrpc_h3_ingress_runtime* previous = trevrpc_h3_ingress_shutdown_callback_runtime;
    trevrpc_h3_ingress_shutdown_callback_runtime = runtime;
    if (fatal) {
        runtime->ops.conn_shutdown_error(runtime->conn, application_error);
    } else {
        runtime->ops.conn_shutdown(runtime->conn);
    }
    trevrpc_h3_ingress_shutdown_callback_runtime = previous;
    trevrpc_h3_ingress_finish_shutdown_call(runtime);
}

static void trevrpc_h3_ingress_mark_stopping_locked(trevrpc_h3_ingress_runtime* runtime) {
    if (runtime->stopping) {
        return;
    }
    runtime->stopping = true;
    if (runtime->settings_status == TREV_H3_INGRESS_SETTINGS_PENDING) {
        runtime->settings_status = TREV_H3_INGRESS_SETTINGS_FAILED;
        runtime->settings_error = runtime->fatal ? runtime->fatal_error : 0;
    }
    pthread_cond_broadcast(&runtime->cond);
    pthread_cond_broadcast(&runtime->pump_cond);
}

static void trevrpc_h3_ingress_stop_without_error(trevrpc_h3_ingress_runtime* runtime) {
    bool send_shutdown = false;
    pthread_mutex_lock(&runtime->mutex);
    trevrpc_h3_ingress_mark_stopping_locked(runtime);
    if (!runtime->connection_shutdown_sent) {
        runtime->connection_shutdown_sent = true;
        runtime->connection_shutdown_calls_in_progress++;
        send_shutdown = true;
    }
    pthread_mutex_unlock(&runtime->mutex);
    if (send_shutdown) {
        trevrpc_h3_ingress_invoke_shutdown(runtime, false, 0);
    }
}

static int trevrpc_h3_ingress_fail_internal(
    trevrpc_h3_ingress_runtime* runtime, uint64_t application_error, bool require_pending_settings) {
    if (runtime == NULL || application_error == 0) {
        return -EINVAL;
    }

    bool send_shutdown = false;
    pthread_mutex_lock(&runtime->mutex);
    if (runtime->fatal || runtime->stopping ||
        (require_pending_settings && runtime->settings_status != TREV_H3_INGRESS_SETTINGS_PENDING)) {
        pthread_mutex_unlock(&runtime->mutex);
        return -EALREADY;
    }
    runtime->fatal = true;
    runtime->fatal_error = application_error;
    runtime->stopping = true;
    if (runtime->settings_status == TREV_H3_INGRESS_SETTINGS_PENDING ||
        (runtime->settings_status == TREV_H3_INGRESS_SETTINGS_FAILED && runtime->settings_error == 0)) {
        runtime->settings_status = TREV_H3_INGRESS_SETTINGS_FAILED;
        runtime->settings_error = application_error;
    }
    if (!runtime->connection_shutdown_sent) {
        runtime->connection_shutdown_sent = true;
        runtime->connection_shutdown_calls_in_progress++;
        send_shutdown = true;
    }
    pthread_cond_broadcast(&runtime->cond);
    pthread_cond_broadcast(&runtime->pump_cond);
    pthread_mutex_unlock(&runtime->mutex);

    if (send_shutdown) {
        trevrpc_h3_ingress_invoke_shutdown(runtime, true, application_error);
    }
    return 0;
}

int trevrpc_h3_ingress_fail(trevrpc_h3_ingress* handle, uint64_t application_error) {
    trevrpc_h3_ingress_runtime* runtime = NULL;
    int result = trevrpc_h3_ingress_api_enter(handle, &runtime);
    if (result != 0) {
        return result;
    }
    result = trevrpc_h3_ingress_fail_internal(runtime, application_error, false);
    trevrpc_h3_ingress_api_leave(handle);
    return result;
}

static void trevrpc_h3_ingress_fail_transport(trevrpc_h3_ingress_runtime* runtime, intptr_t transport_error) {
    if (transport_error == TREV_MSQUIC_ERR_CLOSED) {
        trevrpc_h3_ingress_stop_without_error(runtime);
    } else if (transport_error == TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED || transport_error == -ENOMEM) {
        (void)trevrpc_h3_ingress_fail_internal(runtime, TREV_H3_INGRESS_APP_EXCESSIVE_LOAD, false);
    } else {
        (void)trevrpc_h3_ingress_fail_internal(runtime, TREV_H3_INGRESS_APP_INTERNAL_ERROR, false);
    }
}

static void trevrpc_h3_ingress_reset_entry_locked(
    trevrpc_h3_ingress_runtime* runtime, trevrpc_h3_ingress_entry* entry) {
    entry->stream = NULL;
    entry->stream_id = 0;
    entry->accepted_at_nanos = 0;
    entry->ready_sequence = 0;
    entry->result = (trevrpc_h3_demux_result){0};
    entry->state = TREV_H3_INGRESS_ENTRY_FREE;
    entry->pending_flags = 0;
    entry->observer_installed = false;
    entry->install_pending = false;
    entry->processing = false;
    entry->terminal_seen = false;
    if (runtime->live_entries > 0) {
        runtime->live_entries--;
    }
}

static void trevrpc_h3_ingress_close_entry(trevrpc_h3_ingress_runtime* runtime, size_t index) {
    void* stream = NULL;
    bool observer_installed = false;

    pthread_mutex_lock(&runtime->mutex);
    trevrpc_h3_ingress_entry* entry = &runtime->entries[index];
    if (entry->state == TREV_H3_INGRESS_ENTRY_LIVE || entry->state == TREV_H3_INGRESS_ENTRY_CLASSIFIED ||
        entry->state == TREV_H3_INGRESS_ENTRY_ACCEPTED) {
        entry->state = TREV_H3_INGRESS_ENTRY_CLOSING;
        entry->processing = false;
        stream = entry->stream;
        observer_installed = entry->observer_installed;
    }
    pthread_mutex_unlock(&runtime->mutex);

    if (stream == NULL) {
        return;
    }
    if (observer_installed) {
        runtime->ops.stream_clear_observer(stream);
        runtime->ops.stream_drain_observer(stream);
    }

    pthread_mutex_lock(&runtime->mutex);
    trevrpc_h3_ingress_reset_entry_locked(runtime, entry);
    pthread_cond_broadcast(&runtime->cond);
    pthread_mutex_unlock(&runtime->mutex);

    runtime->ops.stream_close(stream);
}

static size_t trevrpc_h3_ingress_read_size(const trevrpc_h3_demux_stream* classifier) {
    if (classifier->phase == TREV_H3_DEMUX_SKIP_UNKNOWN_PAYLOAD) {
        return classifier->unknown_payload_remaining < TREV_H3_INGRESS_CLASSIFIER_READ_SIZE
                   ? (size_t)classifier->unknown_payload_remaining
                   : TREV_H3_INGRESS_CLASSIFIER_READ_SIZE;
    }
    if (classifier->varint.have == 0 || classifier->varint.need <= classifier->varint.have) {
        return 1;
    }
    return (size_t)(classifier->varint.need - classifier->varint.have);
}

static trevrpc_h3_demux_status trevrpc_h3_ingress_classify_ready(trevrpc_h3_ingress_runtime* runtime,
    trevrpc_h3_ingress_entry* entry,
    uint32_t flags,
    trevrpc_h3_demux_result* out_result,
    intptr_t* out_transport_error) {
    uint8_t prefix[TREV_H3_INGRESS_CLASSIFIER_READ_SIZE];
    pthread_mutex_lock(&runtime->mutex);
    entry->terminal_seen = entry->terminal_seen || (flags & TREV_MSQUIC_STREAM_OBSERVER_TERMINAL) != 0;
    bool terminal = entry->terminal_seen;
    trevrpc_h3_demux_profile_capabilities profile = runtime->profile;
    pthread_mutex_unlock(&runtime->mutex);
    *out_transport_error = 0;

    trevrpc_h3_demux_status status = trevrpc_h3_demux_stream_feed(&entry->classifier, &profile, NULL, 0, out_result);
    if (status != TREV_H3_DEMUX_NEED_MORE) {
        return status;
    }

    for (;;) {
        size_t read_size = trevrpc_h3_ingress_read_size(&entry->classifier);
        intptr_t n = runtime->ops.stream_read_protocol_ready(entry->stream, prefix, read_size);
        if (n > 0) {
            status = trevrpc_h3_demux_stream_feed(&entry->classifier, &profile, prefix, (size_t)n, out_result);
            if (status != TREV_H3_DEMUX_NEED_MORE) {
                return status;
            }
            continue;
        }
        if (n == TREV_MSQUIC_ERR_TIMEOUT && !terminal) {
            return TREV_H3_DEMUX_NEED_MORE;
        }
        if (n == TREV_MSQUIC_ERR_CLOSED) {
            *out_transport_error = n;
            return TREV_H3_DEMUX_NEED_MORE;
        }
        if (n == 0 || n == TREV_MSQUIC_ERR_TIMEOUT) {
            return trevrpc_h3_demux_stream_terminal(&entry->classifier, &profile, out_result);
        }
        *out_transport_error = n;
        return TREV_H3_DEMUX_NEED_MORE;
    }
}

static bool trevrpc_h3_ingress_finish_classification(trevrpc_h3_ingress_runtime* runtime,
    size_t index,
    trevrpc_h3_demux_status status,
    const trevrpc_h3_demux_result* result) {
    if (status == TREV_H3_DEMUX_PROTOCOL_ERROR) {
        pthread_mutex_lock(&runtime->mutex);
        runtime->entries[index].processing = false;
        pthread_mutex_unlock(&runtime->mutex);
        (void)trevrpc_h3_ingress_fail_internal(runtime, result->application_error, false);
        return false;
    }
    if (status != TREV_H3_DEMUX_ACTION_READY) {
        pthread_mutex_lock(&runtime->mutex);
        trevrpc_h3_ingress_entry* entry = &runtime->entries[index];
        entry->processing = false;
        if (status == TREV_H3_DEMUX_WAIT_PROFILE && runtime->profile.resolved && !runtime->stopping) {
            entry->pending_flags |= TREV_MSQUIC_STREAM_OBSERVER_READABLE;
        }
        pthread_cond_signal(&runtime->pump_cond);
        pthread_mutex_unlock(&runtime->mutex);
        return true;
    }

    if (result->action == TREV_H3_DEMUX_ACTION_UNKNOWN_UNIDIRECTIONAL) {
        (void)runtime->ops.stream_abort_with_error(
            runtime->entries[index].stream, TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR);
        trevrpc_h3_ingress_close_entry(runtime, index);
        return true;
    }
    if (result->action == TREV_H3_DEMUX_ACTION_RECLAIM_UNCLASSIFIED) {
        trevrpc_h3_ingress_close_entry(runtime, index);
        return true;
    }

    uint64_t role_error = 0;
    if (trevrpc_h3_demux_roles_claim(&runtime->roles, result->action, &role_error) != 0) {
        pthread_mutex_lock(&runtime->mutex);
        runtime->entries[index].processing = false;
        pthread_mutex_unlock(&runtime->mutex);
        (void)trevrpc_h3_ingress_fail_internal(runtime, role_error, false);
        return false;
    }

    int queue_index = trevrpc_h3_ingress_action_queue_index(result->action);
    bool overflow = false;
    pthread_mutex_lock(&runtime->mutex);
    trevrpc_h3_ingress_entry* entry = &runtime->entries[index];
    entry->processing = false;
    entry->result = *result;
    if (runtime->stopping) {
        pthread_mutex_unlock(&runtime->mutex);
        return true;
    }
    trevrpc_h3_ingress_queue* queue = &runtime->queues[(size_t)queue_index];
    if (queue->count == runtime->max_queue_entries) {
        overflow = true;
    } else {
        size_t tail = (queue->head + queue->count) % runtime->max_queue_entries;
        queue->entries[tail] = index;
        queue->count++;
        entry->ready_sequence = runtime->next_ready_sequence++;
        entry->state = TREV_H3_INGRESS_ENTRY_CLASSIFIED;
        pthread_cond_broadcast(&runtime->cond);
    }
    pthread_mutex_unlock(&runtime->mutex);

    if (overflow) {
        (void)trevrpc_h3_ingress_fail_internal(runtime, TREV_H3_INGRESS_APP_EXCESSIVE_LOAD, false);
        return false;
    }
    return true;
}

static bool trevrpc_h3_ingress_take_ready_entry(
    trevrpc_h3_ingress_runtime* runtime, size_t* out_index, uint32_t* out_flags) {
    for (size_t i = 0; i < runtime->max_live_entries; i++) {
        trevrpc_h3_ingress_entry* entry = &runtime->entries[i];
        if (entry->state == TREV_H3_INGRESS_ENTRY_LIVE && !entry->install_pending && !entry->processing &&
            entry->pending_flags != 0) {
            entry->processing = true;
            *out_index = i;
            *out_flags = entry->pending_flags;
            entry->pending_flags = 0;
            return true;
        }
    }
    return false;
}

static void* trevrpc_h3_ingress_pump_main(void* context) {
    trevrpc_h3_ingress_runtime* runtime = context;
    for (;;) {
        size_t index = 0;
        uint32_t flags = 0;
        pthread_mutex_lock(&runtime->mutex);
        while (!runtime->stopping && !trevrpc_h3_ingress_take_ready_entry(runtime, &index, &flags)) {
            pthread_cond_wait(&runtime->pump_cond, &runtime->mutex);
        }
        bool stopping = runtime->stopping;
        pthread_mutex_unlock(&runtime->mutex);
        if (stopping) {
            return NULL;
        }

        trevrpc_h3_demux_result result = {0};
        intptr_t transport_error = 0;
        trevrpc_h3_demux_status status =
            trevrpc_h3_ingress_classify_ready(runtime, &runtime->entries[index], flags, &result, &transport_error);
        if (transport_error != 0) {
            if (transport_error == TREV_MSQUIC_ERR_CLOSED) {
                trevrpc_h3_ingress_close_entry(runtime, index);
                continue;
            }
            pthread_mutex_lock(&runtime->mutex);
            runtime->entries[index].processing = false;
            pthread_mutex_unlock(&runtime->mutex);
            trevrpc_h3_ingress_fail_transport(runtime, transport_error);
            return NULL;
        }
        if (!trevrpc_h3_ingress_finish_classification(runtime, index, status, &result)) {
            return NULL;
        }
    }
}

static size_t trevrpc_h3_ingress_find_free_entry(trevrpc_h3_ingress_runtime* runtime) {
    for (size_t i = 0; i < runtime->max_live_entries; i++) {
        if (runtime->entries[i].state == TREV_H3_INGRESS_ENTRY_FREE) {
            return i;
        }
    }
    return runtime->max_live_entries;
}

static void* trevrpc_h3_ingress_accept_main(void* context) {
    trevrpc_h3_ingress_runtime* runtime = context;
    for (;;) {
        pthread_mutex_lock(&runtime->mutex);
        bool stopping = runtime->stopping;
        pthread_mutex_unlock(&runtime->mutex);
        if (stopping) {
            return NULL;
        }

        void* stream = NULL;
        int err = runtime->ops.conn_accept_stream(runtime->conn, &stream);
        if (err != 0) {
            trevrpc_h3_ingress_fail_transport(runtime, err);
            return NULL;
        }

        uint64_t accepted_at_nanos = 0;
        if (runtime->bidi_mode == TREV_H3_INGRESS_BIDI_HANDOFF_UNCLASSIFIED) {
            err = trevrpc_h3_ingress_monotonic_nanos(runtime, &accepted_at_nanos);
            if (err != 0) {
                runtime->ops.stream_close(stream);
                trevrpc_h3_ingress_fail_transport(runtime, err);
                return NULL;
            }
        }

        uint64_t stream_id = 0;
        err = runtime->ops.stream_id(stream, &stream_id);
        if (err != 0) {
            runtime->ops.stream_close(stream);
            if (err == TREV_MSQUIC_ERR_CLOSED) {
                continue;
            }
            trevrpc_h3_ingress_fail_transport(runtime, err);
            return NULL;
        }
        trevrpc_h3_demux_direction direction = trevrpc_h3_demux_stream_id_is_unidirectional(stream_id)
                                                   ? TREV_H3_DEMUX_UNIDIRECTIONAL
                                                   : TREV_H3_DEMUX_BIDIRECTIONAL;
        bool handoff_unclassified =
            direction == TREV_H3_DEMUX_BIDIRECTIONAL && runtime->bidi_mode == TREV_H3_INGRESS_BIDI_HANDOFF_UNCLASSIFIED;

        size_t index = runtime->max_live_entries;
        bool overflow = false;
        pthread_mutex_lock(&runtime->mutex);
        if (!runtime->stopping) {
            trevrpc_h3_ingress_queue* accepted_queue = &runtime->queues[TREV_H3_INGRESS_UNCLASSIFIED_BIDI_QUEUE];
            index = trevrpc_h3_ingress_find_free_entry(runtime);
            if (index == runtime->max_live_entries ||
                (handoff_unclassified && accepted_queue->count == runtime->max_queue_entries)) {
                overflow = true;
            } else {
                trevrpc_h3_ingress_entry* entry = &runtime->entries[index];
                entry->stream = stream;
                entry->stream_id = stream_id;
                entry->accepted_at_nanos = handoff_unclassified ? accepted_at_nanos : 0;
                entry->ready_sequence = 0;
                entry->direction = direction;
                entry->result = (trevrpc_h3_demux_result){0};
                entry->pending_flags = 0;
                entry->observer_installed = false;
                entry->processing = false;
                entry->terminal_seen = false;
                runtime->live_entries++;
                if (handoff_unclassified) {
                    size_t tail = (accepted_queue->head + accepted_queue->count) % runtime->max_queue_entries;
                    accepted_queue->entries[tail] = index;
                    accepted_queue->count++;
                    entry->state = TREV_H3_INGRESS_ENTRY_ACCEPTED;
                    entry->install_pending = false;
                    pthread_cond_broadcast(&runtime->cond);
                } else {
                    (void)trevrpc_h3_demux_stream_init(&entry->classifier, direction);
                    entry->state = TREV_H3_INGRESS_ENTRY_LIVE;
                    entry->wake_generation++;
                    entry->install_pending = true;
                }
            }
        }
        stopping = runtime->stopping;
        pthread_mutex_unlock(&runtime->mutex);

        if (overflow) {
            runtime->ops.stream_close(stream);
            (void)trevrpc_h3_ingress_fail_internal(runtime, TREV_H3_INGRESS_APP_EXCESSIVE_LOAD, false);
            return NULL;
        }
        if (stopping || index == runtime->max_live_entries) {
            runtime->ops.stream_close(stream);
            return NULL;
        }
        if (handoff_unclassified) {
            continue;
        }

        trevrpc_h3_ingress_entry* entry = &runtime->entries[index];
        err = runtime->ops.stream_set_observer(stream, trevrpc_h3_ingress_observer_wake, entry);
        pthread_mutex_lock(&runtime->mutex);
        entry->install_pending = false;
        if (err == 0) {
            entry->observer_installed = true;
            entry->pending_flags |= TREV_MSQUIC_STREAM_OBSERVER_READABLE;
            pthread_cond_signal(&runtime->pump_cond);
        }
        pthread_mutex_unlock(&runtime->mutex);
        if (err != 0) {
            trevrpc_h3_ingress_close_entry(runtime, index);
            if (err == TREV_MSQUIC_ERR_CLOSED) {
                continue;
            }
            trevrpc_h3_ingress_fail_transport(runtime, err);
            return NULL;
        }
    }
}

static trevrpc_h3_ingress_runtime* trevrpc_h3_ingress_runtime_allocate(void) {
#ifdef TREVRPC_H3_INGRESS_TESTING
    pthread_mutex_lock(&trevrpc_h3_ingress_registry_mutex);
    trevrpc_h3_ingress_runtime* runtime = trevrpc_h3_ingress_test_recycled_runtime;
    trevrpc_h3_ingress_test_recycled_runtime = NULL;
    pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);
    if (runtime != NULL) {
        memset(runtime, 0, sizeof(*runtime));
        return runtime;
    }
#endif
    return calloc(1, sizeof(trevrpc_h3_ingress_runtime));
}

static void trevrpc_h3_ingress_runtime_free(trevrpc_h3_ingress_runtime* runtime) {
#ifdef TREVRPC_H3_INGRESS_TESTING
    pthread_mutex_lock(&trevrpc_h3_ingress_registry_mutex);
    if (trevrpc_h3_ingress_test_recycle_runtime && trevrpc_h3_ingress_test_recycled_runtime == NULL) {
        trevrpc_h3_ingress_test_recycled_runtime = runtime;
        pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);
        return;
    }
    pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);
#endif
    free(runtime);
}

static void trevrpc_h3_ingress_reclaim_runtime(trevrpc_h3_ingress_runtime* runtime) {
    trevrpc_h3_ingress* handle = runtime->handle;
    trevrpc_h3_ingress_shutdown_internal(runtime);

    pthread_mutex_lock(&runtime->mutex);
    while (runtime->teardown_calls_active != 0) {
        pthread_cond_wait(&runtime->cond, &runtime->mutex);
    }
    pthread_mutex_unlock(&runtime->mutex);
    pthread_cond_destroy(&runtime->pump_cond);
    pthread_cond_destroy(&runtime->cond);
    pthread_mutex_destroy(&runtime->mutex);
    free(runtime->queue_storage);
    free(runtime->entries);
    trevrpc_h3_ingress_runtime_free(runtime);

#ifdef TREVRPC_H3_INGRESS_TESTING
    pthread_mutex_lock(&handle->test_admission_mutex);
    handle->test_runtime_reap_count++;
    pthread_cond_broadcast(&handle->test_admission_cond);
    pthread_mutex_unlock(&handle->test_admission_mutex);
#else
    (void)handle;
#endif
}

int trevrpc_h3_ingress_create_with_ops(void* conn,
    const trevrpc_h3_ingress_transport_ops* ops,
    const trevrpc_h3_ingress_config* config,
    trevrpc_h3_ingress** out_runtime) {
    if (conn == NULL || !trevrpc_h3_ingress_ops_valid(ops) || config == NULL || out_runtime == NULL ||
        config->max_live_entries == 0 || config->max_queue_entries == 0 ||
        config->max_queue_entries > SIZE_MAX / TREV_H3_INGRESS_QUEUE_COUNT ||
        (config->bidi_mode != TREV_H3_INGRESS_BIDI_CLASSIFY &&
            config->bidi_mode != TREV_H3_INGRESS_BIDI_HANDOFF_UNCLASSIFIED)) {
        return -EINVAL;
    }
    *out_runtime = NULL;

    trevrpc_h3_ingress* handle = calloc(1, sizeof(*handle));
    trevrpc_h3_ingress_runtime* runtime = trevrpc_h3_ingress_runtime_allocate();
    if (handle == NULL || runtime == NULL) {
        free(handle);
        trevrpc_h3_ingress_runtime_free(runtime);
        return -ENOMEM;
    }
    int err = 0;
#ifdef TREVRPC_H3_INGRESS_TESTING
    err = pthread_mutex_init(&handle->test_admission_mutex, NULL);
    if (err != 0) {
        free(handle);
        trevrpc_h3_ingress_runtime_free(runtime);
        return -err;
    }
    err = pthread_cond_init(&handle->test_admission_cond, NULL);
    if (err != 0) {
        pthread_mutex_destroy(&handle->test_admission_mutex);
        free(handle);
        trevrpc_h3_ingress_runtime_free(runtime);
        return -err;
    }
#endif

    runtime->entries = calloc(config->max_live_entries, sizeof(*runtime->entries));
    runtime->queue_storage =
        calloc(config->max_queue_entries * TREV_H3_INGRESS_QUEUE_COUNT, sizeof(*runtime->queue_storage));
    if (runtime->entries == NULL || runtime->queue_storage == NULL) {
        free(runtime->queue_storage);
        free(runtime->entries);
#ifdef TREVRPC_H3_INGRESS_TESTING
        pthread_cond_destroy(&handle->test_admission_cond);
        pthread_mutex_destroy(&handle->test_admission_mutex);
#endif
        free(handle);
        trevrpc_h3_ingress_runtime_free(runtime);
        return -ENOMEM;
    }

    err = pthread_mutex_init(&runtime->mutex, NULL);
    if (err != 0) {
        goto fail_runtime_storage;
    }
    err = trevrpc_h3_ingress_condition_init(&runtime->cond);
    if (err != 0) {
        pthread_mutex_destroy(&runtime->mutex);
        goto fail_runtime_storage;
    }
    err = pthread_cond_init(&runtime->pump_cond, NULL);
    if (err != 0) {
        pthread_cond_destroy(&runtime->cond);
        pthread_mutex_destroy(&runtime->mutex);
        goto fail_runtime_storage;
    }

    runtime->handle = handle;
    runtime->conn = conn;
    runtime->ops = *ops;
    runtime->max_live_entries = config->max_live_entries;
    runtime->max_queue_entries = config->max_queue_entries;
    runtime->bidi_mode = config->bidi_mode;
#ifdef TREVRPC_H3_INGRESS_TESTING
    runtime->clock_gettime = config->clock_gettime;
    runtime->clock_context = config->clock_context;
#endif
    runtime->settings_status = TREV_H3_INGRESS_SETTINGS_PENDING;
    for (size_t i = 0; i < config->max_live_entries; i++) {
        runtime->entries[i].runtime = runtime;
    }
    for (size_t i = 0; i < TREV_H3_INGRESS_QUEUE_COUNT; i++) {
        runtime->queues[i].entries = runtime->queue_storage + i * config->max_queue_entries;
    }

    pthread_mutex_lock(&trevrpc_h3_ingress_registry_mutex);
    handle->runtime = runtime;
    handle->registry_next = trevrpc_h3_ingress_registry_head;
    trevrpc_h3_ingress_registry_head = handle;
    pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);
    *out_runtime = handle;
    return 0;

fail_runtime_storage:
    free(runtime->queue_storage);
    free(runtime->entries);
#ifdef TREVRPC_H3_INGRESS_TESTING
    pthread_cond_destroy(&handle->test_admission_cond);
    pthread_mutex_destroy(&handle->test_admission_mutex);
#endif
    free(handle);
    trevrpc_h3_ingress_runtime_free(runtime);
    return -err;
}

#ifdef TREVRPC_H3_INGRESS_TESTING
void trevrpc_h3_ingress_test_pause_before_reference(trevrpc_h3_ingress* handle, int pause) {
    if (handle == NULL) {
        return;
    }
    pthread_mutex_lock(&handle->test_admission_mutex);
    handle->test_pause_before_reference = pause != 0;
    pthread_cond_broadcast(&handle->test_admission_cond);
    pthread_mutex_unlock(&handle->test_admission_mutex);
}

static bool trevrpc_h3_ingress_test_before_reference_is_paused(void* context) {
    trevrpc_h3_ingress* handle = context;
    return handle->test_before_reference_paused;
}

int trevrpc_h3_ingress_test_wait_before_reference_paused(trevrpc_h3_ingress* handle) {
    if (handle == NULL) {
        return -EINVAL;
    }
    return trevrpc_h3_ingress_test_wait_until(&handle->test_admission_cond,
        &handle->test_admission_mutex,
        trevrpc_h3_ingress_test_before_reference_is_paused,
        handle);
}

void trevrpc_h3_ingress_test_pause_after_admission(trevrpc_h3_ingress* handle, int pause) {
    if (handle == NULL) {
        return;
    }
    pthread_mutex_lock(&handle->test_admission_mutex);
    handle->test_pause_after_admission = pause != 0;
    pthread_cond_broadcast(&handle->test_admission_cond);
    pthread_mutex_unlock(&handle->test_admission_mutex);
}

static bool trevrpc_h3_ingress_test_admission_is_paused(void* context) {
    trevrpc_h3_ingress* handle = context;
    return handle->test_admission_paused;
}

int trevrpc_h3_ingress_test_wait_admission_paused(trevrpc_h3_ingress* handle) {
    if (handle == NULL) {
        return -EINVAL;
    }
    return trevrpc_h3_ingress_test_wait_until(&handle->test_admission_cond,
        &handle->test_admission_mutex,
        trevrpc_h3_ingress_test_admission_is_paused,
        handle);
}

void trevrpc_h3_ingress_test_recycle_runtime_allocations(int recycle) {
    trevrpc_h3_ingress_runtime* unused = NULL;
    pthread_mutex_lock(&trevrpc_h3_ingress_registry_mutex);
    trevrpc_h3_ingress_test_recycle_runtime = recycle != 0;
    if (!trevrpc_h3_ingress_test_recycle_runtime) {
        unused = trevrpc_h3_ingress_test_recycled_runtime;
        trevrpc_h3_ingress_test_recycled_runtime = NULL;
    }
    pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);
    free(unused);
}

void* trevrpc_h3_ingress_test_runtime_address(trevrpc_h3_ingress* handle) {
    void* address = NULL;
    pthread_mutex_lock(&trevrpc_h3_ingress_registry_mutex);
    if (handle != NULL && *trevrpc_h3_ingress_registry_find_slot(handle) != NULL) {
        address = handle->runtime;
    }
    pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);
    return address;
}

static bool trevrpc_h3_ingress_test_runtime_is_detached(void* context) {
    trevrpc_h3_ingress* handle = context;
    return *trevrpc_h3_ingress_registry_find_slot(handle) == NULL || handle->runtime == NULL;
}

int trevrpc_h3_ingress_test_wait_runtime_detached(trevrpc_h3_ingress* handle) {
    if (handle == NULL) {
        return -EINVAL;
    }
    return trevrpc_h3_ingress_test_wait_until(&trevrpc_h3_ingress_registry_cond,
        &trevrpc_h3_ingress_registry_mutex,
        trevrpc_h3_ingress_test_runtime_is_detached,
        handle);
}

static bool trevrpc_h3_ingress_test_runtime_is_reaped(void* context) {
    trevrpc_h3_ingress* handle = context;
    return handle->test_runtime_reap_count != 0;
}

int trevrpc_h3_ingress_test_wait_runtime_reaped(trevrpc_h3_ingress* handle) {
    if (handle == NULL) {
        return -EINVAL;
    }
    return trevrpc_h3_ingress_test_wait_until(
        &handle->test_admission_cond, &handle->test_admission_mutex, trevrpc_h3_ingress_test_runtime_is_reaped, handle);
}

size_t trevrpc_h3_ingress_test_runtime_reap_count(trevrpc_h3_ingress* handle) {
    if (handle == NULL) {
        return 0;
    }
    pthread_mutex_lock(&handle->test_admission_mutex);
    size_t count = handle->test_runtime_reap_count;
    pthread_mutex_unlock(&handle->test_admission_mutex);
    return count;
}

void trevrpc_h3_ingress_test_released_count_reset(void) {
    pthread_mutex_lock(&trevrpc_h3_ingress_registry_mutex);
    trevrpc_h3_ingress_test_released_count = 0;
    pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);
}

size_t trevrpc_h3_ingress_test_released_count_get(void) {
    pthread_mutex_lock(&trevrpc_h3_ingress_registry_mutex);
    size_t count = trevrpc_h3_ingress_test_released_count;
    pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);
    return count;
}

size_t trevrpc_h3_ingress_test_registry_size(void) {
    size_t size = 0;
    pthread_mutex_lock(&trevrpc_h3_ingress_registry_mutex);
    for (trevrpc_h3_ingress* node = trevrpc_h3_ingress_registry_head; node != NULL; node = node->registry_next) {
        size++;
    }
    pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);
    return size;
}
#endif

int trevrpc_h3_ingress_start(trevrpc_h3_ingress* handle) {
    trevrpc_h3_ingress_runtime* runtime = NULL;
    int result = trevrpc_h3_ingress_api_enter(handle, &runtime);
    if (result != 0) {
        return result;
    }
    pthread_mutex_lock(&runtime->mutex);
    if (runtime->start_attempted || runtime->stopping) {
        pthread_mutex_unlock(&runtime->mutex);
        trevrpc_h3_ingress_api_leave(handle);
        return -EALREADY;
    }
    runtime->start_attempted = true;
    runtime->start_in_progress = true;

    int err = pthread_create(&runtime->pump_thread, NULL, trevrpc_h3_ingress_pump_main, runtime);
    if (err != 0) {
        runtime->start_in_progress = false;
        pthread_cond_broadcast(&runtime->cond);
        pthread_mutex_unlock(&runtime->mutex);
        trevrpc_h3_ingress_api_leave(handle);
        return -err;
    }
    runtime->pump_thread_started = true;
    err = pthread_create(&runtime->accept_thread, NULL, trevrpc_h3_ingress_accept_main, runtime);
    if (err == 0) {
        runtime->accept_thread_started = true;
        runtime->start_in_progress = false;
        pthread_cond_broadcast(&runtime->cond);
        pthread_mutex_unlock(&runtime->mutex);
        trevrpc_h3_ingress_api_leave(handle);
        return 0;
    }

    runtime->stopping = true;
    if (runtime->settings_status == TREV_H3_INGRESS_SETTINGS_PENDING) {
        runtime->settings_status = TREV_H3_INGRESS_SETTINGS_FAILED;
        runtime->settings_error = 0;
    }
    pthread_cond_broadcast(&runtime->cond);
    pthread_cond_broadcast(&runtime->pump_cond);
    pthread_mutex_unlock(&runtime->mutex);
    trevrpc_h3_ingress_stop_without_error(runtime);
    pthread_join(runtime->pump_thread, NULL);

    pthread_mutex_lock(&runtime->mutex);
    runtime->pump_thread_started = false;
    runtime->start_in_progress = false;
    pthread_cond_broadcast(&runtime->cond);
    pthread_mutex_unlock(&runtime->mutex);
    trevrpc_h3_ingress_api_leave(handle);
    return -err;
}

typedef enum trevrpc_h3_ingress_pop_kind {
    TREV_H3_INGRESS_POP_ACTION = 0,
    TREV_H3_INGRESS_POP_CLASSIFIED_BIDI,
    TREV_H3_INGRESS_POP_UNCLASSIFIED_BIDI,
} trevrpc_h3_ingress_pop_kind;

static bool trevrpc_h3_ingress_select_queue(trevrpc_h3_ingress_runtime* runtime,
    trevrpc_h3_ingress_pop_kind kind,
    size_t action_queue_index,
    size_t* out_queue_index) {
    if (kind == TREV_H3_INGRESS_POP_ACTION) {
        if (runtime->queues[action_queue_index].count == 0) {
            return false;
        }
        *out_queue_index = action_queue_index;
        return true;
    }
    if (kind == TREV_H3_INGRESS_POP_UNCLASSIFIED_BIDI) {
        if (runtime->queues[TREV_H3_INGRESS_UNCLASSIFIED_BIDI_QUEUE].count == 0) {
            return false;
        }
        *out_queue_index = TREV_H3_INGRESS_UNCLASSIFIED_BIDI_QUEUE;
        return true;
    }

    size_t request_queue_index = (size_t)trevrpc_h3_ingress_action_queue_index(TREV_H3_DEMUX_ACTION_REQUEST);
    size_t webtransport_queue_index = (size_t)trevrpc_h3_ingress_action_queue_index(TREV_H3_DEMUX_ACTION_WEBTRANSPORT);
    trevrpc_h3_ingress_queue* request_queue = &runtime->queues[request_queue_index];
    trevrpc_h3_ingress_queue* webtransport_queue = &runtime->queues[webtransport_queue_index];
    if (request_queue->count == 0 && webtransport_queue->count == 0) {
        return false;
    }
    if (request_queue->count == 0) {
        *out_queue_index = webtransport_queue_index;
        return true;
    }
    if (webtransport_queue->count == 0) {
        *out_queue_index = request_queue_index;
        return true;
    }

    size_t request_index = request_queue->entries[request_queue->head];
    size_t webtransport_index = webtransport_queue->entries[webtransport_queue->head];
    *out_queue_index =
        runtime->entries[request_index].ready_sequence < runtime->entries[webtransport_index].ready_sequence
            ? request_queue_index
            : webtransport_queue_index;
    return true;
}

static int trevrpc_h3_ingress_pop_internal(trevrpc_h3_ingress* handle,
    trevrpc_h3_ingress_pop_kind kind,
    size_t action_queue_index,
    uint64_t timeout_nanos,
    trevrpc_h3_ingress_item* out_item) {
    trevrpc_h3_ingress_runtime* runtime = NULL;
    int admission_result = trevrpc_h3_ingress_api_enter(handle, &runtime);
    if (admission_result != 0) {
        return admission_result;
    }
    size_t request_queue_index = (size_t)trevrpc_h3_ingress_action_queue_index(TREV_H3_DEMUX_ACTION_REQUEST);
    size_t webtransport_queue_index = (size_t)trevrpc_h3_ingress_action_queue_index(TREV_H3_DEMUX_ACTION_WEBTRANSPORT);
    bool incompatible_action =
        kind == TREV_H3_INGRESS_POP_ACTION && runtime->bidi_mode == TREV_H3_INGRESS_BIDI_HANDOFF_UNCLASSIFIED &&
        (action_queue_index == request_queue_index || action_queue_index == webtransport_queue_index);
    if (out_item == NULL || incompatible_action ||
        (kind == TREV_H3_INGRESS_POP_CLASSIFIED_BIDI && runtime->bidi_mode != TREV_H3_INGRESS_BIDI_CLASSIFY) ||
        (kind == TREV_H3_INGRESS_POP_UNCLASSIFIED_BIDI &&
            runtime->bidi_mode != TREV_H3_INGRESS_BIDI_HANDOFF_UNCLASSIFIED)) {
        trevrpc_h3_ingress_api_leave(handle);
        return -EINVAL;
    }

    struct timespec deadline = {0};
    if (timeout_nanos != 0) {
        int err = trevrpc_h3_ingress_deadline(runtime, timeout_nanos, &deadline);
        if (err != 0) {
            trevrpc_h3_ingress_api_leave(handle);
            return err;
        }
    }

    pthread_mutex_lock(&runtime->mutex);
    runtime->active_waiters++;
    size_t queue_index = 0;
    while (!trevrpc_h3_ingress_select_queue(runtime, kind, action_queue_index, &queue_index) && !runtime->stopping) {
        int err = timeout_nanos == 0
                      ? pthread_cond_wait(&runtime->cond, &runtime->mutex)
                      : trevrpc_h3_ingress_cond_timedwait(runtime, &runtime->cond, &runtime->mutex, &deadline);
        if (err == ETIMEDOUT && !trevrpc_h3_ingress_select_queue(runtime, kind, action_queue_index, &queue_index) &&
            !runtime->stopping) {
            runtime->active_waiters--;
            pthread_cond_broadcast(&runtime->cond);
            pthread_mutex_unlock(&runtime->mutex);
            trevrpc_h3_ingress_api_leave(handle);
            return -ETIMEDOUT;
        }
        if (err != 0 && err != ETIMEDOUT) {
            runtime->active_waiters--;
            pthread_cond_broadcast(&runtime->cond);
            pthread_mutex_unlock(&runtime->mutex);
            trevrpc_h3_ingress_api_leave(handle);
            return -err;
        }
    }
    if (runtime->stopping) {
        int result = runtime->fatal ? -EPROTO : -ECANCELED;
        runtime->active_waiters--;
        pthread_cond_broadcast(&runtime->cond);
        pthread_mutex_unlock(&runtime->mutex);
        trevrpc_h3_ingress_api_leave(handle);
        return result;
    }

    trevrpc_h3_ingress_queue* queue = &runtime->queues[queue_index];
    size_t index = queue->entries[queue->head];
    queue->head = (queue->head + 1) % runtime->max_queue_entries;
    queue->count--;
    trevrpc_h3_ingress_entry* entry = &runtime->entries[index];
    entry->state = TREV_H3_INGRESS_ENTRY_HANDOFF;
    runtime->handoffs_in_progress++;
    void* stream = entry->stream;
    bool observer_installed = entry->observer_installed;
    trevrpc_h3_demux_result result = entry->result;
    trevrpc_h3_ingress_item item = {
        .stream = stream,
        .stream_id = entry->stream_id,
        .accepted_at_nanos = entry->accepted_at_nanos,
        .direction = entry->direction,
        .action = result.action,
        .first_value = result.first_value,
        .session_id = result.session_id,
        ._ops = runtime->ops,
        ._transport_stream = stream,
    };
    pthread_mutex_unlock(&runtime->mutex);

    if (observer_installed) {
        runtime->ops.stream_clear_observer(stream);
        runtime->ops.stream_drain_observer(stream);
    }

    pthread_mutex_lock(&runtime->mutex);
    trevrpc_h3_ingress_reset_entry_locked(runtime, entry);
    *out_item = item;
    runtime->handoffs_in_progress--;
    runtime->active_waiters--;
    pthread_cond_broadcast(&runtime->cond);
    pthread_mutex_unlock(&runtime->mutex);
    trevrpc_h3_ingress_api_leave(handle);
    return 0;
}

int trevrpc_h3_ingress_pop(trevrpc_h3_ingress* handle,
    trevrpc_h3_demux_action action,
    uint64_t timeout_nanos,
    trevrpc_h3_ingress_item* out_item) {
    int queue_index = trevrpc_h3_ingress_action_queue_index(action);
    if (queue_index < 0) {
        return -EINVAL;
    }
    return trevrpc_h3_ingress_pop_internal(
        handle, TREV_H3_INGRESS_POP_ACTION, (size_t)queue_index, timeout_nanos, out_item);
}

int trevrpc_h3_ingress_pop_bidi(trevrpc_h3_ingress* handle, uint64_t timeout_nanos, trevrpc_h3_ingress_item* out_item) {
    return trevrpc_h3_ingress_pop_internal(handle, TREV_H3_INGRESS_POP_CLASSIFIED_BIDI, 0, timeout_nanos, out_item);
}

int trevrpc_h3_ingress_pop_unclassified_bidi(
    trevrpc_h3_ingress* handle, uint64_t timeout_nanos, trevrpc_h3_ingress_item* out_item) {
    return trevrpc_h3_ingress_pop_internal(handle, TREV_H3_INGRESS_POP_UNCLASSIFIED_BIDI, 0, timeout_nanos, out_item);
}

void trevrpc_h3_ingress_item_close(trevrpc_h3_ingress_item* item) {
    if (item == NULL || item->_transport_stream == NULL || item->_ops.stream_close == NULL) {
        return;
    }
    item->_ops.stream_close(item->_transport_stream);
    memset(item, 0, sizeof(*item));
}

trevrpc_msquic_stream* trevrpc_h3_ingress_item_take_stream(trevrpc_h3_ingress_item* item) {
    if (item == NULL) {
        return NULL;
    }
    trevrpc_msquic_stream* stream = item->stream;
    memset(item, 0, sizeof(*item));
    return stream;
}

int trevrpc_h3_ingress_fatal_error(trevrpc_h3_ingress* handle, uint64_t* out_application_error) {
    trevrpc_h3_ingress_runtime* runtime = NULL;
    int result = trevrpc_h3_ingress_api_enter(handle, &runtime);
    if (result != 0) {
        return result;
    }
    if (out_application_error == NULL) {
        trevrpc_h3_ingress_api_leave(handle);
        return -EINVAL;
    }
    pthread_mutex_lock(&runtime->mutex);
    if (!runtime->fatal) {
        pthread_mutex_unlock(&runtime->mutex);
        trevrpc_h3_ingress_api_leave(handle);
        return -EAGAIN;
    }
    *out_application_error = runtime->fatal_error;
    pthread_mutex_unlock(&runtime->mutex);
    trevrpc_h3_ingress_api_leave(handle);
    return 0;
}

int trevrpc_h3_ingress_publish_peer_settings(
    trevrpc_h3_ingress* handle, int status, trevrpc_wt_profile_id selected_profile, uint64_t application_error) {
    trevrpc_h3_ingress_runtime* runtime = NULL;
    int result = trevrpc_h3_ingress_api_enter(handle, &runtime);
    if (result != 0) {
        return result;
    }
    if ((status != TREV_H3_INGRESS_SETTINGS_READY && status != TREV_H3_INGRESS_SETTINGS_FAILED) ||
        (status == TREV_H3_INGRESS_SETTINGS_READY &&
            (application_error != 0 ||
                (selected_profile != TREV_WT_PROFILE_NONE && !trevrpc_wt_profile_is_supported(selected_profile)))) ||
        (status == TREV_H3_INGRESS_SETTINGS_FAILED &&
            (application_error == 0 || selected_profile != TREV_WT_PROFILE_NONE))) {
        trevrpc_h3_ingress_api_leave(handle);
        return -EINVAL;
    }
    if (status == TREV_H3_INGRESS_SETTINGS_FAILED) {
        result = trevrpc_h3_ingress_fail_internal(runtime, application_error, true);
        trevrpc_h3_ingress_api_leave(handle);
        return result;
    }

    pthread_mutex_lock(&runtime->mutex);
    if (runtime->settings_status != TREV_H3_INGRESS_SETTINGS_PENDING) {
        pthread_mutex_unlock(&runtime->mutex);
        trevrpc_h3_ingress_api_leave(handle);
        return -EALREADY;
    }
    runtime->settings_status = TREV_H3_INGRESS_SETTINGS_READY;
    runtime->settings_error = 0;
    runtime->profile.resolved = true;
    runtime->profile.selected_profile = selected_profile;
    for (size_t i = 0; i < runtime->max_live_entries; i++) {
        trevrpc_h3_ingress_entry* entry = &runtime->entries[i];
        if (entry->state == TREV_H3_INGRESS_ENTRY_LIVE && !entry->processing &&
            entry->classifier.phase == TREV_H3_DEMUX_WAIT_NEGOTIATED_PROFILE) {
            entry->pending_flags |= TREV_MSQUIC_STREAM_OBSERVER_READABLE;
        }
    }
    pthread_cond_broadcast(&runtime->cond);
    pthread_cond_broadcast(&runtime->pump_cond);
    pthread_mutex_unlock(&runtime->mutex);
    trevrpc_h3_ingress_api_leave(handle);
    return 0;
}

int trevrpc_h3_ingress_wait_peer_settings(trevrpc_h3_ingress* handle,
    int* out_status,
    trevrpc_wt_profile_id* out_selected_profile,
    uint64_t* out_application_error) {
    trevrpc_h3_ingress_runtime* runtime = NULL;
    int result = trevrpc_h3_ingress_api_enter(handle, &runtime);
    if (result != 0) {
        return result;
    }
    if (out_status == NULL || out_selected_profile == NULL || out_application_error == NULL) {
        trevrpc_h3_ingress_api_leave(handle);
        return -EINVAL;
    }
    pthread_mutex_lock(&runtime->mutex);
    runtime->active_waiters++;
    while (runtime->settings_status == TREV_H3_INGRESS_SETTINGS_PENDING) {
        int err = pthread_cond_wait(&runtime->cond, &runtime->mutex);
        if (err != 0) {
            runtime->active_waiters--;
            pthread_cond_broadcast(&runtime->cond);
            pthread_mutex_unlock(&runtime->mutex);
            trevrpc_h3_ingress_api_leave(handle);
            return -err;
        }
    }
    if (runtime->fatal) {
        *out_status = TREV_H3_INGRESS_SETTINGS_FAILED;
        *out_selected_profile = TREV_WT_PROFILE_NONE;
        *out_application_error = runtime->fatal_error;
    } else {
        *out_status = runtime->settings_status;
        *out_selected_profile = runtime->settings_status == TREV_H3_INGRESS_SETTINGS_READY
                                    ? runtime->profile.selected_profile
                                    : TREV_WT_PROFILE_NONE;
        *out_application_error = runtime->settings_error;
    }
    runtime->active_waiters--;
    pthread_cond_broadcast(&runtime->cond);
    pthread_mutex_unlock(&runtime->mutex);
    trevrpc_h3_ingress_api_leave(handle);
    return 0;
}

static void trevrpc_h3_ingress_shutdown_internal(trevrpc_h3_ingress_runtime* runtime) {
    if (runtime == NULL || runtime == trevrpc_h3_ingress_shutdown_callback_runtime) {
        return;
    }

    pthread_mutex_lock(&runtime->mutex);
    runtime->teardown_calls_active++;
    while (runtime->start_in_progress) {
        pthread_cond_wait(&runtime->cond, &runtime->mutex);
    }
    if (runtime->teardown_started) {
        runtime->teardown_waiters++;
        while (!runtime->teardown_complete) {
            pthread_cond_wait(&runtime->cond, &runtime->mutex);
        }
        runtime->teardown_waiters--;
        runtime->teardown_calls_active--;
        pthread_cond_broadcast(&runtime->cond);
        pthread_mutex_unlock(&runtime->mutex);
        return;
    }
    runtime->teardown_started = true;
    trevrpc_h3_ingress_mark_stopping_locked(runtime);
    bool fatal = runtime->fatal;
    uint64_t fatal_error = runtime->fatal_error;
    bool send_shutdown = !runtime->connection_shutdown_sent;
    if (send_shutdown) {
        runtime->connection_shutdown_sent = true;
        runtime->connection_shutdown_calls_in_progress++;
    }
    pthread_cond_broadcast(&runtime->cond);
    pthread_cond_broadcast(&runtime->pump_cond);
    pthread_mutex_unlock(&runtime->mutex);

    if (send_shutdown) {
        trevrpc_h3_ingress_invoke_shutdown(runtime, fatal, fatal_error);
    }

    if (runtime->accept_thread_started) {
        if (pthread_equal(pthread_self(), runtime->accept_thread)) {
            (void)pthread_detach(runtime->accept_thread);
        } else {
            pthread_join(runtime->accept_thread, NULL);
        }
        runtime->accept_thread_started = false;
    }
    if (runtime->pump_thread_started) {
        if (pthread_equal(pthread_self(), runtime->pump_thread)) {
            (void)pthread_detach(runtime->pump_thread);
        } else {
            pthread_join(runtime->pump_thread, NULL);
        }
        runtime->pump_thread_started = false;
    }

    pthread_mutex_lock(&runtime->mutex);
    while (runtime->handoffs_in_progress != 0 || runtime->active_waiters != 0 ||
           runtime->connection_shutdown_calls_in_progress != 0) {
        pthread_cond_wait(&runtime->cond, &runtime->mutex);
    }
    pthread_mutex_unlock(&runtime->mutex);

    for (size_t i = 0; i < runtime->max_live_entries; i++) {
        trevrpc_h3_ingress_close_entry(runtime, i);
    }

    pthread_mutex_lock(&runtime->mutex);
    for (size_t i = 0; i < TREV_H3_INGRESS_QUEUE_COUNT; i++) {
        runtime->queues[i].head = 0;
        runtime->queues[i].count = 0;
    }
    runtime->teardown_complete = true;
    pthread_cond_broadcast(&runtime->cond);
    while (runtime->teardown_waiters != 0) {
        pthread_cond_wait(&runtime->cond, &runtime->mutex);
    }
    runtime->teardown_calls_active--;
    pthread_cond_broadcast(&runtime->cond);
    pthread_mutex_unlock(&runtime->mutex);
}

void trevrpc_h3_ingress_shutdown(trevrpc_h3_ingress* handle) {
    trevrpc_h3_ingress_runtime* runtime = NULL;
    int result = trevrpc_h3_ingress_api_enter(handle, &runtime);
    if (result != 0) {
        return;
    }
    trevrpc_h3_ingress_shutdown_internal(runtime);
    trevrpc_h3_ingress_api_leave(handle);
}

void trevrpc_h3_ingress_release(trevrpc_h3_ingress* handle) {
    if (handle == NULL) {
        return;
    }

    pthread_mutex_lock(&trevrpc_h3_ingress_registry_mutex);
    trevrpc_h3_ingress** slot = trevrpc_h3_ingress_registry_find_slot(handle);
    if (*slot == NULL || handle->runtime == NULL) {
        pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);
        return;
    }
    trevrpc_h3_ingress_runtime* runtime = handle->runtime;
    bool callback_reentry = runtime == trevrpc_h3_ingress_shutdown_callback_runtime;
    /* Close admission and the runtime state as one ordered transition.  Calls
     * admitted before release may still hold a runtime reference, so mark the
     * runtime stopping before the detach becomes observable to the release
     * waiter or those calls can race ahead and publish new state. */
    pthread_mutex_lock(&runtime->mutex);
    trevrpc_h3_ingress_mark_stopping_locked(runtime);
    pthread_mutex_unlock(&runtime->mutex);
    handle->runtime = NULL;
    handle->retired_runtime = runtime;
    handle->retired_reclaim_deferred = callback_reentry;
    /* Move to the graveyard before the detach broadcast: admission must stop
     * finding this handle, but its memory must stay allocated and reachable
     * so a stale raw pointer cannot alias a future allocation. */
    *slot = handle->registry_next;
    handle->registry_next = trevrpc_h3_ingress_graveyard_head;
    trevrpc_h3_ingress_graveyard_head = handle;
#ifdef TREVRPC_H3_INGRESS_TESTING
    trevrpc_h3_ingress_test_released_count++;
#endif
    pthread_cond_broadcast(&trevrpc_h3_ingress_registry_cond);
    pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);

    if (callback_reentry) {
        return;
    }

    trevrpc_h3_ingress_shutdown_internal(runtime);

    pthread_mutex_lock(&trevrpc_h3_ingress_registry_mutex);
    while (handle->api_references != 0) {
        pthread_cond_wait(&trevrpc_h3_ingress_registry_cond, &trevrpc_h3_ingress_registry_mutex);
    }
    if (handle->retired_runtime == runtime) {
        handle->retired_runtime = NULL;
    }
    pthread_mutex_unlock(&trevrpc_h3_ingress_registry_mutex);
    trevrpc_h3_ingress_reclaim_runtime(runtime);
}
