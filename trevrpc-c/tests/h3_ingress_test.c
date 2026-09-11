#define _POSIX_C_SOURCE 200809L

#include "trevrpc_h3_ingress_internal.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#define TEST_TIMEOUT_NANOS 5000000000u
#define FAKE_MAX_STREAMS 16u
#define FAKE_STREAM_BYTES 64u

#define CHECK_GOTO(condition)                                                                                          \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            goto cleanup;                                                                                              \
        }                                                                                                              \
    } while (0)

typedef struct fake_stream {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    uint64_t id;
    uint8_t bytes[FAKE_STREAM_BYTES];
    size_t len;
    size_t offset;
    size_t max_read_size;
    size_t observer_set_calls;
    intptr_t read_error;
    trevrpc_h3_ingress_observer observer;
    void* observer_context;
    size_t active_callbacks;
    size_t clear_calls;
    size_t drain_calls;
    size_t abort_calls;
    uint64_t abort_application_error;
    size_t close_calls;
    int id_error;
    int observer_error;
    int abort_error;
    bool close_observed_abort;
    bool terminal;
    bool notify_on_observer_install;
    bool block_observer_set;
    bool observer_set_entered;
    bool release_observer_set;
    bool block_callback;
    bool callback_entered;
    bool release_callback;
    bool block_drain;
    bool drain_entered;
    bool release_drain;
} fake_stream;

typedef struct fake_conn {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    fake_stream* streams[FAKE_MAX_STREAMS];
    size_t head;
    size_t count;
    size_t shutdown_calls;
    size_t shutdown_error_calls;
    int accept_error;
    uint64_t first_error;
    bool shutdown;
    bool block_shutdown;
    bool shutdown_entered;
    bool release_shutdown;
    trevrpc_h3_ingress* release_runtime_on_shutdown;
    bool release_runtime_returned;
} fake_conn;

typedef struct settings_wait {
    trevrpc_h3_ingress* runtime;
    int result;
    int status;
    trevrpc_wt_profile_id selected_profile;
    uint64_t application_error;
} settings_wait;

typedef enum pop_wait_kind {
    POP_WAIT_ACTION = 0,
    POP_WAIT_CLASSIFIED_BIDI,
    POP_WAIT_UNCLASSIFIED_BIDI,
} pop_wait_kind;

typedef struct pop_wait {
    trevrpc_h3_ingress* runtime;
    pop_wait_kind kind;
    uint64_t timeout_nanos;
    int result;
    trevrpc_h3_ingress_item item;
} pop_wait;

typedef struct fail_race {
    trevrpc_h3_ingress* runtime;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    size_t ready;
    bool go;
    uint64_t errors[2];
    int results[2];
} fail_race;

typedef struct fail_race_arg {
    fail_race* race;
    size_t index;
} fail_race_arg;

typedef struct start_wait {
    trevrpc_h3_ingress* runtime;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int result;
    bool ready;
    bool go;
} start_wait;

typedef struct lifecycle_wait {
    trevrpc_h3_ingress* runtime;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool release_runtime;
    bool started;
    bool finished;
} lifecycle_wait;

typedef enum admitted_api_kind {
    ADMITTED_API_START = 0,
    ADMITTED_API_POP,
    ADMITTED_API_POP_BIDI,
    ADMITTED_API_POP_UNCLASSIFIED,
    ADMITTED_API_FAIL,
    ADMITTED_API_FATAL_ERROR,
    ADMITTED_API_PUBLISH_SETTINGS,
    ADMITTED_API_WAIT_SETTINGS,
    ADMITTED_API_SHUTDOWN,
} admitted_api_kind;

typedef struct admitted_api_call {
    trevrpc_h3_ingress* runtime;
    admitted_api_kind kind;
    int result;
    trevrpc_h3_ingress_item item;
    int settings_status;
    trevrpc_wt_profile_id selected_profile;
    uint64_t application_error;
} admitted_api_call;

static int test_deadline(struct timespec* deadline) {
    if (clock_gettime(CLOCK_REALTIME, deadline) != 0) {
        return -errno;
    }
    deadline->tv_sec++;
    return 0;
}

static int test_monotonic_nanos(uint64_t* out_nanos) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        return -errno;
    }
    *out_nanos = (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
    return 0;
}

static int fake_conn_init(fake_conn* conn) {
    memset(conn, 0, sizeof(*conn));
    int err = pthread_mutex_init(&conn->mutex, NULL);
    if (err != 0) {
        return -err;
    }
    err = pthread_cond_init(&conn->cond, NULL);
    if (err != 0) {
        pthread_mutex_destroy(&conn->mutex);
        return -err;
    }
    return 0;
}

static void fake_conn_destroy(fake_conn* conn) {
    pthread_cond_destroy(&conn->cond);
    pthread_mutex_destroy(&conn->mutex);
}

static int fake_stream_init(fake_stream* stream, uint64_t id, const uint8_t* bytes, size_t len, bool terminal) {
    if (len > sizeof(stream->bytes)) {
        return -EINVAL;
    }
    memset(stream, 0, sizeof(*stream));
    int err = pthread_mutex_init(&stream->mutex, NULL);
    if (err != 0) {
        return -err;
    }
    err = pthread_cond_init(&stream->cond, NULL);
    if (err != 0) {
        pthread_mutex_destroy(&stream->mutex);
        return -err;
    }
    stream->id = id;
    if (len != 0) {
        memcpy(stream->bytes, bytes, len);
    }
    stream->len = len;
    stream->terminal = terminal;
    stream->notify_on_observer_install = true;
    return 0;
}

static void fake_stream_destroy(fake_stream* stream) {
    pthread_cond_destroy(&stream->cond);
    pthread_mutex_destroy(&stream->mutex);
}

static int fake_conn_enqueue(fake_conn* conn, fake_stream* stream) {
    pthread_mutex_lock(&conn->mutex);
    if (conn->count == FAKE_MAX_STREAMS || conn->shutdown) {
        pthread_mutex_unlock(&conn->mutex);
        return -ENOSPC;
    }
    size_t tail = (conn->head + conn->count) % FAKE_MAX_STREAMS;
    conn->streams[tail] = stream;
    conn->count++;
    pthread_cond_broadcast(&conn->cond);
    pthread_mutex_unlock(&conn->mutex);
    return 0;
}

static int fake_accept(void* context, void** out_stream) {
    fake_conn* conn = context;
    *out_stream = NULL;
    pthread_mutex_lock(&conn->mutex);
    if (conn->accept_error != 0) {
        int error = conn->accept_error;
        conn->accept_error = 0;
        pthread_mutex_unlock(&conn->mutex);
        return error;
    }
    while (conn->count == 0 && !conn->shutdown) {
        pthread_cond_wait(&conn->cond, &conn->mutex);
    }
    if (conn->count == 0) {
        pthread_mutex_unlock(&conn->mutex);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    fake_stream* stream = conn->streams[conn->head];
    conn->head = (conn->head + 1) % FAKE_MAX_STREAMS;
    conn->count--;
    pthread_mutex_unlock(&conn->mutex);
    *out_stream = stream;
    return 0;
}

static void fake_shutdown(void* context) {
    fake_conn* conn = context;
    pthread_mutex_lock(&conn->mutex);
    conn->shutdown_calls++;
    conn->shutdown = true;
    conn->shutdown_entered = true;
    trevrpc_h3_ingress* release_runtime = conn->release_runtime_on_shutdown;
    pthread_cond_broadcast(&conn->cond);
    if (release_runtime != NULL) {
        pthread_mutex_unlock(&conn->mutex);
        trevrpc_h3_ingress_release(release_runtime);
        pthread_mutex_lock(&conn->mutex);
        conn->release_runtime_returned = true;
        pthread_cond_broadcast(&conn->cond);
        pthread_mutex_unlock(&conn->mutex);
        return;
    }
    while (conn->block_shutdown && !conn->release_shutdown) {
        pthread_cond_wait(&conn->cond, &conn->mutex);
    }
    pthread_mutex_unlock(&conn->mutex);
}

static void fake_shutdown_error(void* context, uint64_t application_error) {
    fake_conn* conn = context;
    pthread_mutex_lock(&conn->mutex);
    conn->shutdown_error_calls++;
    if (conn->first_error == 0) {
        conn->first_error = application_error;
    }
    conn->shutdown = true;
    conn->shutdown_entered = true;
    trevrpc_h3_ingress* release_runtime = conn->release_runtime_on_shutdown;
    pthread_cond_broadcast(&conn->cond);
    if (release_runtime != NULL) {
        pthread_mutex_unlock(&conn->mutex);
        trevrpc_h3_ingress_release(release_runtime);
        pthread_mutex_lock(&conn->mutex);
        conn->release_runtime_returned = true;
        pthread_cond_broadcast(&conn->cond);
        pthread_mutex_unlock(&conn->mutex);
        return;
    }
    while (conn->block_shutdown && !conn->release_shutdown) {
        pthread_cond_wait(&conn->cond, &conn->mutex);
    }
    pthread_mutex_unlock(&conn->mutex);
}

static int fake_stream_id(void* context, uint64_t* out_stream_id) {
    fake_stream* stream = context;
    if (stream->id_error != 0) {
        return stream->id_error;
    }
    *out_stream_id = stream->id;
    return 0;
}

static void fake_stream_notify(fake_stream* stream, uint32_t flags) {
    pthread_mutex_lock(&stream->mutex);
    trevrpc_h3_ingress_observer observer = stream->observer;
    void* observer_context = stream->observer_context;
    if (observer != NULL) {
        stream->active_callbacks++;
    }
    pthread_mutex_unlock(&stream->mutex);

    if (observer != NULL) {
        observer(observer_context, flags);
        pthread_mutex_lock(&stream->mutex);
        stream->callback_entered = true;
        pthread_cond_broadcast(&stream->cond);
        while (stream->block_callback && !stream->release_callback) {
            pthread_cond_wait(&stream->cond, &stream->mutex);
        }
        stream->active_callbacks--;
        pthread_cond_broadcast(&stream->cond);
        pthread_mutex_unlock(&stream->mutex);
    }
}

static int fake_set_observer(void* context, trevrpc_h3_ingress_observer observer, void* observer_context) {
    fake_stream* stream = context;
    uint32_t flags = 0;
    pthread_mutex_lock(&stream->mutex);
    stream->observer_set_calls++;
    stream->observer_set_entered = true;
    pthread_cond_broadcast(&stream->cond);
    while (stream->block_observer_set && !stream->release_observer_set) {
        pthread_cond_wait(&stream->cond, &stream->mutex);
    }
    if (stream->observer_error != 0) {
        int error = stream->observer_error;
        pthread_mutex_unlock(&stream->mutex);
        return error;
    }
    if (stream->observer != NULL) {
        pthread_mutex_unlock(&stream->mutex);
        return -EBUSY;
    }
    stream->observer = observer;
    stream->observer_context = observer_context;
    if (stream->offset < stream->len) {
        flags |= TREV_MSQUIC_STREAM_OBSERVER_READABLE;
    }
    if (stream->terminal || stream->read_error != 0) {
        flags |= TREV_MSQUIC_STREAM_OBSERVER_TERMINAL;
    }
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);
    if (flags != 0 && stream->notify_on_observer_install) {
        fake_stream_notify(stream, flags);
    }
    return 0;
}

static void fake_clear_observer(void* context) {
    fake_stream* stream = context;
    pthread_mutex_lock(&stream->mutex);
    stream->observer = NULL;
    stream->observer_context = NULL;
    stream->clear_calls++;
    pthread_mutex_unlock(&stream->mutex);
}

static void fake_drain_observer(void* context) {
    fake_stream* stream = context;
    pthread_mutex_lock(&stream->mutex);
    stream->drain_calls++;
    stream->drain_entered = true;
    pthread_cond_broadcast(&stream->cond);
    while (stream->active_callbacks != 0 || (stream->block_drain && !stream->release_drain)) {
        pthread_cond_wait(&stream->cond, &stream->mutex);
    }
    pthread_mutex_unlock(&stream->mutex);
}

static intptr_t fake_read_ready(void* context, uint8_t* data, size_t len) {
    fake_stream* stream = context;
    pthread_mutex_lock(&stream->mutex);
    if (len > stream->max_read_size) {
        stream->max_read_size = len;
    }
    if (stream->offset < stream->len) {
        size_t available = stream->len - stream->offset;
        size_t count = available < len ? available : len;
        memcpy(data, stream->bytes + stream->offset, count);
        stream->offset += count;
        pthread_cond_broadcast(&stream->cond);
        pthread_mutex_unlock(&stream->mutex);
        return (intptr_t)count;
    }
    intptr_t read_error = stream->read_error;
    bool terminal = stream->terminal;
    pthread_mutex_unlock(&stream->mutex);
    return read_error != 0 ? read_error : (terminal ? 0 : TREV_MSQUIC_ERR_TIMEOUT);
}

static int fake_stream_abort_with_error(void* context, uint64_t application_error) {
    fake_stream* stream = context;
    pthread_mutex_lock(&stream->mutex);
    stream->abort_calls++;
    stream->abort_application_error = application_error;
    int error = stream->abort_error;
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);
    return error;
}

static void fake_stream_close(void* context) {
    fake_stream* stream = context;
    pthread_mutex_lock(&stream->mutex);
    stream->close_calls++;
    stream->close_observed_abort = stream->abort_calls != 0;
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);
}

static const trevrpc_h3_ingress_transport_ops fake_ops = {
    .conn_accept_stream = fake_accept,
    .conn_shutdown = fake_shutdown,
    .conn_shutdown_error = fake_shutdown_error,
    .stream_id = fake_stream_id,
    .stream_set_observer = fake_set_observer,
    .stream_clear_observer = fake_clear_observer,
    .stream_drain_observer = fake_drain_observer,
    .stream_read_protocol_ready = fake_read_ready,
    .stream_abort_with_error = fake_stream_abort_with_error,
    .stream_close = fake_stream_close,
};

static trevrpc_h3_ingress_config test_config(size_t live, size_t queued) {
    return (trevrpc_h3_ingress_config){
        .max_live_entries = live,
        .max_queue_entries = queued,
    };
}

static int fake_stream_wait_close_calls(fake_stream* stream, size_t calls) {
    struct timespec deadline;
    int err = test_deadline(&deadline);
    if (err != 0) {
        return err;
    }
    pthread_mutex_lock(&stream->mutex);
    while (stream->close_calls < calls) {
        err = pthread_cond_timedwait(&stream->cond, &stream->mutex, &deadline);
        if (err != 0) {
            pthread_mutex_unlock(&stream->mutex);
            return err == ETIMEDOUT ? -ETIMEDOUT : -err;
        }
    }
    pthread_mutex_unlock(&stream->mutex);
    return 0;
}

static int fake_stream_wait_observer_set(fake_stream* stream) {
    struct timespec deadline;
    int err = test_deadline(&deadline);
    if (err != 0) {
        return err;
    }
    pthread_mutex_lock(&stream->mutex);
    while (stream->observer_set_calls == 0) {
        err = pthread_cond_timedwait(&stream->cond, &stream->mutex, &deadline);
        if (err != 0) {
            pthread_mutex_unlock(&stream->mutex);
            return err == ETIMEDOUT ? -ETIMEDOUT : -err;
        }
    }
    pthread_mutex_unlock(&stream->mutex);
    return 0;
}

static int fake_stream_wait_observer_set_entered(fake_stream* stream) {
    struct timespec deadline;
    int err = test_deadline(&deadline);
    if (err != 0) {
        return err;
    }
    pthread_mutex_lock(&stream->mutex);
    while (!stream->observer_set_entered) {
        err = pthread_cond_timedwait(&stream->cond, &stream->mutex, &deadline);
        if (err != 0) {
            pthread_mutex_unlock(&stream->mutex);
            return err == ETIMEDOUT ? -ETIMEDOUT : -err;
        }
    }
    pthread_mutex_unlock(&stream->mutex);
    return 0;
}

static int fake_stream_wait_callback_entered(fake_stream* stream) {
    struct timespec deadline;
    int err = test_deadline(&deadline);
    if (err != 0) {
        return err;
    }
    pthread_mutex_lock(&stream->mutex);
    while (!stream->callback_entered) {
        err = pthread_cond_timedwait(&stream->cond, &stream->mutex, &deadline);
        if (err != 0) {
            pthread_mutex_unlock(&stream->mutex);
            return err == ETIMEDOUT ? -ETIMEDOUT : -err;
        }
    }
    pthread_mutex_unlock(&stream->mutex);
    return 0;
}

static void fake_stream_release_observer_set(fake_stream* stream) {
    pthread_mutex_lock(&stream->mutex);
    stream->release_observer_set = true;
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);
}

static void fake_stream_release_callback(fake_stream* stream) {
    pthread_mutex_lock(&stream->mutex);
    stream->release_callback = true;
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);
}

static int fake_stream_wait_offset(fake_stream* stream, size_t offset) {
    struct timespec deadline;
    int err = test_deadline(&deadline);
    if (err != 0) {
        return err;
    }
    pthread_mutex_lock(&stream->mutex);
    while (stream->offset < offset) {
        err = pthread_cond_timedwait(&stream->cond, &stream->mutex, &deadline);
        if (err != 0) {
            pthread_mutex_unlock(&stream->mutex);
            return err == ETIMEDOUT ? -ETIMEDOUT : -err;
        }
    }
    pthread_mutex_unlock(&stream->mutex);
    return 0;
}

static int fake_stream_wait_drain(fake_stream* stream) {
    struct timespec deadline;
    int err = test_deadline(&deadline);
    if (err != 0) {
        return err;
    }
    pthread_mutex_lock(&stream->mutex);
    while (!stream->drain_entered) {
        err = pthread_cond_timedwait(&stream->cond, &stream->mutex, &deadline);
        if (err != 0) {
            pthread_mutex_unlock(&stream->mutex);
            return err == ETIMEDOUT ? -ETIMEDOUT : -err;
        }
    }
    pthread_mutex_unlock(&stream->mutex);
    return 0;
}

static void fake_stream_release_drain(fake_stream* stream) {
    pthread_mutex_lock(&stream->mutex);
    stream->release_drain = true;
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);
}

static int fake_conn_wait_shutdown(fake_conn* conn) {
    struct timespec deadline;
    int err = test_deadline(&deadline);
    if (err != 0) {
        return err;
    }
    pthread_mutex_lock(&conn->mutex);
    while (!conn->shutdown_entered) {
        err = pthread_cond_timedwait(&conn->cond, &conn->mutex, &deadline);
        if (err != 0) {
            pthread_mutex_unlock(&conn->mutex);
            return err == ETIMEDOUT ? -ETIMEDOUT : -err;
        }
    }
    pthread_mutex_unlock(&conn->mutex);
    return 0;
}

static int fake_conn_wait_shutdown_error_calls(fake_conn* conn, size_t calls) {
    struct timespec deadline;
    int err = test_deadline(&deadline);
    if (err != 0) {
        return err;
    }
    pthread_mutex_lock(&conn->mutex);
    while (conn->shutdown_error_calls < calls) {
        err = pthread_cond_timedwait(&conn->cond, &conn->mutex, &deadline);
        if (err != 0) {
            pthread_mutex_unlock(&conn->mutex);
            return err == ETIMEDOUT ? -ETIMEDOUT : -err;
        }
    }
    pthread_mutex_unlock(&conn->mutex);
    return 0;
}

static void fake_conn_release_shutdown(fake_conn* conn) {
    pthread_mutex_lock(&conn->mutex);
    conn->release_shutdown = true;
    pthread_cond_broadcast(&conn->cond);
    pthread_mutex_unlock(&conn->mutex);
}

static size_t fake_conn_shutdown_calls(fake_conn* conn) {
    pthread_mutex_lock(&conn->mutex);
    size_t calls = conn->shutdown_calls;
    pthread_mutex_unlock(&conn->mutex);
    return calls;
}

static size_t fake_conn_shutdown_error_calls(fake_conn* conn) {
    pthread_mutex_lock(&conn->mutex);
    size_t calls = conn->shutdown_error_calls;
    pthread_mutex_unlock(&conn->mutex);
    return calls;
}

static uint64_t fake_conn_first_error(fake_conn* conn) {
    pthread_mutex_lock(&conn->mutex);
    uint64_t error = conn->first_error;
    pthread_mutex_unlock(&conn->mutex);
    return error;
}

static int fake_stream_append(fake_stream* stream, const uint8_t* bytes, size_t len, uint32_t flags) {
    pthread_mutex_lock(&stream->mutex);
    if (len > sizeof(stream->bytes) - stream->len) {
        pthread_mutex_unlock(&stream->mutex);
        return -ENOSPC;
    }
    memcpy(stream->bytes + stream->len, bytes, len);
    stream->len += len;
    pthread_mutex_unlock(&stream->mutex);
    fake_stream_notify(stream, flags);
    return 0;
}

static void* settings_wait_main(void* context) {
    settings_wait* wait = context;
    wait->result = trevrpc_h3_ingress_wait_peer_settings(
        wait->runtime, &wait->status, &wait->selected_profile, &wait->application_error);
    return NULL;
}

static void* fail_race_main(void* context) {
    fail_race_arg* arg = context;
    fail_race* race = arg->race;
    pthread_mutex_lock(&race->mutex);
    race->ready++;
    pthread_cond_broadcast(&race->cond);
    while (!race->go) {
        pthread_cond_wait(&race->cond, &race->mutex);
    }
    pthread_mutex_unlock(&race->mutex);
    race->results[arg->index] = trevrpc_h3_ingress_fail(race->runtime, race->errors[arg->index]);
    return NULL;
}

static void* pop_wait_main(void* context) {
    pop_wait* wait = context;
    switch (wait->kind) {
    case POP_WAIT_ACTION:
        wait->result =
            trevrpc_h3_ingress_pop(wait->runtime, TREV_H3_DEMUX_ACTION_REQUEST, wait->timeout_nanos, &wait->item);
        break;
    case POP_WAIT_CLASSIFIED_BIDI:
        wait->result = trevrpc_h3_ingress_pop_bidi(wait->runtime, wait->timeout_nanos, &wait->item);
        break;
    case POP_WAIT_UNCLASSIFIED_BIDI:
        wait->result = trevrpc_h3_ingress_pop_unclassified_bidi(wait->runtime, wait->timeout_nanos, &wait->item);
        break;
    }
    return NULL;
}

static void* admitted_api_call_main(void* context) {
    admitted_api_call* call = context;
    switch (call->kind) {
    case ADMITTED_API_START:
        call->result = trevrpc_h3_ingress_start(call->runtime);
        break;
    case ADMITTED_API_POP:
        call->result =
            trevrpc_h3_ingress_pop(call->runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &call->item);
        break;
    case ADMITTED_API_POP_BIDI:
        call->result = trevrpc_h3_ingress_pop_bidi(call->runtime, TEST_TIMEOUT_NANOS, &call->item);
        break;
    case ADMITTED_API_POP_UNCLASSIFIED:
        call->result = trevrpc_h3_ingress_pop_unclassified_bidi(call->runtime, TEST_TIMEOUT_NANOS, &call->item);
        break;
    case ADMITTED_API_FAIL:
        call->result = trevrpc_h3_ingress_fail(call->runtime, TREV_H3_DEMUX_APP_FRAME_ERROR);
        break;
    case ADMITTED_API_FATAL_ERROR:
        call->result = trevrpc_h3_ingress_fatal_error(call->runtime, &call->application_error);
        break;
    case ADMITTED_API_PUBLISH_SETTINGS:
        call->result = trevrpc_h3_ingress_publish_peer_settings(
            call->runtime, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_DRAFT_15, 0);
        break;
    case ADMITTED_API_WAIT_SETTINGS:
        call->result = trevrpc_h3_ingress_wait_peer_settings(
            call->runtime, &call->settings_status, &call->selected_profile, &call->application_error);
        break;
    case ADMITTED_API_SHUTDOWN:
        trevrpc_h3_ingress_shutdown(call->runtime);
        call->result = 0;
        break;
    }
    return NULL;
}

static void* stream_readable_notify_main(void* context) {
    fake_stream_notify(context, TREV_MSQUIC_STREAM_OBSERVER_READABLE);
    return NULL;
}

static void* start_wait_main(void* context) {
    start_wait* wait = context;
    pthread_mutex_lock(&wait->mutex);
    wait->ready = true;
    pthread_cond_broadcast(&wait->cond);
    while (!wait->go) {
        pthread_cond_wait(&wait->cond, &wait->mutex);
    }
    pthread_mutex_unlock(&wait->mutex);
    wait->result = trevrpc_h3_ingress_start(wait->runtime);
    return NULL;
}

static int lifecycle_wait_init(lifecycle_wait* wait, trevrpc_h3_ingress* runtime, bool release_runtime) {
    memset(wait, 0, sizeof(*wait));
    int err = pthread_mutex_init(&wait->mutex, NULL);
    if (err != 0) {
        return -err;
    }
    err = pthread_cond_init(&wait->cond, NULL);
    if (err != 0) {
        pthread_mutex_destroy(&wait->mutex);
        return -err;
    }
    wait->runtime = runtime;
    wait->release_runtime = release_runtime;
    return 0;
}

static void lifecycle_wait_destroy(lifecycle_wait* wait) {
    pthread_cond_destroy(&wait->cond);
    pthread_mutex_destroy(&wait->mutex);
}

static void* lifecycle_wait_main(void* context) {
    lifecycle_wait* wait = context;
    pthread_mutex_lock(&wait->mutex);
    wait->started = true;
    pthread_cond_broadcast(&wait->cond);
    pthread_mutex_unlock(&wait->mutex);
    if (wait->release_runtime) {
        trevrpc_h3_ingress_release(wait->runtime);
    } else {
        trevrpc_h3_ingress_shutdown(wait->runtime);
    }
    pthread_mutex_lock(&wait->mutex);
    wait->finished = true;
    pthread_cond_broadcast(&wait->cond);
    pthread_mutex_unlock(&wait->mutex);
    return NULL;
}

static void lifecycle_wait_started(lifecycle_wait* wait) {
    pthread_mutex_lock(&wait->mutex);
    while (!wait->started) {
        pthread_cond_wait(&wait->cond, &wait->mutex);
    }
    pthread_mutex_unlock(&wait->mutex);
}

static bool lifecycle_wait_finished(lifecycle_wait* wait) {
    pthread_mutex_lock(&wait->mutex);
    bool finished = wait->finished;
    pthread_mutex_unlock(&wait->mutex);
    return finished;
}

static int test_handoff_unclassified_bidi_skips_classifier(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream bidi;
    fake_stream control;
    bool bidi_initialized = false;
    bool control_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    const uint8_t request_bytes[] = {0x01, 0xa0};
    const uint8_t control_bytes[] = {0x00, 0xa1};
    uint64_t before = 0;
    uint64_t after = 0;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&bidi, 0, request_bytes, sizeof(request_bytes), false) == 0);
    bidi_initialized = true;
    bidi.observer_error = -EIO;
    bidi.read_error = -ENOMEM;
    CHECK_GOTO(fake_stream_init(&control, 2, control_bytes, sizeof(control_bytes), false) == 0);
    control_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &bidi) == 0);
    CHECK_GOTO(fake_conn_enqueue(&conn, &control) == 0);
    CHECK_GOTO(test_monotonic_nanos(&before) == 0);

    trevrpc_h3_ingress_config config = test_config(2, 2);
    config.bidi_mode = TREV_H3_INGRESS_BIDI_HANDOFF_UNCLASSIFIED;
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop_unclassified_bidi(runtime, TEST_TIMEOUT_NANOS, &item) == 0);
    CHECK_GOTO(test_monotonic_nanos(&after) == 0);
    CHECK_GOTO(item._transport_stream == &bidi);
    CHECK_GOTO(item.stream_id == 0);
    CHECK_GOTO(item.accepted_at_nanos >= before && item.accepted_at_nanos <= after);
    CHECK_GOTO(item.direction == TREV_H3_DEMUX_BIDIRECTIONAL);
    CHECK_GOTO(item.action == TREV_H3_DEMUX_ACTION_NONE);
    CHECK_GOTO(bidi.offset == 0 && bidi.max_read_size == 0);
    CHECK_GOTO(bidi.observer == NULL && bidi.observer_set_calls == 0);
    CHECK_GOTO(bidi.clear_calls == 0 && bidi.drain_calls == 0);
    trevrpc_h3_ingress_item_close(&item);
    CHECK_GOTO(bidi.close_calls == 1);

    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_CONTROL, TEST_TIMEOUT_NANOS, &item) == 0);
    CHECK_GOTO(item._transport_stream == &control);
    CHECK_GOTO(item.accepted_at_nanos == 0);
    CHECK_GOTO(control.offset == 1);
    CHECK_GOTO(control.clear_calls == 1 && control.drain_calls == 1);
    trevrpc_h3_ingress_item_close(&item);
    CHECK_GOTO(control.close_calls == 1);

    item = (trevrpc_h3_ingress_item){
        .stream_id = UINT64_C(0x1234),
        .accepted_at_nanos = UINT64_C(0x5678),
    };
    trevrpc_h3_ingress_item unchanged = item;
    CHECK_GOTO(trevrpc_h3_ingress_pop_bidi(runtime, 1, &item) == -EINVAL);
    CHECK_GOTO(memcmp(&item, &unchanged, sizeof(item)) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_REQUEST, 1, &item) == -EINVAL);
    CHECK_GOTO(memcmp(&item, &unchanged, sizeof(item)) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_WEBTRANSPORT, 1, &item) == -EINVAL);
    CHECK_GOTO(memcmp(&item, &unchanged, sizeof(item)) == 0);
    item = (trevrpc_h3_ingress_item){0};
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (control_initialized) {
        fake_stream_destroy(&control);
    }
    if (bidi_initialized) {
        fake_stream_destroy(&bidi);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_transport_ops_and_item_own_callback_table(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream stream;
    bool stream_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    const uint8_t request[] = {0x01};

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&stream, 0, request, sizeof(request), false) == 0);
    stream_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &stream) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    trevrpc_h3_ingress_transport_ops missing_abort_ops = fake_ops;
    missing_abort_ops.stream_abort_with_error = NULL;
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &missing_abort_ops, &config, &runtime) == -EINVAL);
    CHECK_GOTO(runtime == NULL);
    trevrpc_h3_ingress_transport_ops local_ops = fake_ops;
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &local_ops, &config, &runtime) == 0);
    memset(&local_ops, 0, sizeof(local_ops));
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &item) == 0);
    trevrpc_h3_ingress_release(runtime);
    runtime = NULL;
    trevrpc_h3_ingress_item_close(&item);
    CHECK_GOTO(stream.close_calls == 1);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (stream_initialized) {
        fake_stream_destroy(&stream);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_closed_stream_id_retires_only_accepted_stream(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream retired;
    fake_stream request;
    bool retired_initialized = false;
    bool request_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    uint64_t fatal_error = 0;
    const uint8_t request_bytes[] = {0x01};

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&retired, 0, NULL, 0, false) == 0);
    retired_initialized = true;
    retired.id_error = TREV_MSQUIC_ERR_CLOSED;
    CHECK_GOTO(fake_stream_init(&request, 4, request_bytes, sizeof(request_bytes), false) == 0);
    request_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &retired) == 0);
    CHECK_GOTO(fake_conn_enqueue(&conn, &request) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &item) == 0);
    CHECK_GOTO(item._transport_stream == &request);
    CHECK_GOTO(retired.close_calls == 1);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(runtime, &fatal_error) == -EAGAIN);
    CHECK_GOTO(fake_conn_shutdown_calls(&conn) == 0 && fake_conn_shutdown_error_calls(&conn) == 0);
    trevrpc_h3_ingress_item_close(&item);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (request_initialized) {
        fake_stream_destroy(&request);
    }
    if (retired_initialized) {
        fake_stream_destroy(&retired);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_classifies_and_hands_off_without_overread(void) {
    int result = 1;
    fake_conn conn;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    const uint8_t control_bytes[] = {0x00, 0xa0};
    const uint8_t encoder_bytes[] = {0x02, 0xa1};
    const uint8_t decoder_bytes[] = {0x03, 0xa2};
    const uint8_t request_bytes[] = {0x01, 0xa3};
    const uint8_t webtransport_bytes[] = {0x40, 0x41, 0x40, 0x40, 0xa4};
    fake_stream streams[5];
    size_t initialized = 0;
    CHECK_GOTO(fake_conn_init(&conn) == 0);
    CHECK_GOTO(fake_stream_init(&streams[0], 2, control_bytes, sizeof(control_bytes), false) == 0);
    initialized++;
    CHECK_GOTO(fake_stream_init(&streams[1], 6, encoder_bytes, sizeof(encoder_bytes), false) == 0);
    initialized++;
    CHECK_GOTO(fake_stream_init(&streams[2], 10, decoder_bytes, sizeof(decoder_bytes), false) == 0);
    initialized++;
    CHECK_GOTO(fake_stream_init(&streams[3], 0, request_bytes, sizeof(request_bytes), false) == 0);
    initialized++;
    CHECK_GOTO(fake_stream_init(&streams[4], 4, webtransport_bytes, sizeof(webtransport_bytes), false) == 0);
    initialized++;
    const size_t shuffled_arrival[] = {4, 2, 0, 3, 1};
    for (size_t i = 0; i < initialized; i++) {
        CHECK_GOTO(fake_conn_enqueue(&conn, &streams[shuffled_arrival[i]]) == 0);
    }

    trevrpc_h3_ingress_config config = test_config(8, 4);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   runtime, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_DRAFT_15, 0) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);

    const trevrpc_h3_demux_action actions[] = {
        TREV_H3_DEMUX_ACTION_CONTROL,
        TREV_H3_DEMUX_ACTION_QPACK_ENCODER,
        TREV_H3_DEMUX_ACTION_QPACK_DECODER,
        TREV_H3_DEMUX_ACTION_REQUEST,
        TREV_H3_DEMUX_ACTION_WEBTRANSPORT,
    };
    const size_t expected_offsets[] = {1, 1, 1, 1, 4};
    for (size_t i = 0; i < initialized; i++) {
        CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, actions[i], TEST_TIMEOUT_NANOS, &item) == 0);
        CHECK_GOTO(item._transport_stream == &streams[i]);
        CHECK_GOTO(item.stream_id == streams[i].id);
        CHECK_GOTO(item.action == actions[i]);
        CHECK_GOTO(item.direction == (i < 3 ? TREV_H3_DEMUX_UNIDIRECTIONAL : TREV_H3_DEMUX_BIDIRECTIONAL));
        CHECK_GOTO(streams[i].offset == expected_offsets[i]);
        CHECK_GOTO(streams[i].clear_calls == 1);
        CHECK_GOTO(streams[i].drain_calls == 1);
        if (i == 4) {
            CHECK_GOTO(item.first_value == TREV_H3_DEMUX_WEBTRANSPORT_BIDI);
            CHECK_GOTO(item.session_id == 64);
        }
        if (i == 3) {
            CHECK_GOTO(trevrpc_h3_ingress_item_take_stream(&item) == (trevrpc_msquic_stream*)&streams[i]);
            CHECK_GOTO(item.stream == NULL && item._transport_stream == NULL);
            fake_stream_close(&streams[i]);
        } else {
            trevrpc_h3_ingress_item_close(&item);
        }
        CHECK_GOTO(streams[i].close_calls == 1);
    }

    item = (trevrpc_h3_ingress_item){.stream_id = UINT64_C(0x1234)};
    trevrpc_h3_ingress_item unchanged = item;
    CHECK_GOTO(trevrpc_h3_ingress_pop_unclassified_bidi(runtime, 1, &item) == -EINVAL);
    CHECK_GOTO(memcmp(&item, &unchanged, sizeof(item)) == 0);
    item = (trevrpc_h3_ingress_item){0};
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    for (size_t i = 0; i < initialized; i++) {
        fake_stream_destroy(&streams[i]);
    }
    fake_conn_destroy(&conn);
    return result;
}

static int test_request_classification_skips_unknown_frames_without_overread(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream stream;
    bool stream_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    const uint8_t bytes[] = {0x21, 0x04, 0xaa, 0xbb, 0xcc, 0xdd, 0x01, 0xa3};

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&stream, 0, bytes, sizeof(bytes), false) == 0);
    stream_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &stream) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   runtime, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_NONE, 0) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &item) == 0);
    CHECK_GOTO(item._transport_stream == &stream);
    CHECK_GOTO(item.first_value == TREV_H3_FRAME_HEADERS);
    CHECK_GOTO(stream.offset == 7);
    CHECK_GOTO(stream.max_read_size == 4);
    trevrpc_h3_ingress_item_close(&item);
    CHECK_GOTO(stream.close_calls == 1);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (stream_initialized) {
        fake_stream_destroy(&stream);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_pop_bidi_preserves_classification_ready_order(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream request;
    fake_stream webtransport;
    bool request_initialized = false;
    bool webtransport_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&request, 0, NULL, 0, false) == 0);
    request_initialized = true;
    CHECK_GOTO(fake_stream_init(&webtransport, 4, NULL, 0, false) == 0);
    webtransport_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &webtransport) == 0);
    CHECK_GOTO(fake_conn_enqueue(&conn, &request) == 0);
    trevrpc_h3_ingress_config config = test_config(2, 2);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   runtime, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_DRAFT_15, 0) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(fake_stream_wait_observer_set(&request) == 0);
    CHECK_GOTO(fake_stream_wait_observer_set(&webtransport) == 0);

    const uint8_t webtransport_bytes[] = {0x40, 0x41, 0x00};
    CHECK_GOTO(
        fake_stream_append(
            &webtransport, webtransport_bytes, sizeof(webtransport_bytes), TREV_MSQUIC_STREAM_OBSERVER_READABLE) == 0);
    CHECK_GOTO(fake_stream_wait_offset(&webtransport, sizeof(webtransport_bytes)) == 0);
    const uint8_t request_bytes[] = {0x01};
    CHECK_GOTO(
        fake_stream_append(&request, request_bytes, sizeof(request_bytes), TREV_MSQUIC_STREAM_OBSERVER_READABLE) == 0);
    CHECK_GOTO(fake_stream_wait_offset(&request, sizeof(request_bytes)) == 0);

    CHECK_GOTO(trevrpc_h3_ingress_pop_bidi(runtime, TEST_TIMEOUT_NANOS, &item) == 0);
    CHECK_GOTO(item._transport_stream == &webtransport);
    CHECK_GOTO(item.action == TREV_H3_DEMUX_ACTION_WEBTRANSPORT);
    trevrpc_h3_ingress_item_close(&item);
    CHECK_GOTO(trevrpc_h3_ingress_pop_bidi(runtime, TEST_TIMEOUT_NANOS, &item) == 0);
    CHECK_GOTO(item._transport_stream == &request);
    CHECK_GOTO(item.action == TREV_H3_DEMUX_ACTION_REQUEST);
    trevrpc_h3_ingress_item_close(&item);
    CHECK_GOTO(webtransport.close_calls == 1 && request.close_calls == 1);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (webtransport_initialized) {
        fake_stream_destroy(&webtransport);
    }
    if (request_initialized) {
        fake_stream_destroy(&request);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_fragmented_coalesced_wakes_and_terminal_after_classification(void) {
    int result = 1;
    fake_conn conn;
    fake_stream stream;
    bool stream_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    const uint8_t first[] = {0x40};
    CHECK_GOTO(fake_conn_init(&conn) == 0);
    CHECK_GOTO(fake_stream_init(&stream, 0, first, sizeof(first), false) == 0);
    stream_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &stream) == 0);
    trevrpc_h3_ingress_config config = test_config(2, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   runtime, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_DRAFT_15, 0) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);

    const uint8_t second[] = {0x41};
    CHECK_GOTO(fake_stream_append(&stream, second, sizeof(second), TREV_MSQUIC_STREAM_OBSERVER_READABLE) == 0);
    fake_stream_notify(&stream, TREV_MSQUIC_STREAM_OBSERVER_READABLE);
    const uint8_t session[] = {0x00};
    CHECK_GOTO(fake_stream_append(&stream,
                   session,
                   sizeof(session),
                   TREV_MSQUIC_STREAM_OBSERVER_READABLE | TREV_MSQUIC_STREAM_OBSERVER_TERMINAL) == 0);

    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_WEBTRANSPORT, TEST_TIMEOUT_NANOS, &item) == 0);
    CHECK_GOTO(item.first_value == TREV_H3_DEMUX_WEBTRANSPORT_BIDI);
    CHECK_GOTO(item.session_id == 0);
    CHECK_GOTO(stream.offset == 3);
    CHECK_GOTO(stream.max_read_size <= 2);
    trevrpc_h3_ingress_item_close(&item);
    CHECK_GOTO(stream.close_calls == 1);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (stream_initialized) {
        fake_stream_destroy(&stream);
    }
    fake_conn_destroy(&conn);
    return result;
}

static int test_fragmented_legal_eight_byte_request_prefix(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream stream;
    bool stream_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    const uint8_t first[] = {0xc0};
    const uint8_t rest[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01};

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&stream, 0, NULL, 0, false) == 0);
    stream_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &stream) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(fake_stream_wait_observer_set(&stream) == 0);
    CHECK_GOTO(fake_stream_append(&stream, first, sizeof(first), TREV_MSQUIC_STREAM_OBSERVER_READABLE) == 0);
    CHECK_GOTO(fake_stream_wait_offset(&stream, sizeof(first)) == 0);
    CHECK_GOTO(fake_stream_append(&stream, rest, sizeof(rest), TREV_MSQUIC_STREAM_OBSERVER_READABLE) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &item) == 0);
    CHECK_GOTO(stream.offset == sizeof(first) + sizeof(rest));
    CHECK_GOTO(stream.max_read_size == sizeof(rest));
    trevrpc_h3_ingress_item_close(&item);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (stream_initialized) {
        fake_stream_destroy(&stream);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_fragmented_legal_four_byte_request_prefix(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream stream;
    bool stream_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    const uint8_t first[] = {0x80};
    const uint8_t rest[] = {0x00, 0x00, 0x01};

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&stream, 0, NULL, 0, false) == 0);
    stream_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &stream) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(fake_stream_wait_observer_set(&stream) == 0);
    CHECK_GOTO(fake_stream_append(&stream, first, sizeof(first), TREV_MSQUIC_STREAM_OBSERVER_READABLE) == 0);
    CHECK_GOTO(fake_stream_wait_offset(&stream, sizeof(first)) == 0);
    CHECK_GOTO(fake_stream_append(&stream, rest, sizeof(rest), TREV_MSQUIC_STREAM_OBSERVER_READABLE) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &item) == 0);
    CHECK_GOTO(stream.offset == sizeof(first) + sizeof(rest));
    CHECK_GOTO(stream.max_read_size == sizeof(rest));
    trevrpc_h3_ingress_item_close(&item);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (stream_initialized) {
        fake_stream_destroy(&stream);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_unknown_slot_is_reused_sequentially(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream unknown;
    fake_stream request;
    bool unknown_initialized = false;
    bool request_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    const uint8_t unknown_type[] = {0x21};
    const uint8_t request_bytes[] = {0x01};

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&unknown, 2, unknown_type, sizeof(unknown_type), false) == 0);
    unknown_initialized = true;
    unknown.abort_error = -EIO;
    CHECK_GOTO(fake_stream_init(&request, 0, request_bytes, sizeof(request_bytes), false) == 0);
    request_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &unknown) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(fake_stream_wait_close_calls(&unknown, 1) == 0);
    CHECK_GOTO(unknown.abort_calls == 1);
    CHECK_GOTO(unknown.abort_application_error == TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR);
    CHECK_GOTO(unknown.close_observed_abort);
    CHECK_GOTO(fake_conn_enqueue(&conn, &request) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &item) == 0);
    CHECK_GOTO(item._transport_stream == &request);
    trevrpc_h3_ingress_item_close(&item);
    CHECK_GOTO(request.close_calls == 1);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (request_initialized) {
        fake_stream_destroy(&request);
    }
    if (unknown_initialized) {
        fake_stream_destroy(&unknown);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_unknown_and_terminal_unidirectional_are_reclaimed(void) {
    int result = 1;
    fake_conn conn;
    fake_stream unknown;
    fake_stream partial;
    bool unknown_initialized = false;
    bool partial_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    const uint8_t unknown_type[] = {0x21};
    const uint8_t partial_type[] = {0x40};
    CHECK_GOTO(fake_conn_init(&conn) == 0);
    CHECK_GOTO(fake_stream_init(&unknown, 2, unknown_type, sizeof(unknown_type), false) == 0);
    unknown_initialized = true;
    CHECK_GOTO(fake_stream_init(&partial, 6, partial_type, sizeof(partial_type), true) == 0);
    partial_initialized = true;
    unknown.notify_on_observer_install = false;
    partial.notify_on_observer_install = false;
    CHECK_GOTO(fake_conn_enqueue(&conn, &unknown) == 0);
    CHECK_GOTO(fake_conn_enqueue(&conn, &partial) == 0);
    trevrpc_h3_ingress_config config = test_config(2, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(fake_stream_wait_close_calls(&unknown, 1) == 0);
    CHECK_GOTO(fake_stream_wait_close_calls(&partial, 1) == 0);
    CHECK_GOTO(unknown.clear_calls == 1 && unknown.drain_calls == 1);
    CHECK_GOTO(unknown.abort_calls == 1);
    CHECK_GOTO(unknown.abort_application_error == TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR);
    CHECK_GOTO(unknown.close_observed_abort);
    CHECK_GOTO(partial.clear_calls == 1 && partial.drain_calls == 1);
    CHECK_GOTO(partial.abort_calls == 0);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(runtime, &unknown.id) == -EAGAIN);
    result = 0;

cleanup:
    trevrpc_h3_ingress_release(runtime);
    if (partial_initialized) {
        fake_stream_destroy(&partial);
    }
    if (unknown_initialized) {
        fake_stream_destroy(&unknown);
    }
    fake_conn_destroy(&conn);
    return result;
}

static int test_terminal_error_is_first_wins_and_wakes_settings(void) {
    int result = 1;
    fake_conn conn;
    fake_stream terminal;
    bool terminal_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    pthread_t waiter_thread;
    bool waiter_started = false;
    settings_wait wait = {0};
    trevrpc_h3_ingress_item item = {0};
    uint64_t fatal_error = 0;
    CHECK_GOTO(fake_conn_init(&conn) == 0);
    CHECK_GOTO(fake_stream_init(&terminal, 0, NULL, 0, true) == 0);
    terminal_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &terminal) == 0);
    trevrpc_h3_ingress_config config = test_config(2, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    wait.runtime = runtime;
    CHECK_GOTO(pthread_create(&waiter_thread, NULL, settings_wait_main, &wait) == 0);
    waiter_started = true;
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &item) == -EPROTO);
    CHECK_GOTO(pthread_join(waiter_thread, NULL) == 0);
    waiter_started = false;
    CHECK_GOTO(wait.result == 0);
    CHECK_GOTO(wait.status == TREV_H3_INGRESS_SETTINGS_FAILED);
    CHECK_GOTO(wait.application_error == TREV_H3_DEMUX_APP_FRAME_ERROR);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(runtime, &fatal_error) == 0);
    CHECK_GOTO(fatal_error == TREV_H3_DEMUX_APP_FRAME_ERROR);
    CHECK_GOTO(trevrpc_h3_ingress_fail(runtime, TREV_H3_INGRESS_APP_EXCESSIVE_LOAD) == -EALREADY);
    trevrpc_h3_ingress_shutdown(runtime);
    CHECK_GOTO(fake_conn_first_error(&conn) == TREV_H3_DEMUX_APP_FRAME_ERROR);
    CHECK_GOTO(fake_conn_shutdown_error_calls(&conn) == 1);
    CHECK_GOTO(terminal.close_calls == 1);
    result = 0;

cleanup:
    if (waiter_started) {
        trevrpc_h3_ingress_shutdown(runtime);
        (void)pthread_join(waiter_thread, NULL);
    }
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (terminal_initialized) {
        fake_stream_destroy(&terminal);
    }
    fake_conn_destroy(&conn);
    return result;
}

static int run_duplicate_critical_role_case(uint8_t stream_type) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream streams[2];
    size_t initialized = 0;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    uint64_t fatal_error = 0;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&streams[0], 2, &stream_type, 1, false) == 0);
    initialized++;
    CHECK_GOTO(fake_stream_init(&streams[1], 6, &stream_type, 1, false) == 0);
    initialized++;
    CHECK_GOTO(fake_conn_enqueue(&conn, &streams[0]) == 0);
    CHECK_GOTO(fake_conn_enqueue(&conn, &streams[1]) == 0);
    trevrpc_h3_ingress_config config = test_config(2, 2);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &item) == -EPROTO);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(runtime, &fatal_error) == 0);
    CHECK_GOTO(fatal_error == TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR);
    CHECK_GOTO(fake_conn_wait_shutdown_error_calls(&conn, 1) == 0);
    CHECK_GOTO(fake_conn_shutdown_error_calls(&conn) == 1);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    for (size_t i = 0; i < initialized; i++) {
        fake_stream_destroy(&streams[i]);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_duplicate_qpack_roles_are_fatal(void) {
    if (run_duplicate_critical_role_case(0x02) != 0) {
        return 1;
    }
    return run_duplicate_critical_role_case(0x03);
}

static int test_duplicate_role_and_capacity_overflow_errors(void) {
    int result = 1;
    fake_conn duplicate_conn;
    fake_conn capacity_conn;
    bool duplicate_conn_initialized = false;
    bool capacity_conn_initialized = false;
    fake_stream controls[2];
    fake_stream pending;
    fake_stream overflow;
    size_t controls_initialized = 0;
    bool pending_initialized = false;
    bool overflow_initialized = false;
    trevrpc_h3_ingress* duplicate_runtime = NULL;
    trevrpc_h3_ingress* capacity_runtime = NULL;
    uint64_t fatal_error = 0;
    const uint8_t control[] = {0x00};
    const uint8_t request[] = {0x01};

    CHECK_GOTO(fake_conn_init(&duplicate_conn) == 0);
    duplicate_conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&controls[0], 2, control, sizeof(control), false) == 0);
    controls_initialized++;
    CHECK_GOTO(fake_stream_init(&controls[1], 6, control, sizeof(control), false) == 0);
    controls_initialized++;
    CHECK_GOTO(fake_conn_enqueue(&duplicate_conn, &controls[0]) == 0);
    CHECK_GOTO(fake_conn_enqueue(&duplicate_conn, &controls[1]) == 0);
    trevrpc_h3_ingress_config duplicate_config = test_config(3, 2);
    CHECK_GOTO(
        trevrpc_h3_ingress_create_with_ops(&duplicate_conn, &fake_ops, &duplicate_config, &duplicate_runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(duplicate_runtime) == 0);
    trevrpc_h3_ingress_item duplicate_item = {0};
    CHECK_GOTO(trevrpc_h3_ingress_pop(
                   duplicate_runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &duplicate_item) == -EPROTO);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(duplicate_runtime, &fatal_error) == 0);
    CHECK_GOTO(fatal_error == TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR);
    trevrpc_h3_ingress_shutdown(duplicate_runtime);
    CHECK_GOTO(fake_conn_shutdown_error_calls(&duplicate_conn) == 1);

    CHECK_GOTO(fake_conn_init(&capacity_conn) == 0);
    capacity_conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&pending, 0, NULL, 0, false) == 0);
    pending_initialized = true;
    CHECK_GOTO(fake_stream_init(&overflow, 4, request, sizeof(request), false) == 0);
    overflow_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&capacity_conn, &pending) == 0);
    CHECK_GOTO(fake_conn_enqueue(&capacity_conn, &overflow) == 0);
    trevrpc_h3_ingress_config capacity_config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&capacity_conn, &fake_ops, &capacity_config, &capacity_runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(capacity_runtime) == 0);
    trevrpc_h3_ingress_item capacity_item = {0};
    CHECK_GOTO(trevrpc_h3_ingress_pop(
                   capacity_runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &capacity_item) == -EPROTO);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(capacity_runtime, &fatal_error) == 0);
    CHECK_GOTO(fatal_error == TREV_H3_INGRESS_APP_EXCESSIVE_LOAD);
    trevrpc_h3_ingress_shutdown(capacity_runtime);
    CHECK_GOTO(fake_conn_shutdown_error_calls(&capacity_conn) == 1);
    CHECK_GOTO(pending.close_calls == 1);
    CHECK_GOTO(overflow.close_calls == 1);
    result = 0;

cleanup:
    trevrpc_h3_ingress_release(capacity_runtime);
    trevrpc_h3_ingress_release(duplicate_runtime);
    if (overflow_initialized) {
        fake_stream_destroy(&overflow);
    }
    if (pending_initialized) {
        fake_stream_destroy(&pending);
    }
    for (size_t i = 0; i < controls_initialized; i++) {
        fake_stream_destroy(&controls[i]);
    }
    if (capacity_conn_initialized) {
        fake_conn_destroy(&capacity_conn);
    }
    if (duplicate_conn_initialized) {
        fake_conn_destroy(&duplicate_conn);
    }
    return result;
}

static int test_queue_overflow_and_settings_publication(void) {
    int result = 1;
    fake_conn conn;
    fake_stream requests[2];
    size_t initialized = 0;
    trevrpc_h3_ingress* runtime = NULL;
    const uint8_t request[] = {0x01};
    uint64_t fatal_error = 0;
    CHECK_GOTO(fake_conn_init(&conn) == 0);
    CHECK_GOTO(fake_stream_init(&requests[0], 0, request, sizeof(request), false) == 0);
    initialized++;
    CHECK_GOTO(fake_stream_init(&requests[1], 4, request, sizeof(request), false) == 0);
    initialized++;
    CHECK_GOTO(fake_conn_enqueue(&conn, &requests[0]) == 0);
    CHECK_GOTO(fake_conn_enqueue(&conn, &requests[1]) == 0);
    trevrpc_h3_ingress_config config = test_config(2, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   runtime, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_DRAFT_15, 0) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   runtime, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_DRAFT_15, 0) == -EALREADY);
    int settings_status = 0;
    trevrpc_wt_profile_id selected_profile = TREV_WT_PROFILE_NONE;
    uint64_t settings_error = UINT64_MAX;
    CHECK_GOTO(
        trevrpc_h3_ingress_wait_peer_settings(runtime, &settings_status, &selected_profile, &settings_error) == 0);
    CHECK_GOTO(settings_status == TREV_H3_INGRESS_SETTINGS_READY);
    CHECK_GOTO(selected_profile == TREV_WT_PROFILE_DRAFT_15);
    CHECK_GOTO(settings_error == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    trevrpc_h3_ingress_item item = {0};
    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_CONTROL, TEST_TIMEOUT_NANOS, &item) == -EPROTO);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(runtime, &fatal_error) == 0);
    CHECK_GOTO(fatal_error == TREV_H3_INGRESS_APP_EXCESSIVE_LOAD);
    trevrpc_h3_ingress_shutdown(runtime);
    CHECK_GOTO(fake_conn_shutdown_error_calls(&conn) == 1);
    CHECK_GOTO(requests[0].close_calls == 1);
    CHECK_GOTO(requests[1].close_calls == 1);
    result = 0;

cleanup:
    trevrpc_h3_ingress_release(runtime);
    for (size_t i = 0; i < initialized; i++) {
        fake_stream_destroy(&requests[i]);
    }
    fake_conn_destroy(&conn);
    return result;
}

static int test_unclassified_bidi_queue_overflow_is_first_wins(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream streams[2];
    size_t initialized = 0;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {
        .stream = (trevrpc_msquic_stream*)(uintptr_t)1,
        .stream_id = UINT64_C(0xaaaa),
        .accepted_at_nanos = UINT64_C(0xbbbb),
    };
    trevrpc_h3_ingress_item original = item;
    uint64_t fatal_error = 0;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&streams[0], 0, NULL, 0, false) == 0);
    initialized++;
    CHECK_GOTO(fake_stream_init(&streams[1], 4, NULL, 0, false) == 0);
    initialized++;
    CHECK_GOTO(fake_conn_enqueue(&conn, &streams[0]) == 0);
    CHECK_GOTO(fake_conn_enqueue(&conn, &streams[1]) == 0);
    trevrpc_h3_ingress_config config = test_config(2, 1);
    config.bidi_mode = TREV_H3_INGRESS_BIDI_HANDOFF_UNCLASSIFIED;
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(fake_conn_wait_shutdown(&conn) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop_unclassified_bidi(runtime, TEST_TIMEOUT_NANOS, &item) == -EPROTO);
    CHECK_GOTO(memcmp(&item, &original, sizeof(item)) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(runtime, &fatal_error) == 0);
    CHECK_GOTO(fatal_error == TREV_H3_INGRESS_APP_EXCESSIVE_LOAD);
    CHECK_GOTO(trevrpc_h3_ingress_fail(runtime, TREV_H3_INGRESS_APP_INTERNAL_ERROR) == -EALREADY);
    trevrpc_h3_ingress_shutdown(runtime);
    CHECK_GOTO(fake_conn_shutdown_error_calls(&conn) == 1 &&
               fake_conn_first_error(&conn) == TREV_H3_INGRESS_APP_EXCESSIVE_LOAD);
    CHECK_GOTO(streams[0].close_calls == 1 && streams[1].close_calls == 1);
    CHECK_GOTO(streams[0].observer_set_calls == 0 && streams[1].observer_set_calls == 0);
    result = 0;

cleanup:
    trevrpc_h3_ingress_release(runtime);
    for (size_t i = 0; i < initialized; i++) {
        fake_stream_destroy(&streams[i]);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_settings_profile_publication_validation(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   runtime, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_DRAFT_16_RESERVED, 0) == -EINVAL);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   runtime, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_DRAFT_15, TREV_H3_DEMUX_APP_FRAME_ERROR) ==
               -EINVAL);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   runtime, TREV_H3_INGRESS_SETTINGS_FAILED, TREV_WT_PROFILE_DRAFT_15, TREV_H3_DEMUX_APP_FRAME_ERROR) ==
               -EINVAL);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   runtime, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_DRAFT_14, 0) == 0);

    int settings_status = 0;
    trevrpc_wt_profile_id selected_profile = TREV_WT_PROFILE_NONE;
    uint64_t settings_error = UINT64_MAX;
    CHECK_GOTO(
        trevrpc_h3_ingress_wait_peer_settings(runtime, &settings_status, &selected_profile, &settings_error) == 0);
    CHECK_GOTO(settings_status == TREV_H3_INGRESS_SETTINGS_READY);
    CHECK_GOTO(selected_profile == TREV_WT_PROFILE_DRAFT_14);
    CHECK_GOTO(settings_error == 0);
    result = 0;

cleanup:
    trevrpc_h3_ingress_release(runtime);
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_settings_failure_publication_is_fatal(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(
        trevrpc_h3_ingress_publish_peer_settings(
            runtime, TREV_H3_INGRESS_SETTINGS_FAILED, TREV_WT_PROFILE_NONE, TREV_H3_DEMUX_APP_FRAME_UNEXPECTED) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   runtime, TREV_H3_INGRESS_SETTINGS_FAILED, TREV_WT_PROFILE_NONE, TREV_H3_DEMUX_APP_FRAME_ERROR) ==
               -EALREADY);
    int settings_status = 0;
    trevrpc_wt_profile_id selected_profile = TREV_WT_PROFILE_DRAFT_15;
    uint64_t settings_error = 0;
    CHECK_GOTO(
        trevrpc_h3_ingress_wait_peer_settings(runtime, &settings_status, &selected_profile, &settings_error) == 0);
    CHECK_GOTO(settings_status == TREV_H3_INGRESS_SETTINGS_FAILED);
    CHECK_GOTO(selected_profile == TREV_WT_PROFILE_NONE);
    CHECK_GOTO(settings_error == TREV_H3_DEMUX_APP_FRAME_UNEXPECTED);
    CHECK_GOTO(fake_conn_shutdown_error_calls(&conn) == 1);
    CHECK_GOTO(fake_conn_first_error(&conn) == TREV_H3_DEMUX_APP_FRAME_UNEXPECTED);
    result = 0;

cleanup:
    trevrpc_h3_ingress_release(runtime);
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_concurrent_first_fatal_arbitration(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    fail_race race = {0};
    bool race_mutex_initialized = false;
    bool race_cond_initialized = false;
    pthread_t threads[2];
    size_t threads_started = 0;
    fail_race_arg args[2] = {{.race = &race, .index = 0}, {.race = &race, .index = 1}};
    uint64_t fatal_error = 0;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(pthread_mutex_init(&race.mutex, NULL) == 0);
    race_mutex_initialized = true;
    CHECK_GOTO(pthread_cond_init(&race.cond, NULL) == 0);
    race_cond_initialized = true;
    race.runtime = runtime;
    race.errors[0] = TREV_H3_DEMUX_APP_FRAME_ERROR;
    race.errors[1] = TREV_H3_INGRESS_APP_EXCESSIVE_LOAD;
    CHECK_GOTO(pthread_create(&threads[0], NULL, fail_race_main, &args[0]) == 0);
    threads_started++;
    CHECK_GOTO(pthread_create(&threads[1], NULL, fail_race_main, &args[1]) == 0);
    threads_started++;
    pthread_mutex_lock(&race.mutex);
    while (race.ready != 2) {
        pthread_cond_wait(&race.cond, &race.mutex);
    }
    race.go = true;
    pthread_cond_broadcast(&race.cond);
    pthread_mutex_unlock(&race.mutex);
    CHECK_GOTO(pthread_join(threads[0], NULL) == 0);
    threads_started--;
    CHECK_GOTO(pthread_join(threads[1], NULL) == 0);
    threads_started--;
    CHECK_GOTO((race.results[0] == 0 && race.results[1] == -EALREADY) ||
               (race.results[1] == 0 && race.results[0] == -EALREADY));
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(runtime, &fatal_error) == 0);
    CHECK_GOTO(fatal_error == (race.results[0] == 0 ? race.errors[0] : race.errors[1]));
    CHECK_GOTO(fake_conn_shutdown_error_calls(&conn) == 1 && fake_conn_first_error(&conn) == fatal_error);
    result = 0;

cleanup:
    if (race_mutex_initialized) {
        pthread_mutex_lock(&race.mutex);
        race.go = true;
        pthread_cond_broadcast(&race.cond);
        pthread_mutex_unlock(&race.mutex);
    }
    while (threads_started != 0) {
        threads_started--;
        (void)pthread_join(threads[threads_started], NULL);
    }
    trevrpc_h3_ingress_release(runtime);
    if (race_cond_initialized) {
        pthread_cond_destroy(&race.cond);
    }
    if (race_mutex_initialized) {
        pthread_mutex_destroy(&race.mutex);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_settings_barrier_observes_fatal_and_normal_stop_wins(void) {
    int result = 1;
    fake_conn fatal_conn;
    fake_conn stopped_conn;
    bool fatal_conn_initialized = false;
    bool stopped_conn_initialized = false;
    trevrpc_h3_ingress* fatal_runtime = NULL;
    trevrpc_h3_ingress* stopped_runtime = NULL;
    int settings_status = 0;
    trevrpc_wt_profile_id selected_profile = TREV_WT_PROFILE_DRAFT_15;
    uint64_t settings_error = 0;
    uint64_t fatal_error = 0;

    CHECK_GOTO(fake_conn_init(&fatal_conn) == 0);
    fatal_conn_initialized = true;
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&fatal_conn, &fake_ops, &config, &fatal_runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   fatal_runtime, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_DRAFT_15, 0) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_fail(fatal_runtime, TREV_H3_DEMUX_APP_FRAME_UNEXPECTED) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_wait_peer_settings(
                   fatal_runtime, &settings_status, &selected_profile, &settings_error) == 0);
    CHECK_GOTO(settings_status == TREV_H3_INGRESS_SETTINGS_FAILED);
    CHECK_GOTO(selected_profile == TREV_WT_PROFILE_NONE);
    CHECK_GOTO(settings_error == TREV_H3_DEMUX_APP_FRAME_UNEXPECTED);
    CHECK_GOTO(fake_conn_shutdown_error_calls(&fatal_conn) == 1);

    CHECK_GOTO(fake_conn_init(&stopped_conn) == 0);
    stopped_conn_initialized = true;
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&stopped_conn, &fake_ops, &config, &stopped_runtime) == 0);
    trevrpc_h3_ingress_shutdown(stopped_runtime);
    CHECK_GOTO(trevrpc_h3_ingress_fail(stopped_runtime, TREV_H3_DEMUX_APP_FRAME_ERROR) == -EALREADY);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(stopped_runtime, &fatal_error) == -EAGAIN);
    CHECK_GOTO(fake_conn_shutdown_calls(&stopped_conn) == 1);
    CHECK_GOTO(fake_conn_shutdown_error_calls(&stopped_conn) == 0);
    result = 0;

cleanup:
    trevrpc_h3_ingress_release(stopped_runtime);
    trevrpc_h3_ingress_release(fatal_runtime);
    if (stopped_conn_initialized) {
        fake_conn_destroy(&stopped_conn);
    }
    if (fatal_conn_initialized) {
        fake_conn_destroy(&fatal_conn);
    }
    return result;
}

static int test_shutdown_wakes_blocked_waiters(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    pthread_t pop_thread;
    pthread_t settings_thread;
    bool pop_started = false;
    bool settings_started = false;
    pop_wait pop = {0};
    settings_wait settings = {0};
    pop.item = (trevrpc_h3_ingress_item){
        .stream = (trevrpc_msquic_stream*)(uintptr_t)1,
        .stream_id = UINT64_C(0xaaaa),
        .action = TREV_H3_DEMUX_ACTION_WEBTRANSPORT,
        .session_id = UINT64_C(0xbbbb),
    };
    trevrpc_h3_ingress_item original_item = pop.item;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    pop.runtime = runtime;
    settings.runtime = runtime;
    CHECK_GOTO(pthread_create(&pop_thread, NULL, pop_wait_main, &pop) == 0);
    pop_started = true;
    CHECK_GOTO(pthread_create(&settings_thread, NULL, settings_wait_main, &settings) == 0);
    settings_started = true;

    trevrpc_h3_ingress_shutdown(runtime);
    CHECK_GOTO(pthread_join(pop_thread, NULL) == 0);
    pop_started = false;
    CHECK_GOTO(pthread_join(settings_thread, NULL) == 0);
    settings_started = false;
    CHECK_GOTO(pop.result == -ECANCELED);
    CHECK_GOTO(memcmp(&pop.item, &original_item, sizeof(pop.item)) == 0);
    CHECK_GOTO(settings.result == 0);
    CHECK_GOTO(settings.status == TREV_H3_INGRESS_SETTINGS_FAILED);
    CHECK_GOTO(settings.application_error == 0);
    CHECK_GOTO(fake_conn_shutdown_calls(&conn) == 1);
    CHECK_GOTO(fake_conn_shutdown_error_calls(&conn) == 0);
    result = 0;

cleanup:
    trevrpc_h3_ingress_shutdown(runtime);
    if (pop_started) {
        (void)pthread_join(pop_thread, NULL);
    }
    if (settings_started) {
        (void)pthread_join(settings_thread, NULL);
    }
    trevrpc_h3_ingress_item_close(&pop.item);
    trevrpc_h3_ingress_release(runtime);
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_bidi_pop_waiters_wake_on_shutdown(void) {
    int result = 1;
    fake_conn classify_conn;
    fake_conn handoff_conn;
    bool classify_conn_initialized = false;
    bool handoff_conn_initialized = false;
    trevrpc_h3_ingress* classify_runtime = NULL;
    trevrpc_h3_ingress* handoff_runtime = NULL;
    pop_wait classified = {.kind = POP_WAIT_CLASSIFIED_BIDI};
    pop_wait unclassified = {.kind = POP_WAIT_UNCLASSIFIED_BIDI};
    pthread_t classified_thread;
    pthread_t unclassified_thread;
    bool classified_started = false;
    bool unclassified_started = false;

    classified.item = (trevrpc_h3_ingress_item){.stream_id = UINT64_C(0x1111)};
    unclassified.item = (trevrpc_h3_ingress_item){.accepted_at_nanos = UINT64_C(0x2222)};
    trevrpc_h3_ingress_item classified_original = classified.item;
    trevrpc_h3_ingress_item unclassified_original = unclassified.item;

    CHECK_GOTO(fake_conn_init(&classify_conn) == 0);
    classify_conn_initialized = true;
    CHECK_GOTO(fake_conn_init(&handoff_conn) == 0);
    handoff_conn_initialized = true;
    trevrpc_h3_ingress_config classify_config = test_config(1, 1);
    trevrpc_h3_ingress_config handoff_config = test_config(1, 1);
    handoff_config.bidi_mode = TREV_H3_INGRESS_BIDI_HANDOFF_UNCLASSIFIED;
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&classify_conn, &fake_ops, &classify_config, &classify_runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&handoff_conn, &fake_ops, &handoff_config, &handoff_runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(classify_runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(handoff_runtime) == 0);
    classified.runtime = classify_runtime;
    unclassified.runtime = handoff_runtime;
    CHECK_GOTO(pthread_create(&classified_thread, NULL, pop_wait_main, &classified) == 0);
    classified_started = true;
    CHECK_GOTO(pthread_create(&unclassified_thread, NULL, pop_wait_main, &unclassified) == 0);
    unclassified_started = true;

    trevrpc_h3_ingress_shutdown(classify_runtime);
    trevrpc_h3_ingress_shutdown(handoff_runtime);
    CHECK_GOTO(pthread_join(classified_thread, NULL) == 0);
    classified_started = false;
    CHECK_GOTO(pthread_join(unclassified_thread, NULL) == 0);
    unclassified_started = false;
    CHECK_GOTO(classified.result == -ECANCELED && unclassified.result == -ECANCELED);
    CHECK_GOTO(memcmp(&classified.item, &classified_original, sizeof(classified.item)) == 0);
    CHECK_GOTO(memcmp(&unclassified.item, &unclassified_original, sizeof(unclassified.item)) == 0);
    result = 0;

cleanup:
    trevrpc_h3_ingress_shutdown(classify_runtime);
    trevrpc_h3_ingress_shutdown(handoff_runtime);
    if (classified_started) {
        (void)pthread_join(classified_thread, NULL);
    }
    if (unclassified_started) {
        (void)pthread_join(unclassified_thread, NULL);
    }
    trevrpc_h3_ingress_release(handoff_runtime);
    trevrpc_h3_ingress_release(classify_runtime);
    if (handoff_conn_initialized) {
        fake_conn_destroy(&handoff_conn);
    }
    if (classify_conn_initialized) {
        fake_conn_destroy(&classify_conn);
    }
    return result;
}

static int test_start_shutdown_race_is_serialized(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    start_wait start = {0};
    bool start_mutex_initialized = false;
    bool start_cond_initialized = false;
    pthread_t start_thread;
    bool start_thread_started = false;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(pthread_mutex_init(&start.mutex, NULL) == 0);
    start_mutex_initialized = true;
    CHECK_GOTO(pthread_cond_init(&start.cond, NULL) == 0);
    start_cond_initialized = true;
    start.runtime = runtime;
    CHECK_GOTO(pthread_create(&start_thread, NULL, start_wait_main, &start) == 0);
    start_thread_started = true;

    pthread_mutex_lock(&start.mutex);
    while (!start.ready) {
        pthread_cond_wait(&start.cond, &start.mutex);
    }
    start.go = true;
    pthread_cond_broadcast(&start.cond);
    pthread_mutex_unlock(&start.mutex);

    trevrpc_h3_ingress_shutdown(runtime);
    CHECK_GOTO(pthread_join(start_thread, NULL) == 0);
    start_thread_started = false;
    CHECK_GOTO(start.result == 0 || start.result == -EALREADY);
    CHECK_GOTO(fake_conn_shutdown_calls(&conn) == 1);
    CHECK_GOTO(fake_conn_shutdown_error_calls(&conn) == 0);
    result = 0;

cleanup:
    trevrpc_h3_ingress_shutdown(runtime);
    if (start_thread_started) {
        pthread_mutex_lock(&start.mutex);
        start.go = true;
        pthread_cond_broadcast(&start.cond);
        pthread_mutex_unlock(&start.mutex);
        (void)pthread_join(start_thread, NULL);
    }
    trevrpc_h3_ingress_release(runtime);
    if (start_cond_initialized) {
        pthread_cond_destroy(&start.cond);
    }
    if (start_mutex_initialized) {
        pthread_mutex_destroy(&start.mutex);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int run_accept_error_case(int transport_error, uint64_t expected_fatal) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    uint64_t fatal_error = 0;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    conn.accept_error = transport_error;
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    int pop_result = trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &item);
    CHECK_GOTO(pop_result == (expected_fatal == 0 ? -ECANCELED : -EPROTO));
    trevrpc_h3_ingress_shutdown(runtime);
    if (expected_fatal == 0) {
        CHECK_GOTO(trevrpc_h3_ingress_fatal_error(runtime, &fatal_error) == -EAGAIN);
        CHECK_GOTO(fake_conn_shutdown_calls(&conn) == 1 && fake_conn_shutdown_error_calls(&conn) == 0);
    } else {
        CHECK_GOTO(trevrpc_h3_ingress_fatal_error(runtime, &fatal_error) == 0);
        CHECK_GOTO(fatal_error == expected_fatal);
        CHECK_GOTO(fake_conn_shutdown_calls(&conn) == 0 && fake_conn_shutdown_error_calls(&conn) == 1);
    }
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int run_stream_setup_error_case(int id_error, int observer_error, uint64_t expected_fatal) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream stream;
    bool stream_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    uint64_t fatal_error = 0;
    const uint8_t request[] = {0x01};

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&stream, 0, request, sizeof(request), false) == 0);
    stream_initialized = true;
    stream.id_error = id_error;
    stream.observer_error = observer_error;
    CHECK_GOTO(fake_conn_enqueue(&conn, &stream) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &item) == -EPROTO);
    trevrpc_h3_ingress_shutdown(runtime);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(runtime, &fatal_error) == 0);
    CHECK_GOTO(fatal_error == expected_fatal);
    CHECK_GOTO(fake_conn_shutdown_error_calls(&conn) == 1);
    CHECK_GOTO(fake_stream_wait_close_calls(&stream, 1) == 0);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (stream_initialized) {
        fake_stream_destroy(&stream);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int run_stream_read_error_case(uint64_t stream_id, intptr_t read_error, uint64_t expected_fatal) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream stream;
    bool stream_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    uint64_t fatal_error = 0;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&stream, stream_id, NULL, 0, false) == 0);
    stream_initialized = true;
    stream.read_error = read_error;
    CHECK_GOTO(fake_conn_enqueue(&conn, &stream) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &item) == -EPROTO);
    trevrpc_h3_ingress_shutdown(runtime);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(runtime, &fatal_error) == 0);
    CHECK_GOTO(fatal_error == expected_fatal);
    CHECK_GOTO(fake_conn_shutdown_error_calls(&conn) == 1);
    CHECK_GOTO(fake_stream_wait_close_calls(&stream, 1) == 0);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (stream_initialized) {
        fake_stream_destroy(&stream);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_closed_prefix_read_retires_only_accepted_stream(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream retired;
    fake_stream request;
    bool retired_initialized = false;
    bool request_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    uint64_t fatal_error = 0;
    const uint8_t request_bytes[] = {0x01};

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&retired, 0, NULL, 0, false) == 0);
    retired_initialized = true;
    retired.read_error = TREV_MSQUIC_ERR_CLOSED;
    CHECK_GOTO(fake_stream_init(&request, 4, request_bytes, sizeof(request_bytes), false) == 0);
    request_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &retired) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(fake_stream_wait_close_calls(&retired, 1) == 0);
    CHECK_GOTO(fake_conn_enqueue(&conn, &request) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &item) == 0);
    CHECK_GOTO(item._transport_stream == &request);
    CHECK_GOTO(retired.close_calls == 1);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(runtime, &fatal_error) == -EAGAIN);
    CHECK_GOTO(fake_conn_shutdown_calls(&conn) == 0 && fake_conn_shutdown_error_calls(&conn) == 0);
    trevrpc_h3_ingress_item_close(&item);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (request_initialized) {
        fake_stream_destroy(&request);
    }
    if (retired_initialized) {
        fake_stream_destroy(&retired);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_transport_failures_publish_exact_h3_errors(void) {
    if (run_accept_error_case(TREV_MSQUIC_ERR_CLOSED, 0) != 0) {
        return 1;
    }
    if (run_accept_error_case(TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED, TREV_H3_INGRESS_APP_EXCESSIVE_LOAD) != 0) {
        return 1;
    }
    if (run_accept_error_case(-EIO, TREV_H3_INGRESS_APP_INTERNAL_ERROR) != 0) {
        return 1;
    }
    if (run_stream_setup_error_case(TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED, 0, TREV_H3_INGRESS_APP_EXCESSIVE_LOAD) != 0) {
        return 1;
    }
    if (run_stream_setup_error_case(0, -EIO, TREV_H3_INGRESS_APP_INTERNAL_ERROR) != 0) {
        return 1;
    }
    if (run_stream_read_error_case(0, -ENOMEM, TREV_H3_INGRESS_APP_EXCESSIVE_LOAD) != 0) {
        return 1;
    }
    if (run_stream_read_error_case(0, -EIO, TREV_H3_INGRESS_APP_INTERNAL_ERROR) != 0) {
        return 1;
    }
    return run_stream_read_error_case(2, TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED, TREV_H3_INGRESS_APP_EXCESSIVE_LOAD);
}

static int test_release_rejects_pre_registry_call_after_runtime_address_reuse(void) {
    int result = 1;
    fake_conn old_conn;
    fake_conn new_conn;
    bool old_conn_initialized = false;
    bool new_conn_initialized = false;
    bool recycle_enabled = false;
    trevrpc_h3_ingress* old_handle = NULL;
    trevrpc_h3_ingress* new_handle = NULL;
    admitted_api_call call = {.kind = ADMITTED_API_PUBLISH_SETTINGS};
    pthread_t call_thread;
    bool call_started = false;
    void* old_runtime_address = NULL;
    uint64_t fatal_error = 0;

    CHECK_GOTO(fake_conn_init(&old_conn) == 0);
    old_conn_initialized = true;
    CHECK_GOTO(fake_conn_init(&new_conn) == 0);
    new_conn_initialized = true;
    trevrpc_h3_ingress_test_recycle_runtime_allocations(1);
    recycle_enabled = true;

    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&old_conn, &fake_ops, &config, &old_handle) == 0);
    old_runtime_address = trevrpc_h3_ingress_test_runtime_address(old_handle);
    CHECK_GOTO(old_runtime_address != NULL);
    call.runtime = old_handle;
    trevrpc_h3_ingress_test_pause_before_reference(old_handle, 1);
    CHECK_GOTO(pthread_create(&call_thread, NULL, admitted_api_call_main, &call) == 0);
    call_started = true;
    trevrpc_h3_ingress_test_wait_before_reference_paused(old_handle);

    trevrpc_h3_ingress_release(old_handle);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&new_conn, &fake_ops, &config, &new_handle) == 0);
    CHECK_GOTO(new_handle != old_handle);
    CHECK_GOTO(trevrpc_h3_ingress_test_runtime_address(new_handle) == old_runtime_address);

    trevrpc_h3_ingress_test_pause_before_reference(old_handle, 0);
    CHECK_GOTO(pthread_join(call_thread, NULL) == 0);
    call_started = false;
    CHECK_GOTO(call.result == -EPIPE);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   new_handle, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_NONE, 0) == 0);
    int settings_status = 0;
    trevrpc_wt_profile_id selected_profile = TREV_WT_PROFILE_DRAFT_15;
    uint64_t settings_error = UINT64_MAX;
    CHECK_GOTO(
        trevrpc_h3_ingress_wait_peer_settings(new_handle, &settings_status, &selected_profile, &settings_error) == 0);
    CHECK_GOTO(settings_status == TREV_H3_INGRESS_SETTINGS_READY && selected_profile == TREV_WT_PROFILE_NONE &&
               settings_error == 0);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(new_handle, &fatal_error) == -EAGAIN);
    CHECK_GOTO(fake_conn_shutdown_error_calls(&new_conn) == 0);
    result = 0;

cleanup:
    trevrpc_h3_ingress_test_pause_before_reference(old_handle, 0);
    if (call_started) {
        (void)pthread_join(call_thread, NULL);
    }
    trevrpc_h3_ingress_release(new_handle);
    trevrpc_h3_ingress_release(old_handle);
    if (recycle_enabled) {
        trevrpc_h3_ingress_test_recycle_runtime_allocations(0);
    }
    if (new_conn_initialized) {
        fake_conn_destroy(&new_conn);
    }
    if (old_conn_initialized) {
        fake_conn_destroy(&old_conn);
    }
    return result;
}

static int run_release_admission_case(admitted_api_kind kind) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    admitted_api_call call = {.kind = kind};
    pthread_t call_thread;
    bool call_started = false;
    lifecycle_wait release = {0};
    bool release_initialized = false;
    pthread_t release_thread;
    bool release_started = false;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    trevrpc_h3_ingress_config config = test_config(1, 1);
    if (kind == ADMITTED_API_POP_UNCLASSIFIED) {
        config.bidi_mode = TREV_H3_INGRESS_BIDI_HANDOFF_UNCLASSIFIED;
    }
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    call.runtime = runtime;
    trevrpc_h3_ingress_test_pause_after_admission(runtime, 1);
    CHECK_GOTO(pthread_create(&call_thread, NULL, admitted_api_call_main, &call) == 0);
    call_started = true;
    trevrpc_h3_ingress_test_wait_admission_paused(runtime);
    CHECK_GOTO(lifecycle_wait_init(&release, runtime, true) == 0);
    release_initialized = true;
    CHECK_GOTO(pthread_create(&release_thread, NULL, lifecycle_wait_main, &release) == 0);
    release_started = true;
    lifecycle_wait_started(&release);
    trevrpc_h3_ingress_test_wait_runtime_detached(runtime);
    CHECK_GOTO(!lifecycle_wait_finished(&release));

    trevrpc_h3_ingress_test_pause_after_admission(runtime, 0);
    CHECK_GOTO(pthread_join(call_thread, NULL) == 0);
    call_started = false;
    CHECK_GOTO(pthread_join(release_thread, NULL) == 0);
    release_started = false;
    runtime = NULL;
    switch (kind) {
    case ADMITTED_API_START:
    case ADMITTED_API_FAIL:
    case ADMITTED_API_PUBLISH_SETTINGS:
        CHECK_GOTO(call.result == -EALREADY);
        break;
    case ADMITTED_API_POP:
    case ADMITTED_API_POP_BIDI:
    case ADMITTED_API_POP_UNCLASSIFIED:
        CHECK_GOTO(call.result == -ECANCELED);
        break;
    case ADMITTED_API_FATAL_ERROR:
        CHECK_GOTO(call.result == -EAGAIN);
        break;
    case ADMITTED_API_WAIT_SETTINGS:
        CHECK_GOTO(call.result == 0);
        CHECK_GOTO(call.settings_status == TREV_H3_INGRESS_SETTINGS_FAILED);
        CHECK_GOTO(call.application_error == 0);
        break;
    case ADMITTED_API_SHUTDOWN:
        CHECK_GOTO(call.result == 0);
        break;
    }
    result = 0;

cleanup:
    if (runtime != NULL) {
        trevrpc_h3_ingress_test_pause_after_admission(runtime, 0);
        if (call_started && !release_started) {
            trevrpc_h3_ingress_shutdown(runtime);
        }
    }
    if (call_started) {
        (void)pthread_join(call_thread, NULL);
    }
    if (release_started) {
        (void)pthread_join(release_thread, NULL);
        runtime = NULL;
    }
    trevrpc_h3_ingress_item_close(&call.item);
    trevrpc_h3_ingress_release(runtime);
    if (release_initialized) {
        lifecycle_wait_destroy(&release);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_release_admission_gate_covers_runtime_apis(void) {
    const admitted_api_kind kinds[] = {
        ADMITTED_API_START,
        ADMITTED_API_POP,
        ADMITTED_API_POP_BIDI,
        ADMITTED_API_POP_UNCLASSIFIED,
        ADMITTED_API_FAIL,
        ADMITTED_API_FATAL_ERROR,
        ADMITTED_API_PUBLISH_SETTINGS,
        ADMITTED_API_WAIT_SETTINGS,
        ADMITTED_API_SHUTDOWN,
    };
    for (size_t i = 0; i < sizeof(kinds) / sizeof(kinds[0]); i++) {
        if (run_release_admission_case(kinds[i]) != 0) {
            return 1;
        }
    }
    return 0;
}

static int test_shutdown_waits_for_observer_install(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream stream;
    bool stream_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    lifecycle_wait shutdown = {0};
    bool shutdown_initialized = false;
    pthread_t shutdown_thread;
    bool shutdown_started = false;
    const uint8_t request[] = {0x01};

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&stream, 0, request, sizeof(request), false) == 0);
    stream_initialized = true;
    stream.block_observer_set = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &stream) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(fake_stream_wait_observer_set_entered(&stream) == 0);
    CHECK_GOTO(lifecycle_wait_init(&shutdown, runtime, false) == 0);
    shutdown_initialized = true;
    CHECK_GOTO(pthread_create(&shutdown_thread, NULL, lifecycle_wait_main, &shutdown) == 0);
    shutdown_started = true;
    lifecycle_wait_started(&shutdown);
    CHECK_GOTO(!lifecycle_wait_finished(&shutdown));

    fake_stream_release_observer_set(&stream);
    CHECK_GOTO(pthread_join(shutdown_thread, NULL) == 0);
    shutdown_started = false;
    CHECK_GOTO(stream.observer_set_calls == 1);
    CHECK_GOTO(stream.clear_calls == 1 && stream.drain_calls == 1 && stream.close_calls == 1);
    result = 0;

cleanup:
    if (stream_initialized) {
        fake_stream_release_observer_set(&stream);
    }
    if (shutdown_started) {
        (void)pthread_join(shutdown_thread, NULL);
    }
    trevrpc_h3_ingress_release(runtime);
    if (shutdown_initialized) {
        lifecycle_wait_destroy(&shutdown);
    }
    if (stream_initialized) {
        fake_stream_destroy(&stream);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_shutdown_waits_for_active_observer_callback(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream stream;
    bool stream_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    pthread_t notify_thread;
    bool notify_started = false;
    lifecycle_wait shutdown = {0};
    bool shutdown_initialized = false;
    pthread_t shutdown_thread;
    bool shutdown_started = false;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&stream, 0, NULL, 0, false) == 0);
    stream_initialized = true;
    stream.block_callback = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &stream) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(fake_stream_wait_observer_set(&stream) == 0);
    CHECK_GOTO(pthread_create(&notify_thread, NULL, stream_readable_notify_main, &stream) == 0);
    notify_started = true;
    CHECK_GOTO(fake_stream_wait_callback_entered(&stream) == 0);
    CHECK_GOTO(lifecycle_wait_init(&shutdown, runtime, false) == 0);
    shutdown_initialized = true;
    CHECK_GOTO(pthread_create(&shutdown_thread, NULL, lifecycle_wait_main, &shutdown) == 0);
    shutdown_started = true;
    CHECK_GOTO(fake_stream_wait_drain(&stream) == 0);
    CHECK_GOTO(!lifecycle_wait_finished(&shutdown));

    fake_stream_release_callback(&stream);
    CHECK_GOTO(pthread_join(notify_thread, NULL) == 0);
    notify_started = false;
    CHECK_GOTO(pthread_join(shutdown_thread, NULL) == 0);
    shutdown_started = false;
    CHECK_GOTO(stream.clear_calls == 1 && stream.drain_calls == 1 && stream.close_calls == 1);
    result = 0;

cleanup:
    if (stream_initialized) {
        fake_stream_release_callback(&stream);
    }
    if (notify_started) {
        (void)pthread_join(notify_thread, NULL);
    }
    if (shutdown_started) {
        (void)pthread_join(shutdown_thread, NULL);
    }
    trevrpc_h3_ingress_release(runtime);
    if (shutdown_initialized) {
        lifecycle_wait_destroy(&shutdown);
    }
    if (stream_initialized) {
        fake_stream_destroy(&stream);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_release_waits_for_pop_finalization(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream stream;
    bool stream_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    const uint8_t request[] = {0x01};
    pop_wait pop = {0};
    pthread_t pop_thread;
    bool pop_started = false;
    lifecycle_wait release = {0};
    bool release_initialized = false;
    pthread_t release_thread;
    bool release_started = false;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&stream, 0, request, sizeof(request), false) == 0);
    stream_initialized = true;
    stream.block_drain = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &stream) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    pop.runtime = runtime;
    CHECK_GOTO(pthread_create(&pop_thread, NULL, pop_wait_main, &pop) == 0);
    pop_started = true;
    CHECK_GOTO(fake_stream_wait_drain(&stream) == 0);
    CHECK_GOTO(lifecycle_wait_init(&release, runtime, true) == 0);
    release_initialized = true;
    CHECK_GOTO(pthread_create(&release_thread, NULL, lifecycle_wait_main, &release) == 0);
    release_started = true;
    lifecycle_wait_started(&release);
    CHECK_GOTO(!lifecycle_wait_finished(&release));

    fake_stream_release_drain(&stream);
    CHECK_GOTO(pthread_join(pop_thread, NULL) == 0);
    pop_started = false;
    CHECK_GOTO(pthread_join(release_thread, NULL) == 0);
    release_started = false;
    runtime = NULL;
    CHECK_GOTO(pop.result == 0);
    CHECK_GOTO(pop.item._transport_stream == &stream);
    trevrpc_h3_ingress_item_close(&pop.item);
    CHECK_GOTO(stream.close_calls == 1);
    result = 0;

cleanup:
    if (stream_initialized) {
        fake_stream_release_drain(&stream);
    }
    if (pop_started) {
        (void)pthread_join(pop_thread, NULL);
    }
    if (release_started) {
        (void)pthread_join(release_thread, NULL);
        runtime = NULL;
    }
    trevrpc_h3_ingress_item_close(&pop.item);
    trevrpc_h3_ingress_release(runtime);
    if (release_initialized) {
        lifecycle_wait_destroy(&release);
    }
    if (stream_initialized) {
        fake_stream_destroy(&stream);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_release_waits_for_concurrent_shutdown_leader(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    lifecycle_wait shutdown = {0};
    lifecycle_wait release = {0};
    bool shutdown_initialized = false;
    bool release_initialized = false;
    pthread_t shutdown_thread;
    pthread_t release_thread;
    bool shutdown_started = false;
    bool release_started = false;
    uint64_t fatal_error = 0;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    conn.block_shutdown = true;
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(lifecycle_wait_init(&shutdown, runtime, false) == 0);
    shutdown_initialized = true;
    CHECK_GOTO(lifecycle_wait_init(&release, runtime, true) == 0);
    release_initialized = true;
    CHECK_GOTO(pthread_create(&shutdown_thread, NULL, lifecycle_wait_main, &shutdown) == 0);
    shutdown_started = true;
    CHECK_GOTO(fake_conn_wait_shutdown(&conn) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_fail(runtime, TREV_H3_DEMUX_APP_FRAME_ERROR) == -EALREADY);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(runtime, &fatal_error) == -EAGAIN);
    CHECK_GOTO(fake_conn_shutdown_error_calls(&conn) == 0);
    CHECK_GOTO(pthread_create(&release_thread, NULL, lifecycle_wait_main, &release) == 0);
    release_started = true;
    lifecycle_wait_started(&release);
    CHECK_GOTO(!lifecycle_wait_finished(&release));

    fake_conn_release_shutdown(&conn);
    CHECK_GOTO(pthread_join(shutdown_thread, NULL) == 0);
    shutdown_started = false;
    CHECK_GOTO(pthread_join(release_thread, NULL) == 0);
    release_started = false;
    runtime = NULL;
    CHECK_GOTO(fake_conn_shutdown_calls(&conn) == 1);
    result = 0;

cleanup:
    if (conn_initialized) {
        fake_conn_release_shutdown(&conn);
    }
    if (shutdown_started) {
        (void)pthread_join(shutdown_thread, NULL);
    }
    if (release_started) {
        (void)pthread_join(release_thread, NULL);
        runtime = NULL;
    }
    trevrpc_h3_ingress_release(runtime);
    if (release_initialized) {
        lifecycle_wait_destroy(&release);
    }
    if (shutdown_initialized) {
        lifecycle_wait_destroy(&shutdown);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int run_shutdown_callback_release_reentry(bool fatal) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    const uint64_t application_error = TREV_H3_DEMUX_APP_FRAME_ERROR;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    pthread_mutex_lock(&conn.mutex);
    conn.release_runtime_on_shutdown = runtime;
    pthread_mutex_unlock(&conn.mutex);

    if (fatal) {
        CHECK_GOTO(trevrpc_h3_ingress_fail(runtime, application_error) == 0);
    } else {
        trevrpc_h3_ingress_shutdown(runtime);
    }
    trevrpc_h3_ingress_test_wait_runtime_reaped(runtime);
    CHECK_GOTO(trevrpc_h3_ingress_test_runtime_reap_count(runtime) == 1);
    trevrpc_h3_ingress_release(runtime);
    CHECK_GOTO(trevrpc_h3_ingress_test_runtime_reap_count(runtime) == 1);

    pthread_mutex_lock(&conn.mutex);
    bool release_returned = conn.release_runtime_returned;
    size_t shutdown_calls = conn.shutdown_calls;
    size_t shutdown_error_calls = conn.shutdown_error_calls;
    uint64_t first_error = conn.first_error;
    pthread_mutex_unlock(&conn.mutex);
    CHECK_GOTO(release_returned);
    CHECK_GOTO(shutdown_calls == (fatal ? 0u : 1u));
    CHECK_GOTO(shutdown_error_calls == (fatal ? 1u : 0u));
    CHECK_GOTO(first_error == (fatal ? application_error : 0));
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == -EPIPE);
    result = 0;

cleanup:
    trevrpc_h3_ingress_release(runtime);
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_shutdown_callbacks_allow_synchronous_release_reentry(void) {
    CHECK_GOTO(run_shutdown_callback_release_reentry(false) == 0);
    CHECK_GOTO(run_shutdown_callback_release_reentry(true) == 0);
    return 0;
cleanup:
    return 1;
}

static int test_internal_shutdown_callback_reentry_reaps_after_thread_exit(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream stream;
    bool stream_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&stream, 0, NULL, 0, false) == 0);
    stream_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &stream) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    pthread_mutex_lock(&conn.mutex);
    conn.release_runtime_on_shutdown = runtime;
    pthread_mutex_unlock(&conn.mutex);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(fake_stream_wait_observer_set(&stream) == 0);

    pthread_mutex_lock(&stream.mutex);
    stream.read_error = -EIO;
    pthread_mutex_unlock(&stream.mutex);
    fake_stream_notify(&stream, TREV_MSQUIC_STREAM_OBSERVER_TERMINAL);
    trevrpc_h3_ingress_test_wait_runtime_detached(runtime);
    trevrpc_h3_ingress_test_wait_runtime_reaped(runtime);
    CHECK_GOTO(trevrpc_h3_ingress_test_runtime_reap_count(runtime) == 1);
    trevrpc_h3_ingress_release(runtime);
    CHECK_GOTO(trevrpc_h3_ingress_test_runtime_reap_count(runtime) == 1);

    CHECK_GOTO(fake_conn_shutdown_error_calls(&conn) == 1);
    CHECK_GOTO(fake_conn_first_error(&conn) == TREV_H3_INGRESS_APP_INTERNAL_ERROR);
    CHECK_GOTO(stream.close_calls == 1);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == -EPIPE);
    result = 0;

cleanup:
    trevrpc_h3_ingress_release(runtime);
    if (stream_initialized) {
        fake_stream_destroy(&stream);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_pending_profile_wakes_fragmented_webtransport_streams(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream bidi;
    fake_stream uni;
    bool bidi_initialized = false;
    bool uni_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    const uint8_t bidi_bytes[] = {0x40, 0x41, 0x40, 0x40, 0xa4};
    const uint8_t uni_bytes[] = {0x40, 0x54, 0x40, 0x40, 0xa5};

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&bidi, 0, bidi_bytes, sizeof(bidi_bytes), false) == 0);
    bidi_initialized = true;
    CHECK_GOTO(fake_stream_init(&uni, 2, uni_bytes, sizeof(uni_bytes), false) == 0);
    uni_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &bidi) == 0);
    CHECK_GOTO(fake_conn_enqueue(&conn, &uni) == 0);
    trevrpc_h3_ingress_config config = test_config(2, 2);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(fake_stream_wait_offset(&bidi, 2) == 0);
    CHECK_GOTO(fake_stream_wait_offset(&uni, 2) == 0);
    CHECK_GOTO(bidi.offset == 2 && uni.offset == 2);

    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   runtime, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_DRAFT_15, 0) == 0);
    bool saw_bidi = false;
    bool saw_uni = false;
    for (size_t i = 0; i < 2; i++) {
        CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_WEBTRANSPORT, TEST_TIMEOUT_NANOS, &item) == 0);
        CHECK_GOTO(item.session_id == 64);
        if (item._transport_stream == &bidi) {
            CHECK_GOTO(item.direction == TREV_H3_DEMUX_BIDIRECTIONAL);
            CHECK_GOTO(item.first_value == TREV_H3_DEMUX_WEBTRANSPORT_BIDI);
            saw_bidi = true;
        } else {
            CHECK_GOTO(item._transport_stream == &uni);
            CHECK_GOTO(item.direction == TREV_H3_DEMUX_UNIDIRECTIONAL);
            CHECK_GOTO(item.first_value == TREV_H3_DEMUX_WEBTRANSPORT_UNI);
            saw_uni = true;
        }
        trevrpc_h3_ingress_item_close(&item);
    }
    CHECK_GOTO(saw_bidi && saw_uni);
    CHECK_GOTO(bidi.offset == 4 && uni.offset == 4);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (uni_initialized) {
        fake_stream_destroy(&uni);
    }
    if (bidi_initialized) {
        fake_stream_destroy(&bidi);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_none_profile_treats_bidi_marker_as_unknown_and_retires_uni(void) {
    int result = 1;
    fake_conn uni_conn;
    fake_conn bidi_conn;
    bool uni_conn_initialized = false;
    bool bidi_conn_initialized = false;
    fake_stream uni;
    fake_stream bidi;
    bool uni_initialized = false;
    bool bidi_initialized = false;
    trevrpc_h3_ingress* uni_runtime = NULL;
    trevrpc_h3_ingress* bidi_runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    uint64_t fatal_error = 0;
    const uint8_t uni_bytes[] = {0x40, 0x54, 0x00, 0xa5};
    const uint8_t bidi_bytes[] = {0x40, 0x41, 0x00, 0x01, 0xa4};

    CHECK_GOTO(fake_conn_init(&uni_conn) == 0);
    uni_conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&uni, 2, uni_bytes, sizeof(uni_bytes), false) == 0);
    uni_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&uni_conn, &uni) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&uni_conn, &fake_ops, &config, &uni_runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(uni_runtime) == 0);
    CHECK_GOTO(fake_stream_wait_offset(&uni, 2) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   uni_runtime, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_NONE, 0) == 0);
    CHECK_GOTO(fake_stream_wait_close_calls(&uni, 1) == 0);
    CHECK_GOTO(uni.offset == 2);
    CHECK_GOTO(uni.abort_calls == 1);
    CHECK_GOTO(uni.abort_application_error == TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(uni_runtime, &fatal_error) == -EAGAIN);

    CHECK_GOTO(fake_conn_init(&bidi_conn) == 0);
    bidi_conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&bidi, 0, bidi_bytes, sizeof(bidi_bytes), false) == 0);
    bidi_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&bidi_conn, &bidi) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&bidi_conn, &fake_ops, &config, &bidi_runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(bidi_runtime) == 0);
    CHECK_GOTO(fake_stream_wait_offset(&bidi, 2) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   bidi_runtime, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_NONE, 0) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop(bidi_runtime, TREV_H3_DEMUX_ACTION_REQUEST, TEST_TIMEOUT_NANOS, &item) == 0);
    CHECK_GOTO(item._transport_stream == &bidi);
    CHECK_GOTO(item.first_value == TREV_H3_FRAME_HEADERS);
    CHECK_GOTO(bidi.offset == 4);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(bidi_runtime, &fatal_error) == -EAGAIN);
    trevrpc_h3_ingress_item_close(&item);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(bidi_runtime);
    trevrpc_h3_ingress_release(uni_runtime);
    if (bidi_initialized) {
        fake_stream_destroy(&bidi);
    }
    if (uni_initialized) {
        fake_stream_destroy(&uni);
    }
    if (bidi_conn_initialized) {
        fake_conn_destroy(&bidi_conn);
    }
    if (uni_conn_initialized) {
        fake_conn_destroy(&uni_conn);
    }
    return result;
}

static int run_invalid_webtransport_session_case(uint64_t stream_id, uint8_t stream_type) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream stream;
    bool stream_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    uint64_t fatal_error = 0;
    const uint8_t bytes[] = {0x40, stream_type, 0x02, 0xaa};

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&stream, stream_id, bytes, sizeof(bytes), false) == 0);
    stream_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &stream) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_publish_peer_settings(
                   runtime, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_DRAFT_15, 0) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(
        trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_WEBTRANSPORT, TEST_TIMEOUT_NANOS, &item) == -EPROTO);
    CHECK_GOTO(stream.offset == 3);
    CHECK_GOTO(trevrpc_h3_ingress_fatal_error(runtime, &fatal_error) == 0);
    CHECK_GOTO(fatal_error == TREV_H3_DEMUX_APP_ID_ERROR);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (stream_initialized) {
        fake_stream_destroy(&stream);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_invalid_webtransport_session_ids_are_direction_independent(void) {
    CHECK_GOTO(run_invalid_webtransport_session_case(0, TREV_H3_DEMUX_WEBTRANSPORT_BIDI) == 0);
    CHECK_GOTO(run_invalid_webtransport_session_case(2, TREV_H3_DEMUX_WEBTRANSPORT_UNI) == 0);
    return 0;
cleanup:
    return 1;
}

static int test_profile_publication_release_race_closes_parked_stream(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream stream;
    bool stream_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    admitted_api_call publish = {.kind = ADMITTED_API_PUBLISH_SETTINGS};
    pthread_t publish_thread;
    bool publish_started = false;
    lifecycle_wait release = {0};
    bool release_initialized = false;
    pthread_t release_thread;
    bool release_started = false;
    const uint8_t bytes[] = {0x40, 0x54, 0x00};

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&stream, 2, bytes, sizeof(bytes), false) == 0);
    stream_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &stream) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(fake_stream_wait_offset(&stream, 2) == 0);

    publish.runtime = runtime;
    trevrpc_h3_ingress_test_pause_after_admission(runtime, 1);
    CHECK_GOTO(pthread_create(&publish_thread, NULL, admitted_api_call_main, &publish) == 0);
    publish_started = true;
    trevrpc_h3_ingress_test_wait_admission_paused(runtime);
    CHECK_GOTO(lifecycle_wait_init(&release, runtime, true) == 0);
    release_initialized = true;
    CHECK_GOTO(pthread_create(&release_thread, NULL, lifecycle_wait_main, &release) == 0);
    release_started = true;
    lifecycle_wait_started(&release);
    trevrpc_h3_ingress_test_wait_runtime_detached(runtime);
    CHECK_GOTO(!lifecycle_wait_finished(&release));
    CHECK_GOTO(fake_stream_wait_close_calls(&stream, 1) == 0);

    trevrpc_h3_ingress_test_pause_after_admission(runtime, 0);
    CHECK_GOTO(pthread_join(publish_thread, NULL) == 0);
    publish_started = false;
    CHECK_GOTO(pthread_join(release_thread, NULL) == 0);
    release_started = false;
    runtime = NULL;
    CHECK_GOTO(publish.result == -EALREADY);
    CHECK_GOTO(stream.close_calls == 1);
    CHECK_GOTO(stream.abort_calls == 0);
    CHECK_GOTO(stream.offset == 2);
    result = 0;

cleanup:
    if (runtime != NULL) {
        trevrpc_h3_ingress_test_pause_after_admission(runtime, 0);
    }
    if (publish_started) {
        (void)pthread_join(publish_thread, NULL);
    }
    if (release_started) {
        (void)pthread_join(release_thread, NULL);
        runtime = NULL;
    }
    trevrpc_h3_ingress_release(runtime);
    if (release_initialized) {
        lifecycle_wait_destroy(&release);
    }
    if (stream_initialized) {
        fake_stream_destroy(&stream);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_shutdown_closes_profile_parked_stream(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream stream;
    bool stream_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    const uint8_t bytes[] = {0x40, 0x54, 0x00};

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&stream, 2, bytes, sizeof(bytes), false) == 0);
    stream_initialized = true;
    CHECK_GOTO(fake_conn_enqueue(&conn, &stream) == 0);
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);
    CHECK_GOTO(fake_stream_wait_offset(&stream, 2) == 0);
    trevrpc_h3_ingress_shutdown(runtime);
    CHECK_GOTO(stream.close_calls == 1);
    CHECK_GOTO(stream.abort_calls == 0);
    CHECK_GOTO(stream.offset == 2);
    result = 0;

cleanup:
    trevrpc_h3_ingress_release(runtime);
    if (stream_initialized) {
        fake_stream_destroy(&stream);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

typedef struct skew_clock {
    size_t monotonic_calls;
    size_t realtime_calls;
} skew_clock;

static int skew_realtime_clock(void* context, clockid_t clock_id, struct timespec* out_time) {
    skew_clock* clock = context;
    int result = clock_gettime(clock_id, out_time);
    if (result != 0) {
        return result;
    }
    if (clock_id == CLOCK_REALTIME) {
        clock->realtime_calls++;
        out_time->tv_sec -= 3600;
    } else if (clock_id == CLOCK_MONOTONIC) {
        clock->monotonic_calls++;
    }
    return 0;
}

static int test_deadline_boundary_enqueue_wins_over_timeout(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    fake_stream stream;
    bool stream_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    pop_wait pop = {0};
    pthread_t pop_thread;
    bool pop_started = false;
    const uint8_t request[] = {0x01};

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    CHECK_GOTO(fake_stream_init(&stream, 0, request, sizeof(request), false) == 0);
    stream_initialized = true;
    trevrpc_h3_ingress_config config = test_config(1, 1);
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_start(runtime) == 0);

    pop.runtime = runtime;
    pop.timeout_nanos = TEST_TIMEOUT_NANOS;
    trevrpc_h3_ingress_test_force_next_timed_wait_timeout();
    CHECK_GOTO(pthread_create(&pop_thread, NULL, pop_wait_main, &pop) == 0);
    pop_started = true;
    CHECK_GOTO(trevrpc_h3_ingress_test_wait_timed_wait_entered() == 0);
    CHECK_GOTO(fake_conn_enqueue(&conn, &stream) == 0);
    CHECK_GOTO(pthread_join(pop_thread, NULL) == 0);
    pop_started = false;
    CHECK_GOTO(pop.result == 0);
    CHECK_GOTO(pop.item._transport_stream == &stream);
    trevrpc_h3_ingress_item_close(&pop.item);
    CHECK_GOTO(stream.close_calls == 1);
    result = 0;

cleanup:
    if (pop_started) {
        trevrpc_h3_ingress_shutdown(runtime);
        (void)pthread_join(pop_thread, NULL);
    }
    trevrpc_h3_ingress_item_close(&pop.item);
    trevrpc_h3_ingress_release(runtime);
    if (stream_initialized) {
        fake_stream_destroy(&stream);
    }
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_pop_timeout_uses_monotonic_clock(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    trevrpc_h3_ingress* runtime = NULL;
    trevrpc_h3_ingress_item item = {0};
    skew_clock clock = {0};
    uint64_t before = 0;
    uint64_t after = 0;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    trevrpc_h3_ingress_config config = test_config(1, 1);
    config.clock_gettime = skew_realtime_clock;
    config.clock_context = &clock;
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &runtime) == 0);
    CHECK_GOTO(test_monotonic_nanos(&before) == 0);
    CHECK_GOTO(trevrpc_h3_ingress_pop(runtime, TREV_H3_DEMUX_ACTION_REQUEST, UINT64_C(200000000), &item) == -ETIMEDOUT);
    CHECK_GOTO(test_monotonic_nanos(&after) == 0);
    CHECK_GOTO(after - before >= UINT64_C(100000000));
    CHECK_GOTO(after - before < UINT64_C(2000000000));
    CHECK_GOTO(clock.monotonic_calls != 0);
    CHECK_GOTO(clock.realtime_calls == 0);
    result = 0;

cleanup:
    trevrpc_h3_ingress_item_close(&item);
    trevrpc_h3_ingress_release(runtime);
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

static int test_release_unlinks_tombstone_and_bounds_registry(void) {
    int result = 1;
    fake_conn conn;
    bool conn_initialized = false;
    trevrpc_h3_ingress* live = NULL;
    trevrpc_h3_ingress* released = NULL;
    admitted_api_call call = {.kind = ADMITTED_API_START};
    pthread_t call_thread;
    bool call_started = false;
    size_t baseline = 0;
    size_t baseline_released = 0;

    CHECK_GOTO(fake_conn_init(&conn) == 0);
    conn_initialized = true;
    trevrpc_h3_ingress_config config = test_config(1, 1);

    trevrpc_h3_ingress_test_released_count_reset();
    CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &live) == 0);
    baseline = trevrpc_h3_ingress_test_registry_size();
    baseline_released = trevrpc_h3_ingress_test_released_count_get();

    /*
     * Churn: create/release a handle while a stale call is paused before its
     * registry reference. Each release must move the tombstone out of the
     * live registry (which admission walks) into the graveyard while keeping
     * its memory allocated, and the stale call must still fail closed with
     * -EPIPE on the membership check.
     */
    for (size_t i = 0; i < 16; i++) {
        CHECK_GOTO(trevrpc_h3_ingress_create_with_ops(&conn, &fake_ops, &config, &released) == 0);
        CHECK_GOTO(released != NULL);
        call.runtime = released;
        trevrpc_h3_ingress_test_pause_before_reference(released, 1);
        CHECK_GOTO(pthread_create(&call_thread, NULL, admitted_api_call_main, &call) == 0);
        call_started = true;
        trevrpc_h3_ingress_test_wait_before_reference_paused(released);

        trevrpc_h3_ingress_release(released);
        CHECK_GOTO(trevrpc_h3_ingress_test_registry_size() == baseline);
        CHECK_GOTO(trevrpc_h3_ingress_test_released_count_get() == baseline_released + i + 1);

        trevrpc_h3_ingress_test_pause_before_reference(released, 0);
        CHECK_GOTO(pthread_join(call_thread, NULL) == 0);
        call_started = false;
        CHECK_GOTO(call.result == -EPIPE);

        trevrpc_h3_ingress_release(released);
        released = NULL;
        CHECK_GOTO(trevrpc_h3_ingress_test_registry_size() == baseline);
        CHECK_GOTO(trevrpc_h3_ingress_test_released_count_get() == baseline_released + i + 1);
    }

    CHECK_GOTO(
        trevrpc_h3_ingress_publish_peer_settings(live, TREV_H3_INGRESS_SETTINGS_READY, TREV_WT_PROFILE_NONE, 0) == 0);
    result = 0;

cleanup:
    if (call_started) {
        trevrpc_h3_ingress_test_pause_before_reference(call.runtime, 0);
        (void)pthread_join(call_thread, NULL);
    }
    trevrpc_h3_ingress_release(released);
    trevrpc_h3_ingress_release(live);
    if (conn_initialized) {
        fake_conn_destroy(&conn);
    }
    return result;
}

int main(void) {
    typedef struct ingress_test_case {
        const char* name;
        int (*run)(void);
    } ingress_test_case;
#define TEST_CASE(test_function) ((ingress_test_case){#test_function, test_function})
    const ingress_test_case tests[] = {
        TEST_CASE(test_handoff_unclassified_bidi_skips_classifier),
        TEST_CASE(test_transport_ops_and_item_own_callback_table),
        TEST_CASE(test_closed_stream_id_retires_only_accepted_stream),
        TEST_CASE(test_classifies_and_hands_off_without_overread),
        TEST_CASE(test_request_classification_skips_unknown_frames_without_overread),
        TEST_CASE(test_pop_bidi_preserves_classification_ready_order),
        TEST_CASE(test_fragmented_coalesced_wakes_and_terminal_after_classification),
        TEST_CASE(test_pending_profile_wakes_fragmented_webtransport_streams),
        TEST_CASE(test_none_profile_treats_bidi_marker_as_unknown_and_retires_uni),
        TEST_CASE(test_invalid_webtransport_session_ids_are_direction_independent),
        TEST_CASE(test_profile_publication_release_race_closes_parked_stream),
        TEST_CASE(test_shutdown_closes_profile_parked_stream),
        TEST_CASE(test_deadline_boundary_enqueue_wins_over_timeout),
        TEST_CASE(test_pop_timeout_uses_monotonic_clock),
        TEST_CASE(test_fragmented_legal_four_byte_request_prefix),
        TEST_CASE(test_fragmented_legal_eight_byte_request_prefix),
        TEST_CASE(test_unknown_slot_is_reused_sequentially),
        TEST_CASE(test_unknown_and_terminal_unidirectional_are_reclaimed),
        TEST_CASE(test_terminal_error_is_first_wins_and_wakes_settings),
        TEST_CASE(test_duplicate_qpack_roles_are_fatal),
        TEST_CASE(test_duplicate_role_and_capacity_overflow_errors),
        TEST_CASE(test_queue_overflow_and_settings_publication),
        TEST_CASE(test_unclassified_bidi_queue_overflow_is_first_wins),
        TEST_CASE(test_settings_profile_publication_validation),
        TEST_CASE(test_settings_failure_publication_is_fatal),
        TEST_CASE(test_concurrent_first_fatal_arbitration),
        TEST_CASE(test_settings_barrier_observes_fatal_and_normal_stop_wins),
        TEST_CASE(test_shutdown_wakes_blocked_waiters),
        TEST_CASE(test_bidi_pop_waiters_wake_on_shutdown),
        TEST_CASE(test_start_shutdown_race_is_serialized),
        TEST_CASE(test_closed_prefix_read_retires_only_accepted_stream),
        TEST_CASE(test_transport_failures_publish_exact_h3_errors),
        TEST_CASE(test_release_rejects_pre_registry_call_after_runtime_address_reuse),
        TEST_CASE(test_release_admission_gate_covers_runtime_apis),
        TEST_CASE(test_shutdown_waits_for_observer_install),
        TEST_CASE(test_shutdown_waits_for_active_observer_callback),
        TEST_CASE(test_release_waits_for_pop_finalization),
        TEST_CASE(test_release_waits_for_concurrent_shutdown_leader),
        TEST_CASE(test_shutdown_callbacks_allow_synchronous_release_reentry),
        TEST_CASE(test_internal_shutdown_callback_reentry_reaps_after_thread_exit),
        TEST_CASE(test_release_unlinks_tombstone_and_bounds_registry),
    };
#undef TEST_CASE
    const size_t test_count = sizeof(tests) / sizeof(tests[0]);
    for (size_t i = 0; i < test_count; i++) {
        fprintf(stderr, "trevrpc_h3_ingress: running case %zu/%zu: %s\n", i + 1, test_count, tests[i].name);
        fflush(stderr);
        int err = tests[i].run();
        if (err != 0) {
            return err;
        }
    }
    return 0;
}
