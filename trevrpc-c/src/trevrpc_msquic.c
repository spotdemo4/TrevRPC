#define _POSIX_C_SOURCE 200809L

#include "trevrpc_msquic.h"

#include "trevrpc_msquic_internal.h"

#include <arpa/inet.h>
#include <errno.h> // IWYU pragma: keep
#if __has_include(<msquic.h>)
#include <msquic.h>
#else
#include <inc/msquic.h>
#endif
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define TREV_MSQUIC_ERR_EOF 0
#define TREV_MSQUIC_NANOS_PER_SEC 1000000000ull
#define TREV_MSQUIC_SEND_POOL_LIMIT 64
#define TREV_MSQUIC_SEND_POOL_MAX_CAPACITY 65536
#define TREV_MSQUIC_SEND_MAX_BUFFERS 4
#define TREV_MSQUIC_TREVRPC_ALPN "trevrpc/1"
#define TREV_MSQUIC_DEFAULT_MAX_FRAME_SIZE (4u * 1024u * 1024u)

typedef struct trevrpc_msquic_send trevrpc_msquic_send;
typedef struct trevrpc_msquic_frame trevrpc_msquic_frame;

typedef enum trevrpc_msquic_recv_mode {
    TREV_MSQUIC_RECV_BYTES = 0,
    TREV_MSQUIC_RECV_FRAMES = 1,
} trevrpc_msquic_recv_mode;

typedef struct trevrpc_msquic_chunk {
    struct trevrpc_msquic_chunk* next;
    size_t len;
    size_t offset;
    uint8_t data[];
} trevrpc_msquic_chunk;

struct trevrpc_msquic_frame {
    trevrpc_msquic_frame* next;
    uint8_t* body;
    size_t len;
    intptr_t err;
};

typedef struct trevrpc_msquic_stream_node {
    struct trevrpc_msquic_stream_node* next;
    trevrpc_msquic_stream* stream;
} trevrpc_msquic_stream_node;

typedef struct trevrpc_msquic_conn_node {
    struct trevrpc_msquic_conn_node* next;
    trevrpc_msquic_conn* conn;
} trevrpc_msquic_conn_node;

struct trevrpc_msquic_stream {
    HQUIC handle;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_msquic_recv_mode recv_mode;
    trevrpc_msquic_chunk* recv_head;
    trevrpc_msquic_chunk* recv_tail;
    size_t recv_buffered;
    trevrpc_msquic_frame* frame_head;
    trevrpc_msquic_frame* frame_tail;
    size_t frame_max_len;
    uint8_t frame_header[4];
    size_t frame_header_len;
    size_t frame_body_len;
    size_t frame_body_offset;
    uint8_t* frame_body;
    size_t frame_skip_remaining;
    bool recv_fin;
    bool send_closed;
    bool send_aborted;
    bool api_closing;
    bool shutdown_complete;
    bool close_pending;
    bool closed;
    bool api_ref_acquired;
    size_t active_send_ops;
    size_t active_handle_ops;
    size_t active_send_completions;
    size_t send_capacity_waiters;
    size_t max_pending_send_bytes;
    size_t max_pending_send_count;
    size_t pending_send_bytes;
    size_t pending_send_count;
    int err;
    trevrpc_msquic_send* send_pool;
    size_t send_pool_count;
};

struct trevrpc_msquic_conn {
    HQUIC handle;
    HQUIC registration;
    HQUIC configuration;
    uint8_t negotiated_alpn[UINT8_MAX];
    uint8_t negotiated_alpn_len;
    size_t max_frame_size;
    size_t max_pending_send_bytes;
    size_t max_pending_send_count;
    bool owns_endpoint;
    bool api_ref_acquired;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_msquic_stream_node* stream_head;
    trevrpc_msquic_stream_node* stream_tail;
    bool connected;
    bool shutdown_complete;
    bool close_pending;
    bool closed;
    size_t active_handle_ops;
    trevrpc_msquic_conn_observer observer;
    void* observer_context;
    size_t active_observer_callbacks;
    uint64_t peer_close_error;
    bool peer_close_error_set;
    int err;
};

struct trevrpc_msquic_listener {
    HQUIC registration;
    HQUIC configuration;
    HQUIC listener;
    size_t max_frame_size;
    size_t max_pending_send_bytes;
    size_t max_pending_send_count;
    bool api_ref_acquired;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_msquic_conn_node* conn_head;
    trevrpc_msquic_conn_node* conn_tail;
    size_t active_callbacks;
    size_t shutdown_waiters;
    bool shutdown_in_progress;
    bool closed;
    int err;
};

struct trevrpc_msquic_send {
    trevrpc_msquic_send* next;
    QUIC_BUFFER buffers[TREV_MSQUIC_SEND_MAX_BUFFERS];
    QUIC_BUFFER* dynamic_buffers;
    trevrpc_msquic_send_completion* completion;
    uint32_t buffer_count;
    size_t capacity;
    size_t pending_len;
    bool poolable;
    bool pending_accounted;
    uint8_t data[];
};

struct trevrpc_msquic_send_completion {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool completed;
    int result;
};

static const QUIC_API_TABLE* TrevMsQuic;
static pthread_mutex_t TrevMsQuicApiMutex = PTHREAD_MUTEX_INITIALIZER;
static size_t TrevMsQuicApiRefCount;

#ifdef TREVRPC_MSQUIC_TESTING
static pthread_mutex_t TrevMsQuicTestStreamHookMutex = PTHREAD_MUTEX_INITIALIZER;
static trevrpc_msquic_test_stream_hook TrevMsQuicTestStreamHook;
static void* TrevMsQuicTestStreamHookContext;
static bool TrevMsQuicTestFailNextStreamSend;
static bool TrevMsQuicTestFailNextGracefulShutdown;
#endif

static QUIC_STATUS QUIC_API trevrpc_msquic_listener_callback(HQUIC listener, void* context, QUIC_LISTENER_EVENT* event);
static QUIC_STATUS QUIC_API trevrpc_msquic_conn_callback(HQUIC connection, void* context, QUIC_CONNECTION_EVENT* event);
static QUIC_STATUS QUIC_API trevrpc_msquic_stream_callback(HQUIC stream, void* context, QUIC_STREAM_EVENT* event);
static void trevrpc_msquic_stream_complete_close(trevrpc_msquic_stream* stream, HQUIC handle);
static void trevrpc_msquic_stream_send_op_release(trevrpc_msquic_stream* stream);

#ifdef TREVRPC_MSQUIC_TESTING
void trevrpc_msquic_test_set_stream_hook(trevrpc_msquic_test_stream_hook hook, void* context) {
    pthread_mutex_lock(&TrevMsQuicTestStreamHookMutex);
    TrevMsQuicTestStreamHook = hook;
    TrevMsQuicTestStreamHookContext = context;
    if (hook == NULL) {
        TrevMsQuicTestFailNextStreamSend = false;
        TrevMsQuicTestFailNextGracefulShutdown = false;
    }
    pthread_mutex_unlock(&TrevMsQuicTestStreamHookMutex);
}

void trevrpc_msquic_test_fail_next_stream_send(void) {
    pthread_mutex_lock(&TrevMsQuicTestStreamHookMutex);
    TrevMsQuicTestFailNextStreamSend = true;
    pthread_mutex_unlock(&TrevMsQuicTestStreamHookMutex);
}

void trevrpc_msquic_test_fail_next_graceful_shutdown(void) {
    pthread_mutex_lock(&TrevMsQuicTestStreamHookMutex);
    TrevMsQuicTestFailNextGracefulShutdown = true;
    pthread_mutex_unlock(&TrevMsQuicTestStreamHookMutex);
}

void trevrpc_msquic_test_wait_stream_shutdown_complete(trevrpc_msquic_stream* stream) {
    pthread_mutex_lock(&stream->mutex);
    while (!stream->shutdown_complete) {
        pthread_cond_wait(&stream->cond, &stream->mutex);
    }
    pthread_mutex_unlock(&stream->mutex);
}

static void trevrpc_msquic_test_emit_stream_event(trevrpc_msquic_test_stream_event event) {
    pthread_mutex_lock(&TrevMsQuicTestStreamHookMutex);
    trevrpc_msquic_test_stream_hook hook = TrevMsQuicTestStreamHook;
    void* context = TrevMsQuicTestStreamHookContext;
    pthread_mutex_unlock(&TrevMsQuicTestStreamHookMutex);
    if (hook != NULL) {
        hook(event, context);
    }
}

static QUIC_STATUS trevrpc_msquic_test_stream_send(
    HQUIC stream, const QUIC_BUFFER* buffers, uint32_t buffer_count, QUIC_SEND_FLAGS flags, void* client_context) {
    pthread_mutex_lock(&TrevMsQuicTestStreamHookMutex);
    bool fail = TrevMsQuicTestFailNextStreamSend;
    TrevMsQuicTestFailNextStreamSend = false;
    pthread_mutex_unlock(&TrevMsQuicTestStreamHookMutex);
    return fail ? (QUIC_STATUS)EIO : TrevMsQuic->StreamSend(stream, buffers, buffer_count, flags, client_context);
}

static QUIC_STATUS trevrpc_msquic_test_stream_shutdown(
    HQUIC stream, QUIC_STREAM_SHUTDOWN_FLAGS flags, QUIC_UINT62 error_code) {
    bool fail = false;
    if (flags == QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL) {
        pthread_mutex_lock(&TrevMsQuicTestStreamHookMutex);
        fail = TrevMsQuicTestFailNextGracefulShutdown;
        TrevMsQuicTestFailNextGracefulShutdown = false;
        pthread_mutex_unlock(&TrevMsQuicTestStreamHookMutex);
    }
    return fail ? (QUIC_STATUS)EIO : TrevMsQuic->StreamShutdown(stream, flags, error_code);
}
#else
#define trevrpc_msquic_test_emit_stream_event(event) ((void)0)
#define trevrpc_msquic_test_stream_send(stream, buffers, buffer_count, flags, client_context)                          \
    TrevMsQuic->StreamSend(stream, buffers, buffer_count, flags, client_context)
#define trevrpc_msquic_test_stream_shutdown(stream, flags, error_code)                                                 \
    TrevMsQuic->StreamShutdown(stream, flags, error_code)
#endif

static int trevrpc_msquic_api_acquire(void) {
    pthread_mutex_lock(&TrevMsQuicApiMutex);
    if (TrevMsQuic == NULL) {
        QUIC_STATUS status = MsQuicOpen2(&TrevMsQuic);
        if (QUIC_FAILED(status)) {
            pthread_mutex_unlock(&TrevMsQuicApiMutex);
            return (int)status;
        }
    }
    if (TrevMsQuicApiRefCount == SIZE_MAX) {
        pthread_mutex_unlock(&TrevMsQuicApiMutex);
        return ENOMEM;
    }
    TrevMsQuicApiRefCount++;
    pthread_mutex_unlock(&TrevMsQuicApiMutex);
    return 0;
}

static bool trevrpc_msquic_api_retain_open(void) {
    pthread_mutex_lock(&TrevMsQuicApiMutex);
    bool retained = TrevMsQuic != NULL && TrevMsQuicApiRefCount < SIZE_MAX;
    if (retained) {
        TrevMsQuicApiRefCount++;
    }
    pthread_mutex_unlock(&TrevMsQuicApiMutex);
    return retained;
}

static void trevrpc_msquic_api_release(void) {
    pthread_mutex_lock(&TrevMsQuicApiMutex);
    if (TrevMsQuicApiRefCount > 0) {
        TrevMsQuicApiRefCount--;
    }
    pthread_mutex_unlock(&TrevMsQuicApiMutex);
}

static void trevrpc_msquic_listener_callback_finish(trevrpc_msquic_listener* listener) {
    pthread_mutex_lock(&listener->mutex);
    if (listener->active_callbacks > 0) {
        listener->active_callbacks--;
    }
    pthread_cond_broadcast(&listener->cond);
    pthread_mutex_unlock(&listener->mutex);
}

static void trevrpc_msquic_conn_notify(trevrpc_msquic_conn* conn, const trevrpc_msquic_conn_event* event) {
    trevrpc_msquic_conn_observer observer = NULL;
    void* observer_context = NULL;
    pthread_mutex_lock(&conn->mutex);
    observer = conn->observer;
    observer_context = conn->observer_context;
    if (observer != NULL) {
        conn->active_observer_callbacks++;
    }
    pthread_mutex_unlock(&conn->mutex);

    if (observer == NULL) {
        return;
    }
    observer(observer_context, event);

    pthread_mutex_lock(&conn->mutex);
    conn->active_observer_callbacks--;
    pthread_cond_broadcast(&conn->cond);
    pthread_mutex_unlock(&conn->mutex);
}

static size_t trevrpc_msquic_effective_max_pending_send_bytes(size_t configured) {
    return configured == 0 ? TREV_MSQUIC_DEFAULT_MAX_PENDING_SEND_BYTES : configured;
}

static size_t trevrpc_msquic_effective_max_pending_send_count(size_t configured) {
    return configured == 0 ? TREV_MSQUIC_DEFAULT_MAX_PENDING_SEND_COUNT : configured;
}

static size_t trevrpc_msquic_effective_max_frame_size(size_t configured) {
    return configured == 0 ? TREV_MSQUIC_DEFAULT_MAX_FRAME_SIZE : configured;
}

static bool trevrpc_msquic_alpn_equals(const uint8_t* alpn, size_t alpn_len, const char* expected) {
    size_t expected_len = strlen(expected);
    return alpn_len == expected_len && memcmp(alpn, expected, expected_len) == 0;
}

static bool trevrpc_msquic_conn_uses_native_frames(const trevrpc_msquic_conn* conn) {
    return conn != NULL &&
           trevrpc_msquic_alpn_equals(conn->negotiated_alpn, conn->negotiated_alpn_len, TREV_MSQUIC_TREVRPC_ALPN);
}

static bool trevrpc_msquic_stream_send_ops_idle(const trevrpc_msquic_stream* stream) {
    return stream->active_send_ops == 0;
}

static HQUIC trevrpc_msquic_stream_close_handle_if_ready_locked(trevrpc_msquic_stream* stream) {
    if (trevrpc_msquic_stream_send_ops_idle(stream) && stream->active_handle_ops == 0 &&
        stream->pending_send_count == 0 && stream->send_capacity_waiters == 0 && stream->close_pending &&
        stream->handle != NULL) {
        HQUIC handle = stream->handle;
        stream->handle = NULL;
        return handle;
    }
    return NULL;
}

static int trevrpc_msquic_stream_send_op_acquire(trevrpc_msquic_stream* stream) {
    if (stream == NULL) {
        return -EINVAL;
    }

    int result = 0;
    pthread_mutex_lock(&stream->mutex);
    if (stream->api_closing) {
        result = -ECANCELED;
    } else if (stream->active_send_ops == SIZE_MAX) {
        result = -EOVERFLOW;
    } else {
        stream->active_send_ops++;
    }
    pthread_mutex_unlock(&stream->mutex);
    if (result == 0) {
        trevrpc_msquic_test_emit_stream_event(TREV_MSQUIC_TEST_STREAM_SEND_PREPARE);
    }
    return result;
}

static void trevrpc_msquic_stream_send_op_release(trevrpc_msquic_stream* stream) {
    HQUIC close_handle = NULL;
    pthread_mutex_lock(&stream->mutex);
    if (stream->active_send_ops > 0) {
        stream->active_send_ops--;
    }
    if (stream->active_send_ops == 0) {
        close_handle = trevrpc_msquic_stream_close_handle_if_ready_locked(stream);
    }
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);
    if (close_handle != NULL) {
        trevrpc_msquic_stream_complete_close(stream, close_handle);
    }
}

static void trevrpc_msquic_stream_complete_close(trevrpc_msquic_stream* stream, HQUIC handle) {
    TrevMsQuic->StreamClose(handle);

    pthread_mutex_lock(&stream->mutex);
    stream->shutdown_complete = true;
    stream->close_pending = false;
    stream->closed = true;
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);
}

static HQUIC trevrpc_msquic_stream_handle_acquire(trevrpc_msquic_stream* stream) {
    pthread_mutex_lock(&stream->mutex);
    HQUIC handle = stream->handle;
    if (handle != NULL && !stream->close_pending) {
        stream->active_handle_ops++;
    } else {
        handle = NULL;
    }
    pthread_mutex_unlock(&stream->mutex);
    return handle;
}

static void trevrpc_msquic_stream_handle_release(trevrpc_msquic_stream* stream) {
    HQUIC close_handle = NULL;
    pthread_mutex_lock(&stream->mutex);
    if (stream->active_handle_ops > 0) {
        stream->active_handle_ops--;
    }
    close_handle = trevrpc_msquic_stream_close_handle_if_ready_locked(stream);
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);

    if (close_handle != NULL) {
        trevrpc_msquic_stream_complete_close(stream, close_handle);
    }
}

static void trevrpc_msquic_stream_shutdown_complete(trevrpc_msquic_stream* stream, HQUIC stream_handle) {
    (void)stream_handle;
    HQUIC close_handle = NULL;
    pthread_mutex_lock(&stream->mutex);
    stream->closed = true;
    stream->close_pending = true;
    close_handle = trevrpc_msquic_stream_close_handle_if_ready_locked(stream);
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);

    if (close_handle != NULL) {
        trevrpc_msquic_stream_complete_close(stream, close_handle);
    }
}

static void trevrpc_msquic_conn_complete_close(trevrpc_msquic_conn* conn, HQUIC handle) {
    TrevMsQuic->ConnectionClose(handle);

    pthread_mutex_lock(&conn->mutex);
    conn->shutdown_complete = true;
    conn->close_pending = false;
    conn->closed = true;
    pthread_cond_broadcast(&conn->cond);
    pthread_mutex_unlock(&conn->mutex);
}

static HQUIC trevrpc_msquic_conn_handle_acquire(trevrpc_msquic_conn* conn) {
    pthread_mutex_lock(&conn->mutex);
    HQUIC handle = conn->handle;
    if (handle != NULL && !conn->close_pending) {
        conn->active_handle_ops++;
    } else {
        handle = NULL;
    }
    pthread_mutex_unlock(&conn->mutex);
    return handle;
}

static void trevrpc_msquic_conn_handle_release(trevrpc_msquic_conn* conn) {
    HQUIC close_handle = NULL;
    pthread_mutex_lock(&conn->mutex);
    if (conn->active_handle_ops > 0) {
        conn->active_handle_ops--;
    }
    if (conn->active_handle_ops == 0 && conn->close_pending && conn->handle != NULL) {
        close_handle = conn->handle;
        conn->handle = NULL;
    }
    pthread_mutex_unlock(&conn->mutex);

    if (close_handle != NULL) {
        trevrpc_msquic_conn_complete_close(conn, close_handle);
    }
}

static void trevrpc_msquic_conn_shutdown_complete(trevrpc_msquic_conn* conn, HQUIC connection_handle) {
    bool close_now = false;
    pthread_mutex_lock(&conn->mutex);
    conn->closed = true;
    if (conn->active_handle_ops == 0) {
        if (conn->handle == connection_handle) {
            conn->handle = NULL;
        }
        conn->close_pending = true;
        close_now = true;
    } else {
        conn->close_pending = true;
    }
    pthread_cond_broadcast(&conn->cond);
    pthread_mutex_unlock(&conn->mutex);

    if (close_now) {
        trevrpc_msquic_conn_complete_close(conn, connection_handle);
    }
}

static trevrpc_msquic_stream* trevrpc_msquic_stream_alloc(
    HQUIC handle, size_t max_pending_send_bytes, size_t max_pending_send_count, size_t initial_frame_max_len) {
    trevrpc_msquic_stream* stream = calloc(1, sizeof(*stream));
    if (stream == NULL) {
        return NULL;
    }
    if (!trevrpc_msquic_api_retain_open()) {
        free(stream);
        return NULL;
    }
    stream->api_ref_acquired = true;

    stream->handle = handle;
    stream->max_pending_send_bytes = trevrpc_msquic_effective_max_pending_send_bytes(max_pending_send_bytes);
    stream->max_pending_send_count = trevrpc_msquic_effective_max_pending_send_count(max_pending_send_count);
    if (initial_frame_max_len > 0) {
        stream->recv_mode = TREV_MSQUIC_RECV_FRAMES;
        stream->frame_max_len = initial_frame_max_len;
    }
    pthread_mutex_init(&stream->mutex, NULL);
    pthread_cond_init(&stream->cond, NULL);
    return stream;
}

static trevrpc_msquic_conn* trevrpc_msquic_conn_alloc(HQUIC handle) {
    trevrpc_msquic_conn* conn = calloc(1, sizeof(*conn));
    if (conn == NULL) {
        return NULL;
    }
    if (!trevrpc_msquic_api_retain_open()) {
        free(conn);
        return NULL;
    }
    conn->api_ref_acquired = true;

    conn->handle = handle;
    conn->max_frame_size = TREV_MSQUIC_DEFAULT_MAX_FRAME_SIZE;
    conn->max_pending_send_bytes = trevrpc_msquic_effective_max_pending_send_bytes(0);
    conn->max_pending_send_count = trevrpc_msquic_effective_max_pending_send_count(0);
    pthread_mutex_init(&conn->mutex, NULL);
    pthread_cond_init(&conn->cond, NULL);
    return conn;
}

static void trevrpc_msquic_free_chunks(trevrpc_msquic_stream* stream) {
    trevrpc_msquic_chunk* chunk = stream->recv_head;
    while (chunk != NULL) {
        trevrpc_msquic_chunk* next = chunk->next;
        free(chunk);
        chunk = next;
    }
    stream->recv_head = NULL;
    stream->recv_tail = NULL;
    stream->recv_buffered = 0;
}

static void trevrpc_msquic_free_frames(trevrpc_msquic_stream* stream) {
    trevrpc_msquic_frame* frame = stream->frame_head;
    while (frame != NULL) {
        trevrpc_msquic_frame* next = frame->next;
        free(frame->body);
        free(frame);
        frame = next;
    }
    stream->frame_head = NULL;
    stream->frame_tail = NULL;
    free(stream->frame_body);
    stream->frame_body = NULL;
    stream->frame_header_len = 0;
    stream->frame_body_len = 0;
    stream->frame_body_offset = 0;
    stream->frame_skip_remaining = 0;
}

static void trevrpc_msquic_free_send_pool(trevrpc_msquic_stream* stream) {
    trevrpc_msquic_send* send = stream->send_pool;
    while (send != NULL) {
        trevrpc_msquic_send* next = send->next;
        free(send);
        send = next;
    }
    stream->send_pool = NULL;
    stream->send_pool_count = 0;
}

static trevrpc_msquic_send* trevrpc_msquic_send_alloc(size_t len, bool poolable) {
    if (len > UINT32_MAX) {
        return NULL;
    }

    trevrpc_msquic_send* send = malloc(sizeof(*send) + len);
    if (send == NULL) {
        return NULL;
    }
    send->next = NULL;
    send->dynamic_buffers = NULL;
    send->completion = NULL;
    send->buffer_count = 0;
    send->capacity = len;
    send->pending_len = 0;
    send->poolable = poolable;
    send->pending_accounted = false;
    return send;
}

static trevrpc_msquic_send* trevrpc_msquic_send_acquire(trevrpc_msquic_stream* stream, size_t len) {
    if (len > UINT32_MAX) {
        return NULL;
    }

    pthread_mutex_lock(&stream->mutex);
    trevrpc_msquic_send** link = &stream->send_pool;
    while (*link != NULL) {
        trevrpc_msquic_send* send = *link;
        if (send->capacity >= len) {
            *link = send->next;
            stream->send_pool_count--;
            pthread_mutex_unlock(&stream->mutex);
            send->next = NULL;
            send->dynamic_buffers = NULL;
            send->completion = NULL;
            send->poolable = true;
            send->buffer_count = 0;
            send->pending_len = 0;
            send->pending_accounted = false;
            return send;
        }
        link = &send->next;
    }
    pthread_mutex_unlock(&stream->mutex);

    return trevrpc_msquic_send_alloc(len, true);
}

static QUIC_BUFFER* trevrpc_msquic_send_buffers(trevrpc_msquic_send* send) {
    return send->dynamic_buffers != NULL ? send->dynamic_buffers : send->buffers;
}

static int trevrpc_msquic_send_prepare_buffers(trevrpc_msquic_send* send, uint32_t buffer_count) {
    if (buffer_count <= TREV_MSQUIC_SEND_MAX_BUFFERS) {
        return 0;
    }
    send->dynamic_buffers = calloc(buffer_count, sizeof(*send->dynamic_buffers));
    return send->dynamic_buffers == NULL ? -ENOMEM : 0;
}

static trevrpc_msquic_send_completion* trevrpc_msquic_send_completion_new(void) {
    trevrpc_msquic_send_completion* completion = calloc(1, sizeof(*completion));
    if (completion == NULL) {
        return NULL;
    }
    if (pthread_mutex_init(&completion->mutex, NULL) != 0) {
        free(completion);
        return NULL;
    }
    if (pthread_cond_init(&completion->cond, NULL) != 0) {
        pthread_mutex_destroy(&completion->mutex);
        free(completion);
        return NULL;
    }
    return completion;
}

static void trevrpc_msquic_send_completion_signal(trevrpc_msquic_send_completion* completion, bool canceled) {
    if (completion == NULL) {
        return;
    }
    pthread_mutex_lock(&completion->mutex);
    completion->result = canceled ? -ECANCELED : 0;
    completion->completed = true;
    pthread_cond_broadcast(&completion->cond);
    pthread_mutex_unlock(&completion->mutex);
}

static int trevrpc_msquic_stream_pending_send_reserve_locked(
    trevrpc_msquic_stream* stream, trevrpc_msquic_send* send, size_t len) {
    if (stream->pending_send_count >= stream->max_pending_send_count ||
        stream->pending_send_bytes > stream->max_pending_send_bytes ||
        len > stream->max_pending_send_bytes - stream->pending_send_bytes) {
        return TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED;
    }

    stream->pending_send_count++;
    stream->pending_send_bytes += len;
    send->pending_len = len;
    send->pending_accounted = true;
    return 0;
}

static bool trevrpc_msquic_stream_pending_send_has_capacity_locked(const trevrpc_msquic_stream* stream, size_t len) {
    return stream->pending_send_count < stream->max_pending_send_count &&
           stream->pending_send_bytes <= stream->max_pending_send_bytes &&
           len <= stream->max_pending_send_bytes - stream->pending_send_bytes;
}

static HQUIC trevrpc_msquic_stream_pending_send_complete_locked(
    trevrpc_msquic_stream* stream, trevrpc_msquic_send* send) {
    if (send->pending_accounted) {
        if (stream->pending_send_count > 0) {
            stream->pending_send_count--;
        }
        if (stream->pending_send_bytes >= send->pending_len) {
            stream->pending_send_bytes -= send->pending_len;
        } else {
            stream->pending_send_bytes = 0;
        }
        send->pending_len = 0;
        send->pending_accounted = false;
    }
    return trevrpc_msquic_stream_close_handle_if_ready_locked(stream);
}

static void trevrpc_msquic_send_release(trevrpc_msquic_stream* stream, trevrpc_msquic_send* send) {
    free(send->dynamic_buffers);
    send->dynamic_buffers = NULL;
    send->completion = NULL;
    send->buffer_count = 0;
    send->pending_len = 0;
    send->pending_accounted = false;
    if (!send->poolable) {
        free(send);
        return;
    }

    pthread_mutex_lock(&stream->mutex);
    bool keep = !stream->closed && send->capacity <= TREV_MSQUIC_SEND_POOL_MAX_CAPACITY &&
                stream->send_pool_count < TREV_MSQUIC_SEND_POOL_LIMIT;
    if (keep) {
        send->next = stream->send_pool;
        stream->send_pool = send;
        stream->send_pool_count++;
    }
    pthread_mutex_unlock(&stream->mutex);

    if (!keep) {
        free(send);
    }
}

static void trevrpc_msquic_stream_send_complete_begin(trevrpc_msquic_stream* stream) {
    pthread_mutex_lock(&stream->mutex);
    stream->active_send_completions++;
    pthread_mutex_unlock(&stream->mutex);
}

static void trevrpc_msquic_stream_send_complete_end(trevrpc_msquic_stream* stream) {
    pthread_mutex_lock(&stream->mutex);
    if (stream->active_send_completions > 0) {
        stream->active_send_completions--;
    }
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);
}

static void trevrpc_msquic_stream_send_complete(
    trevrpc_msquic_stream* stream, trevrpc_msquic_send* send, bool canceled) {
    HQUIC close_handle = NULL;
    trevrpc_msquic_stream_send_complete_begin(stream);

    pthread_mutex_lock(&stream->mutex);
    close_handle = trevrpc_msquic_stream_pending_send_complete_locked(stream, send);
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);

    trevrpc_msquic_send_completion_signal(send->completion, canceled);
    trevrpc_msquic_send_release(stream, send);
    if (close_handle != NULL) {
        trevrpc_msquic_stream_complete_close(stream, close_handle);
    }

    trevrpc_msquic_stream_send_complete_end(stream);
}

static intptr_t trevrpc_msquic_error_result(int err) {
    if (err == 0) {
        return TREV_MSQUIC_ERR_CLOSED;
    }
    if (err < 0) {
        return err;
    }

    return -(intptr_t)err;
}

static int trevrpc_msquic_realtime_deadline(uint64_t timeout_nanos, struct timespec* out_deadline) {
    if (clock_gettime(CLOCK_REALTIME, out_deadline) != 0) {
        return -errno;
    }

    uint64_t seconds = timeout_nanos / TREV_MSQUIC_NANOS_PER_SEC;
    uint64_t nanos = timeout_nanos % TREV_MSQUIC_NANOS_PER_SEC;
    if (seconds > (uint64_t)(INT64_MAX - out_deadline->tv_sec)) {
        return -EOVERFLOW;
    }

    out_deadline->tv_sec += (time_t)seconds;
    out_deadline->tv_nsec += (long)nanos;
    if (out_deadline->tv_nsec >= (long)TREV_MSQUIC_NANOS_PER_SEC) {
        out_deadline->tv_sec++;
        out_deadline->tv_nsec -= (long)TREV_MSQUIC_NANOS_PER_SEC;
    }
    return 0;
}

static int trevrpc_msquic_stream_wait_recv_locked(trevrpc_msquic_stream* stream, const struct timespec* deadline) {
    while (stream->recv_head == NULL && !stream->recv_fin && !stream->closed && stream->err == 0) {
        int err = deadline == NULL ? pthread_cond_wait(&stream->cond, &stream->mutex)
                                   : pthread_cond_timedwait(&stream->cond, &stream->mutex, deadline);
        if (err == ETIMEDOUT) {
            return TREV_MSQUIC_ERR_TIMEOUT;
        }
        if (err != 0) {
            return -err;
        }
    }

    return 0;
}

static int trevrpc_msquic_frame_enqueue_locked(trevrpc_msquic_stream* stream, uint8_t* body, size_t len, intptr_t err) {
    trevrpc_msquic_frame* frame = malloc(sizeof(*frame));
    if (frame == NULL) {
        free(body);
        return -ENOMEM;
    }
    frame->next = NULL;
    frame->body = body;
    frame->len = len;
    frame->err = err;

    if (stream->frame_tail != NULL) {
        stream->frame_tail->next = frame;
    } else {
        stream->frame_head = frame;
    }
    stream->frame_tail = frame;
    return 0;
}

static void trevrpc_msquic_frame_parser_reset_body(trevrpc_msquic_stream* stream) {
    stream->frame_header_len = 0;
    stream->frame_body_len = 0;
    stream->frame_body_offset = 0;
    stream->frame_body = NULL;
}

static bool trevrpc_msquic_frame_parser_active(const trevrpc_msquic_stream* stream) {
    return stream->frame_header_len > 0 || stream->frame_body != NULL || stream->frame_skip_remaining > 0;
}

static size_t trevrpc_msquic_frame_header_len(const uint8_t header[4]) {
    return ((size_t)header[0] << 24) | ((size_t)header[1] << 16) | ((size_t)header[2] << 8) | (size_t)header[3];
}

static int trevrpc_msquic_frame_parser_start_body_locked(trevrpc_msquic_stream* stream) {
    size_t body_len = trevrpc_msquic_frame_header_len(stream->frame_header);
    stream->frame_body_len = body_len;
    stream->frame_body_offset = 0;

    if (body_len > stream->frame_max_len) {
        int err = trevrpc_msquic_frame_enqueue_locked(stream, NULL, body_len, TREV_MSQUIC_ERR_FRAME_TOO_LARGE);
        if (err != 0) {
            return err;
        }
        stream->frame_skip_remaining = body_len;
        stream->frame_header_len = 0;
        stream->frame_body_len = 0;
        return 0;
    }

    if (body_len == 0) {
        stream->frame_header_len = 0;
        stream->frame_body_len = 0;
        return trevrpc_msquic_frame_enqueue_locked(stream, NULL, 0, 0);
    }

    stream->frame_body = malloc(body_len);
    if (stream->frame_body == NULL) {
        return -ENOMEM;
    }
    return 0;
}

static int trevrpc_msquic_stream_append_frame_bytes_locked(
    trevrpc_msquic_stream* stream, const uint8_t* data, size_t len) {
    size_t offset = 0;
    while (offset < len) {
        if (stream->frame_skip_remaining > 0) {
            size_t remaining = len - offset;
            size_t skipped = remaining < stream->frame_skip_remaining ? remaining : stream->frame_skip_remaining;
            stream->frame_skip_remaining -= skipped;
            offset += skipped;
            continue;
        }

        if (stream->frame_header_len < sizeof(stream->frame_header)) {
            size_t remaining = len - offset;
            size_t header_remaining = sizeof(stream->frame_header) - stream->frame_header_len;
            size_t copied = remaining < header_remaining ? remaining : header_remaining;
            memcpy(stream->frame_header + stream->frame_header_len, data + offset, copied);
            stream->frame_header_len += copied;
            offset += copied;
            if (stream->frame_header_len < sizeof(stream->frame_header)) {
                continue;
            }
            int err = trevrpc_msquic_frame_parser_start_body_locked(stream);
            if (err != 0) {
                return err;
            }
            continue;
        }

        size_t remaining = len - offset;
        size_t body_remaining = stream->frame_body_len - stream->frame_body_offset;
        size_t copied = remaining < body_remaining ? remaining : body_remaining;
        memcpy(stream->frame_body + stream->frame_body_offset, data + offset, copied);
        stream->frame_body_offset += copied;
        offset += copied;

        if (stream->frame_body_offset == stream->frame_body_len) {
            uint8_t* body = stream->frame_body;
            size_t body_len = stream->frame_body_len;
            trevrpc_msquic_frame_parser_reset_body(stream);
            int err = trevrpc_msquic_frame_enqueue_locked(stream, body, body_len, 0);
            if (err != 0) {
                return err;
            }
        }
    }

    return 0;
}

static int trevrpc_msquic_stream_enable_frame_mode_locked(trevrpc_msquic_stream* stream, size_t max_len) {
    if (stream->recv_mode == TREV_MSQUIC_RECV_FRAMES) {
        stream->frame_max_len = max_len;
        return 0;
    }

    stream->recv_mode = TREV_MSQUIC_RECV_FRAMES;
    stream->frame_max_len = max_len;
    while (stream->recv_head != NULL) {
        trevrpc_msquic_chunk* chunk = stream->recv_head;
        stream->recv_head = chunk->next;
        if (stream->recv_head == NULL) {
            stream->recv_tail = NULL;
        }
        size_t available = chunk->len - chunk->offset;
        stream->recv_buffered -= available;
        int err = trevrpc_msquic_stream_append_frame_bytes_locked(stream, chunk->data + chunk->offset, available);
        free(chunk);
        if (err != 0) {
            stream->err = -err;
            return err;
        }
    }
    stream->recv_buffered = 0;
    return 0;
}

static intptr_t trevrpc_msquic_stream_wait_frame_locked(
    trevrpc_msquic_stream* stream, const struct timespec* deadline) {
    while (stream->frame_head == NULL && !stream->recv_fin && !stream->closed && stream->err == 0) {
        int err = deadline == NULL ? pthread_cond_wait(&stream->cond, &stream->mutex)
                                   : pthread_cond_timedwait(&stream->cond, &stream->mutex, deadline);
        if (err == ETIMEDOUT) {
            return TREV_MSQUIC_ERR_TIMEOUT;
        }
        if (err != 0) {
            return -err;
        }
    }

    if (stream->frame_head != NULL) {
        return 0;
    }
    if (stream->err != 0) {
        return trevrpc_msquic_error_result(stream->err);
    }
    return trevrpc_msquic_frame_parser_active(stream) ? TREV_MSQUIC_ERR_CLOSED : TREV_MSQUIC_ERR_EOF;
}

static intptr_t trevrpc_msquic_stream_frame_not_ready_locked(trevrpc_msquic_stream* stream) {
    if (stream->frame_head != NULL) {
        return 0;
    }
    if (stream->err != 0) {
        return trevrpc_msquic_error_result(stream->err);
    }
    if (stream->recv_fin || stream->closed) {
        return trevrpc_msquic_frame_parser_active(stream) ? TREV_MSQUIC_ERR_CLOSED : TREV_MSQUIC_ERR_EOF;
    }
    return TREV_MSQUIC_ERR_TIMEOUT;
}

static trevrpc_msquic_frame* trevrpc_msquic_stream_pop_frame_locked(trevrpc_msquic_stream* stream) {
    trevrpc_msquic_frame* frame = stream->frame_head;
    if (frame != NULL) {
        stream->frame_head = frame->next;
        if (stream->frame_head == NULL) {
            stream->frame_tail = NULL;
        }
        frame->next = NULL;
    }
    return frame;
}

static intptr_t trevrpc_msquic_stream_send_buffers_with_flags(trevrpc_msquic_stream* stream,
    trevrpc_msquic_send* send,
    uint32_t buffer_count,
    size_t len,
    QUIC_SEND_FLAGS flags,
    trevrpc_msquic_send_completion** out_completion,
    bool wait_for_capacity) {
    if (out_completion != NULL) {
        *out_completion = NULL;
        send->completion = trevrpc_msquic_send_completion_new();
        if (send->completion == NULL) {
            trevrpc_msquic_send_release(stream, send);
            return -ENOMEM;
        }
    }
    trevrpc_msquic_send_completion* completion = send->completion;
    bool finish_send = (flags & QUIC_SEND_FLAG_FIN) != 0;
    int reserve_err = 0;
    bool waited_for_capacity = false;
    bool cleanup_ref = false;
    pthread_mutex_lock(&stream->mutex);
    if (wait_for_capacity && stream->handle != NULL && !stream->send_closed && !stream->send_aborted &&
        !stream->api_closing && !stream->close_pending && !stream->closed && len > stream->max_pending_send_bytes) {
        reserve_err = TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED;
    }
    if (wait_for_capacity && reserve_err == 0 && stream->handle != NULL && !stream->send_closed &&
        !stream->send_aborted && !stream->api_closing && !stream->close_pending && !stream->closed &&
        !trevrpc_msquic_stream_pending_send_has_capacity_locked(stream, len)) {
        stream->send_capacity_waiters++;
        waited_for_capacity = true;
        trevrpc_msquic_test_emit_stream_event(TREV_MSQUIC_TEST_STREAM_SEND_CAPACITY_WAIT);
        while (stream->handle != NULL && !stream->send_closed && !stream->send_aborted && !stream->api_closing &&
               !stream->close_pending && !stream->closed &&
               !trevrpc_msquic_stream_pending_send_has_capacity_locked(stream, len)) {
            int wait_err = pthread_cond_wait(&stream->cond, &stream->mutex);
            if (wait_err != 0) {
                reserve_err = -wait_err;
                break;
            }
        }
    }
    HQUIC handle = stream->handle;
    bool send_closed = stream->send_closed;
    bool send_cancelled = stream->send_aborted || stream->api_closing;
    bool stream_closed = stream->closed;
    if (reserve_err == 0 && handle != NULL && !send_closed && !send_cancelled && !stream->close_pending &&
        !stream_closed) {
        reserve_err = trevrpc_msquic_stream_pending_send_reserve_locked(stream, send, len);
        if (reserve_err == 0) {
            if (waited_for_capacity) {
                stream->send_capacity_waiters--;
            }
            if (finish_send) {
                stream->send_closed = true;
            }
            stream->active_handle_ops++;
            pthread_cond_broadcast(&stream->cond);
        } else {
            handle = NULL;
        }
    } else {
        handle = NULL;
    }
    if (handle == NULL && waited_for_capacity) {
        stream->send_capacity_waiters--;
        stream->active_handle_ops++;
        cleanup_ref = true;
        pthread_cond_broadcast(&stream->cond);
    }
    pthread_mutex_unlock(&stream->mutex);
    if (handle != NULL) {
        trevrpc_msquic_test_emit_stream_event(TREV_MSQUIC_TEST_STREAM_SEND_RESERVED);
        if (finish_send) {
            trevrpc_msquic_test_emit_stream_event(TREV_MSQUIC_TEST_STREAM_SEND_TERMINAL);
        }
    }
    if (handle == NULL || send_closed || send_cancelled || stream_closed || reserve_err != 0) {
        trevrpc_msquic_send_completion_free(send->completion);
        send->completion = NULL;
        trevrpc_msquic_send_release(stream, send);
        if (cleanup_ref) {
            trevrpc_msquic_stream_handle_release(stream);
        }
        if (reserve_err != 0) {
            return reserve_err;
        }
        return send_cancelled ? -ECANCELED : TREV_MSQUIC_ERR_CLOSED;
    }

    send->buffer_count = buffer_count;
    QUIC_STATUS status =
        trevrpc_msquic_test_stream_send(handle, trevrpc_msquic_send_buffers(send), buffer_count, flags, send);
    trevrpc_msquic_stream_handle_release(stream);
    if (QUIC_FAILED(status)) {
        HQUIC close_handle = NULL;
        pthread_mutex_lock(&stream->mutex);
        if (finish_send) {
            stream->send_closed = false;
        }
        close_handle = trevrpc_msquic_stream_pending_send_complete_locked(stream, send);
        pthread_cond_broadcast(&stream->cond);
        pthread_mutex_unlock(&stream->mutex);
        trevrpc_msquic_send_completion_free(send->completion);
        send->completion = NULL;
        trevrpc_msquic_send_release(stream, send);
        if (close_handle != NULL) {
            trevrpc_msquic_stream_complete_close(stream, close_handle);
        }
        return trevrpc_msquic_error_result((int)status);
    }

    if (out_completion != NULL) {
        *out_completion = completion;
    }
    return (intptr_t)len;
}

static intptr_t trevrpc_msquic_stream_send_buffer_with_flags(trevrpc_msquic_stream* stream,
    trevrpc_msquic_send* send,
    size_t len,
    QUIC_SEND_FLAGS flags,
    bool wait_for_capacity) {
    send->buffers[0].Buffer = send->data;
    send->buffers[0].Length = (uint32_t)len;
    return trevrpc_msquic_stream_send_buffers_with_flags(stream, send, 1, len, flags, NULL, wait_for_capacity);
}

static intptr_t trevrpc_msquic_stream_send_buffer(
    trevrpc_msquic_stream* stream, trevrpc_msquic_send* send, size_t len) {
    return trevrpc_msquic_stream_send_buffer_with_flags(stream, send, len, QUIC_SEND_FLAG_NONE, false);
}

static size_t trevrpc_msquic_varint_len(size_t value) {
    size_t len = 1;
    while (value >= 0x80) {
        len++;
        value >>= 7;
    }
    return len;
}

static uint8_t* trevrpc_msquic_append_varint(uint8_t* out, size_t value) {
    while (value >= 0x80) {
        *out++ = (uint8_t)value | 0x80;
        value >>= 7;
    }
    *out++ = (uint8_t)value;
    return out;
}

static int trevrpc_msquic_build_alpn_buffers(const trevrpc_msquic_config* config,
    const trevrpc_msquic_alpn* alpns,
    size_t alpns_len,
    QUIC_BUFFER** out_buffers,
    uint32_t* out_count) {
    *out_buffers = NULL;
    *out_count = 0;

    size_t count = alpns_len;
    if (alpns == NULL || alpns_len == 0) {
        if (config == NULL || config->alpn == NULL || config->alpn_len == 0) {
            return EINVAL;
        }
        count = 1;
    }
    if (count > UINT32_MAX) {
        return EINVAL;
    }

    QUIC_BUFFER* buffers = calloc(count, sizeof(*buffers));
    if (buffers == NULL) {
        return ENOMEM;
    }

    if (alpns == NULL || alpns_len == 0) {
        buffers[0].Buffer = (uint8_t*)config->alpn;
        buffers[0].Length = config->alpn_len;
    } else {
        for (size_t i = 0; i < count; i++) {
            if (alpns[i].alpn == NULL || alpns[i].alpn_len == 0) {
                free(buffers);
                return EINVAL;
            }
            buffers[i].Buffer = (uint8_t*)alpns[i].alpn;
            buffers[i].Length = alpns[i].alpn_len;
        }
    }

    *out_buffers = buffers;
    *out_count = (uint32_t)count;
    return 0;
}

static int trevrpc_msquic_configure_endpoint_with_alpns(const trevrpc_msquic_config* config,
    const trevrpc_msquic_alpn* alpns,
    size_t alpns_len,
    bool server,
    HQUIC* registration,
    HQUIC* configuration) {
    int err = trevrpc_msquic_api_acquire();
    if (err != 0) {
        return err;
    }

    QUIC_EXECUTION_PROFILE execution_profile;
    switch (config->execution_profile) {
    case TREV_MSQUIC_EXECUTION_PROFILE_LOW_LATENCY:
        execution_profile = QUIC_EXECUTION_PROFILE_LOW_LATENCY;
        break;
    case TREV_MSQUIC_EXECUTION_PROFILE_MAX_THROUGHPUT:
        execution_profile = QUIC_EXECUTION_PROFILE_TYPE_MAX_THROUGHPUT;
        break;
    case TREV_MSQUIC_EXECUTION_PROFILE_SCAVENGER:
        execution_profile = QUIC_EXECUTION_PROFILE_TYPE_SCAVENGER;
        break;
    case TREV_MSQUIC_EXECUTION_PROFILE_REAL_TIME:
        execution_profile = QUIC_EXECUTION_PROFILE_TYPE_REAL_TIME;
        break;
    default:
        trevrpc_msquic_api_release();
        return EINVAL;
    }

    QUIC_REGISTRATION_CONFIG registration_config = {0};
    registration_config.AppName = server ? "trevrpc-c-server" : "trevrpc-c-client";
    registration_config.ExecutionProfile = execution_profile;

    QUIC_STATUS status = TrevMsQuic->RegistrationOpen(&registration_config, registration);
    if (QUIC_FAILED(status)) {
        trevrpc_msquic_api_release();
        return (int)status;
    }

    QUIC_BUFFER* alpn_buffers = NULL;
    uint32_t alpn_count = 0;
    err = trevrpc_msquic_build_alpn_buffers(config, alpns, alpns_len, &alpn_buffers, &alpn_count);
    if (err != 0) {
        TrevMsQuic->RegistrationClose(*registration);
        *registration = NULL;
        trevrpc_msquic_api_release();
        return err;
    }

    QUIC_SETTINGS settings = {0};
    if (config->max_idle_timeout_ms > 0) {
        settings.IsSet.IdleTimeoutMs = TRUE;
        settings.IdleTimeoutMs = config->max_idle_timeout_ms;
    }
    if (config->keep_alive_ms > 0) {
        settings.IsSet.KeepAliveIntervalMs = TRUE;
        settings.KeepAliveIntervalMs = config->keep_alive_ms;
    }
    if (config->peer_bidi_stream_count > 0) {
        settings.IsSet.PeerBidiStreamCount = TRUE;
        settings.PeerBidiStreamCount = config->peer_bidi_stream_count;
    }
    if (config->peer_unidi_stream_count > 0) {
        settings.IsSet.PeerUnidiStreamCount = TRUE;
        settings.PeerUnidiStreamCount = config->peer_unidi_stream_count;
    }
    if (config->max_stateless_operations > 0) {
        settings.IsSet.MaxStatelessOperations = TRUE;
        settings.MaxStatelessOperations = config->max_stateless_operations;
    }
    if (config->max_binding_stateless_operations > 0) {
        settings.IsSet.MaxBindingStatelessOperations = TRUE;
        settings.MaxBindingStatelessOperations = config->max_binding_stateless_operations;
    }
    if (config->stream_recv_window > 0) {
        settings.IsSet.StreamRecvWindowDefault = TRUE;
        settings.StreamRecvWindowDefault = config->stream_recv_window;
    }
    if (config->conn_flow_control_window > 0) {
        settings.IsSet.ConnFlowControlWindow = TRUE;
        settings.ConnFlowControlWindow = config->conn_flow_control_window;
    }
    settings.IsSet.SendBufferingEnabled = TRUE;
    settings.SendBufferingEnabled = config->send_buffering_enabled != 0;
    settings.IsSet.DatagramReceiveEnabled = TRUE;
    settings.DatagramReceiveEnabled = TRUE;
    if (server) {
        settings.IsSet.ServerResumptionLevel = TRUE;
        settings.ServerResumptionLevel = QUIC_SERVER_RESUME_ONLY;
    }

    status = TrevMsQuic->ConfigurationOpen(
        *registration, alpn_buffers, alpn_count, &settings, sizeof(settings), NULL, configuration);
    free(alpn_buffers);
    if (QUIC_FAILED(status)) {
        TrevMsQuic->RegistrationClose(*registration);
        *registration = NULL;
        trevrpc_msquic_api_release();
        return (int)status;
    }

    QUIC_CREDENTIAL_CONFIG credential = {0};
    QUIC_CERTIFICATE_FILE certificate_file = {0};
    if (server) {
        certificate_file.CertificateFile = config->cert_file;
        certificate_file.PrivateKeyFile = config->key_file;
        credential.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE;
        credential.CertificateFile = &certificate_file;
        credential.Flags = QUIC_CREDENTIAL_FLAG_NONE;
    } else {
        credential.Type = QUIC_CREDENTIAL_TYPE_NONE;
        credential.Flags = QUIC_CREDENTIAL_FLAG_CLIENT;
        if (config->skip_certificate_validation) {
            credential.Flags |= QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION;
        }
        if (config->ca_cert_file != NULL) {
            credential.Flags |= QUIC_CREDENTIAL_FLAG_SET_CA_CERTIFICATE_FILE;
            credential.CaCertificateFile = config->ca_cert_file;
        }
    }

    status = TrevMsQuic->ConfigurationLoadCredential(*configuration, &credential);
    if (QUIC_FAILED(status)) {
        TrevMsQuic->ConfigurationClose(*configuration);
        TrevMsQuic->RegistrationClose(*registration);
        *configuration = NULL;
        *registration = NULL;
        trevrpc_msquic_api_release();
        return (int)status;
    }

    return 0;
}

static int trevrpc_msquic_configure_endpoint(
    const trevrpc_msquic_config* config, bool server, HQUIC* registration, HQUIC* configuration) {
    return trevrpc_msquic_configure_endpoint_with_alpns(config, NULL, 0, server, registration, configuration);
}

static int trevrpc_msquic_addr(const char* host, uint16_t port, QUIC_ADDR* addr) {
    memset(addr, 0, sizeof(*addr));
    if (strchr(host, ':') != NULL) {
        addr->Ipv6.sin6_family = QUIC_ADDRESS_FAMILY_INET6;
        addr->Ipv6.sin6_port = htons(port);
        if (inet_pton(AF_INET6, host, &addr->Ipv6.sin6_addr) != 1) {
            return EINVAL;
        }
        return 0;
    }

    addr->Ipv4.sin_family = QUIC_ADDRESS_FAMILY_INET;
    addr->Ipv4.sin_port = htons(port);
    if (inet_pton(AF_INET, host, &addr->Ipv4.sin_addr) != 1) {
        return EINVAL;
    }

    return 0;
}

int trevrpc_msquic_listen(
    const char* host, uint16_t port, const trevrpc_msquic_config* config, trevrpc_msquic_listener** out_listener) {
    return trevrpc_msquic_listen_alpns(host, port, config, NULL, 0, out_listener);
}

int trevrpc_msquic_listen_alpns(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    const trevrpc_msquic_alpn* alpns,
    size_t alpns_len,
    trevrpc_msquic_listener** out_listener) {
    if (host == NULL || config == NULL || out_listener == NULL) {
        return EINVAL;
    }
    *out_listener = NULL;
    trevrpc_msquic_listener* listener = calloc(1, sizeof(*listener));
    if (listener == NULL) {
        return ENOMEM;
    }

    pthread_mutex_init(&listener->mutex, NULL);
    pthread_cond_init(&listener->cond, NULL);
    listener->max_frame_size = trevrpc_msquic_effective_max_frame_size(config->max_frame_size);
    listener->max_pending_send_bytes = trevrpc_msquic_effective_max_pending_send_bytes(config->max_pending_send_bytes);
    listener->max_pending_send_count = trevrpc_msquic_effective_max_pending_send_count(config->max_pending_send_count);

    int err = trevrpc_msquic_configure_endpoint_with_alpns(
        config, alpns, alpns_len, true, &listener->registration, &listener->configuration);
    if (err != 0) {
        trevrpc_msquic_listener_close(listener);
        return err;
    }
    listener->api_ref_acquired = true;

    QUIC_STATUS status = TrevMsQuic->ListenerOpen(
        listener->registration, trevrpc_msquic_listener_callback, listener, &listener->listener);
    if (QUIC_FAILED(status)) {
        trevrpc_msquic_listener_close(listener);
        return (int)status;
    }

    QUIC_ADDR addr = {0};
    err = trevrpc_msquic_addr(host, port, &addr);
    if (err != 0) {
        trevrpc_msquic_listener_close(listener);
        return err;
    }

    QUIC_BUFFER* alpn_buffers = NULL;
    uint32_t alpn_count = 0;
    err = trevrpc_msquic_build_alpn_buffers(config, alpns, alpns_len, &alpn_buffers, &alpn_count);
    if (err != 0) {
        trevrpc_msquic_listener_close(listener);
        return err;
    }
    status = TrevMsQuic->ListenerStart(listener->listener, alpn_buffers, alpn_count, &addr);
    free(alpn_buffers);
    if (QUIC_FAILED(status)) {
        trevrpc_msquic_listener_close(listener);
        return (int)status;
    }

    *out_listener = listener;
    return 0;
}

int trevrpc_msquic_listener_accept(trevrpc_msquic_listener* listener, trevrpc_msquic_conn** out_conn) {
    *out_conn = NULL;
    pthread_mutex_lock(&listener->mutex);
    while (listener->conn_head == NULL && !listener->closed) {
        pthread_cond_wait(&listener->cond, &listener->mutex);
    }

    if (listener->conn_head == NULL) {
        int err = listener->err != 0 ? listener->err : TREV_MSQUIC_ERR_CLOSED;
        pthread_mutex_unlock(&listener->mutex);
        return err;
    }

    trevrpc_msquic_conn_node* node = listener->conn_head;
    listener->conn_head = node->next;
    if (listener->conn_head == NULL) {
        listener->conn_tail = NULL;
    }
    pthread_mutex_unlock(&listener->mutex);

    trevrpc_msquic_conn* conn = node->conn;
    free(node);

    pthread_mutex_lock(&conn->mutex);
    while (!conn->connected && !conn->shutdown_complete && conn->err == 0) {
        pthread_cond_wait(&conn->cond, &conn->mutex);
    }
    int err = conn->err;
    bool connected = conn->connected;
    pthread_mutex_unlock(&conn->mutex);

    if (!connected) {
        trevrpc_msquic_conn_close(conn);
        return err != 0 ? err : TREV_MSQUIC_ERR_CLOSED;
    }

    *out_conn = conn;
    return 0;
}

int trevrpc_msquic_listener_port(trevrpc_msquic_listener* listener, uint16_t* out_port) {
    if (listener == NULL || out_port == NULL) {
        return EINVAL;
    }

    QUIC_ADDR addr = {0};
    uint32_t addr_len = sizeof(addr);
    QUIC_STATUS status = TrevMsQuic->GetParam(listener->listener, QUIC_PARAM_LISTENER_LOCAL_ADDRESS, &addr_len, &addr);
    if (QUIC_FAILED(status)) {
        return (int)status;
    }
    *out_port = ntohs(addr.Ipv4.sin_port);
    return 0;
}

void trevrpc_msquic_listener_close(trevrpc_msquic_listener* listener) {
    if (listener == NULL) {
        return;
    }

    trevrpc_msquic_listener_shutdown(listener);

    pthread_mutex_lock(&listener->mutex);
    while (listener->shutdown_waiters > 0) {
        pthread_cond_wait(&listener->cond, &listener->mutex);
    }
    HQUIC handle = listener->listener;
    listener->listener = NULL;
    pthread_mutex_unlock(&listener->mutex);
    if (handle != NULL) {
        TrevMsQuic->ListenerClose(handle);
    }

    pthread_mutex_lock(&listener->mutex);
    while (listener->active_callbacks > 0) {
        pthread_cond_wait(&listener->cond, &listener->mutex);
    }
    pthread_mutex_unlock(&listener->mutex);

    while (listener->conn_head != NULL) {
        trevrpc_msquic_conn_node* node = listener->conn_head;
        listener->conn_head = node->next;
        trevrpc_msquic_conn_close(node->conn);
        free(node);
    }

    if (listener->configuration != NULL) {
        TrevMsQuic->ConfigurationClose(listener->configuration);
        listener->configuration = NULL;
    }
    if (listener->registration != NULL) {
#ifndef TREVRPC_SANITIZER_BUILD
        TrevMsQuic->RegistrationClose(listener->registration);
#endif
        listener->registration = NULL;
    }
    if (listener->api_ref_acquired) {
        listener->api_ref_acquired = false;
        trevrpc_msquic_api_release();
    }

    pthread_cond_destroy(&listener->cond);
    pthread_mutex_destroy(&listener->mutex);
    free(listener);
}

void trevrpc_msquic_listener_shutdown(trevrpc_msquic_listener* listener) {
    if (listener == NULL) {
        return;
    }

    HQUIC handle = NULL;
    pthread_mutex_lock(&listener->mutex);
    if (!listener->closed) {
        listener->closed = true;
        handle = listener->listener;
        listener->shutdown_in_progress = handle != NULL;
        pthread_cond_broadcast(&listener->cond);
    } else if (listener->shutdown_in_progress) {
        listener->shutdown_waiters++;
        while (listener->shutdown_in_progress) {
            pthread_cond_wait(&listener->cond, &listener->mutex);
        }
        listener->shutdown_waiters--;
        pthread_cond_broadcast(&listener->cond);
    }
    pthread_mutex_unlock(&listener->mutex);

    if (handle != NULL) {
        TrevMsQuic->ListenerStop(handle);
        pthread_mutex_lock(&listener->mutex);
        listener->shutdown_in_progress = false;
        pthread_cond_broadcast(&listener->cond);
        pthread_mutex_unlock(&listener->mutex);
    }
}

int trevrpc_msquic_dial(
    const char* host, uint16_t port, const trevrpc_msquic_config* config, trevrpc_msquic_conn** out_conn) {
    return trevrpc_msquic_dial_cancellable(host, port, config, NULL, NULL, out_conn);
}

int trevrpc_msquic_dial_cancellable(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    trevrpc_msquic_cancelled_fn cancelled,
    void* cancellation_context,
    trevrpc_msquic_conn** out_conn) {
    return trevrpc_msquic_dial_observed(
        host, port, config, cancelled, cancellation_context, NULL, 0, NULL, NULL, out_conn);
}

int trevrpc_msquic_dial_observed(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    trevrpc_msquic_cancelled_fn cancelled,
    void* cancellation_context,
    const uint8_t* resumption_ticket,
    size_t resumption_ticket_len,
    trevrpc_msquic_conn_observer observer,
    void* observer_context,
    trevrpc_msquic_conn** out_conn) {
    if (host == NULL || config == NULL || out_conn == NULL ||
        (resumption_ticket == NULL && resumption_ticket_len != 0) || resumption_ticket_len > UINT32_MAX) {
        return EINVAL;
    }
    *out_conn = NULL;
    HQUIC registration = NULL;
    HQUIC configuration = NULL;
    int err = trevrpc_msquic_configure_endpoint(config, false, &registration, &configuration);
    if (err != 0) {
        return err;
    }

    trevrpc_msquic_conn* conn = trevrpc_msquic_conn_alloc(NULL);
    if (conn == NULL) {
        TrevMsQuic->ConfigurationClose(configuration);
        TrevMsQuic->RegistrationClose(registration);
        trevrpc_msquic_api_release();
        return ENOMEM;
    }
    conn->registration = registration;
    conn->configuration = configuration;
    conn->owns_endpoint = true;
    conn->observer = observer;
    conn->observer_context = observer_context;
    conn->max_frame_size = trevrpc_msquic_effective_max_frame_size(config->max_frame_size);
    conn->max_pending_send_bytes = trevrpc_msquic_effective_max_pending_send_bytes(config->max_pending_send_bytes);
    conn->max_pending_send_count = trevrpc_msquic_effective_max_pending_send_count(config->max_pending_send_count);
    if (config->alpn != NULL && config->alpn_len > 0 && config->alpn_len <= sizeof(conn->negotiated_alpn)) {
        conn->negotiated_alpn_len = (uint8_t)config->alpn_len;
        memcpy(conn->negotiated_alpn, config->alpn, conn->negotiated_alpn_len);
    }

    QUIC_STATUS status = TrevMsQuic->ConnectionOpen(registration, trevrpc_msquic_conn_callback, conn, &conn->handle);
    if (QUIC_FAILED(status)) {
        trevrpc_msquic_conn_close(conn);
        return (int)status;
    }

    if (resumption_ticket_len > 0) {
        status = TrevMsQuic->SetParam(
            conn->handle, QUIC_PARAM_CONN_RESUMPTION_TICKET, (uint32_t)resumption_ticket_len, resumption_ticket);
        if (QUIC_FAILED(status)) {
            trevrpc_msquic_conn_close(conn);
            return (int)status;
        }
    }

    HQUIC connection_handle = trevrpc_msquic_conn_handle_acquire(conn);
    if (connection_handle == NULL) {
        trevrpc_msquic_conn_close(conn);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    status = TrevMsQuic->ConnectionStart(connection_handle, configuration, QUIC_ADDRESS_FAMILY_UNSPEC, host, port);
    trevrpc_msquic_conn_handle_release(conn);
    if (QUIC_FAILED(status)) {
        trevrpc_msquic_conn_close(conn);
        return (int)status;
    }

    pthread_mutex_lock(&conn->mutex);
    bool was_cancelled = false;
    int wait_err = 0;
    while (!conn->connected && !conn->shutdown_complete && conn->err == 0) {
        if (cancelled != NULL && cancelled(cancellation_context)) {
            was_cancelled = true;
            break;
        }
        struct timespec wake_at = {0};
        if (clock_gettime(CLOCK_REALTIME, &wake_at) != 0) {
            wait_err = -errno;
            break;
        }
        wake_at.tv_nsec += 10 * 1000 * 1000;
        if (wake_at.tv_nsec >= 1000 * 1000 * 1000) {
            wake_at.tv_sec++;
            wake_at.tv_nsec -= 1000 * 1000 * 1000;
        }
        (void)pthread_cond_timedwait(&conn->cond, &conn->mutex, &wake_at);
    }
    err = wait_err != 0 ? wait_err : conn->err;
    bool connected = conn->connected;
    pthread_mutex_unlock(&conn->mutex);

    if (was_cancelled) {
        trevrpc_msquic_conn_shutdown(conn);
        trevrpc_msquic_conn_close(conn);
        return -ECANCELED;
    }

    if (!connected) {
        trevrpc_msquic_conn_close(conn);
        return err != 0 ? err : TREV_MSQUIC_ERR_CLOSED;
    }

    *out_conn = conn;
    return 0;
}

int trevrpc_msquic_conn_negotiated_alpn(trevrpc_msquic_conn* conn, const uint8_t** alpn, size_t* alpn_len) {
    if (conn == NULL || alpn == NULL || alpn_len == NULL) {
        return EINVAL;
    }
    if (conn->negotiated_alpn_len == 0) {
        return EINVAL;
    }
    *alpn = conn->negotiated_alpn;
    *alpn_len = conn->negotiated_alpn_len;
    return 0;
}

int trevrpc_msquic_conn_peer_close_error(trevrpc_msquic_conn* conn, uint64_t* error_code) {
    if (conn == NULL || error_code == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&conn->mutex);
    bool available = conn->peer_close_error_set;
    *error_code = conn->peer_close_error;
    pthread_mutex_unlock(&conn->mutex);
    return available ? 0 : -EAGAIN;
}

void trevrpc_msquic_conn_clear_observer(trevrpc_msquic_conn* conn) {
    if (conn == NULL) {
        return;
    }
    pthread_mutex_lock(&conn->mutex);
    conn->observer = NULL;
    conn->observer_context = NULL;
    while (conn->active_observer_callbacks > 0) {
        pthread_cond_wait(&conn->cond, &conn->mutex);
    }
    pthread_mutex_unlock(&conn->mutex);
}

int trevrpc_msquic_conn_accept_stream(trevrpc_msquic_conn* conn, trevrpc_msquic_stream** out_stream) {
    *out_stream = NULL;
    pthread_mutex_lock(&conn->mutex);
    while (conn->stream_head == NULL && !conn->closed && !conn->shutdown_complete && conn->err == 0) {
        pthread_cond_wait(&conn->cond, &conn->mutex);
    }

    if (conn->stream_head == NULL) {
        int err = conn->err != 0 ? conn->err : TREV_MSQUIC_ERR_CLOSED;
        pthread_mutex_unlock(&conn->mutex);
        return err;
    }

    trevrpc_msquic_stream_node* node = conn->stream_head;
    conn->stream_head = node->next;
    if (conn->stream_head == NULL) {
        conn->stream_tail = NULL;
    }
    pthread_mutex_unlock(&conn->mutex);

    *out_stream = node->stream;
    free(node);
    return 0;
}

static int trevrpc_msquic_conn_open_stream_with_flags(
    trevrpc_msquic_conn* conn, trevrpc_msquic_stream** out_stream, QUIC_STREAM_OPEN_FLAGS flags) {
    *out_stream = NULL;
    trevrpc_msquic_stream* stream =
        trevrpc_msquic_stream_alloc(NULL, conn->max_pending_send_bytes, conn->max_pending_send_count, 0);
    if (stream == NULL) {
        return ENOMEM;
    }

    HQUIC connection_handle = trevrpc_msquic_conn_handle_acquire(conn);
    if (connection_handle == NULL) {
        trevrpc_msquic_stream_close(stream);
        return TREV_MSQUIC_ERR_CLOSED;
    }

    QUIC_STATUS status =
        TrevMsQuic->StreamOpen(connection_handle, flags, trevrpc_msquic_stream_callback, stream, &stream->handle);
    trevrpc_msquic_conn_handle_release(conn);
    if (QUIC_FAILED(status)) {
        trevrpc_msquic_stream_close(stream);
        return (int)status;
    }

    HQUIC stream_handle = trevrpc_msquic_stream_handle_acquire(stream);
    if (stream_handle == NULL) {
        trevrpc_msquic_stream_close(stream);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    status = TrevMsQuic->StreamStart(stream_handle, QUIC_STREAM_START_FLAG_IMMEDIATE);
    trevrpc_msquic_stream_handle_release(stream);
    if (QUIC_FAILED(status)) {
        trevrpc_msquic_stream_close(stream);
        return (int)status;
    }

    *out_stream = stream;
    return 0;
}

int trevrpc_msquic_conn_open_stream(trevrpc_msquic_conn* conn, trevrpc_msquic_stream** out_stream) {
    return trevrpc_msquic_conn_open_stream_with_flags(conn, out_stream, QUIC_STREAM_OPEN_FLAG_NONE);
}

int trevrpc_msquic_conn_open_uni_stream(trevrpc_msquic_conn* conn, trevrpc_msquic_stream** out_stream) {
    return trevrpc_msquic_conn_open_stream_with_flags(conn, out_stream, QUIC_STREAM_OPEN_FLAG_UNIDIRECTIONAL);
}

int trevrpc_msquic_stream_id(trevrpc_msquic_stream* stream, uint64_t* out_stream_id) {
    if (stream == NULL || out_stream_id == NULL) {
        return EINVAL;
    }

    HQUIC handle = trevrpc_msquic_stream_handle_acquire(stream);
    if (handle == NULL) {
        return TREV_MSQUIC_ERR_CLOSED;
    }

    uint32_t stream_id_len = sizeof(*out_stream_id);
    QUIC_STATUS status = TrevMsQuic->GetParam(handle, QUIC_PARAM_STREAM_ID, &stream_id_len, out_stream_id);
    trevrpc_msquic_stream_handle_release(stream);
    return QUIC_FAILED(status) ? (int)status : 0;
}

void trevrpc_msquic_conn_close(trevrpc_msquic_conn* conn) {
    if (conn == NULL) {
        return;
    }
    bool release_api = conn->api_ref_acquired;

    trevrpc_msquic_conn_shutdown(conn);

    pthread_mutex_lock(&conn->mutex);
    while (conn->handle != NULL || conn->close_pending) {
        pthread_cond_wait(&conn->cond, &conn->mutex);
    }
    pthread_mutex_unlock(&conn->mutex);

    while (conn->stream_head != NULL) {
        trevrpc_msquic_stream_node* node = conn->stream_head;
        conn->stream_head = node->next;
        trevrpc_msquic_stream_close(node->stream);
        free(node);
    }

    if (conn->owns_endpoint) {
        if (conn->configuration != NULL) {
            TrevMsQuic->ConfigurationClose(conn->configuration);
        }
        if (conn->registration != NULL) {
#ifndef TREVRPC_SANITIZER_BUILD
            TrevMsQuic->RegistrationClose(conn->registration);
#endif
            conn->registration = NULL;
        }
        trevrpc_msquic_api_release();
    }

    pthread_cond_destroy(&conn->cond);
    pthread_mutex_destroy(&conn->mutex);
    free(conn);
    if (release_api) {
        trevrpc_msquic_api_release();
    }
}

void trevrpc_msquic_conn_shutdown_error(trevrpc_msquic_conn* conn, uint64_t error_code) {
    if (conn == NULL) {
        return;
    }

    pthread_mutex_lock(&conn->mutex);
    HQUIC handle = conn->handle;
    conn->closed = true;
    if (handle != NULL && !conn->close_pending) {
        conn->active_handle_ops++;
    } else {
        handle = NULL;
    }
    pthread_cond_broadcast(&conn->cond);
    pthread_mutex_unlock(&conn->mutex);
    if (handle != NULL) {
        TrevMsQuic->ConnectionShutdown(handle, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, error_code);
        trevrpc_msquic_conn_handle_release(conn);
    }
}

void trevrpc_msquic_conn_shutdown(trevrpc_msquic_conn* conn) {
    trevrpc_msquic_conn_shutdown_error(conn, 0);
}

static intptr_t trevrpc_msquic_stream_read_until(
    trevrpc_msquic_stream* stream, uint8_t* data, size_t len, const struct timespec* deadline, bool ready_only) {
    if (stream == NULL || (data == NULL && len > 0)) {
        return -EINVAL;
    }
    if (len == 0) {
        return 0;
    }

    pthread_mutex_lock(&stream->mutex);
    if (stream->recv_mode == TREV_MSQUIC_RECV_FRAMES) {
        pthread_mutex_unlock(&stream->mutex);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    int wait_err = 0;
    if (ready_only && stream->recv_head == NULL && !stream->recv_fin && !stream->closed && stream->err == 0) {
        wait_err = TREV_MSQUIC_ERR_TIMEOUT;
    } else {
        wait_err = trevrpc_msquic_stream_wait_recv_locked(stream, deadline);
    }
    if (wait_err != 0) {
        pthread_mutex_unlock(&stream->mutex);
        return wait_err < 0 ? wait_err : -wait_err;
    }

    if (stream->recv_head == NULL) {
        intptr_t result = stream->err != 0 ? trevrpc_msquic_error_result(stream->err) : TREV_MSQUIC_ERR_EOF;
        pthread_mutex_unlock(&stream->mutex);
        return result;
    }

    trevrpc_msquic_chunk* chunk = stream->recv_head;
    size_t available = chunk->len - chunk->offset;
    size_t copied = available < len ? available : len;
    memcpy(data, chunk->data + chunk->offset, copied);
    chunk->offset += copied;
    stream->recv_buffered -= copied;
    if (chunk->offset == chunk->len) {
        stream->recv_head = chunk->next;
        if (stream->recv_head == NULL) {
            stream->recv_tail = NULL;
        }
        free(chunk);
    }
    pthread_mutex_unlock(&stream->mutex);

    return (intptr_t)copied;
}

intptr_t trevrpc_msquic_stream_read(trevrpc_msquic_stream* stream, uint8_t* data, size_t len) {
    return trevrpc_msquic_stream_read_until(stream, data, len, NULL, false);
}

intptr_t trevrpc_msquic_stream_read_timeout(
    trevrpc_msquic_stream* stream, uint8_t* data, size_t len, uint64_t timeout_nanos) {
    if (timeout_nanos == 0) {
        return trevrpc_msquic_stream_read(stream, data, len);
    }
    struct timespec deadline = {0};
    int err = trevrpc_msquic_realtime_deadline(timeout_nanos, &deadline);
    if (err != 0) {
        return err;
    }
    return trevrpc_msquic_stream_read_until(stream, data, len, &deadline, false);
}

intptr_t trevrpc_msquic_stream_read_ready(trevrpc_msquic_stream* stream, uint8_t* data, size_t len) {
    return trevrpc_msquic_stream_read_until(stream, data, len, NULL, true);
}

static intptr_t trevrpc_msquic_stream_read_frame_until(
    trevrpc_msquic_stream* stream, uint8_t** body, size_t* len, size_t max_len, const struct timespec* deadline) {
    *body = NULL;
    *len = 0;

    pthread_mutex_lock(&stream->mutex);
    int err = trevrpc_msquic_stream_enable_frame_mode_locked(stream, max_len);
    if (err != 0) {
        pthread_mutex_unlock(&stream->mutex);
        return err;
    }
    intptr_t ready = trevrpc_msquic_stream_wait_frame_locked(stream, deadline);
    if (ready != 0) {
        pthread_mutex_unlock(&stream->mutex);
        return ready;
    }
    if (stream->frame_head == NULL) {
        pthread_mutex_unlock(&stream->mutex);
        return TREV_MSQUIC_ERR_EOF;
    }

    trevrpc_msquic_frame* frame = trevrpc_msquic_stream_pop_frame_locked(stream);
    pthread_mutex_unlock(&stream->mutex);
    *len = frame->len;
    if (frame->err != 0) {
        intptr_t result = frame->err;
        free(frame->body);
        free(frame);
        return result;
    }
    if (frame->len > max_len) {
        free(frame->body);
        free(frame);
        return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
    }
    *body = frame->body;
    free(frame);
    return 1;
}

intptr_t trevrpc_msquic_stream_read_frame(trevrpc_msquic_stream* stream, uint8_t** body, size_t* len, size_t max_len) {
    return trevrpc_msquic_stream_read_frame_until(stream, body, len, max_len, NULL);
}

intptr_t trevrpc_msquic_stream_read_frame_timeout(
    trevrpc_msquic_stream* stream, uint8_t** body, size_t* len, size_t max_len, uint64_t timeout_nanos) {
    if (timeout_nanos == 0) {
        return trevrpc_msquic_stream_read_frame(stream, body, len, max_len);
    }

    struct timespec deadline = {0};
    int err = trevrpc_msquic_realtime_deadline(timeout_nanos, &deadline);
    if (err != 0) {
        return err;
    }

    return trevrpc_msquic_stream_read_frame_until(stream, body, len, max_len, &deadline);
}

intptr_t trevrpc_msquic_stream_read_frame_ready(
    trevrpc_msquic_stream* stream, uint8_t** body, size_t* len, size_t max_len) {
    *body = NULL;
    *len = 0;

    pthread_mutex_lock(&stream->mutex);
    int err = trevrpc_msquic_stream_enable_frame_mode_locked(stream, max_len);
    if (err != 0) {
        pthread_mutex_unlock(&stream->mutex);
        return err;
    }
    intptr_t ready = trevrpc_msquic_stream_frame_not_ready_locked(stream);
    if (ready != 0) {
        pthread_mutex_unlock(&stream->mutex);
        return ready;
    }
    if (stream->frame_head == NULL) {
        pthread_mutex_unlock(&stream->mutex);
        return TREV_MSQUIC_ERR_EOF;
    }

    trevrpc_msquic_frame* frame = trevrpc_msquic_stream_pop_frame_locked(stream);
    pthread_mutex_unlock(&stream->mutex);
    *len = frame->len;
    if (frame->err != 0) {
        intptr_t result = frame->err;
        free(frame->body);
        free(frame);
        return result;
    }
    if (frame->len > max_len) {
        free(frame->body);
        free(frame);
        return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
    }
    *body = frame->body;
    free(frame);
    return 1;
}

intptr_t trevrpc_msquic_stream_write(trevrpc_msquic_stream* stream, const uint8_t* data, size_t len) {
    if (len == 0) {
        return 0;
    }
    if (data == NULL) {
        return -EINVAL;
    }
    if (len > UINT32_MAX) {
        return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
    }

    int err = trevrpc_msquic_stream_send_op_acquire(stream);
    if (err != 0) {
        return err;
    }

    trevrpc_msquic_send* send = trevrpc_msquic_send_alloc(len, false);
    if (send == NULL) {
        trevrpc_msquic_stream_send_op_release(stream);
        return -ENOMEM;
    }
    memcpy(send->data, data, len);

    intptr_t result = trevrpc_msquic_stream_send_buffer(stream, send, len);
    trevrpc_msquic_stream_send_op_release(stream);
    return result;
}

intptr_t trevrpc_msquic_stream_write_fin(trevrpc_msquic_stream* stream, const uint8_t* data, size_t len) {
    if (len == 0) {
        return trevrpc_msquic_stream_shutdown_send(stream);
    }
    if (data == NULL) {
        return -EINVAL;
    }
    if (len > UINT32_MAX) {
        return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
    }

    int err = trevrpc_msquic_stream_send_op_acquire(stream);
    if (err != 0) {
        return err;
    }

    trevrpc_msquic_send* send = trevrpc_msquic_send_alloc(len, false);
    if (send == NULL) {
        trevrpc_msquic_stream_send_op_release(stream);
        return -ENOMEM;
    }
    memcpy(send->data, data, len);

    intptr_t result = trevrpc_msquic_stream_send_buffer_with_flags(stream, send, len, QUIC_SEND_FLAG_FIN, false);
    trevrpc_msquic_stream_send_op_release(stream);
    return result;
}

static intptr_t trevrpc_msquic_stream_write_frame_parts_with_flags(trevrpc_msquic_stream* stream,
    const trevrpc_msquic_frame_part* parts,
    size_t parts_len,
    size_t max_len,
    QUIC_SEND_FLAGS flags,
    trevrpc_msquic_send_completion** completion,
    bool wait_for_capacity) {
    if (completion != NULL) {
        *completion = NULL;
    }
    if (parts == NULL && parts_len > 0) {
        return -EINVAL;
    }
    if (parts_len > TREV_MSQUIC_SEND_MAX_BUFFERS - 1) {
        return -EINVAL;
    }

    size_t frame_body_len = 0;
    uint32_t buffer_count = 1;
    for (size_t i = 0; i < parts_len; i++) {
        if (parts[i].data == NULL && parts[i].len > 0) {
            return -EINVAL;
        }
        if (parts[i].len > UINT32_MAX) {
            return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
        }
        if (frame_body_len > SIZE_MAX - parts[i].len) {
            return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
        }
        frame_body_len += parts[i].len;
        if (parts[i].len > 0) {
            buffer_count++;
        }
    }
    if (frame_body_len > max_len || frame_body_len > UINT32_MAX) {
        return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
    }
    if (frame_body_len > SIZE_MAX - 4) {
        return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
    }

    int op_err = trevrpc_msquic_stream_send_op_acquire(stream);
    if (op_err != 0) {
        return op_err;
    }

    intptr_t result = 0;
    trevrpc_msquic_send* send = trevrpc_msquic_send_alloc(4, false);
    if (send == NULL) {
        result = -ENOMEM;
        goto cleanup;
    }

    send->data[0] = (uint8_t)(frame_body_len >> 24);
    send->data[1] = (uint8_t)(frame_body_len >> 16);
    send->data[2] = (uint8_t)(frame_body_len >> 8);
    send->data[3] = (uint8_t)frame_body_len;
    send->buffers[0].Buffer = send->data;
    send->buffers[0].Length = 4;
    uint32_t out = 1;
    for (size_t i = 0; i < parts_len; i++) {
        if (parts[i].len == 0) {
            continue;
        }
        send->buffers[out].Buffer = (uint8_t*)parts[i].data;
        send->buffers[out].Length = (uint32_t)parts[i].len;
        out++;
    }

    result = trevrpc_msquic_stream_send_buffers_with_flags(
        stream, send, buffer_count, 4 + frame_body_len, flags, completion, wait_for_capacity);
cleanup:
    trevrpc_msquic_stream_send_op_release(stream);
    return result;
}

intptr_t trevrpc_msquic_stream_write_frame_parts(
    trevrpc_msquic_stream* stream, const trevrpc_msquic_frame_part* parts, size_t parts_len, size_t max_len) {
    return trevrpc_msquic_stream_write_frame_parts_with_flags(
        stream, parts, parts_len, max_len, QUIC_SEND_FLAG_NONE, NULL, false);
}

intptr_t trevrpc_msquic_stream_write_frame_parts_fin(
    trevrpc_msquic_stream* stream, const trevrpc_msquic_frame_part* parts, size_t parts_len, size_t max_len) {
    return trevrpc_msquic_stream_write_frame_parts_with_flags(
        stream, parts, parts_len, max_len, QUIC_SEND_FLAG_FIN, NULL, false);
}

intptr_t trevrpc_msquic_stream_write_frame_parts_with_completion(trevrpc_msquic_stream* stream,
    const trevrpc_msquic_frame_part* parts,
    size_t parts_len,
    size_t max_len,
    trevrpc_msquic_send_completion** completion) {
    if (completion == NULL) {
        return -EINVAL;
    }
    return trevrpc_msquic_stream_write_frame_parts_with_flags(
        stream, parts, parts_len, max_len, QUIC_SEND_FLAG_NONE, completion, true);
}

intptr_t trevrpc_msquic_stream_write_frame_parts_fin_with_completion(trevrpc_msquic_stream* stream,
    const trevrpc_msquic_frame_part* parts,
    size_t parts_len,
    size_t max_len,
    trevrpc_msquic_send_completion** completion) {
    if (completion == NULL) {
        return -EINVAL;
    }
    return trevrpc_msquic_stream_write_frame_parts_with_flags(
        stream, parts, parts_len, max_len, QUIC_SEND_FLAG_FIN, completion, true);
}

static intptr_t trevrpc_msquic_stream_write_message_frame_internal(
    trevrpc_msquic_stream* stream, const uint8_t* body, size_t body_len, size_t max_len, bool wait_for_capacity) {
    if (body == NULL && body_len > 0) {
        return -EINVAL;
    }
    size_t frame_body_len = 0;
    if (body_len > 0) {
        size_t varint_len = trevrpc_msquic_varint_len(body_len);
        if (body_len > SIZE_MAX - 1 - varint_len) {
            return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
        }
        frame_body_len = 1 + varint_len + body_len;
    }
    if (frame_body_len > max_len || frame_body_len > UINT32_MAX - 4) {
        return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
    }

    int op_err = trevrpc_msquic_stream_send_op_acquire(stream);
    if (op_err != 0) {
        return op_err;
    }

    intptr_t result = 0;
    size_t frame_len = 4 + frame_body_len;
    trevrpc_msquic_send* send = trevrpc_msquic_send_acquire(stream, frame_len);
    if (send == NULL) {
        result = -ENOMEM;
        goto cleanup;
    }

    send->data[0] = (uint8_t)(frame_body_len >> 24);
    send->data[1] = (uint8_t)(frame_body_len >> 16);
    send->data[2] = (uint8_t)(frame_body_len >> 8);
    send->data[3] = (uint8_t)frame_body_len;
    if (body_len > 0) {
        uint8_t* out = send->data + 4;
        *out++ = 0x22;
        out = trevrpc_msquic_append_varint(out, body_len);
        memcpy(out, body, body_len);
    }

    result =
        trevrpc_msquic_stream_send_buffer_with_flags(stream, send, frame_len, QUIC_SEND_FLAG_NONE, wait_for_capacity);
cleanup:
    trevrpc_msquic_stream_send_op_release(stream);
    return result;
}

intptr_t trevrpc_msquic_stream_write_message_frame(
    trevrpc_msquic_stream* stream, const uint8_t* body, size_t body_len, size_t max_len) {
    return trevrpc_msquic_stream_write_message_frame_internal(stream, body, body_len, max_len, false);
}

intptr_t trevrpc_msquic_stream_write_message_frame_wait_capacity(
    trevrpc_msquic_stream* stream, const uint8_t* body, size_t body_len, size_t max_len) {
    return trevrpc_msquic_stream_write_message_frame_internal(stream, body, body_len, max_len, true);
}

intptr_t trevrpc_msquic_stream_write_message_frames(
    trevrpc_msquic_stream* stream, const uint8_t* bodies, const size_t* body_lens, size_t count, size_t max_len) {
    if (count == 0) {
        return 0;
    }
    if (bodies == NULL || body_lens == NULL) {
        return -EINVAL;
    }

    size_t frame_len = 0;
    for (size_t i = 0; i < count; i++) {
        size_t frame_body_len = 0;
        if (body_lens[i] > 0) {
            size_t varint_len = trevrpc_msquic_varint_len(body_lens[i]);
            if (body_lens[i] > SIZE_MAX - 1 - varint_len) {
                return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
            }
            frame_body_len = 1 + varint_len + body_lens[i];
        }
        if (frame_body_len > max_len || frame_body_len > UINT32_MAX - 4) {
            return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
        }
        if (frame_len > SIZE_MAX - 4 - frame_body_len) {
            return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
        }
        frame_len += 4 + frame_body_len;
    }

    int op_err = trevrpc_msquic_stream_send_op_acquire(stream);
    if (op_err != 0) {
        return op_err;
    }

    intptr_t result = 0;
    trevrpc_msquic_send* send = trevrpc_msquic_send_acquire(stream, frame_len);
    if (send == NULL) {
        result = -ENOMEM;
        goto cleanup;
    }

    uint8_t* out = send->data;
    size_t body_offset = 0;
    for (size_t i = 0; i < count; i++) {
        size_t frame_body_len = 0;
        if (body_lens[i] > 0) {
            frame_body_len = 1 + trevrpc_msquic_varint_len(body_lens[i]) + body_lens[i];
        }

        *out++ = (uint8_t)(frame_body_len >> 24);
        *out++ = (uint8_t)(frame_body_len >> 16);
        *out++ = (uint8_t)(frame_body_len >> 8);
        *out++ = (uint8_t)frame_body_len;
        if (body_lens[i] > 0) {
            *out++ = 0x22;
            out = trevrpc_msquic_append_varint(out, body_lens[i]);
            memcpy(out, bodies + body_offset, body_lens[i]);
            out += body_lens[i];
            body_offset += body_lens[i];
        }
    }

    result = trevrpc_msquic_stream_send_buffer(stream, send, frame_len);
cleanup:
    trevrpc_msquic_stream_send_op_release(stream);
    return result;
}

intptr_t trevrpc_msquic_stream_write_message_frames_borrowed(trevrpc_msquic_stream* stream,
    const uint8_t* const* bodies,
    const size_t* body_lens,
    size_t count,
    size_t max_len,
    trevrpc_msquic_send_completion** completion) {
    if (stream == NULL || completion == NULL || (body_lens == NULL && count > 0) || (bodies == NULL && count > 0)) {
        return -EINVAL;
    }
    *completion = NULL;
    if (count == 0) {
        return 0;
    }
    if (count > UINT32_MAX / 2u) {
        return -EOVERFLOW;
    }

    size_t header_data_len = 0;
    size_t frame_len = 0;
    uint32_t buffer_count = 0;
    for (size_t i = 0; i < count; i++) {
        if (bodies[i] == NULL && body_lens[i] > 0) {
            return -EINVAL;
        }
        size_t field_header_len = body_lens[i] == 0 ? 0 : 1 + trevrpc_msquic_varint_len(body_lens[i]);
        if (body_lens[i] > SIZE_MAX - field_header_len) {
            return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
        }
        size_t frame_body_len = field_header_len + body_lens[i];
        if (frame_body_len > max_len || frame_body_len > UINT32_MAX) {
            return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
        }
        if (header_data_len > SIZE_MAX - 4 - field_header_len || frame_len > SIZE_MAX - 4 - frame_body_len) {
            return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
        }
        header_data_len += 4 + field_header_len;
        frame_len += 4 + frame_body_len;
        buffer_count++;
        if (body_lens[i] > 0) {
            buffer_count++;
        }
    }

    int op_err = trevrpc_msquic_stream_send_op_acquire(stream);
    if (op_err != 0) {
        return op_err;
    }

    intptr_t result = 0;
    trevrpc_msquic_send* send = trevrpc_msquic_send_alloc(header_data_len, false);
    if (send == NULL) {
        result = -ENOMEM;
        goto cleanup;
    }
    int err = trevrpc_msquic_send_prepare_buffers(send, buffer_count);
    if (err != 0) {
        trevrpc_msquic_send_release(stream, send);
        result = err;
        goto cleanup;
    }

    QUIC_BUFFER* buffers = trevrpc_msquic_send_buffers(send);
    uint8_t* out = send->data;
    uint32_t buffer_index = 0;
    for (size_t i = 0; i < count; i++) {
        size_t field_header_len = body_lens[i] == 0 ? 0 : 1 + trevrpc_msquic_varint_len(body_lens[i]);
        size_t frame_body_len = field_header_len + body_lens[i];
        uint8_t* header = out;
        *out++ = (uint8_t)(frame_body_len >> 24);
        *out++ = (uint8_t)(frame_body_len >> 16);
        *out++ = (uint8_t)(frame_body_len >> 8);
        *out++ = (uint8_t)frame_body_len;
        if (body_lens[i] > 0) {
            *out++ = 0x22;
            out = trevrpc_msquic_append_varint(out, body_lens[i]);
        }
        buffers[buffer_index].Buffer = header;
        buffers[buffer_index].Length = (uint32_t)(4 + field_header_len);
        buffer_index++;
        if (body_lens[i] > 0) {
            buffers[buffer_index].Buffer = (uint8_t*)bodies[i];
            buffers[buffer_index].Length = (uint32_t)body_lens[i];
            buffer_index++;
        }
    }

    result = trevrpc_msquic_stream_send_buffers_with_flags(
        stream, send, buffer_count, frame_len, QUIC_SEND_FLAG_NONE, completion, true);
cleanup:
    trevrpc_msquic_stream_send_op_release(stream);
    return result;
}

int trevrpc_msquic_send_completion_wait(trevrpc_msquic_send_completion* completion) {
    if (completion == NULL) {
        return EINVAL;
    }
    pthread_mutex_lock(&completion->mutex);
    while (!completion->completed) {
        pthread_cond_wait(&completion->cond, &completion->mutex);
    }
    int result = completion->result;
    pthread_mutex_unlock(&completion->mutex);
    return result;
}

void trevrpc_msquic_send_completion_free(trevrpc_msquic_send_completion* completion) {
    if (completion == NULL) {
        return;
    }
    pthread_cond_destroy(&completion->cond);
    pthread_mutex_destroy(&completion->mutex);
    free(completion);
}

int trevrpc_msquic_stream_wait_pending_sends(trevrpc_msquic_stream* stream) {
    if (stream == NULL) {
        return EINVAL;
    }

    pthread_mutex_lock(&stream->mutex);
    while (stream->pending_send_count > 0 || stream->active_send_completions > 0) {
        pthread_cond_wait(&stream->cond, &stream->mutex);
    }
    pthread_mutex_unlock(&stream->mutex);
    return 0;
}

int trevrpc_msquic_stream_shutdown_send(trevrpc_msquic_stream* stream) {
    int err = trevrpc_msquic_stream_send_op_acquire(stream);
    if (err != 0) {
        return err;
    }

    int result = 0;
    pthread_mutex_lock(&stream->mutex);
    HQUIC handle = stream->handle;
    if (stream->api_closing || stream->send_aborted) {
        pthread_mutex_unlock(&stream->mutex);
        result = -ECANCELED;
        goto cleanup;
    }
    if (stream->send_closed) {
        pthread_mutex_unlock(&stream->mutex);
        goto cleanup;
    }
    if (handle == NULL || stream->close_pending || stream->closed) {
        pthread_mutex_unlock(&stream->mutex);
        result = TREV_MSQUIC_ERR_CLOSED;
        goto cleanup;
    }
    stream->send_closed = true;
    stream->active_handle_ops++;
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);
    trevrpc_msquic_test_emit_stream_event(TREV_MSQUIC_TEST_STREAM_SEND_TERMINAL);
    QUIC_STATUS status = trevrpc_msquic_test_stream_shutdown(handle, QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, 0);
    if (QUIC_FAILED(status)) {
        pthread_mutex_lock(&stream->mutex);
        stream->send_closed = false;
        pthread_cond_broadcast(&stream->cond);
        pthread_mutex_unlock(&stream->mutex);
    }
    trevrpc_msquic_stream_handle_release(stream);
    result = QUIC_FAILED(status) ? (int)status : 0;

cleanup:
    trevrpc_msquic_stream_send_op_release(stream);
    return result;
}

int trevrpc_msquic_stream_abort(trevrpc_msquic_stream* stream) {
    pthread_mutex_lock(&stream->mutex);
    HQUIC handle = stream->handle;
    if (handle == NULL || stream->close_pending) {
        stream->send_aborted = true;
        pthread_cond_broadcast(&stream->cond);
        pthread_mutex_unlock(&stream->mutex);
        return 0;
    }
    stream->send_aborted = true;
    stream->active_handle_ops++;
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);

    QUIC_STATUS status = TrevMsQuic->StreamShutdown(handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
    trevrpc_msquic_stream_handle_release(stream);
    return QUIC_FAILED(status) ? (int)status : 0;
}

int trevrpc_msquic_stream_abort_receive(trevrpc_msquic_stream* stream) {
    pthread_mutex_lock(&stream->mutex);
    HQUIC handle = stream->handle;
    if (handle == NULL || stream->close_pending) {
        pthread_mutex_unlock(&stream->mutex);
        return 0;
    }
    stream->active_handle_ops++;
    pthread_mutex_unlock(&stream->mutex);

    QUIC_STATUS status = TrevMsQuic->StreamShutdown(handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, 0);
    trevrpc_msquic_stream_handle_release(stream);
    return QUIC_FAILED(status) ? (int)status : 0;
}

void trevrpc_msquic_stream_close(trevrpc_msquic_stream* stream) {
    if (stream == NULL) {
        return;
    }
    bool release_api = stream->api_ref_acquired;

    pthread_mutex_lock(&stream->mutex);
    stream->api_closing = true;
    pthread_cond_broadcast(&stream->cond);
    trevrpc_msquic_test_emit_stream_event(TREV_MSQUIC_TEST_STREAM_CLOSE_STARTED);
    while (!trevrpc_msquic_stream_send_ops_idle(stream)) {
        pthread_cond_wait(&stream->cond, &stream->mutex);
    }
    HQUIC handle = stream->handle;
    bool graceful = stream->send_closed;
    if (handle != NULL && !stream->close_pending) {
        stream->active_handle_ops++;
        pthread_mutex_unlock(&stream->mutex);
        QUIC_STREAM_SHUTDOWN_FLAGS flags =
            graceful ? QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE : QUIC_STREAM_SHUTDOWN_FLAG_ABORT;
        trevrpc_msquic_test_stream_shutdown(handle, flags, 0);
        trevrpc_msquic_stream_handle_release(stream);
        pthread_mutex_lock(&stream->mutex);
    }
    while (!trevrpc_msquic_stream_send_ops_idle(stream) || stream->handle != NULL || stream->close_pending ||
           stream->active_handle_ops > 0 || stream->pending_send_count > 0 || stream->active_send_completions > 0 ||
           stream->send_capacity_waiters > 0) {
        pthread_cond_wait(&stream->cond, &stream->mutex);
    }
    trevrpc_msquic_free_chunks(stream);
    trevrpc_msquic_free_frames(stream);
    trevrpc_msquic_free_send_pool(stream);
    pthread_mutex_unlock(&stream->mutex);

    pthread_cond_destroy(&stream->cond);
    pthread_mutex_destroy(&stream->mutex);
    free(stream);
    if (release_api) {
        trevrpc_msquic_api_release();
    }
}

void trevrpc_msquic_free(void* ptr) {
    free(ptr);
}

const char* trevrpc_msquic_error(int code) {
    switch (code) {
    case 0:
        return "ok";
    case TREV_MSQUIC_ERR_CLOSED:
        return "closed";
    case TREV_MSQUIC_ERR_FRAME_TOO_LARGE:
        return "frame too large";
    case TREV_MSQUIC_ERR_TIMEOUT:
        return "timed out";
    case TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED:
        return "resource exhausted";
    case ENOMEM:
    case -ENOMEM:
        return "out of memory";
    case EINVAL:
    case -EINVAL:
        return "invalid argument";
    default:
        return "MsQuic operation failed";
    }
}

static QUIC_STATUS QUIC_API trevrpc_msquic_listener_callback(
    HQUIC listener_handle, void* context, QUIC_LISTENER_EVENT* event) {
    (void)listener_handle;
    trevrpc_msquic_listener* listener = context;
    if (event->Type != QUIC_LISTENER_EVENT_NEW_CONNECTION) {
        return QUIC_STATUS_SUCCESS;
    }

    pthread_mutex_lock(&listener->mutex);
    if (listener->closed) {
        pthread_mutex_unlock(&listener->mutex);
        return QUIC_STATUS_ABORTED;
    }
    listener->active_callbacks++;
    HQUIC configuration = listener->configuration;
    HQUIC registration = listener->registration;
    size_t max_frame_size = listener->max_frame_size;
    size_t max_pending_send_bytes = listener->max_pending_send_bytes;
    size_t max_pending_send_count = listener->max_pending_send_count;
    pthread_mutex_unlock(&listener->mutex);

    trevrpc_msquic_conn* conn = trevrpc_msquic_conn_alloc(event->NEW_CONNECTION.Connection);
    if (conn == NULL) {
        trevrpc_msquic_listener_callback_finish(listener);
        return QUIC_STATUS_OUT_OF_MEMORY;
    }
    if (event->NEW_CONNECTION.Info != NULL && event->NEW_CONNECTION.Info->NegotiatedAlpnLength > 0) {
        conn->negotiated_alpn_len = event->NEW_CONNECTION.Info->NegotiatedAlpnLength;
        memcpy(conn->negotiated_alpn, event->NEW_CONNECTION.Info->NegotiatedAlpn, conn->negotiated_alpn_len);
    }
    conn->configuration = configuration;
    conn->registration = registration;
    conn->max_frame_size = max_frame_size;
    conn->max_pending_send_bytes = max_pending_send_bytes;
    conn->max_pending_send_count = max_pending_send_count;
    HQUIC connection_handle = trevrpc_msquic_conn_handle_acquire(conn);
    if (connection_handle == NULL) {
        trevrpc_msquic_conn_close(conn);
        trevrpc_msquic_listener_callback_finish(listener);
        return QUIC_STATUS_ABORTED;
    }
    TrevMsQuic->SetCallbackHandler(connection_handle, (void*)trevrpc_msquic_conn_callback, conn);

    QUIC_STATUS status = TrevMsQuic->ConnectionSetConfiguration(connection_handle, configuration);
    trevrpc_msquic_conn_handle_release(conn);
    if (QUIC_FAILED(status)) {
        trevrpc_msquic_conn_close(conn);
        trevrpc_msquic_listener_callback_finish(listener);
        return status;
    }

    trevrpc_msquic_conn_node* node = malloc(sizeof(*node));
    if (node == NULL) {
        trevrpc_msquic_conn_close(conn);
        trevrpc_msquic_listener_callback_finish(listener);
        return QUIC_STATUS_OUT_OF_MEMORY;
    }
    node->conn = conn;
    node->next = NULL;

    pthread_mutex_lock(&listener->mutex);
    if (listener->closed) {
        pthread_mutex_unlock(&listener->mutex);
        free(node);
        trevrpc_msquic_conn_close(conn);
        trevrpc_msquic_listener_callback_finish(listener);
        return QUIC_STATUS_ABORTED;
    }
    if (listener->conn_tail != NULL) {
        listener->conn_tail->next = node;
    } else {
        listener->conn_head = node;
    }
    listener->conn_tail = node;
    pthread_cond_signal(&listener->cond);
    pthread_mutex_unlock(&listener->mutex);

    trevrpc_msquic_listener_callback_finish(listener);
    return QUIC_STATUS_SUCCESS;
}

static QUIC_STATUS QUIC_API trevrpc_msquic_conn_callback(
    HQUIC connection_handle, void* context, QUIC_CONNECTION_EVENT* event) {
    trevrpc_msquic_conn* conn = context;
    switch (event->Type) {
    case QUIC_CONNECTION_EVENT_CONNECTED:
        if (!conn->owns_endpoint && trevrpc_msquic_conn_uses_native_frames(conn)) {
            (void)TrevMsQuic->ConnectionSendResumptionTicket(
                connection_handle, QUIC_SEND_RESUMPTION_FLAG_NONE, 0, NULL);
        }
        pthread_mutex_lock(&conn->mutex);
        conn->connected = true;
        pthread_cond_broadcast(&conn->cond);
        pthread_mutex_unlock(&conn->mutex);
        trevrpc_msquic_conn_event connected_event = {
            .kind = TREV_MSQUIC_CONN_EVENT_CONNECTED,
            .session_resumed = event->CONNECTED.SessionResumed != FALSE,
        };
        trevrpc_msquic_conn_notify(conn, &connected_event);
        return QUIC_STATUS_SUCCESS;
    case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED: {
        size_t initial_frame_max_len = trevrpc_msquic_conn_uses_native_frames(conn) ? conn->max_frame_size : 0;
        trevrpc_msquic_stream* stream = trevrpc_msquic_stream_alloc(event->PEER_STREAM_STARTED.Stream,
            conn->max_pending_send_bytes,
            conn->max_pending_send_count,
            initial_frame_max_len);
        if (stream == NULL) {
            return QUIC_STATUS_OUT_OF_MEMORY;
        }
        TrevMsQuic->SetCallbackHandler(
            event->PEER_STREAM_STARTED.Stream, (void*)trevrpc_msquic_stream_callback, stream);
        trevrpc_msquic_stream_node* node = malloc(sizeof(*node));
        if (node == NULL) {
            trevrpc_msquic_stream_close(stream);
            return QUIC_STATUS_OUT_OF_MEMORY;
        }
        node->stream = stream;
        node->next = NULL;

        pthread_mutex_lock(&conn->mutex);
        if (conn->stream_tail != NULL) {
            conn->stream_tail->next = node;
        } else {
            conn->stream_head = node;
        }
        conn->stream_tail = node;
        pthread_cond_signal(&conn->cond);
        pthread_mutex_unlock(&conn->mutex);
        return QUIC_STATUS_SUCCESS;
    }
    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT:
        pthread_mutex_lock(&conn->mutex);
        conn->err = (int)event->SHUTDOWN_INITIATED_BY_TRANSPORT.Status;
        pthread_cond_broadcast(&conn->cond);
        pthread_mutex_unlock(&conn->mutex);
        return QUIC_STATUS_SUCCESS;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
        pthread_mutex_lock(&conn->mutex);
        conn->peer_close_error = event->SHUTDOWN_INITIATED_BY_PEER.ErrorCode;
        conn->peer_close_error_set = true;
        pthread_cond_broadcast(&conn->cond);
        pthread_mutex_unlock(&conn->mutex);
        return QUIC_STATUS_SUCCESS;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE: {
        trevrpc_msquic_conn_event shutdown_event = {
            .kind = TREV_MSQUIC_CONN_EVENT_SHUTDOWN_COMPLETE,
            .error_code = conn->err,
        };
        trevrpc_msquic_conn_notify(conn, &shutdown_event);
        trevrpc_msquic_conn_shutdown_complete(conn, connection_handle);
        return QUIC_STATUS_SUCCESS;
    }
    case QUIC_CONNECTION_EVENT_LOCAL_ADDRESS_CHANGED: {
        trevrpc_msquic_conn_event address_event = {
            .kind = TREV_MSQUIC_CONN_EVENT_LOCAL_ADDRESS_CHANGED,
        };
        trevrpc_msquic_conn_notify(conn, &address_event);
        return QUIC_STATUS_SUCCESS;
    }
    case QUIC_CONNECTION_EVENT_PEER_ADDRESS_CHANGED: {
        trevrpc_msquic_conn_event address_event = {
            .kind = TREV_MSQUIC_CONN_EVENT_PEER_ADDRESS_CHANGED,
        };
        trevrpc_msquic_conn_notify(conn, &address_event);
        return QUIC_STATUS_SUCCESS;
    }
    case QUIC_CONNECTION_EVENT_RESUMPTION_TICKET_RECEIVED: {
        trevrpc_msquic_conn_event ticket_event = {
            .kind = TREV_MSQUIC_CONN_EVENT_RESUMPTION_TICKET_RECEIVED,
            .resumption_ticket = event->RESUMPTION_TICKET_RECEIVED.ResumptionTicket,
            .resumption_ticket_len = event->RESUMPTION_TICKET_RECEIVED.ResumptionTicketLength,
        };
        trevrpc_msquic_conn_notify(conn, &ticket_event);
        return QUIC_STATUS_SUCCESS;
    }
    default:
        return QUIC_STATUS_SUCCESS;
    }
}

static QUIC_STATUS QUIC_API trevrpc_msquic_stream_callback(
    HQUIC stream_handle, void* context, QUIC_STREAM_EVENT* event) {
    trevrpc_msquic_stream* stream = context;
    switch (event->Type) {
    case QUIC_STREAM_EVENT_RECEIVE: {
        if (event->RECEIVE.TotalBufferLength > 0) {
            pthread_mutex_lock(&stream->mutex);
            if (stream->recv_mode == TREV_MSQUIC_RECV_FRAMES) {
                int err = 0;
                for (uint32_t i = 0; i < event->RECEIVE.BufferCount; i++) {
                    err = trevrpc_msquic_stream_append_frame_bytes_locked(
                        stream, event->RECEIVE.Buffers[i].Buffer, event->RECEIVE.Buffers[i].Length);
                    if (err != 0) {
                        break;
                    }
                }
                if (err != 0) {
                    stream->err = -err;
                    pthread_cond_broadcast(&stream->cond);
                    pthread_mutex_unlock(&stream->mutex);
                    return QUIC_STATUS_OUT_OF_MEMORY;
                }
            } else {
                if (event->RECEIVE.TotalBufferLength > SIZE_MAX - sizeof(trevrpc_msquic_chunk)) {
                    pthread_mutex_unlock(&stream->mutex);
                    return QUIC_STATUS_OUT_OF_MEMORY;
                }
                trevrpc_msquic_chunk* chunk = malloc(sizeof(*chunk) + (size_t)event->RECEIVE.TotalBufferLength);
                if (chunk == NULL) {
                    pthread_mutex_unlock(&stream->mutex);
                    return QUIC_STATUS_OUT_OF_MEMORY;
                }
                chunk->next = NULL;
                chunk->len = (size_t)event->RECEIVE.TotalBufferLength;
                chunk->offset = 0;
                size_t offset = 0;
                for (uint32_t i = 0; i < event->RECEIVE.BufferCount; i++) {
                    memcpy(chunk->data + offset, event->RECEIVE.Buffers[i].Buffer, event->RECEIVE.Buffers[i].Length);
                    offset += event->RECEIVE.Buffers[i].Length;
                }

                if (stream->recv_tail != NULL) {
                    stream->recv_tail->next = chunk;
                } else {
                    stream->recv_head = chunk;
                }
                stream->recv_tail = chunk;
                stream->recv_buffered += chunk->len;
            }
            pthread_mutex_unlock(&stream->mutex);
        }

        pthread_mutex_lock(&stream->mutex);
        if ((event->RECEIVE.Flags & QUIC_RECEIVE_FLAG_FIN) != 0) {
            stream->recv_fin = true;
        }
        pthread_cond_broadcast(&stream->cond);
        pthread_mutex_unlock(&stream->mutex);
        return QUIC_STATUS_SUCCESS;
    }
    case QUIC_STREAM_EVENT_SEND_COMPLETE:
        trevrpc_msquic_stream_send_complete(
            stream, event->SEND_COMPLETE.ClientContext, event->SEND_COMPLETE.Canceled != FALSE);
        return QUIC_STATUS_SUCCESS;
    case QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN:
        pthread_mutex_lock(&stream->mutex);
        stream->recv_fin = true;
        pthread_cond_broadcast(&stream->cond);
        pthread_mutex_unlock(&stream->mutex);
        return QUIC_STATUS_SUCCESS;
    case QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
        pthread_mutex_lock(&stream->mutex);
        stream->err = TREV_MSQUIC_ERR_CLOSED;
        stream->closed = true;
        pthread_cond_broadcast(&stream->cond);
        pthread_mutex_unlock(&stream->mutex);
        return QUIC_STATUS_SUCCESS;
    case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
        trevrpc_msquic_stream_shutdown_complete(stream, stream_handle);
        return QUIC_STATUS_SUCCESS;
    default:
        return QUIC_STATUS_SUCCESS;
    }
}
