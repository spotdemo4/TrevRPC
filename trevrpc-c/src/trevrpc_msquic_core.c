#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
#define _DARWIN_C_SOURCE
#endif

#define _POSIX_C_SOURCE 200809L
#define QUIC_API_ENABLE_VERSIONED_FEATURES 1

#include "trevrpc_msquic.h"

#include "trevrpc_msquic_internal.h"

#include "trevrpc_frame_internal.h"
#include "trevrpc_msquic_api_owner.h"

#include <arpa/inet.h>
#include <errno.h> // IWYU pragma: keep
#if __has_include(<msquic.h>)
#include <msquic.h>
#else
#include <inc/msquic.h>
#endif
#include <pthread.h>
#include <stdatomic.h>
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
#define TREV_MSQUIC_DEFAULT_STREAM_RECV_BYTES (16u * 1024u * 1024u)
#define TREV_MSQUIC_DEFAULT_STREAM_RECV_COUNT 1024u
#define TREV_MSQUIC_DEFAULT_CONNECTION_RECV_BYTES (64u * 1024u * 1024u)
#define TREV_MSQUIC_DEFAULT_CONNECTION_RECV_COUNT 4096u

typedef struct trevrpc_msquic_send trevrpc_msquic_send;
typedef struct trevrpc_msquic_frame trevrpc_msquic_frame;

typedef enum trevrpc_msquic_recv_mode {
    TREV_MSQUIC_RECV_UNDECIDED = 0,
    TREV_MSQUIC_RECV_BYTES = 1,
    TREV_MSQUIC_RECV_FRAMES = 2,
} trevrpc_msquic_recv_mode;

typedef enum trevrpc_msquic_receive_pause_kind {
    TREV_MSQUIC_RECV_PAUSE_NONE = 0,
    TREV_MSQUIC_RECV_PAUSE_RAW_CHUNK,
    TREV_MSQUIC_RECV_PAUSE_PARSER_BODY,
    TREV_MSQUIC_RECV_PAUSE_FRAME_NODE,
} trevrpc_msquic_receive_pause_kind;

typedef enum trevrpc_msquic_reserve_result {
    TREV_MSQUIC_RESERVE_OK = 0,
    TREV_MSQUIC_RESERVE_PRESSURE,
    TREV_MSQUIC_RESERVE_OVERFLOW,
} trevrpc_msquic_reserve_result;

typedef struct trevrpc_msquic_receive_budget {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    size_t ref_count;
    size_t owned_bytes;
    size_t owned_count;
    size_t max_owned_bytes;
    size_t max_owned_count;
    size_t undecided_admission_max_bytes;
    size_t undecided_admission_max_count;
    trevrpc_msquic_stream* paused_head;
    trevrpc_msquic_stream* paused_tail;
    bool resumer_active;
    bool kick_pending;
#ifdef TREVRPC_MSQUIC_TESTING
    uint64_t resume_scan_count;
#endif
} trevrpc_msquic_receive_budget;

typedef struct trevrpc_msquic_chunk {
    struct trevrpc_msquic_chunk* next;
    size_t len;
    size_t offset;
    size_t charge_bytes;
    size_t charge_count;
    uint8_t data[];
} trevrpc_msquic_chunk;

struct trevrpc_msquic_frame {
    trevrpc_msquic_frame* next;
    trevrpc_owned_bytes body;
    size_t declared_len;
    intptr_t err;
    size_t charge_bytes;
    size_t charge_count;
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
    trevrpc_frame_parser frame_parser;
    trevrpc_msquic_receive_budget* recv_budget;
    size_t configured_max_frame_size;
    size_t max_recv_owned_bytes;
    size_t max_recv_owned_count;
    size_t recv_owned_bytes;
    size_t recv_owned_count;
    size_t parser_owned_bytes;
    size_t parser_owned_count;
    bool parser_budgeted;
    trevrpc_msquic_receive_pause_kind recv_pause_kind;
    size_t recv_pause_need_bytes;
    size_t recv_pause_need_count;
    bool receive_disabled;
    bool receive_waiting_on_raw_pump;
    bool receive_resume_ready;
    _Atomic bool receive_closing;
    bool pause_listed;
    _Atomic size_t active_resume_pins;
#ifdef TREVRPC_MSQUIC_TESTING
    bool synthetic_receive_fixture;
#endif
    trevrpc_msquic_stream* pause_prev;
    trevrpc_msquic_stream* pause_next;
    size_t recv_grant_bytes;
    size_t recv_grant_count;
    trevrpc_msquic_receive_pause_kind recv_grant_kind;
    bool pending_frame_valid;
    trevrpc_msquic_frame pending_frame;
    enum {
        TREV_MSQUIC_PARSER_ALLOC_NONE,
        TREV_MSQUIC_PARSER_ALLOC_PRESSURE,
        TREV_MSQUIC_PARSER_ALLOC_OOM,
    } parser_alloc_result;
    bool recv_fin;
    bool send_closed;
    bool send_aborted;
    bool api_closing;
    bool destroy_requested;
    bool destroy_started;
    bool destroy_complete;
    bool close_shutdown_started;
    bool shutdown_complete;
    bool close_pending;
    bool closed;
    bool api_ref_acquired;
    size_t active_send_ops;
    size_t active_handle_ops;
    size_t active_send_completions;
    size_t active_lifecycle_refs;
    size_t active_close_calls;
    trevrpc_msquic_stream_observer observer;
    void* observer_context;
    size_t active_observer_callbacks;
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
    size_t max_stream_recv_owned_bytes;
    size_t max_stream_recv_owned_count;
    trevrpc_msquic_receive_budget* recv_budget;
    bool owns_endpoint;
    bool api_ref_acquired;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_msquic_stream_node* stream_head;
    trevrpc_msquic_stream_node* stream_tail;
    bool connected;
    bool connected_session_resumed;
    trevrpc_msquic_feature_state features;
    bool observer_terminal;
    bool destroy_requested;
    bool destroy_started;
    bool destroy_complete;
    bool shutdown_complete;
    bool close_pending;
    bool closed;
    size_t active_handle_ops;
    size_t active_lifecycle_refs;
    size_t active_close_calls;
    trevrpc_msquic_conn_observer observer;
    void* observer_context;
    size_t active_observer_callbacks;
    trevrpc_msquic_conn* destroy_next;
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
    trevrpc_msquic_receive_policy receive_policy;
    trevrpc_msquic_feature_request features;
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

static _Atomic(const QUIC_API_TABLE*) TrevMsQuic;
static pthread_mutex_t TrevMsQuicApiMutex = PTHREAD_MUTEX_INITIALIZER;
static size_t TrevMsQuicApiLeases;
static pthread_mutex_t TrevMsQuicFinalizerMutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t TrevMsQuicFinalizerCond = PTHREAD_COND_INITIALIZER;
static trevrpc_msquic_conn* TrevMsQuicFinalizerHead;
static trevrpc_msquic_conn* TrevMsQuicFinalizerTail;
static pthread_t TrevMsQuicFinalizerThread;
static bool TrevMsQuicFinalizerStarted;
static bool TrevMsQuicFinalizerStopping;
static bool TrevMsQuicFinalizerAtExitRegistered;
static _Thread_local trevrpc_msquic_conn* TrevMsQuicObserverConn;
static _Thread_local trevrpc_msquic_stream* TrevMsQuicObserverStream;

static const QUIC_API_TABLE* trevrpc_msquic_api(void) {
    return atomic_load_explicit(&TrevMsQuic, memory_order_acquire);
}

#ifdef TREVRPC_MSQUIC_TESTING
static pthread_mutex_t TrevMsQuicTestStreamHookMutex = PTHREAD_MUTEX_INITIALIZER;
static trevrpc_msquic_test_stream_hook TrevMsQuicTestStreamHook;
static void* TrevMsQuicTestStreamHookContext;
static bool TrevMsQuicTestFailNextStreamSend;
static bool TrevMsQuicTestFailNextGracefulShutdown;
typedef enum trevrpc_msquic_test_receive_alloc_kind {
    TREV_MSQUIC_TEST_RECV_ALLOC_NONE = 0,
    TREV_MSQUIC_TEST_RECV_ALLOC_RAW_CHUNK,
    TREV_MSQUIC_TEST_RECV_ALLOC_PARSER_BODY,
    TREV_MSQUIC_TEST_RECV_ALLOC_FRAME_NODE,
} trevrpc_msquic_test_receive_alloc_kind;
static trevrpc_msquic_test_receive_alloc_kind TrevMsQuicTestFailNextReceiveAllocation;
struct trevrpc_msquic_test_receive_fixture {
    trevrpc_msquic_receive_budget* budget;
    trevrpc_msquic_stream** streams;
    size_t stream_count;
    bool connection_ref;
};
#endif

static QUIC_STATUS QUIC_API trevrpc_msquic_listener_callback(HQUIC listener, void* context, QUIC_LISTENER_EVENT* event);
static QUIC_STATUS QUIC_API trevrpc_msquic_conn_callback(HQUIC connection, void* context, QUIC_CONNECTION_EVENT* event);
static QUIC_STATUS QUIC_API trevrpc_msquic_stream_callback(HQUIC stream, void* context, QUIC_STREAM_EVENT* event);
static void trevrpc_msquic_stream_complete_close(trevrpc_msquic_stream* stream, HQUIC handle);
static void trevrpc_msquic_stream_destroy_owned(trevrpc_msquic_stream* stream, bool owns_close_call);
static void trevrpc_msquic_stream_try_destroy_deferred(trevrpc_msquic_stream* stream);
static void trevrpc_msquic_conn_destroy_owned(trevrpc_msquic_conn* conn, bool owns_close_call);
static int trevrpc_msquic_conn_finalizer_ensure_started(void);
static void trevrpc_msquic_stream_send_op_release(trevrpc_msquic_stream* stream);
static void trevrpc_msquic_receive_budget_kick(trevrpc_msquic_receive_budget* budget);
static int trevrpc_msquic_materialize_pending_frame_locked(trevrpc_msquic_stream* stream);
static void trevrpc_msquic_stream_resume_receive_if_ready(trevrpc_msquic_stream* stream);
static void trevrpc_msquic_stream_receive_progress(
    trevrpc_msquic_stream* stream, trevrpc_msquic_receive_budget* budget);

#ifdef TREVRPC_MSQUIC_TESTING
void trevrpc_msquic_test_set_stream_hook(trevrpc_msquic_test_stream_hook hook, void* context) {
    pthread_mutex_lock(&TrevMsQuicTestStreamHookMutex);
    TrevMsQuicTestStreamHook = hook;
    TrevMsQuicTestStreamHookContext = context;
    if (hook == NULL) {
        TrevMsQuicTestFailNextStreamSend = false;
        TrevMsQuicTestFailNextGracefulShutdown = false;
        TrevMsQuicTestFailNextReceiveAllocation = TREV_MSQUIC_TEST_RECV_ALLOC_NONE;
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

static void trevrpc_msquic_test_fail_next_receive_allocation(trevrpc_msquic_test_receive_alloc_kind kind) {
    pthread_mutex_lock(&TrevMsQuicTestStreamHookMutex);
    TrevMsQuicTestFailNextReceiveAllocation = kind;
    pthread_mutex_unlock(&TrevMsQuicTestStreamHookMutex);
}

void trevrpc_msquic_test_fail_next_receive_raw_allocation(void) {
    trevrpc_msquic_test_fail_next_receive_allocation(TREV_MSQUIC_TEST_RECV_ALLOC_RAW_CHUNK);
}

void trevrpc_msquic_test_fail_next_receive_parser_allocation(void) {
    trevrpc_msquic_test_fail_next_receive_allocation(TREV_MSQUIC_TEST_RECV_ALLOC_PARSER_BODY);
}

void trevrpc_msquic_test_fail_next_receive_frame_allocation(void) {
    trevrpc_msquic_test_fail_next_receive_allocation(TREV_MSQUIC_TEST_RECV_ALLOC_FRAME_NODE);
}

size_t trevrpc_msquic_test_api_leases(void) {
    pthread_mutex_lock(&TrevMsQuicApiMutex);
    size_t leases = TrevMsQuicApiLeases;
    pthread_mutex_unlock(&TrevMsQuicApiMutex);
    return leases;
}

static bool trevrpc_msquic_test_should_fail_receive_allocation(trevrpc_msquic_test_receive_alloc_kind kind) {
    pthread_mutex_lock(&TrevMsQuicTestStreamHookMutex);
    bool fail = TrevMsQuicTestFailNextReceiveAllocation == kind;
    if (fail) {
        TrevMsQuicTestFailNextReceiveAllocation = TREV_MSQUIC_TEST_RECV_ALLOC_NONE;
    }
    pthread_mutex_unlock(&TrevMsQuicTestStreamHookMutex);
    return fail;
}

int trevrpc_msquic_test_reliable_reset_negotiated(trevrpc_msquic_conn* conn) {
    trevrpc_msquic_feature_snapshot snapshot = {0};
    return trevrpc_msquic_conn_feature_snapshot(conn, &snapshot) == 0 &&
           snapshot.negotiated_reset_dialects != TREV_MSQUIC_RESET_DIALECTS_NONE;
}

void trevrpc_msquic_test_wait_stream_shutdown_complete(trevrpc_msquic_stream* stream) {
    pthread_mutex_lock(&stream->mutex);
    while (!stream->shutdown_complete) {
        pthread_cond_wait(&stream->cond, &stream->mutex);
    }
    pthread_mutex_unlock(&stream->mutex);
}

int trevrpc_msquic_test_inject_peer_send_aborted(trevrpc_msquic_stream* stream, uint64_t error_code) {
    if (stream == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&stream->mutex);
    HQUIC handle = stream->handle;
    pthread_mutex_unlock(&stream->mutex);
    QUIC_STREAM_EVENT event = {.Type = QUIC_STREAM_EVENT_PEER_SEND_ABORTED};
    event.PEER_SEND_ABORTED.ErrorCode = error_code;
    return (int)trevrpc_msquic_stream_callback(handle, stream, &event);
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
    return fail ? (QUIC_STATUS)EIO
                : trevrpc_msquic_api()->StreamSend(stream, buffers, buffer_count, flags, client_context);
}

static QUIC_STATUS trevrpc_msquic_test_stream_shutdown(
    HQUIC stream, QUIC_STREAM_SHUTDOWN_FLAGS flags, QUIC_UINT62 error_code) {
    trevrpc_msquic_test_stream_event event = TREV_MSQUIC_TEST_STREAM_EVENT_COUNT;
    if (flags == QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL) {
        event = TREV_MSQUIC_TEST_STREAM_SHUTDOWN_GRACEFUL;
    } else if (flags == QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE) {
        event = TREV_MSQUIC_TEST_STREAM_SHUTDOWN_ABORT_RECEIVE;
    } else if (flags == QUIC_STREAM_SHUTDOWN_FLAG_ABORT) {
        event = TREV_MSQUIC_TEST_STREAM_SHUTDOWN_ABORT;
    }
    if (event != TREV_MSQUIC_TEST_STREAM_EVENT_COUNT) {
        trevrpc_msquic_test_emit_stream_event(event);
    }

    bool fail = false;
    if (flags == QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL) {
        pthread_mutex_lock(&TrevMsQuicTestStreamHookMutex);
        fail = TrevMsQuicTestFailNextGracefulShutdown;
        TrevMsQuicTestFailNextGracefulShutdown = false;
        pthread_mutex_unlock(&TrevMsQuicTestStreamHookMutex);
    }
    return fail ? (QUIC_STATUS)EIO : trevrpc_msquic_api()->StreamShutdown(stream, flags, error_code);
}
#else
#define TREV_MSQUIC_TEST_RECV_ALLOC_RAW_CHUNK 1
#define TREV_MSQUIC_TEST_RECV_ALLOC_PARSER_BODY 2
#define TREV_MSQUIC_TEST_RECV_ALLOC_FRAME_NODE 3
#define trevrpc_msquic_test_should_fail_receive_allocation(kind) false
#define trevrpc_msquic_test_emit_stream_event(event) ((void)0)
#define trevrpc_msquic_test_stream_send(stream, buffers, buffer_count, flags, client_context)                          \
    trevrpc_msquic_api()->StreamSend(stream, buffers, buffer_count, flags, client_context)
#define trevrpc_msquic_test_stream_shutdown(stream, flags, error_code)                                                 \
    trevrpc_msquic_api()->StreamShutdown(stream, flags, error_code)
#endif

static int trevrpc_msquic_api_acquire(void) {
    int result = trevrpc_msquic_conn_finalizer_ensure_started();
    if (result != 0) {
        return result;
    }

    const QUIC_API_TABLE* api = NULL;
    result = trevrpc_msquic_api_owner_acquire(&api);
    if (result != 0) {
        return result;
    }
    pthread_mutex_lock(&TrevMsQuicApiMutex);
    const QUIC_API_TABLE* current = atomic_load_explicit(&TrevMsQuic, memory_order_relaxed);
    if (TrevMsQuicApiLeases == 0) {
        atomic_store_explicit(&TrevMsQuic, api, memory_order_release);
    } else if (current != api) {
        result = -EIO;
    }
    if (result == 0) {
        TrevMsQuicApiLeases++;
    }
    pthread_mutex_unlock(&TrevMsQuicApiMutex);
    if (result != 0) {
        trevrpc_msquic_api_owner_release(api);
    }
    return result;
}

static bool trevrpc_msquic_api_retain_open(void) {
    return trevrpc_msquic_api_acquire() == 0;
}

static void trevrpc_msquic_api_release(void) {
    const QUIC_API_TABLE* api;
    pthread_mutex_lock(&TrevMsQuicApiMutex);
    api = atomic_load_explicit(&TrevMsQuic, memory_order_acquire);
    if (TrevMsQuicApiLeases > 0) {
        TrevMsQuicApiLeases--;
        if (TrevMsQuicApiLeases == 0) {
            atomic_store_explicit(&TrevMsQuic, NULL, memory_order_release);
        }
    } else {
        api = NULL;
    }
    pthread_mutex_unlock(&TrevMsQuicApiMutex);
    trevrpc_msquic_api_owner_release(api);
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
    bool terminal = event != NULL && event->kind == TREV_MSQUIC_CONN_EVENT_SHUTDOWN_COMPLETE;
    if (!conn->observer_terminal || terminal) {
        if (terminal) {
            conn->observer_terminal = true;
        }
        observer = conn->observer;
        observer_context = conn->observer_context;
        if (observer != NULL) {
            conn->active_observer_callbacks++;
        }
    }
    pthread_mutex_unlock(&conn->mutex);

    if (observer == NULL) {
        return;
    }
    trevrpc_msquic_conn* previous = TrevMsQuicObserverConn;
    TrevMsQuicObserverConn = conn;
    observer(observer_context, event);
    TrevMsQuicObserverConn = previous;

    pthread_mutex_lock(&conn->mutex);
    if (conn->active_observer_callbacks > 0) {
        conn->active_observer_callbacks--;
    }
    pthread_cond_broadcast(&conn->cond);
    pthread_mutex_unlock(&conn->mutex);
}

static bool trevrpc_msquic_conn_commit_ready_locked(trevrpc_msquic_conn* conn) {
    if (conn->connected || !trevrpc_msquic_feature_ready(&conn->features)) {
        return false;
    }
    conn->connected = true;
    pthread_cond_broadcast(&conn->cond);
    return true;
}

static void trevrpc_msquic_conn_publish_ready(trevrpc_msquic_conn* conn) {
    bool publish = false;
    bool resumed = false;
    pthread_mutex_lock(&conn->mutex);
    publish = trevrpc_msquic_conn_commit_ready_locked(conn);
    resumed = conn->connected_session_resumed;
    pthread_mutex_unlock(&conn->mutex);
    if (publish) {
        trevrpc_msquic_conn_event connected_event = {
            .kind = TREV_MSQUIC_CONN_EVENT_CONNECTED,
            .session_resumed = resumed,
        };
        trevrpc_msquic_conn_notify(conn, &connected_event);
    }
}

static uint32_t trevrpc_msquic_stream_observer_flags_locked(const trevrpc_msquic_stream* stream) {
    uint32_t flags = 0;
    if (stream->recv_head != NULL || stream->frame_head != NULL) {
        flags |= TREV_MSQUIC_STREAM_OBSERVER_READABLE;
    }
    if (stream->recv_fin || stream->closed || stream->shutdown_complete || stream->err != 0) {
        flags |= TREV_MSQUIC_STREAM_OBSERVER_TERMINAL;
    }
    return flags;
}

static int trevrpc_msquic_stream_api_lifecycle_acquire(trevrpc_msquic_stream* stream) {
    if (stream == NULL) {
        return -EINVAL;
    }
    int result = 0;
    pthread_mutex_lock(&stream->mutex);
    if (stream->api_closing) {
        result = -ECANCELED;
    } else if (stream->active_lifecycle_refs == SIZE_MAX) {
        result = -EOVERFLOW;
    } else {
        stream->active_lifecycle_refs++;
    }
    pthread_mutex_unlock(&stream->mutex);
    return result;
}

static void trevrpc_msquic_stream_lifecycle_retain(trevrpc_msquic_stream* stream) {
    pthread_mutex_lock(&stream->mutex);
    stream->active_lifecycle_refs++;
    pthread_mutex_unlock(&stream->mutex);
}

static void trevrpc_msquic_stream_lifecycle_release(trevrpc_msquic_stream* stream) {
    bool destroy = false;
    pthread_mutex_lock(&stream->mutex);
    if (stream->active_lifecycle_refs > 0) {
        stream->active_lifecycle_refs--;
    }
    if (stream->destroy_requested && !stream->destroy_started && stream->active_observer_callbacks == 0 &&
        stream->active_lifecycle_refs == 0 && stream->active_send_ops == 0 && stream->active_handle_ops == 0 &&
        stream->active_send_completions == 0 && stream->active_close_calls == 0 &&
        atomic_load_explicit(&stream->active_resume_pins, memory_order_acquire) == 0 &&
        stream->pending_send_count == 0 && stream->send_capacity_waiters == 0 && stream->shutdown_complete &&
        stream->handle == NULL && !stream->close_pending) {
        stream->destroy_started = true;
        destroy = true;
    }
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);
    if (destroy) {
        trevrpc_msquic_stream_destroy_owned(stream, false);
    }
}

static void* trevrpc_msquic_conn_finalizer_worker(void* context) {
    (void)context;
    for (;;) {
        pthread_mutex_lock(&TrevMsQuicFinalizerMutex);
        while (TrevMsQuicFinalizerHead == NULL && !TrevMsQuicFinalizerStopping) {
            pthread_cond_wait(&TrevMsQuicFinalizerCond, &TrevMsQuicFinalizerMutex);
        }
        if (TrevMsQuicFinalizerHead == NULL) {
            pthread_mutex_unlock(&TrevMsQuicFinalizerMutex);
            return NULL;
        }
        trevrpc_msquic_conn* conn = TrevMsQuicFinalizerHead;
        TrevMsQuicFinalizerHead = conn->destroy_next;
        if (TrevMsQuicFinalizerHead == NULL) {
            TrevMsQuicFinalizerTail = NULL;
        }
        conn->destroy_next = NULL;
        pthread_mutex_unlock(&TrevMsQuicFinalizerMutex);

        trevrpc_msquic_conn_destroy_owned(conn, false);
    }
}

static void trevrpc_msquic_conn_finalizer_shutdown(void) {
    pthread_mutex_lock(&TrevMsQuicFinalizerMutex);
    bool join = TrevMsQuicFinalizerStarted;
    TrevMsQuicFinalizerStopping = true;
    pthread_cond_broadcast(&TrevMsQuicFinalizerCond);
    pthread_mutex_unlock(&TrevMsQuicFinalizerMutex);

    if (join) {
        (void)pthread_join(TrevMsQuicFinalizerThread, NULL);
    }
}

static int trevrpc_msquic_conn_finalizer_ensure_started(void) {
    int err = 0;
    pthread_mutex_lock(&TrevMsQuicFinalizerMutex);
    if (!TrevMsQuicFinalizerAtExitRegistered) {
        if (atexit(trevrpc_msquic_conn_finalizer_shutdown) != 0) {
            err = ENOMEM;
        } else {
            TrevMsQuicFinalizerAtExitRegistered = true;
        }
    }
    if (err == 0 && !TrevMsQuicFinalizerStarted) {
        err = pthread_create(&TrevMsQuicFinalizerThread, NULL, trevrpc_msquic_conn_finalizer_worker, NULL);
        if (err == 0) {
            TrevMsQuicFinalizerStarted = true;
        }
    }
    pthread_mutex_unlock(&TrevMsQuicFinalizerMutex);
    return err == 0 ? 0 : -err;
}

static void trevrpc_msquic_conn_schedule_destroy(trevrpc_msquic_conn* conn) {
    pthread_mutex_lock(&TrevMsQuicFinalizerMutex);
    conn->destroy_next = NULL;
    if (TrevMsQuicFinalizerTail != NULL) {
        TrevMsQuicFinalizerTail->destroy_next = conn;
    } else {
        TrevMsQuicFinalizerHead = conn;
    }
    TrevMsQuicFinalizerTail = conn;
    pthread_cond_signal(&TrevMsQuicFinalizerCond);
    pthread_mutex_unlock(&TrevMsQuicFinalizerMutex);
}

static void trevrpc_msquic_conn_lifecycle_retain(trevrpc_msquic_conn* conn) {
    pthread_mutex_lock(&conn->mutex);
    conn->active_lifecycle_refs++;
    pthread_mutex_unlock(&conn->mutex);
}

static void trevrpc_msquic_conn_lifecycle_release(trevrpc_msquic_conn* conn) {
    bool destroy = false;
    pthread_mutex_lock(&conn->mutex);
    if (conn->active_lifecycle_refs > 0) {
        conn->active_lifecycle_refs--;
    }
    if (conn->destroy_requested && !conn->destroy_started && conn->active_observer_callbacks == 0 &&
        conn->active_lifecycle_refs == 0 && conn->active_handle_ops == 0 && conn->active_close_calls == 0 &&
        conn->shutdown_complete && conn->handle == NULL && !conn->close_pending) {
        conn->destroy_started = true;
        destroy = true;
    }
    pthread_cond_broadcast(&conn->cond);
    pthread_mutex_unlock(&conn->mutex);
    if (destroy) {
        trevrpc_msquic_conn_schedule_destroy(conn);
    }
}

static void trevrpc_msquic_stream_observer_finish(trevrpc_msquic_stream* stream) {
    pthread_mutex_lock(&stream->mutex);
    if (stream->active_observer_callbacks > 0) {
        stream->active_observer_callbacks--;
    }
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);
}

static void trevrpc_msquic_stream_notify(trevrpc_msquic_stream* stream, uint32_t flags) {
    trevrpc_msquic_stream_observer observer = NULL;
    void* observer_context = NULL;
    pthread_mutex_lock(&stream->mutex);
    observer = stream->observer;
    observer_context = stream->observer_context;
    if (observer != NULL) {
        stream->active_observer_callbacks++;
    }
    pthread_mutex_unlock(&stream->mutex);

    if (observer == NULL) {
        return;
    }
    trevrpc_msquic_stream* previous = TrevMsQuicObserverStream;
    TrevMsQuicObserverStream = stream;
    observer(observer_context, flags);
    TrevMsQuicObserverStream = previous;
    trevrpc_msquic_stream_observer_finish(stream);
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

static bool trevrpc_msquic_checked_add(size_t left, size_t right, size_t* out) {
    if (right > SIZE_MAX - left) {
        return false;
    }
    *out = left + right;
    return true;
}

static bool trevrpc_msquic_receive_minimum_bytes_checked(size_t max_frame_size, size_t* out) {
    size_t required = sizeof(trevrpc_msquic_chunk);
    if (!trevrpc_msquic_checked_add(required, sizeof(uint32_t), &required) ||
        !trevrpc_msquic_checked_add(required, max_frame_size, &required) ||
        !trevrpc_msquic_checked_add(required, max_frame_size, &required) ||
        !trevrpc_msquic_checked_add(required, sizeof(trevrpc_msquic_frame), &required)) {
        return false;
    }
    *out = required;
    return true;
}

static int trevrpc_msquic_receive_policy_effective(
    const trevrpc_msquic_receive_policy* configured, size_t max_frame_size, trevrpc_msquic_receive_policy* effective) {
    bool explicit_stream_bytes = configured != NULL && configured->max_stream_owned_bytes != 0;
    bool explicit_connection_bytes = configured != NULL && configured->max_connection_owned_bytes != 0;
    effective->max_stream_owned_bytes =
        explicit_stream_bytes ? configured->max_stream_owned_bytes : TREV_MSQUIC_DEFAULT_STREAM_RECV_BYTES;
    effective->max_stream_owned_count = configured != NULL && configured->max_stream_owned_count != 0
                                            ? configured->max_stream_owned_count
                                            : TREV_MSQUIC_DEFAULT_STREAM_RECV_COUNT;
    effective->max_connection_owned_bytes =
        explicit_connection_bytes ? configured->max_connection_owned_bytes : TREV_MSQUIC_DEFAULT_CONNECTION_RECV_BYTES;
    effective->max_connection_owned_count = configured != NULL && configured->max_connection_owned_count != 0
                                                ? configured->max_connection_owned_count
                                                : TREV_MSQUIC_DEFAULT_CONNECTION_RECV_COUNT;

    size_t required = 0;
    if (!trevrpc_msquic_receive_minimum_bytes_checked(max_frame_size, &required)) {
        return EOVERFLOW;
    }
    if (effective->max_stream_owned_bytes < required) {
        if (explicit_stream_bytes) {
            return EINVAL;
        }
        effective->max_stream_owned_bytes = required;
    }
    if (effective->max_connection_owned_bytes < required) {
        if (explicit_connection_bytes) {
            return EINVAL;
        }
        effective->max_connection_owned_bytes = required;
    }
    if (effective->max_stream_owned_count < 3 || effective->max_connection_owned_count < 3 ||
        effective->max_stream_owned_bytes > effective->max_connection_owned_bytes ||
        effective->max_stream_owned_count > effective->max_connection_owned_count) {
        return EINVAL;
    }
    return 0;
}

static trevrpc_msquic_receive_budget* trevrpc_msquic_receive_budget_new(
    const trevrpc_msquic_receive_policy* policy, size_t max_frame_size) {
    trevrpc_msquic_receive_budget* budget = calloc(1, sizeof(*budget));
    if (budget == NULL) {
        return NULL;
    }
    if (pthread_mutex_init(&budget->mutex, NULL) != 0) {
        free(budget);
        return NULL;
    }
    if (pthread_cond_init(&budget->cond, NULL) != 0) {
        pthread_mutex_destroy(&budget->mutex);
        free(budget);
        return NULL;
    }
    budget->ref_count = 1;
    budget->max_owned_bytes = policy->max_connection_owned_bytes;
    budget->max_owned_count = policy->max_connection_owned_count;
    size_t conversion_bytes = 0;
    if (!trevrpc_msquic_checked_add(max_frame_size, sizeof(trevrpc_msquic_frame), &conversion_bytes) ||
        conversion_bytes > budget->max_owned_bytes || budget->max_owned_count < 2) {
        pthread_cond_destroy(&budget->cond);
        pthread_mutex_destroy(&budget->mutex);
        free(budget);
        return NULL;
    }
    budget->undecided_admission_max_bytes = budget->max_owned_bytes - conversion_bytes;
    budget->undecided_admission_max_count = budget->max_owned_count - 2;
    return budget;
}

static bool trevrpc_msquic_receive_budget_retain(trevrpc_msquic_receive_budget* budget) {
    bool retained = false;
    pthread_mutex_lock(&budget->mutex);
    if (budget->ref_count != SIZE_MAX) {
        budget->ref_count++;
        retained = true;
    }
    pthread_mutex_unlock(&budget->mutex);
    return retained;
}

static void trevrpc_msquic_receive_budget_release(trevrpc_msquic_receive_budget* budget) {
    if (budget == NULL) {
        return;
    }
    bool destroy = false;
    pthread_mutex_lock(&budget->mutex);
    if (budget->ref_count > 0) {
        budget->ref_count--;
        destroy = budget->ref_count == 0;
    }
    pthread_mutex_unlock(&budget->mutex);
    if (destroy) {
        pthread_cond_destroy(&budget->cond);
        pthread_mutex_destroy(&budget->mutex);
        free(budget);
    }
}

static bool trevrpc_msquic_recv_local_fits_locked(
    const trevrpc_msquic_stream* stream, size_t bytes, size_t count, bool undecided) {
    size_t max_bytes = stream->max_recv_owned_bytes;
    size_t max_count = stream->max_recv_owned_count;
    if (undecided) {
        size_t conversion_bytes = 0;
        if (!trevrpc_msquic_checked_add(
                stream->configured_max_frame_size, sizeof(trevrpc_msquic_frame), &conversion_bytes) ||
            conversion_bytes > max_bytes || max_count < 2) {
            return false;
        }
        max_bytes -= conversion_bytes;
        max_count -= 2;
    }
    return stream->recv_owned_bytes <= max_bytes && bytes <= max_bytes - stream->recv_owned_bytes &&
           stream->recv_owned_count <= max_count && count <= max_count - stream->recv_owned_count;
}

static trevrpc_msquic_reserve_result trevrpc_msquic_recv_try_reserve_locked(trevrpc_msquic_stream* stream,
    size_t bytes,
    size_t count,
    trevrpc_msquic_receive_pause_kind kind,
    bool use_undecided_admission_cap) {
    trevrpc_msquic_receive_budget* budget = stream->recv_budget;
    if (stream->recv_grant_kind == kind && stream->recv_grant_bytes == bytes && stream->recv_grant_count == count) {
        stream->recv_grant_kind = TREV_MSQUIC_RECV_PAUSE_NONE;
        stream->recv_grant_bytes = 0;
        stream->recv_grant_count = 0;
        return TREV_MSQUIC_RESERVE_OK;
    }
    if (!trevrpc_msquic_recv_local_fits_locked(stream, bytes, count, use_undecided_admission_cap)) {
        return TREV_MSQUIC_RESERVE_PRESSURE;
    }
    pthread_mutex_lock(&budget->mutex);
    size_t max_bytes = use_undecided_admission_cap ? budget->undecided_admission_max_bytes : budget->max_owned_bytes;
    size_t max_count = use_undecided_admission_cap ? budget->undecided_admission_max_count : budget->max_owned_count;
    bool fits = budget->owned_bytes <= max_bytes && bytes <= max_bytes - budget->owned_bytes &&
                budget->owned_count <= max_count && count <= max_count - budget->owned_count;
    if (fits) {
        budget->owned_bytes += bytes;
        budget->owned_count += count;
        stream->recv_owned_bytes += bytes;
        stream->recv_owned_count += count;
    }
    pthread_mutex_unlock(&budget->mutex);
    return fits ? TREV_MSQUIC_RESERVE_OK : TREV_MSQUIC_RESERVE_PRESSURE;
}

static void trevrpc_msquic_recv_release_locked(trevrpc_msquic_stream* stream, size_t bytes, size_t count) {
    if (bytes > stream->recv_owned_bytes || count > stream->recv_owned_count) {
        abort();
    }
    stream->recv_owned_bytes -= bytes;
    stream->recv_owned_count -= count;
    trevrpc_msquic_receive_budget* budget = stream->recv_budget;
    pthread_mutex_lock(&budget->mutex);
    budget->owned_bytes -= bytes;
    budget->owned_count -= count;
    budget->kick_pending = true;
    pthread_mutex_unlock(&budget->mutex);
}

static void trevrpc_msquic_recv_pause_remove_budget_locked(
    trevrpc_msquic_receive_budget* budget, trevrpc_msquic_stream* stream) {
    if (!stream->pause_listed) {
        return;
    }
    if (stream->pause_prev != NULL) {
        stream->pause_prev->pause_next = stream->pause_next;
    } else {
        budget->paused_head = stream->pause_next;
    }
    if (stream->pause_next != NULL) {
        stream->pause_next->pause_prev = stream->pause_prev;
    } else {
        budget->paused_tail = stream->pause_prev;
    }
    stream->pause_prev = NULL;
    stream->pause_next = NULL;
    stream->pause_listed = false;
}

static void trevrpc_msquic_recv_pause_locked(trevrpc_msquic_stream* stream,
    trevrpc_msquic_receive_pause_kind kind,
    size_t needed_bytes,
    size_t needed_count,
    bool receive_disabled) {
    stream->recv_pause_kind = kind;
    stream->recv_pause_need_bytes = needed_bytes;
    stream->recv_pause_need_count = needed_count;
    stream->receive_disabled = stream->receive_disabled || receive_disabled;
    trevrpc_msquic_receive_budget* budget = stream->recv_budget;
    pthread_mutex_lock(&budget->mutex);
    if (!stream->receive_closing && !stream->pause_listed) {
        stream->pause_prev = budget->paused_tail;
        stream->pause_next = NULL;
        if (budget->paused_tail != NULL) {
            budget->paused_tail->pause_next = stream;
        } else {
            budget->paused_head = stream;
        }
        budget->paused_tail = stream;
        stream->pause_listed = true;
    }
    pthread_mutex_unlock(&budget->mutex);
}

static void trevrpc_msquic_recv_unpause_locked(trevrpc_msquic_stream* stream) {
    trevrpc_msquic_receive_budget* budget = stream->recv_budget;
    pthread_mutex_lock(&budget->mutex);
    trevrpc_msquic_recv_pause_remove_budget_locked(budget, stream);
    pthread_mutex_unlock(&budget->mutex);
    stream->recv_pause_kind = TREV_MSQUIC_RECV_PAUSE_NONE;
    stream->recv_pause_need_bytes = 0;
    stream->recv_pause_need_count = 0;
}

static void trevrpc_msquic_recv_release_grant_locked(trevrpc_msquic_stream* stream) {
    if (stream->recv_grant_kind == TREV_MSQUIC_RECV_PAUSE_NONE) {
        return;
    }
    size_t bytes = stream->recv_grant_bytes;
    size_t count = stream->recv_grant_count;
    stream->recv_grant_kind = TREV_MSQUIC_RECV_PAUSE_NONE;
    stream->recv_grant_bytes = 0;
    stream->recv_grant_count = 0;
    trevrpc_msquic_recv_release_locked(stream, bytes, count);
}

static void* trevrpc_msquic_parser_alloc(size_t size, void* context) {
    trevrpc_msquic_stream* stream = context;
    stream->parser_alloc_result = TREV_MSQUIC_PARSER_ALLOC_NONE;
    trevrpc_msquic_reserve_result reserved =
        trevrpc_msquic_recv_try_reserve_locked(stream, size, 1, TREV_MSQUIC_RECV_PAUSE_PARSER_BODY, false);
    if (reserved == TREV_MSQUIC_RESERVE_PRESSURE) {
        stream->parser_alloc_result = TREV_MSQUIC_PARSER_ALLOC_PRESSURE;
        return NULL;
    }
    if (reserved != TREV_MSQUIC_RESERVE_OK) {
        stream->parser_alloc_result = TREV_MSQUIC_PARSER_ALLOC_OOM;
        return NULL;
    }
    void* body = trevrpc_msquic_test_should_fail_receive_allocation(TREV_MSQUIC_TEST_RECV_ALLOC_PARSER_BODY)
                     ? NULL
                     : malloc(size);
    if (body == NULL) {
        trevrpc_msquic_recv_release_locked(stream, size, 1);
        stream->parser_alloc_result = TREV_MSQUIC_PARSER_ALLOC_OOM;
        return NULL;
    }
    stream->parser_owned_bytes = size;
    stream->parser_owned_count = 1;
    return body;
}

static void trevrpc_msquic_parser_free(void* ptr, void* context) {
    trevrpc_msquic_stream* stream = context;
    free(ptr);
    size_t bytes = stream->parser_owned_bytes;
    size_t count = stream->parser_owned_count;
    stream->parser_owned_bytes = 0;
    stream->parser_owned_count = 0;
    if (bytes != 0 || count != 0) {
        trevrpc_msquic_recv_release_locked(stream, bytes, count);
    }
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
    trevrpc_msquic_api()->StreamClose(handle);

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

    trevrpc_msquic_stream_notify(stream, TREV_MSQUIC_STREAM_OBSERVER_TERMINAL);
    if (close_handle != NULL) {
        trevrpc_msquic_stream_complete_close(stream, close_handle);
    }
}

static void trevrpc_msquic_conn_complete_close(trevrpc_msquic_conn* conn, HQUIC handle) {
    trevrpc_msquic_api()->ConnectionClose(handle);

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

static trevrpc_msquic_stream* trevrpc_msquic_stream_alloc(HQUIC handle,
    size_t max_pending_send_bytes,
    size_t max_pending_send_count,
    size_t configured_max_frame_size,
    size_t initial_frame_max_len,
    size_t max_recv_owned_bytes,
    size_t max_recv_owned_count,
    trevrpc_msquic_receive_budget* budget) {
    trevrpc_msquic_stream* stream = calloc(1, sizeof(*stream));
    if (stream == NULL || !trevrpc_msquic_receive_budget_retain(budget)) {
        free(stream);
        return NULL;
    }
    atomic_init(&stream->receive_closing, false);
    atomic_init(&stream->active_resume_pins, 0);
    if (!trevrpc_msquic_api_retain_open()) {
        trevrpc_msquic_receive_budget_release(budget);
        free(stream);
        return NULL;
    }
    stream->api_ref_acquired = true;
    stream->handle = handle;
    stream->recv_budget = budget;
    stream->configured_max_frame_size = configured_max_frame_size;
    stream->max_recv_owned_bytes = max_recv_owned_bytes;
    stream->max_recv_owned_count = max_recv_owned_count;
    stream->parser_budgeted = true;
    stream->max_pending_send_bytes = trevrpc_msquic_effective_max_pending_send_bytes(max_pending_send_bytes);
    stream->max_pending_send_count = trevrpc_msquic_effective_max_pending_send_count(max_pending_send_count);
    trevrpc_owned_bytes_init(&stream->pending_frame.body);
    trevrpc_frame_parser_init_with_allocator(
        &stream->frame_parser, initial_frame_max_len, trevrpc_msquic_parser_alloc, trevrpc_msquic_parser_free, stream);
    trevrpc_frame_parser_set_retain_on_allocation_failure(&stream->frame_parser, true);
    if (initial_frame_max_len > 0) {
        stream->recv_mode = TREV_MSQUIC_RECV_FRAMES;
    }
    pthread_mutex_init(&stream->mutex, NULL);
    pthread_cond_init(&stream->cond, NULL);
    return stream;
}

static trevrpc_msquic_conn* trevrpc_msquic_conn_alloc(
    HQUIC handle, size_t max_frame_size, const trevrpc_msquic_receive_policy* receive_policy) {
    trevrpc_msquic_conn* conn = calloc(1, sizeof(*conn));
    if (conn == NULL) {
        return NULL;
    }
    conn->recv_budget = trevrpc_msquic_receive_budget_new(receive_policy, max_frame_size);
    if (conn->recv_budget == NULL || !trevrpc_msquic_api_retain_open()) {
        trevrpc_msquic_receive_budget_release(conn->recv_budget);
        free(conn);
        return NULL;
    }
    conn->api_ref_acquired = true;
    conn->handle = handle;
    conn->max_frame_size = max_frame_size;
    conn->max_stream_recv_owned_bytes = receive_policy->max_stream_owned_bytes;
    conn->max_stream_recv_owned_count = receive_policy->max_stream_owned_count;
    conn->max_pending_send_bytes = trevrpc_msquic_effective_max_pending_send_bytes(0);
    conn->max_pending_send_count = trevrpc_msquic_effective_max_pending_send_count(0);
    trevrpc_msquic_feature_request generic = trevrpc_msquic_generic_feature_request();
    trevrpc_msquic_feature_state_init(&conn->features, &generic);
    pthread_mutex_init(&conn->mutex, NULL);
    pthread_cond_init(&conn->cond, NULL);
    return conn;
}

static void trevrpc_msquic_free_chunks(trevrpc_msquic_stream* stream) {
    trevrpc_msquic_chunk* chunk = stream->recv_head;
    while (chunk != NULL) {
        trevrpc_msquic_chunk* next = chunk->next;
        size_t bytes = chunk->charge_bytes;
        size_t count = chunk->charge_count;
        chunk->charge_bytes = 0;
        chunk->charge_count = 0;
        trevrpc_msquic_recv_release_locked(stream, bytes, count);
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
        size_t bytes = frame->charge_bytes;
        size_t count = frame->charge_count;
        frame->charge_bytes = 0;
        frame->charge_count = 0;
        trevrpc_msquic_recv_release_locked(stream, bytes, count);
        trevrpc_owned_bytes_reset(&frame->body);
        free(frame);
        frame = next;
    }
    stream->frame_head = NULL;
    stream->frame_tail = NULL;
    if (stream->pending_frame_valid) {
        stream->pending_frame_valid = false;
        trevrpc_msquic_recv_release_locked(
            stream, stream->pending_frame.charge_bytes, stream->pending_frame.charge_count);
        stream->pending_frame.charge_bytes = 0;
        stream->pending_frame.charge_count = 0;
        trevrpc_owned_bytes_reset(&stream->pending_frame.body);
    }
    trevrpc_frame_parser_reset(&stream->frame_parser);
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
    trevrpc_msquic_test_emit_stream_event(TREV_MSQUIC_TEST_STREAM_SEND_COMPLETE_ENTERED);

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

static void trevrpc_msquic_frame_link_locked(trevrpc_msquic_stream* stream, trevrpc_msquic_frame* frame) {
    frame->next = NULL;
    if (stream->frame_tail != NULL) {
        stream->frame_tail->next = frame;
    } else {
        stream->frame_head = frame;
    }
    stream->frame_tail = frame;
}

static int trevrpc_msquic_frame_enqueue_locked(trevrpc_msquic_stream* stream,
    trevrpc_owned_bytes* body,
    size_t declared_len,
    intptr_t err,
    size_t body_charge_bytes,
    size_t body_charge_count) {
    if (trevrpc_msquic_recv_try_reserve_locked(
            stream, sizeof(trevrpc_msquic_frame), 1, TREV_MSQUIC_RECV_PAUSE_FRAME_NODE, false) !=
        TREV_MSQUIC_RESERVE_OK) {
        stream->pending_frame_valid = true;
        trevrpc_owned_bytes_move(&stream->pending_frame.body, body);
        stream->pending_frame.declared_len = declared_len;
        stream->pending_frame.err = err;
        stream->pending_frame.charge_bytes = body_charge_bytes;
        stream->pending_frame.charge_count = body_charge_count;
        trevrpc_msquic_recv_pause_locked(
            stream, TREV_MSQUIC_RECV_PAUSE_FRAME_NODE, sizeof(trevrpc_msquic_frame), 1, false);
        return 1;
    }
    trevrpc_msquic_frame* frame =
        trevrpc_msquic_test_should_fail_receive_allocation(TREV_MSQUIC_TEST_RECV_ALLOC_FRAME_NODE)
            ? NULL
            : malloc(sizeof(*frame));
    if (frame == NULL) {
        trevrpc_msquic_recv_release_locked(stream, sizeof(*frame), 1);
        trevrpc_msquic_recv_release_locked(stream, body_charge_bytes, body_charge_count);
        trevrpc_owned_bytes_reset(body);
        return -ENOMEM;
    }
    trevrpc_owned_bytes_init(&frame->body);
    trevrpc_owned_bytes_move(&frame->body, body);
    frame->declared_len = declared_len;
    frame->err = err;
    frame->charge_bytes = sizeof(*frame) + body_charge_bytes;
    frame->charge_count = 1 + body_charge_count;
    trevrpc_msquic_frame_link_locked(stream, frame);
    return 0;
}

static int trevrpc_msquic_materialize_pending_frame_locked(trevrpc_msquic_stream* stream) {
    if (!stream->pending_frame_valid) {
        return 0;
    }
    if (trevrpc_msquic_recv_try_reserve_locked(
            stream, sizeof(trevrpc_msquic_frame), 1, TREV_MSQUIC_RECV_PAUSE_FRAME_NODE, false) !=
        TREV_MSQUIC_RESERVE_OK) {
        trevrpc_msquic_recv_pause_locked(
            stream, TREV_MSQUIC_RECV_PAUSE_FRAME_NODE, sizeof(trevrpc_msquic_frame), 1, false);
        return 1;
    }
    trevrpc_msquic_frame* frame =
        trevrpc_msquic_test_should_fail_receive_allocation(TREV_MSQUIC_TEST_RECV_ALLOC_FRAME_NODE)
            ? NULL
            : malloc(sizeof(*frame));
    if (frame == NULL) {
        trevrpc_msquic_recv_release_locked(stream, sizeof(*frame), 1);
        trevrpc_msquic_recv_release_locked(
            stream, stream->pending_frame.charge_bytes, stream->pending_frame.charge_count);
        trevrpc_owned_bytes_reset(&stream->pending_frame.body);
        stream->pending_frame_valid = false;
        stream->pending_frame.charge_bytes = 0;
        stream->pending_frame.charge_count = 0;
        return -ENOMEM;
    }
    trevrpc_owned_bytes_init(&frame->body);
    trevrpc_owned_bytes_move(&frame->body, &stream->pending_frame.body);
    frame->declared_len = stream->pending_frame.declared_len;
    frame->err = stream->pending_frame.err;
    frame->charge_bytes = sizeof(*frame) + stream->pending_frame.charge_bytes;
    frame->charge_count = 1 + stream->pending_frame.charge_count;
    stream->pending_frame_valid = false;
    stream->pending_frame.charge_bytes = 0;
    stream->pending_frame.charge_count = 0;
    trevrpc_msquic_recv_unpause_locked(stream);
    trevrpc_msquic_frame_link_locked(stream, frame);
    return 0;
}

static bool trevrpc_msquic_frame_parser_active(const trevrpc_msquic_stream* stream) {
    return stream->pending_frame_valid || trevrpc_frame_parser_finish(&stream->frame_parser) != TREVRPC_FRAME_CLEAN_EOF;
}

static int trevrpc_msquic_stream_append_frame_bytes_locked(
    trevrpc_msquic_stream* stream, const uint8_t* data, size_t len, size_t* out_consumed, bool stop_after_frame) {
    size_t offset = 0;
    size_t initial_frames = stream->frame_head == NULL ? 0 : 1;
    while (offset < len) {
        if (stream->pending_frame_valid) {
            int pending = trevrpc_msquic_materialize_pending_frame_locked(stream);
            if (pending != 0) {
                *out_consumed = offset;
                return pending;
            }
        }
        size_t consumed = 0;
        trevrpc_owned_bytes body;
        size_t declared_body_len = 0;
        stream->parser_alloc_result = TREV_MSQUIC_PARSER_ALLOC_NONE;
        trevrpc_frame_result result = trevrpc_frame_parser_consume_owned(
            &stream->frame_parser, data + offset, len - offset, &consumed, &body, &declared_body_len);
        offset += consumed;
        if (result == TREVRPC_FRAME_READY) {
            size_t body_charge_bytes = stream->parser_budgeted ? stream->parser_owned_bytes : 0;
            size_t body_charge_count = stream->parser_budgeted ? stream->parser_owned_count : 0;
            stream->parser_owned_bytes = 0;
            stream->parser_owned_count = 0;
            if (stream->parser_budgeted) {
                body.release = trevrpc_frame_default_free;
                body.release_context = NULL;
            }
            int queued =
                trevrpc_msquic_frame_enqueue_locked(stream, &body, body.len, 0, body_charge_bytes, body_charge_count);
            if (queued != 0) {
                *out_consumed = offset;
                return queued;
            }
            if (stop_after_frame && initial_frames == 0) {
                break;
            }
        } else if (result == TREVRPC_FRAME_TOO_LARGE) {
            int queued = trevrpc_msquic_frame_enqueue_locked(
                stream, &body, declared_body_len, TREV_MSQUIC_ERR_FRAME_TOO_LARGE, 0, 0);
            if (queued != 0) {
                *out_consumed = offset;
                return queued;
            }
            if (stop_after_frame && initial_frames == 0) {
                break;
            }
        } else if (result == TREVRPC_FRAME_ALLOCATION_FAILURE) {
            *out_consumed = offset;
            if (stream->parser_alloc_result == TREV_MSQUIC_PARSER_ALLOC_PRESSURE) {
                trevrpc_msquic_recv_pause_locked(
                    stream, TREV_MSQUIC_RECV_PAUSE_PARSER_BODY, stream->frame_parser.body_len, 1, false);
                return 1;
            }
            return -ENOMEM;
        } else if (result != TREVRPC_FRAME_NEED_MORE) {
            *out_consumed = offset;
            return -EINVAL;
        }
        if (consumed == 0 && result == TREVRPC_FRAME_NEED_MORE) {
            break;
        }
    }
    *out_consumed = offset;
    return 0;
}

static size_t trevrpc_msquic_stream_frame_max_locked(const trevrpc_msquic_stream* stream, size_t requested) {
    size_t result = requested < stream->configured_max_frame_size ? requested : stream->configured_max_frame_size;
    size_t stream_cap = stream->max_recv_owned_bytes - sizeof(trevrpc_msquic_frame);
    size_t connection_cap = stream->recv_budget->max_owned_bytes - sizeof(trevrpc_msquic_frame);
    if (result > stream_cap) {
        result = stream_cap;
    }
    if (result > connection_cap) {
        result = connection_cap;
    }
    return result;
}

static int trevrpc_msquic_stream_pump_raw_locked(trevrpc_msquic_stream* stream) {
    while (stream->recv_head != NULL && stream->frame_head == NULL) {
        trevrpc_msquic_chunk* chunk = stream->recv_head;
        size_t available = chunk->len - chunk->offset;
        size_t consumed = 0;
        int result = trevrpc_msquic_stream_append_frame_bytes_locked(
            stream, chunk->data + chunk->offset, available, &consumed, true);
        if (consumed > 0) {
            chunk->offset += consumed;
            stream->recv_buffered -= consumed;
            chunk->charge_bytes -= consumed;
            trevrpc_msquic_recv_release_locked(stream, consumed, 0);
        }
        if (chunk->offset == chunk->len) {
            stream->recv_head = chunk->next;
            if (stream->recv_head == NULL) {
                stream->recv_tail = NULL;
            }
            trevrpc_msquic_recv_release_locked(stream, chunk->charge_bytes, chunk->charge_count);
            chunk->charge_bytes = 0;
            chunk->charge_count = 0;
            free(chunk);
        }
        if (result != 0 || stream->frame_head != NULL) {
            return result;
        }
        if (consumed == 0) {
            break;
        }
    }
    return 0;
}

static int trevrpc_msquic_stream_enable_frame_mode_locked(trevrpc_msquic_stream* stream, size_t max_len) {
    if (stream->recv_mode == TREV_MSQUIC_RECV_BYTES) {
        return TREV_MSQUIC_ERR_CLOSED;
    }
    if (stream->recv_mode == TREV_MSQUIC_RECV_UNDECIDED) {
        bool raw_pause = stream->recv_pause_kind == TREV_MSQUIC_RECV_PAUSE_RAW_CHUNK;
        bool raw_grant = stream->recv_grant_kind == TREV_MSQUIC_RECV_PAUSE_RAW_CHUNK;
        if (raw_pause) {
            trevrpc_msquic_recv_unpause_locked(stream);
        }
        if (raw_grant) {
            trevrpc_msquic_recv_release_grant_locked(stream);
        }
    }
    stream->recv_mode = TREV_MSQUIC_RECV_FRAMES;
    trevrpc_frame_parser_set_max_body_len(
        &stream->frame_parser, trevrpc_msquic_stream_frame_max_locked(stream, max_len));
    int pumped = trevrpc_msquic_stream_pump_raw_locked(stream);
    if (pumped < 0) {
        stream->err = -pumped;
        trevrpc_msquic_free_chunks(stream);
        return pumped;
    }
    if (stream->receive_disabled && stream->recv_pause_kind == TREV_MSQUIC_RECV_PAUSE_NONE &&
        !stream->pending_frame_valid) {
        stream->receive_resume_ready = true;
    }
    return 0;
}

static bool trevrpc_msquic_stream_update_raw_resume_locked(trevrpc_msquic_stream* stream) {
    if (stream->receive_waiting_on_raw_pump && stream->recv_head == NULL &&
        stream->recv_pause_kind == TREV_MSQUIC_RECV_PAUSE_NONE && !stream->pending_frame_valid) {
        stream->receive_waiting_on_raw_pump = false;
        stream->receive_resume_ready = stream->receive_disabled;
    }
    return stream->receive_resume_ready;
}

static intptr_t trevrpc_msquic_stream_wait_frame_locked(
    trevrpc_msquic_stream* stream, const struct timespec* deadline) {
    for (;;) {
        int pumped = trevrpc_msquic_stream_pump_raw_locked(stream);
        if (pumped < 0) {
            stream->err = -pumped;
            trevrpc_msquic_free_chunks(stream);
        }
        if (stream->frame_head == NULL && trevrpc_msquic_stream_update_raw_resume_locked(stream)) {
            pthread_mutex_unlock(&stream->mutex);
            trevrpc_msquic_stream_resume_receive_if_ready(stream);
            pthread_mutex_lock(&stream->mutex);
        }
        if (stream->frame_head != NULL || stream->err != 0 ||
            ((stream->recv_fin || stream->closed) && !stream->pending_frame_valid && stream->recv_head == NULL)) {
            break;
        }
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
    int pumped = trevrpc_msquic_stream_pump_raw_locked(stream);
    if (pumped < 0) {
        stream->err = -pumped;
        trevrpc_msquic_free_chunks(stream);
    }
    (void)trevrpc_msquic_stream_update_raw_resume_locked(stream);
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
        trevrpc_msquic_recv_release_locked(stream, frame->charge_bytes, frame->charge_count);
        frame->charge_bytes = 0;
        frame->charge_count = 0;
        if (stream->receive_waiting_on_raw_pump && stream->frame_head == NULL) {
            int pumped = trevrpc_msquic_stream_pump_raw_locked(stream);
            if (pumped < 0) {
                stream->err = -pumped;
                trevrpc_msquic_free_chunks(stream);
            }
            if (stream->recv_head == NULL && stream->recv_pause_kind == TREV_MSQUIC_RECV_PAUSE_NONE &&
                !stream->pending_frame_valid) {
                stream->receive_waiting_on_raw_pump = false;
                stream->receive_resume_ready = stream->receive_disabled;
            }
        }
    }
    return frame;
}

static bool trevrpc_msquic_budget_aggregate_fits_locked(
    const trevrpc_msquic_receive_budget* budget, const trevrpc_msquic_stream* stream, size_t bytes, size_t count) {
    bool undecided =
        stream->recv_mode == TREV_MSQUIC_RECV_UNDECIDED && stream->recv_pause_kind == TREV_MSQUIC_RECV_PAUSE_RAW_CHUNK;
    size_t max_bytes = undecided ? budget->undecided_admission_max_bytes : budget->max_owned_bytes;
    size_t max_count = undecided ? budget->undecided_admission_max_count : budget->max_owned_count;
    return budget->owned_bytes <= max_bytes && bytes <= max_bytes - budget->owned_bytes &&
           budget->owned_count <= max_count && count <= max_count - budget->owned_count;
}

static void trevrpc_msquic_receive_budget_kick(trevrpc_msquic_receive_budget* budget) {
    if (budget == NULL) {
        return;
    }
    pthread_mutex_lock(&budget->mutex);
    if (!budget->kick_pending) {
        pthread_mutex_unlock(&budget->mutex);
        return;
    }
    if (budget->resumer_active) {
        pthread_mutex_unlock(&budget->mutex);
        return;
    }
    budget->resumer_active = true;
    for (;;) {
        budget->kick_pending = false;
        trevrpc_msquic_stream* selected = budget->paused_head;
#ifdef TREVRPC_MSQUIC_TESTING
        if (selected != NULL) {
            budget->resume_scan_count++;
        }
#endif
        if (selected == NULL) {
            if (!budget->kick_pending) {
                budget->resumer_active = false;
                pthread_cond_broadcast(&budget->cond);
                pthread_mutex_unlock(&budget->mutex);
                return;
            }
            continue;
        }
        trevrpc_msquic_recv_pause_remove_budget_locked(budget, selected);
        selected->active_resume_pins++;
        pthread_mutex_unlock(&budget->mutex);
        trevrpc_msquic_test_emit_stream_event(TREV_MSQUIC_TEST_STREAM_RECV_RESUME_PINNED);

        HQUIC enable_handle = NULL;
        bool enable_receive = false;
        bool fatal = false;
        bool blocked = false;
        uint32_t observer_flags = 0;
        pthread_mutex_lock(&selected->mutex);
        if (!selected->receive_closing && selected->recv_pause_kind != TREV_MSQUIC_RECV_PAUSE_NONE) {
            size_t need_bytes = selected->recv_pause_need_bytes;
            size_t need_count = selected->recv_pause_need_count;
            trevrpc_msquic_receive_pause_kind need_kind = selected->recv_pause_kind;
            bool undecided =
                selected->recv_mode == TREV_MSQUIC_RECV_UNDECIDED && need_kind == TREV_MSQUIC_RECV_PAUSE_RAW_CHUNK;
            bool local_fits = trevrpc_msquic_recv_local_fits_locked(selected, need_bytes, need_count, undecided);
            pthread_mutex_lock(&budget->mutex);
            bool aggregate_fits = trevrpc_msquic_budget_aggregate_fits_locked(budget, selected, need_bytes, need_count);
            if (local_fits && aggregate_fits) {
                selected->recv_owned_bytes += need_bytes;
                selected->recv_owned_count += need_count;
                budget->owned_bytes += need_bytes;
                budget->owned_count += need_count;
                selected->recv_grant_bytes = need_bytes;
                selected->recv_grant_count = need_count;
                selected->recv_grant_kind = need_kind;
                selected->recv_pause_kind = TREV_MSQUIC_RECV_PAUSE_NONE;
                selected->recv_pause_need_bytes = 0;
                selected->recv_pause_need_count = 0;
            } else if (!selected->receive_closing) {
                selected->pause_prev = NULL;
                selected->pause_next = budget->paused_head;
                if (budget->paused_head != NULL) {
                    budget->paused_head->pause_prev = selected;
                } else {
                    budget->paused_tail = selected;
                }
                budget->paused_head = selected;
                selected->pause_listed = true;
                blocked = true;
            }
            pthread_mutex_unlock(&budget->mutex);

            if (selected->recv_grant_kind != TREV_MSQUIC_RECV_PAUSE_NONE) {
                if (selected->pending_frame_valid) {
                    int materialized = trevrpc_msquic_materialize_pending_frame_locked(selected);
                    if (materialized < 0) {
                        selected->err = ENOMEM;
                        fatal = true;
                    } else if (materialized == 0 && selected->frame_head != NULL) {
                        observer_flags |= TREV_MSQUIC_STREAM_OBSERVER_READABLE;
                    }
                }
                if (selected->receive_disabled && selected->recv_mode == TREV_MSQUIC_RECV_FRAMES &&
                    selected->recv_head != NULL) {
                    selected->receive_waiting_on_raw_pump = true;
                }
                if (selected->receive_disabled && !fatal && !selected->receive_waiting_on_raw_pump) {
                    enable_handle = selected->handle;
                    if (enable_handle != NULL && !selected->close_pending) {
                        selected->receive_disabled = false;
                        selected->active_handle_ops++;
                        enable_receive = true;
                    } else {
#ifdef TREVRPC_MSQUIC_TESTING
                        if (!selected->receive_closing && selected->synthetic_receive_fixture) {
                            selected->receive_disabled = false;
                        } else
#endif
                        {
                            trevrpc_msquic_recv_release_grant_locked(selected);
                        }
                    }
                }
                pthread_cond_broadcast(&selected->cond);
            }
        }
        pthread_mutex_unlock(&selected->mutex);

        if (observer_flags != 0) {
            trevrpc_msquic_stream_notify(selected, observer_flags);
        }
        if (enable_receive) {
            QUIC_STATUS status = trevrpc_msquic_api()->StreamReceiveSetEnabled(enable_handle, TRUE);
            if (QUIC_FAILED(status)) {
                pthread_mutex_lock(&selected->mutex);
                trevrpc_msquic_recv_release_grant_locked(selected);
                selected->err = (int)status;
                pthread_cond_broadcast(&selected->cond);
                pthread_mutex_unlock(&selected->mutex);
                trevrpc_msquic_stream_notify(selected, TREV_MSQUIC_STREAM_OBSERVER_TERMINAL);
                (void)trevrpc_msquic_test_stream_shutdown(enable_handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, 0);
            }
            trevrpc_msquic_stream_handle_release(selected);
        } else if (fatal) {
            trevrpc_msquic_stream_notify(selected, TREV_MSQUIC_STREAM_OBSERVER_TERMINAL);
            (void)trevrpc_msquic_stream_abort_receive(selected);
        }

        trevrpc_msquic_stream_lifecycle_retain(selected);
        pthread_mutex_lock(&budget->mutex);
        if (atomic_load_explicit(&selected->active_resume_pins, memory_order_relaxed) > 0) {
            atomic_fetch_sub_explicit(&selected->active_resume_pins, 1, memory_order_release);
        }
        pthread_cond_broadcast(&budget->cond);
        bool stop = blocked && !budget->kick_pending;
        if (stop) {
            budget->resumer_active = false;
        }
        pthread_mutex_unlock(&budget->mutex);
        trevrpc_msquic_stream_lifecycle_release(selected);
        if (stop) {
            return;
        }
        pthread_mutex_lock(&budget->mutex);
    }
}

static void trevrpc_msquic_stream_resume_receive_if_ready(trevrpc_msquic_stream* stream) {
    HQUIC handle = NULL;
    pthread_mutex_lock(&stream->mutex);
    if (stream->receive_resume_ready && stream->receive_disabled && !stream->receive_closing) {
        stream->receive_resume_ready = false;
        stream->receive_disabled = false;
        handle = stream->handle;
        if (handle != NULL && !stream->close_pending) {
            stream->active_handle_ops++;
        } else {
            handle = NULL;
        }
    }
    pthread_mutex_unlock(&stream->mutex);
    if (handle == NULL) {
        return;
    }

    QUIC_STATUS status = trevrpc_msquic_api()->StreamReceiveSetEnabled(handle, TRUE);
    if (QUIC_FAILED(status)) {
        pthread_mutex_lock(&stream->mutex);
        stream->err = (int)status;
        pthread_cond_broadcast(&stream->cond);
        pthread_mutex_unlock(&stream->mutex);
        trevrpc_msquic_stream_notify(stream, TREV_MSQUIC_STREAM_OBSERVER_TERMINAL);
        (void)trevrpc_msquic_test_stream_shutdown(handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, 0);
    }
    trevrpc_msquic_stream_handle_release(stream);
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

#if defined(QUIC_API_HAS_RESET_STREAM_AT_DIALECTS)
static uint32_t trevrpc_msquic_native_reset_dialect_mask(uint32_t dialects) {
    uint32_t native_dialects = 0;
    if ((dialects & TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT) != 0) {
        native_dialects |= QUIC_RESET_STREAM_AT_DIALECT_DRAFT_07;
    }
    if ((dialects & TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT) != 0) {
        native_dialects |= QUIC_RESET_STREAM_AT_DIALECT_DRAFT_10;
    }
    return native_dialects;
}

static uint32_t trevrpc_msquic_reset_dialect_mask_from_native(uint32_t native_dialects) {
    uint32_t dialects = TREV_MSQUIC_RESET_DIALECTS_NONE;
    if ((native_dialects & QUIC_RESET_STREAM_AT_DIALECT_DRAFT_07) != 0) {
        dialects |= TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT;
    }
    if ((native_dialects & QUIC_RESET_STREAM_AT_DIALECT_DRAFT_10) != 0) {
        dialects |= TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT;
    }
    return dialects;
}
#endif

static int trevrpc_msquic_configure_endpoint_with_alpns(const trevrpc_msquic_config* config,
    const trevrpc_msquic_alpn* alpns,
    size_t alpns_len,
    const trevrpc_msquic_feature_request* features,
    bool server,
    HQUIC* registration,
    HQUIC* configuration) {
    trevrpc_msquic_provider_features provider = trevrpc_msquic_api_owner_features();
    int err = trevrpc_msquic_feature_request_validate(&provider, features);
    if (err != 0) {
        return err;
    }
    err = trevrpc_msquic_api_acquire();
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

    QUIC_STATUS status = trevrpc_msquic_api()->RegistrationOpen(&registration_config, registration);
    if (QUIC_FAILED(status)) {
        trevrpc_msquic_api_release();
        return (int)status;
    }

    QUIC_BUFFER* alpn_buffers = NULL;
    uint32_t alpn_count = 0;
    err = trevrpc_msquic_build_alpn_buffers(config, alpns, alpns_len, &alpn_buffers, &alpn_count);
    if (err != 0) {
        trevrpc_msquic_api()->RegistrationClose(*registration);
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
    settings.DatagramReceiveEnabled = features->datagram_receive ? TRUE : FALSE;
    settings.IsSet.ReliableResetEnabled = TRUE;
    settings.ReliableResetEnabled =
        features->requested_reset_dialects != TREV_MSQUIC_RESET_DIALECTS_NONE ? TRUE : FALSE;
    if (server) {
        settings.IsSet.ServerResumptionLevel = TRUE;
        settings.ServerResumptionLevel = QUIC_SERVER_RESUME_ONLY;
    }

    status = trevrpc_msquic_api()->ConfigurationOpen(
        *registration, alpn_buffers, alpn_count, &settings, sizeof(settings), NULL, configuration);
    free(alpn_buffers);
    if (QUIC_FAILED(status)) {
        trevrpc_msquic_api()->RegistrationClose(*registration);
        *registration = NULL;
        trevrpc_msquic_api_release();
        return (int)status;
    }

#if defined(QUIC_API_HAS_RESET_STREAM_AT_DIALECTS)
    uint32_t native_reset_dialects = trevrpc_msquic_native_reset_dialect_mask(features->requested_reset_dialects);
    status = trevrpc_msquic_api()->SetParam(*configuration,
        QUIC_PARAM_CONFIGURATION_RESET_STREAM_AT_DIALECT_MASK,
        sizeof(native_reset_dialects),
        &native_reset_dialects);
    if (QUIC_FAILED(status)) {
        trevrpc_msquic_api()->ConfigurationClose(*configuration);
        trevrpc_msquic_api()->RegistrationClose(*registration);
        *configuration = NULL;
        *registration = NULL;
        trevrpc_msquic_api_release();
        return (int)status;
    }
#endif

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

    status = trevrpc_msquic_api()->ConfigurationLoadCredential(*configuration, &credential);
    if (QUIC_FAILED(status)) {
        trevrpc_msquic_api()->ConfigurationClose(*configuration);
        trevrpc_msquic_api()->RegistrationClose(*registration);
        *configuration = NULL;
        *registration = NULL;
        trevrpc_msquic_api_release();
        return (int)status;
    }

    return 0;
}

static int trevrpc_msquic_configure_endpoint(const trevrpc_msquic_config* config,
    const trevrpc_msquic_feature_request* features,
    bool server,
    HQUIC* registration,
    HQUIC* configuration) {
    return trevrpc_msquic_configure_endpoint_with_alpns(config, NULL, 0, features, server, registration, configuration);
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

trevrpc_msquic_provider_features trevrpc_msquic_provider_features_current(void) {
    return trevrpc_msquic_api_owner_features();
}

trevrpc_msquic_feature_request trevrpc_msquic_default_h3_features(void) {
    trevrpc_msquic_provider_features provider = trevrpc_msquic_provider_features_current();
    return trevrpc_msquic_h3_feature_request(&provider);
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
    trevrpc_msquic_feature_request features = trevrpc_msquic_generic_feature_request();
    return trevrpc_msquic_listen_alpns_features(host, port, config, alpns, alpns_len, &features, out_listener);
}

int trevrpc_msquic_listen_alpns_features(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    const trevrpc_msquic_alpn* alpns,
    size_t alpns_len,
    const trevrpc_msquic_feature_request* features,
    trevrpc_msquic_listener** out_listener) {
    return trevrpc_msquic_listen_alpns_features_with_receive_policy(
        host, port, config, alpns, alpns_len, features, NULL, out_listener);
}

int trevrpc_msquic_listen_alpns_features_with_receive_policy(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    const trevrpc_msquic_alpn* alpns,
    size_t alpns_len,
    const trevrpc_msquic_feature_request* features,
    const trevrpc_msquic_receive_policy* receive_policy,
    trevrpc_msquic_listener** out_listener) {
    if (host == NULL || config == NULL || features == NULL || out_listener == NULL) {
        return EINVAL;
    }
    *out_listener = NULL;
    size_t max_frame_size = trevrpc_msquic_effective_max_frame_size(config->max_frame_size);
    trevrpc_msquic_receive_policy effective_policy;
    int policy_err = trevrpc_msquic_receive_policy_effective(receive_policy, max_frame_size, &effective_policy);
    if (policy_err != 0) {
        return policy_err;
    }
    trevrpc_msquic_listener* listener = calloc(1, sizeof(*listener));
    if (listener == NULL) {
        return ENOMEM;
    }

    pthread_mutex_init(&listener->mutex, NULL);
    pthread_cond_init(&listener->cond, NULL);
    listener->max_frame_size = max_frame_size;
    listener->max_pending_send_bytes = trevrpc_msquic_effective_max_pending_send_bytes(config->max_pending_send_bytes);
    listener->max_pending_send_count = trevrpc_msquic_effective_max_pending_send_count(config->max_pending_send_count);
    listener->receive_policy = effective_policy;
    listener->features = *features;

    int err = trevrpc_msquic_configure_endpoint_with_alpns(
        config, alpns, alpns_len, features, true, &listener->registration, &listener->configuration);
    if (err != 0) {
        trevrpc_msquic_listener_close(listener);
        return err;
    }
    listener->api_ref_acquired = true;

    QUIC_STATUS status = trevrpc_msquic_api()->ListenerOpen(
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
    status = trevrpc_msquic_api()->ListenerStart(listener->listener, alpn_buffers, alpn_count, &addr);
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
    QUIC_STATUS status =
        trevrpc_msquic_api()->GetParam(listener->listener, QUIC_PARAM_LISTENER_LOCAL_ADDRESS, &addr_len, &addr);
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
        trevrpc_msquic_api()->ListenerClose(handle);
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
        trevrpc_msquic_api()->ConfigurationClose(listener->configuration);
        listener->configuration = NULL;
    }
    if (listener->registration != NULL) {
#ifndef TREVRPC_SANITIZER_BUILD
        trevrpc_msquic_api()->RegistrationClose(listener->registration);
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
        trevrpc_msquic_api()->ListenerStop(handle);
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
    trevrpc_msquic_feature_request features = trevrpc_msquic_generic_feature_request();
    return trevrpc_msquic_dial_observed_features(host,
        port,
        config,
        &features,
        cancelled,
        cancellation_context,
        resumption_ticket,
        resumption_ticket_len,
        observer,
        observer_context,
        out_conn);
}

int trevrpc_msquic_dial_observed_features(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    const trevrpc_msquic_feature_request* features,
    trevrpc_msquic_cancelled_fn cancelled,
    void* cancellation_context,
    const uint8_t* resumption_ticket,
    size_t resumption_ticket_len,
    trevrpc_msquic_conn_observer observer,
    void* observer_context,
    trevrpc_msquic_conn** out_conn) {
    return trevrpc_msquic_dial_observed_features_with_receive_policy(host,
        port,
        config,
        features,
        cancelled,
        cancellation_context,
        resumption_ticket,
        resumption_ticket_len,
        observer,
        observer_context,
        NULL,
        out_conn);
}

int trevrpc_msquic_dial_observed_features_with_receive_policy(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    const trevrpc_msquic_feature_request* features,
    trevrpc_msquic_cancelled_fn cancelled,
    void* cancellation_context,
    const uint8_t* resumption_ticket,
    size_t resumption_ticket_len,
    trevrpc_msquic_conn_observer observer,
    void* observer_context,
    const trevrpc_msquic_receive_policy* receive_policy,
    trevrpc_msquic_conn** out_conn) {
    if (host == NULL || config == NULL || features == NULL || out_conn == NULL ||
        (resumption_ticket == NULL && resumption_ticket_len != 0) || resumption_ticket_len > UINT32_MAX) {
        return EINVAL;
    }
    *out_conn = NULL;
    size_t max_frame_size = trevrpc_msquic_effective_max_frame_size(config->max_frame_size);
    trevrpc_msquic_receive_policy effective_policy;
    int err = trevrpc_msquic_receive_policy_effective(receive_policy, max_frame_size, &effective_policy);
    if (err != 0) {
        return err;
    }
    HQUIC registration = NULL;
    HQUIC configuration = NULL;
    err = trevrpc_msquic_configure_endpoint(config, features, false, &registration, &configuration);
    if (err != 0) {
        return err;
    }

    trevrpc_msquic_conn* conn = trevrpc_msquic_conn_alloc(NULL, max_frame_size, &effective_policy);
    if (conn == NULL) {
        trevrpc_msquic_api()->ConfigurationClose(configuration);
        trevrpc_msquic_api()->RegistrationClose(registration);
        trevrpc_msquic_api_release();
        return ENOMEM;
    }
    conn->registration = registration;
    conn->configuration = configuration;
    conn->owns_endpoint = true;
    trevrpc_msquic_feature_state_init(&conn->features, features);
    conn->observer = observer;
    conn->observer_context = observer_context;
    conn->max_frame_size = max_frame_size;
    conn->max_pending_send_bytes = trevrpc_msquic_effective_max_pending_send_bytes(config->max_pending_send_bytes);
    conn->max_pending_send_count = trevrpc_msquic_effective_max_pending_send_count(config->max_pending_send_count);
    if (config->alpn != NULL && config->alpn_len > 0 && config->alpn_len <= sizeof(conn->negotiated_alpn)) {
        conn->negotiated_alpn_len = (uint8_t)config->alpn_len;
        memcpy(conn->negotiated_alpn, config->alpn, conn->negotiated_alpn_len);
    }

    QUIC_STATUS status =
        trevrpc_msquic_api()->ConnectionOpen(registration, trevrpc_msquic_conn_callback, conn, &conn->handle);
    if (QUIC_FAILED(status)) {
        trevrpc_msquic_conn_close(conn);
        return (int)status;
    }

    if (resumption_ticket_len > 0) {
        status = trevrpc_msquic_api()->SetParam(
            conn->handle, QUIC_PARAM_CONN_RESUMPTION_TICKET, (uint32_t)resumption_ticket_len, resumption_ticket);
        if (QUIC_FAILED(status)) {
            trevrpc_msquic_conn_close(conn);
            return (int)status;
        }
    }

    if (cancelled != NULL && cancelled(cancellation_context)) {
        trevrpc_msquic_conn_close(conn);
        return -ECANCELED;
    }

    HQUIC connection_handle = trevrpc_msquic_conn_handle_acquire(conn);
    if (connection_handle == NULL) {
        trevrpc_msquic_conn_close(conn);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    status =
        trevrpc_msquic_api()->ConnectionStart(connection_handle, configuration, QUIC_ADDRESS_FAMILY_UNSPEC, host, port);
    trevrpc_msquic_conn_handle_release(conn);
    if (QUIC_FAILED(status)) {
        if (cancelled != NULL && cancelled(cancellation_context)) {
            trevrpc_msquic_conn_close(conn);
            return -ECANCELED;
        }
        trevrpc_msquic_conn_close(conn);
        return (int)status;
    }

    pthread_mutex_lock(&conn->mutex);
    bool was_cancelled = false;
    int wait_err = 0;
    while (!conn->connected && !conn->shutdown_complete && !was_cancelled && conn->err == 0) {
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

    if (was_cancelled || (cancelled != NULL && cancelled(cancellation_context))) {
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

int trevrpc_msquic_conn_feature_snapshot(trevrpc_msquic_conn* conn, trevrpc_msquic_feature_snapshot* snapshot) {
    if (conn == NULL || snapshot == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&conn->mutex);
    int result = trevrpc_msquic_feature_snapshot_get(&conn->features, snapshot);
    pthread_mutex_unlock(&conn->mutex);
    return result;
}

void trevrpc_msquic_conn_clear_observer(trevrpc_msquic_conn* conn) {
    if (conn == NULL) {
        return;
    }
    pthread_mutex_lock(&conn->mutex);
    conn->observer = NULL;
    conn->observer_context = NULL;
    while (conn->active_observer_callbacks > 0 && TrevMsQuicObserverConn != conn) {
        pthread_cond_wait(&conn->cond, &conn->mutex);
    }
    pthread_mutex_unlock(&conn->mutex);
}

int trevrpc_msquic_stream_set_observer(
    trevrpc_msquic_stream* stream, trevrpc_msquic_stream_observer observer, void* context) {
    if (stream == NULL || observer == NULL) {
        return -EINVAL;
    }

    uint32_t flags = 0;
    int result = 0;
    pthread_mutex_lock(&stream->mutex);
    if (stream->api_closing) {
        result = -ECANCELED;
    } else if (stream->observer != NULL || stream->active_observer_callbacks != 0) {
        result = -EBUSY;
    } else {
        stream->observer = observer;
        stream->observer_context = context;
        flags = trevrpc_msquic_stream_observer_flags_locked(stream);
        if (flags != 0) {
            stream->active_observer_callbacks++;
            stream->active_lifecycle_refs++;
        }
    }
    pthread_mutex_unlock(&stream->mutex);

    if (result == 0 && flags != 0) {
        trevrpc_msquic_stream* previous = TrevMsQuicObserverStream;
        TrevMsQuicObserverStream = stream;
        observer(context, flags);
        TrevMsQuicObserverStream = previous;
        trevrpc_msquic_stream_observer_finish(stream);
        trevrpc_msquic_stream_lifecycle_release(stream);
    }
    return result;
}

void trevrpc_msquic_stream_clear_observer(trevrpc_msquic_stream* stream) {
    if (stream == NULL) {
        return;
    }
    pthread_mutex_lock(&stream->mutex);
    stream->observer = NULL;
    stream->observer_context = NULL;
    pthread_mutex_unlock(&stream->mutex);
}

void trevrpc_msquic_stream_drain_observer(trevrpc_msquic_stream* stream) {
    if (stream == NULL || trevrpc_msquic_stream_api_lifecycle_acquire(stream) != 0) {
        return;
    }
    trevrpc_msquic_test_emit_stream_event(TREV_MSQUIC_TEST_STREAM_DRAIN_OBSERVER_PINNED);
    pthread_mutex_lock(&stream->mutex);
    while (stream->active_observer_callbacks > 0) {
        pthread_cond_wait(&stream->cond, &stream->mutex);
    }
    pthread_mutex_unlock(&stream->mutex);
    trevrpc_msquic_stream_lifecycle_release(stream);
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
    size_t initial_frame_max_len = trevrpc_msquic_conn_uses_native_frames(conn) ? conn->max_frame_size : 0;
    trevrpc_msquic_stream* stream = trevrpc_msquic_stream_alloc(NULL,
        conn->max_pending_send_bytes,
        conn->max_pending_send_count,
        conn->max_frame_size,
        initial_frame_max_len,
        conn->max_stream_recv_owned_bytes,
        conn->max_stream_recv_owned_count,
        conn->recv_budget);
    if (stream == NULL) {
        return ENOMEM;
    }

    HQUIC connection_handle = trevrpc_msquic_conn_handle_acquire(conn);
    if (connection_handle == NULL) {
        trevrpc_msquic_stream_close(stream);
        return TREV_MSQUIC_ERR_CLOSED;
    }

    QUIC_STATUS status = trevrpc_msquic_api()->StreamOpen(
        connection_handle, flags, trevrpc_msquic_stream_callback, stream, &stream->handle);
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
    status = trevrpc_msquic_api()->StreamStart(stream_handle, QUIC_STREAM_START_FLAG_IMMEDIATE);
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
    QUIC_STATUS status = trevrpc_msquic_api()->GetParam(handle, QUIC_PARAM_STREAM_ID, &stream_id_len, out_stream_id);
    trevrpc_msquic_stream_handle_release(stream);
    return QUIC_FAILED(status) ? (int)status : 0;
}

static void trevrpc_msquic_conn_destroy_owned(trevrpc_msquic_conn* conn, bool owns_close_call) {
    trevrpc_msquic_stream_node* streams = NULL;
    trevrpc_msquic_receive_budget* recv_budget = NULL;
    HQUIC configuration = NULL;
    HQUIC registration = NULL;
    bool owns_endpoint = false;
    bool release_api = false;

    pthread_mutex_lock(&conn->mutex);
    conn->observer = NULL;
    conn->observer_context = NULL;
    while (conn->active_observer_callbacks > 0 || conn->active_lifecycle_refs > 0 || conn->active_handle_ops > 0 ||
           conn->handle != NULL || conn->close_pending) {
        pthread_cond_wait(&conn->cond, &conn->mutex);
    }
    streams = conn->stream_head;
    conn->stream_head = NULL;
    conn->stream_tail = NULL;
    recv_budget = conn->recv_budget;
    conn->recv_budget = NULL;
    configuration = conn->configuration;
    conn->configuration = NULL;
    registration = conn->registration;
    conn->registration = NULL;
    owns_endpoint = conn->owns_endpoint;
    conn->owns_endpoint = false;
    release_api = conn->api_ref_acquired;
    conn->api_ref_acquired = false;
    pthread_mutex_unlock(&conn->mutex);

    while (streams != NULL) {
        trevrpc_msquic_stream_node* node = streams;
        streams = node->next;
        trevrpc_msquic_stream_close(node->stream);
        free(node);
    }

    if (owns_endpoint) {
        if (configuration != NULL) {
            trevrpc_msquic_api()->ConfigurationClose(configuration);
        }
        if (registration != NULL) {
#ifndef TREVRPC_SANITIZER_BUILD
            trevrpc_msquic_api()->RegistrationClose(registration);
#endif
        }
        trevrpc_msquic_api_release();
    }
    trevrpc_msquic_receive_budget_release(recv_budget);
    if (release_api) {
        trevrpc_msquic_api_release();
    }

    pthread_mutex_lock(&conn->mutex);
    conn->destroy_complete = true;
    pthread_cond_broadcast(&conn->cond);
    size_t retained_calls = owns_close_call ? 1 : 0;
    while (conn->active_close_calls > retained_calls) {
        pthread_cond_wait(&conn->cond, &conn->mutex);
    }
    if (owns_close_call && conn->active_close_calls > 0) {
        conn->active_close_calls--;
    }
    pthread_mutex_unlock(&conn->mutex);

    pthread_cond_destroy(&conn->cond);
    pthread_mutex_destroy(&conn->mutex);
    free(conn);
}

void trevrpc_msquic_conn_close(trevrpc_msquic_conn* conn) {
    if (conn == NULL) {
        return;
    }

    bool observer_reentry = TrevMsQuicObserverConn == conn;
    bool request_shutdown = false;
    bool destroy = false;
    pthread_mutex_lock(&conn->mutex);
    conn->active_close_calls++;
    if (conn->destroy_complete) {
        conn->active_close_calls--;
        pthread_cond_broadcast(&conn->cond);
        pthread_mutex_unlock(&conn->mutex);
        return;
    }
    request_shutdown = !conn->destroy_requested;
    conn->destroy_requested = true;
    conn->observer = NULL;
    conn->observer_context = NULL;
    if (!observer_reentry && !conn->destroy_started) {
        conn->destroy_started = true;
        destroy = true;
    }
    if (observer_reentry && !destroy) {
        conn->active_close_calls--;
    }
    pthread_cond_broadcast(&conn->cond);
    pthread_mutex_unlock(&conn->mutex);

    if (request_shutdown) {
        trevrpc_msquic_conn_shutdown(conn);
    }
    if (destroy) {
        trevrpc_msquic_conn_destroy_owned(conn, true);
        return;
    }
    if (observer_reentry) {
        return;
    }

    pthread_mutex_lock(&conn->mutex);
    while (!conn->destroy_complete) {
        pthread_cond_wait(&conn->cond, &conn->mutex);
    }
    if (conn->active_close_calls > 0) {
        conn->active_close_calls--;
    }
    pthread_cond_broadcast(&conn->cond);
    pthread_mutex_unlock(&conn->mutex);
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
        trevrpc_msquic_api()->ConnectionShutdown(handle, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, error_code);
        trevrpc_msquic_conn_handle_release(conn);
    }
}

void trevrpc_msquic_conn_shutdown(trevrpc_msquic_conn* conn) {
    trevrpc_msquic_conn_shutdown_error(conn, 0);
}

static intptr_t trevrpc_msquic_stream_read_until(trevrpc_msquic_stream* stream,
    uint8_t* data,
    size_t len,
    const struct timespec* deadline,
    bool ready_only,
    bool select_codec) {
    if (stream == NULL || (data == NULL && len > 0)) {
        return -EINVAL;
    }
    if (len == 0) {
        return 0;
    }
    int op_err = trevrpc_msquic_stream_api_lifecycle_acquire(stream);
    if (op_err != 0) {
        return op_err;
    }
    pthread_mutex_lock(&stream->mutex);
    if (stream->recv_mode == TREV_MSQUIC_RECV_FRAMES) {
        pthread_mutex_unlock(&stream->mutex);
        trevrpc_msquic_stream_lifecycle_release(stream);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    if (select_codec && stream->recv_mode == TREV_MSQUIC_RECV_UNDECIDED) {
        stream->recv_mode = TREV_MSQUIC_RECV_BYTES;
    }
    int wait_err = 0;
    if (ready_only && stream->recv_head == NULL && !stream->recv_fin && !stream->closed && stream->err == 0) {
        wait_err = TREV_MSQUIC_ERR_TIMEOUT;
    } else {
        wait_err = trevrpc_msquic_stream_wait_recv_locked(stream, deadline);
    }
    if (wait_err != 0) {
        pthread_mutex_unlock(&stream->mutex);
        trevrpc_msquic_stream_lifecycle_release(stream);
        return wait_err < 0 ? wait_err : -wait_err;
    }

    if (stream->recv_head == NULL) {
        intptr_t result = stream->err != 0 ? trevrpc_msquic_error_result(stream->err) : TREV_MSQUIC_ERR_EOF;
        pthread_mutex_unlock(&stream->mutex);
        trevrpc_msquic_stream_lifecycle_release(stream);
        return result;
    }

    trevrpc_msquic_chunk* chunk = stream->recv_head;
    size_t available = chunk->len - chunk->offset;
    size_t copied = available < len ? available : len;
    memcpy(data, chunk->data + chunk->offset, copied);
    chunk->offset += copied;
    stream->recv_buffered -= copied;
    chunk->charge_bytes -= copied;
    trevrpc_msquic_recv_release_locked(stream, copied, 0);
    if (chunk->offset == chunk->len) {
        stream->recv_head = chunk->next;
        if (stream->recv_head == NULL) {
            stream->recv_tail = NULL;
        }
        trevrpc_msquic_recv_release_locked(stream, chunk->charge_bytes, chunk->charge_count);
        chunk->charge_bytes = 0;
        chunk->charge_count = 0;
        free(chunk);
    }
    trevrpc_msquic_receive_budget* budget = stream->recv_budget;
    pthread_mutex_unlock(&stream->mutex);
    trevrpc_msquic_stream_receive_progress(stream, budget);
    trevrpc_msquic_stream_lifecycle_release(stream);

    return (intptr_t)copied;
}

intptr_t trevrpc_msquic_stream_read(trevrpc_msquic_stream* stream, uint8_t* data, size_t len) {
    return trevrpc_msquic_stream_read_until(stream, data, len, NULL, false, true);
}

intptr_t trevrpc_msquic_stream_read_protocol(trevrpc_msquic_stream* stream, uint8_t* data, size_t len) {
    return trevrpc_msquic_stream_read_until(stream, data, len, NULL, false, false);
}

intptr_t trevrpc_msquic_stream_read_protocol_timeout(
    trevrpc_msquic_stream* stream, uint8_t* data, size_t len, uint64_t timeout_nanos) {
    if (timeout_nanos == 0) {
        return trevrpc_msquic_stream_read_protocol(stream, data, len);
    }
    struct timespec deadline = {0};
    int err = trevrpc_msquic_realtime_deadline(timeout_nanos, &deadline);
    if (err != 0) {
        return err;
    }
    return trevrpc_msquic_stream_read_until(stream, data, len, &deadline, false, false);
}

intptr_t trevrpc_msquic_stream_read_protocol_ready(trevrpc_msquic_stream* stream, uint8_t* data, size_t len) {
    return trevrpc_msquic_stream_read_until(stream, data, len, NULL, true, false);
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
    return trevrpc_msquic_stream_read_until(stream, data, len, &deadline, false, true);
}

intptr_t trevrpc_msquic_stream_read_ready(trevrpc_msquic_stream* stream, uint8_t* data, size_t len) {
    return trevrpc_msquic_stream_read_until(stream, data, len, NULL, true, true);
}

static void trevrpc_msquic_stream_receive_progress(
    trevrpc_msquic_stream* stream, trevrpc_msquic_receive_budget* budget) {
    trevrpc_msquic_receive_budget_kick(budget);
    trevrpc_msquic_stream_resume_receive_if_ready(stream);
}

typedef enum trevrpc_msquic_frame_wait_mode {
    TREV_MSQUIC_FRAME_WAIT_BLOCKING = 0,
    TREV_MSQUIC_FRAME_WAIT_READY,
} trevrpc_msquic_frame_wait_mode;

static intptr_t trevrpc_msquic_stream_dequeue_frame(trevrpc_msquic_stream* stream,
    size_t max_len,
    const struct timespec* deadline,
    trevrpc_msquic_frame_wait_mode wait_mode,
    trevrpc_msquic_frame** out_frame) {
    if (stream == NULL || out_frame == NULL) {
        return -EINVAL;
    }
    *out_frame = NULL;
    int op_err = trevrpc_msquic_stream_api_lifecycle_acquire(stream);
    if (op_err != 0) {
        return op_err;
    }
    pthread_mutex_lock(&stream->mutex);
    int err = trevrpc_msquic_stream_enable_frame_mode_locked(stream, max_len);
    trevrpc_msquic_receive_budget* budget = stream->recv_budget;
    intptr_t ready = err;
    if (err == 0) {
        ready = wait_mode == TREV_MSQUIC_FRAME_WAIT_READY ? trevrpc_msquic_stream_frame_not_ready_locked(stream)
                                                          : trevrpc_msquic_stream_wait_frame_locked(stream, deadline);
        if (ready == 0) {
            *out_frame = trevrpc_msquic_stream_pop_frame_locked(stream);
            if (*out_frame == NULL) {
                ready = TREV_MSQUIC_ERR_EOF;
            }
        }
    }
    pthread_mutex_unlock(&stream->mutex);
    trevrpc_msquic_stream_receive_progress(stream, budget);
    trevrpc_msquic_stream_lifecycle_release(stream);
    return ready;
}

static intptr_t trevrpc_msquic_frame_take_bytes(
    trevrpc_msquic_frame* frame, uint8_t** body, size_t* len, size_t max_len) {
    *len = frame->declared_len;
    if (frame->err != 0) {
        intptr_t result = frame->err;
        trevrpc_owned_bytes_reset(&frame->body);
        free(frame);
        return result;
    }
    if (frame->body.len > max_len) {
        trevrpc_owned_bytes_reset(&frame->body);
        free(frame);
        return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
    }
    if (frame->body.owner == (void*)frame->body.data && frame->body.release == trevrpc_frame_default_free &&
        frame->body.release_context == NULL) {
        *body = (uint8_t*)frame->body.data;
        trevrpc_owned_bytes_init(&frame->body);
    } else if (frame->body.len > 0) {
        *body = malloc(frame->body.len);
        if (*body == NULL) {
            trevrpc_owned_bytes_reset(&frame->body);
            free(frame);
            return -ENOMEM;
        }
        memcpy(*body, frame->body.data, frame->body.len);
        trevrpc_owned_bytes_reset(&frame->body);
    }
    free(frame);
    return 1;
}

static intptr_t trevrpc_msquic_stream_read_frame_until(
    trevrpc_msquic_stream* stream, uint8_t** body, size_t* len, size_t max_len, const struct timespec* deadline) {
    if (stream == NULL || body == NULL || len == NULL) {
        return -EINVAL;
    }
    *body = NULL;
    *len = 0;

    trevrpc_msquic_frame* frame = NULL;
    intptr_t ready =
        trevrpc_msquic_stream_dequeue_frame(stream, max_len, deadline, TREV_MSQUIC_FRAME_WAIT_BLOCKING, &frame);
    if (ready != 0 || frame == NULL) {
        return ready;
    }
    return trevrpc_msquic_frame_take_bytes(frame, body, len, max_len);
}

static intptr_t trevrpc_msquic_stream_read_frame_owned_until(
    trevrpc_msquic_stream* stream, trevrpc_owned_bytes* body, size_t max_len, const struct timespec* deadline) {
    if (stream == NULL || body == NULL) {
        return -EINVAL;
    }
    trevrpc_owned_bytes_init(body);

    trevrpc_msquic_frame* frame = NULL;
    intptr_t ready =
        trevrpc_msquic_stream_dequeue_frame(stream, max_len, deadline, TREV_MSQUIC_FRAME_WAIT_BLOCKING, &frame);
    if (ready != 0 || frame == NULL) {
        return ready;
    }
    if (frame->err != 0) {
        intptr_t result = frame->err;
        trevrpc_owned_bytes_reset(&frame->body);
        free(frame);
        return result;
    }
    if (frame->body.len > max_len) {
        trevrpc_owned_bytes_reset(&frame->body);
        free(frame);
        return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
    }
    trevrpc_owned_bytes_move(body, &frame->body);
    free(frame);
    return 1;
}

intptr_t trevrpc_msquic_stream_read_frame_owned(
    trevrpc_msquic_stream* stream, trevrpc_owned_bytes* body, size_t max_len) {
    return trevrpc_msquic_stream_read_frame_owned_until(stream, body, max_len, NULL);
}

intptr_t trevrpc_msquic_stream_read_frame_owned_timeout(
    trevrpc_msquic_stream* stream, trevrpc_owned_bytes* body, size_t max_len, uint64_t timeout_nanos) {
    if (timeout_nanos == 0) {
        return trevrpc_msquic_stream_read_frame_owned(stream, body, max_len);
    }
    struct timespec deadline = {0};
    int err = trevrpc_msquic_realtime_deadline(timeout_nanos, &deadline);
    return err == 0 ? trevrpc_msquic_stream_read_frame_owned_until(stream, body, max_len, &deadline) : err;
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

intptr_t trevrpc_msquic_stream_read_frame_owned_ready(
    trevrpc_msquic_stream* stream, trevrpc_owned_bytes* body, size_t max_len) {
    if (stream == NULL || body == NULL) {
        return -EINVAL;
    }
    trevrpc_owned_bytes_init(body);

    trevrpc_msquic_frame* frame = NULL;
    intptr_t ready = trevrpc_msquic_stream_dequeue_frame(stream, max_len, NULL, TREV_MSQUIC_FRAME_WAIT_READY, &frame);
    if (ready != 0 || frame == NULL) {
        return ready;
    }
    if (frame->err != 0) {
        intptr_t result = frame->err;
        trevrpc_owned_bytes_reset(&frame->body);
        free(frame);
        return result;
    }
    if (frame->body.len > max_len) {
        trevrpc_owned_bytes_reset(&frame->body);
        free(frame);
        return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
    }
    trevrpc_owned_bytes_move(body, &frame->body);
    free(frame);
    return 1;
}

intptr_t trevrpc_msquic_stream_read_frame_ready(
    trevrpc_msquic_stream* stream, uint8_t** body, size_t* len, size_t max_len) {
    if (stream == NULL || body == NULL || len == NULL) {
        return -EINVAL;
    }
    *body = NULL;
    *len = 0;

    trevrpc_msquic_frame* frame = NULL;
    intptr_t ready = trevrpc_msquic_stream_dequeue_frame(stream, max_len, NULL, TREV_MSQUIC_FRAME_WAIT_READY, &frame);
    if (ready != 0 || frame == NULL) {
        return ready;
    }
    return trevrpc_msquic_frame_take_bytes(frame, body, len, max_len);
}

int trevrpc_msquic_test_parse_frame_owned(const uint8_t* data,
    size_t len,
    size_t max_len,
    trevrpc_frame_alloc_fn alloc,
    trevrpc_frame_free_fn dealloc,
    void* allocator_context,
    trevrpc_owned_bytes* body) {
    if ((data == NULL && len > 0) || body == NULL) {
        return -EINVAL;
    }
    trevrpc_owned_bytes_init(body);
    trevrpc_msquic_receive_policy policy;
    int err = trevrpc_msquic_receive_policy_effective(NULL, max_len, &policy);
    if (err != 0) {
        return -err;
    }
    trevrpc_msquic_receive_budget* budget = trevrpc_msquic_receive_budget_new(&policy, max_len);
    if (budget == NULL) {
        return -ENOMEM;
    }
    trevrpc_msquic_stream stream = {
        .recv_mode = TREV_MSQUIC_RECV_FRAMES,
        .recv_budget = budget,
        .configured_max_frame_size = max_len,
        .max_recv_owned_bytes = policy.max_stream_owned_bytes,
        .max_recv_owned_count = policy.max_stream_owned_count,
    };
    if (pthread_mutex_init(&stream.mutex, NULL) != 0) {
        trevrpc_msquic_receive_budget_release(budget);
        return -ENOMEM;
    }
    if (pthread_cond_init(&stream.cond, NULL) != 0) {
        pthread_mutex_destroy(&stream.mutex);
        trevrpc_msquic_receive_budget_release(budget);
        return -ENOMEM;
    }
    trevrpc_owned_bytes_init(&stream.pending_frame.body);
    trevrpc_frame_parser_init_with_allocator(&stream.frame_parser, max_len, alloc, dealloc, allocator_context);

    size_t consumed = 0;
    pthread_mutex_lock(&stream.mutex);
    err = trevrpc_msquic_stream_append_frame_bytes_locked(&stream, data, len, &consumed, false);
    pthread_mutex_unlock(&stream.mutex);
    if (err == 0 && consumed == len) {
        intptr_t result = trevrpc_msquic_stream_read_frame_owned_ready(&stream, body, max_len);
        err = result == 1 ? 0 : (int)result;
    } else if (err == 0) {
        err = -EINVAL;
    }
    pthread_mutex_lock(&stream.mutex);
    trevrpc_msquic_free_frames(&stream);
    pthread_mutex_unlock(&stream.mutex);
    pthread_cond_destroy(&stream.cond);
    pthread_mutex_destroy(&stream.mutex);
    trevrpc_msquic_receive_budget_release(budget);
    return err;
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

static bool trevrpc_msquic_message_frame_layout(size_t body_len, size_t* field_header_len, size_t* frame_body_len) {
    *field_header_len = 0;
    *frame_body_len = 0;
    if (body_len == 0) {
        return true;
    }
    size_t header_len = 0;
    if (!trevrpc_msquic_checked_add(1, trevrpc_msquic_varint_len(body_len), &header_len) ||
        !trevrpc_msquic_checked_add(header_len, body_len, frame_body_len)) {
        return false;
    }
    *field_header_len = header_len;
    return true;
}

static uint8_t* trevrpc_msquic_append_message_field_header(uint8_t* out, size_t body_len) {
    if (body_len > 0) {
        *out++ = 0x22;
        out = trevrpc_msquic_append_varint(out, body_len);
    }
    return out;
}

static intptr_t trevrpc_msquic_stream_write_message_frame_internal(
    trevrpc_msquic_stream* stream, const uint8_t* body, size_t body_len, size_t max_len, bool wait_for_capacity) {
    if (body == NULL && body_len > 0) {
        return -EINVAL;
    }
    size_t field_header_len = 0;
    size_t frame_body_len = 0;
    if (!trevrpc_msquic_message_frame_layout(body_len, &field_header_len, &frame_body_len)) {
        return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
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
        uint8_t* out = trevrpc_msquic_append_message_field_header(send->data + 4, body_len);
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
        size_t field_header_len = 0;
        size_t frame_body_len = 0;
        if (!trevrpc_msquic_message_frame_layout(body_lens[i], &field_header_len, &frame_body_len)) {
            return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
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
        size_t field_header_len = 0;
        size_t frame_body_len = 0;
        if (!trevrpc_msquic_message_frame_layout(body_lens[i], &field_header_len, &frame_body_len)) {
            result = TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
            goto cleanup;
        }

        *out++ = (uint8_t)(frame_body_len >> 24);
        *out++ = (uint8_t)(frame_body_len >> 16);
        *out++ = (uint8_t)(frame_body_len >> 8);
        *out++ = (uint8_t)frame_body_len;
        if (body_lens[i] > 0) {
            out = trevrpc_msquic_append_message_field_header(out, body_lens[i]);
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
        size_t field_header_len = 0;
        size_t frame_body_len = 0;
        if (!trevrpc_msquic_message_frame_layout(body_lens[i], &field_header_len, &frame_body_len)) {
            return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
        }
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
        size_t field_header_len = 0;
        size_t frame_body_len = 0;
        if (!trevrpc_msquic_message_frame_layout(body_lens[i], &field_header_len, &frame_body_len)) {
            result = TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
            trevrpc_msquic_send_release(stream, send);
            goto cleanup;
        }
        uint8_t* header = out;
        *out++ = (uint8_t)(frame_body_len >> 24);
        *out++ = (uint8_t)(frame_body_len >> 16);
        *out++ = (uint8_t)(frame_body_len >> 8);
        *out++ = (uint8_t)frame_body_len;
        out = trevrpc_msquic_append_message_field_header(out, body_lens[i]);
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
    int err = trevrpc_msquic_stream_api_lifecycle_acquire(stream);
    if (err != 0) {
        return err;
    }
    trevrpc_msquic_test_emit_stream_event(TREV_MSQUIC_TEST_STREAM_WAIT_SENDS_PINNED);

    pthread_mutex_lock(&stream->mutex);
    while (stream->pending_send_count > 0 || stream->active_send_completions > 0) {
        pthread_cond_wait(&stream->cond, &stream->mutex);
    }
    pthread_mutex_unlock(&stream->mutex);
    trevrpc_msquic_stream_lifecycle_release(stream);
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

int trevrpc_msquic_stream_abort_with_error(trevrpc_msquic_stream* stream, uint64_t error_code) {
    if (stream == NULL) {
        return -EINVAL;
    }
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

    QUIC_STATUS status = trevrpc_msquic_test_stream_shutdown(handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, error_code);
    trevrpc_msquic_stream_handle_release(stream);
    return QUIC_FAILED(status) ? (int)status : 0;
}

int trevrpc_msquic_stream_abort(trevrpc_msquic_stream* stream) {
    return trevrpc_msquic_stream_abort_with_error(stream, 0);
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

    QUIC_STATUS status = trevrpc_msquic_test_stream_shutdown(handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, 0);
    trevrpc_msquic_stream_handle_release(stream);
    return QUIC_FAILED(status) ? (int)status : 0;
}

static void trevrpc_msquic_stream_request_deferred_close(trevrpc_msquic_stream* stream) {
    trevrpc_msquic_receive_budget* recv_budget = stream->recv_budget;
    pthread_mutex_lock(&recv_budget->mutex);
    stream->receive_closing = true;
    trevrpc_msquic_recv_pause_remove_budget_locked(recv_budget, stream);
    pthread_mutex_unlock(&recv_budget->mutex);

    pthread_mutex_lock(&stream->mutex);
    HQUIC handle = stream->handle;
    bool graceful = stream->send_closed;
    if (handle != NULL && !stream->close_pending) {
        stream->active_handle_ops++;
    } else {
        handle = NULL;
        if (stream->handle == NULL) {
            stream->shutdown_complete = true;
            stream->closed = true;
        }
    }
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);

    if (handle != NULL) {
        QUIC_STREAM_SHUTDOWN_FLAGS flags =
            graceful ? QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE : QUIC_STREAM_SHUTDOWN_FLAG_ABORT;
        (void)trevrpc_msquic_test_stream_shutdown(handle, flags, 0);
        trevrpc_msquic_stream_handle_release(stream);
    }
}

static void trevrpc_msquic_stream_destroy_owned(trevrpc_msquic_stream* stream, bool owns_close_call) {
    trevrpc_msquic_receive_budget* recv_budget = stream->recv_budget;
    bool release_api = false;

    trevrpc_msquic_test_emit_stream_event(TREV_MSQUIC_TEST_STREAM_CLOSE_STARTED);
    pthread_mutex_lock(&recv_budget->mutex);
    stream->receive_closing = true;
    trevrpc_msquic_recv_pause_remove_budget_locked(recv_budget, stream);
    while (atomic_load_explicit(&stream->active_resume_pins, memory_order_acquire) > 0) {
        pthread_cond_wait(&recv_budget->cond, &recv_budget->mutex);
    }
    pthread_mutex_unlock(&recv_budget->mutex);

    pthread_mutex_lock(&stream->mutex);
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
        (void)trevrpc_msquic_test_stream_shutdown(handle, flags, 0);
        trevrpc_msquic_stream_handle_release(stream);
        pthread_mutex_lock(&stream->mutex);
    }
    while (!trevrpc_msquic_stream_send_ops_idle(stream) || stream->handle != NULL || stream->close_pending ||
           stream->active_handle_ops > 0 || stream->pending_send_count > 0 || stream->active_send_completions > 0 ||
           stream->active_observer_callbacks > 0 || stream->active_lifecycle_refs > 0 ||
           stream->send_capacity_waiters > 0) {
        pthread_cond_wait(&stream->cond, &stream->mutex);
    }
    trevrpc_msquic_recv_release_grant_locked(stream);
    trevrpc_msquic_free_chunks(stream);
    trevrpc_msquic_free_frames(stream);
    trevrpc_msquic_free_send_pool(stream);
    stream->recv_budget = NULL;
    release_api = stream->api_ref_acquired;
    stream->api_ref_acquired = false;
    pthread_mutex_unlock(&stream->mutex);
    trevrpc_msquic_receive_budget_kick(recv_budget);

    trevrpc_msquic_test_emit_stream_event(TREV_MSQUIC_TEST_STREAM_CLOSE_COMPLETED);
    trevrpc_msquic_receive_budget_release(recv_budget);
    if (release_api) {
        trevrpc_msquic_api_release();
    }

    pthread_mutex_lock(&stream->mutex);
    stream->destroy_complete = true;
    pthread_cond_broadcast(&stream->cond);
    size_t retained_calls = owns_close_call ? 1 : 0;
    while (stream->active_close_calls > retained_calls) {
        pthread_cond_wait(&stream->cond, &stream->mutex);
    }
    if (owns_close_call && stream->active_close_calls > 0) {
        stream->active_close_calls--;
    }
    pthread_mutex_unlock(&stream->mutex);

    pthread_cond_destroy(&stream->cond);
    pthread_mutex_destroy(&stream->mutex);
    free(stream);
}

static void trevrpc_msquic_stream_try_destroy_deferred(trevrpc_msquic_stream* stream) {
    bool destroy = false;
    pthread_mutex_lock(&stream->mutex);
    if (stream->destroy_requested && !stream->destroy_started && stream->active_observer_callbacks == 0 &&
        stream->active_lifecycle_refs == 0 && stream->active_send_ops == 0 && stream->active_handle_ops == 0 &&
        stream->active_send_completions == 0 && stream->active_close_calls == 0 &&
        atomic_load_explicit(&stream->active_resume_pins, memory_order_acquire) == 0 &&
        stream->pending_send_count == 0 && stream->send_capacity_waiters == 0 && stream->shutdown_complete &&
        stream->handle == NULL && !stream->close_pending) {
        stream->destroy_started = true;
        destroy = true;
    }
    pthread_mutex_unlock(&stream->mutex);
    if (destroy) {
        trevrpc_msquic_stream_destroy_owned(stream, false);
    }
}

void trevrpc_msquic_stream_close(trevrpc_msquic_stream* stream) {
    if (stream == NULL) {
        return;
    }

    bool observer_reentry = TrevMsQuicObserverStream == stream;
    bool request_shutdown = false;
    bool destroy = false;
    pthread_mutex_lock(&stream->mutex);
    stream->active_close_calls++;
    if (stream->destroy_complete) {
        stream->active_close_calls--;
        pthread_cond_broadcast(&stream->cond);
        pthread_mutex_unlock(&stream->mutex);
        return;
    }
    request_shutdown = !stream->close_shutdown_started;
    stream->close_shutdown_started = true;
    stream->api_closing = true;
    stream->destroy_requested = true;
    stream->observer = NULL;
    stream->observer_context = NULL;
    if (!observer_reentry && !stream->destroy_started) {
        stream->destroy_started = true;
        destroy = true;
    }
    if (observer_reentry && !destroy) {
        stream->active_close_calls--;
    }
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);

    if (observer_reentry && request_shutdown) {
        trevrpc_msquic_stream_request_deferred_close(stream);
    }
    if (destroy) {
        trevrpc_msquic_stream_destroy_owned(stream, true);
        return;
    }
    if (observer_reentry) {
        trevrpc_msquic_stream_try_destroy_deferred(stream);
        return;
    }

    pthread_mutex_lock(&stream->mutex);
    while (!stream->destroy_complete) {
        pthread_cond_wait(&stream->cond, &stream->mutex);
    }
    if (stream->active_close_calls > 0) {
        stream->active_close_calls--;
    }
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);
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
    trevrpc_msquic_receive_policy receive_policy = listener->receive_policy;
    pthread_mutex_unlock(&listener->mutex);

    trevrpc_msquic_conn* conn =
        trevrpc_msquic_conn_alloc(event->NEW_CONNECTION.Connection, max_frame_size, &receive_policy);
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
    trevrpc_msquic_feature_state_init(&conn->features, &listener->features);
    conn->max_frame_size = max_frame_size;
    conn->max_pending_send_bytes = max_pending_send_bytes;
    conn->max_pending_send_count = max_pending_send_count;
    HQUIC connection_handle = trevrpc_msquic_conn_handle_acquire(conn);
    if (connection_handle == NULL) {
        trevrpc_msquic_conn_close(conn);
        trevrpc_msquic_listener_callback_finish(listener);
        return QUIC_STATUS_ABORTED;
    }
    trevrpc_msquic_api()->SetCallbackHandler(connection_handle, (void*)trevrpc_msquic_conn_callback, conn);

    QUIC_STATUS status = trevrpc_msquic_api()->ConnectionSetConfiguration(connection_handle, configuration);
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

static QUIC_STATUS trevrpc_msquic_conn_callback_impl(
    HQUIC connection_handle, trevrpc_msquic_conn* conn, QUIC_CONNECTION_EVENT* event) {
    switch (event->Type) {
    case QUIC_CONNECTION_EVENT_CONNECTED: {
        if (!conn->owns_endpoint && trevrpc_msquic_conn_uses_native_frames(conn)) {
            (void)trevrpc_msquic_api()->ConnectionSendResumptionTicket(
                connection_handle, QUIC_SEND_RESUMPTION_FLAG_NONE, 0, NULL);
        }
        trevrpc_msquic_feature_event feature_event = {
            .kind = TREV_MSQUIC_FEATURE_EVENT_CONNECTED,
        };
        pthread_mutex_lock(&conn->mutex);
        conn->connected_session_resumed = event->CONNECTED.SessionResumed != FALSE;
        trevrpc_msquic_feature_reduce(&conn->features, &feature_event);
        pthread_mutex_unlock(&conn->mutex);
        trevrpc_msquic_conn_publish_ready(conn);
        return QUIC_STATUS_SUCCESS;
    }
    case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED: {
        size_t initial_frame_max_len = trevrpc_msquic_conn_uses_native_frames(conn) ? conn->max_frame_size : 0;
        trevrpc_msquic_stream* stream = trevrpc_msquic_stream_alloc(event->PEER_STREAM_STARTED.Stream,
            conn->max_pending_send_bytes,
            conn->max_pending_send_count,
            conn->max_frame_size,
            initial_frame_max_len,
            conn->max_stream_recv_owned_bytes,
            conn->max_stream_recv_owned_count,
            conn->recv_budget);
        if (stream == NULL) {
            return QUIC_STATUS_OUT_OF_MEMORY;
        }
        trevrpc_msquic_api()->SetCallbackHandler(
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
        pthread_mutex_lock(&conn->mutex);
        trevrpc_msquic_feature_event feature_event = {
            .kind = TREV_MSQUIC_FEATURE_EVENT_TERMINAL,
            .terminal_status = conn->err != 0 ? conn->err : TREV_MSQUIC_ERR_CLOSED,
        };
        trevrpc_msquic_feature_reduce(&conn->features, &feature_event);
        conn->observer_terminal = true;
        int error_code = conn->err;
        pthread_cond_broadcast(&conn->cond);
        pthread_mutex_unlock(&conn->mutex);
        trevrpc_msquic_conn_event shutdown_event = {
            .kind = TREV_MSQUIC_CONN_EVENT_SHUTDOWN_COMPLETE,
            .error_code = error_code,
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
    case QUIC_CONNECTION_EVENT_DATAGRAM_STATE_CHANGED: {
        trevrpc_msquic_feature_event feature_event = {
            .kind = TREV_MSQUIC_FEATURE_EVENT_DATAGRAM_STATE_CHANGED,
            .value = event->DATAGRAM_STATE_CHANGED.SendEnabled != FALSE,
            .max_send_length = event->DATAGRAM_STATE_CHANGED.MaxSendLength,
        };
        pthread_mutex_lock(&conn->mutex);
        trevrpc_msquic_feature_reduce(&conn->features, &feature_event);
        pthread_cond_broadcast(&conn->cond);
        pthread_mutex_unlock(&conn->mutex);
        trevrpc_msquic_conn_publish_ready(conn);
        return QUIC_STATUS_SUCCESS;
    }
    case QUIC_CONNECTION_EVENT_RELIABLE_RESET_NEGOTIATED: {
        trevrpc_msquic_feature_event feature_event = {
            .kind = TREV_MSQUIC_FEATURE_EVENT_RELIABLE_RESET_NEGOTIATED,
#if defined(QUIC_API_HAS_RESET_STREAM_AT_DIALECTS)
            .requested_reset_dialects =
                trevrpc_msquic_reset_dialect_mask_from_native(event->RELIABLE_RESET_NEGOTIATED.RequestedDialectMask),
            .peer_reset_dialects =
                trevrpc_msquic_reset_dialect_mask_from_native(event->RELIABLE_RESET_NEGOTIATED.PeerDialectMask),
            .negotiated_reset_dialects =
                trevrpc_msquic_reset_dialect_mask_from_native(event->RELIABLE_RESET_NEGOTIATED.NegotiatedDialectMask),
#else
            .requested_reset_dialects = conn->features.request.requested_reset_dialects,
            .peer_reset_dialects = event->RELIABLE_RESET_NEGOTIATED.IsNegotiated != FALSE
                                       ? conn->features.request.requested_reset_dialects
                                       : TREV_MSQUIC_RESET_DIALECTS_NONE,
            .negotiated_reset_dialects = event->RELIABLE_RESET_NEGOTIATED.IsNegotiated != FALSE
                                             ? conn->features.request.requested_reset_dialects
                                             : TREV_MSQUIC_RESET_DIALECTS_NONE,
#endif
        };
        pthread_mutex_lock(&conn->mutex);
        trevrpc_msquic_feature_reduce(&conn->features, &feature_event);
        pthread_cond_broadcast(&conn->cond);
        pthread_mutex_unlock(&conn->mutex);
        trevrpc_msquic_conn_publish_ready(conn);
        return QUIC_STATUS_SUCCESS;
    }
    default:
        return QUIC_STATUS_SUCCESS;
    }
}

static QUIC_STATUS QUIC_API trevrpc_msquic_conn_callback(
    HQUIC connection_handle, void* context, QUIC_CONNECTION_EVENT* event) {
    trevrpc_msquic_conn* conn = context;
    trevrpc_msquic_conn_lifecycle_retain(conn);
    QUIC_STATUS status = trevrpc_msquic_conn_callback_impl(connection_handle, conn, event);
    trevrpc_msquic_conn_lifecycle_release(conn);
    return status;
}

static void trevrpc_msquic_receive_copy_prefix(
    const QUIC_BUFFER* buffers, uint32_t buffer_count, uint8_t* out, size_t len) {
    size_t copied = 0;
    for (uint32_t index = 0; index < buffer_count && copied < len; index++) {
        size_t take = buffers[index].Length;
        if (take > len - copied) {
            take = len - copied;
        }
        memcpy(out + copied, buffers[index].Buffer, take);
        copied += take;
    }
}

static int trevrpc_msquic_receive_raw_locked(
    trevrpc_msquic_stream* stream, const QUIC_STREAM_EVENT* event, size_t available, size_t* accepted) {
    *accepted = 0;
    if (available == 0) {
        return 0;
    }
    bool undecided = stream->recv_mode == TREV_MSQUIC_RECV_UNDECIDED;
    size_t charge = sizeof(trevrpc_msquic_chunk) + 1;
    size_t payload = 0;
    if (stream->recv_grant_kind == TREV_MSQUIC_RECV_PAUSE_RAW_CHUNK &&
        stream->recv_grant_bytes > sizeof(trevrpc_msquic_chunk) && stream->recv_grant_count == 1) {
        charge = stream->recv_grant_bytes;
        payload = charge - sizeof(trevrpc_msquic_chunk);
        if (payload > available) {
            payload = available;
            charge = sizeof(trevrpc_msquic_chunk) + payload;
            size_t excess = stream->recv_grant_bytes - charge;
            stream->recv_grant_bytes = charge;
            trevrpc_msquic_recv_release_locked(stream, excess, 0);
        }
        if (trevrpc_msquic_recv_try_reserve_locked(stream, charge, 1, TREV_MSQUIC_RECV_PAUSE_RAW_CHUNK, undecided) !=
            TREV_MSQUIC_RESERVE_OK) {
            return 1;
        }
    } else {
        size_t local_max_bytes = stream->max_recv_owned_bytes;
        size_t local_max_count = stream->max_recv_owned_count;
        if (undecided) {
            size_t workspace = stream->configured_max_frame_size + sizeof(trevrpc_msquic_frame);
            local_max_bytes -= workspace;
            local_max_count -= 2;
        }
        if (stream->recv_owned_count >= local_max_count || stream->recv_owned_bytes >= local_max_bytes ||
            local_max_bytes - stream->recv_owned_bytes <= sizeof(trevrpc_msquic_chunk)) {
            return 1;
        }
        size_t local_payload = local_max_bytes - stream->recv_owned_bytes - sizeof(trevrpc_msquic_chunk);
        trevrpc_msquic_receive_budget* budget = stream->recv_budget;
        pthread_mutex_lock(&budget->mutex);
        size_t aggregate_max_bytes = undecided ? budget->undecided_admission_max_bytes : budget->max_owned_bytes;
        size_t aggregate_max_count = undecided ? budget->undecided_admission_max_count : budget->max_owned_count;
        if (budget->owned_count < aggregate_max_count && budget->owned_bytes < aggregate_max_bytes &&
            aggregate_max_bytes - budget->owned_bytes > sizeof(trevrpc_msquic_chunk)) {
            size_t aggregate_payload = aggregate_max_bytes - budget->owned_bytes - sizeof(trevrpc_msquic_chunk);
            payload = available;
            if (payload > local_payload) {
                payload = local_payload;
            }
            if (payload > aggregate_payload) {
                payload = aggregate_payload;
            }
            charge = sizeof(trevrpc_msquic_chunk) + payload;
            budget->owned_bytes += charge;
            budget->owned_count++;
            stream->recv_owned_bytes += charge;
            stream->recv_owned_count++;
        }
        pthread_mutex_unlock(&budget->mutex);
        if (payload == 0) {
            return 1;
        }
    }

    trevrpc_msquic_chunk* chunk =
        trevrpc_msquic_test_should_fail_receive_allocation(TREV_MSQUIC_TEST_RECV_ALLOC_RAW_CHUNK)
            ? NULL
            : malloc(sizeof(*chunk) + payload);
    if (chunk == NULL) {
        trevrpc_msquic_recv_release_locked(stream, sizeof(*chunk) + payload, 1);
        return -ENOMEM;
    }
    chunk->next = NULL;
    chunk->len = payload;
    chunk->offset = 0;
    chunk->charge_bytes = sizeof(*chunk) + payload;
    chunk->charge_count = 1;
    trevrpc_msquic_receive_copy_prefix(event->RECEIVE.Buffers, event->RECEIVE.BufferCount, chunk->data, payload);
    if (stream->recv_tail != NULL) {
        stream->recv_tail->next = chunk;
    } else {
        stream->recv_head = chunk;
    }
    stream->recv_tail = chunk;
    stream->recv_buffered += payload;
    *accepted = payload;
    return 0;
}

static QUIC_STATUS trevrpc_msquic_stream_callback_impl(
    HQUIC stream_handle, trevrpc_msquic_stream* stream, QUIC_STREAM_EVENT* event) {
    switch (event->Type) {
    case QUIC_STREAM_EVENT_RECEIVE: {
        uint64_t original_total = event->RECEIVE.TotalBufferLength;
        size_t available = original_total > SIZE_MAX ? SIZE_MAX : (size_t)original_total;
        size_t accepted_total = 0;
        uint32_t observer_flags = 0;
        bool fatal = false;
        HQUIC abort_handle = NULL;
        pthread_mutex_lock(&stream->mutex);
        if (stream->recv_mode == TREV_MSQUIC_RECV_FRAMES) {
            int pending = trevrpc_msquic_materialize_pending_frame_locked(stream);
            if (pending < 0) {
                fatal = true;
            } else if (pending == 0) {
                int pumped = trevrpc_msquic_stream_pump_raw_locked(stream);
                if (pumped < 0) {
                    trevrpc_msquic_free_chunks(stream);
                    fatal = true;
                } else {
                    if (stream->frame_head != NULL) {
                        observer_flags |= TREV_MSQUIC_STREAM_OBSERVER_READABLE;
                    }
                    if (stream->recv_head != NULL) {
                        stream->receive_disabled = available > 0 || stream->receive_disabled;
                        stream->receive_waiting_on_raw_pump = available > 0 || stream->receive_waiting_on_raw_pump;
                    } else {
                        for (uint32_t index = 0; index < event->RECEIVE.BufferCount && accepted_total < available;
                            index++) {
                            size_t buffer_len = event->RECEIVE.Buffers[index].Length;
                            if (buffer_len > available - accepted_total) {
                                buffer_len = available - accepted_total;
                            }
                            size_t consumed = 0;
                            int parsed = trevrpc_msquic_stream_append_frame_bytes_locked(
                                stream, event->RECEIVE.Buffers[index].Buffer, buffer_len, &consumed, false);
                            accepted_total += consumed;
                            if (parsed < 0) {
                                fatal = true;
                                break;
                            }
                            if (parsed > 0) {
                                break;
                            }
                        }
                        if (stream->frame_head != NULL) {
                            observer_flags |= TREV_MSQUIC_STREAM_OBSERVER_READABLE;
                        }
                    }
                }
            }
        } else {
            int raw = trevrpc_msquic_receive_raw_locked(stream, event, available, &accepted_total);
            if (raw < 0) {
                fatal = true;
            } else if (accepted_total > 0) {
                observer_flags |= TREV_MSQUIC_STREAM_OBSERVER_READABLE;
            }
        }

        if (!fatal && accepted_total < original_total) {
            if (stream->receive_waiting_on_raw_pump) {
                stream->receive_disabled = true;
            } else if (stream->recv_pause_kind == TREV_MSQUIC_RECV_PAUSE_NONE) {
                trevrpc_msquic_receive_pause_kind kind = stream->recv_mode == TREV_MSQUIC_RECV_FRAMES
                                                             ? TREV_MSQUIC_RECV_PAUSE_FRAME_NODE
                                                             : TREV_MSQUIC_RECV_PAUSE_RAW_CHUNK;
                size_t remaining = (size_t)(original_total - accepted_total);
                size_t raw_resume_payload = remaining < 65536 ? remaining : 65536;
                size_t need = kind == TREV_MSQUIC_RECV_PAUSE_FRAME_NODE
                                  ? sizeof(trevrpc_msquic_frame)
                                  : sizeof(trevrpc_msquic_chunk) + raw_resume_payload;
                size_t count = 1;
                if (stream->recv_mode == TREV_MSQUIC_RECV_FRAMES && stream->frame_parser.header_len == 4 &&
                    stream->frame_parser.body == NULL && stream->frame_parser.body_len > 0) {
                    kind = TREV_MSQUIC_RECV_PAUSE_PARSER_BODY;
                    need = stream->frame_parser.body_len;
                }
                trevrpc_msquic_recv_pause_locked(stream, kind, need, count, true);
            } else {
                stream->receive_disabled = true;
            }
        }
        if (!fatal && accepted_total == original_total && (event->RECEIVE.Flags & QUIC_RECEIVE_FLAG_FIN) != 0) {
            stream->recv_fin = true;
            observer_flags |= TREV_MSQUIC_STREAM_OBSERVER_TERMINAL;
        }
        if (fatal) {
            stream->err = ENOMEM;
            observer_flags |= TREV_MSQUIC_STREAM_OBSERVER_TERMINAL;
            abort_handle = stream->handle;
            if (abort_handle != NULL && !stream->close_pending) {
                stream->active_handle_ops++;
            } else {
                abort_handle = NULL;
            }
        }
        if (stream->err != 0) {
            observer_flags |= TREV_MSQUIC_STREAM_OBSERVER_TERMINAL;
        }
        event->RECEIVE.TotalBufferLength = accepted_total;
        pthread_cond_broadcast(&stream->cond);
        trevrpc_msquic_receive_budget* budget = stream->recv_budget;
        pthread_mutex_unlock(&stream->mutex);
        trevrpc_msquic_receive_budget_kick(budget);
        if (observer_flags != 0) {
            trevrpc_msquic_stream_notify(stream, observer_flags);
        }
        if (abort_handle != NULL) {
            (void)trevrpc_msquic_test_stream_shutdown(abort_handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, 0);
            trevrpc_msquic_stream_handle_release(stream);
        } else if (fatal) {
            /* Synthetic receive fixtures have no provider handle, but still record that the
             * fatal path reached the out-of-lock abort point. */
            trevrpc_msquic_test_emit_stream_event(TREV_MSQUIC_TEST_STREAM_SHUTDOWN_ABORT_RECEIVE);
        }
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
        trevrpc_msquic_stream_notify(stream, TREV_MSQUIC_STREAM_OBSERVER_TERMINAL);
        return QUIC_STATUS_SUCCESS;
    case QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
        pthread_mutex_lock(&stream->mutex);
        stream->err = TREV_MSQUIC_ERR_CLOSED;
        pthread_cond_broadcast(&stream->cond);
        pthread_mutex_unlock(&stream->mutex);
        trevrpc_msquic_stream_notify(stream, TREV_MSQUIC_STREAM_OBSERVER_TERMINAL);
        return QUIC_STATUS_SUCCESS;
    case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
        trevrpc_msquic_stream_shutdown_complete(stream, stream_handle);
        return QUIC_STATUS_SUCCESS;
    default:
        return QUIC_STATUS_SUCCESS;
    }
}

static QUIC_STATUS QUIC_API trevrpc_msquic_stream_callback(
    HQUIC stream_handle, void* context, QUIC_STREAM_EVENT* event) {
    trevrpc_msquic_stream* stream = context;
    trevrpc_msquic_stream_lifecycle_retain(stream);
    QUIC_STATUS status = trevrpc_msquic_stream_callback_impl(stream_handle, stream, event);
    trevrpc_msquic_stream_lifecycle_release(stream);
    return status;
}

#ifdef TREVRPC_MSQUIC_TESTING
size_t trevrpc_msquic_test_receive_minimum_bytes(size_t max_frame_size) {
    size_t required = 0;
    return trevrpc_msquic_receive_minimum_bytes_checked(max_frame_size, &required) ? required : 0;
}

int trevrpc_msquic_test_receive_fixture_create(const trevrpc_msquic_receive_policy* policy,
    size_t max_frame_size,
    size_t stream_count,
    trevrpc_msquic_test_receive_fixture** out_fixture) {
    if (out_fixture == NULL || stream_count == 0) {
        return -EINVAL;
    }
    *out_fixture = NULL;
    max_frame_size = trevrpc_msquic_effective_max_frame_size(max_frame_size);
    trevrpc_msquic_receive_policy effective;
    int err = trevrpc_msquic_receive_policy_effective(policy, max_frame_size, &effective);
    if (err != 0) {
        return -err;
    }
    trevrpc_msquic_test_receive_fixture* fixture = calloc(1, sizeof(*fixture));
    if (fixture == NULL) {
        return -ENOMEM;
    }
    fixture->streams = calloc(stream_count, sizeof(*fixture->streams));
    fixture->budget = trevrpc_msquic_receive_budget_new(&effective, max_frame_size);
    if (fixture->streams == NULL || fixture->budget == NULL) {
        free(fixture->streams);
        trevrpc_msquic_receive_budget_release(fixture->budget);
        free(fixture);
        return -ENOMEM;
    }
    fixture->stream_count = stream_count;
    fixture->connection_ref = true;
    for (size_t index = 0; index < stream_count; index++) {
        trevrpc_msquic_stream* stream = calloc(1, sizeof(*stream));
        if (stream == NULL || !trevrpc_msquic_receive_budget_retain(fixture->budget)) {
            free(stream);
            trevrpc_msquic_test_receive_fixture_destroy(fixture);
            return -ENOMEM;
        }
        atomic_init(&stream->receive_closing, false);
        atomic_init(&stream->active_resume_pins, 0);
        stream->recv_budget = fixture->budget;
        stream->configured_max_frame_size = max_frame_size;
        stream->max_recv_owned_bytes = effective.max_stream_owned_bytes;
        stream->max_recv_owned_count = effective.max_stream_owned_count;
        stream->parser_budgeted = true;
        stream->synthetic_receive_fixture = true;
        stream->max_pending_send_bytes = trevrpc_msquic_effective_max_pending_send_bytes(0);
        stream->max_pending_send_count = trevrpc_msquic_effective_max_pending_send_count(0);
        trevrpc_owned_bytes_init(&stream->pending_frame.body);
        pthread_mutex_init(&stream->mutex, NULL);
        pthread_cond_init(&stream->cond, NULL);
        trevrpc_frame_parser_init_with_allocator(
            &stream->frame_parser, 0, trevrpc_msquic_parser_alloc, trevrpc_msquic_parser_free, stream);
        trevrpc_frame_parser_set_retain_on_allocation_failure(&stream->frame_parser, true);
        fixture->streams[index] = stream;
    }
    *out_fixture = fixture;
    return 0;
}

trevrpc_msquic_stream* trevrpc_msquic_test_receive_fixture_stream(
    trevrpc_msquic_test_receive_fixture* fixture, size_t index) {
    return fixture != NULL && index < fixture->stream_count ? fixture->streams[index] : NULL;
}

trevrpc_msquic_stream* trevrpc_msquic_test_receive_fixture_take_stream(
    trevrpc_msquic_test_receive_fixture* fixture, size_t index) {
    if (fixture == NULL || index >= fixture->stream_count) {
        return NULL;
    }
    trevrpc_msquic_stream* stream = fixture->streams[index];
    fixture->streams[index] = NULL;
    return stream;
}

int trevrpc_msquic_test_receive_inject(trevrpc_msquic_stream* stream,
    const uint8_t* const* buffers,
    const size_t* lengths,
    size_t buffer_count,
    bool fin,
    size_t* accepted) {
    if (stream == NULL || accepted == NULL || buffer_count > UINT32_MAX ||
        (buffer_count > 0 && (buffers == NULL || lengths == NULL))) {
        return -EINVAL;
    }
    *accepted = 0;
    QUIC_BUFFER* native_buffers = buffer_count == 0 ? NULL : calloc(buffer_count, sizeof(*native_buffers));
    if (buffer_count > 0 && native_buffers == NULL) {
        return -ENOMEM;
    }
    uint64_t total = 0;
    for (size_t index = 0; index < buffer_count; index++) {
        if ((buffers[index] == NULL && lengths[index] != 0) || lengths[index] > UINT32_MAX ||
            lengths[index] > UINT64_MAX - total) {
            free(native_buffers);
            return -EINVAL;
        }
        native_buffers[index].Buffer = (uint8_t*)buffers[index];
        native_buffers[index].Length = (uint32_t)lengths[index];
        total += lengths[index];
    }
    QUIC_STREAM_EVENT event = {0};
    event.Type = QUIC_STREAM_EVENT_RECEIVE;
    event.RECEIVE.AbsoluteOffset = 0;
    event.RECEIVE.TotalBufferLength = total;
    event.RECEIVE.Buffers = native_buffers;
    event.RECEIVE.BufferCount = (uint32_t)buffer_count;
    event.RECEIVE.Flags = fin ? QUIC_RECEIVE_FLAG_FIN : QUIC_RECEIVE_FLAG_NONE;
    QUIC_STATUS status = trevrpc_msquic_stream_callback(NULL, stream, &event);
    *accepted = (size_t)event.RECEIVE.TotalBufferLength;
    free(native_buffers);
    return QUIC_FAILED(status) ? -EIO : 0;
}

void trevrpc_msquic_test_receive_snapshot_get(
    trevrpc_msquic_stream* stream, trevrpc_msquic_test_receive_snapshot* snapshot) {
    if (stream == NULL || snapshot == NULL) {
        return;
    }
    memset(snapshot, 0, sizeof(*snapshot));
    pthread_mutex_lock(&stream->mutex);
    snapshot->stream_owned_bytes = stream->recv_owned_bytes;
    snapshot->stream_owned_count = stream->recv_owned_count;
    snapshot->buffered_bytes = stream->recv_buffered;
    snapshot->active_resume_pins = atomic_load_explicit(&stream->active_resume_pins, memory_order_acquire);
    snapshot->paused = stream->recv_pause_kind != TREV_MSQUIC_RECV_PAUSE_NONE;
    snapshot->receive_disabled = stream->receive_disabled;
    snapshot->recv_fin = stream->recv_fin;
    snapshot->pending_frame = stream->pending_frame_valid;
    snapshot->err = stream->err;
    for (trevrpc_msquic_frame* frame = stream->frame_head; frame != NULL; frame = frame->next) {
        snapshot->queued_frames++;
    }
    pthread_mutex_lock(&stream->recv_budget->mutex);
    snapshot->connection_owned_bytes = stream->recv_budget->owned_bytes;
    snapshot->connection_owned_count = stream->recv_budget->owned_count;
    snapshot->resume_scan_count = stream->recv_budget->resume_scan_count;
    pthread_mutex_unlock(&stream->recv_budget->mutex);
    pthread_mutex_unlock(&stream->mutex);
}

void trevrpc_msquic_test_receive_fixture_drop_connection(trevrpc_msquic_test_receive_fixture* fixture) {
    if (fixture != NULL && fixture->connection_ref) {
        fixture->connection_ref = false;
        trevrpc_msquic_receive_budget_release(fixture->budget);
    }
}

void trevrpc_msquic_test_receive_simulate_handle_loss(trevrpc_msquic_stream* stream) {
    if (stream == NULL) {
        return;
    }
    pthread_mutex_lock(&stream->mutex);
    stream->synthetic_receive_fixture = false;
    stream->handle = NULL;
    pthread_mutex_unlock(&stream->mutex);
}

void trevrpc_msquic_test_receive_fixture_destroy(trevrpc_msquic_test_receive_fixture* fixture) {
    if (fixture == NULL) {
        return;
    }
    for (size_t index = 0; index < fixture->stream_count; index++) {
        trevrpc_msquic_stream_close(fixture->streams[index]);
    }
    if (fixture->connection_ref) {
        trevrpc_msquic_receive_budget_release(fixture->budget);
    }
    free(fixture->streams);
    free(fixture);
}
#endif

int trevrpc_msquic_native_core_api_acquire(const QUIC_API_TABLE** out_api) {
    if (out_api == NULL) {
        return -EINVAL;
    }
    int result = trevrpc_msquic_api_acquire();
    if (result != 0) {
        return result < 0 ? result : -result;
    }
    *out_api = TrevMsQuic;
    return 0;
}

void trevrpc_msquic_native_core_api_release(void) {
    trevrpc_msquic_api_release();
}
