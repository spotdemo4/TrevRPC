#ifndef TREVRPC_MSQUIC_OBJECTS_INTERNAL_H
#define TREVRPC_MSQUIC_OBJECTS_INTERNAL_H

#include "trevrpc_msquic_internal.h"

#if __has_include(<msquic.h>)
#include <msquic.h>
#else
#include <inc/msquic.h>
#endif

#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>

#define TREV_MSQUIC_SEND_MAX_BUFFERS 4

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

typedef struct trevrpc_msquic_endpoint_lease {
    atomic_size_t refs;
    const QUIC_API_TABLE* api;
    HQUIC registration;
    HQUIC configuration;
    bool api_ref_acquired;
} trevrpc_msquic_endpoint_lease;

typedef enum trevrpc_msquic_finalizer_kind {
    TREV_MSQUIC_FINALIZE_CONN_DESTROY = 0,
    TREV_MSQUIC_FINALIZE_STREAM_CLOSE,
    TREV_MSQUIC_FINALIZE_STREAM_CLOSE_IMMEDIATE,
    TREV_MSQUIC_FINALIZE_STREAM_DESTROY,
    TREV_MSQUIC_FINALIZE_CONN_CLOSE,
    TREV_MSQUIC_FINALIZE_LISTENER_STOP,
    TREV_MSQUIC_FINALIZE_LISTENER_CLOSE,
} trevrpc_msquic_finalizer_kind;

typedef struct trevrpc_msquic_finalizer_item {
    struct trevrpc_msquic_finalizer_item* next;
    void* object;
    trevrpc_msquic_finalizer_kind kind;
} trevrpc_msquic_finalizer_item;

struct trevrpc_msquic_stream {
    HQUIC handle;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
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
    size_t recv_pause_need_bytes;
    size_t recv_pause_need_count;
    _Atomic size_t active_resume_pins;
    trevrpc_msquic_stream* pause_prev;
    trevrpc_msquic_stream* pause_next;
    size_t recv_grant_bytes;
    size_t recv_grant_count;
    uint64_t stream_id;
    trevrpc_msquic_frame pending_frame;
    size_t active_send_ops;
    size_t active_handle_ops;
    size_t active_send_completions;
    size_t active_lifecycle_refs;
    size_t active_close_calls;
    trevrpc_msquic_stream_observer observer;
    void* observer_context;
    size_t active_observer_callbacks;
    trevrpc_msquic_stream_start_observer start_observer;
    void* start_observer_context;
    size_t active_start_observer_callbacks;
    int start_status;
    bool start_complete;
    size_t send_capacity_waiters;
    size_t max_pending_send_bytes;
    size_t max_pending_send_count;
    size_t pending_send_bytes;
    size_t pending_send_count;
    trevrpc_msquic_send* send_pool;
    size_t send_pool_count;
    trevrpc_msquic_recv_mode recv_mode;
    trevrpc_msquic_receive_pause_kind recv_pause_kind;
    trevrpc_msquic_receive_pause_kind recv_grant_kind;
    enum {
        TREV_MSQUIC_PARSER_ALLOC_NONE,
        TREV_MSQUIC_PARSER_ALLOC_PRESSURE,
        TREV_MSQUIC_PARSER_ALLOC_OOM,
    } parser_alloc_result;
    int err;
    uint64_t peer_send_error;
    uint64_t peer_receive_error;
    bool peer_send_error_set;
    bool peer_receive_error_set;
    bool parser_budgeted;
    bool receive_disabled;
    bool receive_waiting_on_raw_pump;
    bool receive_resume_ready;
    _Atomic bool receive_closing;
    bool pause_listed;
#ifdef TREVRPC_MSQUIC_TESTING
    bool synthetic_receive_fixture;
#endif
    bool pending_frame_valid;
    bool recv_fin;
    bool stream_id_valid;
    bool receive_capable;
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
    bool deferred_finalizer_owned;
    trevrpc_msquic_finalizer_item close_finalizer;
    trevrpc_msquic_finalizer_item destroy_finalizer;
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
    void* endpoint_lease;
    trevrpc_msquic_endpoint_lease_release endpoint_lease_release;
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
    trevrpc_msquic_finalizer_item close_finalizer;
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
    trevrpc_msquic_listener_observer observer;
    void* observer_context;
    trevrpc_msquic_accept_dispatch accept_dispatch;
    void* accept_dispatch_context;
    trevrpc_msquic_context_destroy accept_dispatch_context_destroy;
    trevrpc_msquic_endpoint_lease* endpoint_lease;
    size_t active_observer_callbacks;
    size_t active_callbacks;
    size_t shutdown_waiters;
    bool shutdown_in_progress;
    bool closed;
    trevrpc_msquic_listener* deferred_close_next;
    trevrpc_msquic_finalizer_item stop_finalizer;
    trevrpc_msquic_finalizer_item close_finalizer;
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

#endif
