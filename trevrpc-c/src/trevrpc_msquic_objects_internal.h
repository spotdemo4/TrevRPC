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

#endif
