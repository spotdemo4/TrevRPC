#ifndef TREVRPC_MSQUIC_INTERNAL_H
#define TREVRPC_MSQUIC_INTERNAL_H

#include "trevrpc_msquic.h"
#include "trevrpc_frame_internal.h"
#include "trevrpc_owned_bytes_internal.h"

#define TREV_MSQUIC_CONN_EVENT_CONNECTED 0u
#define TREV_MSQUIC_CONN_EVENT_SHUTDOWN_COMPLETE 1u
#define TREV_MSQUIC_CONN_EVENT_LOCAL_ADDRESS_CHANGED 2u
#define TREV_MSQUIC_CONN_EVENT_PEER_ADDRESS_CHANGED 3u
#define TREV_MSQUIC_CONN_EVENT_RESUMPTION_TICKET_RECEIVED 4u

typedef struct trevrpc_msquic_conn_event {
    uint32_t kind;
    int error_code;
    int session_resumed;
    const uint8_t* resumption_ticket;
    size_t resumption_ticket_len;
} trevrpc_msquic_conn_event;

typedef void (*trevrpc_msquic_conn_observer)(void* context, const trevrpc_msquic_conn_event* event);

int trevrpc_msquic_dial_observed(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    trevrpc_msquic_cancelled_fn cancelled,
    void* cancellation_context,
    const uint8_t* resumption_ticket,
    size_t resumption_ticket_len,
    trevrpc_msquic_conn_observer observer,
    void* observer_context,
    trevrpc_msquic_conn** conn);
void trevrpc_msquic_conn_clear_observer(trevrpc_msquic_conn* conn);
int trevrpc_msquic_stream_abort_with_error(trevrpc_msquic_stream* stream, uint64_t error_code);
intptr_t trevrpc_msquic_stream_read_frame_owned(
    trevrpc_msquic_stream* stream, trevrpc_owned_bytes* body, size_t max_len);
intptr_t trevrpc_msquic_stream_read_frame_owned_timeout(
    trevrpc_msquic_stream* stream, trevrpc_owned_bytes* body, size_t max_len, uint64_t timeout_nanos);
intptr_t trevrpc_msquic_stream_read_frame_owned_ready(
    trevrpc_msquic_stream* stream, trevrpc_owned_bytes* body, size_t max_len);

typedef enum trevrpc_msquic_test_stream_event {
    TREV_MSQUIC_TEST_STREAM_SEND_PREPARE = 0,
    TREV_MSQUIC_TEST_STREAM_SEND_RESERVED = 1,
    TREV_MSQUIC_TEST_STREAM_SEND_CAPACITY_WAIT = 2,
    TREV_MSQUIC_TEST_STREAM_CLOSE_STARTED = 3,
    TREV_MSQUIC_TEST_STREAM_SEND_TERMINAL = 4,
    TREV_MSQUIC_TEST_STREAM_SHUTDOWN_GRACEFUL = 5,
    TREV_MSQUIC_TEST_STREAM_SHUTDOWN_ABORT_RECEIVE = 6,
    TREV_MSQUIC_TEST_STREAM_SHUTDOWN_ABORT = 7,
    TREV_MSQUIC_TEST_STREAM_CLOSE_COMPLETED = 8,
    TREV_MSQUIC_TEST_STREAM_EVENT_COUNT = 9,
} trevrpc_msquic_test_stream_event;

typedef void (*trevrpc_msquic_test_stream_hook)(trevrpc_msquic_test_stream_event event, void* context);

void trevrpc_msquic_test_set_stream_hook(trevrpc_msquic_test_stream_hook hook, void* context);
void trevrpc_msquic_test_fail_next_stream_send(void);
void trevrpc_msquic_test_fail_next_graceful_shutdown(void);
int trevrpc_msquic_test_reliable_reset_negotiated(trevrpc_msquic_conn* conn);
void trevrpc_msquic_test_wait_stream_shutdown_complete(trevrpc_msquic_stream* stream);
int trevrpc_msquic_test_parse_frame_owned(const uint8_t* data,
    size_t len,
    size_t max_len,
    trevrpc_frame_alloc_fn alloc,
    trevrpc_frame_free_fn dealloc,
    void* allocator_context,
    trevrpc_owned_bytes* body);

#endif
