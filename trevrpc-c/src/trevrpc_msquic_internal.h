#ifndef TREVRPC_MSQUIC_INTERNAL_H
#define TREVRPC_MSQUIC_INTERNAL_H

#include "trevrpc_msquic.h"

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

typedef enum trevrpc_msquic_test_stream_event {
    TREV_MSQUIC_TEST_STREAM_SEND_PREPARE = 0,
    TREV_MSQUIC_TEST_STREAM_SEND_RESERVED = 1,
    TREV_MSQUIC_TEST_STREAM_SEND_CAPACITY_WAIT = 2,
    TREV_MSQUIC_TEST_STREAM_CLOSE_STARTED = 3,
    TREV_MSQUIC_TEST_STREAM_SEND_TERMINAL = 4,
    TREV_MSQUIC_TEST_STREAM_EVENT_COUNT = 5,
} trevrpc_msquic_test_stream_event;

typedef void (*trevrpc_msquic_test_stream_hook)(trevrpc_msquic_test_stream_event event, void* context);

void trevrpc_msquic_test_set_stream_hook(trevrpc_msquic_test_stream_hook hook, void* context);
void trevrpc_msquic_test_fail_next_stream_send(void);
void trevrpc_msquic_test_fail_next_graceful_shutdown(void);
void trevrpc_msquic_test_wait_stream_shutdown_complete(trevrpc_msquic_stream* stream);

#endif
