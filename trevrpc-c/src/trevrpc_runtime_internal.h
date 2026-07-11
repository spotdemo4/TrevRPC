#ifndef TREVRPC_RUNTIME_INTERNAL_H
#define TREVRPC_RUNTIME_INTERNAL_H

#include "trevrpc.h"

#include "trevrpc_msquic_internal.h"

int trevrpc_client_connect_observed(const char* host,
    uint16_t port,
    const trevrpc_config* config,
    trevrpc_msquic_cancelled_fn cancelled,
    void* cancellation_context,
    const uint8_t* resumption_ticket,
    size_t resumption_ticket_len,
    trevrpc_msquic_conn_observer observer,
    void* observer_context,
    trevrpc_client** client);
void trevrpc_client_clear_observer(trevrpc_client* client);
void trevrpc_stream_set_release(trevrpc_stream* stream, void (*release)(void* context), void* context);

#endif
