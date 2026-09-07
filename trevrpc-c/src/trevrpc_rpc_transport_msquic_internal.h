#ifndef TREVRPC_RPC_TRANSPORT_MSQUIC_INTERNAL_H
#define TREVRPC_RPC_TRANSPORT_MSQUIC_INTERNAL_H

#include "trevrpc_rpc_transport_internal.h"

int trevrpc_rpc_transport_msquic_adopt(trevrpc_rpc_transport* native_transport,
    trevrpc_rpc_transport* h3_transport,
    const trevrpc_rpc_transport_config* config,
    trevrpc_rpc_transport** out_transport);

#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
int trevrpc_rpc_transport_msquic_test_event_refs(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle, uint32_t* out_refs);
int trevrpc_rpc_transport_msquic_test_rollback_shared_listener(
    trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle listener,
    trevrpc_rpc_transport_handle local_child,
    trevrpc_rpc_transport_handle* out_child);
#endif

#endif
