#ifndef TREVRPC_RPC_TRANSPORT_ENGINE_INTERNAL_H
#define TREVRPC_RPC_TRANSPORT_ENGINE_INTERNAL_H

#include "trevrpc_engine.h"
#include "trevrpc_msquic_accept_internal.h"
#include "trevrpc_rpc_transport_internal.h"

int trevrpc_rpc_transport_engine_adopt(trevrpc_engine* engine, trevrpc_rpc_transport** out_transport);
int trevrpc_rpc_transport_engine_adopt_accepted_connection(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_endpoint_config* config,
    trevrpc_msquic_accepted_connection* accepted,
    trevrpc_rpc_transport_handle* out_connection);

#endif
