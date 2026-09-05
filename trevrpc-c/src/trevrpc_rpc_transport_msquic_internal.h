#ifndef TREVRPC_RPC_TRANSPORT_MSQUIC_INTERNAL_H
#define TREVRPC_RPC_TRANSPORT_MSQUIC_INTERNAL_H

#include "trevrpc_rpc_transport_internal.h"

int trevrpc_rpc_transport_msquic_adopt(trevrpc_rpc_transport* native_transport,
    trevrpc_rpc_transport* h3_transport,
    const trevrpc_rpc_transport_config* config,
    trevrpc_rpc_transport** out_transport);

#endif
