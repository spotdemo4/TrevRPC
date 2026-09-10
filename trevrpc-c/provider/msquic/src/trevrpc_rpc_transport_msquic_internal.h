#ifndef TREVRPC_RPC_TRANSPORT_MSQUIC_INTERNAL_H
#define TREVRPC_RPC_TRANSPORT_MSQUIC_INTERNAL_H

#include "trevrpc_engine_provider.h"
#include "trevrpc_rpc_transport_internal.h"
#include "trevrpc_transport_msquic.h"
#include "trevrpc_transport_provider.h"

int trevrpc_transport_msquic_provider_descriptor_create_v1(const trevrpc_engine_provider_host_v1* host,
    const trevrpc_transport_config_v1* transport_config,
    const trevrpc_transport_msquic_config_v1* provider_config,
    trevrpc_transport_provider_descriptor_v1* out_descriptor);

#ifndef TREVRPC_GO_PROVIDER_BUILD
int trevrpc_transport_msquic_create_rpc_private(const trevrpc_transport_config_v1* transport_config,
    const trevrpc_transport_msquic_config_v1* provider_config,
    trevrpc_rpc_transport** out_transport);
#endif

int trevrpc_rpc_transport_msquic_adopt(trevrpc_rpc_transport* native_transport,
    trevrpc_rpc_transport* h3_transport,
    const trevrpc_rpc_transport_config* config,
    trevrpc_rpc_transport** out_transport);

#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
int trevrpc_rpc_transport_msquic_test_event_refs(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle, uint32_t* out_refs);
int trevrpc_rpc_transport_msquic_test_rollback_shared_listener(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle listener,
    trevrpc_rpc_transport_handle local_child,
    trevrpc_rpc_transport_handle* out_child);
#endif

#endif
