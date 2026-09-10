#ifndef TREVRPC_ENGINE_MSQUIC_INTERNAL_H
#define TREVRPC_ENGINE_MSQUIC_INTERNAL_H

#include "trevrpc_engine.h"
#include "trevrpc_engine_msquic.h"
#include "trevrpc_engine_provider.h"
#include "trevrpc_msquic_accept_internal.h"

int trevrpc_engine_msquic_provider_descriptor_create_v1(const trevrpc_engine_provider_host_v1* host,
    const trevrpc_engine_config_v1* engine_config,
    const trevrpc_engine_msquic_config_v1* provider_config,
    trevrpc_engine_provider_descriptor_v1* out_descriptor);

int trevrpc_engine_msquic_adopt_accepted_connection_with_host_v1(const trevrpc_engine_provider_host_v1* host,
    trevrpc_engine* engine,
    const trevrpc_engine_endpoint_config_v1* config,
    trevrpc_msquic_accepted_connection* accepted,
    trevrpc_engine_handle_v1* out_connection);

#ifndef TREVRPC_GO_PROVIDER_BUILD
int trevrpc_engine_msquic_adopt_accepted_connection_v1(trevrpc_engine* engine,
    const trevrpc_engine_endpoint_config_v1* config,
    trevrpc_msquic_accepted_connection* accepted,
    trevrpc_engine_handle_v1* out_connection);
#endif

#endif
