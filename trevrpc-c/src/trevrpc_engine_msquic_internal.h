#ifndef TREVRPC_ENGINE_MSQUIC_INTERNAL_H
#define TREVRPC_ENGINE_MSQUIC_INTERNAL_H

#include "trevrpc_engine.h"
#include "trevrpc_msquic_accept_internal.h"

int trevrpc_engine_msquic_adopt_accepted_connection_v1(trevrpc_engine* engine,
    const trevrpc_engine_endpoint_config_v1* config,
    trevrpc_msquic_accepted_connection* accepted,
    trevrpc_engine_handle_v1* out_connection);

#endif
