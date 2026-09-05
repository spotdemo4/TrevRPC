#ifndef TREVRPC_EXAMPLES_GREETER_TRANSPORT_H
#define TREVRPC_EXAMPLES_GREETER_TRANSPORT_H

#include "trevrpc_rpc_msquic.h"

#include <stdint.h>
#include <string.h>

static inline int trevrpc_example_transport_parse(const char* value, uint32_t* out_transport) {
    if (value == NULL || out_transport == NULL)
        return -1;
    if (strcmp(value, "native") == 0)
        *out_transport = TREVRPC_RPC_MSQUIC_TRANSPORT_NATIVE;
    else if (strcmp(value, "http3") == 0)
        *out_transport = TREVRPC_RPC_MSQUIC_TRANSPORT_HTTP3;
    else if (strcmp(value, "webtransport") == 0)
        *out_transport = TREVRPC_RPC_MSQUIC_TRANSPORT_WEBTRANSPORT;
    else
        return -1;
    return 0;
}

#endif
