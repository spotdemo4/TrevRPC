#ifndef TREVRPC_WIRE_ABI6_INTERNAL_H
#define TREVRPC_WIRE_ABI6_INTERNAL_H

#include "trevrpc_ownership_abi6_internal.h"
#include "trevrpc_wire_internal.h"

int trevrpc_wire_decode_response_owned(trevrpc_owned_bytes* data, trevrpc_inbound_response** out_response);
int trevrpc_wire_decode_stream_frame_owned(trevrpc_owned_bytes* data, trevrpc_inbound_stream_frame** out_frame);
int trevrpc_wire_decode_stream_frame_owned_diagnostic(
    trevrpc_owned_bytes* data, trevrpc_inbound_stream_frame** out_frame, trevrpc_wire_diagnostic* diagnostic);

#endif
