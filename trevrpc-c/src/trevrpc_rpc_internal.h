#ifndef TREVRPC_RPC_INTERNAL_H
#define TREVRPC_RPC_INTERNAL_H

#include "trevrpc_rpc.h"
#include "trevrpc_rpc_transport_internal.h"
#include "trevrpc_wire_internal.h"

int trevrpc_rpc_validate_runtime_config(const trevrpc_rpc_runtime_config_v1* config);

int trevrpc_rpc_runtime_adopt_transport_v1(
    const trevrpc_rpc_runtime_config_v1* config, trevrpc_rpc_transport* transport, trevrpc_rpc_runtime** out_runtime);
trevrpc_wire_diagnostic_reason trevrpc_rpc_stream_last_receive_diagnostic(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream);
void trevrpc_rpc_internal_test_fail_next_receive_copy(trevrpc_rpc_runtime* runtime);
void trevrpc_rpc_internal_test_set_call_release_result(trevrpc_rpc_runtime* runtime, int result, bool persistent);
void trevrpc_rpc_internal_test_malformed_next_incoming(trevrpc_rpc_runtime* runtime, uint32_t kind);

#define TREVRPC_RPC_INTERNAL_TEST_MALFORMED_SERVICE 1u
#define TREVRPC_RPC_INTERNAL_TEST_MALFORMED_METHOD 2u
#define TREVRPC_RPC_INTERNAL_TEST_MALFORMED_DATA 3u
#define TREVRPC_RPC_INTERNAL_TEST_MALFORMED_METADATA 4u

int trevrpc_rpc_runtime_start_transport_endpoint_v1(trevrpc_rpc_runtime* runtime,
    const trevrpc_rpc_transport_endpoint_config* config,
    uint32_t mode,
    uint64_t operation_id,
    trevrpc_rpc_endpoint_v1* out_endpoint);

#endif
