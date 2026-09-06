#pragma once

#include <trevrpc_rpc.h>

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct trevrpc_cpp_rpc_fake_fixture trevrpc_cpp_rpc_fake_fixture;

int trevrpc_cpp_rpc_fake_fixture_create(trevrpc_cpp_rpc_fake_fixture** out_fixture,
    trevrpc_rpc_runtime** out_runtime);
int trevrpc_cpp_rpc_fake_start_endpoint(trevrpc_cpp_rpc_fake_fixture* fixture,
    trevrpc_rpc_runtime* runtime,
    uint32_t mode,
    uint64_t operation_id,
    trevrpc_rpc_endpoint_v1* out_endpoint);
void trevrpc_cpp_rpc_fake_set_close_result(trevrpc_cpp_rpc_fake_fixture* fixture, int result);
void trevrpc_cpp_rpc_fake_set_close_status(trevrpc_cpp_rpc_fake_fixture* fixture, int status);
void trevrpc_cpp_rpc_fake_block_close(trevrpc_cpp_rpc_fake_fixture* fixture);
void trevrpc_cpp_rpc_fake_wait_close_entered(trevrpc_cpp_rpc_fake_fixture* fixture);
void trevrpc_cpp_rpc_fake_release_close(trevrpc_cpp_rpc_fake_fixture* fixture);
int trevrpc_cpp_rpc_fake_push_connection_ready(trevrpc_cpp_rpc_fake_fixture* fixture);
int trevrpc_cpp_rpc_fake_push_connection_closed(trevrpc_cpp_rpc_fake_fixture* fixture, int status);
int trevrpc_cpp_rpc_fake_push_stream_ready(trevrpc_cpp_rpc_fake_fixture* fixture);
int trevrpc_cpp_rpc_fake_push_stream_readable(trevrpc_cpp_rpc_fake_fixture* fixture);
int trevrpc_cpp_rpc_fake_push_last_send_complete(trevrpc_cpp_rpc_fake_fixture* fixture);
int trevrpc_cpp_rpc_fake_push_stream_closed(trevrpc_cpp_rpc_fake_fixture* fixture, int status);
int trevrpc_cpp_rpc_fake_push_incoming(trevrpc_cpp_rpc_fake_fixture* fixture,
    const char* service,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len);
void trevrpc_cpp_rpc_fake_wait_stream_open_calls(
    trevrpc_cpp_rpc_fake_fixture* fixture, unsigned int minimum_calls);
void trevrpc_cpp_rpc_fake_wait_stream_send_calls(
    trevrpc_cpp_rpc_fake_fixture* fixture, unsigned int minimum_calls);

#ifdef __cplusplus
}
#endif
