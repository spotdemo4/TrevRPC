#pragma once

#include <trevrpc_rpc.h>

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct trevrpc_cpp_rpc_fake_fixture trevrpc_cpp_rpc_fake_fixture;

#define TREVRPC_CPP_RPC_FAKE_MALFORMED_SERVICE 1u
#define TREVRPC_CPP_RPC_FAKE_MALFORMED_METHOD 2u
#define TREVRPC_CPP_RPC_FAKE_MALFORMED_DATA 3u
#define TREVRPC_CPP_RPC_FAKE_MALFORMED_METADATA 4u

int trevrpc_cpp_rpc_fake_fixture_create(trevrpc_cpp_rpc_fake_fixture** out_fixture,
                                        const trevrpc_rpc_runtime_config_v1* config,
                                        trevrpc_rpc_runtime** out_runtime);
int trevrpc_cpp_rpc_fake_start_endpoint(trevrpc_cpp_rpc_fake_fixture* fixture,
                                        trevrpc_rpc_runtime* runtime, uint32_t mode,
                                        uint64_t operation_id,
                                        trevrpc_rpc_endpoint_v1* out_endpoint);
void trevrpc_cpp_rpc_fake_set_close_result(trevrpc_cpp_rpc_fake_fixture* fixture, int result);
void trevrpc_cpp_rpc_fake_set_persistent_close_result(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                      int result);
void trevrpc_cpp_rpc_fake_set_close_status(trevrpc_cpp_rpc_fake_fixture* fixture, int status);
void trevrpc_cpp_rpc_fake_set_connection_close_result(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                      int result, bool persistent);
void trevrpc_cpp_rpc_fake_set_release_handle_result(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                    int result);
void trevrpc_cpp_rpc_fake_set_stream_release_result(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                    int result, bool persistent);
void trevrpc_cpp_rpc_fake_set_call_release_result(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                  trevrpc_rpc_runtime* runtime, int result,
                                                  bool persistent);
void trevrpc_cpp_rpc_fake_malformed_next_incoming(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                  trevrpc_rpc_runtime* runtime, uint32_t kind);
void trevrpc_cpp_rpc_fake_set_stream_abort_result(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                  int result);
void trevrpc_cpp_rpc_fake_block_send(trevrpc_cpp_rpc_fake_fixture* fixture);
void trevrpc_cpp_rpc_fake_wait_send_entered(trevrpc_cpp_rpc_fake_fixture* fixture);
void trevrpc_cpp_rpc_fake_release_send(trevrpc_cpp_rpc_fake_fixture* fixture);
void trevrpc_cpp_rpc_fake_block_close(trevrpc_cpp_rpc_fake_fixture* fixture);
void trevrpc_cpp_rpc_fake_wait_close_entered(trevrpc_cpp_rpc_fake_fixture* fixture);
void trevrpc_cpp_rpc_fake_release_close(trevrpc_cpp_rpc_fake_fixture* fixture);
int trevrpc_cpp_rpc_fake_force_transport_stop(trevrpc_cpp_rpc_fake_fixture* fixture, int status);
int trevrpc_cpp_rpc_fake_push_connection_ready(trevrpc_cpp_rpc_fake_fixture* fixture);
int trevrpc_cpp_rpc_fake_push_connection_closed(trevrpc_cpp_rpc_fake_fixture* fixture, int status);
int trevrpc_cpp_rpc_fake_push_stream_ready(trevrpc_cpp_rpc_fake_fixture* fixture);
int trevrpc_cpp_rpc_fake_push_stream_readable(trevrpc_cpp_rpc_fake_fixture* fixture);
int trevrpc_cpp_rpc_fake_push_last_send_complete(trevrpc_cpp_rpc_fake_fixture* fixture);
int trevrpc_cpp_rpc_fake_push_last_server_send_complete(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                       bool second_stream);
int trevrpc_cpp_rpc_fake_push_receive(trevrpc_cpp_rpc_fake_fixture* fixture, const uint8_t* data,
                                      size_t data_len);
int trevrpc_cpp_rpc_fake_push_response_message(trevrpc_cpp_rpc_fake_fixture* fixture,
                                               const uint8_t* data, size_t data_len);
int trevrpc_cpp_rpc_fake_push_response_status(trevrpc_cpp_rpc_fake_fixture* fixture,
                                              uint32_t status);
int trevrpc_cpp_rpc_fake_push_stream_receive_fin(trevrpc_cpp_rpc_fake_fixture* fixture);
int trevrpc_cpp_rpc_fake_push_stream_closed(trevrpc_cpp_rpc_fake_fixture* fixture, int status);
int trevrpc_cpp_rpc_fake_push_incoming(trevrpc_cpp_rpc_fake_fixture* fixture, const char* service,
                                       const char* method, uint32_t kind, const uint8_t* body,
                                       size_t body_len);
int trevrpc_cpp_rpc_fake_push_second_incoming(trevrpc_cpp_rpc_fake_fixture* fixture,
                                              const char* service, const char* method,
                                              uint32_t kind, const uint8_t* body, size_t body_len);
int trevrpc_cpp_rpc_fake_push_second_stream_readable(trevrpc_cpp_rpc_fake_fixture* fixture);
int trevrpc_cpp_rpc_fake_push_second_stream_receive_fin(trevrpc_cpp_rpc_fake_fixture* fixture);
int trevrpc_cpp_rpc_fake_push_second_stream_closed(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                   int status);
void trevrpc_cpp_rpc_fake_wait_stream_open_calls(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                 unsigned int minimum_calls);
void trevrpc_cpp_rpc_fake_wait_stream_send_calls(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                 unsigned int minimum_calls);
size_t trevrpc_cpp_rpc_fake_release_handle_count(trevrpc_cpp_rpc_fake_fixture* fixture);

#ifdef __cplusplus
}
#endif
