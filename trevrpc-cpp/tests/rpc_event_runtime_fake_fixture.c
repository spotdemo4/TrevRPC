#include "rpc_event_runtime_fake_fixture.h"

#include "rpc_fake_transport_support.h"
#include "trevrpc_rpc_internal.h"
#include "trevrpc_wire_internal.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

static fake_transport* fixture_transport(trevrpc_cpp_rpc_fake_fixture* fixture) {
  return (fake_transport*)fixture;
}

int trevrpc_cpp_rpc_fake_fixture_create(trevrpc_cpp_rpc_fake_fixture** out_fixture,
                                        const trevrpc_rpc_runtime_config_v1* config,
                                        trevrpc_rpc_runtime** out_runtime) {
  fake_transport* fake;
  int result;
  if (out_fixture == NULL || config == NULL || out_runtime == NULL) {
    return -EINVAL;
  }
  fake = fake_create();
  if (fake == NULL) {
    return -ENOMEM;
  }
  result = trevrpc_rpc_runtime_adopt_transport_v1(config, &fake->base, out_runtime);
  if (result != 0) {
    fake_destroy(&fake->base);
    return result;
  }
  *out_fixture = (trevrpc_cpp_rpc_fake_fixture*)fake;
  return 0;
}

int trevrpc_cpp_rpc_fake_start_endpoint(trevrpc_cpp_rpc_fake_fixture* fixture,
                                        trevrpc_rpc_runtime* runtime, uint32_t mode,
                                        uint64_t operation_id,
                                        trevrpc_rpc_endpoint_v1* out_endpoint) {
  trevrpc_rpc_transport_endpoint_config config = {0};
  fake_transport* fake;
  if (fixture == NULL || runtime == NULL) {
    return -EINVAL;
  }
  fake = fixture_transport(fixture);
  pthread_mutex_lock(&fake->mutex);
  if (mode == TREVRPC_RPC_ENDPOINT_SERVER) {
    fake->listener_closed = false;
  } else if (mode == TREVRPC_RPC_ENDPOINT_CLIENT) {
    fake->connection_closed = false;
  }
  pthread_mutex_unlock(&fake->mutex);
  return trevrpc_rpc_runtime_start_transport_endpoint_v1(runtime, &config, mode, operation_id,
                                                         out_endpoint);
}

void trevrpc_cpp_rpc_fake_set_close_result(trevrpc_cpp_rpc_fake_fixture* fixture, int result) {
  atomic_store_explicit(&fixture_transport(fixture)->close_result, result, memory_order_release);
}

void trevrpc_cpp_rpc_fake_set_persistent_close_result(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                      int result) {
  fake_transport* fake = fixture_transport(fixture);
  atomic_store_explicit(&fake->close_result, result, memory_order_release);
  atomic_store_explicit(&fake->close_result_persistent, result != 0, memory_order_release);
}

void trevrpc_cpp_rpc_fake_set_close_status(trevrpc_cpp_rpc_fake_fixture* fixture, int status) {
  atomic_store_explicit(&fixture_transport(fixture)->close_status, status, memory_order_release);
}

void trevrpc_cpp_rpc_fake_set_connection_close_result(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                      int result, bool persistent) {
  fake_transport* fake = fixture_transport(fixture);
  atomic_store_explicit(&fake->connection_close_result, result, memory_order_release);
  atomic_store_explicit(&fake->connection_close_result_persistent, persistent,
                        memory_order_release);
}

void trevrpc_cpp_rpc_fake_set_release_handle_result(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                    int result) {
  fake_transport* fake = fixture_transport(fixture);
  atomic_store_explicit(&fake->stream_release_handle_result, result, memory_order_release);
  atomic_store_explicit(&fake->stream_release_handle_result_persistent, result != 0,
                        memory_order_release);
  atomic_store_explicit(&fake->call_release_handle_result, result, memory_order_release);
  atomic_store_explicit(&fake->call_release_handle_result_persistent, result != 0,
                        memory_order_release);
}

void trevrpc_cpp_rpc_fake_set_stream_release_result(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                    int result, bool persistent) {
  fake_transport* fake = fixture_transport(fixture);
  atomic_store_explicit(&fake->stream_release_handle_result, result, memory_order_release);
  atomic_store_explicit(&fake->stream_release_handle_result_persistent, persistent,
                        memory_order_release);
}

void trevrpc_cpp_rpc_fake_set_call_release_result(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                  trevrpc_rpc_runtime* runtime, int result,
                                                  bool persistent) {
  (void)fixture;
  trevrpc_rpc_internal_test_set_call_release_result(runtime, result, persistent);
}

void trevrpc_cpp_rpc_fake_malformed_next_incoming(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                  trevrpc_rpc_runtime* runtime, uint32_t kind) {
  (void)fixture;
  trevrpc_rpc_internal_test_malformed_next_incoming(runtime, kind);
}

void trevrpc_cpp_rpc_fake_set_stream_abort_result(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                  int result) {
  fixture_transport(fixture)->stream_abort_result = result;
}

void trevrpc_cpp_rpc_fake_block_send(trevrpc_cpp_rpc_fake_fixture* fixture) {
  fake_transport* fake = fixture_transport(fixture);
  pthread_mutex_lock(&fake->mutex);
  fake->send_blocked = true;
  fake->send_entered = false;
  fake->send_release = false;
  pthread_mutex_unlock(&fake->mutex);
}

void trevrpc_cpp_rpc_fake_wait_send_entered(trevrpc_cpp_rpc_fake_fixture* fixture) {
  fake_transport* fake = fixture_transport(fixture);
  pthread_mutex_lock(&fake->mutex);
  while (!fake->send_entered) {
    pthread_cond_wait(&fake->condition, &fake->mutex);
  }
  pthread_mutex_unlock(&fake->mutex);
}

void trevrpc_cpp_rpc_fake_release_send(trevrpc_cpp_rpc_fake_fixture* fixture) {
  fake_transport* fake = fixture_transport(fixture);
  pthread_mutex_lock(&fake->mutex);
  fake->send_release = true;
  fake->send_blocked = false;
  pthread_cond_broadcast(&fake->condition);
  pthread_mutex_unlock(&fake->mutex);
}

void trevrpc_cpp_rpc_fake_block_close(trevrpc_cpp_rpc_fake_fixture* fixture) {
  fake_transport* fake = fixture_transport(fixture);
  pthread_mutex_lock(&fake->mutex);
  fake->close_blocked = true;
  pthread_mutex_unlock(&fake->mutex);
}

void trevrpc_cpp_rpc_fake_wait_close_entered(trevrpc_cpp_rpc_fake_fixture* fixture) {
  fake_transport* fake = fixture_transport(fixture);
  pthread_mutex_lock(&fake->mutex);
  while (!fake->close_entered) {
    pthread_cond_wait(&fake->condition, &fake->mutex);
  }
  pthread_mutex_unlock(&fake->mutex);
}

void trevrpc_cpp_rpc_fake_release_close(trevrpc_cpp_rpc_fake_fixture* fixture) {
  fake_transport* fake = fixture_transport(fixture);
  pthread_mutex_lock(&fake->mutex);
  fake->close_release = true;
  pthread_cond_broadcast(&fake->condition);
  pthread_mutex_unlock(&fake->mutex);
}

int trevrpc_cpp_rpc_fake_force_transport_stop(trevrpc_cpp_rpc_fake_fixture* fixture, int status) {
  fake_transport* fake = fixture_transport(fixture);
  pthread_mutex_lock(&fake->mutex);
  fake->close_requested = true;
  pthread_mutex_unlock(&fake->mutex);
  return fake_push_status_event(
      fake, TREVRPC_RPC_TRANSPORT_EVENT_STOPPED,
      TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL,
      (trevrpc_rpc_transport_handle){0}, (trevrpc_rpc_transport_handle){0}, status);
}

int trevrpc_cpp_rpc_fake_push_connection_ready(trevrpc_cpp_rpc_fake_fixture* fixture) {
  return fake_push_event(fixture_transport(fixture), TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
                         TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL |
                             TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
                         fake_connection_handle, (trevrpc_rpc_transport_handle){0});
}

int trevrpc_cpp_rpc_fake_push_connection_closed(trevrpc_cpp_rpc_fake_fixture* fixture, int status) {
  return fake_push_status_event(
      fixture_transport(fixture), TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED,
      TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
          TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
      fake_connection_handle, (trevrpc_rpc_transport_handle){0}, status);
}

int trevrpc_cpp_rpc_fake_push_stream_ready(trevrpc_cpp_rpc_fake_fixture* fixture) {
  return fake_push_event(fixture_transport(fixture), TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
                         TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL |
                             TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
                         fake_stream_handle, fake_connection_handle);
}

int trevrpc_cpp_rpc_fake_push_stream_readable(trevrpc_cpp_rpc_fake_fixture* fixture) {
  return fake_push_event(fixture_transport(fixture), TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
                         TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
                             TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
                         fake_stream_handle, fake_connection_handle);
}

int trevrpc_cpp_rpc_fake_push_last_send_complete(trevrpc_cpp_rpc_fake_fixture* fixture) {
  fake_transport* fake = fixture_transport(fixture);
  uint64_t operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
  if (operation_id == 0) {
    return -EAGAIN;
  }
  return fake_push_operation_event(fake, TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
                                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL |
                                       TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
                                   fake_stream_handle, fake_connection_handle, operation_id);
}

int trevrpc_cpp_rpc_fake_push_last_server_send_complete(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                        bool second_stream) {
  fake_transport* fake = fixture_transport(fixture);
  uint64_t operation_id = atomic_load_explicit(&fake->last_send_operation_id, memory_order_acquire);
  if (operation_id == 0) {
    return -EAGAIN;
  }
  return fake_push_operation_event(fake, TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
                                   TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL |
                                       TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
                                   second_stream ? fake_second_stream_handle : fake_stream_handle,
                                   fake_listener_handle, operation_id);
}

int trevrpc_cpp_rpc_fake_push_receive(trevrpc_cpp_rpc_fake_fixture* fixture, const uint8_t* data,
                                      size_t data_len) {
  if (fixture == NULL || (data == NULL && data_len != 0)) {
    return -EINVAL;
  }
  return fake_push_receive(fixture_transport(fixture), data, data_len);
}

int trevrpc_cpp_rpc_fake_push_response_message(trevrpc_cpp_rpc_fake_fixture* fixture,
                                               const uint8_t* data, size_t data_len) {
  fake_transport* fake;
  uint8_t* frame = NULL;
  size_t frame_len = 0;
  int result;
  if (fixture == NULL || (data == NULL && data_len != 0)) {
    return -EINVAL;
  }
  fake = fixture_transport(fixture);
  result = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_MESSAGE,
                                            TREVRPC_RPC_STATUS_OK, NULL, 0, data, data_len, NULL,
                                            1024u * 1024u, &frame, &frame_len);
  if (result != 0) {
    return result;
  }
  if (frame_len < 4) {
    free(frame);
    return -EPROTO;
  }
  result = fake_push_receive(fake, frame + 4, frame_len - 4);
  free(frame);
  return result;
}

int trevrpc_cpp_rpc_fake_push_response_status(trevrpc_cpp_rpc_fake_fixture* fixture,
                                              uint32_t status) {
  fake_transport* fake;
  uint8_t* frame = NULL;
  size_t frame_len = 0;
  int result;
  if (fixture == NULL) {
    return -EINVAL;
  }
  fake = fixture_transport(fixture);
  result = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_STATUS, status, NULL, 0, NULL,
                                            0, NULL, 1024u * 1024u, &frame, &frame_len);
  if (result != 0) {
    return result;
  }
  if (frame_len < 4) {
    free(frame);
    return -EPROTO;
  }
  result = fake_push_receive(fake, frame + 4, frame_len - 4);
  free(frame);
  return result;
}

int trevrpc_cpp_rpc_fake_push_stream_receive_fin(trevrpc_cpp_rpc_fake_fixture* fixture) {
  if (fixture == NULL) {
    return -EINVAL;
  }
  return fake_push_event(
      fixture_transport(fixture), TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN,
      TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
          TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN,
      fake_stream_handle, fake_connection_handle);
}

int trevrpc_cpp_rpc_fake_push_stream_closed(trevrpc_cpp_rpc_fake_fixture* fixture, int status) {
  return fake_push_operation_status_event(
      fixture_transport(fixture), TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
      TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
      fake_stream_handle, fake_connection_handle, 0, status);
}

static int trevrpc_cpp_rpc_fake_push_incoming_impl(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                   const char* service, const char* method,
                                                   uint32_t kind, const uint8_t* body,
                                                   size_t body_len, bool second_stream) {
  fake_transport* fake = fixture_transport(fixture);
  uint8_t* frame = NULL;
  size_t frame_len = 0;
  char metadata_key[] = "empty-incoming";
  trevrpc_metadata_entry metadata_entry = {
      .key = metadata_key,
      .key_len = sizeof(metadata_key) - 1u,
      .value = NULL,
      .value_len = 0,
  };
  trevrpc_metadata metadata = {
      .entries = &metadata_entry,
      .entries_len = 1,
  };
  int result;
  if (service == NULL || method == NULL || (body == NULL && body_len != 0)) {
    return -EINVAL;
  }
  result = trevrpc_wire_encode_request_view(
      service, strlen(service), method, strlen(method), kind, TREVRPC_RPC_ABI_VERSION, body,
      body_len, &metadata, TREVRPC_RPC_DEADLINE_INFINITE, 1024u * 1024u, &frame, &frame_len);
  if (result != 0) {
    return result;
  }
  if (frame_len < 4) {
    free(frame);
    return -EPROTO;
  }
  result = fake_push_incoming_stream_for_handle(
      fake, second_stream ? fake_second_stream_handle : fake_stream_handle, frame + 4,
      frame_len - 4);
  free(frame);
  return result;
}

int trevrpc_cpp_rpc_fake_push_incoming(trevrpc_cpp_rpc_fake_fixture* fixture, const char* service,
                                       const char* method, uint32_t kind, const uint8_t* body,
                                       size_t body_len) {
  return trevrpc_cpp_rpc_fake_push_incoming_impl(fixture, service, method, kind, body, body_len,
                                                 false);
}

int trevrpc_cpp_rpc_fake_push_second_incoming(trevrpc_cpp_rpc_fake_fixture* fixture,
                                              const char* service, const char* method,
                                              uint32_t kind, const uint8_t* body, size_t body_len) {
  return trevrpc_cpp_rpc_fake_push_incoming_impl(fixture, service, method, kind, body, body_len,
                                                 true);
}

int trevrpc_cpp_rpc_fake_push_second_stream_readable(trevrpc_cpp_rpc_fake_fixture* fixture) {
  return fake_push_event(fixture_transport(fixture), TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
                         TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
                             TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
                         fake_second_stream_handle, fake_listener_handle);
}

int trevrpc_cpp_rpc_fake_push_second_stream_receive_fin(trevrpc_cpp_rpc_fake_fixture* fixture) {
  return fake_push_event(fixture_transport(fixture), TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN,
                         TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL |
                             TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
                             TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
                         fake_second_stream_handle, fake_listener_handle);
}

int trevrpc_cpp_rpc_fake_push_second_stream_closed(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                   int status) {
  return fake_push_operation_status_event(
      fixture_transport(fixture), TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
      TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
          TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER,
      fake_second_stream_handle, fake_listener_handle, 0, status);
}

void trevrpc_cpp_rpc_fake_wait_stream_open_calls(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                 unsigned int minimum_calls) {
  fake_transport* fake = fixture_transport(fixture);
  pthread_mutex_lock(&fake->mutex);
  while (atomic_load_explicit(&fake->stream_open_calls, memory_order_acquire) < minimum_calls) {
    pthread_cond_wait(&fake->condition, &fake->mutex);
  }
  pthread_mutex_unlock(&fake->mutex);
}

void trevrpc_cpp_rpc_fake_wait_stream_send_calls(trevrpc_cpp_rpc_fake_fixture* fixture,
                                                 unsigned int minimum_calls) {
  fake_transport* fake = fixture_transport(fixture);
  pthread_mutex_lock(&fake->mutex);
  while (atomic_load_explicit(&fake->stream_send_calls, memory_order_acquire) < minimum_calls) {
    pthread_cond_wait(&fake->condition, &fake->mutex);
  }
  pthread_mutex_unlock(&fake->mutex);
}

size_t trevrpc_cpp_rpc_fake_release_handle_count(trevrpc_cpp_rpc_fake_fixture* fixture) {
  return fake_release_handle_count(fixture_transport(fixture));
}
