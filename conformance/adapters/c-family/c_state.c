#include "peer.h"

#include "rpc_fake_transport_support.h"
#include "trevrpc_rpc.h"
#include "trevrpc_rpc_internal.h"
#include "trevrpc_wire_internal.h"

#include <errno.h>
#include <poll.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

static void cf_append_close_count(cf_json *payload, size_t close_count) {
  cf_json_append(payload, ",\"transport_close_count\":");
  cf_json_append_size_string(payload, close_count);
}

static void cf_classify_receive_error(trevrpc_rpc_runtime *runtime,
                                      trevrpc_rpc_stream_v1 stream,
                                      int native_error, cf_error *error) {
  trevrpc_wire_diagnostic_reason reason =
      trevrpc_rpc_stream_last_receive_diagnostic(runtime, stream);
  if (native_error == CF_WIRE_ERR_FRAME_TOO_LARGE ||
      native_error == -EMSGSIZE) {
    cf_error_set(error, "frame_too_large",
                 TREVRPC_RPC_STATUS_RESOURCE_EXHAUSTED);
  } else if (reason == TREVRPC_WIRE_DIAGNOSTIC_UNSUPPORTED_FRAME_KIND) {
    cf_error_set(error, "unsupported_frame_kind",
                 TREVRPC_RPC_STATUS_INVALID_ARGUMENT);
  } else if (reason == TREVRPC_WIRE_DIAGNOSTIC_INVALID_METADATA) {
    cf_error_set(error, "invalid_metadata", TREVRPC_RPC_STATUS_INTERNAL);
  } else {
    cf_error_set(error, "malformed_protobuf", TREVRPC_RPC_STATUS_INTERNAL);
  }
}

typedef struct cf_body_list {
  cf_bytes *values;
  size_t count;
  size_t capacity;
} cf_body_list;

static void cf_body_list_reset(cf_body_list *list) {
  for (size_t i = 0; i < list->count; ++i) {
    free(list->values[i].data);
  }
  free(list->values);
  memset(list, 0, sizeof(*list));
}

static int cf_body_list_append(cf_body_list *list, const uint8_t *body,
                               size_t body_len) {
  if (list->count == list->capacity) {
    size_t capacity = list->capacity == 0 ? 4 : list->capacity * 2;
    if (capacity < list->capacity ||
        capacity > SIZE_MAX / sizeof(*list->values)) {
      return -EOVERFLOW;
    }
    cf_bytes *values = realloc(list->values, capacity * sizeof(*values));
    if (values == NULL) {
      return -ENOMEM;
    }
    list->values = values;
    list->capacity = capacity;
  }
  uint8_t *copy = NULL;
  if (body_len != 0) {
    copy = malloc(body_len);
    if (copy == NULL) {
      return -ENOMEM;
    }
    memcpy(copy, body, body_len);
  }
  list->values[list->count++] = (cf_bytes){copy, body_len};
  return 0;
}

typedef struct cf_rpc_state_harness {
  fake_transport *fake;
  trevrpc_rpc_runtime *runtime;
  trevrpc_rpc_wake_source_v1 wake;
  trevrpc_rpc_endpoint_v1 endpoint;
  trevrpc_rpc_call_v1 call;
  trevrpc_rpc_stream_v1 stream;
  bool endpoint_ready;
  bool call_open;
  bool call_ready;
  size_t close_count;
} cf_rpc_state_harness;

static int cf_wait_event(cf_rpc_state_harness *harness,
                         trevrpc_rpc_event **out_event) {
  for (unsigned attempt = 0; attempt < 1000; ++attempt) {
    int result = trevrpc_rpc_runtime_next_event(harness->runtime, out_event);
    if (result == 0) {
      return 0;
    }
    if (result != -EAGAIN) {
      return result;
    }
    struct pollfd descriptor = {
        .fd = (int)harness->wake.native_handle,
        .events = POLLIN,
        .revents = 0,
    };
    if (poll(&descriptor, 1, 10) < 0 && errno != EINTR) {
      return -errno;
    }
  }
  return -ETIMEDOUT;
}

static int cf_expect_event(cf_rpc_state_harness *harness, uint32_t kind,
                           trevrpc_rpc_event_info_v1 *out_info) {
  trevrpc_rpc_event *event = NULL;
  int result = cf_wait_event(harness, &event);
  if (result != 0) {
    return result;
  }
  trevrpc_rpc_event_info_v1 info;
  if (trevrpc_rpc_event_info_v1_init(&info, sizeof(info)) != 0 ||
      trevrpc_rpc_event_get_info_v1(event, &info) != 0 || info.kind != kind) {
    trevrpc_rpc_event_release(event);
    return -EPROTO;
  }
  if (out_info != NULL) {
    *out_info = info;
  }
  trevrpc_rpc_event_release(event);
  return 0;
}

static int cf_rpc_state_harness_start(cf_rpc_state_harness *harness,
                                      uint32_t kind) {
  trevrpc_rpc_runtime_config_v1 config;
  trevrpc_rpc_transport_endpoint_config endpoint_config = {0};
  trevrpc_rpc_call_config_v1 call_config;
  memset(harness, 0, sizeof(*harness));
  harness->fake = fake_create();
  if (harness->fake == NULL ||
      trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config)) != 0 ||
      trevrpc_rpc_runtime_adopt_transport_v1(&config, &harness->fake->base,
                                             &harness->runtime) != 0 ||
      trevrpc_rpc_wake_source_v1_init(&harness->wake, sizeof(harness->wake)) !=
          0 ||
      trevrpc_rpc_runtime_get_wake_source_v1(harness->runtime,
                                             &harness->wake) != 0 ||
      trevrpc_rpc_runtime_start_transport_endpoint_v1(
          harness->runtime, &endpoint_config, TREVRPC_RPC_ENDPOINT_CLIENT, 1,
          &harness->endpoint) != 0) {
    return -EIO;
  }
  if (fake_push_event(
          harness->fake, TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
          TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL |
              TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
          fake_connection_handle, (trevrpc_rpc_transport_handle){0}) != 0 ||
      cf_expect_event(harness, TREVRPC_RPC_EVENT_ENDPOINT_READY, NULL) != 0) {
    return -EIO;
  }
  harness->endpoint_ready = true;

  if (trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) != 0) {
    return -EIO;
  }
  call_config.kind = kind;
  call_config.service = "conformance.State";
  call_config.service_len = (uint32_t)strlen(call_config.service);
  call_config.method = "State";
  call_config.method_len = (uint32_t)strlen(call_config.method);
  call_config.initial_message = (const uint8_t *)"request";
  call_config.initial_message_len = strlen("request");
  if (trevrpc_rpc_call_open_v1(harness->runtime, harness->endpoint,
                               &call_config, 2, &harness->call,
                               &harness->stream) != 0) {
    return -EIO;
  }
  harness->call_open = true;
  if (fake_push_event(harness->fake, TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
                      TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL |
                          TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
                      fake_stream_handle, fake_connection_handle) != 0 ||
      fake_push_operation_event(
          harness->fake, TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
          TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL |
              TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
          fake_stream_handle, fake_connection_handle, 2) != 0) {
    return -EIO;
  }
  trevrpc_rpc_event_info_v1 info;
  if (cf_expect_event(harness, TREVRPC_RPC_EVENT_CALL_READY, &info) != 0 ||
      info.operation_id != 2) {
    return -EIO;
  }
  harness->call_ready = true;
  return 0;
}

static void cf_rpc_state_harness_finish(cf_rpc_state_harness *harness) {
  if (harness->runtime == NULL) {
    return;
  }
  if (harness->call_open) {
    int close_result = trevrpc_rpc_call_close(
        harness->runtime, harness->call, 100, TREVRPC_RPC_CLOSE_FLAG_NONE, 0);
    if (close_result == 0 || close_result == -EALREADY) {
      (void)cf_expect_event(harness, TREVRPC_RPC_EVENT_STREAM_CLOSED, NULL);
      (void)cf_expect_event(harness, TREVRPC_RPC_EVENT_CALL_CLOSED, NULL);
    }
    (void)trevrpc_rpc_stream_release(harness->runtime, harness->stream);
    (void)trevrpc_rpc_call_release(harness->runtime, harness->call);
    harness->call_open = false;
  }
  if (harness->endpoint_ready) {
    if (trevrpc_rpc_endpoint_close(harness->runtime, harness->endpoint, 101) ==
        0) {
      (void)cf_expect_event(harness, TREVRPC_RPC_EVENT_ENDPOINT_CLOSED, NULL);
    }
    (void)trevrpc_rpc_endpoint_release(harness->runtime, harness->endpoint);
    harness->endpoint_ready = false;
  }
  if (trevrpc_rpc_runtime_close(harness->runtime, 102) == 0) {
    (void)cf_expect_event(harness, TREVRPC_RPC_EVENT_STOPPED, NULL);
  }
  (void)trevrpc_rpc_runtime_drain(harness->runtime);
  harness->close_count = fake_close_count(harness->fake);
  (void)trevrpc_rpc_runtime_release(harness->runtime);
  harness->runtime = NULL;
  harness->fake = NULL;
}

static int cf_receive_frame(cf_rpc_state_harness *harness, const cf_bytes *body,
                            trevrpc_rpc_receive **out_receive) {
  if (fake_push_receive(harness->fake, body->data, body->len) != 0 ||
      fake_push_event(harness->fake,
                      TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
                      TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
                          TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
                      fake_stream_handle, fake_connection_handle) != 0) {
    return -EIO;
  }
  trevrpc_rpc_event_info_v1 info;
  if (cf_expect_event(harness, TREVRPC_RPC_EVENT_STREAM_READABLE, &info) != 0) {
    return -EPROTO;
  }
  return trevrpc_rpc_stream_receive(harness->runtime, harness->stream,
                                    out_receive);
}

static int cf_copy_receive_metadata(const trevrpc_rpc_receive_info_v1 *info,
                                    trevrpc_metadata *metadata) {
  memset(metadata, 0, sizeof(*metadata));
  for (uint32_t i = 0; i < info->metadata_count; ++i) {
    int result = trevrpc_metadata_set(
        metadata, info->metadata[i].key, info->metadata[i].key_len,
        info->metadata[i].value, (size_t)info->metadata[i].value_len);
    if (result != 0) {
      trevrpc_metadata_reset(metadata);
      return result;
    }
  }
  return 0;
}

static int cf_run_state(const cf_command *command, cf_json *payload,
                        cf_error *error, bool server_stream) {
  cf_rpc_state_harness harness;
  cf_body_list messages = {0};
  /* Both state operations consume the canonical stream-frame wire shape. */
  uint32_t rpc_kind = server_stream ? TREVRPC_RPC_KIND_SERVER_STREAMING
                                    : TREVRPC_RPC_KIND_CLIENT_STREAMING;
  int result = -1;
  bool terminal = false;
  size_t frame_index = 0;
  if (cf_rpc_state_harness_start(&harness, rpc_kind) != 0) {
    cf_error_set(error, "malformed_protobuf", TREVRPC_RPC_STATUS_INTERNAL);
    goto cleanup;
  }

  while (frame_index < command->frame_count) {
    trevrpc_rpc_receive *receive = NULL;
    int native_error =
        cf_receive_frame(&harness, &command->frames[frame_index], &receive);
    ++frame_index;
    if (native_error != 0) {
      cf_classify_receive_error(harness.runtime, harness.stream, native_error,
                                error);
      goto cleanup;
    }
    trevrpc_rpc_receive_info_v1 info;
    if (trevrpc_rpc_receive_info_v1_init(&info, sizeof(info)) != 0 ||
        trevrpc_rpc_receive_get_info_v1(receive, &info) != 0) {
      trevrpc_rpc_receive_release(receive);
      cf_error_set(error, "malformed_protobuf", TREVRPC_RPC_STATUS_INTERNAL);
      goto cleanup;
    }
    if (info.kind == TREVRPC_RPC_RECEIVE_STATUS) {
      terminal = true;
      uint32_t status_code = cf_status_code_from_uint32(info.rpc_status);
      if (status_code != TREVRPC_RPC_STATUS_OK) {
        cf_error_set(error, "remote_status", status_code);
        trevrpc_rpc_receive_release(receive);
        goto cleanup;
      }
      if (!server_stream && frame_index < command->frame_count) {
        cf_error_set(error, "trailing_frame", TREVRPC_RPC_STATUS_INTERNAL);
        trevrpc_rpc_receive_release(receive);
        goto cleanup;
      }
      if (!server_stream && messages.count != 1) {
        cf_error_set(error, "response_cardinality",
                     TREVRPC_RPC_STATUS_INTERNAL);
        trevrpc_rpc_receive_release(receive);
        goto cleanup;
      }
      if (server_stream && frame_index < command->frame_count) {
        trevrpc_rpc_receive_release(receive);
        cf_error_set(error, "trailing_frame", TREVRPC_RPC_STATUS_INTERNAL);
        goto cleanup;
      }
      if (server_stream) {
        cf_json_append(payload, ",\"events\":[");
        for (size_t i = 0; i < messages.count; ++i) {
          if (i != 0) {
            cf_json_append_char(payload, ',');
          }
          cf_json_append(payload, "{\"event\":\"message\",\"body_hex\":");
          cf_json_append_hex(payload, messages.values[i].data,
                             messages.values[i].len);
          cf_json_append_char(payload, '}');
        }
        if (messages.count != 0) {
          cf_json_append_char(payload, ',');
        }
        cf_json_append(payload, "{\"event\":\"eof\"},{\"event\":\"eof\"}]");
        cf_json_append(payload, ",\"terminal_status\":{\"status_raw\":");
        cf_json_append_u64_string(payload, info.rpc_status);
        cf_json_append(payload, ",\"status_code\":");
        cf_json_append_u32(payload, status_code);
        cf_json_append(payload, ",\"message_hex\":");
        cf_json_append_hex(payload, (const uint8_t *)info.message,
                           info.message_len);
        trevrpc_metadata metadata = {0};
        if (cf_copy_receive_metadata(&info, &metadata) != 0) {
          trevrpc_rpc_receive_release(receive);
          cf_error_set(error, "malformed_protobuf",
                       TREVRPC_RPC_STATUS_INTERNAL);
          goto cleanup;
        }
        cf_json_append(payload, ",\"metadata\":");
        cf_json_append_native_metadata(payload, &metadata);
        cf_json_append_char(payload, '}');
        trevrpc_metadata_reset(&metadata);
        result = payload->failed ? -1 : 0;
      }
      if (!server_stream) {
        cf_json_append(payload, ",\"response_body_hex\":");
        cf_json_append_hex(payload, messages.values[0].data,
                           messages.values[0].len);
        result = payload->failed ? -1 : 0;
      }
      trevrpc_rpc_receive_release(receive);
      goto cleanup;
    }
    if (info.kind != TREVRPC_RPC_RECEIVE_MESSAGE) {
      trevrpc_rpc_receive_release(receive);
      cf_error_set(error, "malformed_protobuf", TREVRPC_RPC_STATUS_INTERNAL);
      goto cleanup;
    }
    uint8_t *canonical = NULL;
    size_t canonical_len = 0;
    native_error = trevrpc_wire_canonicalize_bytes_field(
        info.data, (size_t)info.data_len, 3, &canonical, &canonical_len);
    if (native_error != 0) {
      free(canonical);
      trevrpc_rpc_receive_release(receive);
      cf_error_set(error, "malformed_protobuf", TREVRPC_RPC_STATUS_INTERNAL);
      goto cleanup;
    }
    if (cf_body_list_append(&messages, canonical, canonical_len) != 0) {
      free(canonical);
      trevrpc_rpc_receive_release(receive);
      cf_error_set(error, "malformed_protobuf", TREVRPC_RPC_STATUS_INTERNAL);
      goto cleanup;
    }
    free(canonical);
    trevrpc_rpc_receive_release(receive);
  }

  if (!terminal) {
    trevrpc_rpc_receive *receive = NULL;
    int native_error =
        trevrpc_rpc_stream_receive(harness.runtime, harness.stream, &receive);
    if (native_error != -EAGAIN || receive != NULL) {
      trevrpc_rpc_receive_release(receive);
      cf_error_set(error, "malformed_protobuf", TREVRPC_RPC_STATUS_INTERNAL);
      goto cleanup;
    }
    if (fake_push_event(harness.fake, TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN,
                        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER |
                            TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT,
                        fake_stream_handle, fake_connection_handle) == 0) {
      (void)cf_expect_event(&harness, TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN,
                            NULL);
    }
    cf_error_set(error, "missing_terminal_status", TREVRPC_RPC_STATUS_INTERNAL);
    goto cleanup;
  }

cleanup:
  if (harness.runtime != NULL) {
    cf_rpc_state_harness_finish(&harness);
  } else if (harness.fake != NULL) {
    harness.close_count = fake_close_count(harness.fake);
    fake_destroy(&harness.fake->base);
    harness.fake = NULL;
  }
  if (server_stream) {
    cf_append_close_count(payload, harness.close_count);
  }
  cf_body_list_reset(&messages);
  return result;
}

int cf_c_state_dispatch(const cf_command *command, cf_json *payload,
                        cf_error *error) {
  return cf_run_state(command, payload, error,
                      strcmp(command->operation, "state.server_stream") == 0);
}
