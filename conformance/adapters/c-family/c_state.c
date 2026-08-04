#include "peer.h"

#include "trevrpc.h"
#include "trevrpc_runtime_internal.h"
#include "trevrpc_wire_internal.h"

#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

typedef struct cf_body_list {
  cf_bytes *values;
  size_t count;
  size_t capacity;
} cf_body_list;

static void cf_body_list_reset(cf_body_list *list) {
  for (size_t i = 0; i < list->count; i++) {
    free(list->values[i].data);
  }
  free(list->values);
  memset(list, 0, sizeof(*list));
}

static int cf_body_list_append(cf_body_list *list, uint8_t *body,
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
  list->values[list->count++] = (cf_bytes){.data = body, .len = body_len};
  return 0;
}

static int cf_decode_state_payload(const uint8_t *body, size_t body_len,
                                   uint8_t **decoded, size_t *decoded_len,
                                   void *context) {
  (void)context;
  return trevrpc_wire_canonicalize_bytes_field(body, body_len, 3, decoded,
                                               decoded_len);
}

static void cf_classify_native_error(trevrpc_stream *stream, int native_error,
                                     cf_error *error) {
  trevrpc_wire_diagnostic_reason reason =
      trevrpc_stream_last_wire_diagnostic(stream);
  if (native_error == TREVRPC_ERR_FRAME_TOO_LARGE) {
    cf_error_set(error, "frame_too_large", TREVRPC_STATUS_RESOURCE_EXHAUSTED);
  } else if (reason == TREVRPC_WIRE_DIAGNOSTIC_UNSUPPORTED_FRAME_KIND) {
    cf_error_set(error, "unsupported_frame_kind",
                 TREVRPC_STATUS_INVALID_ARGUMENT);
  } else if (reason == TREVRPC_WIRE_DIAGNOSTIC_INVALID_METADATA) {
    cf_error_set(error, "invalid_metadata", TREVRPC_STATUS_INTERNAL);
  } else {
    cf_error_set(error, "malformed_protobuf", TREVRPC_STATUS_INTERNAL);
  }
}

static void cf_classify_state_error(const trevrpc_response_state *state,
                                    cf_error *error) {
  switch (state->failure) {
  case TREVRPC_RESPONSE_STATE_FAILURE_FRAME_TOO_LARGE:
    cf_error_set(error, "frame_too_large", TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    return;
  case TREVRPC_RESPONSE_STATE_FAILURE_INVALID_METADATA:
    cf_error_set(error, "invalid_metadata", TREVRPC_STATUS_INTERNAL);
    return;
  case TREVRPC_RESPONSE_STATE_FAILURE_MALFORMED_PROTOBUF:
    cf_error_set(error, "malformed_protobuf", TREVRPC_STATUS_INTERNAL);
    return;
  case TREVRPC_RESPONSE_STATE_FAILURE_MISSING_TERMINAL_STATUS:
    cf_error_set(error, "missing_terminal_status", TREVRPC_STATUS_INTERNAL);
    return;
  case TREVRPC_RESPONSE_STATE_FAILURE_REMOTE_STATUS:
    cf_error_set(error, "remote_status", state->failure_status);
    return;
  case TREVRPC_RESPONSE_STATE_FAILURE_RESPONSE_CARDINALITY:
    cf_error_set(error, "response_cardinality", TREVRPC_STATUS_INTERNAL);
    return;
  case TREVRPC_RESPONSE_STATE_FAILURE_TRAILING_FRAME:
    cf_error_set(error, "trailing_frame", TREVRPC_STATUS_INTERNAL);
    return;
  case TREVRPC_RESPONSE_STATE_FAILURE_UNSUPPORTED_FRAME_KIND:
    cf_error_set(error, "unsupported_frame_kind",
                 TREVRPC_STATUS_INVALID_ARGUMENT);
    return;
  case TREVRPC_RESPONSE_STATE_FAILURE_NATIVE:
    cf_classify_native_error(state->stream, state->native_error, error);
    return;
  case TREVRPC_RESPONSE_STATE_FAILURE_NONE:
    break;
  }
  cf_error_set(error, "malformed_protobuf", TREVRPC_STATUS_INTERNAL);
}

static int cf_scripted_stream(const cf_command *command,
                              trevrpc_stream **stream,
                              trevrpc_scripted_stream_source **source,
                              trevrpc_scripted_frame_body **scripted_frames) {
  *scripted_frames = NULL;
  if (command->frame_count > 0) {
    if (command->frame_count > SIZE_MAX / sizeof(**scripted_frames)) {
      return -EOVERFLOW;
    }
    *scripted_frames = malloc(command->frame_count * sizeof(**scripted_frames));
    if (*scripted_frames == NULL) {
      return -ENOMEM;
    }
    for (size_t i = 0; i < command->frame_count; i++) {
      (*scripted_frames)[i] = (trevrpc_scripted_frame_body){
          .body = command->frames[i].data,
          .body_len = command->frames[i].len,
      };
    }
  }
  int native_error = trevrpc_scripted_stream_new(
      *scripted_frames, command->frame_count, 0, TREVRPC_DEFAULT_MAX_FRAME_SIZE,
      stream, source);
  if (native_error != 0) {
    free(*scripted_frames);
    *scripted_frames = NULL;
  }
  return native_error;
}

static void cf_append_close_count(cf_json *payload, size_t close_count) {
  cf_json_append(payload, ",\"transport_close_count\":");
  cf_json_append_size_string(payload, close_count);
}

static int
cf_copy_inbound_frame_metadata(const trevrpc_inbound_stream_frame *frame,
                               trevrpc_metadata *metadata) {
  memset(metadata, 0, sizeof(*metadata));
  size_t count = trevrpc_inbound_stream_frame_metadata_count(frame);
  for (size_t i = 0; i < count; i++) {
    trevrpc_bytes_view key = {0};
    trevrpc_bytes_view value = {0};
    int native_error =
        trevrpc_inbound_stream_frame_metadata_at(frame, i, &key, &value);
    if (native_error == 0) {
      native_error = trevrpc_metadata_set(metadata, (const char *)key.data,
                                          key.len, value.data, value.len);
    }
    if (native_error != 0) {
      trevrpc_metadata_reset(metadata);
      return native_error;
    }
  }
  return 0;
}

static int cf_run_server_state(const cf_command *command, cf_json *payload,
                               cf_error *error) {
  trevrpc_stream *stream = NULL;
  trevrpc_scripted_stream_source *source = NULL;
  trevrpc_scripted_frame_body *scripted_frames = NULL;
  trevrpc_response_state state = {0};
  cf_body_list messages = {0};
  int result = -1;

  if (cf_scripted_stream(command, &stream, &source, &scripted_frames) != 0) {
    cf_error_set(error, "malformed_protobuf", TREVRPC_STATUS_INTERNAL);
    goto cleanup;
  }
  trevrpc_response_state_init(&state, stream, cf_decode_state_payload, NULL);
  for (;;) {
    uint8_t *body = NULL;
    size_t body_len = 0;
    int eof = 0;
    if (trevrpc_response_state_next(&state, &body, &body_len, &eof) != 0) {
      free(body);
      cf_classify_state_error(&state, error);
      goto close_stream;
    }
    if (eof) {
      break;
    }
    if (cf_body_list_append(&messages, body, body_len) != 0) {
      free(body);
      cf_error_set(error, "malformed_protobuf", TREVRPC_STATUS_INTERNAL);
      goto close_stream;
    }
  }

  const trevrpc_inbound_stream_frame *terminal =
      trevrpc_response_state_terminal(&state);
  if (terminal == NULL) {
    cf_error_set(error, "missing_terminal_status", TREVRPC_STATUS_INTERNAL);
    goto close_stream;
  }
  uint32_t terminal_status = TREVRPC_STATUS_UNKNOWN;
  trevrpc_bytes_view terminal_message = {0};
  trevrpc_metadata terminal_metadata = {0};
  int terminal_error =
      trevrpc_inbound_stream_frame_get_status(terminal, &terminal_status);
  if (terminal_error == 0) {
    terminal_error =
        trevrpc_inbound_stream_frame_get_message(terminal, &terminal_message);
  }
  if (terminal_error == 0) {
    terminal_error =
        cf_copy_inbound_frame_metadata(terminal, &terminal_metadata);
  }
  if (terminal_error != 0) {
    trevrpc_metadata_reset(&terminal_metadata);
    cf_error_set(error, "malformed_protobuf", TREVRPC_STATUS_INTERNAL);
    goto close_stream;
  }
  cf_json_append(payload, ",\"events\":[");
  for (size_t i = 0; i < messages.count; i++) {
    if (i > 0) {
      cf_json_append_char(payload, ',');
    }
    cf_json_append(payload, "{\"event\":\"message\",\"body_hex\":");
    cf_json_append_hex(payload, messages.values[i].data,
                       messages.values[i].len);
    cf_json_append_char(payload, '}');
  }
  if (messages.count > 0) {
    cf_json_append_char(payload, ',');
  }
  cf_json_append(payload, "{\"event\":\"eof\"},{\"event\":\"eof\"}]");
  cf_json_append(payload, ",\"terminal_status\":{\"status_raw\":");
  cf_json_append_u64_string(payload, terminal_status);
  cf_json_append(payload, ",\"status_code\":");
  cf_json_append_u32(payload, trevrpc_status_code_from_uint32(terminal_status));
  cf_json_append(payload, ",\"message_hex\":");
  cf_json_append_hex(payload, terminal_message.data, terminal_message.len);
  cf_json_append(payload, ",\"metadata\":");
  cf_json_append_native_metadata(payload, &terminal_metadata);
  cf_json_append_char(payload, '}');
  result = payload->failed ? -1 : 0;
  trevrpc_metadata_reset(&terminal_metadata);

close_stream:
  trevrpc_stream_close(stream);
  stream = NULL;
  cf_append_close_count(payload, trevrpc_scripted_stream_close_count(source));
cleanup:
  trevrpc_response_state_reset(&state);
  cf_body_list_reset(&messages);
  free(scripted_frames);
  if (stream != NULL) {
    trevrpc_stream_close(stream);
  }
  trevrpc_scripted_stream_source_free(source);
  return result;
}

static int cf_run_client_state(const cf_command *command, cf_json *payload,
                               cf_error *error) {
  trevrpc_stream *stream = NULL;
  trevrpc_scripted_stream_source *source = NULL;
  trevrpc_scripted_frame_body *scripted_frames = NULL;
  trevrpc_response_state state = {0};
  uint8_t *response = NULL;
  size_t response_len = 0;
  int result = -1;

  if (cf_scripted_stream(command, &stream, &source, &scripted_frames) != 0) {
    cf_error_set(error, "malformed_protobuf", TREVRPC_STATUS_INTERNAL);
    goto cleanup;
  }
  trevrpc_response_state_init(&state, stream, cf_decode_state_payload, NULL);
  if (trevrpc_response_state_read_exactly_one(&state, &response,
                                              &response_len) != 0) {
    cf_classify_state_error(&state, error);
    goto cleanup;
  }
  cf_json_append(payload, ",\"response_body_hex\":");
  cf_json_append_hex(payload, response, response_len);
  result = payload->failed ? -1 : 0;

cleanup:
  free(response);
  trevrpc_response_state_reset(&state);
  free(scripted_frames);
  if (stream != NULL) {
    trevrpc_stream_close(stream);
  }
  trevrpc_scripted_stream_source_free(source);
  return result;
}

int cf_c_state_dispatch(const cf_command *command, cf_json *payload,
                        cf_error *error) {
  if (strcmp(command->operation, "state.server_stream") == 0) {
    return cf_run_server_state(command, payload, error);
  }
  return cf_run_client_state(command, payload, error);
}
