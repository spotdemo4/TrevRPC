#include "operations.h"

#include "trevrpc.h"
#include "trevrpc_frame_internal.h"
#include "trevrpc_wire_internal.h"

#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

static const char *cf_rpc_kind_token(uint32_t kind) {
  switch (kind) {
  case TREVRPC_RPC_KIND_UNARY:
    return "unary";
  case TREVRPC_RPC_KIND_CLIENT_STREAMING:
    return "client_stream";
  case TREVRPC_RPC_KIND_SERVER_STREAMING:
    return "server_stream";
  case TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING:
    return "bidi";
  default:
    return "";
  }
}

static const char *cf_frame_kind_token(uint32_t kind) {
  return kind == TREVRPC_STREAM_FRAME_KIND_STATUS ? "status" : "message";
}

static void cf_classify_wire_error(cf_message_type type, int native_error,
                                   trevrpc_wire_diagnostic_reason reason,
                                   cf_error *error) {
  if (native_error == TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION) {
    cf_error_set(error, "unsupported_wire_version",
                 TREVRPC_STATUS_FAILED_PRECONDITION);
  } else if (native_error == TREVRPC_ERR_UNSUPPORTED_RPC_KIND) {
    cf_error_set(error, "unsupported_rpc_kind",
                 TREVRPC_STATUS_INVALID_ARGUMENT);
  } else if (native_error == TREVRPC_ERR_FRAME_TOO_LARGE) {
    cf_error_set(error, "frame_too_large", TREVRPC_STATUS_RESOURCE_EXHAUSTED);
  } else if (reason == TREVRPC_WIRE_DIAGNOSTIC_INVALID_METADATA) {
    cf_error_set(error, "invalid_metadata",
                 type == CF_MESSAGE_REQUEST ? TREVRPC_STATUS_INVALID_ARGUMENT
                                            : TREVRPC_STATUS_INTERNAL);
  } else if (reason == TREVRPC_WIRE_DIAGNOSTIC_UNSUPPORTED_FRAME_KIND) {
    cf_error_set(error, "unsupported_frame_kind",
                 TREVRPC_STATUS_INVALID_ARGUMENT);
  } else {
    cf_error_set(error, "malformed_protobuf",
                 type == CF_MESSAGE_REQUEST ? TREVRPC_STATUS_INVALID_ARGUMENT
                                            : TREVRPC_STATUS_INTERNAL);
  }
}

static int cf_native_metadata(const cf_metadata *source,
                              trevrpc_metadata *target) {
  memset(target, 0, sizeof(*target));
  for (size_t i = 0; i < source->count; i++) {
    int error = trevrpc_metadata_set(
        target, (const char *)source->entries[i].key.data,
        source->entries[i].key.len, source->entries[i].value.data,
        source->entries[i].value.len);
    if (error != 0) {
      trevrpc_metadata_reset(target);
      return error;
    }
  }
  return 0;
}

static int cf_encode_message(const cf_message *message, size_t max_frame_size,
                             uint8_t **frame, size_t *frame_len,
                             cf_error *error) {
  trevrpc_metadata metadata = {0};
  const cf_metadata *source_metadata = NULL;
  if (message->type == CF_MESSAGE_REQUEST) {
    source_metadata = &message->value.request.metadata;
  } else if (message->type == CF_MESSAGE_RESPONSE) {
    source_metadata = &message->value.response.metadata;
  } else {
    source_metadata = &message->value.stream_frame.metadata;
  }
  if (cf_native_metadata(source_metadata, &metadata) != 0) {
    cf_error_set(error, "invalid_metadata",
                 message->type == CF_MESSAGE_REQUEST
                     ? TREVRPC_STATUS_INVALID_ARGUMENT
                     : TREVRPC_STATUS_INTERNAL);
    return -1;
  }

  int native_error = 0;
  if (message->type == CF_MESSAGE_REQUEST) {
    const cf_request_message *request = &message->value.request;
    native_error = trevrpc_wire_encode_request_view(
        (const char *)request->service.data, request->service.len,
        (const char *)request->method.data, request->method.len, request->kind,
        request->version, request->body.data, request->body.len, &metadata,
        request->timeout_nanos, max_frame_size, frame, frame_len);
  } else if (message->type == CF_MESSAGE_RESPONSE) {
    const cf_response_message *response_message = &message->value.response;
    trevrpc_wire_response_values response = {
        .status = response_message->status,
        .message = (char *)response_message->message.data,
        .message_len = response_message->message.len,
        .body =
            {
                .data = response_message->body.data,
                .len = response_message->body.len,
            },
        .metadata = metadata,
    };
    native_error = trevrpc_wire_encode_response(&response, max_frame_size,
                                                frame, frame_len);
  } else {
    const cf_stream_frame_message *stream_frame = &message->value.stream_frame;
    native_error = trevrpc_wire_encode_stream_frame(
        stream_frame->kind, stream_frame->status,
        (const char *)stream_frame->message.data, stream_frame->message.len,
        stream_frame->body.data, stream_frame->body.len, &metadata,
        max_frame_size, frame, frame_len);
  }
  trevrpc_metadata_reset(&metadata);
  if (native_error != 0) {
    cf_classify_wire_error(message->type, native_error,
                           TREVRPC_WIRE_DIAGNOSTIC_NONE, error);
    return -1;
  }
  return 0;
}

static int cf_codec_encode(const cf_command *command, cf_json *payload,
                           cf_error *error) {
  size_t max_frame_size = strcmp(command->operation, "framing.encode") == 0
                              ? command->max_frame_size
                              : TREVRPC_DEFAULT_MAX_FRAME_SIZE;
  uint8_t *frame = NULL;
  size_t frame_len = 0;
  if (cf_encode_message(&command->message, max_frame_size, &frame, &frame_len,
                        error) != 0) {
    free(frame);
    return -1;
  }
  if (frame_len < 4) {
    free(frame);
    cf_error_set(error, "malformed_protobuf", TREVRPC_STATUS_INTERNAL);
    return -1;
  }
  cf_json_append(payload, ",\"body_hex\":");
  cf_json_append_hex(payload, frame + 4, frame_len - 4);
  cf_json_append(payload, ",\"frame_hex\":");
  cf_json_append_hex(payload, frame, frame_len);
  free(frame);
  return payload->failed ? -1 : 0;
}

static void cf_append_request(cf_json *payload,
                              const trevrpc_request *request) {
  cf_json_append(payload,
                 ",\"message\":{\"type\":\"rpc_request\",\"service_hex\":");
  cf_json_append_hex(payload, (const uint8_t *)request->service,
                     request->service_len);
  cf_json_append(payload, ",\"method_hex\":");
  cf_json_append_hex(payload, (const uint8_t *)request->method,
                     request->method_len);
  cf_json_append(payload, ",\"body_hex\":");
  cf_json_append_hex(payload, request->body, request->body_len);
  cf_json_append(payload, ",\"metadata\":");
  cf_json_append_native_metadata(payload, &request->metadata);
  cf_json_append(payload, ",\"kind\":\"");
  cf_json_append(payload, cf_rpc_kind_token(request->kind));
  cf_json_append(payload, "\",\"version\":");
  cf_json_append_u64_string(payload, request->version);
  cf_json_append(payload, ",\"timeout_nanos\":");
  cf_json_append_u64_string(payload, request->timeout_nanos);
  cf_json_append_char(payload, '}');
}

typedef size_t (*cf_inbound_metadata_count_fn)(const void *value);
typedef int (*cf_inbound_metadata_at_fn)(const void *value, size_t index,
                                         trevrpc_bytes_view *key,
                                         trevrpc_bytes_view *entry_value);

static size_t cf_inbound_response_metadata_count(const void *value) {
  return trevrpc_inbound_response_metadata_count(value);
}

static int cf_inbound_response_metadata_at(const void *value, size_t index,
                                           trevrpc_bytes_view *key,
                                           trevrpc_bytes_view *entry_value) {
  return trevrpc_inbound_response_metadata_at(value, index, key, entry_value);
}

static size_t cf_inbound_stream_frame_metadata_count(const void *value) {
  return trevrpc_inbound_stream_frame_metadata_count(value);
}

static int
cf_inbound_stream_frame_metadata_at(const void *value, size_t index,
                                    trevrpc_bytes_view *key,
                                    trevrpc_bytes_view *entry_value) {
  return trevrpc_inbound_stream_frame_metadata_at(value, index, key,
                                                  entry_value);
}

static int cf_copy_inbound_metadata(const void *value,
                                    cf_inbound_metadata_count_fn count_fn,
                                    cf_inbound_metadata_at_fn at_fn,
                                    trevrpc_metadata *metadata) {
  memset(metadata, 0, sizeof(*metadata));
  size_t count = count_fn(value);
  for (size_t i = 0; i < count; i++) {
    trevrpc_bytes_view key = {0};
    trevrpc_bytes_view entry_value = {0};
    int error = at_fn(value, i, &key, &entry_value);
    if (error == 0) {
      error = trevrpc_metadata_set(metadata, (const char *)key.data, key.len,
                                   entry_value.data, entry_value.len);
    }
    if (error != 0) {
      trevrpc_metadata_reset(metadata);
      return error;
    }
  }
  return 0;
}

static int cf_inbound_response_values(const trevrpc_inbound_response *response,
                                      trevrpc_wire_response_values *values) {
  memset(values, 0, sizeof(*values));
  trevrpc_bytes_view message = {0};
  trevrpc_bytes_view body = {0};
  int error = trevrpc_inbound_response_get_status(response, &values->status);
  if (error == 0) {
    error = trevrpc_inbound_response_get_message(response, &message);
  }
  if (error == 0) {
    error = trevrpc_inbound_response_get_body(response, &body);
  }
  if (error == 0) {
    error = cf_copy_inbound_metadata(
        response, cf_inbound_response_metadata_count,
        cf_inbound_response_metadata_at, &values->metadata);
  }
  if (error != 0) {
    return error;
  }
  values->message = (char *)message.data;
  values->message_len = message.len;
  values->body.data = body.data;
  values->body.len = body.len;
  return 0;
}

static int
cf_inbound_stream_frame_values(const trevrpc_inbound_stream_frame *frame,
                               trevrpc_wire_stream_frame_values *values) {
  memset(values, 0, sizeof(*values));
  trevrpc_bytes_view message = {0};
  trevrpc_bytes_view body = {0};
  int error = trevrpc_inbound_stream_frame_get_kind(frame, &values->kind);
  if (error == 0) {
    error = trevrpc_inbound_stream_frame_get_status(frame, &values->status);
  }
  if (error == 0) {
    error = trevrpc_inbound_stream_frame_get_message(frame, &message);
  }
  if (error == 0) {
    error = trevrpc_inbound_stream_frame_get_body(frame, &body);
  }
  if (error == 0) {
    error = cf_copy_inbound_metadata(
        frame, cf_inbound_stream_frame_metadata_count,
        cf_inbound_stream_frame_metadata_at, &values->metadata);
  }
  if (error != 0) {
    return error;
  }
  values->message = (char *)message.data;
  values->message_len = message.len;
  values->body.data = body.data;
  values->body.len = body.len;
  return 0;
}

static void cf_append_response(cf_json *payload,
                               const trevrpc_wire_response_values *response) {
  cf_json_append(payload,
                 ",\"message\":{\"type\":\"rpc_response\",\"status_raw\":");
  cf_json_append_u64_string(payload, response->status);
  cf_json_append(payload, ",\"status_code\":");
  cf_json_append_u32(payload,
                     trevrpc_status_code_from_uint32(response->status));
  cf_json_append(payload, ",\"message_hex\":");
  cf_json_append_hex(payload, (const uint8_t *)response->message,
                     response->message_len);
  cf_json_append(payload, ",\"body_hex\":");
  cf_json_append_hex(payload, response->body.data, response->body.len);
  cf_json_append(payload, ",\"metadata\":");
  cf_json_append_native_metadata(payload, &response->metadata);
  cf_json_append_char(payload, '}');
}

static void
cf_append_stream_frame(cf_json *payload,
                       const trevrpc_wire_stream_frame_values *frame) {
  cf_json_append(payload,
                 ",\"message\":{\"type\":\"rpc_stream_frame\",\"kind\":\"");
  cf_json_append(payload, cf_frame_kind_token(frame->kind));
  cf_json_append(payload, "\",\"kind_raw\":");
  cf_json_append_u64_string(payload, frame->kind);
  cf_json_append(payload, ",\"status_raw\":");
  cf_json_append_u64_string(payload, frame->status);
  cf_json_append(payload, ",\"status_code\":");
  cf_json_append_u32(payload, trevrpc_status_code_from_uint32(frame->status));
  cf_json_append(payload, ",\"message_hex\":");
  cf_json_append_hex(payload, (const uint8_t *)frame->message,
                     frame->message_len);
  cf_json_append(payload, ",\"body_hex\":");
  cf_json_append_hex(payload, frame->body.data, frame->body.len);
  cf_json_append(payload, ",\"metadata\":");
  cf_json_append_native_metadata(payload, &frame->metadata);
  cf_json_append_char(payload, '}');
}

static int cf_codec_decode(const cf_command *command, cf_json *payload,
                           cf_error *error) {
  const cf_bytes *body = &command->message.value.request.body;
  uint8_t *canonical_frame = NULL;
  size_t canonical_frame_len = 0;
  int native_error = 0;
  trevrpc_wire_diagnostic_reason reason = TREVRPC_WIRE_DIAGNOSTIC_NONE;

  if (command->message_type == CF_MESSAGE_REQUEST) {
    trevrpc_request request = {0};
    trevrpc_wire_request_diagnostic diagnostic = {0};
    native_error = trevrpc_wire_decode_request_diagnostic(
        body->data, body->len, &request, &diagnostic);
    reason = diagnostic.reason;
    if (native_error == 0) {
      native_error = trevrpc_wire_encode_request_view(
          request.service, request.service_len, request.method,
          request.method_len, request.kind, request.version, request.body,
          request.body_len, &request.metadata, request.timeout_nanos,
          TREVRPC_DEFAULT_MAX_FRAME_SIZE, &canonical_frame,
          &canonical_frame_len);
    }
    if (native_error == 0) {
      cf_append_request(payload, &request);
    }
    trevrpc_request_reset(&request);
  } else if (command->message_type == CF_MESSAGE_RESPONSE) {
    trevrpc_owned_bytes owned = {
        .data = body->data,
        .len = body->len,
    };
    trevrpc_inbound_response *response = NULL;
    native_error = trevrpc_wire_decode_response_owned(&owned, &response);
    if (native_error != 0) {
      trevrpc_wire_response_values *diagnostic_response = NULL;
      trevrpc_wire_diagnostic diagnostic = {0};
      (void)trevrpc_wire_decode_response_diagnostic(
          body->data, body->len, &diagnostic_response, &diagnostic);
      reason = diagnostic.reason;
      trevrpc_internal_response_free(diagnostic_response);
    }
    trevrpc_wire_response_values values = {0};
    if (native_error == 0) {
      native_error = cf_inbound_response_values(response, &values);
    }
    if (native_error == 0) {
      native_error =
          trevrpc_wire_encode_response(&values, TREVRPC_DEFAULT_MAX_FRAME_SIZE,
                                       &canonical_frame, &canonical_frame_len);
    }
    if (native_error == 0) {
      cf_append_response(payload, &values);
    }
    trevrpc_metadata_reset(&values.metadata);
    trevrpc_inbound_response_release(response);
    trevrpc_owned_bytes_reset(&owned);
  } else {
    trevrpc_owned_bytes owned = {
        .data = body->data,
        .len = body->len,
    };
    trevrpc_inbound_stream_frame *frame = NULL;
    trevrpc_wire_diagnostic diagnostic = {0};
    native_error = trevrpc_wire_decode_stream_frame_owned_diagnostic(
        &owned, &frame, &diagnostic);
    reason = diagnostic.reason;
    trevrpc_wire_stream_frame_values values = {0};
    if (native_error == 0) {
      native_error = cf_inbound_stream_frame_values(frame, &values);
    }
    if (native_error == 0) {
      native_error = trevrpc_wire_encode_stream_frame(
          values.kind, values.status, values.message, values.message_len,
          values.body.data, values.body.len, &values.metadata,
          TREVRPC_DEFAULT_MAX_FRAME_SIZE, &canonical_frame,
          &canonical_frame_len);
    }
    if (native_error == 0) {
      cf_append_stream_frame(payload, &values);
    }
    trevrpc_metadata_reset(&values.metadata);
    trevrpc_inbound_stream_frame_release(frame);
    trevrpc_owned_bytes_reset(&owned);
  }

  if (native_error != 0 || canonical_frame_len < 4) {
    free(canonical_frame);
    cf_classify_wire_error(command->message_type, native_error, reason, error);
    return -1;
  }
  cf_json_append(payload, ",\"canonical_body_hex\":");
  cf_json_append_hex(payload, canonical_frame + 4, canonical_frame_len - 4);
  free(canonical_frame);
  return payload->failed ? -1 : 0;
}

static int cf_framing_decode(const cf_command *command, cf_json *payload,
                             cf_error *error) {
  trevrpc_frame_parser parser;
  trevrpc_frame_parser_init(&parser, command->max_frame_size);
  cf_json_append(payload, ",\"bodies_hex\":[");
  size_t body_count = 0;
  for (size_t chunk = 0; chunk < command->chunk_count; chunk++) {
    size_t offset = 0;
    while (offset < command->chunks[chunk].len) {
      size_t consumed = 0;
      uint8_t *body = NULL;
      size_t body_len = 0;
      size_t declared_body_len = 0;
      trevrpc_frame_result result = trevrpc_frame_parser_consume(
          &parser, command->chunks[chunk].data + offset,
          command->chunks[chunk].len - offset, &consumed, &body, &body_len,
          &declared_body_len);
      (void)declared_body_len;
      offset += consumed;
      if (result == TREVRPC_FRAME_READY) {
        if (body_count++ > 0) {
          cf_json_append_char(payload, ',');
        }
        cf_json_append_hex(payload, body, body_len);
        free(body);
        continue;
      }
      free(body);
      if (result == TREVRPC_FRAME_TOO_LARGE) {
        trevrpc_frame_parser_reset(&parser);
        cf_error_set(error, "frame_too_large",
                     TREVRPC_STATUS_RESOURCE_EXHAUSTED);
        return -1;
      }
      if (result == TREVRPC_FRAME_ALLOCATION_FAILURE) {
        trevrpc_frame_parser_reset(&parser);
        cf_error_set(error, "malformed_protobuf", TREVRPC_STATUS_INTERNAL);
        return -1;
      }
      if (consumed == 0) {
        trevrpc_frame_parser_reset(&parser);
        cf_error_set(error, "incomplete_frame", TREVRPC_STATUS_INTERNAL);
        return -1;
      }
    }
  }
  trevrpc_frame_result finished = trevrpc_frame_parser_finish(&parser);
  trevrpc_frame_parser_reset(&parser);
  if (finished == TREVRPC_FRAME_INCOMPLETE) {
    cf_error_set(error, "incomplete_frame", TREVRPC_STATUS_INTERNAL);
    return -1;
  }
  cf_json_append(payload, "],\"eof\":true");
  return payload->failed ? -1 : 0;
}

int cf_dispatch_operation(const cf_command *command,
                          cf_state_dispatch_fn state_dispatch, cf_json *payload,
                          cf_error *error) {
  int result = -1;
  if (strcmp(command->operation, "codec.encode") == 0 ||
      strcmp(command->operation, "framing.encode") == 0) {
    result = cf_codec_encode(command, payload, error);
  } else if (strcmp(command->operation, "codec.decode") == 0) {
    result = cf_codec_decode(command, payload, error);
  } else if (strcmp(command->operation, "framing.decode_stream") == 0) {
    result = cf_framing_decode(command, payload, error);
  } else if (strcmp(command->operation, "state.server_stream") == 0 ||
             strcmp(command->operation, "state.client_stream") == 0) {
    result = state_dispatch(command, payload, error);
  } else {
    cf_error_set(error, "malformed_protobuf", TREVRPC_STATUS_INTERNAL);
  }

  if (result != 0 && !payload->failed &&
      strcmp(command->operation, "state.server_stream") != 0) {
    payload->len = 0;
  }
  return result;
}
