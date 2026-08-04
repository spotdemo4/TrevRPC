#define _POSIX_C_SOURCE 200809L

#include "peer.h"

#include "operations.h"
#include "trevrpc.h"

#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct cf_field_parser {
  char **fields;
  size_t count;
  size_t next;
} cf_field_parser;

static void cf_command_reset(cf_command *command);

void cf_json_init(cf_json *json, char *data, size_t capacity) {
  json->data = data;
  json->len = 0;
  json->capacity = capacity;
  json->failed = capacity == 0;
  if (capacity > 0) {
    data[0] = '\0';
  }
}

static void cf_json_append_bytes(cf_json *json, const char *value, size_t len) {
  if (json->failed || len > json->capacity - json->len - 1) {
    json->failed = 1;
    return;
  }
  memcpy(json->data + json->len, value, len);
  json->len += len;
  json->data[json->len] = '\0';
}

void cf_json_append(cf_json *json, const char *value) {
  cf_json_append_bytes(json, value, strlen(value));
}

void cf_json_append_char(cf_json *json, char value) {
  cf_json_append_bytes(json, &value, 1);
}

static void cf_json_append_uint(cf_json *json, uint64_t value) {
  char digits[32];
  int length =
      snprintf(digits, sizeof(digits), "%llu", (unsigned long long)value);
  if (length < 0 || (size_t)length >= sizeof(digits)) {
    json->failed = 1;
    return;
  }
  cf_json_append_bytes(json, digits, (size_t)length);
}

void cf_json_append_u32(cf_json *json, uint32_t value) {
  cf_json_append_uint(json, value);
}

void cf_json_append_size_string(cf_json *json, size_t value) {
  cf_json_append_char(json, '"');
  cf_json_append_uint(json, (uint64_t)value);
  cf_json_append_char(json, '"');
}

void cf_json_append_u64_string(cf_json *json, uint64_t value) {
  cf_json_append_char(json, '"');
  cf_json_append_uint(json, value);
  cf_json_append_char(json, '"');
}

void cf_json_append_hex(cf_json *json, const uint8_t *data, size_t len) {
  static const char digits[] = "0123456789abcdef";
  cf_json_append_char(json, '"');
  for (size_t i = 0; i < len; i++) {
    char encoded[2] = {digits[data[i] >> 4], digits[data[i] & 0x0f]};
    cf_json_append_bytes(json, encoded, sizeof(encoded));
  }
  cf_json_append_char(json, '"');
}

void cf_json_append_metadata(cf_json *json, const cf_metadata *metadata) {
  cf_json_append_char(json, '[');
  for (size_t i = 0; i < metadata->count; i++) {
    if (i > 0) {
      cf_json_append_char(json, ',');
    }
    cf_json_append(json, "{\"key_hex\":");
    cf_json_append_hex(json, metadata->entries[i].key.data,
                       metadata->entries[i].key.len);
    cf_json_append(json, ",\"value_hex\":");
    cf_json_append_hex(json, metadata->entries[i].value.data,
                       metadata->entries[i].value.len);
    cf_json_append_char(json, '}');
  }
  cf_json_append_char(json, ']');
}

static int cf_native_metadata_compare(const void *left, const void *right) {
  const trevrpc_metadata_entry *const *left_entry = left;
  const trevrpc_metadata_entry *const *right_entry = right;
  size_t common = (*left_entry)->key_len < (*right_entry)->key_len
                      ? (*left_entry)->key_len
                      : (*right_entry)->key_len;
  int compared = memcmp((*left_entry)->key, (*right_entry)->key, common);
  if (compared != 0) {
    return compared;
  }
  return ((*left_entry)->key_len > (*right_entry)->key_len) -
         ((*left_entry)->key_len < (*right_entry)->key_len);
}

void cf_json_append_native_metadata(cf_json *json,
                                    const void *native_metadata) {
  const trevrpc_metadata *metadata = native_metadata;
  trevrpc_metadata_entry const **sorted = NULL;
  if (metadata->entries_len > 0) {
    if (metadata->entries_len > SIZE_MAX / sizeof(*sorted)) {
      json->failed = 1;
      return;
    }
    sorted = malloc(metadata->entries_len * sizeof(*sorted));
    if (sorted == NULL) {
      json->failed = 1;
      return;
    }
    for (size_t i = 0; i < metadata->entries_len; i++) {
      sorted[i] = &metadata->entries[i];
    }
    qsort(sorted, metadata->entries_len, sizeof(*sorted),
          cf_native_metadata_compare);
  }

  cf_json_append_char(json, '[');
  for (size_t i = 0; i < metadata->entries_len; i++) {
    if (i > 0) {
      cf_json_append_char(json, ',');
    }
    cf_json_append(json, "{\"key_hex\":");
    cf_json_append_hex(json, (const uint8_t *)sorted[i]->key,
                       sorted[i]->key_len);
    cf_json_append(json, ",\"value_hex\":");
    cf_json_append_hex(json, sorted[i]->value, sorted[i]->value_len);
    cf_json_append_char(json, '}');
  }
  cf_json_append_char(json, ']');
  free(sorted);
}

void cf_error_set(cf_error *error, const char *category, uint32_t status_code) {
  error->category = category;
  error->status_code = status_code;
}

static int cf_bytes_compare(const cf_bytes *left, const cf_bytes *right) {
  size_t common = left->len < right->len ? left->len : right->len;
  int compared = memcmp(left->data, right->data, common);
  if (compared != 0) {
    return compared;
  }
  return (left->len > right->len) - (left->len < right->len);
}

static int cf_parse_decimal(const char *value, uint64_t maximum,
                            uint64_t *parsed) {
  if (value[0] == '\0' || (value[0] == '0' && value[1] != '\0')) {
    return -1;
  }
  uint64_t result = 0;
  for (const unsigned char *cursor = (const unsigned char *)value;
       *cursor != '\0'; cursor++) {
    if (*cursor < '0' || *cursor > '9') {
      return -1;
    }
    uint64_t digit = (uint64_t)(*cursor - '0');
    if (result > (maximum - digit) / 10) {
      return -1;
    }
    result = result * 10 + digit;
  }
  *parsed = result;
  return 0;
}

static int cf_valid_id(const char *value) {
  if (value[0] == '\0') {
    return 0;
  }
  for (const unsigned char *cursor = (const unsigned char *)value;
       *cursor != '\0'; cursor++) {
    if (!((*cursor >= 'a' && *cursor <= 'z') ||
          (*cursor >= '0' && *cursor <= '9') || *cursor == '.' ||
          *cursor == '_' || *cursor == '-')) {
      return 0;
    }
  }
  return 1;
}

static int cf_hex_value(unsigned char value) {
  if (value >= '0' && value <= '9') {
    return value - '0';
  }
  if (value >= 'a' && value <= 'f') {
    return value - 'a' + 10;
  }
  return -1;
}

static int cf_decode_hex(const char *value, cf_bytes *bytes) {
  size_t length = strlen(value);
  if ((length & 1u) != 0) {
    return -1;
  }
  bytes->data = NULL;
  bytes->len = length / 2;
  if (bytes->len > 0) {
    bytes->data = malloc(bytes->len);
    if (bytes->data == NULL) {
      return -2;
    }
  }
  for (size_t i = 0; i < bytes->len; i++) {
    int high = cf_hex_value((unsigned char)value[i * 2]);
    int low = cf_hex_value((unsigned char)value[i * 2 + 1]);
    if (high < 0 || low < 0) {
      free(bytes->data);
      bytes->data = NULL;
      bytes->len = 0;
      return -1;
    }
    bytes->data[i] = (uint8_t)((high << 4) | low);
  }
  return 0;
}

static char *cf_next_field(cf_field_parser *parser) {
  if (parser->next >= parser->count) {
    return NULL;
  }
  return parser->fields[parser->next++];
}

static int cf_parse_size_field(cf_field_parser *parser, size_t *value) {
  char *field = cf_next_field(parser);
  uint64_t parsed = 0;
  if (field == NULL || cf_parse_decimal(field, SIZE_MAX, &parsed) != 0) {
    return -1;
  }
  *value = (size_t)parsed;
  return 0;
}

static int cf_parse_u32_field(cf_field_parser *parser, uint32_t *value) {
  char *field = cf_next_field(parser);
  uint64_t parsed = 0;
  if (field == NULL || cf_parse_decimal(field, UINT32_MAX, &parsed) != 0) {
    return -1;
  }
  *value = (uint32_t)parsed;
  return 0;
}

static int cf_parse_u64_field(cf_field_parser *parser, uint64_t *value) {
  char *field = cf_next_field(parser);
  return field == NULL ? -1 : cf_parse_decimal(field, UINT64_MAX, value);
}

static int cf_parse_hex_field(cf_field_parser *parser, cf_bytes *value) {
  char *field = cf_next_field(parser);
  return field == NULL ? -1 : cf_decode_hex(field, value);
}

static int cf_parse_metadata(cf_field_parser *parser, cf_metadata *metadata) {
  size_t count = 0;
  if (cf_parse_size_field(parser, &count) != 0 ||
      count > (parser->count - parser->next) / 2) {
    return -1;
  }
  metadata->entries = NULL;
  metadata->count = 0;
  if (count > 0) {
    if (count > SIZE_MAX / sizeof(*metadata->entries)) {
      return -1;
    }
    metadata->entries = calloc(count, sizeof(*metadata->entries));
    if (metadata->entries == NULL) {
      return -2;
    }
  }
  metadata->count = count;
  for (size_t i = 0; i < count; i++) {
    int parsed = cf_parse_hex_field(parser, &metadata->entries[i].key);
    if (parsed == 0) {
      parsed = cf_parse_hex_field(parser, &metadata->entries[i].value);
    }
    if (parsed != 0) {
      return parsed;
    }
    if (i > 0 && cf_bytes_compare(&metadata->entries[i - 1].key,
                                  &metadata->entries[i].key) >= 0) {
      return -1;
    }
  }
  return 0;
}

static int cf_parse_message_type(const char *field, cf_message_type *type) {
  if (field == NULL) {
    return -1;
  }
  if (strcmp(field, "rpc_request") == 0) {
    *type = CF_MESSAGE_REQUEST;
    return 0;
  }
  if (strcmp(field, "rpc_response") == 0) {
    *type = CF_MESSAGE_RESPONSE;
    return 0;
  }
  if (strcmp(field, "rpc_stream_frame") == 0) {
    *type = CF_MESSAGE_STREAM_FRAME;
    return 0;
  }
  return -1;
}

static int cf_parse_message(cf_field_parser *parser, cf_message_type type,
                            cf_message *message) {
  message->type = type;
  if (type == CF_MESSAGE_REQUEST) {
    cf_request_message *request = &message->value.request;
    char *kind = NULL;
    if (cf_parse_hex_field(parser, &request->service) != 0 ||
        cf_parse_hex_field(parser, &request->method) != 0 ||
        cf_parse_hex_field(parser, &request->body) != 0 ||
        cf_parse_metadata(parser, &request->metadata) != 0 ||
        (kind = cf_next_field(parser)) == NULL) {
      return -1;
    }
    if (strcmp(kind, "unary") == 0) {
      request->kind = TREVRPC_RPC_KIND_UNARY;
    } else if (strcmp(kind, "client_stream") == 0) {
      request->kind = TREVRPC_RPC_KIND_CLIENT_STREAMING;
    } else if (strcmp(kind, "server_stream") == 0) {
      request->kind = TREVRPC_RPC_KIND_SERVER_STREAMING;
    } else if (strcmp(kind, "bidi") == 0) {
      request->kind = TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING;
    } else {
      return -1;
    }
    return cf_parse_u32_field(parser, &request->version) == 0 &&
                   cf_parse_u64_field(parser, &request->timeout_nanos) == 0
               ? 0
               : -1;
  }
  if (type == CF_MESSAGE_RESPONSE) {
    cf_response_message *response = &message->value.response;
    return cf_parse_u32_field(parser, &response->status) == 0 &&
                   cf_parse_hex_field(parser, &response->message) == 0 &&
                   cf_parse_hex_field(parser, &response->body) == 0 &&
                   cf_parse_metadata(parser, &response->metadata) == 0
               ? 0
               : -1;
  }
  cf_stream_frame_message *frame = &message->value.stream_frame;
  return cf_parse_u32_field(parser, &frame->kind) == 0 &&
                 cf_parse_u32_field(parser, &frame->status) == 0 &&
                 cf_parse_hex_field(parser, &frame->message) == 0 &&
                 cf_parse_hex_field(parser, &frame->body) == 0 &&
                 cf_parse_metadata(parser, &frame->metadata) == 0
             ? 0
             : -1;
}

static int cf_parse_hex_list(cf_field_parser *parser, cf_bytes **values,
                             size_t *count) {
  size_t parsed_count = 0;
  if (cf_parse_size_field(parser, &parsed_count) != 0 ||
      parsed_count > parser->count - parser->next) {
    return -1;
  }
  *values = NULL;
  *count = 0;
  if (parsed_count > 0) {
    if (parsed_count > SIZE_MAX / sizeof(**values)) {
      return -1;
    }
    *values = calloc(parsed_count, sizeof(**values));
    if (*values == NULL) {
      return -2;
    }
  }
  *count = parsed_count;
  for (size_t i = 0; i < parsed_count; i++) {
    int parsed = cf_parse_hex_field(parser, &(*values)[i]);
    if (parsed != 0) {
      return parsed;
    }
  }
  return 0;
}

static int cf_split_fields(char *line, size_t length, char ***out_fields,
                           size_t *out_count) {
  size_t count = 1;
  for (size_t i = 0; i < length; i++) {
    if (line[i] == '\t') {
      count++;
    }
  }
  if (count > SIZE_MAX / sizeof(**out_fields)) {
    return -1;
  }
  char **fields = malloc(count * sizeof(*fields));
  if (fields == NULL) {
    return -2;
  }
  size_t next = 0;
  fields[next++] = line;
  for (size_t i = 0; i < length; i++) {
    if (line[i] == '\t') {
      line[i] = '\0';
      fields[next++] = &line[i + 1];
    }
  }
  *out_fields = fields;
  *out_count = count;
  return 0;
}

static int cf_parse_command(char *line, size_t length, cf_command *command,
                            int *stop) {
  memset(command, 0, sizeof(*command));
  *stop = 0;
  if (length == 4 && memcmp(line, "STOP", 4) == 0) {
    *stop = 1;
    return 0;
  }
  for (size_t i = 0; i < length; i++) {
    unsigned char value = (unsigned char)line[i];
    if (value == '\r' || value == '\0' || value > 0x7f) {
      return -1;
    }
  }

  char **fields = NULL;
  size_t field_count = 0;
  int split = cf_split_fields(line, length, &fields, &field_count);
  if (split != 0) {
    return split;
  }
  if (field_count < 4 || strcmp(fields[0], "RUN") != 0) {
    free(fields);
    return -1;
  }
  uint64_t sequence = 0;
  if (cf_parse_decimal(fields[1], UINT64_MAX, &sequence) != 0 ||
      !cf_valid_id(fields[2])) {
    free(fields);
    return -1;
  }
  (void)sequence;
  command->sequence = fields[1];
  command->case_id = fields[2];
  command->operation = fields[3];
  cf_field_parser parser = {.fields = fields, .count = field_count, .next = 4};
  int parsed = 0;
  if (strcmp(command->operation, "codec.encode") == 0) {
    parsed =
        cf_parse_message_type(cf_next_field(&parser), &command->message_type);
    if (parsed == 0) {
      parsed =
          cf_parse_message(&parser, command->message_type, &command->message);
    }
  } else if (strcmp(command->operation, "codec.decode") == 0) {
    parsed =
        cf_parse_message_type(cf_next_field(&parser), &command->message_type);
    if (parsed == 0) {
      parsed =
          cf_parse_hex_field(&parser, &command->message.value.request.body);
    }
  } else if (strcmp(command->operation, "framing.encode") == 0) {
    parsed =
        cf_parse_message_type(cf_next_field(&parser), &command->message_type);
    if (parsed == 0) {
      parsed = cf_parse_size_field(&parser, &command->max_frame_size);
    }
    if (parsed == 0) {
      parsed =
          cf_parse_message(&parser, command->message_type, &command->message);
    }
  } else if (strcmp(command->operation, "framing.decode_stream") == 0) {
    parsed =
        cf_parse_message_type(cf_next_field(&parser), &command->message_type);
    if (parsed == 0) {
      parsed = cf_parse_size_field(&parser, &command->max_frame_size);
    }
    if (parsed == 0) {
      parsed =
          cf_parse_hex_list(&parser, &command->chunks, &command->chunk_count);
    }
  } else if (strcmp(command->operation, "state.server_stream") == 0 ||
             strcmp(command->operation, "state.client_stream") == 0) {
    parsed =
        cf_parse_hex_list(&parser, &command->frames, &command->frame_count);
  } else {
    parsed = -1;
  }
  if (parsed == 0 && parser.next != parser.count) {
    parsed = -1;
  }
  free(fields);
  if (parsed != 0) {
    cf_command_reset(command);
  }
  return parsed;
}

static void cf_bytes_reset(cf_bytes *bytes) {
  free(bytes->data);
  bytes->data = NULL;
  bytes->len = 0;
}

static void cf_metadata_reset(cf_metadata *metadata) {
  for (size_t i = 0; i < metadata->count; i++) {
    cf_bytes_reset(&metadata->entries[i].key);
    cf_bytes_reset(&metadata->entries[i].value);
  }
  free(metadata->entries);
  metadata->entries = NULL;
  metadata->count = 0;
}

static void cf_message_reset(cf_message *message) {
  if (message->type == CF_MESSAGE_REQUEST) {
    cf_request_message *request = &message->value.request;
    cf_bytes_reset(&request->service);
    cf_bytes_reset(&request->method);
    cf_bytes_reset(&request->body);
    cf_metadata_reset(&request->metadata);
  } else if (message->type == CF_MESSAGE_RESPONSE) {
    cf_response_message *response = &message->value.response;
    cf_bytes_reset(&response->message);
    cf_bytes_reset(&response->body);
    cf_metadata_reset(&response->metadata);
  } else {
    cf_stream_frame_message *frame = &message->value.stream_frame;
    cf_bytes_reset(&frame->message);
    cf_bytes_reset(&frame->body);
    cf_metadata_reset(&frame->metadata);
  }
}

static void cf_command_reset(cf_command *command) {
  if (strcmp(command->operation == NULL ? "" : command->operation,
             "codec.decode") == 0) {
    cf_bytes_reset(&command->message.value.request.body);
  } else if (strcmp(command->operation == NULL ? "" : command->operation,
                    "codec.encode") == 0 ||
             strcmp(command->operation == NULL ? "" : command->operation,
                    "framing.encode") == 0) {
    cf_message_reset(&command->message);
  }
  for (size_t i = 0; i < command->chunk_count; i++) {
    cf_bytes_reset(&command->chunks[i]);
  }
  free(command->chunks);
  for (size_t i = 0; i < command->frame_count; i++) {
    cf_bytes_reset(&command->frames[i]);
  }
  free(command->frames);
  memset(command, 0, sizeof(*command));
}

static int cf_read_command(char *buffer, size_t capacity, size_t *length,
                           const char **message) {
  *length = 0;
  for (;;) {
    int value = fgetc(stdin);
    if (value == EOF) {
      *message = *length == 0 ? "controller input ended without STOP"
                              : "command was not LF-terminated";
      return -1;
    }
    if (value == '\n') {
      buffer[*length] = '\0';
      return 0;
    }
    if (*length >= capacity) {
      *message = "command line exceeded limit";
      return -1;
    }
    buffer[(*length)++] = (char)value;
  }
}

static void cf_json_append_escaped(cf_json *json, const char *value) {
  cf_json_append_char(json, '"');
  for (const unsigned char *cursor = (const unsigned char *)value;
       *cursor != '\0'; cursor++) {
    if (*cursor == '"' || *cursor == '\\') {
      cf_json_append_char(json, '\\');
      cf_json_append_char(json, (char)*cursor);
    } else if (*cursor >= 0x20 && *cursor <= 0x7e) {
      cf_json_append_char(json, (char)*cursor);
    } else {
      cf_json_append(json, "?");
    }
  }
  cf_json_append_char(json, '"');
}

static int cf_emit(const cf_json *json) {
  if (json->failed || json->len + 1 > CF_MAX_EVENT_BYTES) {
    return -1;
  }
  if (fwrite(json->data, 1, json->len, stdout) != json->len ||
      fputc('\n', stdout) == EOF || fflush(stdout) != 0) {
    return -1;
  }
  return 0;
}

static void cf_fatal(const char *peer, const char *message) {
  char buffer[CF_MAX_EVENT_BYTES];
  cf_json json;
  cf_json_init(&json, buffer, sizeof(buffer));
  cf_json_append(&json, "{\"schema_version\":1,\"event\":\"fatal\",\"peer\":");
  cf_json_append_escaped(&json, peer);
  cf_json_append(&json, ",\"message\":");
  cf_json_append_escaped(&json, message);
  cf_json_append_char(&json, '}');
  (void)cf_emit(&json);
  fprintf(stderr, "%s\n", message);
  exit(2);
}

static void cf_emit_ready(const char *peer) {
  char buffer[CF_MAX_EVENT_BYTES];
  cf_json json;
  cf_json_init(&json, buffer, sizeof(buffer));
  cf_json_append(&json, "{\"schema_version\":1,\"event\":\"ready\",\"peer\":");
  cf_json_append_escaped(&json, peer);
  cf_json_append(&json, ",\"pid\":");
  cf_json_append_uint(&json, (uint64_t)getpid());
  cf_json_append(
      &json,
      ",\"capabilities\":[\"codec.decode\",\"codec.encode\",\"framing.decode_"
      "stream\","
      "\"framing.encode\",\"state.client_stream\",\"state.server_stream\"]}");
  if (cf_emit(&json) != 0) {
    fprintf(stderr, "failed to emit ready event\n");
    exit(2);
  }
}

static void cf_emit_result(const char *peer, const cf_command *command,
                           int operation_result, const cf_json *payload,
                           const cf_error *error) {
  char buffer[CF_MAX_EVENT_BYTES];
  cf_json json;
  cf_json_init(&json, buffer, sizeof(buffer));
  cf_json_append(&json, "{\"schema_version\":1,\"event\":\"result\",\"peer\":");
  cf_json_append_escaped(&json, peer);
  cf_json_append(&json, ",\"sequence\":");
  cf_json_append_escaped(&json, command->sequence);
  cf_json_append(&json, ",\"case_id\":");
  cf_json_append_escaped(&json, command->case_id);
  cf_json_append(&json, ",\"operation\":");
  cf_json_append_escaped(&json, command->operation);
  if (operation_result == 0) {
    cf_json_append(&json, ",\"outcome\":\"success\"");
    cf_json_append_bytes(&json, payload->data, payload->len);
  } else {
    cf_json_append(&json, ",\"outcome\":\"error\",\"category\":");
    cf_json_append_escaped(&json, error->category);
    cf_json_append(&json, ",\"status_code\":");
    cf_json_append_u32(&json, error->status_code);
    cf_json_append_bytes(&json, payload->data, payload->len);
  }
  cf_json_append_char(&json, '}');
  if (cf_emit(&json) != 0) {
    cf_fatal(peer, "result event exceeded limit or could not be written");
  }
}

int cf_peer_main(const char *peer, int argc, char **argv,
                 cf_state_dispatch_fn state_dispatch) {
  if (argc != 3 || strcmp(argv[1], "--protocol") != 0 ||
      strcmp(argv[2], "1") != 0) {
    fprintf(stderr, "usage: trevrpc-conformance-%s --protocol 1\n", peer);
    return 2;
  }
  cf_emit_ready(peer);

  char *line = malloc(CF_MAX_COMMAND_BYTES + 1);
  if (line == NULL) {
    cf_fatal(peer, "failed to allocate command buffer");
  }
  for (;;) {
    size_t length = 0;
    const char *read_message = NULL;
    if (cf_read_command(line, CF_MAX_COMMAND_BYTES, &length, &read_message) !=
        0) {
      free(line);
      cf_fatal(peer, read_message);
    }
    cf_command command;
    int stop = 0;
    int parsed = cf_parse_command(line, length, &command, &stop);
    if (parsed != 0) {
      free(line);
      cf_fatal(peer,
               parsed == -2 ? "command allocation failed" : "invalid command");
    }
    if (stop) {
      free(line);
      return 0;
    }

    char payload_buffer[CF_MAX_EVENT_BYTES];
    cf_json payload;
    cf_json_init(&payload, payload_buffer, sizeof(payload_buffer));
    cf_error error = {.category = "malformed_protobuf",
                      .status_code = TREVRPC_STATUS_INTERNAL};
    int operation_result =
        cf_dispatch_operation(&command, state_dispatch, &payload, &error);
    if (payload.failed) {
      cf_command_reset(&command);
      free(line);
      cf_fatal(peer, "result event exceeded limit or allocation failed");
    }
    cf_emit_result(peer, &command, operation_result, &payload, &error);
    cf_command_reset(&command);
  }
}
