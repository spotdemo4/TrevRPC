#ifndef TREVRPC_CONFORMANCE_C_FAMILY_PEER_H
#define TREVRPC_CONFORMANCE_C_FAMILY_PEER_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CF_MAX_COMMAND_BYTES 262144u
#define CF_MAX_EVENT_BYTES 65536u

typedef struct cf_bytes {
  uint8_t *data;
  size_t len;
} cf_bytes;

typedef struct cf_metadata_entry {
  cf_bytes key;
  cf_bytes value;
} cf_metadata_entry;

typedef struct cf_metadata {
  cf_metadata_entry *entries;
  size_t count;
} cf_metadata;

typedef enum cf_message_type {
  CF_MESSAGE_REQUEST = 0,
  CF_MESSAGE_RESPONSE = 1,
  CF_MESSAGE_STREAM_FRAME = 2,
} cf_message_type;

typedef struct cf_request_message {
  cf_bytes service;
  cf_bytes method;
  cf_bytes body;
  cf_metadata metadata;
  uint32_t kind;
  uint32_t version;
  uint64_t timeout_nanos;
} cf_request_message;

typedef struct cf_response_message {
  uint32_t status;
  cf_bytes message;
  cf_bytes body;
  cf_metadata metadata;
} cf_response_message;

typedef struct cf_stream_frame_message {
  uint32_t kind;
  uint32_t status;
  cf_bytes message;
  cf_bytes body;
  cf_metadata metadata;
} cf_stream_frame_message;

typedef struct cf_message {
  cf_message_type type;
  union {
    cf_request_message request;
    cf_response_message response;
    cf_stream_frame_message stream_frame;
  } value;
} cf_message;

typedef struct cf_command {
  char *sequence;
  char *case_id;
  char *operation;
  cf_message_type message_type;
  cf_message message;
  size_t max_frame_size;
  cf_bytes *chunks;
  size_t chunk_count;
  cf_bytes *frames;
  size_t frame_count;
} cf_command;

typedef struct cf_error {
  const char *category;
  uint32_t status_code;
} cf_error;

typedef struct cf_json {
  char *data;
  size_t len;
  size_t capacity;
  int failed;
} cf_json;

typedef int (*cf_state_dispatch_fn)(const cf_command *command, cf_json *payload,
                                    cf_error *error);

int cf_peer_main(const char *peer, int argc, char **argv,
                 cf_state_dispatch_fn state_dispatch);
int cf_c_state_dispatch(const cf_command *command, cf_json *payload,
                        cf_error *error);

void cf_json_init(cf_json *json, char *data, size_t capacity);
void cf_json_append(cf_json *json, const char *value);
void cf_json_append_char(cf_json *json, char value);
void cf_json_append_u32(cf_json *json, uint32_t value);
void cf_json_append_size_string(cf_json *json, size_t value);
void cf_json_append_u64_string(cf_json *json, uint64_t value);
void cf_json_append_hex(cf_json *json, const uint8_t *data, size_t len);
void cf_json_append_metadata(cf_json *json, const cf_metadata *metadata);
void cf_json_append_native_metadata(cf_json *json, const void *metadata);
void cf_error_set(cf_error *error, const char *category, uint32_t status_code);

#ifdef __cplusplus
}
#endif

#endif
