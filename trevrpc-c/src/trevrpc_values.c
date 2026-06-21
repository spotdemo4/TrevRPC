#include "trevrpc.h"

#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

static bool trevrpc_metadata_key_byte_valid(char value) {
    return (value >= 'a' && value <= 'z') || (value >= '0' && value <= '9') || value == '.' || value == '_' ||
           value == '-';
}

static int trevrpc_metadata_entry_validate(const trevrpc_metadata_entry* entry) {
    if (entry == NULL || entry->key == NULL || entry->key_len == 0 || (entry->value == NULL && entry->value_len > 0)) {
        return -EINVAL;
    }
    if (entry->key_len > TREVRPC_MAX_METADATA_KEY_LEN || entry->value_len > TREVRPC_MAX_METADATA_VALUE_LEN) {
        return -EINVAL;
    }
    if (entry->key_len >= sizeof(TREVRPC_RESERVED_METADATA_PREFIX) - 1 &&
        memcmp(entry->key, TREVRPC_RESERVED_METADATA_PREFIX, sizeof(TREVRPC_RESERVED_METADATA_PREFIX) - 1) == 0) {
        return -EINVAL;
    }
    for (size_t i = 0; i < entry->key_len; i++) {
        if (!trevrpc_metadata_key_byte_valid(entry->key[i])) {
            return -EINVAL;
        }
    }

    return 0;
}

static int trevrpc_copy_chars(char** dst, size_t* dst_len, const char* src, size_t src_len) {
    if (src == NULL && src_len > 0) {
        return -EINVAL;
    }

    char* copy = NULL;
    if (src_len > 0) {
        copy = malloc(src_len);
        if (copy == NULL) {
            return -ENOMEM;
        }
        memcpy(copy, src, src_len);
    }

    free(*dst);
    *dst = copy;
    *dst_len = src_len;
    return 0;
}

static int trevrpc_copy_bytes(uint8_t** dst, size_t* dst_len, const uint8_t* src, size_t src_len) {
    if (src == NULL && src_len > 0) {
        return -EINVAL;
    }

    uint8_t* copy = NULL;
    if (src_len > 0) {
        copy = malloc(src_len);
        if (copy == NULL) {
            return -ENOMEM;
        }
        memcpy(copy, src, src_len);
    }

    free(*dst);
    *dst = copy;
    *dst_len = src_len;
    return 0;
}

uint32_t trevrpc_status_code_from_uint32(uint32_t code) {
    switch (code) {
    case TREVRPC_STATUS_OK:
    case TREVRPC_STATUS_CANCELLED:
    case TREVRPC_STATUS_UNKNOWN:
    case TREVRPC_STATUS_INVALID_ARGUMENT:
    case TREVRPC_STATUS_DEADLINE_EXCEEDED:
    case TREVRPC_STATUS_NOT_FOUND:
    case TREVRPC_STATUS_ALREADY_EXISTS:
    case TREVRPC_STATUS_PERMISSION_DENIED:
    case TREVRPC_STATUS_RESOURCE_EXHAUSTED:
    case TREVRPC_STATUS_FAILED_PRECONDITION:
    case TREVRPC_STATUS_ABORTED:
    case TREVRPC_STATUS_OUT_OF_RANGE:
    case TREVRPC_STATUS_UNIMPLEMENTED:
    case TREVRPC_STATUS_INTERNAL:
    case TREVRPC_STATUS_UNAVAILABLE:
    case TREVRPC_STATUS_DATA_LOSS:
    case TREVRPC_STATUS_UNAUTHENTICATED:
        return code;
    default:
        return TREVRPC_STATUS_UNKNOWN;
    }
}

const char* trevrpc_status_code_string(uint32_t code) {
    switch (trevrpc_status_code_from_uint32(code)) {
    case TREVRPC_STATUS_OK:
        return "Ok";
    case TREVRPC_STATUS_CANCELLED:
        return "Cancelled";
    case TREVRPC_STATUS_INVALID_ARGUMENT:
        return "InvalidArgument";
    case TREVRPC_STATUS_DEADLINE_EXCEEDED:
        return "DeadlineExceeded";
    case TREVRPC_STATUS_NOT_FOUND:
        return "NotFound";
    case TREVRPC_STATUS_ALREADY_EXISTS:
        return "AlreadyExists";
    case TREVRPC_STATUS_PERMISSION_DENIED:
        return "PermissionDenied";
    case TREVRPC_STATUS_RESOURCE_EXHAUSTED:
        return "ResourceExhausted";
    case TREVRPC_STATUS_FAILED_PRECONDITION:
        return "FailedPrecondition";
    case TREVRPC_STATUS_ABORTED:
        return "Aborted";
    case TREVRPC_STATUS_OUT_OF_RANGE:
        return "OutOfRange";
    case TREVRPC_STATUS_UNIMPLEMENTED:
        return "Unimplemented";
    case TREVRPC_STATUS_INTERNAL:
        return "Internal";
    case TREVRPC_STATUS_UNAVAILABLE:
        return "Unavailable";
    case TREVRPC_STATUS_DATA_LOSS:
        return "DataLoss";
    case TREVRPC_STATUS_UNAUTHENTICATED:
        return "Unauthenticated";
    case TREVRPC_STATUS_UNKNOWN:
    default:
        return "Unknown";
    }
}

trevrpc_status trevrpc_status_new(uint32_t code, const char* message, size_t message_len) {
    return (trevrpc_status){
        .code = trevrpc_status_code_from_uint32(code),
        .message = message,
        .message_len = message == NULL ? 0 : message_len,
    };
}

trevrpc_status trevrpc_status_ok(void) {
    return trevrpc_status_new(TREVRPC_STATUS_OK, NULL, 0);
}

trevrpc_status trevrpc_status_cancelled(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_CANCELLED, message, message_len);
}

trevrpc_status trevrpc_status_unknown(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_UNKNOWN, message, message_len);
}

trevrpc_status trevrpc_status_invalid_argument(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_INVALID_ARGUMENT, message, message_len);
}

trevrpc_status trevrpc_status_deadline_exceeded(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_DEADLINE_EXCEEDED, message, message_len);
}

trevrpc_status trevrpc_status_not_found(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_NOT_FOUND, message, message_len);
}

trevrpc_status trevrpc_status_already_exists(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_ALREADY_EXISTS, message, message_len);
}

trevrpc_status trevrpc_status_permission_denied(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_PERMISSION_DENIED, message, message_len);
}

trevrpc_status trevrpc_status_resource_exhausted(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_RESOURCE_EXHAUSTED, message, message_len);
}

trevrpc_status trevrpc_status_failed_precondition(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_FAILED_PRECONDITION, message, message_len);
}

trevrpc_status trevrpc_status_aborted(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_ABORTED, message, message_len);
}

trevrpc_status trevrpc_status_out_of_range(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_OUT_OF_RANGE, message, message_len);
}

trevrpc_status trevrpc_status_unimplemented(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_UNIMPLEMENTED, message, message_len);
}

trevrpc_status trevrpc_status_internal(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_INTERNAL, message, message_len);
}

trevrpc_status trevrpc_status_unavailable(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_UNAVAILABLE, message, message_len);
}

trevrpc_status trevrpc_status_data_loss(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_DATA_LOSS, message, message_len);
}

trevrpc_status trevrpc_status_unauthenticated(const char* message, size_t message_len) {
    return trevrpc_status_new(TREVRPC_STATUS_UNAUTHENTICATED, message, message_len);
}

int trevrpc_metadata_set(
    trevrpc_metadata* metadata, const char* key, size_t key_len, const uint8_t* value, size_t value_len) {
    if (metadata == NULL || key == NULL || key_len == 0 || (value == NULL && value_len > 0)) {
        return -EINVAL;
    }

    trevrpc_metadata_entry new_entry = {
        .key = (char*)key,
        .key_len = key_len,
        .value = (uint8_t*)value,
        .value_len = value_len,
    };
    int err = trevrpc_metadata_entry_validate(&new_entry);
    if (err != 0) {
        return err;
    }

    size_t existing_index = SIZE_MAX;
    size_t total_size = 0;
    if (metadata->entries_len > 0 && metadata->entries == NULL) {
        return -EINVAL;
    }
    for (size_t i = 0; i < metadata->entries_len; i++) {
        trevrpc_metadata_entry* entry = &metadata->entries[i];
        err = trevrpc_metadata_entry_validate(entry);
        if (err != 0) {
            return err;
        }
        if (entry->key_len == key_len && memcmp(entry->key, key, key_len) == 0) {
            existing_index = i;
            continue;
        }
        if (entry->key_len > SIZE_MAX - total_size || entry->value_len > SIZE_MAX - total_size - entry->key_len) {
            return -EINVAL;
        }
        total_size += entry->key_len + entry->value_len;
    }
    if (key_len > SIZE_MAX - total_size || value_len > SIZE_MAX - total_size - key_len) {
        return -EINVAL;
    }
    if (total_size + key_len + value_len > TREVRPC_MAX_METADATA_TOTAL_SIZE) {
        return -EINVAL;
    }

    char* key_copy = NULL;
    if (existing_index == SIZE_MAX) {
        if (metadata->entries_len >= TREVRPC_MAX_METADATA_ENTRIES) {
            return -EINVAL;
        }
        if (key_len == SIZE_MAX) {
            return -EINVAL;
        }
        key_copy = malloc(key_len + 1);
        if (key_copy == NULL) {
            return -ENOMEM;
        }
        memcpy(key_copy, key, key_len);
        key_copy[key_len] = '\0';
    }

    uint8_t* value_copy = NULL;
    if (value_len > 0) {
        value_copy = malloc(value_len);
        if (value_copy == NULL) {
            free(key_copy);
            return -ENOMEM;
        }
        memcpy(value_copy, value, value_len);
    }

    if (existing_index != SIZE_MAX) {
        trevrpc_metadata_entry* entry = &metadata->entries[existing_index];
        free(entry->value);
        entry->value = value_copy;
        entry->value_len = value_len;
        return 0;
    }

    trevrpc_metadata_entry* entries = realloc(metadata->entries, (metadata->entries_len + 1) * sizeof(*entries));
    if (entries == NULL) {
        free(key_copy);
        free(value_copy);
        return -ENOMEM;
    }

    metadata->entries = entries;
    metadata->entries[metadata->entries_len] = (trevrpc_metadata_entry){
        .key = key_copy,
        .key_len = key_len,
        .value = value_copy,
        .value_len = value_len,
    };
    metadata->entries_len++;
    return 0;
}

int trevrpc_metadata_validate(const trevrpc_metadata* metadata) {
    if (metadata == NULL) {
        return 0;
    }
    if (metadata->entries_len > TREVRPC_MAX_METADATA_ENTRIES ||
        (metadata->entries_len > 0 && metadata->entries == NULL)) {
        return -EINVAL;
    }

    size_t total_size = 0;
    for (size_t i = 0; i < metadata->entries_len; i++) {
        const trevrpc_metadata_entry* entry = &metadata->entries[i];
        int err = trevrpc_metadata_entry_validate(entry);
        if (err != 0) {
            return err;
        }
        for (size_t j = i + 1; j < metadata->entries_len; j++) {
            const trevrpc_metadata_entry* other = &metadata->entries[j];
            if (entry->key != NULL && entry->key_len == other->key_len && other->key != NULL &&
                memcmp(entry->key, other->key, entry->key_len) == 0) {
                return -EINVAL;
            }
        }
        if (entry->key_len > SIZE_MAX - total_size || entry->value_len > SIZE_MAX - total_size - entry->key_len) {
            return -EINVAL;
        }
        total_size += entry->key_len + entry->value_len;
    }

    return total_size > TREVRPC_MAX_METADATA_TOTAL_SIZE ? -EINVAL : 0;
}

void trevrpc_metadata_reset(trevrpc_metadata* metadata) {
    if (metadata == NULL) {
        return;
    }

    for (size_t i = 0; i < metadata->entries_len; i++) {
        free(metadata->entries[i].key);
        free(metadata->entries[i].value);
    }
    free(metadata->entries);
    metadata->entries = NULL;
    metadata->entries_len = 0;
}

static int trevrpc_status_copy_message(trevrpc_status* status, uint32_t code, const char* message) {
    if (status == NULL) {
        return -EINVAL;
    }

    *status = trevrpc_status_new(code, message, message == NULL ? 0 : strlen(message));
    return 0;
}

static int trevrpc_metadata_contains_value(
    const trevrpc_metadata* metadata, const char* key, size_t key_len, const uint8_t* value, size_t value_len) {
    if (metadata == NULL || key == NULL || key_len == 0 || (value == NULL && value_len > 0)) {
        return 0;
    }

    for (size_t i = 0; i < metadata->entries_len; i++) {
        const trevrpc_metadata_entry* entry = &metadata->entries[i];
        if (entry->key_len == key_len && entry->value_len == value_len && memcmp(entry->key, key, key_len) == 0 &&
            (value_len == 0 || memcmp(entry->value, value, value_len) == 0)) {
            return 1;
        }
    }

    return 0;
}

int trevrpc_authorize_metadata_value(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_status* status) {
    (void)context;
    const trevrpc_metadata_value_authorizer* authorizer = user_data;
    if (authorizer == NULL || request == NULL || status == NULL || authorizer->key == NULL ||
        authorizer->key_len == 0 || (authorizer->value == NULL && authorizer->value_len > 0)) {
        return -EINVAL;
    }

    if (trevrpc_metadata_contains_value(
            &request->metadata, authorizer->key, authorizer->key_len, authorizer->value, authorizer->value_len)) {
        *status = trevrpc_status_ok();
        return 0;
    }

    return trevrpc_status_copy_message(status, TREVRPC_STATUS_UNAUTHENTICATED, "request is not authenticated");
}

int trevrpc_authorize_bearer_token(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_status* status) {
    (void)context;
    const trevrpc_bearer_authorizer* authorizer = user_data;
    if (authorizer == NULL || request == NULL || status == NULL || authorizer->token == NULL) {
        return -EINVAL;
    }

    const char prefix[] = "Bearer ";
    if (authorizer->token_len > SIZE_MAX - (sizeof(prefix) - 1)) {
        return -EINVAL;
    }

    size_t expected_len = sizeof(prefix) - 1 + authorizer->token_len;
    uint8_t* expected = malloc(expected_len == 0 ? 1 : expected_len);
    if (expected == NULL) {
        return -ENOMEM;
    }
    memcpy(expected, prefix, sizeof(prefix) - 1);
    memcpy(expected + sizeof(prefix) - 1, authorizer->token, authorizer->token_len);

    const char key[] = "authorization";
    trevrpc_metadata_value_authorizer metadata_authorizer = {
        .key = key,
        .key_len = sizeof(key) - 1,
        .value = expected,
        .value_len = expected_len,
    };
    int err = trevrpc_authorize_metadata_value(&metadata_authorizer, context, request, status);
    free(expected);
    return err;
}

void trevrpc_request_reset(trevrpc_request* request) {
    if (request == NULL) {
        return;
    }

    trevrpc_metadata_reset(&request->metadata);
    memset(request, 0, sizeof(*request));
}

int trevrpc_response_set_message(trevrpc_response* response, const char* message, size_t message_len) {
    if (response == NULL) {
        return -EINVAL;
    }

    return trevrpc_copy_chars(&response->message, &response->message_len, message, message_len);
}

int trevrpc_response_set_body(trevrpc_response* response, const uint8_t* body, size_t body_len) {
    if (response == NULL) {
        return -EINVAL;
    }

    return trevrpc_copy_bytes(&response->body, &response->body_len, body, body_len);
}

int trevrpc_response_set_status(trevrpc_response* response, trevrpc_status status) {
    if (response == NULL) {
        return -EINVAL;
    }

    int err = trevrpc_response_set_message(response, status.message, status.message_len);
    if (err != 0) {
        return err;
    }
    response->status = trevrpc_status_code_from_uint32(status.code);
    return 0;
}

void trevrpc_response_reset(trevrpc_response* response) {
    if (response == NULL) {
        return;
    }

    free(response->message);
    free(response->body);
    trevrpc_metadata_reset(&response->metadata);
    response->status = TREVRPC_STATUS_OK;
    response->message = NULL;
    response->message_len = 0;
    response->body = NULL;
    response->body_len = 0;
}

void trevrpc_response_free(trevrpc_response* response) {
    if (response == NULL) {
        return;
    }

    trevrpc_response_reset(response);
    free(response);
}

int trevrpc_stream_frame_set_message(trevrpc_stream_frame* frame, const char* message, size_t message_len) {
    if (frame == NULL) {
        return -EINVAL;
    }

    return trevrpc_copy_chars(&frame->message, &frame->message_len, message, message_len);
}

int trevrpc_stream_frame_set_body(trevrpc_stream_frame* frame, const uint8_t* body, size_t body_len) {
    if (frame == NULL) {
        return -EINVAL;
    }

    return trevrpc_copy_bytes(&frame->body, &frame->body_len, body, body_len);
}

int trevrpc_stream_frame_set_status(trevrpc_stream_frame* frame, trevrpc_status status) {
    if (frame == NULL) {
        return -EINVAL;
    }

    int err = trevrpc_stream_frame_set_message(frame, status.message, status.message_len);
    if (err != 0) {
        return err;
    }
    err = trevrpc_stream_frame_set_body(frame, NULL, 0);
    if (err != 0) {
        return err;
    }
    frame->kind = TREVRPC_STREAM_FRAME_KIND_STATUS;
    frame->status = trevrpc_status_code_from_uint32(status.code);
    return 0;
}

void trevrpc_stream_frame_reset(trevrpc_stream_frame* frame) {
    if (frame == NULL) {
        return;
    }

    free(frame->message);
    free(frame->body);
    trevrpc_metadata_reset(&frame->metadata);
    frame->kind = TREVRPC_STREAM_FRAME_KIND_MESSAGE;
    frame->status = TREVRPC_STATUS_OK;
    frame->message = NULL;
    frame->message_len = 0;
    frame->body = NULL;
    frame->body_len = 0;
}

void trevrpc_stream_frame_free(trevrpc_stream_frame* frame) {
    if (frame == NULL) {
        return;
    }

    trevrpc_stream_frame_reset(frame);
    free(frame);
}
