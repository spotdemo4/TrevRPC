#include "trevrpc_values_internal.h"

#include <errno.h> // IWYU pragma: keep
#include <stdlib.h>
#include <string.h>

uint32_t trevrpc_status_code_from_uint32(uint32_t code) {
    return trevrpc_internal_status_code_from_uint32(code);
}

const char* trevrpc_status_code_string(uint32_t code) {
    switch (trevrpc_internal_status_code_from_uint32(code)) {
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
        .code = trevrpc_internal_status_code_from_uint32(code),
        .message = message,
        .message_len = message == NULL ? 0 : message_len,
    };
}

trevrpc_status trevrpc_status_ok(void) {
    return trevrpc_status_new(TREVRPC_STATUS_OK, NULL, 0);
}
#define TREVRPC_DEFINE_STATUS(name, code)                                                                              \
    trevrpc_status trevrpc_status_##name(const char* message, size_t message_len) {                                    \
        return trevrpc_status_new(code, message, message_len);                                                         \
    }
TREVRPC_DEFINE_STATUS(cancelled, TREVRPC_STATUS_CANCELLED)
TREVRPC_DEFINE_STATUS(unknown, TREVRPC_STATUS_UNKNOWN)
TREVRPC_DEFINE_STATUS(invalid_argument, TREVRPC_STATUS_INVALID_ARGUMENT)
TREVRPC_DEFINE_STATUS(deadline_exceeded, TREVRPC_STATUS_DEADLINE_EXCEEDED)
TREVRPC_DEFINE_STATUS(not_found, TREVRPC_STATUS_NOT_FOUND)
TREVRPC_DEFINE_STATUS(already_exists, TREVRPC_STATUS_ALREADY_EXISTS)
TREVRPC_DEFINE_STATUS(permission_denied, TREVRPC_STATUS_PERMISSION_DENIED)
TREVRPC_DEFINE_STATUS(resource_exhausted, TREVRPC_STATUS_RESOURCE_EXHAUSTED)
TREVRPC_DEFINE_STATUS(failed_precondition, TREVRPC_STATUS_FAILED_PRECONDITION)
TREVRPC_DEFINE_STATUS(aborted, TREVRPC_STATUS_ABORTED)
TREVRPC_DEFINE_STATUS(out_of_range, TREVRPC_STATUS_OUT_OF_RANGE)
TREVRPC_DEFINE_STATUS(unimplemented, TREVRPC_STATUS_UNIMPLEMENTED)
TREVRPC_DEFINE_STATUS(internal, TREVRPC_STATUS_INTERNAL)
TREVRPC_DEFINE_STATUS(unavailable, TREVRPC_STATUS_UNAVAILABLE)
TREVRPC_DEFINE_STATUS(data_loss, TREVRPC_STATUS_DATA_LOSS)
TREVRPC_DEFINE_STATUS(unauthenticated, TREVRPC_STATUS_UNAUTHENTICATED)
#undef TREVRPC_DEFINE_STATUS

int trevrpc_metadata_set(
    trevrpc_metadata* metadata, const char* key, size_t key_len, const uint8_t* value, size_t value_len) {
    return trevrpc_internal_metadata_set(metadata, key, key_len, value, value_len);
}

int trevrpc_metadata_set_normalized(
    trevrpc_metadata* metadata, const char* key, size_t key_len, const uint8_t* value, size_t value_len) {
    if (metadata == NULL || key == NULL || key_len == 0 || (value == NULL && value_len > 0) || key_len == SIZE_MAX) {
        return -EINVAL;
    }
    char* normalized_key = malloc(key_len + 1);
    if (normalized_key == NULL) {
        return -ENOMEM;
    }
    for (size_t i = 0; i < key_len; ++i) {
        char ch = key[i];
        normalized_key[i] = (ch >= 'A' && ch <= 'Z') ? (char)(ch - 'A' + 'a') : ch;
    }
    normalized_key[key_len] = '\0';
    int err = trevrpc_internal_metadata_set(metadata, normalized_key, key_len, value, value_len);
    free(normalized_key);
    return err;
}

int trevrpc_metadata_validate(const trevrpc_metadata* metadata) {
    return trevrpc_internal_metadata_validate(metadata);
}

void trevrpc_metadata_reset(trevrpc_metadata* metadata) {
    trevrpc_internal_metadata_reset(metadata);
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
    for (size_t i = 0; i < metadata->entries_len; ++i) {
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
        .key = key, .key_len = sizeof(key) - 1, .value = expected, .value_len = expected_len};
    int err = trevrpc_authorize_metadata_value(&metadata_authorizer, context, request, status);
    free(expected);
    return err;
}

void trevrpc_request_reset(trevrpc_request* request) {
    trevrpc_internal_request_reset(request);
}
