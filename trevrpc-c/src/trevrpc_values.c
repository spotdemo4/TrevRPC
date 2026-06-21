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
