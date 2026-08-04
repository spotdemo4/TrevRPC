#include "trevrpc_wire_internal.h"

#include <errno.h> // IWYU pragma: keep
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void trevrpc_wire_owned_free(void* owner, void* context) {
    (void)context;
    free(owner);
}

static atomic_int trevrpc_wire_trace_state = ATOMIC_VAR_INIT(0);

static bool trevrpc_wire_trace_env_enabled(void) {
    const char* value = getenv("TREVRPC_C_FRAME_TRACE");
    return value != NULL && (strcmp(value, "1") == 0 || strcmp(value, "true") == 0 || strcmp(value, "TRUE") == 0 ||
                                strcmp(value, "yes") == 0 || strcmp(value, "on") == 0);
}

static bool trevrpc_wire_should_trace(void) {
    int state = atomic_load_explicit(&trevrpc_wire_trace_state, memory_order_acquire);
    if (state == 0) {
        int initialized = trevrpc_wire_trace_env_enabled() ? 2 : 1;
        int expected = 0;
        (void)atomic_compare_exchange_strong_explicit(
            &trevrpc_wire_trace_state, &expected, initialized, memory_order_release, memory_order_relaxed);
        state = atomic_load_explicit(&trevrpc_wire_trace_state, memory_order_acquire);
    }
    return state == 2;
}

static void trevrpc_wire_trace_frame(
    const char* direction, const char* frame, uint32_t kind, uint32_t status, size_t body_len, size_t encoded_len) {
    if (!trevrpc_wire_should_trace()) {
        return;
    }
    (void)fprintf(stderr,
        "trevrpc-c-frame direction=%s frame=%s kind=%u status=%u body_len=%zu encoded_len=%zu\n",
        direction,
        frame,
        kind,
        status,
        body_len,
        encoded_len);
}

static size_t trevrpc_wire_varint_len(uint64_t value) {
    size_t len = 1;
    while (value >= 0x80) {
        len++;
        value >>= 7;
    }
    return len;
}

static uint8_t* trevrpc_wire_append_varint(uint8_t* out, uint64_t value) {
    while (value >= 0x80) {
        *out++ = (uint8_t)value | 0x80;
        value >>= 7;
    }
    *out++ = (uint8_t)value;
    return out;
}

static size_t trevrpc_wire_bytes_field_len(uint32_t field_number, size_t len) {
    if (len == 0) {
        return 0;
    }

    return trevrpc_wire_varint_len((uint64_t)(field_number << 3u | 2u)) + trevrpc_wire_varint_len((uint64_t)len) + len;
}

static size_t trevrpc_wire_varint_field_len(uint32_t field_number, uint64_t value) {
    if (value == 0) {
        return 0;
    }

    return trevrpc_wire_varint_len((uint64_t)(field_number << 3u)) + trevrpc_wire_varint_len(value);
}

static uint8_t* trevrpc_wire_append_bytes_field(uint8_t* out, uint32_t field_number, const uint8_t* value, size_t len) {
    if (len == 0) {
        return out;
    }

    out = trevrpc_wire_append_varint(out, (uint64_t)(field_number << 3u | 2u));
    out = trevrpc_wire_append_varint(out, (uint64_t)len);
    memcpy(out, value, len);
    return out + len;
}

static uint8_t* trevrpc_wire_append_bytes_field_header(uint8_t* out, uint32_t field_number, size_t len) {
    if (len == 0) {
        return out;
    }

    out = trevrpc_wire_append_varint(out, (uint64_t)(field_number << 3u | 2u));
    return trevrpc_wire_append_varint(out, (uint64_t)len);
}

static uint8_t* trevrpc_wire_append_varint_field(uint8_t* out, uint32_t field_number, uint64_t value) {
    if (value == 0) {
        return out;
    }

    out = trevrpc_wire_append_varint(out, (uint64_t)(field_number << 3u));
    return trevrpc_wire_append_varint(out, value);
}

static size_t trevrpc_wire_metadata_entry_len(const trevrpc_metadata_entry* entry) {
    return trevrpc_wire_bytes_field_len(1, entry->key_len) + trevrpc_wire_bytes_field_len(2, entry->value_len);
}

static size_t trevrpc_wire_metadata_field_len(uint32_t field_number, const trevrpc_metadata* metadata) {
    if (metadata == NULL) {
        return 0;
    }

    size_t len = 0;
    for (size_t i = 0; i < metadata->entries_len; i++) {
        size_t entry_len = trevrpc_wire_metadata_entry_len(&metadata->entries[i]);
        len += trevrpc_wire_varint_len((uint64_t)(field_number << 3u | 2u)) +
               trevrpc_wire_varint_len((uint64_t)entry_len) + entry_len;
    }
    return len;
}

static uint8_t* trevrpc_wire_append_metadata_field(
    uint8_t* out, uint32_t field_number, const trevrpc_metadata* metadata) {
    if (metadata == NULL) {
        return out;
    }

    for (size_t i = 0; i < metadata->entries_len; i++) {
        const trevrpc_metadata_entry* entry = &metadata->entries[i];
        size_t entry_len = trevrpc_wire_metadata_entry_len(entry);
        out = trevrpc_wire_append_varint(out, (uint64_t)(field_number << 3u | 2u));
        out = trevrpc_wire_append_varint(out, (uint64_t)entry_len);
        out = trevrpc_wire_append_bytes_field(out, 1, (const uint8_t*)entry->key, entry->key_len);
        out = trevrpc_wire_append_bytes_field(out, 2, entry->value, entry->value_len);
    }
    return out;
}

static int trevrpc_wire_alloc_frame(size_t body_len, size_t max_frame_size, uint8_t** frame, size_t* frame_len) {
    *frame = NULL;
    *frame_len = 0;
    if (body_len > max_frame_size || body_len > UINT32_MAX) {
        return TREVRPC_ERR_FRAME_TOO_LARGE;
    }
    if (body_len > SIZE_MAX - 4) {
        return TREVRPC_ERR_FRAME_TOO_LARGE;
    }

    uint8_t* buffer = malloc(4 + body_len);
    if (buffer == NULL) {
        return -ENOMEM;
    }

    buffer[0] = (uint8_t)(body_len >> 24);
    buffer[1] = (uint8_t)(body_len >> 16);
    buffer[2] = (uint8_t)(body_len >> 8);
    buffer[3] = (uint8_t)body_len;
    *frame = buffer;
    *frame_len = 4 + body_len;
    return 0;
}

int trevrpc_wire_encode_request(const char* service,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len,
    const trevrpc_metadata* metadata,
    uint64_t timeout_nanos,
    size_t max_frame_size,
    uint8_t** frame,
    size_t* frame_len) {
    if (service == NULL || method == NULL || (body == NULL && body_len > 0)) {
        return -EINVAL;
    }

    return trevrpc_wire_encode_request_view(service,
        strlen(service),
        method,
        strlen(method),
        kind,
        TREVRPC_WIRE_VERSION,
        body,
        body_len,
        metadata,
        timeout_nanos,
        max_frame_size,
        frame,
        frame_len);
}

int trevrpc_wire_encode_request_view(const char* service,
    size_t service_len,
    const char* method,
    size_t method_len,
    uint32_t kind,
    uint32_t version,
    const uint8_t* body,
    size_t body_len,
    const trevrpc_metadata* metadata,
    uint64_t timeout_nanos,
    size_t max_frame_size,
    uint8_t** frame,
    size_t* frame_len) {
    if (service == NULL || method == NULL || (body == NULL && body_len > 0)) {
        return -EINVAL;
    }
    int err = trevrpc_metadata_validate(metadata);
    if (err != 0) {
        return err;
    }

    size_t body_frame_len = trevrpc_wire_bytes_field_len(1, service_len) + trevrpc_wire_bytes_field_len(2, method_len) +
                            trevrpc_wire_bytes_field_len(3, body_len) + trevrpc_wire_metadata_field_len(4, metadata) +
                            trevrpc_wire_varint_field_len(5, kind) + trevrpc_wire_varint_field_len(6, version) +
                            trevrpc_wire_varint_field_len(7, timeout_nanos);
    err = trevrpc_wire_alloc_frame(body_frame_len, max_frame_size, frame, frame_len);
    if (err != 0) {
        return err;
    }

    uint8_t* out = *frame + 4;
    out = trevrpc_wire_append_bytes_field(out, 1, (const uint8_t*)service, service_len);
    out = trevrpc_wire_append_bytes_field(out, 2, (const uint8_t*)method, method_len);
    out = trevrpc_wire_append_bytes_field(out, 3, body, body_len);
    out = trevrpc_wire_append_metadata_field(out, 4, metadata);
    out = trevrpc_wire_append_varint_field(out, 5, kind);
    out = trevrpc_wire_append_varint_field(out, 6, version);
    out = trevrpc_wire_append_varint_field(out, 7, timeout_nanos);
    (void)out;
    trevrpc_wire_trace_frame("tx", "RpcRequest", kind, TREVRPC_STATUS_OK, body_len, body_frame_len);
    return 0;
}

int trevrpc_wire_encode_request_parts(
    const trevrpc_request* request, size_t max_frame_size, trevrpc_wire_frame_parts* parts) {
    if (request == NULL || parts == NULL || request->service == NULL || request->method == NULL ||
        (request->body == NULL && request->body_len > 0)) {
        return -EINVAL;
    }
    memset(parts, 0, sizeof(*parts));
    int err = trevrpc_metadata_validate(&request->metadata);
    if (err != 0) {
        return err;
    }

    size_t body_header_len = request->body_len == 0 ? 0
                                                    : trevrpc_wire_varint_len((uint64_t)(3u << 3u | 2u)) +
                                                          trevrpc_wire_varint_len((uint64_t)request->body_len);
    size_t prefix_len = trevrpc_wire_bytes_field_len(1, request->service_len) +
                        trevrpc_wire_bytes_field_len(2, request->method_len) + body_header_len;
    size_t suffix_len =
        trevrpc_wire_metadata_field_len(4, &request->metadata) + trevrpc_wire_varint_field_len(5, request->kind) +
        trevrpc_wire_varint_field_len(6, request->version) + trevrpc_wire_varint_field_len(7, request->timeout_nanos);
    if (prefix_len > SIZE_MAX - request->body_len || suffix_len > SIZE_MAX - prefix_len - request->body_len) {
        return TREVRPC_ERR_FRAME_TOO_LARGE;
    }
    size_t body_frame_len = prefix_len + request->body_len + suffix_len;
    if (body_frame_len > max_frame_size || body_frame_len > UINT32_MAX) {
        return TREVRPC_ERR_FRAME_TOO_LARGE;
    }

    uint8_t* out = NULL;
    if (prefix_len > 0) {
        parts->prefix = malloc(prefix_len);
        if (parts->prefix == NULL) {
            return -ENOMEM;
        }
        out = parts->prefix;
        out = trevrpc_wire_append_bytes_field(out, 1, (const uint8_t*)request->service, request->service_len);
        out = trevrpc_wire_append_bytes_field(out, 2, (const uint8_t*)request->method, request->method_len);
        out = trevrpc_wire_append_bytes_field_header(out, 3, request->body_len);
        (void)out;
    }

    if (suffix_len > 0) {
        parts->suffix = malloc(suffix_len);
        if (parts->suffix == NULL) {
            trevrpc_wire_frame_parts_reset(parts);
            return -ENOMEM;
        }
        out = parts->suffix;
        out = trevrpc_wire_append_metadata_field(out, 4, &request->metadata);
        out = trevrpc_wire_append_varint_field(out, 5, request->kind);
        out = trevrpc_wire_append_varint_field(out, 6, request->version);
        out = trevrpc_wire_append_varint_field(out, 7, request->timeout_nanos);
        (void)out;
    }

    parts->prefix_len = prefix_len;
    parts->body = request->body;
    parts->body_len = request->body_len;
    parts->suffix_len = suffix_len;
    parts->frame_body_len = body_frame_len;
    trevrpc_wire_trace_frame("tx", "RpcRequest", request->kind, TREVRPC_STATUS_OK, request->body_len, body_frame_len);
    return 0;
}

int trevrpc_wire_encode_response(
    const trevrpc_wire_response_values* response, size_t max_frame_size, uint8_t** frame, size_t* frame_len) {
    if (response == NULL) {
        return -EINVAL;
    }
    int err = trevrpc_metadata_validate(&response->metadata);
    if (err != 0) {
        return err;
    }

    size_t body_frame_len =
        trevrpc_wire_varint_field_len(1, response->status) + trevrpc_wire_bytes_field_len(2, response->message_len) +
        trevrpc_wire_bytes_field_len(3, response->body.len) + trevrpc_wire_metadata_field_len(4, &response->metadata);
    err = trevrpc_wire_alloc_frame(body_frame_len, max_frame_size, frame, frame_len);
    if (err != 0) {
        return err;
    }

    uint8_t* out = *frame + 4;
    out = trevrpc_wire_append_varint_field(out, 1, response->status);
    out = trevrpc_wire_append_bytes_field(out, 2, (const uint8_t*)response->message, response->message_len);
    out = trevrpc_wire_append_bytes_field(out, 3, response->body.data, response->body.len);
    out = trevrpc_wire_append_metadata_field(out, 4, &response->metadata);
    (void)out;
    trevrpc_wire_trace_frame("tx", "RpcResponse", 0, response->status, response->body.len, body_frame_len);
    return 0;
}

void trevrpc_wire_frame_parts_reset(trevrpc_wire_frame_parts* parts) {
    if (parts == NULL) {
        return;
    }

    free(parts->prefix);
    free(parts->suffix);
    memset(parts, 0, sizeof(*parts));
}

int trevrpc_wire_encode_response_parts(
    const trevrpc_wire_response_values* response, size_t max_frame_size, trevrpc_wire_frame_parts* parts) {
    if (response == NULL || parts == NULL || (response->message == NULL && response->message_len > 0) ||
        (response->body.data == NULL && response->body.len > 0)) {
        return -EINVAL;
    }
    memset(parts, 0, sizeof(*parts));
    int err = trevrpc_metadata_validate(&response->metadata);
    if (err != 0) {
        return err;
    }

    size_t body_header_len = response->body.len == 0 ? 0
                                                     : trevrpc_wire_varint_len((uint64_t)(3u << 3u | 2u)) +
                                                           trevrpc_wire_varint_len((uint64_t)response->body.len);
    size_t prefix_len = trevrpc_wire_varint_field_len(1, response->status) +
                        trevrpc_wire_bytes_field_len(2, response->message_len) + body_header_len;
    size_t suffix_len = trevrpc_wire_metadata_field_len(4, &response->metadata);
    size_t body_frame_len = prefix_len + response->body.len + suffix_len;
    if (body_frame_len > max_frame_size || body_frame_len > UINT32_MAX) {
        return TREVRPC_ERR_FRAME_TOO_LARGE;
    }

    if (prefix_len > 0) {
        parts->prefix = malloc(prefix_len);
        if (parts->prefix == NULL) {
            return -ENOMEM;
        }
        uint8_t* out = parts->prefix;
        out = trevrpc_wire_append_varint_field(out, 1, response->status);
        out = trevrpc_wire_append_bytes_field(out, 2, (const uint8_t*)response->message, response->message_len);
        out = trevrpc_wire_append_bytes_field_header(out, 3, response->body.len);
        (void)out;
    }
    if (suffix_len > 0) {
        parts->suffix = malloc(suffix_len);
        if (parts->suffix == NULL) {
            trevrpc_wire_frame_parts_reset(parts);
            return -ENOMEM;
        }
        uint8_t* out = trevrpc_wire_append_metadata_field(parts->suffix, 4, &response->metadata);
        (void)out;
    }
    parts->prefix_len = prefix_len;
    parts->body = response->body.data;
    parts->body_len = response->body.len;
    parts->suffix_len = suffix_len;
    parts->frame_body_len = body_frame_len;
    trevrpc_wire_trace_frame("tx", "RpcResponse", 0, response->status, response->body.len, body_frame_len);
    return 0;
}

int trevrpc_wire_encode_stream_frame(uint32_t kind,
    uint32_t status,
    const char* message,
    size_t message_len,
    const uint8_t* body,
    size_t body_len,
    const trevrpc_metadata* metadata,
    size_t max_frame_size,
    uint8_t** frame,
    size_t* frame_len) {
    if ((message == NULL && message_len > 0) || (body == NULL && body_len > 0)) {
        return -EINVAL;
    }
    int err = trevrpc_metadata_validate(metadata);
    if (err != 0) {
        return err;
    }

    size_t body_frame_len = trevrpc_wire_varint_field_len(1, kind) + trevrpc_wire_varint_field_len(2, status) +
                            trevrpc_wire_bytes_field_len(3, message_len) + trevrpc_wire_bytes_field_len(4, body_len) +
                            trevrpc_wire_metadata_field_len(5, metadata);
    err = trevrpc_wire_alloc_frame(body_frame_len, max_frame_size, frame, frame_len);
    if (err != 0) {
        return err;
    }

    uint8_t* out = *frame + 4;
    out = trevrpc_wire_append_varint_field(out, 1, kind);
    out = trevrpc_wire_append_varint_field(out, 2, status);
    out = trevrpc_wire_append_bytes_field(out, 3, (const uint8_t*)message, message_len);
    out = trevrpc_wire_append_bytes_field(out, 4, body, body_len);
    out = trevrpc_wire_append_metadata_field(out, 5, metadata);
    (void)out;
    trevrpc_wire_trace_frame("tx", "RpcStreamFrame", kind, status, body_len, body_frame_len);
    return 0;
}

int trevrpc_wire_encode_stream_status_parts(uint32_t status,
    const char* message,
    size_t message_len,
    const trevrpc_metadata* metadata,
    size_t max_frame_size,
    trevrpc_wire_frame_parts* parts) {
    if (parts == NULL || (message == NULL && message_len > 0)) {
        return -EINVAL;
    }
    memset(parts, 0, sizeof(*parts));
    int err = trevrpc_metadata_validate(metadata);
    if (err != 0) {
        return err;
    }

    size_t body_frame_len = trevrpc_wire_varint_field_len(1, TREVRPC_STREAM_FRAME_KIND_STATUS) +
                            trevrpc_wire_varint_field_len(2, status) + trevrpc_wire_bytes_field_len(3, message_len) +
                            trevrpc_wire_metadata_field_len(5, metadata);
    if (body_frame_len > max_frame_size || body_frame_len > UINT32_MAX) {
        return TREVRPC_ERR_FRAME_TOO_LARGE;
    }

    if (body_frame_len > 0) {
        parts->prefix = malloc(body_frame_len);
        if (parts->prefix == NULL) {
            return -ENOMEM;
        }
        uint8_t* out = parts->prefix;
        out = trevrpc_wire_append_varint_field(out, 1, TREVRPC_STREAM_FRAME_KIND_STATUS);
        out = trevrpc_wire_append_varint_field(out, 2, status);
        out = trevrpc_wire_append_bytes_field(out, 3, (const uint8_t*)message, message_len);
        out = trevrpc_wire_append_metadata_field(out, 5, metadata);
        (void)out;
    }
    parts->prefix_len = body_frame_len;
    parts->frame_body_len = body_frame_len;
    trevrpc_wire_trace_frame("tx", "RpcStreamFrame", TREVRPC_STREAM_FRAME_KIND_STATUS, status, 0, body_frame_len);
    return 0;
}

int trevrpc_wire_encode_stream_message_parts(
    const uint8_t* body, size_t body_len, size_t max_frame_size, trevrpc_wire_frame_parts* parts) {
    if (parts == NULL || (body == NULL && body_len > 0)) {
        return -EINVAL;
    }
    memset(parts, 0, sizeof(*parts));

    size_t prefix_len = 0;
    if (body_len > 0) {
        prefix_len = trevrpc_wire_varint_len((uint64_t)(4u << 3u | 2u)) + trevrpc_wire_varint_len((uint64_t)body_len);
    }
    size_t body_frame_len = prefix_len + body_len;
    if (body_frame_len > max_frame_size || body_frame_len > UINT32_MAX) {
        return TREVRPC_ERR_FRAME_TOO_LARGE;
    }

    if (prefix_len > 0) {
        parts->prefix = malloc(prefix_len);
        if (parts->prefix == NULL) {
            return -ENOMEM;
        }
        uint8_t* out = parts->prefix;
        out = trevrpc_wire_append_bytes_field_header(out, 4, body_len);
        (void)out;
    }

    parts->prefix_len = prefix_len;
    parts->body = body;
    parts->body_len = body_len;
    parts->frame_body_len = body_frame_len;
    trevrpc_wire_trace_frame(
        "tx", "RpcStreamFrame", TREVRPC_STREAM_FRAME_KIND_MESSAGE, TREVRPC_STATUS_OK, body_len, body_frame_len);
    return 0;
}

static bool trevrpc_wire_consume_varint(const uint8_t* data, size_t len, size_t* offset, uint64_t* value) {
    uint64_t result = 0;
    for (unsigned int shift = 0; shift < 64; shift += 7) {
        if (*offset >= len) {
            return false;
        }

        uint8_t byte = data[*offset];
        *offset += 1;
        if (shift == 63 && byte > 1) {
            return false;
        }
        result |= (uint64_t)(byte & 0x7f) << shift;
        if (byte < 0x80) {
            *value = result;
            return true;
        }
    }

    return false;
}

static bool trevrpc_wire_consume_bytes(
    const uint8_t* data, size_t len, size_t* offset, const uint8_t** value, size_t* value_len) {
    uint64_t parsed_len = 0;
    if (!trevrpc_wire_consume_varint(data, len, offset, &parsed_len) || parsed_len > len - *offset) {
        return false;
    }

    *value = data + *offset;
    *value_len = (size_t)parsed_len;
    *offset += (size_t)parsed_len;
    return true;
}

static bool trevrpc_wire_skip_field(const uint8_t* data, size_t len, size_t* offset, uint64_t wire_type) {
    uint64_t ignored = 0;
    switch (wire_type) {
    case 0:
        return trevrpc_wire_consume_varint(data, len, offset, &ignored);
    case 1:
        if (len - *offset < 8) {
            return false;
        }
        *offset += 8;
        return true;
    case 2: {
        const uint8_t* value = NULL;
        size_t value_len = 0;
        return trevrpc_wire_consume_bytes(data, len, offset, &value, &value_len);
    }
    case 5:
        if (len - *offset < 4) {
            return false;
        }
        *offset += 4;
        return true;
    default:
        return false;
    }
}

static bool trevrpc_wire_skip_typed_field(
    const uint8_t* data, size_t len, size_t* offset, uint64_t wire_type, uint64_t field_number) {
    if (wire_type != 3) {
        return trevrpc_wire_skip_field(data, len, offset, wire_type);
    }

    while (*offset < len) {
        uint64_t tag = 0;
        if (!trevrpc_wire_consume_varint(data, len, offset, &tag) || tag == 0) {
            return false;
        }
        uint64_t nested_field = tag >> 3u;
        uint64_t nested_wire_type = tag & 0x7u;
        if (nested_field == 0 || nested_field > 0x1fffffffu) {
            return false;
        }
        if (nested_wire_type == 4) {
            return nested_field == field_number;
        }
        if (!trevrpc_wire_skip_typed_field(data, len, offset, nested_wire_type, nested_field)) {
            return false;
        }
    }
    return false;
}

int trevrpc_wire_canonicalize_bytes_field(
    const uint8_t* data, size_t len, uint32_t field_number, uint8_t** canonical, size_t* canonical_len) {
    if ((data == NULL && len > 0) || field_number == 0 || field_number > 0x1fffffffu || canonical == NULL ||
        canonical_len == NULL) {
        return -EINVAL;
    }
    *canonical = NULL;
    *canonical_len = 0;

    const uint8_t* value = NULL;
    size_t value_len = 0;
    for (size_t offset = 0; offset < len;) {
        uint64_t tag = 0;
        if (!trevrpc_wire_consume_varint(data, len, &offset, &tag) || tag == 0) {
            return -EINVAL;
        }
        uint64_t parsed_field = tag >> 3u;
        uint64_t wire_type = tag & 0x7u;
        if (parsed_field == 0 || parsed_field > 0x1fffffffu || wire_type == 4) {
            return -EINVAL;
        }
        if (parsed_field == field_number) {
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                return -EINVAL;
            }
        } else if (!trevrpc_wire_skip_typed_field(data, len, &offset, wire_type, parsed_field)) {
            return -EINVAL;
        }
    }

    if (value_len == 0) {
        return 0;
    }
    size_t tag_len = trevrpc_wire_varint_len((uint64_t)(field_number << 3u | 2u));
    size_t length_len = trevrpc_wire_varint_len((uint64_t)value_len);
    if (tag_len > SIZE_MAX - length_len || value_len > SIZE_MAX - tag_len - length_len) {
        return -EOVERFLOW;
    }
    *canonical_len = tag_len + length_len + value_len;
    *canonical = malloc(*canonical_len);
    if (*canonical == NULL) {
        *canonical_len = 0;
        return -ENOMEM;
    }
    (void)trevrpc_wire_append_bytes_field(*canonical, field_number, value, value_len);
    return 0;
}

static void trevrpc_wire_set_diagnostic(trevrpc_wire_diagnostic_reason* reason, trevrpc_wire_diagnostic_reason value) {
    if (reason != NULL && *reason == TREVRPC_WIRE_DIAGNOSTIC_NONE) {
        *reason = value;
    }
}

static bool trevrpc_wire_utf8_valid(const uint8_t* data, size_t len) {
    size_t offset = 0;
    while (offset < len) {
        uint8_t first = data[offset++];
        if (first < 0x80) {
            continue;
        }
        if (first < 0xc2) {
            return false;
        }
        if (first < 0xe0) {
            if (offset >= len || data[offset] < 0x80 || data[offset] > 0xbf) {
                return false;
            }
            offset++;
            continue;
        }
        if (first < 0xf0) {
            if (offset + 1 >= len) {
                return false;
            }
            uint8_t second = data[offset];
            uint8_t third = data[offset + 1];
            if (third < 0x80 || third > 0xbf || second < 0x80 || second > 0xbf || (first == 0xe0 && second < 0xa0) ||
                (first == 0xed && second > 0x9f)) {
                return false;
            }
            offset += 2;
            continue;
        }
        if (first > 0xf4 || offset + 2 >= len) {
            return false;
        }
        uint8_t second = data[offset];
        uint8_t third = data[offset + 1];
        uint8_t fourth = data[offset + 2];
        if (second < 0x80 || second > 0xbf || third < 0x80 || third > 0xbf || fourth < 0x80 || fourth > 0xbf ||
            (first == 0xf0 && second < 0x90) || (first == 0xf4 && second > 0x8f)) {
            return false;
        }
        offset += 3;
    }
    return true;
}

static trevrpc_wire_stream_frame_values* trevrpc_wire_alloc_stream_frame(void) {
    trevrpc_wire_stream_frame_values* frame = calloc(1, sizeof(*frame));
    if (frame != NULL) {
        frame->kind = TREVRPC_STREAM_FRAME_KIND_MESSAGE;
        frame->status = TREVRPC_STATUS_OK;
    }
    return frame;
}

static int trevrpc_wire_try_decode_message_stream_frame_take(
    uint8_t* data, size_t len, trevrpc_wire_stream_frame_values** out_frame, bool* matched) {
    *matched = false;
    if (len == 0) {
        trevrpc_wire_stream_frame_values* frame = trevrpc_wire_alloc_stream_frame();
        if (frame == NULL) {
            return -ENOMEM;
        }
        frame->body = (trevrpc_owned_bytes){.owner = data, .release = trevrpc_wire_owned_free};
        *out_frame = frame;
        *matched = true;
        return 0;
    }

    size_t offset = 0;
    uint64_t tag = 0;
    if (!trevrpc_wire_consume_varint(data, len, &offset, &tag)) {
        return 0;
    }
    if (tag != 0x22) {
        return 0;
    }

    const uint8_t* value = NULL;
    size_t value_len = 0;
    if (!trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
        return TREVRPC_ERR_INVALID_FRAME;
    }
    if (offset != len) {
        return 0;
    }

    trevrpc_wire_stream_frame_values* frame = trevrpc_wire_alloc_stream_frame();
    if (frame == NULL) {
        return -ENOMEM;
    }
    frame->body = (trevrpc_owned_bytes){
        .data = value_len == 0 ? NULL : value,
        .len = value_len,
        .owner = data,
        .release = trevrpc_wire_owned_free,
    };
    *out_frame = frame;
    *matched = true;
    return 0;
}

static int trevrpc_wire_parse_metadata_entry(
    const uint8_t* data, size_t len, trevrpc_metadata* metadata, trevrpc_wire_diagnostic_reason* reason) {
    const uint8_t* key = NULL;
    size_t key_len = 0;
    const uint8_t* value = NULL;
    size_t value_len = 0;

    for (size_t offset = 0; offset < len;) {
        uint64_t tag = 0;
        if (!trevrpc_wire_consume_varint(data, len, &offset, &tag) || tag == 0) {
            trevrpc_wire_set_diagnostic(reason, TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF);
            return TREVRPC_ERR_INVALID_FRAME;
        }

        uint64_t field = tag >> 3;
        uint64_t wire_type = tag & 0x7;
        switch (field) {
        case 1:
            if (wire_type != 2) {
                trevrpc_wire_set_diagnostic(reason, TREVRPC_WIRE_DIAGNOSTIC_WRONG_WIRE_TYPE);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            if (!trevrpc_wire_consume_bytes(data, len, &offset, &key, &key_len)) {
                trevrpc_wire_set_diagnostic(reason, TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            break;
        case 2:
            if (wire_type != 2) {
                trevrpc_wire_set_diagnostic(reason, TREVRPC_WIRE_DIAGNOSTIC_WRONG_WIRE_TYPE);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            if (!trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                trevrpc_wire_set_diagnostic(reason, TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            break;
        default:
            if (!trevrpc_wire_skip_field(data, len, &offset, wire_type)) {
                trevrpc_wire_set_diagnostic(reason, TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            break;
        }
    }

    int err = trevrpc_metadata_set(metadata, (const char*)key, key_len, value, value_len);
    if (err == -ENOMEM) {
        trevrpc_wire_set_diagnostic(reason, TREVRPC_WIRE_DIAGNOSTIC_ALLOCATION_FAILURE);
        return err;
    }
    if (err != 0) {
        trevrpc_wire_set_diagnostic(reason, TREVRPC_WIRE_DIAGNOSTIC_INVALID_METADATA);
        return TREVRPC_ERR_INVALID_FRAME;
    }
    return 0;
}

static int trevrpc_wire_request_decode_error(trevrpc_request* request, int err) {
    trevrpc_request_reset(request);
    return err;
}

int trevrpc_wire_decode_request_diagnostic(
    const uint8_t* data, size_t len, trevrpc_request* request, trevrpc_wire_request_diagnostic* diagnostic) {
    if (diagnostic != NULL) {
        diagnostic->response_kind = TREVRPC_WIRE_REQUEST_KIND_UNKNOWN;
        diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_NONE;
    }
    if (request == NULL || diagnostic == NULL || (data == NULL && len > 0)) {
        return -EINVAL;
    }
    memset(request, 0, sizeof(*request));
    request->kind = TREVRPC_RPC_KIND_UNARY;

    uint64_t parsed_kind = TREVRPC_RPC_KIND_UNARY;
    int deferred_metadata_err = 0;
    for (size_t offset = 0; offset < len;) {
        uint64_t tag = 0;
        if (!trevrpc_wire_consume_varint(data, len, &offset, &tag) || tag == 0) {
            diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF;
            return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
        }

        uint64_t field = tag >> 3;
        uint64_t wire_type = tag & 0x7;
        uint64_t varint = 0;
        const uint8_t* value = NULL;
        size_t value_len = 0;
        int err = 0;
        switch (field) {
        case 1:
        case 2:
        case 3:
        case 4:
            if (wire_type != 2) {
                diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_WRONG_WIRE_TYPE;
                return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
            }
            if (!trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF;
                return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
            }
            if (field == 1 || field == 2) {
                if (!trevrpc_wire_utf8_valid(value, value_len)) {
                    diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_INVALID_UTF8;
                    return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
                }
                if (field == 1) {
                    request->service = (const char*)value;
                    request->service_len = value_len;
                } else {
                    request->method = (const char*)value;
                    request->method_len = value_len;
                }
            } else if (field == 3) {
                request->body = value;
                request->body_len = value_len;
            } else {
                err = trevrpc_wire_parse_metadata_entry(value, value_len, &request->metadata, &diagnostic->reason);
                if (err != 0 && deferred_metadata_err == 0) {
                    deferred_metadata_err = err;
                }
            }
            break;
        case 5:
        case 6:
        case 7:
            if (wire_type != 0) {
                diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_WRONG_WIRE_TYPE;
                return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
            }
            if (!trevrpc_wire_consume_varint(data, len, &offset, &varint)) {
                diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF;
                return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
            }
            if ((field == 5 || field == 6) && varint > UINT32_MAX) {
                diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_UINT32_OVERFLOW;
                return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
            }
            if (field == 5) {
                parsed_kind = varint;
                request->kind = (uint32_t)varint;
            } else if (field == 6) {
                request->version = (uint32_t)varint;
            } else {
                request->timeout_nanos = varint;
            }
            break;
        default:
            if (!trevrpc_wire_skip_field(data, len, &offset, wire_type)) {
                diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF;
                return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
            }
            break;
        }
    }

    if (parsed_kind <= TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        diagnostic->response_kind = (uint32_t)parsed_kind;
    }
    if (deferred_metadata_err != 0) {
        return trevrpc_wire_request_decode_error(request, deferred_metadata_err);
    }
    if (trevrpc_metadata_validate(&request->metadata) != 0) {
        diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_INVALID_METADATA;
        return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
    }
    if (request->version != TREVRPC_WIRE_VERSION) {
        return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION);
    }
    if (parsed_kind > TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_UNSUPPORTED_FRAME_KIND;
        return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_UNSUPPORTED_RPC_KIND);
    }
    if (request->service == NULL) {
        request->service = "";
    }
    if (request->method == NULL) {
        request->method = "";
    }

    trevrpc_wire_trace_frame("rx", "RpcRequest", request->kind, TREVRPC_STATUS_OK, request->body_len, len);
    return 0;
}

int trevrpc_wire_decode_request(const uint8_t* data, size_t len, trevrpc_request* request) {
    trevrpc_wire_request_diagnostic diagnostic = {0};
    return trevrpc_wire_decode_request_diagnostic(data, len, request, &diagnostic);
}

static int trevrpc_wire_decode_response_internal(const uint8_t* data,
    size_t len,
    trevrpc_wire_response_values** out_response,
    uint8_t* body_owner,
    bool* took_body,
    const uint8_t** body_view,
    size_t* body_view_len,
    trevrpc_wire_diagnostic* diagnostic) {
    if (diagnostic != NULL) {
        diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_NONE;
    }
    if (out_response == NULL || diagnostic == NULL || (data == NULL && len > 0)) {
        return -EINVAL;
    }
    *out_response = NULL;
    if (took_body != NULL) {
        *took_body = false;
    }
    if (body_view != NULL) {
        if (body_view_len == NULL) {
            return -EINVAL;
        }
        *body_view = NULL;
        *body_view_len = 0;
    }
    trevrpc_wire_response_values* response = calloc(1, sizeof(*response));
    if (response == NULL) {
        diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_ALLOCATION_FAILURE;
        return -ENOMEM;
    }

    for (size_t offset = 0; offset < len;) {
        uint64_t tag = 0;
        if (!trevrpc_wire_consume_varint(data, len, &offset, &tag) || tag == 0) {
            diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF;
            trevrpc_internal_response_free(response);
            return TREVRPC_ERR_INVALID_FRAME;
        }

        uint64_t field = tag >> 3;
        uint64_t wire_type = tag & 0x7;
        uint64_t varint = 0;
        const uint8_t* value = NULL;
        size_t value_len = 0;
        int err = 0;
        if ((field == 1 && wire_type != 0) || (field >= 2 && field <= 4 && wire_type != 2)) {
            diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_WRONG_WIRE_TYPE;
            trevrpc_internal_response_free(response);
            return TREVRPC_ERR_INVALID_FRAME;
        }
        switch (field) {
        case 1:
            if (!trevrpc_wire_consume_varint(data, len, &offset, &varint)) {
                diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF;
                trevrpc_internal_response_free(response);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            if (varint > UINT32_MAX) {
                diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_UINT32_OVERFLOW;
                trevrpc_internal_response_free(response);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            response->status = (uint32_t)varint;
            break;
        case 2:
        case 3:
        case 4:
            if (!trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF;
                trevrpc_internal_response_free(response);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            if (field == 2) {
                if (!trevrpc_wire_utf8_valid(value, value_len)) {
                    diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_INVALID_UTF8;
                    trevrpc_internal_response_free(response);
                    return TREVRPC_ERR_INVALID_FRAME;
                }
                err = trevrpc_internal_response_set_message(response, (const char*)value, value_len);
            } else if (field == 3 && body_view != NULL) {
                *body_view = value_len == 0 ? NULL : value;
                *body_view_len = value_len;
            } else if (field == 3 && body_owner != NULL) {
                response->body = (trevrpc_owned_bytes){
                    .data = value_len == 0 ? NULL : value,
                    .len = value_len,
                    .owner = body_owner,
                    .release = trevrpc_wire_owned_free,
                };
                if (took_body != NULL) {
                    *took_body = true;
                }
            } else if (field == 3) {
                err = trevrpc_internal_response_set_body(response, value, value_len);
            } else {
                err = trevrpc_wire_parse_metadata_entry(value, value_len, &response->metadata, &diagnostic->reason);
            }
            if (err != 0) {
                if (err == -ENOMEM) {
                    diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_ALLOCATION_FAILURE;
                }
                trevrpc_internal_response_free(response);
                return err;
            }
            break;
        default:
            if (!trevrpc_wire_skip_field(data, len, &offset, wire_type)) {
                diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF;
                trevrpc_internal_response_free(response);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            break;
        }
    }

    if (trevrpc_metadata_validate(&response->metadata) != 0) {
        diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_INVALID_METADATA;
        trevrpc_internal_response_free(response);
        return TREVRPC_ERR_INVALID_FRAME;
    }

    *out_response = response;
    trevrpc_wire_trace_frame(
        "rx", "RpcResponse", 0, response->status, body_view != NULL ? *body_view_len : response->body.len, len);
    return 0;
}

int trevrpc_wire_decode_response_diagnostic(
    const uint8_t* data, size_t len, trevrpc_wire_response_values** out_response, trevrpc_wire_diagnostic* diagnostic) {
    return trevrpc_wire_decode_response_internal(data, len, out_response, NULL, NULL, NULL, NULL, diagnostic);
}

int trevrpc_wire_decode_response(const uint8_t* data, size_t len, trevrpc_wire_response_values** out_response) {
    trevrpc_wire_diagnostic diagnostic = {0};
    return trevrpc_wire_decode_response_diagnostic(data, len, out_response, &diagnostic);
}

int trevrpc_wire_decode_response_take(
    uint8_t* data, size_t len, trevrpc_wire_response_values** out_response, bool* took_body) {
    if (took_body == NULL) {
        return -EINVAL;
    }
    trevrpc_wire_diagnostic diagnostic = {0};
    return trevrpc_wire_decode_response_internal(data, len, out_response, data, took_body, NULL, NULL, &diagnostic);
}

static int trevrpc_wire_decode_stream_frame_internal(const uint8_t* data,
    size_t len,
    trevrpc_wire_stream_frame_values** out_frame,
    const uint8_t** body_view,
    size_t* body_view_len,
    trevrpc_wire_diagnostic* diagnostic) {
    if (diagnostic != NULL) {
        diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_NONE;
    }
    if (out_frame == NULL || diagnostic == NULL || (data == NULL && len > 0)) {
        return -EINVAL;
    }
    *out_frame = NULL;
    if (body_view != NULL) {
        if (body_view_len == NULL) {
            return -EINVAL;
        }
        *body_view = NULL;
        *body_view_len = 0;
    }
    trevrpc_wire_stream_frame_values* frame = trevrpc_wire_alloc_stream_frame();
    if (frame == NULL) {
        diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_ALLOCATION_FAILURE;
        return -ENOMEM;
    }

    for (size_t offset = 0; offset < len;) {
        uint64_t tag = 0;
        if (!trevrpc_wire_consume_varint(data, len, &offset, &tag) || tag == 0) {
            diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF;
            trevrpc_internal_stream_frame_free(frame);
            return TREVRPC_ERR_INVALID_FRAME;
        }

        uint64_t field = tag >> 3;
        uint64_t wire_type = tag & 0x7;
        uint64_t varint = 0;
        const uint8_t* value = NULL;
        size_t value_len = 0;
        int err = 0;
        if ((field <= 2 && field > 0 && wire_type != 0) || (field >= 3 && field <= 5 && wire_type != 2)) {
            diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_WRONG_WIRE_TYPE;
            trevrpc_internal_stream_frame_free(frame);
            return TREVRPC_ERR_INVALID_FRAME;
        }
        switch (field) {
        case 1:
        case 2:
            if (!trevrpc_wire_consume_varint(data, len, &offset, &varint)) {
                diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF;
                trevrpc_internal_stream_frame_free(frame);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            if (varint > UINT32_MAX) {
                diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_UINT32_OVERFLOW;
                trevrpc_internal_stream_frame_free(frame);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            if (field == 1) {
                frame->kind = (uint32_t)varint;
            } else {
                frame->status = (uint32_t)varint;
            }
            break;
        case 3:
        case 4:
        case 5:
            if (!trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF;
                trevrpc_internal_stream_frame_free(frame);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            if (field == 3) {
                if (!trevrpc_wire_utf8_valid(value, value_len)) {
                    diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_INVALID_UTF8;
                    trevrpc_internal_stream_frame_free(frame);
                    return TREVRPC_ERR_INVALID_FRAME;
                }
                err = trevrpc_internal_stream_frame_set_message(frame, (const char*)value, value_len);
            } else if (field == 4 && body_view != NULL) {
                *body_view = value_len == 0 ? NULL : value;
                *body_view_len = value_len;
            } else if (field == 4) {
                err = trevrpc_internal_stream_frame_set_body(frame, value, value_len);
            } else {
                err = trevrpc_wire_parse_metadata_entry(value, value_len, &frame->metadata, &diagnostic->reason);
            }
            if (err != 0) {
                if (err == -ENOMEM) {
                    diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_ALLOCATION_FAILURE;
                }
                trevrpc_internal_stream_frame_free(frame);
                return err;
            }
            break;
        default:
            if (!trevrpc_wire_skip_field(data, len, &offset, wire_type)) {
                diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF;
                trevrpc_internal_stream_frame_free(frame);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            break;
        }
    }

    if (trevrpc_metadata_validate(&frame->metadata) != 0) {
        diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_INVALID_METADATA;
        trevrpc_internal_stream_frame_free(frame);
        return TREVRPC_ERR_INVALID_FRAME;
    }

    if (frame->kind != TREVRPC_STREAM_FRAME_KIND_MESSAGE && frame->kind != TREVRPC_STREAM_FRAME_KIND_STATUS) {
        diagnostic->reason = TREVRPC_WIRE_DIAGNOSTIC_UNSUPPORTED_FRAME_KIND;
        trevrpc_internal_stream_frame_free(frame);
        return TREVRPC_ERR_INVALID_FRAME;
    }

    *out_frame = frame;
    trevrpc_wire_trace_frame(
        "rx", "RpcStreamFrame", frame->kind, frame->status, body_view != NULL ? *body_view_len : frame->body.len, len);
    return 0;
}

int trevrpc_wire_decode_stream_frame_diagnostic(const uint8_t* data,
    size_t len,
    trevrpc_wire_stream_frame_values** out_frame,
    trevrpc_wire_diagnostic* diagnostic) {
    return trevrpc_wire_decode_stream_frame_internal(data, len, out_frame, NULL, NULL, diagnostic);
}

int trevrpc_wire_decode_stream_frame(const uint8_t* data, size_t len, trevrpc_wire_stream_frame_values** out_frame) {
    trevrpc_wire_diagnostic diagnostic = {0};
    return trevrpc_wire_decode_stream_frame_diagnostic(data, len, out_frame, &diagnostic);
}

int trevrpc_wire_decode_stream_frame_take(
    uint8_t* data, size_t len, trevrpc_wire_stream_frame_values** out_frame, bool* took_body) {
    if (took_body == NULL) {
        return -EINVAL;
    }
    *took_body = false;

    bool matched = false;
    int err = trevrpc_wire_try_decode_message_stream_frame_take(data, len, out_frame, &matched);
    if (err != 0) {
        return err;
    }
    if (matched) {
        *took_body = true;
        trevrpc_wire_trace_frame(
            "rx", "RpcStreamFrame", (*out_frame)->kind, (*out_frame)->status, (*out_frame)->body.len, len);
        return 0;
    }

    return trevrpc_wire_decode_stream_frame(data, len, out_frame);
}

int trevrpc_wire_decode_response_owned(trevrpc_owned_bytes* data, trevrpc_inbound_response** out_response) {
    if (data == NULL) {
        return -EINVAL;
    }
    trevrpc_owned_bytes owned = {0};
    trevrpc_owned_bytes_move(&owned, data);
    if (out_response == NULL || (owned.data == NULL && owned.len > 0)) {
        trevrpc_owned_bytes_reset(&owned);
        return -EINVAL;
    }
    *out_response = NULL;

    const uint8_t* visible_body = NULL;
    size_t visible_body_len = 0;
    trevrpc_wire_response_values* values = NULL;
    trevrpc_wire_diagnostic diagnostic = {0};
    int err = trevrpc_wire_decode_response_internal(
        owned.data, owned.len, &values, NULL, NULL, &visible_body, &visible_body_len, &diagnostic);
    if (err != 0) {
        trevrpc_owned_bytes_reset(&owned);
        return err;
    }
    owned.data = visible_body;
    owned.len = visible_body_len;
    trevrpc_owned_bytes_move(&values->body, &owned);
    err = trevrpc_inbound_response_create(values, out_response);
    trevrpc_internal_response_free(values);
    if (err != 0) {
        trevrpc_owned_bytes_reset(&owned);
    }
    return err;
}

int trevrpc_wire_decode_stream_frame_owned_diagnostic(
    trevrpc_owned_bytes* data, trevrpc_inbound_stream_frame** out_frame, trevrpc_wire_diagnostic* diagnostic) {
    if (data == NULL) {
        return -EINVAL;
    }
    trevrpc_owned_bytes owned = {0};
    trevrpc_owned_bytes_move(&owned, data);
    if (out_frame == NULL || diagnostic == NULL || (owned.data == NULL && owned.len > 0)) {
        trevrpc_owned_bytes_reset(&owned);
        return -EINVAL;
    }
    *out_frame = NULL;

    const uint8_t* visible_body = NULL;
    size_t visible_body_len = 0;
    trevrpc_wire_stream_frame_values* values = NULL;
    int err = trevrpc_wire_decode_stream_frame_internal(
        owned.data, owned.len, &values, &visible_body, &visible_body_len, diagnostic);
    if (err != 0) {
        trevrpc_owned_bytes_reset(&owned);
        return err;
    }
    owned.data = visible_body;
    owned.len = visible_body_len;
    trevrpc_owned_bytes_move(&values->body, &owned);
    err = trevrpc_inbound_stream_frame_create(values, out_frame);
    trevrpc_internal_stream_frame_free(values);
    if (err != 0) {
        trevrpc_owned_bytes_reset(&owned);
    }
    return err;
}

int trevrpc_wire_decode_stream_frame_owned(trevrpc_owned_bytes* data, trevrpc_inbound_stream_frame** out_frame) {
    trevrpc_wire_diagnostic diagnostic = {0};
    return trevrpc_wire_decode_stream_frame_owned_diagnostic(data, out_frame, &diagnostic);
}
