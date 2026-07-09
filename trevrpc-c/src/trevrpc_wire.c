#include "trevrpc_wire_internal.h"

#include <errno.h> // IWYU pragma: keep
#include <stdatomic.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
    if (service == NULL || method == NULL || service_len == 0 || method_len == 0 || (body == NULL && body_len > 0)) {
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

int trevrpc_wire_encode_response(
    const trevrpc_response* response, size_t max_frame_size, uint8_t** frame, size_t* frame_len) {
    if (response == NULL) {
        return -EINVAL;
    }
    int err = trevrpc_metadata_validate(&response->metadata);
    if (err != 0) {
        return err;
    }

    size_t body_frame_len =
        trevrpc_wire_varint_field_len(1, response->status) + trevrpc_wire_bytes_field_len(2, response->message_len) +
        trevrpc_wire_bytes_field_len(3, response->body_len) + trevrpc_wire_metadata_field_len(4, &response->metadata);
    err = trevrpc_wire_alloc_frame(body_frame_len, max_frame_size, frame, frame_len);
    if (err != 0) {
        return err;
    }

    uint8_t* out = *frame + 4;
    out = trevrpc_wire_append_varint_field(out, 1, response->status);
    out = trevrpc_wire_append_bytes_field(out, 2, (const uint8_t*)response->message, response->message_len);
    out = trevrpc_wire_append_bytes_field(out, 3, response->body, response->body_len);
    out = trevrpc_wire_append_metadata_field(out, 4, &response->metadata);
    (void)out;
    trevrpc_wire_trace_frame("tx", "RpcResponse", 0, response->status, response->body_len, body_frame_len);
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

static bool trevrpc_wire_consume_varint(const uint8_t* data, size_t len, size_t* offset, uint64_t* value) {
    uint64_t result = 0;
    for (unsigned int shift = 0; shift < 64; shift += 7) {
        if (*offset >= len) {
            return false;
        }

        uint8_t byte = data[*offset];
        *offset += 1;
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

static trevrpc_stream_frame* trevrpc_wire_alloc_stream_frame(void) {
    trevrpc_stream_frame* frame = calloc(1, sizeof(*frame));
    if (frame != NULL) {
        frame->kind = TREVRPC_STREAM_FRAME_KIND_MESSAGE;
        frame->status = TREVRPC_STATUS_OK;
    }
    return frame;
}

static int trevrpc_wire_try_decode_message_stream_frame_take(
    uint8_t* data, size_t len, trevrpc_stream_frame** out_frame, bool* matched) {
    *matched = false;
    if (len == 0) {
        trevrpc_stream_frame* frame = trevrpc_wire_alloc_stream_frame();
        if (frame == NULL) {
            return -ENOMEM;
        }
        frame->_body_owner = data;
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

    trevrpc_stream_frame* frame = trevrpc_wire_alloc_stream_frame();
    if (frame == NULL) {
        return -ENOMEM;
    }
    frame->body = value_len == 0 ? NULL : (uint8_t*)value;
    frame->body_len = value_len;
    frame->_body_owner = data;
    *out_frame = frame;
    *matched = true;
    return 0;
}

static int trevrpc_wire_parse_metadata_entry(const uint8_t* data, size_t len, trevrpc_metadata* metadata) {
    const uint8_t* key = NULL;
    size_t key_len = 0;
    const uint8_t* value = NULL;
    size_t value_len = 0;

    for (size_t offset = 0; offset < len;) {
        uint64_t tag = 0;
        if (!trevrpc_wire_consume_varint(data, len, &offset, &tag) || tag == 0) {
            return TREVRPC_ERR_INVALID_FRAME;
        }

        uint64_t field = tag >> 3;
        uint64_t wire_type = tag & 0x7;
        switch (field) {
        case 1:
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &key, &key_len)) {
                return TREVRPC_ERR_INVALID_FRAME;
            }
            break;
        case 2:
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                return TREVRPC_ERR_INVALID_FRAME;
            }
            break;
        default:
            if (!trevrpc_wire_skip_field(data, len, &offset, wire_type)) {
                return TREVRPC_ERR_INVALID_FRAME;
            }
            break;
        }
    }

    int err = trevrpc_metadata_set(metadata, (const char*)key, key_len, value, value_len);
    return err == 0 ? 0 : TREVRPC_ERR_INVALID_FRAME;
}

static int trevrpc_wire_request_decode_error(trevrpc_request* request, int err) {
    trevrpc_request_reset(request);
    return err;
}

int trevrpc_wire_decode_request(const uint8_t* data, size_t len, trevrpc_request* request) {
    if (request == NULL || (data == NULL && len > 0)) {
        return -EINVAL;
    }
    memset(request, 0, sizeof(*request));
    request->kind = TREVRPC_RPC_KIND_UNARY;

    for (size_t offset = 0; offset < len;) {
        uint64_t tag = 0;
        if (!trevrpc_wire_consume_varint(data, len, &offset, &tag) || tag == 0) {
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
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
            }
            request->service = (const char*)value;
            request->service_len = value_len;
            break;
        case 2:
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
            }
            request->method = (const char*)value;
            request->method_len = value_len;
            break;
        case 3:
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
            }
            request->body = value;
            request->body_len = value_len;
            break;
        case 4:
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
            }
            err = trevrpc_wire_parse_metadata_entry(value, value_len, &request->metadata);
            if (err != 0) {
                return trevrpc_wire_request_decode_error(request, err);
            }
            break;
        case 5:
            if (wire_type != 0 || !trevrpc_wire_consume_varint(data, len, &offset, &varint)) {
                return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
            }
            request->kind = (uint32_t)varint;
            break;
        case 6:
            if (wire_type != 0 || !trevrpc_wire_consume_varint(data, len, &offset, &varint)) {
                return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
            }
            request->version = (uint32_t)varint;
            break;
        case 7:
            if (wire_type != 0 || !trevrpc_wire_consume_varint(data, len, &offset, &varint)) {
                return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
            }
            request->timeout_nanos = varint;
            break;
        default:
            if (!trevrpc_wire_skip_field(data, len, &offset, wire_type)) {
                return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
            }
            break;
        }
    }

    if (trevrpc_metadata_validate(&request->metadata) != 0) {
        return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
    }
    if (request->version != TREVRPC_WIRE_VERSION) {
        return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION);
    }
    if (request->service_len == 0 || request->method_len == 0) {
        return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_INVALID_FRAME);
    }
    if (request->kind > TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        return trevrpc_wire_request_decode_error(request, TREVRPC_ERR_UNSUPPORTED_RPC_KIND);
    }

    trevrpc_wire_trace_frame("rx", "RpcRequest", request->kind, TREVRPC_STATUS_OK, request->body_len, len);
    return 0;
}

int trevrpc_wire_decode_response(const uint8_t* data, size_t len, trevrpc_response** out_response) {
    if (out_response == NULL || (data == NULL && len > 0)) {
        return -EINVAL;
    }
    *out_response = NULL;
    trevrpc_response* response = calloc(1, sizeof(*response));
    if (response == NULL) {
        return -ENOMEM;
    }

    for (size_t offset = 0; offset < len;) {
        uint64_t tag = 0;
        if (!trevrpc_wire_consume_varint(data, len, &offset, &tag) || tag == 0) {
            trevrpc_response_free(response);
            return TREVRPC_ERR_INVALID_FRAME;
        }

        uint64_t field = tag >> 3;
        uint64_t wire_type = tag & 0x7;
        uint64_t varint = 0;
        const uint8_t* value = NULL;
        size_t value_len = 0;
        int err = 0;
        switch (field) {
        case 1:
            if (wire_type != 0 || !trevrpc_wire_consume_varint(data, len, &offset, &varint)) {
                trevrpc_response_free(response);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            response->status = (uint32_t)varint;
            break;
        case 2:
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                trevrpc_response_free(response);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            err = trevrpc_response_set_message(response, (const char*)value, value_len);
            if (err != 0) {
                trevrpc_response_free(response);
                return err;
            }
            break;
        case 3:
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                trevrpc_response_free(response);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            err = trevrpc_response_set_body(response, value, value_len);
            if (err != 0) {
                trevrpc_response_free(response);
                return err;
            }
            break;
        case 4:
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                trevrpc_response_free(response);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            err = trevrpc_wire_parse_metadata_entry(value, value_len, &response->metadata);
            if (err != 0) {
                trevrpc_response_free(response);
                return err;
            }
            break;
        default:
            if (!trevrpc_wire_skip_field(data, len, &offset, wire_type)) {
                trevrpc_response_free(response);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            break;
        }
    }

    if (trevrpc_metadata_validate(&response->metadata) != 0) {
        trevrpc_response_free(response);
        return TREVRPC_ERR_INVALID_FRAME;
    }

    *out_response = response;
    trevrpc_wire_trace_frame("rx", "RpcResponse", 0, response->status, response->body_len, len);
    return 0;
}

int trevrpc_wire_decode_stream_frame(const uint8_t* data, size_t len, trevrpc_stream_frame** out_frame) {
    if (out_frame == NULL || (data == NULL && len > 0)) {
        return -EINVAL;
    }
    *out_frame = NULL;
    trevrpc_stream_frame* frame = trevrpc_wire_alloc_stream_frame();
    if (frame == NULL) {
        return -ENOMEM;
    }

    for (size_t offset = 0; offset < len;) {
        uint64_t tag = 0;
        if (!trevrpc_wire_consume_varint(data, len, &offset, &tag) || tag == 0) {
            trevrpc_stream_frame_free(frame);
            return TREVRPC_ERR_INVALID_FRAME;
        }

        uint64_t field = tag >> 3;
        uint64_t wire_type = tag & 0x7;
        uint64_t varint = 0;
        const uint8_t* value = NULL;
        size_t value_len = 0;
        int err = 0;
        switch (field) {
        case 1:
            if (wire_type != 0 || !trevrpc_wire_consume_varint(data, len, &offset, &varint)) {
                trevrpc_stream_frame_free(frame);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            frame->kind = (uint32_t)varint;
            break;
        case 2:
            if (wire_type != 0 || !trevrpc_wire_consume_varint(data, len, &offset, &varint)) {
                trevrpc_stream_frame_free(frame);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            frame->status = (uint32_t)varint;
            break;
        case 3:
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                trevrpc_stream_frame_free(frame);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            err = trevrpc_stream_frame_set_message(frame, (const char*)value, value_len);
            if (err != 0) {
                trevrpc_stream_frame_free(frame);
                return err;
            }
            break;
        case 4:
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                trevrpc_stream_frame_free(frame);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            err = trevrpc_stream_frame_set_body(frame, value, value_len);
            if (err != 0) {
                trevrpc_stream_frame_free(frame);
                return err;
            }
            break;
        case 5:
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                trevrpc_stream_frame_free(frame);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            err = trevrpc_wire_parse_metadata_entry(value, value_len, &frame->metadata);
            if (err != 0) {
                trevrpc_stream_frame_free(frame);
                return err;
            }
            break;
        default:
            if (!trevrpc_wire_skip_field(data, len, &offset, wire_type)) {
                trevrpc_stream_frame_free(frame);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            break;
        }
    }

    if (trevrpc_metadata_validate(&frame->metadata) != 0) {
        trevrpc_stream_frame_free(frame);
        return TREVRPC_ERR_INVALID_FRAME;
    }

    if (frame->kind != TREVRPC_STREAM_FRAME_KIND_MESSAGE && frame->kind != TREVRPC_STREAM_FRAME_KIND_STATUS) {
        trevrpc_stream_frame_free(frame);
        return TREVRPC_ERR_INVALID_FRAME;
    }

    *out_frame = frame;
    trevrpc_wire_trace_frame("rx", "RpcStreamFrame", frame->kind, frame->status, frame->body_len, len);
    return 0;
}

int trevrpc_wire_decode_stream_frame_take(
    uint8_t* data, size_t len, trevrpc_stream_frame** out_frame, bool* took_body) {
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
            "rx", "RpcStreamFrame", (*out_frame)->kind, (*out_frame)->status, (*out_frame)->body_len, len);
        return 0;
    }

    return trevrpc_wire_decode_stream_frame(data, len, out_frame);
}
