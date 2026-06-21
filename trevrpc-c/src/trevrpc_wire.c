#include "trevrpc_wire.h"

#include <errno.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

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
    size_t max_frame_size,
    uint8_t** frame,
    size_t* frame_len) {
    if (service == NULL || method == NULL || (body == NULL && body_len > 0)) {
        return -EINVAL;
    }

    size_t service_len = strlen(service);
    size_t method_len = strlen(method);
    size_t body_frame_len = trevrpc_wire_bytes_field_len(1, service_len) + trevrpc_wire_bytes_field_len(2, method_len) +
                            trevrpc_wire_bytes_field_len(3, body_len) + trevrpc_wire_varint_field_len(5, kind) +
                            trevrpc_wire_varint_field_len(6, TREVRPC_WIRE_VERSION);
    int err = trevrpc_wire_alloc_frame(body_frame_len, max_frame_size, frame, frame_len);
    if (err != 0) {
        return err;
    }

    uint8_t* out = *frame + 4;
    out = trevrpc_wire_append_bytes_field(out, 1, (const uint8_t*)service, service_len);
    out = trevrpc_wire_append_bytes_field(out, 2, (const uint8_t*)method, method_len);
    out = trevrpc_wire_append_bytes_field(out, 3, body, body_len);
    out = trevrpc_wire_append_varint_field(out, 5, kind);
    out = trevrpc_wire_append_varint_field(out, 6, TREVRPC_WIRE_VERSION);
    (void)out;
    return 0;
}

int trevrpc_wire_encode_response(
    const trevrpc_response* response, size_t max_frame_size, uint8_t** frame, size_t* frame_len) {
    if (response == NULL) {
        return -EINVAL;
    }

    size_t body_frame_len = trevrpc_wire_varint_field_len(1, response->status) +
                            trevrpc_wire_bytes_field_len(2, response->message_len) +
                            trevrpc_wire_bytes_field_len(3, response->body_len);
    int err = trevrpc_wire_alloc_frame(body_frame_len, max_frame_size, frame, frame_len);
    if (err != 0) {
        return err;
    }

    uint8_t* out = *frame + 4;
    out = trevrpc_wire_append_varint_field(out, 1, response->status);
    out = trevrpc_wire_append_bytes_field(out, 2, (const uint8_t*)response->message, response->message_len);
    out = trevrpc_wire_append_bytes_field(out, 3, response->body, response->body_len);
    (void)out;
    return 0;
}

int trevrpc_wire_encode_stream_frame(uint32_t kind,
    uint32_t status,
    const char* message,
    size_t message_len,
    const uint8_t* body,
    size_t body_len,
    size_t max_frame_size,
    uint8_t** frame,
    size_t* frame_len) {
    if ((message == NULL && message_len > 0) || (body == NULL && body_len > 0)) {
        return -EINVAL;
    }

    size_t body_frame_len = trevrpc_wire_varint_field_len(1, kind) + trevrpc_wire_varint_field_len(2, status) +
                            trevrpc_wire_bytes_field_len(3, message_len) + trevrpc_wire_bytes_field_len(4, body_len);
    int err = trevrpc_wire_alloc_frame(body_frame_len, max_frame_size, frame, frame_len);
    if (err != 0) {
        return err;
    }

    uint8_t* out = *frame + 4;
    out = trevrpc_wire_append_varint_field(out, 1, kind);
    out = trevrpc_wire_append_varint_field(out, 2, status);
    out = trevrpc_wire_append_bytes_field(out, 3, (const uint8_t*)message, message_len);
    out = trevrpc_wire_append_bytes_field(out, 4, body, body_len);
    (void)out;
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

int trevrpc_wire_decode_request(const uint8_t* data, size_t len, trevrpc_request* request) {
    memset(request, 0, sizeof(*request));
    request->kind = TREVRPC_RPC_KIND_UNARY;

    for (size_t offset = 0; offset < len;) {
        uint64_t tag = 0;
        if (!trevrpc_wire_consume_varint(data, len, &offset, &tag) || tag == 0) {
            return TREVRPC_ERR_INVALID_FRAME;
        }

        uint64_t field = tag >> 3;
        uint64_t wire_type = tag & 0x7;
        uint64_t varint = 0;
        const uint8_t* value = NULL;
        size_t value_len = 0;
        switch (field) {
        case 1:
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                return TREVRPC_ERR_INVALID_FRAME;
            }
            request->service = (const char*)value;
            request->service_len = value_len;
            break;
        case 2:
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                return TREVRPC_ERR_INVALID_FRAME;
            }
            request->method = (const char*)value;
            request->method_len = value_len;
            break;
        case 3:
            if (wire_type != 2 || !trevrpc_wire_consume_bytes(data, len, &offset, &value, &value_len)) {
                return TREVRPC_ERR_INVALID_FRAME;
            }
            request->body = value;
            request->body_len = value_len;
            break;
        case 5:
            if (wire_type != 0 || !trevrpc_wire_consume_varint(data, len, &offset, &varint)) {
                return TREVRPC_ERR_INVALID_FRAME;
            }
            request->kind = (uint32_t)varint;
            break;
        case 6:
            if (wire_type != 0 || !trevrpc_wire_consume_varint(data, len, &offset, &varint)) {
                return TREVRPC_ERR_INVALID_FRAME;
            }
            request->version = (uint32_t)varint;
            break;
        case 7:
            if (wire_type != 0 || !trevrpc_wire_consume_varint(data, len, &offset, &varint)) {
                return TREVRPC_ERR_INVALID_FRAME;
            }
            request->timeout_nanos = varint;
            break;
        default:
            if (!trevrpc_wire_skip_field(data, len, &offset, wire_type)) {
                return TREVRPC_ERR_INVALID_FRAME;
            }
            break;
        }
    }

    if (request->version != TREVRPC_WIRE_VERSION) {
        return TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION;
    }
    if (request->service_len == 0 || request->method_len == 0) {
        return TREVRPC_ERR_INVALID_FRAME;
    }
    if (request->kind > TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        return TREVRPC_ERR_UNSUPPORTED_RPC_KIND;
    }

    return 0;
}

int trevrpc_wire_decode_response(const uint8_t* data, size_t len, trevrpc_response** out_response) {
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
        default:
            if (!trevrpc_wire_skip_field(data, len, &offset, wire_type)) {
                trevrpc_response_free(response);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            break;
        }
    }

    *out_response = response;
    return 0;
}

int trevrpc_wire_decode_stream_frame(const uint8_t* data, size_t len, trevrpc_stream_frame** out_frame) {
    *out_frame = NULL;
    trevrpc_stream_frame* frame = calloc(1, sizeof(*frame));
    if (frame == NULL) {
        return -ENOMEM;
    }
    frame->kind = TREVRPC_STREAM_FRAME_KIND_MESSAGE;

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
        default:
            if (!trevrpc_wire_skip_field(data, len, &offset, wire_type)) {
                trevrpc_stream_frame_free(frame);
                return TREVRPC_ERR_INVALID_FRAME;
            }
            break;
        }
    }

    if (frame->kind != TREVRPC_STREAM_FRAME_KIND_MESSAGE && frame->kind != TREVRPC_STREAM_FRAME_KIND_STATUS) {
        trevrpc_stream_frame_free(frame);
        return TREVRPC_ERR_INVALID_FRAME;
    }

    *out_frame = frame;
    return 0;
}
