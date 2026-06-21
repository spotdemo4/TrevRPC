#include "trevrpc.h"

#include "trevrpc_msquic.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

typedef struct trevrpc_method trevrpc_method;
typedef struct trevrpc_server_conn_ref trevrpc_server_conn_ref;

struct trevrpc_client {
    trevrpc_msquic_conn* conn;
    size_t max_frame_size;
};

struct trevrpc_method {
    trevrpc_method* next;
    char* service;
    size_t service_len;
    char* method;
    size_t method_len;
    uint32_t kind;
    trevrpc_unary_handler handler;
    trevrpc_stream_handler stream_handler;
    void* user_data;
};

struct trevrpc_stream {
    trevrpc_msquic_stream* stream;
    size_t max_frame_size;
    bool owns_stream;
    bool sent_status;
};

struct trevrpc_server_conn_ref {
    trevrpc_server_conn_ref* next;
    trevrpc_msquic_conn* conn;
};

struct trevrpc_server {
    trevrpc_msquic_listener* listener;
    size_t max_frame_size;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_method* methods;
    trevrpc_server_conn_ref* conns;
    size_t active_tasks;
    bool shutting_down;
};

typedef struct trevrpc_conn_task {
    trevrpc_server* server;
    trevrpc_msquic_conn* conn;
} trevrpc_conn_task;

typedef struct trevrpc_stream_task {
    trevrpc_server* server;
    trevrpc_msquic_stream* stream;
} trevrpc_stream_task;

static size_t trevrpc_effective_max_frame_size(const trevrpc_config* config) {
    if (config != NULL && config->max_frame_size > 0) {
        return config->max_frame_size;
    }

    return TREVRPC_DEFAULT_MAX_FRAME_SIZE;
}

static trevrpc_msquic_config trevrpc_make_msquic_config(const trevrpc_config* config) {
    trevrpc_config defaults = trevrpc_default_config();
    if (config == NULL) {
        config = &defaults;
    }

    trevrpc_msquic_config msquic_config = {0};
    msquic_config.alpn = TREVRPC_ALPN;
    msquic_config.alpn_len = (uint32_t)(sizeof(TREVRPC_ALPN) - 1);
    msquic_config.cert_file = config->cert_file;
    msquic_config.key_file = config->key_file;
    msquic_config.max_idle_timeout_ms = config->max_idle_timeout_ms;
    msquic_config.keep_alive_ms = config->keep_alive_ms;
    msquic_config.peer_bidi_stream_count = config->peer_bidi_stream_count;
    msquic_config.max_stateless_operations = config->max_stateless_operations;
    msquic_config.max_binding_stateless_operations = config->max_binding_stateless_operations;
    return msquic_config;
}

trevrpc_config trevrpc_default_config(void) {
    trevrpc_config config = {0};
    config.max_idle_timeout_ms = 30000;
    config.keep_alive_ms = 15000;
    config.peer_bidi_stream_count = 100;
    config.max_frame_size = TREVRPC_DEFAULT_MAX_FRAME_SIZE;
    return config;
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
        return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
    }
    if (body_len > SIZE_MAX - 4) {
        return TREV_MSQUIC_ERR_FRAME_TOO_LARGE;
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

static int trevrpc_wire_encode_request(const char* service,
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

static int trevrpc_wire_encode_response(
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

static int trevrpc_wire_encode_stream_frame(uint32_t kind,
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

static int trevrpc_wire_decode_request(const uint8_t* data, size_t len, trevrpc_request* request) {
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

static int trevrpc_wire_decode_response(const uint8_t* data, size_t len, trevrpc_response** out_response) {
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

static int trevrpc_wire_decode_stream_frame(const uint8_t* data, size_t len, trevrpc_stream_frame** out_frame) {
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

static int trevrpc_write_frame(trevrpc_msquic_stream* stream, const uint8_t* frame, size_t frame_len) {
    intptr_t written = trevrpc_msquic_stream_write(stream, frame, frame_len);
    if (written < 0) {
        return (int)written;
    }
    if ((size_t)written != frame_len) {
        return TREV_MSQUIC_ERR_CLOSED;
    }

    return 0;
}

static trevrpc_stream* trevrpc_stream_alloc(trevrpc_msquic_stream* stream, size_t max_frame_size, bool owns_stream) {
    trevrpc_stream* rpc_stream = calloc(1, sizeof(*rpc_stream));
    if (rpc_stream == NULL) {
        return NULL;
    }

    rpc_stream->stream = stream;
    rpc_stream->max_frame_size = max_frame_size;
    rpc_stream->owns_stream = owns_stream;
    return rpc_stream;
}

int trevrpc_stream_send_message(trevrpc_stream* stream, const uint8_t* body, size_t body_len) {
    if (stream == NULL || stream->stream == NULL || (body == NULL && body_len > 0)) {
        return -EINVAL;
    }

    intptr_t written =
        trevrpc_msquic_stream_write_message_frame(stream->stream, body, body_len, stream->max_frame_size);
    return written < 0 ? (int)written : 0;
}

int trevrpc_stream_send_status(trevrpc_stream* stream, uint32_t status, const char* message, size_t message_len) {
    if (stream == NULL || stream->stream == NULL || (message == NULL && message_len > 0)) {
        return -EINVAL;
    }

    uint8_t* frame = NULL;
    size_t frame_len = 0;
    int err = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_STATUS,
        status,
        message,
        message_len,
        NULL,
        0,
        stream->max_frame_size,
        &frame,
        &frame_len);
    if (err == 0) {
        err = trevrpc_write_frame(stream->stream, frame, frame_len);
    }
    free(frame);
    if (err == 0) {
        stream->sent_status = true;
    }
    return err;
}

int trevrpc_stream_recv(trevrpc_stream* stream, trevrpc_stream_frame** out_frame) {
    if (stream == NULL || stream->stream == NULL || out_frame == NULL) {
        return -EINVAL;
    }
    *out_frame = NULL;

    uint8_t* body = NULL;
    size_t body_len = 0;
    intptr_t read = trevrpc_msquic_stream_read_frame(stream->stream, &body, &body_len, stream->max_frame_size);
    if (read < 0) {
        return (int)read;
    }
    if (read == 0) {
        return 0;
    }

    int err = trevrpc_wire_decode_stream_frame(body, body_len, out_frame);
    trevrpc_msquic_free(body);
    return err;
}

int trevrpc_stream_finish_send(trevrpc_stream* stream) {
    if (stream == NULL || stream->stream == NULL) {
        return -EINVAL;
    }

    return trevrpc_msquic_stream_shutdown_send(stream->stream);
}

void trevrpc_stream_close(trevrpc_stream* stream) {
    if (stream == NULL) {
        return;
    }

    if (stream->owns_stream) {
        trevrpc_msquic_stream_close(stream->stream);
    }
    free(stream);
}

int trevrpc_client_connect(const char* host, uint16_t port, const trevrpc_config* config, trevrpc_client** out_client) {
    if (host == NULL || out_client == NULL) {
        return -EINVAL;
    }
    *out_client = NULL;

    trevrpc_client* client = calloc(1, sizeof(*client));
    if (client == NULL) {
        return -ENOMEM;
    }
    client->max_frame_size = trevrpc_effective_max_frame_size(config);

    trevrpc_msquic_config msquic_config = trevrpc_make_msquic_config(config);
    int err = trevrpc_msquic_dial(host, port, &msquic_config, &client->conn);
    if (err != 0) {
        trevrpc_client_close(client);
        return err;
    }

    *out_client = client;
    return 0;
}

int trevrpc_client_call_unary(trevrpc_client* client,
    const char* service,
    const char* method,
    const uint8_t* body,
    size_t body_len,
    trevrpc_response** out_response) {
    if (client == NULL || out_response == NULL) {
        return -EINVAL;
    }
    *out_response = NULL;

    trevrpc_msquic_stream* stream = NULL;
    int err = trevrpc_msquic_conn_open_stream(client->conn, &stream);
    if (err != 0) {
        return err;
    }

    uint8_t* frame = NULL;
    size_t frame_len = 0;
    err = trevrpc_wire_encode_request(
        service, method, TREVRPC_RPC_KIND_UNARY, body, body_len, client->max_frame_size, &frame, &frame_len);
    if (err == 0) {
        err = trevrpc_write_frame(stream, frame, frame_len);
    }
    free(frame);
    if (err == 0) {
        err = trevrpc_msquic_stream_shutdown_send(stream);
    }

    uint8_t* response_body = NULL;
    size_t response_body_len = 0;
    if (err == 0) {
        intptr_t read =
            trevrpc_msquic_stream_read_frame(stream, &response_body, &response_body_len, client->max_frame_size);
        if (read < 0) {
            err = (int)read;
        } else if (read == 0) {
            err = TREV_MSQUIC_ERR_CLOSED;
        }
    }
    if (err == 0) {
        err = trevrpc_wire_decode_response(response_body, response_body_len, out_response);
    }

    trevrpc_msquic_free(response_body);
    trevrpc_msquic_stream_close(stream);
    return err;
}

int trevrpc_client_start_stream(trevrpc_client* client,
    const char* service,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len,
    trevrpc_stream** out_stream) {
    if (client == NULL || out_stream == NULL || service == NULL || method == NULL || (body == NULL && body_len > 0)) {
        return -EINVAL;
    }
    *out_stream = NULL;
    if (kind == TREVRPC_RPC_KIND_UNARY || kind > TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        return TREVRPC_ERR_UNSUPPORTED_RPC_KIND;
    }

    trevrpc_msquic_stream* raw_stream = NULL;
    int err = trevrpc_msquic_conn_open_stream(client->conn, &raw_stream);
    if (err != 0) {
        return err;
    }

    uint8_t* frame = NULL;
    size_t frame_len = 0;
    err =
        trevrpc_wire_encode_request(service, method, kind, body, body_len, client->max_frame_size, &frame, &frame_len);
    if (err == 0) {
        err = trevrpc_write_frame(raw_stream, frame, frame_len);
    }
    free(frame);
    if (err != 0) {
        trevrpc_msquic_stream_close(raw_stream);
        return err;
    }

    trevrpc_stream* stream = trevrpc_stream_alloc(raw_stream, client->max_frame_size, true);
    if (stream == NULL) {
        trevrpc_msquic_stream_close(raw_stream);
        return -ENOMEM;
    }

    *out_stream = stream;
    return 0;
}

void trevrpc_client_close(trevrpc_client* client) {
    if (client == NULL) {
        return;
    }

    trevrpc_msquic_conn_close(client->conn);
    free(client);
}

static bool trevrpc_method_matches(
    const trevrpc_method* method, const char* service, size_t service_len, const char* name, size_t name_len) {
    return method->service_len == service_len && method->method_len == name_len &&
           memcmp(method->service, service, service_len) == 0 && memcmp(method->method, name, name_len) == 0;
}

int trevrpc_server_listen(const char* host, uint16_t port, const trevrpc_config* config, trevrpc_server** out_server) {
    if (host == NULL || out_server == NULL) {
        return -EINVAL;
    }
    *out_server = NULL;

    trevrpc_server* server = calloc(1, sizeof(*server));
    if (server == NULL) {
        return -ENOMEM;
    }
    server->max_frame_size = trevrpc_effective_max_frame_size(config);
    pthread_mutex_init(&server->mutex, NULL);
    pthread_cond_init(&server->cond, NULL);

    trevrpc_msquic_config msquic_config = trevrpc_make_msquic_config(config);
    int err = trevrpc_msquic_listen(host, port, &msquic_config, &server->listener);
    if (err != 0) {
        trevrpc_server_close(server);
        return err;
    }

    *out_server = server;
    return 0;
}

int trevrpc_server_register_unary(
    trevrpc_server* server, const char* service, const char* method, trevrpc_unary_handler handler, void* user_data) {
    if (server == NULL || service == NULL || method == NULL || handler == NULL) {
        return -EINVAL;
    }

    size_t service_len = strlen(service);
    size_t method_len = strlen(method);
    if (service_len == 0 || method_len == 0) {
        return -EINVAL;
    }

    trevrpc_method* registered = calloc(1, sizeof(*registered));
    if (registered == NULL) {
        return -ENOMEM;
    }
    registered->service = malloc(service_len);
    registered->method = malloc(method_len);
    if (registered->service == NULL || registered->method == NULL) {
        free(registered->service);
        free(registered->method);
        free(registered);
        return -ENOMEM;
    }
    memcpy(registered->service, service, service_len);
    memcpy(registered->method, method, method_len);
    registered->service_len = service_len;
    registered->method_len = method_len;
    registered->kind = TREVRPC_RPC_KIND_UNARY;
    registered->handler = handler;
    registered->user_data = user_data;

    pthread_mutex_lock(&server->mutex);
    if (server->shutting_down) {
        pthread_mutex_unlock(&server->mutex);
        free(registered->service);
        free(registered->method);
        free(registered);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    for (trevrpc_method* existing = server->methods; existing != NULL; existing = existing->next) {
        if (trevrpc_method_matches(existing, service, service_len, method, method_len)) {
            pthread_mutex_unlock(&server->mutex);
            free(registered->service);
            free(registered->method);
            free(registered);
            return -EEXIST;
        }
    }
    registered->next = server->methods;
    server->methods = registered;
    pthread_mutex_unlock(&server->mutex);
    return 0;
}

int trevrpc_server_register_streaming(trevrpc_server* server,
    const char* service,
    const char* method,
    uint32_t kind,
    trevrpc_stream_handler handler,
    void* user_data) {
    if (server == NULL || service == NULL || method == NULL || handler == NULL) {
        return -EINVAL;
    }
    if (kind == TREVRPC_RPC_KIND_UNARY || kind > TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        return TREVRPC_ERR_UNSUPPORTED_RPC_KIND;
    }

    size_t service_len = strlen(service);
    size_t method_len = strlen(method);
    if (service_len == 0 || method_len == 0) {
        return -EINVAL;
    }

    trevrpc_method* registered = calloc(1, sizeof(*registered));
    if (registered == NULL) {
        return -ENOMEM;
    }
    registered->service = malloc(service_len);
    registered->method = malloc(method_len);
    if (registered->service == NULL || registered->method == NULL) {
        free(registered->service);
        free(registered->method);
        free(registered);
        return -ENOMEM;
    }
    memcpy(registered->service, service, service_len);
    memcpy(registered->method, method, method_len);
    registered->service_len = service_len;
    registered->method_len = method_len;
    registered->kind = kind;
    registered->stream_handler = handler;
    registered->user_data = user_data;

    pthread_mutex_lock(&server->mutex);
    if (server->shutting_down) {
        pthread_mutex_unlock(&server->mutex);
        free(registered->service);
        free(registered->method);
        free(registered);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    for (trevrpc_method* existing = server->methods; existing != NULL; existing = existing->next) {
        if (trevrpc_method_matches(existing, service, service_len, method, method_len)) {
            pthread_mutex_unlock(&server->mutex);
            free(registered->service);
            free(registered->method);
            free(registered);
            return -EEXIST;
        }
    }
    registered->next = server->methods;
    server->methods = registered;
    pthread_mutex_unlock(&server->mutex);
    return 0;
}

static bool trevrpc_server_task_start(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    bool start = !server->shutting_down;
    if (start) {
        server->active_tasks++;
    }
    pthread_mutex_unlock(&server->mutex);
    return start;
}

static void trevrpc_server_task_finish(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    if (server->active_tasks > 0) {
        server->active_tasks--;
    }
    pthread_cond_broadcast(&server->cond);
    pthread_mutex_unlock(&server->mutex);
}

static bool trevrpc_server_is_shutting_down(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    bool shutting_down = server->shutting_down;
    pthread_mutex_unlock(&server->mutex);
    return shutting_down;
}

static int trevrpc_server_conn_add(
    trevrpc_server* server, trevrpc_msquic_conn* conn, trevrpc_server_conn_ref** out_ref) {
    *out_ref = NULL;
    trevrpc_server_conn_ref* ref = malloc(sizeof(*ref));
    if (ref == NULL) {
        return -ENOMEM;
    }
    ref->conn = conn;

    pthread_mutex_lock(&server->mutex);
    ref->next = server->conns;
    server->conns = ref;
    bool shutting_down = server->shutting_down;
    pthread_mutex_unlock(&server->mutex);

    if (shutting_down) {
        trevrpc_msquic_conn_shutdown(conn);
    }

    *out_ref = ref;
    return 0;
}

static void trevrpc_server_conn_remove(trevrpc_server* server, trevrpc_server_conn_ref* ref) {
    pthread_mutex_lock(&server->mutex);
    trevrpc_server_conn_ref** link = &server->conns;
    while (*link != NULL) {
        if (*link == ref) {
            *link = ref->next;
            break;
        }
        link = &(*link)->next;
    }
    pthread_mutex_unlock(&server->mutex);
    free(ref);
}

static trevrpc_method* trevrpc_server_find_method(trevrpc_server* server, const trevrpc_request* request) {
    pthread_mutex_lock(&server->mutex);
    for (trevrpc_method* method = server->methods; method != NULL; method = method->next) {
        if (trevrpc_method_matches(
                method, request->service, request->service_len, request->method, request->method_len)) {
            pthread_mutex_unlock(&server->mutex);
            return method;
        }
    }
    pthread_mutex_unlock(&server->mutex);
    return NULL;
}

static void trevrpc_set_status(trevrpc_response* response, uint32_t status, const char* message) {
    response->status = status;
    if (message != NULL) {
        (void)trevrpc_response_set_message(response, message, strlen(message));
    }
}

static void trevrpc_server_write_response(
    trevrpc_msquic_stream* stream, size_t max_frame_size, trevrpc_response* response) {
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    int err = trevrpc_wire_encode_response(response, max_frame_size, &frame, &frame_len);
    if (err != 0 && response->status == TREVRPC_STATUS_OK) {
        trevrpc_response_reset(response);
        trevrpc_set_status(response, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "response frame exceeded maximum size");
        err = trevrpc_wire_encode_response(response, max_frame_size, &frame, &frame_len);
    }
    if (err == 0) {
        (void)trevrpc_write_frame(stream, frame, frame_len);
    }
    free(frame);
    (void)trevrpc_msquic_stream_shutdown_send(stream);
}

static void trevrpc_server_write_status(
    trevrpc_msquic_stream* stream, size_t max_frame_size, uint32_t status, const char* message) {
    trevrpc_response response = {0};
    trevrpc_set_status(&response, status, message);
    trevrpc_server_write_response(stream, max_frame_size, &response);
    trevrpc_response_reset(&response);
}

static void trevrpc_server_write_stream_status(
    trevrpc_msquic_stream* stream, size_t max_frame_size, uint32_t status, const char* message) {
    trevrpc_stream rpc_stream = {
        .stream = stream,
        .max_frame_size = max_frame_size,
    };
    (void)trevrpc_stream_send_status(&rpc_stream, status, message, message == NULL ? 0 : strlen(message));
    (void)trevrpc_msquic_stream_shutdown_send(stream);
}

static void trevrpc_handle_stream(trevrpc_server* server, trevrpc_msquic_stream* stream) {
    uint8_t* body = NULL;
    size_t body_len = 0;
    intptr_t read = trevrpc_msquic_stream_read_frame(stream, &body, &body_len, server->max_frame_size);
    if (read < 0) {
        uint32_t status =
            read == TREV_MSQUIC_ERR_FRAME_TOO_LARGE ? TREVRPC_STATUS_RESOURCE_EXHAUSTED : TREVRPC_STATUS_UNAVAILABLE;
        const char* message = read == TREV_MSQUIC_ERR_FRAME_TOO_LARGE ? "request frame exceeded maximum size"
                                                                      : "failed to read request frame";
        trevrpc_server_write_status(stream, server->max_frame_size, status, message);
        return;
    }
    if (read == 0) {
        return;
    }

    trevrpc_request request;
    int err = trevrpc_wire_decode_request(body, body_len, &request);
    if (err == TREVRPC_ERR_INVALID_FRAME) {
        trevrpc_server_write_status(
            stream, server->max_frame_size, TREVRPC_STATUS_INVALID_ARGUMENT, "invalid request frame");
        trevrpc_msquic_free(body);
        return;
    }
    if (err == TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION) {
        trevrpc_server_write_status(
            stream, server->max_frame_size, TREVRPC_STATUS_FAILED_PRECONDITION, "unsupported TrevRPC wire version");
        trevrpc_msquic_free(body);
        return;
    }
    if (err != 0) {
        trevrpc_server_write_status(stream, server->max_frame_size, TREVRPC_STATUS_INVALID_ARGUMENT, "invalid request");
        trevrpc_msquic_free(body);
        return;
    }
    trevrpc_method* method = trevrpc_server_find_method(server, &request);
    if (method == NULL) {
        if (request.kind == TREVRPC_RPC_KIND_UNARY) {
            trevrpc_server_write_status(
                stream, server->max_frame_size, TREVRPC_STATUS_UNIMPLEMENTED, "method is not implemented");
        } else {
            trevrpc_server_write_stream_status(
                stream, server->max_frame_size, TREVRPC_STATUS_UNIMPLEMENTED, "method is not implemented");
        }
        trevrpc_msquic_free(body);
        return;
    }
    if (method->kind != request.kind) {
        if (request.kind == TREVRPC_RPC_KIND_UNARY) {
            trevrpc_server_write_status(
                stream, server->max_frame_size, TREVRPC_STATUS_UNIMPLEMENTED, "method RPC kind mismatch");
        } else {
            trevrpc_server_write_stream_status(
                stream, server->max_frame_size, TREVRPC_STATUS_UNIMPLEMENTED, "method RPC kind mismatch");
        }
        trevrpc_msquic_free(body);
        return;
    }

    if (request.kind != TREVRPC_RPC_KIND_UNARY) {
        trevrpc_stream rpc_stream = {
            .stream = stream,
            .max_frame_size = server->max_frame_size,
        };
        err = method->stream_handler(method->user_data, &request, &rpc_stream);
        if (err != 0 && !rpc_stream.sent_status) {
            (void)trevrpc_stream_send_status(
                &rpc_stream, TREVRPC_STATUS_INTERNAL, "handler failed", strlen("handler failed"));
        } else if (!rpc_stream.sent_status) {
            (void)trevrpc_stream_send_status(&rpc_stream, TREVRPC_STATUS_OK, NULL, 0);
        }
        (void)trevrpc_stream_finish_send(&rpc_stream);
        trevrpc_msquic_free(body);
        return;
    }

    trevrpc_response response = {0};
    err = method->handler(method->user_data, &request, &response);
    if (err != 0) {
        trevrpc_response_reset(&response);
        trevrpc_set_status(&response, TREVRPC_STATUS_INTERNAL, "handler failed");
    }

    trevrpc_server_write_response(stream, server->max_frame_size, &response);
    trevrpc_response_reset(&response);
    trevrpc_msquic_free(body);
}

static void* trevrpc_stream_thread(void* arg) {
    trevrpc_stream_task* task = arg;
    trevrpc_handle_stream(task->server, task->stream);
    trevrpc_msquic_stream_close(task->stream);
    trevrpc_server_task_finish(task->server);
    free(task);
    return NULL;
}

static void* trevrpc_conn_thread(void* arg) {
    trevrpc_conn_task* task = arg;
    trevrpc_server* server = task->server;
    trevrpc_msquic_conn* conn = task->conn;
    free(task);

    trevrpc_server_conn_ref* conn_ref = NULL;
    if (trevrpc_server_conn_add(server, conn, &conn_ref) != 0) {
        trevrpc_msquic_conn_close(conn);
        trevrpc_server_task_finish(server);
        return NULL;
    }

    while (!trevrpc_server_is_shutting_down(server)) {
        trevrpc_msquic_stream* stream = NULL;
        int err = trevrpc_msquic_conn_accept_stream(conn, &stream);
        if (err != 0) {
            break;
        }

        if (!trevrpc_server_task_start(server)) {
            trevrpc_msquic_stream_close(stream);
            break;
        }

        trevrpc_stream_task* stream_task = malloc(sizeof(*stream_task));
        if (stream_task == NULL) {
            trevrpc_msquic_stream_close(stream);
            trevrpc_server_task_finish(server);
            continue;
        }
        stream_task->server = server;
        stream_task->stream = stream;

        pthread_t thread;
        err = pthread_create(&thread, NULL, trevrpc_stream_thread, stream_task);
        if (err != 0) {
            trevrpc_handle_stream(server, stream);
            trevrpc_msquic_stream_close(stream);
            trevrpc_server_task_finish(server);
            free(stream_task);
            continue;
        }
        pthread_detach(thread);
    }

    trevrpc_server_conn_remove(server, conn_ref);
    trevrpc_msquic_conn_close(conn);
    trevrpc_server_task_finish(server);
    return NULL;
}

int trevrpc_server_serve(trevrpc_server* server) {
    if (server == NULL) {
        return -EINVAL;
    }
    if (!trevrpc_server_task_start(server)) {
        return TREV_MSQUIC_ERR_CLOSED;
    }

    int result = 0;
    for (;;) {
        trevrpc_msquic_conn* conn = NULL;
        int err = trevrpc_msquic_listener_accept(server->listener, &conn);
        if (err != 0) {
            result = trevrpc_server_is_shutting_down(server) ? 0 : err;
            break;
        }

        if (!trevrpc_server_task_start(server)) {
            trevrpc_msquic_conn_close(conn);
            continue;
        }

        trevrpc_conn_task* task = malloc(sizeof(*task));
        if (task == NULL) {
            trevrpc_msquic_conn_close(conn);
            trevrpc_server_task_finish(server);
            result = -ENOMEM;
            break;
        }
        task->server = server;
        task->conn = conn;

        pthread_t thread;
        err = pthread_create(&thread, NULL, trevrpc_conn_thread, task);
        if (err != 0) {
            free(task);
            trevrpc_msquic_conn_close(conn);
            trevrpc_server_task_finish(server);
            result = -err;
            break;
        }
        pthread_detach(thread);
    }

    trevrpc_server_task_finish(server);
    return result;
}

void trevrpc_server_shutdown(trevrpc_server* server) {
    if (server == NULL) {
        return;
    }

    pthread_mutex_lock(&server->mutex);
    server->shutting_down = true;
    for (trevrpc_server_conn_ref* ref = server->conns; ref != NULL; ref = ref->next) {
        trevrpc_msquic_conn_shutdown(ref->conn);
    }
    pthread_cond_broadcast(&server->cond);
    pthread_mutex_unlock(&server->mutex);

    trevrpc_msquic_listener_shutdown(server->listener);
}

void trevrpc_server_close(trevrpc_server* server) {
    if (server == NULL) {
        return;
    }

    trevrpc_server_shutdown(server);
    pthread_mutex_lock(&server->mutex);
    while (server->active_tasks > 0) {
        pthread_cond_wait(&server->cond, &server->mutex);
    }
    pthread_mutex_unlock(&server->mutex);

    trevrpc_msquic_listener_close(server->listener);
    trevrpc_method* method = server->methods;
    while (method != NULL) {
        trevrpc_method* next = method->next;
        free(method->service);
        free(method->method);
        free(method);
        method = next;
    }

    pthread_cond_destroy(&server->cond);
    pthread_mutex_destroy(&server->mutex);
    free(server);
}

const char* trevrpc_error(int code) {
    switch (code) {
    case TREVRPC_ERR_INVALID_FRAME:
        return "invalid RPC frame";
    case TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION:
        return "unsupported TrevRPC wire version";
    case TREVRPC_ERR_UNSUPPORTED_RPC_KIND:
        return "unsupported TrevRPC RPC kind";
    case TREVRPC_ERR_HANDLER_FAILED:
        return "RPC handler failed";
    case -ENOMEM:
    case ENOMEM:
        return "out of memory";
    case -EINVAL:
    case EINVAL:
        return "invalid argument";
    case -EEXIST:
    case EEXIST:
        return "method already registered";
    default:
        return trevrpc_msquic_error(code);
    }
}
