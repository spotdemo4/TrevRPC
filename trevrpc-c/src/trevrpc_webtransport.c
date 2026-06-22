#include "trevrpc_webtransport.h"

#include "trevrpc_msquic.h"

#include <errno.h>
#include <limits.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#define TREV_WT_H3_ALPN "h3"
#define TREV_WT_DEFAULT_STREAMS 256
#define TREV_WT_H3_STREAM_TYPE_CONTROL 0x00
#define TREV_WT_H3_FRAME_SETTINGS 0x04
#define TREV_WT_H3_FRAME_HEADERS 0x01
#define TREV_WT_H3_SETTINGS_QPACK_MAX_TABLE_CAPACITY 0x01
#define TREV_WT_H3_SETTINGS_MAX_FIELD_SECTION_SIZE 0x06
#define TREV_WT_H3_SETTINGS_QPACK_BLOCKED_STREAMS 0x07
#define TREV_WT_H3_SETTINGS_ENABLE_CONNECT_PROTOCOL 0x08
#define TREV_WT_H3_SETTINGS_H3_DATAGRAM 0x33
#define TREV_WT_H3_SETTINGS_H3_DRAFT04_DATAGRAM 0xffd277
#define TREV_WT_H3_SETTINGS_WT_INITIAL_MAX_DATA 0x2b61
#define TREV_WT_H3_SETTINGS_WT_INITIAL_MAX_STREAMS_UNI 0x2b64
#define TREV_WT_H3_SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI 0x2b65
#define TREV_WT_H3_SETTINGS_WEBTRANSPORT_DRAFT02 0x2b603742
#define TREV_WT_H3_SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07 0xc671706a
#define TREV_WT_H3_SETTINGS_WT_ENABLED_DRAFT15 0x2c7cf000
#define TREV_WT_H3_SETTINGS_WT_MAX_SESSIONS 0x14e9cd29
#define TREV_WT_CONNECT_STATUS_OK 200
#define TREV_WT_STREAM_TYPE_BIDI 0x41
#define TREV_WT_STREAM_TYPE_BIDI_LEN 2

typedef enum trevrpc_wt_role {
    TREV_WT_ROLE_SERVER = 0,
    TREV_WT_ROLE_CLIENT = 1,
} trevrpc_wt_role;

typedef enum trevrpc_wt_draft {
    TREV_WT_DRAFT_NONE = 0,
    TREV_WT_DRAFT_02 = 2,
    TREV_WT_DRAFT_07 = 7,
    TREV_WT_DRAFT_15 = 15,
} trevrpc_wt_draft;

typedef struct trevrpc_wt_setting_pair {
    uint64_t id;
    uint64_t value;
} trevrpc_wt_setting_pair;

typedef struct trevrpc_wt_peer_settings {
    bool enable_connect_protocol;
    bool enable_webtransport_draft02;
    bool enable_webtransport_draft07;
    bool enable_webtransport_draft15;
    bool h3_datagram_rfc;
    bool h3_datagram_draft04;
} trevrpc_wt_peer_settings;

struct trevrpc_wt_listener {
    trevrpc_msquic_listener* msquic_listener;
    const char* path;
    const char* origin;
};

struct trevrpc_wt_session {
    trevrpc_msquic_conn* msquic_conn;
    trevrpc_msquic_stream* local_control;
    trevrpc_msquic_stream* peer_control;
    trevrpc_msquic_stream* connect_stream;
    uint64_t connect_stream_id;
    trevrpc_wt_draft draft;
};

struct trevrpc_wt_stream {
    trevrpc_msquic_stream* msquic_stream;
};

typedef struct trevrpc_wt_h3_frame {
    uint64_t type;
    uint64_t len;
} trevrpc_wt_h3_frame;

typedef struct trevrpc_wt_headers {
    bool method_connect;
    bool protocol_webtransport;
    bool protocol_webtransport_h3;
    bool scheme_https;
    bool status_200;
    bool draft02_request;
    bool draft02_response;
    const uint8_t* path;
    size_t path_len;
    const uint8_t* authority;
    size_t authority_len;
    const uint8_t* origin;
    size_t origin_len;
} trevrpc_wt_headers;

typedef int (*trevrpc_wt_headers_validate_fn)(const trevrpc_wt_headers* headers, void* context);

typedef struct trevrpc_wt_accept_connect_context {
    const char* expected_path;
    const char* expected_origin;
    trevrpc_wt_draft draft;
} trevrpc_wt_accept_connect_context;

typedef struct trevrpc_wt_connect_response_context {
    trevrpc_wt_draft draft;
} trevrpc_wt_connect_response_context;

static trevrpc_msquic_config trevrpc_wt_msquic_config(const trevrpc_wt_config* config) {
    trevrpc_msquic_config msquic_config = {0};
    msquic_config.alpn = TREV_WT_H3_ALPN;
    msquic_config.alpn_len = sizeof(TREV_WT_H3_ALPN) - 1;
    msquic_config.cert_file = config->cert_file;
    msquic_config.key_file = config->key_file;
    msquic_config.max_idle_timeout_ms = config->idle_timeout_ms;
    msquic_config.peer_bidi_stream_count =
        config->max_streams_per_session > 0 ? (uint16_t)config->max_streams_per_session : TREV_WT_DEFAULT_STREAMS;
    return msquic_config;
}

static int trevrpc_wt_map_msquic_error(int err) {
    switch (err) {
    case TREV_MSQUIC_ERR_CLOSED:
        return TREV_WT_ERR_CLOSED;
    case TREV_MSQUIC_ERR_FRAME_TOO_LARGE:
        return TREV_WT_ERR_FRAME_TOO_LARGE;
    default:
        return err;
    }
}

static size_t trevrpc_wt_varint_len(uint64_t value) {
    if (value == TREV_WT_STREAM_TYPE_BIDI) {
        return TREV_WT_STREAM_TYPE_BIDI_LEN;
    }
    if (value <= 0x3f) {
        return 1;
    }
    if (value <= 0x3fff) {
        return 2;
    }
    if (value <= 0x3fffffff) {
        return 4;
    }
    return 8;
}

static int trevrpc_wt_varint_write(uint8_t* out, size_t out_len, uint64_t value, size_t* written) {
    size_t len = trevrpc_wt_varint_len(value);
    if (out_len < len) {
        return -ENOBUFS;
    }

    switch (len) {
    case 1:
        out[0] = (uint8_t)value;
        break;
    case 2:
        out[0] = (uint8_t)(0x40 | (value >> 8));
        out[1] = (uint8_t)value;
        break;
    case 4:
        out[0] = (uint8_t)(0x80 | (value >> 24));
        out[1] = (uint8_t)(value >> 16);
        out[2] = (uint8_t)(value >> 8);
        out[3] = (uint8_t)value;
        break;
    case 8:
        out[0] = (uint8_t)(0xc0 | (value >> 56));
        out[1] = (uint8_t)(value >> 48);
        out[2] = (uint8_t)(value >> 40);
        out[3] = (uint8_t)(value >> 32);
        out[4] = (uint8_t)(value >> 24);
        out[5] = (uint8_t)(value >> 16);
        out[6] = (uint8_t)(value >> 8);
        out[7] = (uint8_t)value;
        break;
    default:
        return -EINVAL;
    }

    *written = len;
    return 0;
}

static int trevrpc_wt_read_exact(trevrpc_msquic_stream* stream, uint8_t* data, size_t len) {
    size_t offset = 0;
    while (offset < len) {
        intptr_t n = trevrpc_msquic_stream_read(stream, data + offset, len - offset);
        if (n <= 0) {
            return trevrpc_wt_map_msquic_error((int)n);
        }
        offset += (size_t)n;
    }
    return 0;
}

static int trevrpc_wt_read_varint(trevrpc_msquic_stream* stream, uint64_t* value) {
    uint8_t first = 0;
    int err = trevrpc_wt_read_exact(stream, &first, 1);
    if (err != 0) {
        return err;
    }

    size_t len = (size_t)1 << (first >> 6);
    uint64_t result = first & 0x3f;
    for (size_t i = 1; i < len; i++) {
        uint8_t byte = 0;
        err = trevrpc_wt_read_exact(stream, &byte, 1);
        if (err != 0) {
            return err;
        }
        result = (result << 8) | byte;
    }

    *value = result;
    return 0;
}

static int trevrpc_wt_varint_read_buffer(const uint8_t* data, size_t len, size_t* offset, uint64_t* value) {
    if (*offset >= len) {
        return TREV_WT_ERR_REJECTED;
    }

    uint8_t first = data[*offset];
    size_t varint_len = (size_t)1 << (first >> 6);
    if (len - *offset < varint_len) {
        return TREV_WT_ERR_REJECTED;
    }

    uint64_t result = first & 0x3f;
    for (size_t i = 1; i < varint_len; i++) {
        result = (result << 8) | data[*offset + i];
    }
    *offset += varint_len;
    *value = result;
    return 0;
}

static int trevrpc_wt_write_varints(trevrpc_msquic_stream* stream, const uint64_t* values, size_t count) {
    uint8_t buffer[64];
    size_t offset = 0;
    for (size_t i = 0; i < count; i++) {
        size_t written = 0;
        int err = trevrpc_wt_varint_write(buffer + offset, sizeof(buffer) - offset, values[i], &written);
        if (err != 0) {
            return err;
        }
        offset += written;
    }

    intptr_t n = trevrpc_msquic_stream_write(stream, buffer, offset);
    if (n < 0) {
        return trevrpc_wt_map_msquic_error((int)n);
    }
    return n == (intptr_t)offset ? 0 : TREV_WT_ERR_CLOSED;
}

static uint64_t trevrpc_wt_configured_session_limit(const trevrpc_wt_config* config) {
    if (config != NULL && config->max_sessions_per_connection > 0) {
        return config->max_sessions_per_connection;
    }
    return 1;
}

static bool trevrpc_wt_bool_setting_valid(uint64_t value) {
    return value <= 1;
}

static bool trevrpc_wt_valid_session_id(uint64_t stream_id) {
    return (stream_id & 0x03u) == 0;
}

static int trevrpc_wt_capture_connect_stream_id(trevrpc_wt_session* session, trevrpc_msquic_stream* stream) {
    uint64_t stream_id = 0;
    int err = trevrpc_msquic_stream_id(stream, &stream_id);
    if (err != 0) {
        return trevrpc_wt_map_msquic_error(err);
    }
    if (!trevrpc_wt_valid_session_id(stream_id)) {
        return TREV_WT_ERR_REJECTED;
    }
    session->connect_stream_id = stream_id;
    return 0;
}

static trevrpc_wt_draft trevrpc_wt_select_draft(trevrpc_wt_role role, const trevrpc_wt_peer_settings* settings) {
    if (settings == NULL) {
        return TREV_WT_DRAFT_NONE;
    }

    bool server_peer_can_attempt_draft15 = role == TREV_WT_ROLE_SERVER && settings->h3_datagram_rfc &&
                                           !settings->enable_webtransport_draft02 &&
                                           !settings->enable_webtransport_draft07;
    if (settings->enable_webtransport_draft15 || server_peer_can_attempt_draft15) {
        return TREV_WT_DRAFT_15;
    }
    if (settings->enable_webtransport_draft07) {
        return TREV_WT_DRAFT_07;
    }
    if (settings->enable_webtransport_draft02) {
        return TREV_WT_DRAFT_02;
    }
    return TREV_WT_DRAFT_NONE;
}

static int trevrpc_wt_validate_settings_for_draft(
    trevrpc_wt_role role, trevrpc_wt_draft draft, const trevrpc_wt_peer_settings* settings) {
    if (settings == NULL) {
        return TREV_WT_ERR_REJECTED;
    }

    switch (draft) {
    case TREV_WT_DRAFT_15:
        if (role == TREV_WT_ROLE_CLIENT && !settings->enable_connect_protocol) {
            return TREV_WT_ERR_REJECTED;
        }
        return settings->h3_datagram_rfc ? 0 : TREV_WT_ERR_REJECTED;
    case TREV_WT_DRAFT_07:
        if (role == TREV_WT_ROLE_CLIENT && !settings->enable_connect_protocol) {
            return TREV_WT_ERR_REJECTED;
        }
        return settings->h3_datagram_rfc ? 0 : TREV_WT_ERR_REJECTED;
    case TREV_WT_DRAFT_02:
        return (settings->h3_datagram_rfc || settings->h3_datagram_draft04) ? 0 : TREV_WT_ERR_REJECTED;
    case TREV_WT_DRAFT_NONE:
    default:
        return TREV_WT_ERR_REJECTED;
    }
}

static const char* trevrpc_wt_protocol_for_draft(trevrpc_wt_draft draft) {
    return draft == TREV_WT_DRAFT_15 ? "webtransport-h3" : "webtransport";
}

static int trevrpc_wt_write_all(trevrpc_msquic_stream* stream, const uint8_t* data, size_t len) {
    intptr_t n = trevrpc_msquic_stream_write(stream, data, len);
    if (n < 0) {
        return trevrpc_wt_map_msquic_error((int)n);
    }
    return n == (intptr_t)len ? 0 : TREV_WT_ERR_CLOSED;
}

static int trevrpc_wt_header_block_put_literal(
    uint8_t* out, size_t out_len, size_t* offset, const char* name, const char* value) {
    size_t name_len = strlen(name);
    size_t value_len = strlen(value);
    size_t written = 0;
    if (*offset >= out_len) {
        return -ENOBUFS;
    }
    out[(*offset)++] = 0x20;
    int err = trevrpc_wt_varint_write(out + *offset, out_len - *offset, name_len, &written);
    if (err != 0) {
        return err;
    }
    *offset += written;
    if (out_len - *offset < name_len) {
        return -ENOBUFS;
    }
    memcpy(out + *offset, name, name_len);
    *offset += name_len;

    err = trevrpc_wt_varint_write(out + *offset, out_len - *offset, value_len, &written);
    if (err != 0) {
        return err;
    }
    *offset += written;
    if (out_len - *offset < value_len) {
        return -ENOBUFS;
    }
    memcpy(out + *offset, value, value_len);
    *offset += value_len;
    return 0;
}

static int trevrpc_wt_header_block_decode(const uint8_t* data, size_t len, trevrpc_wt_headers* headers) {
    size_t offset = 0;
    uint64_t required_insert_count = 0;
    uint64_t base = 0;
    int err = trevrpc_wt_varint_read_buffer(data, len, &offset, &required_insert_count);
    if (err != 0) {
        return err;
    }
    err = trevrpc_wt_varint_read_buffer(data, len, &offset, &base);
    if (err != 0) {
        return err;
    }
    if (required_insert_count != 0 || base != 0) {
        return TREV_WT_ERR_REJECTED;
    }

    while (offset < len) {
        uint8_t prefix = data[offset++];
        if ((prefix & 0xe0) != 0x20) {
            return TREV_WT_ERR_REJECTED;
        }
        uint64_t name_len = 0;
        uint64_t value_len = 0;
        err = trevrpc_wt_varint_read_buffer(data, len, &offset, &name_len);
        if (err != 0 || name_len > SIZE_MAX || len - offset < (size_t)name_len) {
            return TREV_WT_ERR_REJECTED;
        }
        const uint8_t* name = data + offset;
        offset += (size_t)name_len;
        err = trevrpc_wt_varint_read_buffer(data, len, &offset, &value_len);
        if (err != 0 || value_len > SIZE_MAX || len - offset < (size_t)value_len) {
            return TREV_WT_ERR_REJECTED;
        }
        const uint8_t* value = data + offset;
        offset += (size_t)value_len;

        if (name_len == 7 && memcmp(name, ":method", 7) == 0 && value_len == 7 && memcmp(value, "CONNECT", 7) == 0) {
            headers->method_connect = true;
        } else if (name_len == 9 && memcmp(name, ":protocol", 9) == 0 && value_len == 12 &&
                   memcmp(value, "webtransport", 12) == 0) {
            headers->protocol_webtransport = true;
        } else if (name_len == 9 && memcmp(name, ":protocol", 9) == 0 && value_len == 15 &&
                   memcmp(value, "webtransport-h3", 15) == 0) {
            headers->protocol_webtransport_h3 = true;
        } else if (name_len == 7 && memcmp(name, ":scheme", 7) == 0 && value_len == 5 &&
                   memcmp(value, "https", 5) == 0) {
            headers->scheme_https = true;
        } else if (name_len == 5 && memcmp(name, ":path", 5) == 0) {
            headers->path = value;
            headers->path_len = (size_t)value_len;
        } else if (name_len == 10 && memcmp(name, ":authority", 10) == 0) {
            headers->authority = value;
            headers->authority_len = (size_t)value_len;
        } else if (name_len == 6 && memcmp(name, "origin", 6) == 0) {
            headers->origin = value;
            headers->origin_len = (size_t)value_len;
        } else if (name_len == 7 && memcmp(name, ":status", 7) == 0 && value_len == 3 && memcmp(value, "200", 3) == 0) {
            headers->status_200 = true;
        } else if (name_len == 30 && memcmp(name, "sec-webtransport-http3-draft02", 30) == 0 && value_len == 1 &&
                   memcmp(value, "1", 1) == 0) {
            headers->draft02_request = true;
        } else if (name_len == 28 && memcmp(name, "sec-webtransport-http3-draft", 28) == 0 && value_len == 7 &&
                   memcmp(value, "draft02", 7) == 0) {
            headers->draft02_response = true;
        }
    }
    return 0;
}

static int trevrpc_wt_write_headers_frame(trevrpc_msquic_stream* stream, const uint8_t* block, size_t block_len) {
    uint8_t prefix[16];
    size_t offset = 0;
    size_t written = 0;
    int err = trevrpc_wt_varint_write(prefix + offset, sizeof(prefix) - offset, TREV_WT_H3_FRAME_HEADERS, &written);
    if (err != 0) {
        return err;
    }
    offset += written;
    err = trevrpc_wt_varint_write(prefix + offset, sizeof(prefix) - offset, block_len, &written);
    if (err != 0) {
        return err;
    }
    offset += written;

    err = trevrpc_wt_write_all(stream, prefix, offset);
    if (err != 0) {
        return err;
    }
    return trevrpc_wt_write_all(stream, block, block_len);
}

static int trevrpc_wt_read_headers_frame(
    trevrpc_msquic_stream* stream, trevrpc_wt_headers_validate_fn validate, void* context) {
    uint64_t frame_type = 0;
    uint64_t frame_len = 0;
    int err = trevrpc_wt_read_varint(stream, &frame_type);
    if (err != 0) {
        return err;
    }
    err = trevrpc_wt_read_varint(stream, &frame_len);
    if (err != 0) {
        return err;
    }
    if (frame_type != TREV_WT_H3_FRAME_HEADERS || frame_len > 4096) {
        return TREV_WT_ERR_REJECTED;
    }

    uint8_t* block = malloc((size_t)frame_len);
    if (block == NULL && frame_len > 0) {
        return -ENOMEM;
    }
    trevrpc_wt_headers headers = {0};
    err = trevrpc_wt_read_exact(stream, block, (size_t)frame_len);
    if (err == 0) {
        err = trevrpc_wt_header_block_decode(block, (size_t)frame_len, &headers);
    }
    if (err == 0 && validate != NULL) {
        err = validate(&headers, context);
    }
    free(block);
    return err;
}

static int trevrpc_wt_validate_connect_response(const trevrpc_wt_headers* headers, void* context) {
    trevrpc_wt_connect_response_context* response_context = context;
    if (!headers->status_200) {
        return TREV_WT_ERR_REJECTED;
    }
    if (response_context != NULL && response_context->draft == TREV_WT_DRAFT_02 && !headers->draft02_response) {
        return TREV_WT_ERR_REJECTED;
    }
    return 0;
}

static bool trevrpc_wt_connect_protocol_matches(const trevrpc_wt_headers* headers, trevrpc_wt_draft draft) {
    switch (draft) {
    case TREV_WT_DRAFT_15:
        return headers->protocol_webtransport_h3 || headers->protocol_webtransport;
    case TREV_WT_DRAFT_07:
    case TREV_WT_DRAFT_02:
        return headers->protocol_webtransport;
    case TREV_WT_DRAFT_NONE:
    default:
        return false;
    }
}

static int trevrpc_wt_validate_connect_request(const trevrpc_wt_headers* headers, void* context) {
    trevrpc_wt_accept_connect_context* accept_context = context;
    if (!headers->method_connect || !trevrpc_wt_connect_protocol_matches(headers, accept_context->draft) ||
        !headers->scheme_https || headers->path == NULL || headers->authority == NULL) {
        return TREV_WT_ERR_REJECTED;
    }
    if (accept_context->draft == TREV_WT_DRAFT_02 && !headers->draft02_request) {
        return TREV_WT_ERR_REJECTED;
    }
    if (accept_context->expected_path != NULL &&
        (strlen(accept_context->expected_path) != headers->path_len ||
            memcmp(accept_context->expected_path, headers->path, headers->path_len) != 0)) {
        return TREV_WT_ERR_REJECTED;
    }
    if (accept_context->expected_origin != NULL &&
        (strlen(accept_context->expected_origin) != headers->origin_len ||
            memcmp(accept_context->expected_origin, headers->origin, headers->origin_len) != 0)) {
        return TREV_WT_ERR_REJECTED;
    }
    return 0;
}

static int trevrpc_wt_open_connect_stream(trevrpc_wt_session* session, const trevrpc_wt_config* config) {
    trevrpc_msquic_stream* stream = NULL;
    int err = trevrpc_msquic_conn_open_stream(session->msquic_conn, &stream);
    if (err != 0) {
        return trevrpc_wt_map_msquic_error(err);
    }
    err = trevrpc_wt_capture_connect_stream_id(session, stream);
    if (err != 0) {
        trevrpc_msquic_stream_close(stream);
        return err;
    }

    const char* path = config->path != NULL ? config->path : "/";
    const char* authority = config->url != NULL ? config->url : config->host;
    const char* protocol = trevrpc_wt_protocol_for_draft(session->draft);
    uint8_t block[1024];
    size_t offset = 0;
    block[offset++] = 0;
    block[offset++] = 0;
    err = trevrpc_wt_header_block_put_literal(block, sizeof(block), &offset, ":method", "CONNECT");
    if (err == 0) {
        err = trevrpc_wt_header_block_put_literal(block, sizeof(block), &offset, ":protocol", protocol);
    }
    if (err == 0) {
        err = trevrpc_wt_header_block_put_literal(block, sizeof(block), &offset, ":scheme", "https");
    }
    if (err == 0) {
        err = trevrpc_wt_header_block_put_literal(block, sizeof(block), &offset, ":authority", authority);
    }
    if (err == 0) {
        err = trevrpc_wt_header_block_put_literal(block, sizeof(block), &offset, ":path", path);
    }
    if (err == 0 && config->origin != NULL) {
        err = trevrpc_wt_header_block_put_literal(block, sizeof(block), &offset, "origin", config->origin);
    }
    if (err == 0 && session->draft == TREV_WT_DRAFT_02) {
        err = trevrpc_wt_header_block_put_literal(block, sizeof(block), &offset, "sec-webtransport-http3-draft02", "1");
    }
    if (err == 0) {
        err = trevrpc_wt_write_headers_frame(stream, block, offset);
    }
    if (err == 0) {
        trevrpc_wt_connect_response_context context = {
            .draft = session->draft,
        };
        err = trevrpc_wt_read_headers_frame(stream, trevrpc_wt_validate_connect_response, &context);
    }
    if (err != 0) {
        trevrpc_msquic_stream_close(stream);
        return err;
    }

    session->connect_stream = stream;
    return 0;
}

static int trevrpc_wt_accept_connect_stream(
    trevrpc_wt_session* session, const char* expected_path, const char* expected_origin) {
    trevrpc_msquic_stream* stream = NULL;
    int err = trevrpc_msquic_conn_accept_stream(session->msquic_conn, &stream);
    if (err != 0) {
        return trevrpc_wt_map_msquic_error(err);
    }
    err = trevrpc_wt_capture_connect_stream_id(session, stream);
    if (err != 0) {
        trevrpc_msquic_stream_close(stream);
        return err;
    }

    trevrpc_wt_accept_connect_context context = {
        .expected_path = expected_path,
        .expected_origin = expected_origin,
        .draft = session->draft,
    };
    err = trevrpc_wt_read_headers_frame(stream, trevrpc_wt_validate_connect_request, &context);

    uint8_t block[64];
    size_t offset = 0;
    block[offset++] = 0;
    block[offset++] = 0;
    if (err == 0) {
        err = trevrpc_wt_header_block_put_literal(block, sizeof(block), &offset, ":status", "200");
    }
    if (err == 0 && session->draft == TREV_WT_DRAFT_02) {
        err = trevrpc_wt_header_block_put_literal(
            block, sizeof(block), &offset, "sec-webtransport-http3-draft", "draft02");
    }
    if (err == 0) {
        err = trevrpc_wt_write_headers_frame(stream, block, offset);
    }
    if (err != 0) {
        trevrpc_msquic_stream_close(stream);
        return err;
    }

    session->connect_stream = stream;
    return 0;
}

static int trevrpc_wt_write_control_settings(trevrpc_wt_session* session, const trevrpc_wt_config* config) {
    trevrpc_msquic_stream* control = NULL;
    int err = trevrpc_msquic_conn_open_stream(session->msquic_conn, &control);
    if (err != 0) {
        return trevrpc_wt_map_msquic_error(err);
    }

    uint64_t session_limit = trevrpc_wt_configured_session_limit(config);
    const trevrpc_wt_setting_pair settings[] = {
        {TREV_WT_H3_SETTINGS_QPACK_MAX_TABLE_CAPACITY, 0},
        {TREV_WT_H3_SETTINGS_QPACK_BLOCKED_STREAMS, 0},
        {TREV_WT_H3_SETTINGS_ENABLE_CONNECT_PROTOCOL, 1},
        {TREV_WT_H3_SETTINGS_WT_ENABLED_DRAFT15, 1},
        {TREV_WT_H3_SETTINGS_WT_MAX_SESSIONS, session_limit},
        {TREV_WT_H3_SETTINGS_WEBTRANSPORT_DRAFT02, 1},
        {TREV_WT_H3_SETTINGS_H3_DATAGRAM, 1},
        {TREV_WT_H3_SETTINGS_H3_DRAFT04_DATAGRAM, 1},
        {TREV_WT_H3_SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07, session_limit},
        {TREV_WT_H3_SETTINGS_MAX_FIELD_SECTION_SIZE, 4096},
    };

    size_t payload_len = 0;
    for (size_t i = 0; i < sizeof(settings) / sizeof(settings[0]); i++) {
        payload_len += trevrpc_wt_varint_len(settings[i].id) + trevrpc_wt_varint_len(settings[i].value);
    }

    uint8_t buffer[256];
    size_t offset = 0;
    size_t written = 0;
    err = trevrpc_wt_varint_write(buffer + offset, sizeof(buffer) - offset, TREV_WT_H3_STREAM_TYPE_CONTROL, &written);
    if (err == 0) {
        offset += written;
        err = trevrpc_wt_varint_write(buffer + offset, sizeof(buffer) - offset, TREV_WT_H3_FRAME_SETTINGS, &written);
    }
    if (err == 0) {
        offset += written;
        err = trevrpc_wt_varint_write(buffer + offset, sizeof(buffer) - offset, payload_len, &written);
    }
    if (err == 0) {
        offset += written;
        for (size_t i = 0; i < sizeof(settings) / sizeof(settings[0]); i++) {
            err = trevrpc_wt_varint_write(buffer + offset, sizeof(buffer) - offset, settings[i].id, &written);
            if (err != 0) {
                break;
            }
            offset += written;
            err = trevrpc_wt_varint_write(buffer + offset, sizeof(buffer) - offset, settings[i].value, &written);
            if (err != 0) {
                break;
            }
            offset += written;
        }
    }
    if (err == 0) {
        err = trevrpc_wt_write_all(control, buffer, offset);
    }
    session->local_control = control;
    return err;
}

static int trevrpc_wt_read_settings_payload(
    trevrpc_msquic_stream* control, uint64_t len, trevrpc_wt_peer_settings* settings) {
    uint8_t payload[4096];
    size_t payload_len = (size_t)len;
    size_t offset = 0;
    int err = trevrpc_wt_read_exact(control, payload, payload_len);
    if (err != 0) {
        return err;
    }

    while (offset < payload_len) {
        uint64_t id = 0;
        uint64_t value = 0;
        err = trevrpc_wt_varint_read_buffer(payload, payload_len, &offset, &id);
        if (err != 0) {
            return TREV_WT_ERR_REJECTED;
        }
        err = trevrpc_wt_varint_read_buffer(payload, payload_len, &offset, &value);
        if (err != 0) {
            return TREV_WT_ERR_REJECTED;
        }
        switch (id) {
        case TREV_WT_H3_SETTINGS_ENABLE_CONNECT_PROTOCOL:
            if (!trevrpc_wt_bool_setting_valid(value)) {
                return TREV_WT_ERR_REJECTED;
            }
            settings->enable_connect_protocol = value == 1;
            break;
        case TREV_WT_H3_SETTINGS_WEBTRANSPORT_DRAFT02:
            if (!trevrpc_wt_bool_setting_valid(value)) {
                return TREV_WT_ERR_REJECTED;
            }
            settings->enable_webtransport_draft02 = value == 1;
            break;
        case TREV_WT_H3_SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07:
            settings->enable_webtransport_draft07 = value != 0;
            break;
        case TREV_WT_H3_SETTINGS_WT_ENABLED_DRAFT15:
            settings->enable_webtransport_draft15 = value != 0;
            break;
        case TREV_WT_H3_SETTINGS_WT_MAX_SESSIONS:
            settings->enable_webtransport_draft15 = value != 0;
            break;
        case TREV_WT_H3_SETTINGS_H3_DATAGRAM:
            if (!trevrpc_wt_bool_setting_valid(value)) {
                return TREV_WT_ERR_REJECTED;
            }
            settings->h3_datagram_rfc = value == 1;
            break;
        case TREV_WT_H3_SETTINGS_H3_DRAFT04_DATAGRAM:
            if (!trevrpc_wt_bool_setting_valid(value)) {
                return TREV_WT_ERR_REJECTED;
            }
            settings->h3_datagram_draft04 = value == 1;
            break;
        default:
            break;
        }
    }

    return 0;
}

static int trevrpc_wt_read_peer_control_settings(
    trevrpc_wt_session* session, trevrpc_wt_role role, trevrpc_wt_peer_settings* settings) {
    trevrpc_msquic_stream* control = NULL;
    int err = trevrpc_msquic_conn_accept_stream(session->msquic_conn, &control);
    if (err != 0) {
        return trevrpc_wt_map_msquic_error(err);
    }

    uint64_t stream_type = 0;
    err = trevrpc_wt_read_varint(control, &stream_type);
    if (err == 0 && stream_type != TREV_WT_H3_STREAM_TYPE_CONTROL) {
        err = TREV_WT_ERR_REJECTED;
    }

    trevrpc_wt_h3_frame frame = {0};
    if (err == 0) {
        err = trevrpc_wt_read_varint(control, &frame.type);
    }
    if (err == 0) {
        err = trevrpc_wt_read_varint(control, &frame.len);
    }
    if (err == 0 && (frame.type != TREV_WT_H3_FRAME_SETTINGS || frame.len > 4096)) {
        err = TREV_WT_ERR_REJECTED;
    }

    if (err == 0) {
        err = trevrpc_wt_read_settings_payload(control, frame.len, settings);
    }
    if (err == 0) {
        session->draft = trevrpc_wt_select_draft(role, settings);
        err = trevrpc_wt_validate_settings_for_draft(role, session->draft, settings);
    }

    session->peer_control = control;
    return err;
}

static int trevrpc_wt_h3_handshake(trevrpc_wt_session* session, const trevrpc_wt_config* config, trevrpc_wt_role role) {
    int err = trevrpc_wt_write_control_settings(session, config);
    if (err != 0) {
        return err;
    }
    trevrpc_wt_peer_settings peer_settings = {0};
    err = trevrpc_wt_read_peer_control_settings(session, role, &peer_settings);
    return err;
}

static trevrpc_wt_stream* trevrpc_wt_stream_alloc(trevrpc_msquic_stream* msquic_stream) {
    trevrpc_wt_stream* stream = calloc(1, sizeof(*stream));
    if (stream == NULL) {
        return NULL;
    }
    stream->msquic_stream = msquic_stream;
    return stream;
}

int trevrpc_wt_listen(const trevrpc_wt_config* config, trevrpc_wt_listener** out_listener) {
    if (config == NULL || out_listener == NULL || config->host == NULL || config->cert_file == NULL ||
        config->key_file == NULL) {
        return -EINVAL;
    }
    *out_listener = NULL;

    trevrpc_wt_listener* listener = calloc(1, sizeof(*listener));
    if (listener == NULL) {
        return -ENOMEM;
    }

    trevrpc_msquic_config msquic_config = trevrpc_wt_msquic_config(config);
    int err = trevrpc_msquic_listen(config->host, config->port, &msquic_config, &listener->msquic_listener);
    if (err != 0) {
        free(listener);
        return trevrpc_wt_map_msquic_error(err);
    }
    listener->path = config->path;
    listener->origin = config->origin;

    *out_listener = listener;
    return 0;
}

int trevrpc_wt_listener_accept_session(trevrpc_wt_listener* listener, trevrpc_wt_session** out_session) {
    if (listener == NULL || out_session == NULL) {
        return -EINVAL;
    }
    *out_session = NULL;

    trevrpc_msquic_conn* conn = NULL;
    int err = trevrpc_msquic_listener_accept(listener->msquic_listener, &conn);
    if (err != 0) {
        return trevrpc_wt_map_msquic_error(err);
    }

    trevrpc_wt_config config = {
        .path = listener->path,
        .origin = listener->origin,
    };
    return trevrpc_wt_accept_session_from_msquic(conn, &config, out_session);
}

int trevrpc_wt_accept_session_from_msquic(
    trevrpc_msquic_conn* conn, const trevrpc_wt_config* config, trevrpc_wt_session** out_session) {
    if (conn == NULL || config == NULL || out_session == NULL) {
        return -EINVAL;
    }
    *out_session = NULL;

    trevrpc_wt_session* session = calloc(1, sizeof(*session));
    if (session == NULL) {
        trevrpc_msquic_conn_close(conn);
        return -ENOMEM;
    }
    session->msquic_conn = conn;
    int err = trevrpc_wt_h3_handshake(session, config, TREV_WT_ROLE_SERVER);
    if (err != 0) {
        trevrpc_wt_session_close(session);
        return err;
    }
    err = trevrpc_wt_accept_connect_stream(session, config->path, config->origin);
    if (err != 0) {
        trevrpc_wt_session_close(session);
        return err;
    }
    *out_session = session;
    return 0;
}

int trevrpc_wt_listener_port(trevrpc_wt_listener* listener, uint16_t* out_port) {
    if (listener == NULL || out_port == NULL) {
        return -EINVAL;
    }
    return trevrpc_wt_map_msquic_error(trevrpc_msquic_listener_port(listener->msquic_listener, out_port));
}

void trevrpc_wt_listener_shutdown(trevrpc_wt_listener* listener) {
    if (listener != NULL) {
        trevrpc_msquic_listener_shutdown(listener->msquic_listener);
    }
}

void trevrpc_wt_listener_close(trevrpc_wt_listener* listener) {
    if (listener != NULL) {
        trevrpc_msquic_listener_close(listener->msquic_listener);
    }
    free(listener);
}

int trevrpc_wt_dial(const trevrpc_wt_config* config, trevrpc_wt_session** out_session) {
    if (config == NULL || out_session == NULL || config->host == NULL) {
        return -EINVAL;
    }
    *out_session = NULL;

    trevrpc_wt_session* session = calloc(1, sizeof(*session));
    if (session == NULL) {
        return -ENOMEM;
    }

    trevrpc_msquic_config msquic_config = trevrpc_wt_msquic_config(config);
    int err = trevrpc_msquic_dial(config->host, config->port, &msquic_config, &session->msquic_conn);
    if (err != 0) {
        free(session);
        return trevrpc_wt_map_msquic_error(err);
    }

    err = trevrpc_wt_h3_handshake(session, config, TREV_WT_ROLE_CLIENT);
    if (err != 0) {
        trevrpc_wt_session_close(session);
        return err;
    }

    err = trevrpc_wt_open_connect_stream(session, config);
    if (err != 0) {
        trevrpc_wt_session_close(session);
        return err;
    }

    *out_session = session;
    return 0;
}

int trevrpc_wt_session_accept_stream(trevrpc_wt_session* session, trevrpc_wt_stream** out_stream) {
    if (session == NULL || out_stream == NULL) {
        return -EINVAL;
    }
    *out_stream = NULL;
    if (session->msquic_conn == NULL) {
        return TREV_WT_ERR_CLOSED;
    }

    trevrpc_msquic_stream* msquic_stream = NULL;
    int err = trevrpc_msquic_conn_accept_stream(session->msquic_conn, &msquic_stream);
    if (err != 0) {
        return trevrpc_wt_map_msquic_error(err);
    }

    uint64_t stream_type = 0;
    err = trevrpc_wt_read_varint(msquic_stream, &stream_type);
    if (err != 0) {
        trevrpc_msquic_stream_close(msquic_stream);
        return err;
    }
    if (stream_type != TREV_WT_STREAM_TYPE_BIDI) {
        trevrpc_msquic_stream_close(msquic_stream);
        return TREV_WT_ERR_REJECTED;
    }
    uint64_t session_id = 0;
    err = trevrpc_wt_read_varint(msquic_stream, &session_id);
    if (err != 0) {
        trevrpc_msquic_stream_close(msquic_stream);
        return err;
    }
    if (session_id != session->connect_stream_id || !trevrpc_wt_valid_session_id(session_id)) {
        trevrpc_msquic_stream_close(msquic_stream);
        return TREV_WT_ERR_REJECTED;
    }

    trevrpc_wt_stream* stream = trevrpc_wt_stream_alloc(msquic_stream);
    if (stream == NULL) {
        trevrpc_msquic_stream_close(msquic_stream);
        return -ENOMEM;
    }
    *out_stream = stream;
    return 0;
}

int trevrpc_wt_session_open_stream(trevrpc_wt_session* session, trevrpc_wt_stream** out_stream) {
    if (session == NULL || out_stream == NULL) {
        return -EINVAL;
    }
    *out_stream = NULL;
    if (session->msquic_conn == NULL) {
        return TREV_WT_ERR_CLOSED;
    }

    trevrpc_msquic_stream* msquic_stream = NULL;
    int err = trevrpc_msquic_conn_open_stream(session->msquic_conn, &msquic_stream);
    if (err != 0) {
        return trevrpc_wt_map_msquic_error(err);
    }

    const uint64_t stream_header[] = {TREV_WT_STREAM_TYPE_BIDI, session->connect_stream_id};
    err = trevrpc_wt_write_varints(msquic_stream, stream_header, sizeof(stream_header) / sizeof(stream_header[0]));
    if (err != 0) {
        trevrpc_msquic_stream_close(msquic_stream);
        return err;
    }

    trevrpc_wt_stream* stream = trevrpc_wt_stream_alloc(msquic_stream);
    if (stream == NULL) {
        trevrpc_msquic_stream_close(msquic_stream);
        return -ENOMEM;
    }
    *out_stream = stream;
    return 0;
}

void trevrpc_wt_session_close(trevrpc_wt_session* session) {
    if (session != NULL) {
        trevrpc_wt_session_shutdown(session);
        trevrpc_msquic_stream_close(session->connect_stream);
        trevrpc_msquic_stream_close(session->peer_control);
        trevrpc_msquic_stream_close(session->local_control);
        trevrpc_msquic_conn_close(session->msquic_conn);
        free(session);
    }
}

void trevrpc_wt_session_shutdown(trevrpc_wt_session* session) {
    if (session != NULL) {
        trevrpc_msquic_conn_shutdown(session->msquic_conn);
    }
}

intptr_t trevrpc_wt_stream_read(trevrpc_wt_stream* stream, uint8_t* data, size_t len) {
    if (stream == NULL) {
        return -EINVAL;
    }
    if (stream->msquic_stream == NULL) {
        return TREV_WT_ERR_CLOSED;
    }
    intptr_t n = trevrpc_msquic_stream_read(stream->msquic_stream, data, len);
    return n < 0 ? trevrpc_wt_map_msquic_error((int)n) : n;
}

intptr_t trevrpc_wt_stream_read_frame(trevrpc_wt_stream* stream, uint8_t** body, size_t* len, size_t max_len) {
    if (stream == NULL || body == NULL || len == NULL) {
        return -EINVAL;
    }
    *body = NULL;
    *len = 0;
    if (stream->msquic_stream == NULL) {
        return TREV_WT_ERR_CLOSED;
    }
    intptr_t n = trevrpc_msquic_stream_read_frame(stream->msquic_stream, body, len, max_len);
    return n < 0 ? trevrpc_wt_map_msquic_error((int)n) : n;
}

intptr_t trevrpc_wt_stream_write(trevrpc_wt_stream* stream, const uint8_t* data, size_t len) {
    if (stream == NULL) {
        return -EINVAL;
    }
    if (stream->msquic_stream == NULL) {
        return TREV_WT_ERR_CLOSED;
    }
    intptr_t n = trevrpc_msquic_stream_write(stream->msquic_stream, data, len);
    return n < 0 ? trevrpc_wt_map_msquic_error((int)n) : n;
}

int trevrpc_wt_stream_shutdown_send(trevrpc_wt_stream* stream) {
    if (stream == NULL) {
        return -EINVAL;
    }
    if (stream->msquic_stream == NULL) {
        return TREV_WT_ERR_CLOSED;
    }
    return trevrpc_wt_map_msquic_error(trevrpc_msquic_stream_shutdown_send(stream->msquic_stream));
}

int trevrpc_wt_stream_abort(trevrpc_wt_stream* stream, uint32_t error_code) {
    (void)error_code;
    if (stream == NULL) {
        return -EINVAL;
    }
    if (stream->msquic_stream == NULL) {
        return TREV_WT_ERR_CLOSED;
    }
    return trevrpc_wt_map_msquic_error(trevrpc_msquic_stream_abort_receive(stream->msquic_stream));
}

void trevrpc_wt_stream_close(trevrpc_wt_stream* stream) {
    if (stream != NULL) {
        trevrpc_msquic_stream_close(stream->msquic_stream);
    }
    free(stream);
}

void trevrpc_wt_free(void* ptr) {
    free(ptr);
}

const char* trevrpc_wt_error(int code) {
    switch (code) {
    case 0:
        return "ok";
    case TREV_WT_ERR_CLOSED:
        return "closed";
    case TREV_WT_ERR_FRAME_TOO_LARGE:
        return "frame too large";
    case TREV_WT_ERR_REJECTED:
        return "WebTransport negotiation rejected";
    case -ENOMEM:
    case ENOMEM:
        return "out of memory";
    case -EINVAL:
    case EINVAL:
        return "invalid argument";
    default:
        return "WebTransport operation failed";
    }
}
