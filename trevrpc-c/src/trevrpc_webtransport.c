#include "trevrpc_webtransport.h"

#include "trevrpc_msquic.h"

#include <errno.h> // IWYU pragma: keep
#include <limits.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#define TREV_WT_H3_ALPN "h3"
#define TREV_WT_DEFAULT_STREAMS 256
#define TREV_WT_H3_DEFAULT_UNIDI_STREAMS 16
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
#define TREV_WT_H3_SETTINGS_MAX_FLOW_CONTROL_VALUE (1ull << 60)
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
    char* path;
    char* origin;
};

struct trevrpc_wt_session {
    trevrpc_msquic_conn* msquic_conn;
    trevrpc_msquic_stream* local_control;
    trevrpc_msquic_stream* peer_control;
    trevrpc_msquic_stream* peer_unidi_streams[4];
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
    uint8_t* path;
    size_t path_len;
    uint8_t* authority;
    size_t authority_len;
    uint8_t* origin;
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
    msquic_config.peer_unidi_stream_count = TREV_WT_H3_DEFAULT_UNIDI_STREAMS;
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

static char* trevrpc_wt_strdup(const char* value) {
    if (value == NULL) {
        return NULL;
    }
    size_t len = strlen(value);
    char* copy = malloc(len + 1);
    if (copy == NULL) {
        return NULL;
    }
    memcpy(copy, value, len + 1);
    return copy;
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

static bool trevrpc_wt_peer_stream_id_is_unidirectional(uint64_t stream_id) {
    return (stream_id & 0x03u) == 0x02u;
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

static int trevrpc_wt_remember_peer_unidirectional_stream(trevrpc_wt_session* session, trevrpc_msquic_stream* stream) {
    for (size_t i = 0; i < sizeof(session->peer_unidi_streams) / sizeof(session->peer_unidi_streams[0]); i++) {
        if (session->peer_unidi_streams[i] == NULL) {
            session->peer_unidi_streams[i] = stream;
            return 0;
        }
    }
    trevrpc_msquic_stream_close(stream);
    return TREV_WT_ERR_REJECTED;
}

static int trevrpc_wt_accept_peer_bidirectional_stream(
    trevrpc_wt_session* session, trevrpc_msquic_stream** out_stream) {
    *out_stream = NULL;
    for (;;) {
        trevrpc_msquic_stream* stream = NULL;
        int err = trevrpc_msquic_conn_accept_stream(session->msquic_conn, &stream);
        if (err != 0) {
            return trevrpc_wt_map_msquic_error(err);
        }

        uint64_t stream_id = 0;
        err = trevrpc_msquic_stream_id(stream, &stream_id);
        if (err != 0) {
            trevrpc_msquic_stream_close(stream);
            return trevrpc_wt_map_msquic_error(err);
        }
        if (trevrpc_wt_peer_stream_id_is_unidirectional(stream_id)) {
            err = trevrpc_wt_remember_peer_unidirectional_stream(session, stream);
            if (err != 0) {
                return err;
            }
            continue;
        }

        *out_stream = stream;
        return 0;
    }
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

static const uint32_t trevrpc_wt_hpack_huffman_codes[256] = {
    0x1ff8,
    0x7fffd8,
    0xfffffe2,
    0xfffffe3,
    0xfffffe4,
    0xfffffe5,
    0xfffffe6,
    0xfffffe7,
    0xfffffe8,
    0xffffea,
    0x3ffffffc,
    0xfffffe9,
    0xfffffea,
    0x3ffffffd,
    0xfffffeb,
    0xfffffec,
    0xfffffed,
    0xfffffee,
    0xfffffef,
    0xffffff0,
    0xffffff1,
    0xffffff2,
    0x3ffffffe,
    0xffffff3,
    0xffffff4,
    0xffffff5,
    0xffffff6,
    0xffffff7,
    0xffffff8,
    0xffffff9,
    0xffffffa,
    0xffffffb,
    0x14,
    0x3f8,
    0x3f9,
    0xffa,
    0x1ff9,
    0x15,
    0xf8,
    0x7fa,
    0x3fa,
    0x3fb,
    0xf9,
    0x7fb,
    0xfa,
    0x16,
    0x17,
    0x18,
    0x0,
    0x1,
    0x2,
    0x19,
    0x1a,
    0x1b,
    0x1c,
    0x1d,
    0x1e,
    0x1f,
    0x5c,
    0xfb,
    0x7ffc,
    0x20,
    0xffb,
    0x3fc,
    0x1ffa,
    0x21,
    0x5d,
    0x5e,
    0x5f,
    0x60,
    0x61,
    0x62,
    0x63,
    0x64,
    0x65,
    0x66,
    0x67,
    0x68,
    0x69,
    0x6a,
    0x6b,
    0x6c,
    0x6d,
    0x6e,
    0x6f,
    0x70,
    0x71,
    0x72,
    0xfc,
    0x73,
    0xfd,
    0x1ffb,
    0x7fff0,
    0x1ffc,
    0x3ffc,
    0x22,
    0x7ffd,
    0x3,
    0x23,
    0x4,
    0x24,
    0x5,
    0x25,
    0x26,
    0x27,
    0x6,
    0x74,
    0x75,
    0x28,
    0x29,
    0x2a,
    0x7,
    0x2b,
    0x76,
    0x2c,
    0x8,
    0x9,
    0x2d,
    0x77,
    0x78,
    0x79,
    0x7a,
    0x7b,
    0x7ffe,
    0x7fc,
    0x3ffd,
    0x1ffd,
    0xffffffc,
    0xfffe6,
    0x3fffd2,
    0xfffe7,
    0xfffe8,
    0x3fffd3,
    0x3fffd4,
    0x3fffd5,
    0x7fffd9,
    0x3fffd6,
    0x7fffda,
    0x7fffdb,
    0x7fffdc,
    0x7fffdd,
    0x7fffde,
    0xffffeb,
    0x7fffdf,
    0xffffec,
    0xffffed,
    0x3fffd7,
    0x7fffe0,
    0xffffee,
    0x7fffe1,
    0x7fffe2,
    0x7fffe3,
    0x7fffe4,
    0x1fffdc,
    0x3fffd8,
    0x7fffe5,
    0x3fffd9,
    0x7fffe6,
    0x7fffe7,
    0xffffef,
    0x3fffda,
    0x1fffdd,
    0xfffe9,
    0x3fffdb,
    0x3fffdc,
    0x7fffe8,
    0x7fffe9,
    0x1fffde,
    0x7fffea,
    0x3fffdd,
    0x3fffde,
    0xfffff0,
    0x1fffdf,
    0x3fffdf,
    0x7fffeb,
    0x7fffec,
    0x1fffe0,
    0x1fffe1,
    0x3fffe0,
    0x1fffe2,
    0x7fffed,
    0x3fffe1,
    0x7fffee,
    0x7fffef,
    0xfffea,
    0x3fffe2,
    0x3fffe3,
    0x3fffe4,
    0x7ffff0,
    0x3fffe5,
    0x3fffe6,
    0x7ffff1,
    0x3ffffe0,
    0x3ffffe1,
    0xfffeb,
    0x7fff1,
    0x3fffe7,
    0x7ffff2,
    0x3fffe8,
    0x1ffffec,
    0x3ffffe2,
    0x3ffffe3,
    0x3ffffe4,
    0x7ffffde,
    0x7ffffdf,
    0x3ffffe5,
    0xfffff1,
    0x1ffffed,
    0x7fff2,
    0x1fffe3,
    0x3ffffe6,
    0x7ffffe0,
    0x7ffffe1,
    0x3ffffe7,
    0x7ffffe2,
    0xfffff2,
    0x1fffe4,
    0x1fffe5,
    0x3ffffe8,
    0x3ffffe9,
    0xffffffd,
    0x7ffffe3,
    0x7ffffe4,
    0x7ffffe5,
    0xfffec,
    0xfffff3,
    0xfffed,
    0x1fffe6,
    0x3fffe9,
    0x1fffe7,
    0x1fffe8,
    0x7ffff3,
    0x3fffea,
    0x3fffeb,
    0x1ffffee,
    0x1ffffef,
    0xfffff4,
    0xfffff5,
    0x3ffffea,
    0x7ffff4,
    0x3ffffeb,
    0x7ffffe6,
    0x3ffffec,
    0x3ffffed,
    0x7ffffe7,
    0x7ffffe8,
    0x7ffffe9,
    0x7ffffea,
    0x7ffffeb,
    0xffffffe,
    0x7ffffec,
    0x7ffffed,
    0x7ffffee,
    0x7ffffef,
    0x7fffff0,
    0x3ffffee,
};

static const uint8_t trevrpc_wt_hpack_huffman_code_lens[256] = {
    13,
    23,
    28,
    28,
    28,
    28,
    28,
    28,
    28,
    24,
    30,
    28,
    28,
    30,
    28,
    28,
    28,
    28,
    28,
    28,
    28,
    28,
    30,
    28,
    28,
    28,
    28,
    28,
    28,
    28,
    28,
    28,
    6,
    10,
    10,
    12,
    13,
    6,
    8,
    11,
    10,
    10,
    8,
    11,
    8,
    6,
    6,
    6,
    5,
    5,
    5,
    6,
    6,
    6,
    6,
    6,
    6,
    6,
    7,
    8,
    15,
    6,
    12,
    10,
    13,
    6,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    7,
    8,
    7,
    8,
    13,
    19,
    13,
    14,
    6,
    15,
    5,
    6,
    5,
    6,
    5,
    6,
    6,
    6,
    5,
    7,
    7,
    6,
    6,
    6,
    5,
    6,
    7,
    6,
    5,
    5,
    6,
    7,
    7,
    7,
    7,
    7,
    15,
    11,
    14,
    13,
    28,
    20,
    22,
    20,
    20,
    22,
    22,
    22,
    23,
    22,
    23,
    23,
    23,
    23,
    23,
    24,
    23,
    24,
    24,
    22,
    23,
    24,
    23,
    23,
    23,
    23,
    21,
    22,
    23,
    22,
    23,
    23,
    24,
    22,
    21,
    20,
    22,
    22,
    23,
    23,
    21,
    23,
    22,
    22,
    24,
    21,
    22,
    23,
    23,
    21,
    21,
    22,
    21,
    23,
    22,
    23,
    23,
    20,
    22,
    22,
    22,
    23,
    22,
    22,
    23,
    26,
    26,
    20,
    19,
    22,
    23,
    22,
    25,
    26,
    26,
    26,
    27,
    27,
    26,
    24,
    25,
    19,
    21,
    26,
    27,
    27,
    26,
    27,
    24,
    21,
    21,
    26,
    26,
    28,
    27,
    27,
    27,
    20,
    24,
    20,
    21,
    22,
    21,
    21,
    23,
    22,
    22,
    25,
    25,
    24,
    24,
    26,
    23,
    26,
    27,
    26,
    26,
    27,
    27,
    27,
    27,
    27,
    28,
    27,
    27,
    27,
    27,
    27,
    26,
};

static int trevrpc_wt_qpack_varint_read_buffer(
    const uint8_t* data, size_t len, size_t* offset, uint8_t prefix_bits, uint64_t* value) {
    if (prefix_bits == 0 || prefix_bits > 8 || *offset >= len) {
        return TREV_WT_ERR_REJECTED;
    }

    uint64_t prefix_max = ((uint64_t)1 << prefix_bits) - 1;
    uint64_t result = data[*offset] & prefix_max;
    (*offset)++;
    if (result < prefix_max) {
        *value = result;
        return 0;
    }

    uint64_t shift = 0;
    while (*offset < len) {
        uint8_t byte = data[(*offset)++];
        uint64_t chunk = byte & 0x7f;
        if (shift >= 64 || chunk > ((UINT64_MAX - result) >> shift)) {
            return TREV_WT_ERR_REJECTED;
        }
        result += chunk << shift;
        if ((byte & 0x80) == 0) {
            *value = result;
            return 0;
        }
        shift += 7;
    }
    return TREV_WT_ERR_REJECTED;
}

static int trevrpc_wt_qpack_varint_write(
    uint8_t* out, size_t out_len, size_t* offset, uint8_t prefix_bits, uint8_t flags, uint64_t value) {
    if (prefix_bits == 0 || prefix_bits > 8 || *offset >= out_len) {
        return -ENOBUFS;
    }

    uint64_t prefix_max = ((uint64_t)1 << prefix_bits) - 1;
    if (value < prefix_max) {
        out[(*offset)++] = (uint8_t)(flags | value);
        return 0;
    }

    out[(*offset)++] = (uint8_t)(flags | prefix_max);
    value -= prefix_max;
    while (value >= 128) {
        if (*offset >= out_len) {
            return -ENOBUFS;
        }
        out[(*offset)++] = (uint8_t)(0x80 | (value & 0x7f));
        value >>= 7;
    }
    if (*offset >= out_len) {
        return -ENOBUFS;
    }
    out[(*offset)++] = (uint8_t)value;
    return 0;
}

static bool trevrpc_wt_hpack_huffman_find_symbol(uint32_t code, uint8_t code_len, uint8_t* symbol) {
    for (size_t i = 0; i < 256; i++) {
        if (trevrpc_wt_hpack_huffman_code_lens[i] == code_len && trevrpc_wt_hpack_huffman_codes[i] == code) {
            *symbol = (uint8_t)i;
            return true;
        }
    }
    return false;
}

static int trevrpc_wt_hpack_huffman_decode(
    const uint8_t* data, size_t len, uint8_t* out, size_t out_len, size_t* decoded_len) {
    uint32_t code = 0;
    uint8_t code_len = 0;
    size_t out_offset = 0;

    for (size_t i = 0; i < len; i++) {
        for (int bit = 7; bit >= 0; bit--) {
            code = (code << 1) | ((data[i] >> bit) & 1u);
            code_len++;

            uint8_t symbol = 0;
            if (trevrpc_wt_hpack_huffman_find_symbol(code, code_len, &symbol)) {
                if (out_offset >= out_len) {
                    return -ENOBUFS;
                }
                out[out_offset++] = symbol;
                code = 0;
                code_len = 0;
            } else if (code_len >= 30) {
                return TREV_WT_ERR_REJECTED;
            }
        }
    }

    if (code_len > 0) {
        if (code_len > 7 || code != (((uint32_t)1 << code_len) - 1)) {
            return TREV_WT_ERR_REJECTED;
        }
    }
    *decoded_len = out_offset;
    return 0;
}

static int trevrpc_wt_qpack_read_string(const uint8_t* data,
    size_t len,
    size_t* offset,
    uint8_t prefix_bits,
    uint8_t huffman_flag,
    uint8_t** out,
    size_t* out_len) {
    if (*offset >= len) {
        return TREV_WT_ERR_REJECTED;
    }
    bool huffman = (data[*offset] & huffman_flag) != 0;
    uint64_t encoded_len = 0;
    int err = trevrpc_wt_qpack_varint_read_buffer(data, len, offset, prefix_bits, &encoded_len);
    if (err != 0 || encoded_len > SIZE_MAX || len - *offset < (size_t)encoded_len) {
        return TREV_WT_ERR_REJECTED;
    }

    size_t input_len = (size_t)encoded_len;
    size_t capacity = input_len + 1;
    if (huffman) {
        if (input_len > (SIZE_MAX - 1) / 2) {
            return TREV_WT_ERR_REJECTED;
        }
        capacity = input_len * 2 + 1;
    }
    uint8_t* buffer = malloc(capacity);
    if (buffer == NULL) {
        return -ENOMEM;
    }

    if (huffman) {
        size_t decoded_len = 0;
        err = trevrpc_wt_hpack_huffman_decode(data + *offset, input_len, buffer, capacity - 1, &decoded_len);
        if (err != 0) {
            free(buffer);
            return err;
        }
        buffer[decoded_len] = 0;
        *out_len = decoded_len;
    } else {
        memcpy(buffer, data + *offset, input_len);
        buffer[input_len] = 0;
        *out_len = input_len;
    }
    *offset += input_len;
    *out = buffer;
    return 0;
}

static int trevrpc_wt_qpack_put_string(uint8_t* out, size_t out_len, size_t* offset, const char* value) {
    size_t value_len = strlen(value);
    int err = trevrpc_wt_qpack_varint_write(out, out_len, offset, 7, 0, value_len);
    if (err != 0) {
        return err;
    }
    if (out_len - *offset < value_len) {
        return -ENOBUFS;
    }
    memcpy(out + *offset, value, value_len);
    *offset += value_len;
    return 0;
}

static int trevrpc_wt_qpack_put_indexed_static(uint8_t* out, size_t out_len, size_t* offset, uint64_t index) {
    return trevrpc_wt_qpack_varint_write(out, out_len, offset, 6, 0xc0, index);
}

static int trevrpc_wt_qpack_put_literal_static_name(
    uint8_t* out, size_t out_len, size_t* offset, uint64_t name_index, const char* value) {
    int err = trevrpc_wt_qpack_varint_write(out, out_len, offset, 4, 0x50, name_index);
    if (err != 0) {
        return err;
    }
    return trevrpc_wt_qpack_put_string(out, out_len, offset, value);
}

static int trevrpc_wt_qpack_put_literal(
    uint8_t* out, size_t out_len, size_t* offset, const char* name, const char* value) {
    size_t name_len = strlen(name);
    int err = trevrpc_wt_qpack_varint_write(out, out_len, offset, 3, 0x20, name_len);
    if (err != 0) {
        return err;
    }
    if (out_len - *offset < name_len) {
        return -ENOBUFS;
    }
    memcpy(out + *offset, name, name_len);
    *offset += name_len;
    return trevrpc_wt_qpack_put_string(out, out_len, offset, value);
}

static int trevrpc_wt_qpack_static_header(uint64_t index, const char** name, const char** value) {
    if (index >= 99) {
        return TREV_WT_ERR_REJECTED;
    }

    *name = NULL;
    *value = NULL;
    switch (index) {
    case 0:
        *name = ":authority";
        *value = "";
        break;
    case 1:
        *name = ":path";
        *value = "/";
        break;
    case 15:
        *name = ":method";
        *value = "CONNECT";
        break;
    case 23:
        *name = ":scheme";
        *value = "https";
        break;
    case 25:
        *name = ":status";
        *value = "200";
        break;
    case 90:
        *name = "origin";
        *value = "";
        break;
    default:
        break;
    }
    return 0;
}

static int trevrpc_wt_headers_store_value(uint8_t** field, size_t* field_len, const uint8_t* value, size_t value_len) {
    uint8_t* copy = malloc(value_len + 1);
    if (copy == NULL) {
        return -ENOMEM;
    }
    memcpy(copy, value, value_len);
    copy[value_len] = 0;
    free(*field);
    *field = copy;
    *field_len = value_len;
    return 0;
}

static int trevrpc_wt_headers_apply_field(
    trevrpc_wt_headers* headers, const uint8_t* name, size_t name_len, const uint8_t* value, size_t value_len) {
    if (name_len == 7 && memcmp(name, ":method", 7) == 0 && value_len == 7 && memcmp(value, "CONNECT", 7) == 0) {
        headers->method_connect = true;
    } else if (name_len == 9 && memcmp(name, ":protocol", 9) == 0 && value_len == 12 &&
               memcmp(value, "webtransport", 12) == 0) {
        headers->protocol_webtransport = true;
    } else if (name_len == 9 && memcmp(name, ":protocol", 9) == 0 && value_len == 15 &&
               memcmp(value, "webtransport-h3", 15) == 0) {
        headers->protocol_webtransport_h3 = true;
    } else if (name_len == 7 && memcmp(name, ":scheme", 7) == 0 && value_len == 5 && memcmp(value, "https", 5) == 0) {
        headers->scheme_https = true;
    } else if (name_len == 5 && memcmp(name, ":path", 5) == 0) {
        return trevrpc_wt_headers_store_value(&headers->path, &headers->path_len, value, value_len);
    } else if (name_len == 10 && memcmp(name, ":authority", 10) == 0) {
        return trevrpc_wt_headers_store_value(&headers->authority, &headers->authority_len, value, value_len);
    } else if (name_len == 6 && memcmp(name, "origin", 6) == 0) {
        return trevrpc_wt_headers_store_value(&headers->origin, &headers->origin_len, value, value_len);
    } else if (name_len == 7 && memcmp(name, ":status", 7) == 0 && value_len == 3 && memcmp(value, "200", 3) == 0) {
        headers->status_200 = true;
    } else if (name_len == 30 && memcmp(name, "sec-webtransport-http3-draft02", 30) == 0 && value_len == 1 &&
               memcmp(value, "1", 1) == 0) {
        headers->draft02_request = true;
    } else if (name_len == 28 && memcmp(name, "sec-webtransport-http3-draft", 28) == 0 && value_len == 7 &&
               memcmp(value, "draft02", 7) == 0) {
        headers->draft02_response = true;
    }
    return 0;
}

static void trevrpc_wt_headers_cleanup(trevrpc_wt_headers* headers) {
    free(headers->path);
    free(headers->authority);
    free(headers->origin);
}

static int trevrpc_wt_header_block_decode(const uint8_t* data, size_t len, trevrpc_wt_headers* headers) {
    size_t offset = 0;
    uint64_t required_insert_count = 0;
    uint64_t base = 0;
    int err = trevrpc_wt_qpack_varint_read_buffer(data, len, &offset, 8, &required_insert_count);
    if (err != 0) {
        return err;
    }
    err = trevrpc_wt_qpack_varint_read_buffer(data, len, &offset, 7, &base);
    if (err != 0) {
        return err;
    }
    if (required_insert_count != 0 || base != 0) {
        return TREV_WT_ERR_REJECTED;
    }

    while (offset < len) {
        uint8_t prefix = data[offset];
        if ((prefix & 0x80) != 0) {
            if ((prefix & 0x40) == 0) {
                return TREV_WT_ERR_REJECTED;
            }
            uint64_t index = 0;
            err = trevrpc_wt_qpack_varint_read_buffer(data, len, &offset, 6, &index);
            if (err != 0) {
                return err;
            }
            const char* name = NULL;
            const char* value = NULL;
            err = trevrpc_wt_qpack_static_header(index, &name, &value);
            if (err != 0) {
                return err;
            }
            if (name != NULL) {
                err = trevrpc_wt_headers_apply_field(
                    headers, (const uint8_t*)name, strlen(name), (const uint8_t*)value, strlen(value));
                if (err != 0) {
                    return err;
                }
            }
        } else if ((prefix & 0xc0) == 0x40) {
            if ((prefix & 0x10) == 0) {
                return TREV_WT_ERR_REJECTED;
            }
            uint64_t index = 0;
            err = trevrpc_wt_qpack_varint_read_buffer(data, len, &offset, 4, &index);
            if (err != 0) {
                return err;
            }
            const char* name = NULL;
            const char* static_value = NULL;
            err = trevrpc_wt_qpack_static_header(index, &name, &static_value);
            if (err != 0) {
                return err;
            }
            uint8_t* value = NULL;
            size_t value_len = 0;
            err = trevrpc_wt_qpack_read_string(data, len, &offset, 7, 0x80, &value, &value_len);
            if (err != 0) {
                return err;
            }
            if (name != NULL) {
                err = trevrpc_wt_headers_apply_field(headers, (const uint8_t*)name, strlen(name), value, value_len);
            }
            free(value);
            if (err != 0) {
                return err;
            }
        } else if ((prefix & 0xe0) == 0x20) {
            uint8_t* name = NULL;
            uint8_t* value = NULL;
            size_t name_len = 0;
            size_t value_len = 0;
            err = trevrpc_wt_qpack_read_string(data, len, &offset, 3, 0x08, &name, &name_len);
            if (err == 0) {
                err = trevrpc_wt_qpack_read_string(data, len, &offset, 7, 0x80, &value, &value_len);
            }
            if (err == 0) {
                err = trevrpc_wt_headers_apply_field(headers, name, name_len, value, value_len);
            }
            free(value);
            free(name);
            if (err != 0) {
                return err;
            }
        } else {
            return TREV_WT_ERR_REJECTED;
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
    trevrpc_wt_headers_cleanup(&headers);
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
    err = trevrpc_wt_qpack_put_indexed_static(block, sizeof(block), &offset, 15);
    if (err == 0) {
        err = trevrpc_wt_qpack_put_literal(block, sizeof(block), &offset, ":protocol", protocol);
    }
    if (err == 0) {
        err = trevrpc_wt_qpack_put_indexed_static(block, sizeof(block), &offset, 23);
    }
    if (err == 0) {
        err = trevrpc_wt_qpack_put_literal_static_name(block, sizeof(block), &offset, 0, authority);
    }
    if (err == 0) {
        if (strcmp(path, "/") == 0) {
            err = trevrpc_wt_qpack_put_indexed_static(block, sizeof(block), &offset, 1);
        } else {
            err = trevrpc_wt_qpack_put_literal_static_name(block, sizeof(block), &offset, 1, path);
        }
    }
    if (err == 0 && config->origin != NULL) {
        err = trevrpc_wt_qpack_put_literal_static_name(block, sizeof(block), &offset, 90, config->origin);
    }
    if (err == 0 && session->draft == TREV_WT_DRAFT_02) {
        err = trevrpc_wt_qpack_put_literal(block, sizeof(block), &offset, "sec-webtransport-http3-draft02", "1");
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
    int err = trevrpc_wt_accept_peer_bidirectional_stream(session, &stream);
    if (err != 0) {
        return err;
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
        err = trevrpc_wt_qpack_put_indexed_static(block, sizeof(block), &offset, 25);
    }
    if (err == 0 && session->draft == TREV_WT_DRAFT_02) {
        err = trevrpc_wt_qpack_put_literal(block, sizeof(block), &offset, "sec-webtransport-http3-draft", "draft02");
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
    int err = trevrpc_msquic_conn_open_uni_stream(session->msquic_conn, &control);
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
        {TREV_WT_H3_SETTINGS_WT_INITIAL_MAX_STREAMS_UNI, TREV_WT_H3_SETTINGS_MAX_FLOW_CONTROL_VALUE},
        {TREV_WT_H3_SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI, TREV_WT_H3_SETTINGS_MAX_FLOW_CONTROL_VALUE},
        {TREV_WT_H3_SETTINGS_WT_INITIAL_MAX_DATA, TREV_WT_H3_SETTINGS_MAX_FLOW_CONTROL_VALUE},
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

    listener->path = trevrpc_wt_strdup(config->path);
    if (config->path != NULL && listener->path == NULL) {
        free(listener);
        return -ENOMEM;
    }
    listener->origin = trevrpc_wt_strdup(config->origin);
    if (config->origin != NULL && listener->origin == NULL) {
        free(listener->path);
        free(listener);
        return -ENOMEM;
    }

    trevrpc_msquic_config msquic_config = trevrpc_wt_msquic_config(config);
    int err = trevrpc_msquic_listen(config->host, config->port, &msquic_config, &listener->msquic_listener);
    if (err != 0) {
        free(listener->origin);
        free(listener->path);
        free(listener);
        return trevrpc_wt_map_msquic_error(err);
    }

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
        free(listener->origin);
        free(listener->path);
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
        for (size_t i = 0; i < sizeof(session->peer_unidi_streams) / sizeof(session->peer_unidi_streams[0]); i++) {
            trevrpc_msquic_stream_close(session->peer_unidi_streams[i]);
        }
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
