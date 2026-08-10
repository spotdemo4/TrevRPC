#define _POSIX_C_SOURCE 200809L

#include "trevrpc_webtransport.h"

#include "trevrpc_msquic.h"

#include "trevrpc_frame_internal.h"
#include "trevrpc_msquic_internal.h"

#include <errno.h> // IWYU pragma: keep
#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define TREV_WT_H3_ALPN "h3"
#define TREV_WT_DEFAULT_STREAMS 256
#define TREV_WT_H3_DEFAULT_UNIDI_STREAMS 16
#define TREV_WT_H3_STREAM_TYPE_CONTROL 0x00
#define TREV_WT_H3_FRAME_SETTINGS 0x04
#define TREV_WT_H3_FRAME_DATA 0x00
#define TREV_WT_H3_FRAME_HEADERS 0x01
#define TREV_WT_H3_FRAME_CANCEL_PUSH 0x03
#define TREV_WT_H3_FRAME_PUSH_PROMISE 0x05
#define TREV_WT_H3_FRAME_GOAWAY 0x07
#define TREV_WT_H3_FRAME_MAX_PUSH_ID 0x0d
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
#define TREV_WT_NETWORK_FRAMEWORK_INITIAL_MAX_DATA (8ull * 1024 * 1024)
#define TREV_WT_NETWORK_FRAMEWORK_INITIAL_MAX_STREAMS 100
#define TREV_WT_CAPSULE_MAX_DATA 0x190b4d3d
#define TREV_WT_CAPSULE_MAX_STREAMS_BIDI 0x190b4d3f
#define TREV_WT_CAPSULE_MAX_STREAMS_UNI 0x190b4d40
#define TREV_WT_CONNECT_STATUS_OK 200
#define TREV_WT_STREAM_TYPE_BIDI 0x41
#define TREV_WT_STREAM_TYPE_BIDI_LEN 2
#define TREV_H3_MAX_FIELD_SECTION_SIZE 4096
#define TREV_H3_MAX_ENCODED_FIELD_SECTION_SIZE (16 * 1024)
#define TREV_H3_MAX_UNKNOWN_FRAME_SIZE (16 * 1024 * 1024)
#define TREV_H3_CONTENT_TYPE "application/trevrpc"
#define TREV_H3_REQUEST_TIMEOUT_STATUS 408
#define TREV_H3_QPACK_SET_CAPACITY_ZERO 0x20

#define TREV_H3_ERR_QPACK_DECOMPRESSION_FAILED -3101
#define TREV_H3_ERR_FRAME_UNEXPECTED -3102
#define TREV_H3_ERR_FRAME_ERROR -3103
#define TREV_H3_ERR_FIELD_SECTION_TOO_LARGE -3104
#define TREV_H3_ERR_MESSAGE_ERROR -3105

#define TREV_H3_APP_GENERAL_PROTOCOL_ERROR 0x101
#define TREV_H3_APP_STREAM_CREATION_ERROR 0x103
#define TREV_H3_APP_CLOSED_CRITICAL_STREAM 0x104
#define TREV_H3_APP_FRAME_UNEXPECTED 0x105
#define TREV_H3_APP_FRAME_ERROR 0x106
#define TREV_H3_APP_EXCESSIVE_LOAD 0x107
#define TREV_H3_APP_MISSING_SETTINGS 0x10a
#define TREV_H3_APP_QPACK_DECOMPRESSION_FAILED 0x200
#define TREV_H3_APP_QPACK_ENCODER_STREAM_ERROR 0x201
#define TREV_H3_APP_QPACK_DECODER_STREAM_ERROR 0x202

typedef enum trevrpc_wt_role {
    TREV_WT_ROLE_SERVER = 0,
    TREV_WT_ROLE_CLIENT = 1,
} trevrpc_wt_role;

typedef enum trevrpc_wt_draft {
    TREV_WT_DRAFT_NONE = 0,
    TREV_WT_DRAFT_02 = 2,
    TREV_WT_DRAFT_07 = 7,
    TREV_WT_DRAFT_14 = 14,
    TREV_WT_DRAFT_15 = 15,
} trevrpc_wt_draft;

typedef enum trevrpc_h3_read_mode {
    TREV_H3_READ_BLOCK = 0,
    TREV_H3_READ_READY = 1,
    TREV_H3_READ_DEADLINE = 2,
} trevrpc_h3_read_mode;

typedef struct trevrpc_wt_setting_pair {
    uint64_t id;
    uint64_t value;
} trevrpc_wt_setting_pair;

typedef struct trevrpc_wt_peer_settings {
    bool enable_connect_protocol;
    bool enable_webtransport_draft02;
    bool enable_webtransport_draft07;
    bool enable_webtransport_draft14;
    bool enable_webtransport_draft15;
    uint64_t wt_initial_max_data;
    uint64_t wt_initial_max_streams_uni;
    uint64_t wt_initial_max_streams_bidi;
    bool h3_datagram_rfc;
    bool h3_datagram_draft04;
} trevrpc_wt_peer_settings;

struct trevrpc_wt_listener {
    trevrpc_msquic_listener* msquic_listener;
    char* path;
    char* origin;
    trevrpc_webtransport_admission admission;
    void* admission_user_data;
};

struct trevrpc_wt_session {
    trevrpc_msquic_conn* msquic_conn;
    trevrpc_msquic_stream* local_control;
    trevrpc_msquic_stream* peer_control;
    trevrpc_msquic_stream* peer_unidi_streams[TREV_WT_H3_DEFAULT_UNIDI_STREAMS];
    trevrpc_msquic_stream* connect_stream;
    uint64_t connect_stream_id;
    trevrpc_wt_draft draft;
    bool requires_initial_capsule_flow_control;
};

struct trevrpc_wt_stream {
    trevrpc_msquic_stream* msquic_stream;
};

struct trevrpc_h3_conn {
    trevrpc_wt_session session;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    pthread_t control_thread;
    pthread_t unidi_threads[TREV_WT_H3_DEFAULT_UNIDI_STREAMS];
    const char* webtransport_path;
    const char* webtransport_origin;
    trevrpc_webtransport_admission webtransport_admission;
    void* webtransport_admission_user_data;
    const char* http3_path;
    trevrpc_http3_admission http3_admission;
    void* http3_admission_user_data;
    bool enable_http3;
    bool webtransport_connected;
    bool webtransport_resolving;
    bool shutting_down;
    bool control_thread_started;
    bool unidi_thread_started[TREV_WT_H3_DEFAULT_UNIDI_STREAMS];
    bool qpack_encoder_seen;
    bool qpack_decoder_seen;
};

typedef struct trevrpc_h3_unidi_monitor_context {
    trevrpc_h3_conn* conn;
    size_t index;
} trevrpc_h3_unidi_monitor_context;

struct trevrpc_h3_stream {
    trevrpc_msquic_stream* msquic_stream;
    trevrpc_h3_conn* conn;
    uint64_t data_remaining;
    uint64_t skip_remaining;
    uint64_t frame_type;
    uint8_t varint[8];
    size_t varint_len;
    size_t varint_need;
    bool have_frame_type;
    uint8_t* trailer_block;
    size_t trailer_len;
    size_t trailer_offset;
    trevrpc_frame_parser rpc_parser;
    bool rpc_parser_initialized;
    bool trailers_seen;
    bool owns_msquic_stream;
};

typedef struct trevrpc_wt_h3_frame {
    uint64_t type;
    uint64_t len;
} trevrpc_wt_h3_frame;

typedef struct trevrpc_wt_headers {
    bool method_seen;
    bool method_connect;
    bool method_post;
    bool protocol_seen;
    bool protocol_webtransport;
    bool protocol_webtransport_h3;
    bool scheme_seen;
    bool scheme_https;
    bool status_seen;
    bool status_200;
    bool content_type_seen;
    bool content_type_trevrpc;
    bool draft02_request;
    bool draft02_response;
    bool regular_seen;
    bool saw_pseudo_header;
    size_t field_section_size;
    uint8_t* path;
    size_t path_len;
    uint8_t* authority;
    size_t authority_len;
    uint8_t* origin;
    size_t origin_len;
} trevrpc_wt_headers;

typedef int (*trevrpc_wt_headers_validate_fn)(const trevrpc_wt_headers* headers, void* context);

typedef struct trevrpc_wt_accept_connect_context {
    trevrpc_webtransport_admission admission;
    void* admission_user_data;
    trevrpc_wt_draft draft;
} trevrpc_wt_accept_connect_context;

typedef struct trevrpc_wt_path_origin_policy {
    const char* path;
    const char* origin;
} trevrpc_wt_path_origin_policy;

typedef struct trevrpc_wt_connect_response_context {
    trevrpc_wt_draft draft;
} trevrpc_wt_connect_response_context;

static uint64_t trevrpc_h3_monotonic_nanos(void);
static intptr_t trevrpc_h3_read_varint_incremental(
    trevrpc_h3_stream* stream, uint64_t* value, trevrpc_h3_read_mode mode, uint64_t deadline_nanos);

static uint16_t trevrpc_wt_effective_stream_limit(uint32_t configured) {
    if (configured == 0) {
        return TREV_WT_DEFAULT_STREAMS;
    }
    return configured > UINT16_MAX ? UINT16_MAX : (uint16_t)configured;
}

static trevrpc_msquic_config trevrpc_wt_msquic_config(const trevrpc_wt_config* config) {
    trevrpc_msquic_config msquic_config = {0};
    msquic_config.alpn = TREV_WT_H3_ALPN;
    msquic_config.alpn_len = sizeof(TREV_WT_H3_ALPN) - 1;
    msquic_config.cert_file = config->cert_file;
    msquic_config.key_file = config->key_file;
    msquic_config.ca_cert_file = config->ca_cert_file;
    msquic_config.skip_certificate_validation = config->skip_certificate_validation;
    msquic_config.max_idle_timeout_ms = config->idle_timeout_ms;
    msquic_config.peer_bidi_stream_count = trevrpc_wt_effective_stream_limit(config->max_streams_per_session);
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
            return n == 0 ? TREV_WT_ERR_CLOSED : trevrpc_wt_map_msquic_error((int)n);
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

static bool trevrpc_wt_is_network_framework_legacy_peer(const trevrpc_wt_peer_settings* settings) {
    return settings != NULL && settings->h3_datagram_rfc && settings->enable_webtransport_draft07 &&
           settings->enable_webtransport_draft14 && !settings->enable_webtransport_draft15 &&
           settings->wt_initial_max_data == TREV_WT_NETWORK_FRAMEWORK_INITIAL_MAX_DATA &&
           settings->wt_initial_max_streams_uni == TREV_WT_NETWORK_FRAMEWORK_INITIAL_MAX_STREAMS &&
           settings->wt_initial_max_streams_bidi == TREV_WT_NETWORK_FRAMEWORK_INITIAL_MAX_STREAMS;
}

static trevrpc_wt_draft trevrpc_wt_select_draft(trevrpc_wt_role role, const trevrpc_wt_peer_settings* settings) {
    if (settings == NULL) {
        return TREV_WT_DRAFT_NONE;
    }

    bool server_peer_can_attempt_draft15 =
        role == TREV_WT_ROLE_SERVER && settings->h3_datagram_rfc && !settings->enable_webtransport_draft02 &&
        !settings->enable_webtransport_draft07 && !settings->enable_webtransport_draft14;
    if (settings->enable_webtransport_draft15 || server_peer_can_attempt_draft15) {
        return TREV_WT_DRAFT_15;
    }
    if (role == TREV_WT_ROLE_SERVER && trevrpc_wt_is_network_framework_legacy_peer(settings)) {
        return TREV_WT_DRAFT_07;
    }
    if (settings->enable_webtransport_draft14) {
        return TREV_WT_DRAFT_14;
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
    case TREV_WT_DRAFT_14:
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
    static const struct {
        const char* name;
        const char* value;
    } table[] = {
        {":authority", ""},
        {":path", "/"},
        {"age", "0"},
        {"content-disposition", ""},
        {"content-length", "0"},
        {"cookie", ""},
        {"date", ""},
        {"etag", ""},
        {"if-modified-since", ""},
        {"if-none-match", ""},
        {"last-modified", ""},
        {"link", ""},
        {"location", ""},
        {"referer", ""},
        {"set-cookie", ""},
        {":method", "CONNECT"},
        {":method", "DELETE"},
        {":method", "GET"},
        {":method", "HEAD"},
        {":method", "OPTIONS"},
        {":method", "POST"},
        {":method", "PUT"},
        {":scheme", "http"},
        {":scheme", "https"},
        {":status", "103"},
        {":status", "200"},
        {":status", "304"},
        {":status", "404"},
        {":status", "503"},
        {"accept", "*/*"},
        {"accept", "application/dns-message"},
        {"accept-encoding", "gzip, deflate, br"},
        {"accept-ranges", "bytes"},
        {"access-control-allow-headers", "cache-control"},
        {"access-control-allow-headers", "content-type"},
        {"access-control-allow-origin", "*"},
        {"cache-control", "max-age=0"},
        {"cache-control", "max-age=2592000"},
        {"cache-control", "max-age=604800"},
        {"cache-control", "no-cache"},
        {"cache-control", "no-store"},
        {"cache-control", "public, max-age=31536000"},
        {"content-encoding", "br"},
        {"content-encoding", "gzip"},
        {"content-type", "application/dns-message"},
        {"content-type", "application/javascript"},
        {"content-type", "application/json"},
        {"content-type", "application/x-www-form-urlencoded"},
        {"content-type", "image/gif"},
        {"content-type", "image/jpeg"},
        {"content-type", "image/png"},
        {"content-type", "text/css"},
        {"content-type", "text/html; charset=utf-8"},
        {"content-type", "text/plain"},
        {"content-type", "text/plain;charset=utf-8"},
        {"range", "bytes=0-"},
        {"strict-transport-security", "max-age=31536000"},
        {"strict-transport-security", "max-age=31536000; includesubdomains"},
        {"strict-transport-security", "max-age=31536000; includesubdomains; preload"},
        {"vary", "accept-encoding"},
        {"vary", "origin"},
        {"x-content-type-options", "nosniff"},
        {"x-xss-protection", "1; mode=block"},
        {":status", "100"},
        {":status", "204"},
        {":status", "206"},
        {":status", "302"},
        {":status", "400"},
        {":status", "403"},
        {":status", "421"},
        {":status", "425"},
        {":status", "500"},
        {"accept-language", ""},
        {"access-control-allow-credentials", "FALSE"},
        {"access-control-allow-credentials", "TRUE"},
        {"access-control-allow-headers", "*"},
        {"access-control-allow-methods", "get"},
        {"access-control-allow-methods", "get, post, options"},
        {"access-control-allow-methods", "options"},
        {"access-control-expose-headers", "content-length"},
        {"access-control-request-headers", "content-type"},
        {"access-control-request-method", "get"},
        {"access-control-request-method", "post"},
        {"alt-svc", "clear"},
        {"authorization", ""},
        {"content-security-policy", "script-src 'none'; object-src 'none'; base-uri 'none'"},
        {"early-data", "1"},
        {"expect-ct", ""},
        {"forwarded", ""},
        {"if-range", ""},
        {"origin", ""},
        {"purpose", "prefetch"},
        {"server", ""},
        {"timing-allow-origin", "*"},
        {"upgrade-insecure-requests", "1"},
        {"user-agent", ""},
        {"x-forwarded-for", ""},
        {"x-frame-options", "deny"},
        {"x-frame-options", "sameorigin"},
    };
    if (index >= sizeof(table) / sizeof(table[0])) {
        return TREV_WT_ERR_REJECTED;
    }
    *name = table[index].name;
    *value = table[index].value;
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

static bool trevrpc_h3_ascii_equal_case(const uint8_t* value, size_t value_len, const char* expected) {
    size_t expected_len = strlen(expected);
    if (value_len != expected_len) {
        return false;
    }
    for (size_t i = 0; i < value_len; i++) {
        uint8_t actual = value[i];
        if (actual >= 'A' && actual <= 'Z') {
            actual = (uint8_t)(actual + ('a' - 'A'));
        }
        if (actual != (uint8_t)expected[i]) {
            return false;
        }
    }
    return true;
}

static int trevrpc_wt_headers_apply_field(
    trevrpc_wt_headers* headers, const uint8_t* name, size_t name_len, const uint8_t* value, size_t value_len) {
    if (name_len == 0 || name_len > TREV_H3_MAX_FIELD_SECTION_SIZE || value_len > TREV_H3_MAX_FIELD_SECTION_SIZE) {
        return TREV_H3_ERR_FIELD_SECTION_TOO_LARGE;
    }
    size_t field_size = 32;
    if (name_len > SIZE_MAX - field_size || value_len > SIZE_MAX - field_size - name_len) {
        return TREV_H3_ERR_FIELD_SECTION_TOO_LARGE;
    }
    field_size += name_len + value_len;
    if (field_size > TREV_H3_MAX_FIELD_SECTION_SIZE ||
        headers->field_section_size > TREV_H3_MAX_FIELD_SECTION_SIZE - field_size) {
        return TREV_H3_ERR_FIELD_SECTION_TOO_LARGE;
    }
    headers->field_section_size += field_size;
    for (size_t i = 0; i < name_len; i++) {
        if (name[i] >= 'A' && name[i] <= 'Z') {
            return TREV_H3_ERR_MESSAGE_ERROR;
        }
    }

    bool pseudo = name[0] == ':';
    if (pseudo) {
        if (headers->regular_seen) {
            return TREV_H3_ERR_MESSAGE_ERROR;
        }
        headers->saw_pseudo_header = true;
    } else {
        headers->regular_seen = true;
    }

    if (name_len == 7 && memcmp(name, ":method", 7) == 0) {
        if (headers->method_seen) {
            return TREV_H3_ERR_MESSAGE_ERROR;
        }
        headers->method_seen = true;
        headers->method_connect = value_len == 7 && memcmp(value, "CONNECT", 7) == 0;
        headers->method_post = value_len == 4 && memcmp(value, "POST", 4) == 0;
    } else if (name_len == 9 && memcmp(name, ":protocol", 9) == 0) {
        if (headers->protocol_seen) {
            return TREV_H3_ERR_MESSAGE_ERROR;
        }
        headers->protocol_seen = true;
        headers->protocol_webtransport = value_len == 12 && memcmp(value, "webtransport", 12) == 0;
        headers->protocol_webtransport_h3 = value_len == 15 && memcmp(value, "webtransport-h3", 15) == 0;
        if (!headers->protocol_webtransport && !headers->protocol_webtransport_h3) {
            return TREV_H3_ERR_MESSAGE_ERROR;
        }
    } else if (name_len == 7 && memcmp(name, ":scheme", 7) == 0) {
        if (headers->scheme_seen) {
            return TREV_H3_ERR_MESSAGE_ERROR;
        }
        headers->scheme_seen = true;
        headers->scheme_https = value_len == 5 && memcmp(value, "https", 5) == 0;
    } else if (name_len == 5 && memcmp(name, ":path", 5) == 0) {
        if (headers->path != NULL) {
            return TREV_H3_ERR_MESSAGE_ERROR;
        }
        return trevrpc_wt_headers_store_value(&headers->path, &headers->path_len, value, value_len);
    } else if (name_len == 10 && memcmp(name, ":authority", 10) == 0) {
        if (headers->authority != NULL) {
            return TREV_H3_ERR_MESSAGE_ERROR;
        }
        return trevrpc_wt_headers_store_value(&headers->authority, &headers->authority_len, value, value_len);
    } else if (name_len == 6 && memcmp(name, "origin", 6) == 0) {
        if (headers->origin != NULL) {
            return TREV_H3_ERR_MESSAGE_ERROR;
        }
        return trevrpc_wt_headers_store_value(&headers->origin, &headers->origin_len, value, value_len);
    } else if (name_len == 7 && memcmp(name, ":status", 7) == 0) {
        if (headers->status_seen) {
            return TREV_H3_ERR_MESSAGE_ERROR;
        }
        headers->status_seen = true;
        headers->status_200 = value_len == 3 && memcmp(value, "200", 3) == 0;
    } else if (name_len == 12 && memcmp(name, "content-type", 12) == 0) {
        if (headers->content_type_seen) {
            return TREV_H3_ERR_MESSAGE_ERROR;
        }
        headers->content_type_seen = true;
        headers->content_type_trevrpc = trevrpc_h3_ascii_equal_case(value, value_len, TREV_H3_CONTENT_TYPE);
    } else if (name_len == 30 && memcmp(name, "sec-webtransport-http3-draft02", 30) == 0 && value_len == 1 &&
               memcmp(value, "1", 1) == 0) {
        headers->draft02_request = true;
    } else if (name_len == 28 && memcmp(name, "sec-webtransport-http3-draft", 28) == 0 && value_len == 7 &&
               memcmp(value, "draft02", 7) == 0) {
        headers->draft02_response = true;
    } else if ((name_len == 2 && memcmp(name, "te", 2) == 0 && (value_len != 8 || memcmp(value, "trailers", 8) != 0)) ||
               pseudo || (name_len == 10 && memcmp(name, "connection", 10) == 0) ||
               (name_len == 16 && memcmp(name, "proxy-connection", 16) == 0) ||
               (name_len == 10 && memcmp(name, "keep-alive", 10) == 0) ||
               (name_len == 17 && memcmp(name, "transfer-encoding", 17) == 0) ||
               (name_len == 7 && memcmp(name, "upgrade", 7) == 0)) {
        return TREV_H3_ERR_MESSAGE_ERROR;
    }
    return 0;
}

static void trevrpc_wt_headers_cleanup(trevrpc_wt_headers* headers) {
    free(headers->path);
    free(headers->authority);
    free(headers->origin);
}

static int trevrpc_h3_qpack_error(int err) {
    return err == TREV_WT_ERR_REJECTED ? TREV_H3_ERR_QPACK_DECOMPRESSION_FAILED : err;
}

static int trevrpc_wt_header_block_decode(const uint8_t* data, size_t len, trevrpc_wt_headers* headers) {
    size_t offset = 0;
    uint64_t required_insert_count = 0;
    uint64_t base = 0;
    int err = trevrpc_wt_qpack_varint_read_buffer(data, len, &offset, 8, &required_insert_count);
    if (err != 0) {
        return trevrpc_h3_qpack_error(err);
    }
    bool negative_base = offset < len && (data[offset] & 0x80) != 0;
    err = trevrpc_wt_qpack_varint_read_buffer(data, len, &offset, 7, &base);
    if (err != 0) {
        return trevrpc_h3_qpack_error(err);
    }
    if (required_insert_count != 0 || base != 0 || negative_base) {
        return TREV_H3_ERR_QPACK_DECOMPRESSION_FAILED;
    }

    while (offset < len) {
        uint8_t prefix = data[offset];
        if ((prefix & 0x80) != 0) {
            if ((prefix & 0x40) == 0) {
                return TREV_H3_ERR_QPACK_DECOMPRESSION_FAILED;
            }
            uint64_t index = 0;
            err = trevrpc_wt_qpack_varint_read_buffer(data, len, &offset, 6, &index);
            if (err != 0) {
                return trevrpc_h3_qpack_error(err);
            }
            const char* name = NULL;
            const char* value = NULL;
            err = trevrpc_wt_qpack_static_header(index, &name, &value);
            if (err != 0) {
                return trevrpc_h3_qpack_error(err);
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
                return TREV_H3_ERR_QPACK_DECOMPRESSION_FAILED;
            }
            uint64_t index = 0;
            err = trevrpc_wt_qpack_varint_read_buffer(data, len, &offset, 4, &index);
            if (err != 0) {
                return trevrpc_h3_qpack_error(err);
            }
            const char* name = NULL;
            const char* static_value = NULL;
            err = trevrpc_wt_qpack_static_header(index, &name, &static_value);
            if (err != 0) {
                return trevrpc_h3_qpack_error(err);
            }
            uint8_t* value = NULL;
            size_t value_len = 0;
            err = trevrpc_wt_qpack_read_string(data, len, &offset, 7, 0x80, &value, &value_len);
            if (err != 0) {
                return trevrpc_h3_qpack_error(err);
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
                return trevrpc_h3_qpack_error(err);
            }
        } else {
            return TREV_H3_ERR_QPACK_DECOMPRESSION_FAILED;
        }
    }
    return 0;
}

static int trevrpc_wt_write_headers_frame_with_fin(
    trevrpc_msquic_stream* stream, const uint8_t* block, size_t block_len, bool finish_send) {
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

    if (finish_send) {
        if (block_len > SIZE_MAX - offset) {
            return -EOVERFLOW;
        }
        uint8_t* frame = malloc(offset + block_len);
        if (frame == NULL) {
            return -ENOMEM;
        }
        memcpy(frame, prefix, offset);
        memcpy(frame + offset, block, block_len);
        intptr_t written_bytes = trevrpc_msquic_stream_write_fin(stream, frame, offset + block_len);
        free(frame);
        if (written_bytes < 0) {
            return trevrpc_wt_map_msquic_error((int)written_bytes);
        }
        return (size_t)written_bytes == offset + block_len ? 0 : TREV_WT_ERR_CLOSED;
    }
    err = trevrpc_wt_write_all(stream, prefix, offset);
    return err == 0 ? trevrpc_wt_write_all(stream, block, block_len) : err;
}

static int trevrpc_wt_write_headers_frame(trevrpc_msquic_stream* stream, const uint8_t* block, size_t block_len) {
    return trevrpc_wt_write_headers_frame_with_fin(stream, block, block_len, false);
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
    if (frame_type != TREV_WT_H3_FRAME_HEADERS || frame_len > TREV_H3_MAX_ENCODED_FIELD_SECTION_SIZE) {
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
    if (err <= TREV_H3_ERR_QPACK_DECOMPRESSION_FAILED && err >= TREV_H3_ERR_MESSAGE_ERROR) {
        return TREV_WT_ERR_REJECTED;
    }
    return err;
}

static int trevrpc_h3_write_response_headers(trevrpc_msquic_stream* stream, unsigned status, bool finish_send) {
    char status_text[4] = {
        (char)('0' + status / 100),
        (char)('0' + (status / 10) % 10),
        (char)('0' + status % 10),
        0,
    };
    uint8_t block[128];
    size_t offset = 0;
    block[offset++] = 0;
    block[offset++] = 0;
    int err = status == 200 ? trevrpc_wt_qpack_put_indexed_static(block, sizeof(block), &offset, 25)
                            : trevrpc_wt_qpack_put_literal(block, sizeof(block), &offset, ":status", status_text);
    if (err == 0 && status == 200) {
        err = trevrpc_wt_qpack_put_literal_static_name(block, sizeof(block), &offset, 44, TREV_H3_CONTENT_TYPE);
    }
    return err == 0 ? trevrpc_wt_write_headers_frame_with_fin(stream, block, offset, finish_send) : err;
}

static int trevrpc_wt_write_capsule_value_frame(trevrpc_msquic_stream* stream, uint64_t type, uint64_t value) {
    uint8_t value_bytes[8];
    size_t value_len = 0;
    int err = trevrpc_wt_varint_write(value_bytes, sizeof(value_bytes), value, &value_len);
    if (err != 0) {
        return err;
    }

    uint8_t capsule[24];
    size_t capsule_len = 0;
    size_t written = 0;
    err = trevrpc_wt_varint_write(capsule + capsule_len, sizeof(capsule) - capsule_len, type, &written);
    if (err == 0) {
        capsule_len += written;
        err = trevrpc_wt_varint_write(capsule + capsule_len, sizeof(capsule) - capsule_len, value_len, &written);
    }
    if (err == 0) {
        capsule_len += written;
        memcpy(capsule + capsule_len, value_bytes, value_len);
        capsule_len += value_len;
    }

    uint8_t frame[40];
    size_t frame_len = 0;
    if (err == 0) {
        err = trevrpc_wt_varint_write(frame + frame_len, sizeof(frame) - frame_len, TREV_WT_H3_FRAME_DATA, &written);
    }
    if (err == 0) {
        frame_len += written;
        err = trevrpc_wt_varint_write(frame + frame_len, sizeof(frame) - frame_len, capsule_len, &written);
    }
    if (err == 0) {
        frame_len += written;
        memcpy(frame + frame_len, capsule, capsule_len);
        frame_len += capsule_len;
    }
    return err == 0 ? trevrpc_wt_write_all(stream, frame, frame_len) : err;
}

static int trevrpc_wt_write_initial_capsule_flow_control(trevrpc_wt_session* session, trevrpc_msquic_stream* stream) {
    if (!session->requires_initial_capsule_flow_control) {
        return 0;
    }
    int err = trevrpc_wt_write_capsule_value_frame(
        stream, TREV_WT_CAPSULE_MAX_STREAMS_BIDI, TREV_WT_H3_SETTINGS_MAX_FLOW_CONTROL_VALUE);
    if (err == 0) {
        err = trevrpc_wt_write_capsule_value_frame(stream, TREV_WT_CAPSULE_MAX_STREAMS_UNI, 0);
    }
    if (err == 0) {
        err = trevrpc_wt_write_capsule_value_frame(
            stream, TREV_WT_CAPSULE_MAX_DATA, TREV_WT_H3_SETTINGS_MAX_FLOW_CONTROL_VALUE);
    }
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
    case TREV_WT_DRAFT_14:
    case TREV_WT_DRAFT_07:
    case TREV_WT_DRAFT_02:
        return headers->protocol_webtransport;
    case TREV_WT_DRAFT_NONE:
    default:
        return false;
    }
}

static int trevrpc_wt_path_origin_admission(void* user_data, const trevrpc_webtransport_admission_request* request) {
    const trevrpc_wt_path_origin_policy* policy = user_data;
    if (policy == NULL || request == NULL) {
        return TREV_WT_ERR_REJECTED;
    }
    if (policy->path != NULL &&
        (strlen(policy->path) != request->path_len || memcmp(policy->path, request->path, request->path_len) != 0)) {
        return TREV_WT_ERR_REJECTED;
    }
    if (policy->origin != NULL && (request->origin == NULL || strlen(policy->origin) != request->origin_len ||
                                      memcmp(policy->origin, request->origin, request->origin_len) != 0)) {
        return TREV_WT_ERR_REJECTED;
    }
    return 0;
}

static int trevrpc_wt_validate_connect_request(const trevrpc_wt_headers* headers, void* context) {
    trevrpc_wt_accept_connect_context* accept_context = context;
    if (!headers->method_connect || !trevrpc_wt_connect_protocol_matches(headers, accept_context->draft) ||
        !headers->scheme_https || headers->path == NULL || headers->authority == NULL || headers->authority_len == 0 ||
        headers->status_seen) {
        return TREV_WT_ERR_REJECTED;
    }
    if (accept_context->draft == TREV_WT_DRAFT_02 && !headers->draft02_request) {
        return TREV_WT_ERR_REJECTED;
    }
    trevrpc_webtransport_admission_request request = {
        .path = (const char*)headers->path,
        .path_len = headers->path_len,
        .authority = (const char*)headers->authority,
        .authority_len = headers->authority_len,
        .origin = (const char*)headers->origin,
        .origin_len = headers->origin_len,
        .secure = 1,
    };
    if (accept_context->admission != NULL &&
        accept_context->admission(accept_context->admission_user_data, &request) != 0) {
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

static int trevrpc_wt_accept_connect_stream(trevrpc_wt_session* session, const trevrpc_wt_config* config) {
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

    trevrpc_wt_path_origin_policy path_origin_policy = {
        .path = config == NULL ? NULL : config->path,
        .origin = config == NULL ? NULL : config->origin,
    };
    trevrpc_wt_accept_connect_context context = {
        .admission = config != NULL && config->admission != NULL ? config->admission : trevrpc_wt_path_origin_admission,
        .admission_user_data =
            config != NULL && config->admission != NULL ? config->admission_user_data : &path_origin_policy,
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
    if (err == 0) {
        err = trevrpc_wt_write_initial_capsule_flow_control(session, stream);
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
    trevrpc_wt_setting_pair settings[13];
    size_t settings_len = 0;
    settings[settings_len++] = (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_QPACK_MAX_TABLE_CAPACITY, 0};
    settings[settings_len++] = (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_QPACK_BLOCKED_STREAMS, 0};
    settings[settings_len++] = (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_ENABLE_CONNECT_PROTOCOL, 1};
    settings[settings_len++] = (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_H3_DATAGRAM, 1};

    switch (session->draft) {
    case TREV_WT_DRAFT_15:
        settings[settings_len++] = (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_WT_ENABLED_DRAFT15, 1};
        settings[settings_len++] = (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_WT_MAX_SESSIONS, session_limit};
        settings[settings_len++] = (trevrpc_wt_setting_pair){
            TREV_WT_H3_SETTINGS_WT_INITIAL_MAX_STREAMS_UNI, TREV_WT_H3_SETTINGS_MAX_FLOW_CONTROL_VALUE};
        settings[settings_len++] = (trevrpc_wt_setting_pair){
            TREV_WT_H3_SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI, TREV_WT_H3_SETTINGS_MAX_FLOW_CONTROL_VALUE};
        settings[settings_len++] = (trevrpc_wt_setting_pair){
            TREV_WT_H3_SETTINGS_WT_INITIAL_MAX_DATA, TREV_WT_H3_SETTINGS_MAX_FLOW_CONTROL_VALUE};
        break;
    case TREV_WT_DRAFT_14:
        settings[settings_len++] = (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_WT_MAX_SESSIONS, session_limit};
        break;
    case TREV_WT_DRAFT_07:
        settings[settings_len++] =
            (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07, session_limit};
        break;
    case TREV_WT_DRAFT_02:
        settings[settings_len++] = (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_WEBTRANSPORT_DRAFT02, 1};
        settings[settings_len++] = (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_H3_DRAFT04_DATAGRAM, 1};
        break;
    case TREV_WT_DRAFT_NONE:
    default:
        settings[settings_len++] = (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_WT_ENABLED_DRAFT15, 1};
        settings[settings_len++] = (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_WT_MAX_SESSIONS, session_limit};
        settings[settings_len++] = (trevrpc_wt_setting_pair){
            TREV_WT_H3_SETTINGS_WT_INITIAL_MAX_STREAMS_UNI, TREV_WT_H3_SETTINGS_MAX_FLOW_CONTROL_VALUE};
        settings[settings_len++] = (trevrpc_wt_setting_pair){
            TREV_WT_H3_SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI, TREV_WT_H3_SETTINGS_MAX_FLOW_CONTROL_VALUE};
        settings[settings_len++] = (trevrpc_wt_setting_pair){
            TREV_WT_H3_SETTINGS_WT_INITIAL_MAX_DATA, TREV_WT_H3_SETTINGS_MAX_FLOW_CONTROL_VALUE};
        settings[settings_len++] = (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_WEBTRANSPORT_DRAFT02, 1};
        settings[settings_len++] = (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_H3_DRAFT04_DATAGRAM, 1};
        settings[settings_len++] =
            (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_WEBTRANSPORT_MAX_SESSIONS_DRAFT07, session_limit};
        break;
    }
    settings[settings_len++] = (trevrpc_wt_setting_pair){TREV_WT_H3_SETTINGS_MAX_FIELD_SECTION_SIZE, 4096};

    size_t payload_len = 0;
    for (size_t i = 0; i < settings_len; i++) {
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
        for (size_t i = 0; i < settings_len; i++) {
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
            settings->enable_webtransport_draft14 = value != 0;
            break;
        case TREV_WT_H3_SETTINGS_WT_INITIAL_MAX_DATA:
            settings->wt_initial_max_data = value;
            break;
        case TREV_WT_H3_SETTINGS_WT_INITIAL_MAX_STREAMS_UNI:
            settings->wt_initial_max_streams_uni = value;
            break;
        case TREV_WT_H3_SETTINGS_WT_INITIAL_MAX_STREAMS_BIDI:
            settings->wt_initial_max_streams_bidi = value;
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
    trevrpc_wt_session* session, trevrpc_wt_role role, bool require_webtransport, trevrpc_wt_peer_settings* settings) {
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
        if (require_webtransport || session->draft != TREV_WT_DRAFT_NONE) {
            err = trevrpc_wt_validate_settings_for_draft(role, session->draft, settings);
        }
        session->requires_initial_capsule_flow_control = role == TREV_WT_ROLE_SERVER &&
                                                         session->draft == TREV_WT_DRAFT_07 &&
                                                         trevrpc_wt_is_network_framework_legacy_peer(settings);
    }

    session->peer_control = control;
    return err;
}

static int trevrpc_wt_h3_handshake(
    trevrpc_wt_session* session, const trevrpc_wt_config* config, trevrpc_wt_role role, bool require_webtransport) {
    trevrpc_wt_peer_settings peer_settings = {0};
    int err = 0;
    if (role == TREV_WT_ROLE_SERVER) {
        err = trevrpc_wt_read_peer_control_settings(session, role, require_webtransport, &peer_settings);
        if (err == 0) {
            err = trevrpc_wt_write_control_settings(session, config);
        }
        return err;
    }

    err = trevrpc_wt_write_control_settings(session, config);
    if (err == 0) {
        err = trevrpc_wt_read_peer_control_settings(session, role, require_webtransport, &peer_settings);
    }
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
    listener->admission = config->admission;
    listener->admission_user_data = config->admission_user_data;

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
        .admission = listener->admission,
        .admission_user_data = listener->admission_user_data,
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
    int err = trevrpc_wt_h3_handshake(session, config, TREV_WT_ROLE_SERVER, true);
    if (err != 0) {
        trevrpc_wt_session_close(session);
        return err;
    }
    err = trevrpc_wt_accept_connect_stream(session, config);
    if (err != 0) {
        trevrpc_wt_session_close(session);
        return err;
    }
    *out_session = session;
    return 0;
}

static void* trevrpc_h3_control_monitor(void* context) {
    trevrpc_h3_conn* conn = context;
    for (;;) {
        uint64_t frame_type = 0;
        uint64_t frame_len = 0;
        int err = trevrpc_wt_read_varint(conn->session.peer_control, &frame_type);
        if (err == 0) {
            err = trevrpc_wt_read_varint(conn->session.peer_control, &frame_len);
        }
        pthread_mutex_lock(&conn->mutex);
        bool shutting_down = conn->shutting_down;
        pthread_mutex_unlock(&conn->mutex);
        if (err != 0) {
            if (!shutting_down) {
                trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_CLOSED_CRITICAL_STREAM);
            }
            return NULL;
        }
        if (frame_type == TREV_WT_H3_FRAME_SETTINGS || frame_type == TREV_WT_H3_FRAME_DATA ||
            frame_type == TREV_WT_H3_FRAME_HEADERS || frame_type == TREV_WT_H3_FRAME_PUSH_PROMISE) {
            trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_FRAME_UNEXPECTED);
            return NULL;
        }
        if (frame_type == TREV_WT_H3_FRAME_CANCEL_PUSH || frame_type == TREV_WT_H3_FRAME_GOAWAY ||
            frame_type == TREV_WT_H3_FRAME_MAX_PUSH_ID) {
            uint8_t payload[8];
            if (frame_len == 0 || frame_len > sizeof(payload) ||
                trevrpc_wt_read_exact(conn->session.peer_control, payload, (size_t)frame_len) != 0) {
                trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_FRAME_ERROR);
                return NULL;
            }
            size_t offset = 0;
            uint64_t id = 0;
            if (trevrpc_wt_varint_read_buffer(payload, (size_t)frame_len, &offset, &id) != 0 ||
                offset != (size_t)frame_len) {
                trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_FRAME_ERROR);
                return NULL;
            }
            continue;
        }
        if (frame_len > TREV_H3_MAX_UNKNOWN_FRAME_SIZE) {
            trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_EXCESSIVE_LOAD);
            return NULL;
        }
        uint8_t ignored[1024];
        while (frame_len > 0) {
            size_t chunk = frame_len < sizeof(ignored) ? (size_t)frame_len : sizeof(ignored);
            err = trevrpc_wt_read_exact(conn->session.peer_control, ignored, chunk);
            if (err != 0) {
                trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_CLOSED_CRITICAL_STREAM);
                return NULL;
            }
            frame_len -= chunk;
        }
    }
}

static void* trevrpc_h3_unidi_monitor(void* context) {
    trevrpc_h3_unidi_monitor_context* monitor = context;
    trevrpc_h3_conn* conn = monitor->conn;
    trevrpc_msquic_stream* stream = conn->session.peer_unidi_streams[monitor->index];
    free(monitor);
    uint64_t stream_type = 0;
    int err = trevrpc_wt_read_varint(stream, &stream_type);
    if (err != 0) {
        trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_CLOSED_CRITICAL_STREAM);
        return NULL;
    }
    if (stream_type == TREV_WT_H3_STREAM_TYPE_CONTROL) {
        trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_STREAM_CREATION_ERROR);
        return NULL;
    }
    bool critical = stream_type == 0x02 || stream_type == 0x03;
    if (critical) {
        pthread_mutex_lock(&conn->mutex);
        bool* seen = stream_type == 0x02 ? &conn->qpack_encoder_seen : &conn->qpack_decoder_seen;
        bool duplicate = *seen;
        *seen = true;
        pthread_mutex_unlock(&conn->mutex);
        if (duplicate) {
            trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_STREAM_CREATION_ERROR);
            return NULL;
        }
    }
    uint8_t ignored[1024];
    for (;;) {
        intptr_t n = trevrpc_msquic_stream_read(stream, ignored, sizeof(ignored));
        if (n > 0 && stream_type == 0x02) {
            bool valid = true;
            for (intptr_t i = 0; i < n; i++) {
                if (ignored[i] != TREV_H3_QPACK_SET_CAPACITY_ZERO) {
                    valid = false;
                    break;
                }
            }
            if (valid) {
                continue;
            }
        }
        if (n > 0 && critical) {
            trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn,
                stream_type == 0x02 ? TREV_H3_APP_QPACK_ENCODER_STREAM_ERROR : TREV_H3_APP_QPACK_DECODER_STREAM_ERROR);
            return NULL;
        }
        if (n <= 0) {
            pthread_mutex_lock(&conn->mutex);
            bool shutting_down = conn->shutting_down;
            pthread_mutex_unlock(&conn->mutex);
            if (critical && !shutting_down) {
                trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_CLOSED_CRITICAL_STREAM);
            }
            return NULL;
        }
    }
}

static int trevrpc_h3_start_unidi_monitor(trevrpc_h3_conn* conn, trevrpc_msquic_stream* stream) {
    size_t index = 0;
    while (index < sizeof(conn->session.peer_unidi_streams) / sizeof(conn->session.peer_unidi_streams[0]) &&
           conn->session.peer_unidi_streams[index] != NULL) {
        index++;
    }
    if (index == sizeof(conn->session.peer_unidi_streams) / sizeof(conn->session.peer_unidi_streams[0])) {
        trevrpc_msquic_stream_close(stream);
        trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_STREAM_CREATION_ERROR);
        return TREV_WT_ERR_REJECTED;
    }
    trevrpc_h3_unidi_monitor_context* context = malloc(sizeof(*context));
    if (context == NULL) {
        trevrpc_msquic_stream_close(stream);
        return -ENOMEM;
    }
    conn->session.peer_unidi_streams[index] = stream;
    context->conn = conn;
    context->index = index;
    int err = pthread_create(&conn->unidi_threads[index], NULL, trevrpc_h3_unidi_monitor, context);
    if (err != 0) {
        conn->session.peer_unidi_streams[index] = NULL;
        trevrpc_msquic_stream_close(stream);
        free(context);
        return -err;
    }
    conn->unidi_thread_started[index] = true;
    return 0;
}

int trevrpc_h3_accept_from_msquic(trevrpc_msquic_conn* conn,
    const trevrpc_wt_config* webtransport_config,
    int enable_http3,
    const char* http3_path,
    trevrpc_http3_admission http3_admission,
    void* http3_admission_user_data,
    size_t max_frame_size,
    trevrpc_h3_conn** out_conn) {
    if (conn == NULL || webtransport_config == NULL || out_conn == NULL ||
        (enable_http3 && (http3_path == NULL || http3_path[0] != '/'))) {
        return -EINVAL;
    }
    *out_conn = NULL;

    trevrpc_h3_conn* h3_conn = calloc(1, sizeof(*h3_conn));
    if (h3_conn == NULL) {
        trevrpc_msquic_conn_close(conn);
        return -ENOMEM;
    }
    h3_conn->session.msquic_conn = conn;
    pthread_mutex_init(&h3_conn->mutex, NULL);
    pthread_cond_init(&h3_conn->cond, NULL);
    h3_conn->webtransport_path = webtransport_config->path;
    h3_conn->webtransport_origin = webtransport_config->origin;
    h3_conn->webtransport_admission = webtransport_config->admission;
    h3_conn->webtransport_admission_user_data = webtransport_config->admission_user_data;
    h3_conn->enable_http3 = enable_http3 != 0;
    h3_conn->http3_path = http3_path;
    h3_conn->http3_admission = http3_admission;
    h3_conn->http3_admission_user_data = http3_admission_user_data;
    (void)max_frame_size;

    int err = trevrpc_wt_h3_handshake(&h3_conn->session, webtransport_config, TREV_WT_ROLE_SERVER, false);
    if (err != 0) {
        uint64_t error_code =
            err == TREV_WT_ERR_CLOSED ? TREV_H3_APP_CLOSED_CRITICAL_STREAM : TREV_H3_APP_MISSING_SETTINGS;
        trevrpc_msquic_conn_shutdown_error(conn, error_code);
        trevrpc_h3_conn_close(h3_conn);
        return err;
    }
    err = pthread_create(&h3_conn->control_thread, NULL, trevrpc_h3_control_monitor, h3_conn);
    if (err != 0) {
        trevrpc_h3_conn_close(h3_conn);
        return -err;
    }
    h3_conn->control_thread_started = true;
    *out_conn = h3_conn;
    return 0;
}

static int trevrpc_h3_reject_request(trevrpc_h3_stream* stream, unsigned status) {
    trevrpc_msquic_stream* msquic_stream = stream->msquic_stream;
    int err = trevrpc_h3_write_response_headers(msquic_stream, status, true);
    (void)trevrpc_msquic_stream_abort_receive(msquic_stream);
    trevrpc_msquic_stream_close(msquic_stream);
    stream->msquic_stream = NULL;
    stream->owns_msquic_stream = false;
    return err;
}

static int trevrpc_h3_connection_error(trevrpc_h3_stream* stream, int err) {
    pthread_mutex_lock(&stream->conn->mutex);
    bool shutting_down = stream->conn->shutting_down;
    pthread_mutex_unlock(&stream->conn->mutex);
    if (shutting_down) {
        return TREV_WT_ERR_CLOSED;
    }
    uint64_t error_code = TREV_H3_APP_GENERAL_PROTOCOL_ERROR;
    switch (err) {
    case TREV_H3_ERR_QPACK_DECOMPRESSION_FAILED:
        error_code = TREV_H3_APP_QPACK_DECOMPRESSION_FAILED;
        break;
    case TREV_H3_ERR_FRAME_UNEXPECTED:
        error_code = TREV_H3_APP_FRAME_UNEXPECTED;
        break;
    case TREV_H3_ERR_FRAME_ERROR:
        error_code = TREV_H3_APP_FRAME_ERROR;
        break;
    case TREV_H3_ERR_FIELD_SECTION_TOO_LARGE:
        error_code = TREV_H3_APP_EXCESSIVE_LOAD;
        break;
    default:
        break;
    }
    trevrpc_msquic_conn_shutdown_error(stream->conn->session.msquic_conn, error_code);
    return err;
}

static int trevrpc_h3_read_headers_payload_timeout(
    trevrpc_h3_stream* stream, uint64_t frame_len, uint64_t deadline_nanos, trevrpc_wt_headers* headers) {
    if (frame_len > TREV_H3_MAX_ENCODED_FIELD_SECTION_SIZE) {
        return TREV_H3_ERR_FIELD_SECTION_TOO_LARGE;
    }
    uint8_t* block = malloc(frame_len == 0 ? 1 : (size_t)frame_len);
    if (block == NULL) {
        return -ENOMEM;
    }
    size_t offset = 0;
    int err = 0;
    while (offset < (size_t)frame_len) {
        uint64_t now = trevrpc_h3_monotonic_nanos();
        if (now == 0 || now >= deadline_nanos) {
            err = TREV_MSQUIC_ERR_TIMEOUT;
            break;
        }
        intptr_t n = trevrpc_msquic_stream_read_timeout(
            stream->msquic_stream, block + offset, (size_t)frame_len - offset, deadline_nanos - now);
        if (n <= 0) {
            err = n == TREV_MSQUIC_ERR_TIMEOUT ? TREV_MSQUIC_ERR_TIMEOUT : TREV_H3_ERR_FRAME_ERROR;
            break;
        }
        offset += (size_t)n;
    }
    if (err == 0) {
        err = trevrpc_wt_header_block_decode(block, (size_t)frame_len, headers);
    }
    free(block);
    return err;
}

static int trevrpc_h3_accept_webtransport_connect(
    trevrpc_h3_conn* conn, trevrpc_h3_stream* stream, const trevrpc_wt_headers* headers) {
    pthread_mutex_lock(&conn->mutex);
    bool unavailable = conn->webtransport_connected || conn->webtransport_resolving || conn->shutting_down ||
                       conn->session.draft == TREV_WT_DRAFT_NONE;
    if (!unavailable) {
        conn->webtransport_resolving = true;
    }
    pthread_mutex_unlock(&conn->mutex);
    if (unavailable) {
        (void)trevrpc_h3_reject_request(stream, 400);
        return 0;
    }
    trevrpc_wt_path_origin_policy policy = {
        .path = conn->webtransport_path,
        .origin = conn->webtransport_origin,
    };
    trevrpc_wt_accept_connect_context context = {
        .admission =
            conn->webtransport_admission != NULL ? conn->webtransport_admission : trevrpc_wt_path_origin_admission,
        .admission_user_data = conn->webtransport_admission != NULL ? conn->webtransport_admission_user_data : &policy,
        .draft = conn->session.draft,
    };
    int err = trevrpc_wt_validate_connect_request(headers, &context);
    if (err != 0) {
        (void)trevrpc_h3_reject_request(stream, 403);
        pthread_mutex_lock(&conn->mutex);
        conn->webtransport_resolving = false;
        pthread_cond_broadcast(&conn->cond);
        pthread_mutex_unlock(&conn->mutex);
        return 0;
    }
    err = trevrpc_wt_capture_connect_stream_id(&conn->session, stream->msquic_stream);
    if (err == 0) {
        uint8_t block[64];
        size_t offset = 0;
        block[offset++] = 0;
        block[offset++] = 0;
        err = trevrpc_wt_qpack_put_indexed_static(block, sizeof(block), &offset, 25);
        if (err == 0 && conn->session.draft == TREV_WT_DRAFT_02) {
            err =
                trevrpc_wt_qpack_put_literal(block, sizeof(block), &offset, "sec-webtransport-http3-draft", "draft02");
        }
        if (err == 0) {
            err = trevrpc_wt_write_headers_frame(stream->msquic_stream, block, offset);
        }
        if (err == 0) {
            err = trevrpc_wt_write_initial_capsule_flow_control(&conn->session, stream->msquic_stream);
        }
    }
    if (err != 0) {
        trevrpc_msquic_stream_close(stream->msquic_stream);
        stream->msquic_stream = NULL;
        stream->owns_msquic_stream = false;
        pthread_mutex_lock(&conn->mutex);
        conn->webtransport_resolving = false;
        pthread_cond_broadcast(&conn->cond);
        pthread_mutex_unlock(&conn->mutex);
        return err;
    }
    pthread_mutex_lock(&conn->mutex);
    conn->session.connect_stream = stream->msquic_stream;
    conn->webtransport_connected = true;
    conn->webtransport_resolving = false;
    pthread_cond_broadcast(&conn->cond);
    pthread_mutex_unlock(&conn->mutex);
    stream->msquic_stream = NULL;
    stream->owns_msquic_stream = false;
    return 0;
}

static unsigned trevrpc_h3_validate_post_request(const trevrpc_h3_conn* conn, const trevrpc_wt_headers* headers) {
    if (!headers->method_seen || !headers->scheme_seen || !headers->scheme_https || headers->path == NULL ||
        headers->authority == NULL || headers->authority_len == 0 || headers->status_seen || headers->protocol_seen) {
        return 400;
    }
    if (!headers->method_post) {
        return 405;
    }
    if (!conn->enable_http3 || strlen(conn->http3_path) != headers->path_len ||
        memcmp(conn->http3_path, headers->path, headers->path_len) != 0) {
        return 404;
    }
    if (!headers->content_type_seen || !headers->content_type_trevrpc) {
        return 415;
    }
    if (conn->http3_admission != NULL) {
        trevrpc_http3_admission_request request = {
            .path = (const char*)headers->path,
            .path_len = headers->path_len,
            .authority = (const char*)headers->authority,
            .authority_len = headers->authority_len,
            .secure = 1,
        };
        if (conn->http3_admission(conn->http3_admission_user_data, &request) != 0) {
            return 403;
        }
    }
    return 200;
}

static trevrpc_h3_stream* trevrpc_h3_stream_alloc(trevrpc_h3_conn* conn, trevrpc_msquic_stream* msquic_stream) {
    trevrpc_h3_stream* stream = calloc(1, sizeof(*stream));
    if (stream == NULL) {
        return NULL;
    }
    stream->msquic_stream = msquic_stream;
    stream->conn = conn;
    stream->owns_msquic_stream = true;
    return stream;
}

int trevrpc_h3_conn_accept_stream(trevrpc_h3_conn* conn, trevrpc_h3_stream** out_stream) {
    if (conn == NULL || out_stream == NULL) {
        return -EINVAL;
    }
    *out_stream = NULL;
    for (;;) {
        trevrpc_msquic_stream* msquic_stream = NULL;
        int err = trevrpc_msquic_conn_accept_stream(conn->session.msquic_conn, &msquic_stream);
        if (err != 0) {
            return trevrpc_wt_map_msquic_error(err);
        }
        uint64_t stream_id = 0;
        err = trevrpc_msquic_stream_id(msquic_stream, &stream_id);
        if (err != 0) {
            trevrpc_msquic_stream_close(msquic_stream);
            return trevrpc_wt_map_msquic_error(err);
        }
        if (trevrpc_wt_peer_stream_id_is_unidirectional(stream_id)) {
            err = trevrpc_h3_start_unidi_monitor(conn, msquic_stream);
            if (err != 0) {
                return err;
            }
            continue;
        }
        trevrpc_h3_stream* stream = trevrpc_h3_stream_alloc(conn, msquic_stream);
        if (stream == NULL) {
            trevrpc_msquic_stream_close(msquic_stream);
            return -ENOMEM;
        }
        *out_stream = stream;
        return 0;
    }
}

static int trevrpc_h3_wait_for_webtransport(trevrpc_h3_conn* conn, uint64_t session_id, uint64_t deadline_nanos) {
    pthread_mutex_lock(&conn->mutex);
    while (!conn->webtransport_connected && !conn->shutting_down) {
        uint64_t now = trevrpc_h3_monotonic_nanos();
        if (now == 0 || now >= deadline_nanos) {
            pthread_mutex_unlock(&conn->mutex);
            return TREV_MSQUIC_ERR_TIMEOUT;
        }
        uint64_t remaining = deadline_nanos - now;
        struct timespec realtime = {0};
        if (clock_gettime(CLOCK_REALTIME, &realtime) != 0) {
            pthread_mutex_unlock(&conn->mutex);
            return -errno;
        }
        realtime.tv_sec += (time_t)(remaining / 1000000000ull);
        realtime.tv_nsec += (long)(remaining % 1000000000ull);
        if (realtime.tv_nsec >= 1000000000l) {
            realtime.tv_sec++;
            realtime.tv_nsec -= 1000000000l;
        }
        int err = pthread_cond_timedwait(&conn->cond, &conn->mutex, &realtime);
        if (err == ETIMEDOUT) {
            pthread_mutex_unlock(&conn->mutex);
            return TREV_MSQUIC_ERR_TIMEOUT;
        }
        if (err != 0) {
            pthread_mutex_unlock(&conn->mutex);
            return -err;
        }
    }
    bool matched = conn->webtransport_connected && session_id == conn->session.connect_stream_id &&
                   trevrpc_wt_valid_session_id(session_id);
    pthread_mutex_unlock(&conn->mutex);
    return matched ? 0 : TREV_WT_ERR_REJECTED;
}

int trevrpc_h3_stream_resolve(trevrpc_h3_conn* conn,
    trevrpc_h3_stream* stream,
    uint64_t timeout_nanos,
    trevrpc_wt_stream** out_wt_stream,
    int* resolution) {
    if (conn == NULL || stream == NULL || stream->conn != conn || out_wt_stream == NULL || resolution == NULL) {
        return -EINVAL;
    }
    *out_wt_stream = NULL;
    *resolution = TREV_H3_STREAM_RESOLVED_HANDLED;
    uint64_t now = trevrpc_h3_monotonic_nanos();
    if (now == 0 || timeout_nanos == 0 || timeout_nanos > UINT64_MAX - now) {
        (void)trevrpc_h3_reject_request(stream, TREV_H3_REQUEST_TIMEOUT_STATUS);
        return 0;
    }
    uint64_t deadline = now + timeout_nanos;
    uint64_t first = 0;
    intptr_t ready = trevrpc_h3_read_varint_incremental(stream, &first, TREV_H3_READ_DEADLINE, deadline);
    if (ready == TREV_MSQUIC_ERR_TIMEOUT) {
        (void)trevrpc_h3_reject_request(stream, TREV_H3_REQUEST_TIMEOUT_STATUS);
        return 0;
    }
    if (ready <= 0) {
        return ready == 0 || ready == TREV_WT_ERR_CLOSED ? trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR)
                                                         : (int)ready;
    }

    if (first == TREV_WT_STREAM_TYPE_BIDI) {
        uint64_t session_id = 0;
        ready = trevrpc_h3_read_varint_incremental(stream, &session_id, TREV_H3_READ_DEADLINE, deadline);
        if (ready == TREV_MSQUIC_ERR_TIMEOUT) {
            (void)trevrpc_h3_reject_request(stream, TREV_H3_REQUEST_TIMEOUT_STATUS);
            return 0;
        }
        if (ready <= 0) {
            return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
        }
        int err = trevrpc_h3_wait_for_webtransport(conn, session_id, deadline);
        if (err != 0) {
            trevrpc_msquic_stream_close(stream->msquic_stream);
            stream->msquic_stream = NULL;
            stream->owns_msquic_stream = false;
            return 0;
        }
        trevrpc_wt_stream* wt_stream = trevrpc_wt_stream_alloc(stream->msquic_stream);
        if (wt_stream == NULL) {
            return -ENOMEM;
        }
        stream->msquic_stream = NULL;
        stream->owns_msquic_stream = false;
        *out_wt_stream = wt_stream;
        *resolution = TREV_H3_STREAM_RESOLVED_WEBTRANSPORT;
        return 0;
    }

    if (first != TREV_WT_H3_FRAME_HEADERS) {
        return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_UNEXPECTED);
    }
    uint64_t frame_len = 0;
    ready = trevrpc_h3_read_varint_incremental(stream, &frame_len, TREV_H3_READ_DEADLINE, deadline);
    if (ready == TREV_MSQUIC_ERR_TIMEOUT) {
        (void)trevrpc_h3_reject_request(stream, TREV_H3_REQUEST_TIMEOUT_STATUS);
        return 0;
    }
    if (ready <= 0) {
        return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
    }
    trevrpc_wt_headers headers = {0};
    int err = trevrpc_h3_read_headers_payload_timeout(stream, frame_len, deadline, &headers);
    if (err == TREV_MSQUIC_ERR_TIMEOUT) {
        trevrpc_wt_headers_cleanup(&headers);
        (void)trevrpc_h3_reject_request(stream, TREV_H3_REQUEST_TIMEOUT_STATUS);
        return 0;
    }
    if (err == TREV_H3_ERR_QPACK_DECOMPRESSION_FAILED) {
        trevrpc_wt_headers_cleanup(&headers);
        return trevrpc_h3_connection_error(stream, err);
    }
    if (err == TREV_H3_ERR_FIELD_SECTION_TOO_LARGE) {
        trevrpc_wt_headers_cleanup(&headers);
        (void)trevrpc_h3_reject_request(stream, 431);
        return 0;
    }
    if (err != 0) {
        trevrpc_wt_headers_cleanup(&headers);
        if (err == TREV_H3_ERR_FRAME_ERROR) {
            return trevrpc_h3_connection_error(stream, err);
        }
        (void)trevrpc_h3_reject_request(stream, 400);
        return 0;
    }

    bool connect_request =
        headers.method_connect && (headers.protocol_webtransport || headers.protocol_webtransport_h3);
    if (connect_request) {
        err = trevrpc_h3_accept_webtransport_connect(conn, stream, &headers);
        trevrpc_wt_headers_cleanup(&headers);
        return err;
    }
    unsigned status = trevrpc_h3_validate_post_request(conn, &headers);
    trevrpc_wt_headers_cleanup(&headers);
    if (status != 200) {
        (void)trevrpc_h3_reject_request(stream, status);
        return 0;
    }
    err = trevrpc_h3_write_response_headers(stream->msquic_stream, 200, false);
    if (err != 0) {
        return err;
    }
    *resolution = TREV_H3_STREAM_RESOLVED_HTTP3;
    return 0;
}

void trevrpc_h3_conn_shutdown(trevrpc_h3_conn* conn) {
    if (conn != NULL) {
        pthread_mutex_lock(&conn->mutex);
        conn->shutting_down = true;
        pthread_cond_broadcast(&conn->cond);
        pthread_mutex_unlock(&conn->mutex);
        trevrpc_msquic_conn_shutdown(conn->session.msquic_conn);
    }
}

void trevrpc_h3_conn_close(trevrpc_h3_conn* conn) {
    if (conn == NULL) {
        return;
    }
    trevrpc_h3_conn_shutdown(conn);
    if (conn->control_thread_started && !pthread_equal(pthread_self(), conn->control_thread)) {
        (void)pthread_join(conn->control_thread, NULL);
    }
    for (size_t i = 0; i < sizeof(conn->unidi_threads) / sizeof(conn->unidi_threads[0]); i++) {
        if (conn->unidi_thread_started[i] && !pthread_equal(pthread_self(), conn->unidi_threads[i])) {
            (void)pthread_join(conn->unidi_threads[i], NULL);
        }
    }
    trevrpc_msquic_stream_close(conn->session.connect_stream);
    for (size_t i = 0; i < sizeof(conn->session.peer_unidi_streams) / sizeof(conn->session.peer_unidi_streams[0]);
        i++) {
        trevrpc_msquic_stream_close(conn->session.peer_unidi_streams[i]);
    }
    trevrpc_msquic_stream_close(conn->session.peer_control);
    trevrpc_msquic_stream_close(conn->session.local_control);
    trevrpc_msquic_conn_close(conn->session.msquic_conn);
    pthread_cond_destroy(&conn->cond);
    pthread_mutex_destroy(&conn->mutex);
    free(conn);
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

    err = trevrpc_wt_h3_handshake(session, config, TREV_WT_ROLE_CLIENT, true);
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

intptr_t trevrpc_wt_stream_read_frame_owned(trevrpc_wt_stream* stream, trevrpc_owned_bytes* body, size_t max_len) {
    if (stream == NULL || body == NULL) {
        return -EINVAL;
    }
    trevrpc_owned_bytes_init(body);
    if (stream->msquic_stream == NULL) {
        return TREV_WT_ERR_CLOSED;
    }
    intptr_t n = trevrpc_msquic_stream_read_frame_owned(stream->msquic_stream, body, max_len);
    return n < 0 ? trevrpc_wt_map_msquic_error((int)n) : n;
}

intptr_t trevrpc_wt_stream_read_frame_owned_timeout(
    trevrpc_wt_stream* stream, trevrpc_owned_bytes* body, size_t max_len, uint64_t timeout_nanos) {
    if (stream == NULL || body == NULL) {
        return -EINVAL;
    }
    trevrpc_owned_bytes_init(body);
    if (stream->msquic_stream == NULL) {
        return TREV_WT_ERR_CLOSED;
    }
    intptr_t n = trevrpc_msquic_stream_read_frame_owned_timeout(stream->msquic_stream, body, max_len, timeout_nanos);
    return n < 0 ? trevrpc_wt_map_msquic_error((int)n) : n;
}

intptr_t trevrpc_wt_stream_read_frame_timeout(
    trevrpc_wt_stream* stream, uint8_t** body, size_t* len, size_t max_len, uint64_t timeout_nanos) {
    if (stream == NULL || body == NULL || len == NULL) {
        return -EINVAL;
    }
    *body = NULL;
    *len = 0;
    if (stream->msquic_stream == NULL) {
        return TREV_WT_ERR_CLOSED;
    }
    intptr_t n = trevrpc_msquic_stream_read_frame_timeout(stream->msquic_stream, body, len, max_len, timeout_nanos);
    return n < 0 ? trevrpc_wt_map_msquic_error((int)n) : n;
}

intptr_t trevrpc_wt_stream_read_frame_owned_ready(
    trevrpc_wt_stream* stream, trevrpc_owned_bytes* body, size_t max_len) {
    if (stream == NULL || body == NULL) {
        return -EINVAL;
    }
    trevrpc_owned_bytes_init(body);
    if (stream->msquic_stream == NULL) {
        return TREV_WT_ERR_CLOSED;
    }
    intptr_t n = trevrpc_msquic_stream_read_frame_owned_ready(stream->msquic_stream, body, max_len);
    return n < 0 ? trevrpc_wt_map_msquic_error((int)n) : n;
}

intptr_t trevrpc_wt_stream_read_frame_ready(trevrpc_wt_stream* stream, uint8_t** body, size_t* len, size_t max_len) {
    if (stream == NULL || body == NULL || len == NULL) {
        return -EINVAL;
    }
    *body = NULL;
    *len = 0;
    if (stream->msquic_stream == NULL) {
        return TREV_WT_ERR_CLOSED;
    }
    intptr_t n = trevrpc_msquic_stream_read_frame_ready(stream->msquic_stream, body, len, max_len);
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

intptr_t trevrpc_wt_stream_write_message_frame(
    trevrpc_wt_stream* stream, const uint8_t* body, size_t body_len, size_t max_len) {
    if (stream == NULL) {
        return -EINVAL;
    }
    if (stream->msquic_stream == NULL) {
        return TREV_WT_ERR_CLOSED;
    }
    intptr_t n = trevrpc_msquic_stream_write_message_frame(stream->msquic_stream, body, body_len, max_len);
    return n < 0 ? trevrpc_wt_map_msquic_error((int)n) : n;
}

intptr_t trevrpc_wt_stream_write_message_frames(
    trevrpc_wt_stream* stream, const uint8_t* bodies, const size_t* body_lens, size_t count, size_t max_len) {
    if (stream == NULL) {
        return -EINVAL;
    }
    if (stream->msquic_stream == NULL) {
        return TREV_WT_ERR_CLOSED;
    }
    intptr_t n = trevrpc_msquic_stream_write_message_frames(stream->msquic_stream, bodies, body_lens, count, max_len);
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
    if (stream == NULL) {
        return -EINVAL;
    }
    if (stream->msquic_stream == NULL) {
        return TREV_WT_ERR_CLOSED;
    }
    return trevrpc_wt_map_msquic_error(trevrpc_msquic_stream_abort_with_error(stream->msquic_stream, error_code));
}

int trevrpc_wt_stream_abort_receive(trevrpc_wt_stream* stream) {
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

static uint64_t trevrpc_h3_monotonic_nanos(void) {
    struct timespec now = {0};
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        return 0;
    }
    return (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec;
}

static intptr_t trevrpc_h3_read_msquic(
    trevrpc_h3_stream* stream, uint8_t* data, size_t len, trevrpc_h3_read_mode mode, uint64_t deadline_nanos) {
    intptr_t result = 0;
    if (mode == TREV_H3_READ_READY) {
        result = trevrpc_msquic_stream_read_ready(stream->msquic_stream, data, len);
    } else if (mode == TREV_H3_READ_DEADLINE) {
        uint64_t now = trevrpc_h3_monotonic_nanos();
        if (now == 0 || now >= deadline_nanos) {
            return TREV_MSQUIC_ERR_TIMEOUT;
        }
        result = trevrpc_msquic_stream_read_timeout(stream->msquic_stream, data, len, deadline_nanos - now);
    } else {
        result = trevrpc_msquic_stream_read(stream->msquic_stream, data, len);
    }
    return result < 0 ? trevrpc_wt_map_msquic_error((int)result) : result;
}

static intptr_t trevrpc_h3_read_varint_incremental(
    trevrpc_h3_stream* stream, uint64_t* value, trevrpc_h3_read_mode mode, uint64_t deadline_nanos) {
    while (stream->varint_need == 0 || stream->varint_len < stream->varint_need) {
        intptr_t n = trevrpc_h3_read_msquic(stream, stream->varint + stream->varint_len, 1, mode, deadline_nanos);
        if (n <= 0) {
            return n == 0 && stream->varint_len > 0 ? TREV_WT_ERR_CLOSED : n;
        }
        stream->varint_len++;
        if (stream->varint_need == 0) {
            stream->varint_need = (size_t)1 << (stream->varint[0] >> 6);
        }
    }

    uint64_t decoded = stream->varint[0] & 0x3f;
    for (size_t i = 1; i < stream->varint_need; i++) {
        decoded = (decoded << 8) | stream->varint[i];
    }
    stream->varint_len = 0;
    stream->varint_need = 0;
    *value = decoded;
    return 1;
}

static int trevrpc_h3_validate_trailers(trevrpc_h3_stream* stream) {
    trevrpc_wt_headers headers = {0};
    int err = trevrpc_wt_header_block_decode(stream->trailer_block, stream->trailer_len, &headers);
    if (err == 0 && headers.saw_pseudo_header) {
        err = TREV_H3_ERR_MESSAGE_ERROR;
    }
    trevrpc_wt_headers_cleanup(&headers);
    free(stream->trailer_block);
    stream->trailer_block = NULL;
    stream->trailer_len = 0;
    stream->trailer_offset = 0;
    return err;
}

static intptr_t trevrpc_h3_read_data(
    trevrpc_h3_stream* stream, uint8_t* data, size_t len, trevrpc_h3_read_mode mode, uint64_t deadline_nanos) {
    for (;;) {
        if (stream->data_remaining > 0) {
            size_t requested = stream->data_remaining < len ? (size_t)stream->data_remaining : len;
            intptr_t n = trevrpc_h3_read_msquic(stream, data, requested, mode, deadline_nanos);
            if (n == 0) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
            }
            if (n < 0) {
                return n;
            }
            stream->data_remaining -= (uint64_t)n;
            return n;
        }
        if (stream->trailer_block != NULL) {
            size_t remaining = stream->trailer_len - stream->trailer_offset;
            intptr_t n = trevrpc_h3_read_msquic(
                stream, stream->trailer_block + stream->trailer_offset, remaining, mode, deadline_nanos);
            if (n == 0) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
            }
            if (n < 0) {
                return n;
            }
            stream->trailer_offset += (size_t)n;
            if (stream->trailer_offset == stream->trailer_len) {
                int err = trevrpc_h3_validate_trailers(stream);
                if (err != 0) {
                    if (err == TREV_H3_ERR_QPACK_DECOMPRESSION_FAILED || err == TREV_H3_ERR_FIELD_SECTION_TOO_LARGE) {
                        return trevrpc_h3_connection_error(stream, err);
                    }
                    return err;
                }
            }
            continue;
        }
        if (stream->skip_remaining > 0) {
            uint8_t ignored[1024];
            size_t requested =
                stream->skip_remaining < sizeof(ignored) ? (size_t)stream->skip_remaining : sizeof(ignored);
            intptr_t n = trevrpc_h3_read_msquic(stream, ignored, requested, mode, deadline_nanos);
            if (n == 0) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
            }
            if (n < 0) {
                return n;
            }
            stream->skip_remaining -= (uint64_t)n;
            continue;
        }

        if (!stream->have_frame_type) {
            intptr_t ready = trevrpc_h3_read_varint_incremental(stream, &stream->frame_type, mode, deadline_nanos);
            if (ready <= 0) {
                if (ready == 0 && stream->varint_len == 0) {
                    return 0;
                }
                if (ready == TREV_WT_ERR_CLOSED) {
                    return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
                }
                return ready;
            }
            stream->have_frame_type = true;
        }
        uint64_t frame_len = 0;
        intptr_t ready = trevrpc_h3_read_varint_incremental(stream, &frame_len, mode, deadline_nanos);
        if (ready <= 0) {
            if (ready == 0 || ready == TREV_WT_ERR_CLOSED) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
            }
            return ready;
        }
        stream->have_frame_type = false;

        switch (stream->frame_type) {
        case TREV_WT_H3_FRAME_DATA:
            if (stream->trailers_seen) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_UNEXPECTED);
            }
            stream->data_remaining = frame_len;
            break;
        case TREV_WT_H3_FRAME_HEADERS:
            if (stream->trailers_seen) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_UNEXPECTED);
            }
            if (frame_len > TREV_H3_MAX_ENCODED_FIELD_SECTION_SIZE) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FIELD_SECTION_TOO_LARGE);
            }
            stream->trailers_seen = true;
            stream->trailer_len = (size_t)frame_len;
            stream->trailer_block = malloc(stream->trailer_len == 0 ? 1 : stream->trailer_len);
            if (stream->trailer_block == NULL) {
                return -ENOMEM;
            }
            if (stream->trailer_len == 0) {
                int err = trevrpc_h3_validate_trailers(stream);
                if (err != 0) {
                    return err;
                }
            }
            break;
        case TREV_WT_H3_FRAME_SETTINGS:
        case TREV_WT_H3_FRAME_CANCEL_PUSH:
        case TREV_WT_H3_FRAME_PUSH_PROMISE:
        case TREV_WT_H3_FRAME_GOAWAY:
        case TREV_WT_H3_FRAME_MAX_PUSH_ID:
            return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_UNEXPECTED);
        default:
            if (frame_len > TREV_H3_MAX_UNKNOWN_FRAME_SIZE) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FIELD_SECTION_TOO_LARGE);
            }
            stream->skip_remaining = frame_len;
            break;
        }
    }
}

static intptr_t trevrpc_h3_stream_read_frame_owned_mode(trevrpc_h3_stream* stream,
    trevrpc_owned_bytes* body,
    size_t max_len,
    trevrpc_h3_read_mode mode,
    uint64_t timeout_nanos) {
    if (stream == NULL || body == NULL) {
        return -EINVAL;
    }
    trevrpc_owned_bytes_init(body);
    uint64_t deadline = 0;
    if (mode == TREV_H3_READ_DEADLINE) {
        uint64_t now = trevrpc_h3_monotonic_nanos();
        if (now == 0 || timeout_nanos > UINT64_MAX - now) {
            return -EOVERFLOW;
        }
        deadline = now + timeout_nanos;
    }
    if (!stream->rpc_parser_initialized) {
        trevrpc_frame_parser_init(&stream->rpc_parser, max_len);
        stream->rpc_parser_initialized = true;
    } else {
        trevrpc_frame_parser_set_max_body_len(&stream->rpc_parser, max_len);
    }

    uint8_t buffer[4096];
    for (;;) {
        size_t requested = sizeof(buffer);
        if (stream->rpc_parser.skip_remaining > 0 && stream->rpc_parser.skip_remaining < requested) {
            requested = stream->rpc_parser.skip_remaining;
        } else if (stream->rpc_parser.header_len < sizeof(stream->rpc_parser.header)) {
            requested = sizeof(stream->rpc_parser.header) - stream->rpc_parser.header_len;
        } else if (stream->rpc_parser.body != NULL) {
            size_t body_remaining = stream->rpc_parser.body_len - stream->rpc_parser.body_offset;
            if (body_remaining < requested) {
                requested = body_remaining;
            }
        }

        intptr_t n = trevrpc_h3_read_data(stream, buffer, requested, mode, deadline);
        if (n == 0) {
            return trevrpc_frame_parser_finish(&stream->rpc_parser) == TREVRPC_FRAME_CLEAN_EOF ? 0 : TREV_WT_ERR_CLOSED;
        }
        if (n < 0) {
            return n;
        }

        size_t offset = 0;
        while (offset < (size_t)n) {
            size_t consumed = 0;
            size_t declared_body_len = 0;
            trevrpc_frame_result result = trevrpc_frame_parser_consume_owned(
                &stream->rpc_parser, buffer + offset, (size_t)n - offset, &consumed, body, &declared_body_len);
            (void)declared_body_len;
            offset += consumed;
            if (result == TREVRPC_FRAME_READY) {
                return 1;
            }
            if (result == TREVRPC_FRAME_TOO_LARGE) {
                return TREV_WT_ERR_FRAME_TOO_LARGE;
            }
            if (result == TREVRPC_FRAME_ALLOCATION_FAILURE) {
                return -ENOMEM;
            }
            if (result != TREVRPC_FRAME_NEED_MORE) {
                return TREV_WT_ERR_CLOSED;
            }
            if (consumed == 0) {
                return TREV_WT_ERR_CLOSED;
            }
        }
    }
}

intptr_t trevrpc_h3_stream_read_frame_owned(trevrpc_h3_stream* stream, trevrpc_owned_bytes* body, size_t max_len) {
    return trevrpc_h3_stream_read_frame_owned_mode(stream, body, max_len, TREV_H3_READ_BLOCK, 0);
}

intptr_t trevrpc_h3_stream_read_frame_owned_timeout(
    trevrpc_h3_stream* stream, trevrpc_owned_bytes* body, size_t max_len, uint64_t timeout_nanos) {
    return timeout_nanos == 0
               ? trevrpc_h3_stream_read_frame_owned(stream, body, max_len)
               : trevrpc_h3_stream_read_frame_owned_mode(stream, body, max_len, TREV_H3_READ_DEADLINE, timeout_nanos);
}

intptr_t trevrpc_h3_stream_read_frame_owned_ready(
    trevrpc_h3_stream* stream, trevrpc_owned_bytes* body, size_t max_len) {
    return trevrpc_h3_stream_read_frame_owned_mode(stream, body, max_len, TREV_H3_READ_READY, 0);
}

static intptr_t trevrpc_h3_stream_read_frame_mode(trevrpc_h3_stream* stream,
    uint8_t** body,
    size_t* len,
    size_t max_len,
    trevrpc_h3_read_mode mode,
    uint64_t timeout_nanos) {
    if (body == NULL || len == NULL) {
        return -EINVAL;
    }
    *body = NULL;
    *len = 0;
    trevrpc_owned_bytes owned;
    intptr_t result = trevrpc_h3_stream_read_frame_owned_mode(stream, &owned, max_len, mode, timeout_nanos);
    if (result <= 0) {
        return result;
    }
    *len = owned.len;
    if (owned.owner == (void*)owned.data && owned.release == trevrpc_frame_default_free &&
        owned.release_context == NULL) {
        *body = (uint8_t*)owned.data;
        trevrpc_owned_bytes_init(&owned);
        return result;
    }
    if (owned.len > 0) {
        *body = malloc(owned.len);
        if (*body == NULL) {
            trevrpc_owned_bytes_reset(&owned);
            return -ENOMEM;
        }
        memcpy(*body, owned.data, owned.len);
    }
    trevrpc_owned_bytes_reset(&owned);
    return result;
}

intptr_t trevrpc_h3_stream_read_frame(trevrpc_h3_stream* stream, uint8_t** body, size_t* len, size_t max_len) {
    return trevrpc_h3_stream_read_frame_mode(stream, body, len, max_len, TREV_H3_READ_BLOCK, 0);
}

intptr_t trevrpc_h3_stream_read_frame_timeout(
    trevrpc_h3_stream* stream, uint8_t** body, size_t* len, size_t max_len, uint64_t timeout_nanos) {
    return timeout_nanos == 0
               ? trevrpc_h3_stream_read_frame(stream, body, len, max_len)
               : trevrpc_h3_stream_read_frame_mode(stream, body, len, max_len, TREV_H3_READ_DEADLINE, timeout_nanos);
}

intptr_t trevrpc_h3_stream_read_frame_ready(trevrpc_h3_stream* stream, uint8_t** body, size_t* len, size_t max_len) {
    return trevrpc_h3_stream_read_frame_mode(stream, body, len, max_len, TREV_H3_READ_READY, 0);
}

static intptr_t trevrpc_h3_stream_write_data(
    trevrpc_h3_stream* stream, const uint8_t* data, size_t len, bool finish_send) {
    if (stream == NULL || stream->msquic_stream == NULL || (data == NULL && len > 0)) {
        return -EINVAL;
    }
    uint8_t prefix[16];
    size_t prefix_len = 0;
    size_t written = 0;
    int err = trevrpc_wt_varint_write(prefix, sizeof(prefix), TREV_WT_H3_FRAME_DATA, &written);
    if (err == 0) {
        prefix_len += written;
        err = trevrpc_wt_varint_write(prefix + prefix_len, sizeof(prefix) - prefix_len, len, &written);
        prefix_len += err == 0 ? written : 0;
    }
    if (err != 0 || len > SIZE_MAX - prefix_len) {
        return err != 0 ? err : -EOVERFLOW;
    }
    uint8_t* frame = malloc(prefix_len + len);
    if (frame == NULL) {
        return -ENOMEM;
    }
    memcpy(frame, prefix, prefix_len);
    if (len > 0) {
        memcpy(frame + prefix_len, data, len);
    }
    intptr_t n = finish_send ? trevrpc_msquic_stream_write_fin(stream->msquic_stream, frame, prefix_len + len)
                             : trevrpc_msquic_stream_write(stream->msquic_stream, frame, prefix_len + len);
    free(frame);
    if (n < 0) {
        return trevrpc_wt_map_msquic_error((int)n);
    }
    return (size_t)n == prefix_len + len ? (intptr_t)len : TREV_WT_ERR_CLOSED;
}

intptr_t trevrpc_h3_stream_write(trevrpc_h3_stream* stream, const uint8_t* data, size_t len) {
    return trevrpc_h3_stream_write_data(stream, data, len, false);
}

intptr_t trevrpc_h3_stream_write_fin(trevrpc_h3_stream* stream, const uint8_t* data, size_t len) {
    return trevrpc_h3_stream_write_data(stream, data, len, true);
}

int trevrpc_h3_stream_shutdown_send(trevrpc_h3_stream* stream) {
    return stream == NULL || stream->msquic_stream == NULL
               ? -EINVAL
               : trevrpc_wt_map_msquic_error(trevrpc_msquic_stream_shutdown_send(stream->msquic_stream));
}

int trevrpc_h3_stream_abort(trevrpc_h3_stream* stream) {
    return stream == NULL || stream->msquic_stream == NULL
               ? -EINVAL
               : trevrpc_wt_map_msquic_error(trevrpc_msquic_stream_abort(stream->msquic_stream));
}

int trevrpc_h3_stream_abort_receive(trevrpc_h3_stream* stream) {
    return stream == NULL || stream->msquic_stream == NULL
               ? -EINVAL
               : trevrpc_wt_map_msquic_error(trevrpc_msquic_stream_abort_receive(stream->msquic_stream));
}

void trevrpc_h3_stream_close(trevrpc_h3_stream* stream) {
    if (stream != NULL) {
        free(stream->trailer_block);
        if (stream->rpc_parser_initialized) {
            trevrpc_frame_parser_reset(&stream->rpc_parser);
        }
        if (stream->owns_msquic_stream) {
            trevrpc_msquic_stream_close(stream->msquic_stream);
        }
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
