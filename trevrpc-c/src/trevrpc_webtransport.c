#define _POSIX_C_SOURCE 200809L

#include "trevrpc_webtransport.h"

#include "trevrpc.h"
#include "trevrpc_msquic.h"

#include "trevrpc_frame_internal.h"
#include "trevrpc_http3_frame_internal.h"
#include "trevrpc_http3_headers_internal.h"
#include "trevrpc_http3_settings_internal.h"
#include "trevrpc_msquic_internal.h"
#include "trevrpc_qpack_static_internal.h"
#include "trevrpc_quic_varint_internal.h"
#include "trevrpc_webtransport_profile_internal.h"

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
#define TREV_WT_H3_SETTINGS_QPACK_MAX_TABLE_CAPACITY 0x01
#define TREV_WT_H3_SETTINGS_MAX_FIELD_SECTION_SIZE 0x06
#define TREV_WT_H3_SETTINGS_QPACK_BLOCKED_STREAMS 0x07
#define TREV_WT_H3_SETTINGS_ENABLE_CONNECT_PROTOCOL 0x08
#define TREV_WT_H3_SETTINGS_H3_DATAGRAM 0x33
#define TREV_WT_CAPSULE_MAX_DATA 0x190b4d3d
#define TREV_WT_CAPSULE_MAX_STREAMS_BIDI 0x190b4d3f
#define TREV_WT_CAPSULE_MAX_STREAMS_UNI 0x190b4d40
#define TREV_WT_CONNECT_STATUS_OK 200
#define TREV_WT_STREAM_TYPE_BIDI 0x41
#define TREV_WT_STREAM_TYPE_BIDI_LEN 2
#define TREV_H3_MAX_SETTINGS_PAYLOAD_SIZE 4096
#define TREV_H3_MAX_FIELD_SECTION_SIZE 4096
#define TREV_H3_MAX_ENCODED_FIELD_SECTION_SIZE (16 * 1024)
#define TREV_H3_CONTENT_TYPE "application/trevrpc"
#define TREV_H3_REQUEST_TIMEOUT_STATUS 408
#define TREV_H3_QPACK_SET_CAPACITY_ZERO 0x20

#define TREV_H3_ERR_QPACK_DECOMPRESSION_FAILED -3101
#define TREV_H3_ERR_FRAME_UNEXPECTED -3102
#define TREV_H3_ERR_FRAME_ERROR -3103
#define TREV_H3_ERR_FIELD_SECTION_TOO_LARGE -3104
#define TREV_H3_ERR_MESSAGE_ERROR -3105
#define TREV_H3_ERR_ID_ERROR -3106
#define TREV_H3_ERR_EXCESSIVE_LOAD -3107

#define TREV_H3_APP_GENERAL_PROTOCOL_ERROR 0x101
#define TREV_H3_APP_INTERNAL_ERROR 0x102
#define TREV_H3_APP_STREAM_CREATION_ERROR 0x103
#define TREV_H3_APP_CLOSED_CRITICAL_STREAM 0x104
#define TREV_H3_APP_FRAME_UNEXPECTED 0x105
#define TREV_H3_APP_FRAME_ERROR 0x106
#define TREV_H3_APP_EXCESSIVE_LOAD 0x107
#define TREV_H3_APP_ID_ERROR 0x108
#define TREV_H3_APP_SETTINGS_ERROR 0x109
#define TREV_H3_APP_MESSAGE_ERROR 0x10e
#define TREV_H3_APP_MISSING_SETTINGS 0x10a
#define TREV_H3_APP_QPACK_DECOMPRESSION_FAILED 0x200
#define TREV_H3_APP_QPACK_ENCODER_STREAM_ERROR 0x201
#define TREV_H3_APP_QPACK_DECODER_STREAM_ERROR 0x202

typedef enum trevrpc_h3_read_mode {
    TREV_H3_READ_BLOCK = 0,
    TREV_H3_READ_READY = 1,
    TREV_H3_READ_DEADLINE = 2,
} trevrpc_h3_read_mode;

struct trevrpc_wt_listener {
    trevrpc_msquic_listener* msquic_listener;
    trevrpc_wt_config config;
    char* path;
    char* origin;
};

typedef struct trevrpc_h3_frame_policy {
    uint64_t max_data_payload;
    uint64_t max_control_unknown_discard;
    uint64_t max_request_unknown_discard;
    uint64_t max_settings_payload;
    uint64_t max_encoded_field_section;
} trevrpc_h3_frame_policy;

struct trevrpc_wt_session {
    trevrpc_msquic_conn* msquic_conn;
    trevrpc_msquic_stream* local_control;
    trevrpc_msquic_stream* peer_control;
    trevrpc_msquic_stream* peer_unidi_streams[TREV_WT_H3_DEFAULT_UNIDI_STREAMS];
    trevrpc_msquic_stream* connect_stream;
    uint64_t connect_stream_id;
    uint64_t h3_error_code;
    trevrpc_h3_frame_policy frame_policy;
    trevrpc_msquic_feature_snapshot transport_capabilities;
    trevrpc_wt_profile_id draft;
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
    char* webtransport_path;
    char* webtransport_origin;
    trevrpc_webtransport_admission webtransport_admission;
    void* webtransport_admission_user_data;
    char* http3_path;
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
    trevrpc_h3_unknown_discard_budget unknown_discard;
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
    trevrpc_wt_profile_id draft;
} trevrpc_wt_accept_connect_context;

typedef struct trevrpc_wt_path_origin_policy {
    const char* path;
    const char* origin;
} trevrpc_wt_path_origin_policy;

typedef struct trevrpc_wt_connect_response_context {
    trevrpc_wt_profile_id draft;
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

static trevrpc_h3_frame_policy trevrpc_h3_frame_policy_make(size_t max_frame_size) {
    uint64_t normalized_max_frame_size =
        max_frame_size == 0 ? TREVRPC_DEFAULT_MAX_FRAME_SIZE : (uint64_t)max_frame_size;
    return (trevrpc_h3_frame_policy){
        .max_data_payload = normalized_max_frame_size,
        .max_control_unknown_discard = normalized_max_frame_size,
        .max_request_unknown_discard = normalized_max_frame_size,
        .max_settings_payload = TREV_H3_MAX_SETTINGS_PAYLOAD_SIZE,
        .max_encoded_field_section = TREV_H3_MAX_ENCODED_FIELD_SECTION_SIZE,
    };
}

#ifdef TREVRPC_H3_FRAME_POLICY_TESTING
int trevrpc_h3_test_frame_policy(size_t max_frame_size,
    uint64_t* max_data_payload,
    uint64_t* max_control_unknown_discard,
    uint64_t* max_request_unknown_discard,
    uint64_t* max_settings_payload,
    uint64_t* max_encoded_field_section) {
    if (max_data_payload == NULL || max_control_unknown_discard == NULL || max_request_unknown_discard == NULL ||
        max_settings_payload == NULL || max_encoded_field_section == NULL) {
        return -EINVAL;
    }
    trevrpc_h3_frame_policy policy = trevrpc_h3_frame_policy_make(max_frame_size);
    *max_data_payload = policy.max_data_payload;
    *max_control_unknown_discard = policy.max_control_unknown_discard;
    *max_request_unknown_discard = policy.max_request_unknown_discard;
    *max_settings_payload = policy.max_settings_payload;
    *max_encoded_field_section = policy.max_encoded_field_section;
    return 0;
}
#endif

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

static int trevrpc_wt_read_exact(trevrpc_msquic_stream* stream, uint8_t* data, size_t len) {
    size_t offset = 0;
    while (offset < len) {
        intptr_t n = trevrpc_msquic_stream_read_protocol(stream, data + offset, len - offset);
        if (n <= 0) {
            return n == 0 ? TREV_WT_ERR_CLOSED : trevrpc_wt_map_msquic_error((int)n);
        }
        offset += (size_t)n;
    }
    return 0;
}

static int trevrpc_wt_read_varint(trevrpc_msquic_stream* stream, uint64_t* value) {
    uint8_t encoded[8];
    int err = trevrpc_wt_read_exact(stream, encoded, 1);
    if (err != 0) {
        return err;
    }

    size_t size = trevrpc_quic_varint_size_from_first(encoded[0]);
    if (size > 1) {
        err = trevrpc_wt_read_exact(stream, encoded + 1, size - 1);
        if (err != 0) {
            return err;
        }
    }

    size_t offset = 0;
    uint64_t decoded = 0;
    err = trevrpc_quic_varint_read(encoded, size, &offset, &decoded);
    if (err != 0) {
        return TREV_WT_ERR_REJECTED;
    }
    *value = decoded;
    return 0;
}

static int trevrpc_wt_write_varints(trevrpc_msquic_stream* stream, const uint64_t* values, size_t count) {
    uint8_t buffer[64];
    size_t offset = 0;
    for (size_t i = 0; i < count; i++) {
        size_t written = 0;
        int err = trevrpc_quic_varint_write(buffer + offset, sizeof(buffer) - offset, values[i], &written);
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
    return trevrpc_wt_profile_effective_session_limit(config == NULL ? 0 : config->max_sessions_per_connection);
}

static int trevrpc_wt_capture_connect_stream_id(trevrpc_wt_session* session, trevrpc_msquic_stream* stream) {
    uint64_t stream_id = 0;
    int err = trevrpc_msquic_stream_id(stream, &stream_id);
    if (err != 0) {
        return trevrpc_wt_map_msquic_error(err);
    }
    if (!trevrpc_wt_profile_valid_session_id(stream_id)) {
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
        if (trevrpc_wt_profile_peer_stream_is_unidirectional(stream_id)) {
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

static int trevrpc_wt_write_all(trevrpc_msquic_stream* stream, const uint8_t* data, size_t len) {
    intptr_t n = trevrpc_msquic_stream_write(stream, data, len);
    if (n < 0) {
        return trevrpc_wt_map_msquic_error((int)n);
    }
    return n == (intptr_t)len ? 0 : TREV_WT_ERR_CLOSED;
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

static int trevrpc_h3_static_qpack_error(trevrpc_qpack_status status);

static int trevrpc_wt_header_block_decode(
    const uint8_t* data, size_t len, trevrpc_http3_header_block_kind kind, trevrpc_wt_headers* headers) {
    if (headers == NULL) {
        return -EINVAL;
    }

    trevrpc_qpack_field_section section;
    trevrpc_qpack_field_section_init(&section);
    const trevrpc_qpack_limits limits = {
        .max_encoded_size = TREV_H3_MAX_ENCODED_FIELD_SECTION_SIZE,
        .max_decoded_size = TREV_H3_MAX_FIELD_SECTION_SIZE,
        .max_field_count = TREV_H3_MAX_FIELD_SECTION_SIZE / 32u,
    };
    trevrpc_qpack_status qpack_status = trevrpc_qpack_static_decode(data, len, &limits, &section);
    int err = trevrpc_h3_static_qpack_error(qpack_status);
    if (err == 0) {
        trevrpc_http3_headers_report report;
        trevrpc_http3_headers_status headers_status = trevrpc_http3_headers_validate(&section, kind, &report);
        if (headers_status == TREV_HTTP3_HEADERS_MESSAGE_ERROR) {
            err = TREV_H3_ERR_MESSAGE_ERROR;
        } else if (headers_status != TREV_HTTP3_HEADERS_OK) {
            err = -EINVAL;
        }
    }
    for (size_t i = 0; err == 0 && i < section.field_count; i++) {
        const trevrpc_qpack_field* field = &section.fields[i];
        err = trevrpc_wt_headers_apply_field(
            headers, field->name.data, field->name.len, field->value.data, field->value.len);
    }
    trevrpc_qpack_field_section_release(&section);
    return err;
}

static int trevrpc_wt_write_headers_frame_with_fin(
    trevrpc_msquic_stream* stream, const uint8_t* block, size_t block_len, bool finish_send) {
    uint8_t prefix[16];
    size_t offset = 0;
    size_t written = 0;
    int err = trevrpc_quic_varint_write(prefix + offset, sizeof(prefix) - offset, TREV_H3_FRAME_HEADERS, &written);
    if (err != 0) {
        return err;
    }
    offset += written;
    err = trevrpc_quic_varint_write(prefix + offset, sizeof(prefix) - offset, block_len, &written);
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

static int trevrpc_wt_read_headers_frame(trevrpc_msquic_stream* stream,
    trevrpc_http3_header_block_kind kind,
    trevrpc_wt_headers_validate_fn validate,
    void* context) {
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
    if (frame_type != TREV_H3_FRAME_HEADERS || frame_len > TREV_H3_MAX_ENCODED_FIELD_SECTION_SIZE) {
        return TREV_WT_ERR_REJECTED;
    }

    uint8_t* block = malloc((size_t)frame_len);
    if (block == NULL && frame_len > 0) {
        return -ENOMEM;
    }
    trevrpc_wt_headers headers = {0};
    err = trevrpc_wt_read_exact(stream, block, (size_t)frame_len);
    if (err == 0) {
        err = trevrpc_wt_header_block_decode(block, (size_t)frame_len, kind, &headers);
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
    int err = trevrpc_quic_varint_write(value_bytes, sizeof(value_bytes), value, &value_len);
    if (err != 0) {
        return err;
    }

    uint8_t capsule[24];
    size_t capsule_len = 0;
    size_t written = 0;
    err = trevrpc_quic_varint_write(capsule + capsule_len, sizeof(capsule) - capsule_len, type, &written);
    if (err == 0) {
        capsule_len += written;
        err = trevrpc_quic_varint_write(capsule + capsule_len, sizeof(capsule) - capsule_len, value_len, &written);
    }
    if (err == 0) {
        capsule_len += written;
        memcpy(capsule + capsule_len, value_bytes, value_len);
        capsule_len += value_len;
    }

    uint8_t frame[40];
    size_t frame_len = 0;
    if (err == 0) {
        err = trevrpc_quic_varint_write(frame + frame_len, sizeof(frame) - frame_len, TREV_H3_FRAME_DATA, &written);
    }
    if (err == 0) {
        frame_len += written;
        err = trevrpc_quic_varint_write(frame + frame_len, sizeof(frame) - frame_len, capsule_len, &written);
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
        stream, TREV_WT_CAPSULE_MAX_STREAMS_BIDI, TREV_WT_PROFILE_MAX_FLOW_CONTROL_VALUE);
    if (err == 0) {
        err = trevrpc_wt_write_capsule_value_frame(stream, TREV_WT_CAPSULE_MAX_STREAMS_UNI, 0);
    }
    if (err == 0) {
        err = trevrpc_wt_write_capsule_value_frame(
            stream, TREV_WT_CAPSULE_MAX_DATA, TREV_WT_PROFILE_MAX_FLOW_CONTROL_VALUE);
    }
    return err;
}

static int trevrpc_wt_validate_connect_response(const trevrpc_wt_headers* headers, void* context) {
    trevrpc_wt_connect_response_context* response_context = context;
    if (!headers->status_200 || headers->method_seen || headers->protocol_seen || headers->scheme_seen ||
        headers->path != NULL || headers->authority != NULL) {
        return TREV_WT_ERR_REJECTED;
    }
    if (response_context != NULL && trevrpc_wt_profile_requires_draft02_response_marker(response_context->draft) &&
        !headers->draft02_response) {
        return TREV_WT_ERR_REJECTED;
    }
    return 0;
}

static bool trevrpc_wt_connect_protocol_matches(const trevrpc_wt_headers* headers, trevrpc_wt_profile_id profile) {
    return (headers->protocol_webtransport_h3 &&
               trevrpc_wt_profile_accepts_connect_protocol(profile, "webtransport-h3")) ||
           (headers->protocol_webtransport && trevrpc_wt_profile_accepts_connect_protocol(profile, "webtransport"));
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
    if (trevrpc_wt_profile_requires_draft02_request_marker(accept_context->draft) && !headers->draft02_request) {
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
    const char* protocol = trevrpc_wt_profile_connect_protocol(session->draft);
    if (protocol == NULL) {
        trevrpc_msquic_stream_close(stream);
        return TREV_WT_ERR_REJECTED;
    }
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
    if (err == 0 && trevrpc_wt_profile_requires_draft02_request_marker(session->draft)) {
        err = trevrpc_wt_qpack_put_literal(block, sizeof(block), &offset, "sec-webtransport-http3-draft02", "1");
    }
    if (err == 0) {
        err = trevrpc_wt_write_headers_frame(stream, block, offset);
    }
    if (err == 0) {
        trevrpc_wt_connect_response_context context = {
            .draft = session->draft,
        };
        err = trevrpc_wt_read_headers_frame(
            stream, TREV_HTTP3_HEADERS_RESPONSE, trevrpc_wt_validate_connect_response, &context);
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
    err = trevrpc_wt_read_headers_frame(
        stream, TREV_HTTP3_HEADERS_REQUEST, trevrpc_wt_validate_connect_request, &context);

    uint8_t block[64];
    size_t offset = 0;
    block[offset++] = 0;
    block[offset++] = 0;
    if (err == 0) {
        err = trevrpc_wt_qpack_put_indexed_static(block, sizeof(block), &offset, 25);
    }
    if (err == 0 && trevrpc_wt_profile_requires_draft02_response_marker(session->draft)) {
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

    trevrpc_wt_profile_setting_pair settings[13];
    size_t settings_len = 0;
    settings[settings_len++] = (trevrpc_wt_profile_setting_pair){TREV_WT_H3_SETTINGS_QPACK_MAX_TABLE_CAPACITY, 0};
    settings[settings_len++] = (trevrpc_wt_profile_setting_pair){TREV_WT_H3_SETTINGS_QPACK_BLOCKED_STREAMS, 0};
    bool usable_datagrams = trevrpc_msquic_feature_snapshot_usable_datagrams(&session->transport_capabilities);
    if (trevrpc_wt_profile_any_usable(&session->transport_capabilities)) {
        settings[settings_len++] = (trevrpc_wt_profile_setting_pair){TREV_WT_H3_SETTINGS_ENABLE_CONNECT_PROTOCOL, 1};
    }
    if (usable_datagrams) {
        settings[settings_len++] = (trevrpc_wt_profile_setting_pair){TREV_WT_H3_SETTINGS_H3_DATAGRAM, 1};
    }

    trevrpc_wt_profile_advertisement advertisement = session->draft == TREV_WT_PROFILE_NONE
                                                         ? TREV_WT_PROFILE_ADVERTISEMENT_ALL_SUPPORTED
                                                         : TREV_WT_PROFILE_ADVERTISEMENT_SELECTED;
    size_t profile_settings_len = 0;
    err = trevrpc_wt_profile_materialize_settings(advertisement,
        session->draft,
        &session->transport_capabilities,
        (uint32_t)trevrpc_wt_configured_session_limit(config),
        settings + settings_len,
        sizeof(settings) / sizeof(settings[0]) - settings_len,
        &profile_settings_len);
    if (err != 0) {
        trevrpc_msquic_stream_close(control);
        return TREV_WT_ERR_REJECTED;
    }
    settings_len += profile_settings_len;
    settings[settings_len++] = (trevrpc_wt_profile_setting_pair){TREV_WT_H3_SETTINGS_MAX_FIELD_SECTION_SIZE, 4096};

    size_t payload_len = 0;
    for (size_t i = 0; i < settings_len; i++) {
        size_t id_size = 0;
        size_t value_size = 0;
        err = trevrpc_quic_varint_size(settings[i].id, &id_size);
        if (err == 0) {
            err = trevrpc_quic_varint_size(settings[i].value, &value_size);
        }
        if (err != 0 || SIZE_MAX - payload_len < id_size || SIZE_MAX - payload_len - id_size < value_size) {
            trevrpc_msquic_stream_close(control);
            return TREV_WT_ERR_REJECTED;
        }
        payload_len += id_size + value_size;
    }

    uint8_t buffer[256];
    size_t offset = 0;
    size_t written = 0;
    err = trevrpc_quic_varint_write(buffer + offset, sizeof(buffer) - offset, TREV_WT_H3_STREAM_TYPE_CONTROL, &written);
    if (err == 0) {
        offset += written;
        err = trevrpc_quic_varint_write(buffer + offset, sizeof(buffer) - offset, TREV_H3_FRAME_SETTINGS, &written);
    }
    if (err == 0) {
        offset += written;
        err = trevrpc_quic_varint_write(buffer + offset, sizeof(buffer) - offset, payload_len, &written);
    }
    if (err == 0) {
        offset += written;
        for (size_t i = 0; i < settings_len; i++) {
            err = trevrpc_quic_varint_write(buffer + offset, sizeof(buffer) - offset, settings[i].id, &written);
            if (err != 0) {
                break;
            }
            offset += written;
            err = trevrpc_quic_varint_write(buffer + offset, sizeof(buffer) - offset, settings[i].value, &written);
            if (err != 0) {
                break;
            }
            offset += written;
        }
    }
    if (err == 0) {
        err = trevrpc_wt_write_all(control, buffer, offset);
    }
    if (err == 0) {
        err = trevrpc_wt_map_msquic_error(trevrpc_msquic_stream_wait_pending_sends(control));
    }
    session->local_control = control;
    return err;
}

static int trevrpc_wt_apply_peer_setting(void* context, uint64_t id, uint64_t value) {
    trevrpc_wt_peer_settings* settings = context;
    bool recognized = false;
    return trevrpc_wt_profile_apply_setting(settings, id, value, &recognized);
}

static int trevrpc_wt_read_settings_payload(
    trevrpc_wt_session* session, trevrpc_msquic_stream* control, uint64_t len, trevrpc_wt_peer_settings* settings) {
    uint8_t payload[TREV_H3_MAX_SETTINGS_PAYLOAD_SIZE];
    if (len > session->frame_policy.max_settings_payload) {
        session->h3_error_code = TREV_H3_APP_EXCESSIVE_LOAD;
        return TREV_WT_ERR_REJECTED;
    }

    size_t payload_len = (size_t)len;
    int err = trevrpc_wt_read_exact(control, payload, payload_len);
    if (err != 0) {
        session->h3_error_code = TREV_H3_APP_FRAME_ERROR;
        return err;
    }

    size_t seen_ids_capacity = payload_len / 2;
    uint64_t* seen_ids = NULL;
    if (seen_ids_capacity != 0) {
        seen_ids = malloc(seen_ids_capacity * sizeof(*seen_ids));
        if (seen_ids == NULL) {
            session->h3_error_code = TREV_H3_APP_INTERNAL_ERROR;
            return -ENOMEM;
        }
    }

    trevrpc_h3_settings_report report = {0};
    trevrpc_h3_settings_status status = trevrpc_h3_settings_parse(payload,
        payload_len,
        sizeof(payload),
        seen_ids,
        seen_ids_capacity,
        trevrpc_wt_apply_peer_setting,
        settings,
        &report);
    free(seen_ids);
    if (status != TREV_H3_SETTINGS_OK) {
        session->h3_error_code = trevrpc_h3_settings_status_error_code(status);
        return TREV_WT_ERR_REJECTED;
    }
    return 0;
}

static int trevrpc_wt_read_peer_control_settings(
    trevrpc_wt_session* session, trevrpc_wt_role role, bool require_webtransport, trevrpc_wt_peer_settings* settings) {
    session->h3_error_code = TREV_H3_APP_MISSING_SETTINGS;
    trevrpc_msquic_stream* control = NULL;
    int err = trevrpc_msquic_conn_accept_stream(session->msquic_conn, &control);
    if (err != 0) {
        return trevrpc_wt_map_msquic_error(err);
    }

    uint64_t stream_id = 0;
    err = trevrpc_msquic_stream_id(control, &stream_id);
    if (err == 0 && !trevrpc_wt_profile_peer_stream_is_unidirectional(stream_id)) {
        err = TREV_WT_ERR_REJECTED;
    }

    uint64_t stream_type = 0;
    if (err == 0) {
        err = trevrpc_wt_read_varint(control, &stream_type);
    }
    if (err == 0 && stream_type != TREV_WT_H3_STREAM_TYPE_CONTROL) {
        err = TREV_WT_ERR_REJECTED;
    }

    trevrpc_wt_h3_frame frame = {0};
    if (err == 0) {
        session->h3_error_code = TREV_H3_APP_FRAME_ERROR;
        err = trevrpc_wt_read_varint(control, &frame.type);
    }
    if (err == 0) {
        err = trevrpc_wt_read_varint(control, &frame.len);
    }
    if (err == 0 && frame.type != TREV_H3_FRAME_SETTINGS) {
        session->h3_error_code = TREV_H3_APP_MISSING_SETTINGS;
        err = TREV_WT_ERR_REJECTED;
    }
    if (err == 0 && frame.len > session->frame_policy.max_settings_payload) {
        session->h3_error_code = TREV_H3_APP_EXCESSIVE_LOAD;
        err = TREV_WT_ERR_REJECTED;
    }

    if (err == 0) {
        err = trevrpc_wt_read_settings_payload(session, control, frame.len, settings);
    }
    if (err == 0) {
        trevrpc_wt_profile_negotiation negotiation = {0};
        err = trevrpc_wt_profile_negotiate(
            role, settings, &session->transport_capabilities, require_webtransport, &negotiation);
        if (err != 0) {
            session->h3_error_code = TREV_H3_APP_SETTINGS_ERROR;
            err = TREV_WT_ERR_REJECTED;
        } else {
            session->draft = negotiation.profile;
            session->requires_initial_capsule_flow_control =
                (negotiation.compatibility_flags & TREV_WT_PROFILE_COMPAT_INITIAL_CAPSULE_FLOW_CONTROL) != 0;
            session->h3_error_code = 0;
        }
    }

    session->peer_control = control;
    return err;
}

static int trevrpc_wt_h3_handshake(
    trevrpc_wt_session* session, const trevrpc_wt_config* config, trevrpc_wt_role role, bool require_webtransport) {
    trevrpc_wt_peer_settings peer_settings = {0};
    int err = trevrpc_msquic_conn_feature_snapshot(session->msquic_conn, &session->transport_capabilities);
    if (err != 0) {
        return trevrpc_wt_map_msquic_error(err);
    }
    err = trevrpc_wt_write_control_settings(session, config);
    if (err == 0 && require_webtransport && !trevrpc_wt_profile_any_usable(&session->transport_capabilities)) {
        session->h3_error_code = TREV_H3_APP_SETTINGS_ERROR;
        return TREV_WT_ERR_REJECTED;
    }
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

    listener->config = *config;
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
    listener->config.path = listener->path;
    listener->config.origin = listener->origin;

    trevrpc_msquic_config msquic_config = trevrpc_wt_msquic_config(config);
    trevrpc_msquic_feature_request features = trevrpc_msquic_default_h3_features();
    int err = trevrpc_msquic_listen_alpns_features(
        config->host, config->port, &msquic_config, NULL, 0, &features, &listener->msquic_listener);
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

    return trevrpc_wt_accept_session_from_msquic(conn, &listener->config, out_session);
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
    session->frame_policy = trevrpc_h3_frame_policy_make(0);
    int err = trevrpc_wt_h3_handshake(session, config, TREV_WT_ROLE_SERVER, true);
    if (err != 0) {
        if (session->h3_error_code != 0) {
            trevrpc_msquic_conn_shutdown_error(conn, session->h3_error_code);
        }
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
    trevrpc_h3_unknown_discard_budget unknown_discard;
    (void)trevrpc_h3_unknown_discard_budget_init(
        &unknown_discard, conn->session.frame_policy.max_control_unknown_discard);
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
        if (trevrpc_h3_frame_type_is_http2_reserved(frame_type) || frame_type == TREV_H3_FRAME_SETTINGS ||
            frame_type == TREV_H3_FRAME_DATA || frame_type == TREV_H3_FRAME_HEADERS ||
            frame_type == TREV_H3_FRAME_PUSH_PROMISE) {
            trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_FRAME_UNEXPECTED);
            return NULL;
        }
        if (frame_type == TREV_H3_FRAME_CANCEL_PUSH || frame_type == TREV_H3_FRAME_GOAWAY ||
            frame_type == TREV_H3_FRAME_MAX_PUSH_ID) {
            uint8_t payload[8];
            if (frame_len == 0 || frame_len > sizeof(payload) ||
                trevrpc_wt_read_exact(conn->session.peer_control, payload, (size_t)frame_len) != 0) {
                trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_FRAME_ERROR);
                return NULL;
            }
            size_t offset = 0;
            uint64_t id = 0;
            if (trevrpc_quic_varint_read(payload, (size_t)frame_len, &offset, &id) != 0 ||
                offset != (size_t)frame_len) {
                trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_FRAME_ERROR);
                return NULL;
            }
            continue;
        }
        if (trevrpc_h3_unknown_discard_budget_charge(&unknown_discard, frame_len) != TREV_H3_FRAME_OK) {
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
        intptr_t n = trevrpc_msquic_stream_read_protocol(stream, ignored, sizeof(ignored));
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
    h3_conn->webtransport_path = trevrpc_wt_strdup(webtransport_config->path);
    h3_conn->webtransport_origin = trevrpc_wt_strdup(webtransport_config->origin);
    h3_conn->http3_path = trevrpc_wt_strdup(http3_path);
    if ((webtransport_config->path != NULL && h3_conn->webtransport_path == NULL) ||
        (webtransport_config->origin != NULL && h3_conn->webtransport_origin == NULL) ||
        (http3_path != NULL && h3_conn->http3_path == NULL)) {
        trevrpc_h3_conn_close(h3_conn);
        return -ENOMEM;
    }
    h3_conn->webtransport_admission = webtransport_config->admission;
    h3_conn->webtransport_admission_user_data = webtransport_config->admission_user_data;
    h3_conn->enable_http3 = enable_http3 != 0;
    h3_conn->http3_admission = http3_admission;
    h3_conn->http3_admission_user_data = http3_admission_user_data;
    h3_conn->session.frame_policy = trevrpc_h3_frame_policy_make(max_frame_size);

    int err = trevrpc_wt_h3_handshake(&h3_conn->session, webtransport_config, TREV_WT_ROLE_SERVER, false);
    if (err != 0) {
        uint64_t error_code =
            h3_conn->session.h3_error_code != 0
                ? h3_conn->session.h3_error_code
                : (err == TREV_WT_ERR_CLOSED ? TREV_H3_APP_CLOSED_CRITICAL_STREAM : TREV_H3_APP_MISSING_SETTINGS);
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
    case TREV_H3_ERR_EXCESSIVE_LOAD:
        error_code = TREV_H3_APP_EXCESSIVE_LOAD;
        break;
    case TREV_H3_ERR_ID_ERROR:
        error_code = TREV_H3_APP_ID_ERROR;
        break;
    case TREV_H3_ERR_MESSAGE_ERROR:
        error_code = TREV_H3_APP_MESSAGE_ERROR;
        break;
    default:
        break;
    }
    trevrpc_msquic_conn_shutdown_error(stream->conn->session.msquic_conn, error_code);
    return err;
}

static int trevrpc_h3_read_headers_payload_timeout(
    trevrpc_h3_stream* stream, uint64_t frame_len, uint64_t deadline_nanos, trevrpc_wt_headers* headers) {
    if (frame_len > stream->conn->session.frame_policy.max_encoded_field_section) {
        return TREV_H3_ERR_EXCESSIVE_LOAD;
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
        err = trevrpc_wt_header_block_decode(block, (size_t)frame_len, TREV_HTTP3_HEADERS_REQUEST, headers);
    }
    free(block);
    return err;
}

static int trevrpc_h3_accept_webtransport_connect(
    trevrpc_h3_conn* conn, trevrpc_h3_stream* stream, const trevrpc_wt_headers* headers) {
    pthread_mutex_lock(&conn->mutex);
    bool unavailable = conn->webtransport_connected || conn->webtransport_resolving || conn->shutting_down ||
                       conn->session.draft == TREV_WT_PROFILE_NONE;
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
        if (err == 0 && trevrpc_wt_profile_requires_draft02_response_marker(conn->session.draft)) {
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
    (void)trevrpc_h3_unknown_discard_budget_init(
        &stream->unknown_discard, conn->session.frame_policy.max_request_unknown_discard);
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
        if (trevrpc_wt_profile_peer_stream_is_unidirectional(stream_id)) {
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
                   trevrpc_wt_profile_valid_session_id(session_id);
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
        if (!trevrpc_wt_profile_valid_session_id(session_id)) {
            return trevrpc_h3_connection_error(stream, TREV_H3_ERR_ID_ERROR);
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

    uint64_t frame_len = 0;
    ready = trevrpc_h3_read_varint_incremental(stream, &frame_len, TREV_H3_READ_DEADLINE, deadline);
    if (ready == TREV_MSQUIC_ERR_TIMEOUT) {
        (void)trevrpc_h3_reject_request(stream, TREV_H3_REQUEST_TIMEOUT_STATUS);
        return 0;
    }
    if (ready <= 0) {
        return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
    }
    if (first != TREV_H3_FRAME_HEADERS) {
        return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_UNEXPECTED);
    }
    if (frame_len > conn->session.frame_policy.max_encoded_field_section) {
        return trevrpc_h3_connection_error(stream, TREV_H3_ERR_EXCESSIVE_LOAD);
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
    free(conn->http3_path);
    free(conn->webtransport_origin);
    free(conn->webtransport_path);
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
    session->frame_policy = trevrpc_h3_frame_policy_make(0);

    trevrpc_msquic_config msquic_config = trevrpc_wt_msquic_config(config);
    trevrpc_msquic_feature_request features = trevrpc_msquic_default_h3_features();
    int err = trevrpc_msquic_dial_observed_features(
        config->host, config->port, &msquic_config, &features, NULL, NULL, NULL, 0, NULL, NULL, &session->msquic_conn);
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
    if (!trevrpc_wt_profile_valid_session_id(session_id)) {
        trevrpc_msquic_conn_shutdown_error(session->msquic_conn, TREV_H3_APP_ID_ERROR);
        trevrpc_msquic_stream_close(msquic_stream);
        return TREV_WT_ERR_REJECTED;
    }
    if (session_id != session->connect_stream_id) {
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
        result = trevrpc_msquic_stream_read_protocol_ready(stream->msquic_stream, data, len);
    } else if (mode == TREV_H3_READ_DEADLINE) {
        uint64_t now = trevrpc_h3_monotonic_nanos();
        if (now == 0 || now >= deadline_nanos) {
            return TREV_MSQUIC_ERR_TIMEOUT;
        }
        result = trevrpc_msquic_stream_read_protocol_timeout(stream->msquic_stream, data, len, deadline_nanos - now);
    } else {
        result = trevrpc_msquic_stream_read_protocol(stream->msquic_stream, data, len);
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
            stream->varint_need = trevrpc_quic_varint_size_from_first(stream->varint[0]);
        }
    }

    size_t offset = 0;
    uint64_t decoded = 0;
    int err = trevrpc_quic_varint_read(stream->varint, stream->varint_need, &offset, &decoded);
    stream->varint_len = 0;
    stream->varint_need = 0;
    if (err != 0) {
        return TREV_H3_ERR_FRAME_ERROR;
    }
    *value = decoded;
    return 1;
}

static int trevrpc_h3_static_qpack_error(trevrpc_qpack_status status) {
    switch (status) {
    case TREV_QPACK_OK:
        return 0;
    case TREV_QPACK_DECOMPRESSION_FAILED:
        return TREV_H3_ERR_QPACK_DECOMPRESSION_FAILED;
    case TREV_QPACK_ENCODED_SIZE_EXCEEDED:
    case TREV_QPACK_DECODED_SIZE_EXCEEDED:
    case TREV_QPACK_FIELD_COUNT_EXCEEDED:
        return TREV_H3_ERR_FIELD_SECTION_TOO_LARGE;
    case TREV_QPACK_ALLOCATION_FAILED:
        return -ENOMEM;
    case TREV_QPACK_INVALID_ARGUMENT:
    default:
        return -EINVAL;
    }
}

static int trevrpc_h3_validate_trailers(trevrpc_h3_stream* stream) {
    trevrpc_qpack_field_section section;
    trevrpc_qpack_field_section_init(&section);
    const trevrpc_qpack_limits limits = {
        .max_encoded_size = TREV_H3_MAX_ENCODED_FIELD_SECTION_SIZE,
        .max_decoded_size = TREV_H3_MAX_FIELD_SECTION_SIZE,
        .max_field_count = TREV_H3_MAX_FIELD_SECTION_SIZE / 32u,
    };
    trevrpc_qpack_status qpack_status =
        trevrpc_qpack_static_decode(stream->trailer_block, stream->trailer_len, &limits, &section);
    int err = trevrpc_h3_static_qpack_error(qpack_status);
    if (err == 0) {
        trevrpc_http3_headers_report report;
        trevrpc_http3_headers_status headers_status =
            trevrpc_http3_headers_validate(&section, TREV_HTTP3_HEADERS_TRAILERS, &report);
        if (headers_status == TREV_HTTP3_HEADERS_MESSAGE_ERROR) {
            err = TREV_H3_ERR_MESSAGE_ERROR;
        } else if (headers_status != TREV_HTTP3_HEADERS_OK) {
            err = -EINVAL;
        }
    }
    trevrpc_qpack_field_section_release(&section);
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
                    return err == -ENOMEM ? err : trevrpc_h3_connection_error(stream, err);
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
        if (trevrpc_h3_frame_type_is_http2_reserved(stream->frame_type)) {
            return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_UNEXPECTED);
        }

        switch (stream->frame_type) {
        case TREV_H3_FRAME_DATA:
            if (stream->trailers_seen) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_UNEXPECTED);
            }
            if (frame_len > stream->conn->session.frame_policy.max_data_payload) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_EXCESSIVE_LOAD);
            }
            stream->data_remaining = frame_len;
            break;
        case TREV_H3_FRAME_HEADERS:
            if (stream->trailers_seen) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_UNEXPECTED);
            }
            if (frame_len > stream->conn->session.frame_policy.max_encoded_field_section) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_EXCESSIVE_LOAD);
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
                    return err == -ENOMEM ? err : trevrpc_h3_connection_error(stream, err);
                }
            }
            break;
        case TREV_H3_FRAME_SETTINGS:
        case TREV_H3_FRAME_CANCEL_PUSH:
        case TREV_H3_FRAME_PUSH_PROMISE:
        case TREV_H3_FRAME_GOAWAY:
        case TREV_H3_FRAME_MAX_PUSH_ID:
            return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_UNEXPECTED);
        default:
            if (trevrpc_h3_unknown_discard_budget_charge(&stream->unknown_discard, frame_len) != TREV_H3_FRAME_OK) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_EXCESSIVE_LOAD);
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
    int err = trevrpc_quic_varint_write(prefix, sizeof(prefix), TREV_H3_FRAME_DATA, &written);
    if (err == 0) {
        prefix_len += written;
        err = trevrpc_quic_varint_write(prefix + prefix_len, sizeof(prefix) - prefix_len, len, &written);
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
