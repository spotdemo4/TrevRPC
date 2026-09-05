#define _POSIX_C_SOURCE 200809L

#include "trevrpc_webtransport.h"

#include "trevrpc.h"
#include "trevrpc_msquic.h"

#include "trevrpc_frame_internal.h"
#include "trevrpc_h3_demux_internal.h"
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
#define TREV_H3_UNRESOLVED_STREAM_TIMEOUT_NANOS 5000000000ull
#define TREV_H3_QPACK_SET_CAPACITY_ZERO 0x20
#define TREV_H3_UNIDI_MONITOR_COUNT TREV_WT_H3_DEFAULT_UNIDI_STREAMS

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

typedef enum trevrpc_h3_unidi_monitor_state {
    TREV_H3_UNIDI_MONITOR_FREE = 0,
    TREV_H3_UNIDI_MONITOR_LIVE,
    TREV_H3_UNIDI_MONITOR_CLOSING,
} trevrpc_h3_unidi_monitor_state;

typedef struct trevrpc_h3_unidi_monitor trevrpc_h3_unidi_monitor;

typedef struct trevrpc_h3_unidi_observer_context {
    trevrpc_h3_unidi_monitor* monitor;
    uint64_t generation;
} trevrpc_h3_unidi_observer_context;

struct trevrpc_h3_unidi_monitor {
    trevrpc_h3_conn* conn;
    trevrpc_msquic_stream* stream;
    trevrpc_h3_demux_stream classifier;
    trevrpc_h3_unidi_observer_context observer_context;
    uint64_t deadline_nanos;
    uint64_t generation;
    uint32_t pending_flags;
    trevrpc_h3_unidi_monitor_state state;
    bool observer_installed;
    bool install_pending;
    bool processing;
    bool terminal_seen;
};

struct trevrpc_h3_conn {
    trevrpc_wt_session session;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    pthread_cond_t unidi_cond;
    pthread_t control_thread;
    pthread_t unidi_pump_thread;
    trevrpc_h3_unidi_monitor unidi_monitors[TREV_H3_UNIDI_MONITOR_COUNT];
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
    bool unidi_pump_thread_started;
#ifdef TREVRPC_H3_FRAME_POLICY_TESTING
    size_t unidi_started_count;
    size_t unidi_retired_count;
    size_t unidi_qpack_pending_count;
#endif
    bool qpack_encoder_seen;
    bool qpack_decoder_seen;
};

struct trevrpc_h3_stream {
    trevrpc_msquic_stream* msquic_stream;
    trevrpc_h3_conn* conn;
    trevrpc_h3_frame_prefix_parser frame_prefix;
    trevrpc_h3_request_frame_state frame_state;
    uint8_t* trailer_block;
    size_t trailer_len;
    size_t trailer_offset;
    trevrpc_frame_parser rpc_parser;
    bool rpc_parser_initialized;
    bool owns_msquic_stream;
};

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
static intptr_t trevrpc_h3_read_msquic(
    trevrpc_h3_stream* stream, uint8_t* data, size_t len, trevrpc_h3_read_mode mode, uint64_t deadline_nanos);
static int trevrpc_h3_wait_for_webtransport(trevrpc_h3_conn* conn, uint64_t session_id, uint64_t deadline_nanos);

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

static int trevrpc_wt_read_bounded_varint_payload(
    trevrpc_msquic_stream* stream, uint64_t payload_len, uint64_t* value) {
    uint8_t payload[8];
    if (payload_len == 0 || payload_len > sizeof(payload)) {
        return TREV_WT_ERR_REJECTED;
    }

    int err = trevrpc_wt_read_exact(stream, payload, (size_t)payload_len);
    if (err != 0) {
        return err;
    }

    size_t offset = 0;
    if (trevrpc_quic_varint_read(payload, (size_t)payload_len, &offset, value) != 0 || offset != (size_t)payload_len) {
        return TREV_WT_ERR_REJECTED;
    }
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

static int trevrpc_wt_qpack_put_indexed_static(uint8_t* out, size_t out_len, size_t* offset, uint64_t index) {
    trevrpc_qpack_static_encoder encoder = {out, out_len, *offset};
    int result = trevrpc_qpack_static_encoder_put_indexed(&encoder, index);
    if (result == 0)
        *offset = encoder.length;
    return result;
}

static int trevrpc_wt_qpack_put_literal_static_name(
    uint8_t* out, size_t out_len, size_t* offset, uint64_t name_index, const char* value) {
    trevrpc_qpack_static_encoder encoder = {out, out_len, *offset};
    int result = trevrpc_qpack_static_encoder_put_literal_name_reference(
        &encoder, name_index, (const uint8_t*)value, strlen(value));
    if (result == 0)
        *offset = encoder.length;
    return result;
}

static int trevrpc_wt_qpack_put_literal(
    uint8_t* out, size_t out_len, size_t* offset, const char* name, const char* value) {
    trevrpc_qpack_static_encoder encoder = {out, out_len, *offset};
    int result = trevrpc_qpack_static_encoder_put_literal(
        &encoder, (const uint8_t*)name, strlen(name), (const uint8_t*)value, strlen(value));
    if (result == 0)
        *offset = encoder.length;
    return result;
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
    uint64_t max_encoded_field_section,
    trevrpc_http3_header_block_kind kind,
    trevrpc_wt_headers_validate_fn validate,
    void* context) {
    trevrpc_h3_frame_prefix_parser parser;
    trevrpc_h3_frame_prefix prefix = {0};
    trevrpc_h3_frame_status frame_status = trevrpc_h3_frame_prefix_parser_init(&parser, max_encoded_field_section);
    if (frame_status == TREV_H3_FRAME_INVALID_ARGUMENT) {
        return -EINVAL;
    }

    while (frame_status == TREV_H3_FRAME_NEED_INPUT) {
        uint8_t encoded_byte = 0;
        intptr_t n = trevrpc_msquic_stream_read_protocol(stream, &encoded_byte, 1);
        if (n < 0) {
            return trevrpc_wt_map_msquic_error((int)n);
        }
        if (n == 0) {
            frame_status = trevrpc_h3_frame_prefix_parser_finish(&parser);
            return frame_status == TREV_H3_FRAME_CLEAN_EOF ? TREV_WT_ERR_CLOSED : TREV_WT_ERR_REJECTED;
        }

        size_t consumed = 0;
        frame_status = trevrpc_h3_frame_prefix_parser_feed(&parser, &encoded_byte, 1, &consumed, &prefix);
        if (frame_status != TREV_H3_FRAME_NEED_INPUT && frame_status != TREV_H3_FRAME_OK) {
            return TREV_WT_ERR_REJECTED;
        }
    }
    if (prefix.type != TREV_H3_FRAME_HEADERS) {
        return TREV_WT_ERR_REJECTED;
    }

    size_t frame_len = (size_t)prefix.length;
    uint8_t* block = malloc(frame_len);
    if (block == NULL && frame_len > 0) {
        return -ENOMEM;
    }
    trevrpc_wt_headers headers = {0};
    int err = trevrpc_wt_read_exact(stream, block, frame_len);
    if (err == 0) {
        err = trevrpc_wt_header_block_decode(block, frame_len, kind, &headers);
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
        err = trevrpc_wt_read_headers_frame(stream,
            session->frame_policy.max_encoded_field_section,
            TREV_HTTP3_HEADERS_RESPONSE,
            trevrpc_wt_validate_connect_response,
            &context);
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
    err = trevrpc_wt_read_headers_frame(stream,
        session->frame_policy.max_encoded_field_section,
        TREV_HTTP3_HEADERS_REQUEST,
        trevrpc_wt_validate_connect_request,
        &context);

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

    trevrpc_h3_frame_prefix_parser parser;
    trevrpc_h3_frame_prefix prefix = {0};
    trevrpc_h3_frame_status frame_status = TREV_H3_FRAME_NEED_INPUT;
    if (err == 0) {
        session->h3_error_code = TREV_H3_APP_FRAME_ERROR;
        frame_status = trevrpc_h3_frame_prefix_parser_init(&parser, session->frame_policy.max_settings_payload);
        while (frame_status == TREV_H3_FRAME_NEED_INPUT) {
            uint8_t encoded_byte = 0;
            intptr_t n = trevrpc_msquic_stream_read_protocol(control, &encoded_byte, 1);
            if (n < 0) {
                err = trevrpc_wt_map_msquic_error((int)n);
                break;
            }
            if (n == 0) {
                frame_status = trevrpc_h3_frame_prefix_parser_finish(&parser);
                err = frame_status == TREV_H3_FRAME_CLEAN_EOF ? TREV_WT_ERR_CLOSED : TREV_WT_ERR_REJECTED;
                break;
            }

            size_t consumed = 0;
            frame_status = trevrpc_h3_frame_prefix_parser_feed(&parser, &encoded_byte, 1, &consumed, &prefix);
        }
        if (frame_status == TREV_H3_FRAME_EXCESSIVE_LOAD) {
            session->h3_error_code = TREV_H3_APP_EXCESSIVE_LOAD;
            err = TREV_WT_ERR_REJECTED;
        } else if (err == 0 && frame_status != TREV_H3_FRAME_OK) {
            err = TREV_WT_ERR_REJECTED;
        }
    }
    if (err == 0 && prefix.type != TREV_H3_FRAME_SETTINGS) {
        session->h3_error_code = TREV_H3_APP_MISSING_SETTINGS;
        err = TREV_WT_ERR_REJECTED;
    }

    if (err == 0) {
        err = trevrpc_wt_read_settings_payload(session, control, prefix.length, settings);
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
        trevrpc_h3_frame_prefix_parser parser;
        trevrpc_h3_frame_prefix prefix = {0};
        trevrpc_h3_frame_status frame_status = trevrpc_h3_frame_prefix_parser_init(&parser, UINT64_MAX);
        int err = 0;
        while (frame_status == TREV_H3_FRAME_NEED_INPUT) {
            uint8_t encoded_byte = 0;
            intptr_t n = trevrpc_msquic_stream_read_protocol(conn->session.peer_control, &encoded_byte, 1);
            if (n < 0) {
                err = trevrpc_wt_map_msquic_error((int)n);
                break;
            }
            if (n == 0) {
                frame_status = trevrpc_h3_frame_prefix_parser_finish(&parser);
                err = frame_status == TREV_H3_FRAME_CLEAN_EOF ? TREV_WT_ERR_CLOSED : TREV_WT_ERR_REJECTED;
                break;
            }

            size_t consumed = 0;
            frame_status = trevrpc_h3_frame_prefix_parser_feed(&parser, &encoded_byte, 1, &consumed, &prefix);
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
        if (frame_status == TREV_H3_FRAME_EXCESSIVE_LOAD) {
            trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_EXCESSIVE_LOAD);
            return NULL;
        }
        if (frame_status != TREV_H3_FRAME_OK) {
            if (!shutting_down) {
                trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_CLOSED_CRITICAL_STREAM);
            }
            return NULL;
        }
        if (trevrpc_h3_frame_type_is_http2_reserved(prefix.type) || prefix.type == TREV_H3_FRAME_SETTINGS ||
            prefix.type == TREV_H3_FRAME_DATA || prefix.type == TREV_H3_FRAME_HEADERS ||
            prefix.type == TREV_H3_FRAME_PUSH_PROMISE) {
            trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_FRAME_UNEXPECTED);
            return NULL;
        }
        if (prefix.type == TREV_H3_FRAME_CANCEL_PUSH || prefix.type == TREV_H3_FRAME_GOAWAY ||
            prefix.type == TREV_H3_FRAME_MAX_PUSH_ID) {
            uint64_t id = 0;
            if (trevrpc_wt_read_bounded_varint_payload(conn->session.peer_control, prefix.length, &id) != 0) {
                trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_FRAME_ERROR);
                return NULL;
            }
            continue;
        }
        if (trevrpc_h3_unknown_discard_budget_charge(&unknown_discard, prefix.length) != TREV_H3_FRAME_OK) {
            trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_EXCESSIVE_LOAD);
            return NULL;
        }
        uint8_t ignored[1024];
        uint64_t frame_len = prefix.length;
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

static void trevrpc_h3_unidi_observer_wake(void* context, uint32_t flags) {
    trevrpc_h3_unidi_observer_context* observer_context = context;
    trevrpc_h3_unidi_monitor* monitor = observer_context->monitor;
    trevrpc_h3_conn* conn = monitor->conn;
    pthread_mutex_lock(&conn->mutex);
    if (monitor->state == TREV_H3_UNIDI_MONITOR_LIVE && monitor->generation == observer_context->generation) {
        monitor->pending_flags |= flags;
        pthread_cond_signal(&conn->unidi_cond);
    }
    pthread_mutex_unlock(&conn->mutex);
}

static void trevrpc_h3_unidi_monitor_close(trevrpc_h3_conn* conn, size_t index, uint64_t generation) {
    trevrpc_msquic_stream* stream = NULL;
    bool observer_installed = false;
    pthread_mutex_lock(&conn->mutex);
    trevrpc_h3_unidi_monitor* monitor = &conn->unidi_monitors[index];
    if (monitor->generation == generation && monitor->state != TREV_H3_UNIDI_MONITOR_FREE) {
        monitor->state = TREV_H3_UNIDI_MONITOR_CLOSING;
        monitor->processing = false;
        stream = monitor->stream;
        observer_installed = monitor->observer_installed;
    }
    pthread_mutex_unlock(&conn->mutex);
    if (stream == NULL) {
        return;
    }
    if (observer_installed) {
        trevrpc_msquic_stream_clear_observer(stream);
        trevrpc_msquic_stream_drain_observer(stream);
    }
    trevrpc_msquic_stream_close(stream);
    pthread_mutex_lock(&conn->mutex);
    monitor = &conn->unidi_monitors[index];
    if (monitor->generation == generation && monitor->state == TREV_H3_UNIDI_MONITOR_CLOSING) {
        monitor->stream = NULL;
        monitor->pending_flags = 0;
        monitor->observer_installed = false;
        monitor->install_pending = false;
        monitor->terminal_seen = false;
        monitor->state = TREV_H3_UNIDI_MONITOR_FREE;
#ifdef TREVRPC_H3_FRAME_POLICY_TESTING
        conn->unidi_retired_count++;
#endif
        pthread_cond_broadcast(&conn->unidi_cond);
    }
    pthread_mutex_unlock(&conn->mutex);
}

#ifdef TREVRPC_H3_FRAME_POLICY_TESTING
int trevrpc_h3_test_wait_unidi_progress(
    trevrpc_h3_conn* conn, size_t minimum_started_count, size_t minimum_retired_count, uint64_t timeout_nanos) {
    if (conn == NULL || (minimum_started_count == 0 && minimum_retired_count == 0) || timeout_nanos == 0) {
        return -EINVAL;
    }
    struct timespec deadline = {0};
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        return -errno;
    }
    deadline.tv_sec += (time_t)(timeout_nanos / 1000000000ull);
    deadline.tv_nsec += (long)(timeout_nanos % 1000000000ull);
    if (deadline.tv_nsec >= 1000000000l) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000l;
    }

    int result = 0;
    pthread_mutex_lock(&conn->mutex);
    while (conn->unidi_started_count < minimum_started_count || conn->unidi_retired_count < minimum_retired_count) {
        int err = pthread_cond_timedwait(&conn->unidi_cond, &conn->mutex, &deadline);
        if (err != 0) {
            result = -err;
            break;
        }
    }
    pthread_mutex_unlock(&conn->mutex);
    return result;
}

int trevrpc_h3_test_wait_qpack_pending(trevrpc_h3_conn* conn, size_t minimum_pending_count, uint64_t timeout_nanos) {
    if (conn == NULL || minimum_pending_count == 0 || timeout_nanos == 0) {
        return -EINVAL;
    }
    struct timespec deadline = {0};
    if (clock_gettime(CLOCK_REALTIME, &deadline) != 0) {
        return -errno;
    }
    deadline.tv_sec += (time_t)(timeout_nanos / 1000000000ull);
    deadline.tv_nsec += (long)(timeout_nanos % 1000000000ull);
    if (deadline.tv_nsec >= 1000000000l) {
        deadline.tv_sec++;
        deadline.tv_nsec -= 1000000000l;
    }

    int result = 0;
    pthread_mutex_lock(&conn->mutex);
    while (conn->unidi_qpack_pending_count < minimum_pending_count) {
        int err = pthread_cond_timedwait(&conn->unidi_cond, &conn->mutex, &deadline);
        if (err != 0) {
            result = -err;
            break;
        }
    }
    pthread_mutex_unlock(&conn->mutex);
    return result;
}
#endif

static size_t trevrpc_h3_unidi_read_size(const trevrpc_h3_demux_stream* classifier) {
    if (classifier->phase == TREV_H3_DEMUX_SKIP_UNKNOWN_PAYLOAD) {
        return classifier->unknown_payload_remaining < 1024 ? (size_t)classifier->unknown_payload_remaining : 1024;
    }
    if (classifier->varint.have == 0 || classifier->varint.need <= classifier->varint.have) {
        return 1;
    }
    return classifier->varint.need - classifier->varint.have;
}

typedef enum trevrpc_h3_unidi_process_result {
    TREV_H3_UNIDI_PROCESS_PENDING = 0,
    TREV_H3_UNIDI_PROCESS_DONE,
    TREV_H3_UNIDI_PROCESS_FAILED,
} trevrpc_h3_unidi_process_result;

static trevrpc_h3_unidi_process_result trevrpc_h3_process_unidi_monitor(
    trevrpc_h3_conn* conn, trevrpc_h3_unidi_monitor* monitor, uint32_t flags, uint64_t* out_error) {
    trevrpc_h3_demux_profile_capabilities profile = {
        .resolved = true,
        .selected_profile = conn->session.draft,
    };
    monitor->terminal_seen = monitor->terminal_seen || (flags & TREV_MSQUIC_STREAM_OBSERVER_TERMINAL) != 0;
    bool terminal = monitor->terminal_seen;
    trevrpc_h3_demux_action action = monitor->classifier.action;
    bool newly_classified = monitor->classifier.phase != TREV_H3_DEMUX_READY;
    uint8_t buffer[1024];
    if (newly_classified) {
        trevrpc_h3_demux_result result = {0};
        trevrpc_h3_demux_status status = trevrpc_h3_demux_stream_feed(&monitor->classifier, &profile, NULL, 0, &result);
        while (status == TREV_H3_DEMUX_NEED_MORE || status == TREV_H3_DEMUX_WAIT_PROFILE) {
            uint64_t now = trevrpc_h3_monotonic_nanos();
            if (now == 0) {
                *out_error = TREV_H3_APP_INTERNAL_ERROR;
                return TREV_H3_UNIDI_PROCESS_FAILED;
            }
            if (now >= monitor->deadline_nanos) {
                *out_error = monitor->classifier.phase == TREV_H3_DEMUX_READ_SESSION_ID
                                 ? TREV_H3_APP_ID_ERROR
                                 : TREV_H3_APP_STREAM_CREATION_ERROR;
                return TREV_H3_UNIDI_PROCESS_FAILED;
            }
            size_t read_size = trevrpc_h3_unidi_read_size(&monitor->classifier);
            intptr_t n = trevrpc_msquic_stream_read_protocol_ready(monitor->stream, buffer, read_size);
            if (n > 0) {
                status = trevrpc_h3_demux_stream_feed(&monitor->classifier, &profile, buffer, (size_t)n, &result);
                continue;
            }
            if (n == TREV_MSQUIC_ERR_TIMEOUT && !terminal) {
                return TREV_H3_UNIDI_PROCESS_PENDING;
            }
            if (n == TREV_MSQUIC_ERR_CLOSED) {
                *out_error = TREV_H3_APP_CLOSED_CRITICAL_STREAM;
                return TREV_H3_UNIDI_PROCESS_FAILED;
            }
            if (n == 0 || n == TREV_MSQUIC_ERR_TIMEOUT) {
                status = trevrpc_h3_demux_stream_terminal(&monitor->classifier, &profile, &result);
                break;
            }
            *out_error = TREV_H3_APP_INTERNAL_ERROR;
            return TREV_H3_UNIDI_PROCESS_FAILED;
        }
        if (status == TREV_H3_DEMUX_PROTOCOL_ERROR) {
            *out_error = result.application_error;
            return TREV_H3_UNIDI_PROCESS_FAILED;
        }
        if (status != TREV_H3_DEMUX_ACTION_READY) {
            *out_error = monitor->classifier.phase == TREV_H3_DEMUX_READ_SESSION_ID ? TREV_H3_APP_ID_ERROR
                                                                                    : TREV_H3_APP_STREAM_CREATION_ERROR;
            return TREV_H3_UNIDI_PROCESS_FAILED;
        }
        action = result.action;
        if (action == TREV_H3_DEMUX_ACTION_RECLAIM_UNCLASSIFIED) {
            return TREV_H3_UNIDI_PROCESS_DONE;
        }
        if (action == TREV_H3_DEMUX_ACTION_CONTROL) {
            *out_error = TREV_H3_APP_STREAM_CREATION_ERROR;
            return TREV_H3_UNIDI_PROCESS_FAILED;
        }
        if (action == TREV_H3_DEMUX_ACTION_WEBTRANSPORT) {
            int err = trevrpc_h3_wait_for_webtransport(conn, result.session_id, monitor->deadline_nanos);
            if (err != 0) {
                pthread_mutex_lock(&conn->mutex);
                bool shutting_down = conn->shutting_down;
                pthread_mutex_unlock(&conn->mutex);
                if (shutting_down) {
                    return TREV_H3_UNIDI_PROCESS_DONE;
                }
                *out_error = err == TREV_MSQUIC_ERR_TIMEOUT || err == TREV_WT_ERR_REJECTED ? TREV_H3_APP_ID_ERROR
                                                                                           : TREV_H3_APP_INTERNAL_ERROR;
                return TREV_H3_UNIDI_PROCESS_FAILED;
            }
        }

        bool critical = action == TREV_H3_DEMUX_ACTION_QPACK_ENCODER || action == TREV_H3_DEMUX_ACTION_QPACK_DECODER;
        if (critical) {
            pthread_mutex_lock(&conn->mutex);
            bool* seen =
                action == TREV_H3_DEMUX_ACTION_QPACK_ENCODER ? &conn->qpack_encoder_seen : &conn->qpack_decoder_seen;
            bool duplicate = *seen;
            *seen = true;
            pthread_mutex_unlock(&conn->mutex);
            if (duplicate) {
                *out_error = TREV_H3_APP_STREAM_CREATION_ERROR;
                return TREV_H3_UNIDI_PROCESS_FAILED;
            }
        }
    }

    bool critical = action == TREV_H3_DEMUX_ACTION_QPACK_ENCODER || action == TREV_H3_DEMUX_ACTION_QPACK_DECODER;
    for (;;) {
        intptr_t n = trevrpc_msquic_stream_read_protocol_ready(monitor->stream, buffer, sizeof(buffer));
        if (n > 0 && action == TREV_H3_DEMUX_ACTION_QPACK_ENCODER) {
            for (intptr_t i = 0; i < n; i++) {
                if (buffer[i] != TREV_H3_QPACK_SET_CAPACITY_ZERO) {
                    *out_error = TREV_H3_APP_QPACK_ENCODER_STREAM_ERROR;
                    return TREV_H3_UNIDI_PROCESS_FAILED;
                }
            }
            continue;
        }
        if (n > 0 && critical) {
            *out_error = action == TREV_H3_DEMUX_ACTION_QPACK_ENCODER ? TREV_H3_APP_QPACK_ENCODER_STREAM_ERROR
                                                                      : TREV_H3_APP_QPACK_DECODER_STREAM_ERROR;
            return TREV_H3_UNIDI_PROCESS_FAILED;
        }
        if (n == TREV_MSQUIC_ERR_TIMEOUT) {
            return TREV_H3_UNIDI_PROCESS_PENDING;
        }
        if (n == 0) {
            if (critical) {
                *out_error = TREV_H3_APP_CLOSED_CRITICAL_STREAM;
                return TREV_H3_UNIDI_PROCESS_FAILED;
            }
            return TREV_H3_UNIDI_PROCESS_DONE;
        }
        if (n == TREV_MSQUIC_ERR_CLOSED) {
            pthread_mutex_lock(&conn->mutex);
            bool shutting_down = conn->shutting_down;
            pthread_mutex_unlock(&conn->mutex);
            if (critical && !shutting_down) {
                *out_error = TREV_H3_APP_CLOSED_CRITICAL_STREAM;
                return TREV_H3_UNIDI_PROCESS_FAILED;
            }
            return TREV_H3_UNIDI_PROCESS_DONE;
        }
        *out_error = TREV_H3_APP_INTERNAL_ERROR;
        return TREV_H3_UNIDI_PROCESS_FAILED;
    }
}

static bool trevrpc_h3_take_unidi_monitor(trevrpc_h3_conn* conn, size_t* out_index, uint32_t* out_flags) {
    uint64_t now = trevrpc_h3_monotonic_nanos();
    for (size_t i = 0; i < TREV_H3_UNIDI_MONITOR_COUNT; i++) {
        trevrpc_h3_unidi_monitor* monitor = &conn->unidi_monitors[i];
        if (monitor->state == TREV_H3_UNIDI_MONITOR_LIVE && !monitor->install_pending && !monitor->processing &&
            (monitor->pending_flags != 0 ||
                (monitor->classifier.phase != TREV_H3_DEMUX_READY && now != 0 && now >= monitor->deadline_nanos))) {
            monitor->processing = true;
            *out_index = i;
            *out_flags = monitor->pending_flags;
            monitor->pending_flags = 0;
            return true;
        }
    }
    return false;
}

static int trevrpc_h3_unidi_wait(trevrpc_h3_conn* conn) {
    uint64_t now = trevrpc_h3_monotonic_nanos();
    uint64_t remaining = UINT64_MAX;
    if (now != 0) {
        for (size_t i = 0; i < TREV_H3_UNIDI_MONITOR_COUNT; i++) {
            trevrpc_h3_unidi_monitor* monitor = &conn->unidi_monitors[i];
            if (monitor->state != TREV_H3_UNIDI_MONITOR_LIVE || monitor->install_pending || monitor->processing ||
                monitor->classifier.phase == TREV_H3_DEMUX_READY) {
                continue;
            }
            uint64_t value = monitor->deadline_nanos <= now ? 0 : monitor->deadline_nanos - now;
            if (value < remaining) {
                remaining = value;
            }
        }
    }
    if (remaining == UINT64_MAX) {
        return pthread_cond_wait(&conn->unidi_cond, &conn->mutex);
    }
    struct timespec realtime;
    if (clock_gettime(CLOCK_REALTIME, &realtime) != 0) {
        return errno;
    }
    realtime.tv_sec += (time_t)(remaining / 1000000000ull);
    realtime.tv_nsec += (long)(remaining % 1000000000ull);
    if (realtime.tv_nsec >= 1000000000l) {
        realtime.tv_sec++;
        realtime.tv_nsec -= 1000000000l;
    }
    return pthread_cond_timedwait(&conn->unidi_cond, &conn->mutex, &realtime);
}

static void* trevrpc_h3_unidi_pump(void* context) {
    trevrpc_h3_conn* conn = context;
    for (;;) {
        pthread_mutex_lock(&conn->mutex);
        size_t index = 0;
        uint32_t flags = 0;
        while (!conn->shutting_down && !trevrpc_h3_take_unidi_monitor(conn, &index, &flags)) {
            (void)trevrpc_h3_unidi_wait(conn);
        }
        bool stopping = conn->shutting_down;
        pthread_mutex_unlock(&conn->mutex);
        if (stopping) {
            return NULL;
        }

        trevrpc_h3_unidi_monitor* monitor = &conn->unidi_monitors[index];
        uint64_t error_code = 0;
        trevrpc_h3_unidi_process_result result = trevrpc_h3_process_unidi_monitor(conn, monitor, flags, &error_code);
        if (result == TREV_H3_UNIDI_PROCESS_PENDING) {
            pthread_mutex_lock(&conn->mutex);
            if (monitor->state == TREV_H3_UNIDI_MONITOR_LIVE) {
#ifdef TREVRPC_H3_FRAME_POLICY_TESTING
                if (monitor->classifier.phase == TREV_H3_DEMUX_READY &&
                    (monitor->classifier.action == TREV_H3_DEMUX_ACTION_QPACK_ENCODER ||
                        monitor->classifier.action == TREV_H3_DEMUX_ACTION_QPACK_DECODER)) {
                    conn->unidi_qpack_pending_count++;
                    pthread_cond_broadcast(&conn->unidi_cond);
                }
#endif
                monitor->processing = false;
            }
            pthread_mutex_unlock(&conn->mutex);
            continue;
        }
        if (result == TREV_H3_UNIDI_PROCESS_FAILED) {
            pthread_mutex_lock(&conn->mutex);
            bool shutting_down = conn->shutting_down;
            if (monitor->state == TREV_H3_UNIDI_MONITOR_LIVE) {
                monitor->processing = false;
            }
            pthread_mutex_unlock(&conn->mutex);
            if (!shutting_down && error_code != 0) {
                trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, error_code);
            }
        }
        uint64_t generation;
        pthread_mutex_lock(&conn->mutex);
        generation = monitor->generation;
        pthread_mutex_unlock(&conn->mutex);
        trevrpc_h3_unidi_monitor_close(conn, index, generation);
    }
}

static int trevrpc_h3_start_unidi_monitor(trevrpc_h3_conn* conn, trevrpc_msquic_stream* stream) {
    uint64_t now = trevrpc_h3_monotonic_nanos();
    if (now == 0 || now > UINT64_MAX - TREV_H3_UNRESOLVED_STREAM_TIMEOUT_NANOS) {
        trevrpc_msquic_stream_close(stream);
        return -EOVERFLOW;
    }
    size_t index = TREV_H3_UNIDI_MONITOR_COUNT;
    pthread_mutex_lock(&conn->mutex);
    if (!conn->shutting_down) {
        for (size_t i = 0; i < TREV_H3_UNIDI_MONITOR_COUNT; i++) {
            if (conn->unidi_monitors[i].state == TREV_H3_UNIDI_MONITOR_FREE) {
                index = i;
                trevrpc_h3_unidi_monitor* monitor = &conn->unidi_monitors[i];
                uint64_t generation = monitor->generation + 1;
                if (generation == 0) {
                    generation = 1;
                }
                monitor->conn = conn;
                monitor->stream = stream;
                monitor->deadline_nanos = now + TREV_H3_UNRESOLVED_STREAM_TIMEOUT_NANOS;
                monitor->generation = generation;
                monitor->observer_context.monitor = monitor;
                monitor->observer_context.generation = generation;
                monitor->pending_flags = 0;
                monitor->state = TREV_H3_UNIDI_MONITOR_LIVE;
                monitor->observer_installed = false;
                monitor->install_pending = true;
                monitor->processing = false;
                monitor->terminal_seen = false;
                (void)trevrpc_h3_demux_stream_init(&monitor->classifier, TREV_H3_DEMUX_UNIDIRECTIONAL);
                break;
            }
        }
    }
    pthread_mutex_unlock(&conn->mutex);
    if (index == TREV_H3_UNIDI_MONITOR_COUNT) {
        trevrpc_msquic_stream_close(stream);
        trevrpc_msquic_conn_shutdown_error(conn->session.msquic_conn, TREV_H3_APP_STREAM_CREATION_ERROR);
        return TREV_WT_ERR_REJECTED;
    }

    trevrpc_h3_unidi_monitor* monitor = &conn->unidi_monitors[index];
    int err = trevrpc_msquic_stream_set_observer(stream, trevrpc_h3_unidi_observer_wake, &monitor->observer_context);
    pthread_mutex_lock(&conn->mutex);
    if (monitor->state == TREV_H3_UNIDI_MONITOR_LIVE) {
        monitor->install_pending = false;
        if (err == 0) {
            monitor->observer_installed = true;
            monitor->pending_flags |= TREV_MSQUIC_STREAM_OBSERVER_READABLE;
#ifdef TREVRPC_H3_FRAME_POLICY_TESTING
            conn->unidi_started_count++;
#endif
            pthread_cond_broadcast(&conn->unidi_cond);
        }
    }
    uint64_t generation = monitor->generation;
    pthread_mutex_unlock(&conn->mutex);
    if (err != 0) {
        trevrpc_h3_unidi_monitor_close(conn, index, generation);
        return trevrpc_wt_map_msquic_error(err);
    }
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
    pthread_cond_init(&h3_conn->unidi_cond, NULL);
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
    err = pthread_create(&h3_conn->unidi_pump_thread, NULL, trevrpc_h3_unidi_pump, h3_conn);
    if (err != 0) {
        trevrpc_h3_conn_close(h3_conn);
        return -err;
    }
    h3_conn->unidi_pump_thread_started = true;
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
    if (trevrpc_h3_frame_prefix_parser_init(&stream->frame_prefix, TREV_QUIC_VARINT_MAX) ==
            TREV_H3_FRAME_INVALID_ARGUMENT ||
        trevrpc_h3_request_frame_state_init(&stream->frame_state,
            true,
            conn->session.frame_policy.max_encoded_field_section,
            conn->session.frame_policy.max_request_unknown_discard) != TREV_H3_FRAME_OK) {
        free(stream);
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

    trevrpc_h3_demux_stream classifier;
    trevrpc_h3_demux_result demux_result = {0};
    trevrpc_h3_unknown_discard_budget preamble_unknown_discard;
    trevrpc_h3_demux_profile_capabilities profile = {
        .resolved = true,
        .selected_profile = conn->session.draft,
    };
    if (trevrpc_h3_demux_stream_init(&classifier, TREV_H3_DEMUX_BIDIRECTIONAL) != 0 ||
        trevrpc_h3_unknown_discard_budget_init(
            &preamble_unknown_discard, conn->session.frame_policy.max_request_unknown_discard) != TREV_H3_FRAME_OK) {
        return -EINVAL;
    }

    uint8_t request_type[8] = {0};
    size_t request_type_len = 0;
    trevrpc_h3_demux_status demux_status = TREV_H3_DEMUX_NEED_MORE;
    while (demux_status == TREV_H3_DEMUX_NEED_MORE || demux_status == TREV_H3_DEMUX_WAIT_PROFILE) {
        uint8_t encoded_byte = 0;
        intptr_t n = trevrpc_h3_read_msquic(stream, &encoded_byte, 1, TREV_H3_READ_DEADLINE, deadline);
        if (n == TREV_MSQUIC_ERR_TIMEOUT) {
            (void)trevrpc_h3_reject_request(stream, TREV_H3_REQUEST_TIMEOUT_STATUS);
            return 0;
        }
        if (n <= 0) {
            return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
        }
        if (classifier.phase == TREV_H3_DEMUX_READ_FIRST && classifier.varint.have == 0) {
            request_type_len = 0;
        }
        if (classifier.phase == TREV_H3_DEMUX_READ_FIRST) {
            if (request_type_len == sizeof(request_type)) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
            }
            request_type[request_type_len++] = encoded_byte;
        }
        trevrpc_h3_demux_phase previous_phase = classifier.phase;
        demux_status = trevrpc_h3_demux_stream_feed(&classifier, &profile, &encoded_byte, 1, &demux_result);
        if (previous_phase == TREV_H3_DEMUX_READ_UNKNOWN_LENGTH &&
            (classifier.phase == TREV_H3_DEMUX_SKIP_UNKNOWN_PAYLOAD || classifier.phase == TREV_H3_DEMUX_READ_FIRST)) {
            trevrpc_h3_frame_status budget_status = trevrpc_h3_unknown_discard_budget_charge(
                &preamble_unknown_discard, classifier.unknown_payload_remaining);
            if (budget_status == TREV_H3_FRAME_EXCESSIVE_LOAD) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_EXCESSIVE_LOAD);
            }
            if (budget_status != TREV_H3_FRAME_OK) {
                return -EINVAL;
            }
        }
    }
    if (demux_status == TREV_H3_DEMUX_PROTOCOL_ERROR) {
        int demux_error = TREV_H3_ERR_FRAME_ERROR;
        switch (demux_result.application_error) {
        case TREV_H3_DEMUX_APP_ID_ERROR:
            demux_error = TREV_H3_ERR_ID_ERROR;
            break;
        case TREV_H3_DEMUX_APP_FRAME_UNEXPECTED:
            demux_error = TREV_H3_ERR_FRAME_UNEXPECTED;
            break;
        default:
            break;
        }
        return trevrpc_h3_connection_error(stream, demux_error);
    }
    if (demux_status != TREV_H3_DEMUX_ACTION_READY) {
        return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
    }

    if (demux_result.action == TREV_H3_DEMUX_ACTION_WEBTRANSPORT) {
        int err = trevrpc_h3_wait_for_webtransport(conn, demux_result.session_id, deadline);
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

    stream->frame_state.unknown_discard.used = preamble_unknown_discard.used;

    trevrpc_h3_frame_prefix_parser parser;
    trevrpc_h3_frame_prefix prefix = {0};
    trevrpc_h3_frame_status frame_status =
        trevrpc_h3_frame_prefix_parser_init(&parser, conn->session.frame_policy.max_encoded_field_section);
    if (frame_status == TREV_H3_FRAME_INVALID_ARGUMENT) {
        return -EINVAL;
    }
    size_t consumed = 0;
    frame_status = trevrpc_h3_frame_prefix_parser_feed(&parser, request_type, request_type_len, &consumed, &prefix);
    while (frame_status == TREV_H3_FRAME_NEED_INPUT) {
        uint8_t encoded_byte = 0;
        intptr_t n = trevrpc_h3_read_msquic(stream, &encoded_byte, 1, TREV_H3_READ_DEADLINE, deadline);
        if (n == TREV_MSQUIC_ERR_TIMEOUT) {
            (void)trevrpc_h3_reject_request(stream, TREV_H3_REQUEST_TIMEOUT_STATUS);
            return 0;
        }
        if (n <= 0) {
            (void)trevrpc_h3_frame_prefix_parser_finish(&parser);
            return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
        }
        frame_status = trevrpc_h3_frame_prefix_parser_feed(&parser, &encoded_byte, 1, &consumed, &prefix);
    }
    if (frame_status == TREV_H3_FRAME_EXCESSIVE_LOAD) {
        return trevrpc_h3_connection_error(stream, TREV_H3_ERR_EXCESSIVE_LOAD);
    }
    if (frame_status != TREV_H3_FRAME_OK) {
        return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
    }
    if (prefix.type != TREV_H3_FRAME_HEADERS) {
        return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_UNEXPECTED);
    }

    trevrpc_wt_headers headers = {0};
    int err = trevrpc_h3_read_headers_payload_timeout(stream, prefix.length, deadline, &headers);
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
        pthread_cond_broadcast(&conn->unidi_cond);
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
    if (conn->unidi_pump_thread_started && !pthread_equal(pthread_self(), conn->unidi_pump_thread)) {
        (void)pthread_join(conn->unidi_pump_thread, NULL);
    }
    for (size_t i = 0; i < TREV_H3_UNIDI_MONITOR_COUNT; i++) {
        uint64_t generation = 0;
        pthread_mutex_lock(&conn->mutex);
        if (conn->unidi_monitors[i].state != TREV_H3_UNIDI_MONITOR_FREE) {
            generation = conn->unidi_monitors[i].generation;
        }
        pthread_mutex_unlock(&conn->mutex);
        if (generation != 0) {
            trevrpc_h3_unidi_monitor_close(conn, i, generation);
        }
    }
    trevrpc_msquic_stream_close(conn->session.connect_stream);
    trevrpc_msquic_stream_close(conn->session.peer_control);
    trevrpc_msquic_stream_close(conn->session.local_control);
    trevrpc_msquic_conn_close(conn->session.msquic_conn);
    free(conn->http3_path);
    free(conn->webtransport_origin);
    free(conn->webtransport_path);
    pthread_cond_destroy(&conn->unidi_cond);
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

static int trevrpc_h3_frame_status_error(trevrpc_h3_stream* stream, trevrpc_h3_frame_status status) {
    switch (status) {
    case TREV_H3_FRAME_EXCESSIVE_LOAD:
    case TREV_H3_FRAME_FIELD_SECTION_TOO_LARGE:
        return trevrpc_h3_connection_error(stream, TREV_H3_ERR_EXCESSIVE_LOAD);
    case TREV_H3_FRAME_UNEXPECTED:
        return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_UNEXPECTED);
    case TREV_H3_FRAME_TRUNCATED_TYPE:
    case TREV_H3_FRAME_TYPE_WITHOUT_LENGTH:
    case TREV_H3_FRAME_TRUNCATED_LENGTH:
    case TREV_H3_FRAME_TRUNCATED_PAYLOAD:
        return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
    default:
        return -EINVAL;
    }
}

static intptr_t trevrpc_h3_read_frame_prefix(
    trevrpc_h3_stream* stream, trevrpc_h3_frame_prefix* prefix, trevrpc_h3_read_mode mode, uint64_t deadline_nanos) {
    for (;;) {
        uint8_t encoded_byte = 0;
        intptr_t n = trevrpc_h3_read_msquic(stream, &encoded_byte, 1, mode, deadline_nanos);
        if (n == 0) {
            trevrpc_h3_frame_status status = trevrpc_h3_frame_prefix_parser_finish(&stream->frame_prefix);
            return status == TREV_H3_FRAME_CLEAN_EOF ? 0 : trevrpc_h3_frame_status_error(stream, status);
        }
        if (n < 0) {
            return n;
        }
        size_t consumed = 0;
        trevrpc_h3_frame_status status =
            trevrpc_h3_frame_prefix_parser_feed(&stream->frame_prefix, &encoded_byte, (size_t)n, &consumed, prefix);
        if (status == TREV_H3_FRAME_OK) {
            return 1;
        }
        if (status != TREV_H3_FRAME_NEED_INPUT) {
            return trevrpc_h3_frame_status_error(stream, status);
        }
    }
}

static intptr_t trevrpc_h3_read_data(
    trevrpc_h3_stream* stream, uint8_t* data, size_t len, trevrpc_h3_read_mode mode, uint64_t deadline_nanos) {
    for (;;) {
        trevrpc_h3_request_payload_kind payload_kind = stream->frame_state.active_payload;
        if (stream->frame_state.payload_remaining > 0) {
            uint8_t ignored[1024];
            uint8_t* target = data;
            size_t requested =
                stream->frame_state.payload_remaining < len ? (size_t)stream->frame_state.payload_remaining : len;
            if (payload_kind == TREV_H3_REQUEST_PAYLOAD_TRAILERS) {
                if (stream->trailer_block == NULL) {
                    if (stream->trailer_len == 0) {
                        return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
                    }
                    stream->trailer_block = malloc(stream->trailer_len);
                    if (stream->trailer_block == NULL) {
                        return -ENOMEM;
                    }
                }
                target = stream->trailer_block + stream->trailer_offset;
            } else if (payload_kind == TREV_H3_REQUEST_PAYLOAD_UNKNOWN) {
                target = ignored;
                if (requested > sizeof(ignored)) {
                    requested = sizeof(ignored);
                }
            }
            intptr_t n = trevrpc_h3_read_msquic(stream, target, requested, mode, deadline_nanos);
            if (n == 0) {
                return trevrpc_h3_connection_error(stream, TREV_H3_ERR_FRAME_ERROR);
            }
            if (n < 0) {
                return n;
            }
            trevrpc_h3_frame_status frame_status = trevrpc_h3_request_frame_consume(&stream->frame_state, (uint64_t)n);
            if (frame_status != TREV_H3_FRAME_OK) {
                return trevrpc_h3_frame_status_error(stream, frame_status);
            }
            if (payload_kind == TREV_H3_REQUEST_PAYLOAD_DATA) {
                return n;
            }
            if (payload_kind == TREV_H3_REQUEST_PAYLOAD_TRAILERS) {
                stream->trailer_offset += (size_t)n;
                if (stream->trailer_offset == stream->trailer_len) {
                    int err = trevrpc_h3_validate_trailers(stream);
                    if (err != 0) {
                        return err == -ENOMEM ? err : trevrpc_h3_connection_error(stream, err);
                    }
                }
            }
            continue;
        }

        trevrpc_h3_frame_prefix prefix = {0};
        intptr_t ready = trevrpc_h3_read_frame_prefix(stream, &prefix, mode, deadline_nanos);
        if (ready == 0) {
            trevrpc_h3_frame_status frame_status = trevrpc_h3_request_frame_finish(&stream->frame_state);
            return frame_status == TREV_H3_FRAME_OK ? 0 : trevrpc_h3_frame_status_error(stream, frame_status);
        }
        if (ready < 0) {
            return ready;
        }

        trevrpc_h3_request_payload_kind next_payload_kind = TREV_H3_REQUEST_PAYLOAD_NONE;
        trevrpc_h3_frame_status frame_status =
            trevrpc_h3_request_frame_begin(&stream->frame_state, &prefix, &next_payload_kind);
        if (frame_status != TREV_H3_FRAME_OK) {
            return trevrpc_h3_frame_status_error(stream, frame_status);
        }
        if (next_payload_kind == TREV_H3_REQUEST_PAYLOAD_DATA &&
            prefix.length > stream->conn->session.frame_policy.max_data_payload) {
            return trevrpc_h3_connection_error(stream, TREV_H3_ERR_EXCESSIVE_LOAD);
        }
        if (trevrpc_h3_frame_prefix_parser_reset(&stream->frame_prefix) == TREV_H3_FRAME_INVALID_ARGUMENT) {
            return -EINVAL;
        }
        if (next_payload_kind == TREV_H3_REQUEST_PAYLOAD_TRAILERS) {
            stream->trailer_len = (size_t)prefix.length;
            stream->trailer_offset = 0;
            stream->trailer_block = stream->trailer_len == 0 ? NULL : malloc(stream->trailer_len);
            if (stream->trailer_len != 0 && stream->trailer_block == NULL) {
                return -ENOMEM;
            }
            if (stream->trailer_len == 0) {
                int err = trevrpc_h3_validate_trailers(stream);
                if (err != 0) {
                    return err == -ENOMEM ? err : trevrpc_h3_connection_error(stream, err);
                }
            }
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
