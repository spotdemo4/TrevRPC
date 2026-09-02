#ifndef TREVRPC_H3_DEMUX_INTERNAL_H
#define TREVRPC_H3_DEMUX_INTERNAL_H

#include "trevrpc_http3_frame_internal.h"
#include "trevrpc_webtransport_profile_internal.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR 0x103
#define TREV_H3_DEMUX_APP_FRAME_UNEXPECTED 0x105
#define TREV_H3_DEMUX_APP_FRAME_ERROR 0x106
#define TREV_H3_DEMUX_APP_ID_ERROR 0x108

#define TREV_H3_DEMUX_STREAM_TYPE_CONTROL 0x00
#define TREV_H3_DEMUX_STREAM_TYPE_QPACK_ENCODER 0x02
#define TREV_H3_DEMUX_STREAM_TYPE_QPACK_DECODER 0x03
#define TREV_H3_DEMUX_WEBTRANSPORT_BIDI 0x41
#define TREV_H3_DEMUX_WEBTRANSPORT_UNI 0x54

typedef enum trevrpc_h3_demux_direction {
    TREV_H3_DEMUX_BIDIRECTIONAL = 0,
    TREV_H3_DEMUX_UNIDIRECTIONAL = 1,
} trevrpc_h3_demux_direction;

typedef enum trevrpc_h3_demux_action {
    TREV_H3_DEMUX_ACTION_NONE = 0,
    TREV_H3_DEMUX_ACTION_CONTROL,
    TREV_H3_DEMUX_ACTION_QPACK_ENCODER,
    TREV_H3_DEMUX_ACTION_QPACK_DECODER,
    TREV_H3_DEMUX_ACTION_UNKNOWN_UNIDIRECTIONAL,
    TREV_H3_DEMUX_ACTION_REQUEST,
    TREV_H3_DEMUX_ACTION_WEBTRANSPORT,
    TREV_H3_DEMUX_ACTION_RECLAIM_UNCLASSIFIED,
} trevrpc_h3_demux_action;

typedef enum trevrpc_h3_demux_status {
    TREV_H3_DEMUX_NEED_MORE = 0,
    TREV_H3_DEMUX_WAIT_PROFILE,
    TREV_H3_DEMUX_ACTION_READY,
    TREV_H3_DEMUX_PROTOCOL_ERROR,
    TREV_H3_DEMUX_COMPLETE,
    TREV_H3_DEMUX_INVALID_ARGUMENT,
} trevrpc_h3_demux_status;

typedef enum trevrpc_h3_demux_phase {
    TREV_H3_DEMUX_READ_FIRST = 0,
    TREV_H3_DEMUX_WAIT_NEGOTIATED_PROFILE,
    TREV_H3_DEMUX_READ_SESSION_ID,
    TREV_H3_DEMUX_READ_UNKNOWN_LENGTH,
    TREV_H3_DEMUX_SKIP_UNKNOWN_PAYLOAD,
    TREV_H3_DEMUX_READY,
    TREV_H3_DEMUX_FAILED,
} trevrpc_h3_demux_phase;

/* A resolved value is immutable for the lifetime of a negotiated connection. */
typedef struct trevrpc_h3_demux_profile_capabilities {
    bool resolved;
    trevrpc_wt_profile_id selected_profile;
} trevrpc_h3_demux_profile_capabilities;

typedef struct trevrpc_h3_demux_stream {
    trevrpc_h3_demux_direction direction;
    trevrpc_h3_demux_phase phase;
    trevrpc_h3_demux_action action;
    uint64_t first_value;
    uint64_t session_id;
    uint64_t unknown_payload_remaining;
    uint8_t varint[8];
    size_t varint_len;
    size_t varint_need;
} trevrpc_h3_demux_stream;

typedef struct trevrpc_h3_demux_result {
    size_t consumed;
    trevrpc_h3_demux_action action;
    uint64_t first_value;
    uint64_t session_id;
    uint64_t application_error;
} trevrpc_h3_demux_result;

typedef struct trevrpc_h3_demux_roles {
    bool control;
    bool qpack_encoder;
    bool qpack_decoder;
} trevrpc_h3_demux_roles;

bool trevrpc_h3_demux_stream_id_is_unidirectional(uint64_t stream_id);

int trevrpc_h3_demux_stream_init(trevrpc_h3_demux_stream* stream, trevrpc_h3_demux_direction direction);

trevrpc_h3_demux_status trevrpc_h3_demux_stream_feed(trevrpc_h3_demux_stream* stream,
    const trevrpc_h3_demux_profile_capabilities* profile,
    const uint8_t* data,
    size_t len,
    trevrpc_h3_demux_result* out_result);

trevrpc_h3_demux_status trevrpc_h3_demux_stream_terminal(trevrpc_h3_demux_stream* stream,
    const trevrpc_h3_demux_profile_capabilities* profile,
    trevrpc_h3_demux_result* out_result);

/* Returns zero when the role is available or the action is not critical. */
int trevrpc_h3_demux_roles_claim(
    trevrpc_h3_demux_roles* roles, trevrpc_h3_demux_action action, uint64_t* out_application_error);

#endif
