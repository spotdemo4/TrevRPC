#ifndef TREVRPC_WEBTRANSPORT_CAPSULE_INTERNAL_H
#define TREVRPC_WEBTRANSPORT_CAPSULE_INTERNAL_H

#include "trevrpc_quic_varint_internal.h"
#include "trevrpc_webtransport_profile_internal.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define TREV_WT_CAPSULE_CLOSE_SESSION UINT64_C(0x2843)
#define TREV_WT_CAPSULE_DRAIN_SESSION UINT64_C(0x78ae)
#define TREV_WT_CAPSULE_MAX_DATA UINT64_C(0x190b4d3d)
#define TREV_WT_CAPSULE_MAX_STREAMS_BIDI UINT64_C(0x190b4d3f)
#define TREV_WT_CAPSULE_MAX_STREAMS_UNI UINT64_C(0x190b4d40)

#define TREV_WT_CAPSULE_CLOSE_REASON_MAX ((size_t)1024)
#define TREV_WT_FLOW_MAX_STREAMS (UINT64_C(1) << 60)
#define TREV_WT_FLOW_MAX_DATA ((UINT64_C(1) << 62) - 1)
#define TREV_WT_FLOW_CONTROL_ERROR UINT64_C(0x045d4487)

enum {
    TREV_WT_CAPSULE_CAP_CLOSE = 1u << 0,
    TREV_WT_CAPSULE_CAP_DRAIN = 1u << 1,
    TREV_WT_CAPSULE_CAP_MAX_DATA = 1u << 2,
    TREV_WT_CAPSULE_CAP_MAX_STREAMS = 1u << 3,
};

typedef enum trevrpc_wt_capsule_parse_result {
    TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT = -1,
    TREV_WT_CAPSULE_PARSE_NEED_INPUT = 0,
    TREV_WT_CAPSULE_PARSE_EVENT = 1,
    TREV_WT_CAPSULE_PARSE_COMPLETE = 2,
    TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE = 3,
    TREV_WT_CAPSULE_PARSE_EXCESSIVE_LOAD = 4,
    TREV_WT_CAPSULE_PARSE_H3_DATAGRAM_ERROR = 5,
    TREV_WT_CAPSULE_PARSE_POST_CLOSE_MESSAGE_ERROR = 6,
} trevrpc_wt_capsule_parse_result;

typedef enum trevrpc_wt_capsule_event_type {
    TREV_WT_CAPSULE_EVENT_CLOSE = 0,
    TREV_WT_CAPSULE_EVENT_DRAIN = 1,
    TREV_WT_CAPSULE_EVENT_MAX_DATA = 2,
    TREV_WT_CAPSULE_EVENT_MAX_STREAMS_BIDI = 3,
    TREV_WT_CAPSULE_EVENT_MAX_STREAMS_UNI = 4,
} trevrpc_wt_capsule_event_type;

typedef struct trevrpc_wt_capsule_event {
    trevrpc_wt_capsule_event_type type;
    union {
        struct {
            uint32_t code;
            const uint8_t* reason;
            size_t reason_len;
            bool implicit;
        } close;
        struct {
            uint64_t maximum;
        } max;
    } value;
} trevrpc_wt_capsule_event;

typedef struct trevrpc_wt_capsule_parser_config {
    trevrpc_wt_profile_id profile;
    uint32_t compatibility_flags;
    bool modern_flow_control_enabled;
    uint64_t max_unknown_capsule_payload;
    /*
     * This entire range must be representable in uintptr_t address space and must
     * not overlap the parser object. CLOSE reasons are staged before this output
     * range is written, so bytes from the current capsule may alias it in either
     * direction. Not-yet-fed input must not occupy the eventual
     * [workspace, workspace + reason_len) output range. A feed that detects
     * overlapping unconsumed trailing bytes is rejected atomically.
     */
    uint8_t* close_reason_workspace;
    size_t close_reason_capacity;
} trevrpc_wt_capsule_parser_config;

typedef enum trevrpc_wt_capsule_parser_state {
    TREV_WT_CAPSULE_PARSER_TYPE = 0,
    TREV_WT_CAPSULE_PARSER_LENGTH = 1,
    TREV_WT_CAPSULE_PARSER_CLOSE_PAYLOAD = 2,
    TREV_WT_CAPSULE_PARSER_NUMERIC_PAYLOAD = 3,
    TREV_WT_CAPSULE_PARSER_UNKNOWN_PAYLOAD = 4,
    TREV_WT_CAPSULE_PARSER_CLOSED = 5,
    TREV_WT_CAPSULE_PARSER_FAILED = 6,
} trevrpc_wt_capsule_parser_state;

typedef struct trevrpc_wt_capsule_parser {
    uint8_t* close_reason_workspace;
    size_t close_reason_capacity;
    uint64_t max_unknown_capsule_payload;
    uint32_t capabilities;
    bool recognizes_disabled_flow_control;

    trevrpc_wt_capsule_parser_state state;
    trevrpc_wt_capsule_parse_result failure;

    uint64_t capsule_type;
    uint64_t capsule_length;
    uint64_t payload_remaining;

    union {
        trevrpc_quic_varint_feeder varint;
        struct {
            uint8_t varint_bytes[8];
            uint8_t varint_have;
            uint8_t varint_need;
        };
    };

    uint8_t close_code_bytes[4];
    uint8_t close_code_have;
    uint8_t close_reason_staging[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    size_t close_reason_len;
} trevrpc_wt_capsule_parser;

trevrpc_wt_capsule_parse_result trevrpc_wt_capsule_parser_init(
    trevrpc_wt_capsule_parser* parser, const trevrpc_wt_capsule_parser_config* config);

trevrpc_wt_capsule_parse_result trevrpc_wt_capsule_parser_feed(trevrpc_wt_capsule_parser* parser,
    const uint8_t* data,
    size_t data_len,
    size_t* out_consumed,
    trevrpc_wt_capsule_event* out_event);

trevrpc_wt_capsule_parse_result trevrpc_wt_capsule_parser_finish(
    trevrpc_wt_capsule_parser* parser, trevrpc_wt_capsule_event* out_event);

typedef struct trevrpc_wt_flow_limits {
    uint64_t max_data;
    uint64_t max_streams_bidi;
    uint64_t max_streams_uni;
} trevrpc_wt_flow_limits;

typedef enum trevrpc_wt_stream_direction {
    TREV_WT_STREAM_BIDI = 0,
    TREV_WT_STREAM_UNI = 1,
} trevrpc_wt_stream_direction;

typedef enum trevrpc_wt_flow_result {
    TREV_WT_FLOW_INVALID_ARGUMENT = -1,
    TREV_WT_FLOW_INVALID_STATE = -2,
    TREV_WT_FLOW_OK = 0,
    TREV_WT_FLOW_IGNORED = 1,
    TREV_WT_FLOW_BLOCKED = 2,
    TREV_WT_FLOW_H3_DATAGRAM_ERROR = 3,
    TREV_WT_FLOW_CONTROL_VIOLATION = 4,
} trevrpc_wt_flow_result;

typedef struct trevrpc_wt_flow_reservation trevrpc_wt_flow_reservation;

typedef struct trevrpc_wt_flow_state {
    bool enabled;

    trevrpc_wt_flow_limits peer_max;

    uint64_t admitted_data;
    uint64_t committed_data;

    uint64_t admitted_streams_bidi;
    uint64_t committed_streams_bidi;

    uint64_t admitted_streams_uni;
    uint64_t committed_streams_uni;

    uint64_t generation;
    uint64_t next_reservation_identity;
    trevrpc_wt_flow_reservation* active_reservations;
} trevrpc_wt_flow_state;

typedef enum trevrpc_wt_flow_reservation_kind {
    TREV_WT_FLOW_RESERVATION_NONE = 0,
    TREV_WT_FLOW_RESERVATION_BYPASS = 1,
    TREV_WT_FLOW_RESERVATION_DATA = 2,
    TREV_WT_FLOW_RESERVATION_STREAM_BIDI = 3,
    TREV_WT_FLOW_RESERVATION_STREAM_UNI = 4,
} trevrpc_wt_flow_reservation_kind;

struct trevrpc_wt_flow_reservation {
    trevrpc_wt_flow_state* owner;
    trevrpc_wt_flow_reservation_kind kind;
    uint64_t amount;
    uint64_t original_amount;
    uint64_t owner_generation;
    uint64_t identity;
    trevrpc_wt_flow_reservation* active_next;
    trevrpc_wt_flow_reservation* active_prev;
    /*
     * Retirement validates this token and both immediate neighbors before changing
     * anything. Corruption therefore blocks the damaged token and adjacent
     * retirements, while valid nonadjacent tokens remain independently operable.
     * Immutable token fields and mutable list links are authenticated separately
     * so updating a validated link can never authenticate corrupted token fields.
     */
    uint64_t integrity;
    uint64_t link_integrity;
};

#define TREV_WT_FLOW_RESERVATION_INIT                                                                                  \
    {NULL,                                                                                                             \
        TREV_WT_FLOW_RESERVATION_NONE,                                                                                 \
        UINT64_C(0),                                                                                                   \
        UINT64_C(0),                                                                                                   \
        UINT64_C(0),                                                                                                   \
        UINT64_C(0),                                                                                                   \
        NULL,                                                                                                          \
        NULL,                                                                                                          \
        UINT64_C(0),                                                                                                   \
        UINT64_C(0)}

trevrpc_wt_flow_result trevrpc_wt_flow_state_init(trevrpc_wt_flow_state* state,
    const trevrpc_wt_flow_limits* local_initial,
    const trevrpc_wt_flow_limits* peer_initial);

trevrpc_wt_flow_result trevrpc_wt_flow_apply_max_data(trevrpc_wt_flow_state* state, uint64_t maximum);

trevrpc_wt_flow_result trevrpc_wt_flow_apply_max_streams(
    trevrpc_wt_flow_state* state, trevrpc_wt_stream_direction direction, uint64_t maximum);

trevrpc_wt_flow_result trevrpc_wt_flow_reserve_data(
    trevrpc_wt_flow_state* state, uint64_t amount, trevrpc_wt_flow_reservation* reservation);

trevrpc_wt_flow_result trevrpc_wt_flow_reserve_stream(
    trevrpc_wt_flow_state* state, trevrpc_wt_stream_direction direction, trevrpc_wt_flow_reservation* reservation);

trevrpc_wt_flow_result trevrpc_wt_flow_commit(trevrpc_wt_flow_state* state, trevrpc_wt_flow_reservation* reservation);

trevrpc_wt_flow_result trevrpc_wt_flow_cancel(trevrpc_wt_flow_state* state, trevrpc_wt_flow_reservation* reservation);

#endif
