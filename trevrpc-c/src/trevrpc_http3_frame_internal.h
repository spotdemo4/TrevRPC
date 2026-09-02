#ifndef TREVRPC_HTTP3_FRAME_INTERNAL_H
#define TREVRPC_HTTP3_FRAME_INTERNAL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define TREV_H3_FRAME_DATA UINT64_C(0x00)
#define TREV_H3_FRAME_HEADERS UINT64_C(0x01)
#define TREV_H3_FRAME_CANCEL_PUSH UINT64_C(0x03)
#define TREV_H3_FRAME_SETTINGS UINT64_C(0x04)
#define TREV_H3_FRAME_PUSH_PROMISE UINT64_C(0x05)
#define TREV_H3_FRAME_GOAWAY UINT64_C(0x07)
#define TREV_H3_FRAME_MAX_PUSH_ID UINT64_C(0x0d)

#define TREV_H3_FRAME_ERROR_UNEXPECTED UINT64_C(0x105)
#define TREV_H3_FRAME_ERROR_MALFORMED UINT64_C(0x106)
#define TREV_H3_FRAME_ERROR_EXCESSIVE_LOAD UINT64_C(0x107)

static inline bool trevrpc_h3_frame_type_is_http2_reserved(uint64_t frame_type) {
    return frame_type == 0x02 || frame_type == 0x06 || frame_type == 0x08 || frame_type == 0x09;
}

static inline bool trevrpc_h3_frame_type_is_request_prohibited(uint64_t frame_type) {
    return trevrpc_h3_frame_type_is_http2_reserved(frame_type) || frame_type == TREV_H3_FRAME_SETTINGS ||
           frame_type == TREV_H3_FRAME_CANCEL_PUSH || frame_type == TREV_H3_FRAME_PUSH_PROMISE ||
           frame_type == TREV_H3_FRAME_GOAWAY || frame_type == TREV_H3_FRAME_MAX_PUSH_ID;
}

typedef enum trevrpc_h3_frame_status {
    TREV_H3_FRAME_INVALID_ARGUMENT = -1,
    TREV_H3_FRAME_NEED_INPUT = 0,
    TREV_H3_FRAME_OK = 1,
    TREV_H3_FRAME_CLEAN_EOF = 2,
    TREV_H3_FRAME_TRUNCATED_TYPE = 3,
    TREV_H3_FRAME_TYPE_WITHOUT_LENGTH = 4,
    TREV_H3_FRAME_TRUNCATED_LENGTH = 5,
    TREV_H3_FRAME_EXCESSIVE_LOAD = 6,
    TREV_H3_FRAME_UNEXPECTED = 7,
    TREV_H3_FRAME_FIELD_SECTION_TOO_LARGE = 8,
    TREV_H3_FRAME_PAYLOAD_ACTIVE = 9,
    TREV_H3_FRAME_PAYLOAD_OVERCONSUMED = 10,
    TREV_H3_FRAME_OUTPUT_TOO_SMALL = 11,
    TREV_H3_FRAME_TRUNCATED_PAYLOAD = 12,
} trevrpc_h3_frame_status;

typedef struct trevrpc_h3_frame_prefix {
    uint64_t type;
    uint64_t length;
} trevrpc_h3_frame_prefix;

typedef enum trevrpc_h3_frame_prefix_phase {
    TREV_H3_FRAME_PREFIX_TYPE = 0,
    TREV_H3_FRAME_PREFIX_LENGTH = 1,
    TREV_H3_FRAME_PREFIX_READY = 2,
    TREV_H3_FRAME_PREFIX_FAILED = 3,
} trevrpc_h3_frame_prefix_phase;

typedef struct trevrpc_h3_frame_prefix_parser {
    uint64_t max_payload_length;
    uint64_t frame_type;
    trevrpc_h3_frame_prefix_phase phase;
    trevrpc_h3_frame_status failure;
    uint8_t varint[8];
    uint8_t varint_have;
    uint8_t varint_need;
} trevrpc_h3_frame_prefix_parser;

/* Initializes or resets a parser without changing its configured payload bound. */
trevrpc_h3_frame_status trevrpc_h3_frame_prefix_parser_init(
    trevrpc_h3_frame_prefix_parser* parser, uint64_t max_payload_length);
trevrpc_h3_frame_status trevrpc_h3_frame_prefix_parser_reset(trevrpc_h3_frame_prefix_parser* parser);

/*
 * Consumes only the type and length varints. Payload bytes remain unconsumed.
 * out_prefix is changed only when this call completes a prefix successfully.
 */
trevrpc_h3_frame_status trevrpc_h3_frame_prefix_parser_feed(trevrpc_h3_frame_prefix_parser* parser,
    const uint8_t* data,
    size_t data_len,
    size_t* out_consumed,
    trevrpc_h3_frame_prefix* out_prefix);

/* Reports clean EOF or the exact kind of truncated prefix. */
trevrpc_h3_frame_status trevrpc_h3_frame_prefix_parser_finish(trevrpc_h3_frame_prefix_parser* parser);

/* Emits minimal type and length varints after preflighting the complete output. */
trevrpc_h3_frame_status trevrpc_h3_frame_prefix_build(
    uint64_t type, uint64_t length, uint8_t* out, size_t out_capacity, size_t* out_length);

typedef struct trevrpc_h3_unknown_discard_budget {
    uint64_t limit;
    uint64_t used;
} trevrpc_h3_unknown_discard_budget;

trevrpc_h3_frame_status trevrpc_h3_unknown_discard_budget_init(
    trevrpc_h3_unknown_discard_budget* budget, uint64_t limit);
/* Charges a declared unknown-frame payload before any bytes are discarded. */
trevrpc_h3_frame_status trevrpc_h3_unknown_discard_budget_charge(
    trevrpc_h3_unknown_discard_budget* budget, uint64_t payload_length);

typedef enum trevrpc_h3_request_phase {
    TREV_H3_REQUEST_BEFORE_HEADERS = 0,
    TREV_H3_REQUEST_BODY = 1,
    TREV_H3_REQUEST_TRAILERS = 2,
    TREV_H3_REQUEST_COMPLETE = 3,
} trevrpc_h3_request_phase;

typedef enum trevrpc_h3_request_payload_kind {
    TREV_H3_REQUEST_PAYLOAD_NONE = 0,
    TREV_H3_REQUEST_PAYLOAD_HEADERS = 1,
    TREV_H3_REQUEST_PAYLOAD_DATA = 2,
    TREV_H3_REQUEST_PAYLOAD_TRAILERS = 3,
    TREV_H3_REQUEST_PAYLOAD_UNKNOWN = 4,
} trevrpc_h3_request_payload_kind;

typedef struct trevrpc_h3_request_frame_state {
    trevrpc_h3_request_phase phase;
    trevrpc_h3_request_payload_kind active_payload;
    uint64_t payload_remaining;
    uint64_t max_encoded_field_section_length;
    trevrpc_h3_unknown_discard_budget unknown_discard;
} trevrpc_h3_request_frame_state;

/*
 * initial_headers_received permits use after a caller has separately consumed the
 * initial HEADERS section, as the current production resolver does.
 */
trevrpc_h3_frame_status trevrpc_h3_request_frame_state_init(trevrpc_h3_request_frame_state* state,
    bool initial_headers_received,
    uint64_t max_encoded_field_section_length,
    uint64_t max_unknown_discard);

/* Begins one frame payload and advances only protocol ordering state. */
trevrpc_h3_frame_status trevrpc_h3_request_frame_begin(trevrpc_h3_request_frame_state* state,
    const trevrpc_h3_frame_prefix* prefix,
    trevrpc_h3_request_payload_kind* out_payload_kind);

/* Consumes or discards bytes from the active payload. */
trevrpc_h3_frame_status trevrpc_h3_request_frame_consume(trevrpc_h3_request_frame_state* state, uint64_t amount);

/* Marks a request message complete at a frame boundary. */
trevrpc_h3_frame_status trevrpc_h3_request_frame_finish(trevrpc_h3_request_frame_state* state);

/* Returns zero when a status is not a peer HTTP/3 application error. */
uint64_t trevrpc_h3_frame_status_error_code(trevrpc_h3_frame_status status);

#endif
