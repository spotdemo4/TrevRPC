#include "trevrpc_http3_frame_internal.h"
#include "trevrpc_quic_varint_internal.h"

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            return 1;                                                                                                  \
        }                                                                                                              \
    } while (0)

static trevrpc_h3_frame_prefix sentinel_prefix(void) {
    return (trevrpc_h3_frame_prefix){
        .type = UINT64_C(0xaaaaaaaaaaaaaaaa),
        .length = UINT64_C(0xbbbbbbbbbbbbbbbb),
    };
}

static int prefix_is_sentinel(const trevrpc_h3_frame_prefix* prefix) {
    return prefix->type == UINT64_C(0xaaaaaaaaaaaaaaaa) && prefix->length == UINT64_C(0xbbbbbbbbbbbbbbbb);
}

static size_t encode_varint_width(uint64_t value, size_t width, uint8_t* out) {
    uint8_t prefix = 0;
    if (width == 2) {
        prefix = 0x40;
    } else if (width == 4) {
        prefix = 0x80;
    } else if (width == 8) {
        prefix = 0xc0;
    }
    for (size_t i = 0; i < width; i++) {
        size_t shift = (width - i - 1) * 8;
        out[i] = (uint8_t)(value >> shift);
    }
    out[0] |= prefix;
    return width;
}

static int test_reserved_type_predicate(void) {
    const uint64_t reserved[] = {0x02, 0x06, 0x08, 0x09};
    for (size_t i = 0; i < sizeof(reserved) / sizeof(reserved[0]); i++) {
        CHECK(trevrpc_h3_frame_type_is_http2_reserved(reserved[i]));
    }

    const uint64_t allowed[] = {
        TREV_H3_FRAME_DATA,
        TREV_H3_FRAME_HEADERS,
        TREV_H3_FRAME_CANCEL_PUSH,
        TREV_H3_FRAME_SETTINGS,
        TREV_H3_FRAME_PUSH_PROMISE,
        TREV_H3_FRAME_GOAWAY,
        TREV_H3_FRAME_MAX_PUSH_ID,
        0x21,
    };
    for (size_t i = 0; i < sizeof(allowed) / sizeof(allowed[0]); i++) {
        CHECK(!trevrpc_h3_frame_type_is_http2_reserved(allowed[i]));
    }
    return 0;
}

static int test_prefix_coalesced_stops_before_payload(void) {
    const uint8_t input[] = {TREV_H3_FRAME_HEADERS, 0x03, 0xaa, 0xbb, 0xcc};
    trevrpc_h3_frame_prefix_parser parser;
    CHECK(trevrpc_h3_frame_prefix_parser_init(&parser, 3) == TREV_H3_FRAME_NEED_INPUT);

    size_t consumed = 99;
    trevrpc_h3_frame_prefix prefix = sentinel_prefix();
    CHECK(trevrpc_h3_frame_prefix_parser_feed(&parser, input, sizeof(input), &consumed, &prefix) == TREV_H3_FRAME_OK);
    CHECK(consumed == 2);
    CHECK(prefix.type == TREV_H3_FRAME_HEADERS && prefix.length == 3);
    CHECK(parser.phase == TREV_H3_FRAME_PREFIX_READY);

    prefix = sentinel_prefix();
    consumed = 99;
    CHECK(trevrpc_h3_frame_prefix_parser_feed(&parser, input + 2, 3, &consumed, &prefix) == TREV_H3_FRAME_OK);
    CHECK(consumed == 0);
    CHECK(prefix_is_sentinel(&prefix));
    CHECK(trevrpc_h3_frame_prefix_parser_finish(&parser) == TREV_H3_FRAME_OK);
    return 0;
}

static int test_prefix_byte_at_a_time_and_nonminimal(void) {
    uint8_t input[16];
    size_t input_len = 0;
    input_len += encode_varint_width(TREV_H3_FRAME_HEADERS, 8, input + input_len);
    input_len += encode_varint_width(63, 8, input + input_len);

    trevrpc_h3_frame_prefix_parser parser;
    CHECK(trevrpc_h3_frame_prefix_parser_init(&parser, 63) == TREV_H3_FRAME_NEED_INPUT);
    trevrpc_h3_frame_prefix prefix = sentinel_prefix();
    for (size_t i = 0; i < input_len; i++) {
        size_t consumed = 99;
        trevrpc_h3_frame_status expected = i + 1 == input_len ? TREV_H3_FRAME_OK : TREV_H3_FRAME_NEED_INPUT;
        CHECK(trevrpc_h3_frame_prefix_parser_feed(&parser, input + i, 1, &consumed, &prefix) == expected);
        CHECK(consumed == 1);
        if (expected == TREV_H3_FRAME_NEED_INPUT) {
            CHECK(prefix_is_sentinel(&prefix));
        }
    }
    CHECK(prefix.type == TREV_H3_FRAME_HEADERS && prefix.length == 63);

    CHECK(trevrpc_h3_frame_prefix_parser_reset(&parser) == TREV_H3_FRAME_NEED_INPUT);
    uint8_t mixed[6];
    size_t mixed_len = 0;
    mixed_len += encode_varint_width(1, 2, mixed + mixed_len);
    mixed_len += encode_varint_width(2, 4, mixed + mixed_len);
    size_t consumed = 99;
    prefix = sentinel_prefix();
    CHECK(trevrpc_h3_frame_prefix_parser_feed(&parser, mixed, mixed_len, &consumed, &prefix) == TREV_H3_FRAME_OK);
    CHECK(consumed == mixed_len);
    CHECK(prefix.type == 1 && prefix.length == 2);
    return 0;
}

static int test_prefix_finish_truncation_points(void) {
    trevrpc_h3_frame_prefix_parser parser;
    CHECK(trevrpc_h3_frame_prefix_parser_init(&parser, TREV_QUIC_VARINT_MAX) == TREV_H3_FRAME_NEED_INPUT);
    CHECK(trevrpc_h3_frame_prefix_parser_finish(&parser) == TREV_H3_FRAME_CLEAN_EOF);

    const size_t widths[] = {2, 4, 8};
    for (size_t width_index = 0; width_index < sizeof(widths) / sizeof(widths[0]); width_index++) {
        size_t width = widths[width_index];
        uint8_t encoded[8];
        encode_varint_width(1, width, encoded);
        for (size_t truncation = 1; truncation < width; truncation++) {
            CHECK(trevrpc_h3_frame_prefix_parser_reset(&parser) == TREV_H3_FRAME_NEED_INPUT);
            size_t consumed = 99;
            trevrpc_h3_frame_prefix prefix = sentinel_prefix();
            CHECK(trevrpc_h3_frame_prefix_parser_feed(&parser, encoded, truncation, &consumed, &prefix) ==
                  TREV_H3_FRAME_NEED_INPUT);
            CHECK(consumed == truncation && prefix_is_sentinel(&prefix));
            CHECK(trevrpc_h3_frame_prefix_parser_finish(&parser) == TREV_H3_FRAME_TRUNCATED_TYPE);
            CHECK(trevrpc_h3_frame_prefix_parser_finish(&parser) == TREV_H3_FRAME_TRUNCATED_TYPE);
        }
    }

    const uint8_t type_only[] = {TREV_H3_FRAME_HEADERS};
    CHECK(trevrpc_h3_frame_prefix_parser_reset(&parser) == TREV_H3_FRAME_NEED_INPUT);
    size_t consumed = 99;
    trevrpc_h3_frame_prefix prefix = sentinel_prefix();
    CHECK(trevrpc_h3_frame_prefix_parser_feed(&parser, type_only, sizeof(type_only), &consumed, &prefix) ==
          TREV_H3_FRAME_NEED_INPUT);
    CHECK(trevrpc_h3_frame_prefix_parser_finish(&parser) == TREV_H3_FRAME_TYPE_WITHOUT_LENGTH);

    for (size_t width_index = 0; width_index < sizeof(widths) / sizeof(widths[0]); width_index++) {
        size_t width = widths[width_index];
        uint8_t input[9] = {TREV_H3_FRAME_HEADERS};
        encode_varint_width(1, width, input + 1);
        for (size_t truncation = 1; truncation < width; truncation++) {
            CHECK(trevrpc_h3_frame_prefix_parser_reset(&parser) == TREV_H3_FRAME_NEED_INPUT);
            consumed = 99;
            prefix = sentinel_prefix();
            CHECK(trevrpc_h3_frame_prefix_parser_feed(&parser, input, 1 + truncation, &consumed, &prefix) ==
                  TREV_H3_FRAME_NEED_INPUT);
            CHECK(consumed == 1 + truncation && prefix_is_sentinel(&prefix));
            CHECK(trevrpc_h3_frame_prefix_parser_finish(&parser) == TREV_H3_FRAME_TRUNCATED_LENGTH);
        }
    }
    return 0;
}

static int test_prefix_payload_bound_and_failure_atomicity(void) {
    const uint8_t exact[] = {TREV_H3_FRAME_DATA, 0x40, 0x64};
    const uint8_t excessive[] = {TREV_H3_FRAME_DATA, 0x40, 0x65};
    trevrpc_h3_frame_prefix_parser parser;
    CHECK(trevrpc_h3_frame_prefix_parser_init(&parser, 100) == TREV_H3_FRAME_NEED_INPUT);

    size_t consumed = 99;
    trevrpc_h3_frame_prefix prefix = sentinel_prefix();
    CHECK(trevrpc_h3_frame_prefix_parser_feed(&parser, exact, sizeof(exact), &consumed, &prefix) == TREV_H3_FRAME_OK);
    CHECK(consumed == sizeof(exact) && prefix.length == 100);

    CHECK(trevrpc_h3_frame_prefix_parser_reset(&parser) == TREV_H3_FRAME_NEED_INPUT);
    prefix = sentinel_prefix();
    CHECK(trevrpc_h3_frame_prefix_parser_feed(&parser, excessive, sizeof(excessive), &consumed, &prefix) ==
          TREV_H3_FRAME_EXCESSIVE_LOAD);
    CHECK(consumed == sizeof(excessive));
    CHECK(prefix_is_sentinel(&prefix));
    CHECK(parser.phase == TREV_H3_FRAME_PREFIX_FAILED);
    consumed = 99;
    CHECK(trevrpc_h3_frame_prefix_parser_feed(&parser, exact, sizeof(exact), &consumed, &prefix) ==
          TREV_H3_FRAME_EXCESSIVE_LOAD);
    CHECK(consumed == 0 && prefix_is_sentinel(&prefix));
    CHECK(trevrpc_h3_frame_prefix_parser_finish(&parser) == TREV_H3_FRAME_EXCESSIVE_LOAD);
    return 0;
}

static int test_prefix_builder_widths(void) {
    const uint64_t values[] = {0x3f, 0x40, 0x4000, UINT64_C(0x40000000)};
    const size_t widths[] = {1, 2, 4, 8};
    for (size_t i = 0; i < sizeof(values) / sizeof(values[0]); i++) {
        uint8_t output[16] = {0};
        size_t output_len = 99;
        CHECK(trevrpc_h3_frame_prefix_build(values[i], values[i], output, widths[i] * 2, &output_len) ==
              TREV_H3_FRAME_OK);
        CHECK(output_len == widths[i] * 2);
        CHECK(trevrpc_quic_varint_size_from_first(output[0]) == widths[i]);
        CHECK(trevrpc_quic_varint_size_from_first(output[widths[i]]) == widths[i]);

        trevrpc_h3_frame_prefix_parser parser;
        CHECK(trevrpc_h3_frame_prefix_parser_init(&parser, TREV_QUIC_VARINT_MAX) == TREV_H3_FRAME_NEED_INPUT);
        size_t consumed = 99;
        trevrpc_h3_frame_prefix prefix = sentinel_prefix();
        CHECK(trevrpc_h3_frame_prefix_parser_feed(&parser, output, output_len, &consumed, &prefix) == TREV_H3_FRAME_OK);
        CHECK(consumed == output_len && prefix.type == values[i] && prefix.length == values[i]);
    }
    return 0;
}

static int test_prefix_builder_atomic_failures(void) {
    uint8_t output[16];
    memset(output, 0xa5, sizeof(output));
    uint8_t before[16];
    memcpy(before, output, sizeof(before));
    size_t output_len = 0xaaaa;

    CHECK(trevrpc_h3_frame_prefix_build(0x40, 0x40, output, 3, &output_len) == TREV_H3_FRAME_OUTPUT_TOO_SMALL);
    CHECK(memcmp(output, before, sizeof(output)) == 0);
    CHECK(output_len == 0xaaaa);
    CHECK(trevrpc_h3_frame_prefix_build(0, 0, NULL, 0, &output_len) == TREV_H3_FRAME_OUTPUT_TOO_SMALL);
    CHECK(output_len == 0xaaaa);

    CHECK(trevrpc_h3_frame_prefix_build(TREV_QUIC_VARINT_MAX + 1, 0, output, sizeof(output), &output_len) ==
          TREV_H3_FRAME_INVALID_ARGUMENT);
    CHECK(memcmp(output, before, sizeof(output)) == 0 && output_len == 0xaaaa);
    CHECK(trevrpc_h3_frame_prefix_build(0, TREV_QUIC_VARINT_MAX + 1, output, sizeof(output), &output_len) ==
          TREV_H3_FRAME_INVALID_ARGUMENT);
    CHECK(memcmp(output, before, sizeof(output)) == 0 && output_len == 0xaaaa);
    return 0;
}

static int test_unknown_discard_budget(void) {
    trevrpc_h3_unknown_discard_budget budget;
    CHECK(trevrpc_h3_unknown_discard_budget_init(NULL, 10) == TREV_H3_FRAME_INVALID_ARGUMENT);
    CHECK(trevrpc_h3_unknown_discard_budget_charge(NULL, 1) == TREV_H3_FRAME_INVALID_ARGUMENT);

    CHECK(trevrpc_h3_unknown_discard_budget_init(&budget, 0) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, 0) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, 1) == TREV_H3_FRAME_EXCESSIVE_LOAD);
    CHECK(budget.used == 0);

    CHECK(trevrpc_h3_unknown_discard_budget_init(&budget, 10) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, 0) == TREV_H3_FRAME_OK);
    CHECK(budget.used == 0);
    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, 4) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, 6) == TREV_H3_FRAME_OK);
    CHECK(budget.used == 10);
    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, 1) == TREV_H3_FRAME_EXCESSIVE_LOAD);
    CHECK(budget.used == 10);

    CHECK(trevrpc_h3_unknown_discard_budget_init(&budget, UINT64_MAX) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, UINT64_MAX) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, 1) == TREV_H3_FRAME_EXCESSIVE_LOAD);
    CHECK(budget.used == UINT64_MAX);
    return 0;
}

static int test_request_ordering_and_payload(void) {
    trevrpc_h3_request_frame_state state;
    CHECK(trevrpc_h3_request_frame_state_init(&state, false, 8, 20) == TREV_H3_FRAME_OK);
    trevrpc_h3_request_payload_kind kind = TREV_H3_REQUEST_PAYLOAD_NONE;

    trevrpc_h3_frame_prefix unknown = {.type = 0x21, .length = 2};
    CHECK(trevrpc_h3_request_frame_begin(&state, &unknown, &kind) == TREV_H3_FRAME_OK);
    CHECK(kind == TREV_H3_REQUEST_PAYLOAD_UNKNOWN && state.phase == TREV_H3_REQUEST_BEFORE_HEADERS);
    CHECK(state.payload_remaining == 2 && state.unknown_discard.used == 2);
    CHECK(trevrpc_h3_request_frame_consume(&state, 1) == TREV_H3_FRAME_OK);
    trevrpc_h3_frame_prefix headers = {.type = TREV_H3_FRAME_HEADERS, .length = 3};
    kind = TREV_H3_REQUEST_PAYLOAD_NONE;
    CHECK(trevrpc_h3_request_frame_begin(&state, &headers, &kind) == TREV_H3_FRAME_PAYLOAD_ACTIVE);
    CHECK(kind == TREV_H3_REQUEST_PAYLOAD_NONE && state.phase == TREV_H3_REQUEST_BEFORE_HEADERS);
    CHECK(trevrpc_h3_request_frame_consume(&state, 2) == TREV_H3_FRAME_PAYLOAD_OVERCONSUMED);
    CHECK(state.payload_remaining == 1 && state.active_payload == TREV_H3_REQUEST_PAYLOAD_UNKNOWN);
    CHECK(trevrpc_h3_request_frame_consume(&state, 1) == TREV_H3_FRAME_OK);

    trevrpc_h3_frame_prefix data = {.type = TREV_H3_FRAME_DATA, .length = 1};
    CHECK(trevrpc_h3_request_frame_begin(&state, &data, &kind) == TREV_H3_FRAME_UNEXPECTED);
    CHECK(state.phase == TREV_H3_REQUEST_BEFORE_HEADERS);

    CHECK(trevrpc_h3_request_frame_begin(&state, &headers, &kind) == TREV_H3_FRAME_OK);
    CHECK(kind == TREV_H3_REQUEST_PAYLOAD_HEADERS && state.phase == TREV_H3_REQUEST_BODY);
    CHECK(trevrpc_h3_request_frame_consume(&state, 3) == TREV_H3_FRAME_OK);
    unknown.length = 0;
    CHECK(trevrpc_h3_request_frame_begin(&state, &unknown, &kind) == TREV_H3_FRAME_OK);
    CHECK(state.phase == TREV_H3_REQUEST_BODY && state.unknown_discard.used == 2);

    data.length = 4;
    CHECK(trevrpc_h3_request_frame_begin(&state, &data, &kind) == TREV_H3_FRAME_OK);
    CHECK(kind == TREV_H3_REQUEST_PAYLOAD_DATA && state.payload_remaining == 4);
    CHECK(trevrpc_h3_request_frame_consume(&state, 4) == TREV_H3_FRAME_OK);

    headers.length = 2;
    CHECK(trevrpc_h3_request_frame_begin(&state, &headers, &kind) == TREV_H3_FRAME_OK);
    CHECK(kind == TREV_H3_REQUEST_PAYLOAD_TRAILERS && state.phase == TREV_H3_REQUEST_TRAILERS);
    CHECK(trevrpc_h3_request_frame_consume(&state, 2) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_request_frame_begin(&state, &unknown, &kind) == TREV_H3_FRAME_OK);
    CHECK(state.phase == TREV_H3_REQUEST_TRAILERS && state.unknown_discard.used == 2);
    CHECK(trevrpc_h3_request_frame_begin(&state, &data, &kind) == TREV_H3_FRAME_UNEXPECTED);
    headers.length = 9;
    CHECK(trevrpc_h3_request_frame_begin(&state, &headers, &kind) == TREV_H3_FRAME_UNEXPECTED);
    CHECK(trevrpc_h3_request_frame_finish(&state) == TREV_H3_FRAME_OK);
    CHECK(state.phase == TREV_H3_REQUEST_COMPLETE);
    CHECK(trevrpc_h3_request_frame_begin(&state, &unknown, &kind) == TREV_H3_FRAME_UNEXPECTED);
    return 0;
}

static int test_request_initialization_after_headers_and_bounds(void) {
    trevrpc_h3_request_frame_state state;
    CHECK(trevrpc_h3_request_frame_state_init(&state, true, 4, 5) == TREV_H3_FRAME_OK);
    CHECK(state.phase == TREV_H3_REQUEST_BODY);
    trevrpc_h3_request_payload_kind kind = TREV_H3_REQUEST_PAYLOAD_NONE;

    trevrpc_h3_frame_prefix data = {.type = TREV_H3_FRAME_DATA, .length = 0};
    CHECK(trevrpc_h3_request_frame_begin(&state, &data, &kind) == TREV_H3_FRAME_OK);
    CHECK(kind == TREV_H3_REQUEST_PAYLOAD_DATA && state.active_payload == TREV_H3_REQUEST_PAYLOAD_NONE);

    trevrpc_h3_frame_prefix headers = {.type = TREV_H3_FRAME_HEADERS, .length = 5};
    kind = TREV_H3_REQUEST_PAYLOAD_NONE;
    CHECK(trevrpc_h3_request_frame_begin(&state, &headers, &kind) == TREV_H3_FRAME_FIELD_SECTION_TOO_LARGE);
    CHECK(kind == TREV_H3_REQUEST_PAYLOAD_NONE && state.phase == TREV_H3_REQUEST_BODY);
    headers.length = 4;
    CHECK(trevrpc_h3_request_frame_begin(&state, &headers, &kind) == TREV_H3_FRAME_OK);
    CHECK(kind == TREV_H3_REQUEST_PAYLOAD_TRAILERS);
    CHECK(trevrpc_h3_request_frame_finish(&state) == TREV_H3_FRAME_TRUNCATED_PAYLOAD);
    CHECK(trevrpc_h3_request_frame_consume(&state, 4) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_request_frame_finish(&state) == TREV_H3_FRAME_OK);

    CHECK(trevrpc_h3_request_frame_state_init(&state, false, 4, 5) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_request_frame_finish(&state) == TREV_H3_FRAME_UNEXPECTED);
    CHECK(state.phase == TREV_H3_REQUEST_BEFORE_HEADERS);
    return 0;
}

static int test_request_prohibited_types(void) {
    const uint64_t prohibited[] = {
        TREV_H3_FRAME_SETTINGS,
        TREV_H3_FRAME_CANCEL_PUSH,
        TREV_H3_FRAME_PUSH_PROMISE,
        TREV_H3_FRAME_GOAWAY,
        TREV_H3_FRAME_MAX_PUSH_ID,
        0x02,
        0x06,
        0x08,
        0x09,
    };
    for (size_t i = 0; i < sizeof(prohibited) / sizeof(prohibited[0]); i++) {
        trevrpc_h3_request_frame_state state;
        CHECK(trevrpc_h3_request_frame_state_init(&state, true, 8, 8) == TREV_H3_FRAME_OK);
        trevrpc_h3_frame_prefix prefix = {.type = prohibited[i], .length = 0};
        trevrpc_h3_request_payload_kind kind = TREV_H3_REQUEST_PAYLOAD_DATA;
        CHECK(trevrpc_h3_request_frame_begin(&state, &prefix, &kind) == TREV_H3_FRAME_UNEXPECTED);
        CHECK(kind == TREV_H3_REQUEST_PAYLOAD_DATA && state.phase == TREV_H3_REQUEST_BODY);
    }
    return 0;
}

static int test_request_unknown_cumulative_budget(void) {
    trevrpc_h3_request_frame_state state;
    CHECK(trevrpc_h3_request_frame_state_init(&state, true, 8, 5) == TREV_H3_FRAME_OK);
    trevrpc_h3_request_payload_kind kind = TREV_H3_REQUEST_PAYLOAD_NONE;
    trevrpc_h3_frame_prefix unknown = {.type = 0x21, .length = 0};
    CHECK(trevrpc_h3_request_frame_begin(&state, &unknown, &kind) == TREV_H3_FRAME_OK);
    CHECK(state.unknown_discard.used == 0 && state.active_payload == TREV_H3_REQUEST_PAYLOAD_NONE);

    unknown.length = 2;
    CHECK(trevrpc_h3_request_frame_begin(&state, &unknown, &kind) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_request_frame_consume(&state, 2) == TREV_H3_FRAME_OK);
    unknown.length = 3;
    CHECK(trevrpc_h3_request_frame_begin(&state, &unknown, &kind) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_request_frame_consume(&state, 3) == TREV_H3_FRAME_OK);
    CHECK(state.unknown_discard.used == 5);

    unknown.length = 1;
    kind = TREV_H3_REQUEST_PAYLOAD_DATA;
    CHECK(trevrpc_h3_request_frame_begin(&state, &unknown, &kind) == TREV_H3_FRAME_EXCESSIVE_LOAD);
    CHECK(kind == TREV_H3_REQUEST_PAYLOAD_DATA);
    CHECK(state.unknown_discard.used == 5 && state.active_payload == TREV_H3_REQUEST_PAYLOAD_NONE);
    return 0;
}

static int test_error_mapping(void) {
    CHECK(trevrpc_h3_frame_status_error_code(TREV_H3_FRAME_TRUNCATED_TYPE) == TREV_H3_FRAME_ERROR_MALFORMED);
    CHECK(trevrpc_h3_frame_status_error_code(TREV_H3_FRAME_TYPE_WITHOUT_LENGTH) == TREV_H3_FRAME_ERROR_MALFORMED);
    CHECK(trevrpc_h3_frame_status_error_code(TREV_H3_FRAME_TRUNCATED_LENGTH) == TREV_H3_FRAME_ERROR_MALFORMED);
    CHECK(trevrpc_h3_frame_status_error_code(TREV_H3_FRAME_TRUNCATED_PAYLOAD) == TREV_H3_FRAME_ERROR_MALFORMED);
    CHECK(trevrpc_h3_frame_status_error_code(TREV_H3_FRAME_UNEXPECTED) == TREV_H3_FRAME_ERROR_UNEXPECTED);
    CHECK(trevrpc_h3_frame_status_error_code(TREV_H3_FRAME_EXCESSIVE_LOAD) == TREV_H3_FRAME_ERROR_EXCESSIVE_LOAD);
    CHECK(trevrpc_h3_frame_status_error_code(TREV_H3_FRAME_FIELD_SECTION_TOO_LARGE) ==
          TREV_H3_FRAME_ERROR_EXCESSIVE_LOAD);
    CHECK(trevrpc_h3_frame_status_error_code(TREV_H3_FRAME_OK) == 0);
    CHECK(trevrpc_h3_frame_status_error_code(TREV_H3_FRAME_OUTPUT_TOO_SMALL) == 0);
    return 0;
}

int main(void) {
    int (*const tests[])(void) = {
        test_reserved_type_predicate,
        test_prefix_coalesced_stops_before_payload,
        test_prefix_byte_at_a_time_and_nonminimal,
        test_prefix_finish_truncation_points,
        test_prefix_payload_bound_and_failure_atomicity,
        test_prefix_builder_widths,
        test_prefix_builder_atomic_failures,
        test_unknown_discard_budget,
        test_request_ordering_and_payload,
        test_request_initialization_after_headers_and_bounds,
        test_request_prohibited_types,
        test_request_unknown_cumulative_budget,
        test_error_mapping,
    };
    for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
        int err = tests[i]();
        if (err != 0) {
            return err;
        }
    }
    return 0;
}
