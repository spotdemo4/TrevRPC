#include "trevrpc_http3_settings_internal.h"
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

typedef struct visitor_state {
    uint64_t ids[4];
    uint64_t values[4];
    size_t count;
    uint64_t reject_id;
} visitor_state;

static int record_setting(void* context, uint64_t id, uint64_t value) {
    visitor_state* state = context;
    if (id == state->reject_id) {
        return -1;
    }
    if (state->count < sizeof(state->ids) / sizeof(state->ids[0])) {
        state->ids[state->count] = id;
        state->values[state->count] = value;
    }
    state->count++;
    return 0;
}

static trevrpc_h3_settings_report sentinel_report(void) {
    return (trevrpc_h3_settings_report){
        .payload_len = 0xaaaa,
        .pair_count = 0xbbbb,
    };
}

static int report_is_sentinel(const trevrpc_h3_settings_report* report) {
    return report->payload_len == 0xaaaa && report->pair_count == 0xbbbb;
}

static int test_legal_settings(void) {
    const uint8_t payload[] = {
        0x21,
        0x01, /* GREASE */
        0x40,
        0x40,
        0x02, /* unknown setting 64 */
    };
    uint64_t seen_ids[2] = {0};
    visitor_state state = {.reject_id = UINT64_MAX};
    trevrpc_h3_settings_report report = sentinel_report();

    CHECK(trevrpc_h3_settings_parse(payload,
              sizeof(payload),
              sizeof(payload),
              seen_ids,
              sizeof(seen_ids) / sizeof(seen_ids[0]),
              record_setting,
              &state,
              &report) == TREV_H3_SETTINGS_OK);
    CHECK(report.payload_len == sizeof(payload));
    CHECK(report.pair_count == 2);
    CHECK(seen_ids[0] == 0x21);
    CHECK(seen_ids[1] == 0x40);
    CHECK(state.count == 2);
    CHECK(state.ids[0] == 0x21 && state.values[0] == 1);
    CHECK(state.ids[1] == 0x40 && state.values[1] == 2);

    report = sentinel_report();
    CHECK(trevrpc_h3_settings_parse(NULL, 0, 0, NULL, 0, NULL, NULL, &report) == TREV_H3_SETTINGS_OK);
    CHECK(report.payload_len == 0);
    CHECK(report.pair_count == 0);
    return 0;
}

static int test_nonminimal_varints(void) {
    const uint8_t payload[] = {
        0x40,
        0x01, /* ID 1 in two bytes */
        0x80,
        0x00,
        0x00,
        0x01, /* value 1 in four bytes */
    };
    uint64_t seen_ids[1] = {0};
    visitor_state state = {.reject_id = UINT64_MAX};
    trevrpc_h3_settings_report report = sentinel_report();

    CHECK(trevrpc_h3_settings_parse(
              payload, sizeof(payload), sizeof(payload), seen_ids, 1, record_setting, &state, &report) ==
          TREV_H3_SETTINGS_OK);
    CHECK(report.pair_count == 1);
    CHECK(state.count == 1);
    CHECK(state.ids[0] == 1 && state.values[0] == 1);
    return 0;
}

static int test_malformed_is_atomic(void) {
    const uint8_t truncated_id[] = {0x40};
    const uint8_t missing_value[] = {0x01};
    uint64_t seen_ids[1] = {0};
    trevrpc_h3_settings_report report = sentinel_report();

    CHECK(trevrpc_h3_settings_parse(
              truncated_id, sizeof(truncated_id), sizeof(truncated_id), seen_ids, 1, NULL, NULL, &report) ==
          TREV_H3_SETTINGS_MALFORMED);
    CHECK(report_is_sentinel(&report));

    CHECK(trevrpc_h3_settings_parse(
              missing_value, sizeof(missing_value), sizeof(missing_value), seen_ids, 1, NULL, NULL, &report) ==
          TREV_H3_SETTINGS_MALFORMED);
    CHECK(report_is_sentinel(&report));
    return 0;
}

static int test_duplicate_ids(void) {
    const uint8_t duplicate_unknown[] = {0x1e, 0x01, 0x1e, 0x02};
    const uint8_t duplicate_grease[] = {0x21, 0x01, 0x21, 0x02};
    uint64_t seen_ids[2] = {0};
    trevrpc_h3_settings_report report = sentinel_report();

    CHECK(trevrpc_h3_settings_parse(duplicate_unknown,
              sizeof(duplicate_unknown),
              sizeof(duplicate_unknown),
              seen_ids,
              2,
              NULL,
              NULL,
              &report) == TREV_H3_SETTINGS_DUPLICATE);
    CHECK(report_is_sentinel(&report));

    report = sentinel_report();
    CHECK(trevrpc_h3_settings_parse(
              duplicate_grease, sizeof(duplicate_grease), sizeof(duplicate_grease), seen_ids, 2, NULL, NULL, &report) ==
          TREV_H3_SETTINGS_DUPLICATE);
    CHECK(report_is_sentinel(&report));
    return 0;
}

static int test_reserved_ids(void) {
    uint64_t seen_ids[1] = {0};
    const uint8_t reserved_zero[] = {0x00, 0x01};
    trevrpc_h3_settings_report report = sentinel_report();
    CHECK(trevrpc_h3_settings_parse(
              reserved_zero, sizeof(reserved_zero), sizeof(reserved_zero), seen_ids, 1, NULL, NULL, &report) ==
          TREV_H3_SETTINGS_RESERVED);
    CHECK(report_is_sentinel(&report));

    for (uint8_t id = 0x02; id <= 0x05; id++) {
        const uint8_t payload[] = {id, 0x01};
        report = sentinel_report();
        CHECK(trevrpc_h3_settings_parse(payload, sizeof(payload), sizeof(payload), seen_ids, 1, NULL, NULL, &report) ==
              TREV_H3_SETTINGS_RESERVED);
        CHECK(report_is_sentinel(&report));
    }
    return 0;
}

static int test_invalid_recognized_value(void) {
    const uint8_t payload[] = {0x08, 0x02};
    uint64_t seen_ids[1] = {0};
    visitor_state state = {.reject_id = 0x08};
    trevrpc_h3_settings_report report = sentinel_report();

    CHECK(trevrpc_h3_settings_parse(
              payload, sizeof(payload), sizeof(payload), seen_ids, 1, record_setting, &state, &report) ==
          TREV_H3_SETTINGS_INVALID_VALUE);
    CHECK(report_is_sentinel(&report));
    return 0;
}

static int test_bounds_are_atomic(void) {
    const uint8_t payload[] = {0x01, 0x01, 0x06, 0x01};
    uint64_t seen_ids[2] = {0};
    trevrpc_h3_settings_report report = sentinel_report();

    CHECK(trevrpc_h3_settings_parse(payload, sizeof(payload), sizeof(payload) - 1, seen_ids, 2, NULL, NULL, &report) ==
          TREV_H3_SETTINGS_EXCESSIVE_LOAD);
    CHECK(report_is_sentinel(&report));

    CHECK(trevrpc_h3_settings_parse(payload, sizeof(payload), sizeof(payload), seen_ids, 1, NULL, NULL, &report) ==
          TREV_H3_SETTINGS_WORKSPACE_EXHAUSTED);
    CHECK(report_is_sentinel(&report));
    return 0;
}

static int test_invalid_arguments_are_atomic(void) {
    const uint8_t payload[] = {0x01, 0x01};
    uint64_t seen_ids[1] = {0};
    trevrpc_h3_settings_report report = sentinel_report();

    CHECK(trevrpc_h3_settings_parse(NULL, 1, 1, seen_ids, 1, NULL, NULL, &report) == TREV_H3_SETTINGS_INVALID_ARGUMENT);
    CHECK(report_is_sentinel(&report));
    CHECK(trevrpc_h3_settings_parse(payload, sizeof(payload), sizeof(payload), NULL, 1, NULL, NULL, &report) ==
          TREV_H3_SETTINGS_INVALID_ARGUMENT);
    CHECK(report_is_sentinel(&report));
    CHECK(trevrpc_h3_settings_parse(payload, sizeof(payload), sizeof(payload), seen_ids, 1, NULL, NULL, NULL) ==
          TREV_H3_SETTINGS_INVALID_ARGUMENT);
    return 0;
}

static int test_builder_empty_payload(void) {
    size_t output_len = 0xaaaa;
    CHECK(trevrpc_h3_settings_build(NULL, 0, NULL, 0, &output_len) == TREV_H3_SETTINGS_OK);
    CHECK(output_len == 0);
    return 0;
}

static int test_builder_varint_widths(void) {
    const trevrpc_h3_settings_pair pairs[] = {
        {.id = UINT64_C(0x40000000), .value = UINT64_C(0x40000000)},
        {.id = 0x4000, .value = 0x4000},
        {.id = 0x40, .value = 0x40},
        {.id = 0x01, .value = 0x3f},
    };
    uint8_t output[30] = {0};
    size_t output_len = 0xaaaa;
    CHECK(trevrpc_h3_settings_build(pairs, sizeof(pairs) / sizeof(pairs[0]), output, sizeof(output), &output_len) ==
          TREV_H3_SETTINGS_OK);
    CHECK(output_len == sizeof(output));

    size_t offset = 0;
    const uint64_t expected_ids[] = {0x01, 0x40, 0x4000, UINT64_C(0x40000000)};
    const uint64_t expected_values[] = {0x3f, 0x40, 0x4000, UINT64_C(0x40000000)};
    const size_t expected_widths[] = {1, 2, 4, 8};
    for (size_t i = 0; i < sizeof(expected_ids) / sizeof(expected_ids[0]); i++) {
        size_t pair_start = offset;
        uint64_t id = 0;
        uint64_t value = 0;
        CHECK(trevrpc_quic_varint_read(output, output_len, &offset, &id) == 0);
        CHECK(trevrpc_quic_varint_size_from_first(output[pair_start]) == expected_widths[i]);
        size_t value_start = offset;
        CHECK(trevrpc_quic_varint_read(output, output_len, &offset, &value) == 0);
        CHECK(trevrpc_quic_varint_size_from_first(output[value_start]) == expected_widths[i]);
        CHECK(id == expected_ids[i] && value == expected_values[i]);
    }
    CHECK(offset == output_len);
    return 0;
}

static int test_builder_deterministic_order(void) {
    const trevrpc_h3_settings_pair first[] = {
        {.id = 0x21, .value = 7},
        {.id = 0x01, .value = 3},
        {.id = 0x40, .value = 9},
    };
    const trevrpc_h3_settings_pair second[] = {
        {.id = 0x40, .value = 9},
        {.id = 0x21, .value = 7},
        {.id = 0x01, .value = 3},
    };
    uint8_t first_output[16] = {0};
    uint8_t second_output[16] = {0};
    size_t first_len = 0;
    size_t second_len = 0;
    CHECK(trevrpc_h3_settings_build(first, 3, first_output, sizeof(first_output), &first_len) == TREV_H3_SETTINGS_OK);
    CHECK(
        trevrpc_h3_settings_build(second, 3, second_output, sizeof(second_output), &second_len) == TREV_H3_SETTINGS_OK);
    CHECK(first_len == second_len);
    CHECK(memcmp(first_output, second_output, first_len) == 0);
    return 0;
}

static int builder_failure_preserves(
    const trevrpc_h3_settings_pair* pairs, size_t pair_count, size_t capacity, trevrpc_h3_settings_status expected) {
    uint8_t output[32];
    memset(output, 0xa5, sizeof(output));
    uint8_t before[32];
    memcpy(before, output, sizeof(before));
    size_t output_len = 0xaaaa;
    CHECK(trevrpc_h3_settings_build(pairs, pair_count, output, capacity, &output_len) == expected);
    CHECK(memcmp(output, before, sizeof(output)) == 0);
    CHECK(output_len == 0xaaaa);
    return 0;
}

static int test_builder_atomic_failures(void) {
    const trevrpc_h3_settings_pair valid[] = {
        {.id = 0x01, .value = 0x01},
        {.id = 0x40, .value = 0x40},
    };
    CHECK(builder_failure_preserves(valid, 2, 5, TREV_H3_SETTINGS_OUTPUT_TOO_SMALL) == 0);

    const trevrpc_h3_settings_pair duplicate_unknown[] = {
        {.id = 0x1e, .value = 1},
        {.id = 0x1e, .value = 2},
    };
    CHECK(builder_failure_preserves(duplicate_unknown, 2, 32, TREV_H3_SETTINGS_DUPLICATE) == 0);
    const trevrpc_h3_settings_pair duplicate_grease[] = {
        {.id = 0x21, .value = 1},
        {.id = 0x21, .value = 2},
    };
    CHECK(builder_failure_preserves(duplicate_grease, 2, 32, TREV_H3_SETTINGS_DUPLICATE) == 0);

    trevrpc_h3_settings_pair reserved_zero = {.id = 0x00, .value = 1};
    CHECK(builder_failure_preserves(&reserved_zero, 1, 32, TREV_H3_SETTINGS_RESERVED) == 0);
    for (uint64_t id = 0x02; id <= 0x05; id++) {
        trevrpc_h3_settings_pair reserved = {.id = id, .value = 1};
        CHECK(builder_failure_preserves(&reserved, 1, 32, TREV_H3_SETTINGS_RESERVED) == 0);
    }

    trevrpc_h3_settings_pair invalid_id = {.id = TREV_QUIC_VARINT_MAX + 1, .value = 1};
    CHECK(builder_failure_preserves(&invalid_id, 1, 32, TREV_H3_SETTINGS_OUT_OF_RANGE) == 0);
    trevrpc_h3_settings_pair invalid_value = {.id = 1, .value = TREV_QUIC_VARINT_MAX + 1};
    CHECK(builder_failure_preserves(&invalid_value, 1, 32, TREV_H3_SETTINGS_OUT_OF_RANGE) == 0);
    return 0;
}

static int test_builder_invalid_arguments(void) {
    uint8_t output[2] = {0xa5, 0xa5};
    size_t output_len = 0xaaaa;
    CHECK(trevrpc_h3_settings_build(NULL, 1, output, sizeof(output), &output_len) == TREV_H3_SETTINGS_INVALID_ARGUMENT);
    CHECK(output[0] == 0xa5 && output[1] == 0xa5 && output_len == 0xaaaa);
    CHECK(trevrpc_h3_settings_build(NULL, 0, output, sizeof(output), NULL) == TREV_H3_SETTINGS_INVALID_ARGUMENT);
    return 0;
}

static int test_error_mapping(void) {
    CHECK(trevrpc_h3_settings_status_error_code(TREV_H3_SETTINGS_MALFORMED) == TREV_H3_ERROR_FRAME_ERROR);
    CHECK(trevrpc_h3_settings_status_error_code(TREV_H3_SETTINGS_DUPLICATE) == TREV_H3_ERROR_SETTINGS_ERROR);
    CHECK(trevrpc_h3_settings_status_error_code(TREV_H3_SETTINGS_RESERVED) == TREV_H3_ERROR_SETTINGS_ERROR);
    CHECK(trevrpc_h3_settings_status_error_code(TREV_H3_SETTINGS_INVALID_VALUE) == TREV_H3_ERROR_SETTINGS_ERROR);
    CHECK(trevrpc_h3_settings_status_error_code(TREV_H3_SETTINGS_EXCESSIVE_LOAD) == TREV_H3_ERROR_EXCESSIVE_LOAD);
    CHECK(trevrpc_h3_settings_status_error_code(TREV_H3_SETTINGS_WORKSPACE_EXHAUSTED) == TREV_H3_ERROR_EXCESSIVE_LOAD);
    CHECK(trevrpc_h3_settings_status_error_code(TREV_H3_SETTINGS_OK) == 0);
    CHECK(trevrpc_h3_settings_status_error_code(TREV_H3_SETTINGS_INVALID_ARGUMENT) == 0);
    CHECK(trevrpc_h3_settings_status_error_code(TREV_H3_SETTINGS_OUT_OF_RANGE) == 0);
    CHECK(trevrpc_h3_settings_status_error_code(TREV_H3_SETTINGS_OUTPUT_TOO_SMALL) == 0);
    CHECK(trevrpc_h3_settings_status_error_code(TREV_H3_SETTINGS_SIZE_OVERFLOW) == 0);
    CHECK(trevrpc_h3_settings_status_error_code((trevrpc_h3_settings_status)99) == 0);
    return 0;
}

int main(void) {
    int (*const tests[])(void) = {
        test_legal_settings,
        test_nonminimal_varints,
        test_malformed_is_atomic,
        test_duplicate_ids,
        test_reserved_ids,
        test_invalid_recognized_value,
        test_bounds_are_atomic,
        test_invalid_arguments_are_atomic,
        test_builder_empty_payload,
        test_builder_varint_widths,
        test_builder_deterministic_order,
        test_builder_atomic_failures,
        test_builder_invalid_arguments,
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
