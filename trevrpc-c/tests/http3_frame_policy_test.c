#include "../include/trevrpc.h"
#include "../src/trevrpc_http3_frame_internal.h"

#include <stdint.h>
#include <stdio.h>

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            return 1;                                                                                                  \
        }                                                                                                              \
    } while (0)

int trevrpc_h3_test_frame_policy(size_t max_frame_size,
    uint64_t* max_data_payload,
    uint64_t* max_control_unknown_discard,
    uint64_t* max_request_unknown_discard,
    uint64_t* max_settings_payload,
    uint64_t* max_encoded_field_section);

static int test_default_max_frame_size_is_normalized(void) {
    uint64_t data = 0;
    uint64_t control_unknown = 0;
    uint64_t request_unknown = 0;
    uint64_t settings = 0;
    uint64_t headers = 0;
    CHECK(trevrpc_h3_test_frame_policy(0, &data, &control_unknown, &request_unknown, &settings, &headers) == 0);
    CHECK(data == TREVRPC_DEFAULT_MAX_FRAME_SIZE);
    CHECK(control_unknown == TREVRPC_DEFAULT_MAX_FRAME_SIZE);
    CHECK(request_unknown == TREVRPC_DEFAULT_MAX_FRAME_SIZE);
    CHECK(settings == 4096);
    CHECK(headers == 16 * 1024);
    return 0;
}

static int test_data_and_unknown_limits_are_separate_from_metadata_limits(void) {
    uint64_t data = 0;
    uint64_t control_unknown = 0;
    uint64_t request_unknown = 0;
    uint64_t settings = 0;
    uint64_t headers = 0;
    CHECK(trevrpc_h3_test_frame_policy(64, &data, &control_unknown, &request_unknown, &settings, &headers) == 0);
    CHECK(data == 64);
    CHECK(control_unknown == 64);
    CHECK(request_unknown == 64);
    CHECK(settings == 4096);
    CHECK(headers == 16 * 1024);
    return 0;
}

static int test_unknown_frame_budget_is_cumulative(void) {
    trevrpc_h3_unknown_discard_budget budget;
    CHECK(trevrpc_h3_unknown_discard_budget_init(&budget, 64) == TREV_H3_FRAME_OK);

    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, 20) == TREV_H3_FRAME_OK);
    CHECK(budget.used == 20);

    /* Processing a known frame does not reset the connection/stream budget. */
    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, 44) == TREV_H3_FRAME_OK);
    CHECK(budget.used == 64);

    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, 1) == TREV_H3_FRAME_EXCESSIVE_LOAD);
    CHECK(budget.used == 64);
    return 0;
}

static int test_unknown_frame_budget_rejects_aggregate_below_per_frame_limit(void) {
    trevrpc_h3_unknown_discard_budget budget;
    CHECK(trevrpc_h3_unknown_discard_budget_init(&budget, 64) == TREV_H3_FRAME_OK);

    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, 40) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, 40) == TREV_H3_FRAME_EXCESSIVE_LOAD);
    CHECK(budget.used == 40);
    return 0;
}

static int test_unknown_frame_budget_is_overflow_safe(void) {
    trevrpc_h3_unknown_discard_budget budget;
    CHECK(trevrpc_h3_unknown_discard_budget_init(&budget, UINT64_MAX - 1) == TREV_H3_FRAME_OK);

    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, UINT64_MAX - 2) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, 2) == TREV_H3_FRAME_EXCESSIVE_LOAD);
    CHECK(budget.used == UINT64_MAX - 2);
    return 0;
}

static int test_unknown_frame_budget_validates_arguments(void) {
    trevrpc_h3_unknown_discard_budget budget;
    CHECK(trevrpc_h3_unknown_discard_budget_init(NULL, 64) == TREV_H3_FRAME_INVALID_ARGUMENT);
    CHECK(trevrpc_h3_unknown_discard_budget_charge(NULL, 1) == TREV_H3_FRAME_INVALID_ARGUMENT);

    CHECK(trevrpc_h3_unknown_discard_budget_init(&budget, 0) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, 0) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_unknown_discard_budget_charge(&budget, 1) == TREV_H3_FRAME_EXCESSIVE_LOAD);
    return 0;
}

static int test_request_unknown_budget_survives_valid_data(void) {
    trevrpc_h3_request_frame_state state;
    trevrpc_h3_request_payload_kind kind = TREV_H3_REQUEST_PAYLOAD_NONE;
    CHECK(trevrpc_h3_request_frame_state_init(&state, true, 64, 64) == TREV_H3_FRAME_OK);

    trevrpc_h3_frame_prefix unknown = {.type = 0x21, .length = 20};
    CHECK(trevrpc_h3_request_frame_begin(&state, &unknown, &kind) == TREV_H3_FRAME_OK);
    CHECK(kind == TREV_H3_REQUEST_PAYLOAD_UNKNOWN);
    CHECK(trevrpc_h3_request_frame_consume(&state, 20) == TREV_H3_FRAME_OK);

    trevrpc_h3_frame_prefix data = {.type = TREV_H3_FRAME_DATA, .length = 32};
    CHECK(trevrpc_h3_request_frame_begin(&state, &data, &kind) == TREV_H3_FRAME_OK);
    CHECK(kind == TREV_H3_REQUEST_PAYLOAD_DATA);
    CHECK(trevrpc_h3_request_frame_consume(&state, 32) == TREV_H3_FRAME_OK);
    CHECK(state.unknown_discard.used == 20);

    unknown.type = 0x22;
    unknown.length = 44;
    CHECK(trevrpc_h3_request_frame_begin(&state, &unknown, &kind) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_request_frame_consume(&state, 44) == TREV_H3_FRAME_OK);
    CHECK(state.unknown_discard.used == 64);

    data.length = 1;
    CHECK(trevrpc_h3_request_frame_begin(&state, &data, &kind) == TREV_H3_FRAME_OK);
    CHECK(trevrpc_h3_request_frame_consume(&state, 1) == TREV_H3_FRAME_OK);
    CHECK(state.unknown_discard.used == 64);

    unknown.type = 0x23;
    unknown.length = 1;
    CHECK(trevrpc_h3_request_frame_begin(&state, &unknown, &kind) == TREV_H3_FRAME_EXCESSIVE_LOAD);
    CHECK(state.unknown_discard.used == 64);
    return 0;
}

int main(void) {
    int (*const tests[])(void) = {
        test_default_max_frame_size_is_normalized,
        test_data_and_unknown_limits_are_separate_from_metadata_limits,
        test_unknown_frame_budget_is_cumulative,
        test_unknown_frame_budget_rejects_aggregate_below_per_frame_limit,
        test_unknown_frame_budget_is_overflow_safe,
        test_unknown_frame_budget_validates_arguments,
        test_request_unknown_budget_survives_valid_data,
    };
    for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
        int err = tests[i]();
        if (err != 0) {
            return err;
        }
    }
    return 0;
}
