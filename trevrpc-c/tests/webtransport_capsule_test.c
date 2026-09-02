#include "trevrpc_webtransport_capsule_internal.h"

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

#define ARRAY_LEN(values) (sizeof(values) / sizeof((values)[0]))

static size_t wire_varint(uint8_t* out, uint64_t value, size_t width) {
    switch (width) {
    case 1:
        out[0] = (uint8_t)value;
        break;
    case 2:
        out[0] = (uint8_t)(0x40u | (uint8_t)(value >> 8));
        out[1] = (uint8_t)value;
        break;
    case 4:
        out[0] = (uint8_t)(0x80u | (uint8_t)(value >> 24));
        out[1] = (uint8_t)(value >> 16);
        out[2] = (uint8_t)(value >> 8);
        out[3] = (uint8_t)value;
        break;
    case 8:
        out[0] = (uint8_t)(0xc0u | (uint8_t)(value >> 56));
        out[1] = (uint8_t)(value >> 48);
        out[2] = (uint8_t)(value >> 40);
        out[3] = (uint8_t)(value >> 32);
        out[4] = (uint8_t)(value >> 24);
        out[5] = (uint8_t)(value >> 16);
        out[6] = (uint8_t)(value >> 8);
        out[7] = (uint8_t)value;
        break;
    default:
        return 0;
    }
    return width;
}

static size_t minimal_width(uint64_t value) {
    if (value <= UINT64_C(0x3f)) {
        return 1;
    }
    if (value <= UINT64_C(0x3fff)) {
        return 2;
    }
    if (value <= UINT64_C(0x3fffffff)) {
        return 4;
    }
    return 8;
}

static size_t wire_capsule(uint8_t* out,
    uint64_t type,
    size_t type_width,
    uint64_t declared_length,
    size_t length_width,
    const uint8_t* payload,
    size_t payload_len) {
    size_t offset = wire_varint(out, type, type_width);
    offset += wire_varint(out + offset, declared_length, length_width);
    if (payload_len != 0) {
        memcpy(out + offset, payload, payload_len);
        offset += payload_len;
    }
    return offset;
}

static size_t wire_numeric_capsule(
    uint8_t* out, uint64_t type, size_t type_width, size_t length_width, uint64_t maximum, size_t value_width) {
    uint8_t payload[8];
    wire_varint(payload, maximum, value_width);
    return wire_capsule(out, type, type_width, value_width, length_width, payload, value_width);
}

static size_t wire_close(uint8_t* out, uint32_t code, const uint8_t* reason, size_t reason_len) {
    uint8_t payload[4 + TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    payload[0] = (uint8_t)(code >> 24);
    payload[1] = (uint8_t)(code >> 16);
    payload[2] = (uint8_t)(code >> 8);
    payload[3] = (uint8_t)code;
    if (reason_len != 0) {
        memcpy(payload + 4, reason, reason_len);
    }
    return wire_capsule(out,
        TREV_WT_CAPSULE_CLOSE_SESSION,
        minimal_width(TREV_WT_CAPSULE_CLOSE_SESSION),
        4 + reason_len,
        minimal_width(4 + reason_len),
        payload,
        4 + reason_len);
}

static trevrpc_wt_capsule_parse_result init_parser(trevrpc_wt_capsule_parser* parser,
    uint8_t* workspace,
    trevrpc_wt_profile_id profile,
    uint32_t compatibility_flags,
    bool modern_flow_control_enabled,
    uint64_t unknown_bound) {
    trevrpc_wt_capsule_parser_config config = {
        .profile = profile,
        .compatibility_flags = compatibility_flags,
        .modern_flow_control_enabled = modern_flow_control_enabled,
        .max_unknown_capsule_payload = unknown_bound,
        .close_reason_workspace = workspace,
        .close_reason_capacity = TREV_WT_CAPSULE_CLOSE_REASON_MAX,
    };
    return trevrpc_wt_capsule_parser_init(parser, &config);
}

static int check_event_feed(trevrpc_wt_capsule_parser* parser,
    const uint8_t* data,
    size_t data_len,
    trevrpc_wt_capsule_event_type type,
    uint64_t maximum) {
    size_t consumed = SIZE_MAX;
    trevrpc_wt_capsule_event event;
    memset(&event, 0xa5, sizeof(event));
    CHECK(trevrpc_wt_capsule_parser_feed(parser, data, data_len, &consumed, &event) == TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(consumed == data_len);
    CHECK(event.type == type);
    if (type == TREV_WT_CAPSULE_EVENT_MAX_DATA || type == TREV_WT_CAPSULE_EVENT_MAX_STREAMS_BIDI ||
        type == TREV_WT_CAPSULE_EVENT_MAX_STREAMS_UNI) {
        CHECK(event.value.max.maximum == maximum);
    }
    return 0;
}

static int check_zero_feed_preserves(trevrpc_wt_capsule_parser* parser, trevrpc_wt_capsule_parse_result expected) {
    trevrpc_wt_capsule_parser parser_sentinel = *parser;
    trevrpc_wt_capsule_event event;
    memset(&event, 0x96, sizeof(event));
    trevrpc_wt_capsule_event event_sentinel = event;
    size_t consumed = SIZE_MAX;
    CHECK(trevrpc_wt_capsule_parser_feed(parser, NULL, 0, &consumed, &event) == expected);
    CHECK(consumed == 0);
    CHECK(memcmp(parser, &parser_sentinel, sizeof(*parser)) == 0);
    CHECK(memcmp(&event, &event_sentinel, sizeof(event)) == 0);
    return 0;
}

static int check_nonempty_failure_preserves(
    trevrpc_wt_capsule_parser* parser, trevrpc_wt_capsule_parse_result expected) {
    trevrpc_wt_capsule_parser parser_sentinel = *parser;
    trevrpc_wt_capsule_event event;
    memset(&event, 0x69, sizeof(event));
    trevrpc_wt_capsule_event event_sentinel = event;
    const uint8_t byte = 0;
    size_t consumed = SIZE_MAX;
    CHECK(trevrpc_wt_capsule_parser_feed(parser, &byte, 1, &consumed, &event) == expected);
    CHECK(consumed == 0);
    CHECK(memcmp(parser, &parser_sentinel, sizeof(*parser)) == 0);
    CHECK(memcmp(&event, &event_sentinel, sizeof(event)) == 0);
    return 0;
}

static int check_invalid_feed_preserves(trevrpc_wt_capsule_parser* parser,
    uint8_t* workspace,
    const uint8_t* data,
    size_t data_len,
    size_t* out_consumed,
    trevrpc_wt_capsule_event* out_event) {
    trevrpc_wt_capsule_parser parser_sentinel = *parser;
    uint8_t workspace_sentinel[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    memcpy(workspace_sentinel, workspace, sizeof(workspace_sentinel));
    CHECK(trevrpc_wt_capsule_parser_feed(parser, data, data_len, out_consumed, out_event) ==
          TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(memcmp(parser, &parser_sentinel, sizeof(*parser)) == 0);
    CHECK(memcmp(workspace, workspace_sentinel, sizeof(workspace_sentinel)) == 0);
    return 0;
}

static int check_max_close_fragmentation(const size_t* chunks, size_t chunk_count) {
    struct guarded_workspace {
        uint8_t before[16];
        uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
        uint8_t after[16];
    } guarded;
    memset(guarded.before, 0x3c, sizeof(guarded.before));
    memset(guarded.workspace, 0xcc, sizeof(guarded.workspace));
    memset(guarded.after, 0xc3, sizeof(guarded.after));

    uint8_t reason[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    for (size_t i = 0; i < sizeof(reason); i++) {
        reason[i] = (uint8_t)('a' + (i % 26));
    }
    uint8_t wire[4 + TREV_WT_CAPSULE_CLOSE_REASON_MAX + 16];
    size_t wire_len = wire_close(wire, UINT32_C(0x1234abcd), reason, sizeof(reason));

    trevrpc_wt_capsule_parser parser;
    CHECK(init_parser(&parser, guarded.workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    size_t offset = 0;
    size_t chunk_index = 0;
    trevrpc_wt_capsule_event event;
    memset(&event, 0x5a, sizeof(event));
    trevrpc_wt_capsule_event sentinel = event;
    while (offset < wire_len) {
        size_t take = chunks[chunk_index++ % chunk_count];
        if (take > wire_len - offset) {
            take = wire_len - offset;
        }
        size_t consumed = SIZE_MAX;
        trevrpc_wt_capsule_parse_result expected =
            take == wire_len - offset ? TREV_WT_CAPSULE_PARSE_EVENT : TREV_WT_CAPSULE_PARSE_NEED_INPUT;
        CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire + offset, take, &consumed, &event) == expected);
        CHECK(consumed == take);
        for (size_t i = 0; i < sizeof(guarded.before); i++) {
            CHECK(guarded.before[i] == 0x3c);
            CHECK(guarded.after[i] == 0xc3);
        }
        offset += take;
        if (expected == TREV_WT_CAPSULE_PARSE_NEED_INPUT) {
            CHECK(memcmp(&event, &sentinel, sizeof(event)) == 0);
        }
    }
    CHECK(event.type == TREV_WT_CAPSULE_EVENT_CLOSE);
    CHECK(event.value.close.code == UINT32_C(0x1234abcd));
    CHECK(event.value.close.reason == guarded.workspace);
    CHECK(event.value.close.reason_len == sizeof(reason));
    CHECK(!event.value.close.implicit);
    CHECK(memcmp(event.value.close.reason, reason, sizeof(reason)) == 0);
    return 0;
}

static int test_profile_capability_matrix(void) {
    uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    trevrpc_wt_capsule_parser parser;

    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_02, 0, false, 64) == TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(parser.capabilities == TREV_WT_CAPSULE_CAP_CLOSE);

    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_07, 0, true, 64) == TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(parser.capabilities == (TREV_WT_CAPSULE_CAP_CLOSE | TREV_WT_CAPSULE_CAP_DRAIN));

    CHECK(init_parser(&parser,
              workspace,
              TREV_WT_PROFILE_DRAFT_07,
              TREV_WT_PROFILE_COMPAT_INITIAL_CAPSULE_FLOW_CONTROL,
              false,
              64) == TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(parser.capabilities == (TREV_WT_CAPSULE_CAP_CLOSE | TREV_WT_CAPSULE_CAP_DRAIN | TREV_WT_CAPSULE_CAP_MAX_DATA |
                                     TREV_WT_CAPSULE_CAP_MAX_STREAMS));

    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_14, 0, false, 64) == TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(parser.capabilities == (TREV_WT_CAPSULE_CAP_CLOSE | TREV_WT_CAPSULE_CAP_DRAIN));
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_14, 0, true, 64) == TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK((parser.capabilities & (TREV_WT_CAPSULE_CAP_MAX_DATA | TREV_WT_CAPSULE_CAP_MAX_STREAMS)) != 0);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK((parser.capabilities & (TREV_WT_CAPSULE_CAP_MAX_DATA | TREV_WT_CAPSULE_CAP_MAX_STREAMS)) != 0);

    trevrpc_wt_capsule_parser sentinel;
    memset(&sentinel, 0x5a, sizeof(sentinel));
    parser = sentinel;
    CHECK(
        init_parser(&parser, workspace, TREV_WT_PROFILE_NONE, 0, false, 64) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(memcmp(&parser, &sentinel, sizeof(parser)) == 0);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_16_RESERVED, 0, false, 64) ==
          TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(init_parser(&parser, workspace, (trevrpc_wt_profile_id)99, 0, false, 64) ==
          TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(init_parser(&parser,
              workspace,
              TREV_WT_PROFILE_DRAFT_14,
              TREV_WT_PROFILE_COMPAT_INITIAL_CAPSULE_FLOW_CONTROL,
              true,
              64) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_07, 2u, false, 64) ==
          TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);

    trevrpc_wt_capsule_parser_config bad_config = {
        .profile = TREV_WT_PROFILE_DRAFT_15,
        .close_reason_workspace = workspace,
        .close_reason_capacity = TREV_WT_CAPSULE_CLOSE_REASON_MAX - 1,
    };
    CHECK(trevrpc_wt_capsule_parser_init(&parser, &bad_config) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    bad_config.close_reason_capacity = TREV_WT_CAPSULE_CLOSE_REASON_MAX;
    bad_config.close_reason_workspace = NULL;
    CHECK(trevrpc_wt_capsule_parser_init(&parser, &bad_config) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_capsule_parser_init(NULL, &bad_config) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_capsule_parser_init(&parser, NULL) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);

    trevrpc_wt_capsule_parser overlap_sentinel;
    memset(&overlap_sentinel, 0x39, sizeof(overlap_sentinel));
    parser = overlap_sentinel;
    bad_config.close_reason_workspace = parser.close_reason_staging;
    bad_config.close_reason_capacity = TREV_WT_CAPSULE_CLOSE_REASON_MAX;
    CHECK(trevrpc_wt_capsule_parser_init(&parser, &bad_config) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(memcmp(&parser, &overlap_sentinel, sizeof(parser)) == 0);

    uint8_t wire[64];
    size_t numeric_len = wire_numeric_capsule(wire, TREV_WT_CAPSULE_MAX_DATA, 4, 1, 7, 1);
    size_t drain_len = wire_capsule(
        wire + numeric_len, TREV_WT_CAPSULE_DRAIN_SESSION, minimal_width(TREV_WT_CAPSULE_DRAIN_SESSION), 0, 1, NULL, 0);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_14, 0, false, 0) == 0);
    size_t consumed = 0;
    trevrpc_wt_capsule_event event;
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, numeric_len + drain_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(consumed == numeric_len + drain_len);
    CHECK(event.type == TREV_WT_CAPSULE_EVENT_DRAIN);
    return 0;
}

static int test_disabled_flow_control_capsules(void) {
    const trevrpc_wt_profile_id profiles[] = {TREV_WT_PROFILE_DRAFT_14, TREV_WT_PROFILE_DRAFT_15};
    const uint64_t types[] = {
        TREV_WT_CAPSULE_MAX_DATA,
        TREV_WT_CAPSULE_MAX_STREAMS_BIDI,
        TREV_WT_CAPSULE_MAX_STREAMS_UNI,
    };

    for (size_t profile_index = 0; profile_index < ARRAY_LEN(profiles); profile_index++) {
        for (size_t type_index = 0; type_index < ARRAY_LEN(types); type_index++) {
            uint8_t wire[64];
            size_t numeric_len = wire_numeric_capsule(wire, types[type_index], 4, 1, 7, 1);
            size_t drain_len = wire_capsule(wire + numeric_len,
                TREV_WT_CAPSULE_DRAIN_SESSION,
                minimal_width(TREV_WT_CAPSULE_DRAIN_SESSION),
                0,
                1,
                NULL,
                0);
            uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
            trevrpc_wt_capsule_parser parser;
            CHECK(init_parser(&parser, workspace, profiles[profile_index], 0, false, 0) ==
                  TREV_WT_CAPSULE_PARSE_NEED_INPUT);
            size_t consumed = SIZE_MAX;
            trevrpc_wt_capsule_event event;
            memset(&event, 0xa5, sizeof(event));
            CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, numeric_len + drain_len, &consumed, &event) ==
                  TREV_WT_CAPSULE_PARSE_EVENT);
            CHECK(consumed == numeric_len + drain_len);
            CHECK(event.type == TREV_WT_CAPSULE_EVENT_DRAIN);
        }
    }
    return 0;
}

static int test_parser_workspace_boundaries(void) {
    struct workspace_layout {
        uint8_t before[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
        trevrpc_wt_capsule_parser parser;
        uint8_t after[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    } layout;
    trevrpc_wt_capsule_parser_config config = {
        .profile = TREV_WT_PROFILE_DRAFT_15,
        .modern_flow_control_enabled = true,
        .max_unknown_capsule_payload = 64,
        .close_reason_capacity = TREV_WT_CAPSULE_CLOSE_REASON_MAX,
    };
    uint8_t* parser_bytes = (uint8_t*)&layout.parser;

    config.close_reason_workspace = parser_bytes - TREV_WT_CAPSULE_CLOSE_REASON_MAX;
    CHECK(trevrpc_wt_capsule_parser_init(&layout.parser, &config) == TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(layout.parser.close_reason_workspace == config.close_reason_workspace);

    config.close_reason_workspace = parser_bytes + sizeof(layout.parser);
    CHECK(trevrpc_wt_capsule_parser_init(&layout.parser, &config) == TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(layout.parser.close_reason_workspace == config.close_reason_workspace);

    trevrpc_wt_capsule_parser sentinel;
    memset(&sentinel, 0x4b, sizeof(sentinel));
    layout.parser = sentinel;
    config.close_reason_workspace = parser_bytes - TREV_WT_CAPSULE_CLOSE_REASON_MAX + 1;
    CHECK(trevrpc_wt_capsule_parser_init(&layout.parser, &config) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(memcmp(&layout.parser, &sentinel, sizeof(sentinel)) == 0);

    config.close_reason_workspace = parser_bytes + sizeof(layout.parser) - 1;
    CHECK(trevrpc_wt_capsule_parser_init(&layout.parser, &config) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(memcmp(&layout.parser, &sentinel, sizeof(sentinel)) == 0);

    config.close_reason_workspace = layout.parser.close_reason_staging;
    CHECK(trevrpc_wt_capsule_parser_init(&layout.parser, &config) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(memcmp(&layout.parser, &sentinel, sizeof(sentinel)) == 0);

    config.close_reason_workspace = (uint8_t*)(uintptr_t)(UINTPTR_MAX - (TREV_WT_CAPSULE_CLOSE_REASON_MAX - 1));
    CHECK(trevrpc_wt_capsule_parser_init(&layout.parser, &config) == TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(layout.parser.close_reason_workspace == config.close_reason_workspace);

    layout.parser = sentinel;
    config.close_reason_workspace = (uint8_t*)(uintptr_t)(UINTPTR_MAX - (TREV_WT_CAPSULE_CLOSE_REASON_MAX - 2));
    CHECK(trevrpc_wt_capsule_parser_init(&layout.parser, &config) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(memcmp(&layout.parser, &sentinel, sizeof(sentinel)) == 0);

    config.close_reason_workspace = layout.after;
    config.close_reason_capacity = SIZE_MAX;
    CHECK(trevrpc_wt_capsule_parser_init(&layout.parser, &config) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(memcmp(&layout.parser, &sentinel, sizeof(sentinel)) == 0);

    config.close_reason_capacity = TREV_WT_CAPSULE_CLOSE_REASON_MAX;
    CHECK(trevrpc_wt_capsule_parser_init(&layout.parser, &config) == TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    trevrpc_wt_capsule_parser parser_sentinel = layout.parser;
    trevrpc_wt_capsule_event event;
    memset(&event, 0xc3, sizeof(event));
    trevrpc_wt_capsule_event event_sentinel = event;
    size_t consumed = 91;
    const uint8_t* wrapping_data = (const uint8_t*)(uintptr_t)(UINTPTR_MAX - 1);
    CHECK(trevrpc_wt_capsule_parser_feed(&layout.parser, wrapping_data, 3, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(memcmp(&layout.parser, &parser_sentinel, sizeof(parser_sentinel)) == 0);
    CHECK(consumed == 91);
    CHECK(memcmp(&event, &event_sentinel, sizeof(event)) == 0);
    return 0;
}

static int test_fragmented_eight_byte_varints(void) {
    uint8_t wire[32];
    size_t wire_len = wire_numeric_capsule(wire, TREV_WT_CAPSULE_MAX_DATA, 8, 8, UINT64_C(0x123456789abcdef), 8);
    CHECK(wire_len == 24);

    for (size_t split = 1; split < wire_len; split++) {
        uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
        trevrpc_wt_capsule_parser parser;
        CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
        size_t consumed = SIZE_MAX;
        trevrpc_wt_capsule_event event;
        memset(&event, 0xa5, sizeof(event));
        CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, split, &consumed, &event) ==
              TREV_WT_CAPSULE_PARSE_NEED_INPUT);
        CHECK(consumed == split);
        CHECK(
            check_event_feed(
                &parser, wire + split, wire_len - split, TREV_WT_CAPSULE_EVENT_MAX_DATA, UINT64_C(0x123456789abcdef)) ==
            0);
    }

    uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    trevrpc_wt_capsule_parser parser;
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    for (size_t i = 0; i < wire_len; i++) {
        size_t consumed = 99;
        trevrpc_wt_capsule_event event;
        trevrpc_wt_capsule_parse_result expected =
            i + 1 == wire_len ? TREV_WT_CAPSULE_PARSE_EVENT : TREV_WT_CAPSULE_PARSE_NEED_INPUT;
        CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire + i, 1, &consumed, &event) == expected);
        CHECK(consumed == 1);
        if (expected == TREV_WT_CAPSULE_PARSE_EVENT) {
            CHECK(event.type == TREV_WT_CAPSULE_EVENT_MAX_DATA);
            CHECK(event.value.max.maximum == UINT64_C(0x123456789abcdef));
        }
    }
    return 0;
}

static int test_nonminimal_encodings(void) {
    const size_t widths[] = {2, 4, 8};
    for (size_t i = 0; i < ARRAY_LEN(widths); i++) {
        uint8_t wire[32];
        size_t wire_len = wire_numeric_capsule(wire, TREV_WT_CAPSULE_MAX_DATA, 8, widths[i], 1, widths[i]);
        uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
        trevrpc_wt_capsule_parser parser;
        CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
        CHECK(check_event_feed(&parser, wire, wire_len, TREV_WT_CAPSULE_EVENT_MAX_DATA, 1) == 0);
    }

    uint8_t wire[32];
    size_t wire_len = wire_capsule(wire, TREV_WT_CAPSULE_DRAIN_SESSION, 8, 0, 8, NULL, 0);
    uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    trevrpc_wt_capsule_parser parser;
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(check_event_feed(&parser, wire, wire_len, TREV_WT_CAPSULE_EVENT_DRAIN, 0) == 0);
    return 0;
}

static int test_numeric_malformed_lengths_and_limits(void) {
    const uint64_t invalid_lengths[] = {0, 3, 5, 6, 7, 9};
    for (size_t i = 0; i < ARRAY_LEN(invalid_lengths); i++) {
        uint8_t wire[16];
        size_t wire_len = wire_capsule(
            wire, TREV_WT_CAPSULE_MAX_DATA, minimal_width(TREV_WT_CAPSULE_MAX_DATA), invalid_lengths[i], 1, NULL, 0);
        uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
        trevrpc_wt_capsule_parser parser;
        CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
        size_t consumed = 99;
        trevrpc_wt_capsule_event event;
        CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, wire_len, &consumed, &event) ==
              TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
        CHECK(consumed == wire_len);
    }

    const struct {
        size_t declared;
        size_t encoded;
    } mismatches[] = {{1, 2}, {2, 1}, {4, 8}, {8, 4}};
    for (size_t i = 0; i < ARRAY_LEN(mismatches); i++) {
        uint8_t payload[8];
        wire_varint(payload, 1, mismatches[i].encoded);
        uint8_t wire[32];
        size_t wire_len =
            wire_capsule(wire, TREV_WT_CAPSULE_MAX_DATA, 4, mismatches[i].declared, 1, payload, mismatches[i].encoded);
        uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
        trevrpc_wt_capsule_parser parser;
        CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
        size_t consumed = 99;
        trevrpc_wt_capsule_event event;
        CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, wire_len, &consumed, &event) ==
              TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
    }

    uint8_t wire[32];
    size_t wire_len = wire_numeric_capsule(wire, TREV_WT_CAPSULE_MAX_STREAMS_BIDI, 4, 1, TREV_WT_FLOW_MAX_STREAMS, 8);
    uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    trevrpc_wt_capsule_parser parser;
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(check_event_feed(&parser, wire, wire_len, TREV_WT_CAPSULE_EVENT_MAX_STREAMS_BIDI, TREV_WT_FLOW_MAX_STREAMS) ==
          0);

    wire_len = wire_numeric_capsule(wire, TREV_WT_CAPSULE_MAX_STREAMS_UNI, 4, 1, TREV_WT_FLOW_MAX_STREAMS + 1, 8);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    size_t consumed = 0;
    trevrpc_wt_capsule_event event;
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, wire_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_H3_DATAGRAM_ERROR);
    CHECK(consumed == wire_len);

    wire_len = wire_numeric_capsule(wire, TREV_WT_CAPSULE_MAX_DATA, 4, 1, TREV_WT_FLOW_MAX_DATA, 8);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(check_event_feed(&parser, wire, wire_len, TREV_WT_CAPSULE_EVENT_MAX_DATA, TREV_WT_FLOW_MAX_DATA) == 0);

    wire_len = wire_capsule(wire, TREV_WT_CAPSULE_MAX_DATA, 4, 3, 1, NULL, 0);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_14, 0, false, 0) == 0);
    consumed = SIZE_MAX;
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, wire_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
    CHECK(consumed == wire_len);

    wire_len = wire_numeric_capsule(wire, TREV_WT_CAPSULE_MAX_STREAMS_UNI, 4, 1, TREV_WT_FLOW_MAX_STREAMS + 1, 8);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, false, 0) == 0);
    consumed = SIZE_MAX;
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, wire_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_H3_DATAGRAM_ERROR);
    CHECK(consumed == wire_len);
    return 0;
}

static int test_unknown_capsules(void) {
    uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    trevrpc_wt_capsule_parser parser;
    trevrpc_wt_capsule_event event;
    size_t consumed = 0;
    uint8_t wire[64];

    size_t zero_len = wire_capsule(wire, 42, 1, 0, 1, NULL, 0);
    size_t drain_len = wire_capsule(
        wire + zero_len, TREV_WT_CAPSULE_DRAIN_SESSION, minimal_width(TREV_WT_CAPSULE_DRAIN_SESSION), 0, 1, NULL, 0);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 4) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, zero_len + drain_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(consumed == zero_len + drain_len);
    CHECK(event.type == TREV_WT_CAPSULE_EVENT_DRAIN);

    const uint8_t payload[] = {1, 2, 3, 4};
    size_t bound_len = wire_capsule(wire, 43, 1, 4, 1, payload, sizeof(payload));
    drain_len = wire_capsule(
        wire + bound_len, TREV_WT_CAPSULE_DRAIN_SESSION, minimal_width(TREV_WT_CAPSULE_DRAIN_SESSION), 0, 1, NULL, 0);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 4) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, bound_len + drain_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(consumed == bound_len + drain_len);

    size_t oversized_header = wire_capsule(wire, 44, 1, 5, 1, NULL, 0);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 4) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, oversized_header, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_EXCESSIVE_LOAD);
    CHECK(consumed == oversized_header);

    bound_len = wire_capsule(wire, 45, 1, 4, 1, payload, sizeof(payload));
    drain_len = wire_capsule(
        wire + bound_len, TREV_WT_CAPSULE_DRAIN_SESSION, minimal_width(TREV_WT_CAPSULE_DRAIN_SESSION), 0, 1, NULL, 0);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 4) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, 4, &consumed, &event) == TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(consumed == 4);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire + 4, bound_len + drain_len - 4, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(consumed == bound_len + drain_len - 4);
    CHECK(event.type == TREV_WT_CAPSULE_EVENT_DRAIN);
    return 0;
}

static int test_close_lengths_and_fragmentation(void) {
    uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    trevrpc_wt_capsule_parser parser;
    trevrpc_wt_capsule_event event;
    size_t consumed = 0;
    uint8_t wire[1100];

    size_t wire_len = wire_capsule(wire, TREV_WT_CAPSULE_CLOSE_SESSION, 2, 3, 1, NULL, 0);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, wire_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);

    wire_len = wire_close(wire, UINT32_C(0x12345678), NULL, 0);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, wire_len, &consumed, &event) == TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(event.type == TREV_WT_CAPSULE_EVENT_CLOSE);
    CHECK(event.value.close.code == UINT32_C(0x12345678));
    CHECK(event.value.close.reason_len == 0);
    CHECK(!event.value.close.implicit);

    uint8_t reason[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    memset(reason, 'x', sizeof(reason));
    wire_len = wire_close(wire, 9, reason, sizeof(reason));
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, wire_len, &consumed, &event) == TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(event.value.close.reason_len == sizeof(reason));
    CHECK(memcmp(event.value.close.reason, reason, sizeof(reason)) == 0);

    wire_len = wire_capsule(wire, TREV_WT_CAPSULE_CLOSE_SESSION, 2, 1029, 2, NULL, 0);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, wire_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);

    const uint8_t fragmented_reason[] = {'a', 0, 0xe2, 0x82, 0xac, 0xf0, 0x9f, 0x98, 0x80};
    wire_len = wire_close(wire, UINT32_C(0x89abcdef), fragmented_reason, sizeof(fragmented_reason));
    for (size_t split = 1; split < wire_len; split++) {
        CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
        CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, split, &consumed, &event) ==
              TREV_WT_CAPSULE_PARSE_NEED_INPUT);
        CHECK(consumed == split);
        CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire + split, wire_len - split, &consumed, &event) ==
              TREV_WT_CAPSULE_PARSE_EVENT);
        CHECK(event.value.close.code == UINT32_C(0x89abcdef));
        CHECK(event.value.close.reason_len == sizeof(fragmented_reason));
        CHECK(memcmp(event.value.close.reason, fragmented_reason, sizeof(fragmented_reason)) == 0);
    }

    const size_t one_byte_chunks[] = {1};
    CHECK(check_max_close_fragmentation(one_byte_chunks, ARRAY_LEN(one_byte_chunks)) == 0);
    const size_t irregular_chunks[] = {3, 17, 1, 64, 5, 127, 2, 251, 11};
    CHECK(check_max_close_fragmentation(irregular_chunks, ARRAY_LEN(irregular_chunks)) == 0);

    struct overlap_workspace {
        uint8_t before[8];
        uint8_t bytes[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
        uint8_t after[8];
    } overlap;
    memset(&overlap, 0xa7, sizeof(overlap));
    uint8_t overlap_reason[96];
    for (size_t i = 0; i < sizeof(overlap_reason); i++) {
        overlap_reason[i] = (uint8_t)('A' + (i % 26));
    }
    wire_len = wire_close(overlap.bytes + 1, UINT32_C(0x76543210), overlap_reason, sizeof(overlap_reason));
    CHECK(init_parser(&parser, overlap.bytes, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, overlap.bytes + 1, wire_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(consumed == wire_len);
    CHECK(event.value.close.code == UINT32_C(0x76543210));
    CHECK(event.value.close.reason == overlap.bytes);
    CHECK(event.value.close.reason_len == sizeof(overlap_reason));
    CHECK(memcmp(event.value.close.reason, overlap_reason, sizeof(overlap_reason)) == 0);
    for (size_t i = 0; i < sizeof(overlap.before); i++) {
        CHECK(overlap.before[i] == 0xa7);
        CHECK(overlap.after[i] == 0xa7);
    }

    uint8_t reverse_overlap[1200];
    uint8_t reverse_reason[96];
    for (size_t i = 0; i < sizeof(reverse_reason); i++) {
        reverse_reason[i] = (uint8_t)('A' + (i % 26));
    }
    wire_len = wire_close(reverse_overlap, 17, reverse_reason, sizeof(reverse_reason));
    uint8_t* reverse_workspace = reverse_overlap + 10;
    uint8_t* reverse_trailer = reverse_workspace + sizeof(reverse_reason);
    memset(reverse_trailer, 0x5d, 16);
    size_t reason_offset = wire_len - sizeof(reverse_reason);
    size_t first_fragment = reason_offset + 1;
    CHECK(init_parser(&parser, reverse_workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, reverse_overlap, first_fragment, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(consumed == first_fragment);
    CHECK(trevrpc_wt_capsule_parser_feed(
              &parser, reverse_overlap + first_fragment, wire_len - first_fragment, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(consumed == wire_len - first_fragment);
    CHECK(event.value.close.code == 17);
    CHECK(event.value.close.reason == reverse_workspace);
    CHECK(event.value.close.reason_len == sizeof(reverse_reason));
    CHECK(memcmp(event.value.close.reason, reverse_reason, sizeof(reverse_reason)) == 0);
    for (size_t i = 0; i < 16; i++) {
        CHECK(reverse_trailer[i] == 0x5d);
    }

    uint8_t forward_overlap[1200];
    uint8_t* forward_workspace = forward_overlap + 16;
    uint8_t* forward_wire = forward_overlap + 32;
    wire_len = wire_close(forward_wire, 23, reverse_reason, sizeof(reverse_reason));
    uint8_t* forward_trailer = forward_workspace + sizeof(reverse_reason);
    uint8_t trailer_sentinel[16];
    memcpy(trailer_sentinel, forward_trailer, sizeof(trailer_sentinel));
    first_fragment = wire_len - sizeof(reverse_reason) + 1;
    CHECK(init_parser(&parser, forward_workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, forward_wire, first_fragment, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(consumed == first_fragment);
    CHECK(trevrpc_wt_capsule_parser_feed(
              &parser, forward_wire + first_fragment, wire_len - first_fragment, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(consumed == wire_len - first_fragment);
    CHECK(event.value.close.code == 23);
    CHECK(event.value.close.reason == forward_workspace);
    CHECK(event.value.close.reason_len == sizeof(reverse_reason));
    CHECK(memcmp(event.value.close.reason, reverse_reason, sizeof(reverse_reason)) == 0);
    CHECK(memcmp(forward_trailer, trailer_sentinel, sizeof(trailer_sentinel)) == 0);

    uint8_t trailing_alias[1200];
    size_t close_len = wire_close(trailing_alias, 29, overlap_reason, 32);
    size_t drain_len = wire_capsule(trailing_alias + close_len,
        TREV_WT_CAPSULE_DRAIN_SESSION,
        minimal_width(TREV_WT_CAPSULE_DRAIN_SESSION),
        0,
        1,
        NULL,
        0);
    uint8_t* trailing_workspace = trailing_alias + close_len - 8;
    uint8_t trailing_sentinel[1200];
    memcpy(trailing_sentinel, trailing_alias, sizeof(trailing_sentinel));
    CHECK(init_parser(&parser, trailing_workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    trevrpc_wt_capsule_parser parser_sentinel = parser;
    memset(&event, 0x6d, sizeof(event));
    trevrpc_wt_capsule_event event_sentinel = event;
    consumed = 77;
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, trailing_alias, close_len + drain_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(consumed == 77);
    CHECK(memcmp(&parser, &parser_sentinel, sizeof(parser)) == 0);
    CHECK(memcmp(&event, &event_sentinel, sizeof(event)) == 0);
    CHECK(memcmp(trailing_alias, trailing_sentinel, sizeof(trailing_alias)) == 0);

    uint8_t safe_close[64];
    close_len = wire_close(safe_close, 29, overlap_reason, 32);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, safe_close, close_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(consumed == close_len);
    CHECK(event.value.close.code == 29);
    CHECK(event.value.close.reason == trailing_workspace);
    CHECK(event.value.close.reason_len == 32);
    CHECK(memcmp(event.value.close.reason, overlap_reason, 32) == 0);
    return 0;
}

static int test_invalid_utf8(void) {
    static const uint8_t bad_continuation[] = {0xc2, 0x20};
    static const uint8_t stray_continuation[] = {0x80};
    static const uint8_t overlong_two[] = {0xc0, 0x80};
    static const uint8_t overlong_three[] = {0xe0, 0x80, 0x80};
    static const uint8_t overlong_four[] = {0xf0, 0x80, 0x80, 0x80};
    static const uint8_t surrogate[] = {0xed, 0xa0, 0x80};
    static const uint8_t above_unicode[] = {0xf4, 0x90, 0x80, 0x80};
    static const uint8_t invalid_lead[] = {0xf5, 0x80, 0x80, 0x80};
    static const uint8_t incomplete_two[] = {0xc2};
    static const uint8_t incomplete_three[] = {0xe2, 0x82};
    static const uint8_t incomplete_four[] = {0xf0, 0x90, 0x80};
    const struct {
        const uint8_t* bytes;
        size_t len;
    } cases[] = {
        {bad_continuation, sizeof(bad_continuation)},
        {stray_continuation, sizeof(stray_continuation)},
        {overlong_two, sizeof(overlong_two)},
        {overlong_three, sizeof(overlong_three)},
        {overlong_four, sizeof(overlong_four)},
        {surrogate, sizeof(surrogate)},
        {above_unicode, sizeof(above_unicode)},
        {invalid_lead, sizeof(invalid_lead)},
        {incomplete_two, sizeof(incomplete_two)},
        {incomplete_three, sizeof(incomplete_three)},
        {incomplete_four, sizeof(incomplete_four)},
    };

    for (size_t i = 0; i < ARRAY_LEN(cases); i++) {
        uint8_t wire[64];
        size_t wire_len = wire_close(wire, 0, cases[i].bytes, cases[i].len);
        uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
        trevrpc_wt_capsule_parser parser;
        CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
        size_t consumed = 0;
        trevrpc_wt_capsule_event event;
        memset(&event, 0x3c, sizeof(event));
        trevrpc_wt_capsule_event sentinel = event;
        CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, wire_len, &consumed, &event) ==
              TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
        CHECK(consumed == wire_len);
        CHECK(memcmp(&event, &sentinel, sizeof(event)) == 0);
    }
    return 0;
}

static int test_drain_and_post_close(void) {
    uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    trevrpc_wt_capsule_parser parser;
    trevrpc_wt_capsule_event event;
    size_t consumed = 0;
    uint8_t wire[64];

    uint8_t payload = 0;
    size_t wire_len = wire_capsule(
        wire, TREV_WT_CAPSULE_DRAIN_SESSION, minimal_width(TREV_WT_CAPSULE_DRAIN_SESSION), 1, 1, &payload, 1);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, wire_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);

    size_t one =
        wire_capsule(wire, TREV_WT_CAPSULE_DRAIN_SESSION, minimal_width(TREV_WT_CAPSULE_DRAIN_SESSION), 0, 1, NULL, 0);
    size_t two = wire_capsule(
        wire + one, TREV_WT_CAPSULE_DRAIN_SESSION, minimal_width(TREV_WT_CAPSULE_DRAIN_SESSION), 0, 1, NULL, 0);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, one + two, &consumed, &event) == TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(consumed == one);
    CHECK(event.type == TREV_WT_CAPSULE_EVENT_DRAIN);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire + consumed, one + two - consumed, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(consumed == two);

    size_t close_len = wire_close(wire, 1, NULL, 0);
    size_t drain_len = wire_capsule(
        wire + close_len, TREV_WT_CAPSULE_DRAIN_SESSION, minimal_width(TREV_WT_CAPSULE_DRAIN_SESSION), 0, 1, NULL, 0);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, close_len + drain_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(consumed == close_len);
    CHECK(event.type == TREV_WT_CAPSULE_EVENT_CLOSE);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire + close_len, drain_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_POST_CLOSE_MESSAGE_ERROR);
    CHECK(consumed == 0);
    CHECK(parser.state == TREV_WT_CAPSULE_PARSER_FAILED);
    return 0;
}

static int test_finish_boundaries_and_truncation(void) {
    uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    trevrpc_wt_capsule_parser parser;
    trevrpc_wt_capsule_event event;
    memset(&event, 0xa5, sizeof(event));

    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(event.type == TREV_WT_CAPSULE_EVENT_CLOSE);
    CHECK(event.value.close.code == 0);
    CHECK(event.value.close.reason == NULL);
    CHECK(event.value.close.reason_len == 0);
    CHECK(event.value.close.implicit);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_COMPLETE);

    uint8_t wire[64];
    size_t drain_len =
        wire_capsule(wire, TREV_WT_CAPSULE_DRAIN_SESSION, minimal_width(TREV_WT_CAPSULE_DRAIN_SESSION), 0, 1, NULL, 0);
    size_t consumed = 0;
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, drain_len, &consumed, &event) == TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(event.value.close.implicit);

    size_t unknown_len = wire_capsule(wire, 1, 1, 0, 1, NULL, 0);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, unknown_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_EVENT);

    size_t numeric_len = wire_numeric_capsule(wire, TREV_WT_CAPSULE_MAX_DATA, 4, 1, 1, 1);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, numeric_len, &consumed, &event) == TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_EVENT);

    uint8_t partial_type[] = {0xc0};
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, partial_type, sizeof(partial_type), &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);

    uint8_t partial_length[9];
    size_t partial_length_len = wire_varint(partial_length, 1, 1);
    partial_length[partial_length_len++] = 0xc0;
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, partial_length, partial_length_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);

    size_t close_len = wire_close(wire, 1, (const uint8_t*)"abc", 3);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, close_len - 1, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);

    numeric_len = wire_numeric_capsule(wire, TREV_WT_CAPSULE_MAX_DATA, 4, 1, 64, 2);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, numeric_len - 1, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);

    const uint8_t unknown_payload[] = {1, 2, 3};
    unknown_len = wire_capsule(wire, 2, 1, 3, 1, unknown_payload, sizeof(unknown_payload));
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, unknown_len - 1, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
    return 0;
}

static int test_feed_output_alias_rejection(void) {
    uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    memset(workspace, 0x5c, sizeof(workspace));
    trevrpc_wt_capsule_parser parser;
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == TREV_WT_CAPSULE_PARSE_NEED_INPUT);

    uint8_t drain_wire[16];
    size_t drain_len = wire_capsule(
        drain_wire, TREV_WT_CAPSULE_DRAIN_SESSION, minimal_width(TREV_WT_CAPSULE_DRAIN_SESSION), 0, 1, NULL, 0);
    size_t consumed = 73;
    trevrpc_wt_capsule_event event;
    memset(&event, 0xa5, sizeof(event));
    trevrpc_wt_capsule_event event_sentinel = event;

    size_t* wrapping_consumed = (size_t*)(uintptr_t)(UINTPTR_MAX - (sizeof(size_t) - 2));
    CHECK(check_invalid_feed_preserves(&parser, workspace, drain_wire, drain_len, wrapping_consumed, &event) == 0);
    CHECK(memcmp(&event, &event_sentinel, sizeof(event)) == 0);

    trevrpc_wt_capsule_event* wrapping_event =
        (trevrpc_wt_capsule_event*)(uintptr_t)(UINTPTR_MAX - (sizeof(trevrpc_wt_capsule_event) - 2));
    CHECK(check_invalid_feed_preserves(&parser, workspace, drain_wire, drain_len, &consumed, wrapping_event) == 0);
    CHECK(consumed == 73);

    union output_alias_storage {
        uint64_t alignment;
        uint8_t bytes[sizeof(trevrpc_wt_capsule_event) + sizeof(size_t)];
    } output_alias;
    memset(&output_alias, 0x3d, sizeof(output_alias));
    union output_alias_storage output_alias_sentinel = output_alias;
    CHECK(check_invalid_feed_preserves(&parser,
              workspace,
              drain_wire,
              drain_len,
              (size_t*)(void*)output_alias.bytes,
              (trevrpc_wt_capsule_event*)(void*)output_alias.bytes) == 0);
    CHECK(memcmp(&output_alias, &output_alias_sentinel, sizeof(output_alias)) == 0);

    CHECK(check_invalid_feed_preserves(&parser,
              workspace,
              drain_wire,
              drain_len,
              (size_t*)(void*)output_alias.bytes,
              (trevrpc_wt_capsule_event*)(void*)(output_alias.bytes + sizeof(size_t) - 1)) == 0);
    CHECK(memcmp(&output_alias, &output_alias_sentinel, sizeof(output_alias)) == 0);

    CHECK(
        check_invalid_feed_preserves(&parser, workspace, drain_wire, drain_len, (size_t*)(void*)&parser, &event) == 0);
    CHECK(memcmp(&event, &event_sentinel, sizeof(event)) == 0);
    CHECK(check_invalid_feed_preserves(
              &parser, workspace, drain_wire, drain_len, &consumed, (trevrpc_wt_capsule_event*)(void*)&parser) == 0);
    CHECK(consumed == 73);

    CHECK(check_invalid_feed_preserves(&parser, workspace, drain_wire, drain_len, (size_t*)(void*)workspace, &event) ==
          0);
    CHECK(memcmp(&event, &event_sentinel, sizeof(event)) == 0);
    CHECK(check_invalid_feed_preserves(
              &parser, workspace, drain_wire, drain_len, &consumed, (trevrpc_wt_capsule_event*)(void*)workspace) == 0);
    CHECK(consumed == 73);

    union data_alias_storage {
        uint64_t alignment;
        uint8_t bytes[64];
    } data_alias;
    memset(&data_alias, 0x6e, sizeof(data_alias));
    memcpy(data_alias.bytes, drain_wire, drain_len);
    union data_alias_storage data_alias_sentinel = data_alias;
    CHECK(check_invalid_feed_preserves(
              &parser, workspace, data_alias.bytes, drain_len, (size_t*)(void*)data_alias.bytes, &event) == 0);
    CHECK(memcmp(&data_alias, &data_alias_sentinel, sizeof(data_alias)) == 0);
    CHECK(memcmp(&event, &event_sentinel, sizeof(event)) == 0);

    CHECK(check_invalid_feed_preserves(&parser,
              workspace,
              data_alias.bytes,
              drain_len,
              &consumed,
              (trevrpc_wt_capsule_event*)(void*)data_alias.bytes) == 0);
    CHECK(memcmp(&data_alias, &data_alias_sentinel, sizeof(data_alias)) == 0);
    CHECK(consumed == 73);

    CHECK(trevrpc_wt_capsule_parser_feed(&parser, drain_wire, drain_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(consumed == drain_len);
    CHECK(event.type == TREV_WT_CAPSULE_EVENT_DRAIN);
    return 0;
}

static int test_finish_output_alias_rejection(void) {
    uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    memset(workspace, 0x5c, sizeof(workspace));
    uint8_t workspace_sentinel[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    memcpy(workspace_sentinel, workspace, sizeof(workspace_sentinel));
    trevrpc_wt_capsule_parser parser;
    trevrpc_wt_capsule_event event;

    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    trevrpc_wt_capsule_parser parser_sentinel = parser;
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, (trevrpc_wt_capsule_event*)(void*)&parser) ==
          TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(memcmp(&parser, &parser_sentinel, sizeof(parser)) == 0);

    trevrpc_wt_capsule_event* staging_alias =
        (trevrpc_wt_capsule_event*)(void*)(parser.close_reason_staging + sizeof(parser.close_reason_staging) - 1);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, staging_alias) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(memcmp(&parser, &parser_sentinel, sizeof(parser)) == 0);

    CHECK(trevrpc_wt_capsule_parser_finish(&parser, (trevrpc_wt_capsule_event*)(void*)workspace) ==
          TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(memcmp(&parser, &parser_sentinel, sizeof(parser)) == 0);

    trevrpc_wt_capsule_event* wrapping =
        (trevrpc_wt_capsule_event*)(uintptr_t)(UINTPTR_MAX - (sizeof(trevrpc_wt_capsule_event) - 2));
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, wrapping) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(memcmp(&parser, &parser_sentinel, sizeof(parser)) == 0);

    memset(&event, 0xa5, sizeof(event));
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(event.type == TREV_WT_CAPSULE_EVENT_CLOSE);
    CHECK(event.value.close.implicit);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_COMPLETE);
    CHECK(memcmp(workspace, workspace_sentinel, sizeof(workspace)) == 0);
    return 0;
}

static int test_zero_length_feeds_and_failure_stickiness(void) {
    uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    trevrpc_wt_capsule_parser parser;
    trevrpc_wt_capsule_event event;
    size_t consumed = 0;
    uint8_t wire[64];

    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(check_zero_feed_preserves(&parser, TREV_WT_CAPSULE_PARSE_NEED_INPUT) == 0);

    const uint8_t partial_type[] = {0xc0};
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, partial_type, sizeof(partial_type), &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(parser.state == TREV_WT_CAPSULE_PARSER_TYPE);
    CHECK(parser.varint_have == 1);
    CHECK(check_zero_feed_preserves(&parser, TREV_WT_CAPSULE_PARSE_NEED_INPUT) == 0);

    const uint8_t partial_length[] = {1, 0xc0};
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, partial_length, sizeof(partial_length), &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(parser.state == TREV_WT_CAPSULE_PARSER_LENGTH);
    CHECK(parser.varint_have == 1);
    CHECK(check_zero_feed_preserves(&parser, TREV_WT_CAPSULE_PARSE_NEED_INPUT) == 0);

    size_t wire_len = wire_close(wire, 1, (const uint8_t*)"abc", 3);
    size_t close_header_len = wire_len - 7;
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, close_header_len + 2, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(parser.state == TREV_WT_CAPSULE_PARSER_CLOSE_PAYLOAD);
    CHECK(check_zero_feed_preserves(&parser, TREV_WT_CAPSULE_PARSE_NEED_INPUT) == 0);

    wire_len = wire_numeric_capsule(wire, TREV_WT_CAPSULE_MAX_DATA, 4, 1, 64, 2);
    size_t numeric_header_len = wire_len - 2;
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, numeric_header_len + 1, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(parser.state == TREV_WT_CAPSULE_PARSER_NUMERIC_PAYLOAD);
    CHECK(check_zero_feed_preserves(&parser, TREV_WT_CAPSULE_PARSE_NEED_INPUT) == 0);

    const uint8_t unknown_payload[] = {1, 2, 3};
    wire_len = wire_capsule(wire, 7, 1, 3, 1, unknown_payload, sizeof(unknown_payload));
    size_t unknown_header_len = wire_len - sizeof(unknown_payload);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, unknown_header_len + 1, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    CHECK(parser.state == TREV_WT_CAPSULE_PARSER_UNKNOWN_PAYLOAD);
    CHECK(check_zero_feed_preserves(&parser, TREV_WT_CAPSULE_PARSE_NEED_INPUT) == 0);

    wire_len = wire_close(wire, 2, NULL, 0);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, wire_len, &consumed, &event) == TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(check_zero_feed_preserves(&parser, TREV_WT_CAPSULE_PARSE_NEED_INPUT) == 0);

    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_EVENT);
    CHECK(event.value.close.implicit);
    CHECK(check_zero_feed_preserves(&parser, TREV_WT_CAPSULE_PARSE_NEED_INPUT) == 0);
    trevrpc_wt_capsule_event event_sentinel;
    memset(&event_sentinel, 0x4d, sizeof(event_sentinel));
    event = event_sentinel;
    const uint8_t nonempty = 0;
    consumed = SIZE_MAX;
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, &nonempty, 1, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_POST_CLOSE_MESSAGE_ERROR);
    CHECK(consumed == 0);
    CHECK(memcmp(&event, &event_sentinel, sizeof(event)) == 0);
    CHECK(check_zero_feed_preserves(&parser, TREV_WT_CAPSULE_PARSE_POST_CLOSE_MESSAGE_ERROR) == 0);
    CHECK(check_nonempty_failure_preserves(&parser, TREV_WT_CAPSULE_PARSE_POST_CLOSE_MESSAGE_ERROR) == 0);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_POST_CLOSE_MESSAGE_ERROR);
    CHECK(memcmp(&event, &event_sentinel, sizeof(event)) == 0);

    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, partial_type, sizeof(partial_type), &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_NEED_INPUT);
    event = event_sentinel;
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
    CHECK(memcmp(&event, &event_sentinel, sizeof(event)) == 0);
    CHECK(check_zero_feed_preserves(&parser, TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE) == 0);
    CHECK(check_nonempty_failure_preserves(&parser, TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE) == 0);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
    CHECK(memcmp(&event, &event_sentinel, sizeof(event)) == 0);

    wire_len = wire_numeric_capsule(wire, TREV_WT_CAPSULE_MAX_STREAMS_BIDI, 4, 1, TREV_WT_FLOW_MAX_STREAMS + 1, 8);
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 64) == 0);
    event = event_sentinel;
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, wire_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_H3_DATAGRAM_ERROR);
    CHECK(memcmp(&event, &event_sentinel, sizeof(event)) == 0);
    CHECK(check_zero_feed_preserves(&parser, TREV_WT_CAPSULE_PARSE_H3_DATAGRAM_ERROR) == 0);
    CHECK(check_nonempty_failure_preserves(&parser, TREV_WT_CAPSULE_PARSE_H3_DATAGRAM_ERROR) == 0);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_H3_DATAGRAM_ERROR);
    CHECK(memcmp(&event, &event_sentinel, sizeof(event)) == 0);
    return 0;
}

static int test_parser_atomicity_and_sticky_failures(void) {
    uint8_t workspace[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
    trevrpc_wt_capsule_parser parser;
    CHECK(init_parser(&parser, workspace, TREV_WT_PROFILE_DRAFT_15, 0, true, 4) == 0);
    trevrpc_wt_capsule_parser sentinel_parser = parser;
    size_t consumed = 77;
    trevrpc_wt_capsule_event event;
    memset(&event, 0x6b, sizeof(event));
    trevrpc_wt_capsule_event sentinel_event = event;

    CHECK(trevrpc_wt_capsule_parser_feed(NULL, NULL, 0, &consumed, &event) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(consumed == 77);
    CHECK(memcmp(&event, &sentinel_event, sizeof(event)) == 0);
    CHECK(
        trevrpc_wt_capsule_parser_feed(&parser, NULL, 1, &consumed, &event) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(memcmp(&parser, &sentinel_parser, sizeof(parser)) == 0);
    CHECK(consumed == 77);
    CHECK(memcmp(&event, &sentinel_event, sizeof(event)) == 0);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, NULL, 0, NULL, &event) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, NULL, 0, &consumed, NULL) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, parser.close_reason_staging, 1, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(memcmp(&parser, &sentinel_parser, sizeof(parser)) == 0);
    CHECK(consumed == 77);
    CHECK(memcmp(&event, &sentinel_event, sizeof(event)) == 0);

    uint8_t wire[8];
    size_t wire_len = wire_capsule(wire, 5, 1, 5, 1, NULL, 0);
    consumed = 99;
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, wire_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_EXCESSIVE_LOAD);
    CHECK(consumed == wire_len);
    CHECK(memcmp(&event, &sentinel_event, sizeof(event)) == 0);
    consumed = 99;
    CHECK(trevrpc_wt_capsule_parser_feed(&parser, wire, wire_len, &consumed, &event) ==
          TREV_WT_CAPSULE_PARSE_EXCESSIVE_LOAD);
    CHECK(consumed == 0);
    CHECK(memcmp(&event, &sentinel_event, sizeof(event)) == 0);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, &event) == TREV_WT_CAPSULE_PARSE_EXCESSIVE_LOAD);
    CHECK(memcmp(&event, &sentinel_event, sizeof(event)) == 0);
    CHECK(trevrpc_wt_capsule_parser_finish(NULL, &event) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_capsule_parser_finish(&parser, NULL) == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT);
    return 0;
}

static int test_flow_initialization_and_disabled_bypass(void) {
    const trevrpc_wt_flow_limits zero = {0};
    const trevrpc_wt_flow_limits nonzero = {.max_data = 10, .max_streams_bidi = 2, .max_streams_uni = 3};
    trevrpc_wt_flow_state state;

    CHECK(trevrpc_wt_flow_state_init(&state, &zero, &nonzero) == TREV_WT_FLOW_OK);
    CHECK(!state.enabled);
    CHECK(memcmp(&state.peer_max, &nonzero, sizeof(nonzero)) == 0);
    trevrpc_wt_flow_reservation data = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_data(&state, UINT64_MAX, &data) == TREV_WT_FLOW_OK);
    CHECK(data.owner == &state);
    CHECK(data.kind == TREV_WT_FLOW_RESERVATION_BYPASS);
    CHECK(state.admitted_data == 0);
    CHECK(trevrpc_wt_flow_commit(&state, &data) == TREV_WT_FLOW_OK);
    CHECK(data.kind == TREV_WT_FLOW_RESERVATION_NONE);

    trevrpc_wt_flow_reservation stream = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_stream(&state, TREV_WT_STREAM_UNI, &stream) == TREV_WT_FLOW_OK);
    CHECK(stream.kind == TREV_WT_FLOW_RESERVATION_BYPASS);
    CHECK(trevrpc_wt_flow_cancel(&state, &stream) == TREV_WT_FLOW_OK);
    CHECK(state.admitted_streams_uni == 0);
    CHECK(trevrpc_wt_flow_apply_max_data(&state, 20) == TREV_WT_FLOW_IGNORED);
    CHECK(trevrpc_wt_flow_apply_max_streams(&state, TREV_WT_STREAM_BIDI, 20) == TREV_WT_FLOW_IGNORED);
    CHECK(memcmp(&state.peer_max, &nonzero, sizeof(nonzero)) == 0);

    CHECK(trevrpc_wt_flow_state_init(&state, &nonzero, &zero) == TREV_WT_FLOW_OK);
    CHECK(!state.enabled);
    CHECK(trevrpc_wt_flow_state_init(&state, &nonzero, &nonzero) == TREV_WT_FLOW_OK);
    CHECK(state.enabled);

    trevrpc_wt_flow_state sentinel;
    memset(&sentinel, 0xa5, sizeof(sentinel));
    state = sentinel;
    trevrpc_wt_flow_limits bad_local = nonzero;
    bad_local.max_data = TREV_WT_FLOW_MAX_DATA + 1;
    CHECK(trevrpc_wt_flow_state_init(&state, &bad_local, &nonzero) == TREV_WT_FLOW_INVALID_ARGUMENT);
    CHECK(memcmp(&state, &sentinel, sizeof(state)) == 0);
    bad_local = nonzero;
    bad_local.max_streams_uni = TREV_WT_FLOW_MAX_STREAMS + 1;
    CHECK(trevrpc_wt_flow_state_init(&state, &bad_local, &nonzero) == TREV_WT_FLOW_INVALID_ARGUMENT);
    CHECK(memcmp(&state, &sentinel, sizeof(state)) == 0);
    trevrpc_wt_flow_limits bad_peer = nonzero;
    bad_peer.max_streams_bidi = TREV_WT_FLOW_MAX_STREAMS + 1;
    CHECK(trevrpc_wt_flow_state_init(&state, &nonzero, &bad_peer) == TREV_WT_FLOW_H3_DATAGRAM_ERROR);
    CHECK(memcmp(&state, &sentinel, sizeof(state)) == 0);
    bad_peer = nonzero;
    bad_peer.max_data = TREV_WT_FLOW_MAX_DATA + 1;
    CHECK(trevrpc_wt_flow_state_init(&state, &nonzero, &bad_peer) == TREV_WT_FLOW_INVALID_ARGUMENT);
    CHECK(memcmp(&state, &sentinel, sizeof(state)) == 0);
    CHECK(trevrpc_wt_flow_state_init(NULL, &nonzero, &nonzero) == TREV_WT_FLOW_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_flow_state_init(&state, NULL, &nonzero) == TREV_WT_FLOW_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_flow_state_init(&state, &nonzero, NULL) == TREV_WT_FLOW_INVALID_ARGUMENT);
    return 0;
}

static int test_flow_max_updates(void) {
    const trevrpc_wt_flow_limits initial = {
        .max_data = 100,
        .max_streams_bidi = 10,
        .max_streams_uni = 20,
    };
    trevrpc_wt_flow_state state;
    CHECK(trevrpc_wt_flow_state_init(&state, &initial, &initial) == TREV_WT_FLOW_OK);

    CHECK(trevrpc_wt_flow_apply_max_data(&state, 100) == TREV_WT_FLOW_OK);
    CHECK(state.peer_max.max_data == 100);
    CHECK(trevrpc_wt_flow_apply_max_data(&state, 101) == TREV_WT_FLOW_OK);
    CHECK(state.peer_max.max_data == 101);
    trevrpc_wt_flow_state sentinel = state;
    CHECK(trevrpc_wt_flow_apply_max_data(&state, 100) == TREV_WT_FLOW_CONTROL_VIOLATION);
    CHECK(memcmp(&state, &sentinel, sizeof(state)) == 0);
    CHECK(trevrpc_wt_flow_apply_max_data(&state, TREV_WT_FLOW_MAX_DATA) == TREV_WT_FLOW_OK);

    CHECK(trevrpc_wt_flow_apply_max_streams(&state, TREV_WT_STREAM_BIDI, 10) == TREV_WT_FLOW_OK);
    CHECK(trevrpc_wt_flow_apply_max_streams(&state, TREV_WT_STREAM_BIDI, 11) == TREV_WT_FLOW_OK);
    CHECK(state.peer_max.max_streams_bidi == 11);
    CHECK(state.peer_max.max_streams_uni == 20);
    CHECK(trevrpc_wt_flow_apply_max_streams(&state, TREV_WT_STREAM_UNI, 21) == TREV_WT_FLOW_OK);
    CHECK(state.peer_max.max_streams_bidi == 11);
    CHECK(state.peer_max.max_streams_uni == 21);
    sentinel = state;
    CHECK(trevrpc_wt_flow_apply_max_streams(&state, TREV_WT_STREAM_UNI, 20) == TREV_WT_FLOW_CONTROL_VIOLATION);
    CHECK(memcmp(&state, &sentinel, sizeof(state)) == 0);
    CHECK(trevrpc_wt_flow_apply_max_streams(&state, TREV_WT_STREAM_BIDI, TREV_WT_FLOW_MAX_STREAMS) == TREV_WT_FLOW_OK);
    sentinel = state;
    CHECK(trevrpc_wt_flow_apply_max_streams(&state, TREV_WT_STREAM_BIDI, TREV_WT_FLOW_MAX_STREAMS + 1) ==
          TREV_WT_FLOW_H3_DATAGRAM_ERROR);
    CHECK(memcmp(&state, &sentinel, sizeof(state)) == 0);
    CHECK(
        trevrpc_wt_flow_apply_max_streams(&state, (trevrpc_wt_stream_direction)9, 1) == TREV_WT_FLOW_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_flow_apply_max_data(NULL, 1) == TREV_WT_FLOW_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_flow_apply_max_data(&state, TREV_WT_FLOW_MAX_DATA + 1) == TREV_WT_FLOW_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_flow_apply_max_streams(NULL, TREV_WT_STREAM_BIDI, 1) == TREV_WT_FLOW_INVALID_ARGUMENT);
    return 0;
}

static int test_flow_admission_boundaries(void) {
    const trevrpc_wt_flow_limits local = {.max_data = 1, .max_streams_bidi = 1, .max_streams_uni = 1};
    const trevrpc_wt_flow_limits peer = {
        .max_data = TREV_WT_FLOW_MAX_DATA,
        .max_streams_bidi = TREV_WT_FLOW_MAX_STREAMS,
        .max_streams_uni = 2,
    };
    trevrpc_wt_flow_state state;
    CHECK(trevrpc_wt_flow_state_init(&state, &local, &peer) == TREV_WT_FLOW_OK);

    trevrpc_wt_flow_reservation whole = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_data(&state, TREV_WT_FLOW_MAX_DATA, &whole) == TREV_WT_FLOW_OK);
    CHECK(state.admitted_data == TREV_WT_FLOW_MAX_DATA);
    trevrpc_wt_flow_reservation blocked = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_data(&state, 1, &blocked) == TREV_WT_FLOW_BLOCKED);
    CHECK(blocked.owner == NULL);
    CHECK(blocked.kind == TREV_WT_FLOW_RESERVATION_NONE);
    CHECK(blocked.amount == 0);
    CHECK(blocked.original_amount == 0);
    CHECK(blocked.integrity == 0);
    CHECK(trevrpc_wt_flow_cancel(&state, &whole) == TREV_WT_FLOW_OK);
    CHECK(state.admitted_data == 0);

    trevrpc_wt_flow_reservation first = TREV_WT_FLOW_RESERVATION_INIT;
    trevrpc_wt_flow_reservation second = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_data(&state, TREV_WT_FLOW_MAX_DATA - 1, &first) == TREV_WT_FLOW_OK);
    CHECK(trevrpc_wt_flow_reserve_data(&state, 1, &second) == TREV_WT_FLOW_OK);
    CHECK(trevrpc_wt_flow_commit(&state, &second) == TREV_WT_FLOW_OK);
    CHECK(state.committed_data == 1);
    CHECK(trevrpc_wt_flow_cancel(&state, &first) == TREV_WT_FLOW_OK);
    CHECK(state.admitted_data == 1);
    CHECK(state.committed_data == 1);

    state.admitted_streams_bidi = TREV_WT_FLOW_MAX_STREAMS - 1;
    state.committed_streams_bidi = TREV_WT_FLOW_MAX_STREAMS - 1;
    trevrpc_wt_flow_reservation last_stream = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_stream(&state, TREV_WT_STREAM_BIDI, &last_stream) == TREV_WT_FLOW_OK);
    CHECK(state.admitted_streams_bidi == TREV_WT_FLOW_MAX_STREAMS);
    CHECK(trevrpc_wt_flow_reserve_stream(&state, TREV_WT_STREAM_BIDI, &blocked) == TREV_WT_FLOW_BLOCKED);
    CHECK(trevrpc_wt_flow_commit(&state, &last_stream) == TREV_WT_FLOW_OK);
    CHECK(state.committed_streams_bidi == TREV_WT_FLOW_MAX_STREAMS);

    trevrpc_wt_flow_reservation uni_one = TREV_WT_FLOW_RESERVATION_INIT;
    trevrpc_wt_flow_reservation uni_two = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_stream(&state, TREV_WT_STREAM_UNI, &uni_one) == TREV_WT_FLOW_OK);
    CHECK(trevrpc_wt_flow_reserve_stream(&state, TREV_WT_STREAM_UNI, &uni_two) == TREV_WT_FLOW_OK);
    CHECK(trevrpc_wt_flow_reserve_stream(&state, TREV_WT_STREAM_UNI, &blocked) == TREV_WT_FLOW_BLOCKED);
    CHECK(trevrpc_wt_flow_cancel(&state, &uni_one) == TREV_WT_FLOW_OK);
    CHECK(state.admitted_streams_uni == 1);
    CHECK(trevrpc_wt_flow_commit(&state, &uni_two) == TREV_WT_FLOW_OK);
    CHECK(state.committed_streams_uni == 1);
    return 0;
}

static int test_flow_reservation_ownership_and_atomicity(void) {
    const trevrpc_wt_flow_limits limits = {.max_data = 10, .max_streams_bidi = 2, .max_streams_uni = 2};
    trevrpc_wt_flow_state first_state;
    trevrpc_wt_flow_state second_state;
    CHECK(trevrpc_wt_flow_state_init(&first_state, &limits, &limits) == TREV_WT_FLOW_OK);
    CHECK(trevrpc_wt_flow_state_init(&second_state, &limits, &limits) == TREV_WT_FLOW_OK);

    trevrpc_wt_flow_reservation reservation = TREV_WT_FLOW_RESERVATION_INIT;
    trevrpc_wt_flow_reservation neighboring_data = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_data(&first_state, 4, &reservation) == TREV_WT_FLOW_OK);
    CHECK(trevrpc_wt_flow_reserve_data(&first_state, 3, &neighboring_data) == TREV_WT_FLOW_OK);
    trevrpc_wt_flow_state state_sentinel = first_state;
    trevrpc_wt_flow_reservation reservation_sentinel = reservation;
    CHECK(trevrpc_wt_flow_reserve_data(&first_state, 1, &reservation) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&first_state, &state_sentinel, sizeof(first_state)) == 0);
    CHECK(memcmp(&reservation, &reservation_sentinel, sizeof(reservation)) == 0);

    reservation.amount = 1;
    state_sentinel = first_state;
    reservation_sentinel = reservation;
    CHECK(trevrpc_wt_flow_commit(&first_state, &reservation) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&first_state, &state_sentinel, sizeof(first_state)) == 0);
    CHECK(memcmp(&reservation, &reservation_sentinel, sizeof(reservation)) == 0);
    reservation.amount = 7;
    reservation_sentinel = reservation;
    CHECK(trevrpc_wt_flow_cancel(&first_state, &reservation) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&first_state, &state_sentinel, sizeof(first_state)) == 0);
    CHECK(memcmp(&reservation, &reservation_sentinel, sizeof(reservation)) == 0);
    reservation.amount = 5;
    reservation.original_amount = 5;
    reservation_sentinel = reservation;
    CHECK(trevrpc_wt_flow_commit(&first_state, &reservation) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&first_state, &state_sentinel, sizeof(first_state)) == 0);
    CHECK(memcmp(&reservation, &reservation_sentinel, sizeof(reservation)) == 0);
    reservation.amount = 4;
    reservation.original_amount = 4;

    reservation_sentinel = reservation;
    CHECK(trevrpc_wt_flow_commit(&second_state, &reservation) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&reservation, &reservation_sentinel, sizeof(reservation)) == 0);
    CHECK(trevrpc_wt_flow_cancel(&second_state, &reservation) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(trevrpc_wt_flow_commit(&first_state, &reservation) == TREV_WT_FLOW_OK);
    CHECK(first_state.committed_data == 4);
    CHECK(first_state.admitted_data == 7);
    CHECK(trevrpc_wt_flow_cancel(&first_state, &neighboring_data) == TREV_WT_FLOW_OK);
    CHECK(first_state.admitted_data == 4);
    CHECK(reservation.kind == TREV_WT_FLOW_RESERVATION_NONE);
    CHECK(reservation.amount == 0);
    CHECK(reservation.original_amount == 0);
    CHECK(reservation.integrity == 0);
    CHECK(trevrpc_wt_flow_cancel(&first_state, &reservation) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(first_state.committed_data == 4);
    CHECK(first_state.admitted_data == 4);

    trevrpc_wt_flow_reservation malformed = TREV_WT_FLOW_RESERVATION_INIT;
    malformed.amount = 1;
    state_sentinel = first_state;
    reservation_sentinel = malformed;
    CHECK(trevrpc_wt_flow_reserve_stream(&first_state, TREV_WT_STREAM_BIDI, &malformed) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&first_state, &state_sentinel, sizeof(first_state)) == 0);
    CHECK(memcmp(&malformed, &reservation_sentinel, sizeof(malformed)) == 0);

    trevrpc_wt_flow_reservation stream = TREV_WT_FLOW_RESERVATION_INIT;
    trevrpc_wt_flow_reservation neighboring_stream = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_stream(&first_state, TREV_WT_STREAM_BIDI, &stream) == TREV_WT_FLOW_OK);
    CHECK(trevrpc_wt_flow_reserve_stream(&first_state, TREV_WT_STREAM_BIDI, &neighboring_stream) == TREV_WT_FLOW_OK);
    stream.amount = 2;
    state_sentinel = first_state;
    reservation_sentinel = stream;
    CHECK(trevrpc_wt_flow_commit(&first_state, &stream) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&first_state, &state_sentinel, sizeof(first_state)) == 0);
    CHECK(memcmp(&stream, &reservation_sentinel, sizeof(stream)) == 0);
    stream.original_amount = 2;
    reservation_sentinel = stream;
    CHECK(trevrpc_wt_flow_cancel(&first_state, &stream) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&first_state, &state_sentinel, sizeof(first_state)) == 0);
    CHECK(memcmp(&stream, &reservation_sentinel, sizeof(stream)) == 0);
    stream.amount = 1;
    stream.original_amount = 1;
    CHECK(trevrpc_wt_flow_cancel(&first_state, &stream) == TREV_WT_FLOW_OK);
    CHECK(first_state.admitted_streams_bidi == 1);
    CHECK(trevrpc_wt_flow_cancel(&first_state, &neighboring_stream) == TREV_WT_FLOW_OK);
    CHECK(first_state.admitted_streams_bidi == 0);

    CHECK(trevrpc_wt_flow_reserve_data(NULL, 1, &malformed) == TREV_WT_FLOW_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_flow_reserve_data(&first_state, 1, NULL) == TREV_WT_FLOW_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_flow_reserve_stream(&first_state, (trevrpc_wt_stream_direction)7, &malformed) ==
          TREV_WT_FLOW_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_flow_commit(NULL, &malformed) == TREV_WT_FLOW_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_flow_commit(&first_state, NULL) == TREV_WT_FLOW_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_flow_cancel(NULL, &malformed) == TREV_WT_FLOW_INVALID_ARGUMENT);
    CHECK(trevrpc_wt_flow_cancel(&first_state, NULL) == TREV_WT_FLOW_INVALID_ARGUMENT);

    trevrpc_wt_flow_state reset_state = first_state;
    CHECK(trevrpc_wt_flow_state_init(&reset_state, &limits, &limits) == TREV_WT_FLOW_OK);
    CHECK(reset_state.admitted_data == 0);
    CHECK(reset_state.committed_data == 0);
    CHECK(reset_state.enabled);
    return 0;
}

static int test_flow_reservation_identity_and_generation(void) {
    const trevrpc_wt_flow_limits limits = {
        .max_data = 8,
        .max_streams_bidi = 2,
        .max_streams_uni = 2,
    };
    trevrpc_wt_flow_state state;
    CHECK(trevrpc_wt_flow_state_init(&state, &limits, &limits) == TREV_WT_FLOW_OK);

    trevrpc_wt_flow_reservation data_a = TREV_WT_FLOW_RESERVATION_INIT;
    trevrpc_wt_flow_reservation data_b = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_data(&state, 4, &data_a) == TREV_WT_FLOW_OK);
    trevrpc_wt_flow_reservation data_copy = data_a;
    trevrpc_wt_flow_state state_sentinel = state;
    trevrpc_wt_flow_reservation copy_sentinel = data_copy;
    CHECK(trevrpc_wt_flow_commit(&state, &data_copy) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&state, &state_sentinel, sizeof(state)) == 0);
    CHECK(memcmp(&data_copy, &copy_sentinel, sizeof(data_copy)) == 0);
    CHECK(trevrpc_wt_flow_reserve_data(&state, 4, &data_b) == TREV_WT_FLOW_OK);
    CHECK(trevrpc_wt_flow_commit(&state, &data_a) == TREV_WT_FLOW_OK);
    state_sentinel = state;
    copy_sentinel = data_copy;
    CHECK(trevrpc_wt_flow_commit(&state, &data_copy) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&state, &state_sentinel, sizeof(state)) == 0);
    CHECK(memcmp(&data_copy, &copy_sentinel, sizeof(data_copy)) == 0);
    CHECK(trevrpc_wt_flow_cancel(&state, &data_copy) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(trevrpc_wt_flow_commit(&state, &data_b) == TREV_WT_FLOW_OK);
    CHECK(state.admitted_data == 8);
    CHECK(state.committed_data == 8);

    trevrpc_wt_flow_reservation stream_a = TREV_WT_FLOW_RESERVATION_INIT;
    trevrpc_wt_flow_reservation stream_b = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_stream(&state, TREV_WT_STREAM_BIDI, &stream_a) == TREV_WT_FLOW_OK);
    trevrpc_wt_flow_reservation stream_copy = stream_a;
    CHECK(trevrpc_wt_flow_reserve_stream(&state, TREV_WT_STREAM_BIDI, &stream_b) == TREV_WT_FLOW_OK);
    CHECK(trevrpc_wt_flow_cancel(&state, &stream_a) == TREV_WT_FLOW_OK);
    state_sentinel = state;
    copy_sentinel = stream_copy;
    CHECK(trevrpc_wt_flow_commit(&state, &stream_copy) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&state, &state_sentinel, sizeof(state)) == 0);
    CHECK(memcmp(&stream_copy, &copy_sentinel, sizeof(stream_copy)) == 0);
    CHECK(trevrpc_wt_flow_cancel(&state, &stream_copy) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(trevrpc_wt_flow_commit(&state, &stream_b) == TREV_WT_FLOW_OK);
    CHECK(state.admitted_streams_bidi == 1);
    CHECK(state.committed_streams_bidi == 1);

    trevrpc_wt_flow_reservation stale_data = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_state_init(&state, &limits, &limits) == TREV_WT_FLOW_OK);
    CHECK(trevrpc_wt_flow_reserve_data(&state, 3, &stale_data) == TREV_WT_FLOW_OK);
    uint64_t stale_data_generation = stale_data.owner_generation;
    CHECK(trevrpc_wt_flow_state_init(&state, &limits, &limits) == TREV_WT_FLOW_OK);
    CHECK(state.generation != stale_data_generation);
    trevrpc_wt_flow_reservation current_data = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_data(&state, 3, &current_data) == TREV_WT_FLOW_OK);
    state_sentinel = state;
    CHECK(trevrpc_wt_flow_commit(&state, &stale_data) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(trevrpc_wt_flow_cancel(&state, &stale_data) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&state, &state_sentinel, sizeof(state)) == 0);
    CHECK(trevrpc_wt_flow_commit(&state, &current_data) == TREV_WT_FLOW_OK);
    CHECK(state.admitted_data == 3);
    CHECK(state.committed_data == 3);

    trevrpc_wt_flow_reservation stale_stream = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_stream(&state, TREV_WT_STREAM_UNI, &stale_stream) == TREV_WT_FLOW_OK);
    uint64_t stale_stream_generation = stale_stream.owner_generation;
    CHECK(trevrpc_wt_flow_state_init(&state, &limits, &limits) == TREV_WT_FLOW_OK);
    CHECK(state.generation != stale_stream_generation);
    state_sentinel = state;
    CHECK(trevrpc_wt_flow_commit(&state, &stale_stream) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(trevrpc_wt_flow_cancel(&state, &stale_stream) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&state, &state_sentinel, sizeof(state)) == 0);
    return 0;
}

static int test_flow_reservation_adjacent_corruption_policy(void) {
    const trevrpc_wt_flow_limits limits = {
        .max_data = 8,
        .max_streams_bidi = 2,
        .max_streams_uni = 2,
    };

    enum adjacent_corruption {
        CORRUPT_NEXT,
        CORRUPT_PREV,
        CORRUPT_KIND,
        CORRUPT_AMOUNT,
        CORRUPT_IDENTITY,
        CORRUPT_INTEGRITY,
        CORRUPTION_COUNT,
    };
    for (int mutation = CORRUPT_NEXT; mutation < CORRUPTION_COUNT; mutation++) {
        trevrpc_wt_flow_state state;
        CHECK(trevrpc_wt_flow_state_init(&state, &limits, &limits) == TREV_WT_FLOW_OK);
        trevrpc_wt_flow_reservation damaged_tail = TREV_WT_FLOW_RESERVATION_INIT;
        trevrpc_wt_flow_reservation adjacent = TREV_WT_FLOW_RESERVATION_INIT;
        trevrpc_wt_flow_reservation near_head = TREV_WT_FLOW_RESERVATION_INIT;
        trevrpc_wt_flow_reservation head = TREV_WT_FLOW_RESERVATION_INIT;
        CHECK(trevrpc_wt_flow_reserve_data(&state, 1, &damaged_tail) == TREV_WT_FLOW_OK);
        CHECK(trevrpc_wt_flow_reserve_data(&state, 1, &adjacent) == TREV_WT_FLOW_OK);
        CHECK(trevrpc_wt_flow_reserve_data(&state, 1, &near_head) == TREV_WT_FLOW_OK);
        CHECK(trevrpc_wt_flow_reserve_data(&state, 1, &head) == TREV_WT_FLOW_OK);
        trevrpc_wt_flow_reservation valid_tail = damaged_tail;

        switch ((enum adjacent_corruption)mutation) {
        case CORRUPT_NEXT:
            damaged_tail.active_next = (trevrpc_wt_flow_reservation*)(uintptr_t)1;
            break;
        case CORRUPT_PREV:
            damaged_tail.active_prev = (trevrpc_wt_flow_reservation*)(uintptr_t)1;
            break;
        case CORRUPT_KIND:
            damaged_tail.kind = TREV_WT_FLOW_RESERVATION_STREAM_UNI;
            break;
        case CORRUPT_AMOUNT:
            damaged_tail.amount = 2;
            break;
        case CORRUPT_IDENTITY:
            damaged_tail.identity++;
            break;
        case CORRUPT_INTEGRITY:
            damaged_tail.integrity ^= UINT64_C(1);
            break;
        case CORRUPTION_COUNT:
            CHECK(false);
        }

        trevrpc_wt_flow_reservation damaged_sentinel = damaged_tail;
        CHECK(trevrpc_wt_flow_commit(&state, &head) == TREV_WT_FLOW_OK);
        CHECK(state.admitted_data == 4);
        CHECK(state.committed_data == 1);
        CHECK(memcmp(&damaged_tail, &damaged_sentinel, sizeof(damaged_tail)) == 0);

        trevrpc_wt_flow_state state_sentinel = state;
        trevrpc_wt_flow_reservation adjacent_sentinel = adjacent;
        trevrpc_wt_flow_reservation near_head_sentinel = near_head;
        damaged_sentinel = damaged_tail;
        CHECK(trevrpc_wt_flow_cancel(&state, &adjacent) == TREV_WT_FLOW_INVALID_STATE);
        CHECK(memcmp(&state, &state_sentinel, sizeof(state)) == 0);
        CHECK(memcmp(&damaged_tail, &damaged_sentinel, sizeof(damaged_tail)) == 0);
        CHECK(memcmp(&adjacent, &adjacent_sentinel, sizeof(adjacent)) == 0);
        CHECK(memcmp(&near_head, &near_head_sentinel, sizeof(near_head)) == 0);

        damaged_tail = valid_tail;
        CHECK(trevrpc_wt_flow_cancel(&state, &adjacent) == TREV_WT_FLOW_OK);
        CHECK(trevrpc_wt_flow_commit(&state, &damaged_tail) == TREV_WT_FLOW_OK);
        CHECK(trevrpc_wt_flow_cancel(&state, &near_head) == TREV_WT_FLOW_OK);
        CHECK(state.active_reservations == NULL);
        CHECK(state.admitted_data == 2);
        CHECK(state.committed_data == 2);
    }
    return 0;
}

static int test_flow_reservation_link_integrity_and_identity_wrap(void) {
    const trevrpc_wt_flow_limits limits = {
        .max_data = 8,
        .max_streams_bidi = 2,
        .max_streams_uni = 2,
    };
    trevrpc_wt_flow_state state;
    CHECK(trevrpc_wt_flow_state_init(&state, &limits, &limits) == TREV_WT_FLOW_OK);

    trevrpc_wt_flow_reservation neighbor = TREV_WT_FLOW_RESERVATION_INIT;
    trevrpc_wt_flow_reservation head = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_data(&state, 1, &neighbor) == TREV_WT_FLOW_OK);
    CHECK(trevrpc_wt_flow_reserve_data(&state, 1, &head) == TREV_WT_FLOW_OK);
    trevrpc_wt_flow_reservation valid_neighbor = neighbor;
    neighbor.active_next = (trevrpc_wt_flow_reservation*)(uintptr_t)1;
    trevrpc_wt_flow_state state_sentinel = state;
    trevrpc_wt_flow_reservation neighbor_sentinel = neighbor;
    trevrpc_wt_flow_reservation head_sentinel = head;
    CHECK(trevrpc_wt_flow_commit(&state, &head) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&state, &state_sentinel, sizeof(state)) == 0);
    CHECK(memcmp(&neighbor, &neighbor_sentinel, sizeof(neighbor)) == 0);
    CHECK(memcmp(&head, &head_sentinel, sizeof(head)) == 0);
    neighbor = valid_neighbor;
    CHECK(trevrpc_wt_flow_commit(&state, &head) == TREV_WT_FLOW_OK);
    CHECK(trevrpc_wt_flow_cancel(&state, &neighbor) == TREV_WT_FLOW_OK);

    CHECK(trevrpc_wt_flow_state_init(&state, &limits, &limits) == TREV_WT_FLOW_OK);
    trevrpc_wt_flow_reservation first = TREV_WT_FLOW_RESERVATION_INIT;
    trevrpc_wt_flow_reservation middle = TREV_WT_FLOW_RESERVATION_INIT;
    trevrpc_wt_flow_reservation last = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_data(&state, 1, &first) == TREV_WT_FLOW_OK);
    CHECK(trevrpc_wt_flow_reserve_data(&state, 1, &middle) == TREV_WT_FLOW_OK);
    CHECK(trevrpc_wt_flow_reserve_data(&state, 1, &last) == TREV_WT_FLOW_OK);
    CHECK(state.active_reservations == &last);
    CHECK(last.active_next == &middle);
    CHECK(middle.active_prev == &last);
    CHECK(middle.active_next == &first);
    CHECK(first.active_prev == &middle);

    trevrpc_wt_flow_reservation valid_middle = middle;
    middle.active_next = (trevrpc_wt_flow_reservation*)(uintptr_t)1;
    state_sentinel = state;
    trevrpc_wt_flow_reservation first_sentinel = first;
    trevrpc_wt_flow_reservation middle_sentinel = middle;
    trevrpc_wt_flow_reservation last_sentinel = last;
    CHECK(trevrpc_wt_flow_commit(&state, &middle) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&state, &state_sentinel, sizeof(state)) == 0);
    CHECK(memcmp(&first, &first_sentinel, sizeof(first)) == 0);
    CHECK(memcmp(&middle, &middle_sentinel, sizeof(middle)) == 0);
    CHECK(memcmp(&last, &last_sentinel, sizeof(last)) == 0);
    middle = valid_middle;

    middle.active_prev = (trevrpc_wt_flow_reservation*)(uintptr_t)1;
    middle_sentinel = middle;
    CHECK(trevrpc_wt_flow_cancel(&state, &middle) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&state, &state_sentinel, sizeof(state)) == 0);
    CHECK(memcmp(&middle, &middle_sentinel, sizeof(middle)) == 0);
    middle = valid_middle;

    trevrpc_wt_flow_reservation valid_last = last;
    last.active_next = &first;
    state_sentinel = state;
    first_sentinel = first;
    middle_sentinel = middle;
    last_sentinel = last;
    CHECK(trevrpc_wt_flow_commit(&state, &middle) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&state, &state_sentinel, sizeof(state)) == 0);
    CHECK(memcmp(&first, &first_sentinel, sizeof(first)) == 0);
    CHECK(memcmp(&middle, &middle_sentinel, sizeof(middle)) == 0);
    CHECK(memcmp(&last, &last_sentinel, sizeof(last)) == 0);
    last = valid_last;

    CHECK(trevrpc_wt_flow_cancel(&state, &middle) == TREV_WT_FLOW_OK);
    CHECK(state.active_reservations == &last);
    CHECK(last.active_next == &first);
    CHECK(first.active_prev == &last);
    CHECK(trevrpc_wt_flow_commit(&state, &first) == TREV_WT_FLOW_OK);
    CHECK(last.active_next == NULL);
    CHECK(trevrpc_wt_flow_cancel(&state, &last) == TREV_WT_FLOW_OK);
    CHECK(state.active_reservations == NULL);
    CHECK(state.admitted_data == 1);
    CHECK(state.committed_data == 1);

    CHECK(trevrpc_wt_flow_state_init(&state, &limits, &limits) == TREV_WT_FLOW_OK);
    state.next_reservation_identity = UINT64_MAX;
    trevrpc_wt_flow_reservation final_identity = TREV_WT_FLOW_RESERVATION_INIT;
    CHECK(trevrpc_wt_flow_reserve_data(&state, 1, &final_identity) == TREV_WT_FLOW_OK);
    CHECK(final_identity.identity == UINT64_MAX);
    CHECK(state.next_reservation_identity == 0);

    trevrpc_wt_flow_reservation exhausted = TREV_WT_FLOW_RESERVATION_INIT;
    state_sentinel = state;
    trevrpc_wt_flow_reservation exhausted_sentinel = exhausted;
    CHECK(trevrpc_wt_flow_reserve_data(&state, 1, &exhausted) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&state, &state_sentinel, sizeof(state)) == 0);
    CHECK(memcmp(&exhausted, &exhausted_sentinel, sizeof(exhausted)) == 0);
    CHECK(trevrpc_wt_flow_commit(&state, &final_identity) == TREV_WT_FLOW_OK);
    state_sentinel = state;
    CHECK(trevrpc_wt_flow_reserve_stream(&state, TREV_WT_STREAM_BIDI, &exhausted) == TREV_WT_FLOW_INVALID_STATE);
    CHECK(memcmp(&state, &state_sentinel, sizeof(state)) == 0);
    CHECK(memcmp(&exhausted, &exhausted_sentinel, sizeof(exhausted)) == 0);
    return 0;
}

int main(void) {
    int (*const tests[])(void) = {
        test_profile_capability_matrix,
        test_disabled_flow_control_capsules,
        test_parser_workspace_boundaries,
        test_fragmented_eight_byte_varints,
        test_nonminimal_encodings,
        test_numeric_malformed_lengths_and_limits,
        test_unknown_capsules,
        test_close_lengths_and_fragmentation,
        test_invalid_utf8,
        test_drain_and_post_close,
        test_finish_boundaries_and_truncation,
        test_feed_output_alias_rejection,
        test_finish_output_alias_rejection,
        test_zero_length_feeds_and_failure_stickiness,
        test_parser_atomicity_and_sticky_failures,
        test_flow_initialization_and_disabled_bypass,
        test_flow_max_updates,
        test_flow_admission_boundaries,
        test_flow_reservation_ownership_and_atomicity,
        test_flow_reservation_identity_and_generation,
        test_flow_reservation_adjacent_corruption_policy,
        test_flow_reservation_link_integrity_and_identity_wrap,
    };
    for (size_t i = 0; i < ARRAY_LEN(tests); i++) {
        int err = tests[i]();
        if (err != 0) {
            return err;
        }
    }
    return 0;
}
