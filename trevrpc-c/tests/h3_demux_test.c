#include "trevrpc_h3_demux_internal.h"

// NOLINTNEXTLINE(misc-include-cleaner)
#include <errno.h>
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

static const trevrpc_h3_demux_profile_capabilities pending_profile = {0};
static const trevrpc_h3_demux_profile_capabilities no_webtransport = {
    .resolved = true,
};
static const trevrpc_h3_demux_profile_capabilities webtransport_profile = {
    .resolved = true,
    .selected_profile = TREV_WT_PROFILE_DRAFT_15,
};

static trevrpc_h3_demux_result sentinel_result(void) {
    return (trevrpc_h3_demux_result){
        .consumed = 99,
        .action = TREV_H3_DEMUX_ACTION_WEBTRANSPORT,
        .first_value = 0xaaaa,
        .session_id = 0xbbbb,
        .application_error = 0xcccc,
    };
}

static int result_is_sentinel(const trevrpc_h3_demux_result* result) {
    return result->consumed == 99 && result->action == TREV_H3_DEMUX_ACTION_WEBTRANSPORT &&
           result->first_value == 0xaaaa && result->session_id == 0xbbbb && result->application_error == 0xcccc;
}

static int test_unidirectional_actions(void) {
    const struct {
        uint8_t type;
        trevrpc_h3_demux_action action;
    } cases[] = {
        {0x00, TREV_H3_DEMUX_ACTION_CONTROL},
        {0x02, TREV_H3_DEMUX_ACTION_QPACK_ENCODER},
        {0x03, TREV_H3_DEMUX_ACTION_QPACK_DECODER},
        {0x21, TREV_H3_DEMUX_ACTION_UNKNOWN_UNIDIRECTIONAL},
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        trevrpc_h3_demux_stream stream;
        trevrpc_h3_demux_result result = sentinel_result();
        CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_UNIDIRECTIONAL) == 0);
        CHECK(trevrpc_h3_demux_stream_feed(&stream, &pending_profile, &cases[i].type, 1, &result) ==
              TREV_H3_DEMUX_ACTION_READY);
        CHECK(result.consumed == 1);
        CHECK(result.action == cases[i].action);
        CHECK(result.first_value == cases[i].type);
        CHECK(result.application_error == 0);

        result = sentinel_result();
        CHECK(trevrpc_h3_demux_stream_feed(&stream, &pending_profile, &cases[i].type, 1, &result) ==
              TREV_H3_DEMUX_COMPLETE);
        CHECK(result.consumed == 0);
        CHECK(result.action == TREV_H3_DEMUX_ACTION_NONE);
    }
    return 0;
}

static int test_request_action_is_profile_independent(void) {
    const trevrpc_h3_demux_profile_capabilities profiles[] = {
        pending_profile,
        no_webtransport,
        webtransport_profile,
    };
    const uint8_t request[] = {0x01, 0xaa};
    for (size_t i = 0; i < sizeof(profiles) / sizeof(profiles[0]); i++) {
        trevrpc_h3_demux_stream stream;
        trevrpc_h3_demux_result result = sentinel_result();
        CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_BIDIRECTIONAL) == 0);
        CHECK(trevrpc_h3_demux_stream_feed(&stream, &profiles[i], request, sizeof(request), &result) ==
              TREV_H3_DEMUX_ACTION_READY);
        CHECK(result.consumed == 1);
        CHECK(result.action == TREV_H3_DEMUX_ACTION_REQUEST);
        CHECK(result.first_value == TREV_H3_FRAME_HEADERS);
    }
    return 0;
}

static int test_unknown_request_frames_are_skipped(void) {
    const uint8_t fragmented[] = {0x40, 0x21, 0x40, 0x03, 0xaa, 0xbb, 0xcc, 0x01, 0xee};
    for (size_t split = 1; split < 8; split++) {
        trevrpc_h3_demux_stream stream;
        trevrpc_h3_demux_result result = sentinel_result();
        CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_BIDIRECTIONAL) == 0);
        CHECK(trevrpc_h3_demux_stream_feed(&stream, &no_webtransport, fragmented, split, &result) ==
              TREV_H3_DEMUX_NEED_MORE);
        CHECK(result.consumed == split);
        CHECK(trevrpc_h3_demux_stream_feed(
                  &stream, &no_webtransport, fragmented + split, sizeof(fragmented) - split, &result) ==
              TREV_H3_DEMUX_ACTION_READY);
        CHECK(split + result.consumed == 8);
        CHECK(result.action == TREV_H3_DEMUX_ACTION_REQUEST);
        CHECK(result.first_value == TREV_H3_FRAME_HEADERS);
        CHECK(result.application_error == 0);
    }

    const uint8_t zero_length[] = {0x21, 0x00, 0x01, 0xaa};
    trevrpc_h3_demux_stream stream;
    trevrpc_h3_demux_result result = sentinel_result();
    CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_BIDIRECTIONAL) == 0);
    CHECK(trevrpc_h3_demux_stream_feed(&stream, &no_webtransport, zero_length, sizeof(zero_length), &result) ==
          TREV_H3_DEMUX_ACTION_READY);
    CHECK(result.consumed == 3);
    CHECK(result.action == TREV_H3_DEMUX_ACTION_REQUEST);

    const uint8_t wt_type_after_unknown[] = {0x21, 0x00, 0x40, 0x41, 0x01, 0xaa, 0x01, 0xee};
    CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_BIDIRECTIONAL) == 0);
    CHECK(trevrpc_h3_demux_stream_feed(
              &stream, &webtransport_profile, wt_type_after_unknown, sizeof(wt_type_after_unknown), &result) ==
          TREV_H3_DEMUX_ACTION_READY);
    CHECK(result.consumed == 7);
    CHECK(result.action == TREV_H3_DEMUX_ACTION_REQUEST);
    CHECK(result.first_value == TREV_H3_FRAME_HEADERS);
    return 0;
}

static int test_webtransport_fragmented_at_every_boundary(void) {
    const struct {
        trevrpc_h3_demux_direction direction;
        uint8_t stream_type;
    } cases[] = {
        {TREV_H3_DEMUX_BIDIRECTIONAL, TREV_H3_DEMUX_WEBTRANSPORT_BIDI},
        {TREV_H3_DEMUX_UNIDIRECTIONAL, TREV_H3_DEMUX_WEBTRANSPORT_UNI},
    };

    for (size_t case_index = 0; case_index < sizeof(cases) / sizeof(cases[0]); case_index++) {
        const uint8_t bytes[] = {0x40, cases[case_index].stream_type, 0x40, 0x40, 0xbb};
        for (size_t split = 1; split < 4; split++) {
            trevrpc_h3_demux_stream stream;
            trevrpc_h3_demux_result result = sentinel_result();
            CHECK(trevrpc_h3_demux_stream_init(&stream, cases[case_index].direction) == 0);
            CHECK(trevrpc_h3_demux_stream_feed(&stream, &webtransport_profile, bytes, split, &result) ==
                  TREV_H3_DEMUX_NEED_MORE);
            CHECK(result.consumed == split);
            CHECK(result.action == TREV_H3_DEMUX_ACTION_NONE);

            CHECK(trevrpc_h3_demux_stream_feed(
                      &stream, &webtransport_profile, bytes + split, sizeof(bytes) - split, &result) ==
                  TREV_H3_DEMUX_ACTION_READY);
            CHECK(split + result.consumed == 4);
            CHECK(result.action == TREV_H3_DEMUX_ACTION_WEBTRANSPORT);
            CHECK(result.first_value == cases[case_index].stream_type);
            CHECK(result.session_id == 64);
        }
    }
    return 0;
}

static int test_profile_pending_and_none_routing(void) {
    const uint8_t bidi_bytes[] = {0x40, TREV_H3_DEMUX_WEBTRANSPORT_BIDI, 0x00, 0x01, 0xaa};
    trevrpc_h3_demux_stream stream;
    trevrpc_h3_demux_result result = sentinel_result();
    CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_BIDIRECTIONAL) == 0);
    CHECK(trevrpc_h3_demux_stream_feed(&stream, &pending_profile, bidi_bytes, sizeof(bidi_bytes), &result) ==
          TREV_H3_DEMUX_WAIT_PROFILE);
    CHECK(result.consumed == 2);
    CHECK(result.first_value == TREV_H3_DEMUX_WEBTRANSPORT_BIDI);
    CHECK(result.action == TREV_H3_DEMUX_ACTION_NONE);
    CHECK(trevrpc_h3_demux_stream_feed(&stream, &pending_profile, bidi_bytes + 2, 3, &result) ==
          TREV_H3_DEMUX_WAIT_PROFILE);
    CHECK(result.consumed == 0);

    CHECK(trevrpc_h3_demux_stream_feed(&stream, &no_webtransport, bidi_bytes + 2, 3, &result) ==
          TREV_H3_DEMUX_ACTION_READY);
    CHECK(result.consumed == 2);
    CHECK(result.action == TREV_H3_DEMUX_ACTION_REQUEST);
    CHECK(result.first_value == TREV_H3_FRAME_HEADERS);
    CHECK(result.application_error == 0);

    CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_BIDIRECTIONAL) == 0);
    CHECK(trevrpc_h3_demux_stream_feed(&stream, &no_webtransport, bidi_bytes, sizeof(bidi_bytes), &result) ==
          TREV_H3_DEMUX_ACTION_READY);
    CHECK(result.consumed == 4);
    CHECK(result.action == TREV_H3_DEMUX_ACTION_REQUEST);

    const uint8_t uni_bytes[] = {0x40, TREV_H3_DEMUX_WEBTRANSPORT_UNI, 0x00, 0xaa};
    CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_UNIDIRECTIONAL) == 0);
    CHECK(trevrpc_h3_demux_stream_feed(&stream, &pending_profile, uni_bytes, sizeof(uni_bytes), &result) ==
          TREV_H3_DEMUX_WAIT_PROFILE);
    CHECK(result.consumed == 2);
    CHECK(trevrpc_h3_demux_stream_feed(&stream, &no_webtransport, uni_bytes + 2, 2, &result) ==
          TREV_H3_DEMUX_ACTION_READY);
    CHECK(result.consumed == 0);
    CHECK(result.action == TREV_H3_DEMUX_ACTION_UNKNOWN_UNIDIRECTIONAL);
    CHECK(result.first_value == TREV_H3_DEMUX_WEBTRANSPORT_UNI);
    CHECK(result.application_error == 0);
    return 0;
}

static int test_profile_pending_wakes_to_webtransport(void) {
    const struct {
        trevrpc_h3_demux_direction direction;
        uint8_t stream_type;
    } cases[] = {
        {TREV_H3_DEMUX_BIDIRECTIONAL, TREV_H3_DEMUX_WEBTRANSPORT_BIDI},
        {TREV_H3_DEMUX_UNIDIRECTIONAL, TREV_H3_DEMUX_WEBTRANSPORT_UNI},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        const uint8_t type[] = {0x40, cases[i].stream_type};
        const uint8_t session_and_payload[] = {0x40, 0x40, 0xcc};
        trevrpc_h3_demux_stream stream;
        trevrpc_h3_demux_result result = sentinel_result();
        CHECK(trevrpc_h3_demux_stream_init(&stream, cases[i].direction) == 0);
        CHECK(trevrpc_h3_demux_stream_feed(&stream, &pending_profile, type, sizeof(type), &result) ==
              TREV_H3_DEMUX_WAIT_PROFILE);
        CHECK(result.consumed == sizeof(type));
        CHECK(trevrpc_h3_demux_stream_feed(
                  &stream, &webtransport_profile, session_and_payload, sizeof(session_and_payload), &result) ==
              TREV_H3_DEMUX_ACTION_READY);
        CHECK(result.consumed == 2);
        CHECK(result.action == TREV_H3_DEMUX_ACTION_WEBTRANSPORT);
        CHECK(result.session_id == 64);
    }
    return 0;
}

static int test_valid_and_invalid_session_ids_both_directions(void) {
    const uint8_t valid_ids[][8] = {
        {0x00},
        {0x04},
        {0x40, 0x40},
        {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xfc},
    };
    const size_t valid_lengths[] = {1, 1, 2, 8};
    const uint8_t invalid_ids[][8] = {
        {0x02},
        {0x40, 0x42},
        {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
    };
    const size_t invalid_lengths[] = {1, 2, 8};
    const struct {
        trevrpc_h3_demux_direction direction;
        uint8_t type[2];
    } directions[] = {
        {TREV_H3_DEMUX_BIDIRECTIONAL, {0x40, TREV_H3_DEMUX_WEBTRANSPORT_BIDI}},
        {TREV_H3_DEMUX_UNIDIRECTIONAL, {0x40, TREV_H3_DEMUX_WEBTRANSPORT_UNI}},
    };

    for (size_t direction = 0; direction < sizeof(directions) / sizeof(directions[0]); direction++) {
        for (size_t id = 0; id < sizeof(valid_lengths) / sizeof(valid_lengths[0]); id++) {
            uint8_t bytes[11] = {0};
            memcpy(bytes, directions[direction].type, 2);
            memcpy(bytes + 2, valid_ids[id], valid_lengths[id]);
            bytes[2 + valid_lengths[id]] = 0xdd;
            trevrpc_h3_demux_stream stream;
            trevrpc_h3_demux_result result = sentinel_result();
            CHECK(trevrpc_h3_demux_stream_init(&stream, directions[direction].direction) == 0);
            CHECK(trevrpc_h3_demux_stream_feed(&stream, &webtransport_profile, bytes, 3 + valid_lengths[id], &result) ==
                  TREV_H3_DEMUX_ACTION_READY);
            CHECK(result.consumed == 2 + valid_lengths[id]);
            CHECK(result.action == TREV_H3_DEMUX_ACTION_WEBTRANSPORT);
        }
        for (size_t id = 0; id < sizeof(invalid_lengths) / sizeof(invalid_lengths[0]); id++) {
            uint8_t bytes[10] = {0};
            memcpy(bytes, directions[direction].type, 2);
            memcpy(bytes + 2, invalid_ids[id], invalid_lengths[id]);
            trevrpc_h3_demux_stream stream;
            trevrpc_h3_demux_result result = sentinel_result();
            CHECK(trevrpc_h3_demux_stream_init(&stream, directions[direction].direction) == 0);
            CHECK(
                trevrpc_h3_demux_stream_feed(&stream, &webtransport_profile, bytes, 2 + invalid_lengths[id], &result) ==
                TREV_H3_DEMUX_PROTOCOL_ERROR);
            CHECK(result.consumed == 2 + invalid_lengths[id]);
            CHECK(result.application_error == TREV_H3_DEMUX_APP_ID_ERROR);
        }
    }
    return 0;
}

static int test_all_concrete_profiles_accept_both_webtransport_prefixes(void) {
    const trevrpc_wt_profile_id profiles[] = {
        TREV_WT_PROFILE_DRAFT_02,
        TREV_WT_PROFILE_DRAFT_07,
        TREV_WT_PROFILE_DRAFT_14,
        TREV_WT_PROFILE_DRAFT_15,
    };
    const struct {
        trevrpc_h3_demux_direction direction;
        uint8_t stream_type;
    } streams[] = {
        {TREV_H3_DEMUX_BIDIRECTIONAL, TREV_H3_DEMUX_WEBTRANSPORT_BIDI},
        {TREV_H3_DEMUX_UNIDIRECTIONAL, TREV_H3_DEMUX_WEBTRANSPORT_UNI},
    };

    for (size_t profile_index = 0; profile_index < sizeof(profiles) / sizeof(profiles[0]); profile_index++) {
        const trevrpc_h3_demux_profile_capabilities profile = {
            .resolved = true,
            .selected_profile = profiles[profile_index],
        };
        for (size_t stream_index = 0; stream_index < sizeof(streams) / sizeof(streams[0]); stream_index++) {
            const uint8_t bytes[] = {0x40, streams[stream_index].stream_type, 0x04, 0xaa};
            trevrpc_h3_demux_stream stream;
            trevrpc_h3_demux_result result = sentinel_result();
            CHECK(trevrpc_h3_demux_stream_init(&stream, streams[stream_index].direction) == 0);
            CHECK(trevrpc_h3_demux_stream_feed(&stream, &profile, bytes, sizeof(bytes), &result) ==
                  TREV_H3_DEMUX_ACTION_READY);
            CHECK(result.consumed == 3);
            CHECK(result.action == TREV_H3_DEMUX_ACTION_WEBTRANSPORT);
            CHECK(result.first_value == streams[stream_index].stream_type);
            CHECK(result.session_id == 4);
        }
    }
    return 0;
}

static int test_fragmented_nonminimal_varints(void) {
    const uint8_t webtransport[] = {
        0xc0,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x00,
        0x41,
        0x80,
        0x00,
        0x00,
        0x04,
    };
    trevrpc_h3_demux_stream stream;
    trevrpc_h3_demux_result result = sentinel_result();
    CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_BIDIRECTIONAL) == 0);
    for (size_t i = 0; i < sizeof(webtransport); i++) {
        trevrpc_h3_demux_status status =
            trevrpc_h3_demux_stream_feed(&stream, &webtransport_profile, &webtransport[i], 1, &result);
        CHECK(result.consumed == 1);
        CHECK(status == (i + 1 == sizeof(webtransport) ? TREV_H3_DEMUX_ACTION_READY : TREV_H3_DEMUX_NEED_MORE));
    }
    CHECK(result.action == TREV_H3_DEMUX_ACTION_WEBTRANSPORT);
    CHECK(result.first_value == TREV_H3_DEMUX_WEBTRANSPORT_BIDI);
    CHECK(result.session_id == 4);

    const uint8_t unknown[] = {0x80, 0x00, 0x00, 0x21};
    CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_UNIDIRECTIONAL) == 0);
    for (size_t i = 0; i < sizeof(unknown); i++) {
        trevrpc_h3_demux_status status =
            trevrpc_h3_demux_stream_feed(&stream, &pending_profile, &unknown[i], 1, &result);
        CHECK(status == (i + 1 == sizeof(unknown) ? TREV_H3_DEMUX_ACTION_READY : TREV_H3_DEMUX_NEED_MORE));
    }
    CHECK(result.action == TREV_H3_DEMUX_ACTION_UNKNOWN_UNIDIRECTIONAL);
    CHECK(result.first_value == 0x21);
    return 0;
}

static int test_protocol_errors_and_terminal(void) {
    trevrpc_h3_demux_stream stream;
    trevrpc_h3_demux_result result = sentinel_result();
    const uint8_t prohibited[] = {TREV_H3_FRAME_DATA, TREV_H3_FRAME_SETTINGS, 0x02};
    for (size_t i = 0; i < sizeof(prohibited); i++) {
        CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_BIDIRECTIONAL) == 0);
        CHECK(trevrpc_h3_demux_stream_feed(&stream, &pending_profile, &prohibited[i], 1, &result) ==
              TREV_H3_DEMUX_PROTOCOL_ERROR);
        CHECK(result.consumed == 1);
        CHECK(result.application_error == TREV_H3_DEMUX_APP_FRAME_UNEXPECTED);
    }

    const uint8_t partial[] = {0x40};
    CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_UNIDIRECTIONAL) == 0);
    CHECK(trevrpc_h3_demux_stream_feed(&stream, &pending_profile, partial, sizeof(partial), &result) ==
          TREV_H3_DEMUX_NEED_MORE);
    CHECK(trevrpc_h3_demux_stream_terminal(&stream, &pending_profile, &result) == TREV_H3_DEMUX_ACTION_READY);
    CHECK(result.action == TREV_H3_DEMUX_ACTION_RECLAIM_UNCLASSIFIED);

    CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_BIDIRECTIONAL) == 0);
    CHECK(trevrpc_h3_demux_stream_feed(&stream, &pending_profile, partial, sizeof(partial), &result) ==
          TREV_H3_DEMUX_NEED_MORE);
    CHECK(trevrpc_h3_demux_stream_terminal(&stream, &pending_profile, &result) == TREV_H3_DEMUX_PROTOCOL_ERROR);
    CHECK(result.application_error == TREV_H3_DEMUX_APP_FRAME_ERROR);

    const uint8_t wt_prefix[] = {0x40, TREV_H3_DEMUX_WEBTRANSPORT_BIDI};
    CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_BIDIRECTIONAL) == 0);
    CHECK(trevrpc_h3_demux_stream_feed(&stream, &pending_profile, wt_prefix, sizeof(wt_prefix), &result) ==
          TREV_H3_DEMUX_WAIT_PROFILE);
    CHECK(trevrpc_h3_demux_stream_terminal(&stream, &pending_profile, &result) == TREV_H3_DEMUX_WAIT_PROFILE);
    CHECK(trevrpc_h3_demux_stream_terminal(&stream, &webtransport_profile, &result) == TREV_H3_DEMUX_PROTOCOL_ERROR);
    CHECK(result.first_value == TREV_H3_DEMUX_WEBTRANSPORT_BIDI);
    CHECK(result.application_error == TREV_H3_DEMUX_APP_FRAME_ERROR);

    const uint8_t truncated_unknown_cases[][3] = {
        {0x21, 0x00, 0x00},
        {0x21, 0x00, 0x00},
        {0x21, 0x02, 0xaa},
    };
    const size_t truncated_unknown_lengths[] = {1, 2, 3};
    for (size_t i = 0; i < sizeof(truncated_unknown_lengths) / sizeof(truncated_unknown_lengths[0]); i++) {
        CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_BIDIRECTIONAL) == 0);
        CHECK(trevrpc_h3_demux_stream_feed(
                  &stream, &no_webtransport, truncated_unknown_cases[i], truncated_unknown_lengths[i], &result) ==
              TREV_H3_DEMUX_NEED_MORE);
        CHECK(trevrpc_h3_demux_stream_terminal(&stream, &no_webtransport, &result) == TREV_H3_DEMUX_PROTOCOL_ERROR);
        CHECK(result.application_error == TREV_H3_DEMUX_APP_FRAME_ERROR);
    }
    return 0;
}

static int test_critical_role_claims(void) {
    const trevrpc_h3_demux_action critical[] = {
        TREV_H3_DEMUX_ACTION_CONTROL,
        TREV_H3_DEMUX_ACTION_QPACK_ENCODER,
        TREV_H3_DEMUX_ACTION_QPACK_DECODER,
    };
    trevrpc_h3_demux_roles roles = {0};
    for (size_t i = 0; i < sizeof(critical) / sizeof(critical[0]); i++) {
        uint64_t error = UINT64_MAX;
        CHECK(trevrpc_h3_demux_roles_claim(&roles, critical[i], &error) == 0);
        CHECK(error == 0);
        CHECK(trevrpc_h3_demux_roles_claim(&roles, critical[i], &error) == -EEXIST);
        CHECK(error == TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR);
    }

    for (size_t i = 0; i < 64; i++) {
        uint64_t error = UINT64_MAX;
        CHECK(trevrpc_h3_demux_roles_claim(&roles, TREV_H3_DEMUX_ACTION_UNKNOWN_UNIDIRECTIONAL, &error) == 0);
        CHECK(error == 0);
    }
    return 0;
}

static int test_stream_direction_and_invalid_arguments(void) {
    CHECK(!trevrpc_h3_demux_stream_id_is_unidirectional(0));
    CHECK(!trevrpc_h3_demux_stream_id_is_unidirectional(1));
    CHECK(trevrpc_h3_demux_stream_id_is_unidirectional(2));
    CHECK(trevrpc_h3_demux_stream_id_is_unidirectional(3));

    trevrpc_h3_demux_stream stream;
    memset(&stream, 0xa5, sizeof(stream));
    trevrpc_h3_demux_stream original = stream;
    CHECK(trevrpc_h3_demux_stream_init(NULL, TREV_H3_DEMUX_BIDIRECTIONAL) == -EINVAL);
    CHECK(trevrpc_h3_demux_stream_init(&stream, (trevrpc_h3_demux_direction)99) == -EINVAL);
    CHECK(memcmp(&stream, &original, sizeof(stream)) == 0);

    CHECK(trevrpc_h3_demux_stream_init(&stream, TREV_H3_DEMUX_BIDIRECTIONAL) == 0);
    trevrpc_h3_demux_result result = sentinel_result();
    CHECK(trevrpc_h3_demux_stream_feed(&stream, &pending_profile, NULL, 1, &result) == TREV_H3_DEMUX_INVALID_ARGUMENT);
    CHECK(result_is_sentinel(&result));
    CHECK(trevrpc_h3_demux_stream_feed(&stream, NULL, NULL, 0, &result) == TREV_H3_DEMUX_INVALID_ARGUMENT);
    CHECK(result_is_sentinel(&result));
    const trevrpc_h3_demux_profile_capabilities invalid_profile = {
        .selected_profile = TREV_WT_PROFILE_DRAFT_15,
    };
    CHECK(trevrpc_h3_demux_stream_feed(&stream, &invalid_profile, NULL, 0, &result) == TREV_H3_DEMUX_INVALID_ARGUMENT);
    CHECK(result_is_sentinel(&result));
    CHECK(trevrpc_h3_demux_stream_terminal(NULL, &pending_profile, &result) == TREV_H3_DEMUX_INVALID_ARGUMENT);
    CHECK(result_is_sentinel(&result));
    stream.direction = (trevrpc_h3_demux_direction)99;
    CHECK(trevrpc_h3_demux_stream_terminal(&stream, &pending_profile, &result) == TREV_H3_DEMUX_INVALID_ARGUMENT);
    CHECK(result_is_sentinel(&result));

    trevrpc_h3_demux_roles roles = {0};
    uint64_t error = 0xaaaa;
    CHECK(trevrpc_h3_demux_roles_claim(NULL, TREV_H3_DEMUX_ACTION_CONTROL, &error) == -EINVAL);
    CHECK(error == 0xaaaa);
    CHECK(trevrpc_h3_demux_roles_claim(&roles, TREV_H3_DEMUX_ACTION_CONTROL, NULL) == -EINVAL);
    CHECK(trevrpc_h3_demux_roles_claim(&roles, (trevrpc_h3_demux_action)99, &error) == -EINVAL);
    CHECK(error == 0xaaaa);
    return 0;
}

int main(void) {
    int (*const tests[])(void) = {
        test_unidirectional_actions,
        test_request_action_is_profile_independent,
        test_unknown_request_frames_are_skipped,
        test_webtransport_fragmented_at_every_boundary,
        test_profile_pending_and_none_routing,
        test_profile_pending_wakes_to_webtransport,
        test_valid_and_invalid_session_ids_both_directions,
        test_all_concrete_profiles_accept_both_webtransport_prefixes,
        test_fragmented_nonminimal_varints,
        test_protocol_errors_and_terminal,
        test_critical_role_claims,
        test_stream_direction_and_invalid_arguments,
    };
    for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
        int err = tests[i]();
        if (err != 0) {
            return err;
        }
    }
    return 0;
}
