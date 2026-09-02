#include "trevrpc_msquic_features_internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            return 1;                                                                                                  \
        }                                                                                                              \
    } while (0)

static void reduce_connected(trevrpc_msquic_feature_state* state) {
    const trevrpc_msquic_feature_event event = {
        .kind = TREV_MSQUIC_FEATURE_EVENT_CONNECTED,
    };
    trevrpc_msquic_feature_reduce(state, &event);
}

static void reduce_datagram(trevrpc_msquic_feature_state* state, bool enabled, uint16_t maximum) {
    const trevrpc_msquic_feature_event event = {
        .kind = TREV_MSQUIC_FEATURE_EVENT_DATAGRAM_STATE_CHANGED,
        .value = enabled,
        .max_send_length = maximum,
    };
    trevrpc_msquic_feature_reduce(state, &event);
}

static void reduce_reset(trevrpc_msquic_feature_state* state, uint32_t requested, uint32_t peer, uint32_t negotiated) {
    const trevrpc_msquic_feature_event event = {
        .kind = TREV_MSQUIC_FEATURE_EVENT_RELIABLE_RESET_NEGOTIATED,
        .requested_reset_dialects = requested,
        .peer_reset_dialects = peer,
        .negotiated_reset_dialects = negotiated,
    };
    trevrpc_msquic_feature_reduce(state, &event);
}

static void reduce_terminal(trevrpc_msquic_feature_state* state, int status) {
    const trevrpc_msquic_feature_event event = {
        .kind = TREV_MSQUIC_FEATURE_EVENT_TERMINAL,
        .terminal_status = status,
    };
    trevrpc_msquic_feature_reduce(state, &event);
}

static int test_reset_identities(void) {
    CHECK(trevrpc_msquic_reset_dialect_valid(TREV_MSQUIC_RESET_NONE));
    CHECK(trevrpc_msquic_reset_dialect_valid(TREV_MSQUIC_RESET_MSQUIC_LEGACY));
    CHECK(trevrpc_msquic_reset_dialect_valid(TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07));
    CHECK(trevrpc_msquic_reset_dialect_valid(TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10));
    CHECK(!trevrpc_msquic_reset_dialect_valid((trevrpc_msquic_reset_dialect)99));

    CHECK(trevrpc_msquic_reset_dialect_bit(TREV_MSQUIC_RESET_NONE) == TREV_MSQUIC_RESET_DIALECTS_NONE);
    CHECK(trevrpc_msquic_reset_dialect_bit(TREV_MSQUIC_RESET_MSQUIC_LEGACY) ==
          TREV_MSQUIC_RESET_DIALECT_MSQUIC_LEGACY_BIT);
    CHECK(trevrpc_msquic_reset_dialect_bit(TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07) ==
          TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT);
    CHECK(trevrpc_msquic_reset_dialect_bit(TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10) ==
          TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT);
    CHECK(trevrpc_msquic_reset_dialect_bit((trevrpc_msquic_reset_dialect)99) == TREV_MSQUIC_RESET_DIALECTS_NONE);

    trevrpc_msquic_reset_identity identity = trevrpc_msquic_reset_identity_for(TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07);
    CHECK(identity.dialect == TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07);
    CHECK(identity.transport_parameter == TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07_TRANSPORT_PARAMETER);
    CHECK(identity.frame_type == TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07_FRAME);

    identity = trevrpc_msquic_reset_identity_for(TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10);
    CHECK(identity.dialect == TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10);
    CHECK(identity.transport_parameter == TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10_TRANSPORT_PARAMETER);
    CHECK(identity.frame_type == TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10_FRAME);

    identity = trevrpc_msquic_reset_identity_for(TREV_MSQUIC_RESET_MSQUIC_LEGACY);
    CHECK(identity.dialect == TREV_MSQUIC_RESET_MSQUIC_LEGACY);
    CHECK(identity.transport_parameter == 0);
    CHECK(identity.frame_type == 0);

    identity = trevrpc_msquic_reset_identity_for((trevrpc_msquic_reset_dialect)99);
    CHECK(identity.dialect == TREV_MSQUIC_RESET_NONE);
    CHECK(identity.transport_parameter == 0);
    CHECK(identity.frame_type == 0);
    return 0;
}

static int test_attestation_and_request_policy(void) {
    const trevrpc_msquic_reset_stream_at_dialects_v1 descriptor_v1 = {
        .version = TREV_MSQUIC_RESET_STREAM_AT_DIALECTS_VERSION_1,
        .supported_dialect_mask = TREV_MSQUIC_NATIVE_RESET_STREAM_AT_SUPPORTED_DIALECT_MASK,
        .draft_07_transport_parameter_id = TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07_TRANSPORT_PARAMETER,
        .draft_10_transport_parameter_id = TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10_TRANSPORT_PARAMETER,
        .frame_type = TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10_FRAME,
    };
    trevrpc_msquic_provider_features stock = trevrpc_msquic_provider_features_attested(NULL, 0);
    CHECK(stock.datagrams);
    CHECK(stock.supported_reset_dialects == TREV_MSQUIC_RESET_DIALECTS_NONE);
    CHECK(
        trevrpc_msquic_provider_features_attested(&descriptor_v1, sizeof(descriptor_v1) - 1).supported_reset_dialects ==
        TREV_MSQUIC_RESET_DIALECTS_NONE);
    CHECK(
        trevrpc_msquic_provider_features_attested(&descriptor_v1, sizeof(descriptor_v1) + 1).supported_reset_dialects ==
        TREV_MSQUIC_RESET_DIALECTS_NONE);

    trevrpc_msquic_reset_stream_at_dialects_v1 malformed = descriptor_v1;
    malformed.version++;
    CHECK(trevrpc_msquic_provider_features_attested(&malformed, sizeof(malformed)).supported_reset_dialects ==
          TREV_MSQUIC_RESET_DIALECTS_NONE);
    malformed = descriptor_v1;
    malformed.supported_dialect_mask = TREV_MSQUIC_NATIVE_RESET_STREAM_AT_DIALECT_DRAFT_07;
    CHECK(trevrpc_msquic_provider_features_attested(&malformed, sizeof(malformed)).supported_reset_dialects ==
          TREV_MSQUIC_RESET_DIALECTS_NONE);
    malformed.supported_dialect_mask = TREV_MSQUIC_NATIVE_RESET_STREAM_AT_SUPPORTED_DIALECT_MASK | (UINT32_C(1) << 31);
    CHECK(trevrpc_msquic_provider_features_attested(&malformed, sizeof(malformed)).supported_reset_dialects ==
          TREV_MSQUIC_RESET_DIALECTS_NONE);
    malformed = descriptor_v1;
    malformed.draft_07_transport_parameter_id++;
    CHECK(trevrpc_msquic_provider_features_attested(&malformed, sizeof(malformed)).supported_reset_dialects ==
          TREV_MSQUIC_RESET_DIALECTS_NONE);
    malformed = descriptor_v1;
    malformed.draft_10_transport_parameter_id++;
    CHECK(trevrpc_msquic_provider_features_attested(&malformed, sizeof(malformed)).supported_reset_dialects ==
          TREV_MSQUIC_RESET_DIALECTS_NONE);
    malformed = descriptor_v1;
    malformed.frame_type++;
    CHECK(trevrpc_msquic_provider_features_attested(&malformed, sizeof(malformed)).supported_reset_dialects ==
          TREV_MSQUIC_RESET_DIALECTS_NONE);

    trevrpc_msquic_provider_features trusted =
        trevrpc_msquic_provider_features_attested(&descriptor_v1, sizeof(descriptor_v1));
    CHECK(trusted.datagrams);
    CHECK(trusted.supported_reset_dialects ==
          (TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT | TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT));

    const trevrpc_msquic_provider_features trusted_draft07 = {
        .datagrams = true,
        .supported_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
    };
    const trevrpc_msquic_provider_features trusted_draft10 = {
        .datagrams = true,
        .supported_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT,
    };
    trevrpc_msquic_provider_features no_datagrams = stock;
    no_datagrams.datagrams = false;

    trevrpc_msquic_feature_request request = trevrpc_msquic_generic_feature_request();
    CHECK(!request.datagram_receive);
    CHECK(request.requested_reset_dialects == TREV_MSQUIC_RESET_DIALECTS_NONE);
    CHECK(trevrpc_msquic_feature_request_validate(&stock, &request) == 0);

    request = trevrpc_msquic_h3_feature_request(&stock);
    CHECK(request.datagram_receive);
    CHECK(request.requested_reset_dialects == TREV_MSQUIC_RESET_DIALECTS_NONE);
    CHECK(trevrpc_msquic_feature_request_validate(&stock, &request) == 0);

    request = trevrpc_msquic_h3_feature_request(&trusted_draft07);
    CHECK(request.datagram_receive);
    CHECK(request.requested_reset_dialects == TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT);
    CHECK(trevrpc_msquic_feature_request_validate(&trusted_draft07, &request) == 0);
    CHECK(trevrpc_msquic_feature_request_validate(&stock, &request) == -ENOTSUP);
    CHECK(trevrpc_msquic_feature_request_validate(&trusted_draft10, &request) == -ENOTSUP);

    request.requested_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT;
    CHECK(trevrpc_msquic_feature_request_validate(&trusted_draft10, &request) == 0);
    CHECK(trevrpc_msquic_feature_request_validate(&trusted_draft07, &request) == -ENOTSUP);

    request = trevrpc_msquic_h3_feature_request(&trusted_draft10);
    CHECK(request.datagram_receive);
    CHECK(request.requested_reset_dialects == TREV_MSQUIC_RESET_DIALECTS_NONE);

    request = trevrpc_msquic_h3_feature_request(NULL);
    CHECK(request.datagram_receive);
    CHECK(request.requested_reset_dialects == TREV_MSQUIC_RESET_DIALECTS_NONE);
    CHECK(trevrpc_msquic_feature_request_validate(&no_datagrams, &request) == -ENOTSUP);

    request = trevrpc_msquic_generic_feature_request();
    request.requested_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT | TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT;
    CHECK(trevrpc_msquic_feature_request_validate(&trusted, &request) == -EINVAL);
    request.requested_reset_dialects = UINT32_C(1) << 31;
    CHECK(trevrpc_msquic_feature_request_validate(&stock, &request) == -EINVAL);
    CHECK(trevrpc_msquic_feature_request_validate(NULL, &request) == -EINVAL);
    CHECK(trevrpc_msquic_feature_request_validate(&stock, NULL) == -EINVAL);
    return 0;
}

static int test_generic_ready_at_connected(void) {
    trevrpc_msquic_feature_request request = trevrpc_msquic_generic_feature_request();
    trevrpc_msquic_feature_state state;
    trevrpc_msquic_feature_snapshot snapshot = {
        .local_datagram_receive = true,
        .peer_datagram_receive = true,
        .datagram_send_enabled = true,
        .datagram_max_send_length = 65535,
        .configured_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
        .peer_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
        .negotiated_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
        .datagram_epoch = 99,
    };
    trevrpc_msquic_feature_state_init(&state, &request);
    request.datagram_receive = true;
    request.requested_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT;

    CHECK(!trevrpc_msquic_feature_ready(&state));
    CHECK(trevrpc_msquic_feature_snapshot_get(&state, &snapshot) == -EAGAIN);
    CHECK(!snapshot.local_datagram_receive);
    CHECK(snapshot.datagram_epoch == 0);

    reduce_connected(&state);
    CHECK(trevrpc_msquic_feature_ready(&state));
    CHECK(trevrpc_msquic_feature_snapshot_get(&state, &snapshot) == 0);
    CHECK(!snapshot.local_datagram_receive);
    CHECK(!snapshot.peer_datagram_receive);
    CHECK(!snapshot.datagram_send_enabled);
    CHECK(snapshot.datagram_max_send_length == 0);
    CHECK(snapshot.configured_reset_dialects == TREV_MSQUIC_RESET_DIALECTS_NONE);
    CHECK(snapshot.peer_reset_dialects == TREV_MSQUIC_RESET_DIALECTS_NONE);
    CHECK(snapshot.negotiated_reset_dialects == TREV_MSQUIC_RESET_DIALECTS_NONE);
    CHECK(snapshot.datagram_epoch == 0);
    CHECK(!trevrpc_msquic_feature_snapshot_usable_datagrams(&snapshot));
    CHECK(!trevrpc_msquic_feature_snapshot_has_reset_dialect(&snapshot, TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07));
    return 0;
}

static int test_requested_event_permutations(void) {
    static const unsigned permutations[][3] = {
        {0, 1, 2},
        {0, 2, 1},
        {1, 0, 2},
        {1, 2, 0},
        {2, 0, 1},
        {2, 1, 0},
    };
    const trevrpc_msquic_feature_request request = {
        .datagram_receive = true,
        .requested_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
    };

    for (size_t permutation = 0; permutation < sizeof(permutations) / sizeof(permutations[0]); permutation++) {
        trevrpc_msquic_feature_state state;
        trevrpc_msquic_feature_snapshot snapshot;
        trevrpc_msquic_feature_state_init(&state, &request);
        for (size_t index = 0; index < 3; index++) {
            switch (permutations[permutation][index]) {
            case 0:
                reduce_connected(&state);
                break;
            case 1:
                reduce_datagram(&state, true, 1200);
                break;
            case 2:
                reduce_reset(&state,
                    TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
                    TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
                    TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT);
                break;
            default:
                CHECK(false);
            }
            if (index < 2) {
                CHECK(!trevrpc_msquic_feature_ready(&state));
                CHECK(trevrpc_msquic_feature_snapshot_get(&state, &snapshot) == -EAGAIN);
            }
        }
        CHECK(trevrpc_msquic_feature_ready(&state));
        CHECK(trevrpc_msquic_feature_snapshot_get(&state, &snapshot) == 0);
        CHECK(snapshot.local_datagram_receive);
        CHECK(snapshot.peer_datagram_receive);
        CHECK(snapshot.datagram_send_enabled);
        CHECK(snapshot.datagram_max_send_length == 1200);
        CHECK(snapshot.configured_reset_dialects == TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT);
        CHECK(snapshot.peer_reset_dialects == TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT);
        CHECK(snapshot.negotiated_reset_dialects == TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT);
        CHECK(snapshot.datagram_epoch == 1);
        CHECK(trevrpc_msquic_feature_snapshot_usable_datagrams(&snapshot));
        CHECK(trevrpc_msquic_feature_snapshot_has_reset_dialect(&snapshot, TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07));
        CHECK(!trevrpc_msquic_feature_snapshot_has_reset_dialect(&snapshot, TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10));
    }
    return 0;
}

static int test_replacement_and_finalization(void) {
    const trevrpc_msquic_feature_request request = {
        .datagram_receive = true,
        .requested_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
    };
    trevrpc_msquic_feature_state state;
    trevrpc_msquic_feature_snapshot snapshot;
    trevrpc_msquic_feature_state_init(&state, &request);

    reduce_datagram(&state, true, 1000);
    reduce_datagram(&state, false, 0);
    reduce_datagram(&state, true, 1400);
    reduce_reset(&state, TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT, 0, 0);
    reduce_reset(&state,
        TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
        TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
        TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT);
    reduce_connected(&state);

    CHECK(trevrpc_msquic_feature_snapshot_get(&state, &snapshot) == 0);
    CHECK(snapshot.datagram_send_enabled);
    CHECK(snapshot.datagram_max_send_length == 1400);
    CHECK(snapshot.datagram_epoch == 3);
    CHECK(snapshot.negotiated_reset_dialects == TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT);

    reduce_reset(&state, TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT, 0, 0);
    CHECK(trevrpc_msquic_feature_snapshot_get(&state, &snapshot) == 0);
    CHECK(snapshot.negotiated_reset_dialects == TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT);

    reduce_datagram(&state, false, 0);
    CHECK(trevrpc_msquic_feature_snapshot_get(&state, &snapshot) == 0);
    CHECK(!snapshot.peer_datagram_receive);
    CHECK(!snapshot.datagram_send_enabled);
    CHECK(snapshot.datagram_max_send_length == 0);
    CHECK(snapshot.datagram_epoch == 4);
    CHECK(!trevrpc_msquic_feature_snapshot_usable_datagrams(&snapshot));

    reduce_datagram(&state, true, 1232);
    CHECK(trevrpc_msquic_feature_snapshot_get(&state, &snapshot) == 0);
    CHECK(snapshot.peer_datagram_receive);
    CHECK(snapshot.datagram_send_enabled);
    CHECK(snapshot.datagram_max_send_length == 1232);
    CHECK(snapshot.datagram_epoch == 5);
    CHECK(trevrpc_msquic_feature_snapshot_usable_datagrams(&snapshot));

    state.datagram_epoch = UINT64_MAX;
    reduce_datagram(&state, true, 1350);
    CHECK(trevrpc_msquic_feature_snapshot_get(&state, &snapshot) == 0);
    CHECK(snapshot.datagram_epoch == 1);
    CHECK(snapshot.datagram_max_send_length == 1350);
    return 0;
}

static int test_exact_mismatch_and_first_result_finalization(void) {
    const trevrpc_msquic_feature_request request = {
        .datagram_receive = false,
        .requested_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
    };
    trevrpc_msquic_feature_state state;
    trevrpc_msquic_feature_snapshot snapshot;
    trevrpc_msquic_feature_state_init(&state, &request);

    reduce_connected(&state);
    CHECK(!trevrpc_msquic_feature_ready(&state));
    reduce_reset(&state,
        TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
        TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT,
        TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT);
    CHECK(trevrpc_msquic_feature_ready(&state));
    CHECK(trevrpc_msquic_feature_snapshot_get(&state, &snapshot) == 0);
    CHECK(snapshot.configured_reset_dialects == TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT);
    CHECK(snapshot.peer_reset_dialects == TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT);
    CHECK(snapshot.negotiated_reset_dialects == TREV_MSQUIC_RESET_DIALECTS_NONE);
    CHECK(!trevrpc_msquic_feature_snapshot_has_reset_dialect(&snapshot, TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07));
    CHECK(!trevrpc_msquic_feature_snapshot_has_reset_dialect(&snapshot, TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10));

    reduce_reset(&state,
        TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
        TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
        TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT);
    CHECK(trevrpc_msquic_feature_snapshot_get(&state, &snapshot) == 0);
    CHECK(snapshot.peer_reset_dialects == TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT);
    CHECK(snapshot.negotiated_reset_dialects == TREV_MSQUIC_RESET_DIALECTS_NONE);
    return 0;
}

static int test_terminal_status_and_event_suppression(void) {
    const trevrpc_msquic_feature_request request = {
        .datagram_receive = true,
        .requested_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
    };
    trevrpc_msquic_feature_state state;
    trevrpc_msquic_feature_snapshot snapshot;
    trevrpc_msquic_feature_state_init(&state, &request);

    reduce_datagram(&state, true, 1200);
    reduce_terminal(&state, -ECONNRESET);
    CHECK(!trevrpc_msquic_feature_ready(&state));
    CHECK(trevrpc_msquic_feature_snapshot_get(&state, &snapshot) == -ECONNRESET);

    reduce_connected(&state);
    reduce_datagram(&state, false, 0);
    reduce_reset(&state,
        TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
        TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
        TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT);
    reduce_terminal(&state, -ETIMEDOUT);
    CHECK(!state.connected);
    CHECK(state.datagram_send_enabled);
    CHECK(state.datagram_max_send_length == 1200);
    CHECK(state.datagram_epoch == 1);
    CHECK(!state.reset_event_seen);
    CHECK(state.peer_reset_dialects == TREV_MSQUIC_RESET_DIALECTS_NONE);
    CHECK(state.negotiated_reset_dialects == TREV_MSQUIC_RESET_DIALECTS_NONE);
    CHECK(state.terminal_status == -ECONNRESET);
    CHECK(trevrpc_msquic_feature_snapshot_get(&state, &snapshot) == -ECONNRESET);

    trevrpc_msquic_feature_state_init(&state, NULL);
    reduce_terminal(&state, 0);
    CHECK(trevrpc_msquic_feature_snapshot_get(&state, &snapshot) == -ECANCELED);
    CHECK(trevrpc_msquic_feature_snapshot_get(NULL, &snapshot) == -EINVAL);
    CHECK(trevrpc_msquic_feature_snapshot_get(&state, NULL) == -EINVAL);
    CHECK(!trevrpc_msquic_feature_snapshot_usable_datagrams(NULL));
    CHECK(!trevrpc_msquic_feature_snapshot_has_reset_dialect(NULL, TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07));
    CHECK(!trevrpc_msquic_feature_snapshot_has_reset_dialect(&snapshot, TREV_MSQUIC_RESET_NONE));
    return 0;
}

int main(void) {
    int (*const tests[])(void) = {
        test_reset_identities,
        test_attestation_and_request_policy,
        test_generic_ready_at_connected,
        test_requested_event_permutations,
        test_replacement_and_finalization,
        test_exact_mismatch_and_first_result_finalization,
        test_terminal_status_and_event_suppression,
    };
    for (size_t index = 0; index < sizeof(tests) / sizeof(tests[0]); index++) {
        int err = tests[index]();
        if (err != 0) {
            return err;
        }
    }
    return 0;
}
