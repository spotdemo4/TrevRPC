#include "trevrpc_webtransport_profile_internal.h"

#include <errno.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

static const trevrpc_msquic_feature_snapshot full_capabilities = {
    .local_datagram_receive = true,
    .peer_datagram_receive = true,
    .datagram_send_enabled = true,
    .datagram_max_send_length = 1200,
    .configured_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
    .peer_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
    .negotiated_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT,
    .datagram_epoch = 1,
};

static const trevrpc_msquic_feature_snapshot no_reset_capabilities = {
    .local_datagram_receive = true,
    .peer_datagram_receive = true,
    .datagram_send_enabled = true,
    .datagram_max_send_length = 1200,
    .datagram_epoch = 1,
};

static const trevrpc_msquic_feature_snapshot no_datagram_capabilities = {0};

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            return 1;                                                                                                  \
        }                                                                                                              \
    } while (0)

static bool has_setting(
    const trevrpc_wt_profile_setting_pair* settings, size_t settings_len, uint64_t id, uint64_t value) {
    for (size_t i = 0; i < settings_len; i++) {
        if (settings[i].id == id && settings[i].value == value) {
            return true;
        }
    }
    return false;
}

static int test_setting_validation(void) {
    trevrpc_wt_peer_settings settings = {0};
    bool recognized = false;

    CHECK(trevrpc_wt_profile_apply_setting(&settings, TREV_WT_PROFILE_SETTINGS_DRAFT07_MAX_SESSIONS, 17, &recognized) ==
          0);
    CHECK(recognized);
    CHECK(settings.seen_enable_webtransport_draft07);
    CHECK(settings.enable_webtransport_draft07);
    CHECK(settings.draft07_max_sessions == 17);
    CHECK(trevrpc_wt_profile_apply_setting(&settings, TREV_WT_PROFILE_SETTINGS_DRAFT07_MAX_SESSIONS, 1, &recognized) ==
          -EPROTO);
    CHECK(trevrpc_wt_profile_apply_setting(&settings, TREV_WT_PROFILE_SETTINGS_H3_DATAGRAM, 2, &recognized) == -EPROTO);
    trevrpc_wt_peer_settings draft15_settings = {0};
    CHECK(trevrpc_wt_profile_apply_setting(
              &draft15_settings, TREV_WT_PROFILE_SETTINGS_DRAFT15_ENABLED, 2, &recognized) == 0);
    CHECK(draft15_settings.enable_webtransport_draft15);
    CHECK(trevrpc_wt_profile_apply_setting(&settings, 0xface, 99, &recognized) == 0);
    CHECK(!recognized);
    return 0;
}

static int test_negotiation(void) {
    trevrpc_wt_peer_settings settings = {0};
    trevrpc_wt_profile_negotiation negotiation = {0};

    CHECK(trevrpc_wt_profile_negotiate(TREV_WT_ROLE_SERVER, &settings, &full_capabilities, false, &negotiation) == 0);
    CHECK(negotiation.profile == TREV_WT_PROFILE_NONE);
    CHECK(trevrpc_wt_profile_negotiate(TREV_WT_ROLE_SERVER, &settings, &full_capabilities, true, &negotiation) ==
          -EPROTO);

    settings.h3_datagram_rfc = true;
    CHECK(trevrpc_wt_profile_negotiate(TREV_WT_ROLE_SERVER, &settings, &full_capabilities, true, &negotiation) ==
          -EPROTO);

    settings.enable_webtransport_draft15 = true;
    CHECK(trevrpc_wt_profile_negotiate(TREV_WT_ROLE_CLIENT, &settings, &full_capabilities, true, &negotiation) ==
          -EPROTO);
    settings.enable_connect_protocol = true;
    CHECK(trevrpc_wt_profile_negotiate(TREV_WT_ROLE_CLIENT, &settings, &full_capabilities, true, &negotiation) == 0);
    CHECK(negotiation.profile == TREV_WT_PROFILE_DRAFT_15);

    settings.enable_webtransport_draft14 = true;
    settings.enable_webtransport_draft07 = true;
    settings.enable_webtransport_draft02 = true;
    CHECK(trevrpc_wt_profile_negotiate(TREV_WT_ROLE_SERVER, &settings, &full_capabilities, true, &negotiation) == 0);
    CHECK(negotiation.profile == TREV_WT_PROFILE_DRAFT_15);
    negotiation = (trevrpc_wt_profile_negotiation){
        .profile = TREV_WT_PROFILE_DRAFT_15,
        .compatibility_flags = TREV_WT_PROFILE_COMPAT_INITIAL_CAPSULE_FLOW_CONTROL,
        .peer_modern_flow_control_intent = true,
    };
    CHECK(trevrpc_wt_profile_negotiate((trevrpc_wt_role)42, &settings, &full_capabilities, true, &negotiation) ==
          -EINVAL);
    CHECK(negotiation.profile == TREV_WT_PROFILE_NONE);
    CHECK(negotiation.compatibility_flags == 0);
    CHECK(!negotiation.peer_modern_flow_control_intent);
    return 0;
}

static trevrpc_wt_peer_settings network_framework_settings(void) {
    return (trevrpc_wt_peer_settings){
        .h3_datagram_rfc = true,
        .enable_webtransport_draft07 = true,
        .enable_webtransport_draft14 = true,
        .seen_h3_datagram_rfc = true,
        .seen_enable_webtransport_draft07 = true,
        .seen_enable_webtransport_draft14 = true,
        .seen_initial_max_data = true,
        .seen_initial_max_streams_uni = true,
        .seen_initial_max_streams_bidi = true,
        .draft07_max_sessions = 1,
        .draft14_max_sessions = 1,
        .wt_initial_max_data = 8ull * 1024 * 1024,
        .wt_initial_max_streams_uni = 100,
        .wt_initial_max_streams_bidi = 100,
    };
}

static int test_network_framework_compatibility(void) {
    trevrpc_wt_peer_settings settings = network_framework_settings();
    trevrpc_wt_profile_negotiation negotiation = {0};

    CHECK(trevrpc_wt_profile_negotiate(TREV_WT_ROLE_SERVER, &settings, &full_capabilities, true, &negotiation) == 0);
    CHECK(negotiation.profile == TREV_WT_PROFILE_DRAFT_07);
    CHECK((negotiation.compatibility_flags & TREV_WT_PROFILE_COMPAT_INITIAL_CAPSULE_FLOW_CONTROL) != 0);

    bool recognized = true;
    CHECK(trevrpc_wt_profile_apply_setting(&settings, UINT64_C(0xface), 99, &recognized) == 0);
    CHECK(!recognized);
    CHECK(trevrpc_wt_profile_negotiate(TREV_WT_ROLE_SERVER, &settings, &full_capabilities, true, &negotiation) == 0);
    CHECK(negotiation.profile == TREV_WT_PROFILE_DRAFT_07);
    CHECK((negotiation.compatibility_flags & TREV_WT_PROFILE_COMPAT_INITIAL_CAPSULE_FLOW_CONTROL) != 0);

    settings.wt_initial_max_streams_bidi = 99;
    CHECK(trevrpc_wt_profile_negotiate(TREV_WT_ROLE_SERVER, &settings, &full_capabilities, true, &negotiation) == 0);
    CHECK(negotiation.profile == TREV_WT_PROFILE_DRAFT_14);
    CHECK((negotiation.compatibility_flags & TREV_WT_PROFILE_COMPAT_INITIAL_CAPSULE_FLOW_CONTROL) == 0);

    settings = network_framework_settings();
    settings.draft07_max_sessions = 2;
    CHECK(trevrpc_wt_profile_negotiate(TREV_WT_ROLE_SERVER, &settings, &full_capabilities, true, &negotiation) == 0);
    CHECK(negotiation.profile == TREV_WT_PROFILE_DRAFT_14);
    CHECK((negotiation.compatibility_flags & TREV_WT_PROFILE_COMPAT_INITIAL_CAPSULE_FLOW_CONTROL) == 0);

    const struct {
        size_t seen_offset;
        size_t value_offset;
        bool value;
    } extra_recognized[] = {
        {offsetof(trevrpc_wt_peer_settings, seen_enable_webtransport_draft02),
            offsetof(trevrpc_wt_peer_settings, enable_webtransport_draft02),
            false},
        {offsetof(trevrpc_wt_peer_settings, seen_enable_webtransport_draft02),
            offsetof(trevrpc_wt_peer_settings, enable_webtransport_draft02),
            true},
        {offsetof(trevrpc_wt_peer_settings, seen_enable_webtransport_draft15),
            offsetof(trevrpc_wt_peer_settings, enable_webtransport_draft15),
            false},
        {offsetof(trevrpc_wt_peer_settings, seen_h3_datagram_draft04),
            offsetof(trevrpc_wt_peer_settings, h3_datagram_draft04),
            false},
        {offsetof(trevrpc_wt_peer_settings, seen_enable_connect_protocol),
            offsetof(trevrpc_wt_peer_settings, enable_connect_protocol),
            false},
    };
    for (size_t i = 0; i < sizeof(extra_recognized) / sizeof(extra_recognized[0]); i++) {
        settings = network_framework_settings();
        *(bool*)((uint8_t*)&settings + extra_recognized[i].seen_offset) = true;
        *(bool*)((uint8_t*)&settings + extra_recognized[i].value_offset) = extra_recognized[i].value;
        CHECK(
            trevrpc_wt_profile_negotiate(TREV_WT_ROLE_SERVER, &settings, &full_capabilities, true, &negotiation) == 0);
        CHECK(negotiation.profile == TREV_WT_PROFILE_DRAFT_14);
        CHECK((negotiation.compatibility_flags & TREV_WT_PROFILE_COMPAT_INITIAL_CAPSULE_FLOW_CONTROL) == 0);
    }
    return 0;
}

static int test_flow_control_intent(void) {
    bool recognized = false;
    trevrpc_wt_peer_settings settings = {.h3_datagram_rfc = true};
    trevrpc_wt_profile_negotiation negotiation = {0};

    CHECK(trevrpc_wt_profile_apply_setting(&settings, TREV_WT_PROFILE_SETTINGS_DRAFT14_MAX_SESSIONS, 1, &recognized) ==
          0);
    CHECK(recognized);
    CHECK(trevrpc_wt_profile_negotiate(TREV_WT_ROLE_SERVER, &settings, &full_capabilities, true, &negotiation) == 0);
    CHECK(negotiation.profile == TREV_WT_PROFILE_DRAFT_14);
    CHECK(!negotiation.peer_modern_flow_control_intent);
    CHECK(trevrpc_wt_profile_effective_session_limit(1) == 1);

    settings = (trevrpc_wt_peer_settings){.h3_datagram_rfc = true};
    CHECK(trevrpc_wt_profile_apply_setting(&settings, TREV_WT_PROFILE_SETTINGS_DRAFT14_MAX_SESSIONS, 2, &recognized) ==
          0);
    CHECK(recognized);
    CHECK(trevrpc_wt_profile_negotiate(TREV_WT_ROLE_SERVER, &settings, &full_capabilities, true, &negotiation) == 0);
    CHECK(negotiation.profile == TREV_WT_PROFILE_DRAFT_14);
    CHECK(negotiation.peer_modern_flow_control_intent);
    CHECK(trevrpc_wt_profile_effective_session_limit(2) == 1);

    settings.enable_webtransport_draft15 = true;
    CHECK(trevrpc_wt_profile_negotiate(TREV_WT_ROLE_SERVER, &settings, &full_capabilities, true, &negotiation) == 0);
    CHECK(negotiation.profile == TREV_WT_PROFILE_DRAFT_15);
    CHECK(!negotiation.peer_modern_flow_control_intent);
    return 0;
}

static int test_capability_filtering(void) {
    trevrpc_msquic_feature_snapshot draft10_capabilities = full_capabilities;
    draft10_capabilities.configured_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT;
    draft10_capabilities.peer_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT;
    draft10_capabilities.negotiated_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT;
    trevrpc_msquic_feature_snapshot zero_max_capabilities = no_reset_capabilities;
    zero_max_capabilities.datagram_max_send_length = 0;

    CHECK(trevrpc_wt_profile_is_usable(TREV_WT_PROFILE_DRAFT_02, &no_reset_capabilities));
    CHECK(trevrpc_wt_profile_is_usable(TREV_WT_PROFILE_DRAFT_07, &no_reset_capabilities));
    CHECK(!trevrpc_wt_profile_is_usable(TREV_WT_PROFILE_DRAFT_14, &no_reset_capabilities));
    CHECK(!trevrpc_wt_profile_is_usable(TREV_WT_PROFILE_DRAFT_15, &no_reset_capabilities));
    CHECK(!trevrpc_wt_profile_is_usable(TREV_WT_PROFILE_DRAFT_14, &draft10_capabilities));
    CHECK(!trevrpc_wt_profile_is_usable(TREV_WT_PROFILE_DRAFT_07, &no_datagram_capabilities));
    CHECK(!trevrpc_wt_profile_is_usable(TREV_WT_PROFILE_DRAFT_07, &zero_max_capabilities));
    CHECK(trevrpc_wt_profile_any_usable(&full_capabilities));
    CHECK(trevrpc_wt_profile_any_usable(&no_reset_capabilities));
    CHECK(!trevrpc_wt_profile_any_usable(&no_datagram_capabilities));

    trevrpc_wt_peer_settings settings = {
        .enable_connect_protocol = true,
        .enable_webtransport_draft02 = true,
        .enable_webtransport_draft07 = true,
        .enable_webtransport_draft14 = true,
        .enable_webtransport_draft15 = true,
        .h3_datagram_rfc = true,
        .h3_datagram_draft04 = true,
    };
    trevrpc_wt_profile_negotiation negotiation = {0};
    CHECK(
        trevrpc_wt_profile_negotiate(TREV_WT_ROLE_SERVER, &settings, &no_reset_capabilities, true, &negotiation) == 0);
    CHECK(negotiation.profile == TREV_WT_PROFILE_DRAFT_07);
    CHECK(trevrpc_wt_profile_negotiate(TREV_WT_ROLE_SERVER, &settings, &draft10_capabilities, true, &negotiation) == 0);
    CHECK(negotiation.profile == TREV_WT_PROFILE_DRAFT_07);
    CHECK(trevrpc_wt_profile_negotiate(
              TREV_WT_ROLE_SERVER, &settings, &no_datagram_capabilities, false, &negotiation) == 0);
    CHECK(negotiation.profile == TREV_WT_PROFILE_NONE);
    CHECK(trevrpc_wt_profile_negotiate(TREV_WT_ROLE_SERVER, &settings, &no_datagram_capabilities, true, &negotiation) ==
          -EPROTO);
    return 0;
}

static int test_profile_metadata(void) {
    CHECK(trevrpc_wt_profile_is_supported(TREV_WT_PROFILE_DRAFT_02));
    CHECK(trevrpc_wt_profile_is_supported(TREV_WT_PROFILE_DRAFT_15));
    CHECK(!trevrpc_wt_profile_is_supported(TREV_WT_PROFILE_NONE));
    CHECK(!trevrpc_wt_profile_is_supported(TREV_WT_PROFILE_DRAFT_16_RESERVED));
    CHECK(trevrpc_wt_profile_connect_protocol(TREV_WT_PROFILE_NONE) == NULL);
    CHECK(trevrpc_wt_profile_connect_protocol(TREV_WT_PROFILE_DRAFT_16_RESERVED) == NULL);
    CHECK(strcmp(trevrpc_wt_profile_connect_protocol(TREV_WT_PROFILE_DRAFT_15), "webtransport-h3") == 0);
    CHECK(trevrpc_wt_profile_accepts_connect_protocol(TREV_WT_PROFILE_DRAFT_15, "webtransport-h3"));
    CHECK(!trevrpc_wt_profile_accepts_connect_protocol(TREV_WT_PROFILE_DRAFT_15, "webtransport"));
    CHECK(!trevrpc_wt_profile_accepts_connect_protocol(TREV_WT_PROFILE_DRAFT_15, "unrelated"));
    CHECK(!trevrpc_wt_profile_accepts_connect_protocol(TREV_WT_PROFILE_DRAFT_15, NULL));
    CHECK(trevrpc_wt_profile_accepts_connect_protocol(TREV_WT_PROFILE_DRAFT_14, "webtransport"));
    CHECK(!trevrpc_wt_profile_accepts_connect_protocol(TREV_WT_PROFILE_DRAFT_14, "webtransport-h3"));
    CHECK(!trevrpc_wt_profile_accepts_connect_protocol(TREV_WT_PROFILE_NONE, "webtransport"));
    CHECK(trevrpc_wt_profile_requires_draft02_request_marker(TREV_WT_PROFILE_DRAFT_02));
    CHECK(trevrpc_wt_profile_requires_draft02_response_marker(TREV_WT_PROFILE_DRAFT_02));
    CHECK(!trevrpc_wt_profile_requires_draft02_request_marker(TREV_WT_PROFILE_DRAFT_15));
    CHECK(trevrpc_wt_profile_valid_session_id(0));
    CHECK(trevrpc_wt_profile_valid_session_id((UINT64_C(1) << 62) - 4));
    CHECK(!trevrpc_wt_profile_valid_session_id(2));
    CHECK(!trevrpc_wt_profile_valid_session_id(UINT64_C(1) << 62));
    CHECK(!trevrpc_wt_profile_valid_session_id(UINT64_MAX - 3));
    CHECK(trevrpc_wt_profile_peer_stream_is_unidirectional(2));
    CHECK(trevrpc_wt_profile_peer_stream_is_unidirectional(3));
    CHECK(!trevrpc_wt_profile_peer_stream_is_unidirectional(0));
    return 0;
}

static int test_settings_materialization(void) {
    trevrpc_wt_profile_setting_pair settings[16];
    size_t settings_len = 0;

    CHECK(trevrpc_wt_profile_effective_session_limit(0) == 1);
    CHECK(trevrpc_wt_profile_effective_session_limit(37) == 1);
    CHECK(trevrpc_wt_profile_materialize_settings(TREV_WT_PROFILE_ADVERTISEMENT_SELECTED,
              TREV_WT_PROFILE_DRAFT_15,
              &full_capabilities,
              37,
              settings,
              sizeof(settings) / sizeof(settings[0]),
              &settings_len) == 0);
    CHECK(has_setting(settings, settings_len, TREV_WT_PROFILE_SETTINGS_DRAFT15_ENABLED, 1));
    CHECK(has_setting(
        settings, settings_len, TREV_WT_PROFILE_SETTINGS_INITIAL_MAX_DATA, TREV_WT_PROFILE_MAX_FLOW_CONTROL_VALUE));
    CHECK(has_setting(settings, settings_len, TREV_WT_PROFILE_SETTINGS_INITIAL_MAX_STREAMS_UNI, 0));
    CHECK(!has_setting(settings, settings_len, TREV_WT_PROFILE_SETTINGS_DRAFT14_MAX_SESSIONS, 1));
    CHECK(!has_setting(settings, settings_len, TREV_WT_PROFILE_SETTINGS_DRAFT07_MAX_SESSIONS, 1));

    CHECK(trevrpc_wt_profile_materialize_settings(TREV_WT_PROFILE_ADVERTISEMENT_SELECTED,
              TREV_WT_PROFILE_DRAFT_16_RESERVED,
              &full_capabilities,
              1,
              settings,
              sizeof(settings) / sizeof(settings[0]),
              &settings_len) == -EINVAL);
    CHECK(trevrpc_wt_profile_materialize_settings(TREV_WT_PROFILE_ADVERTISEMENT_SELECTED,
              TREV_WT_PROFILE_NONE,
              &full_capabilities,
              1,
              settings,
              sizeof(settings) / sizeof(settings[0]),
              &settings_len) == -EINVAL);
    CHECK(trevrpc_wt_profile_materialize_settings(TREV_WT_PROFILE_ADVERTISEMENT_ALL_SUPPORTED,
              TREV_WT_PROFILE_NONE,
              &full_capabilities,
              55,
              settings,
              sizeof(settings) / sizeof(settings[0]),
              &settings_len) == 0);
    CHECK(has_setting(settings, settings_len, TREV_WT_PROFILE_SETTINGS_DRAFT07_MAX_SESSIONS, 1));
    CHECK(has_setting(settings, settings_len, TREV_WT_PROFILE_SETTINGS_DRAFT14_MAX_SESSIONS, 1));
    CHECK(has_setting(settings, settings_len, TREV_WT_PROFILE_SETTINGS_DRAFT02, 1));

    CHECK(trevrpc_wt_profile_materialize_settings(TREV_WT_PROFILE_ADVERTISEMENT_ALL_SUPPORTED,
              TREV_WT_PROFILE_NONE,
              &no_reset_capabilities,
              55,
              settings,
              sizeof(settings) / sizeof(settings[0]),
              &settings_len) == 0);
    CHECK(has_setting(settings, settings_len, TREV_WT_PROFILE_SETTINGS_DRAFT07_MAX_SESSIONS, 1));
    CHECK(has_setting(settings, settings_len, TREV_WT_PROFILE_SETTINGS_DRAFT02, 1));
    CHECK(!has_setting(settings, settings_len, TREV_WT_PROFILE_SETTINGS_DRAFT14_MAX_SESSIONS, 1));
    CHECK(!has_setting(settings, settings_len, TREV_WT_PROFILE_SETTINGS_DRAFT15_ENABLED, 1));
    CHECK(trevrpc_wt_profile_materialize_settings(TREV_WT_PROFILE_ADVERTISEMENT_SELECTED,
              TREV_WT_PROFILE_DRAFT_15,
              &no_reset_capabilities,
              1,
              settings,
              sizeof(settings) / sizeof(settings[0]),
              &settings_len) == -ENOTSUP);
    CHECK(trevrpc_wt_profile_materialize_settings(TREV_WT_PROFILE_ADVERTISEMENT_ALL_SUPPORTED,
              TREV_WT_PROFILE_NONE,
              &no_datagram_capabilities,
              1,
              settings,
              sizeof(settings) / sizeof(settings[0]),
              &settings_len) == 0);
    CHECK(settings_len == 0);

    CHECK(trevrpc_wt_profile_materialize_settings(TREV_WT_PROFILE_ADVERTISEMENT_ALL_SUPPORTED,
              TREV_WT_PROFILE_DRAFT_07,
              &full_capabilities,
              1,
              settings,
              sizeof(settings) / sizeof(settings[0]),
              &settings_len) == -EINVAL);
    return 0;
}

int main(void) {
    int (*const tests[])(void) = {
        test_setting_validation,
        test_negotiation,
        test_network_framework_compatibility,
        test_flow_control_intent,
        test_capability_filtering,
        test_profile_metadata,
        test_settings_materialization,
    };
    for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
        int err = tests[i]();
        if (err != 0) {
            return err;
        }
    }
    return 0;
}
