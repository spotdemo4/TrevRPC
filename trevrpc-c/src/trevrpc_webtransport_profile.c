#include "trevrpc_webtransport_profile_internal.h"

#include <errno.h>
#include <string.h>

#define TREV_WT_NETWORK_FRAMEWORK_INITIAL_MAX_DATA (8ull * 1024 * 1024)
#define TREV_WT_NETWORK_FRAMEWORK_INITIAL_MAX_STREAMS 100

typedef struct trevrpc_wt_profile_emitted_setting {
    uint64_t id;
    uint64_t value;
    bool uses_session_limit;
} trevrpc_wt_profile_emitted_setting;

typedef struct trevrpc_wt_profile_descriptor {
    trevrpc_wt_profile_id id;
    const char* emitted_connect_protocol;
    const trevrpc_wt_profile_emitted_setting* emitted_settings;
    size_t emitted_settings_len;
    bool draft14_max_sessions_flow_control_intent;
    bool requires_rfc_h3_datagram;
    bool accepts_draft04_h3_datagram;
    bool requires_reset_stream_at_draft07;
    bool requires_connect_protocol_for_client;
    bool requires_draft02_request_marker;
    bool requires_draft02_response_marker;
} trevrpc_wt_profile_descriptor;

static const trevrpc_wt_profile_emitted_setting trevrpc_wt_profile_draft02_settings[] = {
    {TREV_WT_PROFILE_SETTINGS_DRAFT02, 1, false},
    {TREV_WT_PROFILE_SETTINGS_H3_DRAFT04_DATAGRAM, 1, false},
};

static const trevrpc_wt_profile_emitted_setting trevrpc_wt_profile_draft07_settings[] = {
    {TREV_WT_PROFILE_SETTINGS_DRAFT07_MAX_SESSIONS, 0, true},
};

static const trevrpc_wt_profile_emitted_setting trevrpc_wt_profile_draft14_settings[] = {
    {TREV_WT_PROFILE_SETTINGS_DRAFT14_MAX_SESSIONS, 0, true},
};

static const trevrpc_wt_profile_emitted_setting trevrpc_wt_profile_draft15_settings[] = {
    {TREV_WT_PROFILE_SETTINGS_DRAFT15_ENABLED, 1, false},
    {TREV_WT_PROFILE_SETTINGS_INITIAL_MAX_STREAMS_UNI, 0, false},
    {TREV_WT_PROFILE_SETTINGS_INITIAL_MAX_STREAMS_BIDI, TREV_WT_PROFILE_MAX_FLOW_CONTROL_VALUE, false},
    {TREV_WT_PROFILE_SETTINGS_INITIAL_MAX_DATA, TREV_WT_PROFILE_MAX_FLOW_CONTROL_VALUE, false},
};

static const trevrpc_wt_profile_descriptor trevrpc_wt_profile_descriptors[] = {
    {
        .id = TREV_WT_PROFILE_DRAFT_02,
        .emitted_connect_protocol = "webtransport",
        .emitted_settings = trevrpc_wt_profile_draft02_settings,
        .emitted_settings_len =
            sizeof(trevrpc_wt_profile_draft02_settings) / sizeof(trevrpc_wt_profile_draft02_settings[0]),
        .accepts_draft04_h3_datagram = true,
        .requires_draft02_request_marker = true,
        .requires_draft02_response_marker = true,
    },
    {
        .id = TREV_WT_PROFILE_DRAFT_07,
        .emitted_connect_protocol = "webtransport",
        .emitted_settings = trevrpc_wt_profile_draft07_settings,
        .emitted_settings_len =
            sizeof(trevrpc_wt_profile_draft07_settings) / sizeof(trevrpc_wt_profile_draft07_settings[0]),
        .requires_rfc_h3_datagram = true,
        .requires_connect_protocol_for_client = true,
    },
    {
        .id = TREV_WT_PROFILE_DRAFT_14,
        .emitted_connect_protocol = "webtransport",
        .emitted_settings = trevrpc_wt_profile_draft14_settings,
        .emitted_settings_len =
            sizeof(trevrpc_wt_profile_draft14_settings) / sizeof(trevrpc_wt_profile_draft14_settings[0]),
        .draft14_max_sessions_flow_control_intent = true,
        .requires_rfc_h3_datagram = true,
        .requires_reset_stream_at_draft07 = true,
        .requires_connect_protocol_for_client = true,
    },
    {
        .id = TREV_WT_PROFILE_DRAFT_15,
        .emitted_connect_protocol = "webtransport-h3",
        .emitted_settings = trevrpc_wt_profile_draft15_settings,
        .emitted_settings_len =
            sizeof(trevrpc_wt_profile_draft15_settings) / sizeof(trevrpc_wt_profile_draft15_settings[0]),
        .requires_rfc_h3_datagram = true,
        .requires_reset_stream_at_draft07 = true,
        .requires_connect_protocol_for_client = true,
    },
};

static const trevrpc_wt_profile_descriptor* trevrpc_wt_profile_descriptor_find(trevrpc_wt_profile_id profile) {
    for (size_t i = 0; i < sizeof(trevrpc_wt_profile_descriptors) / sizeof(trevrpc_wt_profile_descriptors[0]); i++) {
        if (trevrpc_wt_profile_descriptors[i].id == profile) {
            return &trevrpc_wt_profile_descriptors[i];
        }
    }
    return NULL;
}

static bool trevrpc_wt_profile_bool_setting_valid(uint64_t value) {
    return value <= 1;
}

static bool trevrpc_wt_profile_network_framework_legacy_peer(const trevrpc_wt_peer_settings* settings) {
    return settings->seen_h3_datagram_rfc && settings->seen_enable_webtransport_draft07 &&
           settings->seen_enable_webtransport_draft14 && settings->seen_initial_max_data &&
           settings->seen_initial_max_streams_uni && settings->seen_initial_max_streams_bidi &&
           !settings->seen_enable_connect_protocol && !settings->seen_enable_webtransport_draft02 &&
           !settings->seen_enable_webtransport_draft15 && !settings->seen_h3_datagram_draft04 &&
           settings->h3_datagram_rfc && settings->enable_webtransport_draft07 &&
           settings->enable_webtransport_draft14 && settings->draft07_max_sessions == 1 &&
           settings->draft14_max_sessions == 1 &&
           settings->wt_initial_max_data == TREV_WT_NETWORK_FRAMEWORK_INITIAL_MAX_DATA &&
           settings->wt_initial_max_streams_uni == TREV_WT_NETWORK_FRAMEWORK_INITIAL_MAX_STREAMS &&
           settings->wt_initial_max_streams_bidi == TREV_WT_NETWORK_FRAMEWORK_INITIAL_MAX_STREAMS;
}

static int trevrpc_wt_profile_append(trevrpc_wt_profile_setting_pair* out_settings,
    size_t out_capacity,
    size_t* out_count,
    uint64_t id,
    uint64_t value) {
    for (size_t i = 0; i < *out_count; i++) {
        if (out_settings[i].id == id) {
            return out_settings[i].value == value ? 0 : -EPROTO;
        }
    }
    if (*out_count == out_capacity) {
        return -ENOBUFS;
    }
    out_settings[(*out_count)++] = (trevrpc_wt_profile_setting_pair){.id = id, .value = value};
    return 0;
}

int trevrpc_wt_profile_apply_setting(
    trevrpc_wt_peer_settings* settings, uint64_t id, uint64_t value, bool* recognized) {
    if (settings == NULL || recognized == NULL) {
        return -EINVAL;
    }
    *recognized = true;

    switch (id) {
    case TREV_WT_PROFILE_SETTINGS_ENABLE_CONNECT_PROTOCOL:
        if (settings->seen_enable_connect_protocol || !trevrpc_wt_profile_bool_setting_valid(value)) {
            return -EPROTO;
        }
        settings->seen_enable_connect_protocol = true;
        settings->enable_connect_protocol = value == 1;
        return 0;
    case TREV_WT_PROFILE_SETTINGS_DRAFT02:
        if (settings->seen_enable_webtransport_draft02 || !trevrpc_wt_profile_bool_setting_valid(value)) {
            return -EPROTO;
        }
        settings->seen_enable_webtransport_draft02 = true;
        settings->enable_webtransport_draft02 = value == 1;
        return 0;
    case TREV_WT_PROFILE_SETTINGS_DRAFT07_MAX_SESSIONS:
        if (settings->seen_enable_webtransport_draft07) {
            return -EPROTO;
        }
        settings->seen_enable_webtransport_draft07 = true;
        settings->draft07_max_sessions = value;
        settings->enable_webtransport_draft07 = value != 0;
        return 0;
    case TREV_WT_PROFILE_SETTINGS_DRAFT14_MAX_SESSIONS:
        if (settings->seen_enable_webtransport_draft14) {
            return -EPROTO;
        }
        settings->seen_enable_webtransport_draft14 = true;
        settings->draft14_max_sessions = value;
        settings->enable_webtransport_draft14 = value != 0;
        return 0;
    case TREV_WT_PROFILE_SETTINGS_DRAFT15_ENABLED:
        if (settings->seen_enable_webtransport_draft15) {
            return -EPROTO;
        }
        settings->seen_enable_webtransport_draft15 = true;
        settings->enable_webtransport_draft15 = value != 0;
        return 0;
    case TREV_WT_PROFILE_SETTINGS_INITIAL_MAX_DATA:
        if (settings->seen_initial_max_data) {
            return -EPROTO;
        }
        settings->seen_initial_max_data = true;
        settings->wt_initial_max_data = value;
        return 0;
    case TREV_WT_PROFILE_SETTINGS_INITIAL_MAX_STREAMS_UNI:
        if (settings->seen_initial_max_streams_uni) {
            return -EPROTO;
        }
        settings->seen_initial_max_streams_uni = true;
        settings->wt_initial_max_streams_uni = value;
        return 0;
    case TREV_WT_PROFILE_SETTINGS_INITIAL_MAX_STREAMS_BIDI:
        if (settings->seen_initial_max_streams_bidi) {
            return -EPROTO;
        }
        settings->seen_initial_max_streams_bidi = true;
        settings->wt_initial_max_streams_bidi = value;
        return 0;
    case TREV_WT_PROFILE_SETTINGS_H3_DATAGRAM:
        if (settings->seen_h3_datagram_rfc || !trevrpc_wt_profile_bool_setting_valid(value)) {
            return -EPROTO;
        }
        settings->seen_h3_datagram_rfc = true;
        settings->h3_datagram_rfc = value == 1;
        return 0;
    case TREV_WT_PROFILE_SETTINGS_H3_DRAFT04_DATAGRAM:
        if (settings->seen_h3_datagram_draft04 || !trevrpc_wt_profile_bool_setting_valid(value)) {
            return -EPROTO;
        }
        settings->seen_h3_datagram_draft04 = true;
        settings->h3_datagram_draft04 = value == 1;
        return 0;
    default:
        *recognized = false;
        return 0;
    }
}

int trevrpc_wt_profile_negotiate(trevrpc_wt_role role,
    const trevrpc_wt_peer_settings* settings,
    const trevrpc_msquic_feature_snapshot* capabilities,
    bool require_webtransport,
    trevrpc_wt_profile_negotiation* out_negotiation) {
    if (out_negotiation != NULL) {
        *out_negotiation = (trevrpc_wt_profile_negotiation){0};
    }
    if (settings == NULL || capabilities == NULL || out_negotiation == NULL ||
        (role != TREV_WT_ROLE_SERVER && role != TREV_WT_ROLE_CLIENT)) {
        return -EINVAL;
    }

    bool network_framework_peer =
        role == TREV_WT_ROLE_SERVER && trevrpc_wt_profile_network_framework_legacy_peer(settings);
    const trevrpc_wt_profile_id candidates[] = {
        settings->enable_webtransport_draft15 ? TREV_WT_PROFILE_DRAFT_15 : TREV_WT_PROFILE_NONE,
        network_framework_peer ? TREV_WT_PROFILE_DRAFT_07 : TREV_WT_PROFILE_NONE,
        settings->enable_webtransport_draft14 ? TREV_WT_PROFILE_DRAFT_14 : TREV_WT_PROFILE_NONE,
        settings->enable_webtransport_draft07 ? TREV_WT_PROFILE_DRAFT_07 : TREV_WT_PROFILE_NONE,
        settings->enable_webtransport_draft02 ? TREV_WT_PROFILE_DRAFT_02 : TREV_WT_PROFILE_NONE,
    };

    const trevrpc_wt_profile_descriptor* descriptor = NULL;
    for (size_t i = 0; i < sizeof(candidates) / sizeof(candidates[0]); i++) {
        if (candidates[i] == TREV_WT_PROFILE_NONE || !trevrpc_wt_profile_is_usable(candidates[i], capabilities)) {
            continue;
        }
        const trevrpc_wt_profile_descriptor* candidate = trevrpc_wt_profile_descriptor_find(candidates[i]);
        if (candidate == NULL || (candidate->requires_rfc_h3_datagram && !settings->h3_datagram_rfc) ||
            (candidate->accepts_draft04_h3_datagram && !settings->h3_datagram_rfc && !settings->h3_datagram_draft04) ||
            (role == TREV_WT_ROLE_CLIENT && candidate->requires_connect_protocol_for_client &&
                !settings->enable_connect_protocol)) {
            continue;
        }
        descriptor = candidate;
        break;
    }

    if (descriptor == NULL) {
        return require_webtransport ? -EPROTO : 0;
    }

    out_negotiation->profile = descriptor->id;
    out_negotiation->peer_modern_flow_control_intent =
        descriptor->draft14_max_sessions_flow_control_intent && settings->draft14_max_sessions > 1;
    if (network_framework_peer && descriptor->id == TREV_WT_PROFILE_DRAFT_07) {
        out_negotiation->compatibility_flags = TREV_WT_PROFILE_COMPAT_INITIAL_CAPSULE_FLOW_CONTROL;
    }
    return 0;
}

bool trevrpc_wt_profile_is_supported(trevrpc_wt_profile_id profile) {
    return trevrpc_wt_profile_descriptor_find(profile) != NULL;
}

bool trevrpc_wt_profile_is_usable(trevrpc_wt_profile_id profile, const trevrpc_msquic_feature_snapshot* capabilities) {
    const trevrpc_wt_profile_descriptor* descriptor = trevrpc_wt_profile_descriptor_find(profile);
    if (descriptor == NULL || capabilities == NULL || !trevrpc_msquic_feature_snapshot_usable_datagrams(capabilities)) {
        return false;
    }
    return !descriptor->requires_reset_stream_at_draft07 ||
           trevrpc_msquic_feature_snapshot_has_reset_dialect(capabilities, TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07);
}

bool trevrpc_wt_profile_any_usable(const trevrpc_msquic_feature_snapshot* capabilities) {
    for (size_t i = 0; i < sizeof(trevrpc_wt_profile_descriptors) / sizeof(trevrpc_wt_profile_descriptors[0]); i++) {
        if (trevrpc_wt_profile_is_usable(trevrpc_wt_profile_descriptors[i].id, capabilities)) {
            return true;
        }
    }
    return false;
}

const char* trevrpc_wt_profile_connect_protocol(trevrpc_wt_profile_id profile) {
    const trevrpc_wt_profile_descriptor* descriptor = trevrpc_wt_profile_descriptor_find(profile);
    return descriptor == NULL ? NULL : descriptor->emitted_connect_protocol;
}

bool trevrpc_wt_profile_accepts_connect_protocol(trevrpc_wt_profile_id profile, const char* protocol) {
    const trevrpc_wt_profile_descriptor* descriptor = trevrpc_wt_profile_descriptor_find(profile);
    if (descriptor == NULL || protocol == NULL) {
        return false;
    }
    return strcmp(protocol, descriptor->emitted_connect_protocol) == 0 ||
           (profile == TREV_WT_PROFILE_DRAFT_15 && strcmp(protocol, "webtransport") == 0);
}

bool trevrpc_wt_profile_requires_draft02_request_marker(trevrpc_wt_profile_id profile) {
    const trevrpc_wt_profile_descriptor* descriptor = trevrpc_wt_profile_descriptor_find(profile);
    return descriptor != NULL && descriptor->requires_draft02_request_marker;
}

bool trevrpc_wt_profile_requires_draft02_response_marker(trevrpc_wt_profile_id profile) {
    const trevrpc_wt_profile_descriptor* descriptor = trevrpc_wt_profile_descriptor_find(profile);
    return descriptor != NULL && descriptor->requires_draft02_response_marker;
}

bool trevrpc_wt_profile_valid_session_id(uint64_t stream_id) {
    return stream_id <= ((UINT64_C(1) << 62) - 1) && (stream_id & 0x03u) == 0;
}

bool trevrpc_wt_profile_peer_stream_is_unidirectional(uint64_t stream_id) {
    return (stream_id & 0x02u) != 0;
}

/* WebTransport stream errors occupy a reserved HTTP/3 range and skip H3 GREASE values. */
#define TREV_WT_APPLICATION_ERROR_FIRST UINT64_C(0x52e4a40fa8db)
#define TREV_WT_APPLICATION_ERROR_GREASE_MODULUS UINT64_C(0x1f)
#define TREV_WT_APPLICATION_ERROR_GREASE_REMAINDER UINT64_C(0x21)

static uint64_t trevrpc_wt_profile_application_error_max(trevrpc_wt_profile_id profile) {
    if (!trevrpc_wt_profile_is_supported(profile)) {
        return 0;
    }
    return profile == TREV_WT_PROFILE_DRAFT_02 ? UINT8_MAX : UINT32_MAX;
}

int trevrpc_wt_profile_encode_application_error(
    trevrpc_wt_profile_id profile, uint64_t application_error, uint64_t* out_http3_error) {
    uint64_t maximum;
    if (out_http3_error == NULL || (maximum = trevrpc_wt_profile_application_error_max(profile)) == 0) {
        return -EINVAL;
    }
    if (application_error > maximum) {
        return -ERANGE;
    }
    *out_http3_error = TREV_WT_APPLICATION_ERROR_FIRST + application_error + application_error / UINT64_C(0x1e);
    return 0;
}

int trevrpc_wt_profile_decode_application_error(
    trevrpc_wt_profile_id profile, uint64_t http3_error, uint64_t* out_application_error) {
    uint64_t maximum;
    uint64_t shifted;
    uint64_t application_error;
    if (out_application_error == NULL || (maximum = trevrpc_wt_profile_application_error_max(profile)) == 0) {
        return -EINVAL;
    }
    if (http3_error < TREV_WT_APPLICATION_ERROR_FIRST ||
        (http3_error - TREV_WT_APPLICATION_ERROR_GREASE_REMAINDER) % TREV_WT_APPLICATION_ERROR_GREASE_MODULUS == 0) {
        return -ERANGE;
    }
    shifted = http3_error - TREV_WT_APPLICATION_ERROR_FIRST;
    application_error = shifted - shifted / TREV_WT_APPLICATION_ERROR_GREASE_MODULUS;
    if (application_error > maximum) {
        return -ERANGE;
    }
    *out_application_error = application_error;
    return 0;
}

uint64_t trevrpc_wt_profile_effective_session_limit(uint32_t configured_limit) {
    (void)configured_limit;
    return 1;
}

int trevrpc_wt_profile_materialize_settings(trevrpc_wt_profile_advertisement advertisement,
    trevrpc_wt_profile_id profile,
    const trevrpc_msquic_feature_snapshot* capabilities,
    uint32_t configured_session_limit,
    trevrpc_wt_profile_setting_pair* out_settings,
    size_t out_capacity,
    size_t* out_count) {
    if (capabilities == NULL || out_settings == NULL || out_count == NULL ||
        (advertisement != TREV_WT_PROFILE_ADVERTISEMENT_SELECTED &&
            advertisement != TREV_WT_PROFILE_ADVERTISEMENT_ALL_SUPPORTED) ||
        (advertisement == TREV_WT_PROFILE_ADVERTISEMENT_SELECTED && !trevrpc_wt_profile_is_supported(profile)) ||
        (advertisement == TREV_WT_PROFILE_ADVERTISEMENT_ALL_SUPPORTED && profile != TREV_WT_PROFILE_NONE)) {
        return -EINVAL;
    }
    *out_count = 0;
    if (advertisement == TREV_WT_PROFILE_ADVERTISEMENT_SELECTED &&
        !trevrpc_wt_profile_is_usable(profile, capabilities)) {
        return -ENOTSUP;
    }
    uint64_t session_limit = trevrpc_wt_profile_effective_session_limit(configured_session_limit);
    int err = 0;

    for (size_t i = 0; i < sizeof(trevrpc_wt_profile_descriptors) / sizeof(trevrpc_wt_profile_descriptors[0]); i++) {
        const trevrpc_wt_profile_descriptor* descriptor = &trevrpc_wt_profile_descriptors[i];
        if ((advertisement != TREV_WT_PROFILE_ADVERTISEMENT_ALL_SUPPORTED && descriptor->id != profile) ||
            !trevrpc_wt_profile_is_usable(descriptor->id, capabilities)) {
            continue;
        }
        for (size_t setting_index = 0; setting_index < descriptor->emitted_settings_len; setting_index++) {
            const trevrpc_wt_profile_emitted_setting* setting = &descriptor->emitted_settings[setting_index];
            err = trevrpc_wt_profile_append(out_settings,
                out_capacity,
                out_count,
                setting->id,
                setting->uses_session_limit ? session_limit : setting->value);
            if (err != 0) {
                break;
            }
        }
        if (err != 0) {
            break;
        }
    }
    if (err != 0) {
        *out_count = 0;
        return err;
    }
    return 0;
}
