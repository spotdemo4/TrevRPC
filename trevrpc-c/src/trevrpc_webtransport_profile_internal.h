#ifndef TREVRPC_WEBTRANSPORT_PROFILE_INTERNAL_H
#define TREVRPC_WEBTRANSPORT_PROFILE_INTERNAL_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "trevrpc_msquic_features_internal.h"

#define TREV_WT_PROFILE_SETTINGS_ENABLE_CONNECT_PROTOCOL 0x08
#define TREV_WT_PROFILE_SETTINGS_H3_DATAGRAM 0x33
#define TREV_WT_PROFILE_SETTINGS_H3_DRAFT04_DATAGRAM 0xffd277
#define TREV_WT_PROFILE_SETTINGS_INITIAL_MAX_DATA 0x2b61
#define TREV_WT_PROFILE_SETTINGS_INITIAL_MAX_STREAMS_UNI 0x2b64
#define TREV_WT_PROFILE_SETTINGS_INITIAL_MAX_STREAMS_BIDI 0x2b65
#define TREV_WT_PROFILE_SETTINGS_DRAFT02 0x2b603742
#define TREV_WT_PROFILE_SETTINGS_DRAFT07_MAX_SESSIONS 0xc671706a
#define TREV_WT_PROFILE_SETTINGS_DRAFT15_ENABLED 0x2c7cf000
#define TREV_WT_PROFILE_SETTINGS_DRAFT14_MAX_SESSIONS 0x14e9cd29
#define TREV_WT_PROFILE_MAX_FLOW_CONTROL_VALUE (1ull << 60)

typedef enum trevrpc_wt_role {
    TREV_WT_ROLE_SERVER = 0,
    TREV_WT_ROLE_CLIENT = 1,
} trevrpc_wt_role;

typedef enum trevrpc_wt_profile_id {
    TREV_WT_PROFILE_NONE = 0,
    TREV_WT_PROFILE_DRAFT_02 = 2,
    TREV_WT_PROFILE_DRAFT_07 = 7,
    TREV_WT_PROFILE_DRAFT_14 = 14,
    TREV_WT_PROFILE_DRAFT_15 = 15,
    /* Draft 16 has no distinct stable negotiation codepoint yet. */
    TREV_WT_PROFILE_DRAFT_16_RESERVED = 16,
} trevrpc_wt_profile_id;

typedef enum trevrpc_wt_profile_advertisement {
    TREV_WT_PROFILE_ADVERTISEMENT_SELECTED = 0,
    TREV_WT_PROFILE_ADVERTISEMENT_ALL_SUPPORTED = 1,
} trevrpc_wt_profile_advertisement;

typedef struct trevrpc_wt_profile_setting_pair {
    uint64_t id;
    uint64_t value;
} trevrpc_wt_profile_setting_pair;

typedef struct trevrpc_wt_peer_settings {
    bool enable_connect_protocol;
    bool enable_webtransport_draft02;
    bool enable_webtransport_draft07;
    bool enable_webtransport_draft14;
    bool enable_webtransport_draft15;
    bool h3_datagram_rfc;
    bool h3_datagram_draft04;
    bool seen_enable_connect_protocol;
    bool seen_enable_webtransport_draft02;
    bool seen_enable_webtransport_draft07;
    bool seen_enable_webtransport_draft14;
    bool seen_enable_webtransport_draft15;
    bool seen_h3_datagram_rfc;
    bool seen_h3_datagram_draft04;
    bool seen_initial_max_data;
    bool seen_initial_max_streams_uni;
    bool seen_initial_max_streams_bidi;
    uint64_t draft07_max_sessions;
    uint64_t draft14_max_sessions;
    uint64_t wt_initial_max_data;
    uint64_t wt_initial_max_streams_uni;
    uint64_t wt_initial_max_streams_bidi;
} trevrpc_wt_peer_settings;

enum {
    TREV_WT_PROFILE_COMPAT_INITIAL_CAPSULE_FLOW_CONTROL = 1u << 0,
};

typedef struct trevrpc_wt_profile_negotiation {
    trevrpc_wt_profile_id profile;
    uint32_t compatibility_flags;
    bool peer_modern_flow_control_intent;
} trevrpc_wt_profile_negotiation;

/* Returns -EPROTO for invalid or duplicate recognized SETTINGS. */
int trevrpc_wt_profile_apply_setting(trevrpc_wt_peer_settings* settings, uint64_t id, uint64_t value, bool* recognized);

int trevrpc_wt_profile_negotiate(trevrpc_wt_role role,
    const trevrpc_wt_peer_settings* settings,
    const trevrpc_msquic_feature_snapshot* capabilities,
    bool require_webtransport,
    trevrpc_wt_profile_negotiation* out_negotiation);

bool trevrpc_wt_profile_is_supported(trevrpc_wt_profile_id profile);
bool trevrpc_wt_profile_is_usable(trevrpc_wt_profile_id profile, const trevrpc_msquic_feature_snapshot* capabilities);
bool trevrpc_wt_profile_any_usable(const trevrpc_msquic_feature_snapshot* capabilities);
/* Returns NULL when profile is not a concrete supported profile. */
const char* trevrpc_wt_profile_connect_protocol(trevrpc_wt_profile_id profile);
bool trevrpc_wt_profile_accepts_connect_protocol(trevrpc_wt_profile_id profile, const char* protocol);
bool trevrpc_wt_profile_requires_draft02_request_marker(trevrpc_wt_profile_id profile);
bool trevrpc_wt_profile_requires_draft02_response_marker(trevrpc_wt_profile_id profile);
bool trevrpc_wt_profile_valid_session_id(uint64_t stream_id);
bool trevrpc_wt_profile_peer_stream_is_unidirectional(uint64_t stream_id);

/* Current transport ownership supports one session; profile wire output is truthful. */
uint64_t trevrpc_wt_profile_effective_session_limit(uint32_t configured_limit);

int trevrpc_wt_profile_materialize_settings(trevrpc_wt_profile_advertisement advertisement,
    trevrpc_wt_profile_id profile,
    const trevrpc_msquic_feature_snapshot* capabilities,
    uint32_t configured_session_limit,
    trevrpc_wt_profile_setting_pair* out_settings,
    size_t out_capacity,
    size_t* out_count);

#endif
