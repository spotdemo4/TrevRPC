#ifndef TREVRPC_MSQUIC_FEATURES_INTERNAL_H
#define TREVRPC_MSQUIC_FEATURES_INTERNAL_H

#include <stdbool.h>
#include <stdint.h>

#define TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07_TRANSPORT_PARAMETER 0x17f7586d2cb571ull
#define TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07_FRAME 0x24ull
#define TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10_TRANSPORT_PARAMETER 0x1dull
#define TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10_FRAME 0x24ull

typedef enum trevrpc_msquic_reset_dialect {
    TREV_MSQUIC_RESET_NONE = 0,
    TREV_MSQUIC_RESET_MSQUIC_LEGACY = 1,
    TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07 = 2,
    TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10 = 3,
} trevrpc_msquic_reset_dialect;

typedef struct trevrpc_msquic_reset_identity {
    trevrpc_msquic_reset_dialect dialect;
    uint64_t transport_parameter;
    uint64_t frame_type;
} trevrpc_msquic_reset_identity;

#define TREV_MSQUIC_RESET_DIALECTS_NONE 0u
#define TREV_MSQUIC_RESET_DIALECT_MSQUIC_LEGACY_BIT (1u << 0)
#define TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT (1u << 1)
#define TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT (1u << 2)
#define TREV_MSQUIC_RESET_DIALECTS_KNOWN                                                                               \
    (TREV_MSQUIC_RESET_DIALECT_MSQUIC_LEGACY_BIT | TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT |                            \
        TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT)

#define TREV_MSQUIC_RESET_STREAM_AT_DIALECTS_VERSION_1 1u
#define TREV_MSQUIC_NATIVE_RESET_STREAM_AT_DIALECT_DRAFT_07 (1u << 0)
#define TREV_MSQUIC_NATIVE_RESET_STREAM_AT_DIALECT_DRAFT_10 (1u << 1)
#define TREV_MSQUIC_NATIVE_RESET_STREAM_AT_SUPPORTED_DIALECT_MASK                                                      \
    (TREV_MSQUIC_NATIVE_RESET_STREAM_AT_DIALECT_DRAFT_07 | TREV_MSQUIC_NATIVE_RESET_STREAM_AT_DIALECT_DRAFT_10)

typedef struct trevrpc_msquic_reset_stream_at_dialects_v1 {
    uint32_t version;
    uint32_t supported_dialect_mask;
    uint64_t draft_07_transport_parameter_id;
    uint64_t draft_10_transport_parameter_id;
    uint64_t frame_type;
} trevrpc_msquic_reset_stream_at_dialects_v1;

typedef struct trevrpc_msquic_feature_request {
    bool datagram_receive;
    uint32_t requested_reset_dialects;
} trevrpc_msquic_feature_request;

typedef struct trevrpc_msquic_provider_features {
    bool datagrams;
    uint32_t supported_reset_dialects;
} trevrpc_msquic_provider_features;

typedef struct trevrpc_msquic_feature_snapshot {
    bool local_datagram_receive;
    bool peer_datagram_receive;
    bool datagram_send_enabled;
    uint16_t datagram_max_send_length;
    uint32_t configured_reset_dialects;
    uint32_t peer_reset_dialects;
    uint32_t negotiated_reset_dialects;
    uint64_t datagram_epoch;
} trevrpc_msquic_feature_snapshot;

typedef enum trevrpc_msquic_feature_event_kind {
    TREV_MSQUIC_FEATURE_EVENT_CONNECTED = 0,
    TREV_MSQUIC_FEATURE_EVENT_DATAGRAM_STATE_CHANGED = 1,
    TREV_MSQUIC_FEATURE_EVENT_RELIABLE_RESET_NEGOTIATED = 2,
    TREV_MSQUIC_FEATURE_EVENT_TERMINAL = 3,
} trevrpc_msquic_feature_event_kind;

typedef struct trevrpc_msquic_feature_event {
    trevrpc_msquic_feature_event_kind kind;
    bool value;
    uint16_t max_send_length;
    uint32_t requested_reset_dialects;
    uint32_t peer_reset_dialects;
    uint32_t negotiated_reset_dialects;
    int terminal_status;
} trevrpc_msquic_feature_event;

typedef struct trevrpc_msquic_feature_state {
    trevrpc_msquic_feature_request request;
    bool connected;
    bool datagram_event_seen;
    bool datagram_send_enabled;
    uint16_t datagram_max_send_length;
    bool reset_event_seen;
    uint32_t peer_reset_dialects;
    uint32_t negotiated_reset_dialects;
    bool terminal;
    int terminal_status;
    uint64_t datagram_epoch;
} trevrpc_msquic_feature_state;

bool trevrpc_msquic_reset_dialect_valid(trevrpc_msquic_reset_dialect dialect);
uint32_t trevrpc_msquic_reset_dialect_bit(trevrpc_msquic_reset_dialect dialect);
trevrpc_msquic_reset_identity trevrpc_msquic_reset_identity_for(trevrpc_msquic_reset_dialect dialect);
int trevrpc_msquic_feature_request_validate(
    const trevrpc_msquic_provider_features* provider, const trevrpc_msquic_feature_request* request);
trevrpc_msquic_provider_features trevrpc_msquic_provider_features_attested(
    const void* descriptor, uint32_t descriptor_len);
#if defined(TREVRPC_MSQUIC_PROVIDER_CAPABILITIES)
trevrpc_msquic_provider_features trevrpc_msquic_api_owner_features(void);
#endif
trevrpc_msquic_feature_request trevrpc_msquic_generic_feature_request(void);
trevrpc_msquic_feature_request trevrpc_msquic_h3_feature_request(const trevrpc_msquic_provider_features* provider);
void trevrpc_msquic_feature_state_init(
    trevrpc_msquic_feature_state* state, const trevrpc_msquic_feature_request* request);
void trevrpc_msquic_feature_reduce(trevrpc_msquic_feature_state* state, const trevrpc_msquic_feature_event* event);
bool trevrpc_msquic_feature_ready(const trevrpc_msquic_feature_state* state);
int trevrpc_msquic_feature_snapshot_get(
    const trevrpc_msquic_feature_state* state, trevrpc_msquic_feature_snapshot* snapshot);
bool trevrpc_msquic_feature_snapshot_negotiated_datagrams(const trevrpc_msquic_feature_snapshot* snapshot);
bool trevrpc_msquic_feature_snapshot_usable_datagrams(const trevrpc_msquic_feature_snapshot* snapshot);
bool trevrpc_msquic_feature_snapshot_has_reset_dialect(
    const trevrpc_msquic_feature_snapshot* snapshot, trevrpc_msquic_reset_dialect dialect);

#endif
