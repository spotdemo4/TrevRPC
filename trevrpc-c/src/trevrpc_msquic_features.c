#include "trevrpc_msquic_features_internal.h"

#if defined(TREVRPC_MSQUIC_PROVIDER_CAPABILITIES)
#include "trevrpc_msquic_api_owner.h"
#endif

#include <errno.h>
#include <stddef.h>
#include <string.h>

_Static_assert(
    sizeof(trevrpc_msquic_reset_stream_at_dialects_v1) == 32, "unexpected RESET_STREAM_AT descriptor layout");
_Static_assert(
    offsetof(trevrpc_msquic_reset_stream_at_dialects_v1, version) == 0, "unexpected RESET_STREAM_AT version offset");
_Static_assert(offsetof(trevrpc_msquic_reset_stream_at_dialects_v1, supported_dialect_mask) == 4,
    "unexpected RESET_STREAM_AT dialect mask offset");
_Static_assert(offsetof(trevrpc_msquic_reset_stream_at_dialects_v1, draft_07_transport_parameter_id) == 8,
    "unexpected RESET_STREAM_AT draft-07 transport parameter offset");
_Static_assert(offsetof(trevrpc_msquic_reset_stream_at_dialects_v1, draft_10_transport_parameter_id) == 16,
    "unexpected RESET_STREAM_AT draft-10 transport parameter offset");
_Static_assert(offsetof(trevrpc_msquic_reset_stream_at_dialects_v1, frame_type) == 24,
    "unexpected RESET_STREAM_AT frame type offset");

#if defined(TREVRPC_MSQUIC_PROVIDER_CAPABILITIES) && defined(QUIC_API_HAS_RESET_STREAM_AT_DIALECTS)
_Static_assert(sizeof(QUIC_RESET_STREAM_AT_DIALECTS) == 32, "unexpected MsQuic RESET_STREAM_AT descriptor layout");
_Static_assert(
    offsetof(QUIC_RESET_STREAM_AT_DIALECTS, Version) == 0, "unexpected MsQuic RESET_STREAM_AT version offset");
_Static_assert(offsetof(QUIC_RESET_STREAM_AT_DIALECTS, SupportedDialectMask) == 4,
    "unexpected MsQuic RESET_STREAM_AT dialect mask offset");
_Static_assert(offsetof(QUIC_RESET_STREAM_AT_DIALECTS, Draft07TransportParameterId) == 8,
    "unexpected MsQuic RESET_STREAM_AT draft-07 transport parameter offset");
_Static_assert(offsetof(QUIC_RESET_STREAM_AT_DIALECTS, Draft10TransportParameterId) == 16,
    "unexpected MsQuic RESET_STREAM_AT draft-10 transport parameter offset");
_Static_assert(
    offsetof(QUIC_RESET_STREAM_AT_DIALECTS, FrameType) == 24, "unexpected MsQuic RESET_STREAM_AT frame type offset");
_Static_assert(QUIC_RESET_STREAM_AT_DIALECTS_VERSION_1 == TREV_MSQUIC_RESET_STREAM_AT_DIALECTS_VERSION_1,
    "unexpected MsQuic RESET_STREAM_AT descriptor version");
_Static_assert(QUIC_RESET_STREAM_AT_DIALECT_DRAFT_07 == TREV_MSQUIC_NATIVE_RESET_STREAM_AT_DIALECT_DRAFT_07,
    "unexpected MsQuic draft-07 dialect bit");
_Static_assert(QUIC_RESET_STREAM_AT_DIALECT_DRAFT_10 == TREV_MSQUIC_NATIVE_RESET_STREAM_AT_DIALECT_DRAFT_10,
    "unexpected MsQuic draft-10 dialect bit");
_Static_assert(QUIC_RESET_STREAM_AT_SUPPORTED_DIALECT_MASK == TREV_MSQUIC_NATIVE_RESET_STREAM_AT_SUPPORTED_DIALECT_MASK,
    "unexpected MsQuic supported dialect mask");
#endif

bool trevrpc_msquic_reset_dialect_valid(trevrpc_msquic_reset_dialect dialect) {
    return dialect == TREV_MSQUIC_RESET_NONE || dialect == TREV_MSQUIC_RESET_MSQUIC_LEGACY ||
           dialect == TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07 || dialect == TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10;
}

uint32_t trevrpc_msquic_reset_dialect_bit(trevrpc_msquic_reset_dialect dialect) {
    switch (dialect) {
    case TREV_MSQUIC_RESET_MSQUIC_LEGACY:
        return TREV_MSQUIC_RESET_DIALECT_MSQUIC_LEGACY_BIT;
    case TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07:
        return TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT;
    case TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10:
        return TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT;
    case TREV_MSQUIC_RESET_NONE:
    default:
        return TREV_MSQUIC_RESET_DIALECTS_NONE;
    }
}

trevrpc_msquic_reset_identity trevrpc_msquic_reset_identity_for(trevrpc_msquic_reset_dialect dialect) {
    switch (dialect) {
    case TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07:
        return (trevrpc_msquic_reset_identity){
            .dialect = dialect,
            .transport_parameter = TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07_TRANSPORT_PARAMETER,
            .frame_type = TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07_FRAME,
        };
    case TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10:
        return (trevrpc_msquic_reset_identity){
            .dialect = dialect,
            .transport_parameter = TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10_TRANSPORT_PARAMETER,
            .frame_type = TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10_FRAME,
        };
    case TREV_MSQUIC_RESET_MSQUIC_LEGACY:
    case TREV_MSQUIC_RESET_NONE:
        return (trevrpc_msquic_reset_identity){.dialect = dialect};
    default:
        return (trevrpc_msquic_reset_identity){.dialect = TREV_MSQUIC_RESET_NONE};
    }
}

static bool trevrpc_msquic_reset_dialect_mask_valid(uint32_t dialects) {
    return (dialects & ~TREV_MSQUIC_RESET_DIALECTS_KNOWN) == 0;
}

static bool trevrpc_msquic_reset_request_is_exact(uint32_t dialects) {
    return dialects == 0 || (dialects & (dialects - 1)) == 0;
}

trevrpc_msquic_provider_features trevrpc_msquic_provider_features_attested(
    const void* descriptor, uint32_t descriptor_len) {
    trevrpc_msquic_provider_features features = {
        .datagrams = true,
        .supported_reset_dialects = TREV_MSQUIC_RESET_DIALECTS_NONE,
    };
    if (descriptor == NULL || descriptor_len != sizeof(trevrpc_msquic_reset_stream_at_dialects_v1)) {
        return features;
    }

    trevrpc_msquic_reset_stream_at_dialects_v1 attested;
    memcpy(&attested, descriptor, sizeof(attested));
    if (attested.version != TREV_MSQUIC_RESET_STREAM_AT_DIALECTS_VERSION_1 ||
        attested.supported_dialect_mask != TREV_MSQUIC_NATIVE_RESET_STREAM_AT_SUPPORTED_DIALECT_MASK ||
        attested.draft_07_transport_parameter_id != TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07_TRANSPORT_PARAMETER ||
        attested.draft_10_transport_parameter_id != TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10_TRANSPORT_PARAMETER ||
        attested.frame_type != TREV_MSQUIC_RESET_STREAM_AT_DRAFT_10_FRAME ||
        attested.frame_type != TREV_MSQUIC_RESET_STREAM_AT_DRAFT_07_FRAME) {
        return features;
    }

    features.supported_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT | TREV_MSQUIC_RESET_DIALECT_DRAFT_10_BIT;
    return features;
}

#if defined(TREVRPC_MSQUIC_PROVIDER_CAPABILITIES)
trevrpc_msquic_provider_features trevrpc_msquic_api_owner_features(void) {
    trevrpc_msquic_provider_features features = trevrpc_msquic_provider_features_attested(NULL, 0);
#if defined(QUIC_API_HAS_RESET_STREAM_AT_DIALECTS)
    const QUIC_API_TABLE* api = NULL;
    if (trevrpc_msquic_api_owner_acquire(&api) != 0) {
        return features;
    }

    QUIC_RESET_STREAM_AT_DIALECTS descriptor = {0};
    uint32_t descriptor_len = sizeof(descriptor);
    QUIC_STATUS status = api->GetParam(NULL, QUIC_PARAM_GLOBAL_RESET_STREAM_AT_DIALECTS, &descriptor_len, &descriptor);
    trevrpc_msquic_api_owner_release(api);
    if (QUIC_FAILED(status)) {
        return features;
    }
    features = trevrpc_msquic_provider_features_attested(&descriptor, descriptor_len);
#endif
    return features;
}
#endif

int trevrpc_msquic_feature_request_validate(
    const trevrpc_msquic_provider_features* provider, const trevrpc_msquic_feature_request* request) {
    if (provider == NULL || request == NULL ||
        !trevrpc_msquic_reset_dialect_mask_valid(provider->supported_reset_dialects) ||
        !trevrpc_msquic_reset_dialect_mask_valid(request->requested_reset_dialects) ||
        !trevrpc_msquic_reset_request_is_exact(request->requested_reset_dialects)) {
        return -EINVAL;
    }
    if (request->datagram_receive && !provider->datagrams) {
        return -ENOTSUP;
    }
    if ((request->requested_reset_dialects & ~provider->supported_reset_dialects) != 0) {
        return -ENOTSUP;
    }
    return 0;
}

trevrpc_msquic_feature_request trevrpc_msquic_generic_feature_request(void) {
    return (trevrpc_msquic_feature_request){
        .datagram_receive = false,
        .requested_reset_dialects = TREV_MSQUIC_RESET_DIALECTS_NONE,
    };
}

trevrpc_msquic_feature_request trevrpc_msquic_h3_feature_request(const trevrpc_msquic_provider_features* provider) {
    trevrpc_msquic_feature_request request = {
        .datagram_receive = true,
        .requested_reset_dialects = TREV_MSQUIC_RESET_DIALECTS_NONE,
    };
    if (provider != NULL && (provider->supported_reset_dialects & TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT) != 0) {
        request.requested_reset_dialects = TREV_MSQUIC_RESET_DIALECT_DRAFT_07_BIT;
    }
    return request;
}

void trevrpc_msquic_feature_state_init(
    trevrpc_msquic_feature_state* state, const trevrpc_msquic_feature_request* request) {
    if (state == NULL) {
        return;
    }
    memset(state, 0, sizeof(*state));
    state->request = request == NULL ? trevrpc_msquic_generic_feature_request() : *request;
}

void trevrpc_msquic_feature_reduce(trevrpc_msquic_feature_state* state, const trevrpc_msquic_feature_event* event) {
    if (state == NULL || event == NULL || state->terminal) {
        return;
    }
    switch (event->kind) {
    case TREV_MSQUIC_FEATURE_EVENT_CONNECTED:
        state->connected = true;
        /*
         * MsQuic does not publish DATAGRAM_STATE_CHANGED when the peer did
         * not negotiate datagrams. Handshake completion therefore finalizes
         * an absent datagram event as an unusable peer capability instead of
         * leaving connection readiness blocked forever. A real state-change
         * event received before or after this point still replaces the
         * snapshot and advances its epoch.
         */
        if (state->request.datagram_receive && !state->datagram_event_seen)
            state->datagram_event_seen = true;
        break;
    case TREV_MSQUIC_FEATURE_EVENT_DATAGRAM_STATE_CHANGED:
        state->datagram_event_seen = true;
        state->datagram_send_enabled = event->value;
        state->datagram_max_send_length = event->max_send_length;
        state->datagram_epoch++;
        if (state->datagram_epoch == 0) {
            state->datagram_epoch = 1;
        }
        break;
    case TREV_MSQUIC_FEATURE_EVENT_RELIABLE_RESET_NEGOTIATED:
        if (!state->connected || !state->reset_event_seen) {
            state->reset_event_seen = true;
            uint32_t requested = event->requested_reset_dialects & TREV_MSQUIC_RESET_DIALECTS_KNOWN;
            state->peer_reset_dialects = event->peer_reset_dialects & TREV_MSQUIC_RESET_DIALECTS_KNOWN;
            state->negotiated_reset_dialects = state->request.requested_reset_dialects & requested &
                                               state->peer_reset_dialects & event->negotiated_reset_dialects &
                                               TREV_MSQUIC_RESET_DIALECTS_KNOWN;
        }
        break;
    case TREV_MSQUIC_FEATURE_EVENT_TERMINAL:
        state->terminal = true;
        state->terminal_status = event->terminal_status;
        break;
    default:
        break;
    }
}

bool trevrpc_msquic_feature_ready(const trevrpc_msquic_feature_state* state) {
    if (state == NULL || state->terminal || !state->connected) {
        return false;
    }
    if (state->request.datagram_receive && !state->datagram_event_seen) {
        return false;
    }
    return state->request.requested_reset_dialects == 0 || state->reset_event_seen;
}

int trevrpc_msquic_feature_snapshot_get(
    const trevrpc_msquic_feature_state* state, trevrpc_msquic_feature_snapshot* snapshot) {
    if (state == NULL || snapshot == NULL) {
        return -EINVAL;
    }
    *snapshot = (trevrpc_msquic_feature_snapshot){0};
    if (state->terminal) {
        return state->terminal_status != 0 ? state->terminal_status : -ECANCELED;
    }
    if (!trevrpc_msquic_feature_ready(state)) {
        return -EAGAIN;
    }
    snapshot->local_datagram_receive = state->request.datagram_receive;
    snapshot->peer_datagram_receive = state->datagram_send_enabled;
    snapshot->datagram_send_enabled = state->datagram_send_enabled;
    snapshot->datagram_max_send_length = state->datagram_max_send_length;
    snapshot->configured_reset_dialects = state->request.requested_reset_dialects;
    snapshot->peer_reset_dialects = state->peer_reset_dialects;
    snapshot->negotiated_reset_dialects = state->negotiated_reset_dialects;
    snapshot->datagram_epoch = state->datagram_epoch;
    return 0;
}

bool trevrpc_msquic_feature_snapshot_negotiated_datagrams(const trevrpc_msquic_feature_snapshot* snapshot) {
    return snapshot != NULL && snapshot->local_datagram_receive && snapshot->peer_datagram_receive &&
           snapshot->datagram_send_enabled;
}

bool trevrpc_msquic_feature_snapshot_usable_datagrams(const trevrpc_msquic_feature_snapshot* snapshot) {
    return trevrpc_msquic_feature_snapshot_negotiated_datagrams(snapshot) && snapshot->datagram_max_send_length > 0;
}

bool trevrpc_msquic_feature_snapshot_has_reset_dialect(
    const trevrpc_msquic_feature_snapshot* snapshot, trevrpc_msquic_reset_dialect dialect) {
    uint32_t bit = trevrpc_msquic_reset_dialect_bit(dialect);
    return snapshot != NULL && bit != 0 && (snapshot->negotiated_reset_dialects & bit) != 0;
}
