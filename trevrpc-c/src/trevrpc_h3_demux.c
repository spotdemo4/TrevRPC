#include "trevrpc_h3_demux_internal.h"

#include "trevrpc_quic_varint_internal.h"
#include "trevrpc_webtransport_profile_internal.h"

#include <errno.h>
#include <string.h>

static int trevrpc_h3_demux_varint_consume(
    trevrpc_h3_demux_stream* stream, const uint8_t* data, size_t len, size_t* consumed, uint64_t* out_value) {
    size_t offset = 0;
    while (offset < len) {
        stream->varint[stream->varint_len++] = data[offset++];
        if (stream->varint_len == 1) {
            stream->varint_need = trevrpc_quic_varint_size_from_first(stream->varint[0]);
        }
        if (stream->varint_len == stream->varint_need) {
            size_t encoded_offset = 0;
            int err = trevrpc_quic_varint_read(stream->varint, stream->varint_len, &encoded_offset, out_value);
            if (err != 0 || encoded_offset != stream->varint_len) {
                return -EPROTO;
            }
            stream->varint_len = 0;
            stream->varint_need = 0;
            *consumed = offset;
            return 1;
        }
    }
    *consumed = offset;
    return 0;
}

static trevrpc_h3_demux_action trevrpc_h3_demux_unidirectional_action(uint64_t stream_type) {
    switch (stream_type) {
    case TREV_H3_DEMUX_STREAM_TYPE_CONTROL:
        return TREV_H3_DEMUX_ACTION_CONTROL;
    case TREV_H3_DEMUX_STREAM_TYPE_QPACK_ENCODER:
        return TREV_H3_DEMUX_ACTION_QPACK_ENCODER;
    case TREV_H3_DEMUX_STREAM_TYPE_QPACK_DECODER:
        return TREV_H3_DEMUX_ACTION_QPACK_DECODER;
    default:
        return TREV_H3_DEMUX_ACTION_UNKNOWN_UNIDIRECTIONAL;
    }
}

static trevrpc_h3_demux_result trevrpc_h3_demux_result_make(const trevrpc_h3_demux_stream* stream,
    size_t consumed,
    trevrpc_h3_demux_action action,
    uint64_t application_error) {
    return (trevrpc_h3_demux_result){
        .consumed = consumed,
        .action = action,
        .first_value = stream->first_value,
        .session_id = stream->session_id,
        .application_error = application_error,
    };
}

static bool trevrpc_h3_demux_profile_valid(const trevrpc_h3_demux_profile_capabilities* profile) {
    return profile != NULL &&
           ((!profile->resolved && profile->selected_profile == TREV_WT_PROFILE_NONE) ||
               (profile->resolved && (profile->selected_profile == TREV_WT_PROFILE_NONE ||
                                         trevrpc_wt_profile_is_supported(profile->selected_profile))));
}

static trevrpc_h3_demux_status trevrpc_h3_demux_resolve_waiting_profile(trevrpc_h3_demux_stream* stream,
    const trevrpc_h3_demux_profile_capabilities* profile,
    size_t consumed,
    trevrpc_h3_demux_result* out_result) {
    if (stream->phase != TREV_H3_DEMUX_WAIT_NEGOTIATED_PROFILE) {
        return TREV_H3_DEMUX_NEED_MORE;
    }
    if (!profile->resolved) {
        *out_result = trevrpc_h3_demux_result_make(stream, consumed, TREV_H3_DEMUX_ACTION_NONE, 0);
        return TREV_H3_DEMUX_WAIT_PROFILE;
    }
    if (profile->selected_profile != TREV_WT_PROFILE_NONE) {
        stream->phase = TREV_H3_DEMUX_READ_SESSION_ID;
        return TREV_H3_DEMUX_NEED_MORE;
    }
    if (stream->direction == TREV_H3_DEMUX_UNIDIRECTIONAL) {
        stream->action = TREV_H3_DEMUX_ACTION_UNKNOWN_UNIDIRECTIONAL;
        stream->phase = TREV_H3_DEMUX_READY;
        *out_result = trevrpc_h3_demux_result_make(stream, consumed, stream->action, 0);
        return TREV_H3_DEMUX_ACTION_READY;
    }

    /* Without WebTransport negotiation, 0x41 is an ordinary unknown HTTP/3 frame. */
    stream->phase = TREV_H3_DEMUX_READ_UNKNOWN_LENGTH;
    return TREV_H3_DEMUX_NEED_MORE;
}

bool trevrpc_h3_demux_stream_id_is_unidirectional(uint64_t stream_id) {
    return (stream_id & 0x02u) != 0;
}

int trevrpc_h3_demux_stream_init(trevrpc_h3_demux_stream* stream, trevrpc_h3_demux_direction direction) {
    if (stream == NULL || (direction != TREV_H3_DEMUX_BIDIRECTIONAL && direction != TREV_H3_DEMUX_UNIDIRECTIONAL)) {
        return -EINVAL;
    }
    *stream = (trevrpc_h3_demux_stream){
        .direction = direction,
        .phase = TREV_H3_DEMUX_READ_FIRST,
    };
    return 0;
}

trevrpc_h3_demux_status trevrpc_h3_demux_stream_feed(trevrpc_h3_demux_stream* stream,
    const trevrpc_h3_demux_profile_capabilities* profile,
    const uint8_t* data,
    size_t len,
    trevrpc_h3_demux_result* out_result) {
    if (stream == NULL || !trevrpc_h3_demux_profile_valid(profile) || out_result == NULL ||
        (data == NULL && len != 0) ||
        (stream->direction != TREV_H3_DEMUX_BIDIRECTIONAL && stream->direction != TREV_H3_DEMUX_UNIDIRECTIONAL)) {
        return TREV_H3_DEMUX_INVALID_ARGUMENT;
    }
    if (stream->phase == TREV_H3_DEMUX_READY || stream->phase == TREV_H3_DEMUX_FAILED) {
        *out_result = trevrpc_h3_demux_result_make(stream, 0, TREV_H3_DEMUX_ACTION_NONE, 0);
        return TREV_H3_DEMUX_COMPLETE;
    }

    trevrpc_h3_demux_stream next = *stream;
    trevrpc_h3_demux_status profile_status = trevrpc_h3_demux_resolve_waiting_profile(&next, profile, 0, out_result);
    if (profile_status != TREV_H3_DEMUX_NEED_MORE) {
        *stream = next;
        return profile_status;
    }

    size_t total_consumed = 0;
    while (total_consumed < len) {
        if (next.phase == TREV_H3_DEMUX_SKIP_UNKNOWN_PAYLOAD) {
            uint64_t available = (uint64_t)(len - total_consumed);
            uint64_t skipped = next.unknown_payload_remaining < available ? next.unknown_payload_remaining : available;
            total_consumed += (size_t)skipped;
            next.unknown_payload_remaining -= skipped;
            if (next.unknown_payload_remaining != 0) {
                break;
            }
            next.phase = TREV_H3_DEMUX_READ_FIRST;
            continue;
        }

        uint64_t value = 0;
        size_t consumed = 0;
        int decoded =
            trevrpc_h3_demux_varint_consume(&next, data + total_consumed, len - total_consumed, &consumed, &value);
        total_consumed += consumed;
        if (decoded < 0) {
            next.phase = TREV_H3_DEMUX_FAILED;
            *stream = next;
            *out_result = trevrpc_h3_demux_result_make(
                stream, total_consumed, TREV_H3_DEMUX_ACTION_NONE, TREV_H3_DEMUX_APP_FRAME_ERROR);
            return TREV_H3_DEMUX_PROTOCOL_ERROR;
        }
        if (decoded == 0) {
            break;
        }

        if (next.phase == TREV_H3_DEMUX_READ_FIRST) {
            next.first_value = value;
            if (next.direction == TREV_H3_DEMUX_UNIDIRECTIONAL) {
                if (value == TREV_H3_DEMUX_WEBTRANSPORT_UNI) {
                    next.phase = TREV_H3_DEMUX_WAIT_NEGOTIATED_PROFILE;
                } else {
                    next.action = trevrpc_h3_demux_unidirectional_action(value);
                    next.phase = TREV_H3_DEMUX_READY;
                }
            } else if (value == TREV_H3_FRAME_HEADERS) {
                next.action = TREV_H3_DEMUX_ACTION_REQUEST;
                next.phase = TREV_H3_DEMUX_READY;
            } else if (value == TREV_H3_DEMUX_WEBTRANSPORT_BIDI) {
                next.phase = TREV_H3_DEMUX_WAIT_NEGOTIATED_PROFILE;
            } else if (value == TREV_H3_FRAME_DATA || trevrpc_h3_frame_type_is_request_prohibited(value)) {
                next.phase = TREV_H3_DEMUX_FAILED;
                *stream = next;
                *out_result = trevrpc_h3_demux_result_make(
                    stream, total_consumed, TREV_H3_DEMUX_ACTION_NONE, TREV_H3_DEMUX_APP_FRAME_UNEXPECTED);
                return TREV_H3_DEMUX_PROTOCOL_ERROR;
            } else {
                next.phase = TREV_H3_DEMUX_READ_UNKNOWN_LENGTH;
            }

            profile_status = trevrpc_h3_demux_resolve_waiting_profile(&next, profile, total_consumed, out_result);
            if (profile_status != TREV_H3_DEMUX_NEED_MORE) {
                *stream = next;
                return profile_status;
            }
        } else if (next.phase == TREV_H3_DEMUX_READ_SESSION_ID) {
            next.session_id = value;
            if (!trevrpc_wt_profile_valid_session_id(value)) {
                next.phase = TREV_H3_DEMUX_FAILED;
                *stream = next;
                *out_result = trevrpc_h3_demux_result_make(
                    stream, total_consumed, TREV_H3_DEMUX_ACTION_NONE, TREV_H3_DEMUX_APP_ID_ERROR);
                return TREV_H3_DEMUX_PROTOCOL_ERROR;
            }
            next.action = TREV_H3_DEMUX_ACTION_WEBTRANSPORT;
            next.phase = TREV_H3_DEMUX_READY;
        } else if (next.phase == TREV_H3_DEMUX_READ_UNKNOWN_LENGTH) {
            next.unknown_payload_remaining = value;
            next.phase = value == 0 ? TREV_H3_DEMUX_READ_FIRST : TREV_H3_DEMUX_SKIP_UNKNOWN_PAYLOAD;
        }

        if (next.phase == TREV_H3_DEMUX_READY) {
            *stream = next;
            *out_result = trevrpc_h3_demux_result_make(stream, total_consumed, stream->action, 0);
            return TREV_H3_DEMUX_ACTION_READY;
        }
    }

    *stream = next;
    *out_result = trevrpc_h3_demux_result_make(stream, total_consumed, TREV_H3_DEMUX_ACTION_NONE, 0);
    return TREV_H3_DEMUX_NEED_MORE;
}

trevrpc_h3_demux_status trevrpc_h3_demux_stream_terminal(trevrpc_h3_demux_stream* stream,
    const trevrpc_h3_demux_profile_capabilities* profile,
    trevrpc_h3_demux_result* out_result) {
    if (stream == NULL || !trevrpc_h3_demux_profile_valid(profile) || out_result == NULL ||
        (stream->direction != TREV_H3_DEMUX_BIDIRECTIONAL && stream->direction != TREV_H3_DEMUX_UNIDIRECTIONAL)) {
        return TREV_H3_DEMUX_INVALID_ARGUMENT;
    }
    if (stream->phase == TREV_H3_DEMUX_READY || stream->phase == TREV_H3_DEMUX_FAILED) {
        *out_result = trevrpc_h3_demux_result_make(stream, 0, TREV_H3_DEMUX_ACTION_NONE, 0);
        return TREV_H3_DEMUX_COMPLETE;
    }

    trevrpc_h3_demux_status status = trevrpc_h3_demux_stream_feed(stream, profile, NULL, 0, out_result);
    if (status != TREV_H3_DEMUX_NEED_MORE) {
        return status;
    }

    if (stream->direction == TREV_H3_DEMUX_UNIDIRECTIONAL && stream->phase == TREV_H3_DEMUX_READ_FIRST) {
        stream->action = TREV_H3_DEMUX_ACTION_RECLAIM_UNCLASSIFIED;
        stream->phase = TREV_H3_DEMUX_READY;
        *out_result = trevrpc_h3_demux_result_make(stream, 0, stream->action, 0);
        return TREV_H3_DEMUX_ACTION_READY;
    }

    stream->phase = TREV_H3_DEMUX_FAILED;
    *out_result = trevrpc_h3_demux_result_make(stream, 0, TREV_H3_DEMUX_ACTION_NONE, TREV_H3_DEMUX_APP_FRAME_ERROR);
    return TREV_H3_DEMUX_PROTOCOL_ERROR;
}

int trevrpc_h3_demux_roles_claim(
    trevrpc_h3_demux_roles* roles, trevrpc_h3_demux_action action, uint64_t* out_application_error) {
    if (roles == NULL || out_application_error == NULL) {
        return -EINVAL;
    }

    bool* claimed = NULL;
    switch (action) {
    case TREV_H3_DEMUX_ACTION_CONTROL:
        claimed = &roles->control;
        break;
    case TREV_H3_DEMUX_ACTION_QPACK_ENCODER:
        claimed = &roles->qpack_encoder;
        break;
    case TREV_H3_DEMUX_ACTION_QPACK_DECODER:
        claimed = &roles->qpack_decoder;
        break;
    case TREV_H3_DEMUX_ACTION_NONE:
    case TREV_H3_DEMUX_ACTION_UNKNOWN_UNIDIRECTIONAL:
    case TREV_H3_DEMUX_ACTION_REQUEST:
    case TREV_H3_DEMUX_ACTION_WEBTRANSPORT:
    case TREV_H3_DEMUX_ACTION_RECLAIM_UNCLASSIFIED:
        *out_application_error = 0;
        return 0;
    default:
        return -EINVAL;
    }

    if (*claimed) {
        *out_application_error = TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR;
        return -EEXIST;
    }
    *claimed = true;
    *out_application_error = 0;
    return 0;
}
