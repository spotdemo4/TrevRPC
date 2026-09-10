#include "trevrpc_http3_frame_internal.h"

#include "trevrpc_quic_varint_internal.h"

static void trevrpc_h3_frame_prefix_clear_varint(trevrpc_h3_frame_prefix_parser* parser) {
    parser->varint_have = 0;
    parser->varint_need = 0;
}

static bool trevrpc_h3_frame_prefix_read_varint(
    trevrpc_h3_frame_prefix_parser* parser, const uint8_t* data, size_t data_len, size_t* offset, uint64_t* out_value) {
    if (parser->varint_have == 0 && *offset < data_len) {
        parser->varint_need = (uint8_t)trevrpc_quic_varint_size_from_first(data[*offset]);
    }
    while (parser->varint_have < parser->varint_need && *offset < data_len) {
        parser->varint[parser->varint_have++] = data[(*offset)++];
    }
    if (parser->varint_need == 0 || parser->varint_have != parser->varint_need) {
        return false;
    }

    size_t varint_offset = 0;
    uint64_t value = 0;
    if (trevrpc_quic_varint_read(parser->varint, parser->varint_need, &varint_offset, &value) != 0 ||
        varint_offset != parser->varint_need) {
        return false;
    }
    trevrpc_h3_frame_prefix_clear_varint(parser);
    *out_value = value;
    return true;
}

static trevrpc_h3_frame_status trevrpc_h3_frame_prefix_fail(
    trevrpc_h3_frame_prefix_parser* parser, trevrpc_h3_frame_status failure) {
    parser->phase = TREV_H3_FRAME_PREFIX_FAILED;
    parser->failure = failure;
    return failure;
}

trevrpc_h3_frame_status trevrpc_h3_frame_prefix_parser_init(
    trevrpc_h3_frame_prefix_parser* parser, uint64_t max_payload_length) {
    if (parser == NULL) {
        return TREV_H3_FRAME_INVALID_ARGUMENT;
    }
    *parser = (trevrpc_h3_frame_prefix_parser){
        .max_payload_length = max_payload_length,
        .phase = TREV_H3_FRAME_PREFIX_TYPE,
        .failure = TREV_H3_FRAME_NEED_INPUT,
    };
    return TREV_H3_FRAME_NEED_INPUT;
}

trevrpc_h3_frame_status trevrpc_h3_frame_prefix_parser_reset(trevrpc_h3_frame_prefix_parser* parser) {
    if (parser == NULL) {
        return TREV_H3_FRAME_INVALID_ARGUMENT;
    }
    uint64_t max_payload_length = parser->max_payload_length;
    return trevrpc_h3_frame_prefix_parser_init(parser, max_payload_length);
}

trevrpc_h3_frame_status trevrpc_h3_frame_prefix_parser_feed(trevrpc_h3_frame_prefix_parser* parser,
    const uint8_t* data,
    size_t data_len,
    size_t* out_consumed,
    trevrpc_h3_frame_prefix* out_prefix) {
    if (parser == NULL || (data == NULL && data_len != 0) || out_consumed == NULL || out_prefix == NULL) {
        return TREV_H3_FRAME_INVALID_ARGUMENT;
    }
    if (parser->phase == TREV_H3_FRAME_PREFIX_FAILED) {
        *out_consumed = 0;
        return parser->failure;
    }
    if (parser->phase == TREV_H3_FRAME_PREFIX_READY) {
        *out_consumed = 0;
        return TREV_H3_FRAME_OK;
    }

    size_t offset = 0;
    while (offset < data_len) {
        if (parser->phase == TREV_H3_FRAME_PREFIX_TYPE) {
            uint64_t frame_type = 0;
            if (!trevrpc_h3_frame_prefix_read_varint(parser, data, data_len, &offset, &frame_type)) {
                break;
            }
            parser->frame_type = frame_type;
            parser->phase = TREV_H3_FRAME_PREFIX_LENGTH;
            continue;
        }

        uint64_t length = 0;
        if (!trevrpc_h3_frame_prefix_read_varint(parser, data, data_len, &offset, &length)) {
            break;
        }
        if (length > parser->max_payload_length) {
            *out_consumed = offset;
            return trevrpc_h3_frame_prefix_fail(parser, TREV_H3_FRAME_EXCESSIVE_LOAD);
        }

        trevrpc_h3_frame_prefix prefix = {
            .type = parser->frame_type,
            .length = length,
        };
        parser->phase = TREV_H3_FRAME_PREFIX_READY;
        *out_prefix = prefix;
        *out_consumed = offset;
        return TREV_H3_FRAME_OK;
    }

    *out_consumed = offset;
    return TREV_H3_FRAME_NEED_INPUT;
}

trevrpc_h3_frame_status trevrpc_h3_frame_prefix_parser_finish(trevrpc_h3_frame_prefix_parser* parser) {
    if (parser == NULL) {
        return TREV_H3_FRAME_INVALID_ARGUMENT;
    }
    if (parser->phase == TREV_H3_FRAME_PREFIX_FAILED) {
        return parser->failure;
    }
    if (parser->phase == TREV_H3_FRAME_PREFIX_READY) {
        return TREV_H3_FRAME_OK;
    }
    if (parser->phase == TREV_H3_FRAME_PREFIX_TYPE) {
        if (parser->varint_have == 0) {
            return TREV_H3_FRAME_CLEAN_EOF;
        }
        return trevrpc_h3_frame_prefix_fail(parser, TREV_H3_FRAME_TRUNCATED_TYPE);
    }
    if (parser->varint_have == 0) {
        return trevrpc_h3_frame_prefix_fail(parser, TREV_H3_FRAME_TYPE_WITHOUT_LENGTH);
    }
    return trevrpc_h3_frame_prefix_fail(parser, TREV_H3_FRAME_TRUNCATED_LENGTH);
}

trevrpc_h3_frame_status trevrpc_h3_frame_prefix_build(
    uint64_t type, uint64_t length, uint8_t* out, size_t out_capacity, size_t* out_length) {
    if (out_length == NULL || (out == NULL && out_capacity != 0)) {
        return TREV_H3_FRAME_INVALID_ARGUMENT;
    }

    size_t type_size = 0;
    size_t length_size = 0;
    if (trevrpc_quic_varint_size(type, &type_size) != 0 || trevrpc_quic_varint_size(length, &length_size) != 0) {
        return TREV_H3_FRAME_INVALID_ARGUMENT;
    }
    if (type_size > SIZE_MAX - length_size) {
        return TREV_H3_FRAME_INVALID_ARGUMENT;
    }
    size_t required = type_size + length_size;
    if (out_capacity < required) {
        return TREV_H3_FRAME_OUTPUT_TOO_SMALL;
    }

    size_t written = 0;
    size_t offset = 0;
    if (trevrpc_quic_varint_write(out, out_capacity, type, &written) != 0) {
        return TREV_H3_FRAME_INVALID_ARGUMENT;
    }
    offset += written;
    if (trevrpc_quic_varint_write(out + offset, out_capacity - offset, length, &written) != 0) {
        return TREV_H3_FRAME_INVALID_ARGUMENT;
    }
    offset += written;
    *out_length = offset;
    return TREV_H3_FRAME_OK;
}

trevrpc_h3_frame_status trevrpc_h3_unknown_discard_budget_init(
    trevrpc_h3_unknown_discard_budget* budget, uint64_t limit) {
    if (budget == NULL) {
        return TREV_H3_FRAME_INVALID_ARGUMENT;
    }
    *budget = (trevrpc_h3_unknown_discard_budget){
        .limit = limit,
        .used = 0,
    };
    return TREV_H3_FRAME_OK;
}

trevrpc_h3_frame_status trevrpc_h3_unknown_discard_budget_charge(
    trevrpc_h3_unknown_discard_budget* budget, uint64_t payload_length) {
    if (budget == NULL) {
        return TREV_H3_FRAME_INVALID_ARGUMENT;
    }
    if (budget->used > budget->limit || payload_length > budget->limit - budget->used) {
        return TREV_H3_FRAME_EXCESSIVE_LOAD;
    }
    budget->used += payload_length;
    return TREV_H3_FRAME_OK;
}

trevrpc_h3_frame_status trevrpc_h3_request_frame_state_init(trevrpc_h3_request_frame_state* state,
    bool initial_headers_received,
    uint64_t max_encoded_field_section_length,
    uint64_t max_unknown_discard) {
    if (state == NULL) {
        return TREV_H3_FRAME_INVALID_ARGUMENT;
    }
    *state = (trevrpc_h3_request_frame_state){
        .phase = initial_headers_received ? TREV_H3_REQUEST_BODY : TREV_H3_REQUEST_BEFORE_HEADERS,
        .active_payload = TREV_H3_REQUEST_PAYLOAD_NONE,
        .payload_remaining = 0,
        .max_encoded_field_section_length = max_encoded_field_section_length,
        .unknown_discard =
            {
                .limit = max_unknown_discard,
                .used = 0,
            },
    };
    return TREV_H3_FRAME_OK;
}

trevrpc_h3_frame_status trevrpc_h3_request_frame_begin(trevrpc_h3_request_frame_state* state,
    const trevrpc_h3_frame_prefix* prefix,
    trevrpc_h3_request_payload_kind* out_payload_kind) {
    if (state == NULL || prefix == NULL || out_payload_kind == NULL) {
        return TREV_H3_FRAME_INVALID_ARGUMENT;
    }
    if (state->active_payload != TREV_H3_REQUEST_PAYLOAD_NONE || state->payload_remaining != 0) {
        return TREV_H3_FRAME_PAYLOAD_ACTIVE;
    }
    if (state->phase == TREV_H3_REQUEST_COMPLETE || trevrpc_h3_frame_type_is_request_prohibited(prefix->type)) {
        return TREV_H3_FRAME_UNEXPECTED;
    }

    trevrpc_h3_request_phase next_phase = state->phase;
    trevrpc_h3_request_payload_kind payload_kind = TREV_H3_REQUEST_PAYLOAD_UNKNOWN;
    if (prefix->type == TREV_H3_FRAME_DATA) {
        if (state->phase != TREV_H3_REQUEST_BODY) {
            return TREV_H3_FRAME_UNEXPECTED;
        }
        payload_kind = TREV_H3_REQUEST_PAYLOAD_DATA;
    } else if (prefix->type == TREV_H3_FRAME_HEADERS) {
        if (state->phase == TREV_H3_REQUEST_BEFORE_HEADERS) {
            payload_kind = TREV_H3_REQUEST_PAYLOAD_HEADERS;
            next_phase = TREV_H3_REQUEST_BODY;
        } else if (state->phase == TREV_H3_REQUEST_BODY) {
            payload_kind = TREV_H3_REQUEST_PAYLOAD_TRAILERS;
            next_phase = TREV_H3_REQUEST_TRAILERS;
        } else {
            return TREV_H3_FRAME_UNEXPECTED;
        }
        if (prefix->length > state->max_encoded_field_section_length) {
            return TREV_H3_FRAME_FIELD_SECTION_TOO_LARGE;
        }
    } else {
        trevrpc_h3_frame_status charged =
            trevrpc_h3_unknown_discard_budget_charge(&state->unknown_discard, prefix->length);
        if (charged != TREV_H3_FRAME_OK) {
            return charged;
        }
    }

    state->phase = next_phase;
    state->payload_remaining = prefix->length;
    state->active_payload = prefix->length == 0 ? TREV_H3_REQUEST_PAYLOAD_NONE : payload_kind;
    *out_payload_kind = payload_kind;
    return TREV_H3_FRAME_OK;
}

trevrpc_h3_frame_status trevrpc_h3_request_frame_consume(trevrpc_h3_request_frame_state* state, uint64_t amount) {
    if (state == NULL) {
        return TREV_H3_FRAME_INVALID_ARGUMENT;
    }
    if (amount > state->payload_remaining || (amount != 0 && state->active_payload == TREV_H3_REQUEST_PAYLOAD_NONE)) {
        return TREV_H3_FRAME_PAYLOAD_OVERCONSUMED;
    }
    state->payload_remaining -= amount;
    if (state->payload_remaining == 0) {
        state->active_payload = TREV_H3_REQUEST_PAYLOAD_NONE;
    }
    return TREV_H3_FRAME_OK;
}

trevrpc_h3_frame_status trevrpc_h3_request_frame_finish(trevrpc_h3_request_frame_state* state) {
    if (state == NULL) {
        return TREV_H3_FRAME_INVALID_ARGUMENT;
    }
    if (state->active_payload != TREV_H3_REQUEST_PAYLOAD_NONE || state->payload_remaining != 0) {
        return TREV_H3_FRAME_TRUNCATED_PAYLOAD;
    }
    if (state->phase == TREV_H3_REQUEST_BEFORE_HEADERS) {
        return TREV_H3_FRAME_UNEXPECTED;
    }
    state->phase = TREV_H3_REQUEST_COMPLETE;
    return TREV_H3_FRAME_OK;
}

uint64_t trevrpc_h3_frame_status_error_code(trevrpc_h3_frame_status status) {
    switch (status) {
    case TREV_H3_FRAME_TRUNCATED_TYPE:
    case TREV_H3_FRAME_TYPE_WITHOUT_LENGTH:
    case TREV_H3_FRAME_TRUNCATED_LENGTH:
    case TREV_H3_FRAME_TRUNCATED_PAYLOAD:
        return TREV_H3_FRAME_ERROR_MALFORMED;
    case TREV_H3_FRAME_EXCESSIVE_LOAD:
    case TREV_H3_FRAME_FIELD_SECTION_TOO_LARGE:
        return TREV_H3_FRAME_ERROR_EXCESSIVE_LOAD;
    case TREV_H3_FRAME_UNEXPECTED:
        return TREV_H3_FRAME_ERROR_UNEXPECTED;
    case TREV_H3_FRAME_INVALID_ARGUMENT:
    case TREV_H3_FRAME_NEED_INPUT:
    case TREV_H3_FRAME_OK:
    case TREV_H3_FRAME_CLEAN_EOF:
    case TREV_H3_FRAME_PAYLOAD_ACTIVE:
    case TREV_H3_FRAME_PAYLOAD_OVERCONSUMED:
    case TREV_H3_FRAME_OUTPUT_TOO_SMALL:
        return 0;
    }
    return 0;
}
