#include "trevrpc_webtransport_capsule_internal.h"

#include "trevrpc_quic_varint_internal.h"

#include <limits.h>
#include <stdatomic.h>
#include <string.h>

static bool trevrpc_wt_range_valid(const void* range, size_t range_len) {
    if (range_len == 0) {
        return true;
    }
    uintptr_t address = (uintptr_t)range;
    return range_len - 1 <= UINTPTR_MAX - address;
}

static bool trevrpc_wt_ranges_overlap(const void* first, size_t first_len, const void* second, size_t second_len) {
    if (first_len == 0 || second_len == 0) {
        return false;
    }
    uintptr_t first_address = (uintptr_t)first;
    uintptr_t second_address = (uintptr_t)second;
    if (first_address <= second_address) {
        return second_address - first_address < first_len;
    }
    return first_address - second_address < second_len;
}

static trevrpc_wt_capsule_parse_result trevrpc_wt_capsule_fail(
    trevrpc_wt_capsule_parser* parser, trevrpc_wt_capsule_parse_result failure) {
    parser->state = TREV_WT_CAPSULE_PARSER_FAILED;
    parser->failure = failure;
    return failure;
}

static int trevrpc_wt_capsule_read_varint(
    trevrpc_wt_capsule_parser* parser, const uint8_t* data, size_t data_len, size_t* offset, uint64_t* value) {
    size_t consumed = 0;
    int result = trevrpc_quic_varint_feed(&parser->varint, data + *offset, data_len - *offset, &consumed, value);
    *offset += consumed;
    return result;
}

static bool trevrpc_wt_capsule_valid_utf8(const uint8_t* data, size_t len) {
    size_t offset = 0;
    while (offset < len) {
        uint8_t first = data[offset++];
        if (first <= 0x7f) {
            continue;
        }
        if (first >= 0xc2 && first <= 0xdf) {
            if (offset == len || data[offset] < 0x80 || data[offset] > 0xbf) {
                return false;
            }
            offset++;
            continue;
        }
        if (first >= 0xe0 && first <= 0xef) {
            if (len - offset < 2) {
                return false;
            }
            uint8_t second = data[offset];
            uint8_t third = data[offset + 1];
            if (third < 0x80 || third > 0xbf || (first == 0xe0 && (second < 0xa0 || second > 0xbf)) ||
                (first == 0xed && (second < 0x80 || second > 0x9f)) ||
                (first != 0xe0 && first != 0xed && (second < 0x80 || second > 0xbf))) {
                return false;
            }
            offset += 2;
            continue;
        }
        if (first >= 0xf0 && first <= 0xf4) {
            if (len - offset < 3) {
                return false;
            }
            uint8_t second = data[offset];
            uint8_t third = data[offset + 1];
            uint8_t fourth = data[offset + 2];
            if (third < 0x80 || third > 0xbf || fourth < 0x80 || fourth > 0xbf ||
                (first == 0xf0 && (second < 0x90 || second > 0xbf)) ||
                (first == 0xf4 && (second < 0x80 || second > 0x8f)) ||
                (first != 0xf0 && first != 0xf4 && (second < 0x80 || second > 0xbf))) {
                return false;
            }
            offset += 3;
            continue;
        }
        return false;
    }
    return true;
}

static bool trevrpc_wt_capsule_is_numeric(uint64_t capsule_type) {
    return capsule_type == TREV_WT_CAPSULE_MAX_DATA || capsule_type == TREV_WT_CAPSULE_MAX_STREAMS_BIDI ||
           capsule_type == TREV_WT_CAPSULE_MAX_STREAMS_UNI;
}

static bool trevrpc_wt_capsule_numeric_enabled(const trevrpc_wt_capsule_parser* parser) {
    if (parser->capsule_type == TREV_WT_CAPSULE_MAX_DATA) {
        return (parser->capabilities & TREV_WT_CAPSULE_CAP_MAX_DATA) != 0;
    }
    return (parser->capabilities & TREV_WT_CAPSULE_CAP_MAX_STREAMS) != 0;
}

static atomic_uint_fast64_t trevrpc_wt_flow_generation = ATOMIC_VAR_INIT(UINT64_C(1));

static bool trevrpc_wt_flow_next_generation(uint64_t* out_generation) {
    uint_fast64_t current = atomic_load_explicit(&trevrpc_wt_flow_generation, memory_order_relaxed);
    for (;;) {
        if (current == 0) {
            return false;
        }
        uint_fast64_t next = current == UINT64_MAX ? 0 : current + 1;
        if (atomic_compare_exchange_weak_explicit(
                &trevrpc_wt_flow_generation, &current, next, memory_order_relaxed, memory_order_relaxed)) {
            *out_generation = (uint64_t)current;
            return true;
        }
    }
}

static uint64_t trevrpc_wt_flow_integrity_mix(uint64_t value) {
    value ^= value >> 30;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value *= UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}

static uint64_t trevrpc_wt_flow_reservation_integrity(const trevrpc_wt_flow_reservation* reservation,
    const trevrpc_wt_flow_state* owner,
    trevrpc_wt_flow_reservation_kind kind,
    uint64_t amount,
    uint64_t original_amount,
    uint64_t owner_generation,
    uint64_t identity) {
    uint64_t integrity = UINT64_C(0xd6e8feb86659fd93);
    integrity = trevrpc_wt_flow_integrity_mix(integrity ^ (uint64_t)(uintptr_t)reservation);
    integrity = trevrpc_wt_flow_integrity_mix(integrity ^ (uint64_t)(uintptr_t)owner);
    integrity = trevrpc_wt_flow_integrity_mix(integrity ^ (uint64_t)kind);
    integrity = trevrpc_wt_flow_integrity_mix(integrity ^ amount);
    integrity = trevrpc_wt_flow_integrity_mix(integrity ^ original_amount);
    integrity = trevrpc_wt_flow_integrity_mix(integrity ^ owner_generation);
    return trevrpc_wt_flow_integrity_mix(integrity ^ identity);
}

static uint64_t trevrpc_wt_flow_reservation_link_integrity(const trevrpc_wt_flow_reservation* reservation,
    const trevrpc_wt_flow_state* owner,
    uint64_t owner_generation,
    uint64_t identity,
    const trevrpc_wt_flow_reservation* active_next,
    const trevrpc_wt_flow_reservation* active_prev) {
    uint64_t integrity = UINT64_C(0xa0761d6478bd642f);
    integrity = trevrpc_wt_flow_integrity_mix(integrity ^ (uint64_t)(uintptr_t)reservation);
    integrity = trevrpc_wt_flow_integrity_mix(integrity ^ (uint64_t)(uintptr_t)owner);
    integrity = trevrpc_wt_flow_integrity_mix(integrity ^ owner_generation);
    integrity = trevrpc_wt_flow_integrity_mix(integrity ^ identity);
    integrity = trevrpc_wt_flow_integrity_mix(integrity ^ (uint64_t)(uintptr_t)active_next);
    return trevrpc_wt_flow_integrity_mix(integrity ^ (uint64_t)(uintptr_t)active_prev);
}

static void trevrpc_wt_flow_reservation_refresh_integrity(trevrpc_wt_flow_reservation* reservation) {
    reservation->integrity = trevrpc_wt_flow_reservation_integrity(reservation,
        reservation->owner,
        reservation->kind,
        reservation->amount,
        reservation->original_amount,
        reservation->owner_generation,
        reservation->identity);
}

static void trevrpc_wt_flow_reservation_refresh_link_integrity(trevrpc_wt_flow_reservation* reservation) {
    reservation->link_integrity = trevrpc_wt_flow_reservation_link_integrity(reservation,
        reservation->owner,
        reservation->owner_generation,
        reservation->identity,
        reservation->active_next,
        reservation->active_prev);
}

static bool trevrpc_wt_capsule_reservation_empty(const trevrpc_wt_flow_reservation* reservation) {
    return reservation->owner == NULL && reservation->kind == TREV_WT_FLOW_RESERVATION_NONE &&
           reservation->amount == 0 && reservation->original_amount == 0 && reservation->owner_generation == 0 &&
           reservation->identity == 0 && reservation->active_next == NULL && reservation->active_prev == NULL &&
           reservation->integrity == 0 && reservation->link_integrity == 0;
}

static bool trevrpc_wt_flow_reservation_fields_valid(
    const trevrpc_wt_flow_state* state, const trevrpc_wt_flow_reservation* reservation) {
    if (reservation == NULL || reservation->owner != state || reservation->kind == TREV_WT_FLOW_RESERVATION_NONE ||
        reservation->kind > TREV_WT_FLOW_RESERVATION_STREAM_UNI ||
        reservation->amount != reservation->original_amount || reservation->identity == 0 ||
        reservation->owner_generation != state->generation ||
        reservation->integrity != trevrpc_wt_flow_reservation_integrity(reservation,
                                      reservation->owner,
                                      reservation->kind,
                                      reservation->amount,
                                      reservation->original_amount,
                                      reservation->owner_generation,
                                      reservation->identity)) {
        return false;
    }
    return (reservation->kind != TREV_WT_FLOW_RESERVATION_STREAM_BIDI &&
               reservation->kind != TREV_WT_FLOW_RESERVATION_STREAM_UNI) ||
           reservation->original_amount == 1;
}

static bool trevrpc_wt_flow_reservation_links_valid(const trevrpc_wt_flow_reservation* reservation) {
    return reservation->link_integrity == trevrpc_wt_flow_reservation_link_integrity(reservation,
                                              reservation->owner,
                                              reservation->owner_generation,
                                              reservation->identity,
                                              reservation->active_next,
                                              reservation->active_prev);
}

static bool trevrpc_wt_flow_active_head_valid(const trevrpc_wt_flow_state* state) {
    return state->active_reservations == NULL ||
           (trevrpc_wt_flow_reservation_fields_valid(state, state->active_reservations) &&
               trevrpc_wt_flow_reservation_links_valid(state->active_reservations) &&
               state->active_reservations->active_prev == NULL);
}

static void trevrpc_wt_capsule_reservation_set(trevrpc_wt_flow_reservation* reservation,
    trevrpc_wt_flow_state* owner,
    trevrpc_wt_flow_reservation_kind kind,
    uint64_t amount) {
    uint64_t identity = owner->next_reservation_identity;
    owner->next_reservation_identity = identity == UINT64_MAX ? 0 : identity + 1;

    trevrpc_wt_flow_reservation* previous_head = owner->active_reservations;
    reservation->owner = owner;
    reservation->kind = kind;
    reservation->amount = amount;
    reservation->original_amount = amount;
    reservation->owner_generation = owner->generation;
    reservation->identity = identity;
    reservation->active_next = previous_head;
    reservation->active_prev = NULL;
    trevrpc_wt_flow_reservation_refresh_integrity(reservation);
    trevrpc_wt_flow_reservation_refresh_link_integrity(reservation);
    owner->active_reservations = reservation;

    if (previous_head != NULL) {
        previous_head->active_prev = reservation;
        trevrpc_wt_flow_reservation_refresh_link_integrity(previous_head);
    }
}

static void trevrpc_wt_capsule_reservation_clear(trevrpc_wt_flow_reservation* reservation) {
    reservation->owner = NULL;
    reservation->kind = TREV_WT_FLOW_RESERVATION_NONE;
    reservation->amount = 0;
    reservation->original_amount = 0;
    reservation->owner_generation = 0;
    reservation->identity = 0;
    reservation->active_next = NULL;
    reservation->active_prev = NULL;
    reservation->integrity = 0;
    reservation->link_integrity = 0;
}

trevrpc_wt_capsule_parse_result trevrpc_wt_capsule_parser_init(
    trevrpc_wt_capsule_parser* parser, const trevrpc_wt_capsule_parser_config* config) {
    if (parser == NULL || config == NULL || config->close_reason_workspace == NULL ||
        config->close_reason_capacity < TREV_WT_CAPSULE_CLOSE_REASON_MAX ||
        !trevrpc_wt_range_valid(config->close_reason_workspace, config->close_reason_capacity) ||
        trevrpc_wt_ranges_overlap(
            parser, sizeof(*parser), config->close_reason_workspace, config->close_reason_capacity) ||
        (config->compatibility_flags & ~TREV_WT_PROFILE_COMPAT_INITIAL_CAPSULE_FLOW_CONTROL) != 0 ||
        (config->compatibility_flags != 0 && config->profile != TREV_WT_PROFILE_DRAFT_07)) {
        return TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT;
    }

    uint32_t capabilities = 0;
    bool recognizes_disabled_flow_control = false;
    switch (config->profile) {
    case TREV_WT_PROFILE_DRAFT_02:
        capabilities = TREV_WT_CAPSULE_CAP_CLOSE;
        break;
    case TREV_WT_PROFILE_DRAFT_07:
        capabilities = TREV_WT_CAPSULE_CAP_CLOSE | TREV_WT_CAPSULE_CAP_DRAIN;
        if ((config->compatibility_flags & TREV_WT_PROFILE_COMPAT_INITIAL_CAPSULE_FLOW_CONTROL) != 0) {
            capabilities |= TREV_WT_CAPSULE_CAP_MAX_DATA | TREV_WT_CAPSULE_CAP_MAX_STREAMS;
        }
        break;
    case TREV_WT_PROFILE_DRAFT_14:
    case TREV_WT_PROFILE_DRAFT_15:
        capabilities = TREV_WT_CAPSULE_CAP_CLOSE | TREV_WT_CAPSULE_CAP_DRAIN;
        recognizes_disabled_flow_control = true;
        if (config->modern_flow_control_enabled) {
            capabilities |= TREV_WT_CAPSULE_CAP_MAX_DATA | TREV_WT_CAPSULE_CAP_MAX_STREAMS;
        }
        break;
    default:
        return TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT;
    }

    trevrpc_wt_capsule_parser initialized = {0};
    initialized.close_reason_workspace = config->close_reason_workspace;
    initialized.close_reason_capacity = config->close_reason_capacity;
    initialized.max_unknown_capsule_payload = config->max_unknown_capsule_payload;
    initialized.capabilities = capabilities;
    initialized.recognizes_disabled_flow_control = recognizes_disabled_flow_control;
    initialized.state = TREV_WT_CAPSULE_PARSER_TYPE;
    initialized.failure = TREV_WT_CAPSULE_PARSE_NEED_INPUT;
    *parser = initialized;
    return TREV_WT_CAPSULE_PARSE_NEED_INPUT;
}

trevrpc_wt_capsule_parse_result trevrpc_wt_capsule_parser_feed(trevrpc_wt_capsule_parser* parser,
    const uint8_t* data,
    size_t data_len,
    size_t* out_consumed,
    trevrpc_wt_capsule_event* out_event) {
    if (parser == NULL || out_consumed == NULL || out_event == NULL ||
        !trevrpc_wt_range_valid(out_consumed, sizeof(*out_consumed)) ||
        !trevrpc_wt_range_valid(out_event, sizeof(*out_event)) || (data == NULL && data_len != 0) ||
        (data_len != 0 && !trevrpc_wt_range_valid(data, data_len))) {
        return TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT;
    }
    if (trevrpc_wt_ranges_overlap(parser, sizeof(*parser), out_consumed, sizeof(*out_consumed)) ||
        trevrpc_wt_ranges_overlap(parser, sizeof(*parser), out_event, sizeof(*out_event)) ||
        trevrpc_wt_ranges_overlap(out_consumed, sizeof(*out_consumed), out_event, sizeof(*out_event)) ||
        trevrpc_wt_ranges_overlap(
            parser->close_reason_workspace, TREV_WT_CAPSULE_CLOSE_REASON_MAX, out_consumed, sizeof(*out_consumed)) ||
        trevrpc_wt_ranges_overlap(
            parser->close_reason_workspace, TREV_WT_CAPSULE_CLOSE_REASON_MAX, out_event, sizeof(*out_event)) ||
        (data_len != 0 && (trevrpc_wt_ranges_overlap(parser, sizeof(*parser), data, data_len) ||
                              trevrpc_wt_ranges_overlap(data, data_len, out_consumed, sizeof(*out_consumed)) ||
                              trevrpc_wt_ranges_overlap(data, data_len, out_event, sizeof(*out_event))))) {
        return TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT;
    }

    bool workspace_alias =
        trevrpc_wt_ranges_overlap(parser->close_reason_workspace, TREV_WT_CAPSULE_CLOSE_REASON_MAX, data, data_len);
    trevrpc_wt_capsule_parser parser_before_feed;
    size_t consumed_before_feed = 0;
    if (workspace_alias) {
        memcpy(&parser_before_feed, parser, sizeof(parser_before_feed));
        memcpy(&consumed_before_feed, out_consumed, sizeof(consumed_before_feed));
    }

    *out_consumed = 0;
    if (parser->state == TREV_WT_CAPSULE_PARSER_FAILED) {
        return parser->failure;
    }
    if (parser->state == TREV_WT_CAPSULE_PARSER_CLOSED) {
        if (data_len == 0) {
            return TREV_WT_CAPSULE_PARSE_NEED_INPUT;
        }
        return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_POST_CLOSE_MESSAGE_ERROR);
    }

    size_t offset = 0;
    while (offset < data_len) {
        if (parser->state == TREV_WT_CAPSULE_PARSER_TYPE) {
            uint64_t value = 0;
            int varint_result = trevrpc_wt_capsule_read_varint(parser, data, data_len, &offset, &value);
            if (varint_result < 0) {
                *out_consumed = offset;
                return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
            }
            if (varint_result == 0) {
                break;
            }
            parser->capsule_type = value;
            parser->state = TREV_WT_CAPSULE_PARSER_LENGTH;
            continue;
        }

        if (parser->state == TREV_WT_CAPSULE_PARSER_LENGTH) {
            uint64_t value = 0;
            int varint_result = trevrpc_wt_capsule_read_varint(parser, data, data_len, &offset, &value);
            if (varint_result < 0) {
                *out_consumed = offset;
                return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
            }
            if (varint_result == 0) {
                break;
            }
            parser->capsule_length = value;
            parser->payload_remaining = value;

            if (parser->capsule_type == TREV_WT_CAPSULE_CLOSE_SESSION &&
                (parser->capabilities & TREV_WT_CAPSULE_CAP_CLOSE) != 0) {
                if (value < 4 || value > 4 + TREV_WT_CAPSULE_CLOSE_REASON_MAX) {
                    *out_consumed = offset;
                    return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
                }
                parser->close_code_have = 0;
                parser->close_reason_len = 0;
                parser->state = TREV_WT_CAPSULE_PARSER_CLOSE_PAYLOAD;
                continue;
            }

            if (parser->capsule_type == TREV_WT_CAPSULE_DRAIN_SESSION &&
                (parser->capabilities & TREV_WT_CAPSULE_CAP_DRAIN) != 0) {
                if (value != 0) {
                    *out_consumed = offset;
                    return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
                }
                parser->state = TREV_WT_CAPSULE_PARSER_TYPE;
                trevrpc_wt_capsule_event event = {.type = TREV_WT_CAPSULE_EVENT_DRAIN};
                *out_event = event;
                *out_consumed = offset;
                return TREV_WT_CAPSULE_PARSE_EVENT;
            }

            if (trevrpc_wt_capsule_is_numeric(parser->capsule_type) &&
                (trevrpc_wt_capsule_numeric_enabled(parser) || parser->recognizes_disabled_flow_control)) {
                if (value != 1 && value != 2 && value != 4 && value != 8) {
                    *out_consumed = offset;
                    return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
                }
                trevrpc_quic_varint_reset(&parser->varint);
                parser->state = TREV_WT_CAPSULE_PARSER_NUMERIC_PAYLOAD;
                continue;
            }

            if (value > parser->max_unknown_capsule_payload) {
                *out_consumed = offset;
                return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_EXCESSIVE_LOAD);
            }
            if (value == 0) {
                parser->state = TREV_WT_CAPSULE_PARSER_TYPE;
                continue;
            }
            parser->state = TREV_WT_CAPSULE_PARSER_UNKNOWN_PAYLOAD;
            continue;
        }

        if (parser->state == TREV_WT_CAPSULE_PARSER_CLOSE_PAYLOAD) {
            while (parser->close_code_have < sizeof(parser->close_code_bytes) && offset < data_len) {
                parser->close_code_bytes[parser->close_code_have++] = data[offset++];
                parser->payload_remaining--;
            }
            if (parser->close_code_have == sizeof(parser->close_code_bytes) && offset < data_len &&
                parser->payload_remaining != 0) {
                size_t available = data_len - offset;
                size_t take = available;
                if (parser->payload_remaining <= SIZE_MAX && (size_t)parser->payload_remaining < take) {
                    take = (size_t)parser->payload_remaining;
                }
                memmove(parser->close_reason_staging + parser->close_reason_len, data + offset, take);
                parser->close_reason_len += take;
                parser->payload_remaining -= take;
                offset += take;
            }
            if (parser->payload_remaining != 0) {
                break;
            }
            if (!trevrpc_wt_capsule_valid_utf8(parser->close_reason_staging, parser->close_reason_len)) {
                *out_consumed = offset;
                return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
            }
            if (workspace_alias &&
                trevrpc_wt_ranges_overlap(
                    parser->close_reason_workspace, parser->close_reason_len, data + offset, data_len - offset)) {
                memcpy(parser, &parser_before_feed, sizeof(*parser));
                memcpy(out_consumed, &consumed_before_feed, sizeof(*out_consumed));
                return TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT;
            }

            uint32_t code = ((uint32_t)parser->close_code_bytes[0] << 24) |
                            ((uint32_t)parser->close_code_bytes[1] << 16) |
                            ((uint32_t)parser->close_code_bytes[2] << 8) | (uint32_t)parser->close_code_bytes[3];
            memmove(parser->close_reason_workspace, parser->close_reason_staging, parser->close_reason_len);
            trevrpc_wt_capsule_event event = {
                .type = TREV_WT_CAPSULE_EVENT_CLOSE,
                .value.close =
                    {
                        .code = code,
                        .reason = parser->close_reason_workspace,
                        .reason_len = parser->close_reason_len,
                        .implicit = false,
                    },
            };
            parser->state = TREV_WT_CAPSULE_PARSER_CLOSED;
            *out_event = event;
            *out_consumed = offset;
            return TREV_WT_CAPSULE_PARSE_EVENT;
        }

        if (parser->state == TREV_WT_CAPSULE_PARSER_NUMERIC_PAYLOAD) {
            uint64_t maximum = 0;
            int varint_result = 0;
            if (parser->varint.have == 0 && offset < data_len) {
                size_t consumed = 0;
                varint_result = trevrpc_quic_varint_feed(&parser->varint, data + offset, 1, &consumed, &maximum);
                parser->payload_remaining -= consumed;
                offset += consumed;
                if (varint_result < 0) {
                    *out_consumed = offset;
                    return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
                }
                if (varint_result == 1) {
                    if ((uint64_t)consumed != parser->capsule_length) {
                        *out_consumed = offset;
                        return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
                    }
                } else if ((uint64_t)parser->varint.need != parser->capsule_length) {
                    *out_consumed = offset;
                    return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
                }
            }
            if (varint_result != 1) {
                if (parser->payload_remaining == 0) {
                    *out_consumed = offset;
                    return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
                }
                size_t available = data_len - offset;
                size_t take = available;
                if (parser->payload_remaining <= SIZE_MAX && (size_t)parser->payload_remaining < take) {
                    take = (size_t)parser->payload_remaining;
                }
                size_t consumed = 0;
                varint_result = trevrpc_quic_varint_feed(&parser->varint, data + offset, take, &consumed, &maximum);
                parser->payload_remaining -= consumed;
                offset += consumed;
                if (varint_result < 0) {
                    *out_consumed = offset;
                    return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
                }
            }
            if (parser->payload_remaining != 0) {
                break;
            }
            if (varint_result != 1) {
                *out_consumed = offset;
                return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
            }

            trevrpc_wt_capsule_event event = {0};
            if (parser->capsule_type == TREV_WT_CAPSULE_MAX_DATA) {
                event.type = TREV_WT_CAPSULE_EVENT_MAX_DATA;
            } else if (parser->capsule_type == TREV_WT_CAPSULE_MAX_STREAMS_BIDI) {
                if (maximum > TREV_WT_FLOW_MAX_STREAMS) {
                    *out_consumed = offset;
                    return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_H3_DATAGRAM_ERROR);
                }
                event.type = TREV_WT_CAPSULE_EVENT_MAX_STREAMS_BIDI;
            } else {
                if (maximum > TREV_WT_FLOW_MAX_STREAMS) {
                    *out_consumed = offset;
                    return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_H3_DATAGRAM_ERROR);
                }
                event.type = TREV_WT_CAPSULE_EVENT_MAX_STREAMS_UNI;
            }
            event.value.max.maximum = maximum;
            parser->state = TREV_WT_CAPSULE_PARSER_TYPE;
            if (!trevrpc_wt_capsule_numeric_enabled(parser)) {
                continue;
            }
            *out_event = event;
            *out_consumed = offset;
            return TREV_WT_CAPSULE_PARSE_EVENT;
        }

        if (parser->state == TREV_WT_CAPSULE_PARSER_UNKNOWN_PAYLOAD) {
            size_t available = data_len - offset;
            size_t take = available;
            if (parser->payload_remaining <= SIZE_MAX && (size_t)parser->payload_remaining < take) {
                take = (size_t)parser->payload_remaining;
            }
            parser->payload_remaining -= take;
            offset += take;
            if (parser->payload_remaining == 0) {
                parser->state = TREV_WT_CAPSULE_PARSER_TYPE;
                continue;
            }
            break;
        }
    }

    *out_consumed = offset;
    return TREV_WT_CAPSULE_PARSE_NEED_INPUT;
}

trevrpc_wt_capsule_parse_result trevrpc_wt_capsule_parser_finish(
    trevrpc_wt_capsule_parser* parser, trevrpc_wt_capsule_event* out_event) {
    if (parser == NULL || out_event == NULL || !trevrpc_wt_range_valid(out_event, sizeof(*out_event)) ||
        trevrpc_wt_ranges_overlap(parser, sizeof(*parser), out_event, sizeof(*out_event)) ||
        trevrpc_wt_ranges_overlap(
            parser->close_reason_workspace, TREV_WT_CAPSULE_CLOSE_REASON_MAX, out_event, sizeof(*out_event))) {
        return TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT;
    }
    if (parser->state == TREV_WT_CAPSULE_PARSER_FAILED) {
        return parser->failure;
    }
    if (parser->state == TREV_WT_CAPSULE_PARSER_CLOSED) {
        return TREV_WT_CAPSULE_PARSE_COMPLETE;
    }
    if (parser->state != TREV_WT_CAPSULE_PARSER_TYPE || parser->varint.have != 0) {
        return trevrpc_wt_capsule_fail(parser, TREV_WT_CAPSULE_PARSE_MALFORMED_MESSAGE);
    }

    trevrpc_wt_capsule_event event = {
        .type = TREV_WT_CAPSULE_EVENT_CLOSE,
        .value.close =
            {
                .code = 0,
                .reason = NULL,
                .reason_len = 0,
                .implicit = true,
            },
    };
    parser->state = TREV_WT_CAPSULE_PARSER_CLOSED;
    *out_event = event;
    return TREV_WT_CAPSULE_PARSE_EVENT;
}

trevrpc_wt_flow_result trevrpc_wt_flow_state_init(trevrpc_wt_flow_state* state,
    const trevrpc_wt_flow_limits* local_initial,
    const trevrpc_wt_flow_limits* peer_initial) {
    if (state == NULL || local_initial == NULL || peer_initial == NULL) {
        return TREV_WT_FLOW_INVALID_ARGUMENT;
    }
    if (local_initial->max_data > TREV_WT_FLOW_MAX_DATA || local_initial->max_streams_bidi > TREV_WT_FLOW_MAX_STREAMS ||
        local_initial->max_streams_uni > TREV_WT_FLOW_MAX_STREAMS || peer_initial->max_data > TREV_WT_FLOW_MAX_DATA) {
        return TREV_WT_FLOW_INVALID_ARGUMENT;
    }
    if (peer_initial->max_streams_bidi > TREV_WT_FLOW_MAX_STREAMS ||
        peer_initial->max_streams_uni > TREV_WT_FLOW_MAX_STREAMS) {
        return TREV_WT_FLOW_H3_DATAGRAM_ERROR;
    }

    uint64_t generation = 0;
    if (!trevrpc_wt_flow_next_generation(&generation)) {
        return TREV_WT_FLOW_INVALID_STATE;
    }

    bool local_intent =
        local_initial->max_data != 0 || local_initial->max_streams_bidi != 0 || local_initial->max_streams_uni != 0;
    bool peer_intent =
        peer_initial->max_data != 0 || peer_initial->max_streams_bidi != 0 || peer_initial->max_streams_uni != 0;
    trevrpc_wt_flow_state initialized = {0};
    initialized.enabled = local_intent && peer_intent;
    initialized.peer_max = *peer_initial;
    initialized.generation = generation;
    initialized.next_reservation_identity = UINT64_C(1);
    *state = initialized;
    return TREV_WT_FLOW_OK;
}

trevrpc_wt_flow_result trevrpc_wt_flow_apply_max_data(trevrpc_wt_flow_state* state, uint64_t maximum) {
    if (state == NULL || maximum > TREV_WT_FLOW_MAX_DATA) {
        return TREV_WT_FLOW_INVALID_ARGUMENT;
    }
    if (!state->enabled) {
        return TREV_WT_FLOW_IGNORED;
    }
    if (maximum < state->peer_max.max_data) {
        return TREV_WT_FLOW_CONTROL_VIOLATION;
    }
    state->peer_max.max_data = maximum;
    return TREV_WT_FLOW_OK;
}

trevrpc_wt_flow_result trevrpc_wt_flow_apply_max_streams(
    trevrpc_wt_flow_state* state, trevrpc_wt_stream_direction direction, uint64_t maximum) {
    if (state == NULL || (direction != TREV_WT_STREAM_BIDI && direction != TREV_WT_STREAM_UNI)) {
        return TREV_WT_FLOW_INVALID_ARGUMENT;
    }
    if (maximum > TREV_WT_FLOW_MAX_STREAMS) {
        return TREV_WT_FLOW_H3_DATAGRAM_ERROR;
    }
    if (!state->enabled) {
        return TREV_WT_FLOW_IGNORED;
    }

    uint64_t* current =
        direction == TREV_WT_STREAM_BIDI ? &state->peer_max.max_streams_bidi : &state->peer_max.max_streams_uni;
    if (maximum < *current) {
        return TREV_WT_FLOW_CONTROL_VIOLATION;
    }
    *current = maximum;
    return TREV_WT_FLOW_OK;
}

trevrpc_wt_flow_result trevrpc_wt_flow_reserve_data(
    trevrpc_wt_flow_state* state, uint64_t amount, trevrpc_wt_flow_reservation* reservation) {
    if (state == NULL || reservation == NULL) {
        return TREV_WT_FLOW_INVALID_ARGUMENT;
    }
    if (!trevrpc_wt_capsule_reservation_empty(reservation) || state->next_reservation_identity == 0 ||
        !trevrpc_wt_flow_active_head_valid(state)) {
        return TREV_WT_FLOW_INVALID_STATE;
    }
    if (!state->enabled) {
        trevrpc_wt_capsule_reservation_set(reservation, state, TREV_WT_FLOW_RESERVATION_BYPASS, amount);
        return TREV_WT_FLOW_OK;
    }
    if (state->admitted_data > state->peer_max.max_data || amount > state->peer_max.max_data - state->admitted_data) {
        return TREV_WT_FLOW_BLOCKED;
    }

    state->admitted_data += amount;
    trevrpc_wt_capsule_reservation_set(reservation, state, TREV_WT_FLOW_RESERVATION_DATA, amount);
    return TREV_WT_FLOW_OK;
}

trevrpc_wt_flow_result trevrpc_wt_flow_reserve_stream(
    trevrpc_wt_flow_state* state, trevrpc_wt_stream_direction direction, trevrpc_wt_flow_reservation* reservation) {
    if (state == NULL || reservation == NULL || (direction != TREV_WT_STREAM_BIDI && direction != TREV_WT_STREAM_UNI)) {
        return TREV_WT_FLOW_INVALID_ARGUMENT;
    }
    if (!trevrpc_wt_capsule_reservation_empty(reservation) || state->next_reservation_identity == 0 ||
        !trevrpc_wt_flow_active_head_valid(state)) {
        return TREV_WT_FLOW_INVALID_STATE;
    }
    if (!state->enabled) {
        trevrpc_wt_capsule_reservation_set(reservation, state, TREV_WT_FLOW_RESERVATION_BYPASS, 1);
        return TREV_WT_FLOW_OK;
    }

    uint64_t* admitted =
        direction == TREV_WT_STREAM_BIDI ? &state->admitted_streams_bidi : &state->admitted_streams_uni;
    uint64_t maximum =
        direction == TREV_WT_STREAM_BIDI ? state->peer_max.max_streams_bidi : state->peer_max.max_streams_uni;
    if (*admitted > maximum || UINT64_C(1) > maximum - *admitted) {
        return TREV_WT_FLOW_BLOCKED;
    }

    (*admitted)++;
    trevrpc_wt_capsule_reservation_set(reservation,
        state,
        direction == TREV_WT_STREAM_BIDI ? TREV_WT_FLOW_RESERVATION_STREAM_BIDI : TREV_WT_FLOW_RESERVATION_STREAM_UNI,
        1);
    return TREV_WT_FLOW_OK;
}

static trevrpc_wt_flow_result trevrpc_wt_flow_validate_live_reservation(
    trevrpc_wt_flow_state* state, const trevrpc_wt_flow_reservation* reservation) {
    if (!trevrpc_wt_flow_reservation_fields_valid(state, reservation) ||
        !trevrpc_wt_flow_reservation_links_valid(reservation)) {
        return TREV_WT_FLOW_INVALID_STATE;
    }

    const trevrpc_wt_flow_reservation* previous = reservation->active_prev;
    const trevrpc_wt_flow_reservation* next = reservation->active_next;
    if ((previous == NULL && state->active_reservations != reservation) ||
        (previous != NULL &&
            (!trevrpc_wt_flow_reservation_fields_valid(state, previous) ||
                !trevrpc_wt_flow_reservation_links_valid(previous) || previous->active_next != reservation)) ||
        (next != NULL && (!trevrpc_wt_flow_reservation_fields_valid(state, next) ||
                             !trevrpc_wt_flow_reservation_links_valid(next) || next->active_prev != reservation))) {
        return TREV_WT_FLOW_INVALID_STATE;
    }
    return TREV_WT_FLOW_OK;
}

static void trevrpc_wt_flow_retire_reservation(trevrpc_wt_flow_state* state, trevrpc_wt_flow_reservation* reservation) {
    trevrpc_wt_flow_reservation* previous = reservation->active_prev;
    trevrpc_wt_flow_reservation* next = reservation->active_next;
    if (previous == NULL) {
        state->active_reservations = next;
    } else {
        previous->active_next = next;
        trevrpc_wt_flow_reservation_refresh_link_integrity(previous);
    }
    if (next != NULL) {
        next->active_prev = previous;
        trevrpc_wt_flow_reservation_refresh_link_integrity(next);
    }
    trevrpc_wt_capsule_reservation_clear(reservation);
}

trevrpc_wt_flow_result trevrpc_wt_flow_commit(trevrpc_wt_flow_state* state, trevrpc_wt_flow_reservation* reservation) {
    if (state == NULL || reservation == NULL) {
        return TREV_WT_FLOW_INVALID_ARGUMENT;
    }
    trevrpc_wt_flow_result valid = trevrpc_wt_flow_validate_live_reservation(state, reservation);
    if (valid != TREV_WT_FLOW_OK) {
        return valid;
    }
    if (reservation->kind == TREV_WT_FLOW_RESERVATION_BYPASS) {
        trevrpc_wt_flow_retire_reservation(state, reservation);
        return TREV_WT_FLOW_OK;
    }

    uint64_t* admitted = NULL;
    uint64_t* committed = NULL;
    if (reservation->kind == TREV_WT_FLOW_RESERVATION_DATA) {
        admitted = &state->admitted_data;
        committed = &state->committed_data;
    } else if (reservation->kind == TREV_WT_FLOW_RESERVATION_STREAM_BIDI) {
        admitted = &state->admitted_streams_bidi;
        committed = &state->committed_streams_bidi;
    } else {
        admitted = &state->admitted_streams_uni;
        committed = &state->committed_streams_uni;
    }
    if (*committed > *admitted || reservation->original_amount > *admitted - *committed) {
        return TREV_WT_FLOW_INVALID_STATE;
    }

    *committed += reservation->original_amount;
    trevrpc_wt_flow_retire_reservation(state, reservation);
    return TREV_WT_FLOW_OK;
}

trevrpc_wt_flow_result trevrpc_wt_flow_cancel(trevrpc_wt_flow_state* state, trevrpc_wt_flow_reservation* reservation) {
    if (state == NULL || reservation == NULL) {
        return TREV_WT_FLOW_INVALID_ARGUMENT;
    }
    trevrpc_wt_flow_result valid = trevrpc_wt_flow_validate_live_reservation(state, reservation);
    if (valid != TREV_WT_FLOW_OK) {
        return valid;
    }
    if (reservation->kind == TREV_WT_FLOW_RESERVATION_BYPASS) {
        trevrpc_wt_flow_retire_reservation(state, reservation);
        return TREV_WT_FLOW_OK;
    }

    uint64_t* admitted = NULL;
    uint64_t committed = 0;
    if (reservation->kind == TREV_WT_FLOW_RESERVATION_DATA) {
        admitted = &state->admitted_data;
        committed = state->committed_data;
    } else if (reservation->kind == TREV_WT_FLOW_RESERVATION_STREAM_BIDI) {
        admitted = &state->admitted_streams_bidi;
        committed = state->committed_streams_bidi;
    } else {
        admitted = &state->admitted_streams_uni;
        committed = state->committed_streams_uni;
    }
    if (committed > *admitted || reservation->original_amount > *admitted - committed) {
        return TREV_WT_FLOW_INVALID_STATE;
    }

    *admitted -= reservation->original_amount;
    trevrpc_wt_flow_retire_reservation(state, reservation);
    return TREV_WT_FLOW_OK;
}
