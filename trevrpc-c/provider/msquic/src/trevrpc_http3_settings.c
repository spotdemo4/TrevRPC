#include "trevrpc_http3_settings_internal.h"

#include "trevrpc_quic_varint_internal.h"

static int trevrpc_h3_settings_id_reserved(uint64_t id) {
    return id == 0x00 || (id >= 0x02 && id <= 0x05);
}

static int trevrpc_h3_settings_id_seen(const uint64_t* seen_ids, size_t seen_ids_len, uint64_t id) {
    for (size_t i = 0; i < seen_ids_len; i++) {
        if (seen_ids[i] == id) {
            return 1;
        }
    }
    return 0;
}

trevrpc_h3_settings_status trevrpc_h3_settings_parse(const uint8_t* payload,
    size_t payload_len,
    size_t max_payload_len,
    uint64_t* seen_ids,
    size_t seen_ids_capacity,
    trevrpc_h3_settings_visitor visitor,
    void* visitor_context,
    trevrpc_h3_settings_report* out_report) {
    if ((payload == NULL && payload_len != 0) || (seen_ids == NULL && seen_ids_capacity != 0) || out_report == NULL) {
        return TREV_H3_SETTINGS_INVALID_ARGUMENT;
    }
    if (payload_len > max_payload_len) {
        return TREV_H3_SETTINGS_EXCESSIVE_LOAD;
    }

    size_t offset = 0;
    size_t pair_count = 0;
    while (offset < payload_len) {
        uint64_t id = 0;
        uint64_t value = 0;
        if (trevrpc_quic_varint_read(payload, payload_len, &offset, &id) != 0 ||
            trevrpc_quic_varint_read(payload, payload_len, &offset, &value) != 0) {
            return TREV_H3_SETTINGS_MALFORMED;
        }
        if (pair_count == seen_ids_capacity) {
            return TREV_H3_SETTINGS_WORKSPACE_EXHAUSTED;
        }
        if (trevrpc_h3_settings_id_seen(seen_ids, pair_count, id)) {
            return TREV_H3_SETTINGS_DUPLICATE;
        }
        seen_ids[pair_count++] = id;
        if (trevrpc_h3_settings_id_reserved(id)) {
            return TREV_H3_SETTINGS_RESERVED;
        }
    }

    if (visitor != NULL) {
        offset = 0;
        while (offset < payload_len) {
            uint64_t id = 0;
            uint64_t value = 0;
            if (trevrpc_quic_varint_read(payload, payload_len, &offset, &id) != 0 ||
                trevrpc_quic_varint_read(payload, payload_len, &offset, &value) != 0) {
                return TREV_H3_SETTINGS_MALFORMED;
            }
            if (visitor(visitor_context, id, value) != 0) {
                return TREV_H3_SETTINGS_INVALID_VALUE;
            }
        }
    }

    *out_report = (trevrpc_h3_settings_report){
        .payload_len = payload_len,
        .pair_count = pair_count,
    };
    return TREV_H3_SETTINGS_OK;
}

trevrpc_h3_settings_status trevrpc_h3_settings_build(const trevrpc_h3_settings_pair* pairs,
    size_t pair_count,
    uint8_t* out,
    size_t out_capacity,
    size_t* out_payload_len) {
    if ((pairs == NULL && pair_count != 0) || (out == NULL && out_capacity != 0) || out_payload_len == NULL) {
        return TREV_H3_SETTINGS_INVALID_ARGUMENT;
    }

    size_t required = 0;
    for (size_t i = 0; i < pair_count; i++) {
        size_t id_size = 0;
        size_t value_size = 0;
        if (trevrpc_quic_varint_size(pairs[i].id, &id_size) != 0 ||
            trevrpc_quic_varint_size(pairs[i].value, &value_size) != 0) {
            return TREV_H3_SETTINGS_OUT_OF_RANGE;
        }
        if (trevrpc_h3_settings_id_reserved(pairs[i].id)) {
            return TREV_H3_SETTINGS_RESERVED;
        }
        for (size_t j = 0; j < i; j++) {
            if (pairs[j].id == pairs[i].id) {
                return TREV_H3_SETTINGS_DUPLICATE;
            }
        }
        if (required > SIZE_MAX - id_size || required + id_size > SIZE_MAX - value_size) {
            return TREV_H3_SETTINGS_SIZE_OVERFLOW;
        }
        required += id_size + value_size;
    }
    if (out_capacity < required) {
        return TREV_H3_SETTINGS_OUTPUT_TOO_SMALL;
    }

    size_t offset = 0;
    uint64_t previous_id = 0;
    int have_previous_id = 0;
    for (size_t emitted = 0; emitted < pair_count; emitted++) {
        size_t selected = SIZE_MAX;
        for (size_t i = 0; i < pair_count; i++) {
            if ((!have_previous_id || pairs[i].id > previous_id) &&
                (selected == SIZE_MAX || pairs[i].id < pairs[selected].id)) {
                selected = i;
            }
        }
        if (selected == SIZE_MAX) {
            return TREV_H3_SETTINGS_INVALID_ARGUMENT;
        }

        size_t written = 0;
        if (trevrpc_quic_varint_write(out + offset, out_capacity - offset, pairs[selected].id, &written) != 0) {
            return TREV_H3_SETTINGS_INVALID_ARGUMENT;
        }
        offset += written;
        if (trevrpc_quic_varint_write(out + offset, out_capacity - offset, pairs[selected].value, &written) != 0) {
            return TREV_H3_SETTINGS_INVALID_ARGUMENT;
        }
        offset += written;
        previous_id = pairs[selected].id;
        have_previous_id = 1;
    }

    *out_payload_len = offset;
    return TREV_H3_SETTINGS_OK;
}

uint64_t trevrpc_h3_settings_status_error_code(trevrpc_h3_settings_status status) {
    switch (status) {
    case TREV_H3_SETTINGS_MALFORMED:
        return TREV_H3_ERROR_FRAME_ERROR;
    case TREV_H3_SETTINGS_DUPLICATE:
    case TREV_H3_SETTINGS_RESERVED:
    case TREV_H3_SETTINGS_INVALID_VALUE:
        return TREV_H3_ERROR_SETTINGS_ERROR;
    case TREV_H3_SETTINGS_EXCESSIVE_LOAD:
    case TREV_H3_SETTINGS_WORKSPACE_EXHAUSTED:
        return TREV_H3_ERROR_EXCESSIVE_LOAD;
    case TREV_H3_SETTINGS_OK:
    case TREV_H3_SETTINGS_INVALID_ARGUMENT:
    case TREV_H3_SETTINGS_OUT_OF_RANGE:
    case TREV_H3_SETTINGS_OUTPUT_TOO_SMALL:
    case TREV_H3_SETTINGS_SIZE_OVERFLOW:
        return 0;
    }
    return 0;
}
