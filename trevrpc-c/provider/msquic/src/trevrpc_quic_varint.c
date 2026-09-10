#include "trevrpc_quic_varint_internal.h"

#include <errno.h>
#include <string.h>

void trevrpc_quic_varint_reset(trevrpc_quic_varint_feeder* state) {
    if (state == NULL) {
        return;
    }
    memset(state, 0, sizeof(*state));
}

int trevrpc_quic_varint_feed(
    trevrpc_quic_varint_feeder* state, const uint8_t* data, size_t len, size_t* out_consumed, uint64_t* out_value) {
    if (state == NULL || data == NULL || out_consumed == NULL || out_value == NULL) {
        return -EINVAL;
    }

    *out_consumed = 0;
    *out_value = 0;

    if (state->need == 0) {
        if (len == 0) {
            return 0;
        }
        state->need = (uint8_t)trevrpc_quic_varint_size_from_first(data[0]);
    }

    while (state->have < state->need && *out_consumed < len) {
        state->bytes[state->have++] = data[(*out_consumed)++];
    }
    if (state->have < state->need) {
        return 0;
    }

    size_t offset = 0;
    int err = trevrpc_quic_varint_read(state->bytes, state->need, &offset, out_value);
    if (err != 0 || offset != state->need) {
        return -EPROTO;
    }
    trevrpc_quic_varint_reset(state);
    return 1;
}

int trevrpc_quic_varint_size(uint64_t value, size_t* out_size) {
    if (out_size == NULL) {
        return -EINVAL;
    }
    if (value > TREV_QUIC_VARINT_MAX) {
        return -ERANGE;
    }

    size_t size = 8;
    if (value <= 0x3f) {
        size = 1;
    } else if (value <= 0x3fff) {
        size = 2;
    } else if (value <= 0x3fffffff) {
        size = 4;
    }
    *out_size = size;
    return 0;
}

int trevrpc_quic_varint_write(uint8_t* out, size_t out_len, uint64_t value, size_t* out_written) {
    if (out_written == NULL) {
        return -EINVAL;
    }
    *out_written = 0;
    if (out == NULL) {
        return -EINVAL;
    }

    size_t size = 0;
    int err = trevrpc_quic_varint_size(value, &size);
    if (err != 0) {
        return err;
    }
    if (out_len < size) {
        return -ENOBUFS;
    }

    uint8_t encoded[8];
    switch (size) {
    case 1:
        encoded[0] = (uint8_t)value;
        break;
    case 2:
        encoded[0] = (uint8_t)(0x40 | (value >> 8));
        encoded[1] = (uint8_t)value;
        break;
    case 4:
        encoded[0] = (uint8_t)(0x80 | (value >> 24));
        encoded[1] = (uint8_t)(value >> 16);
        encoded[2] = (uint8_t)(value >> 8);
        encoded[3] = (uint8_t)value;
        break;
    case 8:
        encoded[0] = (uint8_t)(0xc0 | (value >> 56));
        encoded[1] = (uint8_t)(value >> 48);
        encoded[2] = (uint8_t)(value >> 40);
        encoded[3] = (uint8_t)(value >> 32);
        encoded[4] = (uint8_t)(value >> 24);
        encoded[5] = (uint8_t)(value >> 16);
        encoded[6] = (uint8_t)(value >> 8);
        encoded[7] = (uint8_t)value;
        break;
    default:
        return -EINVAL;
    }

    for (size_t i = 0; i < size; i++) {
        out[i] = encoded[i];
    }
    *out_written = size;
    return 0;
}

int trevrpc_quic_varint_read(const uint8_t* data, size_t len, size_t* offset, uint64_t* out_value) {
    if (data == NULL || offset == NULL || out_value == NULL) {
        return -EINVAL;
    }

    size_t current = *offset;
    if (current >= len) {
        return -EMSGSIZE;
    }
    size_t size = trevrpc_quic_varint_size_from_first(data[current]);
    if (len - current < size) {
        return -EMSGSIZE;
    }

    uint64_t value = data[current] & 0x3f;
    for (size_t i = 1; i < size; i++) {
        value = (value << 8) | data[current + i];
    }
    *offset = current + size;
    *out_value = value;
    return 0;
}
