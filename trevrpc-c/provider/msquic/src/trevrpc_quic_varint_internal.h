#ifndef TREVRPC_QUIC_VARINT_INTERNAL_H
#define TREVRPC_QUIC_VARINT_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

#define TREV_QUIC_VARINT_MAX ((1ull << 62) - 1)

static inline size_t trevrpc_quic_varint_size_from_first(uint8_t first) {
    return (size_t)1 << (first >> 6);
}

typedef struct trevrpc_quic_varint_feeder {
    uint8_t bytes[8];
    uint8_t have;
    uint8_t need;
} trevrpc_quic_varint_feeder;

void trevrpc_quic_varint_reset(trevrpc_quic_varint_feeder* state);
int trevrpc_quic_varint_feed(
    trevrpc_quic_varint_feeder* state, const uint8_t* data, size_t len, size_t* out_consumed, uint64_t* out_value);

int trevrpc_quic_varint_size(uint64_t value, size_t* out_size);
int trevrpc_quic_varint_write(uint8_t* out, size_t out_len, uint64_t value, size_t* out_written);
int trevrpc_quic_varint_read(const uint8_t* data, size_t len, size_t* offset, uint64_t* out_value);

#endif
