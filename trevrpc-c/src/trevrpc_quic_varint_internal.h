#ifndef TREVRPC_QUIC_VARINT_INTERNAL_H
#define TREVRPC_QUIC_VARINT_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

#define TREV_QUIC_VARINT_MAX ((1ull << 62) - 1)

static inline size_t trevrpc_quic_varint_size_from_first(uint8_t first) {
    return (size_t)1 << (first >> 6);
}

int trevrpc_quic_varint_size(uint64_t value, size_t* out_size);
int trevrpc_quic_varint_write(uint8_t* out, size_t out_len, uint64_t value, size_t* out_written);
int trevrpc_quic_varint_read(const uint8_t* data, size_t len, size_t* offset, uint64_t* out_value);

#endif
