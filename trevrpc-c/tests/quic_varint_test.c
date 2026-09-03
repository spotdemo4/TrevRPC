#include "trevrpc_quic_varint_internal.h"

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            return 1;                                                                                                  \
        }                                                                                                              \
    } while (0)

typedef struct quic_varint_vector {
    uint64_t value;
    uint8_t bytes[8];
    size_t len;
} quic_varint_vector;

static const quic_varint_vector vectors[] = {
    {0, {0x00}, 1},
    {63, {0x3f}, 1},
    {64, {0x40, 0x40}, 2},
    {16383, {0x7f, 0xff}, 2},
    {16384, {0x80, 0x00, 0x40, 0x00}, 4},
    {0x3fffffff, {0xbf, 0xff, 0xff, 0xff}, 4},
    {0x40000000, {0xc0, 0x00, 0x00, 0x00, 0x40, 0x00, 0x00, 0x00}, 8},
    {TREV_QUIC_VARINT_MAX, {0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}, 8},
};

static int test_exact_vectors(void) {
    for (size_t i = 0; i < sizeof(vectors) / sizeof(vectors[0]); i++) {
        size_t size = 99;
        CHECK(trevrpc_quic_varint_size(vectors[i].value, &size) == 0);
        CHECK(size == vectors[i].len);

        uint8_t encoded[8] = {0};
        size_t written = 99;
        CHECK(trevrpc_quic_varint_write(encoded, sizeof(encoded), vectors[i].value, &written) == 0);
        CHECK(written == vectors[i].len);
        CHECK(memcmp(encoded, vectors[i].bytes, written) == 0);

        size_t offset = 0;
        uint64_t decoded = UINT64_MAX;
        CHECK(trevrpc_quic_varint_read(vectors[i].bytes, vectors[i].len, &offset, &decoded) == 0);
        CHECK(offset == vectors[i].len);
        CHECK(decoded == vectors[i].value);
    }
    return 0;
}

static int test_size_boundaries(void) {
    const struct {
        uint64_t value;
        size_t expected;
    } cases[] = {
        {62, 1},
        {63, 1},
        {64, 2},
        {16382, 2},
        {16383, 2},
        {16384, 4},
        {0x3ffffffe, 4},
        {0x3fffffff, 4},
        {0x40000000, 8},
        {TREV_QUIC_VARINT_MAX - 1, 8},
        {TREV_QUIC_VARINT_MAX, 8},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        size_t size = 0;
        CHECK(trevrpc_quic_varint_size(cases[i].value, &size) == 0);
        CHECK(size == cases[i].expected);
    }

    size_t unchanged = 37;
    CHECK(trevrpc_quic_varint_size(TREV_QUIC_VARINT_MAX + 1, &unchanged) == -ERANGE);
    CHECK(unchanged == 37);
    CHECK(trevrpc_quic_varint_size(UINT64_MAX, &unchanged) == -ERANGE);
    CHECK(unchanged == 37);
    CHECK(trevrpc_quic_varint_size(0, NULL) == -EINVAL);
    return 0;
}

static int test_write_failures_are_atomic(void) {
    uint8_t buffer[8];
    uint8_t original[8];
    memset(buffer, 0xa5, sizeof(buffer));
    memcpy(original, buffer, sizeof(buffer));

    size_t written = 99;
    CHECK(trevrpc_quic_varint_write(buffer, sizeof(buffer), TREV_QUIC_VARINT_MAX + 1, &written) == -ERANGE);
    CHECK(written == 0);
    CHECK(memcmp(buffer, original, sizeof(buffer)) == 0);

    written = 99;
    CHECK(trevrpc_quic_varint_write(buffer, 1, 64, &written) == -ENOBUFS);
    CHECK(written == 0);
    CHECK(memcmp(buffer, original, sizeof(buffer)) == 0);

    written = 99;
    CHECK(trevrpc_quic_varint_write(NULL, 8, 1, &written) == -EINVAL);
    CHECK(written == 0);
    CHECK(trevrpc_quic_varint_write(buffer, sizeof(buffer), 1, NULL) == -EINVAL);
    return 0;
}

static int test_nonminimal_encodings(void) {
    const quic_varint_vector cases[] = {
        {1, {0x40, 0x01}, 2},
        {1, {0x80, 0x00, 0x00, 0x01}, 4},
        {1, {0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01}, 8},
        {64, {0x80, 0x00, 0x00, 0x40}, 4},
        {16384, {0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x40, 0x00}, 8},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        size_t offset = 0;
        uint64_t value = UINT64_MAX;
        CHECK(trevrpc_quic_varint_read(cases[i].bytes, cases[i].len, &offset, &value) == 0);
        CHECK(offset == cases[i].len);
        CHECK(value == cases[i].value);
    }
    return 0;
}

static int test_incremental_feeder(void) {
    for (size_t i = 0; i < sizeof(vectors) / sizeof(vectors[0]); i++) {
        for (size_t split = 0; split <= vectors[i].len; split++) {
            trevrpc_quic_varint_feeder feeder;
            trevrpc_quic_varint_reset(&feeder);
            size_t consumed = SIZE_MAX;
            uint64_t value = UINT64_MAX;

            if (split != 0) {
                CHECK(trevrpc_quic_varint_feed(&feeder, vectors[i].bytes, split, &consumed, &value) ==
                      (split == vectors[i].len ? 1 : 0));
                CHECK(consumed == split);
            }
            if (split < vectors[i].len) {
                uint8_t suffix[9];
                memcpy(suffix, vectors[i].bytes + split, vectors[i].len - split);
                suffix[vectors[i].len - split] = 0xa5;
                CHECK(trevrpc_quic_varint_feed(&feeder, suffix, vectors[i].len - split + 1, &consumed, &value) == 1);
                CHECK(consumed == vectors[i].len - split);
            }
            CHECK(value == vectors[i].value);
            CHECK(feeder.have == 0);
            CHECK(feeder.need == 0);
        }
    }

    trevrpc_quic_varint_feeder feeder;
    trevrpc_quic_varint_reset(&feeder);
    size_t consumed = SIZE_MAX;
    uint64_t value = UINT64_MAX;
    CHECK(trevrpc_quic_varint_feed(&feeder, vectors[0].bytes, 0, &consumed, &value) == 0);
    CHECK(consumed == 0);
    CHECK(value == 0);
    CHECK(trevrpc_quic_varint_feed(NULL, vectors[0].bytes, 1, &consumed, &value) == -EINVAL);
    CHECK(trevrpc_quic_varint_feed(&feeder, NULL, 1, &consumed, &value) == -EINVAL);
    CHECK(trevrpc_quic_varint_feed(&feeder, vectors[0].bytes, 1, NULL, &value) == -EINVAL);
    CHECK(trevrpc_quic_varint_feed(&feeder, vectors[0].bytes, 1, &consumed, NULL) == -EINVAL);
    return 0;
}

static int test_read_offsets_and_truncation(void) {
    const uint8_t prefixed[] = {0xaa, 0x80, 0x00, 0x40, 0x00, 0xbb};
    size_t offset = 1;
    uint64_t value = UINT64_MAX;
    CHECK(trevrpc_quic_varint_read(prefixed, sizeof(prefixed), &offset, &value) == 0);
    CHECK(offset == 5);
    CHECK(value == 16384);

    for (size_t i = 0; i < sizeof(vectors) / sizeof(vectors[0]); i++) {
        for (size_t truncated = 0; truncated < vectors[i].len; truncated++) {
            size_t unchanged_offset = 0;
            uint64_t unchanged_value = 0xfeed;
            CHECK(trevrpc_quic_varint_read(vectors[i].bytes, truncated, &unchanged_offset, &unchanged_value) ==
                  -EMSGSIZE);
            CHECK(unchanged_offset == 0);
            CHECK(unchanged_value == 0xfeed);
        }
    }

    offset = sizeof(prefixed);
    value = 7;
    CHECK(trevrpc_quic_varint_read(prefixed, sizeof(prefixed), &offset, &value) == -EMSGSIZE);
    CHECK(offset == sizeof(prefixed));
    CHECK(value == 7);
    offset = sizeof(prefixed) + 1;
    CHECK(trevrpc_quic_varint_read(prefixed, sizeof(prefixed), &offset, &value) == -EMSGSIZE);
    CHECK(offset == sizeof(prefixed) + 1);
    CHECK(value == 7);

    offset = 0;
    CHECK(trevrpc_quic_varint_read(NULL, 0, &offset, &value) == -EINVAL);
    CHECK(trevrpc_quic_varint_read(prefixed, sizeof(prefixed), NULL, &value) == -EINVAL);
    CHECK(trevrpc_quic_varint_read(prefixed, sizeof(prefixed), &offset, NULL) == -EINVAL);
    return 0;
}

int main(void) {
    int (*const tests[])(void) = {
        test_exact_vectors,
        test_size_boundaries,
        test_write_failures_are_atomic,
        test_nonminimal_encodings,
        test_incremental_feeder,
        test_read_offsets_and_truncation,
    };
    for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
        int err = tests[i]();
        if (err != 0) {
            return err;
        }
    }
    return 0;
}
