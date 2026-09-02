#include "trevrpc_qpack_static_internal.h"

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

typedef struct test_buffer {
    uint8_t bytes[1024];
    size_t len;
} test_buffer;

static const trevrpc_qpack_limits generous_limits = {
    .max_encoded_size = 1024,
    .max_decoded_size = 4096,
    .max_field_count = 32,
};

static void append_byte(test_buffer* buffer, uint8_t byte) {
    buffer->bytes[buffer->len++] = byte;
}

static void append_bytes(test_buffer* buffer, const uint8_t* bytes, size_t len) {
    memcpy(buffer->bytes + buffer->len, bytes, len);
    buffer->len += len;
}

/* Independent test encoder for RFC 9204 prefixed integers. */
static void append_integer(test_buffer* buffer, uint8_t high_bits, unsigned prefix_bits, uint64_t value) {
    uint8_t prefix_max = (uint8_t)(0xffu >> (8u - prefix_bits));
    if (value < prefix_max) {
        append_byte(buffer, (uint8_t)(high_bits | value));
        return;
    }
    append_byte(buffer, (uint8_t)(high_bits | prefix_max));
    value -= prefix_max;
    while (value >= 128) {
        append_byte(buffer, (uint8_t)((value & 0x7fu) | 0x80u));
        value >>= 7;
    }
    append_byte(buffer, (uint8_t)value);
}

static void append_prefix(test_buffer* buffer, uint64_t delta_base) {
    append_byte(buffer, 0x00);
    append_integer(buffer, 0x00, 7, delta_base);
}

static void append_indexed_static(test_buffer* buffer, size_t index) {
    append_integer(buffer, 0xc0, 6, index);
}

static void append_name_reference(
    test_buffer* buffer, size_t index, int huffman, const uint8_t* value, size_t value_len) {
    append_integer(buffer, 0x50, 4, index);
    append_integer(buffer, huffman ? 0x80 : 0x00, 7, value_len);
    append_bytes(buffer, value, value_len);
}

static void append_literal(test_buffer* buffer,
    int name_huffman,
    const uint8_t* name,
    size_t name_len,
    int value_huffman,
    const uint8_t* value,
    size_t value_len) {
    append_integer(buffer, name_huffman ? 0x28 : 0x20, 3, name_len);
    append_bytes(buffer, name, name_len);
    append_integer(buffer, value_huffman ? 0x80 : 0x00, 7, value_len);
    append_bytes(buffer, value, value_len);
}

static int bytes_are(trevrpc_qpack_bytes bytes, const char* value) {
    return trevrpc_qpack_bytes_equal(bytes, (const uint8_t*)value, strlen(value));
}

static trevrpc_qpack_status decode(
    const uint8_t* bytes, size_t len, const trevrpc_qpack_limits* limits, trevrpc_qpack_field_section* section) {
    return trevrpc_qpack_static_decode(bytes, len, limits, section);
}

static int test_prefix_and_static_vectors(void) {
    test_buffer buffer = {0};
    append_prefix(&buffer, 0);
    append_indexed_static(&buffer, 17);
    append_indexed_static(&buffer, 31);
    append_indexed_static(&buffer, 25);
    trevrpc_qpack_field_section section;
    trevrpc_qpack_field_section_init(&section);

    CHECK(decode(buffer.bytes, buffer.len, &generous_limits, &section) == TREV_QPACK_OK);
    CHECK(section.encoded_size == buffer.len);
    CHECK(section.field_count == 3);
    CHECK(section.decoded_size == (7 + 3 + 32) + (15 + 17 + 32) + (7 + 3 + 32));
    CHECK(bytes_are(section.fields[0].name, ":method"));
    CHECK(bytes_are(section.fields[0].value, "GET"));
    CHECK(bytes_are(section.fields[1].name, "accept-encoding"));
    CHECK(bytes_are(section.fields[1].value, "gzip, deflate, br"));
    CHECK(bytes_are(section.fields[2].name, ":status"));
    CHECK(bytes_are(section.fields[2].value, "200"));
    CHECK(trevrpc_qpack_field_at(&section, 3) == NULL);
    trevrpc_qpack_field_section_release(&section);

    const uint8_t only_prefix[] = {0x00, 0x00};
    CHECK(decode(only_prefix, sizeof(only_prefix), &generous_limits, &section) == TREV_QPACK_OK);
    CHECK(section.field_count == 0);
    CHECK(section.encoded_size == sizeof(only_prefix));
    CHECK(section.decoded_size == 0);
    trevrpc_qpack_field_section_release(&section);
    return 0;
}

static int test_static_base_boundaries(void) {
    static const uint8_t zero_base[] = {0x00, 0x00};
    static const uint8_t delta_one[] = {0x00, 0x01};
    static const uint8_t delta_five[] = {0x00, 0x05};
    static const uint8_t delta_prefix_boundary[] = {0x00, 0x7e};
    static const uint8_t delta_continuation_boundary[] = {0x00, 0x7f, 0x00};
    static const uint8_t negative_zero_delta[] = {0x00, 0x80};
    static const uint8_t negative_prefix_boundary[] = {0x00, 0xfe};
    static const uint8_t negative_continuation_boundary[] = {0x00, 0xff, 0x00};
    static const struct {
        const uint8_t* encoded;
        size_t encoded_len;
        trevrpc_qpack_status expected;
    } cases[] = {
        {zero_base, sizeof(zero_base), TREV_QPACK_OK},
        {delta_one, sizeof(delta_one), TREV_QPACK_DECOMPRESSION_FAILED},
        {delta_five, sizeof(delta_five), TREV_QPACK_DECOMPRESSION_FAILED},
        {delta_prefix_boundary, sizeof(delta_prefix_boundary), TREV_QPACK_DECOMPRESSION_FAILED},
        {delta_continuation_boundary, sizeof(delta_continuation_boundary), TREV_QPACK_DECOMPRESSION_FAILED},
        {negative_zero_delta, sizeof(negative_zero_delta), TREV_QPACK_DECOMPRESSION_FAILED},
        {negative_prefix_boundary, sizeof(negative_prefix_boundary), TREV_QPACK_DECOMPRESSION_FAILED},
        {negative_continuation_boundary, sizeof(negative_continuation_boundary), TREV_QPACK_DECOMPRESSION_FAILED},
    };

    trevrpc_qpack_field_section section;
    trevrpc_qpack_field_section_init(&section);
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        CHECK(decode(cases[i].encoded, cases[i].encoded_len, &generous_limits, &section) == cases[i].expected);
        CHECK(section.allocation == NULL && section.field_count == 0);
    }
    return 0;
}

static int test_required_insert_count_and_truncated_prefixes(void) {
    static const uint8_t missing_delta_base[] = {0x00};
    static const uint8_t truncated_required_insert_count[] = {0xff};
    static const uint8_t truncated_required_insert_count_continuation[] = {0xff, 0x80};
    static const uint8_t truncated_delta_base[] = {0x00, 0x7f};
    static const uint8_t truncated_negative_delta_base[] = {0x00, 0xff};
    static const uint8_t required_insert_count_one_base_one[] = {0x01, 0x00};
    static const uint8_t required_insert_count_one_base_zero[] = {0x01, 0x80};
    static const uint8_t required_insert_count_prefix_boundary[] = {0xfe, 0x00};
    static const uint8_t required_insert_count_continuation_boundary[] = {0xff, 0x00, 0x00};
    static const struct {
        const uint8_t* encoded;
        size_t encoded_len;
    } cases[] = {
        {NULL, 0},
        {missing_delta_base, sizeof(missing_delta_base)},
        {truncated_required_insert_count, sizeof(truncated_required_insert_count)},
        {truncated_required_insert_count_continuation, sizeof(truncated_required_insert_count_continuation)},
        {truncated_delta_base, sizeof(truncated_delta_base)},
        {truncated_negative_delta_base, sizeof(truncated_negative_delta_base)},
        {required_insert_count_one_base_one, sizeof(required_insert_count_one_base_one)},
        {required_insert_count_one_base_zero, sizeof(required_insert_count_one_base_zero)},
        {required_insert_count_prefix_boundary, sizeof(required_insert_count_prefix_boundary)},
        {required_insert_count_continuation_boundary, sizeof(required_insert_count_continuation_boundary)},
    };

    trevrpc_qpack_field_section section;
    trevrpc_qpack_field_section_init(&section);
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        CHECK(decode(cases[i].encoded, cases[i].encoded_len, &generous_limits, &section) ==
              TREV_QPACK_DECOMPRESSION_FAILED);
        CHECK(section.allocation == NULL && section.field_count == 0);
    }
    return 0;
}

static int test_uint64_integer_boundaries(void) {
    static const uint8_t max_delta[] = {
        0x00,
        0x7f,
        0x80,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0x3f,
        0xd1,
    };
    test_buffer generated = {0};
    append_prefix(&generated, UINT64_C(0x3fffffffffffffff));
    append_indexed_static(&generated, 17);
    CHECK(generated.len == sizeof(max_delta));
    CHECK(memcmp(generated.bytes, max_delta, sizeof(max_delta)) == 0);

    trevrpc_qpack_field_section section;
    trevrpc_qpack_field_section_init(&section);
    CHECK(decode(max_delta, sizeof(max_delta), &generous_limits, &section) == TREV_QPACK_DECOMPRESSION_FAILED);
    CHECK(section.allocation == NULL && section.field_count == 0);

    test_buffer max_negative_delta = {0};
    append_byte(&max_negative_delta, 0x00);
    append_integer(&max_negative_delta, 0x80, 7, UINT64_C(0x3fffffffffffffff));
    CHECK(decode(max_negative_delta.bytes, max_negative_delta.len, &generous_limits, &section) ==
          TREV_QPACK_DECOMPRESSION_FAILED);
    CHECK(section.allocation == NULL && section.field_count == 0);

    static const uint8_t exceeds_qpack_limit[] = {
        0x00,
        0x7f,
        0x81,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0x3f,
    };
    static const uint8_t uint64_overflow[] = {
        0x00,
        0x7f,
        0x81,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0x01,
    };
    static const uint8_t overlong_uint64[] = {
        0x00,
        0x7f,
        0x80,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0x81,
        0x00,
    };
    static const uint8_t truncated_uint64[] = {
        0x00,
        0x7f,
        0x80,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
    };
    static const struct {
        const uint8_t* encoded;
        size_t encoded_len;
    } invalid[] = {
        {exceeds_qpack_limit, sizeof(exceeds_qpack_limit)},
        {uint64_overflow, sizeof(uint64_overflow)},
        {overlong_uint64, sizeof(overlong_uint64)},
        {truncated_uint64, sizeof(truncated_uint64)},
    };
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        CHECK(decode(invalid[i].encoded, invalid[i].encoded_len, &generous_limits, &section) ==
              TREV_QPACK_DECOMPRESSION_FAILED);
        CHECK(section.allocation == NULL && section.field_count == 0);
    }
    return 0;
}

static int test_invalid_prefix_and_dynamic_forms(void) {
    static const uint8_t invalid[][3] = {
        {0x00, 0x80, 0x00}, /* S=1 with RIC=0 */
        {0x01, 0x00, 0x00}, /* nonzero Required Insert Count */
        {0x00, 0x00, 0x80}, /* dynamic indexed */
        {0x00, 0x00, 0x40}, /* dynamic name reference */
        {0x00, 0x00, 0x10}, /* post-base indexed */
        {0x00, 0x00, 0x00}, /* post-base name reference */
    };
    trevrpc_qpack_field_section section;
    trevrpc_qpack_field_section_init(&section);
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        CHECK(decode(invalid[i], sizeof(invalid[i]), &generous_limits, &section) == TREV_QPACK_DECOMPRESSION_FAILED);
        CHECK(section.allocation == NULL && section.field_count == 0);
    }
    return 0;
}

static int test_invalid_indexes_and_integers(void) {
    const uint8_t invalid_index[] = {0x00, 0x00, 0xff, 0x24}; /* static index 99 */
    const uint8_t truncated_index[] = {0x00, 0x00, 0xff};
    const uint8_t overflowing_index[] = {
        0x00,
        0x00,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0xff,
        0x7f,
    };
    test_buffer max_index = {0};
    append_prefix(&max_index, 0);
    append_integer(&max_index, 0xc0, 6, UINT64_MAX);
    test_buffer max_name_index = {0};
    append_prefix(&max_name_index, 0);
    append_integer(&max_name_index, 0x50, 4, UINT64_MAX);
    test_buffer max_value_length = {0};
    append_prefix(&max_value_length, 0);
    append_integer(&max_value_length, 0x50, 4, 5);
    append_integer(&max_value_length, 0x00, 7, UINT64_MAX);
    test_buffer above_32bit_name_length = {0};
    append_prefix(&above_32bit_name_length, 0);
    append_integer(&above_32bit_name_length, 0x20, 3, UINT64_C(0x100000000));
    trevrpc_qpack_field_section section;
    trevrpc_qpack_field_section_init(&section);
    CHECK(decode(invalid_index, sizeof(invalid_index), &generous_limits, &section) == TREV_QPACK_DECOMPRESSION_FAILED);
    CHECK(decode(truncated_index, sizeof(truncated_index), &generous_limits, &section) ==
          TREV_QPACK_DECOMPRESSION_FAILED);
    CHECK(decode(overflowing_index, sizeof(overflowing_index), &generous_limits, &section) ==
          TREV_QPACK_DECOMPRESSION_FAILED);
    CHECK(decode(max_index.bytes, max_index.len, &generous_limits, &section) == TREV_QPACK_DECOMPRESSION_FAILED);
    CHECK(decode(max_name_index.bytes, max_name_index.len, &generous_limits, &section) ==
          TREV_QPACK_DECOMPRESSION_FAILED);
    CHECK(decode(max_value_length.bytes, max_value_length.len, &generous_limits, &section) ==
          TREV_QPACK_DECOMPRESSION_FAILED);
    CHECK(decode(above_32bit_name_length.bytes, above_32bit_name_length.len, &generous_limits, &section) ==
          TREV_QPACK_DECOMPRESSION_FAILED);
    return 0;
}

static int test_huffman_rfc_vectors(void) {
    static const uint8_t www_example_com[] = {
        0xf1,
        0xe3,
        0xc2,
        0xe5,
        0xf2,
        0x3a,
        0x6b,
        0xa0,
        0xab,
        0x90,
        0xf4,
        0xff,
    };
    static const uint8_t no_cache[] = {0xa8, 0xeb, 0x10, 0x64, 0x9c, 0xbf};
    static const uint8_t custom_key[] = {0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xa9, 0x7d, 0x7f};
    static const uint8_t custom_value[] = {0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xb8, 0xe8, 0xb4, 0xbf};
    static const struct {
        const uint8_t* encoded;
        size_t encoded_len;
        const char* decoded;
    } vectors[] = {
        {www_example_com, sizeof(www_example_com), "www.example.com"},
        {no_cache, sizeof(no_cache), "no-cache"},
        {custom_key, sizeof(custom_key), "custom-key"},
        {custom_value, sizeof(custom_value), "custom-value"},
    };

    for (size_t i = 0; i < sizeof(vectors) / sizeof(vectors[0]); i++) {
        test_buffer buffer = {0};
        append_prefix(&buffer, 0);
        append_name_reference(&buffer, 5, 1, vectors[i].encoded, vectors[i].encoded_len);
        trevrpc_qpack_field_section section;
        trevrpc_qpack_field_section_init(&section);
        CHECK(decode(buffer.bytes, buffer.len, &generous_limits, &section) == TREV_QPACK_OK);
        CHECK(section.field_count == 1);
        CHECK(bytes_are(section.fields[0].name, "cookie"));
        CHECK(bytes_are(section.fields[0].value, vectors[i].decoded));
        trevrpc_qpack_field_section_release(&section);
    }
    return 0;
}

static int test_huffman_rejections(void) {
    static const uint8_t eos[] = {0xff, 0xff, 0xff, 0xff};
    static const uint8_t too_long_padding[] = {0xff};
    static const uint8_t bad_padding[] = {0x00};
    static const uint8_t truncation[] = {0xfe};
    static const struct {
        const uint8_t* encoded;
        size_t encoded_len;
    } invalid[] = {
        {eos, sizeof(eos)},
        {too_long_padding, sizeof(too_long_padding)},
        {bad_padding, sizeof(bad_padding)},
        {truncation, sizeof(truncation)},
    };

    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        test_buffer buffer = {0};
        append_prefix(&buffer, 0);
        append_name_reference(&buffer, 5, 1, invalid[i].encoded, invalid[i].encoded_len);
        trevrpc_qpack_field_section section;
        trevrpc_qpack_field_section_init(&section);
        CHECK(decode(buffer.bytes, buffer.len, &generous_limits, &section) == TREV_QPACK_DECOMPRESSION_FAILED);
        CHECK(section.allocation == NULL && section.field_count == 0);
    }
    return 0;
}

static int test_ordered_duplicates_and_find(void) {
    test_buffer buffer = {0};
    append_prefix(&buffer, 0);
    append_name_reference(&buffer, 5, 0, (const uint8_t*)"a=1", 3);
    append_name_reference(&buffer, 14, 0, (const uint8_t*)"one=1", 5);
    append_name_reference(&buffer, 5, 0, (const uint8_t*)"b=2", 3);
    append_name_reference(&buffer, 14, 0, (const uint8_t*)"two=2", 5);
    trevrpc_qpack_field_section section;
    trevrpc_qpack_field_section_init(&section);

    CHECK(decode(buffer.bytes, buffer.len, &generous_limits, &section) == TREV_QPACK_OK);
    CHECK(section.field_count == 4);
    CHECK(bytes_are(section.fields[0].name, "cookie") && bytes_are(section.fields[0].value, "a=1"));
    CHECK(bytes_are(section.fields[1].name, "set-cookie") && bytes_are(section.fields[1].value, "one=1"));
    CHECK(bytes_are(section.fields[2].name, "cookie") && bytes_are(section.fields[2].value, "b=2"));
    CHECK(bytes_are(section.fields[3].name, "set-cookie") && bytes_are(section.fields[3].value, "two=2"));
    size_t index = SIZE_MAX;
    CHECK(trevrpc_qpack_find_first(&section, (const uint8_t*)"cookie", 6, 0, &index) && index == 0);
    CHECK(trevrpc_qpack_find_first(&section, (const uint8_t*)"cookie", 6, 1, &index) && index == 2);
    CHECK(!trevrpc_qpack_find_first(&section, (const uint8_t*)"cookie", 6, 3, &index));
    trevrpc_qpack_field_section_release(&section);
    return 0;
}

static int test_exact_bounds_and_failure_atomicity(void) {
    const uint8_t encoded[] = {0x00, 0x00, 0xd1};
    const size_t decoded_size = 7 + 3 + 32;
    trevrpc_qpack_limits limits = {
        .max_encoded_size = sizeof(encoded),
        .max_decoded_size = decoded_size,
        .max_field_count = 1,
    };
    trevrpc_qpack_field_section section;
    trevrpc_qpack_field_section_init(&section);
    CHECK(decode(encoded, sizeof(encoded), &limits, &section) == TREV_QPACK_OK);
    CHECK(section.field_count == 1 && section.decoded_size == decoded_size);
    void* successful_allocation = section.allocation;
    trevrpc_qpack_field* successful_fields = section.fields;

    limits.max_encoded_size--;
    CHECK(decode(encoded, sizeof(encoded), &limits, &section) == TREV_QPACK_ENCODED_SIZE_EXCEEDED);
    CHECK(section.allocation == successful_allocation && section.fields == successful_fields &&
          section.field_count == 1 && bytes_are(section.fields[0].value, "GET"));
    limits.max_encoded_size++;
    limits.max_decoded_size--;
    CHECK(decode(encoded, sizeof(encoded), &limits, &section) == TREV_QPACK_DECODED_SIZE_EXCEEDED);
    CHECK(
        section.allocation == successful_allocation && section.fields == successful_fields && section.field_count == 1);
    limits.max_decoded_size++;
    limits.max_field_count = 0;
    CHECK(decode(encoded, sizeof(encoded), &limits, &section) == TREV_QPACK_FIELD_COUNT_EXCEEDED);
    CHECK(
        section.allocation == successful_allocation && section.fields == successful_fields && section.field_count == 1);
    trevrpc_qpack_field_section_release(&section);
    return 0;
}

static int test_huffman_decoded_bound(void) {
    const uint8_t encoded_value[] = {0xf1, 0xe3, 0xc2, 0xe5, 0xf2, 0x3a, 0x6b, 0xa0, 0xab, 0x90, 0xf4, 0xff};
    test_buffer buffer = {0};
    append_prefix(&buffer, 0);
    append_name_reference(&buffer, 5, 1, encoded_value, sizeof(encoded_value));
    trevrpc_qpack_limits limits = generous_limits;
    limits.max_decoded_size = 6 + strlen("www.example.com") + 32;
    trevrpc_qpack_field_section section;
    trevrpc_qpack_field_section_init(&section);
    CHECK(decode(buffer.bytes, buffer.len, &limits, &section) == TREV_QPACK_OK);
    trevrpc_qpack_field_section_release(&section);
    limits.max_decoded_size--;
    CHECK(decode(buffer.bytes, buffer.len, &limits, &section) == TREV_QPACK_DECODED_SIZE_EXCEEDED);
    return 0;
}

static int test_literal_name_and_huffman_name(void) {
    static const uint8_t custom_key[] = {0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xa9, 0x7d, 0x7f};
    static const uint8_t custom_value[] = {0x25, 0xa8, 0x49, 0xe9, 0x5b, 0xb8, 0xe8, 0xb4, 0xbf};
    test_buffer buffer = {0};
    append_prefix(&buffer, 0);
    append_literal(&buffer, 1, custom_key, sizeof(custom_key), 1, custom_value, sizeof(custom_value));
    append_literal(
        &buffer, 0, (const uint8_t*)":protocol", strlen(":protocol"), 0, (const uint8_t*)"sample", strlen("sample"));
    trevrpc_qpack_field_section section;
    trevrpc_qpack_field_section_init(&section);
    CHECK(decode(buffer.bytes, buffer.len, &generous_limits, &section) == TREV_QPACK_OK);
    CHECK(section.field_count == 2);
    CHECK(bytes_are(section.fields[0].name, "custom-key"));
    CHECK(bytes_are(section.fields[0].value, "custom-value"));
    CHECK(bytes_are(section.fields[1].name, ":protocol"));
    CHECK(bytes_are(section.fields[1].value, "sample"));
    trevrpc_qpack_field_section_release(&section);
    return 0;
}

static int test_status_error_mapping(void) {
    static const struct {
        trevrpc_qpack_status status;
        uint64_t error_code;
    } cases[] = {
        {TREV_QPACK_OK, 0},
        {TREV_QPACK_INVALID_ARGUMENT, 0},
        {TREV_QPACK_DECOMPRESSION_FAILED, UINT64_C(0x200)},
        {TREV_QPACK_ENCODED_SIZE_EXCEEDED, UINT64_C(0x107)},
        {TREV_QPACK_DECODED_SIZE_EXCEEDED, UINT64_C(0x107)},
        {TREV_QPACK_FIELD_COUNT_EXCEEDED, UINT64_C(0x107)},
        {TREV_QPACK_ALLOCATION_FAILED, UINT64_C(0x102)},
        {(trevrpc_qpack_status)-1, 0},
        {(trevrpc_qpack_status)99, 0},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        CHECK(trevrpc_qpack_status_error_code(cases[i].status) == cases[i].error_code);
    }
    return 0;
}

static int test_invalid_arguments(void) {
    const uint8_t prefix[] = {0x00, 0x00};
    trevrpc_qpack_field_section section;
    trevrpc_qpack_field_section_init(&section);
    CHECK(decode(NULL, 1, &generous_limits, &section) == TREV_QPACK_INVALID_ARGUMENT);
    CHECK(decode(prefix, sizeof(prefix), NULL, &section) == TREV_QPACK_INVALID_ARGUMENT);
    CHECK(trevrpc_qpack_static_decode(prefix, sizeof(prefix), &generous_limits, NULL) == TREV_QPACK_INVALID_ARGUMENT);
    CHECK(!trevrpc_qpack_bytes_equal((trevrpc_qpack_bytes){.data = NULL, .len = 1}, NULL, 1));
    return 0;
}

int main(void) {
    int (*const tests[])(void) = {
        test_prefix_and_static_vectors,
        test_static_base_boundaries,
        test_required_insert_count_and_truncated_prefixes,
        test_uint64_integer_boundaries,
        test_invalid_prefix_and_dynamic_forms,
        test_invalid_indexes_and_integers,
        test_huffman_rfc_vectors,
        test_huffman_rejections,
        test_ordered_duplicates_and_find,
        test_exact_bounds_and_failure_atomicity,
        test_huffman_decoded_bound,
        test_literal_name_and_huffman_name,
        test_status_error_mapping,
        test_invalid_arguments,
    };
    for (size_t i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
        int result = tests[i]();
        if (result != 0) {
            return result;
        }
    }
    return 0;
}
