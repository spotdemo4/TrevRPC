#ifndef TREVRPC_QPACK_STATIC_INTERNAL_H
#define TREVRPC_QPACK_STATIC_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

#define TREV_H3_ERROR_INTERNAL_ERROR 0x102
#define TREV_H3_ERROR_EXCESSIVE_LOAD 0x107
#define TREV_QPACK_ERROR_DECOMPRESSION_FAILED 0x200

typedef enum trevrpc_qpack_status {
    TREV_QPACK_OK = 0,
    TREV_QPACK_INVALID_ARGUMENT,
    TREV_QPACK_DECOMPRESSION_FAILED,
    TREV_QPACK_ENCODED_SIZE_EXCEEDED,
    TREV_QPACK_DECODED_SIZE_EXCEEDED,
    TREV_QPACK_FIELD_COUNT_EXCEEDED,
    TREV_QPACK_ALLOCATION_FAILED,
} trevrpc_qpack_status;

typedef struct trevrpc_qpack_bytes {
    const uint8_t* data;
    size_t len;
} trevrpc_qpack_bytes;

typedef struct trevrpc_qpack_field {
    trevrpc_qpack_bytes name;
    trevrpc_qpack_bytes value;
} trevrpc_qpack_field;

typedef struct trevrpc_qpack_limits {
    size_t max_encoded_size;
    size_t max_decoded_size;
    size_t max_field_count;
} trevrpc_qpack_limits;

typedef struct trevrpc_qpack_field_section {
    trevrpc_qpack_field* fields;
    size_t field_count;
    size_t encoded_size;
    size_t decoded_size;
    void* allocation;
} trevrpc_qpack_field_section;

void trevrpc_qpack_field_section_init(trevrpc_qpack_field_section* section);
void trevrpc_qpack_field_section_release(trevrpc_qpack_field_section* section);

/*
 * Decodes a complete field section using no dynamic-table state. out_section
 * must first be initialized. The result owns one allocation containing the
 * ordered field array and all field bytes. Failure leaves out_section unchanged;
 * success releases its prior result and installs the replacement atomically.
 */
trevrpc_qpack_status trevrpc_qpack_static_decode(const uint8_t* encoded,
    size_t encoded_len,
    const trevrpc_qpack_limits* limits,
    trevrpc_qpack_field_section* out_section);

/* Returns zero for statuses that do not represent a peer application error. */
uint64_t trevrpc_qpack_status_error_code(trevrpc_qpack_status status);

const trevrpc_qpack_field* trevrpc_qpack_field_at(const trevrpc_qpack_field_section* section, size_t index);
int trevrpc_qpack_bytes_equal(trevrpc_qpack_bytes bytes, const uint8_t* value, size_t value_len);
int trevrpc_qpack_field_name_equal(const trevrpc_qpack_field* field, const uint8_t* name, size_t name_len);

/* Starts at start_index and preserves wire order; returns zero when absent. */
int trevrpc_qpack_find_first(const trevrpc_qpack_field_section* section,
    const uint8_t* name,
    size_t name_len,
    size_t start_index,
    size_t* out_index);

#endif
