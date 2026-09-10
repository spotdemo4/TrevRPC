#ifndef TREVRPC_HTTP3_SETTINGS_INTERNAL_H
#define TREVRPC_HTTP3_SETTINGS_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

#define TREV_H3_ERROR_FRAME_ERROR 0x106
#define TREV_H3_ERROR_EXCESSIVE_LOAD 0x107
#define TREV_H3_ERROR_SETTINGS_ERROR 0x109

typedef enum trevrpc_h3_settings_status {
    TREV_H3_SETTINGS_OK = 0,
    TREV_H3_SETTINGS_INVALID_ARGUMENT,
    TREV_H3_SETTINGS_MALFORMED,
    TREV_H3_SETTINGS_DUPLICATE,
    TREV_H3_SETTINGS_RESERVED,
    TREV_H3_SETTINGS_INVALID_VALUE,
    TREV_H3_SETTINGS_EXCESSIVE_LOAD,
    TREV_H3_SETTINGS_WORKSPACE_EXHAUSTED,
    TREV_H3_SETTINGS_OUT_OF_RANGE,
    TREV_H3_SETTINGS_OUTPUT_TOO_SMALL,
    TREV_H3_SETTINGS_SIZE_OVERFLOW,
} trevrpc_h3_settings_status;

typedef struct trevrpc_h3_settings_pair {
    uint64_t id;
    uint64_t value;
} trevrpc_h3_settings_pair;

typedef struct trevrpc_h3_settings_report {
    size_t payload_len;
    size_t pair_count;
} trevrpc_h3_settings_report;

/* A nonzero visitor result rejects a recognized setting value. */
typedef int (*trevrpc_h3_settings_visitor)(void* context, uint64_t id, uint64_t value);

/*
 * Parses a complete SETTINGS payload. seen_ids is caller-owned duplicate
 * detection workspace and must hold one entry per setting pair. out_report is
 * updated only on success.
 */
trevrpc_h3_settings_status trevrpc_h3_settings_parse(const uint8_t* payload,
    size_t payload_len,
    size_t max_payload_len,
    uint64_t* seen_ids,
    size_t seen_ids_capacity,
    trevrpc_h3_settings_visitor visitor,
    void* visitor_context,
    trevrpc_h3_settings_report* out_report);

/*
 * Builds only the SETTINGS payload. Input order is ignored and output pairs are
 * emitted by ascending ID. The destination and out_payload_len are unchanged on
 * failure.
 */
trevrpc_h3_settings_status trevrpc_h3_settings_build(const trevrpc_h3_settings_pair* pairs,
    size_t pair_count,
    uint8_t* out,
    size_t out_capacity,
    size_t* out_payload_len);

/* Returns zero for statuses that do not represent a peer H3 application error. */
uint64_t trevrpc_h3_settings_status_error_code(trevrpc_h3_settings_status status);

#endif
