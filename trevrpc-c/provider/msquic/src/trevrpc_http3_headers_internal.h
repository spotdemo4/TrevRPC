#ifndef TREVRPC_HTTP3_HEADERS_INTERNAL_H
#define TREVRPC_HTTP3_HEADERS_INTERNAL_H

#include "trevrpc_qpack_static_internal.h"

#include <stddef.h>

#define TREV_H3_ERROR_MESSAGE_ERROR 0x10e

typedef enum trevrpc_http3_header_block_kind {
    TREV_HTTP3_HEADERS_REQUEST = 0,
    TREV_HTTP3_HEADERS_RESPONSE,
    TREV_HTTP3_HEADERS_TRAILERS,
} trevrpc_http3_header_block_kind;

typedef enum trevrpc_http3_headers_status {
    TREV_HTTP3_HEADERS_OK = 0,
    TREV_HTTP3_HEADERS_INVALID_ARGUMENT,
    TREV_HTTP3_HEADERS_MESSAGE_ERROR,
} trevrpc_http3_headers_status;

typedef struct trevrpc_http3_headers_report {
    size_t pseudo_field_count;
    size_t regular_field_count;
    int response_status;
    int has_content_length;
} trevrpc_http3_headers_report;

/*
 * Validates generic HTTP/3 field syntax and request/response/trailer rules in
 * wire order. This intentionally applies no WebTransport profile policy.
 * Field values must not have leading or trailing SP/HTAB. "te" is parsed as a
 * comma list whose nonempty, case-insensitive tokens must all be "trailers";
 * OWS is accepted only around internal commas and a bounded number of empty
 * members is ignored. Trailer sections reject pseudo-fields, connection-specific
 * fields, Host, Content-Length, and Trailer. Sender-side field-definition or
 * application-policy trailer eligibility is outside this generic validator.
 * out_report is reset on failure.
 */
trevrpc_http3_headers_status trevrpc_http3_headers_validate(const trevrpc_qpack_field_section* section,
    trevrpc_http3_header_block_kind kind,
    trevrpc_http3_headers_report* out_report);

/* Returns zero for statuses that do not represent a peer H3 application error. */
uint64_t trevrpc_http3_headers_status_error_code(trevrpc_http3_headers_status status);

#endif
