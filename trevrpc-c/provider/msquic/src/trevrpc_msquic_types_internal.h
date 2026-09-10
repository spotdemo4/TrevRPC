#ifndef TREVRPC_MSQUIC_TYPES_INTERNAL_H
#define TREVRPC_MSQUIC_TYPES_INTERNAL_H

#include <stddef.h>
#include <stdint.h>

/*
 * Shared MsQuic object declarations used by the canonical Engine, Transport,
 * and RPC implementations.
 */
typedef struct trevrpc_msquic_listener trevrpc_msquic_listener;
typedef struct trevrpc_msquic_conn trevrpc_msquic_conn;
typedef struct trevrpc_msquic_stream trevrpc_msquic_stream;
typedef struct trevrpc_msquic_send_completion trevrpc_msquic_send_completion;
typedef int (*trevrpc_msquic_cancelled_fn)(void* context);

typedef struct trevrpc_msquic_alpn {
    const char* alpn;
    uint32_t alpn_len;
} trevrpc_msquic_alpn;

#define TREV_MSQUIC_ERR_CLOSED -1001
#define TREV_MSQUIC_ERR_FRAME_TOO_LARGE -1002
#define TREV_MSQUIC_ERR_TIMEOUT -1003
#define TREV_MSQUIC_ERR_RESOURCE_EXHAUSTED -1004

#define TREV_MSQUIC_DEFAULT_MAX_PENDING_SEND_BYTES (64u * 1024u * 1024u)
#define TREV_MSQUIC_DEFAULT_MAX_PENDING_SEND_COUNT 1024u

typedef struct trevrpc_msquic_config {
    const char* alpn;
    uint32_t alpn_len;
    const char* cert_file;
    const char* key_file;
    const char* ca_cert_file;
    int skip_certificate_validation;
    uint64_t max_idle_timeout_ms;
    uint32_t keep_alive_ms;
    uint16_t peer_bidi_stream_count;
    uint16_t peer_unidi_stream_count;
    size_t max_pending_send_bytes;
    size_t max_pending_send_count;
    size_t max_frame_size;
    uint32_t stream_recv_window;
    uint32_t conn_flow_control_window;
    int send_buffering_enabled;
} trevrpc_msquic_config;

#endif
