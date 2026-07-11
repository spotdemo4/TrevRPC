#ifndef TREVRPC_MSQUIC_H
#define TREVRPC_MSQUIC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct trevrpc_msquic_listener trevrpc_msquic_listener;
typedef struct trevrpc_msquic_conn trevrpc_msquic_conn;
typedef struct trevrpc_msquic_stream trevrpc_msquic_stream;
typedef struct trevrpc_msquic_send_completion trevrpc_msquic_send_completion;
typedef int (*trevrpc_msquic_cancelled_fn)(void* context);

typedef struct trevrpc_msquic_frame_part {
    const uint8_t* data;
    size_t len;
} trevrpc_msquic_frame_part;

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

typedef enum trevrpc_msquic_execution_profile {
    TREV_MSQUIC_EXECUTION_PROFILE_LOW_LATENCY = 0,
    TREV_MSQUIC_EXECUTION_PROFILE_MAX_THROUGHPUT = 1,
    TREV_MSQUIC_EXECUTION_PROFILE_SCAVENGER = 2,
    TREV_MSQUIC_EXECUTION_PROFILE_REAL_TIME = 3,
} trevrpc_msquic_execution_profile;

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
    uint32_t max_stateless_operations;
    uint16_t max_binding_stateless_operations;
    size_t max_pending_send_bytes;
    size_t max_pending_send_count;
    size_t max_frame_size;
    uint32_t stream_recv_window;
    uint32_t conn_flow_control_window;
    trevrpc_msquic_execution_profile execution_profile;
    int send_buffering_enabled;
} trevrpc_msquic_config;

int trevrpc_msquic_listen(
    const char* host, uint16_t port, const trevrpc_msquic_config* config, trevrpc_msquic_listener** listener);
int trevrpc_msquic_listen_alpns(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    const trevrpc_msquic_alpn* alpns,
    size_t alpns_len,
    trevrpc_msquic_listener** listener);
int trevrpc_msquic_listener_port(trevrpc_msquic_listener* listener, uint16_t* port);
int trevrpc_msquic_listener_accept(trevrpc_msquic_listener* listener, trevrpc_msquic_conn** conn);
void trevrpc_msquic_listener_shutdown(trevrpc_msquic_listener* listener);
void trevrpc_msquic_listener_close(trevrpc_msquic_listener* listener);

int trevrpc_msquic_dial(
    const char* host, uint16_t port, const trevrpc_msquic_config* config, trevrpc_msquic_conn** conn);
int trevrpc_msquic_dial_cancellable(const char* host,
    uint16_t port,
    const trevrpc_msquic_config* config,
    trevrpc_msquic_cancelled_fn cancelled,
    void* cancellation_context,
    trevrpc_msquic_conn** conn);
int trevrpc_msquic_conn_negotiated_alpn(trevrpc_msquic_conn* conn, const uint8_t** alpn, size_t* alpn_len);
int trevrpc_msquic_conn_peer_close_error(trevrpc_msquic_conn* conn, uint64_t* error_code);
int trevrpc_msquic_conn_accept_stream(trevrpc_msquic_conn* conn, trevrpc_msquic_stream** stream);
int trevrpc_msquic_conn_open_stream(trevrpc_msquic_conn* conn, trevrpc_msquic_stream** stream);
int trevrpc_msquic_conn_open_uni_stream(trevrpc_msquic_conn* conn, trevrpc_msquic_stream** stream);
void trevrpc_msquic_conn_shutdown(trevrpc_msquic_conn* conn);
void trevrpc_msquic_conn_shutdown_error(trevrpc_msquic_conn* conn, uint64_t error_code);
void trevrpc_msquic_conn_close(trevrpc_msquic_conn* conn);

int trevrpc_msquic_stream_id(trevrpc_msquic_stream* stream, uint64_t* stream_id);
intptr_t trevrpc_msquic_stream_read(trevrpc_msquic_stream* stream, uint8_t* data, size_t len);
intptr_t trevrpc_msquic_stream_read_timeout(
    trevrpc_msquic_stream* stream, uint8_t* data, size_t len, uint64_t timeout_nanos);
intptr_t trevrpc_msquic_stream_read_ready(trevrpc_msquic_stream* stream, uint8_t* data, size_t len);
intptr_t trevrpc_msquic_stream_read_frame(trevrpc_msquic_stream* stream, uint8_t** body, size_t* len, size_t max_len);
intptr_t trevrpc_msquic_stream_read_frame_timeout(
    trevrpc_msquic_stream* stream, uint8_t** body, size_t* len, size_t max_len, uint64_t timeout_nanos);
intptr_t trevrpc_msquic_stream_read_frame_ready(
    trevrpc_msquic_stream* stream, uint8_t** body, size_t* len, size_t max_len);
intptr_t trevrpc_msquic_stream_write(trevrpc_msquic_stream* stream, const uint8_t* data, size_t len);
intptr_t trevrpc_msquic_stream_write_fin(trevrpc_msquic_stream* stream, const uint8_t* data, size_t len);
/*
 * Sends one length-prefixed native frame from borrowed body parts. MsQuic owns
 * none of the provided part memory: every non-empty part must remain valid
 * until the stream reports SEND_COMPLETE. trevrpc_msquic_stream_close and
 * trevrpc_msquic_stream_wait_pending_sends both drain those completions.
 */
intptr_t trevrpc_msquic_stream_write_frame_parts(
    trevrpc_msquic_stream* stream, const trevrpc_msquic_frame_part* parts, size_t parts_len, size_t max_len);
intptr_t trevrpc_msquic_stream_write_frame_parts_fin(
    trevrpc_msquic_stream* stream, const trevrpc_msquic_frame_part* parts, size_t parts_len, size_t max_len);
/*
 * Tracked variants return one completion for the submitted send. On a
 * synchronous failure, *completion remains NULL and no part memory is owned.
 * On success, every part remains borrowed until the completion is waited and
 * freed, including when the stream is canceled or reset. Waiting returns
 * -ECANCELED when MsQuic reports SEND_COMPLETE with Canceled set.
 */
intptr_t trevrpc_msquic_stream_write_frame_parts_with_completion(trevrpc_msquic_stream* stream,
    const trevrpc_msquic_frame_part* parts,
    size_t parts_len,
    size_t max_len,
    trevrpc_msquic_send_completion** completion);
intptr_t trevrpc_msquic_stream_write_frame_parts_fin_with_completion(trevrpc_msquic_stream* stream,
    const trevrpc_msquic_frame_part* parts,
    size_t parts_len,
    size_t max_len,
    trevrpc_msquic_send_completion** completion);
intptr_t trevrpc_msquic_stream_write_message_frame(
    trevrpc_msquic_stream* stream, const uint8_t* body, size_t body_len, size_t max_len);
intptr_t trevrpc_msquic_stream_write_message_frames(
    trevrpc_msquic_stream* stream, const uint8_t* bodies, const size_t* body_lens, size_t count, size_t max_len);
intptr_t trevrpc_msquic_stream_write_message_frames_borrowed(trevrpc_msquic_stream* stream,
    const uint8_t* const* bodies,
    const size_t* body_lens,
    size_t count,
    size_t max_len,
    trevrpc_msquic_send_completion** completion);
int trevrpc_msquic_send_completion_wait(trevrpc_msquic_send_completion* completion);
void trevrpc_msquic_send_completion_free(trevrpc_msquic_send_completion* completion);
int trevrpc_msquic_stream_wait_pending_sends(trevrpc_msquic_stream* stream);
int trevrpc_msquic_stream_shutdown_send(trevrpc_msquic_stream* stream);
int trevrpc_msquic_stream_abort(trevrpc_msquic_stream* stream);
int trevrpc_msquic_stream_abort_receive(trevrpc_msquic_stream* stream);
void trevrpc_msquic_stream_close(trevrpc_msquic_stream* stream);
void trevrpc_msquic_free(void* ptr);

const char* trevrpc_msquic_error(int code);

#ifdef __cplusplus
}
#endif

#endif
