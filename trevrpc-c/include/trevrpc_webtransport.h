#ifndef TREVRPC_WEBTRANSPORT_H
#define TREVRPC_WEBTRANSPORT_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct trevrpc_wt_listener trevrpc_wt_listener;
typedef struct trevrpc_wt_session trevrpc_wt_session;
typedef struct trevrpc_wt_stream trevrpc_wt_stream;
typedef struct trevrpc_h3_conn trevrpc_h3_conn;
typedef struct trevrpc_h3_stream trevrpc_h3_stream;
typedef struct trevrpc_msquic_conn trevrpc_msquic_conn;

#ifndef TREVRPC_WEBTRANSPORT_ADMISSION_DEFINED
#define TREVRPC_WEBTRANSPORT_ADMISSION_DEFINED
typedef struct trevrpc_webtransport_admission_request {
    const char* path;
    size_t path_len;
    const char* authority;
    size_t authority_len;
    const char* origin;
    size_t origin_len;
    int secure;
} trevrpc_webtransport_admission_request;

typedef int (*trevrpc_webtransport_admission)(void* user_data, const trevrpc_webtransport_admission_request* request);
#endif

#ifndef TREVRPC_HTTP3_ADMISSION_DEFINED
#define TREVRPC_HTTP3_ADMISSION_DEFINED
typedef struct trevrpc_http3_admission_request {
    const char* path;
    size_t path_len;
    const char* authority;
    size_t authority_len;
    int secure;
} trevrpc_http3_admission_request;

typedef int (*trevrpc_http3_admission)(void* user_data, const trevrpc_http3_admission_request* request);
#endif

#define TREV_WT_ERR_CLOSED -3001
#define TREV_WT_ERR_FRAME_TOO_LARGE -3002
#define TREV_WT_ERR_REJECTED -3003

#define TREV_H3_STREAM_RESOLVED_HTTP3 1
#define TREV_H3_STREAM_RESOLVED_WEBTRANSPORT 2
#define TREV_H3_STREAM_RESOLVED_HANDLED 3

typedef struct trevrpc_wt_config {
    const char* host;
    uint16_t port;
    const char* url;
    const char* path;
    const char* origin;
    const char* cert_file;
    const char* key_file;
    const char* ca_cert_file;
    int skip_certificate_validation;
    trevrpc_webtransport_admission admission;
    void* admission_user_data;
    uint32_t max_sessions_per_connection;
    uint32_t max_streams_per_session;
    uint32_t idle_timeout_ms;
} trevrpc_wt_config;

int trevrpc_wt_listen(const trevrpc_wt_config* config, trevrpc_wt_listener** listener);
int trevrpc_wt_listener_port(trevrpc_wt_listener* listener, uint16_t* port);
int trevrpc_wt_listener_accept_session(trevrpc_wt_listener* listener, trevrpc_wt_session** session);
void trevrpc_wt_listener_shutdown(trevrpc_wt_listener* listener);
void trevrpc_wt_listener_close(trevrpc_wt_listener* listener);

int trevrpc_wt_dial(const trevrpc_wt_config* config, trevrpc_wt_session** session);
int trevrpc_wt_accept_session_from_msquic(
    trevrpc_msquic_conn* conn, const trevrpc_wt_config* config, trevrpc_wt_session** session);
int trevrpc_h3_accept_from_msquic(trevrpc_msquic_conn* conn,
    const trevrpc_wt_config* webtransport_config,
    int enable_http3,
    const char* http3_path,
    trevrpc_http3_admission http3_admission,
    void* http3_admission_user_data,
    size_t max_frame_size,
    trevrpc_h3_conn** h3_conn);
int trevrpc_h3_conn_accept_stream(trevrpc_h3_conn* conn, trevrpc_h3_stream** stream);
int trevrpc_h3_stream_resolve(trevrpc_h3_conn* conn,
    trevrpc_h3_stream* stream,
    uint64_t timeout_nanos,
    trevrpc_wt_stream** webtransport_stream,
    int* resolution);
void trevrpc_h3_conn_shutdown(trevrpc_h3_conn* conn);
void trevrpc_h3_conn_close(trevrpc_h3_conn* conn);
int trevrpc_wt_session_accept_stream(trevrpc_wt_session* session, trevrpc_wt_stream** stream);
int trevrpc_wt_session_open_stream(trevrpc_wt_session* session, trevrpc_wt_stream** stream);
void trevrpc_wt_session_shutdown(trevrpc_wt_session* session);
void trevrpc_wt_session_close(trevrpc_wt_session* session);

intptr_t trevrpc_wt_stream_read(trevrpc_wt_stream* stream, uint8_t* data, size_t len);
intptr_t trevrpc_wt_stream_read_frame(trevrpc_wt_stream* stream, uint8_t** body, size_t* len, size_t max_len);
intptr_t trevrpc_wt_stream_read_frame_timeout(
    trevrpc_wt_stream* stream, uint8_t** body, size_t* len, size_t max_len, uint64_t timeout_nanos);
intptr_t trevrpc_wt_stream_read_frame_ready(trevrpc_wt_stream* stream, uint8_t** body, size_t* len, size_t max_len);
intptr_t trevrpc_wt_stream_write(trevrpc_wt_stream* stream, const uint8_t* data, size_t len);
intptr_t trevrpc_wt_stream_write_message_frame(
    trevrpc_wt_stream* stream, const uint8_t* body, size_t body_len, size_t max_len);
intptr_t trevrpc_wt_stream_write_message_frames(
    trevrpc_wt_stream* stream, const uint8_t* bodies, const size_t* body_lens, size_t count, size_t max_len);
int trevrpc_wt_stream_shutdown_send(trevrpc_wt_stream* stream);
int trevrpc_wt_stream_abort(trevrpc_wt_stream* stream, uint32_t error_code);
void trevrpc_wt_stream_close(trevrpc_wt_stream* stream);
intptr_t trevrpc_h3_stream_read_frame(trevrpc_h3_stream* stream, uint8_t** body, size_t* len, size_t max_len);
intptr_t trevrpc_h3_stream_read_frame_timeout(
    trevrpc_h3_stream* stream, uint8_t** body, size_t* len, size_t max_len, uint64_t timeout_nanos);
intptr_t trevrpc_h3_stream_read_frame_ready(trevrpc_h3_stream* stream, uint8_t** body, size_t* len, size_t max_len);
intptr_t trevrpc_h3_stream_write(trevrpc_h3_stream* stream, const uint8_t* data, size_t len);
intptr_t trevrpc_h3_stream_write_fin(trevrpc_h3_stream* stream, const uint8_t* data, size_t len);
int trevrpc_h3_stream_shutdown_send(trevrpc_h3_stream* stream);
int trevrpc_h3_stream_abort(trevrpc_h3_stream* stream);
void trevrpc_h3_stream_close(trevrpc_h3_stream* stream);
void trevrpc_wt_free(void* ptr);

const char* trevrpc_wt_error(int code);

#ifdef __cplusplus
}
#endif

#endif
