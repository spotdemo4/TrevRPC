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

#define TREV_WT_ERR_CLOSED -3001
#define TREV_WT_ERR_FRAME_TOO_LARGE -3002
#define TREV_WT_ERR_REJECTED -3003

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
    uint32_t max_sessions_per_connection;
    uint32_t max_streams_per_session;
    uint64_t max_data_per_session;
    uint32_t stream_recv_window;
    uint32_t conn_flow_control_window;
    uint32_t idle_timeout_ms;
    uint32_t handshake_timeout_ms;
} trevrpc_wt_config;

int trevrpc_wt_listen(const trevrpc_wt_config* config, trevrpc_wt_listener** listener);
int trevrpc_wt_listener_accept_session(trevrpc_wt_listener* listener, trevrpc_wt_session** session);
void trevrpc_wt_listener_shutdown(trevrpc_wt_listener* listener);
void trevrpc_wt_listener_close(trevrpc_wt_listener* listener);

int trevrpc_wt_dial(const trevrpc_wt_config* config, trevrpc_wt_session** session);
int trevrpc_wt_session_accept_stream(trevrpc_wt_session* session, trevrpc_wt_stream** stream);
int trevrpc_wt_session_open_stream(trevrpc_wt_session* session, trevrpc_wt_stream** stream);
void trevrpc_wt_session_close(trevrpc_wt_session* session);

intptr_t trevrpc_wt_stream_read(trevrpc_wt_stream* stream, uint8_t* data, size_t len);
intptr_t trevrpc_wt_stream_read_frame(trevrpc_wt_stream* stream, uint8_t** body, size_t* len, size_t max_len);
intptr_t trevrpc_wt_stream_write(trevrpc_wt_stream* stream, const uint8_t* data, size_t len);
int trevrpc_wt_stream_shutdown_send(trevrpc_wt_stream* stream);
int trevrpc_wt_stream_abort(trevrpc_wt_stream* stream, uint32_t error_code);
void trevrpc_wt_stream_close(trevrpc_wt_stream* stream);
void trevrpc_wt_free(void* ptr);

const char* trevrpc_wt_error(int code);

#ifdef __cplusplus
}
#endif

#endif
