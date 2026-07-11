#ifndef TREVRPC_RAW_H
#define TREVRPC_RAW_H

#include "trevrpc.h"
#include "trevrpc_webtransport.h"

#ifdef __cplusplus
extern "C" {
#endif

/* Advanced single-connection API. These calls never reconnect or replay an RPC. */
typedef struct trevrpc_raw_client trevrpc_raw_client;

/* Advanced channel tuning. Routine channels use the fixed bounded reconnect policy. */
int trevrpc_channel_options_set_backoff(
    trevrpc_channel_options* options, uint64_t initial_backoff_ms, uint64_t max_backoff_ms, uint32_t jitter_percent);

int trevrpc_raw_client_connect(
    const char* host, uint16_t port, const trevrpc_config* config, trevrpc_raw_client** client);
int trevrpc_raw_client_connect_cancellable(const char* host,
    uint16_t port,
    const trevrpc_config* config,
    trevrpc_cancellation* cancellation,
    trevrpc_raw_client** client);
int trevrpc_raw_client_connect_webtransport(
    const trevrpc_wt_config* wt_config, const trevrpc_config* config, trevrpc_raw_client** client);
int trevrpc_raw_client_call_unary(trevrpc_raw_client* client,
    const char* service,
    const char* method,
    const uint8_t* body,
    size_t body_len,
    trevrpc_response** response);
int trevrpc_raw_client_call_unary_with_options(trevrpc_raw_client* client,
    const char* service,
    const char* method,
    const uint8_t* body,
    size_t body_len,
    const trevrpc_call_options* options,
    trevrpc_response** response);
int trevrpc_raw_client_call_request(
    trevrpc_raw_client* client, const trevrpc_request* request, trevrpc_response** response);
int trevrpc_raw_client_call_request_cancellable(trevrpc_raw_client* client,
    const trevrpc_request* request,
    trevrpc_cancellation* cancellation,
    trevrpc_response** response);
/* The request body is borrowed until SEND_COMPLETE drains. */
int trevrpc_raw_client_call_request_borrowed_cancellable(trevrpc_raw_client* client,
    const trevrpc_request* request,
    trevrpc_cancellation* cancellation,
    trevrpc_response** response);
int trevrpc_raw_client_call_request_with_options(trevrpc_raw_client* client,
    const trevrpc_request* request,
    const trevrpc_call_options* options,
    trevrpc_response** response);
int trevrpc_raw_client_start_stream(trevrpc_raw_client* client,
    const char* service,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len,
    trevrpc_stream** stream);
int trevrpc_raw_client_start_stream_with_options(trevrpc_raw_client* client,
    const char* service,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len,
    const trevrpc_call_options* options,
    trevrpc_stream** stream);
int trevrpc_raw_client_start_stream_request(
    trevrpc_raw_client* client, const trevrpc_request* request, trevrpc_stream** stream);
int trevrpc_raw_client_start_stream_request_cancellable(trevrpc_raw_client* client,
    const trevrpc_request* request,
    trevrpc_cancellation* cancellation,
    trevrpc_stream** stream);
/* The request body is borrowed until SEND_COMPLETE drains. */
int trevrpc_raw_client_start_stream_request_borrowed_cancellable(trevrpc_raw_client* client,
    const trevrpc_request* request,
    trevrpc_cancellation* cancellation,
    trevrpc_stream** stream);
int trevrpc_raw_client_start_stream_request_with_options(trevrpc_raw_client* client,
    const trevrpc_request* request,
    const trevrpc_call_options* options,
    trevrpc_stream** stream);
void trevrpc_raw_client_shutdown(trevrpc_raw_client* client);
void trevrpc_raw_client_close(trevrpc_raw_client* client);

#ifdef __cplusplus
}
#endif

#endif
