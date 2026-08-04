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

int trevrpc_raw_client_connect_v1(const char* host,
    uint16_t port,
    const trevrpc_client_config_v1* config,
    trevrpc_cancellation* cancellation,
    trevrpc_raw_client** client);

int trevrpc_raw_client_connect_webtransport_v1(
    const trevrpc_wt_config* wt_config, const trevrpc_client_config_v1* config, trevrpc_raw_client** client);

int trevrpc_raw_client_call_request_inbound_v1(trevrpc_raw_client* client,
    const trevrpc_request* request,
    const trevrpc_call_options_v1* options,
    trevrpc_inbound_response** response);

int trevrpc_raw_client_start_stream_request_v1(trevrpc_raw_client* client,
    const trevrpc_request* request,
    const trevrpc_call_options_v1* options,
    trevrpc_stream** stream);

void trevrpc_raw_client_shutdown(trevrpc_raw_client* client);
void trevrpc_raw_client_close(trevrpc_raw_client* client);

#ifdef __cplusplus
}
#endif

#endif
