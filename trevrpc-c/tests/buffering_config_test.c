#include "trevrpc.h"
#include "trevrpc_msquic.h"
#include "trevrpc_webtransport.h"

#include <stdio.h>

int trevrpc_test_make_client_msquic_config(const trevrpc_config* config, trevrpc_msquic_config* out_config);
int trevrpc_test_make_server_msquic_config(const trevrpc_server_config* config,
    trevrpc_server_config* out_effective,
    trevrpc_msquic_config* out_msquic_config,
    trevrpc_wt_config* out_wt_config);

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            return 1;                                                                                                  \
        }                                                                                                              \
    } while (0)

int main(void) {
    trevrpc_config client = trevrpc_default_config();
    trevrpc_msquic_config client_msquic = {0};
    trevrpc_server_config server = {0};
    trevrpc_server_config effective_server = {0};
    trevrpc_msquic_config server_msquic = {0};
    trevrpc_wt_config server_wt = {0};

    CHECK(TREVRPC_C_ABI_VERSION == 3u);
    CHECK(trevrpc_c_abi_version() == 3u);
    client.stream_recv_window = 1024u * 1024u;
    client.conn_flow_control_window = 64u * 1024u * 1024u;
    client.msquic_execution_profile = TREV_MSQUIC_EXECUTION_PROFILE_MAX_THROUGHPUT;
    client.msquic_send_buffering_enabled = 1;
    CHECK(trevrpc_test_make_client_msquic_config(&client, &client_msquic) == 0);
    CHECK(client_msquic.stream_recv_window == client.stream_recv_window);
    CHECK(client_msquic.conn_flow_control_window == client.conn_flow_control_window);
    CHECK(client_msquic.execution_profile == client.msquic_execution_profile);
    CHECK(client_msquic.send_buffering_enabled == client.msquic_send_buffering_enabled);
    CHECK(client_msquic.peer_bidi_stream_count == client.peer_bidi_stream_count);
    CHECK(client_msquic.max_frame_size == client.max_frame_size);

    server.peer_bidi_stream_count = 8;
    server.max_streams_per_session = 32;
    server.stream_recv_window = 1024u * 1024u;
    server.conn_flow_control_window = 64u * 1024u * 1024u;
    server.msquic_execution_profile = TREV_MSQUIC_EXECUTION_PROFILE_MAX_THROUGHPUT;
    server.msquic_send_buffering_enabled = 1;
    CHECK(trevrpc_test_make_server_msquic_config(&server, &effective_server, &server_msquic, &server_wt) == 0);
    CHECK(effective_server.max_idle_timeout_ms == 30000);
    CHECK(effective_server.max_streams_per_session == 32);
    CHECK(server_msquic.peer_bidi_stream_count == 32);
    CHECK(server_wt.max_streams_per_session == 32);
    CHECK(server_msquic.stream_recv_window == server.stream_recv_window);
    CHECK(server_msquic.conn_flow_control_window == server.conn_flow_control_window);
    CHECK(server_msquic.execution_profile == TREV_MSQUIC_EXECUTION_PROFILE_MAX_THROUGHPUT);
    CHECK(server_msquic.send_buffering_enabled == 1);
    CHECK(server_msquic.max_pending_send_bytes == TREV_MSQUIC_DEFAULT_MAX_PENDING_SEND_BYTES);
    CHECK(server_msquic.max_pending_send_count == TREV_MSQUIC_DEFAULT_MAX_PENDING_SEND_COUNT);
    CHECK(server_msquic.max_frame_size == TREVRPC_DEFAULT_MAX_FRAME_SIZE);
    return 0;
}
