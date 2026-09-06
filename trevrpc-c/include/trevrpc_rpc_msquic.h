#ifndef TREVRPC_RPC_MSQUIC_H
#define TREVRPC_RPC_MSQUIC_H

#include "trevrpc_rpc.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TREVRPC_RPC_MSQUIC_ABI_VERSION 1u
#define TREVRPC_RPC_MSQUIC_STRUCT_VERSION_1 1u

#define TREVRPC_RPC_MSQUIC_ENDPOINT_LISTENER 1u
#define TREVRPC_RPC_MSQUIC_ENDPOINT_CLIENT 2u

#define TREVRPC_RPC_MSQUIC_TRANSPORT_AUTO 0u
#define TREVRPC_RPC_MSQUIC_TRANSPORT_NATIVE 1u
#define TREVRPC_RPC_MSQUIC_TRANSPORT_HTTP3 2u
#define TREVRPC_RPC_MSQUIC_TRANSPORT_WEBTRANSPORT 3u

#define TREVRPC_RPC_MSQUIC_PROFILE_DRAFT_02 0x00000001u
#define TREVRPC_RPC_MSQUIC_PROFILE_DRAFT_07 0x00000002u
#define TREVRPC_RPC_MSQUIC_PROFILE_DRAFT_14 0x00000004u
#define TREVRPC_RPC_MSQUIC_PROFILE_DRAFT_15 0x00000008u
#define TREVRPC_RPC_MSQUIC_PROFILE_ALL_SUPPORTED 0x0000000fu

#define TREVRPC_RPC_MSQUIC_VERIFY_PEER 0x00000001u
#define TREVRPC_RPC_MSQUIC_REQUIRE_CLIENT_CERTIFICATE 0x00000002u
#define TREVRPC_RPC_MSQUIC_ENABLE_ADMISSION_EVENTS 0x00000004u

typedef struct trevrpc_rpc_msquic_config_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t flags;
    uint32_t reserved0;
    uint64_t reserved[6];
} trevrpc_rpc_msquic_config_v1;

typedef struct trevrpc_rpc_msquic_endpoint_config_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t mode;
    uint32_t transport;

    const char* host;
    uint32_t host_len;
    uint16_t port;
    uint16_t peer_bidi_stream_count;

    const char* server_name;
    uint32_t server_name_len;
    uint32_t flags;

    const char* cert_file;
    uint32_t cert_file_len;
    uint32_t reserved0;
    const char* key_file;
    uint32_t key_file_len;
    uint32_t reserved1;
    const char* ca_cert_file;
    uint32_t ca_cert_file_len;
    uint32_t reserved2;

    const uint8_t* cert_data;
    uint64_t cert_data_len;
    const uint8_t* key_data;
    uint64_t key_data_len;
    const uint8_t* ca_cert_data;
    uint64_t ca_cert_data_len;

    const char* path;
    uint32_t path_len;
    uint32_t webtransport_profiles;
    const char* origin;
    uint32_t origin_len;
    uint32_t max_sessions;

    uint64_t max_frame_size;
    uint64_t max_field_section_size;
    uint64_t max_pending_send_bytes;
    uint64_t max_pending_receive_bytes;
    uint64_t max_idle_timeout_ms;

    uint32_t max_pending_send_count;
    uint32_t max_pending_receive_count;
    uint32_t keep_alive_ms;
    uint32_t stream_recv_window;
    uint32_t conn_flow_control_window;
    uint32_t unresolved_stream_count;
    uint64_t unresolved_stream_bytes;
    uint64_t unresolved_stream_timeout_ms;
    uint64_t reserved[5];
} trevrpc_rpc_msquic_endpoint_config_v1;

uint32_t trevrpc_rpc_msquic_abi_version(void);
void trevrpc_rpc_msquic_abi_1_anchor(void);

int trevrpc_rpc_msquic_config_v1_init(trevrpc_rpc_msquic_config_v1* config, size_t struct_size);
int trevrpc_rpc_msquic_endpoint_config_v1_init(trevrpc_rpc_msquic_endpoint_config_v1* config, size_t struct_size);
int trevrpc_rpc_msquic_create_v1(const trevrpc_rpc_runtime_config_v1* runtime_config,
    const trevrpc_rpc_msquic_config_v1* provider_config,
    trevrpc_rpc_runtime** out_runtime);
int trevrpc_rpc_msquic_endpoint_start_v1(trevrpc_rpc_runtime* runtime,
    const trevrpc_rpc_msquic_endpoint_config_v1* config,
    uint64_t operation_id,
    trevrpc_rpc_endpoint_v1* out_endpoint);

#ifdef __cplusplus
}
#endif

#endif
