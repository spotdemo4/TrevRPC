#define _POSIX_C_SOURCE 200809L

#include "trevrpc_rpc_msquic.h"

#include "trevrpc_transport_msquic.h"

#include "trevrpc_msquic_internal.h"
#include "trevrpc_rpc_internal.h"
#include "trevrpc_credential_internal.h"

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>

static int trevrpc_rpc_msquic_initialize_structure(void* structure, size_t struct_size, size_t required_size) {
    uint32_t size_field;
    uint32_t version_field = TREVRPC_RPC_MSQUIC_STRUCT_VERSION_1;
    uint8_t* bytes = structure;
    if (structure == NULL || struct_size < required_size) {
        return -EINVAL;
    }
    if (struct_size > UINT32_MAX) {
        return -EOVERFLOW;
    }
    size_field = (uint32_t)struct_size;
    memset(structure, 0, struct_size);
    memcpy(bytes, &size_field, sizeof(size_field));
    memcpy(bytes + sizeof(size_field), &version_field, sizeof(version_field));
    return 0;
}

uint32_t trevrpc_rpc_msquic_abi_version(void) {
    return TREVRPC_RPC_MSQUIC_ABI_VERSION;
}

void trevrpc_rpc_msquic_abi_1_anchor(void) {
}

int trevrpc_rpc_msquic_config_v1_init(trevrpc_rpc_msquic_config_v1* config, size_t struct_size) {
    return trevrpc_rpc_msquic_initialize_structure(config, struct_size, sizeof(*config));
}

int trevrpc_rpc_msquic_endpoint_config_v1_init(trevrpc_rpc_msquic_endpoint_config_v1* config, size_t struct_size) {
    int result = trevrpc_rpc_msquic_initialize_structure(config, struct_size, sizeof(*config));
    if (result == 0) {
        config->transport = TREVRPC_RPC_MSQUIC_TRANSPORT_AUTO;
        config->peer_bidi_stream_count = 100;
        config->flags = TREVRPC_RPC_MSQUIC_VERIFY_PEER;
        config->webtransport_profiles = TREVRPC_RPC_MSQUIC_PROFILE_ALL_SUPPORTED;
        config->max_sessions = 1;
        config->max_frame_size = TREVRPC_RPC_DEFAULT_MAX_MESSAGE_SIZE;
        config->max_field_section_size = 64u * 1024u;
        config->max_pending_send_bytes = TREVRPC_RPC_DEFAULT_RECEIVE_OWNED_BYTES;
        config->max_pending_receive_bytes = TREVRPC_RPC_DEFAULT_RECEIVE_OWNED_BYTES;
        config->max_pending_send_count = 1024;
        config->max_pending_receive_count = TREVRPC_RPC_DEFAULT_RECEIVE_OWNED_COUNT;
        config->unresolved_stream_count = 64;
        config->unresolved_stream_bytes = TREVRPC_RPC_DEFAULT_RECEIVE_OWNED_BYTES;
        config->unresolved_stream_timeout_ms = 5000;
    }
    return result;
}

static int trevrpc_rpc_msquic_validate_provider_config(const trevrpc_rpc_msquic_config_v1* config) {
    size_t index;
    if (config == NULL || config->struct_size < sizeof(*config)) {
        return -EINVAL;
    }
    if (config->struct_version != TREVRPC_RPC_MSQUIC_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (config->flags != 0 || config->reserved0 != 0) {
        return -EINVAL;
    }
    for (index = 0; index < sizeof(config->reserved) / sizeof(config->reserved[0]); ++index) {
        if (config->reserved[index] != 0) {
            return -EINVAL;
        }
    }
    return 0;
}

static int trevrpc_rpc_msquic_validate_endpoint_config(const trevrpc_rpc_msquic_endpoint_config_v1* config) {
    size_t index;
    int result;
    if (config == NULL || config->struct_size < sizeof(*config)) {
        return -EINVAL;
    }
    if (config->struct_version != TREVRPC_RPC_MSQUIC_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if ((config->mode != TREVRPC_RPC_MSQUIC_ENDPOINT_LISTENER && config->mode != TREVRPC_RPC_MSQUIC_ENDPOINT_CLIENT) ||
        config->transport > TREVRPC_RPC_MSQUIC_TRANSPORT_WEBTRANSPORT ||
        (config->flags & ~(TREVRPC_RPC_MSQUIC_VERIFY_PEER | TREVRPC_RPC_MSQUIC_REQUIRE_CLIENT_CERTIFICATE |
                             TREVRPC_RPC_MSQUIC_ENABLE_ADMISSION_EVENTS)) != 0 ||
        ((config->flags & TREVRPC_RPC_MSQUIC_ENABLE_ADMISSION_EVENTS) != 0 &&
            (config->mode != TREVRPC_RPC_MSQUIC_ENDPOINT_LISTENER ||
                (config->transport != TREVRPC_RPC_MSQUIC_TRANSPORT_HTTP3 &&
                    config->transport != TREVRPC_RPC_MSQUIC_TRANSPORT_WEBTRANSPORT))) ||
        config->reserved0 != 0 || config->reserved1 != 0 || config->reserved2 != 0 ||
        (config->host == NULL && config->host_len != 0) ||
        (config->server_name == NULL && config->server_name_len != 0) ||
        (config->cert_file == NULL && config->cert_file_len != 0) ||
        (config->key_file == NULL && config->key_file_len != 0) ||
        (config->ca_cert_file == NULL && config->ca_cert_file_len != 0) ||
        (config->cert_data == NULL && config->cert_data_len != 0) ||
        (config->key_data == NULL && config->key_data_len != 0) ||
        (config->ca_cert_data == NULL && config->ca_cert_data_len != 0) ||
        (config->path == NULL && config->path_len != 0) || (config->origin == NULL && config->origin_len != 0) ||
        (config->webtransport_profiles & ~TREVRPC_RPC_MSQUIC_PROFILE_ALL_SUPPORTED) != 0 ||
        (config->unresolved_stream_bytes != 0 &&
            config->unresolved_stream_bytes < trevrpc_msquic_receive_minimum_raw_bytes())) {
        return -EINVAL;
    }
    for (index = 0; index < sizeof(config->reserved) / sizeof(config->reserved[0]); ++index) {
        if (config->reserved[index] != 0) {
            return -EINVAL;
        }
    }
    if (config->transport == TREVRPC_RPC_MSQUIC_TRANSPORT_AUTO ||
        config->transport == TREVRPC_RPC_MSQUIC_TRANSPORT_NATIVE) {
        if (config->server_name_len != 0 &&
            (config->mode == TREVRPC_RPC_MSQUIC_ENDPOINT_LISTENER || config->server_name_len != config->host_len ||
                memcmp(config->server_name, config->host, config->host_len) != 0))
            return -ENOTSUP;
    }
    if ((config->flags & TREVRPC_RPC_MSQUIC_REQUIRE_CLIENT_CERTIFICATE) != 0)
        return -ENOTSUP;
    result = trevrpc_credential_validate_endpoint(config->cert_file,
        config->cert_file_len,
        config->key_file,
        config->key_file_len,
        config->ca_cert_file,
        config->ca_cert_file_len,
        config->cert_data,
        config->cert_data_len,
        config->key_data,
        config->key_data_len,
        config->ca_cert_data,
        config->ca_cert_data_len,
        config->mode == TREVRPC_RPC_MSQUIC_ENDPOINT_LISTENER);
    return result;
}

int trevrpc_rpc_msquic_create_v1(const trevrpc_rpc_runtime_config_v1* runtime_config,
    const trevrpc_rpc_msquic_config_v1* provider_config,
    trevrpc_rpc_runtime** out_runtime) {
    trevrpc_transport_config_v1 transport_config;
    trevrpc_transport_msquic_config_v1 transport_provider_config;
    trevrpc_transport* transport = NULL;
    int result;
    if (runtime_config == NULL || out_runtime == NULL) {
        return -EINVAL;
    }
    result = trevrpc_rpc_validate_runtime_config(runtime_config);
    if (result != 0) {
        return result;
    }
    result = trevrpc_rpc_msquic_validate_provider_config(provider_config);
    if (result != 0) {
        return result;
    }
    result = trevrpc_transport_config_v1_init(&transport_config, sizeof(transport_config));
    if (result != 0) {
        return result;
    }
    transport_config.event_capacity = runtime_config->event_capacity;
    transport_config.listener_capacity = runtime_config->endpoint_capacity;
    transport_config.connection_capacity = runtime_config->endpoint_capacity;
    transport_config.stream_capacity = runtime_config->stream_capacity;
    transport_config.max_receive_owned_count = runtime_config->max_receive_owned_count;
    transport_config.max_receive_owned_bytes = runtime_config->max_receive_owned_bytes;
    result = trevrpc_transport_msquic_config_v1_init(&transport_provider_config, sizeof(transport_provider_config));
    if (result != 0) {
        return result;
    }
    result = trevrpc_transport_msquic_create_v1(&transport_config, &transport_provider_config, &transport);
    if (result != 0) {
        return result;
    }
    result = trevrpc_rpc_runtime_adopt_transport_v1(runtime_config, transport, out_runtime);
    if (result != 0) {
        trevrpc_rpc_transport_destroy(transport);
    }
    return result;
}

int trevrpc_rpc_msquic_endpoint_start_v1(trevrpc_rpc_runtime* runtime,
    const trevrpc_rpc_msquic_endpoint_config_v1* config,
    uint64_t operation_id,
    trevrpc_rpc_endpoint_v1* out_endpoint) {
    static const uint8_t native_alpn[] = "trevrpc/1";
    static const uint8_t h3_alpn[] = "h3";
    trevrpc_rpc_transport_endpoint_config transport_config = {0};
    int result = trevrpc_rpc_msquic_validate_endpoint_config(config);
    if (result != 0) {
        return result;
    }
    transport_config.protocol = config->transport;
    transport_config.host = config->host;
    transport_config.host_len = config->host_len;
    transport_config.port = config->port;
    transport_config.peer_bidi_stream_count = config->peer_bidi_stream_count;
    transport_config.server_name = config->server_name;
    transport_config.server_name_len = config->server_name_len;
    transport_config.alpn = config->transport == TREVRPC_RPC_MSQUIC_TRANSPORT_HTTP3 ||
                                    config->transport == TREVRPC_RPC_MSQUIC_TRANSPORT_WEBTRANSPORT
                                ? h3_alpn
                                : native_alpn;
    transport_config.alpn_len = config->transport == TREVRPC_RPC_MSQUIC_TRANSPORT_HTTP3 ||
                                        config->transport == TREVRPC_RPC_MSQUIC_TRANSPORT_WEBTRANSPORT
                                    ? sizeof(h3_alpn) - 1
                                    : sizeof(native_alpn) - 1;
    transport_config.cert_file = config->cert_file;
    transport_config.cert_file_len = config->cert_file_len;
    transport_config.key_file = config->key_file;
    transport_config.key_file_len = config->key_file_len;
    transport_config.ca_cert_file = config->ca_cert_file;
    transport_config.ca_cert_file_len = config->ca_cert_file_len;
    transport_config.cert_data = config->cert_data;
    transport_config.cert_data_len = config->cert_data_len;
    transport_config.key_data = config->key_data;
    transport_config.key_data_len = config->key_data_len;
    transport_config.ca_cert_data = config->ca_cert_data;
    transport_config.ca_cert_data_len = config->ca_cert_data_len;
    transport_config.path = config->path;
    transport_config.path_len = config->path_len;
    transport_config.origin = config->origin;
    transport_config.origin_len = config->origin_len;
    transport_config.webtransport_profiles = config->webtransport_profiles;
    transport_config.max_sessions = config->max_sessions;
    transport_config.max_pending_send_count = config->max_pending_send_count;
    transport_config.max_pending_receive_count = config->max_pending_receive_count;
    transport_config.max_pending_send_bytes = config->max_pending_send_bytes;
    transport_config.max_pending_receive_bytes = config->max_pending_receive_bytes;
    transport_config.max_frame_size = config->max_frame_size;
    transport_config.max_field_section_size = config->max_field_section_size;
    transport_config.max_idle_timeout_ms = config->max_idle_timeout_ms;
    transport_config.keep_alive_ms = config->keep_alive_ms;
    transport_config.stream_recv_window = config->stream_recv_window;
    transport_config.conn_flow_control_window = config->conn_flow_control_window;
    transport_config.unresolved_stream_count = config->unresolved_stream_count;
    transport_config.unresolved_stream_bytes = config->unresolved_stream_bytes;
    transport_config.unresolved_stream_timeout_ms = config->unresolved_stream_timeout_ms;
    if ((config->flags & TREVRPC_RPC_MSQUIC_VERIFY_PEER) == 0) {
        transport_config.flags |= TREVRPC_RPC_TRANSPORT_ENDPOINT_SKIP_CERTIFICATE_VALIDATION;
    }
    if ((config->flags & TREVRPC_RPC_MSQUIC_ENABLE_ADMISSION_EVENTS) != 0) {
        transport_config.flags |= TREVRPC_RPC_TRANSPORT_ENDPOINT_DEFER_ADMISSION;
    }
    return trevrpc_rpc_runtime_start_transport_endpoint_v1(runtime,
        &transport_config,
        config->mode == TREVRPC_RPC_MSQUIC_ENDPOINT_LISTENER ? TREVRPC_RPC_ENDPOINT_SERVER
                                                             : TREVRPC_RPC_ENDPOINT_CLIENT,
        operation_id,
        out_endpoint);
}
