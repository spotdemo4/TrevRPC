#include "trevrpc_rpc.h"

#include <errno.h> // IWYU pragma: keep
#include <limits.h>
#include <stdint.h>
#include <string.h>

static int trevrpc_rpc_initialize_structure(void* structure, size_t struct_size, size_t required_size) {
    uint32_t size_field;
    uint32_t version_field = TREVRPC_RPC_STRUCT_VERSION_1;
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

uint32_t trevrpc_rpc_abi_version(void) {
    return TREVRPC_RPC_ABI_VERSION;
}

void trevrpc_rpc_abi_1_anchor(void) {
}

int trevrpc_rpc_runtime_config_v1_init(trevrpc_rpc_runtime_config_v1* config, size_t struct_size) {
    int result = trevrpc_rpc_initialize_structure(config, struct_size, sizeof(*config));
    if (result == 0) {
        config->event_capacity = TREVRPC_RPC_DEFAULT_EVENT_CAPACITY;
        config->endpoint_capacity = TREVRPC_RPC_DEFAULT_ENDPOINT_CAPACITY;
        config->call_capacity = TREVRPC_RPC_DEFAULT_CALL_CAPACITY;
        config->stream_capacity = TREVRPC_RPC_DEFAULT_STREAM_CAPACITY;
        config->max_receive_owned_count = TREVRPC_RPC_DEFAULT_RECEIVE_OWNED_COUNT;
        config->max_metadata_count = TREVRPC_RPC_DEFAULT_MAX_METADATA_COUNT;
        config->max_status_message_size = TREVRPC_RPC_DEFAULT_MAX_STATUS_MESSAGE_SIZE;
        config->max_receive_owned_bytes = TREVRPC_RPC_DEFAULT_RECEIVE_OWNED_BYTES;
        config->max_message_size = TREVRPC_RPC_DEFAULT_MAX_MESSAGE_SIZE;
        config->max_metadata_bytes = TREVRPC_RPC_DEFAULT_MAX_METADATA_BYTES;
        config->initial_request_timeout_nanos = 0;
        config->max_stream_messages = -1;
        config->max_stream_body_size = -1;
        config->stream_idle_timeout_nanos = 0;
    }
    return result;
}

int trevrpc_rpc_wake_source_v1_init(trevrpc_rpc_wake_source_v1* wake_source, size_t struct_size) {
    return trevrpc_rpc_initialize_structure(wake_source, struct_size, sizeof(*wake_source));
}

int trevrpc_rpc_call_config_v1_init(trevrpc_rpc_call_config_v1* config, size_t struct_size) {
    int result = trevrpc_rpc_initialize_structure(config, struct_size, sizeof(*config));
    if (result == 0) {
        config->timeout_nanos = TREVRPC_RPC_DEADLINE_INFINITE;
        config->max_response_body_size = -1;
        config->max_response_messages = -1;
        config->max_response_stream_body_size = -1;
        config->response_idle_timeout_nanos = 0;
    }
    return result;
}

int trevrpc_rpc_call_context_info_v1_init(trevrpc_rpc_call_context_info_v1* info, size_t struct_size) {
    return trevrpc_rpc_initialize_structure(info, struct_size, sizeof(*info));
}

int trevrpc_rpc_admission_info_v1_init(trevrpc_rpc_admission_info_v1* info, size_t struct_size) {
    return trevrpc_rpc_initialize_structure(info, struct_size, sizeof(*info));
}

int trevrpc_rpc_status_v1_init(trevrpc_rpc_status_v1* status, size_t struct_size) {
    return trevrpc_rpc_initialize_structure(status, struct_size, sizeof(*status));
}

int trevrpc_rpc_event_info_v1_init(trevrpc_rpc_event_info_v1* info, size_t struct_size) {
    return trevrpc_rpc_initialize_structure(info, struct_size, sizeof(*info));
}

int trevrpc_rpc_receive_info_v1_init(trevrpc_rpc_receive_info_v1* info, size_t struct_size) {
    return trevrpc_rpc_initialize_structure(info, struct_size, sizeof(*info));
}

int trevrpc_rpc_diagnostics_v1_init(trevrpc_rpc_diagnostics_v1* diagnostics, size_t struct_size) {
    return trevrpc_rpc_initialize_structure(diagnostics, struct_size, sizeof(*diagnostics));
}
