#include "trevrpc_transport_msquic.h"

#include "trevrpc_engine_msquic.h"
#include "trevrpc_rpc_transport_engine_internal.h"
#include "trevrpc_rpc_transport_h3_internal.h"
#include "trevrpc_rpc_transport_msquic_internal.h"

#include <errno.h>
#include <limits.h>
#include <stdint.h>
#include <string.h>

static int trevrpc_transport_msquic_initialize_structure(void* structure, size_t struct_size, size_t required_size) {
    uint32_t size_field;
    uint32_t version_field = TREVRPC_TRANSPORT_MSQUIC_STRUCT_VERSION_1;
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

static int trevrpc_transport_msquic_validate_transport_config(const trevrpc_transport_config_v1* config) {
    size_t index;
    if (config == NULL || config->struct_size < sizeof(*config)) {
        return -EINVAL;
    }
    if (config->struct_version != TREVRPC_TRANSPORT_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (config->event_capacity == 0 || config->listener_capacity == 0 || config->connection_capacity == 0 ||
        config->stream_capacity == 0 || config->max_receive_owned_count == 0 || config->max_receive_owned_bytes == 0 ||
        config->flags != 0) {
        return -EINVAL;
    }
    for (index = 0; index < sizeof(config->reserved) / sizeof(config->reserved[0]); ++index) {
        if (config->reserved[index] != 0) {
            return -EINVAL;
        }
    }
    return 0;
}

static int trevrpc_transport_msquic_validate_provider_config(const trevrpc_transport_msquic_config_v1* config) {
    size_t index;
    if (config == NULL || config->struct_size < sizeof(*config)) {
        return -EINVAL;
    }
    if (config->struct_version != TREVRPC_TRANSPORT_MSQUIC_STRUCT_VERSION_1) {
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

uint32_t trevrpc_transport_msquic_abi_version(void) {
    return TREVRPC_TRANSPORT_MSQUIC_ABI_VERSION;
}

void trevrpc_transport_msquic_abi_1_anchor(void) {
}

int trevrpc_transport_msquic_config_v1_init(trevrpc_transport_msquic_config_v1* config, size_t struct_size) {
    return trevrpc_transport_msquic_initialize_structure(config, struct_size, sizeof(*config));
}

int trevrpc_transport_msquic_create_v1(const trevrpc_transport_config_v1* transport_config,
    const trevrpc_transport_msquic_config_v1* provider_config,
    trevrpc_transport** out_transport) {
    trevrpc_engine_config_v1 engine_config;
    trevrpc_engine_msquic_config_v1 engine_provider_config;
    trevrpc_engine* engine = NULL;
    trevrpc_rpc_transport* native_transport = NULL;
    trevrpc_rpc_transport* h3_transport = NULL;
    trevrpc_rpc_transport* composite = NULL;
    trevrpc_rpc_transport_config internal_config;
    int result;
    if (out_transport == NULL) {
        return -EINVAL;
    }
    result = trevrpc_transport_msquic_validate_transport_config(transport_config);
    if (result != 0) {
        return result;
    }
    result = trevrpc_transport_msquic_validate_provider_config(provider_config);
    if (result != 0) {
        return result;
    }
    result = trevrpc_engine_config_v1_init(&engine_config, sizeof(engine_config));
    if (result != 0) {
        return result;
    }
    engine_config.event_capacity = transport_config->event_capacity;
    engine_config.listener_capacity = transport_config->listener_capacity;
    engine_config.connection_capacity = transport_config->connection_capacity;
    engine_config.stream_capacity = transport_config->stream_capacity;
    engine_config.max_receive_owned_count = transport_config->max_receive_owned_count;
    engine_config.max_receive_owned_bytes = transport_config->max_receive_owned_bytes;
    result = trevrpc_engine_msquic_config_v1_init(&engine_provider_config, sizeof(engine_provider_config));
    if (result != 0) {
        return result;
    }
    result = trevrpc_engine_msquic_create_v1(&engine_config, &engine_provider_config, &engine);
    if (result != 0) {
        return result;
    }
    result = trevrpc_rpc_transport_engine_adopt(engine, &native_transport);
    if (result != 0) {
        (void)trevrpc_engine_release(engine);
        return result;
    }
    internal_config.event_capacity = transport_config->event_capacity;
    internal_config.listener_capacity = transport_config->listener_capacity;
    internal_config.connection_capacity = transport_config->connection_capacity;
    internal_config.stream_capacity = transport_config->stream_capacity;
    internal_config.max_receive_owned_count = transport_config->max_receive_owned_count;
    internal_config.max_receive_owned_bytes = transport_config->max_receive_owned_bytes;
    result = trevrpc_rpc_transport_h3_create(&internal_config, &h3_transport);
    if (result != 0) {
        trevrpc_rpc_transport_destroy(native_transport);
        return result;
    }
    result = trevrpc_rpc_transport_msquic_adopt(native_transport, h3_transport, &internal_config, &composite);
    if (result != 0) {
        trevrpc_rpc_transport_destroy(native_transport);
        trevrpc_rpc_transport_destroy(h3_transport);
        return result;
    }
    *out_transport = composite;
    return 0;
}
