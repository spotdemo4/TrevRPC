#include "trevrpc_engine.h"

#include <stddef.h>

int main(void) {
    trevrpc_engine_config_v1 config;
    trevrpc_engine_wake_source_v1 wake;
    trevrpc_engine_endpoint_config_v1 endpoint;
    trevrpc_engine_event_info_v1 event;
    trevrpc_engine_receive_info_v1 receive;
    trevrpc_engine_diagnostics_v1 diagnostics;

    trevrpc_engine_abi_1_anchor();
    return TREVRPC_ENGINE_ABI_VERSION != 1u || trevrpc_engine_abi_version() != 1u ||
           trevrpc_engine_config_v1_init(&config, sizeof(config)) != 0 ||
           trevrpc_engine_wake_source_v1_init(&wake, sizeof(wake)) != 0 ||
           trevrpc_engine_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) != 0 ||
           trevrpc_engine_event_info_v1_init(&event, sizeof(event)) != 0 ||
           trevrpc_engine_receive_info_v1_init(&receive, sizeof(receive)) != 0 ||
           trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) != 0;
}
