#include "trevrpc_transport.h"

#include <stddef.h>

int main(void) {
    trevrpc_transport_config_v1 config;
    trevrpc_transport_endpoint_config_v1 endpoint;
    trevrpc_transport_wake_source_v1 wake;
    trevrpc_transport_event_info_v1 event;
    trevrpc_transport_receive_info_v1 receive;
    trevrpc_transport_diagnostics_v1 diagnostics;

    trevrpc_transport_abi_1_anchor();
    return TREVRPC_TRANSPORT_ABI_VERSION != 1u || trevrpc_transport_abi_version() != 1u ||
           trevrpc_transport_config_v1_init(&config, sizeof(config)) != 0 ||
           trevrpc_transport_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) != 0 ||
           trevrpc_transport_wake_source_v1_init(&wake, sizeof(wake)) != 0 ||
           trevrpc_transport_event_info_v1_init(&event, sizeof(event)) != 0 ||
           trevrpc_transport_receive_info_v1_init(&receive, sizeof(receive)) != 0 ||
           trevrpc_transport_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) != 0;
}
