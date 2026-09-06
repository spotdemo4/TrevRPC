#include "trevrpc_transport_msquic.h"

#include <errno.h>
#include <stddef.h>

int main(void) {
    trevrpc_transport_config_v1 transport_config;
    trevrpc_transport_msquic_config_v1 provider_config;
    trevrpc_transport* transport = NULL;

    trevrpc_transport_abi_1_anchor();
    trevrpc_transport_msquic_abi_1_anchor();
    if (TREVRPC_TRANSPORT_ABI_VERSION != 1u || TREVRPC_TRANSPORT_MSQUIC_ABI_VERSION != 1u ||
        trevrpc_transport_abi_version() != 1u || trevrpc_transport_msquic_abi_version() != 1u ||
        trevrpc_transport_config_v1_init(&transport_config, sizeof(transport_config)) != 0 ||
        trevrpc_transport_msquic_config_v1_init(&provider_config, sizeof(provider_config)) != 0) {
        return 1;
    }
    return trevrpc_transport_msquic_create_v1(NULL, &provider_config, &transport) == -EINVAL && transport == NULL ? 0
                                                                                                                  : 1;
}
