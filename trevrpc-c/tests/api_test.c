#include "trevrpc_binding.h"

#include <stdint.h>

static int check_signatures(void) {
    int (*channel_connect)(const char*,
        uint16_t,
        const trevrpc_config*,
        const trevrpc_channel_options*,
        uint64_t,
        trevrpc_cancellation*,
        trevrpc_channel**) = trevrpc_channel_connect;
    int (*channel_unary)(trevrpc_channel*, const char*, const char*, const uint8_t*, size_t, trevrpc_response**) =
        trevrpc_channel_call_unary;
    int (*raw_connect)(const char*, uint16_t, const trevrpc_config*, trevrpc_raw_client**) = trevrpc_raw_client_connect;
    int (*binding_connect)(const char*,
        uint16_t,
        const trevrpc_config*,
        trevrpc_cancellation*,
        trevrpc_raw_client_shutdown_callback,
        void*,
        trevrpc_raw_client**) = trevrpc_raw_client_connect_cancellable_with_shutdown_callback;
    int (*raw_unary)(trevrpc_raw_client*, const char*, const char*, const uint8_t*, size_t, trevrpc_response**) =
        trevrpc_raw_client_call_unary;
    void (*clear_shutdown_callback)(trevrpc_raw_client*) = trevrpc_raw_client_clear_shutdown_callback;
    return channel_connect == NULL || channel_unary == NULL || raw_connect == NULL || binding_connect == NULL ||
           raw_unary == NULL || clear_shutdown_callback == NULL;
}

int main(void) {
    return TREVRPC_C_ABI_VERSION != 5u || trevrpc_c_abi_version() != 5u || check_signatures() != 0;
}
