#include "trevrpc_rpc_msquic.h"

#include <errno.h>
#include <poll.h>
#include <stdbool.h>

int main(void) {
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_msquic_config_v1 provider_config;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_runtime* runtime = NULL;
    bool stopped = false;

    trevrpc_rpc_msquic_abi_1_anchor();
    if (trevrpc_rpc_msquic_abi_version() != TREVRPC_RPC_MSQUIC_ABI_VERSION ||
        trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) != 0 ||
        trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config)) != 0 ||
        trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &runtime) != 0 || runtime == NULL ||
        trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) != 0 ||
        trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) != 0 || trevrpc_rpc_runtime_close(runtime, 1) != 0)
        return 1;

    for (int attempt = 0; attempt < 100 && !stopped; ++attempt) {
        struct pollfd descriptor = {.fd = (int)wake.native_handle, .events = POLLIN, .revents = 0};
        if (poll(&descriptor, 1, 100) < 0)
            return 1;
        for (;;) {
            trevrpc_rpc_event_info_v1 info;
            trevrpc_rpc_event* event = NULL;
            int result = trevrpc_rpc_runtime_next_event(runtime, &event);
            if (result == -EAGAIN)
                break;
            if (result != 0 || trevrpc_rpc_event_info_v1_init(&info, sizeof(info)) != 0 ||
                trevrpc_rpc_event_get_info_v1(event, &info) != 0) {
                trevrpc_rpc_event_release(event);
                return 1;
            }
            if (info.kind == TREVRPC_RPC_EVENT_STOPPED && info.operation_id == 1)
                stopped = true;
            trevrpc_rpc_event_release(event);
        }
    }
    return stopped && trevrpc_rpc_runtime_drain(runtime) == 0 && trevrpc_rpc_runtime_release(runtime) == 0 ? 0 : 1;
}
