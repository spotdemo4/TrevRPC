#include "trevrpc_rpc_msquic.h"

#include <assert.h>
#include <errno.h> // IWYU pragma: keep
#include <poll.h>
#include <stdbool.h>

static void check_factory_config_validation(void) {
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_msquic_config_v1 provider_config;
    trevrpc_rpc_runtime* runtime = NULL;

    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    assert(trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    assert(trevrpc_rpc_msquic_create_v1(NULL, &provider_config, &runtime) == -EINVAL);
    assert(runtime == NULL);

    runtime_config.struct_size = sizeof(runtime_config) - 1;
    assert(trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &runtime) == -EINVAL);
    assert(runtime == NULL);

    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    runtime_config.struct_version++;
    assert(trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &runtime) == -ENOTSUP);
    assert(runtime == NULL);

    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    runtime_config.reserved[0] = 1;
    assert(trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &runtime) == -EINVAL);
    assert(runtime == NULL);

    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    assert(trevrpc_rpc_msquic_create_v1(&runtime_config, NULL, &runtime) == -EINVAL);
    assert(runtime == NULL);

    provider_config.struct_size = sizeof(provider_config) - 1;
    assert(trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &runtime) == -EINVAL);
    assert(runtime == NULL);

    assert(trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    provider_config.struct_version++;
    assert(trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &runtime) == -ENOTSUP);
    assert(runtime == NULL);

    assert(trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    provider_config.reserved0 = 1;
    assert(trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &runtime) == -EINVAL);
    assert(runtime == NULL);

    assert(trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    provider_config.reserved[0] = 1;
    assert(trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &runtime) == -EINVAL);
    assert(runtime == NULL);
}

int main(void) {
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_msquic_config_v1 provider_config;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_runtime* runtime = NULL;
    bool stopped = false;
    int attempt;

    check_factory_config_validation();
    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    assert(trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    assert(trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &runtime) == 0);
    assert(runtime != NULL);

    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);
    assert(wake.kind == TREVRPC_RPC_WAKE_SOURCE_POSIX_FD);
    assert((wake.flags & TREVRPC_RPC_WAKE_FLAG_BORROWED) != 0);
    assert((wake.flags & TREVRPC_RPC_WAKE_FLAG_LEVEL_TRIGGERED) != 0);

    assert(trevrpc_rpc_runtime_close(runtime, 1) == 0);
    assert(trevrpc_rpc_runtime_close(runtime, 2) == -EALREADY);

    for (attempt = 0; attempt < 100 && !stopped; ++attempt) {
        struct pollfd descriptor = {.fd = (int)wake.native_handle, .events = POLLIN, .revents = 0};
        int poll_result = poll(&descriptor, 1, 100);
        assert(poll_result >= 0);
        for (;;) {
            trevrpc_rpc_event_info_v1 info;
            trevrpc_rpc_event* event = NULL;
            int result = trevrpc_rpc_runtime_next_event(runtime, &event);
            if (result == -EAGAIN) {
                break;
            }
            assert(result == 0);
            assert(trevrpc_rpc_event_info_v1_init(&info, sizeof(info)) == 0);
            assert(trevrpc_rpc_event_get_info_v1(event, &info) == 0);
            if (info.kind == TREVRPC_RPC_EVENT_STOPPED) {
                assert(info.operation_id == 1);
                stopped = true;
            }
            trevrpc_rpc_event_release(event);
        }
    }
    assert(stopped);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
    return 0;
}
