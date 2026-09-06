#include "trevrpc_transport_msquic.h"

#include <assert.h>
#include <errno.h> // NOLINT(misc-include-cleaner)
#include <poll.h>
#include <stdbool.h>

_Static_assert(sizeof(trevrpc_transport_msquic_config_v1) == 64, "trevrpc_transport_msquic_config_v1 size");
_Static_assert(_Alignof(trevrpc_transport_msquic_config_v1) == 8, "trevrpc_transport_msquic_config_v1 align");

static void check_factory_config_validation(void) {
    trevrpc_transport_config_v1 transport_config;
    trevrpc_transport_msquic_config_v1 provider_config;
    trevrpc_transport* transport = NULL;

    assert(trevrpc_transport_config_v1_init(&transport_config, sizeof(transport_config)) == 0);
    assert(trevrpc_transport_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    assert(trevrpc_transport_msquic_create_v1(NULL, &provider_config, &transport) == -EINVAL);
    assert(transport == NULL);

    transport_config.struct_size = sizeof(transport_config) - 1;
    assert(trevrpc_transport_msquic_create_v1(&transport_config, &provider_config, &transport) == -EINVAL);
    assert(transport == NULL);

    assert(trevrpc_transport_config_v1_init(&transport_config, sizeof(transport_config)) == 0);
    transport_config.struct_version++;
    assert(trevrpc_transport_msquic_create_v1(&transport_config, &provider_config, &transport) == -ENOTSUP);
    assert(transport == NULL);

    assert(trevrpc_transport_config_v1_init(&transport_config, sizeof(transport_config)) == 0);
    transport_config.reserved[0] = 1;
    assert(trevrpc_transport_msquic_create_v1(&transport_config, &provider_config, &transport) == -EINVAL);
    assert(transport == NULL);

    assert(trevrpc_transport_config_v1_init(&transport_config, sizeof(transport_config)) == 0);
    assert(trevrpc_transport_msquic_create_v1(&transport_config, NULL, &transport) == -EINVAL);
    assert(transport == NULL);

    provider_config.struct_size = sizeof(provider_config) - 1;
    assert(trevrpc_transport_msquic_create_v1(&transport_config, &provider_config, &transport) == -EINVAL);
    assert(transport == NULL);

    assert(trevrpc_transport_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    provider_config.struct_version++;
    assert(trevrpc_transport_msquic_create_v1(&transport_config, &provider_config, &transport) == -ENOTSUP);
    assert(transport == NULL);

    assert(trevrpc_transport_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    provider_config.reserved[0] = 1;
    assert(trevrpc_transport_msquic_create_v1(&transport_config, &provider_config, &transport) == -EINVAL);
    assert(transport == NULL);
}

int main(void) {
    trevrpc_transport_config_v1 transport_config;
    trevrpc_transport_msquic_config_v1 provider_config;
    trevrpc_transport_endpoint_config_v1 endpoint_config;
    trevrpc_transport_wake_source_v1 wakes[TREVRPC_TRANSPORT_MAX_WAKE_SOURCES];
    trevrpc_transport_handle_v1 listener = {0};
    trevrpc_transport* transport = NULL;
    size_t wake_count = 0;
    bool stopped = false;
    uint64_t last_sequence = 0;
    int attempt;
    size_t index;

    check_factory_config_validation();
    assert(TREVRPC_TRANSPORT_MSQUIC_ABI_VERSION == 1u);
    assert(trevrpc_transport_msquic_abi_version() == 1u);
    trevrpc_transport_msquic_abi_1_anchor();
    assert(trevrpc_transport_config_v1_init(&transport_config, sizeof(transport_config)) == 0);
    assert(trevrpc_transport_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    assert(trevrpc_transport_msquic_create_v1(&transport_config, &provider_config, &transport) == 0);
    assert(transport != NULL);
    assert(trevrpc_transport_release(transport) == -EAGAIN);

    assert(trevrpc_transport_endpoint_config_v1_init(&endpoint_config, sizeof(endpoint_config)) == 0);
    endpoint_config.protocol = TREVRPC_TRANSPORT_PROTOCOL_HTTP3;
    endpoint_config.host = "not-an-ip";
    endpoint_config.host_len = 9;
    assert(trevrpc_transport_listen_v1(transport, &endpoint_config, &listener) == -EINVAL);

    assert(trevrpc_transport_endpoint_config_v1_init(&endpoint_config, sizeof(endpoint_config)) == 0);
    endpoint_config.protocol = TREVRPC_TRANSPORT_PROTOCOL_NATIVE;
    endpoint_config.server_name = "example.test";
    endpoint_config.server_name_len = 12;
    assert(trevrpc_transport_listen_v1(transport, &endpoint_config, &listener) == -ENOTSUP);

    for (index = 0; index < TREVRPC_TRANSPORT_MAX_WAKE_SOURCES; ++index) {
        assert(trevrpc_transport_wake_source_v1_init(&wakes[index], sizeof(wakes[index])) == 0);
    }
    assert(
        trevrpc_transport_get_wake_sources_v1(transport, wakes, TREVRPC_TRANSPORT_MAX_WAKE_SOURCES, &wake_count) == 0);
    assert(wake_count >= 3 && wake_count <= TREVRPC_TRANSPORT_MAX_WAKE_SOURCES);
    for (index = 0; index < wake_count; ++index) {
        assert(wakes[index].kind == TREVRPC_TRANSPORT_WAKE_SOURCE_POSIX_FD);
        assert((wakes[index].flags & TREVRPC_TRANSPORT_WAKE_FLAG_BORROWED) != 0);
        assert((wakes[index].flags & TREVRPC_TRANSPORT_WAKE_FLAG_LEVEL_TRIGGERED) != 0);
    }

    assert(trevrpc_transport_close(transport) == 0);
    for (attempt = 0; attempt < 100 && !stopped; ++attempt) {
        struct pollfd descriptors[TREVRPC_TRANSPORT_MAX_WAKE_SOURCES];
        int timeout = trevrpc_transport_poll_timeout_ms(transport);
        int poll_result;
        if (timeout < 0 || timeout > 100) {
            timeout = 100;
        }
        for (index = 0; index < wake_count; ++index) {
            descriptors[index].fd = (int)wakes[index].native_handle;
            descriptors[index].events = POLLIN;
            descriptors[index].revents = 0;
        }
        poll_result = poll(descriptors, wake_count, timeout);
        assert(poll_result >= 0);
        for (;;) {
            trevrpc_transport_event_info_v1 info;
            trevrpc_transport_event_v1* event = NULL;
            int result = trevrpc_transport_next_event(transport, &event);
            if (result == -EAGAIN) {
                break;
            }
            assert(result == 0);
            assert(trevrpc_transport_event_info_v1_init(&info, sizeof(info)) == 0);
            assert(trevrpc_transport_event_get_info_v1(transport, event, &info) == 0);
            assert(info.sequence > last_sequence);
            last_sequence = info.sequence;
            if (info.kind == TREVRPC_TRANSPORT_EVENT_STOPPED) {
                stopped = true;
            }
            trevrpc_transport_event_release(transport, event);
        }
    }
    assert(stopped);
    assert(trevrpc_transport_drain(transport) == 0);
    assert(trevrpc_transport_release(transport) == 0);
    return 0;
}
