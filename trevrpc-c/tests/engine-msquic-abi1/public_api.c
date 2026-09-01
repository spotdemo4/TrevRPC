#include "trevrpc_engine_msquic.h"

#ifdef NDEBUG
#undef NDEBUG
#endif
#include <assert.h>
#include <errno.h> // NOLINT(misc-include-cleaner)
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define CHECK_OFFSET(type, field, value) _Static_assert(offsetof(type, field) == (value), #type "." #field)

_Static_assert(sizeof(trevrpc_engine_msquic_config_v1) == 64, "config size");
_Static_assert(_Alignof(trevrpc_engine_msquic_config_v1) == 8, "config alignment");
CHECK_OFFSET(trevrpc_engine_msquic_config_v1, struct_size, 0);
CHECK_OFFSET(trevrpc_engine_msquic_config_v1, struct_version, 4);
CHECK_OFFSET(trevrpc_engine_msquic_config_v1, flags, 8);
CHECK_OFFSET(trevrpc_engine_msquic_config_v1, reserved0, 12);
CHECK_OFFSET(trevrpc_engine_msquic_config_v1, reserved, 16);

static void test_initializer(void) {
    struct extended_config {
        trevrpc_engine_msquic_config_v1 config;
        uint8_t tail[17];
    } extended;
    memset(&extended, 0xa5, sizeof(extended));
    assert(trevrpc_engine_msquic_config_v1_init(&extended.config, sizeof(extended)) == 0);
    assert(extended.config.struct_size == sizeof(extended));
    assert(extended.config.struct_version == TREVRPC_ENGINE_MSQUIC_STRUCT_VERSION_1);
    assert(extended.config.flags == 0);
    assert(extended.config.reserved0 == 0);
    for (size_t index = 0; index < sizeof(extended.config.reserved) / sizeof(extended.config.reserved[0]); ++index) {
        assert(extended.config.reserved[index] == 0);
    }
    for (size_t index = 0; index < sizeof(extended.tail); ++index) {
        assert(extended.tail[index] == 0);
    }
    assert(trevrpc_engine_msquic_config_v1_init(NULL, sizeof(extended.config)) == -EINVAL);
    assert(trevrpc_engine_msquic_config_v1_init(&extended.config, sizeof(extended.config) - 1) == -EINVAL);
}

static void test_empty_lifecycle(void) {
    trevrpc_engine_config_v1 engine_config;
    trevrpc_engine_msquic_config_v1 provider_config;
    trevrpc_engine* engine = NULL;
    trevrpc_engine_event* event = NULL;
    trevrpc_engine_event_info_v1 info;

    assert(trevrpc_engine_config_v1_init(&engine_config, sizeof(engine_config)) == 0);
    engine_config.listener_capacity = 1;
    engine_config.connection_capacity = 1;
    engine_config.stream_capacity = 1;
    assert(trevrpc_engine_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    assert(trevrpc_engine_msquic_create_v1(&engine_config, &provider_config, &engine) == 0);
    assert(engine != NULL);
    assert(trevrpc_engine_next_event(engine, &event) == -EAGAIN);
    assert(trevrpc_engine_close(engine) == 0);
    assert(trevrpc_engine_drain(engine) == 0);
    assert(trevrpc_engine_next_event(engine, &event) == 0);
    assert(trevrpc_engine_event_info_v1_init(&info, sizeof(info)) == 0);
    assert(trevrpc_engine_event_get_info_v1(event, &info) == 0);
    assert(info.kind == TREVRPC_ENGINE_EVENT_STOPPED);
    assert((info.flags & TREVRPC_ENGINE_EVENT_FLAG_TERMINAL) != 0);
    trevrpc_engine_event_release(event);
    assert(trevrpc_engine_release(engine) == 0);
    assert(trevrpc_engine_release(NULL) == 0);
}

int main(void) {
    assert(trevrpc_engine_msquic_abi_version() == TREVRPC_ENGINE_MSQUIC_ABI_VERSION);
    trevrpc_engine_msquic_abi_1_anchor();
    test_initializer();
    test_empty_lifecycle();
    return 0;
}
