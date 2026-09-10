#ifndef TREVRPC_ENGINE_PROVIDER_HOST_VALIDATE_H
#define TREVRPC_ENGINE_PROVIDER_HOST_VALIDATE_H

#include "trevrpc_engine_provider.h"

#include <errno.h>
#include <stddef.h>

static inline int trevrpc_engine_provider_host_validate_v1(const trevrpc_engine_provider_host_v1* host) {
    const trevrpc_engine_provider_runtime_v1* runtime;
    size_t index;
    if (host == NULL || host->struct_size < sizeof(*host)) {
        return -EINVAL;
    }
    if (host->struct_version != TREVRPC_ENGINE_PROVIDER_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (host->callback_enter == NULL || host->callback_leave == NULL || host->operation_pin == NULL ||
        host->operation_unpin == NULL || host->reserve_mandatory == NULL || host->publish_reserved == NULL ||
        host->cancel_reservation == NULL || host->publish_event == NULL || host->fail == NULL ||
        host->stopped == NULL || host->receive_create_copy == NULL || host->receive_create_owned == NULL ||
        host->owner_cookie == NULL || host->provider_context == NULL || host->runtime == NULL) {
        return -EINVAL;
    }
    for (index = 0; index < sizeof(host->reserved) / sizeof(host->reserved[0]); ++index) {
        if (host->reserved[index] != 0) {
            return -EINVAL;
        }
    }
    runtime = host->runtime;
    if (runtime->struct_size < sizeof(*runtime)) {
        return -EINVAL;
    }
    if (runtime->struct_version != TREVRPC_ENGINE_PROVIDER_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (runtime->config_init == NULL || runtime->wake_source_init == NULL || runtime->endpoint_config_init == NULL ||
        runtime->event_info_init == NULL || runtime->receive_info_init == NULL || runtime->diagnostics_init == NULL ||
        runtime->adopt == NULL || runtime->get_wake_source == NULL || runtime->next_event == NULL ||
        runtime->event_get_info == NULL || runtime->event_release == NULL || runtime->receive_get_info == NULL ||
        runtime->receive_release == NULL || runtime->get_diagnostics == NULL || runtime->listen == NULL ||
        runtime->listener_get_port == NULL || runtime->dial == NULL || runtime->dial_cancel == NULL ||
        runtime->connection_open_bidi_stream == NULL || runtime->stream_send_frame == NULL ||
        runtime->stream_receive_frame == NULL || runtime->stream_finish_send == NULL ||
        runtime->stream_abort_receive == NULL || runtime->stream_abort_send == NULL || runtime->stream_abort == NULL ||
        runtime->stream_close == NULL || runtime->connection_close == NULL || runtime->listener_close == NULL ||
        runtime->close == NULL || runtime->drain == NULL || runtime->release == NULL) {
        return -EINVAL;
    }
    for (index = 0; index < sizeof(runtime->reserved) / sizeof(runtime->reserved[0]); ++index) {
        if (runtime->reserved[index] != 0) {
            return -EINVAL;
        }
    }
    return 0;
}

#endif
