#include "trevrpc_engine.h"

#include <errno.h> // NOLINT(misc-include-cleaner)
#include <stddef.h>
#include <stdint.h>
#include <string.h>

#define LAYOUT(type, expected_size, expected_align)                                                                    \
    _Static_assert(sizeof(type) == (expected_size), #type " size");                                                    \
    _Static_assert(_Alignof(type) == (expected_align), #type " align")

LAYOUT(trevrpc_engine_handle_v1, 16, 8);
LAYOUT(trevrpc_engine_config_v1, 80, 8);
LAYOUT(trevrpc_engine_wake_source_v1, 48, 8);
LAYOUT(trevrpc_engine_endpoint_config_v1, 160, 8);
LAYOUT(trevrpc_engine_event_info_v1, 136, 8);
LAYOUT(trevrpc_engine_receive_info_v1, 64, 8);
LAYOUT(trevrpc_engine_diagnostics_v1, 208, 8);

#define OFFSET(type, field, expected) _Static_assert(offsetof(type, field) == (expected), #type "." #field)

OFFSET(trevrpc_engine_handle_v1, owner, 0);
OFFSET(trevrpc_engine_handle_v1, slot, 8);
OFFSET(trevrpc_engine_handle_v1, generation, 12);

OFFSET(trevrpc_engine_config_v1, struct_size, 0);
OFFSET(trevrpc_engine_config_v1, struct_version, 4);
OFFSET(trevrpc_engine_config_v1, event_capacity, 8);
OFFSET(trevrpc_engine_config_v1, listener_capacity, 12);
OFFSET(trevrpc_engine_config_v1, connection_capacity, 16);
OFFSET(trevrpc_engine_config_v1, stream_capacity, 20);
OFFSET(trevrpc_engine_config_v1, max_receive_owned_count, 24);
OFFSET(trevrpc_engine_config_v1, flags, 28);
OFFSET(trevrpc_engine_config_v1, max_receive_owned_bytes, 32);
OFFSET(trevrpc_engine_config_v1, reserved, 40);

OFFSET(trevrpc_engine_wake_source_v1, struct_size, 0);
OFFSET(trevrpc_engine_wake_source_v1, struct_version, 4);
OFFSET(trevrpc_engine_wake_source_v1, kind, 8);
OFFSET(trevrpc_engine_wake_source_v1, flags, 12);
OFFSET(trevrpc_engine_wake_source_v1, native_handle, 16);
OFFSET(trevrpc_engine_wake_source_v1, reserved, 24);

OFFSET(trevrpc_engine_endpoint_config_v1, struct_size, 0);
OFFSET(trevrpc_engine_endpoint_config_v1, struct_version, 4);
OFFSET(trevrpc_engine_endpoint_config_v1, host, 8);
OFFSET(trevrpc_engine_endpoint_config_v1, host_len, 16);
OFFSET(trevrpc_engine_endpoint_config_v1, port, 20);
OFFSET(trevrpc_engine_endpoint_config_v1, peer_bidi_stream_count, 22);
OFFSET(trevrpc_engine_endpoint_config_v1, alpn, 24);
OFFSET(trevrpc_engine_endpoint_config_v1, alpn_len, 32);
OFFSET(trevrpc_engine_endpoint_config_v1, flags, 36);
OFFSET(trevrpc_engine_endpoint_config_v1, cert_file, 40);
OFFSET(trevrpc_engine_endpoint_config_v1, cert_file_len, 48);
OFFSET(trevrpc_engine_endpoint_config_v1, reserved0, 52);
OFFSET(trevrpc_engine_endpoint_config_v1, key_file, 56);
OFFSET(trevrpc_engine_endpoint_config_v1, key_file_len, 64);
OFFSET(trevrpc_engine_endpoint_config_v1, reserved1, 68);
OFFSET(trevrpc_engine_endpoint_config_v1, ca_cert_file, 72);
OFFSET(trevrpc_engine_endpoint_config_v1, ca_cert_file_len, 80);
OFFSET(trevrpc_engine_endpoint_config_v1, max_pending_send_count, 84);
OFFSET(trevrpc_engine_endpoint_config_v1, max_pending_send_bytes, 88);
OFFSET(trevrpc_engine_endpoint_config_v1, max_frame_size, 96);
OFFSET(trevrpc_engine_endpoint_config_v1, max_idle_timeout_ms, 104);
OFFSET(trevrpc_engine_endpoint_config_v1, keep_alive_ms, 112);
OFFSET(trevrpc_engine_endpoint_config_v1, stream_recv_window, 116);
OFFSET(trevrpc_engine_endpoint_config_v1, conn_flow_control_window, 120);
OFFSET(trevrpc_engine_endpoint_config_v1, reserved2, 124);
OFFSET(trevrpc_engine_endpoint_config_v1, reserved, 128);

OFFSET(trevrpc_engine_event_info_v1, struct_size, 0);
OFFSET(trevrpc_engine_event_info_v1, struct_version, 4);
OFFSET(trevrpc_engine_event_info_v1, kind, 8);
OFFSET(trevrpc_engine_event_info_v1, flags, 12);
OFFSET(trevrpc_engine_event_info_v1, status, 16);
OFFSET(trevrpc_engine_event_info_v1, subject_kind, 20);
OFFSET(trevrpc_engine_event_info_v1, sequence, 24);
OFFSET(trevrpc_engine_event_info_v1, subject, 32);
OFFSET(trevrpc_engine_event_info_v1, parent, 48);
OFFSET(trevrpc_engine_event_info_v1, operation_id, 64);
OFFSET(trevrpc_engine_event_info_v1, application_error_code, 72);
OFFSET(trevrpc_engine_event_info_v1, provider_error_code, 80);
OFFSET(trevrpc_engine_event_info_v1, data, 88);
OFFSET(trevrpc_engine_event_info_v1, data_len, 96);
OFFSET(trevrpc_engine_event_info_v1, reserved, 104);

OFFSET(trevrpc_engine_receive_info_v1, struct_size, 0);
OFFSET(trevrpc_engine_receive_info_v1, struct_version, 4);
OFFSET(trevrpc_engine_receive_info_v1, flags, 8);
OFFSET(trevrpc_engine_receive_info_v1, reserved0, 12);
OFFSET(trevrpc_engine_receive_info_v1, data, 16);
OFFSET(trevrpc_engine_receive_info_v1, data_len, 24);
OFFSET(trevrpc_engine_receive_info_v1, reserved, 32);

OFFSET(trevrpc_engine_diagnostics_v1, struct_size, 0);
OFFSET(trevrpc_engine_diagnostics_v1, struct_version, 4);
OFFSET(trevrpc_engine_diagnostics_v1, engine_abi_version, 8);
OFFSET(trevrpc_engine_diagnostics_v1, state, 12);
OFFSET(trevrpc_engine_diagnostics_v1, terminal_status, 16);
OFFSET(trevrpc_engine_diagnostics_v1, event_capacity, 20);
OFFSET(trevrpc_engine_diagnostics_v1, queue_depth, 24);
OFFSET(trevrpc_engine_diagnostics_v1, ordinary_queue_depth, 28);
OFFSET(trevrpc_engine_diagnostics_v1, events_enqueued, 32);
OFFSET(trevrpc_engine_diagnostics_v1, events_dequeued, 40);
OFFSET(trevrpc_engine_diagnostics_v1, events_rejected, 48);
OFFSET(trevrpc_engine_diagnostics_v1, receive_owned_count, 56);
OFFSET(trevrpc_engine_diagnostics_v1, peak_receive_owned_count, 64);
OFFSET(trevrpc_engine_diagnostics_v1, receive_owned_bytes, 72);
OFFSET(trevrpc_engine_diagnostics_v1, peak_receive_owned_bytes, 80);
OFFSET(trevrpc_engine_diagnostics_v1, pending_send_bytes, 88);
OFFSET(trevrpc_engine_diagnostics_v1, pending_send_count, 96);
OFFSET(trevrpc_engine_diagnostics_v1, live_listeners, 104);
OFFSET(trevrpc_engine_diagnostics_v1, live_connections, 112);
OFFSET(trevrpc_engine_diagnostics_v1, live_streams, 120);
OFFSET(trevrpc_engine_diagnostics_v1, active_callbacks, 128);
OFFSET(trevrpc_engine_diagnostics_v1, active_api_calls, 136);
OFFSET(trevrpc_engine_diagnostics_v1, wake_signals, 144);
OFFSET(trevrpc_engine_diagnostics_v1, wake_write_eagain, 152);
OFFSET(trevrpc_engine_diagnostics_v1, wake_failures, 160);
OFFSET(trevrpc_engine_diagnostics_v1, provider_error_code, 168);
OFFSET(trevrpc_engine_diagnostics_v1, mandatory_reservations, 176);
OFFSET(trevrpc_engine_diagnostics_v1, reserved, 184);

static int check_signatures(void) {
    uint32_t (*abi_version)(void) = trevrpc_engine_abi_version;
    void (*anchor)(void) = trevrpc_engine_abi_1_anchor;
    int (*config_init)(trevrpc_engine_config_v1*, size_t) = trevrpc_engine_config_v1_init;
    int (*wake_init)(trevrpc_engine_wake_source_v1*, size_t) = trevrpc_engine_wake_source_v1_init;
    int (*endpoint_init)(trevrpc_engine_endpoint_config_v1*, size_t) = trevrpc_engine_endpoint_config_v1_init;
    int (*event_init)(trevrpc_engine_event_info_v1*, size_t) = trevrpc_engine_event_info_v1_init;
    int (*receive_init)(trevrpc_engine_receive_info_v1*, size_t) = trevrpc_engine_receive_info_v1_init;
    int (*diagnostics_init)(trevrpc_engine_diagnostics_v1*, size_t) = trevrpc_engine_diagnostics_v1_init;
    int (*get_wake)(trevrpc_engine*, trevrpc_engine_wake_source_v1*) = trevrpc_engine_get_wake_source_v1;
    int (*next_event)(trevrpc_engine*, trevrpc_engine_event**) = trevrpc_engine_next_event;
    int (*event_get_info)(const trevrpc_engine_event*, trevrpc_engine_event_info_v1*) =
        trevrpc_engine_event_get_info_v1;
    void (*event_release)(trevrpc_engine_event*) = trevrpc_engine_event_release;
    int (*receive_get_info)(const trevrpc_engine_receive*, trevrpc_engine_receive_info_v1*) =
        trevrpc_engine_receive_get_info_v1;
    void (*receive_release)(trevrpc_engine_receive*) = trevrpc_engine_receive_release;
    int (*diagnostics)(trevrpc_engine*, trevrpc_engine_diagnostics_v1*) = trevrpc_engine_get_diagnostics_v1;
    int (*listen)(trevrpc_engine*, const trevrpc_engine_endpoint_config_v1*, trevrpc_engine_handle_v1*) =
        trevrpc_engine_listen_v1;
    int (*listener_port)(trevrpc_engine*, trevrpc_engine_handle_v1, uint16_t*) = trevrpc_engine_listener_get_port_v1;
    int (*dial)(trevrpc_engine*, const trevrpc_engine_endpoint_config_v1*, uint64_t, trevrpc_engine_handle_v1*) =
        trevrpc_engine_dial_v1;
    int (*dial_cancel)(trevrpc_engine*, trevrpc_engine_handle_v1) = trevrpc_engine_dial_cancel;
    int (*open_stream)(trevrpc_engine*, trevrpc_engine_handle_v1, uint64_t, trevrpc_engine_handle_v1*) =
        trevrpc_engine_connection_open_bidi_stream_v1;
    int (*send)(trevrpc_engine*, trevrpc_engine_handle_v1, uint64_t, const uint8_t*, size_t) =
        trevrpc_engine_stream_send_frame_v1;
    int (*receive)(trevrpc_engine*, trevrpc_engine_handle_v1, trevrpc_engine_receive**) =
        trevrpc_engine_stream_receive_frame;
    int (*finish)(trevrpc_engine*, trevrpc_engine_handle_v1) = trevrpc_engine_stream_finish_send;
    int (*abort_stream)(trevrpc_engine*, trevrpc_engine_handle_v1, uint64_t) = trevrpc_engine_stream_abort;
    int (*close_stream)(trevrpc_engine*, trevrpc_engine_handle_v1) = trevrpc_engine_stream_close;
    int (*close_connection)(trevrpc_engine*, trevrpc_engine_handle_v1, uint64_t) = trevrpc_engine_connection_close;
    int (*close_listener)(trevrpc_engine*, trevrpc_engine_handle_v1) = trevrpc_engine_listener_close;
    int (*close_engine)(trevrpc_engine*) = trevrpc_engine_close;
    int (*drain)(trevrpc_engine*) = trevrpc_engine_drain;
    int (*release)(trevrpc_engine*) = trevrpc_engine_release;
    anchor();
    return abi_version == NULL || config_init == NULL || wake_init == NULL || endpoint_init == NULL ||
           event_init == NULL || receive_init == NULL || diagnostics_init == NULL || get_wake == NULL ||
           next_event == NULL || event_get_info == NULL || event_release == NULL || receive_get_info == NULL ||
           receive_release == NULL || diagnostics == NULL || listen == NULL || listener_port == NULL || dial == NULL ||
           dial_cancel == NULL || open_stream == NULL || send == NULL || receive == NULL || finish == NULL ||
           abort_stream == NULL || close_stream == NULL || close_connection == NULL || close_listener == NULL ||
           close_engine == NULL || drain == NULL || release == NULL;
}

int main(void) {
    trevrpc_engine_config_v1 config;
    trevrpc_engine_endpoint_config_v1 endpoint;
    trevrpc_engine_wake_source_v1 wake;
    trevrpc_engine_event_info_v1 event;
    trevrpc_engine_receive_info_v1 receive;
    trevrpc_engine_diagnostics_v1 diagnostics;
    if (TREVRPC_ENGINE_ABI_VERSION != 1u || trevrpc_engine_abi_version() != 1u || check_signatures() != 0 ||
        trevrpc_engine_config_v1_init(&config, sizeof(config)) != 0 ||
        trevrpc_engine_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) != 0 ||
        trevrpc_engine_wake_source_v1_init(&wake, sizeof(wake)) != 0 ||
        trevrpc_engine_event_info_v1_init(&event, sizeof(event)) != 0 ||
        trevrpc_engine_receive_info_v1_init(&receive, sizeof(receive)) != 0 ||
        trevrpc_engine_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) != 0) {
        return 1;
    }
    if (config.event_capacity != TREVRPC_ENGINE_DEFAULT_EVENT_CAPACITY ||
        config.listener_capacity != TREVRPC_ENGINE_DEFAULT_LISTENER_CAPACITY ||
        config.connection_capacity != TREVRPC_ENGINE_DEFAULT_CONNECTION_CAPACITY ||
        config.stream_capacity != TREVRPC_ENGINE_DEFAULT_STREAM_CAPACITY ||
        config.max_receive_owned_count != TREVRPC_ENGINE_DEFAULT_MAX_RECEIVE_OWNED_COUNT ||
        config.max_receive_owned_bytes != TREVRPC_ENGINE_DEFAULT_RECEIVE_OWNED_BYTES ||
        endpoint.max_pending_send_count != TREVRPC_ENGINE_DEFAULT_MAX_PENDING_SEND_COUNT ||
        endpoint.max_pending_send_bytes != TREVRPC_ENGINE_DEFAULT_MAX_PENDING_SEND_BYTES ||
        endpoint.max_frame_size != TREVRPC_ENGINE_DEFAULT_MAX_FRAME_SIZE) {
        return 1;
    }
    return trevrpc_engine_config_v1_init(NULL, sizeof(config)) == -EINVAL ? 0 : 1;
}
