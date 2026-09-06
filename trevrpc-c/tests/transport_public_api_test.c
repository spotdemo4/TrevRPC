#include "trevrpc_transport.h"

#include <errno.h> // NOLINT(misc-include-cleaner)
#include <stddef.h>
#include <stdint.h>

#define LAYOUT(type, expected_size, expected_align)                                                                    \
    _Static_assert(sizeof(type) == (expected_size), #type " size");                                                    \
    _Static_assert(_Alignof(type) == (expected_align), #type " align")

LAYOUT(trevrpc_transport_handle_v1, 16, 8);
LAYOUT(trevrpc_transport_config_v1, 80, 8);
LAYOUT(trevrpc_transport_endpoint_config_v1, 312, 8);
LAYOUT(trevrpc_transport_wake_source_v1, 48, 8);
LAYOUT(trevrpc_transport_event_info_v1, 136, 8);
LAYOUT(trevrpc_transport_receive_info_v1, 64, 8);
LAYOUT(trevrpc_transport_header_field_v1, 32, 8);
LAYOUT(trevrpc_transport_admission_info_v1, 144, 8);
LAYOUT(trevrpc_transport_event_protocol_info_v1, 48, 8);
LAYOUT(trevrpc_transport_diagnostics_v1, 208, 8);

static int check_signatures(void) {
    uint32_t (*abi_version)(void) = trevrpc_transport_abi_version;
    void (*anchor)(void) = trevrpc_transport_abi_1_anchor;
    int (*config_init)(trevrpc_transport_config_v1*, size_t) = trevrpc_transport_config_v1_init;
    int (*endpoint_init)(trevrpc_transport_endpoint_config_v1*, size_t) = trevrpc_transport_endpoint_config_v1_init;
    int (*wake_init)(trevrpc_transport_wake_source_v1*, size_t) = trevrpc_transport_wake_source_v1_init;
    int (*event_init)(trevrpc_transport_event_info_v1*, size_t) = trevrpc_transport_event_info_v1_init;
    int (*receive_init)(trevrpc_transport_receive_info_v1*, size_t) = trevrpc_transport_receive_info_v1_init;
    int (*admission_init)(trevrpc_transport_admission_info_v1*, size_t) = trevrpc_transport_admission_info_v1_init;
    int (*protocol_init)(trevrpc_transport_event_protocol_info_v1*, size_t) =
        trevrpc_transport_event_protocol_info_v1_init;
    int (*diagnostics_init)(trevrpc_transport_diagnostics_v1*, size_t) = trevrpc_transport_diagnostics_v1_init;
    int (*get_wakes)(trevrpc_transport*, trevrpc_transport_wake_source_v1*, size_t, size_t*) =
        trevrpc_transport_get_wake_sources_v1;
    int (*poll_timeout)(trevrpc_transport*) = trevrpc_transport_poll_timeout_ms;
    int (*next_event)(trevrpc_transport*, trevrpc_transport_event_v1**) = trevrpc_transport_next_event;
    int (*event_info)(trevrpc_transport*, const trevrpc_transport_event_v1*, trevrpc_transport_event_info_v1*) =
        trevrpc_transport_event_get_info_v1;
    int (*admission_info)(trevrpc_transport*, const trevrpc_transport_event_v1*, trevrpc_transport_admission_info_v1*) =
        trevrpc_transport_event_get_admission_info_v1;
    int (*protocol_info)(
        trevrpc_transport*, const trevrpc_transport_event_v1*, trevrpc_transport_event_protocol_info_v1*) =
        trevrpc_transport_event_get_protocol_info_v1;
    int (*admission_respond)(trevrpc_transport*, const trevrpc_transport_event_v1*, uint16_t) =
        trevrpc_transport_admission_respond_v1;
    void (*event_release)(trevrpc_transport*, trevrpc_transport_event_v1*) = trevrpc_transport_event_release;
    int (*receive_info)(trevrpc_transport*, const trevrpc_transport_receive_v1*, trevrpc_transport_receive_info_v1*) =
        trevrpc_transport_receive_get_info_v1;
    void (*receive_release)(trevrpc_transport*, trevrpc_transport_receive_v1*) = trevrpc_transport_receive_release;
    int (*diagnostics)(trevrpc_transport*, trevrpc_transport_diagnostics_v1*) = trevrpc_transport_get_diagnostics_v1;
    int (*listen)(trevrpc_transport*, const trevrpc_transport_endpoint_config_v1*, trevrpc_transport_handle_v1*) =
        trevrpc_transport_listen_v1;
    int (*listener_port)(trevrpc_transport*, trevrpc_transport_handle_v1, uint16_t*) =
        trevrpc_transport_listener_get_port_v1;
    int (*dial)(
        trevrpc_transport*, const trevrpc_transport_endpoint_config_v1*, uint64_t, trevrpc_transport_handle_v1*) =
        trevrpc_transport_dial_v1;
    int (*open_stream)(trevrpc_transport*, trevrpc_transport_handle_v1, uint64_t, trevrpc_transport_handle_v1*) =
        trevrpc_transport_connection_open_bidi_stream_v1;
    int (*send)(trevrpc_transport*, trevrpc_transport_handle_v1, uint64_t, const uint8_t*, size_t) =
        trevrpc_transport_stream_send_v1;
    int (*receive)(trevrpc_transport*, trevrpc_transport_handle_v1, trevrpc_transport_receive_v1**) =
        trevrpc_transport_stream_receive;
    int (*abort_receive)(trevrpc_transport*, trevrpc_transport_handle_v1, uint64_t) =
        trevrpc_transport_stream_abort_receive;
    int (*abort_send)(trevrpc_transport*, trevrpc_transport_handle_v1, uint64_t) = trevrpc_transport_stream_abort_send;
    int (*release_handle)(trevrpc_transport*, trevrpc_transport_handle_v1, uint32_t) = trevrpc_transport_release_handle;
    int (*close_transport)(trevrpc_transport*) = trevrpc_transport_close;
    int (*drain)(trevrpc_transport*) = trevrpc_transport_drain;
    int (*release)(trevrpc_transport*) = trevrpc_transport_release;
    anchor();
    return abi_version == NULL || config_init == NULL || endpoint_init == NULL || wake_init == NULL ||
           event_init == NULL || receive_init == NULL || admission_init == NULL || protocol_init == NULL ||
           diagnostics_init == NULL || get_wakes == NULL || poll_timeout == NULL || next_event == NULL ||
           event_info == NULL || admission_info == NULL || protocol_info == NULL || admission_respond == NULL ||
           event_release == NULL || receive_info == NULL || receive_release == NULL || diagnostics == NULL ||
           listen == NULL || listener_port == NULL || dial == NULL || open_stream == NULL || send == NULL ||
           receive == NULL || abort_receive == NULL || abort_send == NULL || release_handle == NULL ||
           close_transport == NULL || drain == NULL || release == NULL;
}

int main(void) {
    trevrpc_transport_config_v1 config;
    trevrpc_transport_endpoint_config_v1 endpoint;
    trevrpc_transport_wake_source_v1 wake;
    trevrpc_transport_event_info_v1 event;
    trevrpc_transport_receive_info_v1 receive;
    trevrpc_transport_admission_info_v1 admission;
    trevrpc_transport_event_protocol_info_v1 protocol;
    trevrpc_transport_diagnostics_v1 diagnostics;
    if (TREVRPC_TRANSPORT_ABI_VERSION != 1u || trevrpc_transport_abi_version() != 1u || check_signatures() != 0 ||
        trevrpc_transport_config_v1_init(&config, sizeof(config)) != 0 ||
        trevrpc_transport_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) != 0 ||
        trevrpc_transport_wake_source_v1_init(&wake, sizeof(wake)) != 0 ||
        trevrpc_transport_event_info_v1_init(&event, sizeof(event)) != 0 ||
        trevrpc_transport_receive_info_v1_init(&receive, sizeof(receive)) != 0 ||
        trevrpc_transport_admission_info_v1_init(&admission, sizeof(admission)) != 0 ||
        trevrpc_transport_event_protocol_info_v1_init(&protocol, sizeof(protocol)) != 0 ||
        trevrpc_transport_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) != 0) {
        return 1;
    }
    if (config.event_capacity != TREVRPC_TRANSPORT_DEFAULT_EVENT_CAPACITY ||
        config.listener_capacity != TREVRPC_TRANSPORT_DEFAULT_LISTENER_CAPACITY ||
        config.connection_capacity != TREVRPC_TRANSPORT_DEFAULT_CONNECTION_CAPACITY ||
        config.stream_capacity != TREVRPC_TRANSPORT_DEFAULT_STREAM_CAPACITY ||
        config.max_receive_owned_count != TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_COUNT ||
        config.max_receive_owned_bytes != TREVRPC_TRANSPORT_DEFAULT_MAX_RECEIVE_OWNED_BYTES ||
        endpoint.max_frame_size != TREVRPC_TRANSPORT_DEFAULT_MAX_FRAME_SIZE ||
        endpoint.max_field_section_size != TREVRPC_TRANSPORT_DEFAULT_MAX_FIELD_SECTION_SIZE ||
        diagnostics.transport_abi_version != TREVRPC_TRANSPORT_ABI_VERSION) {
        return 1;
    }
    return trevrpc_transport_config_v1_init(NULL, sizeof(config)) == -EINVAL ? 0 : 1;
}
