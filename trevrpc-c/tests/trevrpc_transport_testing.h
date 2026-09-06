#ifndef TREVRPC_TRANSPORT_TESTING_H
#define TREVRPC_TRANSPORT_TESTING_H

#include "trevrpc_transport.h"

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TREVRPC_TRANSPORT_TESTING_ABI_VERSION 1u
#define TREVRPC_TRANSPORT_TESTING_STRUCT_VERSION_1 1u
#define TREVRPC_TRANSPORT_TESTING_WAKE_COUNT 3u
#define TREVRPC_TRANSPORT_TESTING_DEFAULT_EVENT_CAPACITY 64u
#define TREVRPC_TRANSPORT_TESTING_DEFAULT_RECEIVE_CAPACITY 64u
#define TREVRPC_TRANSPORT_TESTING_DEFAULT_STATUS_CAPACITY 64u
#define TREVRPC_TRANSPORT_TESTING_DEFAULT_PORT 4242u

#define TREVRPC_TRANSPORT_TESTING_OPERATION_LISTEN 1u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_LISTENER_GET_PORT 2u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_DIAL 3u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_DIAL_CANCEL 4u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_OPEN_BIDI_STREAM 5u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_STREAM_SEND 6u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_STREAM_RECEIVE 7u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_FINISH_SEND 8u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_RECEIVE 9u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_SEND 10u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_STREAM 11u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_STREAM 12u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_CONNECTION 13u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_LISTENER 14u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_RELEASE_HANDLE 15u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_TRANSPORT 16u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_DRAIN 17u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_RELEASE_TRANSPORT 18u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_ADMISSION_RESPOND 19u
#define TREVRPC_TRANSPORT_TESTING_OPERATION_COUNT 20u

#define TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_POP 1u
#define TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_SEND_ADMISSION 2u
#define TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_FINISH_SEND_ADMISSION 3u

typedef struct trevrpc_transport_testing_control trevrpc_transport_testing_control;

typedef struct trevrpc_transport_testing_config_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t event_capacity;
    uint32_t receive_capacity;
    uint32_t status_capacity;
    uint16_t listener_port;
    uint16_t reserved0;
    uint32_t reserved1;
    uint64_t reserved[4];
} trevrpc_transport_testing_config_v1;

typedef struct trevrpc_transport_testing_event_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t kind;
    uint32_t flags;
    int32_t status;
    uint32_t subject_kind;
    trevrpc_transport_handle_v1 subject;
    trevrpc_transport_handle_v1 parent;
    uint64_t operation_id;
    uint64_t application_error_code;
    uint64_t provider_error_code;
    uint32_t protocol;
    uint32_t reserved0;
    const uint8_t* data;
    uint64_t data_len;
    const trevrpc_transport_header_field_v1* headers;
    uint64_t header_count;
    const uint8_t* method;
    uint64_t method_len;
    const uint8_t* path;
    uint64_t path_len;
    const uint8_t* authority;
    uint64_t authority_len;
    const uint8_t* origin;
    uint64_t origin_len;
    uint64_t reserved[3];
} trevrpc_transport_testing_event_v1;

typedef struct trevrpc_transport_testing_receive_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t flags;
    uint32_t reserved0;
    trevrpc_transport_handle_v1 stream;
    const uint8_t* data;
    uint64_t data_len;
    uint64_t reserved[3];
} trevrpc_transport_testing_receive_v1;

typedef struct trevrpc_transport_testing_counters_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint64_t events_injected;
    uint64_t events_popped;
    uint64_t events_released;
    uint64_t receives_injected;
    uint64_t receives_popped;
    uint64_t receives_released;
    uint64_t admissions_responded;
    uint64_t operation_calls[TREVRPC_TRANSPORT_TESTING_OPERATION_COUNT];
    uint64_t last_operation_id;
    uint64_t last_send_operation_id;
    uint64_t last_dial_operation_id;
    uint64_t last_open_operation_id;
    uint64_t last_admission_status;
    int32_t last_status;
    uint32_t last_operation;
    uint64_t reserved[4];
} trevrpc_transport_testing_counters_v1;

uint32_t trevrpc_transport_testing_abi_version(void);
void trevrpc_transport_testing_abi_1_anchor(void);
int trevrpc_transport_testing_config_v1_init(trevrpc_transport_testing_config_v1* config, size_t struct_size);
int trevrpc_transport_testing_event_v1_init(trevrpc_transport_testing_event_v1* event, size_t struct_size);
int trevrpc_transport_testing_receive_v1_init(trevrpc_transport_testing_receive_v1* receive, size_t struct_size);
int trevrpc_transport_testing_counters_v1_init(trevrpc_transport_testing_counters_v1* counters, size_t struct_size);

int trevrpc_transport_testing_create_v1(const trevrpc_transport_config_v1* transport_config,
    const trevrpc_transport_testing_config_v1* provider_config,
    trevrpc_transport** out_transport,
    trevrpc_transport_testing_control** out_control);

int trevrpc_transport_testing_push_event_v1(
    trevrpc_transport_testing_control* control, const trevrpc_transport_testing_event_v1* event);
int trevrpc_transport_testing_push_receive_v1(
    trevrpc_transport_testing_control* control, const trevrpc_transport_testing_receive_v1* receive);
int trevrpc_transport_testing_script_status_v1(trevrpc_transport_testing_control* control, int32_t status);
int trevrpc_transport_testing_clear_status_script_v1(trevrpc_transport_testing_control* control);
int trevrpc_transport_testing_set_checkpoint_v1(
    trevrpc_transport_testing_control* control, uint32_t checkpoint, uint32_t enabled);
int trevrpc_transport_testing_wait_checkpoint_v1(trevrpc_transport_testing_control* control, uint32_t checkpoint);
int trevrpc_transport_testing_release_checkpoint_v1(trevrpc_transport_testing_control* control, uint32_t checkpoint);
int trevrpc_transport_testing_push_stopped_v1(trevrpc_transport_testing_control* control, int32_t status);
int trevrpc_transport_testing_get_counters_v1(
    trevrpc_transport_testing_control* control, trevrpc_transport_testing_counters_v1* counters);
int trevrpc_transport_testing_get_wake_fd_v1(trevrpc_transport_testing_control* control, uint32_t index);
int trevrpc_transport_testing_set_poll_timeout_ms_v1(trevrpc_transport_testing_control* control, int32_t timeout_ms);

#ifdef __cplusplus
}
#endif

#endif
