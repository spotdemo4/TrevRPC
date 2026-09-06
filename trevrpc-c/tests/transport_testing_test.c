#define _POSIX_C_SOURCE 200809L

#include "trevrpc_transport_testing.h"

#include <assert.h>
#include <errno.h> // NOLINT(misc-include-cleaner)
#include <pthread.h>
#include <stdint.h>
#include <string.h>

static const trevrpc_transport_handle_v1 listener = {UINT64_C(0x5445535452414e53), 1u, 1u};
static const trevrpc_transport_handle_v1 connection = {UINT64_C(0x5445535452414e53), 2u, 1u};
static const trevrpc_transport_handle_v1 stream = {UINT64_C(0x5445535452414e53), 3u, 1u};

typedef struct event_thread_args {
    trevrpc_transport* transport;
    trevrpc_transport_event_v1* event;
    int result;
} event_thread_args;

typedef struct send_thread_args {
    trevrpc_transport* transport;
    int result;
} send_thread_args;

static void* pop_event_thread(void* context) {
    event_thread_args* args = context;
    args->result = trevrpc_transport_next_event(args->transport, &args->event);
    return NULL;
}

static void* send_thread(void* context) {
    send_thread_args* args = context;
    args->result = trevrpc_transport_stream_send_v1(args->transport, stream, 77, (const uint8_t*)"body", 4);
    return NULL;
}

static trevrpc_transport_testing_control* make_transport_with_event_capacity(
    trevrpc_transport** out_transport, uint32_t event_capacity) {
    trevrpc_transport_config_v1 transport_config;
    trevrpc_transport_testing_config_v1 provider_config;
    trevrpc_transport_testing_control* control = NULL;
    assert(trevrpc_transport_config_v1_init(&transport_config, sizeof(transport_config)) == 0);
    assert(trevrpc_transport_testing_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    provider_config.event_capacity = event_capacity;
    assert(trevrpc_transport_testing_create_v1(&transport_config, &provider_config, out_transport, &control) == 0);
    return control;
}

static trevrpc_transport_testing_control* make_transport(trevrpc_transport** out_transport) {
    return make_transport_with_event_capacity(out_transport, TREVRPC_TRANSPORT_TESTING_DEFAULT_EVENT_CAPACITY);
}

static void test_wakes_events_and_protocol(void) {
    trevrpc_transport* transport;
    trevrpc_transport_testing_control* control = make_transport(&transport);
    trevrpc_transport_wake_source_v1 wakes[TREVRPC_TRANSPORT_TESTING_WAKE_COUNT];
    trevrpc_transport_testing_event_v1 injected;
    trevrpc_transport_event_v1* event = NULL;
    trevrpc_transport_event_info_v1 info;
    trevrpc_transport_event_protocol_info_v1 protocol;
    size_t wake_count = 0;
    size_t index;
    int descriptors[TREVRPC_TRANSPORT_TESTING_WAKE_COUNT];

    for (index = 0; index < TREVRPC_TRANSPORT_TESTING_WAKE_COUNT; ++index) {
        assert(trevrpc_transport_wake_source_v1_init(&wakes[index], sizeof(wakes[index])) == 0);
    }
    assert(trevrpc_transport_get_wake_sources_v1(transport, wakes, TREVRPC_TRANSPORT_TESTING_WAKE_COUNT, &wake_count) ==
           0);
    assert(wake_count == TREVRPC_TRANSPORT_TESTING_WAKE_COUNT);
    for (index = 0; index < wake_count; ++index) {
        assert(wakes[index].kind == TREVRPC_TRANSPORT_WAKE_SOURCE_POSIX_FD);
        assert((wakes[index].flags &
                   (TREVRPC_TRANSPORT_WAKE_FLAG_BORROWED | TREVRPC_TRANSPORT_WAKE_FLAG_LEVEL_TRIGGERED)) ==
               (TREVRPC_TRANSPORT_WAKE_FLAG_BORROWED | TREVRPC_TRANSPORT_WAKE_FLAG_LEVEL_TRIGGERED));
        descriptors[index] = (int)wakes[index].native_handle;
        assert(descriptors[index] >= 0);
        for (size_t prior = 0; prior < index; ++prior) {
            assert(descriptors[prior] != descriptors[index]);
        }
    }

    assert(trevrpc_transport_testing_event_v1_init(&injected, sizeof(injected)) == 0);
    injected.kind = TREVRPC_TRANSPORT_EVENT_CONNECTION_READY;
    injected.flags = TREVRPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_TRANSPORT_EVENT_FLAG_SERVER;
    injected.subject_kind = TREVRPC_TRANSPORT_OBJECT_CONNECTION;
    injected.subject = connection;
    injected.parent = listener;
    injected.protocol = TREVRPC_TRANSPORT_PROTOCOL_NATIVE;
    assert(trevrpc_transport_testing_push_event_v1(control, &injected) == 0);
    injected.kind = TREVRPC_TRANSPORT_EVENT_STREAM_READY;
    injected.subject_kind = TREVRPC_TRANSPORT_OBJECT_STREAM;
    injected.subject = stream;
    injected.operation_id = 23;
    assert(trevrpc_transport_testing_push_event_v1(control, &injected) == 0);

    assert(trevrpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_transport_event_info_v1_init(&info, sizeof(info)) == 0);
    assert(trevrpc_transport_event_get_info_v1(transport, event, &info) == 0);
    assert(info.sequence == 1 && info.kind == TREVRPC_TRANSPORT_EVENT_CONNECTION_READY);
    assert(trevrpc_transport_event_protocol_info_v1_init(&protocol, sizeof(protocol)) == 0);
    assert(trevrpc_transport_event_get_protocol_info_v1(transport, event, &protocol) == 0);
    assert(protocol.protocol == TREVRPC_TRANSPORT_PROTOCOL_NATIVE);
    trevrpc_transport_event_release(transport, event);

    assert(trevrpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_transport_event_get_info_v1(transport, event, &info) == 0);
    assert(info.sequence == 2 && info.operation_id == 23);
    trevrpc_transport_event_release(transport, event);

    assert(trevrpc_transport_testing_get_wake_fd_v1(control, 0) == descriptors[0]);
    assert(trevrpc_transport_release(transport) == 0);
}

static void test_admission_and_receive(void) {
    trevrpc_transport* transport;
    trevrpc_transport_testing_control* control = make_transport(&transport);
    trevrpc_transport_testing_event_v1 event_spec;
    trevrpc_transport_testing_receive_v1 receive_spec;
    trevrpc_transport_header_field_v1 header = {(const uint8_t*)"x-test", 6, (const uint8_t*)"yes", 3};
    trevrpc_transport_event_v1* event = NULL;
    trevrpc_transport_event_info_v1 event_info;
    trevrpc_transport_admission_info_v1 admission;
    trevrpc_transport_receive_v1* receive = NULL;
    trevrpc_transport_receive_info_v1 receive_info;

    assert(trevrpc_transport_testing_event_v1_init(&event_spec, sizeof(event_spec)) == 0);
    event_spec.kind = TREVRPC_TRANSPORT_EVENT_HTTP3_ADMISSION;
    event_spec.flags = TREVRPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_TRANSPORT_EVENT_FLAG_SERVER;
    event_spec.subject_kind = TREVRPC_TRANSPORT_OBJECT_CONNECTION;
    event_spec.subject = connection;
    event_spec.parent = listener;
    event_spec.protocol = TREVRPC_TRANSPORT_PROTOCOL_HTTP3;
    event_spec.headers = &header;
    event_spec.header_count = 1;
    event_spec.method = (const uint8_t*)"POST";
    event_spec.method_len = 4;
    event_spec.path = (const uint8_t*)"/rpc";
    event_spec.path_len = 4;
    event_spec.authority = (const uint8_t*)"localhost";
    event_spec.authority_len = 9;
    assert(trevrpc_transport_testing_push_event_v1(control, &event_spec) == 0);
    assert(trevrpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_transport_admission_info_v1_init(&admission, sizeof(admission)) == 0);
    assert(trevrpc_transport_event_get_admission_info_v1(transport, event, &admission) == 0);
    assert(admission.protocol == TREVRPC_TRANSPORT_PROTOCOL_HTTP3 && admission.header_count == 1);
    assert(admission.listener.owner == listener.owner && admission.listener.slot == listener.slot &&
           admission.listener.generation == listener.generation);
    assert(admission.method_len == 4 && memcmp(admission.method, "POST", 4) == 0);
    assert(trevrpc_transport_drain(transport) == -EBUSY);
    assert(trevrpc_transport_admission_respond_v1(transport, event, 200) == 0);
    assert(trevrpc_transport_admission_respond_v1(transport, event, 503) == -EINVAL);
    trevrpc_transport_event_release(transport, event);
    assert(trevrpc_transport_drain(transport) == 0);

    assert(trevrpc_transport_testing_receive_v1_init(&receive_spec, sizeof(receive_spec)) == 0);
    receive_spec.stream = stream;
    receive_spec.data = (const uint8_t*)"message";
    receive_spec.data_len = 7;
    assert(trevrpc_transport_testing_push_receive_v1(control, &receive_spec) == 0);
    assert(trevrpc_transport_drain(transport) == -EBUSY);
    assert(trevrpc_transport_stream_receive(transport, stream, &receive) == 0);
    assert(trevrpc_transport_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
    assert(trevrpc_transport_receive_get_info_v1(transport, receive, &receive_info) == 0);
    assert(receive_info.data_len == 7 && memcmp(receive_info.data, "message", 7) == 0);
    assert(trevrpc_transport_drain(transport) == -EBUSY);
    trevrpc_transport_receive_release(transport, receive);
    assert(trevrpc_transport_drain(transport) == 0);
    assert(trevrpc_transport_stream_receive(transport, stream, &receive) == -EAGAIN);

    assert(trevrpc_transport_testing_event_v1_init(&event_spec, sizeof(event_spec)) == 0);
    event_spec.kind = TREVRPC_TRANSPORT_EVENT_RECEIVE_FIN;
    event_spec.flags = TREVRPC_TRANSPORT_EVENT_FLAG_PEER;
    event_spec.status = -EMSGSIZE;
    event_spec.subject_kind = TREVRPC_TRANSPORT_OBJECT_STREAM;
    event_spec.subject = stream;
    assert(trevrpc_transport_testing_push_event_v1(control, &event_spec) == 0);
    assert(trevrpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_transport_event_info_v1_init(&event_info, sizeof(event_info)) == 0);
    assert(trevrpc_transport_event_get_info_v1(transport, event, &event_info) == 0);
    assert(event_info.kind == TREVRPC_TRANSPORT_EVENT_RECEIVE_FIN);
    assert(event_info.status == -EMSGSIZE);
    assert(event_info.subject_kind == TREVRPC_TRANSPORT_OBJECT_STREAM);
    assert(event_info.subject.owner == stream.owner && event_info.subject.slot == stream.slot &&
           event_info.subject.generation == stream.generation);
    assert((event_info.flags & TREVRPC_TRANSPORT_EVENT_FLAG_PEER) != 0);
    trevrpc_transport_event_release(transport, event);

    assert(trevrpc_transport_testing_event_v1_init(&event_spec, sizeof(event_spec)) == 0);
    event_spec.kind = TREVRPC_TRANSPORT_EVENT_SEND_STOPPED;
    event_spec.flags = TREVRPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_TRANSPORT_EVENT_FLAG_PEER |
                       TREVRPC_TRANSPORT_EVENT_FLAG_PEER_RESET;
    event_spec.status = -ECANCELED;
    event_spec.subject_kind = TREVRPC_TRANSPORT_OBJECT_STREAM;
    event_spec.subject = stream;
    event_spec.application_error_code = 23;
    assert(trevrpc_transport_testing_push_event_v1(control, &event_spec) == 0);
    assert(trevrpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_transport_event_info_v1_init(&event_info, sizeof(event_info)) == 0);
    assert(trevrpc_transport_event_get_info_v1(transport, event, &event_info) == 0);
    assert(event_info.kind == TREVRPC_TRANSPORT_EVENT_SEND_STOPPED);
    assert(event_info.status == -ECANCELED);
    assert(event_info.application_error_code == 23);
    trevrpc_transport_event_release(transport, event);
    assert(trevrpc_transport_release(transport) == 0);
}

static void test_stopped_state_changes_only_after_enqueue(void) {
    trevrpc_transport* transport;
    trevrpc_transport_testing_control* control = make_transport_with_event_capacity(&transport, 1);
    trevrpc_transport_testing_event_v1 event_spec;
    trevrpc_transport_event_v1* event = NULL;
    trevrpc_transport_event_info_v1 event_info;
    trevrpc_transport_diagnostics_v1 diagnostics;

    assert(trevrpc_transport_testing_event_v1_init(&event_spec, sizeof(event_spec)) == 0);
    event_spec.kind = TREVRPC_TRANSPORT_EVENT_DIAGNOSTIC;
    assert(trevrpc_transport_testing_push_event_v1(control, &event_spec) == 0);
    assert(trevrpc_transport_testing_push_stopped_v1(control, -EIO) == -EAGAIN);
    assert(trevrpc_transport_diagnostics_v1_init(&diagnostics, sizeof(diagnostics)) == 0);
    assert(trevrpc_transport_get_diagnostics_v1(transport, &diagnostics) == 0);
    assert(diagnostics.state == TREVRPC_TRANSPORT_STATE_RUNNING && diagnostics.terminal_status == 0);

    assert(trevrpc_transport_next_event(transport, &event) == 0);
    trevrpc_transport_event_release(transport, event);
    assert(trevrpc_transport_testing_push_stopped_v1(control, -EIO) == 0);
    assert(trevrpc_transport_get_diagnostics_v1(transport, &diagnostics) == 0);
    assert(diagnostics.state == TREVRPC_TRANSPORT_STATE_STOPPED && diagnostics.terminal_status == -EIO);
    assert(trevrpc_transport_next_event(transport, &event) == 0);
    assert(trevrpc_transport_event_info_v1_init(&event_info, sizeof(event_info)) == 0);
    assert(trevrpc_transport_event_get_info_v1(transport, event, &event_info) == 0);
    assert(event_info.kind == TREVRPC_TRANSPORT_EVENT_STOPPED && event_info.status == -EIO);
    trevrpc_transport_event_release(transport, event);
    assert(trevrpc_transport_release(transport) == 0);
}

static void test_statuses_checkpoints_counters_and_stop(void) {
    trevrpc_transport* transport;
    trevrpc_transport_testing_control* control = make_transport(&transport);
    trevrpc_transport_endpoint_config_v1 endpoint;
    trevrpc_transport_handle_v1 out_listener;
    trevrpc_transport_testing_counters_v1 counters;
    trevrpc_transport_testing_event_v1 event_spec;
    event_thread_args pop_args = {transport, NULL, -1};
    send_thread_args send_args = {transport, -1};
    pthread_t pop_thread;
    pthread_t send_thread_id;

    assert(trevrpc_transport_endpoint_config_v1_init(&endpoint, sizeof(endpoint)) == 0);
    assert(trevrpc_transport_testing_script_status_v1(control, -EIO) == 0);
    assert(trevrpc_transport_testing_script_status_v1(control, 0) == 0);
    assert(trevrpc_transport_listen_v1(transport, &endpoint, &out_listener) == -EIO);
    assert(trevrpc_transport_listen_v1(transport, &endpoint, &out_listener) == 0);

    assert(trevrpc_transport_testing_set_checkpoint_v1(
               control, TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_SEND_ADMISSION, 1) == 0);
    assert(pthread_create(&send_thread_id, NULL, send_thread, &send_args) == 0);
    assert(trevrpc_transport_testing_wait_checkpoint_v1(
               control, TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_SEND_ADMISSION) == 0);
    assert(trevrpc_transport_testing_counters_v1_init(&counters, sizeof(counters)) == 0);
    assert(trevrpc_transport_testing_get_counters_v1(control, &counters) == 0);
    assert(counters.operation_calls[TREVRPC_TRANSPORT_TESTING_OPERATION_STREAM_SEND] == 1);
    assert(trevrpc_transport_testing_release_checkpoint_v1(
               control, TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_SEND_ADMISSION) == 0);
    assert(pthread_join(send_thread_id, NULL) == 0);
    assert(send_args.result == 0);

    assert(trevrpc_transport_testing_event_v1_init(&event_spec, sizeof(event_spec)) == 0);
    event_spec.kind = TREVRPC_TRANSPORT_EVENT_DIAGNOSTIC;
    event_spec.subject_kind = TREVRPC_TRANSPORT_OBJECT_NONE;
    assert(trevrpc_transport_testing_push_event_v1(control, &event_spec) == 0);
    assert(trevrpc_transport_testing_set_checkpoint_v1(control, TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_POP, 1) == 0);
    assert(pthread_create(&pop_thread, NULL, pop_event_thread, &pop_args) == 0);
    assert(trevrpc_transport_testing_wait_checkpoint_v1(control, TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_POP) == 0);
    assert(pop_args.event == NULL);
    assert(
        trevrpc_transport_testing_release_checkpoint_v1(control, TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_POP) == 0);
    assert(pthread_join(pop_thread, NULL) == 0);
    assert(trevrpc_transport_testing_set_checkpoint_v1(control, TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_POP, 0) == 0);
    assert(pop_args.result == 0 && pop_args.event != NULL);
    trevrpc_transport_event_release(transport, pop_args.event);

    assert(trevrpc_transport_testing_push_stopped_v1(control, -ECONNRESET) == 0);
    assert(trevrpc_transport_next_event(transport, &pop_args.event) == 0);
    trevrpc_transport_event_release(transport, pop_args.event);
    assert(trevrpc_transport_testing_counters_v1_init(&counters, sizeof(counters)) == 0);
    assert(trevrpc_transport_testing_get_counters_v1(control, &counters) == 0);
    assert(counters.events_injected == 2 && counters.events_popped == 2);
    assert(counters.last_send_operation_id == 77);
    assert(trevrpc_transport_release(transport) == 0);
}

int main(void) {
    assert(trevrpc_transport_testing_abi_version() == TREVRPC_TRANSPORT_TESTING_ABI_VERSION);
    test_wakes_events_and_protocol();
    test_admission_and_receive();
    test_stopped_state_changes_only_after_enqueue();
    test_statuses_checkpoints_counters_and_stop();
    return 0;
}
