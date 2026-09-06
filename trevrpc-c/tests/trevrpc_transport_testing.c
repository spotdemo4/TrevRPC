#define _POSIX_C_SOURCE 200809L

#include "trevrpc_transport_testing.h"

#include "trevrpc_rpc_transport_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define TESTING_OWNER UINT64_C(0x5445535452414e53)
#define TESTING_LISTENER_SLOT 1u
#define TESTING_CONNECTION_SLOT 2u
#define TESTING_STREAM_SLOT 3u

struct trevrpc_transport_testing_control;
typedef struct testing_event testing_event;
typedef struct testing_receive testing_receive;

typedef struct testing_checkpoint {
    int enabled;
    int entered;
    int release;
} testing_checkpoint;

struct testing_event {
    struct trevrpc_transport_testing_control* control;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_admission_info admission;
    trevrpc_rpc_transport_event_protocol_info protocol;
    uint8_t* data;
    uint8_t* method;
    uint8_t* path;
    uint8_t* authority;
    uint8_t* origin;
    trevrpc_transport_header_field_v1* headers;
    size_t header_count;
    int is_admission;
    int admission_answered;
};

struct testing_receive {
    struct trevrpc_transport_testing_control* control;
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_transport_receive_info info;
    uint8_t* data;
    testing_receive* next;
};

struct trevrpc_transport_testing_control {
    trevrpc_rpc_transport base;
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    int wake_read[TREVRPC_TRANSPORT_TESTING_WAKE_COUNT];
    int wake_write[TREVRPC_TRANSPORT_TESTING_WAKE_COUNT];
    size_t next_wake;
    trevrpc_rpc_transport_event** events;
    size_t event_capacity;
    size_t event_head;
    size_t event_tail;
    size_t event_count;
    size_t receive_capacity;
    size_t receive_count;
    testing_receive* receives_head;
    testing_receive* receives_tail;
    int32_t* statuses;
    size_t status_capacity;
    size_t status_head;
    size_t status_tail;
    size_t status_count;
    uint64_t next_sequence;
    uint16_t listener_port;
    uint32_t state;
    int32_t terminal_status;
    int32_t poll_timeout_ms;
    testing_checkpoint post_pop;
    testing_checkpoint post_send_admission;
    testing_checkpoint post_finish_send_admission;
    trevrpc_transport_testing_counters_v1 counters;
};

static trevrpc_transport_handle_v1 listener_handle(void) {
    trevrpc_transport_handle_v1 handle = {TESTING_OWNER, TESTING_LISTENER_SLOT, 1u};
    return handle;
}

static trevrpc_transport_handle_v1 connection_handle(void) {
    trevrpc_transport_handle_v1 handle = {TESTING_OWNER, TESTING_CONNECTION_SLOT, 1u};
    return handle;
}

static trevrpc_transport_handle_v1 stream_handle(void) {
    trevrpc_transport_handle_v1 handle = {TESTING_OWNER, TESTING_STREAM_SLOT, 1u};
    return handle;
}

static int initialize_structure(void* structure, size_t size, size_t required_size, uint32_t* version) {
    uint32_t size32;
    if (structure == NULL || size < required_size || size > UINT32_MAX) {
        return -EINVAL;
    }
    size32 = (uint32_t)size;
    memset(structure, 0, size);
    memcpy(structure, &size32, sizeof(size32));
    memcpy((uint8_t*)structure + sizeof(size32), version, sizeof(*version));
    return 0;
}

static int valid_structure(const void* structure, uint32_t size, uint32_t version, size_t required_size) {
    if (structure == NULL || size < required_size) {
        return -EINVAL;
    }
    return version == TREVRPC_TRANSPORT_TESTING_STRUCT_VERSION_1 ? 0 : -ENOTSUP;
}

static int handles_equal(trevrpc_rpc_transport_handle left, trevrpc_rpc_transport_handle right) {
    return left.owner == right.owner && left.slot == right.slot && left.generation == right.generation;
}

static int valid_handle(trevrpc_transport_handle_v1 handle, uint32_t kind) {
    trevrpc_transport_handle_v1 expected;
    if (kind == TREVRPC_TRANSPORT_OBJECT_LISTENER) {
        expected = listener_handle();
    } else if (kind == TREVRPC_TRANSPORT_OBJECT_CONNECTION) {
        expected = connection_handle();
    } else if (kind == TREVRPC_TRANSPORT_OBJECT_STREAM) {
        expected = stream_handle();
    } else {
        return 0;
    }
    return handle.owner == expected.owner && handle.slot == expected.slot && handle.generation == expected.generation;
}

/* Keep this spelling local to make accidental use of the public opaque type impossible. */
typedef struct trevrpc_transport_testing_control trevrc_testing_control;

static void drain_wake(trevrc_testing_control* control) {
    uint8_t bytes[64];
    size_t index;
    for (index = 0; index < TREVRPC_TRANSPORT_TESTING_WAKE_COUNT; ++index) {
        while (read(control->wake_read[index], bytes, sizeof(bytes)) > 0) {
        }
    }
}

static void signal_wake_locked(trevrc_testing_control* control) {
    uint8_t byte = 1;
    size_t index = control->next_wake++ % TREVRPC_TRANSPORT_TESTING_WAKE_COUNT;
    for (;;) {
        ssize_t written = write(control->wake_write[index], &byte, sizeof(byte));
        if (written == (ssize_t)sizeof(byte) || (written < 0 && errno == EAGAIN)) {
            return;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        return;
    }
}

static int take_status_locked(trevrc_testing_control* control, uint32_t operation) {
    int32_t result = 0;
    control->counters.last_operation = operation;
    if (operation < TREVRPC_TRANSPORT_TESTING_OPERATION_COUNT) {
        ++control->counters.operation_calls[operation];
    }
    if (control->status_count != 0) {
        result = control->statuses[control->status_head];
        control->status_head = (control->status_head + 1u) % control->status_capacity;
        --control->status_count;
    }
    control->counters.last_status = result;
    return result;
}

static void checkpoint_locked(trevrc_testing_control* control, testing_checkpoint* checkpoint) {
    if (!checkpoint->enabled) {
        return;
    }
    checkpoint->entered = 1;
    checkpoint->release = 0;
    pthread_cond_broadcast(&control->condition);
    while (!checkpoint->release) {
        pthread_cond_wait(&control->condition, &control->mutex);
    }
    checkpoint->entered = 0;
}

static testing_checkpoint* checkpoint_for(trevrc_testing_control* control, uint32_t checkpoint) {
    switch (checkpoint) {
    case TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_POP:
        return &control->post_pop;
    case TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_SEND_ADMISSION:
        return &control->post_send_admission;
    case TREVRPC_TRANSPORT_TESTING_CHECKPOINT_POST_FINISH_SEND_ADMISSION:
        return &control->post_finish_send_admission;
    default:
        return NULL;
    }
}

static int copy_bytes(uint8_t** out, const uint8_t* data, uint64_t length) {
    if (length > SIZE_MAX || (data == NULL && length != 0)) {
        return -EINVAL;
    }
    *out = NULL;
    if (length == 0) {
        return 0;
    }
    *out = malloc((size_t)length);
    if (*out == NULL) {
        return -ENOMEM;
    }
    memcpy(*out, data, (size_t)length);
    return 0;
}

static void free_event(testing_event* event) {
    size_t index;
    if (event == NULL) {
        return;
    }
    free(event->data);
    free(event->method);
    free(event->path);
    free(event->authority);
    free(event->origin);
    if (event->headers != NULL) {
        for (index = 0; index < event->header_count; ++index) {
            free((void*)event->headers[index].name);
            free((void*)event->headers[index].value);
        }
    }
    free(event->headers);
    free(event);
}

static int copy_event(testing_event** out, const trevrpc_transport_testing_event_v1* source) {
    testing_event* event;
    size_t index;
    int result;
    if (valid_structure(source, source->struct_size, source->struct_version, sizeof(*source)) != 0 ||
        source->reserved0 != 0 || source->data_len > SIZE_MAX || source->header_count > SIZE_MAX ||
        source->method_len > SIZE_MAX || source->path_len > SIZE_MAX || source->authority_len > SIZE_MAX ||
        source->origin_len > SIZE_MAX || source->header_count > SIZE_MAX / sizeof(*event->headers)) {
        return -EINVAL;
    }
    event = calloc(1, sizeof(*event));
    if (event == NULL) {
        return -ENOMEM;
    }
    event->info.kind = source->kind;
    event->info.flags = source->flags;
    event->info.status = source->status;
    event->info.subject_kind = source->subject_kind;
    event->info.subject =
        (trevrpc_rpc_transport_handle){source->subject.owner, source->subject.slot, source->subject.generation};
    event->info.parent =
        (trevrpc_rpc_transport_handle){source->parent.owner, source->parent.slot, source->parent.generation};
    event->info.operation_id = source->operation_id;
    event->info.application_error_code = source->application_error_code;
    event->info.provider_error_code = source->provider_error_code;
    event->protocol.protocol = source->protocol;
    result = copy_bytes(&event->data, source->data, source->data_len);
    if (result != 0) {
        free_event(event);
        return result;
    }
    event->info.data = event->data;
    event->info.data_len = source->data_len;
    result = copy_bytes(&event->method, source->method, source->method_len);
    if (result != 0) {
        free_event(event);
        return result;
    }
    result = copy_bytes(&event->path, source->path, source->path_len);
    if (result != 0) {
        free_event(event);
        return result;
    }
    result = copy_bytes(&event->authority, source->authority, source->authority_len);
    if (result != 0) {
        free_event(event);
        return result;
    }
    result = copy_bytes(&event->origin, source->origin, source->origin_len);
    if (result != 0) {
        free_event(event);
        return result;
    }
    if (source->header_count != 0) {
        event->headers = calloc((size_t)source->header_count, sizeof(*event->headers));
        if (event->headers == NULL) {
            free_event(event);
            return -ENOMEM;
        }
        event->header_count = (size_t)source->header_count;
        for (index = 0; index < event->header_count; ++index) {
            result = copy_bytes(
                (uint8_t**)&event->headers[index].name, source->headers[index].name, source->headers[index].name_len);
            if (result != 0) {
                free_event(event);
                return result;
            }
            result = copy_bytes((uint8_t**)&event->headers[index].value,
                source->headers[index].value,
                source->headers[index].value_len);
            if (result != 0) {
                free_event(event);
                return result;
            }
            event->headers[index].name_len = source->headers[index].name_len;
            event->headers[index].value_len = source->headers[index].value_len;
        }
    }
    event->info.sequence = 0;
    if (source->kind == TREVRPC_TRANSPORT_EVENT_HTTP3_ADMISSION ||
        source->kind == TREVRPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION) {
        event->is_admission = 1;
        event->admission.protocol = source->protocol;
        event->admission.listener = event->info.parent;
        event->admission.headers = event->headers;
        event->admission.header_count = source->header_count;
        event->admission.method = event->method;
        event->admission.method_len = source->method_len;
        event->admission.path = event->path;
        event->admission.path_len = source->path_len;
        event->admission.authority = event->authority;
        event->admission.authority_len = source->authority_len;
        event->admission.origin = event->origin;
        event->admission.origin_len = source->origin_len;
    }
    *out = event;
    return 0;
}

static int push_event_locked(trevrc_testing_control* control, testing_event* event) {
    if (control->event_count == control->event_capacity) {
        return -EAGAIN;
    }
    event->info.sequence = ++control->next_sequence;
    control->events[control->event_tail] = (trevrpc_rpc_transport_event*)event;
    control->event_tail = (control->event_tail + 1u) % control->event_capacity;
    ++control->event_count;
    ++control->counters.events_injected;
    signal_wake_locked(control);
    pthread_cond_broadcast(&control->condition);
    return 0;
}

static int testing_get_wake_source(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_wake* wake) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    if (wake == NULL) {
        return -EINVAL;
    }
    wake->kind = TREVRPC_RPC_TRANSPORT_WAKE_SOURCE_POSIX_FD;
    wake->flags = TREVRPC_RPC_TRANSPORT_WAKE_FLAG_BORROWED | TREVRPC_RPC_TRANSPORT_WAKE_FLAG_LEVEL_TRIGGERED;
    wake->native_handle = control->wake_read[0];
    return 0;
}

static int testing_get_wake_sources(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_wake* wakes, size_t capacity, size_t* out_count) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    size_t index;
    if (wakes == NULL || out_count == NULL || capacity < TREVRPC_TRANSPORT_TESTING_WAKE_COUNT) {
        return -ENOSPC;
    }
    for (index = 0; index < TREVRPC_TRANSPORT_TESTING_WAKE_COUNT; ++index) {
        wakes[index].kind = TREVRPC_RPC_TRANSPORT_WAKE_SOURCE_POSIX_FD;
        wakes[index].flags = TREVRPC_RPC_TRANSPORT_WAKE_FLAG_BORROWED | TREVRPC_RPC_TRANSPORT_WAKE_FLAG_LEVEL_TRIGGERED;
        wakes[index].native_handle = control->wake_read[index];
    }
    *out_count = TREVRPC_TRANSPORT_TESTING_WAKE_COUNT;
    return 0;
}

static int testing_next_event(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_event** out_event) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    testing_event* event;
    int drain_wake_source;
    if (out_event == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&control->mutex);
    if (control->event_count == 0) {
        pthread_mutex_unlock(&control->mutex);
        return -EAGAIN;
    }
    event = (testing_event*)control->events[control->event_head];
    control->events[control->event_head] = NULL;
    control->event_head = (control->event_head + 1u) % control->event_capacity;
    --control->event_count;
    ++control->counters.events_popped;
    drain_wake_source = control->event_count == 0;
    checkpoint_locked(control, &control->post_pop);
    pthread_mutex_unlock(&control->mutex);

    if (drain_wake_source) {
        drain_wake(control);
        pthread_mutex_lock(&control->mutex);
        if (control->event_count != 0) {
            signal_wake_locked(control);
        }
        pthread_mutex_unlock(&control->mutex);
    }
    *out_event = (trevrpc_rpc_transport_event*)event;
    return 0;
}

static int testing_event_get_info(
    const trevrpc_rpc_transport_event* transport_event, trevrpc_rpc_transport_event_info* info) {
    const testing_event* event = (const testing_event*)transport_event;
    if (event == NULL || info == NULL) {
        return -EINVAL;
    }
    *info = event->info;
    return 0;
}

static int testing_event_get_admission_info(
    const trevrpc_rpc_transport_event* transport_event, trevrpc_rpc_transport_admission_info* info) {
    const testing_event* event = (const testing_event*)transport_event;
    if (event == NULL || info == NULL) {
        return -EINVAL;
    }
    return event->is_admission ? ((*info = event->admission), 0) : -ENOTSUP;
}

static int testing_event_get_protocol_info(
    const trevrpc_rpc_transport_event* transport_event, trevrpc_rpc_transport_event_protocol_info* info) {
    const testing_event* event = (const testing_event*)transport_event;
    if (event == NULL || info == NULL) {
        return -EINVAL;
    }
    *info = event->protocol;
    return 0;
}

static void testing_event_release(trevrpc_rpc_transport_event* transport_event) {
    testing_event* event = (testing_event*)transport_event;
    if (event == NULL) {
        return;
    }
    pthread_mutex_lock(&((trevrc_testing_control*)event->control)->mutex);
    ++((trevrc_testing_control*)event->control)->counters.events_released;
    pthread_mutex_unlock(&((trevrc_testing_control*)event->control)->mutex);
    free_event(event);
}

static int testing_admission_respond(const trevrpc_rpc_transport_event* transport_event, uint16_t status) {
    testing_event* event = (testing_event*)transport_event;
    trevrc_testing_control* control;
    int result;
    if (event == NULL || !event->is_admission || event->admission_answered ||
        (status != 200 && (status < 400 || status > 599))) {
        return -EINVAL;
    }
    control = (trevrc_testing_control*)event->control;
    pthread_mutex_lock(&control->mutex);
    event->admission_answered = 1;
    ++control->counters.admissions_responded;
    control->counters.last_admission_status = status;
    result = take_status_locked(control, TREVRPC_TRANSPORT_TESTING_OPERATION_ADMISSION_RESPOND);
    pthread_mutex_unlock(&control->mutex);
    return result;
}

static int testing_receive_get_info(
    const trevrpc_rpc_transport_receive* transport_receive, trevrpc_rpc_transport_receive_info* info) {
    const testing_receive* receive = (const testing_receive*)transport_receive;
    if (receive == NULL || info == NULL) {
        return -EINVAL;
    }
    *info = receive->info;
    return 0;
}

static void testing_receive_release(trevrpc_rpc_transport_receive* transport_receive) {
    testing_receive* receive = (testing_receive*)transport_receive;
    if (receive == NULL) {
        return;
    }
    pthread_mutex_lock(&((trevrc_testing_control*)receive->control)->mutex);
    ++((trevrc_testing_control*)receive->control)->counters.receives_released;
    pthread_mutex_unlock(&((trevrc_testing_control*)receive->control)->mutex);
    free(receive->data);
    free(receive);
}

static int testing_get_diagnostics(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_diagnostics* diagnostics) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    if (diagnostics == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&control->mutex);
    memset(diagnostics, 0, sizeof(*diagnostics));
    diagnostics->state = control->state;
    diagnostics->terminal_status = control->terminal_status;
    diagnostics->event_capacity = (uint32_t)control->event_capacity;
    diagnostics->queue_depth = (uint32_t)control->event_count;
    diagnostics->events_enqueued = control->counters.events_injected;
    diagnostics->events_dequeued = control->counters.events_popped;
    diagnostics->events_rejected = 0;
    diagnostics->live_listeners = 1;
    diagnostics->live_connections = 1;
    diagnostics->live_streams = 1;
    pthread_mutex_unlock(&control->mutex);
    return 0;
}

static int testing_poll_timeout_ms(trevrpc_rpc_transport* transport) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    int result;
    pthread_mutex_lock(&control->mutex);
    result = control->poll_timeout_ms;
    pthread_mutex_unlock(&control->mutex);
    return result;
}

static int testing_listen(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_endpoint_config* config,
    trevrpc_rpc_transport_handle* out_listener) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    int result;
    (void)config;
    if (out_listener == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&control->mutex);
    result = take_status_locked(control, TREVRPC_TRANSPORT_TESTING_OPERATION_LISTEN);
    if (result == 0) {
        *out_listener = (trevrpc_rpc_transport_handle){TESTING_OWNER, TESTING_LISTENER_SLOT, 1u};
    }
    pthread_mutex_unlock(&control->mutex);
    return result;
}

static int testing_listener_port(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle listener, uint16_t* out_port) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    int result;
    if (out_port == NULL ||
        !valid_handle((trevrpc_transport_handle_v1){listener.owner, listener.slot, listener.generation},
            TREVRPC_TRANSPORT_OBJECT_LISTENER)) {
        return -ESTALE;
    }
    pthread_mutex_lock(&control->mutex);
    result = take_status_locked(control, TREVRPC_TRANSPORT_TESTING_OPERATION_LISTENER_GET_PORT);
    if (result == 0) {
        *out_port = control->listener_port;
    }
    pthread_mutex_unlock(&control->mutex);
    return result;
}

static int testing_dial(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_endpoint_config* config,
    uint64_t operation_id,
    trevrpc_rpc_transport_handle* out_connection) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    int result;
    (void)config;
    if (out_connection == NULL || operation_id == 0) {
        return -EINVAL;
    }
    pthread_mutex_lock(&control->mutex);
    result = take_status_locked(control, TREVRPC_TRANSPORT_TESTING_OPERATION_DIAL);
    if (result == 0) {
        *out_connection = (trevrpc_rpc_transport_handle){TESTING_OWNER, TESTING_CONNECTION_SLOT, 1u};
        control->counters.last_operation_id = operation_id;
        control->counters.last_dial_operation_id = operation_id;
    }
    pthread_mutex_unlock(&control->mutex);
    return result;
}

static int testing_dial_cancel(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    int result;
    if (!valid_handle((trevrpc_transport_handle_v1){connection.owner, connection.slot, connection.generation},
            TREVRPC_TRANSPORT_OBJECT_CONNECTION)) {
        return -ESTALE;
    }
    pthread_mutex_lock(&control->mutex);
    result = take_status_locked(control, TREVRPC_TRANSPORT_TESTING_OPERATION_DIAL_CANCEL);
    pthread_mutex_unlock(&control->mutex);
    return result;
}

static int testing_open_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    uint64_t operation_id,
    trevrpc_rpc_transport_handle* out_stream) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    int result;
    if (out_stream == NULL || operation_id == 0 ||
        !valid_handle((trevrpc_transport_handle_v1){connection.owner, connection.slot, connection.generation},
            TREVRPC_TRANSPORT_OBJECT_CONNECTION)) {
        return -EINVAL;
    }
    pthread_mutex_lock(&control->mutex);
    result = take_status_locked(control, TREVRPC_TRANSPORT_TESTING_OPERATION_OPEN_BIDI_STREAM);
    if (result == 0) {
        *out_stream = (trevrpc_rpc_transport_handle){TESTING_OWNER, TESTING_STREAM_SLOT, 1u};
        control->counters.last_operation_id = operation_id;
        control->counters.last_open_operation_id = operation_id;
    }
    pthread_mutex_unlock(&control->mutex);
    return result;
}

static int testing_stream_send(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle stream,
    uint64_t operation_id,
    const uint8_t* data,
    size_t data_len) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    int result;
    (void)data;
    (void)data_len;
    if (operation_id == 0 || (data == NULL && data_len != 0) ||
        !valid_handle((trevrpc_transport_handle_v1){stream.owner, stream.slot, stream.generation},
            TREVRPC_TRANSPORT_OBJECT_STREAM)) {
        return -EINVAL;
    }
    pthread_mutex_lock(&control->mutex);
    result = take_status_locked(control, TREVRPC_TRANSPORT_TESTING_OPERATION_STREAM_SEND);
    if (result == 0) {
        control->counters.last_operation_id = operation_id;
        control->counters.last_send_operation_id = operation_id;
        checkpoint_locked(control, &control->post_send_admission);
    }
    pthread_mutex_unlock(&control->mutex);
    return result;
}

static int testing_stream_receive(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle stream,
    trevrpc_rpc_transport_receive** out_receive) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    testing_receive* receive;
    int result;
    if (out_receive == NULL ||
        !valid_handle((trevrpc_transport_handle_v1){stream.owner, stream.slot, stream.generation},
            TREVRPC_TRANSPORT_OBJECT_STREAM)) {
        return -ESTALE;
    }
    pthread_mutex_lock(&control->mutex);
    result = take_status_locked(control, TREVRPC_TRANSPORT_TESTING_OPERATION_STREAM_RECEIVE);
    if (result != 0) {
        pthread_mutex_unlock(&control->mutex);
        return result;
    }
    receive = control->receives_head;
    while (receive != NULL && !handles_equal(receive->stream,
                                  (trevrpc_rpc_transport_handle){stream.owner, stream.slot, stream.generation})) {
        receive = receive->next;
    }
    if (receive != NULL) {
        testing_receive** link = &control->receives_head;
        while (*link != receive) {
            link = &(*link)->next;
        }
        *link = receive->next;
        if (control->receives_tail == receive) {
            control->receives_tail = NULL;
            if (control->receives_head != NULL) {
                testing_receive* tail = control->receives_head;
                while (tail->next != NULL) {
                    tail = tail->next;
                }
                control->receives_tail = tail;
            }
        }
        receive->next = NULL;
        --control->receive_count;
        ++control->counters.receives_popped;
    }
    pthread_mutex_unlock(&control->mutex);
    if (receive == NULL) {
        return -EAGAIN;
    }
    *out_receive = (trevrpc_rpc_transport_receive*)receive;
    return 0;
}

static int testing_finish_send(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    int result;
    if (!valid_handle((trevrpc_transport_handle_v1){stream.owner, stream.slot, stream.generation},
            TREVRPC_TRANSPORT_OBJECT_STREAM)) {
        return -ESTALE;
    }
    pthread_mutex_lock(&control->mutex);
    result = take_status_locked(control, TREVRPC_TRANSPORT_TESTING_OPERATION_FINISH_SEND);
    if (result == 0) {
        checkpoint_locked(control, &control->post_finish_send_admission);
    }
    pthread_mutex_unlock(&control->mutex);
    return result;
}

static int testing_abort_half(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, uint64_t code, uint32_t operation) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    int result;
    (void)code;
    if (!valid_handle((trevrpc_transport_handle_v1){stream.owner, stream.slot, stream.generation},
            TREVRPC_TRANSPORT_OBJECT_STREAM)) {
        return -ESTALE;
    }
    pthread_mutex_lock(&control->mutex);
    result = take_status_locked(control, operation);
    pthread_mutex_unlock(&control->mutex);
    return result;
}

static int testing_abort_receive(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, uint64_t code) {
    return testing_abort_half(transport, stream, code, TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_RECEIVE);
}
static int testing_abort_send(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, uint64_t code) {
    return testing_abort_half(transport, stream, code, TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_SEND);
}
static int testing_abort_stream(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, uint64_t code) {
    return testing_abort_half(transport, stream, code, TREVRPC_TRANSPORT_TESTING_OPERATION_ABORT_STREAM);
}

static int testing_close_stream(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream) {
    return testing_abort_half(transport, stream, 0, TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_STREAM);
}
static int testing_close_connection(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, uint64_t code) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    int result;
    (void)code;
    if (!valid_handle((trevrpc_transport_handle_v1){connection.owner, connection.slot, connection.generation},
            TREVRPC_TRANSPORT_OBJECT_CONNECTION)) {
        return -ESTALE;
    }
    pthread_mutex_lock(&control->mutex);
    result = take_status_locked(control, TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_CONNECTION);
    pthread_mutex_unlock(&control->mutex);
    return result;
}
static int testing_close_listener(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle listener) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    int result;
    if (!valid_handle((trevrpc_transport_handle_v1){listener.owner, listener.slot, listener.generation},
            TREVRPC_TRANSPORT_OBJECT_LISTENER)) {
        return -ESTALE;
    }
    pthread_mutex_lock(&control->mutex);
    result = take_status_locked(control, TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_LISTENER);
    pthread_mutex_unlock(&control->mutex);
    return result;
}

static int testing_release_handle(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle, uint32_t kind) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    int result;
    (void)handle;
    (void)kind;
    pthread_mutex_lock(&control->mutex);
    result = take_status_locked(control, TREVRPC_TRANSPORT_TESTING_OPERATION_RELEASE_HANDLE);
    pthread_mutex_unlock(&control->mutex);
    return result;
}

static int testing_close(trevrpc_rpc_transport* transport) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    int result;
    pthread_mutex_lock(&control->mutex);
    result = take_status_locked(control, TREVRPC_TRANSPORT_TESTING_OPERATION_CLOSE_TRANSPORT);
    if (result == 0 && control->state == TREVRPC_TRANSPORT_STATE_RUNNING) {
        control->state = TREVRPC_TRANSPORT_STATE_STOPPING;
    }
    pthread_mutex_unlock(&control->mutex);
    return result;
}

static int testing_drain(trevrpc_rpc_transport* transport) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    int result;
    pthread_mutex_lock(&control->mutex);
    result = take_status_locked(control, TREVRPC_TRANSPORT_TESTING_OPERATION_DRAIN);
    if (result == 0 && (control->event_count != 0 || control->receive_count != 0 ||
                           control->counters.events_popped != control->counters.events_released ||
                           control->counters.receives_popped != control->counters.receives_released)) {
        result = -EBUSY;
    }
    pthread_mutex_unlock(&control->mutex);
    return result;
}

static void testing_destroy(trevrpc_rpc_transport* transport) {
    trevrc_testing_control* control = (trevrc_testing_control*)transport;
    size_t index;
    testing_receive* receive;
    pthread_mutex_lock(&control->mutex);
    for (index = 0; index < control->event_capacity; ++index) {
        free_event((testing_event*)control->events[index]);
    }
    receive = control->receives_head;
    while (receive != NULL) {
        testing_receive* next = receive->next;
        free(receive->data);
        free(receive);
        receive = next;
    }
    pthread_mutex_unlock(&control->mutex);
    for (index = 0; index < TREVRPC_TRANSPORT_TESTING_WAKE_COUNT; ++index) {
        if (control->wake_read[index] >= 0) {
            close(control->wake_read[index]);
        }
        if (control->wake_write[index] >= 0) {
            close(control->wake_write[index]);
        }
    }
    free(control->events);
    free(control->statuses);
    pthread_cond_destroy(&control->condition);
    pthread_mutex_destroy(&control->mutex);
    free(control);
}

static const trevrpc_rpc_transport_ops testing_ops = {
    .get_wake_source = testing_get_wake_source,
    .next_event = testing_next_event,
    .event_get_info = testing_event_get_info,
    .event_release = testing_event_release,
    .receive_get_info = testing_receive_get_info,
    .receive_release = testing_receive_release,
    .get_diagnostics = testing_get_diagnostics,
    .poll_timeout_ms = testing_poll_timeout_ms,
    .endpoint_listen = testing_listen,
    .endpoint_get_port = testing_listener_port,
    .endpoint_dial = testing_dial,
    .dial_cancel = testing_dial_cancel,
    .stream_open = testing_open_stream,
    .stream_send = testing_stream_send,
    .stream_receive = testing_stream_receive,
    .stream_finish_send = testing_finish_send,
    .stream_abort_receive = testing_abort_receive,
    .stream_abort_send = testing_abort_send,
    .stream_abort = testing_abort_stream,
    .stream_close = testing_close_stream,
    .connection_close = testing_close_connection,
    .listener_close = testing_close_listener,
    .release_handle = testing_release_handle,
    .close = testing_close,
    .drain = testing_drain,
    .destroy = testing_destroy,
    .get_wake_sources = testing_get_wake_sources,
    .event_get_admission_info = testing_event_get_admission_info,
    .event_get_protocol_info = testing_event_get_protocol_info,
    .admission_respond = testing_admission_respond,
};

uint32_t trevrpc_transport_testing_abi_version(void) {
    return TREVRPC_TRANSPORT_TESTING_ABI_VERSION;
}

void trevrpc_transport_testing_abi_1_anchor(void) {
}

int trevrpc_transport_testing_config_v1_init(trevrpc_transport_testing_config_v1* config, size_t struct_size) {
    uint32_t version = TREVRPC_TRANSPORT_TESTING_STRUCT_VERSION_1;
    int result = initialize_structure(config, struct_size, sizeof(*config), &version);
    if (result == 0) {
        config->event_capacity = TREVRPC_TRANSPORT_TESTING_DEFAULT_EVENT_CAPACITY;
        config->receive_capacity = TREVRPC_TRANSPORT_TESTING_DEFAULT_RECEIVE_CAPACITY;
        config->status_capacity = TREVRPC_TRANSPORT_TESTING_DEFAULT_STATUS_CAPACITY;
        config->listener_port = TREVRPC_TRANSPORT_TESTING_DEFAULT_PORT;
    }
    return result;
}

int trevrpc_transport_testing_event_v1_init(trevrpc_transport_testing_event_v1* event, size_t struct_size) {
    uint32_t version = TREVRPC_TRANSPORT_TESTING_STRUCT_VERSION_1;
    return initialize_structure(event, struct_size, sizeof(*event), &version);
}

int trevrpc_transport_testing_receive_v1_init(trevrpc_transport_testing_receive_v1* receive, size_t struct_size) {
    uint32_t version = TREVRPC_TRANSPORT_TESTING_STRUCT_VERSION_1;
    return initialize_structure(receive, struct_size, sizeof(*receive), &version);
}

int trevrpc_transport_testing_counters_v1_init(trevrpc_transport_testing_counters_v1* counters, size_t struct_size) {
    uint32_t version = TREVRPC_TRANSPORT_TESTING_STRUCT_VERSION_1;
    return initialize_structure(counters, struct_size, sizeof(*counters), &version);
}

int trevrpc_transport_testing_create_v1(const trevrpc_transport_config_v1* transport_config,
    const trevrpc_transport_testing_config_v1* provider_config,
    trevrpc_transport** out_transport,
    trevrpc_transport_testing_control** out_control) {
    trevrc_testing_control* control;
    size_t index;
    int descriptors[2];
    (void)transport_config;
    if (out_transport == NULL || out_control == NULL || provider_config == NULL ||
        valid_structure(
            provider_config, provider_config->struct_size, provider_config->struct_version, sizeof(*provider_config)) !=
            0 ||
        provider_config->event_capacity == 0 || provider_config->receive_capacity == 0 ||
        provider_config->status_capacity == 0 || provider_config->reserved0 != 0 || provider_config->reserved1 != 0 ||
        memcmp(provider_config->reserved, (uint64_t[4]){0, 0, 0, 0}, sizeof(provider_config->reserved)) != 0) {
        return -EINVAL;
    }
    control = calloc(1, sizeof(*control));
    if (control == NULL) {
        return -ENOMEM;
    }
    for (index = 0; index < TREVRPC_TRANSPORT_TESTING_WAKE_COUNT; ++index) {
        control->wake_read[index] = -1;
        control->wake_write[index] = -1;
    }
    control->event_capacity = provider_config->event_capacity;
    control->receive_capacity = provider_config->receive_capacity;
    control->status_capacity = provider_config->status_capacity;
    control->listener_port = provider_config->listener_port;
    control->poll_timeout_ms = -1;
    control->state = TREVRPC_TRANSPORT_STATE_RUNNING;
    control->events = calloc(control->event_capacity, sizeof(*control->events));
    control->statuses = calloc(control->status_capacity, sizeof(*control->statuses));
    if (control->events == NULL || control->statuses == NULL) {
        free(control->events);
        free(control->statuses);
        free(control);
        return -ENOMEM;
    }
    if (pthread_mutex_init(&control->mutex, NULL) != 0) {
        free(control->events);
        free(control->statuses);
        free(control);
        return -ENOMEM;
    }
    if (pthread_cond_init(&control->condition, NULL) != 0) {
        pthread_mutex_destroy(&control->mutex);
        free(control->events);
        free(control->statuses);
        free(control);
        return -ENOMEM;
    }
    for (index = 0; index < TREVRPC_TRANSPORT_TESTING_WAKE_COUNT; ++index) {
        if (pipe(descriptors) != 0) {
            testing_destroy((trevrpc_rpc_transport*)control);
            return -errno;
        }
        control->wake_read[index] = descriptors[0];
        control->wake_write[index] = descriptors[1];
        (void)fcntl(descriptors[0], F_SETFL, O_NONBLOCK);
        (void)fcntl(descriptors[1], F_SETFL, O_NONBLOCK);
    }
    control->base.ops = &testing_ops;
    control->counters.struct_size = sizeof(control->counters);
    control->counters.struct_version = TREVRPC_TRANSPORT_TESTING_STRUCT_VERSION_1;
    *out_transport = (trevrpc_transport*)control;
    *out_control = control;
    return 0;
}

int trevrpc_transport_testing_push_event_v1(
    trevrpc_transport_testing_control* opaque_control, const trevrpc_transport_testing_event_v1* event_spec) {
    trevrc_testing_control* control = (trevrc_testing_control*)opaque_control;
    testing_event* event = NULL;
    int result;
    if (control == NULL || event_spec == NULL) {
        return -EINVAL;
    }
    result = copy_event(&event, event_spec);
    if (result != 0) {
        return result;
    }
    event->control = control;
    pthread_mutex_lock(&control->mutex);
    result = push_event_locked(control, event);
    pthread_mutex_unlock(&control->mutex);
    if (result != 0) {
        free_event(event);
    }
    return result;
}

int trevrpc_transport_testing_push_receive_v1(
    trevrpc_transport_testing_control* opaque_control, const trevrpc_transport_testing_receive_v1* receive_spec) {
    trevrc_testing_control* control = (trevrc_testing_control*)opaque_control;
    testing_receive* receive;
    int result;
    if (control == NULL || receive_spec == NULL ||
        valid_structure(receive_spec, receive_spec->struct_size, receive_spec->struct_version, sizeof(*receive_spec)) !=
            0 ||
        receive_spec->reserved0 != 0 || receive_spec->data_len > SIZE_MAX ||
        memcmp(receive_spec->reserved, (uint64_t[3]){0, 0, 0}, sizeof(receive_spec->reserved)) != 0) {
        return -EINVAL;
    }
    receive = calloc(1, sizeof(*receive));
    if (receive == NULL) {
        return -ENOMEM;
    }
    result = copy_bytes(&receive->data, receive_spec->data, receive_spec->data_len);
    if (result != 0) {
        free(receive);
        return result;
    }
    receive->control = control;
    receive->stream = (trevrpc_rpc_transport_handle){
        receive_spec->stream.owner, receive_spec->stream.slot, receive_spec->stream.generation};
    receive->info.flags = receive_spec->flags;
    receive->info.data = receive->data;
    receive->info.data_len = receive_spec->data_len;
    pthread_mutex_lock(&control->mutex);
    if (control->receive_count == control->receive_capacity) {
        pthread_mutex_unlock(&control->mutex);
        free(receive->data);
        free(receive);
        return -EAGAIN;
    }
    if (control->receives_head == NULL) {
        control->receives_head = receive;
    } else {
        control->receives_tail->next = receive;
    }
    control->receives_tail = receive;
    ++control->receive_count;
    ++control->counters.receives_injected;
    signal_wake_locked(control);
    pthread_cond_broadcast(&control->condition);
    pthread_mutex_unlock(&control->mutex);
    return 0;
}

int trevrpc_transport_testing_script_status_v1(trevrpc_transport_testing_control* opaque_control, int32_t status) {
    trevrc_testing_control* control = (trevrc_testing_control*)opaque_control;
    if (control == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&control->mutex);
    if (control->status_count == control->status_capacity) {
        pthread_mutex_unlock(&control->mutex);
        return -EAGAIN;
    }
    control->statuses[control->status_tail] = status;
    control->status_tail = (control->status_tail + 1u) % control->status_capacity;
    ++control->status_count;
    pthread_mutex_unlock(&control->mutex);
    return 0;
}

int trevrpc_transport_testing_clear_status_script_v1(trevrpc_transport_testing_control* opaque_control) {
    trevrc_testing_control* control = (trevrc_testing_control*)opaque_control;
    if (control == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&control->mutex);
    control->status_head = 0;
    control->status_tail = 0;
    control->status_count = 0;
    pthread_mutex_unlock(&control->mutex);
    return 0;
}

int trevrpc_transport_testing_set_checkpoint_v1(
    trevrpc_transport_testing_control* opaque_control, uint32_t checkpoint, uint32_t enabled) {
    trevrc_testing_control* control = (trevrc_testing_control*)opaque_control;
    testing_checkpoint* target;
    if (control == NULL) {
        return -EINVAL;
    }
    target = checkpoint_for(control, checkpoint);
    if (target == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&control->mutex);
    target->enabled = enabled != 0;
    target->entered = 0;
    target->release = enabled == 0;
    pthread_mutex_unlock(&control->mutex);
    return 0;
}

int trevrpc_transport_testing_wait_checkpoint_v1(
    trevrpc_transport_testing_control* opaque_control, uint32_t checkpoint) {
    trevrc_testing_control* control = (trevrc_testing_control*)opaque_control;
    testing_checkpoint* target;
    if (control == NULL) {
        return -EINVAL;
    }
    target = checkpoint_for(control, checkpoint);
    if (target == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&control->mutex);
    while (!target->entered) {
        pthread_cond_wait(&control->condition, &control->mutex);
    }
    pthread_mutex_unlock(&control->mutex);
    return 0;
}

int trevrpc_transport_testing_release_checkpoint_v1(
    trevrpc_transport_testing_control* opaque_control, uint32_t checkpoint) {
    trevrc_testing_control* control = (trevrc_testing_control*)opaque_control;
    testing_checkpoint* target;
    if (control == NULL) {
        return -EINVAL;
    }
    target = checkpoint_for(control, checkpoint);
    if (target == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&control->mutex);
    target->release = 1;
    pthread_cond_broadcast(&control->condition);
    pthread_mutex_unlock(&control->mutex);
    return 0;
}

int trevrpc_transport_testing_push_stopped_v1(trevrpc_transport_testing_control* opaque_control, int32_t status) {
    trevrc_testing_control* control = (trevrc_testing_control*)opaque_control;
    trevrpc_transport_testing_event_v1 event_spec;
    testing_event* event = NULL;
    int result;
    if (control == NULL) {
        return -EINVAL;
    }
    if (trevrpc_transport_testing_event_v1_init(&event_spec, sizeof(event_spec)) != 0) {
        return -EINVAL;
    }
    event_spec.kind = TREVRPC_TRANSPORT_EVENT_STOPPED;
    event_spec.flags = TREVRPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_TRANSPORT_EVENT_FLAG_LOCAL;
    event_spec.status = status;
    event_spec.subject_kind = TREVRPC_TRANSPORT_OBJECT_NONE;
    result = copy_event(&event, &event_spec);
    if (result != 0) {
        return result;
    }
    event->control = control;
    pthread_mutex_lock(&control->mutex);
    result = push_event_locked(control, event);
    if (result == 0) {
        control->state = TREVRPC_TRANSPORT_STATE_STOPPED;
        control->terminal_status = status;
    }
    pthread_mutex_unlock(&control->mutex);
    if (result != 0) {
        free_event(event);
    }
    return result;
}

int trevrpc_transport_testing_get_counters_v1(
    trevrpc_transport_testing_control* opaque_control, trevrpc_transport_testing_counters_v1* counters) {
    trevrc_testing_control* control = (trevrc_testing_control*)opaque_control;
    if (control == NULL || counters == NULL ||
        valid_structure(counters, counters->struct_size, counters->struct_version, sizeof(*counters)) != 0) {
        return -EINVAL;
    }
    pthread_mutex_lock(&control->mutex);
    *counters = control->counters;
    pthread_mutex_unlock(&control->mutex);
    return 0;
}

int trevrpc_transport_testing_get_wake_fd_v1(trevrpc_transport_testing_control* opaque_control, uint32_t index) {
    trevrc_testing_control* control = (trevrc_testing_control*)opaque_control;
    if (control == NULL || index >= TREVRPC_TRANSPORT_TESTING_WAKE_COUNT) {
        return -EINVAL;
    }
    return control->wake_read[index];
}

int trevrpc_transport_testing_set_poll_timeout_ms_v1(
    trevrpc_transport_testing_control* opaque_control, int32_t timeout_ms) {
    trevrc_testing_control* control = (trevrc_testing_control*)opaque_control;
    if (control == NULL || timeout_ms < -1) {
        return -EINVAL;
    }
    pthread_mutex_lock(&control->mutex);
    control->poll_timeout_ms = timeout_ms;
    pthread_mutex_unlock(&control->mutex);
    return 0;
}
