//go:build cgo && (linux || darwin)

#include "provider.h"

#include <errno.h> // IWYU pragma: keep
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

typedef struct test_engine_provider {
    const trevrpc_engine_provider_host_v1* host;
    trevrpc_engine* engine;
} test_engine_provider;

static atomic_uint_fast64_t engine_destroy_count;
static atomic_uint_fast64_t transport_destroy_count;
static atomic_uint_fast64_t transport_destroy_before_drain_count;

static int test_engine_attach(void* context, trevrpc_engine* engine) {
    test_engine_provider* provider = context;
    provider->engine = engine;
    return 0;
}

static int test_engine_close(void* context) {
    test_engine_provider* provider = context;
    provider->host->stopped(provider->engine, 0, 0);
    return 0;
}

static void test_engine_destroy(void* context) {
    free(context);
    atomic_fetch_add_explicit(&engine_destroy_count, 1, memory_order_relaxed);
}

static const trevrpc_engine_provider_ops_v1 test_engine_ops = {
    .struct_size = sizeof(trevrpc_engine_provider_ops_v1),
    .struct_version = TREVRPC_ENGINE_PROVIDER_STRUCT_VERSION_1,
    .attach = test_engine_attach,
    .close = test_engine_close,
    .destroy = test_engine_destroy,
};

int trevrpc_go_test_engine_provider_create(const trevrpc_engine_provider_host_v1* host,
    uint64_t owner_cookie,
    const trevrpc_engine_provider_ops_v1** out_ops,
    void** out_context) {
    test_engine_provider* provider;
    if (host == NULL || host->struct_size < sizeof(*host) ||
        host->struct_version != TREVRPC_ENGINE_PROVIDER_STRUCT_VERSION_1 || owner_cookie == 0 || out_ops == NULL ||
        out_context == NULL) {
        return -EINVAL;
    }
    provider = calloc(1, sizeof(*provider));
    if (provider == NULL) {
        return -ENOMEM;
    }
    provider->host = host;
    *out_ops = &test_engine_ops;
    *out_context = provider;
    return 0;
}

void trevrpc_go_test_engine_provider_dispose(void* context) {
    test_engine_destroy(context);
}

uint64_t trevrpc_go_test_engine_destroy_count(void) {
    return atomic_load_explicit(&engine_destroy_count, memory_order_relaxed);
}

typedef struct test_transport {
    trevrpc_transport_provider_ops_v1 operations;
    uint64_t calls;
    int drained;
} test_transport;

static int test_transport_get_wake_sources(
    void* context, trevrpc_transport_provider_wake_v1* wakes, size_t capacity, size_t* out_count) {
    test_transport* transport = context;
    if (wakes == NULL || capacity == 0 || out_count == NULL) {
        return -EINVAL;
    }
    ++transport->calls;
    wakes[0].kind = TREVRPC_TRANSPORT_WAKE_SOURCE_POSIX_FD;
    wakes[0].flags = TREVRPC_TRANSPORT_WAKE_FLAG_BORROWED;
    wakes[0].native_handle = 7;
    *out_count = 1;
    return 0;
}

static int test_transport_poll_timeout(void* context) {
    ++((test_transport*)context)->calls;
    return -1;
}

static int test_transport_next_event(void* context, trevrpc_transport_event_v1** out_event) {
    ++((test_transport*)context)->calls;
    if (out_event == NULL) {
        return -EINVAL;
    }
    *out_event = NULL;
    return -EAGAIN;
}

static int test_transport_event_get_info(
    void* context, const trevrpc_transport_event_v1* event, trevrpc_transport_provider_event_info_v1* info) {
    (void)context;
    (void)event;
    (void)info;
    return -ENOTSUP;
}

static int test_transport_event_get_admission_info(
    void* context, const trevrpc_transport_event_v1* event, trevrpc_transport_provider_admission_info_v1* info) {
    (void)context;
    (void)event;
    (void)info;
    return -ENOTSUP;
}

static int test_transport_event_get_protocol_info(
    void* context, const trevrpc_transport_event_v1* event, trevrpc_transport_provider_event_protocol_info_v1* info) {
    (void)context;
    (void)event;
    (void)info;
    return -ENOTSUP;
}

static int test_transport_admission_respond(void* context, const trevrpc_transport_event_v1* event, uint16_t status) {
    (void)context;
    (void)event;
    (void)status;
    return -ENOTSUP;
}

static void test_transport_event_release(void* context, trevrpc_transport_event_v1* event) {
    (void)context;
    (void)event;
}

static int test_transport_receive_get_info(
    void* context, const trevrpc_transport_receive_v1* receive, trevrpc_transport_provider_receive_info_v1* info) {
    (void)context;
    (void)receive;
    (void)info;
    return -ENOTSUP;
}

static void test_transport_receive_release(void* context, trevrpc_transport_receive_v1* receive) {
    (void)context;
    (void)receive;
}

static int test_transport_get_diagnostics(void* context, trevrpc_transport_provider_diagnostics_v1* diagnostics) {
    test_transport* transport = context;
    if (diagnostics == NULL) {
        return -EINVAL;
    }
    ++transport->calls;
    memset(diagnostics, 0, sizeof(*diagnostics));
    diagnostics->state = TREVRPC_TRANSPORT_STATE_RUNNING;
    diagnostics->events_enqueued = transport->calls;
    return 0;
}

static int test_transport_listen(
    void* context, const trevrpc_transport_provider_endpoint_config_v1* config, trevrpc_transport_handle_v1* listener) {
    ++((test_transport*)context)->calls;
    if (config == NULL || listener == NULL) {
        return -EINVAL;
    }
    *listener = (trevrpc_transport_handle_v1){UINT64_C(0x7472616e73706f72), 11, 12};
    return 0;
}

static int test_transport_listener_get_port(void* context, trevrpc_transport_handle_v1 listener, uint16_t* port) {
    ++((test_transport*)context)->calls;
    if (listener.slot != 11 || port == NULL) {
        return -EINVAL;
    }
    *port = 4242;
    return 0;
}

static int test_transport_dial(void* context,
    const trevrpc_transport_provider_endpoint_config_v1* config,
    uint64_t operation_id,
    trevrpc_transport_handle_v1* connection) {
    ++((test_transport*)context)->calls;
    if (config == NULL || operation_id == 0 || connection == NULL) {
        return -EINVAL;
    }
    *connection = (trevrpc_transport_handle_v1){UINT64_C(0x7472616e73706f72), 21, 22};
    return 0;
}

static int test_transport_dial_cancel(void* context, trevrpc_transport_handle_v1 connection) {
    ++((test_transport*)context)->calls;
    return connection.slot == 21 ? 0 : -EINVAL;
}

static int test_transport_open_stream(
    void* context, trevrpc_transport_handle_v1 connection, uint64_t operation_id, trevrpc_transport_handle_v1* stream) {
    ++((test_transport*)context)->calls;
    if (connection.slot != 21 || operation_id == 0 || stream == NULL) {
        return -EINVAL;
    }
    *stream = (trevrpc_transport_handle_v1){UINT64_C(0x7472616e73706f72), 31, 32};
    return 0;
}

static int test_transport_send(
    void* context, trevrpc_transport_handle_v1 stream, uint64_t operation_id, const uint8_t* body, size_t body_len) {
    ++((test_transport*)context)->calls;
    return stream.slot == 31 && operation_id != 0 && (body != NULL || body_len == 0) ? 0 : -EINVAL;
}

static int test_transport_receive(
    void* context, trevrpc_transport_handle_v1 stream, trevrpc_transport_receive_v1** receive) {
    ++((test_transport*)context)->calls;
    if (stream.slot != 31 || receive == NULL) {
        return -EINVAL;
    }
    *receive = NULL;
    return -EAGAIN;
}

static int test_transport_stream_operation(void* context, trevrpc_transport_handle_v1 stream) {
    ++((test_transport*)context)->calls;
    return stream.slot == 31 ? 0 : -EINVAL;
}

static int test_transport_stream_error_operation(
    void* context, trevrpc_transport_handle_v1 stream, uint64_t error_code) {
    (void)error_code;
    return test_transport_stream_operation(context, stream);
}

static int test_transport_connection_close(void* context, trevrpc_transport_handle_v1 connection, uint64_t error_code) {
    (void)error_code;
    ++((test_transport*)context)->calls;
    return connection.slot == 21 ? 0 : -EINVAL;
}

static int test_transport_listener_close(void* context, trevrpc_transport_handle_v1 listener) {
    ++((test_transport*)context)->calls;
    return listener.slot == 11 ? 0 : -EINVAL;
}

static int test_transport_release_handle(void* context, trevrpc_transport_handle_v1 handle, uint32_t object_kind) {
    ++((test_transport*)context)->calls;
    return handle.owner != 0 && object_kind >= TREVRPC_TRANSPORT_OBJECT_LISTENER &&
                   object_kind <= TREVRPC_TRANSPORT_OBJECT_STREAM
               ? 0
               : -EINVAL;
}

static int test_transport_close(void* context) {
    ++((test_transport*)context)->calls;
    return 0;
}

static int test_transport_drain(void* context) {
    test_transport* transport = context;
    ++transport->calls;
    transport->drained = 1;
    return 0;
}

static int test_transport_prepare_release(void* context) {
    ++((test_transport*)context)->calls;
    return 0;
}

static void test_transport_destroy(void* context) {
    test_transport* transport = context;
    if (transport->calls != 0 && !transport->drained) {
        atomic_fetch_add_explicit(&transport_destroy_before_drain_count, 1, memory_order_relaxed);
    }
    free(transport);
    atomic_fetch_add_explicit(&transport_destroy_count, 1, memory_order_relaxed);
}

static const trevrpc_transport_provider_ops_v1 test_transport_ops = {
    .struct_size = sizeof(trevrpc_transport_provider_ops_v1),
    .struct_version = TREVRPC_TRANSPORT_PROVIDER_STRUCT_VERSION_1,
    .get_wake_sources = test_transport_get_wake_sources,
    .poll_timeout_ms = test_transport_poll_timeout,
    .next_event = test_transport_next_event,
    .event_get_info = test_transport_event_get_info,
    .event_get_admission_info = test_transport_event_get_admission_info,
    .event_get_protocol_info = test_transport_event_get_protocol_info,
    .admission_respond = test_transport_admission_respond,
    .event_release = test_transport_event_release,
    .receive_get_info = test_transport_receive_get_info,
    .receive_release = test_transport_receive_release,
    .get_diagnostics = test_transport_get_diagnostics,
    .listen = test_transport_listen,
    .listener_get_port = test_transport_listener_get_port,
    .dial = test_transport_dial,
    .dial_cancel = test_transport_dial_cancel,
    .connection_open_bidi_stream = test_transport_open_stream,
    .stream_send = test_transport_send,
    .stream_receive = test_transport_receive,
    .stream_finish_send = test_transport_stream_operation,
    .stream_abort_receive = test_transport_stream_error_operation,
    .stream_abort_send = test_transport_stream_error_operation,
    .stream_abort = test_transport_stream_error_operation,
    .stream_close = test_transport_stream_operation,
    .connection_close = test_transport_connection_close,
    .listener_close = test_transport_listener_close,
    .release_handle = test_transport_release_handle,
    .close = test_transport_close,
    .drain = test_transport_drain,
    .prepare_release = test_transport_prepare_release,
    .destroy = test_transport_destroy,
};

int trevrpc_go_test_transport_provider_create(const trevrpc_transport_provider_ops_v1** out_ops, void** out_context) {
    test_transport* transport;
    if (out_ops == NULL || out_context == NULL) {
        return -EINVAL;
    }
    transport = calloc(1, sizeof(*transport));
    if (transport == NULL) {
        return -ENOMEM;
    }
    transport->operations = test_transport_ops;
    *out_ops = &transport->operations;
    *out_context = transport;
    return 0;
}

void trevrpc_go_test_transport_provider_dispose(void* context) {
    test_transport_destroy(context);
}

void trevrpc_go_test_transport_ops_set_size(const trevrpc_transport_provider_ops_v1* operations, uint32_t size) {
    ((trevrpc_transport_provider_ops_v1*)operations)->struct_size = size;
}

void trevrpc_go_test_transport_ops_set_version(const trevrpc_transport_provider_ops_v1* operations, uint32_t version) {
    ((trevrpc_transport_provider_ops_v1*)operations)->struct_version = version;
}

void trevrpc_go_test_transport_ops_set_reserved(const trevrpc_transport_provider_ops_v1* operations, uint64_t value) {
    ((trevrpc_transport_provider_ops_v1*)operations)->reserved[0] = value;
}

uint64_t trevrpc_go_test_transport_destroy_count(void) {
    return atomic_load_explicit(&transport_destroy_count, memory_order_relaxed);
}

uint64_t trevrpc_go_test_transport_destroy_before_drain_count(void) {
    return atomic_load_explicit(&transport_destroy_before_drain_count, memory_order_relaxed);
}
