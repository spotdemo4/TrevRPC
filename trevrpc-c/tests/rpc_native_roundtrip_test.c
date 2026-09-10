#include "trevrpc_rpc_msquic.h"

#include <assert.h>
#include <errno.h> // IWYU pragma: keep
#include <inttypes.h>
#include <poll.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef TREVRPC_MSQUIC_TEST_CERT
#error "TREVRPC_MSQUIC_TEST_CERT must be defined"
#endif
#ifndef TREVRPC_MSQUIC_TEST_KEY
#error "TREVRPC_MSQUIC_TEST_KEY must be defined"
#endif
#ifndef TREVRPC_MSQUIC_TEST_CA_CERT
#error "TREVRPC_MSQUIC_TEST_CA_CERT must be defined"
#endif

typedef struct observed_state {
    trevrpc_rpc_endpoint_v1 listener;
    trevrpc_rpc_endpoint_v1 client_endpoint;
    trevrpc_rpc_call_v1 client_call;
    trevrpc_rpc_call_v1 server_call;
    trevrpc_rpc_stream_v1 server_stream;
    trevrpc_rpc_stream_v1 client_stream;
    trevrpc_rpc_cancellation_v1 cancellation;
    trevrpc_rpc_event* held_incoming;
    const char* expected_method;
    uint64_t call_open_operation_id;
    uint64_t accept_operation_id;
    uint64_t call_close_operation_id;
    bool listener_ready;
    bool client_ready;
    bool call_ready;
    bool incoming;
    bool accepted;
    bool response_sent;
    bool response_readable;
    bool client_stream_closed;
    bool client_call_closed;
    bool server_stream_closed;
    bool server_call_closed;
    bool listener_closed;
    bool client_endpoint_closed;
    bool cancellation_completed;
    bool operation_16_send_completed;
    unsigned operation_16_send_completions;
    bool server_message_readable;
    bool hold_incoming;
    bool expect_deadline;
    bool stopped;
} observed_state;

static void assert_operation_id(const trevrpc_rpc_event_info_v1* info, uint64_t expected) {
    if (info->operation_id != expected) {
        fprintf(stderr,
            "operation ID mismatch: expected=%" PRIu64 " actual=%" PRIu64 " kind=%" PRIu32 " subject_kind=%" PRIu32
            " status=%" PRId32 " flags=0x%08" PRIx32 " sequence=%" PRIu64 " endpoint={owner=%" PRIu64 ",slot=%" PRIu32
            ",generation=%" PRIu32 "}"
            " call={owner=%" PRIu64 ",slot=%" PRIu32 ",generation=%" PRIu32 "}"
            " stream={owner=%" PRIu64 ",slot=%" PRIu32 ",generation=%" PRIu32 "}"
            " application_error=%" PRIu64 " provider_error=%" PRIu64 "\n",
            expected,
            info->operation_id,
            info->kind,
            info->subject_kind,
            info->status,
            info->flags,
            info->sequence,
            info->endpoint.owner,
            info->endpoint.slot,
            info->endpoint.generation,
            info->call.owner,
            info->call.slot,
            info->call.generation,
            info->stream.owner,
            info->stream.slot,
            info->stream.generation,
            info->application_error_code,
            info->provider_error_code);
    }
    assert(info->operation_id == expected);
}

static void drain_events(trevrpc_rpc_runtime* runtime, observed_state* observed) {
    for (;;) {
        trevrpc_rpc_event_info_v1 info;
        trevrpc_rpc_event* event = NULL;
        bool release_event = true;
        int result = trevrpc_rpc_runtime_next_event(runtime, &event);
        if (result == -EAGAIN) {
            return;
        }
        assert(result == 0);
        assert(trevrpc_rpc_event_info_v1_init(&info, sizeof(info)) == 0);
        assert(trevrpc_rpc_event_get_info_v1(event, &info) == 0);
        if (info.kind == TREVRPC_RPC_EVENT_ENDPOINT_READY && info.operation_id == 1) {
            assert(info.endpoint.owner == observed->listener.owner);
            observed->listener_ready = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_ENDPOINT_READY && info.operation_id == 2) {
            observed->client_endpoint = info.endpoint;
            observed->client_ready = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_CALL_READY && info.operation_id == observed->call_open_operation_id) {
            observed->call_ready = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_CALL_INCOMING) {
            trevrpc_rpc_receive* receive = NULL;
            trevrpc_rpc_receive_info_v1 receive_info;
            assert(info.service_len == strlen("test.Greeter"));
            assert(memcmp(info.service, "test.Greeter", info.service_len) == 0);
            assert(info.method_len == strlen(observed->expected_method));
            assert(memcmp(info.method, observed->expected_method, info.method_len) == 0);
            if (observed->hold_incoming) {
                assert(observed->held_incoming == NULL);
                observed->held_incoming = event;
                release_event = false;
            } else {
                assert(trevrpc_rpc_event_take_incoming_call(
                           event, &observed->server_call, &observed->server_stream, &receive) == 0);
                assert(trevrpc_rpc_receive_info_v1_init(&receive_info, sizeof(receive_info)) == 0);
                assert(trevrpc_rpc_receive_get_info_v1(receive, &receive_info) == 0);
                assert(receive_info.kind == TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE);
                assert(receive_info.data_len == strlen("hello"));
                assert(memcmp(receive_info.data, "hello", receive_info.data_len) == 0);
                assert(receive_info.metadata_count == 1);
                assert(receive_info.metadata[0].key_len == strlen("request-key"));
                assert(memcmp(receive_info.metadata[0].key, "request-key", receive_info.metadata[0].key_len) == 0);
                assert(receive_info.metadata[0].value_len == strlen("request-value"));
                assert(
                    memcmp(receive_info.metadata[0].value, "request-value", receive_info.metadata[0].value_len) == 0);
                trevrpc_rpc_receive_release(receive);
            }
            observed->incoming = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_CALL_ACCEPTED && info.operation_id == observed->accept_operation_id) {
            observed->accepted = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_SEND_COMPLETE && info.operation_id == 5) {
            observed->response_sent = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_SEND_COMPLETE && info.operation_id == 16) {
            assert(info.status == 0);
            assert(info.stream.owner == observed->client_stream.owner);
            assert(info.stream.slot == observed->client_stream.slot);
            assert(info.stream.generation == observed->client_stream.generation);
            observed->operation_16_send_completed = true;
            ++observed->operation_16_send_completions;
        } else if (info.kind == TREVRPC_RPC_EVENT_STREAM_READABLE &&
                   info.stream.owner == observed->client_stream.owner &&
                   info.stream.slot == observed->client_stream.slot &&
                   info.stream.generation == observed->client_stream.generation) {
            observed->response_readable = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_STREAM_READABLE &&
                   info.stream.owner == observed->server_stream.owner &&
                   info.stream.slot == observed->server_stream.slot &&
                   info.stream.generation == observed->server_stream.generation) {
            observed->server_message_readable = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED && info.stream.owner == observed->client_stream.owner &&
                   info.stream.slot == observed->client_stream.slot &&
                   info.stream.generation == observed->client_stream.generation) {
            assert_operation_id(&info, 0);
            if (observed->expect_deadline) {
                assert(info.status == -ETIMEDOUT);
                assert(info.rpc_status == TREVRPC_RPC_STATUS_DEADLINE_EXCEEDED);
                assert(info.application_error_code == TREVRPC_RPC_STATUS_DEADLINE_EXCEEDED);
            }
            observed->client_stream_closed = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED && info.call.owner == observed->client_call.owner &&
                   info.call.slot == observed->client_call.slot &&
                   info.call.generation == observed->client_call.generation) {
            assert_operation_id(&info, observed->call_close_operation_id);
            if (observed->expect_deadline) {
                assert(info.status == -ETIMEDOUT);
                assert(info.rpc_status == TREVRPC_RPC_STATUS_DEADLINE_EXCEEDED);
                assert(info.application_error_code == TREVRPC_RPC_STATUS_DEADLINE_EXCEEDED);
            }
            observed->client_call_closed = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED && info.stream.owner == observed->server_stream.owner &&
                   info.stream.slot == observed->server_stream.slot &&
                   info.stream.generation == observed->server_stream.generation) {
            assert_operation_id(&info, 0);
            observed->server_stream_closed = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED && info.call.owner == observed->server_call.owner &&
                   info.call.slot == observed->server_call.slot &&
                   info.call.generation == observed->server_call.generation) {
            assert_operation_id(&info, 0);
            observed->server_call_closed = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED &&
                   info.endpoint.owner == observed->client_endpoint.owner &&
                   info.endpoint.slot == observed->client_endpoint.slot &&
                   info.endpoint.generation == observed->client_endpoint.generation) {
            assert_operation_id(&info, 7);
            observed->client_endpoint_closed = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED && info.endpoint.owner == observed->listener.owner &&
                   info.endpoint.slot == observed->listener.slot &&
                   info.endpoint.generation == observed->listener.generation) {
            assert_operation_id(&info, 8);
            observed->listener_closed = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_CANCELLED && info.operation_id == 14) {
            assert(info.cancellation.owner == observed->cancellation.owner);
            assert(info.cancellation.slot == observed->cancellation.slot);
            assert(info.cancellation.generation == observed->cancellation.generation);
            observed->cancellation_completed = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_STOPPED) {
            assert_operation_id(&info, 10);
            observed->stopped = true;
        }
        if (release_event) {
            trevrpc_rpc_event_release(event);
        }
    }
}

static void finish_readable(trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream) {
    trevrpc_rpc_receive* receive = NULL;
    assert(trevrpc_rpc_stream_receive(runtime, stream, &receive) == -EAGAIN);
    assert(receive == NULL);
}

static void pump_until(trevrpc_rpc_runtime* runtime,
    const trevrpc_rpc_wake_source_v1* wake,
    observed_state* observed,
    const bool* condition) {
    int attempt;
    for (attempt = 0; attempt < 100 && !*condition; ++attempt) {
        struct pollfd descriptor = {.fd = (int)wake->native_handle, .events = POLLIN, .revents = 0};
        int result = poll(&descriptor, 1, 100);
        assert(result >= 0);
        drain_events(runtime, observed);
    }
    assert(*condition);
}

static uint8_t* read_file(const char* path, size_t* out_len) {
    FILE* file = fopen(path, "rb");
    long length;
    uint8_t* data;
    if (file == NULL)
        return NULL;
    assert(fseek(file, 0, SEEK_END) == 0);
    length = ftell(file);
    assert(length > 0);
    assert(fseek(file, 0, SEEK_SET) == 0);
    data = malloc((size_t)length);
    assert(data != NULL);
    assert(fread(data, 1, (size_t)length, file) == (size_t)length);
    assert(fclose(file) == 0);
    *out_len = (size_t)length;
    return data;
}

int main(void) {
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_msquic_config_v1 provider_config;
    trevrpc_rpc_msquic_endpoint_config_v1 listener_config;
    trevrpc_rpc_msquic_endpoint_config_v1 client_config;
    trevrpc_rpc_call_config_v1 call_config;
    trevrpc_rpc_status_v1 status;
    trevrpc_rpc_metadata_entry_v1 request_metadata = {0};
    trevrpc_rpc_metadata_entry_v1 response_metadata = {0};
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_runtime* runtime = NULL;
    observed_state observed = {0};
    uint16_t port = 0;
    uint8_t* server_cert;
    uint8_t* server_key;
    uint8_t* client_cert;
    uint8_t* client_key;
    uint8_t* client_ca;
    size_t server_cert_len;
    size_t server_key_len;
    size_t client_cert_len;
    size_t client_key_len;
    size_t client_ca_len;

    server_cert = read_file(TREVRPC_MSQUIC_TEST_CERT, &server_cert_len);
    server_key = read_file(TREVRPC_MSQUIC_TEST_KEY, &server_key_len);
    client_cert = read_file(TREVRPC_MSQUIC_TEST_CERT, &client_cert_len);
    client_key = read_file(TREVRPC_MSQUIC_TEST_KEY, &client_key_len);
    client_ca = read_file(TREVRPC_MSQUIC_TEST_CA_CERT, &client_ca_len);
    assert(server_cert != NULL && server_key != NULL && client_cert != NULL && client_key != NULL && client_ca != NULL);
    assert(trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config)) == 0);
    assert(trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config)) == 0);
    assert(trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &runtime) == 0);
    assert(trevrpc_rpc_wake_source_v1_init(&wake, sizeof(wake)) == 0);
    assert(trevrpc_rpc_runtime_get_wake_source_v1(runtime, &wake) == 0);

    assert(trevrpc_rpc_msquic_endpoint_config_v1_init(&listener_config, sizeof(listener_config)) == 0);
    listener_config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_LISTENER;
    listener_config.host = "127.0.0.1";
    listener_config.host_len = (uint32_t)strlen(listener_config.host);
    listener_config.cert_data_len = 1;
    assert(trevrpc_rpc_msquic_endpoint_start_v1(runtime, &listener_config, 99, &observed.listener) == -EINVAL);
    listener_config.cert_data = server_cert;
    listener_config.cert_data_len = server_cert_len;
    listener_config.key_data_len = 1;
    assert(trevrpc_rpc_msquic_endpoint_start_v1(runtime, &listener_config, 99, &observed.listener) == -EINVAL);
    listener_config.key_data = server_key;
    listener_config.key_data_len = server_key_len;
    listener_config.cert_data = NULL;
    listener_config.cert_data_len = 0;
    listener_config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    listener_config.cert_file_len = 0;
    assert(trevrpc_rpc_msquic_endpoint_start_v1(runtime, &listener_config, 99, &observed.listener) == -EINVAL);
    listener_config.cert_data = server_cert;
    listener_config.cert_data_len = server_cert_len;
    listener_config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    listener_config.cert_file_len = (uint32_t)strlen(listener_config.cert_file);
    assert(trevrpc_rpc_msquic_endpoint_start_v1(runtime, &listener_config, 99, &observed.listener) == -EINVAL);
    listener_config.cert_file = NULL;
    listener_config.cert_file_len = 0;
    listener_config.ca_cert_data = server_cert;
    listener_config.ca_cert_data_len = server_cert_len;
    assert(trevrpc_rpc_msquic_endpoint_start_v1(runtime, &listener_config, 99, &observed.listener) == -ENOTSUP);

    assert(trevrpc_rpc_msquic_endpoint_config_v1_init(&listener_config, sizeof(listener_config)) == 0);
    listener_config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_LISTENER;
    listener_config.host = "127.0.0.1";
    listener_config.host_len = (uint32_t)strlen(listener_config.host);
    listener_config.server_name = "127.0.0.1";
    listener_config.server_name_len = (uint32_t)strlen(listener_config.server_name);
    assert(trevrpc_rpc_msquic_endpoint_start_v1(runtime, &listener_config, 99, &observed.listener) == -ENOTSUP);

    assert(trevrpc_rpc_msquic_endpoint_config_v1_init(&listener_config, sizeof(listener_config)) == 0);
    listener_config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_LISTENER;
    listener_config.host = "127.0.0.1";
    listener_config.host_len = (uint32_t)strlen(listener_config.host);
    listener_config.cert_data = server_cert;
    listener_config.cert_data_len = server_cert_len;
    listener_config.key_data = server_key;
    listener_config.key_data_len = server_key_len;
    assert(trevrpc_rpc_msquic_endpoint_start_v1(runtime, &listener_config, 1, &observed.listener) == 0);
    memset(server_cert, 0, server_cert_len);
    memset(server_key, 0, server_key_len);
    free(server_cert);
    free(server_key);
    server_cert = NULL;
    server_key = NULL;
    assert(trevrpc_rpc_endpoint_get_port_v1(runtime, observed.listener, &port) == 0);
    assert(port != 0);
    drain_events(runtime, &observed);
    assert(observed.listener_ready);

    assert(trevrpc_rpc_msquic_endpoint_config_v1_init(&client_config, sizeof(client_config)) == 0);
    client_config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_CLIENT;
    client_config.host = "127.0.0.1";
    client_config.host_len = (uint32_t)strlen(client_config.host);
    client_config.server_name = "127.0.0.1";
    client_config.server_name_len = (uint32_t)strlen(client_config.server_name);
    client_config.port = port;
    client_config.cert_data = client_cert;
    client_config.cert_data_len = client_cert_len;
    client_config.key_data = client_key;
    client_config.key_data_len = client_key_len;
    client_config.ca_cert_data = client_ca;
    client_config.ca_cert_data_len = client_ca_len;
    client_config.server_name = "localhost";
    client_config.server_name_len = (uint32_t)strlen(client_config.server_name);
    assert(trevrpc_rpc_msquic_endpoint_start_v1(runtime, &client_config, 99, &observed.client_endpoint) == -ENOTSUP);
    client_config.server_name = "127.0.0.1";
    client_config.server_name_len = (uint32_t)strlen(client_config.server_name);
    assert(trevrpc_rpc_msquic_endpoint_start_v1(runtime, &client_config, 2, &observed.client_endpoint) == 0);
    memset(client_cert, 0, client_cert_len);
    memset(client_key, 0, client_key_len);
    memset(client_ca, 0, client_ca_len);
    free(client_cert);
    free(client_key);
    free(client_ca);
    client_cert = NULL;
    client_key = NULL;
    client_ca = NULL;
    pump_until(runtime, &wake, &observed, &observed.client_ready);

    assert(trevrpc_rpc_call_config_v1_init(&call_config, sizeof(call_config)) == 0);
    observed.expected_method = "SayHello";
    observed.call_open_operation_id = 3;
    observed.accept_operation_id = 4;
    observed.call_close_operation_id = 6;
    call_config.kind = TREVRPC_RPC_KIND_CLIENT_STREAMING;
    call_config.service = "test.Greeter";
    call_config.service_len = (uint32_t)strlen(call_config.service);
    call_config.method = "SayHello";
    call_config.method_len = (uint32_t)strlen(call_config.method);
    request_metadata.key = "request-key";
    request_metadata.key_len = (uint32_t)strlen(request_metadata.key);
    request_metadata.value = (const uint8_t*)"request-value";
    request_metadata.value_len = strlen("request-value");
    call_config.metadata = &request_metadata;
    call_config.metadata_count = 1;
    call_config.initial_message = (const uint8_t*)"hello";
    call_config.initial_message_len = strlen("hello");
    assert(
        trevrpc_rpc_call_open_v1(
            runtime, observed.client_endpoint, &call_config, 3, &observed.client_call, &observed.client_stream) == 0);
    pump_until(runtime, &wake, &observed, &observed.call_ready);
    pump_until(runtime, &wake, &observed, &observed.incoming);

    assert(trevrpc_rpc_call_accept(runtime, observed.server_call, 4) == 0);
    pump_until(runtime, &wake, &observed, &observed.accepted);
    assert(observed.server_message_readable);
    finish_readable(runtime, observed.server_stream);
    observed.server_message_readable = false;
    assert(trevrpc_rpc_status_v1_init(&status, sizeof(status)) == 0);
    response_metadata.key = "response-key";
    response_metadata.key_len = (uint32_t)strlen(response_metadata.key);
    response_metadata.value = (const uint8_t*)"response-value";
    response_metadata.value_len = strlen("response-value");
    status.metadata = &response_metadata;
    status.metadata_count = 1;
    assert(trevrpc_rpc_call_respond_copy_v1(
               runtime, observed.server_call, 5, &status, (const uint8_t*)"world", strlen("world")) == 0);
    pump_until(runtime, &wake, &observed, &observed.response_sent);
    pump_until(runtime, &wake, &observed, &observed.response_readable);
    {
        trevrpc_rpc_receive* receive = NULL;
        trevrpc_rpc_receive_info_v1 info;
        assert(trevrpc_rpc_stream_receive(runtime, observed.client_stream, &receive) == 0);
        assert(trevrpc_rpc_receive_info_v1_init(&info, sizeof(info)) == 0);
        assert(trevrpc_rpc_receive_get_info_v1(receive, &info) == 0);
        assert(info.kind == TREVRPC_RPC_RECEIVE_MESSAGE);
        assert(info.data_len == strlen("world"));
        assert(memcmp(info.data, "world", info.data_len) == 0);
        assert(info.metadata_count == 0);
        trevrpc_rpc_receive_release(receive);

        receive = NULL;
        for (int attempt = 0;; ++attempt) {
            int result = trevrpc_rpc_stream_receive(runtime, observed.client_stream, &receive);
            if (result == 0) {
                break;
            }
            assert(result == -EAGAIN);
            assert(attempt < 100);
            observed.response_readable = false;
            pump_until(runtime, &wake, &observed, &observed.response_readable);
        }
        assert(trevrpc_rpc_receive_info_v1_init(&info, sizeof(info)) == 0);
        assert(trevrpc_rpc_receive_get_info_v1(receive, &info) == 0);
        assert(info.kind == TREVRPC_RPC_RECEIVE_STATUS);
        assert(info.rpc_status == TREVRPC_RPC_STATUS_OK);
        assert(info.metadata_count == 1);
        assert(info.metadata[0].key_len == strlen("response-key"));
        assert(memcmp(info.metadata[0].key, "response-key", info.metadata[0].key_len) == 0);
        assert(info.metadata[0].value_len == strlen("response-value"));
        assert(memcmp(info.metadata[0].value, "response-value", info.metadata[0].value_len) == 0);
        trevrpc_rpc_receive_release(receive);
        finish_readable(runtime, observed.client_stream);
    }

    assert(trevrpc_rpc_call_release(runtime, observed.client_call) == -EBUSY);
    assert(trevrpc_rpc_stream_release(runtime, observed.client_stream) == -EBUSY);
    assert(trevrpc_rpc_call_close(runtime, observed.client_call, 6, TREVRPC_RPC_CLOSE_FLAG_NONE, 0) == 0);
    assert(trevrpc_rpc_stream_close(runtime, observed.client_stream, 9, TREVRPC_RPC_CLOSE_FLAG_NONE, 0) == -EALREADY);
    pump_until(runtime, &wake, &observed, &observed.client_stream_closed);
    pump_until(runtime, &wake, &observed, &observed.client_call_closed);
    pump_until(runtime, &wake, &observed, &observed.server_stream_closed);
    pump_until(runtime, &wake, &observed, &observed.server_call_closed);
    assert(trevrpc_rpc_stream_release(runtime, observed.client_stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, observed.client_call) == 0);
    assert(trevrpc_rpc_stream_release(runtime, observed.server_stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, observed.server_call) == 0);

    observed.call_ready = false;
    observed.incoming = false;
    observed.accepted = false;
    observed.client_stream_closed = false;
    observed.client_call_closed = false;
    observed.server_stream_closed = false;
    observed.server_call_closed = false;
    observed.expected_method = "Cancel";
    observed.call_open_operation_id = 12;
    observed.accept_operation_id = 13;
    observed.call_close_operation_id = 0;
    call_config.kind = TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING;
    call_config.method = observed.expected_method;
    call_config.method_len = (uint32_t)strlen(call_config.method);
    assert(trevrpc_rpc_cancellation_create(runtime, &observed.cancellation) == 0);
    call_config.cancellation = observed.cancellation;
    assert(
        trevrpc_rpc_call_open_v1(
            runtime, observed.client_endpoint, &call_config, 12, &observed.client_call, &observed.client_stream) == 0);
    pump_until(runtime, &wake, &observed, &observed.call_ready);
    pump_until(runtime, &wake, &observed, &observed.incoming);
    assert(trevrpc_rpc_call_accept(runtime, observed.server_call, 13) == 0);
    pump_until(runtime, &wake, &observed, &observed.accepted);
    assert(observed.server_message_readable);
    finish_readable(runtime, observed.server_stream);
    observed.server_message_readable = false;
    assert(trevrpc_rpc_stream_send_copy_v1(runtime,
               observed.client_stream,
               16,
               (const uint8_t*)"first",
               strlen("first"),
               TREVRPC_RPC_SEND_FLAG_NONE) == 0);
    pump_until(runtime, &wake, &observed, &observed.operation_16_send_completed);
    assert(observed.operation_16_send_completions == 1);
    pump_until(runtime, &wake, &observed, &observed.server_message_readable);
    {
        trevrpc_rpc_receive* receive = NULL;
        trevrpc_rpc_receive_info_v1 info;
        assert(trevrpc_rpc_stream_receive(runtime, observed.server_stream, &receive) == 0);
        assert(trevrpc_rpc_receive_info_v1_init(&info, sizeof(info)) == 0);
        assert(trevrpc_rpc_receive_get_info_v1(receive, &info) == 0);
        assert(info.kind == TREVRPC_RPC_RECEIVE_MESSAGE);
        assert(info.data_len == strlen("first"));
        assert(memcmp(info.data, "first", info.data_len) == 0);
        trevrpc_rpc_receive_release(receive);
        finish_readable(runtime, observed.server_stream);
    }
    observed.operation_16_send_completed = false;
    observed.server_message_readable = false;
    assert(trevrpc_rpc_stream_send_copy_v1(runtime,
               observed.client_stream,
               16,
               (const uint8_t*)"second",
               strlen("second"),
               TREVRPC_RPC_SEND_FLAG_NONE) == 0);
    pump_until(runtime, &wake, &observed, &observed.operation_16_send_completed);
    assert(observed.operation_16_send_completions == 2);
    pump_until(runtime, &wake, &observed, &observed.server_message_readable);
    {
        trevrpc_rpc_receive* receive = NULL;
        trevrpc_rpc_receive_info_v1 info;
        assert(trevrpc_rpc_stream_receive(runtime, observed.server_stream, &receive) == 0);
        assert(trevrpc_rpc_receive_info_v1_init(&info, sizeof(info)) == 0);
        assert(trevrpc_rpc_receive_get_info_v1(receive, &info) == 0);
        assert(info.kind == TREVRPC_RPC_RECEIVE_MESSAGE);
        assert(info.data_len == strlen("second"));
        assert(memcmp(info.data, "second", info.data_len) == 0);
        trevrpc_rpc_receive_release(receive);
        finish_readable(runtime, observed.server_stream);
    }
    assert(trevrpc_rpc_cancellation_release(runtime, observed.cancellation) == -EBUSY);
    assert(trevrpc_rpc_cancellation_cancel(runtime, observed.cancellation, 14) == 0);
    assert(trevrpc_rpc_cancellation_cancel(runtime, observed.cancellation, 15) == -EALREADY);
    pump_until(runtime, &wake, &observed, &observed.cancellation_completed);
    pump_until(runtime, &wake, &observed, &observed.client_stream_closed);
    pump_until(runtime, &wake, &observed, &observed.client_call_closed);
    pump_until(runtime, &wake, &observed, &observed.server_stream_closed);
    pump_until(runtime, &wake, &observed, &observed.server_call_closed);
    assert(trevrpc_rpc_stream_release(runtime, observed.client_stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, observed.client_call) == 0);
    assert(trevrpc_rpc_stream_release(runtime, observed.server_stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, observed.server_call) == 0);
    assert(trevrpc_rpc_cancellation_release(runtime, observed.cancellation) == 0);

    observed.call_ready = false;
    observed.incoming = false;
    observed.accepted = false;
    observed.client_stream_closed = false;
    observed.client_call_closed = false;
    observed.server_stream_closed = false;
    observed.server_call_closed = false;
    observed.server_message_readable = false;
    observed.expected_method = "Deadline";
    observed.call_open_operation_id = 20;
    observed.accept_operation_id = 21;
    observed.call_close_operation_id = 0;
    observed.expect_deadline = true;
    call_config.kind = TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING;
    call_config.method = observed.expected_method;
    call_config.method_len = (uint32_t)strlen(call_config.method);
    call_config.cancellation = (trevrpc_rpc_cancellation_v1){0};
    call_config.timeout_nanos = UINT64_C(500000000);
    assert(trevrpc_rpc_call_open_v1(runtime,
               observed.client_endpoint,
               &call_config,
               observed.call_open_operation_id,
               &observed.client_call,
               &observed.client_stream) == 0);
    pump_until(runtime, &wake, &observed, &observed.call_ready);
    pump_until(runtime, &wake, &observed, &observed.incoming);
    assert(trevrpc_rpc_call_accept(runtime, observed.server_call, observed.accept_operation_id) == 0);
    pump_until(runtime, &wake, &observed, &observed.accepted);
    assert(observed.server_message_readable);
    finish_readable(runtime, observed.server_stream);
    observed.server_message_readable = false;
    pump_until(runtime, &wake, &observed, &observed.client_stream_closed);
    pump_until(runtime, &wake, &observed, &observed.client_call_closed);
    pump_until(runtime, &wake, &observed, &observed.server_stream_closed);
    pump_until(runtime, &wake, &observed, &observed.server_call_closed);
    assert(trevrpc_rpc_call_cancel(runtime, observed.client_call, 22, 0) == -EALREADY);
    assert(trevrpc_rpc_stream_release(runtime, observed.client_stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, observed.client_call) == 0);
    assert(trevrpc_rpc_stream_release(runtime, observed.server_stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, observed.server_call) == 0);

    observed.call_ready = false;
    observed.incoming = false;
    observed.client_stream_closed = false;
    observed.client_call_closed = false;
    observed.expected_method = "Reject";
    observed.call_open_operation_id = 17;
    observed.expect_deadline = false;
    call_config.timeout_nanos = TREVRPC_RPC_DEADLINE_INFINITE;
    observed.call_close_operation_id = 0;
    observed.hold_incoming = true;
    call_config.kind = TREVRPC_RPC_KIND_UNARY;
    call_config.method = observed.expected_method;
    call_config.method_len = (uint32_t)strlen(call_config.method);
    call_config.cancellation = (trevrpc_rpc_cancellation_v1){0};
    assert(
        trevrpc_rpc_call_open_v1(
            runtime, observed.client_endpoint, &call_config, 17, &observed.client_call, &observed.client_stream) == 0);
    pump_until(runtime, &wake, &observed, &observed.call_ready);
    pump_until(runtime, &wake, &observed, &observed.incoming);
    assert(observed.held_incoming != NULL);

    assert(trevrpc_rpc_endpoint_release(runtime, observed.client_endpoint) == -EBUSY);
    assert(trevrpc_rpc_endpoint_close(runtime, observed.client_endpoint, 7) == 0);
    assert(trevrpc_rpc_endpoint_close(runtime, observed.listener, 8) == 0);
    pump_until(runtime, &wake, &observed, &observed.client_endpoint_closed);
    pump_until(runtime, &wake, &observed, &observed.listener_closed);
    pump_until(runtime, &wake, &observed, &observed.client_stream_closed);
    pump_until(runtime, &wake, &observed, &observed.client_call_closed);
    assert(trevrpc_rpc_stream_release(runtime, observed.client_stream) == 0);
    assert(trevrpc_rpc_call_release(runtime, observed.client_call) == 0);
    assert(trevrpc_rpc_endpoint_release(runtime, observed.client_endpoint) == 0);
    assert(trevrpc_rpc_endpoint_release(runtime, observed.listener) == 0);

    assert(trevrpc_rpc_runtime_close(runtime, 10) == 0);
    pump_until(runtime, &wake, &observed, &observed.stopped);
    assert(trevrpc_rpc_runtime_drain(runtime) == 0);
    assert(trevrpc_rpc_runtime_release(runtime) == 0);
    {
        trevrpc_rpc_event_info_v1 info;
        assert(trevrpc_rpc_event_info_v1_init(&info, sizeof(info)) == 0);
        assert(trevrpc_rpc_event_get_info_v1(observed.held_incoming, &info) == 0);
        assert(info.kind == TREVRPC_RPC_EVENT_CALL_INCOMING);
        assert(info.method_len == strlen("Reject"));
        assert(memcmp(info.method, "Reject", info.method_len) == 0);
        trevrpc_rpc_event_release(observed.held_incoming);
    }
    return 0;
}
