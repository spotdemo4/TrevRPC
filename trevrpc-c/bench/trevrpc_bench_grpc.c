#define _POSIX_C_SOURCE 200809L

#include "trevrpc_bench_peer.h"

#include "benchmark.pb-c.h"

#include <errno.h>
#include <grpc/byte_buffer.h>
#include <grpc/byte_buffer_reader.h> // IWYU pragma: keep
#include <grpc/credentials.h>
#include <grpc/grpc.h>
#include <grpc/impl/channel_arg_names.h>
#include <grpc/impl/propagation_bits.h>
#include <grpc/slice.h>
#include <grpc/status.h>
#include <grpc/support/time.h>
#include <openssl/crypto.h>
#include <limits.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define GRPC_BENCH_MAX_MESSAGE_BYTES ((int)(BENCHMARK_MAX_PAYLOAD_BYTES + 1024u))
#define GRPC_BENCH_MAX_CREDENTIAL_BYTES (16u * 1024u * 1024u)
#define GRPC_BENCH_ACCEPTS BENCHMARK_SERVER_WORKERS

#define GRPC_UNARY_METHOD "/trevrpc.benchmark.v1.BenchmarkService/Unary"
#define GRPC_CLIENT_STREAM_METHOD "/trevrpc.benchmark.v1.BenchmarkService/ClientStream"
#define GRPC_SERVER_STREAM_METHOD "/trevrpc.benchmark.v1.BenchmarkService/ServerStream"
#define GRPC_BIDI_METHOD "/trevrpc.benchmark.v1.BenchmarkService/Bidi"

typedef Trevrpc__Benchmark__V1__BenchmarkRequest BenchmarkRequest;
typedef Trevrpc__Benchmark__V1__BenchmarkResponse BenchmarkResponse;
typedef Trevrpc__Benchmark__V1__BenchmarkSummary BenchmarkSummary;
typedef Trevrpc__Benchmark__V1__StreamRequest StreamRequest;

typedef struct grpc_server_call grpc_server_call;

struct trevrpc_bench_grpc_client {
    grpc_channel* channel;
};

struct trevrpc_bench_grpc_lane {
    trevrpc_bench_grpc_client* client;
    grpc_completion_queue* cq;
};

typedef struct grpc_call_queue {
    grpc_server_call** entries;
    size_t capacity;
    size_t head;
    size_t count;
    bool closed;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} grpc_call_queue;

struct grpc_server_call {
    grpc_call* call;
    grpc_call_details details;
    grpc_metadata_array metadata;
    grpc_completion_queue* cq;
};

struct trevrpc_bench_grpc_server {
    grpc_server* core;
    grpc_completion_queue* notify_cq;
    grpc_completion_queue* shutdown_cq;
    grpc_call_queue calls;
    pthread_t accept_thread;
    pthread_t workers[BENCHMARK_SERVER_WORKERS];
    size_t worker_count;
    pthread_mutex_t state_mutex;
    bool accepting;
    bool shutdown_started;
    bool shutdown_finished;
    bool accept_thread_started;
    size_t pending_accepts;
    int accept_result;
};

static void server_record_error(trevrpc_bench_grpc_server* server, int error) {
    pthread_mutex_lock(&server->state_mutex);
    if (server->accept_result == 0) {
        server->accept_result = error;
    }
    server->accepting = false;
    pthread_mutex_unlock(&server->state_mutex);
}

static int grpc_wait(grpc_completion_queue* cq, void* tag) {
    grpc_event event = grpc_completion_queue_next(cq, gpr_inf_future(GPR_CLOCK_REALTIME), NULL);
    if (event.type != GRPC_OP_COMPLETE || event.tag != tag || !event.success) {
        return -EIO;
    }
    return 0;
}

static int grpc_start_and_wait(grpc_call* call, grpc_completion_queue* cq, grpc_op* ops, size_t count, void* tag) {
    grpc_call_error error = grpc_call_start_batch(call, ops, count, tag, NULL);
    if (error != GRPC_CALL_OK) {
        return -EINVAL;
    }
    return grpc_wait(cq, tag);
}

static int grpc_bench_init(void) {
    // gRPC's default EventEngine can outlive grpc_shutdown, so it must not race OpenSSL's exit handler.
    if (OPENSSL_init_crypto(OPENSSL_INIT_NO_ATEXIT, NULL) != 1) {
        return -EIO;
    }
    grpc_init();
    return 0;
}

static void grpc_cq_destroy(grpc_completion_queue* cq) {
    if (cq == NULL) {
        return;
    }
    grpc_completion_queue_shutdown(cq);
    while (grpc_completion_queue_next(cq, gpr_inf_future(GPR_CLOCK_REALTIME), NULL).type != GRPC_QUEUE_SHUTDOWN) {
    }
    grpc_completion_queue_destroy(cq);
}

static int read_file(const char* path, char** contents) {
    *contents = NULL;
    FILE* file = fopen(path, "rb");
    if (file == NULL) {
        return -errno;
    }
    int result = 0;
    if (fseek(file, 0, SEEK_END) != 0) {
        result = -errno;
    }
    long length = result == 0 ? ftell(file) : -1;
    if (length < 0 || (unsigned long)length > GRPC_BENCH_MAX_CREDENTIAL_BYTES) {
        result = result == 0 ? -EFBIG : result;
    }
    if (result == 0 && fseek(file, 0, SEEK_SET) != 0) {
        result = -errno;
    }
    char* data = NULL;
    size_t size = result == 0 ? (size_t)length : 0;
    if (result == 0) {
        data = calloc(size + 1, 1);
        if (data == NULL) {
            result = -ENOMEM;
        }
    }
    if (result == 0 && fread(data, 1, size, file) != size) {
        result = ferror(file) ? -EIO : -EINVAL;
    }
    if (fclose(file) != 0 && result == 0) {
        result = -errno;
    }
    if (result != 0) {
        free(data);
        return result;
    }
    *contents = data;
    return 0;
}

static int format_address(const char* host, uint16_t port, char* address, size_t address_len) {
    int length = strchr(host, ':') == NULL ? snprintf(address, address_len, "%s:%u", host, port)
                                           : snprintf(address, address_len, "[%s]:%u", host, port);
    return length < 0 || (size_t)length >= address_len ? -ENAMETOOLONG : 0;
}

static void configure_args(grpc_arg args[7], grpc_channel_args* channel_args) {
    memset(args, 0, 7 * sizeof(*args));
    args[0] = (grpc_arg){
        .type = GRPC_ARG_INTEGER,
        .key = (char*)GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH,
        .value.integer = GRPC_BENCH_MAX_MESSAGE_BYTES,
    };
    args[1] = (grpc_arg){
        .type = GRPC_ARG_INTEGER,
        .key = (char*)GRPC_ARG_MAX_SEND_MESSAGE_LENGTH,
        .value.integer = GRPC_BENCH_MAX_MESSAGE_BYTES,
    };
    args[2] = (grpc_arg){
        .type = GRPC_ARG_INTEGER,
        .key = (char*)GRPC_ARG_ENABLE_PER_MESSAGE_COMPRESSION,
        .value.integer = 0,
    };
    args[3] = (grpc_arg){
        .type = GRPC_ARG_INTEGER,
        .key = (char*)GRPC_ARG_KEEPALIVE_TIME_MS,
        .value.integer = (int)BENCHMARK_KEEP_ALIVE_MS,
    };
    args[4] = (grpc_arg){
        .type = GRPC_ARG_INTEGER,
        .key = (char*)GRPC_ARG_KEEPALIVE_TIMEOUT_MS,
        .value.integer = 20000,
    };
    args[5] = (grpc_arg){
        .type = GRPC_ARG_INTEGER,
        .key = (char*)GRPC_ARG_ENABLE_RETRIES,
        .value.integer = 0,
    };
    args[6] = (grpc_arg){
        .type = GRPC_ARG_INTEGER,
        .key = (char*)GRPC_ARG_MAX_CONCURRENT_STREAMS,
        .value.integer = BENCHMARK_SERVER_STREAMS,
    };
    *channel_args = (grpc_channel_args){.num_args = 7, .args = args};
}

static grpc_byte_buffer* byte_buffer_from_data(const uint8_t* data, size_t length) {
    grpc_slice slice = grpc_slice_from_copied_buffer((const char*)data, length);
    grpc_byte_buffer* buffer = grpc_raw_byte_buffer_create(&slice, 1);
    grpc_slice_unref(slice);
    return buffer;
}

static int unpack_byte_buffer(grpc_byte_buffer* buffer, const ProtobufCMessageDescriptor* descriptor, void** message) {
    *message = NULL;
    if (buffer == NULL || grpc_byte_buffer_length(buffer) > (size_t)GRPC_BENCH_MAX_MESSAGE_BYTES) {
        return -EINVAL;
    }
    grpc_byte_buffer_reader reader;
    if (!grpc_byte_buffer_reader_init(&reader, buffer)) {
        return -EIO;
    }
    grpc_slice slice = grpc_byte_buffer_reader_readall(&reader);
    *message = protobuf_c_message_unpack(descriptor, NULL, GRPC_SLICE_LENGTH(slice), GRPC_SLICE_START_PTR(slice));
    grpc_slice_unref(slice);
    grpc_byte_buffer_reader_destroy(&reader);
    return *message == NULL ? -EINVAL : 0;
}

static grpc_byte_buffer* pack_message(const ProtobufCMessage* message) {
    size_t length = protobuf_c_message_get_packed_size(message);
    if (length > (size_t)GRPC_BENCH_MAX_MESSAGE_BYTES) {
        return NULL;
    }
    uint8_t* data = length == 0 ? NULL : malloc(length);
    if (length > 0 && data == NULL) {
        return NULL;
    }
    protobuf_c_message_pack(message, data);
    grpc_byte_buffer* buffer = byte_buffer_from_data(data, length);
    free(data);
    return buffer;
}

static uint8_t* new_payload(size_t length) {
    return length == 0 ? NULL : calloc(length, 1);
}

static bool payload_is_zero(const ProtobufCBinaryData* payload) {
    if (payload->len > 0 && payload->data == NULL) {
        return false;
    }
    for (size_t i = 0; i < payload->len; i++) {
        if (payload->data[i] != 0) {
            return false;
        }
    }
    return true;
}

static BenchmarkResponse* new_response(uint64_t sequence, uint32_t payload_len) {
    BenchmarkResponse* response = malloc(sizeof(*response));
    if (response == NULL) {
        return NULL;
    }
    trevrpc__benchmark__v1__benchmark_response__init(response);
    response->sequence = sequence;
    response->payload.data = new_payload(payload_len);
    if (payload_len > 0 && response->payload.data == NULL) {
        free(response);
        return NULL;
    }
    response->payload.len = payload_len;
    return response;
}

static BenchmarkSummary* new_summary(uint64_t message_count, uint64_t payload_bytes) {
    BenchmarkSummary* summary = malloc(sizeof(*summary));
    if (summary == NULL) {
        return NULL;
    }
    trevrpc__benchmark__v1__benchmark_summary__init(summary);
    summary->message_count = message_count;
    summary->payload_bytes = payload_bytes;
    return summary;
}

static int validate_response(const BenchmarkResponse* response, uint64_t sequence, uint32_t payload_len) {
    return response != NULL && response->sequence == sequence && response->payload.len == payload_len &&
                   payload_is_zero(&response->payload)
               ? 0
               : -EINVAL;
}

static grpc_call* client_new_call(trevrpc_bench_grpc_lane* lane, const char* method) {
    gpr_timespec deadline =
        gpr_time_add(gpr_now(GPR_CLOCK_REALTIME), gpr_time_from_millis(BENCHMARK_IDLE_TIMEOUT_MS, GPR_TIMESPAN));
    return grpc_channel_create_call(lane->client->channel,
        NULL,
        GRPC_PROPAGATE_DEFAULTS,
        lane->cq,
        grpc_slice_from_static_string(method),
        NULL,
        deadline,
        NULL);
}

static int call_send_initial_message_and_close(grpc_call* call, grpc_completion_queue* cq, grpc_byte_buffer* message) {
    int tag = 0;
    grpc_op ops[3] = {0};
    ops[0].op = GRPC_OP_SEND_INITIAL_METADATA;
    ops[0].data.send_initial_metadata.count = 0;
    ops[1].op = GRPC_OP_SEND_MESSAGE;
    ops[1].flags = GRPC_WRITE_NO_COMPRESS;
    ops[1].data.send_message.send_message = message;
    ops[2].op = GRPC_OP_SEND_CLOSE_FROM_CLIENT;
    return grpc_start_and_wait(call, cq, ops, 3, &tag);
}

static int call_send_initial(grpc_call* call, grpc_completion_queue* cq) {
    int tag = 0;
    grpc_op op = {0};
    op.op = GRPC_OP_SEND_INITIAL_METADATA;
    op.data.send_initial_metadata.count = 0;
    return grpc_start_and_wait(call, cq, &op, 1, &tag);
}

static int call_send_message(grpc_call* call, grpc_completion_queue* cq, grpc_byte_buffer* message) {
    int tag = 0;
    grpc_op op = {0};
    op.op = GRPC_OP_SEND_MESSAGE;
    op.flags = GRPC_WRITE_NO_COMPRESS;
    op.data.send_message.send_message = message;
    return grpc_start_and_wait(call, cq, &op, 1, &tag);
}

static int call_send_close(grpc_call* call, grpc_completion_queue* cq) {
    int tag = 0;
    grpc_op op = {0};
    op.op = GRPC_OP_SEND_CLOSE_FROM_CLIENT;
    return grpc_start_and_wait(call, cq, &op, 1, &tag);
}

static int call_recv_initial(grpc_call* call, grpc_completion_queue* cq) {
    int tag = 0;
    grpc_metadata_array metadata;
    grpc_metadata_array_init(&metadata);
    grpc_op op = {0};
    op.op = GRPC_OP_RECV_INITIAL_METADATA;
    op.data.recv_initial_metadata.recv_initial_metadata = &metadata;
    int result = grpc_start_and_wait(call, cq, &op, 1, &tag);
    grpc_metadata_array_destroy(&metadata);
    return result;
}

static int call_recv_message(
    grpc_call* call, grpc_completion_queue* cq, const ProtobufCMessageDescriptor* descriptor, void** message) {
    *message = NULL;
    int tag = 0;
    grpc_byte_buffer* buffer = NULL;
    grpc_op op = {0};
    op.op = GRPC_OP_RECV_MESSAGE;
    op.data.recv_message.recv_message = &buffer;
    int result = grpc_start_and_wait(call, cq, &op, 1, &tag);
    if (result == 0 && buffer != NULL) {
        result = unpack_byte_buffer(buffer, descriptor, message);
    }
    grpc_byte_buffer_destroy(buffer);
    return result;
}

static int call_recv_status(grpc_call* call, grpc_completion_queue* cq) {
    int tag = 0;
    grpc_metadata_array trailing_metadata;
    grpc_metadata_array_init(&trailing_metadata);
    grpc_status_code status = GRPC_STATUS_UNKNOWN;
    grpc_slice details = grpc_empty_slice();
    grpc_op op = {0};
    op.op = GRPC_OP_RECV_STATUS_ON_CLIENT;
    op.data.recv_status_on_client.trailing_metadata = &trailing_metadata;
    op.data.recv_status_on_client.status = &status;
    op.data.recv_status_on_client.status_details = &details;
    int result = grpc_start_and_wait(call, cq, &op, 1, &tag);
    if (result == 0 && status != GRPC_STATUS_OK) {
        result = -EIO;
    }
    grpc_slice_unref(details);
    grpc_metadata_array_destroy(&trailing_metadata);
    return result;
}

static int client_finish_call(grpc_call* call, grpc_completion_queue* cq, int result) {
    if (result != 0) {
        (void)grpc_call_cancel(call, NULL);
    }
    int status_result = call_recv_status(call, cq);
    if (result == 0) {
        result = status_result;
    }
    grpc_call_unref(call);
    return result;
}

static int client_recv_exact_response(
    grpc_call* call, grpc_completion_queue* cq, uint64_t sequence, uint32_t payload_len) {
    BenchmarkResponse* response = NULL;
    int result =
        call_recv_message(call, cq, &trevrpc__benchmark__v1__benchmark_response__descriptor, (void**)&response);
    if (result == 0) {
        result = validate_response(response, sequence, payload_len);
    }
    if (response != NULL) {
        trevrpc__benchmark__v1__benchmark_response__free_unpacked(response, NULL);
    }
    return result;
}

static int client_expect_eos(grpc_call* call, grpc_completion_queue* cq) {
    BenchmarkResponse* extra = NULL;
    int result = call_recv_message(call, cq, &trevrpc__benchmark__v1__benchmark_response__descriptor, (void**)&extra);
    if (extra != NULL) {
        trevrpc__benchmark__v1__benchmark_response__free_unpacked(extra, NULL);
        return -EINVAL;
    }
    return result;
}

static int client_run_unary(trevrpc_bench_grpc_lane* lane, const client_options* options, uint64_t operation_sequence) {
    BenchmarkRequest request = TREVRPC__BENCHMARK__V1__BENCHMARK_REQUEST__INIT;
    request.sequence = operation_sequence;
    request.payload.len = options->request_bytes;
    request.payload.data = new_payload(options->request_bytes);
    request.response_bytes = options->response_bytes;
    if (options->request_bytes > 0 && request.payload.data == NULL) {
        return -ENOMEM;
    }
    grpc_byte_buffer* request_buffer = pack_message(&request.base);
    free(request.payload.data);
    if (request_buffer == NULL) {
        return -ENOMEM;
    }
    grpc_call* call = client_new_call(lane, GRPC_UNARY_METHOD);
    if (call == NULL) {
        grpc_byte_buffer_destroy(request_buffer);
        return -ENOMEM;
    }
    int result = call_send_initial_message_and_close(call, lane->cq, request_buffer);
    grpc_byte_buffer_destroy(request_buffer);
    if (result == 0) {
        result = call_recv_initial(call, lane->cq);
    }
    if (result == 0) {
        result = client_recv_exact_response(call, lane->cq, operation_sequence, options->response_bytes);
    }
    if (result == 0) {
        result = client_expect_eos(call, lane->cq);
    }
    return client_finish_call(call, lane->cq, result);
}

static int client_run_client_stream(trevrpc_bench_grpc_lane* lane, const client_options* options) {
    grpc_call* call = client_new_call(lane, GRPC_CLIENT_STREAM_METHOD);
    if (call == NULL) {
        return -ENOMEM;
    }
    int result = call_send_initial(call, lane->cq);
    uint8_t* payload = new_payload(options->request_bytes);
    if (result == 0 && options->request_bytes > 0 && payload == NULL) {
        result = -ENOMEM;
    }
    for (uint64_t sequence = 0; result == 0 && sequence < options->messages_per_stream; sequence++) {
        BenchmarkRequest request = TREVRPC__BENCHMARK__V1__BENCHMARK_REQUEST__INIT;
        request.sequence = sequence;
        request.payload = (ProtobufCBinaryData){.len = options->request_bytes, .data = payload};
        request.response_bytes = options->response_bytes;
        grpc_byte_buffer* buffer = pack_message(&request.base);
        if (buffer == NULL) {
            result = -ENOMEM;
            break;
        }
        result = call_send_message(call, lane->cq, buffer);
        grpc_byte_buffer_destroy(buffer);
    }
    free(payload);
    if (result == 0) {
        result = call_send_close(call, lane->cq);
    }
    if (result == 0) {
        result = call_recv_initial(call, lane->cq);
    }
    BenchmarkSummary* summary = NULL;
    if (result == 0) {
        result =
            call_recv_message(call, lane->cq, &trevrpc__benchmark__v1__benchmark_summary__descriptor, (void**)&summary);
    }
    uint64_t expected_bytes = (uint64_t)options->request_bytes * options->messages_per_stream;
    if (result == 0 && (summary == NULL || summary->message_count != options->messages_per_stream ||
                           summary->payload_bytes != expected_bytes)) {
        result = -EINVAL;
    }
    if (summary != NULL) {
        trevrpc__benchmark__v1__benchmark_summary__free_unpacked(summary, NULL);
    }
    BenchmarkSummary* extra = NULL;
    if (result == 0) {
        result =
            call_recv_message(call, lane->cq, &trevrpc__benchmark__v1__benchmark_summary__descriptor, (void**)&extra);
        if (extra != NULL) {
            result = -EINVAL;
            trevrpc__benchmark__v1__benchmark_summary__free_unpacked(extra, NULL);
        }
    }
    return client_finish_call(call, lane->cq, result);
}

static int client_run_server_stream(trevrpc_bench_grpc_lane* lane, const client_options* options) {
    StreamRequest request = TREVRPC__BENCHMARK__V1__STREAM_REQUEST__INIT;
    request.message_count = options->messages_per_stream;
    request.payload.len = options->request_bytes;
    request.payload.data = new_payload(options->request_bytes);
    request.response_bytes = options->response_bytes;
    if (options->request_bytes > 0 && request.payload.data == NULL) {
        return -ENOMEM;
    }
    grpc_byte_buffer* request_buffer = pack_message(&request.base);
    free(request.payload.data);
    if (request_buffer == NULL) {
        return -ENOMEM;
    }
    grpc_call* call = client_new_call(lane, GRPC_SERVER_STREAM_METHOD);
    if (call == NULL) {
        grpc_byte_buffer_destroy(request_buffer);
        return -ENOMEM;
    }
    int result = call_send_initial_message_and_close(call, lane->cq, request_buffer);
    grpc_byte_buffer_destroy(request_buffer);
    if (result == 0) {
        result = call_recv_initial(call, lane->cq);
    }
    for (uint64_t sequence = 0; result == 0 && sequence < options->messages_per_stream; sequence++) {
        result = client_recv_exact_response(call, lane->cq, sequence, options->response_bytes);
    }
    if (result == 0) {
        result = client_expect_eos(call, lane->cq);
    }
    return client_finish_call(call, lane->cq, result);
}

static grpc_byte_buffer* client_bidi_request(const client_options* options, uint64_t sequence, uint8_t* payload) {
    BenchmarkRequest request = TREVRPC__BENCHMARK__V1__BENCHMARK_REQUEST__INIT;
    request.sequence = sequence;
    request.payload = (ProtobufCBinaryData){.len = options->request_bytes, .data = payload};
    request.response_bytes = options->response_bytes;
    return pack_message(&request.base);
}

static int client_run_bidi(trevrpc_bench_grpc_lane* lane, const client_options* options) {
    grpc_call* call = client_new_call(lane, GRPC_BIDI_METHOD);
    if (call == NULL) {
        return -ENOMEM;
    }
    uint8_t* payload = new_payload(options->request_bytes);
    if (options->request_bytes > 0 && payload == NULL) {
        return client_finish_call(call, lane->cq, -ENOMEM);
    }
    grpc_byte_buffer* send_buffer = client_bidi_request(options, 0, payload);
    if (send_buffer == NULL) {
        free(payload);
        return client_finish_call(call, lane->cq, -ENOMEM);
    }

    int send_tag = 0;
    int metadata_tag = 0;
    int receive_tag = 0;
    int close_tag = 0;
    grpc_op send_ops[2] = {0};
    send_ops[0].op = GRPC_OP_SEND_INITIAL_METADATA;
    send_ops[0].data.send_initial_metadata.count = 0;
    send_ops[1].op = GRPC_OP_SEND_MESSAGE;
    send_ops[1].flags = GRPC_WRITE_NO_COMPRESS;
    send_ops[1].data.send_message.send_message = send_buffer;
    grpc_call_error call_error = grpc_call_start_batch(call, send_ops, 2, &send_tag, NULL);
    bool send_pending = call_error == GRPC_CALL_OK;

    grpc_metadata_array initial_metadata;
    grpc_metadata_array_init(&initial_metadata);
    grpc_op metadata_op = {0};
    metadata_op.op = GRPC_OP_RECV_INITIAL_METADATA;
    metadata_op.data.recv_initial_metadata.recv_initial_metadata = &initial_metadata;
    if (call_error == GRPC_CALL_OK) {
        call_error = grpc_call_start_batch(call, &metadata_op, 1, &metadata_tag, NULL);
    }
    bool metadata_pending = call_error == GRPC_CALL_OK;

    uint64_t sent = 0;
    uint64_t received = 0;
    bool send_done = false;
    bool metadata_done = false;
    bool receive_pending = false;
    bool receive_done = false;
    bool close_pending = false;
    bool close_done = false;
    grpc_byte_buffer* receive_buffer = NULL;
    int result = call_error == GRPC_CALL_OK ? 0 : -EINVAL;
    while (result == 0 && (!send_done || !receive_done || !close_done)) {
        grpc_event event = grpc_completion_queue_next(lane->cq, gpr_inf_future(GPR_CLOCK_REALTIME), NULL);
        if (event.type != GRPC_OP_COMPLETE) {
            result = -EIO;
            break;
        }
        if (event.tag == &send_tag) {
            send_pending = false;
            grpc_byte_buffer_destroy(send_buffer);
            send_buffer = NULL;
            if (!event.success) {
                result = -EIO;
            } else if (++sent < options->messages_per_stream) {
                send_buffer = client_bidi_request(options, sent, payload);
                if (send_buffer == NULL) {
                    result = -ENOMEM;
                    break;
                }
                grpc_op send_op = {0};
                send_op.op = GRPC_OP_SEND_MESSAGE;
                send_op.flags = GRPC_WRITE_NO_COMPRESS;
                send_op.data.send_message.send_message = send_buffer;
                if (grpc_call_start_batch(call, &send_op, 1, &send_tag, NULL) != GRPC_CALL_OK) {
                    result = -EINVAL;
                } else {
                    send_pending = true;
                }
            } else {
                send_done = true;
                grpc_op close_op = {0};
                close_op.op = GRPC_OP_SEND_CLOSE_FROM_CLIENT;
                if (grpc_call_start_batch(call, &close_op, 1, &close_tag, NULL) != GRPC_CALL_OK) {
                    result = -EINVAL;
                } else {
                    close_pending = true;
                }
            }
        } else if (event.tag == &metadata_tag) {
            metadata_pending = false;
            if (event.success) {
                metadata_done = true;
            } else {
                result = -EIO;
            }
        } else if (event.tag == &receive_tag) {
            receive_pending = false;
            if (!event.success) {
                grpc_byte_buffer_destroy(receive_buffer);
                receive_buffer = NULL;
                result = -EIO;
            } else if (receive_buffer == NULL) {
                receive_done = true;
                if (received != options->messages_per_stream) {
                    result = -EINVAL;
                }
            } else {
                BenchmarkResponse* response = NULL;
                result = unpack_byte_buffer(
                    receive_buffer, &trevrpc__benchmark__v1__benchmark_response__descriptor, (void**)&response);
                grpc_byte_buffer_destroy(receive_buffer);
                receive_buffer = NULL;
                if (result == 0) {
                    result = validate_response(response, received, options->response_bytes);
                }
                if (response != NULL) {
                    trevrpc__benchmark__v1__benchmark_response__free_unpacked(response, NULL);
                }
                received++;
            }
        } else if (event.tag == &close_tag) {
            close_pending = false;
            if (event.success) {
                close_done = true;
            } else {
                result = -EIO;
            }
        } else {
            result = -EIO;
        }
        if (result == 0 && metadata_done && !receive_pending && !receive_done) {
            grpc_op receive_op = {0};
            receive_op.op = GRPC_OP_RECV_MESSAGE;
            receive_op.data.recv_message.recv_message = &receive_buffer;
            if (grpc_call_start_batch(call, &receive_op, 1, &receive_tag, NULL) != GRPC_CALL_OK) {
                result = -EINVAL;
            } else {
                receive_pending = true;
            }
        }
    }

    if (result != 0) {
        (void)grpc_call_cancel(call, NULL);
    }
    while (send_pending || metadata_pending || receive_pending || close_pending) {
        grpc_event event = grpc_completion_queue_next(lane->cq, gpr_inf_future(GPR_CLOCK_REALTIME), NULL);
        if (event.type != GRPC_OP_COMPLETE) {
            result = -EIO;
            continue;
        }
        if (event.tag == &send_tag && send_pending) {
            send_pending = false;
            grpc_byte_buffer_destroy(send_buffer);
            send_buffer = NULL;
        } else if (event.tag == &metadata_tag && metadata_pending) {
            metadata_pending = false;
        } else if (event.tag == &receive_tag && receive_pending) {
            receive_pending = false;
            grpc_byte_buffer_destroy(receive_buffer);
            receive_buffer = NULL;
        } else if (event.tag == &close_tag && close_pending) {
            close_pending = false;
        } else {
            result = -EIO;
        }
    }
    grpc_byte_buffer_destroy(send_buffer);
    grpc_byte_buffer_destroy(receive_buffer);
    free(payload);
    grpc_metadata_array_destroy(&initial_metadata);
    return client_finish_call(call, lane->cq, result);
}

int trevrpc_bench_grpc_client_connect(const client_options* options, trevrpc_bench_grpc_client** client) {
    *client = NULL;
    char* roots = NULL;
    int result = read_file(options->cert, &roots);
    if (result != 0) {
        return result;
    }
    char target[512];
    result = format_address(options->host, options->port, target, sizeof(target));
    if (result != 0) {
        free(roots);
        return result;
    }

    result = grpc_bench_init();
    if (result != 0) {
        free(roots);
        return result;
    }
    grpc_channel_credentials* credentials = grpc_ssl_credentials_create(roots, NULL, NULL, NULL);
    free(roots);
    if (credentials == NULL) {
        grpc_shutdown();
        return -EINVAL;
    }
    grpc_arg args[7];
    grpc_channel_args channel_args;
    configure_args(args, &channel_args);
    trevrpc_bench_grpc_client* created = calloc(1, sizeof(*created));
    if (created != NULL) {
        created->channel = grpc_channel_create(target, credentials, &channel_args);
    }
    grpc_channel_credentials_release(credentials);
    if (created == NULL || created->channel == NULL) {
        free(created);
        grpc_shutdown();
        return -ENOMEM;
    }
    *client = created;
    return 0;
}

void trevrpc_bench_grpc_client_close(trevrpc_bench_grpc_client* client) {
    if (client == NULL) {
        return;
    }
    grpc_channel_destroy(client->channel);
    free(client);
    grpc_shutdown();
}

int trevrpc_bench_grpc_lane_open(trevrpc_bench_grpc_client* client, trevrpc_bench_grpc_lane** lane) {
    *lane = calloc(1, sizeof(**lane));
    if (*lane == NULL) {
        return -ENOMEM;
    }
    (*lane)->client = client;
    (*lane)->cq = grpc_completion_queue_create_for_next(NULL);
    if ((*lane)->cq == NULL) {
        free(*lane);
        *lane = NULL;
        return -ENOMEM;
    }
    return 0;
}

void trevrpc_bench_grpc_lane_close(trevrpc_bench_grpc_lane* lane) {
    if (lane == NULL) {
        return;
    }
    grpc_cq_destroy(lane->cq);
    free(lane);
}

int trevrpc_bench_grpc_run_operation(
    trevrpc_bench_grpc_lane* lane, const client_options* options, uint64_t operation_sequence) {
    switch (options->rpc_kind) {
    case BENCHMARK_RPC_UNARY:
        return client_run_unary(lane, options, operation_sequence);
    case BENCHMARK_RPC_CLIENT_STREAM:
        return client_run_client_stream(lane, options);
    case BENCHMARK_RPC_SERVER_STREAM:
        return client_run_server_stream(lane, options);
    case BENCHMARK_RPC_BIDI:
        return client_run_bidi(lane, options);
    }
    return -EINVAL;
}

static int call_queue_init(grpc_call_queue* queue, size_t capacity) {
    memset(queue, 0, sizeof(*queue));
    queue->entries = calloc(capacity, sizeof(*queue->entries));
    if (queue->entries == NULL) {
        return -ENOMEM;
    }
    queue->capacity = capacity;
    int result = pthread_mutex_init(&queue->mutex, NULL);
    bool mutex_initialized = result == 0;
    if (result == 0) {
        result = pthread_cond_init(&queue->not_empty, NULL);
    }
    bool not_empty_initialized = result == 0;
    if (result == 0) {
        result = pthread_cond_init(&queue->not_full, NULL);
    }
    if (result != 0) {
        if (not_empty_initialized) {
            pthread_cond_destroy(&queue->not_empty);
        }
        if (mutex_initialized) {
            pthread_mutex_destroy(&queue->mutex);
        }
        free(queue->entries);
        queue->entries = NULL;
        return -result;
    }
    return 0;
}

static void call_queue_close(grpc_call_queue* queue) {
    pthread_mutex_lock(&queue->mutex);
    queue->closed = true;
    pthread_cond_broadcast(&queue->not_empty);
    pthread_cond_broadcast(&queue->not_full);
    pthread_mutex_unlock(&queue->mutex);
}

static int call_queue_push(grpc_call_queue* queue, grpc_server_call* call) {
    pthread_mutex_lock(&queue->mutex);
    while (!queue->closed && queue->count == queue->capacity) {
        pthread_cond_wait(&queue->not_full, &queue->mutex);
    }
    if (queue->closed) {
        pthread_mutex_unlock(&queue->mutex);
        return -ECANCELED;
    }
    size_t index = (queue->head + queue->count) % queue->capacity;
    queue->entries[index] = call;
    queue->count++;
    pthread_cond_signal(&queue->not_empty);
    pthread_mutex_unlock(&queue->mutex);
    return 0;
}

static grpc_server_call* call_queue_pop(grpc_call_queue* queue) {
    pthread_mutex_lock(&queue->mutex);
    while (!queue->closed && queue->count == 0) {
        pthread_cond_wait(&queue->not_empty, &queue->mutex);
    }
    grpc_server_call* call = NULL;
    if (queue->count > 0) {
        call = queue->entries[queue->head];
        queue->head = (queue->head + 1) % queue->capacity;
        queue->count--;
        pthread_cond_signal(&queue->not_full);
    }
    pthread_mutex_unlock(&queue->mutex);
    return call;
}

static void call_queue_destroy(grpc_call_queue* queue) {
    free(queue->entries);
    pthread_cond_destroy(&queue->not_full);
    pthread_cond_destroy(&queue->not_empty);
    pthread_mutex_destroy(&queue->mutex);
}

static grpc_server_call* server_call_create(void) {
    grpc_server_call* call = calloc(1, sizeof(*call));
    if (call == NULL) {
        return NULL;
    }
    grpc_call_details_init(&call->details);
    grpc_metadata_array_init(&call->metadata);
    call->cq = grpc_completion_queue_create_for_next(NULL);
    if (call->cq == NULL) {
        grpc_metadata_array_destroy(&call->metadata);
        grpc_call_details_destroy(&call->details);
        free(call);
        return NULL;
    }
    return call;
}

static void server_call_destroy(grpc_server_call* call) {
    if (call == NULL) {
        return;
    }
    grpc_metadata_array_destroy(&call->metadata);
    grpc_call_details_destroy(&call->details);
    if (call->call != NULL) {
        grpc_call_unref(call->call);
    }
    grpc_cq_destroy(call->cq);
    free(call);
}

static int server_post_accept(trevrpc_bench_grpc_server* server) {
    grpc_server_call* call = server_call_create();
    if (call == NULL) {
        return -ENOMEM;
    }
    pthread_mutex_lock(&server->state_mutex);
    server->pending_accepts++;
    pthread_mutex_unlock(&server->state_mutex);
    grpc_call_error error = grpc_server_request_call(
        server->core, &call->call, &call->details, &call->metadata, call->cq, server->notify_cq, call);
    if (error != GRPC_CALL_OK) {
        pthread_mutex_lock(&server->state_mutex);
        server->pending_accepts--;
        pthread_mutex_unlock(&server->state_mutex);
        server_call_destroy(call);
        return -EIO;
    }
    return 0;
}

static int server_recv_message(grpc_server_call* call, const ProtobufCMessageDescriptor* descriptor, void** message) {
    return call_recv_message(call->call, call->cq, descriptor, message);
}

static int server_send_initial(grpc_server_call* call, bool* started) {
    int tag = 0;
    grpc_op op = {0};
    op.op = GRPC_OP_SEND_INITIAL_METADATA;
    op.data.send_initial_metadata.count = 0;
    grpc_call_error error = grpc_call_start_batch(call->call, &op, 1, &tag, NULL);
    if (error != GRPC_CALL_OK) {
        return -EINVAL;
    }
    *started = true;
    return grpc_wait(call->cq, &tag);
}

static int server_send_message(grpc_server_call* call, const ProtobufCMessage* message) {
    grpc_byte_buffer* buffer = pack_message(message);
    if (buffer == NULL) {
        return -ENOMEM;
    }
    int result = call_send_message(call->call, call->cq, buffer);
    grpc_byte_buffer_destroy(buffer);
    return result;
}

static int server_finish(grpc_server_call* call, bool initial_started, grpc_status_code status, const char* details) {
    int tag = 0;
    int cancelled = 0;
    grpc_slice status_details = grpc_slice_from_static_string(details);
    grpc_op ops[3] = {0};
    size_t count = 0;
    if (!initial_started) {
        ops[count].op = GRPC_OP_SEND_INITIAL_METADATA;
        ops[count].data.send_initial_metadata.count = 0;
        count++;
    }
    ops[count].op = GRPC_OP_SEND_STATUS_FROM_SERVER;
    ops[count].data.send_status_from_server.status = status;
    ops[count].data.send_status_from_server.status_details = &status_details;
    count++;
    ops[count].op = GRPC_OP_RECV_CLOSE_ON_SERVER;
    ops[count].data.recv_close_on_server.cancelled = &cancelled;
    count++;
    grpc_call_error error = grpc_call_start_batch(call->call, ops, count, &tag, NULL);
    if (error == GRPC_CALL_OK) {
        return grpc_wait(call->cq, &tag);
    }

    (void)grpc_call_cancel(call->call, NULL);
    grpc_op recv_close = {0};
    recv_close.op = GRPC_OP_RECV_CLOSE_ON_SERVER;
    recv_close.data.recv_close_on_server.cancelled = &cancelled;
    return grpc_start_and_wait(call->call, call->cq, &recv_close, 1, &tag);
}

static grpc_status_code server_status_for_error(int error) {
    switch (error) {
    case -ENOMEM:
    case -E2BIG:
        return GRPC_STATUS_RESOURCE_EXHAUSTED;
    case -EINVAL:
        return GRPC_STATUS_INVALID_ARGUMENT;
    case -ECANCELED:
        return GRPC_STATUS_CANCELLED;
    default:
        return GRPC_STATUS_INTERNAL;
    }
}

static int server_recv_eos(grpc_server_call* call, const ProtobufCMessageDescriptor* descriptor) {
    ProtobufCMessage* extra = NULL;
    int result = server_recv_message(call, descriptor, (void**)&extra);
    if (extra != NULL) {
        protobuf_c_message_free_unpacked(extra, NULL);
        return -EINVAL;
    }
    return result;
}

static int server_handle_unary(grpc_server_call* call) {
    BenchmarkRequest* request = NULL;
    int result = server_recv_message(call, &trevrpc__benchmark__v1__benchmark_request__descriptor, (void**)&request);
    if (result == 0 && (request == NULL || request->response_bytes > BENCHMARK_MAX_PAYLOAD_BYTES ||
                           request->payload.len > BENCHMARK_MAX_PAYLOAD_BYTES || !payload_is_zero(&request->payload))) {
        result = -EINVAL;
    }
    if (result == 0) {
        result = server_recv_eos(call, &trevrpc__benchmark__v1__benchmark_request__descriptor);
    }
    BenchmarkResponse* response = result == 0 ? new_response(request->sequence, request->response_bytes) : NULL;
    if (result == 0 && response == NULL) {
        result = -ENOMEM;
    }
    if (request != NULL) {
        trevrpc__benchmark__v1__benchmark_request__free_unpacked(request, NULL);
    }
    bool initial_started = false;
    if (result == 0) {
        result = server_send_initial(call, &initial_started);
    }
    if (result == 0) {
        result = server_send_message(call, &response->base);
    }
    if (response != NULL) {
        trevrpc__benchmark__v1__benchmark_response__free_unpacked(response, NULL);
    }
    if (result == 0) {
        return server_finish(call, initial_started, GRPC_STATUS_OK, "");
    }
    (void)server_finish(call, initial_started, server_status_for_error(result), "invalid unary request");
    return result;
}

static int server_handle_client_stream(grpc_server_call* call) {
    uint64_t count = 0;
    uint64_t payload_bytes = 0;
    int result = 0;
    for (;;) {
        BenchmarkRequest* request = NULL;
        result = server_recv_message(call, &trevrpc__benchmark__v1__benchmark_request__descriptor, (void**)&request);
        if (result != 0 || request == NULL) {
            break;
        }
        if (count >= BENCHMARK_MAX_MESSAGES_PER_STREAM || request->payload.len > BENCHMARK_MAX_PAYLOAD_BYTES ||
            !payload_is_zero(&request->payload) || UINT64_MAX - payload_bytes < request->payload.len) {
            result = -EINVAL;
        } else {
            count++;
            payload_bytes += request->payload.len;
        }
        trevrpc__benchmark__v1__benchmark_request__free_unpacked(request, NULL);
        if (result != 0) {
            break;
        }
    }
    BenchmarkSummary* summary = result == 0 ? new_summary(count, payload_bytes) : NULL;
    if (result == 0 && summary == NULL) {
        result = -ENOMEM;
    }
    bool initial_started = false;
    if (result == 0) {
        result = server_send_initial(call, &initial_started);
    }
    if (result == 0) {
        result = server_send_message(call, &summary->base);
    }
    if (summary != NULL) {
        trevrpc__benchmark__v1__benchmark_summary__free_unpacked(summary, NULL);
    }
    if (result == 0) {
        return server_finish(call, initial_started, GRPC_STATUS_OK, "");
    }
    (void)server_finish(call, initial_started, server_status_for_error(result), "invalid client stream");
    return result;
}

static int server_handle_server_stream(grpc_server_call* call) {
    StreamRequest* request = NULL;
    int result = server_recv_message(call, &trevrpc__benchmark__v1__stream_request__descriptor, (void**)&request);
    if (result == 0 &&
        (request == NULL || request->message_count == 0 || request->message_count > BENCHMARK_MAX_MESSAGES_PER_STREAM ||
            request->response_bytes > BENCHMARK_MAX_PAYLOAD_BYTES ||
            request->payload.len > BENCHMARK_MAX_PAYLOAD_BYTES || !payload_is_zero(&request->payload))) {
        result = -EINVAL;
    }
    if (result == 0) {
        result = server_recv_eos(call, &trevrpc__benchmark__v1__stream_request__descriptor);
    }
    bool initial_started = false;
    if (result == 0) {
        result = server_send_initial(call, &initial_started);
    }
    for (uint64_t sequence = 0; result == 0 && sequence < request->message_count; sequence++) {
        BenchmarkResponse* response = new_response(sequence, request->response_bytes);
        if (response == NULL) {
            result = -ENOMEM;
            break;
        }
        result = server_send_message(call, &response->base);
        trevrpc__benchmark__v1__benchmark_response__free_unpacked(response, NULL);
    }
    if (request != NULL) {
        trevrpc__benchmark__v1__stream_request__free_unpacked(request, NULL);
    }
    if (result == 0) {
        return server_finish(call, initial_started, GRPC_STATUS_OK, "");
    }
    (void)server_finish(call, initial_started, server_status_for_error(result), "invalid server stream request");
    return result;
}

static int server_handle_bidi(grpc_server_call* call) {
    bool initial_started = false;
    int result = server_send_initial(call, &initial_started);
    uint64_t count = 0;
    while (result == 0) {
        BenchmarkRequest* request = NULL;
        result = server_recv_message(call, &trevrpc__benchmark__v1__benchmark_request__descriptor, (void**)&request);
        if (result != 0 || request == NULL) {
            break;
        }
        if (count >= BENCHMARK_MAX_MESSAGES_PER_STREAM || request->response_bytes > BENCHMARK_MAX_PAYLOAD_BYTES ||
            request->payload.len > BENCHMARK_MAX_PAYLOAD_BYTES || !payload_is_zero(&request->payload)) {
            result = -EINVAL;
        }
        BenchmarkResponse* response = result == 0 ? new_response(request->sequence, request->response_bytes) : NULL;
        trevrpc__benchmark__v1__benchmark_request__free_unpacked(request, NULL);
        if (result == 0 && response == NULL) {
            result = -ENOMEM;
        }
        if (result == 0) {
            result = server_send_message(call, &response->base);
            count++;
        }
        if (response != NULL) {
            trevrpc__benchmark__v1__benchmark_response__free_unpacked(response, NULL);
        }
    }
    if (result == 0) {
        return server_finish(call, initial_started, GRPC_STATUS_OK, "");
    }
    (void)server_finish(call, initial_started, server_status_for_error(result), "invalid bidi stream");
    return result;
}

static bool method_is(grpc_slice method, const char* expected) {
    return grpc_slice_str_cmp(method, expected) == 0;
}

static void server_handle_call(grpc_server_call* call) {
    if (method_is(call->details.method, GRPC_UNARY_METHOD)) {
        (void)server_handle_unary(call);
    } else if (method_is(call->details.method, GRPC_CLIENT_STREAM_METHOD)) {
        (void)server_handle_client_stream(call);
    } else if (method_is(call->details.method, GRPC_SERVER_STREAM_METHOD)) {
        (void)server_handle_server_stream(call);
    } else if (method_is(call->details.method, GRPC_BIDI_METHOD)) {
        (void)server_handle_bidi(call);
    } else {
        (void)server_finish(call, false, GRPC_STATUS_UNIMPLEMENTED, "unknown method");
    }
}

static void* server_worker(void* context) {
    trevrpc_bench_grpc_server* server = context;
    for (;;) {
        grpc_server_call* call = call_queue_pop(&server->calls);
        if (call == NULL) {
            break;
        }
        server_handle_call(call);
        server_call_destroy(call);
    }
    return NULL;
}

static void* server_accept(void* context) {
    trevrpc_bench_grpc_server* server = context;
    for (;;) {
        grpc_event event = grpc_completion_queue_next(server->notify_cq, gpr_inf_future(GPR_CLOCK_REALTIME), NULL);
        if (event.type != GRPC_OP_COMPLETE) {
            server_record_error(server, -EIO);
            pthread_mutex_lock(&server->state_mutex);
            size_t pending = server->pending_accepts;
            pthread_mutex_unlock(&server->state_mutex);
            if (pending == 0) {
                break;
            }
            continue;
        }
        grpc_server_call* call = event.tag;
        pthread_mutex_lock(&server->state_mutex);
        server->pending_accepts--;
        bool accepting = server->accepting;
        pthread_mutex_unlock(&server->state_mutex);

        if (event.success) {
            if (accepting) {
                int post_result = server_post_accept(server);
                if (post_result != 0) {
                    server_record_error(server, post_result);
                }
            }
            if (call_queue_push(&server->calls, call) != 0) {
                (void)grpc_call_cancel(call->call, NULL);
                server_call_destroy(call);
            }
        } else {
            server_call_destroy(call);
        }

        pthread_mutex_lock(&server->state_mutex);
        accepting = server->accepting;
        size_t pending = server->pending_accepts;
        pthread_mutex_unlock(&server->state_mutex);
        if (!accepting && pending == 0) {
            break;
        }
    }
    call_queue_close(&server->calls);
    return NULL;
}

static int server_drain_accepts(trevrpc_bench_grpc_server* server) {
    int result = 0;
    for (;;) {
        pthread_mutex_lock(&server->state_mutex);
        size_t pending = server->pending_accepts;
        pthread_mutex_unlock(&server->state_mutex);
        if (pending == 0) {
            break;
        }
        grpc_event event = grpc_completion_queue_next(server->notify_cq, gpr_inf_future(GPR_CLOCK_REALTIME), NULL);
        if (event.type != GRPC_OP_COMPLETE || event.tag == NULL) {
            result = -EIO;
            continue;
        }
        pthread_mutex_lock(&server->state_mutex);
        server->pending_accepts--;
        pthread_mutex_unlock(&server->state_mutex);
        grpc_server_call* call = event.tag;
        if (event.success && call->call != NULL) {
            (void)grpc_call_cancel(call->call, NULL);
        }
        server_call_destroy(call);
    }
    return result;
}

static void server_drain_call_queue(trevrpc_bench_grpc_server* server) {
    grpc_server_call* call = NULL;
    while ((call = call_queue_pop(&server->calls)) != NULL) {
        if (call->call != NULL) {
            (void)grpc_call_cancel(call->call, NULL);
        }
        server_call_destroy(call);
    }
}

static void server_start_cleanup(trevrpc_bench_grpc_server* server) {
    if (server->core != NULL) {
        grpc_server_shutdown_and_notify(server->core, server->shutdown_cq, server);
        (void)grpc_completion_queue_next(server->shutdown_cq, gpr_inf_future(GPR_CLOCK_REALTIME), NULL);
        grpc_server_destroy(server->core);
    }
    grpc_cq_destroy(server->notify_cq);
    grpc_cq_destroy(server->shutdown_cq);
    call_queue_destroy(&server->calls);
    pthread_mutex_destroy(&server->state_mutex);
    free(server);
    grpc_shutdown();
}

int trevrpc_bench_grpc_server_start(
    const server_options* options, trevrpc_bench_grpc_server** server, uint16_t* actual_port) {
    *server = NULL;
    *actual_port = 0;
    char* certificate = NULL;
    char* private_key = NULL;
    int result = read_file(options->cert, &certificate);
    if (result == 0) {
        result = read_file(options->key, &private_key);
    }
    if (result != 0) {
        free(certificate);
        free(private_key);
        return result;
    }
    char address[512];
    result = format_address(options->host, options->port, address, sizeof(address));
    if (result != 0) {
        free(certificate);
        free(private_key);
        return result;
    }

    result = grpc_bench_init();
    if (result != 0) {
        free(certificate);
        free(private_key);
        return result;
    }
    trevrpc_bench_grpc_server* created = calloc(1, sizeof(*created));
    if (created == NULL) {
        free(certificate);
        free(private_key);
        grpc_shutdown();
        return -ENOMEM;
    }
    int init_result = pthread_mutex_init(&created->state_mutex, NULL);
    if (init_result != 0) {
        free(certificate);
        free(private_key);
        free(created);
        grpc_shutdown();
        return -init_result;
    }
    result = call_queue_init(&created->calls, BENCHMARK_SERVER_REQUESTS);
    if (result != 0) {
        free(certificate);
        free(private_key);
        pthread_mutex_destroy(&created->state_mutex);
        free(created);
        grpc_shutdown();
        return result;
    }
    created->notify_cq = grpc_completion_queue_create_for_next(NULL);
    created->shutdown_cq = grpc_completion_queue_create_for_next(NULL);
    if (created->notify_cq == NULL || created->shutdown_cq == NULL) {
        free(certificate);
        free(private_key);
        grpc_cq_destroy(created->notify_cq);
        grpc_cq_destroy(created->shutdown_cq);
        call_queue_destroy(&created->calls);
        pthread_mutex_destroy(&created->state_mutex);
        free(created);
        grpc_shutdown();
        return -ENOMEM;
    }
    grpc_arg args[7];
    grpc_channel_args channel_args;
    configure_args(args, &channel_args);
    created->core = grpc_server_create(&channel_args, NULL);
    if (created->core == NULL) {
        free(certificate);
        free(private_key);
        grpc_cq_destroy(created->notify_cq);
        grpc_cq_destroy(created->shutdown_cq);
        call_queue_destroy(&created->calls);
        pthread_mutex_destroy(&created->state_mutex);
        free(created);
        grpc_shutdown();
        return -ENOMEM;
    }
    grpc_server_register_completion_queue(created->core, created->notify_cq, NULL);
    grpc_server_register_completion_queue(created->core, created->shutdown_cq, NULL);
    grpc_ssl_pem_key_cert_pair pair = {.private_key = private_key, .cert_chain = certificate};
    grpc_server_credentials* credentials = grpc_ssl_server_credentials_create(NULL, &pair, 1, 0, NULL);
    int port = credentials == NULL ? 0 : grpc_server_add_http2_port(created->core, address, credentials);
    if (credentials != NULL) {
        grpc_server_credentials_release(credentials);
    }
    free(certificate);
    free(private_key);
    if (port <= 0 || port > UINT16_MAX) {
        server_start_cleanup(created);
        return -EADDRNOTAVAIL;
    }
    grpc_server_start(created->core);
    created->accepting = true;
    for (size_t i = 0; i < GRPC_BENCH_ACCEPTS; i++) {
        result = server_post_accept(created);
        if (result != 0) {
            break;
        }
    }
    if (result == 0) {
        int thread_result = pthread_create(&created->accept_thread, NULL, server_accept, created);
        if (thread_result != 0) {
            result = -thread_result;
        } else {
            created->accept_thread_started = true;
        }
    }
    for (size_t i = 0; result == 0 && i < BENCHMARK_SERVER_WORKERS; i++) {
        int thread_result = pthread_create(&created->workers[i], NULL, server_worker, created);
        if (thread_result != 0) {
            result = -thread_result;
            break;
        }
        created->worker_count++;
    }
    if (result != 0) {
        pthread_mutex_lock(&created->state_mutex);
        created->accepting = false;
        pthread_mutex_unlock(&created->state_mutex);
        grpc_server_shutdown_and_notify(created->core, created->shutdown_cq, created);
        grpc_server_cancel_all_calls(created->core);
        (void)grpc_completion_queue_next(created->shutdown_cq, gpr_inf_future(GPR_CLOCK_REALTIME), NULL);
        if (created->accept_thread_started) {
            (void)pthread_join(created->accept_thread, NULL);
        } else {
            (void)server_drain_accepts(created);
        }
        call_queue_close(&created->calls);
        for (size_t i = 0; i < created->worker_count; i++) {
            (void)pthread_join(created->workers[i], NULL);
        }
        server_drain_call_queue(created);
        grpc_server_destroy(created->core);
        grpc_cq_destroy(created->notify_cq);
        grpc_cq_destroy(created->shutdown_cq);
        call_queue_destroy(&created->calls);
        pthread_mutex_destroy(&created->state_mutex);
        free(created);
        grpc_shutdown();
        return result;
    }
    *actual_port = (uint16_t)port;
    *server = created;
    return 0;
}

bool trevrpc_bench_grpc_server_done(trevrpc_bench_grpc_server* server, int* result) {
    pthread_mutex_lock(&server->state_mutex);
    bool done = server->shutdown_finished || server->accept_result != 0;
    *result = server->accept_result;
    pthread_mutex_unlock(&server->state_mutex);
    return done;
}

int trevrpc_bench_grpc_server_shutdown(trevrpc_bench_grpc_server* server) {
    pthread_mutex_lock(&server->state_mutex);
    if (server->shutdown_started) {
        pthread_mutex_unlock(&server->state_mutex);
        return 0;
    }
    server->shutdown_started = true;
    server->accepting = false;
    pthread_mutex_unlock(&server->state_mutex);

    grpc_server_shutdown_and_notify(server->core, server->shutdown_cq, server);
    grpc_event event = grpc_completion_queue_next(server->shutdown_cq,
        gpr_time_add(gpr_now(GPR_CLOCK_REALTIME), gpr_time_from_nanos(BENCHMARK_GRACEFUL_SHUTDOWN_NS, GPR_TIMESPAN)),
        NULL);
    if (event.type == GRPC_QUEUE_TIMEOUT) {
        grpc_server_cancel_all_calls(server->core);
        event = grpc_completion_queue_next(server->shutdown_cq, gpr_inf_future(GPR_CLOCK_REALTIME), NULL);
    }
    int result = event.type == GRPC_OP_COMPLETE && event.tag == server && event.success ? 0 : -EIO;
    pthread_mutex_lock(&server->state_mutex);
    server->shutdown_finished = true;
    pthread_mutex_unlock(&server->state_mutex);
    return result;
}

int trevrpc_bench_grpc_server_close(trevrpc_bench_grpc_server* server) {
    if (server == NULL) {
        return 0;
    }
    int result = trevrpc_bench_grpc_server_shutdown(server);
    int join_result = server->accept_thread_started ? pthread_join(server->accept_thread, NULL) : 0;
    if (join_result != 0 && result == 0) {
        result = -join_result;
    }
    int drain_result = server_drain_accepts(server);
    if (drain_result != 0 && result == 0) {
        result = drain_result;
    }
    call_queue_close(&server->calls);
    for (size_t i = 0; i < server->worker_count; i++) {
        join_result = pthread_join(server->workers[i], NULL);
        if (join_result != 0 && result == 0) {
            result = -join_result;
        }
    }
    server_drain_call_queue(server);
    pthread_mutex_lock(&server->state_mutex);
    int accept_result = server->accept_result;
    pthread_mutex_unlock(&server->state_mutex);
    if (accept_result != 0 && result == 0) {
        result = accept_result;
    }
    grpc_server_destroy(server->core);
    grpc_cq_destroy(server->notify_cq);
    grpc_cq_destroy(server->shutdown_cq);
    call_queue_destroy(&server->calls);
    pthread_mutex_destroy(&server->state_mutex);
    free(server);
    grpc_shutdown();
    return result;
}
