#define _POSIX_C_SOURCE 200809L

#include "trevrpc.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

#define NANOS_PER_SEC 1000000000ull

struct trevrpc_call_context {
    trevrpc_server* server;
    bool has_deadline;
    struct timespec deadline;
};

typedef struct trevrpc_msquic_stream trevrpc_msquic_stream;

struct trevrpc_stream {
    trevrpc_msquic_stream* stream;
    const trevrpc_call_context* context;
    size_t max_frame_size;
    bool owns_stream;
    bool sent_status;
    int64_t max_stream_messages;
    int64_t request_message_count;
    int64_t response_message_count;
    uint32_t failure_status;
    const char* failure_message;
};

#define CHECK_GOTO(condition)                                                                                          \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            result = 1;                                                                                                \
            goto cleanup;                                                                                              \
        }                                                                                                              \
    } while (0)

static int now(struct timespec* out) {
    if (clock_gettime(CLOCK_MONOTONIC, out) != 0) {
        return -errno;
    }
    return 0;
}

static int test_null_context(void) {
    int result = 1;
    uint64_t remaining = 123;

    CHECK_GOTO(trevrpc_call_context_has_deadline(NULL) == 0);
    CHECK_GOTO(trevrpc_call_context_deadline_expired(NULL) == 0);
    CHECK_GOTO(trevrpc_call_context_cancelled(NULL) == 1);
    CHECK_GOTO(trevrpc_call_context_time_remaining_nanos(NULL, NULL) == -EINVAL);
    CHECK_GOTO(trevrpc_call_context_time_remaining_nanos(NULL, &remaining) == 0);
    CHECK_GOTO(remaining == 0);

    result = 0;

cleanup:
    return result;
}

static int test_context_without_deadline(void) {
    int result = 1;
    uint64_t remaining = 123;
    trevrpc_call_context context = {0};

    CHECK_GOTO(trevrpc_call_context_has_deadline(&context) == 0);
    CHECK_GOTO(trevrpc_call_context_deadline_expired(&context) == 0);
    CHECK_GOTO(trevrpc_call_context_cancelled(&context) == 0);
    CHECK_GOTO(trevrpc_call_context_time_remaining_nanos(&context, &remaining) == 0);
    CHECK_GOTO(remaining == 0);

    result = 0;

cleanup:
    return result;
}

static int test_future_deadline(void) {
    int result = 1;
    uint64_t remaining = 0;
    trevrpc_call_context context = {0};

    CHECK_GOTO(now(&context.deadline) == 0);
    context.has_deadline = true;
    context.deadline.tv_sec++;

    CHECK_GOTO(trevrpc_call_context_has_deadline(&context) == 1);
    CHECK_GOTO(trevrpc_call_context_deadline_expired(&context) == 0);
    CHECK_GOTO(trevrpc_call_context_cancelled(&context) == 0);
    CHECK_GOTO(trevrpc_call_context_time_remaining_nanos(&context, &remaining) == 1);
    CHECK_GOTO(remaining > 0);
    CHECK_GOTO(remaining <= 1000000000ull);

    result = 0;

cleanup:
    return result;
}

static int test_expired_deadline(void) {
    int result = 1;
    uint64_t remaining = 123;
    trevrpc_call_context context = {0};

    CHECK_GOTO(now(&context.deadline) == 0);
    context.has_deadline = true;
    context.deadline.tv_sec--;

    CHECK_GOTO(trevrpc_call_context_has_deadline(&context) == 1);
    CHECK_GOTO(trevrpc_call_context_deadline_expired(&context) == 1);
    CHECK_GOTO(trevrpc_call_context_cancelled(&context) == 1);
    CHECK_GOTO(trevrpc_call_context_time_remaining_nanos(&context, &remaining) == 1);
    CHECK_GOTO(remaining == 0);

    result = 0;

cleanup:
    return result;
}

static int test_default_server_options(void) {
    int result = 1;
    trevrpc_server_options options = trevrpc_default_server_options();

    CHECK_GOTO(options.max_concurrent_connections == 256);
    CHECK_GOTO(options.max_concurrent_streams_per_connection == 64);
    CHECK_GOTO(options.max_concurrent_requests == 1024);
    CHECK_GOTO(options.graceful_shutdown_timeout_nanos == 30ull * NANOS_PER_SEC);
    CHECK_GOTO(options.initial_request_timeout_nanos == 10ull * NANOS_PER_SEC);
    CHECK_GOTO(options.max_stream_messages == 4096);
    CHECK_GOTO(options.max_stream_body_size == 16 * 1024 * 1024);
    CHECK_GOTO(options.stream_idle_timeout_nanos == 30ull * NANOS_PER_SEC);

    result = 0;

cleanup:
    return result;
}

static int test_request_stream_message_limit(void) {
    int result = 1;
    trevrpc_stream stream = {
        .stream = (trevrpc_msquic_stream*)1,
        .max_stream_messages = 0,
        .failure_status = TREVRPC_STATUS_OK,
    };
    trevrpc_stream_frame* frame = (trevrpc_stream_frame*)1;

    CHECK_GOTO(trevrpc_stream_recv(&stream, &frame) == TREVRPC_ERR_STREAM_LIMIT_EXCEEDED);
    CHECK_GOTO(frame == NULL);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    CHECK_GOTO(stream.failure_message != NULL);

    result = 0;

cleanup:
    return result;
}

static int test_response_stream_message_limit(void) {
    int result = 1;
    trevrpc_stream stream = {
        .stream = (trevrpc_msquic_stream*)1,
        .max_stream_messages = 0,
        .failure_status = TREVRPC_STATUS_OK,
    };
    const uint8_t body[] = {1};

    CHECK_GOTO(trevrpc_stream_send_message(&stream, body, sizeof(body)) == TREVRPC_ERR_STREAM_LIMIT_EXCEEDED);
    CHECK_GOTO(stream.response_message_count == 0);
    CHECK_GOTO(stream.failure_status == TREVRPC_STATUS_RESOURCE_EXHAUSTED);
    CHECK_GOTO(stream.failure_message != NULL);

    result = 0;

cleanup:
    return result;
}

int main(void) {
    if (test_null_context() != 0) {
        return 1;
    }
    if (test_context_without_deadline() != 0) {
        return 1;
    }
    if (test_future_deadline() != 0) {
        return 1;
    }
    if (test_expired_deadline() != 0) {
        return 1;
    }
    if (test_default_server_options() != 0) {
        return 1;
    }
    if (test_request_stream_message_limit() != 0) {
        return 1;
    }
    if (test_response_stream_message_limit() != 0) {
        return 1;
    }
    return 0;
}
