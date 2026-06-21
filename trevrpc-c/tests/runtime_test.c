#define _POSIX_C_SOURCE 200809L

#include "trevrpc.h"

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

struct trevrpc_call_context {
    trevrpc_server* server;
    bool has_deadline;
    struct timespec deadline;
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
    return 0;
}
