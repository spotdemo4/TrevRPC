#define _XOPEN_SOURCE 700

#include "trevrpc_runtime_internal.h"

#include <errno.h> // IWYU pragma: keep
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            return 1;                                                                                                  \
        }                                                                                                              \
    } while (0)

typedef struct stream_race_user {
    pthread_barrier_t* barrier;
    trevrpc_stream* stream;
    int result;
} stream_race_user;

static int barrier_wait(pthread_barrier_t* barrier) {
    int err = pthread_barrier_wait(barrier);
    return err == 0 || err == PTHREAD_BARRIER_SERIAL_THREAD ? 0 : err;
}

static void* send_main(void* context) {
    stream_race_user* user = context;
    static const uint8_t body[] = {1, 2, 3, 4};
    if (barrier_wait(user->barrier) != 0) {
        user->result = -1;
        return NULL;
    }
    for (size_t i = 0; i < 10000; i++) {
        int err = trevrpc_stream_send_message(user->stream, body, sizeof(body));
        if (err != -EINVAL && err != -EPIPE) {
            user->result = err == 0 ? -1 : err;
            return NULL;
        }
    }
    return NULL;
}

static void* status_main(void* context) {
    stream_race_user* user = context;
    if (barrier_wait(user->barrier) != 0) {
        user->result = -1;
        return NULL;
    }
    int err = trevrpc_stream_send_status(user->stream, TREVRPC_STATUS_OK, NULL, 0);
    if (err != -EINVAL && err != -EPIPE) {
        user->result = err == 0 ? -1 : err;
    }
    return NULL;
}

static void* cancel_main(void* context) {
    stream_race_user* user = context;
    if (barrier_wait(user->barrier) != 0) {
        user->result = -1;
        return NULL;
    }
    for (size_t i = 0; i < 10000; i++) {
        trevrpc_stream_cancel(user->stream);
    }
    return NULL;
}

static int test_send_status_cancel_race(void) {
    trevrpc_stream* stream = NULL;
    trevrpc_scripted_stream_source* source = NULL;
    CHECK(trevrpc_scripted_stream_new(NULL, 0, 0, 1024, &stream, &source) == 0);

    pthread_barrier_t barrier;
    CHECK(pthread_barrier_init(&barrier, NULL, 5) == 0);
    stream_race_user users[4] = {
        {.barrier = &barrier, .stream = stream},
        {.barrier = &barrier, .stream = stream},
        {.barrier = &barrier, .stream = stream},
        {.barrier = &barrier, .stream = stream},
    };
    pthread_t threads[4];
    CHECK(pthread_create(&threads[0], NULL, send_main, &users[0]) == 0);
    CHECK(pthread_create(&threads[1], NULL, send_main, &users[1]) == 0);
    CHECK(pthread_create(&threads[2], NULL, status_main, &users[2]) == 0);
    CHECK(pthread_create(&threads[3], NULL, cancel_main, &users[3]) == 0);
    CHECK(barrier_wait(&barrier) == 0);

    for (size_t i = 0; i < 4; i++) {
        CHECK(pthread_join(threads[i], NULL) == 0);
        CHECK(users[i].result == 0);
    }
    CHECK(pthread_barrier_destroy(&barrier) == 0);
    CHECK(trevrpc_stream_send_message(stream, NULL, 0) == -EPIPE);

    trevrpc_stream_close(stream);
    CHECK(trevrpc_scripted_stream_close_count(source) == 1);
    trevrpc_scripted_stream_source_free(source);
    return 0;
}

int main(void) {
    return test_send_status_cancel_race();
}
