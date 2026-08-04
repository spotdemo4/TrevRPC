#define _POSIX_C_SOURCE 200809L

#include "trevrpc.h"

#include <errno.h> // IWYU pragma: keep
#include <pthread.h>
#include <stdio.h>

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            return 1;                                                                                                  \
        }                                                                                                              \
    } while (0)

typedef struct retained_user {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_cancellation* cancellation;
    int ready;
    int proceed;
    int observed_cancelled;
} retained_user;

static void* retained_user_main(void* context) {
    retained_user* user = context;
    pthread_mutex_lock(&user->mutex);
    user->ready = 1;
    pthread_cond_broadcast(&user->cond);
    while (!user->proceed) {
        pthread_cond_wait(&user->cond, &user->mutex);
    }
    pthread_mutex_unlock(&user->mutex);

    trevrpc_cancellation_cancel(user->cancellation);
    user->observed_cancelled = trevrpc_cancellation_cancelled(user->cancellation);
    trevrpc_cancellation_release(user->cancellation);
    return NULL;
}

static int test_retain_release_and_sticky_cancel(void) {
    CHECK(trevrpc_cancellation_retain(NULL) == -EINVAL);
    trevrpc_cancellation_release(NULL);

    trevrpc_cancellation* cancellation = trevrpc_cancellation_new();
    CHECK(cancellation != NULL);
    CHECK(trevrpc_cancellation_retain(cancellation) == 0);
    trevrpc_cancellation_release(cancellation);
    CHECK(trevrpc_cancellation_cancelled(cancellation) == 0);
    trevrpc_cancellation_cancel(cancellation);
    CHECK(trevrpc_cancellation_cancelled(cancellation) == 1);
    trevrpc_cancellation_cancel(cancellation);
    CHECK(trevrpc_cancellation_cancelled(cancellation) == 1);
    trevrpc_cancellation_release(cancellation);
    return 0;
}

static int test_last_caller_reference_released_while_user_active(void) {
    trevrpc_cancellation* cancellation = trevrpc_cancellation_new();
    CHECK(cancellation != NULL);
    CHECK(trevrpc_cancellation_retain(cancellation) == 0);

    retained_user user = {
        .cancellation = cancellation,
    };
    CHECK(pthread_mutex_init(&user.mutex, NULL) == 0);
    CHECK(pthread_cond_init(&user.cond, NULL) == 0);
    pthread_t thread;
    CHECK(pthread_create(&thread, NULL, retained_user_main, &user) == 0);

    pthread_mutex_lock(&user.mutex);
    while (!user.ready) {
        pthread_cond_wait(&user.cond, &user.mutex);
    }
    trevrpc_cancellation_release(cancellation);
    user.proceed = 1;
    pthread_cond_broadcast(&user.cond);
    pthread_mutex_unlock(&user.mutex);

    CHECK(pthread_join(thread, NULL) == 0);
    CHECK(user.observed_cancelled == 1);
    pthread_cond_destroy(&user.cond);
    pthread_mutex_destroy(&user.mutex);
    return 0;
}

int main(void) {
    if (test_retain_release_and_sticky_cancel() != 0 || test_last_caller_reference_released_while_user_active() != 0) {
        return 1;
    }
    return 0;
}
