#define _POSIX_C_SOURCE 200809L

#include "trevrpc_runtime_internal.h"

#include <errno.h> // IWYU pragma: keep
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            return 1;                                                                                                  \
        }                                                                                                              \
    } while (0)

int trevrpc_test_server_new(const trevrpc_client_config_internal* config, trevrpc_server** out_server);
bool trevrpc_test_server_request_try_start(trevrpc_server* server);
void trevrpc_test_server_request_finish(trevrpc_server* server);
bool trevrpc_test_server_connection_try_start(trevrpc_server* server);
void trevrpc_test_server_connection_finish(trevrpc_server* server);
int trevrpc_test_server_wait_from_callback(trevrpc_server* server);
int trevrpc_test_server_release_from_callback(trevrpc_server* server);
void trevrpc_test_server_set_wait_hook(
    trevrpc_server* server, void (*hook)(uint32_t event, void* user_data), void* user_data);
int trevrpc_test_server_try_lock_wait_mutex(trevrpc_server* server);

enum {
    TREVRPC_TEST_SERVER_WAIT_AFTER_PREDICATE = 1u,
    TREVRPC_TEST_SERVER_WAIT_BEFORE_COND_WAIT = 2u,
};

static int unused_authorizer(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_status* status) {
    (void)user_data;
    (void)context;
    (void)request;
    (void)status;
    return 0;
}

typedef struct wait_race {
    trevrpc_server* server;
    pthread_mutex_t control_mutex;
    pthread_cond_t control_cond;
    bool after_predicate;
    bool release_after_predicate;
    bool before_cond_wait;
    unsigned after_predicate_count;
    unsigned before_cond_wait_count;
    int hook_error;
    int mutex_probe_result;
    int waiter_result;
} wait_race;

static void wait_race_record_error(wait_race* race, int err) {
    if (err != 0 && race->hook_error == 0) {
        race->hook_error = -err;
    }
}

static void wait_race_hook(uint32_t event, void* user_data) {
    wait_race* race = user_data;
    int err = pthread_mutex_lock(&race->control_mutex);
    if (err != 0) {
        race->hook_error = -err;
        return;
    }

    if (event == TREVRPC_TEST_SERVER_WAIT_AFTER_PREDICATE) {
        race->after_predicate_count++;
        race->after_predicate = true;
        wait_race_record_error(race, pthread_cond_broadcast(&race->control_cond));
        while (!race->release_after_predicate && race->hook_error == 0) {
            err = pthread_cond_wait(&race->control_cond, &race->control_mutex);
            wait_race_record_error(race, err);
        }
    } else if (event == TREVRPC_TEST_SERVER_WAIT_BEFORE_COND_WAIT) {
        race->before_cond_wait_count++;
        race->before_cond_wait = true;
        wait_race_record_error(race, pthread_cond_broadcast(&race->control_cond));
    } else {
        race->hook_error = -EINVAL;
        wait_race_record_error(race, pthread_cond_broadcast(&race->control_cond));
    }

    err = pthread_mutex_unlock(&race->control_mutex);
    if (err != 0 && race->hook_error == 0) {
        race->hook_error = -err;
    }
}

static int wait_race_wait_for(wait_race* race, bool* predicate) {
    int err = pthread_mutex_lock(&race->control_mutex);
    if (err != 0) {
        return -err;
    }
    while (!*predicate && race->hook_error == 0) {
        err = pthread_cond_wait(&race->control_cond, &race->control_mutex);
        if (err != 0) {
            race->hook_error = -err;
        }
    }
    int result = race->hook_error;
    err = pthread_mutex_unlock(&race->control_mutex);
    if (result != 0) {
        return result;
    }
    return err == 0 ? 0 : -err;
}

static int wait_race_release_after_predicate(wait_race* race) {
    int err = pthread_mutex_lock(&race->control_mutex);
    if (err != 0) {
        return -err;
    }
    race->release_after_predicate = true;
    int broadcast_err = pthread_cond_broadcast(&race->control_cond);
    int unlock_err = pthread_mutex_unlock(&race->control_mutex);
    if (broadcast_err != 0) {
        return -broadcast_err;
    }
    return unlock_err == 0 ? 0 : -unlock_err;
}

static void* wait_until_stopped(void* user_data) {
    wait_race* race = user_data;
    race->waiter_result = trevrpc_server_wait_until(race->server, TREVRPC_DEADLINE_INFINITE);
    return NULL;
}

static int test_freeze_and_mutation_rejection(void) {
    trevrpc_server* server = NULL;
    CHECK(trevrpc_test_server_new(NULL, &server) == 0);
    uint32_t phase = UINT32_MAX;
    CHECK(trevrpc_server_get_phase(server, &phase) == 0);
    CHECK(phase == TREVRPC_SERVER_PHASE_CONFIGURING);

    trevrpc_server_options_v1 options;
    CHECK(trevrpc_server_options_v1_init(&options, sizeof(options)) == 0);
    options.worker_count = 3;
    CHECK(trevrpc_server_set_options_v1(server, &options) == 0);
    CHECK(trevrpc_server_set_authorizer(server, unused_authorizer, NULL) == 0);
    CHECK(trevrpc_server_freeze(server) == 0);
    CHECK(trevrpc_server_freeze(server) == 0);
    CHECK(trevrpc_server_get_phase(server, &phase) == 0);
    CHECK(phase == TREVRPC_SERVER_PHASE_FROZEN);

    options.worker_count = 7;
    CHECK(trevrpc_server_set_options_v1(server, &options) == -EALREADY);
    CHECK(trevrpc_server_set_authorizer(server, unused_authorizer, (void*)1) == -EALREADY);
    trevrpc_server_clear_authorizer(server);
    trevrpc_server_options_v1 observed;
    CHECK(trevrpc_server_options_v1_init(&observed, sizeof(observed)) == 0);
    CHECK(trevrpc_server_get_options_v1(server, &observed) == 0);
    CHECK(observed.worker_count == 3);

    CHECK(trevrpc_server_stop(server) == 0);
    CHECK(trevrpc_server_stop(server) == 0);
    CHECK(trevrpc_server_get_phase(server, &phase) == 0);
    CHECK(phase == TREVRPC_SERVER_PHASE_STOPPING);
    CHECK(trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE) == 0);
    CHECK(trevrpc_server_get_phase(server, &phase) == 0);
    CHECK(phase == TREVRPC_SERVER_PHASE_STOPPED);
    CHECK(trevrpc_server_release(server) == 0);
    return 0;
}

static int test_stop_waits_for_noncooperative_work(void) {
    trevrpc_server* server = NULL;
    CHECK(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK(trevrpc_server_wait_until(server, 0) == -EPERM);
    CHECK(trevrpc_test_server_request_try_start(server));
    CHECK(trevrpc_test_server_connection_try_start(server));
    CHECK(trevrpc_server_stop(server) == 0);
    CHECK(!trevrpc_test_server_request_try_start(server));
    CHECK(!trevrpc_test_server_connection_try_start(server));

    uint64_t now = 0;
    CHECK(trevrpc_monotonic_now_nanos(&now) == 0);
    CHECK(trevrpc_server_wait_until(server, now) == -ETIMEDOUT);
    CHECK(trevrpc_server_release(server) == -EBUSY);
    CHECK(trevrpc_test_server_wait_from_callback(server) == -EDEADLK);
    CHECK(trevrpc_test_server_release_from_callback(server) == -EDEADLK);

    trevrpc_test_server_request_finish(server);
    trevrpc_test_server_connection_finish(server);
    CHECK(trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE) == 0);
    CHECK(trevrpc_server_release(server) == 0);
    return 0;
}

static int test_wait_does_not_lose_final_wakeup(void) {
    trevrpc_server* server = NULL;
    CHECK(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK(trevrpc_test_server_request_try_start(server));
    CHECK(trevrpc_server_stop(server) == 0);

    wait_race race = {
        .server = server,
    };
    CHECK(pthread_mutex_init(&race.control_mutex, NULL) == 0);
    CHECK(pthread_cond_init(&race.control_cond, NULL) == 0);
    trevrpc_test_server_set_wait_hook(server, wait_race_hook, &race);

    pthread_t waiter;
    CHECK(pthread_create(&waiter, NULL, wait_until_stopped, &race) == 0);
    CHECK(wait_race_wait_for(&race, &race.after_predicate) == 0);

    /* The predicate mutex must remain held until the atomic condition wait. */
    race.mutex_probe_result = trevrpc_test_server_try_lock_wait_mutex(server);
    if (race.mutex_probe_result == 0) {
        /* Complete the final request inside a proven historical wait gap. */
        trevrpc_test_server_request_finish(server);
    }

    CHECK(wait_race_release_after_predicate(&race) == 0);
    if (race.mutex_probe_result != 0) {
        trevrpc_test_server_request_finish(server);
    }

    CHECK(wait_race_wait_for(&race, &race.before_cond_wait) == 0);
    /* Rescue a deliberately restored historical implementation after it waits. */
    CHECK(trevrpc_server_stop(server) == 0);

    CHECK(pthread_join(waiter, NULL) == 0);
    trevrpc_test_server_set_wait_hook(server, NULL, NULL);

    CHECK(race.hook_error == 0);
    CHECK(race.after_predicate_count == 1);
    CHECK(race.before_cond_wait_count == 1);
    CHECK(race.mutex_probe_result == -EBUSY);
    CHECK(race.waiter_result == 0);
    CHECK(trevrpc_server_release(server) == 0);
    CHECK(pthread_cond_destroy(&race.control_cond) == 0);
    CHECK(pthread_mutex_destroy(&race.control_mutex) == 0);
    return 0;
}

static int test_cancel_is_monotonic_and_idempotent(void) {
    trevrpc_server* server = NULL;
    CHECK(trevrpc_test_server_new(NULL, &server) == 0);
    CHECK(trevrpc_server_cancel(server) == 0);
    CHECK(trevrpc_server_cancel(server) == 0);
    uint32_t phase = 0;
    CHECK(trevrpc_server_get_phase(server, &phase) == 0);
    CHECK(phase == TREVRPC_SERVER_PHASE_CANCELLING);
    CHECK(trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE) == 0);
    CHECK(trevrpc_server_get_phase(server, &phase) == 0);
    CHECK(phase == TREVRPC_SERVER_PHASE_STOPPED);
    CHECK(trevrpc_server_cancel(server) == 0);
    CHECK(trevrpc_server_get_phase(server, &phase) == 0);
    CHECK(phase == TREVRPC_SERVER_PHASE_STOPPED);
    CHECK(trevrpc_server_release(server) == 0);
    return 0;
}

int main(void) {
    return test_freeze_and_mutation_rejection() != 0 || test_stop_waits_for_noncooperative_work() != 0 ||
           test_wait_does_not_lose_final_wakeup() != 0 || test_cancel_is_monotonic_and_idempotent() != 0;
}
