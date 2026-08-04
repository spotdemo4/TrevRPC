#include "trevrpc_runtime_internal.h"

#include <errno.h> // IWYU pragma: keep
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

static int unused_authorizer(
    void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_status* status) {
    (void)user_data;
    (void)context;
    (void)request;
    (void)status;
    return 0;
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
           test_cancel_is_monotonic_and_idempotent() != 0;
}
