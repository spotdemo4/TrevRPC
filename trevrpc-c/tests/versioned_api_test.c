#include "trevrpc.h"
#include "trevrpc_runtime_internal.h"

#include <errno.h>
#include <pthread.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            return 1;                                                                                                  \
        }                                                                                                              \
    } while (0)

static int test_initializers_and_defaults(void) {
    CHECK(TREVRPC_C_ABI_VERSION == 6u);
    CHECK(trevrpc_c_abi_version() == 6u);
    CHECK(trevrpc_client_config_v1_init(NULL, sizeof(trevrpc_client_config_v1)) == -EINVAL);

    trevrpc_client_config_v1 client;
    memset(&client, 0xa5, sizeof(client));
    CHECK(trevrpc_client_config_v1_init(&client, sizeof(client) - 1) == -EINVAL);
#if SIZE_MAX > UINT32_MAX
    CHECK(trevrpc_client_config_v1_init(&client, (size_t)UINT32_MAX + 1) == -EOVERFLOW);
#endif
    CHECK(trevrpc_client_config_v1_init(&client, sizeof(client)) == 0);
    CHECK(client.struct_size == sizeof(client));
    CHECK(client.struct_version == TREVRPC_STRUCT_VERSION_1);
    trevrpc_client_config_internal client_defaults = trevrpc_internal_default_config();
    CHECK(client.max_frame_size == client_defaults.max_frame_size);
    CHECK(client.max_pending_send_bytes == client_defaults.max_pending_send_bytes);

    trevrpc_server_config_v1 server;
    CHECK(trevrpc_server_config_v1_init(&server, sizeof(server)) == 0);
    CHECK(server.struct_size == sizeof(server));
    CHECK(server.struct_version == TREVRPC_STRUCT_VERSION_1);
    trevrpc_server_config_internal server_defaults = trevrpc_internal_default_server_config();
    CHECK(server.port == server_defaults.port);
    CHECK(server.max_frame_size == server_defaults.max_frame_size);

    trevrpc_server_options_v1 server_options;
    CHECK(trevrpc_server_options_v1_init(&server_options, sizeof(server_options)) == 0);
    trevrpc_server_options_internal server_option_defaults = trevrpc_internal_default_server_options();
    CHECK(server_options.worker_count == server_option_defaults.worker_count);
    CHECK(server_options.graceful_shutdown_timeout_nanos == server_option_defaults.graceful_shutdown_timeout_nanos);

    trevrpc_call_options_v1 call_options;
    CHECK(trevrpc_call_options_v1_init(&call_options, sizeof(call_options)) == 0);
    CHECK(call_options.request_body_lifetime == TREVRPC_REQUEST_BODY_COPY);
    CHECK(call_options.reserved0 == 0);

    trevrpc_response_view_v1 response;
    CHECK(trevrpc_response_view_v1_init(&response, sizeof(response)) == 0);
    CHECK(response.struct_size == sizeof(response));
    CHECK(response.struct_version == TREVRPC_STRUCT_VERSION_1);

    trevrpc_status_view_v1 status;
    CHECK(trevrpc_status_view_v1_init(&status, sizeof(status)) == 0);
    CHECK(status.struct_size == sizeof(status));
    CHECK(status.struct_version == TREVRPC_STRUCT_VERSION_1);
    return 0;
}

static int test_consumer_header_validation(void) {
    trevrpc_server_options_v1 options;
    CHECK(trevrpc_server_options_v1_init(&options, sizeof(options)) == 0);
    options.struct_version = 0;
    CHECK(trevrpc_server_set_options_v1(NULL, &options) == -ENOTSUP);
    options.struct_version = 99;
    CHECK(trevrpc_server_set_options_v1(NULL, &options) == -ENOTSUP);
    options.struct_version = TREVRPC_STRUCT_VERSION_1;
    options.struct_size = sizeof(options) - 1;
    CHECK(trevrpc_server_set_options_v1(NULL, &options) == -EINVAL);

    trevrpc_call_options_v1 call_options;
    CHECK(trevrpc_call_options_v1_init(&call_options, sizeof(call_options)) == 0);
    call_options.request_body_lifetime = 99;
    trevrpc_request request = {
        .kind = TREVRPC_RPC_KIND_UNARY,
        .version = TREVRPC_WIRE_VERSION,
    };
    trevrpc_inbound_response* response = (trevrpc_inbound_response*)1;
    CHECK(trevrpc_raw_client_call_request_inbound_v1(NULL, &request, &call_options, &response) == -EINVAL);
    CHECK(response == (trevrpc_inbound_response*)1);
    return 0;
}

typedef struct connect_hook_state {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    int entered;
    int released;
} connect_hook_state;

typedef struct connect_thread_args {
    trevrpc_client_config_v1 config;
    trevrpc_cancellation* cancellation;
    int result;
} connect_thread_args;

static void connect_retained_hook(void* context) {
    connect_hook_state* state = context;
    pthread_mutex_lock(&state->mutex);
    state->entered = 1;
    pthread_cond_broadcast(&state->cond);
    while (!state->released) {
        pthread_cond_wait(&state->cond, &state->mutex);
    }
    pthread_mutex_unlock(&state->mutex);
}

static void* connect_thread_main(void* context) {
    connect_thread_args* args = context;
    trevrpc_raw_client* client = NULL;
    args->result = trevrpc_raw_client_connect_v1("127.0.0.1", 1, &args->config, args->cancellation, &client);
    trevrpc_raw_client_close(client);
    return NULL;
}

static int test_v1_connect_retains_cancellation(void) {
    connect_hook_state hook = {0};
    CHECK(pthread_mutex_init(&hook.mutex, NULL) == 0);
    CHECK(pthread_cond_init(&hook.cond, NULL) == 0);
    connect_thread_args args = {
        .cancellation = trevrpc_cancellation_new(),
    };
    CHECK(args.cancellation != NULL);
    CHECK(trevrpc_client_config_v1_init(&args.config, sizeof(args.config)) == 0);
    trevrpc_versioned_test_set_connect_hook(connect_retained_hook, &hook);

    pthread_t thread;
    CHECK(pthread_create(&thread, NULL, connect_thread_main, &args) == 0);
    pthread_mutex_lock(&hook.mutex);
    while (!hook.entered) {
        pthread_cond_wait(&hook.cond, &hook.mutex);
    }
    pthread_mutex_unlock(&hook.mutex);

    trevrpc_cancellation_release(args.cancellation);
    CHECK(trevrpc_test_cancellation_ref_count(args.cancellation) == 1);
    trevrpc_cancellation_cancel(args.cancellation);

    pthread_mutex_lock(&hook.mutex);
    hook.released = 1;
    pthread_cond_broadcast(&hook.cond);
    pthread_mutex_unlock(&hook.mutex);
    CHECK(pthread_join(thread, NULL) == 0);
    CHECK(args.result == -ECANCELED);

    trevrpc_versioned_test_set_connect_hook(NULL, NULL);
    CHECK(pthread_cond_destroy(&hook.cond) == 0);
    CHECK(pthread_mutex_destroy(&hook.mutex) == 0);
    return 0;
}

int main(void) {
    return test_initializers_and_defaults() != 0 || test_consumer_header_validation() != 0 ||
           test_v1_connect_retains_cancellation() != 0;
}
