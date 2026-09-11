#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
#define _DARWIN_C_SOURCE
#endif
#define _POSIX_C_SOURCE 200809L

#include "trevrpc_rpc.h"
#include "trevrpc_rpc_msquic.h"

#include <errno.h>
#include <inttypes.h>
#include <node_api.h>
#include <poll.h>
#include <sched.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <uv.h>

#if !defined(TREVRPC_RPC_ABI_VERSION) || TREVRPC_RPC_ABI_VERSION != 1u
#error "The native addon requires TrevRPC RPC ABI 1"
#endif
#if !defined(TREVRPC_RPC_MSQUIC_ABI_VERSION) || TREVRPC_RPC_MSQUIC_ABI_VERSION != 1u
#error "The native addon requires TrevRPC RPC MsQuic ABI 1"
#endif

_Static_assert(TREVRPC_RPC_ABI_VERSION == 1u, "RPC ABI anchor must be version 1");
_Static_assert(TREVRPC_RPC_MSQUIC_ABI_VERSION == 1u, "RPC MsQuic ABI anchor must be version 1");

#define TREV_NODE_INITIAL_OPERATION_ID 1u

/* The JavaScript wrapper deliberately owns no provider pointers.  These cores
 * are detached from wrappers and remain in the registry until the RPC ABI has
 * committed and delivered their terminal event. */
typedef struct node_handle_key {
    uint64_t owner;
    uint32_t slot;
    uint32_t generation;
} node_handle_key;

typedef struct node_subject node_subject;
typedef struct node_operation node_operation;
typedef struct node_runtime node_runtime;
typedef struct node_runtime_instance_holder node_runtime_instance_holder;
typedef struct node_cancellation node_cancellation;
typedef struct node_registry node_registry;
typedef struct node_server node_server;
typedef struct node_server_route node_server_route;
typedef struct node_client node_client;
typedef struct node_call node_call;
typedef struct node_send_batch node_send_batch;
typedef struct node_receive_waiter node_receive_waiter;
typedef struct node_receive_item node_receive_item;

typedef enum node_operation_action {
    NODE_OPERATION_NONE = 0,
    NODE_OPERATION_ENDPOINT_START,
    NODE_OPERATION_ENDPOINT_LISTEN,
    NODE_OPERATION_ENDPOINT_CLOSE,
    NODE_OPERATION_CALL_OPEN,
    NODE_OPERATION_CALL_ACCEPT,
    NODE_OPERATION_CALL_CLOSE,
    NODE_OPERATION_STREAM_CLOSE,
    NODE_OPERATION_CALL_RESPOND,
    NODE_OPERATION_CALL_FINISH,
    NODE_OPERATION_SEND,
    NODE_OPERATION_FINISH_SEND,
} node_operation_action;

typedef enum node_lifecycle_state {
    NODE_LIFECYCLE_RUNNING = 0,
    NODE_LIFECYCLE_CLOSING = 1,
    NODE_LIFECYCLE_STOPPED = 2,
    NODE_LIFECYCLE_RELEASED = 3,
} node_lifecycle_state;

typedef enum node_cleanup_owner {
    NODE_CLEANUP_OWNER_NONE = 0,
    NODE_CLEANUP_OWNER_ASYNC_HOOK = 1,
    NODE_CLEANUP_OWNER_INSTANCE_FINALIZER = 2,
} node_cleanup_owner;

typedef enum node_cleanup_state {
    NODE_CLEANUP_OPEN = 0,
    NODE_CLEANUP_CLAIMED = 1,
    NODE_CLEANUP_COMPLETE = 2,
} node_cleanup_state;

typedef enum node_subject_kind {
    NODE_SUBJECT_ENDPOINT = TREVRPC_RPC_OBJECT_ENDPOINT,
    NODE_SUBJECT_CALL = TREVRPC_RPC_OBJECT_CALL,
    NODE_SUBJECT_STREAM = TREVRPC_RPC_OBJECT_STREAM,
    NODE_SUBJECT_CANCELLATION = TREVRPC_RPC_OBJECT_CANCELLATION,
} node_subject_kind;

typedef struct node_metadata_value {
    char* key;
    uint8_t* value;
    size_t value_len;
} node_metadata_value;

typedef struct node_status_value {
    uint32_t code;
    char* message;
    node_metadata_value* metadata;
    size_t metadata_count;
} node_status_value;

struct node_subject {
    node_subject* next;
    node_handle_key key;
    node_subject_kind kind;
    trevrpc_rpc_cancellation_v1 cancellation;
    node_cancellation* wrapper;
    node_client* client_head;
    bool terminal_event_seen;
    bool release_requested;
    bool release_retry_pending;
    bool native_released;
    bool cancel_submitted;
    bool cancel_completed;
    void* core;
};

struct node_operation {
    node_operation* next;
    uint64_t id;
    node_handle_key subject;
    uint32_t subject_kind;
    bool subject_bound;
    node_operation_action action;
    void* context;
    napi_deferred deferred;
    bool has_deferred;
    bool completion_seen;
};

struct node_receive_item {
    node_receive_item* next;
    trevrpc_rpc_receive* receive;
};

struct node_receive_waiter {
    node_receive_waiter* next;
    napi_deferred deferred;
    uint32_t max_items;
    bool body_batch;
};

struct node_send_batch {
    node_send_batch* next;
    node_call* call;
    uint8_t** bodies;
    size_t* body_lengths;
    size_t count;
    size_t index;
    bool finish;
    napi_deferred deferred;
    bool has_deferred;
    uint64_t operation_id;
};

struct node_server_route {
    node_server_route* next;
    node_server* server;
    char* service;
    char* method;
    uint32_t kind;
    napi_ref handler_ref;
    size_t active_calls;
};

struct node_server {
    node_runtime* runtime;
    node_client* endpoint;
    node_server_route* routes;
    napi_ref wrapper_ref;
    napi_ref serve_promise_ref;
    napi_ref admission_ref;
    napi_deferred serve_deferred;
    bool enable_http3;
    bool wrapper_alive;
    bool ready;
    bool serve_called;
    bool closing;
    bool closed;
    bool serve_settled;
    uint16_t port;
};

struct node_client {
    node_runtime* runtime;
    node_subject* subject;
    node_server* server;
    node_subject* cancellation_subject;
    node_client* cancellation_next;
    trevrpc_rpc_endpoint_v1 endpoint;
    napi_ref wrapper_ref;
    napi_deferred closed_deferred;
    bool wrapper_alive;
    bool invalidated;
    bool ready;
    bool close_submitted;
    bool close_retry_pending;
    bool release_retry_pending;
    bool closed;
    bool terminal_settled;
};

struct node_call {
    node_runtime* runtime;
    node_client* client;
    node_server* server;
    node_server_route* route;
    node_subject* call_subject;
    node_subject* stream_subject;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_cancellation_v1 cancellation;
    uint32_t kind;
    napi_ref wrapper_ref;
    bool wrapper_alive;
    bool server_side;
    bool accepted;
    bool accept_submitted;
    bool request_finished;
    bool response_submitted;
    bool invalidated;
    bool ready;
    bool close_submitted;
    bool stream_close_submitted;
    bool close_retry_pending;
    bool stream_close_retry_pending;
    bool call_release_retry_pending;
    bool stream_release_retry_pending;
    bool call_closed;
    bool stream_closed;
    bool fin_seen;
    bool fin_clean;
    bool status_seen;
    bool message_seen;
    bool receive_finished;
    bool receive_succeeded;
    bool response_settled;
    bool terminal_settled;
    bool close_requested;
    bool close_abort;
    bool send_finished;
    bool finish_enqueued;
    bool failed;
    int failure_code;
    napi_deferred response_deferred;
    bool has_response_deferred;
    node_status_value status;
    trevrpc_rpc_receive* unary_message;
    node_receive_item* receive_head;
    node_receive_item* receive_tail;
    node_receive_waiter* waiter_head;
    node_receive_waiter* waiter_tail;
    node_send_batch* send_head;
    node_send_batch* send_tail;
    node_send_batch* send_inflight;
    uint8_t* server_response_body;
    size_t server_response_body_len;
    bool server_response_body_set;
};

struct node_registry {
    node_subject* head;
    size_t length;
};

struct node_cancellation {
    node_runtime* runtime;
    node_subject* subject;
    trevrpc_rpc_cancellation_v1 handle;
    bool wrapper_alive;
    bool cancel_submitted;
    bool cancel_completed;
};

struct node_runtime_instance_holder {
    node_runtime* runtime;
};

struct node_runtime {
    napi_env env;
    uv_loop_t* loop;
    uv_poll_t poll;
    uv_timer_t failure_progress;
    uint64_t failure_progress_delay_ms;
    uv_handle_t* liveness_handle;
    bool poll_initialized;
    bool poll_started;
    bool poll_closing;
    bool poll_closed;
    bool failure_progress_initialized;
    bool failure_progress_started;
    bool failure_progress_closing;
    bool cleanup_hook_registered;
    bool instance_data_registered;
    bool environment_teardown;
    bool cleanup_started;
    bool close_submitted;
    node_cleanup_owner cleanup_owner;
    node_cleanup_state cleanup_state;
    uint64_t test_runtime_id;
    bool close_retry_pending;
    bool runtime_release_retry_pending;
    bool stopped_seen;
    bool runtime_released;
    bool impossible_event;
    bool construction_failed;
    bool poll_start_failed;
    bool test_failure_consumed;
    bool cancellation_release_busy_injected;
    uint32_t cancellation_release_failures_remaining;
    bool close_submission_failure_injected;
    uint32_t close_submission_failures_remaining;
    uint32_t runtime_release_failures_remaining;
    bool send_operation_id_failure_injected;
    bool send_operation_record_failure_injected;
    bool shutdown_operation_in_use;
    uint32_t close_attempts;
    uint32_t release_attempts;
    node_lifecycle_state state;
    trevrpc_rpc_runtime* rpc;
    trevrpc_rpc_wake_source_v1 wake;
    uint64_t next_operation_id;
    uint64_t last_event_sequence;
    node_operation shutdown_operation;
    node_operation* operations;
    node_registry endpoints;
    node_registry calls;
    node_registry streams;
    node_registry cancellations;
    napi_async_cleanup_hook_handle cleanup_hook;
    node_runtime_instance_holder* instance_holder;
};

static void node_runtime_free(node_runtime* runtime);
static void node_runtime_finalize_free(node_runtime* runtime);
static void node_runtime_begin_cleanup(node_runtime* runtime);
static void node_runtime_poll_close(node_runtime* runtime);
static void node_runtime_start_failure_progress(node_runtime* runtime);
static void node_runtime_stop_failure_progress(node_runtime* runtime);
static void node_runtime_close_failure_progress(node_runtime* runtime);
static bool node_runtime_claim_cleanup(node_runtime* runtime, node_cleanup_owner owner);
static bool node_runtime_has_pending_progress(node_runtime* runtime);
static void node_runtime_update_liveness(node_runtime* runtime);
static void node_runtime_detach_instance_data(node_runtime* runtime);
static int node_runtime_shutdown_sync(node_runtime* runtime);
static int node_complete_operation(
    node_runtime* runtime, node_operation* operation, const trevrpc_rpc_event_info_v1* info);
static int node_route_incoming_call(
    node_runtime* runtime, trevrpc_rpc_event* event, const trevrpc_rpc_event_info_v1* info);
static int node_route_admission(node_runtime* runtime, trevrpc_rpc_event* event, const trevrpc_rpc_event_info_v1* info);
static void node_process_call(node_call* call);
static void node_call_request_close(node_call* call);
static void node_call_maybe_finish_client_receive(node_call* call);
static void node_call_maybe_release(node_call* call);
static void node_call_detach_server(node_runtime* runtime, node_call* call);
static int node_client_request_close(node_client* client);
static void node_resolve_undefined(napi_env env, napi_deferred deferred);
static int node_reject_deferred_native(napi_env env, napi_deferred deferred, int error_code, const char* operation);
static void node_send_batch_free(node_send_batch* batch);
static void node_send_batch_remove(node_call* call, node_send_batch* batch);
static void node_call_dispose_buffers(node_call* call);
static void node_status_free(node_status_value* status);
static bool node_close_retryable(int result);
static napi_value node_make_server_call_object(napi_env env, node_call* call);
static int node_call_server_fail(node_call* call, uint32_t status, const char* message);
static void node_server_free_routes(napi_env env, node_server* server);
static void node_client_detach_server(node_runtime* runtime, node_client* client);

#ifdef TREVRPC_NODE_TEST_HOOKS
static bool node_test_flag(const char* name) {
    const char* value = getenv(name);
    return value != NULL && strcmp(value, "1") == 0;
}

static _Atomic uint64_t node_test_next_runtime_id = 1u;

static void node_test_trace_runtime(const node_runtime* runtime, const char* event) {
    uint64_t runtime_id = runtime == NULL ? 0u : runtime->test_runtime_id;
    long process_id = (long)uv_os_getpid();
    fprintf(stderr, "trevrpc-node-test:pid=%ld runtime=%" PRIu64 " event=%s\n", process_id, runtime_id, event);
    fflush(stderr);
    const char* trace_file = getenv("TREVRPC_NODE_TRACE_FILE");
    if (trace_file != NULL) {
        FILE* file = fopen(trace_file, "a");
        if (file != NULL) {
            fprintf(file, "pid=%ld runtime=%" PRIu64 " event=%s\n", process_id, runtime_id, event);
            fclose(file);
        }
    }
}

static void node_test_trace_key(const node_runtime* runtime, const char* event, uint32_t kind, node_handle_key key) {
    uint64_t runtime_id = runtime == NULL ? 0u : runtime->test_runtime_id;
    long process_id = (long)uv_os_getpid();
    fprintf(stderr,
        "trevrpc-node-test:pid=%ld runtime=%" PRIu64 " event=%s kind=%" PRIu32 " owner=%" PRIu64 " slot=%" PRIu32
        " generation=%" PRIu32 "\n",
        process_id,
        runtime_id,
        event,
        kind,
        key.owner,
        key.slot,
        key.generation);
    fflush(stderr);
    const char* trace_file = getenv("TREVRPC_NODE_TRACE_FILE");
    if (trace_file != NULL) {
        FILE* file = fopen(trace_file, "a");
        if (file != NULL) {
            fprintf(file,
                "pid=%ld runtime=%" PRIu64 " event=%s kind=%" PRIu32 " owner=%" PRIu64 " slot=%" PRIu32
                " generation=%" PRIu32 "\n",
                process_id,
                runtime_id,
                event,
                kind,
                key.owner,
                key.slot,
                key.generation);
            fclose(file);
        }
    }
}

#else
static bool node_test_flag(const char* name) {
    (void)name;
    return false;
}

static void node_test_trace_runtime(const node_runtime* runtime, const char* event) {
    (void)runtime;
    (void)event;
}

static void node_test_trace_key(const node_runtime* runtime, const char* event, uint32_t kind, node_handle_key key) {
    (void)runtime;
    (void)event;
    (void)kind;
    (void)key;
}

#endif

static bool node_send_test_failure(node_runtime* runtime, bool record_failure) {
    if (runtime == NULL) {
        return false;
    }
    bool* injected = record_failure ? &runtime->send_operation_record_failure_injected
                                    : &runtime->send_operation_id_failure_injected;
    const char* flag =
        record_failure ? "TREVRPC_NODE_FAIL_SEND_OPERATION_RECORD" : "TREVRPC_NODE_FAIL_SEND_OPERATION_ID";
    if (!*injected && node_test_flag(flag)) {
        *injected = true;
        return true;
    }
    return false;
}

static bool node_key_equal(node_handle_key left, node_handle_key right) {
    return left.owner == right.owner && left.slot == right.slot && left.generation == right.generation;
}

static node_handle_key node_key_from_endpoint(trevrpc_rpc_endpoint_v1 value) {
    node_handle_key key = {value.owner, value.slot, value.generation};
    return key;
}

static node_handle_key node_key_from_call(trevrpc_rpc_call_v1 value) {
    node_handle_key key = {value.owner, value.slot, value.generation};
    return key;
}

static node_handle_key node_key_from_stream(trevrpc_rpc_stream_v1 value) {
    node_handle_key key = {value.owner, value.slot, value.generation};
    return key;
}

static node_handle_key node_key_from_cancellation(trevrpc_rpc_cancellation_v1 value) {
    node_handle_key key = {value.owner, value.slot, value.generation};
    return key;
}

static bool node_runtime_napi_legal(const node_runtime* runtime) {
    return runtime != NULL && !runtime->environment_teardown;
}

static void node_client_detach_cancellation(node_client* client);
static void node_client_maybe_release(node_client* client);
static void node_call_settle_terminal(node_call* call, int error_code, const char* operation);
static void node_call_delete_wrapper_ref(node_call* call);
static void node_client_delete_wrapper_ref(node_client* client);

static bool node_client_usable(const node_client* client) {
    return client != NULL && client->wrapper_alive && !client->invalidated && client->runtime != NULL &&
           client->runtime->rpc != NULL && client->runtime->state == NODE_LIFECYCLE_RUNNING;
}

static bool node_call_usable(const node_call* call) {
    return call != NULL && call->wrapper_alive && !call->invalidated && call->runtime != NULL &&
           call->runtime->rpc != NULL && call->runtime->state == NODE_LIFECYCLE_RUNNING;
}

static node_registry* node_registry_for_kind(node_runtime* runtime, uint32_t kind) {
    switch (kind) {
    case TREVRPC_RPC_OBJECT_ENDPOINT:
        return &runtime->endpoints;
    case TREVRPC_RPC_OBJECT_CALL:
        return &runtime->calls;
    case TREVRPC_RPC_OBJECT_STREAM:
        return &runtime->streams;
    case TREVRPC_RPC_OBJECT_CANCELLATION:
        return &runtime->cancellations;
    default:
        return NULL;
    }
}

static node_subject* node_registry_find(node_registry* registry, node_handle_key key) {
    for (node_subject* subject = registry->head; subject != NULL; subject = subject->next) {
        if (node_key_equal(subject->key, key)) {
            return subject;
        }
    }
    return NULL;
}

static node_subject* node_subject_allocate(uint32_t kind, node_handle_key key) {
    node_subject* subject = calloc(1, sizeof(*subject));
    if (subject != NULL) {
        subject->key = key;
        subject->kind = (node_subject_kind)kind;
    }
    return subject;
}

static void node_registry_insert(node_runtime* runtime, node_subject* subject) {
    node_registry* registry = node_registry_for_kind(runtime, subject->kind);
    subject->next = registry->head;
    registry->head = subject;
    registry->length++;
    node_test_trace_key(runtime, "key-insert", (uint32_t)subject->kind, subject->key);
}

static node_subject* node_registry_add(node_runtime* runtime, uint32_t kind, node_handle_key key) {
    node_registry* registry = node_registry_for_kind(runtime, kind);
    if (registry == NULL || node_registry_find(registry, key) != NULL) {
        return NULL;
    }
    node_subject* subject = node_subject_allocate(kind, key);
    if (subject == NULL) {
        return NULL;
    }
    node_registry_insert(runtime, subject);
    return subject;
}

static void node_registry_remove(node_runtime* runtime, node_subject* target) {
    node_registry* registry = node_registry_for_kind(runtime, target->kind);
    if (registry == NULL) {
        return;
    }
    node_subject** link = &registry->head;
    while (*link != NULL) {
        if (*link == target) {
            *link = target->next;
            registry->length--;
            node_test_trace_key(runtime, "key-remove", (uint32_t)target->kind, target->key);
            free(target);
            return;
        }
        link = &(*link)->next;
    }
}

static void node_registry_discard_all(node_registry* registry) {
    node_subject* subject = registry->head;
    while (subject != NULL) {
        node_subject* next = subject->next;
        free(subject);
        subject = next;
    }
    registry->head = NULL;
    registry->length = 0;
}

static void node_client_attach_cancellation(node_client* client, node_subject* subject) {
    if (client == NULL || client->cancellation_subject == subject) {
        return;
    }
    node_client_detach_cancellation(client);
    if (subject == NULL || subject->kind != NODE_SUBJECT_CANCELLATION) {
        return;
    }
    client->cancellation_subject = subject;
    client->cancellation_next = subject->client_head;
    subject->client_head = client;
}

static void node_client_detach_cancellation(node_client* client) {
    if (client == NULL || client->cancellation_subject == NULL) {
        return;
    }
    node_subject* subject = client->cancellation_subject;
    node_client** link = &subject->client_head;
    while (*link != NULL) {
        if (*link == client) {
            *link = client->cancellation_next;
            break;
        }
        link = &(*link)->cancellation_next;
    }
    client->cancellation_subject = NULL;
    client->cancellation_next = NULL;
}

static bool node_event_key(const trevrpc_rpc_event_info_v1* info, node_handle_key* out_key) {
    switch (info->subject_kind) {
    case TREVRPC_RPC_OBJECT_ENDPOINT:
        *out_key = node_key_from_endpoint(info->endpoint);
        return true;
    case TREVRPC_RPC_OBJECT_CALL:
        *out_key = node_key_from_call(info->call);
        return true;
    case TREVRPC_RPC_OBJECT_STREAM:
        *out_key = node_key_from_stream(info->stream);
        return true;
    case TREVRPC_RPC_OBJECT_CANCELLATION:
        *out_key = node_key_from_cancellation(info->cancellation);
        return true;
    default:
        return false;
    }
}

static node_subject* node_subject_from_event(node_runtime* runtime, const trevrpc_rpc_event_info_v1* info) {
    node_handle_key key;
    if (!node_event_key(info, &key)) {
        return NULL;
    }
    node_registry* registry = node_registry_for_kind(runtime, info->subject_kind);
    return registry == NULL ? NULL : node_registry_find(registry, key);
}

static void node_cancellation_try_release(node_runtime* runtime, node_subject* subject) {
    if (runtime == NULL || runtime->rpc == NULL || subject == NULL || subject->kind != NODE_SUBJECT_CANCELLATION ||
        subject->native_released || !subject->release_requested || !subject->cancel_completed) {
        return;
    }
    int result;
    if (node_test_flag("TREVRPC_NODE_FAIL_CANCELLATION_RELEASE_PERSISTENT")) {
        node_test_trace_runtime(runtime, "cancellation-release-busy");
        result = -EBUSY;
    } else {
        if (!runtime->cancellation_release_busy_injected &&
            node_test_flag("TREVRPC_NODE_FAIL_CANCELLATION_RELEASE_FIVE")) {
            runtime->cancellation_release_busy_injected = true;
            runtime->cancellation_release_failures_remaining = 5u;
        }
        if (runtime->cancellation_release_failures_remaining > 0) {
            runtime->cancellation_release_failures_remaining--;
            node_test_trace_runtime(runtime, "cancellation-release-busy");
            result = -EBUSY;
        } else if (!runtime->cancellation_release_busy_injected &&
                   node_test_flag("TREVRPC_NODE_FAIL_CANCELLATION_RELEASE_BUSY")) {
            runtime->cancellation_release_busy_injected = true;
            node_test_trace_runtime(runtime, "cancellation-release-busy");
            result = -EBUSY;
        } else {
            result = trevrpc_rpc_cancellation_release(runtime->rpc, subject->cancellation);
        }
    }
    if (result == 0 || result == -ESTALE) {
        while (subject->client_head != NULL) {
            node_client_detach_cancellation(subject->client_head);
        }
        if (subject->wrapper != NULL) {
            subject->wrapper->subject = NULL;
            subject->wrapper->runtime = NULL;
            subject->wrapper = NULL;
        }
        subject->native_released = true;
        subject->release_retry_pending = false;
        node_registry_remove(runtime, subject);
        node_runtime_update_liveness(runtime);
        node_test_trace_runtime(runtime, "cancellation-release");
    } else {
        subject->release_retry_pending = true;
        node_runtime_start_failure_progress(runtime);
    }
}

static void node_runtime_release_pending_cancellations(node_runtime* runtime) {
    node_subject* subject = runtime->cancellations.head;
    while (subject != NULL) {
        node_subject* next = subject->next;
        node_cancellation_try_release(runtime, subject);
        subject = next;
    }
}

static node_operation* node_operation_find(node_runtime* runtime, uint64_t id) {
    for (node_operation* operation = runtime->operations; operation != NULL; operation = operation->next) {
        if (operation->id == id) {
            return operation;
        }
    }
    return NULL;
}

static void node_operation_remove(node_runtime* runtime, node_operation* target) {
    node_operation** link = &runtime->operations;
    while (*link != NULL) {
        if (*link == target) {
            *link = target->next;
            node_test_trace_key(runtime, "key-unbind", target->subject_kind, target->subject);
            if (target == &runtime->shutdown_operation) {
                runtime->shutdown_operation_in_use = false;
                memset(target, 0, sizeof(*target));
            } else {
                free(target);
            }
            node_runtime_update_liveness(runtime);
            return;
        }
        link = &(*link)->next;
    }
}

static uint64_t node_operation_allocate(node_runtime* runtime) {
    uint64_t id = runtime->next_operation_id++;
    while (id == TREVRPC_RPC_OPERATION_ID_NONE || id == runtime->shutdown_operation.id ||
           node_operation_find(runtime, id) != NULL) {
        id = runtime->next_operation_id++;
        if (id == TREVRPC_RPC_OPERATION_ID_NONE) {
            return TREVRPC_RPC_OPERATION_ID_NONE;
        }
    }
    return id;
}

static node_operation* node_operation_add_id(node_runtime* runtime,
    uint64_t id,
    uint32_t subject_kind,
    node_handle_key subject,
    node_operation_action action,
    void* context,
    napi_deferred deferred,
    bool has_deferred) {
    if (id == TREVRPC_RPC_OPERATION_ID_NONE || node_operation_find(runtime, id) != NULL) {
        return NULL;
    }
    node_operation* operation = calloc(1, sizeof(*operation));
    if (operation == NULL) {
        return NULL;
    }
    operation->id = id;
    operation->subject = subject;
    operation->subject_kind = subject_kind;
    operation->subject_bound = subject_kind == TREVRPC_RPC_OBJECT_NONE ||
                               !(subject.owner == 0 && subject.slot == 0 && subject.generation == 0);
    operation->action = action;
    operation->context = context;
    operation->deferred = deferred;
    operation->has_deferred = has_deferred;
    operation->next = runtime->operations;
    runtime->operations = operation;
    node_test_trace_key(runtime, "key-bind", subject_kind, subject);
    node_runtime_update_liveness(runtime);
    return operation;
}

static node_operation* node_operation_add(node_runtime* runtime, uint32_t subject_kind, node_handle_key subject) {
    if (node_test_flag("TREVRPC_NODE_FAIL_OPERATION_ALLOCATION")) {
        return NULL;
    }
    uint64_t id = node_operation_allocate(runtime);
    return node_operation_add_id(runtime, id, subject_kind, subject, NODE_OPERATION_NONE, NULL, NULL, false);
}

static int node_copy_event_info(node_runtime* runtime,
    const trevrpc_rpc_event* event,
    trevrpc_rpc_event_info_v1* out_info,
    char** out_service,
    char** out_method) {
    int result = trevrpc_rpc_event_info_v1_init(out_info, sizeof(*out_info));
    if (result != 0) {
        return result;
    }
    result = trevrpc_rpc_event_get_info_v1(event, out_info);
    if (result != 0) {
        return result;
    }
    if (out_info->sequence <= runtime->last_event_sequence) {
        return -EILSEQ;
    }
    runtime->last_event_sequence = out_info->sequence;
    if (out_info->service_len != 0) {
        if (out_info->service == NULL) {
            return -EPROTO;
        }
        *out_service = malloc((size_t)out_info->service_len + 1u);
        if (*out_service == NULL) {
            return -ENOMEM;
        }
        memcpy(*out_service, out_info->service, out_info->service_len);
        (*out_service)[out_info->service_len] = '\0';
    }
    if (out_info->method_len != 0) {
        if (out_info->method == NULL) {
            free(*out_service);
            *out_service = NULL;
            return -EPROTO;
        }
        *out_method = malloc((size_t)out_info->method_len + 1u);
        if (*out_method == NULL) {
            free(*out_service);
            *out_service = NULL;
            return -ENOMEM;
        }
        memcpy(*out_method, out_info->method, out_info->method_len);
        (*out_method)[out_info->method_len] = '\0';
    }
    return 0;
}

static bool node_event_kind_known(uint32_t kind) {
    return kind >= TREVRPC_RPC_EVENT_DIAGNOSTIC && kind <= TREVRPC_RPC_EVENT_WEBTRANSPORT_ADMISSION;
}

static bool node_event_releases_subject(uint32_t kind) {
    return kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED || kind == TREVRPC_RPC_EVENT_ENDPOINT_FAILED ||
           kind == TREVRPC_RPC_EVENT_CALL_CLOSED || kind == TREVRPC_RPC_EVENT_CALL_FAILED ||
           kind == TREVRPC_RPC_EVENT_STREAM_CLOSED;
}

static int node_route_cleanup_event(node_runtime* runtime, const trevrpc_rpc_event_info_v1* info) {
    if (!node_event_kind_known(info->kind)) {
        return -EPROTO;
    }
    if (info->kind == TREVRPC_RPC_EVENT_STOPPED) {
        runtime->stopped_seen = true;
        runtime->state = NODE_LIFECYCLE_STOPPED;
        node_test_trace_runtime(runtime, "stopped");
        return 0;
    }
    node_subject* subject = node_subject_from_event(runtime, info);
    if (subject == NULL) {
        if (info->operation_id != TREVRPC_RPC_OPERATION_ID_NONE) {
            node_operation* operation = node_operation_find(runtime, info->operation_id);
            if (operation != NULL) {
                node_operation_remove(runtime, operation);
            }
            return 0;
        }
        return -ESTALE;
    }
    if ((info->flags & TREVRPC_RPC_EVENT_FLAG_TERMINAL) != 0 && node_event_releases_subject(info->kind)) {
        if (subject->terminal_event_seen) {
            return -EALREADY;
        }
        subject->terminal_event_seen = true;
    }
    if (info->operation_id != TREVRPC_RPC_OPERATION_ID_NONE) {
        node_operation* operation = node_operation_find(runtime, info->operation_id);
        if (operation != NULL) {
            if ((operation->action == NODE_OPERATION_SEND || operation->action == NODE_OPERATION_FINISH_SEND) &&
                operation->context != NULL) {
                node_send_batch* batch = operation->context;
                node_call* call = batch->call;
                if (call != NULL) {
                    if (call->send_inflight == batch) {
                        call->send_inflight = NULL;
                    }
                    node_send_batch_remove(call, batch);
                }
                node_send_batch_free(batch);
            }
            node_operation_remove(runtime, operation);
        }
    }
    if (subject->kind == NODE_SUBJECT_ENDPOINT && subject->core != NULL) {
        node_client* client = subject->core;
        if (info->kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED || info->kind == TREVRPC_RPC_EVENT_ENDPOINT_FAILED) {
            client->closed = true;
            client->invalidated = true;
            node_client_maybe_release(client);
            node_runtime_update_liveness(runtime);
        }
    } else if ((subject->kind == NODE_SUBJECT_CALL || subject->kind == NODE_SUBJECT_STREAM) && subject->core != NULL) {
        node_call* call = subject->core;
        if (info->kind == TREVRPC_RPC_EVENT_CALL_CLOSED || info->kind == TREVRPC_RPC_EVENT_CALL_FAILED) {
            call->call_closed = true;
            call->failed = call->failed || info->kind == TREVRPC_RPC_EVENT_CALL_FAILED;
        } else if (info->kind == TREVRPC_RPC_EVENT_STREAM_CLOSED) {
            call->stream_closed = true;
        } else if (info->kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN) {
            call->fin_seen = true;
            call->fin_clean = (info->flags & TREVRPC_RPC_EVENT_FLAG_CLEAN_FIN) != 0;
        }
        node_call_maybe_release(call);
    }
    return 0;
}

static int node_route_event(node_runtime* runtime, trevrpc_rpc_event* event, const trevrpc_rpc_event_info_v1* info) {
    if (runtime->cleanup_started) {
        return node_route_cleanup_event(runtime, info);
    }
    if (!node_event_kind_known(info->kind)) {
        return -EPROTO;
    }
    if (info->kind == TREVRPC_RPC_EVENT_CALL_INCOMING) {
        return event == NULL ? -EPROTO : node_route_incoming_call(runtime, event, info);
    }
    if (info->kind == TREVRPC_RPC_EVENT_HTTP3_ADMISSION || info->kind == TREVRPC_RPC_EVENT_WEBTRANSPORT_ADMISSION) {
        return event == NULL ? -EPROTO : node_route_admission(runtime, event, info);
    }
    if ((info->flags & TREVRPC_RPC_EVENT_FLAG_TERMINAL) != 0 && node_event_releases_subject(info->kind)) {
        node_subject* subject = node_subject_from_event(runtime, info);
        if (subject == NULL) {
            return -ESTALE;
        }
        if (subject->terminal_event_seen) {
            return -EALREADY;
        }
        subject->terminal_event_seen = true;
    }
    if (info->operation_id != TREVRPC_RPC_OPERATION_ID_NONE) {
        node_operation* operation = node_operation_find(runtime, info->operation_id);
        if (operation == NULL || operation->completion_seen) {
            return -EALREADY;
        }
        if (operation->subject_kind != info->subject_kind) {
            return -EPROTO;
        }
        node_handle_key event_key = {0, 0, 0};
        if (operation->subject_kind != TREVRPC_RPC_OBJECT_NONE && operation->subject_bound &&
            (!node_event_key(info, &event_key) || !node_key_equal(operation->subject, event_key))) {
            return -EPROTO;
        }
        operation->completion_seen = true;
        int completion_result = node_complete_operation(runtime, operation, info);
        if (operation->subject_kind == TREVRPC_RPC_OBJECT_CANCELLATION) {
            node_subject* subject = node_subject_from_event(runtime, info);
            if (subject != NULL) {
                subject->cancel_completed = true;
                if (subject->wrapper != NULL) {
                    subject->wrapper->cancel_completed = true;
                }
                node_runtime_update_liveness(runtime);
            }
        }
        node_operation_remove(runtime, operation);
        if (completion_result != 0) {
            return completion_result;
        }
    }
    if (info->kind == TREVRPC_RPC_EVENT_STOPPED) {
        runtime->stopped_seen = true;
        runtime->state = NODE_LIFECYCLE_STOPPED;
        node_test_trace_runtime(runtime, "stopped");
        return 0;
    }
    node_subject* event_subject = node_subject_from_event(runtime, info);
    if (event_subject != NULL && event_subject->core != NULL && event_subject->kind == NODE_SUBJECT_ENDPOINT) {
        node_client* client = event_subject->core;
        if (info->kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED || info->kind == TREVRPC_RPC_EVENT_ENDPOINT_FAILED) {
            client->closed = true;
            if (client->closed_deferred != NULL) {
                node_resolve_undefined(runtime->env, client->closed_deferred);
                client->closed_deferred = NULL;
                node_runtime_update_liveness(runtime);
            }
            node_client_maybe_release(client);
            node_runtime_update_liveness(runtime);
        }
    } else if (event_subject != NULL && event_subject->kind == NODE_SUBJECT_CANCELLATION) {
        for (node_client* client = event_subject->client_head; client != NULL; client = client->cancellation_next) {
            if (!client->ready && client->runtime == runtime && !client->closed) {
                client->invalidated = true;
                for (node_operation* pending = runtime->operations; pending != NULL; pending = pending->next) {
                    if (pending->context == client && pending->action == NODE_OPERATION_ENDPOINT_START &&
                        pending->has_deferred) {
                        (void)node_reject_deferred_native(runtime->env, pending->deferred, -ECANCELED, "connectMsQuic");
                        pending->has_deferred = false;
                        pending->deferred = NULL;
                        break;
                    }
                }
                (void)node_client_request_close(client);
            }
        }
    } else if (event_subject != NULL && event_subject->core != NULL &&
               (event_subject->kind == NODE_SUBJECT_CALL || event_subject->kind == NODE_SUBJECT_STREAM)) {
        node_call* call = event_subject->core;
        if (info->kind == TREVRPC_RPC_EVENT_CALL_CLOSED || info->kind == TREVRPC_RPC_EVENT_CALL_FAILED) {
            call->call_closed = true;
            call->failed = call->failed || info->kind == TREVRPC_RPC_EVENT_CALL_FAILED;
        } else if (info->kind == TREVRPC_RPC_EVENT_STREAM_CLOSED) {
            call->stream_closed = true;
        } else if (info->kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN) {
            call->fin_seen = true;
            call->fin_clean = (info->flags & TREVRPC_RPC_EVENT_FLAG_CLEAN_FIN) != 0;
            if (call->server_side) {
                call->request_finished = true;
            }
        }
        if (info->kind == TREVRPC_RPC_EVENT_STREAM_CLOSED &&
            (call->cancellation.owner != 0 || call->cancellation.slot != 0 || call->cancellation.generation != 0)) {
            node_subject* cancellation_subject =
                node_registry_find(&runtime->cancellations, node_key_from_cancellation(call->cancellation));
            if (cancellation_subject != NULL && cancellation_subject->cancel_submitted) {
                node_call_settle_terminal(call, -ECANCELED, "RPC call cancelled");
            }
        }
        node_process_call(call);
        if (info->kind == TREVRPC_RPC_EVENT_CALL_FAILED) {
            node_call_delete_wrapper_ref(call);
            node_call_settle_terminal(call, -EIO, "RPC call failed");
        } else if (info->kind == TREVRPC_RPC_EVENT_CALL_CLOSED || info->kind == TREVRPC_RPC_EVENT_STREAM_CLOSED) {
            node_call_delete_wrapper_ref(call);
            /* Clean terminal events can race buffered response delivery. Preserve
             * queued messages and status; synthesize EPIPE only when there is no
             * authoritative terminal status left to deliver. */
            if (call->kind == TREVRPC_RPC_KIND_UNARY && call->receive_head == NULL &&
                !(call->fin_seen && call->status_seen)) {
                node_call_settle_terminal(
                    call, -EPIPE, info->kind == TREVRPC_RPC_EVENT_STREAM_CLOSED ? "stream closed" : "RPC call closed");
            }
        }
        node_call_maybe_release(call);
    }
    return 0;
}

static void node_runtime_retry_call_cleanup(node_runtime* runtime) {
    node_subject* endpoint_subject = runtime->endpoints.head;
    while (endpoint_subject != NULL) {
        node_subject* next = endpoint_subject->next;
        node_client* client = endpoint_subject->core;
        if (client != NULL) {
            if (client->close_retry_pending) {
                (void)node_client_request_close(client);
                /* An already-closed endpoint may be released by the request
                 * itself, including freeing a wrapperless client. */
                endpoint_subject = next;
                continue;
            }
            if (client->release_retry_pending || client->closed) {
                node_client_maybe_release(client);
            }
        }
        endpoint_subject = next;
    }
    node_subject* subject = runtime->calls.head;
    while (subject != NULL) {
        node_subject* next = subject->next;
        node_call* call = subject->core;
        if (call != NULL) {
            if (call->close_retry_pending || call->stream_close_retry_pending) {
                node_call_request_close(call);
            }
            node_call_maybe_release(call);
        }
        subject = next;
    }
    subject = runtime->streams.head;
    while (subject != NULL) {
        node_subject* next = subject->next;
        node_call* call = subject->core;
        if (call != NULL) {
            if (call->close_retry_pending || call->stream_close_retry_pending) {
                node_call_request_close(call);
            }
            node_call_maybe_release(call);
        }
        subject = next;
    }
}

static bool node_runtime_has_pending_progress(node_runtime* runtime) {
    if (runtime == NULL) {
        return false;
    }
    if (runtime->cleanup_started && !runtime->close_submitted) {
        return true;
    }
    if (runtime->close_retry_pending || runtime->runtime_release_retry_pending) {
        return true;
    }
    for (node_subject* subject = runtime->endpoints.head; subject != NULL; subject = subject->next) {
        node_client* client = subject->core;
        if (client != NULL && (client->close_retry_pending || client->release_retry_pending)) {
            return true;
        }
    }
    for (node_subject* subject = runtime->calls.head; subject != NULL; subject = subject->next) {
        node_call* call = subject->core;
        if (call != NULL && (call->close_retry_pending || call->stream_close_retry_pending ||
                                call->call_release_retry_pending || call->stream_release_retry_pending)) {
            return true;
        }
    }
    for (node_subject* subject = runtime->cancellations.head; subject != NULL; subject = subject->next) {
        if (subject->release_retry_pending || subject->release_requested ||
            (subject->cancel_submitted && !subject->cancel_completed)) {
            return true;
        }
    }
    return false;
}

static bool node_runtime_needs_liveness(const node_runtime* runtime) {
    if (runtime == NULL || runtime->rpc == NULL || runtime->state == NODE_LIFECYCLE_RELEASED) {
        return false;
    }
    if (runtime->environment_teardown || runtime->operations != NULL || runtime->close_retry_pending ||
        runtime->runtime_release_retry_pending) {
        return true;
    }
    for (node_subject* subject = runtime->endpoints.head; subject != NULL; subject = subject->next) {
        const node_client* client = subject->core;
        if (client != NULL && ((client->ready && !client->closed) || client->closed_deferred != NULL ||
                                  client->close_retry_pending || client->release_retry_pending)) {
            return true;
        }
    }
    for (node_subject* subject = runtime->calls.head; subject != NULL; subject = subject->next) {
        const node_call* call = subject->core;
        if (call == NULL || call->runtime != runtime) {
            continue;
        }
        bool server_is_closing = call->server != NULL && (call->server->closing || call->server->closed);
        bool terminal_keeps_runtime_live = !(call->server_side && call->response_settled && server_is_closing) &&
                                           (!call->call_closed || !call->stream_closed);
        if (terminal_keeps_runtime_live || call->has_response_deferred || call->waiter_head != NULL ||
            call->send_inflight != NULL || call->close_retry_pending || call->stream_close_retry_pending ||
            call->call_release_retry_pending || call->stream_release_retry_pending ||
            (!call->server_side && call->kind != TREVRPC_RPC_KIND_UNARY &&
                !(call->receive_finished || call->terminal_settled))) {
            return true;
        }
    }
    for (node_subject* subject = runtime->cancellations.head; subject != NULL; subject = subject->next) {
        if (subject->release_retry_pending || subject->release_requested ||
            (subject->cancel_submitted && !subject->cancel_completed)) {
            return true;
        }
    }
    return false;
}

static void node_runtime_update_liveness(node_runtime* runtime) {
    if (runtime == NULL) {
        return;
    }
    bool needed = node_runtime_needs_liveness(runtime);
    uv_handle_t* desired = NULL;
    if (needed) {
        if (runtime->poll_initialized && !runtime->poll_closing) {
            desired = (uv_handle_t*)&runtime->poll;
        } else if (runtime->failure_progress_initialized && !runtime->failure_progress_closing &&
                   runtime->failure_progress_started) {
            desired = (uv_handle_t*)&runtime->failure_progress;
        }
    }
    if (runtime->liveness_handle == desired) {
        return;
    }
    if (runtime->liveness_handle != NULL) {
        uv_unref(runtime->liveness_handle);
    }
    runtime->liveness_handle = desired;
    if (desired != NULL) {
        uv_ref(desired);
    }
}

static int node_runtime_drain(node_runtime* runtime) {
    for (;;) {
        trevrpc_rpc_event* event = NULL;
        int result = trevrpc_rpc_runtime_next_event(runtime->rpc, &event);
        if (result == -EAGAIN) {
            node_test_trace_runtime(runtime, "drain-eagain");
            return 0;
        }
        if (result != 0 || event == NULL) {
            return result == 0 ? -EPROTO : result;
        }
        trevrpc_rpc_event_info_v1 info;
        char* service = NULL;
        char* method = NULL;
        result = node_copy_event_info(runtime, event, &info, &service, &method);
        if (result == 0) {
            result = node_route_event(runtime, event, &info);
        }
        free(service);
        free(method);
        trevrpc_rpc_event_release(event);
        if (result == 0) {
            node_runtime_retry_call_cleanup(runtime);
        }
        node_runtime_release_pending_cancellations(runtime);
        if (result != 0) {
            return result;
        }
        if (!runtime->test_failure_consumed && node_test_flag("TREVRPC_NODE_FAIL_INVALID_EVENT")) {
            runtime->test_failure_consumed = true;
            trevrpc_rpc_event_info_v1 invalid_info = info;
            invalid_info.kind = UINT32_MAX;
            result = node_route_event(runtime, NULL, &invalid_info);
            if (result == 0) {
                return -EPROTO;
            }
            return result;
        }
    }
}

static void node_runtime_invalidate_wrappers(node_runtime* runtime) {
    if (runtime == NULL) {
        return;
    }
    bool napi_legal = node_runtime_napi_legal(runtime);
    for (node_operation* operation = runtime->operations; operation != NULL; operation = operation->next) {
        if (operation->has_deferred) {
            if (napi_legal) {
                (void)node_reject_deferred_native(runtime->env, operation->deferred, -ESHUTDOWN, "runtime cleanup");
            }
            operation->has_deferred = false;
            operation->deferred = NULL;
        }
    }
    for (node_subject* subject = runtime->endpoints.head; subject != NULL; subject = subject->next) {
        node_client* client = subject->core;
        if (client == NULL) {
            continue;
        }
        client->invalidated = true;
        if (napi_legal) {
            node_client_delete_wrapper_ref(client);
        }
        if (client->closed_deferred != NULL) {
            if (napi_legal) {
                (void)node_reject_deferred_native(runtime->env, client->closed_deferred, -ESHUTDOWN, "client.close");
            }
            client->closed_deferred = NULL;
        }
    }
    for (node_subject* subject = runtime->calls.head; subject != NULL; subject = subject->next) {
        node_call* call = subject->core;
        if (call == NULL || call->runtime != runtime) {
            continue;
        }
        call->invalidated = true;
        call->close_abort = true;
        node_call_settle_terminal(call, -ESHUTDOWN, "RPC call");
    }
    for (node_subject* subject = runtime->cancellations.head; subject != NULL; subject = subject->next) {
        while (subject->client_head != NULL) {
            node_client_detach_cancellation(subject->client_head);
        }
        if (subject->wrapper != NULL) {
            subject->wrapper->runtime = NULL;
            subject->wrapper->subject = NULL;
            subject->wrapper->wrapper_alive = false;
            subject->wrapper = NULL;
        }
        subject->release_requested = true;
        if (!subject->cancel_submitted) {
            subject->cancel_completed = true;
        }
    }
    node_runtime_release_pending_cancellations(runtime);
}

static void node_runtime_controlled_shutdown(node_runtime* runtime) {
    runtime->impossible_event = true;
    node_runtime_begin_cleanup(runtime);
    node_runtime_start_failure_progress(runtime);
}

static bool node_runtime_claim_cleanup(node_runtime* runtime, node_cleanup_owner owner) {
    if (runtime == NULL || runtime->cleanup_state == NODE_CLEANUP_COMPLETE) {
        return false;
    }
    if (runtime->cleanup_state == NODE_CLEANUP_CLAIMED) {
        return runtime->cleanup_owner == owner;
    }
    runtime->cleanup_owner = owner;
    runtime->cleanup_state = NODE_CLEANUP_CLAIMED;
    runtime->environment_teardown = true;
    node_test_trace_runtime(
        runtime, owner == NODE_CLEANUP_OWNER_ASYNC_HOOK ? "cleanup-owner-async" : "cleanup-owner-instance");
    return true;
}

static void node_runtime_finalize_free(node_runtime* runtime) {
    if (runtime == NULL || !runtime->poll_closed || runtime->failure_progress_initialized ||
        (runtime->rpc != NULL && !runtime->runtime_released)) {
        return;
    }
    runtime->state = NODE_LIFECYCLE_RELEASED;
    if (runtime->cleanup_state == NODE_CLEANUP_CLAIMED) {
        runtime->cleanup_state = NODE_CLEANUP_COMPLETE;
        node_test_trace_runtime(runtime, "cleanup-complete");
    }
    node_runtime_detach_instance_data(runtime);
    if (runtime->cleanup_hook_registered && runtime->cleanup_hook != NULL) {
        napi_async_cleanup_hook_handle hook = runtime->cleanup_hook;
        runtime->cleanup_hook = NULL;
        runtime->cleanup_hook_registered = false;
        (void)napi_remove_async_cleanup_hook(hook);
    }
    node_runtime_free(runtime);
}

static void node_runtime_failure_progress_closed(uv_handle_t* handle) {
    node_runtime* runtime = handle->data;
    if (runtime == NULL) {
        return;
    }
    runtime->failure_progress_closing = false;
    runtime->failure_progress_initialized = false;
    runtime->failure_progress_started = false;
    node_test_trace_runtime(runtime, "failure-progress-closed");
    napi_env env = runtime->env;
    napi_handle_scope scope = NULL;
    if (node_runtime_napi_legal(runtime)) {
        (void)napi_open_handle_scope(env, &scope);
    }
    node_runtime_finalize_free(runtime);
    if (scope != NULL) {
        (void)napi_close_handle_scope(env, scope);
    }
}

static int node_runtime_try_release(node_runtime* runtime) {
    int result;
    if (runtime == NULL || runtime->rpc == NULL || runtime->runtime_released)
        return 0;
    if (!runtime->stopped_seen)
        return -EAGAIN;
    result = trevrpc_rpc_runtime_drain(runtime->rpc);
    if (result == 0) {
        if (runtime->release_attempts == 0 && node_test_flag("TREVRPC_NODE_FAIL_RUNTIME_RELEASE_FIVE"))
            runtime->runtime_release_failures_remaining = 5u;
        if (runtime->runtime_release_failures_remaining != 0) {
            --runtime->runtime_release_failures_remaining;
            result = -EIO;
            node_test_trace_runtime(runtime, "release-retry");
        } else {
            result = trevrpc_rpc_runtime_release(runtime->rpc);
        }
    }
    ++runtime->release_attempts;
    if (result == 0) {
        runtime->rpc = NULL;
        runtime->runtime_released = true;
        runtime->runtime_release_retry_pending = false;
        node_test_trace_runtime(runtime, "release");
    }
    return result;
}

static void node_runtime_retry_failure_progress(node_runtime* runtime) {
    if (runtime->failure_progress_delay_ms < 1000u) {
        runtime->failure_progress_delay_ms *= 2u;
        if (runtime->failure_progress_delay_ms > 1000u)
            runtime->failure_progress_delay_ms = 1000u;
    }
    node_runtime_start_failure_progress(runtime);
}

static void node_runtime_failure_progress_cb(uv_timer_t* handle) {
    node_runtime* runtime = handle->data;
    if (runtime == NULL)
        return;
    runtime->failure_progress_started = false;
    node_test_trace_runtime(runtime, "failure-progress-pass");
    if (runtime->state == NODE_LIFECYCLE_RELEASED)
        return;
    if (runtime->runtime_release_retry_pending) {
        int result = node_runtime_try_release(runtime);
        if (result == 0) {
            node_runtime_close_failure_progress(runtime);
            return;
        }
        if (runtime->release_attempts >= 16u) {
            napi_fatal_error("TrevRPC native runtime cleanup",
                NAPI_AUTO_LENGTH,
                "runtime release failed after bounded retries",
                NAPI_AUTO_LENGTH);
        }
        node_runtime_retry_failure_progress(runtime);
        return;
    }
    if (runtime->rpc == NULL)
        return;
    if (!runtime->cleanup_started) {
        node_runtime_retry_call_cleanup(runtime);
        node_runtime_release_pending_cancellations(runtime);
    } else {
        if (!runtime->close_submitted) {
            node_runtime_begin_cleanup(runtime);
        }
        int result = node_runtime_drain(runtime);
        if (result != 0 && result != -EAGAIN) {
            runtime->impossible_event = true;
            if (!runtime->close_submitted) {
                node_runtime_begin_cleanup(runtime);
            }
        }
        node_runtime_release_pending_cancellations(runtime);
    }
    if (runtime->stopped_seen && !node_runtime_has_pending_progress(runtime)) {
        node_runtime_poll_close(runtime);
        return;
    }
    if (node_runtime_has_pending_progress(runtime)) {
        if (runtime->failure_progress_delay_ms < 1000u) {
            runtime->failure_progress_delay_ms *= 2u;
            if (runtime->failure_progress_delay_ms > 1000u) {
                runtime->failure_progress_delay_ms = 1000u;
            }
        }
        node_runtime_start_failure_progress(runtime);
    } else {
        runtime->failure_progress_delay_ms = 1u;
    }
}

static void node_runtime_start_failure_progress(node_runtime* runtime) {
    if (runtime == NULL || runtime->loop == NULL || runtime->state == NODE_LIFECYCLE_RELEASED) {
        return;
    }
    if (!runtime->failure_progress_initialized) {
        int result = node_test_flag("TREVRPC_NODE_FAIL_FAILURE_PROGRESS_INIT")
                         ? UV_EIO
                         : uv_timer_init(runtime->loop, &runtime->failure_progress);
        if (result != 0) {
            runtime->impossible_event = true;
            napi_fatal_error("TrevRPC native runtime cleanup",
                NAPI_AUTO_LENGTH,
                "failure cleanup progress handle initialization failed",
                NAPI_AUTO_LENGTH);
            return;
        }
        runtime->failure_progress_initialized = true;
        runtime->failure_progress.data = runtime;
        runtime->failure_progress_delay_ms = 1u;
        uv_unref((uv_handle_t*)&runtime->failure_progress);
        node_test_trace_runtime(runtime, "failure-progress-init");
    }
    if (!runtime->failure_progress_started) {
        int result = node_test_flag("TREVRPC_NODE_FAIL_FAILURE_PROGRESS_START")
                         ? UV_EIO
                         : uv_timer_start(&runtime->failure_progress,
                               node_runtime_failure_progress_cb,
                               runtime->failure_progress_delay_ms,
                               0);
        if (result != 0) {
            runtime->impossible_event = true;
            napi_fatal_error("TrevRPC native runtime cleanup",
                NAPI_AUTO_LENGTH,
                "failure cleanup progress handle start failed",
                NAPI_AUTO_LENGTH);
            return;
        }
        runtime->failure_progress_started = true;
        node_test_trace_runtime(runtime, "failure-progress-start");
    }
    node_runtime_update_liveness(runtime);
}

static void node_runtime_stop_failure_progress(node_runtime* runtime) {
    if (runtime == NULL || !runtime->failure_progress_initialized || runtime->failure_progress_closing) {
        return;
    }
    if (runtime->failure_progress_started) {
        uv_timer_stop(&runtime->failure_progress);
        runtime->failure_progress_started = false;
    }
    node_runtime_update_liveness(runtime);
}

static void node_runtime_close_failure_progress(node_runtime* runtime) {
    if (runtime == NULL || !runtime->failure_progress_initialized || runtime->failure_progress_closing) {
        return;
    }
    node_runtime_stop_failure_progress(runtime);
    runtime->failure_progress_closing = true;
    node_runtime_update_liveness(runtime);
    uv_close((uv_handle_t*)&runtime->failure_progress, node_runtime_failure_progress_closed);
}

static void node_runtime_poll_cb(uv_poll_t* handle, int status, int events) {
    node_runtime* runtime = handle->data;
    if (runtime == NULL || runtime->rpc == NULL || runtime->state == NODE_LIFECYCLE_RELEASED) {
        return;
    }
    if (runtime->environment_teardown) {
        if (!runtime->cleanup_started || !runtime->close_submitted) {
            node_runtime_begin_cleanup(runtime);
        }
        (void)node_runtime_drain(runtime);
        if (runtime->stopped_seen && !node_runtime_has_pending_progress(runtime)) {
            node_runtime_poll_close(runtime);
        }
        return;
    }
    napi_handle_scope scope = NULL;
    (void)napi_open_handle_scope(runtime->env, &scope);
    if (status < 0 || (events & (UV_READABLE | UV_DISCONNECT | UV_PRIORITIZED)) == 0) {
        if (status < 0 || (events & UV_DISCONNECT) != 0) {
            node_runtime_controlled_shutdown(runtime);
        }
        goto done;
    }
    if (runtime->cleanup_started && !runtime->close_submitted) {
        node_runtime_begin_cleanup(runtime);
    }
    int result = node_runtime_drain(runtime);
    if (result != 0) {
        node_runtime_controlled_shutdown(runtime);
        goto done;
    }
    if (runtime->cleanup_started && runtime->stopped_seen && !node_runtime_has_pending_progress(runtime)) {
        node_runtime_poll_close(runtime);
    }
done:
    if (scope != NULL) {
        (void)napi_close_handle_scope(runtime->env, scope);
    }
}

static void node_runtime_poll_closed(uv_handle_t* handle) {
    node_runtime* runtime = handle->data;
    if (runtime == NULL) {
        return;
    }
    runtime->poll_closing = false;
    runtime->poll_initialized = false;
    runtime->poll_closed = true;
    if (runtime->rpc != NULL && !runtime->runtime_released) {
        if (!runtime->stopped_seen && node_runtime_shutdown_sync(runtime) != 0) {
            runtime->impossible_event = true;
            napi_fatal_error("TrevRPC native runtime cleanup",
                NAPI_AUTO_LENGTH,
                "runtime shutdown failed after poll closure",
                NAPI_AUTO_LENGTH);
            return;
        }
        node_runtime_release_pending_cancellations(runtime);
        if (runtime->stopped_seen && !runtime->runtime_released) {
            int result = node_runtime_try_release(runtime);
            if (result != 0) {
                runtime->impossible_event = true;
                runtime->runtime_release_retry_pending = true;
            }
        }
    }
    node_test_trace_runtime(runtime, "poll-closed");
    if (runtime->runtime_release_retry_pending)
        node_runtime_start_failure_progress(runtime);
    else
        node_runtime_close_failure_progress(runtime);
    napi_env env = runtime->env;
    napi_handle_scope scope = NULL;
    if (node_runtime_napi_legal(runtime)) {
        (void)napi_open_handle_scope(env, &scope);
    }
    node_runtime_finalize_free(runtime);
    if (scope != NULL) {
        (void)napi_close_handle_scope(env, scope);
    }
}

static void node_runtime_poll_close(node_runtime* runtime) {
    if (!runtime->poll_initialized || runtime->poll_closing) {
        return;
    }
    runtime->poll_closing = true;
    uv_poll_stop(&runtime->poll);
    node_runtime_update_liveness(runtime);
    uv_close((uv_handle_t*)&runtime->poll, node_runtime_poll_closed);
}

static node_operation* node_runtime_reserve_shutdown_operation(node_runtime* runtime) {
    if (runtime->shutdown_operation_in_use) {
        return &runtime->shutdown_operation;
    }
    memset(&runtime->shutdown_operation, 0, sizeof(runtime->shutdown_operation));
    runtime->shutdown_operation.id = TREV_NODE_INITIAL_OPERATION_ID;
    runtime->shutdown_operation.subject_kind = TREVRPC_RPC_OBJECT_NONE;
    runtime->shutdown_operation_in_use = true;
    runtime->shutdown_operation.next = runtime->operations;
    runtime->operations = &runtime->shutdown_operation;
    return &runtime->shutdown_operation;
}

static int node_runtime_submit_close(node_runtime* runtime) {
    if (runtime->rpc == NULL || runtime->close_submitted) {
        return 0;
    }
    node_operation* operation = node_runtime_reserve_shutdown_operation(runtime);
    if (operation == NULL) {
        return -ENOMEM;
    }
    int result;
    if (node_test_flag("TREVRPC_NODE_FAIL_CLOSE_SUBMISSION")) {
        node_test_trace_runtime(runtime, "close-admission-retry");
        result = -EIO;
    } else {
        if (!runtime->close_submission_failure_injected && node_test_flag("TREVRPC_NODE_FAIL_CLOSE_SUBMISSION_FIVE")) {
            runtime->close_submission_failure_injected = true;
            runtime->close_submission_failures_remaining = 5u;
        }
        if (runtime->close_submission_failures_remaining > 0) {
            runtime->close_submission_failures_remaining--;
            node_test_trace_runtime(runtime, "close-admission-retry");
            result = -EIO;
        } else if (!runtime->close_submission_failure_injected &&
                   node_test_flag("TREVRPC_NODE_FAIL_CLOSE_SUBMISSION_ONCE")) {
            runtime->close_submission_failure_injected = true;
            result = -EIO;
        } else {
            result = trevrpc_rpc_runtime_close(runtime->rpc, operation->id);
        }
    }
    runtime->close_attempts++;
    if (result == 0 || result == -EALREADY) {
        runtime->close_submitted = true;
        if (result == -EALREADY) {
            node_operation_remove(runtime, operation);
        }
        node_test_trace_runtime(runtime, "close-submitted");
        return 0;
    }
    node_operation_remove(runtime, operation);
    return result;
}

static void node_runtime_begin_cleanup(node_runtime* runtime) {
    if (runtime == NULL || runtime->rpc == NULL) {
        return;
    }
    if (!runtime->cleanup_started) {
        runtime->state = NODE_LIFECYCLE_CLOSING;
        node_runtime_invalidate_wrappers(runtime);
        runtime->cleanup_started = true;
    }
    if (!runtime->close_submitted) {
        int result = node_runtime_submit_close(runtime);
        if (result != 0) {
            runtime->close_retry_pending = true;
            node_runtime_start_failure_progress(runtime);
            uint32_t close_attempt_limit = node_test_flag("TREVRPC_NODE_FAIL_CLOSE_SUBMISSION_FIVE") ? 8u : 3u;
            if (runtime->close_attempts >= close_attempt_limit) {
                node_test_trace_runtime(runtime, "close-failed-bounded");
                napi_fatal_error("TrevRPC native runtime cleanup",
                    NAPI_AUTO_LENGTH,
                    "runtime_close admission failed after bounded retries",
                    NAPI_AUTO_LENGTH);
                return;
            }
            return;
        }
        runtime->close_retry_pending = false;
    }
    int result = node_runtime_drain(runtime);
    if (result != 0 && result != -EAGAIN) {
        runtime->impossible_event = true;
        node_runtime_start_failure_progress(runtime);
    }
    node_runtime_release_pending_cancellations(runtime);
    if (runtime->stopped_seen && !node_runtime_has_pending_progress(runtime)) {
        node_runtime_poll_close(runtime);
    }
    node_runtime_update_liveness(runtime);
}

static int node_runtime_shutdown_sync(node_runtime* runtime) {
    if (runtime->rpc == NULL) {
        return 0;
    }
    for (unsigned attempt = 0; attempt < 3u && !runtime->close_submitted; ++attempt) {
        if (node_runtime_submit_close(runtime) == 0) {
            break;
        }
    }
    if (!runtime->close_submitted) {
        return -EIO;
    }
    struct timespec start;
    if (clock_gettime(CLOCK_MONOTONIC, &start) != 0) {
        return -EIO;
    }
    for (;;) {
        int result = node_runtime_drain(runtime);
        if (result != 0 && result != -EAGAIN) {
            return result;
        }
        if (runtime->stopped_seen) {
            result = node_runtime_try_release(runtime);
            if (result == 0)
                return 0;
            if (runtime->release_attempts >= 16u)
                return result;
        }
        struct timespec now;
        if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
            return -EIO;
        }
        time_t elapsed_seconds = now.tv_sec - start.tv_sec;
        long elapsed_nanos = now.tv_nsec - start.tv_nsec;
        if (elapsed_nanos < 0) {
            elapsed_seconds--;
        }
        if (elapsed_seconds >= 5) {
            return -ETIMEDOUT;
        }
        struct pollfd descriptor = {.fd = (int)runtime->wake.native_handle, .events = POLLIN};
        if (descriptor.fd >= 0) {
            int wait_result = poll(&descriptor, 1, 25);
            if (wait_result < 0 && errno != EINTR) {
                return -errno;
            }
        } else {
            sched_yield();
        }
    }
}

static void node_async_cleanup_hook(napi_async_cleanup_hook_handle handle, void* data) {
    node_runtime* runtime = data;
    if (runtime == NULL) {
        return;
    }
    runtime->cleanup_hook = handle;
    if (!node_runtime_claim_cleanup(runtime, NODE_CLEANUP_OWNER_ASYNC_HOOK)) {
        return;
    }
    node_runtime_begin_cleanup(runtime);
    if (runtime->poll_initialized && !runtime->poll_closing) {
        /* Keep the loop alive until STOPPED is observed.  The borrowed fd
         * remains owned by the RPC runtime; libuv only polls it. */
        uv_ref((uv_handle_t*)&runtime->poll);
    } else if (!runtime->poll_initialized) {
        if (runtime->rpc != NULL && node_runtime_shutdown_sync(runtime) != 0) {
            runtime->impossible_event = true;
            napi_fatal_error("TrevRPC native runtime cleanup",
                NAPI_AUTO_LENGTH,
                "synchronous runtime shutdown failed",
                NAPI_AUTO_LENGTH);
            return;
        }
        runtime->poll_closed = true;
        node_runtime_finalize_free(runtime);
    }
}

static void node_runtime_instance_finalizer(napi_env env, void* data, void* hint) {
    (void)env;
    (void)hint;
    node_runtime_instance_holder* holder = data;
    if (holder == NULL) {
        return;
    }
    node_runtime* runtime = holder->runtime;
    if (runtime != NULL) {
        runtime->instance_holder = NULL;
        runtime->instance_data_registered = false;
    }
    holder->runtime = NULL;
    free(holder);
    if (runtime != NULL && runtime->state != NODE_LIFECYCLE_RELEASED && !runtime->cleanup_hook_registered &&
        node_runtime_claim_cleanup(runtime, NODE_CLEANUP_OWNER_INSTANCE_FINALIZER)) {
        node_runtime_begin_cleanup(runtime);
        if (!runtime->poll_initialized) {
            if (runtime->rpc != NULL && node_runtime_shutdown_sync(runtime) != 0) {
                runtime->impossible_event = true;
                napi_fatal_error("TrevRPC native runtime cleanup",
                    NAPI_AUTO_LENGTH,
                    "synchronous instance-finalizer shutdown failed",
                    NAPI_AUTO_LENGTH);
                return;
            }
            runtime->poll_closed = true;
            node_runtime_finalize_free(runtime);
        }
    }
}

static void node_runtime_detach_instance_data(node_runtime* runtime) {
    if (runtime == NULL || !runtime->instance_data_registered || runtime->environment_teardown) {
        return;
    }
    void* current = NULL;
    if (napi_get_instance_data(runtime->env, &current) == napi_ok && current == runtime->instance_holder) {
        (void)napi_set_instance_data(runtime->env, NULL, NULL, NULL);
    }
    runtime->instance_data_registered = false;
    if (runtime->instance_holder != NULL) {
        runtime->instance_holder->runtime = NULL;
        free(runtime->instance_holder);
        runtime->instance_holder = NULL;
    }
}

static void node_runtime_free(node_runtime* runtime) {
    if (runtime == NULL) {
        return;
    }
    for (node_subject* subject = runtime->calls.head; subject != NULL; subject = subject->next) {
        node_call* call = subject->core;
        if (call == NULL) {
            continue;
        }
        node_call_detach_server(runtime, call);
        call->runtime = NULL;
        node_call_dispose_buffers(call);
        subject->core = NULL;
        if (call->stream_subject != NULL) {
            call->stream_subject->core = NULL;
        }
        if (!call->wrapper_alive) {
            free(call);
        }
    }
    for (node_subject* subject = runtime->endpoints.head; subject != NULL; subject = subject->next) {
        if (subject->core != NULL) {
            node_client* client = subject->core;
            node_client_detach_cancellation(client);
            node_client_detach_server(runtime, client);
            client->runtime = NULL;
        }
    }
    for (node_subject* subject = runtime->streams.head; subject != NULL; subject = subject->next) {
        subject->core = NULL;
    }
    if (runtime->instance_holder != NULL) {
        runtime->instance_holder->runtime = NULL;
        runtime->instance_holder = NULL;
    }
    node_operation* operation = runtime->operations;
    while (operation != NULL) {
        node_operation* next = operation->next;
        if (operation != &runtime->shutdown_operation) {
            free(operation);
        }
        operation = next;
    }
    node_registry_discard_all(&runtime->endpoints);
    node_registry_discard_all(&runtime->calls);
    node_registry_discard_all(&runtime->streams);
    node_registry_discard_all(&runtime->cancellations);
    node_test_trace_runtime(runtime, "free");
    free(runtime);
}

static int node_runtime_fail_construction(node_runtime* runtime, int error_code) {
    runtime->construction_failed = true;
    runtime->state = NODE_LIFECYCLE_CLOSING;
    if (!runtime->cleanup_started) {
        node_runtime_invalidate_wrappers(runtime);
        runtime->cleanup_started = true;
    }
    if (runtime->cleanup_hook_registered && runtime->cleanup_hook != NULL) {
        napi_async_cleanup_hook_handle hook = runtime->cleanup_hook;
        runtime->cleanup_hook = NULL;
        runtime->cleanup_hook_registered = false;
        (void)napi_remove_async_cleanup_hook(hook);
    }
    node_runtime_detach_instance_data(runtime);
    if (runtime->poll_initialized) {
        node_runtime_poll_close(runtime);
        return error_code;
    }
    if (runtime->rpc != NULL) {
        int result = node_runtime_shutdown_sync(runtime);
        if (result != 0) {
            napi_fatal_error("TrevRPC native addon construction",
                NAPI_AUTO_LENGTH,
                "runtime close failed during construction failure",
                NAPI_AUTO_LENGTH);
            return result;
        }
    }
    node_runtime_free(runtime);
    return error_code;
}

static int node_runtime_create(napi_env env, node_runtime** out_runtime) {
    *out_runtime = NULL;
    node_runtime* runtime = calloc(1, sizeof(*runtime));
    if (runtime == NULL) {
        return -ENOMEM;
    }
    runtime->env = env;
#ifdef TREVRPC_NODE_TEST_HOOKS
    runtime->test_runtime_id = atomic_fetch_add_explicit(&node_test_next_runtime_id, 1u, memory_order_relaxed);
#endif
    runtime->next_operation_id = TREV_NODE_INITIAL_OPERATION_ID + 1u;
    runtime->shutdown_operation.id = TREV_NODE_INITIAL_OPERATION_ID;
    runtime->state = NODE_LIFECYCLE_RUNNING;
    int result = napi_get_uv_event_loop(env, &runtime->loop) == napi_ok ? 0 : -EIO;
    if (result != 0) {
        node_runtime_free(runtime);
        return result;
    }
    trevrpc_rpc_runtime_config_v1 config;
    trevrpc_rpc_msquic_config_v1 provider_config;
    result = trevrpc_rpc_runtime_config_v1_init(&config, sizeof(config));
    if (result == 0) {
        result = trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config));
    }
    if (result == 0) {
        trevrpc_rpc_abi_1_anchor();
        trevrpc_rpc_msquic_abi_1_anchor();
        result = trevrpc_rpc_msquic_create_v1(&config, &provider_config, &runtime->rpc);
    }
    if (result != 0) {
        node_runtime_free(runtime);
        return result;
    }
    result = trevrpc_rpc_wake_source_v1_init(&runtime->wake, sizeof(runtime->wake));
    if (result == 0 && node_test_flag("TREVRPC_NODE_FAIL_WAKE_SOURCE")) {
        runtime->wake.kind = TREVRPC_RPC_WAKE_SOURCE_POSIX_FD;
        runtime->wake.flags = TREVRPC_RPC_WAKE_FLAG_BORROWED;
        runtime->wake.native_handle = -1;
        result = -EPROTO;
    }
    if (result == 0) {
        result = trevrpc_rpc_runtime_get_wake_source_v1(runtime->rpc, &runtime->wake);
    }
    if (result != 0 || runtime->wake.kind != TREVRPC_RPC_WAKE_SOURCE_POSIX_FD ||
        (runtime->wake.flags & (TREVRPC_RPC_WAKE_FLAG_BORROWED | TREVRPC_RPC_WAKE_FLAG_LEVEL_TRIGGERED)) !=
            (TREVRPC_RPC_WAKE_FLAG_BORROWED | TREVRPC_RPC_WAKE_FLAG_LEVEL_TRIGGERED) ||
        runtime->wake.native_handle < 0) {
        return node_runtime_fail_construction(runtime, result != 0 ? result : -EPROTO);
    }
    if (node_test_flag("TREVRPC_NODE_FAIL_UV_POLL_INIT")) {
        return node_runtime_fail_construction(runtime, -EIO);
    }
    result = uv_poll_init(runtime->loop, &runtime->poll, (int)runtime->wake.native_handle);
    if (result != 0) {
        return node_runtime_fail_construction(runtime, -result);
    }
    runtime->poll_initialized = true;
    runtime->poll.data = runtime;
    node_test_trace_runtime(runtime, "poll-init");
    if (node_test_flag("TREVRPC_NODE_FAIL_NAPI_INSTANCE_DATA")) {
        return node_runtime_fail_construction(runtime, -EIO);
    }
    runtime->instance_holder = calloc(1, sizeof(*runtime->instance_holder));
    if (runtime->instance_holder == NULL) {
        return node_runtime_fail_construction(runtime, -ENOMEM);
    }
    runtime->instance_holder->runtime = runtime;
    if (napi_set_instance_data(env, runtime->instance_holder, node_runtime_instance_finalizer, NULL) != napi_ok) {
        free(runtime->instance_holder);
        runtime->instance_holder = NULL;
        return node_runtime_fail_construction(runtime, -EIO);
    }
    runtime->instance_data_registered = true;
    if (node_test_flag("TREVRPC_NODE_FAIL_NAPI_CLEANUP_HOOK") ||
        napi_add_async_cleanup_hook(env, node_async_cleanup_hook, runtime, &runtime->cleanup_hook) != napi_ok) {
        return node_runtime_fail_construction(runtime, -EIO);
    }
    runtime->cleanup_hook_registered = true;
    if (node_test_flag("TREVRPC_NODE_FAIL_UV_POLL_START")) {
        runtime->poll_start_failed = true;
        return node_runtime_fail_construction(runtime, -EIO);
    }
    result = uv_poll_start(&runtime->poll, UV_READABLE, node_runtime_poll_cb);
    if (result != 0) {
        runtime->poll_start_failed = true;
        return node_runtime_fail_construction(runtime, -result);
    }
    runtime->poll_started = true;
    node_test_trace_runtime(runtime, "poll-start");
    if (node_test_flag("TREVRPC_NODE_FAIL_CLOSE_SUBMISSION")) {
        node_runtime_controlled_shutdown(runtime);
        for (unsigned attempt = 0; attempt < 3u && !runtime->close_submitted; ++attempt) {
            node_runtime_begin_cleanup(runtime);
        }
    }
    if (node_test_flag("TREVRPC_NODE_FAIL_POLL_ERROR")) {
        uv_poll_stop(&runtime->poll);
        node_runtime_poll_cb(&runtime->poll, UV_EIO, 0);
    }
    /* A lazily-created runtime must not make an otherwise idle Node process
     * immortal.  The poll remains active whenever the loop is running. */
    uv_unref((uv_handle_t*)&runtime->poll);
    *out_runtime = runtime;
    return 0;
}

static int node_runtime_get(napi_env env, node_runtime** out_runtime) {
    node_runtime_instance_holder* holder = NULL;
    napi_status status = napi_get_instance_data(env, (void**)&holder);
    if (status != napi_ok) {
        return -EIO;
    }
    if (holder != NULL && holder->runtime != NULL) {
        *out_runtime = holder->runtime;
        return (*out_runtime)->state == NODE_LIFECYCLE_RUNNING ? 0 : -ESHUTDOWN;
    }
    *out_runtime = NULL;
    return node_runtime_create(env, out_runtime);
}

static napi_value node_throw_error(napi_env env, int error_code, const char* operation) {
    napi_value message;
    napi_value error;
    char text[160];
    int written = snprintf(text, sizeof(text), "%s failed (%d)", operation, error_code);
    if (written < 0 || napi_create_string_utf8(env, text, NAPI_AUTO_LENGTH, &message) != napi_ok ||
        napi_create_error(env, NULL, message, &error) != napi_ok) {
        napi_throw_error(env, NULL, operation);
        return NULL;
    }
    napi_value code;
    if (napi_create_int32(env, error_code, &code) == napi_ok) {
        (void)napi_set_named_property(env, error, "nativeCode", code);
    }
    napi_throw(env, error);
    return NULL;
}

static void node_cancellation_finalizer(napi_env env, void* data, void* hint) {
    (void)env;
    (void)hint;
    node_cancellation* cancellation = data;
    if (cancellation == NULL) {
        return;
    }
    node_runtime* runtime = cancellation->runtime;
    node_subject* subject = cancellation->subject;
    cancellation->wrapper_alive = false;
    if (subject != NULL) {
        subject->wrapper = NULL;
    }
    if (runtime != NULL && runtime->rpc != NULL && subject != NULL) {
        subject->release_requested = true;
        if (!subject->cancel_submitted) {
            subject->cancel_completed = true;
        }
        node_cancellation_try_release(runtime, subject);
        cancellation->subject = NULL;
    }
    cancellation->runtime = NULL;
    cancellation->subject = NULL;
    free(cancellation);
}

static napi_value node_cancellation_cancel(napi_env env, napi_callback_info info) {
    napi_value this_value;
    size_t argc = 0;
    if (napi_get_cb_info(env, info, &argc, NULL, &this_value, NULL) != napi_ok) {
        return node_throw_error(env, -EIO, "cancellation.cancel");
    }
    node_cancellation* cancellation = NULL;
    if (napi_unwrap(env, this_value, (void**)&cancellation) != napi_ok || cancellation == NULL ||
        cancellation->runtime == NULL || cancellation->runtime->rpc == NULL || cancellation->subject == NULL ||
        !cancellation->wrapper_alive) {
        return node_throw_error(env, -ESTALE, "cancellation.cancel");
    }
    if (cancellation->cancel_submitted) {
        napi_value undefined;
        (void)napi_get_undefined(env, &undefined);
        return undefined;
    }
    node_runtime* runtime = cancellation->runtime;
    node_operation* operation =
        node_operation_add(runtime, TREVRPC_RPC_OBJECT_CANCELLATION, cancellation->subject->key);
    if (operation == NULL) {
        return node_throw_error(env, -EOVERFLOW, "cancellation.cancel");
    }
    int result = trevrpc_rpc_cancellation_cancel(runtime->rpc, cancellation->handle, operation->id);
    if (result != 0) {
        node_operation_remove(runtime, operation);
        return node_throw_error(env, result, "cancellation.cancel");
    }
    cancellation->cancel_submitted = true;
    cancellation->subject->cancel_submitted = true;
    napi_value undefined;
    (void)napi_get_undefined(env, &undefined);
    return undefined;
}

static napi_value node_create_cancellation(napi_env env, napi_callback_info info) {
    (void)info;
    node_runtime* runtime = NULL;
    int result = node_runtime_get(env, &runtime);
    if (result != 0 || runtime == NULL || runtime->rpc == NULL) {
        return node_throw_error(env, result != 0 ? result : -EIO, "createCancellation");
    }
    node_cancellation* cancellation = calloc(1, sizeof(*cancellation));
    if (cancellation == NULL) {
        return node_throw_error(env, -ENOMEM, "createCancellation");
    }
    result = trevrpc_rpc_cancellation_create(runtime->rpc, &cancellation->handle);
    if (result != 0) {
        free(cancellation);
        return node_throw_error(env, result, "createCancellation");
    }
    cancellation->runtime = runtime;
    cancellation->subject =
        node_registry_add(runtime, TREVRPC_RPC_OBJECT_CANCELLATION, node_key_from_cancellation(cancellation->handle));
    if (cancellation->subject == NULL) {
        (void)trevrpc_rpc_cancellation_release(runtime->rpc, cancellation->handle);
        free(cancellation);
        return node_throw_error(env, -ENOMEM, "createCancellation");
    }
    cancellation->subject->cancellation = cancellation->handle;
    cancellation->subject->wrapper = cancellation;
    cancellation->subject->cancel_submitted = false;
    cancellation->subject->cancel_completed = false;
    cancellation->wrapper_alive = true;
    napi_value object;
    napi_value cancel;
    if (napi_create_object(env, &object) != napi_ok ||
        napi_create_function(env, "cancel", NAPI_AUTO_LENGTH, node_cancellation_cancel, NULL, &cancel) != napi_ok ||
        napi_set_named_property(env, object, "cancel", cancel) != napi_ok ||
        napi_wrap(env, object, cancellation, node_cancellation_finalizer, NULL, NULL) != napi_ok) {
        cancellation->subject->wrapper = NULL;
        if (trevrpc_rpc_cancellation_release(runtime->rpc, cancellation->handle) == 0) {
            node_registry_remove(runtime, cancellation->subject);
        }
        free(cancellation);
        return node_throw_error(env, -EIO, "createCancellation");
    }
    return object;
}

typedef struct node_js_metadata {
    node_metadata_value* values;
    trevrpc_rpc_metadata_entry_v1* entries;
    size_t count;
} node_js_metadata;

typedef struct node_js_request {
    char* service;
    uint32_t service_len;
    char* method;
    uint32_t method_len;
    uint8_t* body;
    size_t body_len;
    node_js_metadata metadata;
    uint32_t kind;
    uint64_t timeout_nanos;
    int64_t max_response_body_size;
    int64_t max_response_messages;
    int64_t max_response_stream_body_size;
    uint64_t response_idle_timeout_nanos;
    trevrpc_rpc_cancellation_v1 cancellation;
} node_js_request;

typedef struct node_external_receive_owner {
    trevrpc_rpc_receive* receive;
    void* data;
} node_external_receive_owner;

static void node_metadata_free(node_js_metadata* metadata) {
    if (metadata == NULL) {
        return;
    }
    if (metadata->values != NULL) {
        for (size_t index = 0; index < metadata->count; ++index) {
            free(metadata->values[index].key);
            free(metadata->values[index].value);
        }
    }
    free(metadata->values);
    free(metadata->entries);
    memset(metadata, 0, sizeof(*metadata));
}

static void node_request_free(node_js_request* request) {
    if (request == NULL) {
        return;
    }
    free(request->service);
    free(request->method);
    free(request->body);
    node_metadata_free(&request->metadata);
    memset(request, 0, sizeof(*request));
}

static int node_copy_js_string(napi_env env, napi_value value, char** out, uint32_t* out_len) {
    napi_valuetype type;
    if (napi_typeof(env, value, &type) != napi_ok || type != napi_string) {
        return -EINVAL;
    }
    size_t length = 0;
    if (napi_get_value_string_utf8(env, value, NULL, 0, &length) != napi_ok || length > UINT32_MAX) {
        return -EOVERFLOW;
    }
    char* result = malloc(length + 1u);
    if (result == NULL) {
        return -ENOMEM;
    }
    if (napi_get_value_string_utf8(env, value, result, length + 1u, &length) != napi_ok) {
        free(result);
        return -EINVAL;
    }
    result[length] = '\0';
    *out = result;
    *out_len = (uint32_t)length;
    return 0;
}

static int node_copy_js_bytes(napi_env env, napi_value value, uint8_t** out, size_t* out_len) {
    bool is_arraybuffer = false;
    if (napi_is_arraybuffer(env, value, &is_arraybuffer) != napi_ok) {
        return -EINVAL;
    }
    if (is_arraybuffer) {
        void* data = NULL;
        size_t length = 0;
        if (napi_get_arraybuffer_info(env, value, &data, &length) != napi_ok) {
            return -EINVAL;
        }
        uint8_t* copy = length == 0 ? NULL : malloc(length);
        if (length != 0 && copy == NULL) {
            return -ENOMEM;
        }
        if (length != 0) {
            memcpy(copy, data, length);
        }
        *out = copy;
        *out_len = length;
        return 0;
    }
    bool is_typedarray = false;
    if (napi_is_typedarray(env, value, &is_typedarray) != napi_ok) {
        return -EINVAL;
    }
    if (is_typedarray) {
        napi_typedarray_type type;
        size_t length = 0;
        void* data = NULL;
        napi_value arraybuffer = NULL;
        size_t offset = 0;
        if (napi_get_typedarray_info(env, value, &type, &length, &data, &arraybuffer, &offset) != napi_ok ||
            type == napi_bigint64_array || type == napi_biguint64_array) {
            return -EINVAL;
        }
        size_t element_size = 1;
        switch (type) {
        case napi_int16_array:
        case napi_uint16_array:
            element_size = 2;
            break;
        case napi_int32_array:
        case napi_uint32_array:
        case napi_float32_array:
            element_size = 4;
            break;
        case napi_float64_array:
            element_size = 8;
            break;
        default:
            break;
        }
        if (length > SIZE_MAX / element_size) {
            return -EOVERFLOW;
        }
        size_t bytes = length * element_size;
        uint8_t* copy = bytes == 0 ? NULL : malloc(bytes);
        if (bytes != 0 && copy == NULL) {
            return -ENOMEM;
        }
        if (bytes != 0) {
            memcpy(copy, data, bytes);
        }
        *out = copy;
        *out_len = bytes;
        return 0;
    }
    napi_valuetype type;
    if (napi_typeof(env, value, &type) != napi_ok || type != napi_object) {
        return -EINVAL;
    }
    bool is_array = false;
    if (napi_is_array(env, value, &is_array) != napi_ok || !is_array) {
        return -EINVAL;
    }
    uint32_t length = 0;
    if (napi_get_array_length(env, value, &length) != napi_ok) {
        return -EINVAL;
    }
    uint8_t* copy = length == 0 ? NULL : malloc(length);
    if (length != 0 && copy == NULL) {
        return -ENOMEM;
    }
    for (uint32_t index = 0; index < length; ++index) {
        napi_value element;
        int32_t number;
        if (napi_get_element(env, value, index, &element) != napi_ok ||
            napi_get_value_int32(env, element, &number) != napi_ok || number < 0 || number > 255) {
            free(copy);
            return -EINVAL;
        }
        copy[index] = (uint8_t)number;
    }
    *out = copy;
    *out_len = length;
    return 0;
}

static int node_copy_js_metadata(napi_env env, napi_value value, node_js_metadata* out) {
    node_js_metadata parsed;
    memset(&parsed, 0, sizeof(parsed));
    memset(out, 0, sizeof(*out));
    napi_valuetype type;
    if (value == NULL || napi_typeof(env, value, &type) != napi_ok || type == napi_undefined || type == napi_null) {
        return 0;
    }
    if (type != napi_object) {
        return -EINVAL;
    }
    napi_value names;
    uint32_t count = 0;
    if (napi_get_property_names(env, value, &names) != napi_ok ||
        napi_get_array_length(env, names, &count) != napi_ok) {
        return -EINVAL;
    }
    if (count == 0) {
        return 0;
    }
    parsed.values = calloc(count, sizeof(*parsed.values));
    parsed.entries = calloc(count, sizeof(*parsed.entries));
    if (parsed.values == NULL || parsed.entries == NULL) {
        node_metadata_free(&parsed);
        return -ENOMEM;
    }
    for (uint32_t index = 0; index < count; ++index) {
        napi_value key;
        napi_value item;
        uint32_t key_len = 0;
        if (napi_get_element(env, names, index, &key) != napi_ok ||
            node_copy_js_string(env, key, &parsed.values[index].key, &key_len) != 0 ||
            napi_get_property(env, value, key, &item) != napi_ok) {
            node_metadata_free(&parsed);
            return -EINVAL;
        }
        int result = node_copy_js_bytes(env, item, &parsed.values[index].value, &parsed.values[index].value_len);
        if (result != 0) {
            node_metadata_free(&parsed);
            return result;
        }
        parsed.entries[index].key = parsed.values[index].key;
        parsed.entries[index].key_len = key_len;
        parsed.entries[index].value = parsed.values[index].value;
        parsed.entries[index].value_len = parsed.values[index].value_len;
        parsed.count++;
    }
    *out = parsed;
    return 0;
}

static int node_get_named_value(napi_env env, napi_value object, const char* name, napi_value* out, bool* present) {
    bool has = false;
    if (napi_has_named_property(env, object, name, &has) != napi_ok) {
        return -EINVAL;
    }
    *present = has;
    if (!has) {
        *out = NULL;
        return 0;
    }
    return napi_get_named_property(env, object, name, out) == napi_ok ? 0 : -EINVAL;
}

static int node_get_named_u64(napi_env env, napi_value object, const char* name, uint64_t* out) {
    napi_value value;
    bool present;
    int result = node_get_named_value(env, object, name, &value, &present);
    if (result != 0 || !present) {
        return result;
    }
    double number;
    if (napi_get_value_double(env, value, &number) != napi_ok || number < 0 || number > (double)UINT64_MAX) {
        return -EINVAL;
    }
    *out = (uint64_t)number;
    return 0;
}

static int node_get_named_i64(napi_env env, napi_value object, const char* name, int64_t* out) {
    napi_value value;
    bool present;
    int result = node_get_named_value(env, object, name, &value, &present);
    if (result != 0 || !present) {
        return result;
    }
    double number;
    if (napi_get_value_double(env, value, &number) != napi_ok || number < (double)INT64_MIN ||
        number > (double)INT64_MAX) {
        return -EINVAL;
    }
    *out = (int64_t)number;
    return 0;
}

static int node_get_named_bool(napi_env env, napi_value object, const char* name, bool* out, bool* present) {
    napi_value value;
    int result = node_get_named_value(env, object, name, &value, present);
    if (result != 0 || !*present) {
        return result;
    }
    return napi_get_value_bool(env, value, out) == napi_ok ? 0 : -EINVAL;
}

static int node_parse_request(napi_env env, napi_value value, uint32_t default_kind, node_js_request* out) {
    memset(out, 0, sizeof(*out));
    napi_valuetype type;
    if (napi_typeof(env, value, &type) != napi_ok || type != napi_object) {
        return -EINVAL;
    }
    napi_value item;
    bool present;
    if (node_get_named_value(env, value, "service", &item, &present) != 0 || !present ||
        node_copy_js_string(env, item, &out->service, &out->service_len) != 0 || out->service_len == 0 ||
        node_get_named_value(env, value, "method", &item, &present) != 0 || !present ||
        node_copy_js_string(env, item, &out->method, &out->method_len) != 0 || out->method_len == 0) {
        node_request_free(out);
        return -EINVAL;
    }
    if (node_get_named_value(env, value, "body", &item, &present) != 0) {
        node_request_free(out);
        return -EINVAL;
    }
    if (present && node_copy_js_bytes(env, item, &out->body, &out->body_len) != 0) {
        node_request_free(out);
        return -EINVAL;
    }
    if (node_get_named_value(env, value, "metadata", &item, &present) != 0 ||
        (present && node_copy_js_metadata(env, item, &out->metadata) != 0)) {
        node_request_free(out);
        return -EINVAL;
    }
    out->kind = default_kind;
    if (node_get_named_value(env, value, "kind", &item, &present) != 0) {
        node_request_free(out);
        return -EINVAL;
    }
    if (present) {
        uint32_t kind;
        if (napi_get_value_uint32(env, item, &kind) != napi_ok || kind > TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
            node_request_free(out);
            return -EINVAL;
        }
        out->kind = kind;
    }
    out->timeout_nanos = TREVRPC_RPC_DEADLINE_INFINITE;
    out->max_response_body_size = (int64_t)TREVRPC_RPC_DEFAULT_MAX_MESSAGE_SIZE;
    out->max_response_messages = 4096;
    out->max_response_stream_body_size = 16 * 1024 * 1024;
    (void)node_get_named_u64(env, value, "timeoutNanos", &out->timeout_nanos);
    (void)node_get_named_i64(env, value, "maxResponseBodySize", &out->max_response_body_size);
    (void)node_get_named_i64(env, value, "maxResponseMessages", &out->max_response_messages);
    (void)node_get_named_i64(env, value, "maxResponseStreamBodySize", &out->max_response_stream_body_size);
    (void)node_get_named_u64(env, value, "responseIdleTimeoutNanos", &out->response_idle_timeout_nanos);
    return 0;
}

static void node_external_receive_finalizer(napi_env env, void* data, void* hint) {
    (void)env;
    (void)data;
    node_external_receive_owner* owner = hint;
    if (owner != NULL) {
        if (owner->receive != NULL) {
            trevrpc_rpc_receive_release(owner->receive);
        }
        free(owner->data);
        free(owner);
    }
}

static int node_copy_receive_status(const trevrpc_rpc_receive_info_v1* info, node_status_value* out) {
    memset(out, 0, sizeof(*out));
    out->code = info->rpc_status;
    out->message = calloc((size_t)info->message_len + 1u, 1u);
    if (out->message == NULL) {
        return -ENOMEM;
    }
    if (info->message_len != 0) {
        if (info->message == NULL) {
            free(out->message);
            out->message = NULL;
            return -EPROTO;
        }
        memcpy(out->message, info->message, info->message_len);
    }
    if (info->metadata_count != 0) {
        if (info->metadata == NULL) {
            free(out->message);
            out->message = NULL;
            return -EPROTO;
        }
        out->metadata = calloc(info->metadata_count, sizeof(*out->metadata));
        if (out->metadata == NULL) {
            free(out->message);
            out->message = NULL;
            return -ENOMEM;
        }
        for (uint32_t index = 0; index < info->metadata_count; ++index) {
            const trevrpc_rpc_metadata_entry_v1* entry = &info->metadata[index];
            out->metadata[index].key = calloc((size_t)entry->key_len + 1u, 1u);
            out->metadata[index].value = entry->value_len == 0 ? NULL : malloc((size_t)entry->value_len);
            if (out->metadata[index].key == NULL || (entry->value_len != 0 && out->metadata[index].value == NULL) ||
                (entry->key_len != 0 && entry->key == NULL) || (entry->value_len != 0 && entry->value == NULL)) {
                for (uint32_t cleanup = 0; cleanup <= index; ++cleanup) {
                    free(out->metadata[cleanup].key);
                    free(out->metadata[cleanup].value);
                }
                free(out->metadata);
                free(out->message);
                memset(out, 0, sizeof(*out));
                return -ENOMEM;
            }
            memcpy(out->metadata[index].key, entry->key, entry->key_len);
            if (entry->value_len != 0) {
                memcpy(out->metadata[index].value, entry->value, (size_t)entry->value_len);
            }
            out->metadata[index].value_len = (size_t)entry->value_len;
            out->metadata_count++;
        }
    }
    return 0;
}

static void node_status_free(node_status_value* status) {
    if (status == NULL) {
        return;
    }
    free(status->message);
    for (size_t index = 0; index < status->metadata_count; ++index) {
        free(status->metadata[index].key);
        free(status->metadata[index].value);
    }
    free(status->metadata);
    memset(status, 0, sizeof(*status));
}

static int node_make_bytes_value(napi_env env, const uint8_t* data, size_t length, napi_value* out) {
    napi_value arraybuffer;
    void* storage = NULL;
    if (napi_create_arraybuffer(env, length, &storage, &arraybuffer) != napi_ok) {
        return -ENOMEM;
    }
    if (length != 0 && data != NULL) {
        memcpy(storage, data, length);
    }
    return napi_create_typedarray(env, napi_uint8_array, length, arraybuffer, 0, out) == napi_ok ? 0 : -ENOMEM;
}

static int node_make_metadata_value(napi_env env, const node_metadata_value* metadata, size_t count, napi_value* out) {
    if (napi_create_object(env, out) != napi_ok) {
        return -ENOMEM;
    }
    for (size_t index = 0; index < count; ++index) {
        napi_value key;
        napi_value value;
        if (napi_create_string_utf8(env, metadata[index].key, NAPI_AUTO_LENGTH, &key) != napi_ok ||
            node_make_bytes_value(env, metadata[index].value, metadata[index].value_len, &value) != 0 ||
            napi_set_property(env, *out, key, value) != napi_ok) {
            return -ENOMEM;
        }
    }
    return 0;
}

static int node_make_metadata_entries_value(
    napi_env env, const trevrpc_rpc_metadata_entry_v1* entries, uint32_t count, napi_value* out) {
    if (napi_create_object(env, out) != napi_ok) {
        return -ENOMEM;
    }
    for (uint32_t index = 0; index < count; ++index) {
        const trevrpc_rpc_metadata_entry_v1* entry = &entries[index];
        napi_value key;
        napi_value value;
        char* name = calloc((size_t)entry->key_len + 1u, 1u);
        if (name == NULL) {
            return -ENOMEM;
        }
        if (entry->key_len != 0 && entry->key == NULL) {
            free(name);
            return -EPROTO;
        }
        memcpy(name, entry->key, entry->key_len);
        int result = node_make_bytes_value(env, entry->value, (size_t)entry->value_len, &value);
        if (result == 0 && napi_create_string_utf8(env, name, NAPI_AUTO_LENGTH, &key) != napi_ok) {
            result = -ENOMEM;
        }
        free(name);
        if (result != 0 || napi_set_property(env, *out, key, value) != napi_ok) {
            return result == 0 ? -ENOMEM : result;
        }
    }
    return 0;
}

static int node_make_status_object(napi_env env, const node_status_value* status, napi_value* out) {
    napi_value code;
    napi_value message;
    napi_value metadata;
    if (napi_create_object(env, out) != napi_ok || napi_create_uint32(env, status->code, &code) != napi_ok ||
        napi_create_string_utf8(env, status->message == NULL ? "" : status->message, NAPI_AUTO_LENGTH, &message) !=
            napi_ok ||
        node_make_metadata_value(env, status->metadata, status->metadata_count, &metadata) != 0 ||
        napi_set_named_property(env, *out, "status", code) != napi_ok ||
        napi_set_named_property(env, *out, "message", message) != napi_ok ||
        napi_set_named_property(env, *out, "metadata", metadata) != napi_ok) {
        return -ENOMEM;
    }
    return 0;
}

static int node_reject_deferred_native(napi_env env, napi_deferred deferred, int error_code, const char* operation) {
    napi_value message;
    napi_value error;
    char text[192];
    int written = snprintf(text, sizeof(text), "%s failed (%d)", operation, error_code);
    if (written < 0 || napi_create_string_utf8(env, text, NAPI_AUTO_LENGTH, &message) != napi_ok ||
        napi_create_error(env, NULL, message, &error) != napi_ok) {
        return -EIO;
    }
    napi_value native_code;
    if (napi_create_int32(env, error_code, &native_code) == napi_ok) {
        (void)napi_set_named_property(env, error, "nativeCode", native_code);
    }
    (void)napi_reject_deferred(env, deferred, error);
    return 0;
}

static void node_resolve_undefined(napi_env env, napi_deferred deferred) {
    napi_value undefined;
    if (napi_get_undefined(env, &undefined) == napi_ok) {
        (void)napi_resolve_deferred(env, deferred, undefined);
    }
}

static bool node_handle_is_null_endpoint(trevrpc_rpc_endpoint_v1 value) {
    return value.owner == 0 && value.slot == 0 && value.generation == 0;
}

static bool node_handle_is_null_call(trevrpc_rpc_call_v1 value) {
    return value.owner == 0 && value.slot == 0 && value.generation == 0;
}

static bool node_handle_is_null_stream(trevrpc_rpc_stream_v1 value) {
    return value.owner == 0 && value.slot == 0 && value.generation == 0;
}

typedef struct node_endpoint_config_storage {
    trevrpc_rpc_msquic_endpoint_config_v1 config;
    char* host;
    char* server_name;
    char* ca_cert_file;
    char* path;
    char* origin;
} node_endpoint_config_storage;

static void node_endpoint_config_free(node_endpoint_config_storage* storage) {
    free(storage->host);
    free(storage->server_name);
    free(storage->ca_cert_file);
    free(storage->path);
    free(storage->origin);
    memset(storage, 0, sizeof(*storage));
}

static int node_parse_endpoint_config(
    napi_env env, napi_value options, node_endpoint_config_storage* out, bool allow_zero_port) {
    memset(out, 0, sizeof(*out));
    napi_valuetype type;
    if (napi_typeof(env, options, &type) != napi_ok || type != napi_object) {
        return -EINVAL;
    }
    napi_value value;
    bool present;
    if (node_get_named_value(env, options, "host", &value, &present) != 0 || !present ||
        node_copy_js_string(env, value, &out->host, &(uint32_t){0}) != 0) {
        node_endpoint_config_free(out);
        return -EINVAL;
    }
    uint32_t host_len = (uint32_t)strlen(out->host);
    if (host_len == 0 || host_len > UINT32_MAX) {
        node_endpoint_config_free(out);
        return -EINVAL;
    }
    uint64_t port = 0;
    if (node_get_named_u64(env, options, "port", &port) != 0 || (!allow_zero_port && port == 0) || port > 65535) {
        node_endpoint_config_free(out);
        return -EINVAL;
    }
    if (node_get_named_value(env, options, "serverName", &value, &present) != 0 ||
        (present && node_copy_js_string(env, value, &out->server_name, &(uint32_t){0}) != 0)) {
        node_endpoint_config_free(out);
        return -EINVAL;
    }
    if (out->server_name == NULL) {
        out->server_name = strdup(out->host);
        if (out->server_name == NULL) {
            node_endpoint_config_free(out);
            return -ENOMEM;
        }
    }
    if (node_get_named_value(env, options, "caCertFile", &value, &present) != 0 ||
        (present && node_copy_js_string(env, value, &out->ca_cert_file, &(uint32_t){0}) != 0)) {
        node_endpoint_config_free(out);
        return -EINVAL;
    }
    if (node_get_named_value(env, options, "http3Path", &value, &present) != 0) {
        node_endpoint_config_free(out);
        return -EINVAL;
    }
    if (present && node_copy_js_string(env, value, &out->path, &(uint32_t){0}) != 0) {
        node_endpoint_config_free(out);
        return -EINVAL;
    }
    if (out->path == NULL && (node_get_named_value(env, options, "path", &value, &present) != 0 ||
                                 (present && node_copy_js_string(env, value, &out->path, &(uint32_t){0}) != 0))) {
        node_endpoint_config_free(out);
        return -EINVAL;
    }
    if (node_get_named_value(env, options, "origin", &value, &present) != 0 ||
        (present && node_copy_js_string(env, value, &out->origin, &(uint32_t){0}) != 0)) {
        node_endpoint_config_free(out);
        return -EINVAL;
    }
    if (trevrpc_rpc_msquic_endpoint_config_v1_init(&out->config, sizeof(out->config)) != 0) {
        node_endpoint_config_free(out);
        return -EINVAL;
    }
    out->config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_CLIENT;
    out->config.transport = TREVRPC_RPC_MSQUIC_TRANSPORT_AUTO;
    if (node_get_named_value(env, options, "transport", &value, &present) != 0) {
        node_endpoint_config_free(out);
        return -EINVAL;
    }
    if (present) {
        napi_valuetype transport_type;
        if (napi_typeof(env, value, &transport_type) != napi_ok) {
            node_endpoint_config_free(out);
            return -EINVAL;
        }
        if (transport_type == napi_string) {
            char* transport_name = NULL;
            uint32_t transport_len = 0;
            if (node_copy_js_string(env, value, &transport_name, &transport_len) != 0) {
                node_endpoint_config_free(out);
                return -EINVAL;
            }
            if (strcmp(transport_name, "native") == 0) {
                out->config.transport = TREVRPC_RPC_MSQUIC_TRANSPORT_NATIVE;
            } else if (strcmp(transport_name, "http3") == 0) {
                out->config.transport = TREVRPC_RPC_MSQUIC_TRANSPORT_HTTP3;
            } else if (strcmp(transport_name, "webtransport") == 0) {
                out->config.transport = TREVRPC_RPC_MSQUIC_TRANSPORT_WEBTRANSPORT;
            } else if (strcmp(transport_name, "auto") != 0) {
                free(transport_name);
                node_endpoint_config_free(out);
                return -EINVAL;
            }
            free(transport_name);
        } else {
            uint32_t transport = 0;
            if (napi_get_value_uint32(env, value, &transport) != napi_ok ||
                transport > TREVRPC_RPC_MSQUIC_TRANSPORT_WEBTRANSPORT) {
                node_endpoint_config_free(out);
                return -EINVAL;
            }
            out->config.transport = transport;
        }
    }
    out->config.host = out->host;
    out->config.host_len = host_len;
    out->config.port = (uint16_t)port;
    out->config.server_name = out->server_name;
    out->config.server_name_len = (uint32_t)strlen(out->server_name);
    out->config.ca_cert_file = out->ca_cert_file;
    out->config.ca_cert_file_len = out->ca_cert_file == NULL ? 0 : (uint32_t)strlen(out->ca_cert_file);
    out->config.path = out->path;
    out->config.path_len = out->path == NULL ? 0 : (uint32_t)strlen(out->path);
    out->config.origin = out->origin;
    out->config.origin_len = out->origin == NULL ? 0 : (uint32_t)strlen(out->origin);
    bool skip_validation = false;
    if (node_get_named_bool(env, options, "skipCertificateValidation", &skip_validation, &present) != 0) {
        node_endpoint_config_free(out);
        return -EINVAL;
    }
    if (skip_validation) {
        out->config.flags &= ~TREVRPC_RPC_MSQUIC_VERIFY_PEER;
    } else {
        out->config.flags |= TREVRPC_RPC_MSQUIC_VERIFY_PEER;
    }
    uint64_t idle_timeout = 0;
    if (node_get_named_u64(env, options, "idleTimeoutMs", &idle_timeout) == 0) {
        out->config.max_idle_timeout_ms = idle_timeout;
    }
    return 0;
}

static void node_receive_item_free(node_receive_item* item) {
    if (item == NULL) {
        return;
    }
    trevrpc_rpc_receive_release(item->receive);
    free(item);
}

static void node_call_clear_receives(node_call* call) {
    node_receive_item* item = call->receive_head;
    while (item != NULL) {
        node_receive_item* next = item->next;
        node_receive_item_free(item);
        item = next;
    }
    call->receive_head = NULL;
    call->receive_tail = NULL;
    if (call->unary_message != NULL) {
        trevrpc_rpc_receive_release(call->unary_message);
        call->unary_message = NULL;
    }
}

static void node_call_dispose_buffers(node_call* call) {
    if (call == NULL) {
        return;
    }
    node_call_clear_receives(call);
    node_receive_waiter* waiter = call->waiter_head;
    while (waiter != NULL) {
        node_receive_waiter* next = waiter->next;
        free(waiter);
        waiter = next;
    }
    call->waiter_head = NULL;
    call->waiter_tail = NULL;
    node_send_batch* batch = call->send_head;
    call->send_head = NULL;
    call->send_tail = NULL;
    while (batch != NULL) {
        node_send_batch* next = batch->next;
        batch->next = NULL;
        node_send_batch_free(batch);
        batch = next;
    }
    call->send_inflight = NULL;
    node_status_free(&call->status);
}

static int node_call_append_receive(node_call* call, trevrpc_rpc_receive* receive) {
    node_receive_item* item = calloc(1, sizeof(*item));
    if (item == NULL) {
        trevrpc_rpc_receive_release(receive);
        return -ENOMEM;
    }
    item->receive = receive;
    if (call->receive_tail == NULL) {
        call->receive_head = item;
    } else {
        call->receive_tail->next = item;
    }
    call->receive_tail = item;
    return 0;
}

static node_receive_item* node_call_take_receive(node_call* call) {
    node_receive_item* item = call->receive_head;
    if (item == NULL) {
        return NULL;
    }
    call->receive_head = item->next;
    if (call->receive_head == NULL) {
        call->receive_tail = NULL;
    }
    item->next = NULL;
    return item;
}

static void node_call_detach_waiter(node_call* call, node_receive_waiter* waiter) {
    if (call == NULL || waiter == NULL || call->waiter_head != waiter) {
        return;
    }
    call->waiter_head = waiter->next;
    if (call->waiter_tail == waiter) {
        call->waiter_tail = NULL;
    }
    waiter->next = NULL;
}

static void node_call_reject_waiters(node_call* call, int error_code, const char* operation) {
    node_receive_waiter* waiter = call->waiter_head;
    call->waiter_head = NULL;
    call->waiter_tail = NULL;
    while (waiter != NULL) {
        node_receive_waiter* next = waiter->next;
        if (node_runtime_napi_legal(call->runtime)) {
            (void)node_reject_deferred_native(call->runtime->env, waiter->deferred, error_code, operation);
        }
        free(waiter);
        waiter = next;
    }
}

static void node_call_delete_wrapper_ref(node_call* call) {
    if (call != NULL && call->wrapper_ref != NULL && node_runtime_napi_legal(call->runtime)) {
        (void)napi_delete_reference(call->runtime->env, call->wrapper_ref);
        call->wrapper_ref = NULL;
    }
}

static void node_client_delete_wrapper_ref(node_client* client) {
    if (client != NULL && client->wrapper_ref != NULL && node_runtime_napi_legal(client->runtime)) {
        (void)napi_delete_reference(client->runtime->env, client->wrapper_ref);
        client->wrapper_ref = NULL;
    }
}

static void node_call_clear_open_deferred(node_call* call) {
    if (call == NULL || call->runtime == NULL) {
        return;
    }
    for (node_operation* operation = call->runtime->operations; operation != NULL; operation = operation->next) {
        if (operation->context == call && operation->action == NODE_OPERATION_CALL_OPEN) {
            operation->has_deferred = false;
            operation->deferred = NULL;
        }
    }
}

static void node_client_maybe_release(node_client* client) {
    if (client == NULL || !client->closed || client->subject == NULL || client->runtime == NULL ||
        client->runtime->rpc == NULL) {
        return;
    }
    node_runtime* runtime = client->runtime;
    int result = trevrpc_rpc_endpoint_release(runtime->rpc, client->endpoint);
    if (result != 0 && result != -ESTALE) {
        client->release_retry_pending = true;
        node_runtime_start_failure_progress(runtime);
        return;
    }
    node_client_detach_cancellation(client);
    node_client_detach_server(runtime, client);
    node_subject* subject = client->subject;
    client->subject = NULL;
    client->release_retry_pending = false;
    node_registry_remove(runtime, subject);
    node_runtime_update_liveness(runtime);
    client->runtime = NULL;
    if (!client->wrapper_alive) {
        free(client);
    }
}

static void node_call_settle_terminal(node_call* call, int error_code, const char* operation) {
    if (call == NULL) {
        return;
    }
    if (call->terminal_settled) {
        return;
    }
    call->terminal_settled = true;
    node_call_delete_wrapper_ref(call);
    call->failed = true;
    call->receive_finished = true;
    call->failure_code = error_code == 0 ? -EIO : error_code;
    node_call_reject_waiters(call, call->failure_code, operation);
    node_send_batch* batch = call->send_head;
    node_send_batch* previous = NULL;
    while (batch != NULL) {
        node_send_batch* next = batch->next;
        if (batch != call->send_inflight) {
            if (batch->has_deferred && node_runtime_napi_legal(call->runtime)) {
                (void)node_reject_deferred_native(call->runtime->env, batch->deferred, call->failure_code, operation);
            }
            if (previous == NULL) {
                call->send_head = next;
            } else {
                previous->next = next;
            }
            node_send_batch_free(batch);
        } else {
            previous = batch;
        }
        batch = next;
    }
    call->send_tail = NULL;
    for (batch = call->send_head; batch != NULL; batch = batch->next) {
        call->send_tail = batch;
    }
    if (!call->response_settled && call->has_response_deferred) {
        if (node_runtime_napi_legal(call->runtime)) {
            (void)node_reject_deferred_native(
                call->runtime->env, call->response_deferred, call->failure_code, operation);
        }
        call->has_response_deferred = false;
        call->response_deferred = NULL;
    }
}

static int node_call_receive_info(
    node_runtime* runtime, trevrpc_rpc_receive* receive, trevrpc_rpc_receive_info_v1* out_info) {
    int result = trevrpc_rpc_receive_info_v1_init(out_info, sizeof(*out_info));
    if (result == 0) {
        result = trevrpc_rpc_receive_get_info_v1(receive, out_info);
    }
    (void)runtime;
    return result;
}

static int node_make_body_from_receive(napi_env env,
    trevrpc_rpc_receive* receive,
    const trevrpc_rpc_receive_info_v1* info,
    napi_value* out,
    bool* transferred) {
    *transferred = false;
    if (node_test_flag("TREVRPC_NODE_FAIL_BODY_CONVERSION")) {
        trevrpc_rpc_receive_release(receive);
        return -EPROTO;
    }
    if (info->data_len == 0) {
        if (node_make_bytes_value(env, NULL, 0, out) != 0) {
            trevrpc_rpc_receive_release(receive);
            return -ENOMEM;
        }
        trevrpc_rpc_receive_release(receive);
        return 0;
    }
    if (info->data == NULL || info->data_len > SIZE_MAX) {
        trevrpc_rpc_receive_release(receive);
        return -EPROTO;
    }
    node_external_receive_owner* owner = calloc(1, sizeof(*owner));
    if (owner != NULL) {
        napi_value arraybuffer;
        owner->data = malloc((size_t)info->data_len);
        if (owner->data != NULL) {
            memcpy(owner->data, info->data, (size_t)info->data_len);
        }
        if (owner->data != NULL &&
            napi_create_external_arraybuffer(
                env, owner->data, (size_t)info->data_len, node_external_receive_finalizer, owner, &arraybuffer) ==
                napi_ok) {
            if (napi_create_typedarray(env, napi_uint8_array, (size_t)info->data_len, arraybuffer, 0, out) == napi_ok) {
                trevrpc_rpc_receive_release(receive);
                owner->receive = NULL;
                *transferred = true;
                return 0;
            }
            /* The ArrayBuffer now owns owner through its finalizer.  Keep that
             * owner alive while returning the independent copied fallback. */
            int result = node_make_bytes_value(env, owner->data, (size_t)info->data_len, out);
            trevrpc_rpc_receive_release(receive);
            owner->receive = NULL;
            return result;
        }
        free(owner->data);
        free(owner);
    }
    int result = node_make_bytes_value(env, info->data, (size_t)info->data_len, out);
    trevrpc_rpc_receive_release(receive);
    return result;
}

static int node_make_message_frame(napi_env env,
    node_call* call,
    trevrpc_rpc_receive* receive,
    const trevrpc_rpc_receive_info_v1* info,
    napi_value* out) {
    if (call->status_seen) {
        trevrpc_rpc_receive_release(receive);
        return -EPROTO;
    }
    call->message_seen = true;
    napi_value kind;
    napi_value body;
    bool transferred = false;
    int result = node_make_body_from_receive(env, receive, info, &body, &transferred);
    (void)transferred;
    if (result != 0) {
        return result;
    }
    if (napi_create_object(env, out) != napi_ok || napi_create_uint32(env, 0, &kind) != napi_ok ||
        napi_set_named_property(env, *out, "kind", kind) != napi_ok ||
        napi_set_named_property(env, *out, "body", body) != napi_ok) {
        return -ENOMEM;
    }
    return 0;
}

static int node_make_status_frame(napi_env env,
    node_call* call,
    trevrpc_rpc_receive* receive,
    const trevrpc_rpc_receive_info_v1* info,
    napi_value* out) {
    if (call->status_seen) {
        trevrpc_rpc_receive_release(receive);
        return -EPROTO;
    }
    node_status_value status;
    int result = node_copy_receive_status(info, &status);
    trevrpc_rpc_receive_release(receive);
    if (result != 0) {
        return result;
    }
    if (napi_create_object(env, out) != napi_ok) {
        node_status_free(&status);
        return -ENOMEM;
    }
    napi_value kind;
    napi_value status_object;
    if (napi_create_uint32(env, 1, &kind) != napi_ok || napi_set_named_property(env, *out, "kind", kind) != napi_ok ||
        node_make_status_object(env, &status, &status_object) != 0) {
        node_status_free(&status);
        return -ENOMEM;
    }
    const char* names[] = {"status", "message", "metadata"};
    for (size_t index = 0; index < 3; ++index) {
        napi_value property;
        if (napi_get_named_property(env, status_object, names[index], &property) != napi_ok ||
            napi_set_named_property(env, *out, names[index], property) != napi_ok) {
            node_status_free(&status);
            return -ENOMEM;
        }
    }
    node_status_free(&call->status);
    call->status = status;
    call->status_seen = true;
    return 0;
}

static int node_call_fetch_receives(node_call* call) {
    if (!call->ready || call->receive_finished || call->failed) {
        return 0;
    }
    for (;;) {
        trevrpc_rpc_receive* receive = NULL;
        int result = trevrpc_rpc_stream_receive(call->runtime->rpc, call->stream, &receive);
        if (result == -EAGAIN) {
            return 0;
        }
        if (result != 0 || receive == NULL) {
            int error = result == 0 ? -EPROTO : result;
            bool cancellation_requested = false;
            if (call->cancellation.owner != 0 || call->cancellation.slot != 0 || call->cancellation.generation != 0) {
                node_subject* cancellation_subject =
                    node_registry_find(&call->runtime->cancellations, node_key_from_cancellation(call->cancellation));
                cancellation_requested = cancellation_subject != NULL && cancellation_subject->cancel_submitted;
            }
            if (error == -EPIPE && call->stream_closed && !cancellation_requested &&
                (call->client == NULL || !call->client->invalidated)) {
                return 0;
            }
            return error;
        }
        result = node_call_append_receive(call, receive);
        if (result != 0) {
            return result;
        }
    }
}

static void node_call_fail(node_call* call, int error_code, const char* operation) {
    if (call == NULL) {
        return;
    }
    node_call_settle_terminal(call, error_code, operation);
    call->close_abort = true;
    node_call_request_close(call);
}

static int node_call_make_unary_response(node_call* call, napi_value* out) {
    napi_env env = call->runtime->env;
    if (napi_create_object(env, out) != napi_ok) {
        return -ENOMEM;
    }
    napi_value status_object;
    if (node_make_status_object(env, &call->status, &status_object) != 0) {
        return -ENOMEM;
    }
    const char* names[] = {"status", "message", "metadata"};
    for (size_t index = 0; index < 3; ++index) {
        napi_value property;
        if (napi_get_named_property(env, status_object, names[index], &property) != napi_ok ||
            napi_set_named_property(env, *out, names[index], property) != napi_ok) {
            return -ENOMEM;
        }
    }
    napi_value body;
    if (call->unary_message != NULL) {
        trevrpc_rpc_receive_info_v1 info;
        int result = node_call_receive_info(call->runtime, call->unary_message, &info);
        if (result != 0) {
            trevrpc_rpc_receive_release(call->unary_message);
            call->unary_message = NULL;
            return result;
        }
        bool transferred = false;
        result = node_make_body_from_receive(env, call->unary_message, &info, &body, &transferred);
        call->unary_message = NULL;
        if (result != 0) {
            return result;
        }
    } else if (node_make_bytes_value(env, NULL, 0, &body) != 0) {
        return -ENOMEM;
    }
    if (napi_set_named_property(env, *out, "body", body) != napi_ok) {
        return -ENOMEM;
    }
    return 0;
}

static int node_call_process_unary(node_call* call) {
    while (call->receive_head != NULL) {
        node_receive_item* item = node_call_take_receive(call);
        trevrpc_rpc_receive_info_v1 info;
        int result = node_call_receive_info(call->runtime, item->receive, &info);
        if (result != 0) {
            node_receive_item_free(item);
            return result;
        }
        if (info.kind == TREVRPC_RPC_RECEIVE_MESSAGE || info.kind == TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE) {
            if (call->status_seen || call->message_seen) {
                node_receive_item_free(item);
                return -EPROTO;
            }
            result = node_copy_receive_status(&info, &call->status);
            if (result != 0) {
                node_receive_item_free(item);
                return result;
            }
            call->status_seen = true;
            call->message_seen = true;
            call->unary_message = item->receive;
            item->receive = NULL;
            free(item);
        } else if (info.kind == TREVRPC_RPC_RECEIVE_STATUS) {
            if (call->status_seen) {
                node_receive_item_free(item);
                return -EPROTO;
            }
            result = node_copy_receive_status(&info, &call->status);
            node_receive_item_free(item);
            if (result != 0) {
                return result;
            }
            call->status_seen = true;
        } else {
            node_receive_item_free(item);
            return -EPROTO;
        }
    }
    if (!call->fin_seen || call->response_settled || !call->status_seen) {
        return 0;
    }
    if (!call->fin_clean || (call->status.code == TREVRPC_RPC_STATUS_OK && !call->message_seen)) {
        return -EPROTO;
    }
    napi_value response;
    int result = node_call_make_unary_response(call, &response);
    if (result != 0) {
        return result;
    }
    call->response_settled = true;
    if (call->has_response_deferred) {
        (void)napi_resolve_deferred(call->runtime->env, call->response_deferred, response);
    }
    node_call_request_close(call);
    return 0;
}

static int node_call_take_stream_frame(node_call* call, node_receive_item* item, napi_value* out) {
    trevrpc_rpc_receive_info_v1 info;
    int result = node_call_receive_info(call->runtime, item->receive, &info);
    if (result != 0) {
        node_receive_item_free(item);
        return result;
    }
    if (info.kind == TREVRPC_RPC_RECEIVE_MESSAGE || info.kind == TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE) {
        result = node_make_message_frame(call->runtime->env, call, item->receive, &info, out);
        item->receive = NULL;
        free(item);
        return result;
    }
    if (info.kind == TREVRPC_RPC_RECEIVE_STATUS) {
        result = node_make_status_frame(call->runtime->env, call, item->receive, &info, out);
        item->receive = NULL;
        free(item);
        return result;
    }
    node_receive_item_free(item);
    return -EPROTO;
}

static int node_call_make_body_batch(node_call* call, node_receive_waiter* waiter, napi_value* out) {
    napi_env env = call->runtime->env;
    napi_value bodies;
    if (napi_create_array(env, &bodies) != napi_ok) {
        return -ENOMEM;
    }
    uint32_t count = 0;
    napi_value status = NULL;
    while (call->receive_head != NULL && count < waiter->max_items) {
        node_receive_item* item = node_call_take_receive(call);
        trevrpc_rpc_receive_info_v1 info;
        int result = node_call_receive_info(call->runtime, item->receive, &info);
        if (result != 0) {
            node_receive_item_free(item);
            return result;
        }
        if (info.kind == TREVRPC_RPC_RECEIVE_STATUS) {
            result = node_make_status_frame(env, call, item->receive, &info, &status);
            item->receive = NULL;
            free(item);
            if (result != 0) {
                return result;
            }
            break;
        }
        if (info.kind != TREVRPC_RPC_RECEIVE_MESSAGE && info.kind != TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE) {
            node_receive_item_free(item);
            return -EPROTO;
        }
        napi_value body;
        bool transferred = false;
        result = node_make_body_from_receive(env, item->receive, &info, &body, &transferred);
        item->receive = NULL;
        free(item);
        if (result != 0 || napi_set_element(env, bodies, count++, body) != napi_ok) {
            return result == 0 ? -ENOMEM : result;
        }
        call->message_seen = true;
    }
    if (napi_create_object(env, out) != napi_ok || napi_set_named_property(env, *out, "bodies", bodies) != napi_ok) {
        return -ENOMEM;
    }
    if (status != NULL && napi_set_named_property(env, *out, "status", status) != napi_ok) {
        return -ENOMEM;
    }
    bool eof = call->fin_seen && call->fin_clean && call->receive_head == NULL && call->status_seen;
    napi_value eof_value;
    if (napi_get_boolean(env, eof, &eof_value) != napi_ok ||
        napi_set_named_property(env, *out, "eof", eof_value) != napi_ok) {
        return -ENOMEM;
    }
    if (eof) {
        call->receive_finished = true;
        call->receive_succeeded = true;
        node_call_request_close(call);
    }
    return 0;
}

static void node_call_process_stream_waiters(node_call* call) {
    while (call->waiter_head != NULL) {
        node_receive_waiter* waiter = call->waiter_head;
        napi_value value = NULL;
        int result = 0;
        if (waiter->body_batch) {
            if (call->receive_head == NULL && !(call->fin_seen && call->status_seen)) {
                break;
            }
            /* The body-batch completion path may submit close, which can synchronously
             * settle the call and reject every still-linked waiter. Detach this waiter
             * before entering that path so it cannot be freed underneath us. */
            node_call_detach_waiter(call, waiter);
            result = node_call_make_body_batch(call, waiter, &value);
        } else if (call->receive_head != NULL) {
            node_call_detach_waiter(call, waiter);
            if (waiter->max_items <= 1) {
                node_receive_item* item = node_call_take_receive(call);
                result = node_call_take_stream_frame(call, item, &value);
            } else {
                if (napi_create_array(call->runtime->env, &value) != napi_ok) {
                    result = -ENOMEM;
                } else {
                    uint32_t frame_count = 0;
                    while (call->receive_head != NULL && frame_count < waiter->max_items) {
                        node_receive_item* item = node_call_take_receive(call);
                        napi_value frame = NULL;
                        result = node_call_take_stream_frame(call, item, &frame);
                        if (result != 0 ||
                            napi_set_element(call->runtime->env, value, frame_count++, frame) != napi_ok) {
                            if (result == 0) {
                                result = -ENOMEM;
                            }
                            break;
                        }
                    }
                }
            }
        } else if (call->fin_seen) {
            if (!call->status_seen) {
                break;
            }
            node_call_detach_waiter(call, waiter);
            if (!call->fin_clean) {
                result = -EPROTO;
            } else {
                if (waiter->max_items <= 1) {
                    if (napi_get_null(call->runtime->env, &value) != napi_ok) {
                        result = -ENOMEM;
                    }
                } else {
                    napi_value array = NULL;
                    napi_value null_value = NULL;
                    if (napi_create_array(call->runtime->env, &array) != napi_ok ||
                        napi_get_null(call->runtime->env, &null_value) != napi_ok ||
                        napi_set_element(call->runtime->env, array, 0, null_value) != napi_ok) {
                        result = -ENOMEM;
                    } else {
                        value = array;
                    }
                }
                call->receive_finished = true;
                call->receive_succeeded = true;
                node_call_request_close(call);
            }
        } else {
            break;
        }
        if (result == 0) {
            if (!waiter->body_batch && call->fin_seen && call->fin_clean && call->status_seen &&
                call->receive_head == NULL) {
                call->receive_finished = true;
                call->receive_succeeded = true;
            }
            (void)napi_resolve_deferred(call->runtime->env, waiter->deferred, value);
        } else {
            (void)node_reject_deferred_native(call->runtime->env, waiter->deferred, result, "receive response stream");
            node_call_fail(call, result, "receive response stream");
        }
        free(waiter);
    }
    node_call_maybe_finish_client_receive(call);
    node_call_maybe_release(call);
}

static int node_make_request_frame(napi_env env, node_call* call, node_receive_item* item, napi_value* out) {
    trevrpc_rpc_receive_info_v1 info;
    int result = node_call_receive_info(call->runtime, item->receive, &info);
    if (result != 0) {
        node_receive_item_free(item);
        return result;
    }
    if (info.kind == TREVRPC_RPC_RECEIVE_MESSAGE || info.kind == TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE) {
        napi_value body;
        bool transferred = false;
        result = node_make_body_from_receive(env, item->receive, &info, &body, &transferred);
        (void)transferred;
        item->receive = NULL;
        free(item);
        if (result != 0) {
            return result;
        }
        napi_value kind;
        if (napi_create_object(env, out) != napi_ok || napi_create_uint32(env, 0, &kind) != napi_ok ||
            napi_set_named_property(env, *out, "kind", kind) != napi_ok ||
            napi_set_named_property(env, *out, "body", body) != napi_ok) {
            return -ENOMEM;
        }
        return 0;
    }
    if (info.kind == TREVRPC_RPC_RECEIVE_STATUS) {
        node_status_value status;
        result = node_copy_receive_status(&info, &status);
        node_receive_item_free(item);
        if (result != 0) {
            return result;
        }
        napi_value status_object;
        if (napi_create_object(env, out) != napi_ok || node_make_status_object(env, &status, &status_object) != 0) {
            node_status_free(&status);
            return -ENOMEM;
        }
        napi_value kind;
        if (napi_create_uint32(env, 1, &kind) != napi_ok ||
            napi_set_named_property(env, *out, "kind", kind) != napi_ok) {
            node_status_free(&status);
            return -ENOMEM;
        }
        const char* names[] = {"status", "message", "metadata"};
        for (size_t index = 0; index < 3; ++index) {
            napi_value property;
            if (napi_get_named_property(env, status_object, names[index], &property) != napi_ok ||
                napi_set_named_property(env, *out, names[index], property) != napi_ok) {
                node_status_free(&status);
                return -ENOMEM;
            }
        }
        if (status.code == TREVRPC_RPC_STATUS_OK) {
            call->request_finished = true;
        }
        node_status_free(&status);
        return 0;
    }
    node_receive_item_free(item);
    return -EPROTO;
}

static void node_call_discard_empty_initial_request(node_call* call) {
    if (call == NULL ||
        (call->kind != TREVRPC_RPC_KIND_CLIENT_STREAMING && call->kind != TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING)) {
        return;
    }
    while (call->receive_head != NULL) {
        trevrpc_rpc_receive_info_v1 info;
        node_receive_item* item = call->receive_head;
        if (node_call_receive_info(call->runtime, item->receive, &info) != 0 ||
            info.kind != TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE || info.data_len != 0) {
            return;
        }
        (void)node_call_take_receive(call);
        node_receive_item_free(item);
    }
}

static void node_call_process_server_waiters(node_call* call) {
    while (call->waiter_head != NULL) {
        node_call_discard_empty_initial_request(call);
        node_receive_waiter* waiter = call->waiter_head;
        napi_value value = NULL;
        int result = 0;
        if (call->receive_head != NULL) {
            if (waiter->max_items <= 1) {
                result = node_make_request_frame(call->runtime->env, call, node_call_take_receive(call), &value);
            } else {
                if (napi_create_array(call->runtime->env, &value) != napi_ok) {
                    result = -ENOMEM;
                } else {
                    uint32_t count = 0;
                    while (call->receive_head != NULL && count < waiter->max_items) {
                        napi_value frame = NULL;
                        result =
                            node_make_request_frame(call->runtime->env, call, node_call_take_receive(call), &frame);
                        if (result != 0 || napi_set_element(call->runtime->env, value, count++, frame) != napi_ok) {
                            if (result == 0) {
                                result = -ENOMEM;
                            }
                            break;
                        }
                    }
                }
            }
        } else if (call->request_finished && call->fin_seen) {
            if (waiter->max_items <= 1) {
                if (napi_get_null(call->runtime->env, &value) != napi_ok) {
                    result = -ENOMEM;
                }
            } else {
                napi_value array = NULL;
                napi_value null_value = NULL;
                if (napi_create_array(call->runtime->env, &array) != napi_ok ||
                    napi_get_null(call->runtime->env, &null_value) != napi_ok ||
                    napi_set_element(call->runtime->env, array, 0, null_value) != napi_ok) {
                    result = -ENOMEM;
                } else {
                    value = array;
                }
            }
            call->receive_finished = true;
        } else {
            break;
        }
        call->waiter_head = waiter->next;
        if (call->waiter_head == NULL) {
            call->waiter_tail = NULL;
        }
        if (result == 0) {
            (void)napi_resolve_deferred(call->runtime->env, waiter->deferred, value);
        } else {
            (void)node_reject_deferred_native(call->runtime->env, waiter->deferred, result, "receive request stream");
            node_call_fail(call, result, "receive request stream");
        }
        free(waiter);
    }
}

static void node_call_maybe_finish_client_receive(node_call* call) {
    if (call == NULL || call->server_side || call->kind == TREVRPC_RPC_KIND_UNARY || call->terminal_settled ||
        !call->fin_seen || !call->fin_clean || !call->status_seen || call->receive_head != NULL) {
        return;
    }
    call->receive_finished = true;
    call->receive_succeeded = true;
    if (!call->close_requested) {
        node_call_request_close(call);
    }
}

static void node_process_call(node_call* call) {
    if (call == NULL || call->runtime == NULL || call->runtime->rpc == NULL || call->failed || call->receive_finished ||
        (!call->server_side && call->kind == TREVRPC_RPC_KIND_UNARY && call->response_settled)) {
        return;
    }
    int result = node_call_fetch_receives(call);
    if (result != 0) {
        node_call_fail(call, result, "receive response stream");
        return;
    }
    if (call->server_side) {
        if (call->response_settled) {
            /* Once the application response is complete, request-side frames are
             * no longer observable. Release them while continuing to drain until
             * the peer FIN arrives so natural stream and call terminals can retire
             * the native handles without an abortive close. */
            node_call_clear_receives(call);
            call->receive_finished = call->fin_seen;
            node_call_maybe_release(call);
        } else {
            node_call_process_server_waiters(call);
        }
        return;
    }
    result = call->kind == TREVRPC_RPC_KIND_UNARY ? node_call_process_unary(call) : 0;
    if (result != 0) {
        node_call_fail(call, result, "receive response stream");
        return;
    }
    if (call->kind != TREVRPC_RPC_KIND_UNARY) {
        node_call_process_stream_waiters(call);
        return;
    }
    if (call->call_closed && !call->response_settled) {
        node_call_fail(call, -EPIPE, "RPC call closed before response completion");
    }
}

static napi_value node_connect_msquic(napi_env env, napi_callback_info info);
static napi_value node_client_call(napi_env env, napi_callback_info info);
static int node_client_request_close(node_client* client);
static napi_value node_make_client_object(napi_env env, node_client* client);
static napi_value node_make_stream_object(napi_env env, node_call* call);
static int node_parse_cancellation(
    napi_env env, napi_value value, node_runtime* runtime, trevrpc_rpc_cancellation_v1* out);
static int node_call_register_handles(node_call* call);
static napi_value node_client_start_stream(napi_env env, napi_callback_info info);
static napi_value node_client_close(napi_env env, napi_callback_info info);
static napi_value node_client_create_cancellation(napi_env env, napi_callback_info info);
static napi_value node_stream_send_message(napi_env env, napi_callback_info info);
static napi_value node_stream_send_messages(napi_env env, napi_callback_info info);
static napi_value node_stream_finish_send(napi_env env, napi_callback_info info);
static napi_value node_stream_recv(napi_env env, napi_callback_info info);
static napi_value node_stream_recv_many(napi_env env, napi_callback_info info);
static napi_value node_stream_recv_body_batch(napi_env env, napi_callback_info info);
static napi_value node_stream_close(napi_env env, napi_callback_info info);
static void node_client_finalizer(napi_env env, void* data, void* hint);
static void node_call_finalizer(napi_env env, void* data, void* hint);

static void node_call_detach_server(node_runtime* runtime, node_call* call) {
    if (call == NULL) {
        return;
    }
    node_server* server = call->server;
    if (call->route != NULL && call->route->active_calls > 0) {
        call->route->active_calls--;
    }
    call->route = NULL;
    call->server = NULL;
    if (server != NULL && (server->closed || server->runtime == NULL)) {
        node_server_free_routes(runtime == NULL ? NULL : runtime->env, server);
        if (!server->wrapper_alive && server->endpoint == NULL && server->routes == NULL) {
            free(server);
        }
    }
}

static void node_call_maybe_release(node_call* call) {
    if (call == NULL || !call->call_closed || !call->stream_closed || call->runtime == NULL ||
        call->runtime->rpc == NULL || call->send_inflight != NULL || call->receive_head != NULL ||
        call->waiter_head != NULL ||
        (!call->server_side && call->kind != TREVRPC_RPC_KIND_UNARY &&
            !(call->receive_finished || call->terminal_settled))) {
        return;
    }
    node_runtime* runtime = call->runtime;
    if (call->stream_subject != NULL) {
        int result = trevrpc_rpc_stream_release(runtime->rpc, call->stream);
        if (result != 0 && result != -ESTALE) {
            call->stream_release_retry_pending = true;
            node_runtime_start_failure_progress(runtime);
            return;
        }
        call->stream_release_retry_pending = false;
        node_subject* subject = call->stream_subject;
        call->stream_subject = NULL;
        node_test_trace_key(runtime, "stream-release", (uint32_t)subject->kind, subject->key);
        node_registry_remove(runtime, subject);
    }
    if (call->call_subject != NULL) {
        int result = trevrpc_rpc_call_release(runtime->rpc, call->call);
        if (result != 0 && result != -ESTALE) {
            call->call_release_retry_pending = true;
            node_runtime_start_failure_progress(runtime);
            return;
        }
        call->call_release_retry_pending = false;
        node_subject* subject = call->call_subject;
        call->call_subject = NULL;
        node_test_trace_key(runtime, "call-release", (uint32_t)subject->kind, subject->key);
        node_registry_remove(runtime, subject);
    }
    node_call_clear_receives(call);
    free(call->server_response_body);
    call->server_response_body = NULL;
    call->server_response_body_len = 0;
    call->server_response_body_set = false;
    node_status_free(&call->status);
    node_call_detach_server(runtime, call);
    node_runtime_update_liveness(runtime);
    call->runtime = NULL;
    if (!call->wrapper_alive) {
        free(call);
    }
}

static bool node_close_retryable(int result) {
    return result == -EAGAIN || result == -EBUSY || result == -EINPROGRESS;
}

static void node_call_request_close_with_flags(node_call* call, uint32_t flags) {
    if (call == NULL || call->runtime == NULL || call->runtime->rpc == NULL || node_handle_is_null_call(call->call) ||
        node_handle_is_null_stream(call->stream)) {
        return;
    }
    call->close_requested = true;
    bool response_complete = call->response_settled || (call->fin_seen && call->fin_clean && call->status_seen);
    bool close_stream = flags == TREVRPC_RPC_CLOSE_FLAG_ABORT || !response_complete;
    for (unsigned attempt = 0; close_stream && attempt < 8u && !call->stream_close_submitted; ++attempt) {
        uint64_t id = node_operation_allocate(call->runtime);
        if (id == TREVRPC_RPC_OPERATION_ID_NONE) {
            call->stream_close_retry_pending = true;
            node_runtime_start_failure_progress(call->runtime);
            break;
        }
        node_operation* operation = node_operation_add_id(call->runtime,
            id,
            TREVRPC_RPC_OBJECT_STREAM,
            node_key_from_stream(call->stream),
            NODE_OPERATION_STREAM_CLOSE,
            call,
            NULL,
            false);
        if (operation == NULL) {
            call->stream_close_retry_pending = true;
            node_runtime_start_failure_progress(call->runtime);
            break;
        }
        int result = trevrpc_rpc_stream_close(call->runtime->rpc, call->stream, id, flags, 0);
        if (result == 0 || result == -EALREADY) {
            if (result == -EALREADY) {
                node_operation_remove(call->runtime, operation);
                call->stream_closed = true;
            }
            call->stream_close_submitted = true;
            call->stream_close_retry_pending = false;
            break;
        }
        node_operation_remove(call->runtime, operation);
        if (!node_close_retryable(result)) {
            call->stream_close_retry_pending = false;
            node_call_settle_terminal(call, result, "close RPC stream");
            break;
        }
        call->stream_close_retry_pending = true;
    }
    for (unsigned attempt = 0; attempt < 8u && !call->close_submitted; ++attempt) {
        uint64_t id = node_operation_allocate(call->runtime);
        if (id == TREVRPC_RPC_OPERATION_ID_NONE) {
            call->close_retry_pending = true;
            node_runtime_start_failure_progress(call->runtime);
            break;
        }
        node_operation* operation = node_operation_add_id(call->runtime,
            id,
            TREVRPC_RPC_OBJECT_CALL,
            node_key_from_call(call->call),
            NODE_OPERATION_CALL_CLOSE,
            call,
            NULL,
            false);
        if (operation == NULL) {
            call->close_retry_pending = true;
            node_runtime_start_failure_progress(call->runtime);
            break;
        }
        int result = trevrpc_rpc_call_close(call->runtime->rpc, call->call, id, flags, 0);
        if (result == 0 || result == -EALREADY) {
            if (result == -EALREADY) {
                node_operation_remove(call->runtime, operation);
                call->call_closed = true;
            }
            call->close_submitted = true;
            call->close_retry_pending = false;
            break;
        }
        node_operation_remove(call->runtime, operation);
        if (!node_close_retryable(result)) {
            call->close_retry_pending = false;
            node_call_settle_terminal(call, result, "close RPC call");
            break;
        }
        call->close_retry_pending = true;
    }
    bool retry_pending = call->close_retry_pending || call->stream_close_retry_pending ||
                         call->call_release_retry_pending || call->stream_release_retry_pending;
    if (call->runtime != NULL && retry_pending) {
        node_runtime_start_failure_progress(call->runtime);
    }
}

static void node_call_request_close(node_call* call) {
    node_call_request_close_with_flags(
        call, call != NULL && call->close_abort ? TREVRPC_RPC_CLOSE_FLAG_ABORT : TREVRPC_RPC_CLOSE_FLAG_NONE);
}

static void node_call_complete_server_response(node_call* call, bool succeeded) {
    call->response_settled = succeeded;
    call->response_submitted = false;
    if (!succeeded) {
        call->close_abort = true;
        node_call_request_close(call);
    }
}

static void node_send_pump(node_call* call);

static napi_value node_new_promise(napi_env env, napi_deferred* deferred) {
    napi_value promise;
    if (napi_create_promise(env, deferred, &promise) != napi_ok) {
        return NULL;
    }
    return promise;
}

static void node_send_batch_remove(node_call* call, node_send_batch* batch) {
    if (call->send_head == batch) {
        call->send_head = batch->next;
    } else {
        node_send_batch* previous = call->send_head;
        while (previous != NULL && previous->next != batch) {
            previous = previous->next;
        }
        if (previous != NULL) {
            previous->next = batch->next;
        }
    }
    if (call->send_tail == batch) {
        call->send_tail = NULL;
        for (node_send_batch* item = call->send_head; item != NULL; item = item->next) {
            call->send_tail = item;
        }
    }
    batch->next = NULL;
}

static void node_send_batch_free(node_send_batch* batch) {
    if (batch == NULL) {
        return;
    }
    for (size_t index = batch->index; index < batch->count; ++index) {
        free(batch->bodies[index]);
    }
    free(batch->bodies);
    free(batch->body_lengths);
    free(batch);
}

static void node_send_batch_fail_admission(
    node_call* call, node_send_batch* batch, int error_code, const char* operation) {
    if (call == NULL || batch == NULL) {
        return;
    }
    node_runtime* runtime = call->runtime;
    node_send_batch_remove(call, batch);
    if (batch->has_deferred && runtime != NULL && node_runtime_napi_legal(runtime)) {
        (void)node_reject_deferred_native(runtime->env, batch->deferred, error_code, operation);
    }
    node_send_batch_free(batch);
    node_call_fail(call, error_code, operation);
}

static void node_send_pump(node_call* call) {
    if (call == NULL || call->runtime == NULL || call->runtime->rpc == NULL || !call->ready || call->failed ||
        call->send_inflight != NULL || call->send_head == NULL) {
        return;
    }
    node_send_batch* batch = call->send_head;
    if (batch->finish) {
        uint64_t id = node_send_test_failure(call->runtime, false) ? TREVRPC_RPC_OPERATION_ID_NONE
                                                                   : node_operation_allocate(call->runtime);
        if (id == TREVRPC_RPC_OPERATION_ID_NONE) {
            node_send_batch_fail_admission(call, batch, -ENOMEM, "finishSend operation allocation");
            return;
        }
        node_operation* operation = node_send_test_failure(call->runtime, true)
                                        ? NULL
                                        : node_operation_add_id(call->runtime,
                                              id,
                                              TREVRPC_RPC_OBJECT_STREAM,
                                              node_key_from_stream(call->stream),
                                              NODE_OPERATION_FINISH_SEND,
                                              batch,
                                              batch->deferred,
                                              batch->has_deferred);
        if (operation == NULL) {
            node_send_batch_fail_admission(call, batch, -ENOMEM, "finishSend operation record");
            return;
        }
        int result = trevrpc_rpc_stream_finish_send(call->runtime->rpc, call->stream, id);
        if (result == -EALREADY) {
            node_operation_remove(call->runtime, operation);
            node_send_batch_remove(call, batch);
            if (batch->has_deferred) {
                node_resolve_undefined(call->runtime->env, batch->deferred);
            }
            node_send_batch_free(batch);
            call->send_finished = true;
            return;
        }
        if (result != 0) {
            node_operation_remove(call->runtime, operation);
            node_send_batch_remove(call, batch);
            if (batch->has_deferred) {
                (void)node_reject_deferred_native(call->runtime->env, batch->deferred, result, "finishSend");
            }
            node_send_batch_free(batch);
            node_call_fail(call, result, "finishSend");
            return;
        }
        batch->operation_id = id;
        call->send_inflight = batch;
        return;
    }
    if (batch->index >= batch->count) {
        node_send_batch_remove(call, batch);
        if (batch->has_deferred) {
            node_resolve_undefined(call->runtime->env, batch->deferred);
        }
        node_send_batch_free(batch);
        node_send_pump(call);
        return;
    }
    uint64_t id = node_send_test_failure(call->runtime, false) ? TREVRPC_RPC_OPERATION_ID_NONE
                                                               : node_operation_allocate(call->runtime);
    if (id == TREVRPC_RPC_OPERATION_ID_NONE) {
        node_send_batch_fail_admission(call, batch, -ENOMEM, "sendMessage operation allocation");
        return;
    }
    node_operation* operation = node_send_test_failure(call->runtime, true) ? NULL
                                                                            : node_operation_add_id(call->runtime,
                                                                                  id,
                                                                                  TREVRPC_RPC_OBJECT_STREAM,
                                                                                  node_key_from_stream(call->stream),
                                                                                  NODE_OPERATION_SEND,
                                                                                  batch,
                                                                                  NULL,
                                                                                  false);
    if (operation == NULL) {
        node_send_batch_fail_admission(call, batch, -ENOMEM, "sendMessage operation record");
        return;
    }
    int result = trevrpc_rpc_stream_send_copy_v1(call->runtime->rpc,
        call->stream,
        id,
        batch->bodies[batch->index],
        batch->body_lengths[batch->index],
        TREVRPC_RPC_SEND_FLAG_NONE);
    if (result != 0) {
        node_operation_remove(call->runtime, operation);
        node_send_batch_remove(call, batch);
        if (batch->has_deferred) {
            (void)node_reject_deferred_native(call->runtime->env, batch->deferred, result, "sendMessage");
        }
        node_send_batch_free(batch);
        node_call_fail(call, result, "sendMessage");
        return;
    }
    batch->operation_id = id;
    call->send_inflight = batch;
}

static int node_complete_operation(
    node_runtime* runtime, node_operation* operation, const trevrpc_rpc_event_info_v1* info) {
    napi_env env = runtime->env;
    switch (operation->action) {
    case NODE_OPERATION_ENDPOINT_START: {
        node_client* client = operation->context;
        if (client == NULL) {
            return -ESTALE;
        }
        if (info->kind == TREVRPC_RPC_EVENT_ENDPOINT_READY && info->status == 0) {
            client->ready = true;
            node_runtime_update_liveness(runtime);
            bool delivered = false;
            if (operation->has_deferred) {
                napi_value object;
                if (client->wrapper_ref != NULL &&
                    napi_get_reference_value(env, client->wrapper_ref, &object) == napi_ok) {
                    (void)napi_resolve_deferred(env, operation->deferred, object);
                    delivered = true;
                } else {
                    (void)node_reject_deferred_native(env, operation->deferred, -EIO, "connectMsQuic");
                }
            }
            node_client_delete_wrapper_ref(client);
            if (!delivered) {
                client->invalidated = true;
                (void)node_client_request_close(client);
            }
            return 0;
        }
        int error = info->status == 0 ? -EIO : info->status;
        if (info->provider_error_code != 0 && info->provider_error_code <= (uint64_t)INT32_MAX) {
            error = -(int)info->provider_error_code;
        }
        if (operation->has_deferred) {
            (void)node_reject_deferred_native(env, operation->deferred, error, "connectMsQuic");
        }
        client->closed = true;
        if (client->closed_deferred != NULL) {
            node_resolve_undefined(env, client->closed_deferred);
            client->closed_deferred = NULL;
        }
        node_client_delete_wrapper_ref(client);
        node_client_maybe_release(client);
        return 0;
    }
    case NODE_OPERATION_ENDPOINT_LISTEN: {
        node_server* server = operation->context;
        if (server == NULL) {
            return -ESTALE;
        }
        if (info->kind != TREVRPC_RPC_EVENT_ENDPOINT_READY || info->status != 0) {
            int error = info->status == 0 ? -EIO : info->status;
            if (operation->has_deferred) {
                (void)node_reject_deferred_native(env, operation->deferred, error, "listenMsQuic");
            }
            server->closed = true;
            return 0;
        }
        server->ready = true;
        (void)trevrpc_rpc_endpoint_get_port_v1(runtime->rpc, server->endpoint->endpoint, &server->port);
        napi_value object = NULL;
        if (server->wrapper_ref == NULL || napi_get_reference_value(env, server->wrapper_ref, &object) != napi_ok) {
            if (operation->has_deferred)
                (void)node_reject_deferred_native(env, operation->deferred, -EIO, "listenMsQuic");
            server->closing = true;
            (void)node_client_request_close(server->endpoint);
        } else {
            napi_value port;
            if (napi_create_uint32(env, server->port, &port) == napi_ok) {
                (void)napi_set_named_property(env, object, "port", port);
            }
            if (operation->has_deferred)
                (void)napi_resolve_deferred(env, operation->deferred, object);
        }
        return 0;
    }
    case NODE_OPERATION_ENDPOINT_CLOSE: {
        node_client* client = operation->context;
        if (client == NULL) {
            return -ESTALE;
        }
        if (info->kind != TREVRPC_RPC_EVENT_ENDPOINT_CLOSED && info->kind != TREVRPC_RPC_EVENT_ENDPOINT_FAILED) {
            return -EPROTO;
        }
        client->closed = true;
        if (client->server != NULL) {
            node_server* server = client->server;
            server->closed = true;
            if (server->serve_deferred != NULL && node_runtime_napi_legal(runtime)) {
                node_resolve_undefined(env, server->serve_deferred);
                server->serve_deferred = NULL;
                if (server->serve_promise_ref != NULL) {
                    (void)napi_delete_reference(env, server->serve_promise_ref);
                    server->serve_promise_ref = NULL;
                }
            }
            node_server_free_routes(env, server);
        }
        if (client->closed_deferred != NULL) {
            node_resolve_undefined(env, client->closed_deferred);
            client->closed_deferred = NULL;
            node_runtime_update_liveness(runtime);
        }
        node_client_maybe_release(client);
        node_runtime_update_liveness(runtime);
        return 0;
    }
    case NODE_OPERATION_CALL_OPEN: {
        node_call* call = operation->context;
        if (call == NULL) {
            return -ESTALE;
        }
        if (info->kind == TREVRPC_RPC_EVENT_CALL_READY && info->status == 0) {
            call->ready = true;
            if (operation->has_deferred) {
                napi_value object;
                if (call->wrapper_ref == NULL || napi_get_reference_value(env, call->wrapper_ref, &object) != napi_ok) {
                    (void)node_reject_deferred_native(env, operation->deferred, -EIO, "startStream");
                } else {
                    (void)napi_resolve_deferred(env, operation->deferred, object);
                }
                node_call_delete_wrapper_ref(call);
            }
            node_process_call(call);
            return 0;
        }
        int error = info->status == 0 ? -EIO : info->status;
        if (operation->has_deferred && call->kind == TREVRPC_RPC_KIND_UNARY) {
            call->has_response_deferred = false;
            call->response_deferred = NULL;
        }
        napi_deferred open_deferred = operation->deferred;
        bool has_open_deferred = operation->has_deferred;
        operation->has_deferred = false;
        operation->deferred = NULL;
        node_call_fail(call, error, has_open_deferred ? "startStream" : "call");
        if (has_open_deferred) {
            (void)node_reject_deferred_native(env, open_deferred, error, "startStream");
        }
        node_call_delete_wrapper_ref(call);
        return 0;
    }
    case NODE_OPERATION_CALL_ACCEPT: {
        node_call* call = operation->context;
        if (call == NULL || info->kind != TREVRPC_RPC_EVENT_CALL_ACCEPTED) {
            return call == NULL ? -ESTALE : -EPROTO;
        }
        if (info->status != 0) {
            node_call_settle_terminal(call, info->status, "accept incoming call");
            return 0;
        }
        call->accepted = true;
        call->ready = true;
        if (call->route == NULL || call->route->handler_ref == NULL) {
            call->response_submitted = false;
            return node_call_server_fail(call, TREVRPC_RPC_STATUS_UNIMPLEMENTED, "method not registered");
        }
        napi_value handler = NULL;
        napi_value call_object = NULL;
        napi_value global = NULL;
        napi_status status = napi_get_reference_value(env, call->route->handler_ref, &handler);
        if (status == napi_ok) {
            if (node_make_server_call_object(env, call) == NULL ||
                napi_get_reference_value(env, call->wrapper_ref, &call_object) != napi_ok) {
                status = napi_generic_failure;
            }
        }
        if (status == napi_ok)
            status = napi_get_global(env, &global);
        if (status == napi_ok) {
            napi_value ignored = NULL;
            status = napi_call_function(env, global, handler, 1, &call_object, &ignored);
        }
        if (status != napi_ok) {
            napi_value exception = NULL;
            (void)napi_get_and_clear_last_exception(env, &exception);
            return node_call_server_fail(call, TREVRPC_RPC_STATUS_INTERNAL, "server handler failed");
        }
        node_process_call(call);
        return 0;
    }
    case NODE_OPERATION_CALL_RESPOND: {
        node_call* call = operation->context;
        if (call == NULL ||
            (info->kind != TREVRPC_RPC_EVENT_SEND_COMPLETE && info->kind != TREVRPC_RPC_EVENT_SEND_FINISHED)) {
            return call == NULL ? -ESTALE : -EPROTO;
        }
        if (info->status != 0) {
            node_call_settle_terminal(call, info->status, "respond");
            if (operation->has_deferred) {
                (void)node_reject_deferred_native(env, operation->deferred, info->status, "respond");
            }
        } else if (operation->has_deferred) {
            node_resolve_undefined(env, operation->deferred);
        }
        node_call_complete_server_response(call, info->status == 0);
        return 0;
    }
    case NODE_OPERATION_CALL_FINISH: {
        node_call* call = operation->context;
        if (call == NULL || info->kind != TREVRPC_RPC_EVENT_CALL_FINISHED) {
            return call == NULL ? -ESTALE : -EPROTO;
        }
        if (info->status != 0) {
            node_call_settle_terminal(call, info->status, "finishStream");
            if (operation->has_deferred) {
                (void)node_reject_deferred_native(env, operation->deferred, info->status, "finishStream");
            }
        } else if (operation->has_deferred) {
            node_resolve_undefined(env, operation->deferred);
        }
        node_call_complete_server_response(call, info->status == 0);
        return 0;
    }
    case NODE_OPERATION_CALL_CLOSE: {
        node_call* call = operation->context;
        if (call == NULL) {
            return -ESTALE;
        }
        if (info->kind != TREVRPC_RPC_EVENT_CALL_CLOSED && info->kind != TREVRPC_RPC_EVENT_CALL_FAILED) {
            return -EPROTO;
        }
        call->call_closed = true;
        if (info->kind == TREVRPC_RPC_EVENT_CALL_FAILED) {
            node_call_settle_terminal(call, info->status == 0 ? -EIO : info->status, "RPC call failed");
        }
        node_call_maybe_release(call);
        return 0;
    }
    case NODE_OPERATION_STREAM_CLOSE: {
        node_call* call = operation->context;
        if (call == NULL || info->kind != TREVRPC_RPC_EVENT_STREAM_CLOSED) {
            return call == NULL ? -ESTALE : -EPROTO;
        }
        call->stream_closed = true;
        node_call_settle_terminal(call, info->status == 0 ? -EPIPE : info->status, "stream closed");
        node_call_maybe_release(call);
        return 0;
    }
    case NODE_OPERATION_SEND: {
        node_send_batch* batch = operation->context;
        if (batch == NULL || batch->call == NULL) {
            return -ESTALE;
        }
        if (info->kind != TREVRPC_RPC_EVENT_SEND_COMPLETE || info->status != 0) {
            int error = info->status == 0 ? -EIO : info->status;
            if (batch->has_deferred) {
                (void)node_reject_deferred_native(env, batch->deferred, error, "sendMessage");
            }
            node_call* call = batch->call;
            call->send_inflight = NULL;
            node_send_batch_remove(call, batch);
            node_send_batch_free(batch);
            node_call_fail(call, error, "sendMessage");
            node_call_maybe_release(call);
            return 0;
        }
        free(batch->bodies[batch->index]);
        batch->bodies[batch->index] = NULL;
        batch->index++;
        batch->operation_id = 0;
        batch->call->send_inflight = NULL;
        if (batch->index >= batch->count) {
            node_call* call = batch->call;
            node_send_batch_remove(call, batch);
            if (batch->has_deferred) {
                node_resolve_undefined(env, batch->deferred);
            }
            node_send_batch_free(batch);
            node_send_pump(call);
            node_call_maybe_release(call);
        } else {
            node_send_pump(batch->call);
            node_call_maybe_release(batch->call);
        }
        return 0;
    }
    case NODE_OPERATION_FINISH_SEND: {
        node_send_batch* batch = operation->context;
        if (batch == NULL || batch->call == NULL) {
            return -ESTALE;
        }
        int error = (info->kind == TREVRPC_RPC_EVENT_SEND_FINISHED && info->status == 0) || info->status == -EALREADY
                        ? 0
                        : (info->status == 0 ? -EIO : info->status);
        node_call* call = batch->call;
        call->send_inflight = NULL;
        if (error == 0) {
            node_send_batch_remove(call, batch);
            if (batch->has_deferred) {
                node_resolve_undefined(env, batch->deferred);
            }
            call->send_finished = true;
            node_send_batch_free(batch);
            node_call_maybe_release(call);
        } else {
            if (batch->has_deferred) {
                (void)node_reject_deferred_native(env, batch->deferred, error, "finishSend");
            }
            node_send_batch_remove(call, batch);
            node_send_batch_free(batch);
            node_call_fail(call, error, "finishSend");
            node_call_maybe_release(call);
        }
        return 0;
    }
    default:
        return 0;
    }
}

static napi_value node_rejected_native_promise(napi_env env, int error_code, const char* operation) {
    napi_deferred deferred;
    napi_value promise = node_new_promise(env, &deferred);
    if (promise == NULL) {
        return NULL;
    }
    (void)node_reject_deferred_native(env, deferred, error_code, operation);
    return promise;
}

static int node_make_call_config(const node_js_request* request, trevrpc_rpc_call_config_v1* config) {
    int result = trevrpc_rpc_call_config_v1_init(config, sizeof(*config));
    if (result != 0) {
        return result;
    }
    config->kind = request->kind;
    config->service = request->service;
    config->service_len = request->service_len;
    config->method = request->method;
    config->method_len = request->method_len;
    config->metadata = request->metadata.entries;
    config->metadata_count = (uint32_t)request->metadata.count;
    config->timeout_nanos = request->timeout_nanos;
    config->cancellation = request->cancellation;
    config->initial_message = request->body;
    config->initial_message_len = request->body_len;
    config->max_response_body_size = request->max_response_body_size;
    config->max_response_messages = request->max_response_messages;
    config->max_response_stream_body_size = request->max_response_stream_body_size;
    config->response_idle_timeout_nanos = request->response_idle_timeout_nanos;
    return 0;
}

static int node_enqueue_send_batch(node_call* call, node_send_batch* batch) {
    if (batch->finish) {
        if (call->finish_enqueued || call->send_finished) {
            return -EALREADY;
        }
        call->finish_enqueued = true;
    }
    if (call->send_tail == NULL) {
        call->send_head = batch;
    } else {
        call->send_tail->next = batch;
    }
    call->send_tail = batch;
    node_send_pump(call);
    return 0;
}

static int node_make_send_batch(napi_env env,
    node_call* call,
    napi_value value,
    bool finish,
    napi_deferred deferred,
    bool has_deferred,
    node_send_batch** out_batch) {
    node_send_batch* batch = calloc(1, sizeof(*batch));
    if (batch == NULL) {
        return -ENOMEM;
    }
    batch->call = call;
    batch->finish = finish;
    batch->deferred = deferred;
    batch->has_deferred = has_deferred;
    if (finish) {
        *out_batch = batch;
        return 0;
    }
    bool is_array = false;
    if (napi_is_array(env, value, &is_array) != napi_ok || is_array) {
        free(batch);
        return -EINVAL;
    }
    batch->count = 1;
    batch->bodies = calloc(1, sizeof(*batch->bodies));
    batch->body_lengths = calloc(1, sizeof(*batch->body_lengths));
    if (batch->bodies == NULL || batch->body_lengths == NULL) {
        node_send_batch_free(batch);
        return -ENOMEM;
    }
    int result = node_copy_js_bytes(env, value, &batch->bodies[0], &batch->body_lengths[0]);
    if (result != 0) {
        node_send_batch_free(batch);
        return result;
    }
    *out_batch = batch;
    return 0;
}

static int node_make_send_batch_array(
    napi_env env, node_call* call, napi_value value, napi_deferred deferred, node_send_batch** out_batch) {
    bool is_array = false;
    if (napi_is_array(env, value, &is_array) != napi_ok || !is_array) {
        return -EINVAL;
    }
    uint32_t count = 0;
    if (napi_get_array_length(env, value, &count) != napi_ok) {
        return -EINVAL;
    }
    node_send_batch* batch = calloc(1, sizeof(*batch));
    if (batch == NULL) {
        return -ENOMEM;
    }
    batch->call = call;
    batch->count = count;
    batch->deferred = deferred;
    batch->has_deferred = true;
    batch->bodies = count == 0 ? NULL : calloc(count, sizeof(*batch->bodies));
    batch->body_lengths = count == 0 ? NULL : calloc(count, sizeof(*batch->body_lengths));
    if (count != 0 && (batch->bodies == NULL || batch->body_lengths == NULL)) {
        node_send_batch_free(batch);
        return -ENOMEM;
    }
    for (uint32_t index = 0; index < count; ++index) {
        napi_value item;
        if (napi_get_element(env, value, index, &item) != napi_ok ||
            node_copy_js_bytes(env, item, &batch->bodies[index], &batch->body_lengths[index]) != 0) {
            node_send_batch_free(batch);
            return -EINVAL;
        }
    }
    *out_batch = batch;
    return 0;
}

static int node_buffer_server_response_body(napi_env env, node_call* call, napi_value value) {
    if (call->server_response_body_set) {
        return -EALREADY;
    }
    uint8_t* body = NULL;
    size_t body_len = 0;
    int result = node_copy_js_bytes(env, value, &body, &body_len);
    if (result != 0) {
        return result;
    }
    call->server_response_body = body;
    call->server_response_body_len = body_len;
    call->server_response_body_set = true;
    return 0;
}

static int node_start_call(napi_env env,
    node_client* client,
    node_js_request* request,
    bool stream_result,
    napi_deferred deferred,
    node_call** out_call) {
    (void)env;
    node_runtime* runtime = client->runtime;
    trevrpc_rpc_call_config_v1 config;
    int result = node_make_call_config(request, &config);
    if (result != 0) {
        return result;
    }
    uint64_t operation_id = node_operation_allocate(runtime);
    if (operation_id == TREVRPC_RPC_OPERATION_ID_NONE) {
        return -EOVERFLOW;
    }
    node_call* call = calloc(1, sizeof(*call));
    if (call == NULL) {
        return -ENOMEM;
    }
    call->runtime = runtime;
    call->client = client;
    call->cancellation = request->cancellation;
    call->kind = request->kind;
    call->response_deferred = deferred;
    call->has_response_deferred = !stream_result;
    call->call_subject = node_subject_allocate(TREVRPC_RPC_OBJECT_CALL, (node_handle_key){0, 0, 0});
    call->stream_subject = node_subject_allocate(TREVRPC_RPC_OBJECT_STREAM, (node_handle_key){0, 0, 0});
    if (call->call_subject == NULL || call->stream_subject == NULL) {
        free(call->call_subject);
        free(call->stream_subject);
        free(call);
        return -ENOMEM;
    }
    node_operation* operation = node_operation_add_id(runtime,
        operation_id,
        TREVRPC_RPC_OBJECT_CALL,
        (node_handle_key){0, 0, 0},
        NODE_OPERATION_CALL_OPEN,
        call,
        deferred,
        stream_result);
    if (operation == NULL) {
        free(call->call_subject);
        free(call->stream_subject);
        free(call);
        return -ENOMEM;
    }
    result =
        trevrpc_rpc_call_open_v1(runtime->rpc, client->endpoint, &config, operation_id, &call->call, &call->stream);
    if (result != 0) {
        node_operation_remove(runtime, operation);
        free(call->call_subject);
        free(call->stream_subject);
        free(call);
        return result;
    }
    if (node_handle_is_null_call(call->call) || node_handle_is_null_stream(call->stream)) {
        operation->subject_bound = false;
        call->failed = true;
        call->close_abort = true;
        (void)node_reject_deferred_native(runtime->env, deferred, -EPROTO, "call");
        return -EPROTO;
    }
    if (node_call_register_handles(call) != 0) {
        call->failed = true;
        call->close_abort = true;
        (void)node_call_request_close(call);
        return -ENOMEM;
    }
    operation->subject = call->call_subject->key;
    node_test_trace_key(runtime, "key-rebind", operation->subject_kind, operation->subject);
    operation->subject_bound = true;
    *out_call = call;
    return 0;
}

static napi_value node_connect_msquic(napi_env env, napi_callback_info info) {
    napi_value argv[2] = {NULL, NULL};
    napi_value this_value;
    size_t argc = 2;
    if (napi_get_cb_info(env, info, &argc, argv, &this_value, NULL) != napi_ok || argc < 1) {
        return node_rejected_native_promise(env, -EINVAL, "connectMsQuic");
    }
    node_runtime* runtime = NULL;
    int result = node_runtime_get(env, &runtime);
    if (result != 0) {
        return node_rejected_native_promise(env, result, "connectMsQuic");
    }
    node_endpoint_config_storage endpoint;
    result = node_parse_endpoint_config(env, argv[0], &endpoint, false);
    if (result != 0) {
        return node_rejected_native_promise(env, result, "connectMsQuic");
    }
    trevrpc_rpc_cancellation_v1 cancellation;
    result = node_parse_cancellation(env, argc > 1 ? argv[1] : NULL, runtime, &cancellation);
    if (result != 0) {
        node_endpoint_config_free(&endpoint);
        return node_rejected_native_promise(env, result, "connectMsQuic");
    }
    napi_deferred deferred;
    napi_value promise = node_new_promise(env, &deferred);
    if (promise == NULL) {
        node_endpoint_config_free(&endpoint);
        return NULL;
    }
    node_client* client = calloc(1, sizeof(*client));
    if (client == NULL) {
        node_endpoint_config_free(&endpoint);
        (void)node_reject_deferred_native(env, deferred, -ENOMEM, "connectMsQuic");
        return promise;
    }
    client->runtime = runtime;
    node_subject* reserved_subject = node_subject_allocate(TREVRPC_RPC_OBJECT_ENDPOINT, (node_handle_key){0, 0, 0});
    if (reserved_subject == NULL) {
        free(client);
        node_endpoint_config_free(&endpoint);
        (void)node_reject_deferred_native(env, deferred, -ENOMEM, "connectMsQuic");
        return promise;
    }
    uint64_t operation_id = node_operation_allocate(runtime);
    if (operation_id == TREVRPC_RPC_OPERATION_ID_NONE) {
        free(reserved_subject);
        free(client);
        node_endpoint_config_free(&endpoint);
        (void)node_reject_deferred_native(env, deferred, -EOVERFLOW, "connectMsQuic");
        return promise;
    }
    node_operation* operation = node_operation_add_id(runtime,
        operation_id,
        TREVRPC_RPC_OBJECT_ENDPOINT,
        (node_handle_key){0, 0, 0},
        NODE_OPERATION_ENDPOINT_START,
        client,
        deferred,
        true);
    if (operation == NULL) {
        free(reserved_subject);
        free(client);
        node_endpoint_config_free(&endpoint);
        (void)node_reject_deferred_native(env, deferred, -ENOMEM, "connectMsQuic");
        return promise;
    }
    result = trevrpc_rpc_msquic_endpoint_start_v1(runtime->rpc, &endpoint.config, operation_id, &client->endpoint);
    node_endpoint_config_free(&endpoint);
    if (result != 0) {
        node_operation_remove(runtime, operation);
        free(reserved_subject);
        free(client);
        (void)node_reject_deferred_native(env, deferred, result, "connectMsQuic");
        return promise;
    }
    reserved_subject->key = node_key_from_endpoint(client->endpoint);
    reserved_subject->core = client;
    client->subject = reserved_subject;
    node_registry_insert(runtime, reserved_subject);
    if (cancellation.owner != 0 || cancellation.slot != 0 || cancellation.generation != 0) {
        node_client_attach_cancellation(
            client, node_registry_find(&runtime->cancellations, node_key_from_cancellation(cancellation)));
    }
    operation->subject = reserved_subject->key;
    node_test_trace_key(runtime, "key-rebind", operation->subject_kind, operation->subject);
    operation->subject_bound = true;
    napi_value object = node_make_client_object(env, client);
    if (object == NULL) {
        client->invalidated = true;
        operation->has_deferred = false;
        operation->deferred = NULL;
        (void)node_reject_deferred_native(env, deferred, -EIO, "connectMsQuic");
        (void)node_client_request_close(client);
        return promise;
    }
    (void)object;
    return promise;
}

static napi_value node_client_call(napi_env env, napi_callback_info info) {
    napi_value argv[2] = {NULL, NULL};
    napi_value this_value;
    size_t argc = 2;
    if (napi_get_cb_info(env, info, &argc, argv, &this_value, NULL) != napi_ok || argc < 1) {
        return node_rejected_native_promise(env, -EINVAL, "call");
    }
    node_client* client = NULL;
    if (napi_unwrap(env, this_value, (void**)&client) != napi_ok || !node_client_usable(client) || !client->ready ||
        client->closed) {
        return node_rejected_native_promise(env, -EPIPE, "call");
    }
    node_js_request request;
    int result = node_parse_request(env, argv[0], TREVRPC_RPC_KIND_UNARY, &request);
    if (result != 0) {
        return node_rejected_native_promise(env, result, "call");
    }
    result = node_parse_cancellation(env, argc > 1 ? argv[1] : NULL, client->runtime, &request.cancellation);
    if (result != 0) {
        node_request_free(&request);
        return node_rejected_native_promise(env, result, "call");
    }
    napi_deferred deferred;
    napi_value promise = node_new_promise(env, &deferred);
    if (promise == NULL) {
        node_request_free(&request);
        return NULL;
    }
    node_call* call = NULL;
    result = node_start_call(env, client, &request, false, deferred, &call);
    node_request_free(&request);
    if (result != 0) {
        (void)node_reject_deferred_native(env, deferred, result, "call");
    }
    return promise;
}

static napi_value node_client_start_stream(napi_env env, napi_callback_info info) {
    napi_value argv[2] = {NULL, NULL};
    napi_value this_value;
    size_t argc = 2;
    if (napi_get_cb_info(env, info, &argc, argv, &this_value, NULL) != napi_ok || argc < 1) {
        return node_rejected_native_promise(env, -EINVAL, "startStream");
    }
    node_client* client = NULL;
    if (napi_unwrap(env, this_value, (void**)&client) != napi_ok || !node_client_usable(client) || !client->ready ||
        client->closed) {
        return node_rejected_native_promise(env, -EPIPE, "startStream");
    }
    node_js_request request;
    int result = node_parse_request(env, argv[0], TREVRPC_RPC_KIND_SERVER_STREAMING, &request);
    if (result != 0) {
        return node_rejected_native_promise(env, result, "startStream");
    }
    result = node_parse_cancellation(env, argc > 1 ? argv[1] : NULL, client->runtime, &request.cancellation);
    if (result != 0) {
        node_request_free(&request);
        return node_rejected_native_promise(env, result, "startStream");
    }
    napi_deferred deferred;
    napi_value promise = node_new_promise(env, &deferred);
    if (promise == NULL) {
        node_request_free(&request);
        return NULL;
    }
    node_call* call = NULL;
    result = node_start_call(env, client, &request, true, deferred, &call);
    node_request_free(&request);
    if (result != 0) {
        (void)node_reject_deferred_native(env, deferred, result, "startStream");
        return promise;
    }
    napi_value stream = node_make_stream_object(env, call);
    if (stream == NULL) {
        node_call_clear_open_deferred(call);
        node_call_fail(call, -ENOMEM, "startStream");
        (void)node_reject_deferred_native(env, deferred, -ENOMEM, "startStream");
        return promise;
    }
    return promise;
}

static napi_value node_stream_send_message(napi_env env, napi_callback_info info) {
    napi_value argv[1] = {NULL};
    napi_value this_value;
    size_t argc = 1;
    if (napi_get_cb_info(env, info, &argc, argv, &this_value, NULL) != napi_ok || argc < 1) {
        return node_rejected_native_promise(env, -EINVAL, "sendMessage");
    }
    node_call* call = NULL;
    if (napi_unwrap(env, this_value, (void**)&call) != napi_ok || !node_call_usable(call) || !call->ready ||
        call->failed || call->send_finished || call->finish_enqueued) {
        return node_rejected_native_promise(env, -EPIPE, "sendMessage");
    }
    napi_deferred deferred;
    napi_value promise = node_new_promise(env, &deferred);
    if (promise == NULL) {
        return NULL;
    }
    if (call->server_side && call->kind == TREVRPC_RPC_KIND_CLIENT_STREAMING) {
        int result = node_buffer_server_response_body(env, call, argv[0]);
        if (result == 0) {
            node_resolve_undefined(env, deferred);
        } else {
            (void)node_reject_deferred_native(env, deferred, result, "sendMessage");
        }
        return promise;
    }
    node_send_batch* batch = NULL;
    int result = node_make_send_batch(env, call, argv[0], false, deferred, true, &batch);
    if (result == 0) {
        result = node_enqueue_send_batch(call, batch);
    }
    if (result != 0) {
        if (batch != NULL) {
            node_send_batch_free(batch);
        }
        (void)node_reject_deferred_native(env, deferred, result, "sendMessage");
    }
    return promise;
}

static napi_value node_stream_send_messages(napi_env env, napi_callback_info info) {
    napi_value argv[1] = {NULL};
    napi_value this_value;
    size_t argc = 1;
    if (napi_get_cb_info(env, info, &argc, argv, &this_value, NULL) != napi_ok || argc < 1) {
        return node_rejected_native_promise(env, -EINVAL, "sendMessages");
    }
    node_call* call = NULL;
    if (napi_unwrap(env, this_value, (void**)&call) != napi_ok || !node_call_usable(call) || !call->ready ||
        call->failed || call->send_finished || call->finish_enqueued) {
        return node_rejected_native_promise(env, -EPIPE, "sendMessages");
    }
    napi_deferred deferred;
    napi_value promise = node_new_promise(env, &deferred);
    if (promise == NULL) {
        return NULL;
    }
    if (call->server_side && call->kind == TREVRPC_RPC_KIND_CLIENT_STREAMING) {
        bool is_array = false;
        uint32_t count = 0;
        int result = napi_is_array(env, argv[0], &is_array) == napi_ok && is_array
                         ? napi_get_array_length(env, argv[0], &count) == napi_ok ? 0 : -EINVAL
                         : -EINVAL;
        if (result == 0 && count > 1) {
            result = -EINVAL;
        }
        if (result == 0 && count == 1) {
            napi_value item;
            result = napi_get_element(env, argv[0], 0, &item) == napi_ok
                         ? node_buffer_server_response_body(env, call, item)
                         : -EINVAL;
        }
        if (result == 0) {
            node_resolve_undefined(env, deferred);
        } else {
            (void)node_reject_deferred_native(env, deferred, result, "sendMessages");
        }
        return promise;
    }
    node_send_batch* batch = NULL;
    int result = node_make_send_batch_array(env, call, argv[0], deferred, &batch);
    if (result == 0 && batch->count == 0) {
        free(batch->bodies);
        free(batch->body_lengths);
        free(batch);
        node_resolve_undefined(env, deferred);
        return promise;
    }
    if (result == 0) {
        result = node_enqueue_send_batch(call, batch);
    }
    if (result != 0) {
        if (batch != NULL) {
            node_send_batch_free(batch);
        }
        (void)node_reject_deferred_native(env, deferred, result, "sendMessages");
    }
    return promise;
}

static napi_value node_stream_finish_send(napi_env env, napi_callback_info info) {
    napi_value this_value;
    size_t argc = 0;
    if (napi_get_cb_info(env, info, &argc, NULL, &this_value, NULL) != napi_ok) {
        return node_rejected_native_promise(env, -EIO, "finishSend");
    }
    node_call* call = NULL;
    if (napi_unwrap(env, this_value, (void**)&call) != napi_ok || !node_call_usable(call) || !call->ready ||
        call->failed) {
        return node_rejected_native_promise(env, -EPIPE, "finishSend");
    }
    if (call->send_finished || call->finish_enqueued) {
        return node_rejected_native_promise(env, -EALREADY, "finishSend");
    }
    napi_deferred deferred;
    napi_value promise = node_new_promise(env, &deferred);
    if (promise == NULL) {
        return NULL;
    }
    node_send_batch* batch = calloc(1, sizeof(*batch));
    if (batch == NULL) {
        (void)node_reject_deferred_native(env, deferred, -ENOMEM, "finishSend");
        return promise;
    }
    batch->call = call;
    batch->finish = true;
    batch->deferred = deferred;
    batch->has_deferred = true;
    int result = node_enqueue_send_batch(call, batch);
    if (result != 0) {
        node_send_batch_free(batch);
        (void)node_reject_deferred_native(env, deferred, result, "finishSend");
    }
    return promise;
}

static napi_value node_stream_recv_common(
    napi_env env, napi_callback_info info, bool body_batch, bool many, uint32_t default_max) {
    napi_value argv[1] = {NULL};
    napi_value this_value;
    size_t argc = 1;
    if (napi_get_cb_info(env, info, &argc, argv, &this_value, NULL) != napi_ok) {
        return node_rejected_native_promise(env, -EIO, "receive");
    }
    node_call* call = NULL;
    if (napi_unwrap(env, this_value, (void**)&call) != napi_ok || call == NULL) {
        return node_rejected_native_promise(env, -EPIPE, "receive");
    }
    bool cached_eof = !call->server_side && call->kind != TREVRPC_RPC_KIND_UNARY && call->receive_succeeded;
    if (!cached_eof && (!node_call_usable(call) || call->failed)) {
        return node_rejected_native_promise(env, -EPIPE, "receive");
    }
    uint32_t max_items = default_max;
    if (argc > 0 && argv[0] != NULL) {
        uint32_t supplied = 0;
        if (napi_get_value_uint32(env, argv[0], &supplied) == napi_ok && supplied > 0) {
            max_items = supplied;
        }
    }
    napi_deferred deferred;
    napi_value promise = node_new_promise(env, &deferred);
    if (promise == NULL) {
        return NULL;
    }
    if (cached_eof) {
        napi_value terminal = NULL;
        if (body_batch) {
            if (napi_get_null(env, &terminal) != napi_ok) {
                (void)node_reject_deferred_native(env, deferred, -ENOMEM, "receive");
                return promise;
            }
        } else if (!many) {
            if (napi_get_null(env, &terminal) != napi_ok) {
                (void)node_reject_deferred_native(env, deferred, -ENOMEM, "receive");
                return promise;
            }
        } else {
            napi_value null_value = NULL;
            if (napi_create_array(env, &terminal) != napi_ok || napi_get_null(env, &null_value) != napi_ok ||
                napi_set_element(env, terminal, 0, null_value) != napi_ok) {
                (void)node_reject_deferred_native(env, deferred, -ENOMEM, "receive");
                return promise;
            }
        }
        (void)napi_resolve_deferred(env, deferred, terminal);
        return promise;
    }
    node_receive_waiter* waiter = calloc(1, sizeof(*waiter));
    if (waiter == NULL) {
        (void)node_reject_deferred_native(env, deferred, -ENOMEM, "receive");
        return promise;
    }
    waiter->deferred = deferred;
    waiter->max_items = max_items;
    waiter->body_batch = body_batch;
    if (call->waiter_tail == NULL) {
        call->waiter_head = waiter;
    } else {
        call->waiter_tail->next = waiter;
    }
    call->waiter_tail = waiter;
    node_process_call(call);
    return promise;
}

static napi_value node_stream_recv(napi_env env, napi_callback_info info) {
    return node_stream_recv_common(env, info, false, false, 1);
}

static napi_value node_stream_recv_many(napi_env env, napi_callback_info info) {
    return node_stream_recv_common(env, info, false, true, 32);
}

static napi_value node_stream_recv_body_batch(napi_env env, napi_callback_info info) {
    return node_stream_recv_common(env, info, true, false, 32);
}

static int node_client_request_close(node_client* client) {
    if (client == NULL || client->runtime == NULL || client->runtime->rpc == NULL || client->closed ||
        client->close_submitted || node_handle_is_null_endpoint(client->endpoint)) {
        return 0;
    }
    for (unsigned attempt = 0; attempt < 8u; ++attempt) {
        uint64_t id = node_operation_allocate(client->runtime);
        if (id == TREVRPC_RPC_OPERATION_ID_NONE) {
            client->close_retry_pending = true;
            node_runtime_start_failure_progress(client->runtime);
            return 0;
        }
        node_operation* operation = node_operation_add_id(client->runtime,
            id,
            TREVRPC_RPC_OBJECT_ENDPOINT,
            node_key_from_endpoint(client->endpoint),
            NODE_OPERATION_ENDPOINT_CLOSE,
            client,
            NULL,
            false);
        if (operation == NULL) {
            client->close_retry_pending = true;
            node_runtime_start_failure_progress(client->runtime);
            return 0;
        }
        int result = trevrpc_rpc_endpoint_close(client->runtime->rpc, client->endpoint, id);
        if (result == 0) {
            client->close_submitted = true;
            client->close_retry_pending = false;
            return 0;
        }
        if (result == -EALREADY) {
            node_operation_remove(client->runtime, operation);
            client->closed = true;
            client->close_submitted = true;
            client->close_retry_pending = false;
            if (client->closed_deferred != NULL && node_runtime_napi_legal(client->runtime)) {
                node_resolve_undefined(client->runtime->env, client->closed_deferred);
                client->closed_deferred = NULL;
                node_runtime_update_liveness(client->runtime);
            }
            node_runtime* runtime = client->runtime;
            node_client_maybe_release(client);
            node_runtime_update_liveness(runtime);
            return 0;
        }
        node_operation_remove(client->runtime, operation);
        if (!node_close_retryable(result)) {
            client->close_retry_pending = false;
            return result;
        }
        client->close_retry_pending = true;
    }
    if (client->close_retry_pending) {
        node_runtime_start_failure_progress(client->runtime);
    }
    return 0;
}

static napi_value node_make_client_object(napi_env env, node_client* client) {
    napi_value object;
    napi_value method;
    napi_value closed;
    if (napi_create_object(env, &object) != napi_ok ||
        napi_create_function(env, "call", NAPI_AUTO_LENGTH, node_client_call, NULL, &method) != napi_ok ||
        napi_set_named_property(env, object, "call", method) != napi_ok ||
        napi_create_function(env, "startStream", NAPI_AUTO_LENGTH, node_client_start_stream, NULL, &method) !=
            napi_ok ||
        napi_set_named_property(env, object, "startStream", method) != napi_ok ||
        napi_create_function(env, "close", NAPI_AUTO_LENGTH, node_client_close, NULL, &method) != napi_ok ||
        napi_set_named_property(env, object, "close", method) != napi_ok ||
        napi_create_function(
            env, "createCancellation", NAPI_AUTO_LENGTH, node_client_create_cancellation, NULL, &method) != napi_ok ||
        napi_set_named_property(env, object, "createCancellation", method) != napi_ok) {
        return NULL;
    }
    closed = node_new_promise(env, &client->closed_deferred);
    if (closed == NULL || napi_set_named_property(env, object, "closed", closed) != napi_ok) {
        if (client->closed_deferred != NULL) {
            (void)node_reject_deferred_native(env, client->closed_deferred, -EIO, "client.close");
            client->closed_deferred = NULL;
        }
        return NULL;
    }
    if (napi_create_reference(env, object, 1, &client->wrapper_ref) != napi_ok) {
        (void)node_reject_deferred_native(env, client->closed_deferred, -EIO, "client.close");
        client->closed_deferred = NULL;
        client->wrapper_ref = NULL;
        return NULL;
    }
    if (napi_wrap(env, object, client, node_client_finalizer, NULL, NULL) != napi_ok) {
        (void)napi_delete_reference(env, client->wrapper_ref);
        client->wrapper_ref = NULL;
        (void)node_reject_deferred_native(env, client->closed_deferred, -EIO, "client.close");
        client->closed_deferred = NULL;
        return NULL;
    }
    client->wrapper_alive = true;
    return object;
}

static napi_value node_make_stream_object(napi_env env, node_call* call) {
    napi_value object;
    napi_value method;
    if (napi_create_object(env, &object) != napi_ok ||
        napi_create_function(env, "sendMessage", NAPI_AUTO_LENGTH, node_stream_send_message, NULL, &method) !=
            napi_ok ||
        napi_set_named_property(env, object, "sendMessage", method) != napi_ok ||
        napi_create_function(env, "sendMessages", NAPI_AUTO_LENGTH, node_stream_send_messages, NULL, &method) !=
            napi_ok ||
        napi_set_named_property(env, object, "sendMessages", method) != napi_ok ||
        napi_create_function(env, "finishSend", NAPI_AUTO_LENGTH, node_stream_finish_send, NULL, &method) != napi_ok ||
        napi_set_named_property(env, object, "finishSend", method) != napi_ok ||
        napi_create_function(env, "recv", NAPI_AUTO_LENGTH, node_stream_recv, NULL, &method) != napi_ok ||
        napi_set_named_property(env, object, "recv", method) != napi_ok ||
        napi_create_function(env, "recvMany", NAPI_AUTO_LENGTH, node_stream_recv_many, NULL, &method) != napi_ok ||
        napi_set_named_property(env, object, "recvMany", method) != napi_ok ||
        napi_create_function(env, "recvBodyBatch", NAPI_AUTO_LENGTH, node_stream_recv_body_batch, NULL, &method) !=
            napi_ok ||
        napi_set_named_property(env, object, "recvBodyBatch", method) != napi_ok ||
        napi_create_function(env, "close", NAPI_AUTO_LENGTH, node_stream_close, NULL, &method) != napi_ok ||
        napi_set_named_property(env, object, "close", method) != napi_ok) {
        return NULL;
    }
    if (napi_create_reference(env, object, 1, &call->wrapper_ref) != napi_ok) {
        call->wrapper_ref = NULL;
        return NULL;
    }
    if (napi_wrap(env, object, call, node_call_finalizer, NULL, NULL) != napi_ok) {
        (void)napi_delete_reference(env, call->wrapper_ref);
        call->wrapper_ref = NULL;
        return NULL;
    }
    call->wrapper_alive = true;
    return object;
}

static int node_parse_cancellation(
    napi_env env, napi_value value, node_runtime* runtime, trevrpc_rpc_cancellation_v1* out) {
    memset(out, 0, sizeof(*out));
    if (value == NULL) {
        return 0;
    }
    napi_valuetype value_type;
    if (napi_typeof(env, value, &value_type) != napi_ok || value_type == napi_undefined || value_type == napi_null) {
        return 0;
    }
    node_cancellation* cancellation = NULL;
    if (napi_unwrap(env, value, (void**)&cancellation) != napi_ok || cancellation == NULL ||
        cancellation->runtime != runtime || cancellation->subject == NULL) {
        return -EINVAL;
    }
    *out = cancellation->handle;
    return 0;
}

static int node_call_register_handles(node_call* call) {
    node_runtime* runtime = call->runtime;
    if (call->call_subject == NULL) {
        call->call_subject = node_registry_add(runtime, TREVRPC_RPC_OBJECT_CALL, node_key_from_call(call->call));
    } else {
        call->call_subject->key = node_key_from_call(call->call);
        node_registry_insert(runtime, call->call_subject);
    }
    if (call->stream_subject == NULL) {
        call->stream_subject =
            node_registry_add(runtime, TREVRPC_RPC_OBJECT_STREAM, node_key_from_stream(call->stream));
    } else {
        call->stream_subject->key = node_key_from_stream(call->stream);
        node_registry_insert(runtime, call->stream_subject);
    }
    if (call->call_subject == NULL || call->stream_subject == NULL) {
        return -ENOMEM;
    }
    call->call_subject->core = call;
    call->stream_subject->core = call;
    return 0;
}

static void node_client_finalizer(napi_env env, void* data, void* hint) {
    (void)env;
    (void)hint;
    node_client* client = data;
    if (client == NULL) {
        return;
    }
    client->wrapper_alive = false;
    if (client->wrapper_ref != NULL && (client->runtime == NULL || !client->runtime->environment_teardown)) {
        (void)napi_delete_reference(env, client->wrapper_ref);
        client->wrapper_ref = NULL;
    }
    if (client->runtime == NULL) {
        free(client);
        return;
    }
    if (client->invalidated || client->runtime->cleanup_started) {
        return;
    }
    (void)node_client_request_close(client);
}

static void node_call_finalizer(napi_env env, void* data, void* hint) {
    (void)env;
    (void)hint;
    node_call* call = data;
    if (call == NULL) {
        return;
    }
    call->wrapper_alive = false;
    if (call->wrapper_ref != NULL && (call->runtime == NULL || !call->runtime->environment_teardown)) {
        (void)napi_delete_reference(env, call->wrapper_ref);
        call->wrapper_ref = NULL;
    }
    if (call->runtime == NULL) {
        free(call);
        return;
    }
    if (call->invalidated || call->runtime->cleanup_started) {
        return;
    }
    call->close_abort = true;
    node_call_settle_terminal(call, -ECANCELED, "stream finalizer");
    node_call_request_close(call);
    node_call_maybe_release(call);
}

static napi_value node_client_create_cancellation(napi_env env, napi_callback_info info) {
    (void)info;
    return node_create_cancellation(env, info);
}

static napi_value node_client_close(napi_env env, napi_callback_info info) {
    napi_value this_value;
    size_t argc = 0;
    if (napi_get_cb_info(env, info, &argc, NULL, &this_value, NULL) != napi_ok) {
        return node_throw_error(env, -EIO, "client.close");
    }
    node_client* client = NULL;
    if (napi_unwrap(env, this_value, (void**)&client) != napi_ok || client == NULL) {
        return node_throw_error(env, -ESTALE, "client.close");
    }
    if (client->closed || client->runtime == NULL) {
        napi_value undefined;
        (void)napi_get_undefined(env, &undefined);
        return undefined;
    }
    if (!node_client_usable(client)) {
        return node_throw_error(env, -ESTALE, "client.close");
    }
    int result = node_client_request_close(client);
    if (result != 0) {
        return node_throw_error(env, result, "client.close");
    }
    napi_value undefined;
    (void)napi_get_undefined(env, &undefined);
    return undefined;
}

static napi_value node_stream_close(napi_env env, napi_callback_info info) {
    napi_value this_value;
    size_t argc = 0;
    if (napi_get_cb_info(env, info, &argc, NULL, &this_value, NULL) != napi_ok) {
        return node_throw_error(env, -EIO, "stream.close");
    }
    node_call* call = NULL;
    if (napi_unwrap(env, this_value, (void**)&call) != napi_ok || call == NULL) {
        return node_throw_error(env, -ESTALE, "stream.close");
    }
    if (call->runtime == NULL || call->terminal_settled) {
        napi_value undefined;
        (void)napi_get_undefined(env, &undefined);
        return undefined;
    }
    if (!node_call_usable(call)) {
        return node_throw_error(env, -ESTALE, "stream.close");
    }
    call->close_abort = true;
    node_call_settle_terminal(call, -ECANCELED, "stream.close");
    node_call_request_close(call);
    napi_value undefined;
    (void)napi_get_undefined(env, &undefined);
    return undefined;
}

typedef struct node_server_response_input {
    node_status_value status;
    node_js_metadata metadata;
    uint8_t* body;
    size_t body_len;
} node_server_response_input;

static void node_server_response_input_free(node_server_response_input* input) {
    if (input == NULL) {
        return;
    }
    node_status_free(&input->status);
    node_metadata_free(&input->metadata);
    free(input->body);
    memset(input, 0, sizeof(*input));
}

static int node_parse_server_response(napi_env env, napi_value value, node_server_response_input* out) {
    memset(out, 0, sizeof(*out));
    if (value == NULL || napi_typeof(env, value, &(napi_valuetype){0}) != napi_ok) {
        return -EINVAL;
    }
    napi_valuetype type;
    if (napi_typeof(env, value, &type) != napi_ok || type != napi_object) {
        return -EINVAL;
    }
    napi_value item;
    bool present = false;
    uint32_t code = TREVRPC_RPC_STATUS_OK;
    if (node_get_named_value(env, value, "status", &item, &present) != 0) {
        return -EINVAL;
    }
    if (present) {
        napi_valuetype property_type;
        if (napi_typeof(env, item, &property_type) != napi_ok) {
            return -EINVAL;
        }
        if (property_type == napi_undefined || property_type == napi_null) {
            present = false;
        } else if (napi_get_value_uint32(env, item, &code) != napi_ok) {
            return -EINVAL;
        }
    }
    if (!present && node_get_named_value(env, value, "code", &item, &present) != 0) {
        return -EINVAL;
    }
    if (present) {
        napi_valuetype property_type;
        if (napi_typeof(env, item, &property_type) != napi_ok) {
            return -EINVAL;
        }
        if (property_type == napi_undefined || property_type == napi_null) {
            present = false;
        } else if (napi_get_value_uint32(env, item, &code) != napi_ok) {
            return -EINVAL;
        }
    }
    if (code > TREVRPC_RPC_STATUS_UNAUTHENTICATED) {
        return -EINVAL;
    }
    out->status.code = code;
    if (node_get_named_value(env, value, "message", &item, &present) != 0) {
        return -EINVAL;
    }
    if (present && node_copy_js_string(env, item, &out->status.message, &(uint32_t){0}) != 0) {
        return -EINVAL;
    }
    if (node_get_named_value(env, value, "metadata", &item, &present) != 0 ||
        (present && node_copy_js_metadata(env, item, &out->metadata) != 0)) {
        node_server_response_input_free(out);
        return -EINVAL;
    }
    out->status.metadata = NULL;
    out->status.metadata_count = 0;
    if (node_get_named_value(env, value, "body", &item, &present) != 0) {
        node_server_response_input_free(out);
        return -EINVAL;
    }
    if (out->status.code == TREVRPC_RPC_STATUS_OK) {
        if (!present || node_copy_js_bytes(env, item, &out->body, &out->body_len) != 0) {
            node_server_response_input_free(out);
            return -EINVAL;
        }
    }
    return 0;
}

static node_server_route* node_server_find_route(
    node_server* server, const char* service, const char* method, uint32_t kind) {
    for (node_server_route* route = server == NULL ? NULL : server->routes; route != NULL; route = route->next) {
        if (route->kind == kind && strcmp(route->service, service) == 0 && strcmp(route->method, method) == 0) {
            return route;
        }
    }
    return NULL;
}

static int node_make_server_call_request(napi_env env, node_call* call, napi_value* out) {
    napi_value object;
    napi_value value;
    if (napi_create_object(env, &object) != napi_ok ||
        napi_create_string_utf8(env, call->route == NULL ? "" : call->route->service, NAPI_AUTO_LENGTH, &value) !=
            napi_ok ||
        napi_set_named_property(env, object, "service", value) != napi_ok ||
        napi_create_string_utf8(env, call->route == NULL ? "" : call->route->method, NAPI_AUTO_LENGTH, &value) !=
            napi_ok ||
        napi_set_named_property(env, object, "method", value) != napi_ok ||
        napi_create_uint32(env, call->kind, &value) != napi_ok ||
        napi_set_named_property(env, object, "kind", value) != napi_ok ||
        napi_create_uint32(env, 1, &value) != napi_ok ||
        napi_set_named_property(env, object, "version", value) != napi_ok) {
        return -ENOMEM;
    }
    napi_value body = NULL;
    napi_value metadata = NULL;
    if (call->receive_head != NULL) {
        trevrpc_rpc_receive_info_v1 info;
        if (node_call_receive_info(call->runtime, call->receive_head->receive, &info) != 0 ||
            node_make_bytes_value(env, info.data, (size_t)info.data_len, &body) != 0 ||
            node_make_metadata_entries_value(env, info.metadata, info.metadata_count, &metadata) != 0) {
            return -EPROTO;
        }
    } else if (node_make_bytes_value(env, NULL, 0, &body) != 0 ||
               node_make_metadata_entries_value(env, NULL, 0, &metadata) != 0) {
        return -ENOMEM;
    }
    if (napi_set_named_property(env, object, "body", body) != napi_ok ||
        napi_set_named_property(env, object, "metadata", metadata) != napi_ok) {
        return -ENOMEM;
    }
    trevrpc_rpc_call_context_info_v1 context_info;
    if (trevrpc_rpc_call_context_info_v1_init(&context_info, sizeof(context_info)) != 0 ||
        trevrpc_rpc_call_get_context_v1(call->runtime->rpc, call->call, &context_info) != 0) {
        return -EIO;
    }
    napi_value context;
    napi_value boolean;
    napi_value remaining;
    if (napi_create_object(env, &context) != napi_ok ||
        napi_get_boolean(env, (context_info.flags & TREVRPC_RPC_CALL_CONTEXT_HAS_DEADLINE) != 0, &boolean) != napi_ok ||
        napi_set_named_property(env, context, "hasDeadline", boolean) != napi_ok ||
        napi_create_bigint_uint64(env, context_info.time_remaining_nanos, &remaining) != napi_ok ||
        napi_set_named_property(env, context, "timeRemainingNanos", remaining) != napi_ok ||
        napi_get_boolean(env, (context_info.flags & TREVRPC_RPC_CALL_CONTEXT_CANCELLED) != 0, &boolean) != napi_ok ||
        napi_set_named_property(env, context, "cancelled", boolean) != napi_ok ||
        napi_set_named_property(env, object, "context", context) != napi_ok) {
        return -ENOMEM;
    }
    *out = object;
    return 0;
}

static napi_value node_server_call_respond(napi_env env, napi_callback_info info) {
    napi_value argv[1] = {NULL};
    napi_value this_value;
    size_t argc = 1;
    if (napi_get_cb_info(env, info, &argc, argv, &this_value, NULL) != napi_ok || argc < 1) {
        return node_rejected_native_promise(env, -EINVAL, "respond");
    }
    node_call* call = NULL;
    if (napi_unwrap(env, this_value, (void**)&call) != napi_ok || !node_call_usable(call) || !call->server_side ||
        call->response_submitted || call->terminal_settled) {
        return node_rejected_native_promise(env, -EPIPE, "respond");
    }
    napi_deferred deferred;
    napi_value promise = node_new_promise(env, &deferred);
    if (promise == NULL) {
        return NULL;
    }
    node_server_response_input input;
    int result = node_parse_server_response(env, argv[0], &input);
    if (result == 0) {
        uint64_t id = node_operation_allocate(call->runtime);
        node_operation* operation = id == TREVRPC_RPC_OPERATION_ID_NONE ? NULL
                                                                        : node_operation_add_id(call->runtime,
                                                                              id,
                                                                              TREVRPC_RPC_OBJECT_STREAM,
                                                                              node_key_from_stream(call->stream),
                                                                              NODE_OPERATION_CALL_RESPOND,
                                                                              call,
                                                                              deferred,
                                                                              true);
        if (operation == NULL) {
            result = -ENOMEM;
        } else {
            trevrpc_rpc_status_v1 status;
            result = trevrpc_rpc_status_v1_init(&status, sizeof(status));
            if (result == 0) {
                status.code = input.status.code;
                status.message = input.status.message;
                status.message_len = (input.status.message == NULL ? 0u : (uint32_t)strlen(input.status.message));
                status.metadata = input.metadata.entries;
                status.metadata_count = (uint32_t)input.metadata.count;
                const uint8_t* body = input.status.code == TREVRPC_RPC_STATUS_OK
                                          ? (input.body_len == 0 ? (const uint8_t*)"" : input.body)
                                          : NULL;
                result =
                    trevrpc_rpc_call_respond_copy_v1(call->runtime->rpc, call->call, id, &status, body, input.body_len);
            }
            if (result == 0) {
                call->response_submitted = true;
            } else {
                node_operation_remove(call->runtime, operation);
            }
        }
    }
    node_server_response_input_free(&input);
    if (result != 0) {
        (void)node_reject_deferred_native(env, deferred, result, "respond");
    }
    return promise;
}

static napi_value node_server_call_finish(napi_env env, napi_callback_info info) {
    napi_value argv[3] = {NULL, NULL, NULL};
    napi_value this_value;
    size_t argc = 3;
    if (napi_get_cb_info(env, info, &argc, argv, &this_value, NULL) != napi_ok) {
        return node_rejected_native_promise(env, -EINVAL, "finishStream");
    }
    node_call* call = NULL;
    if (napi_unwrap(env, this_value, (void**)&call) != napi_ok || !node_call_usable(call) || !call->server_side ||
        call->response_submitted || call->terminal_settled) {
        return node_rejected_native_promise(env, -EPIPE, "finishStream");
    }
    napi_deferred deferred;
    napi_value promise = node_new_promise(env, &deferred);
    if (promise == NULL) {
        return NULL;
    }
    node_server_response_input input;
    memset(&input, 0, sizeof(input));
    input.status.code = TREVRPC_RPC_STATUS_OK;
    if (argc > 0 && argv[0] != NULL && napi_get_value_uint32(env, argv[0], &input.status.code) != napi_ok) {
        (void)node_reject_deferred_native(env, deferred, -EINVAL, "finishStream");
        return promise;
    }
    if (input.status.code > TREVRPC_RPC_STATUS_UNAUTHENTICATED) {
        (void)node_reject_deferred_native(env, deferred, -EINVAL, "finishStream");
        return promise;
    }
    if (argc > 1 && argv[1] != NULL && node_copy_js_string(env, argv[1], &input.status.message, &(uint32_t){0}) != 0) {
        (void)node_reject_deferred_native(env, deferred, -EINVAL, "finishStream");
        return promise;
    }
    if (argc > 2 && argv[2] != NULL && node_copy_js_metadata(env, argv[2], &input.metadata) != 0) {
        node_server_response_input_free(&input);
        (void)node_reject_deferred_native(env, deferred, -EINVAL, "finishStream");
        return promise;
    }
    bool client_stream = call->kind == TREVRPC_RPC_KIND_CLIENT_STREAMING;
    uint64_t id = node_operation_allocate(call->runtime);
    node_operation* operation =
        id == TREVRPC_RPC_OPERATION_ID_NONE
            ? NULL
            : node_operation_add_id(call->runtime,
                  id,
                  client_stream ? TREVRPC_RPC_OBJECT_STREAM : TREVRPC_RPC_OBJECT_CALL,
                  client_stream ? node_key_from_stream(call->stream) : node_key_from_call(call->call),
                  client_stream ? NODE_OPERATION_CALL_RESPOND : NODE_OPERATION_CALL_FINISH,
                  call,
                  deferred,
                  true);
    int result = operation == NULL ? -ENOMEM : 0;
    if (result == 0) {
        trevrpc_rpc_status_v1 status;
        result = trevrpc_rpc_status_v1_init(&status, sizeof(status));
        if (result == 0) {
            status.code = input.status.code;
            status.message = input.status.message;
            status.message_len = (input.status.message == NULL ? 0u : (uint32_t)strlen(input.status.message));
            status.metadata = input.metadata.entries;
            status.metadata_count = (uint32_t)input.metadata.count;
            if (client_stream) {
                const uint8_t* body = NULL;
                size_t body_len = 0;
                if (input.status.code == TREVRPC_RPC_STATUS_OK) {
                    body = call->server_response_body_set ? call->server_response_body : (const uint8_t*)"";
                    body_len = call->server_response_body_set ? call->server_response_body_len : 0;
                }
                result = trevrpc_rpc_call_respond_copy_v1(call->runtime->rpc, call->call, id, &status, body, body_len);
            } else {
                result = trevrpc_rpc_call_finish_v1(call->runtime->rpc, call->call, id, &status);
            }
        }
        if (result == 0) {
            call->response_submitted = true;
        } else {
            node_operation_remove(call->runtime, operation);
        }
    }
    node_server_response_input_free(&input);
    if (result != 0) {
        (void)node_reject_deferred_native(env, deferred, result, "finishStream");
    }
    return promise;
}

static int node_call_server_fail(node_call* call, uint32_t status_code, const char* message) {
    if (call == NULL || call->runtime == NULL || call->response_submitted || call->terminal_settled) {
        return -EINVAL;
    }
    trevrpc_rpc_status_v1 status;
    int result = trevrpc_rpc_status_v1_init(&status, sizeof(status));
    if (result != 0) {
        return result;
    }
    status.code = status_code;
    status.message = message == NULL ? "" : message;
    status.message_len = (uint32_t)strlen(status.message);
    uint64_t operation_id = node_operation_allocate(call->runtime);
    node_operation* operation = operation_id == TREVRPC_RPC_OPERATION_ID_NONE ? NULL
                                                                              : node_operation_add_id(call->runtime,
                                                                                    operation_id,
                                                                                    TREVRPC_RPC_OBJECT_CALL,
                                                                                    node_key_from_call(call->call),
                                                                                    NODE_OPERATION_CALL_FINISH,
                                                                                    call,
                                                                                    NULL,
                                                                                    false);
    if (operation == NULL) {
        return -ENOMEM;
    }
    result = trevrpc_rpc_call_finish_v1(call->runtime->rpc, call->call, operation_id, &status);
    if (result != 0) {
        node_operation_remove(call->runtime, operation);
        return result;
    }
    call->response_submitted = true;
    return 0;
}

static napi_value node_make_server_call_object(napi_env env, node_call* call) {
    napi_value object;
    napi_value method;
    napi_value request;
    if (napi_create_object(env, &object) != napi_ok || node_make_server_call_request(env, call, &request) != 0 ||
        napi_set_named_property(env, object, "request", request) != napi_ok ||
        napi_create_function(env, "respond", NAPI_AUTO_LENGTH, node_server_call_respond, NULL, &method) != napi_ok ||
        napi_set_named_property(env, object, "respond", method) != napi_ok ||
        napi_create_function(env, "finishStream", NAPI_AUTO_LENGTH, node_server_call_finish, NULL, &method) !=
            napi_ok ||
        napi_set_named_property(env, object, "finishStream", method) != napi_ok ||
        napi_create_function(env, "sendMessage", NAPI_AUTO_LENGTH, node_stream_send_message, NULL, &method) !=
            napi_ok ||
        napi_set_named_property(env, object, "sendMessage", method) != napi_ok ||
        napi_create_function(env, "sendMessages", NAPI_AUTO_LENGTH, node_stream_send_messages, NULL, &method) !=
            napi_ok ||
        napi_set_named_property(env, object, "sendMessages", method) != napi_ok ||
        napi_create_function(env, "finishSend", NAPI_AUTO_LENGTH, node_stream_finish_send, NULL, &method) != napi_ok ||
        napi_set_named_property(env, object, "finishSend", method) != napi_ok ||
        napi_create_function(env, "recv", NAPI_AUTO_LENGTH, node_stream_recv, NULL, &method) != napi_ok ||
        napi_set_named_property(env, object, "recv", method) != napi_ok ||
        napi_create_function(env, "recvMany", NAPI_AUTO_LENGTH, node_stream_recv_many, NULL, &method) != napi_ok ||
        napi_set_named_property(env, object, "recvMany", method) != napi_ok ||
        napi_create_function(env, "close", NAPI_AUTO_LENGTH, node_stream_close, NULL, &method) != napi_ok ||
        napi_set_named_property(env, object, "close", method) != napi_ok) {
        return NULL;
    }
    if (napi_create_reference(env, object, 1, &call->wrapper_ref) != napi_ok ||
        napi_wrap(env, object, call, node_call_finalizer, NULL, NULL) != napi_ok) {
        return NULL;
    }
    call->wrapper_alive = true;
    return object;
}

static void node_server_free_routes(napi_env env, node_server* server) {
    node_server_route* route = server == NULL ? NULL : server->routes;
    for (node_server_route* pending = route; pending != NULL; pending = pending->next) {
        if (pending->active_calls != 0) {
            return;
        }
    }
    while (route != NULL) {
        node_server_route* next = route->next;
        if (route->handler_ref != NULL && node_runtime_napi_legal(server->runtime)) {
            (void)napi_delete_reference(env, route->handler_ref);
        }
        free(route->service);
        free(route->method);
        free(route);
        route = next;
    }
    if (server != NULL) {
        server->routes = NULL;
    }
}

static void node_client_detach_server(node_runtime* runtime, node_client* client) {
    if (client == NULL || client->server == NULL) {
        return;
    }
    node_server* server = client->server;
    if (node_runtime_napi_legal(runtime)) {
        if (server->wrapper_ref != NULL) {
            (void)napi_delete_reference(runtime->env, server->wrapper_ref);
            server->wrapper_ref = NULL;
        }
        if (server->serve_promise_ref != NULL) {
            (void)napi_delete_reference(runtime->env, server->serve_promise_ref);
            server->serve_promise_ref = NULL;
        }
        if (server->admission_ref != NULL) {
            (void)napi_delete_reference(runtime->env, server->admission_ref);
            server->admission_ref = NULL;
        }
    }
    node_server_free_routes(runtime == NULL ? NULL : runtime->env, server);
    client->server = NULL;
    server->endpoint = NULL;
    server->runtime = NULL;
    node_test_trace_runtime(runtime, "server-detached");
    if (!server->wrapper_alive && server->routes == NULL) {
        free(server);
    }
}

static napi_value node_server_register(napi_env env, napi_callback_info info) {
    napi_value argv[4] = {NULL, NULL, NULL, NULL};
    napi_value this_value;
    size_t argc = 4;
    if (napi_get_cb_info(env, info, &argc, argv, &this_value, NULL) != napi_ok || argc != 4) {
        return node_throw_error(env, -EINVAL, "register");
    }
    node_server* server = NULL;
    if (napi_unwrap(env, this_value, (void**)&server) != napi_ok || server == NULL || server->closing ||
        server->closed) {
        return node_throw_error(env, -ESHUTDOWN, "register");
    }
    node_server_route* route = calloc(1, sizeof(*route));
    if (route == NULL) {
        return node_throw_error(env, -ENOMEM, "register");
    }
    uint32_t service_len = 0;
    uint32_t method_len = 0;
    if (node_copy_js_string(env, argv[0], &route->service, &service_len) != 0 ||
        node_copy_js_string(env, argv[1], &route->method, &method_len) != 0 ||
        napi_get_value_uint32(env, argv[2], &route->kind) != napi_ok ||
        route->kind > TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        free(route->service);
        free(route->method);
        free(route);
        return node_throw_error(env, -EINVAL, "register");
    }
    napi_valuetype type;
    if (napi_typeof(env, argv[3], &type) != napi_ok || type != napi_function ||
        napi_create_reference(env, argv[3], 1, &route->handler_ref) != napi_ok) {
        free(route->service);
        free(route->method);
        free(route);
        return node_throw_error(env, -EINVAL, "register");
    }
    route->server = server;
    route->next = server->routes;
    server->routes = route;
    napi_value undefined;
    (void)napi_get_undefined(env, &undefined);
    return undefined;
}

static napi_value node_server_serve(napi_env env, napi_callback_info info) {
    napi_value this_value;
    size_t argc = 0;
    if (napi_get_cb_info(env, info, &argc, NULL, &this_value, NULL) != napi_ok) {
        return node_rejected_native_promise(env, -EIO, "serve");
    }
    node_server* server = NULL;
    if (napi_unwrap(env, this_value, (void**)&server) != napi_ok || server == NULL) {
        return node_rejected_native_promise(env, -ESTALE, "serve");
    }
    if (server->serve_promise_ref != NULL) {
        napi_value promise = NULL;
        if (napi_get_reference_value(env, server->serve_promise_ref, &promise) == napi_ok) {
            return promise;
        }
        return node_rejected_native_promise(env, -EIO, "serve");
    }
    napi_value promise = node_new_promise(env, &server->serve_deferred);
    if (promise == NULL || napi_create_reference(env, promise, 1, &server->serve_promise_ref) != napi_ok) {
        return promise == NULL ? NULL : node_rejected_native_promise(env, -ENOMEM, "serve");
    }
    server->serve_called = true;
    if (server->closed) {
        node_resolve_undefined(env, server->serve_deferred);
        server->serve_deferred = NULL;
    }
    return promise;
}

static napi_value node_server_close(napi_env env, napi_callback_info info) {
    napi_value this_value;
    size_t argc = 0;
    if (napi_get_cb_info(env, info, &argc, NULL, &this_value, NULL) != napi_ok) {
        return node_throw_error(env, -EIO, "server.close");
    }
    node_server* server = NULL;
    if (napi_unwrap(env, this_value, (void**)&server) != napi_ok || server == NULL) {
        return node_throw_error(env, -ESTALE, "server.close");
    }
    server->closing = true;
    if (server->endpoint != NULL) {
        (void)node_client_request_close(server->endpoint);
    }
    napi_value undefined;
    (void)napi_get_undefined(env, &undefined);
    return undefined;
}

static void node_server_finalizer(napi_env env, void* data, void* hint) {
    (void)hint;
    node_server* server = data;
    if (server == NULL)
        return;
    server->wrapper_alive = false;
    if (server->wrapper_ref != NULL && node_runtime_napi_legal(server->runtime)) {
        (void)napi_delete_reference(env, server->wrapper_ref);
        server->wrapper_ref = NULL;
    }
    if (server->serve_promise_ref != NULL && node_runtime_napi_legal(server->runtime)) {
        (void)napi_delete_reference(env, server->serve_promise_ref);
        server->serve_promise_ref = NULL;
    }
    if (server->admission_ref != NULL && node_runtime_napi_legal(server->runtime)) {
        (void)napi_delete_reference(env, server->admission_ref);
        server->admission_ref = NULL;
    }
    if (server->runtime != NULL && !server->runtime->cleanup_started) {
        server->closing = true;
        if (server->endpoint != NULL)
            (void)node_client_request_close(server->endpoint);
    }
    if (server->runtime == NULL) {
        node_server_free_routes(env, server);
        if (server->routes == NULL) {
            free(server);
        }
    }
}

static napi_value node_make_server_object(napi_env env, node_server* server) {
    napi_value object;
    napi_value method;
    napi_value port;
    if (napi_create_object(env, &object) != napi_ok || napi_create_uint32(env, server->port, &port) != napi_ok ||
        napi_set_named_property(env, object, "port", port) != napi_ok ||
        napi_create_function(env, "register", NAPI_AUTO_LENGTH, node_server_register, NULL, &method) != napi_ok ||
        napi_set_named_property(env, object, "register", method) != napi_ok ||
        napi_create_function(env, "serve", NAPI_AUTO_LENGTH, node_server_serve, NULL, &method) != napi_ok ||
        napi_set_named_property(env, object, "serve", method) != napi_ok ||
        napi_create_function(env, "close", NAPI_AUTO_LENGTH, node_server_close, NULL, &method) != napi_ok ||
        napi_set_named_property(env, object, "close", method) != napi_ok ||
        napi_create_reference(env, object, 1, &server->wrapper_ref) != napi_ok ||
        napi_wrap(env, object, server, node_server_finalizer, NULL, NULL) != napi_ok) {
        return NULL;
    }
    server->wrapper_alive = true;
    return object;
}

static int node_route_incoming_call(
    node_runtime* runtime, trevrpc_rpc_event* event, const trevrpc_rpc_event_info_v1* info) {
    node_subject* endpoint_subject = node_registry_find(&runtime->endpoints, node_key_from_endpoint(info->endpoint));
    node_client* endpoint = endpoint_subject == NULL ? NULL : endpoint_subject->core;
    node_server* server = endpoint == NULL ? NULL : endpoint->server;
    char* service = info->service_len == 0 ? strdup("") : strndup(info->service, info->service_len);
    char* method = info->method_len == 0 ? strdup("") : strndup(info->method, info->method_len);
    node_call* call = calloc(1, sizeof(*call));
    node_subject* call_subject = node_subject_allocate(TREVRPC_RPC_OBJECT_CALL, (node_handle_key){0, 0, 0});
    node_subject* stream_subject = node_subject_allocate(TREVRPC_RPC_OBJECT_STREAM, (node_handle_key){0, 0, 0});
    node_receive_item* initial_item = calloc(1, sizeof(*initial_item));
    if (service == NULL || method == NULL || call == NULL || call_subject == NULL || stream_subject == NULL ||
        initial_item == NULL) {
        free(service);
        free(method);
        free(call);
        free(call_subject);
        free(stream_subject);
        free(initial_item);
        return -ENOMEM;
    }
    trevrpc_rpc_call_v1 call_handle;
    trevrpc_rpc_stream_v1 stream_handle;
    trevrpc_rpc_receive* initial = NULL;
    int result = trevrpc_rpc_event_take_incoming_call(event, &call_handle, &stream_handle, &initial);
    if (result != 0) {
        free(service);
        free(method);
        free(call);
        free(call_subject);
        free(stream_subject);
        free(initial_item);
        return result;
    }
    node_server_route* route = server == NULL ? NULL : node_server_find_route(server, service, method, info->rpc_kind);
    call->runtime = runtime;
    call->server = server;
    call->server_side = true;
    call->kind = info->rpc_kind;
    call->call = call_handle;
    call->stream = stream_handle;
    call->route = route;
    call->call_subject = call_subject;
    call->stream_subject = stream_subject;
    call_subject->key = node_key_from_call(call_handle);
    stream_subject->key = node_key_from_stream(stream_handle);
    call_subject->core = call;
    stream_subject->core = call;
    node_registry_insert(runtime, call_subject);
    node_registry_insert(runtime, stream_subject);
    if (route != NULL) {
        route->active_calls++;
    }
    if (initial != NULL) {
        initial_item->receive = initial;
        call->receive_head = initial_item;
        call->receive_tail = initial_item;
    } else {
        free(initial_item);
    }
    free(service);
    free(method);
    uint64_t id = node_operation_allocate(runtime);
    node_operation* operation = id == TREVRPC_RPC_OPERATION_ID_NONE ? NULL
                                                                    : node_operation_add_id(runtime,
                                                                          id,
                                                                          TREVRPC_RPC_OBJECT_CALL,
                                                                          node_key_from_call(call_handle),
                                                                          NODE_OPERATION_CALL_ACCEPT,
                                                                          call,
                                                                          NULL,
                                                                          false);
    if (operation == NULL) {
        node_call_settle_terminal(call, -ENOMEM, "accept incoming call");
        call->close_abort = true;
        node_call_request_close(call);
        node_call_maybe_release(call);
        return -ENOMEM;
    }
    result = trevrpc_rpc_call_accept(runtime->rpc, call_handle, id);
    if (result != 0) {
        node_operation_remove(runtime, operation);
        node_call_settle_terminal(call, result, "accept incoming call");
        call->close_abort = true;
        node_call_request_close(call);
        node_call_maybe_release(call);
        return result;
    }
    call->accept_submitted = true;
    return 0;
}

static int node_route_admission(
    node_runtime* runtime, trevrpc_rpc_event* event, const trevrpc_rpc_event_info_v1* info) {
    node_subject* subject = node_registry_find(&runtime->endpoints, node_key_from_endpoint(info->endpoint));
    node_client* endpoint = subject == NULL ? NULL : subject->core;
    node_server* server = endpoint == NULL ? NULL : endpoint->server;
    uint16_t response = 500;
    if (server != NULL && server->admission_ref != NULL) {
        trevrpc_rpc_admission_info_v1 admission;
        if (trevrpc_rpc_admission_info_v1_init(&admission, sizeof(admission)) == 0 &&
            trevrpc_rpc_event_get_admission_info_v1(event, &admission) == 0) {
            napi_value callback = NULL;
            napi_value global = NULL;
            napi_value request = NULL;
            napi_value result_value = NULL;
            if (napi_get_reference_value(runtime->env, server->admission_ref, &callback) == napi_ok &&
                napi_get_global(runtime->env, &global) == napi_ok &&
                napi_create_object(runtime->env, &request) == napi_ok) {
                napi_value path;
                napi_value authority;
                napi_value origin;
                napi_value secure;
                if (napi_create_string_utf8(runtime->env,
                        admission.path == NULL ? "" : (const char*)admission.path,
                        admission.path_len,
                        &path) == napi_ok &&
                    napi_create_string_utf8(runtime->env,
                        admission.authority == NULL ? "" : (const char*)admission.authority,
                        admission.authority_len,
                        &authority) == napi_ok &&
                    napi_create_string_utf8(runtime->env,
                        admission.origin == NULL ? "" : (const char*)admission.origin,
                        admission.origin_len,
                        &origin) == napi_ok &&
                    napi_get_boolean(
                        runtime->env, (admission.flags & TREVRPC_RPC_ADMISSION_FLAG_SECURE) != 0, &secure) == napi_ok &&
                    napi_set_named_property(runtime->env, request, "path", path) == napi_ok &&
                    napi_set_named_property(runtime->env, request, "authority", authority) == napi_ok &&
                    napi_set_named_property(runtime->env, request, "origin", origin) == napi_ok &&
                    napi_set_named_property(runtime->env, request, "secure", secure) == napi_ok &&
                    napi_call_function(runtime->env, global, callback, 1, &request, &result_value) == napi_ok) {
                    bool is_promise = false;
                    if (napi_is_promise(runtime->env, result_value, &is_promise) != napi_ok || is_promise) {
                        response = 400;
                    } else {
                        bool allowed = false;
                        if (napi_get_value_bool(runtime->env, result_value, &allowed) == napi_ok) {
                            response = allowed ? 200 : 403;
                        } else {
                            uint32_t status = 0;
                            if (napi_get_value_uint32(runtime->env, result_value, &status) == napi_ok &&
                                (status == 200 || (status >= 400 && status <= 599))) {
                                response = (uint16_t)status;
                            } else {
                                response = 400;
                            }
                        }
                    }
                }
            }
        }
    }
    return trevrpc_rpc_admission_respond_v1(event, response);
}

static napi_value node_listen_msquic(napi_env env, napi_callback_info info) {
    napi_value argv[1] = {NULL};
    size_t argc = 1;
    if (napi_get_cb_info(env, info, &argc, argv, NULL, NULL) != napi_ok || argc < 1) {
        return node_rejected_native_promise(env, -EINVAL, "listenMsQuic");
    }
    node_runtime* runtime = NULL;
    int result = node_runtime_get(env, &runtime);
    if (result != 0 || runtime == NULL) {
        return node_rejected_native_promise(env, result == 0 ? -EIO : result, "listenMsQuic");
    }
    node_endpoint_config_storage storage;
    result = node_parse_endpoint_config(env, argv[0], &storage, true);
    if (result != 0) {
        return node_rejected_native_promise(env, result, "listenMsQuic");
    }
    storage.config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_LISTENER;
    storage.config.server_name = NULL;
    storage.config.server_name_len = 0;
    napi_value value;
    bool present = false;
    napi_valuetype value_type;
    if (node_get_named_value(env, argv[0], "http3Admission", &value, &present) != 0) {
        node_endpoint_config_free(&storage);
        return node_rejected_native_promise(env, -EINVAL, "listenMsQuic");
    }
    if (present && (napi_typeof(env, value, &value_type) != napi_ok || value_type != napi_function)) {
        node_endpoint_config_free(&storage);
        napi_throw_type_error(env, NULL, "http3Admission must be a function");
        return NULL;
    }
    bool has_admission = present;
    bool transport_present = false;
    if (node_get_named_value(env, argv[0], "transport", &value, &transport_present) != 0) {
        node_endpoint_config_free(&storage);
        return node_rejected_native_promise(env, -EINVAL, "listenMsQuic");
    }
    bool enable_native = true;
    bool enable_native_present = false;
    if (node_get_named_bool(env, argv[0], "enableNative", &enable_native, &enable_native_present) != 0) {
        node_endpoint_config_free(&storage);
        return node_rejected_native_promise(env, -EINVAL, "listenMsQuic");
    }
    bool enable_http3 = false;
    bool enable_http3_present = false;
    if (node_get_named_bool(env, argv[0], "enableHttp3", &enable_http3, &enable_http3_present) != 0) {
        node_endpoint_config_free(&storage);
        return node_rejected_native_promise(env, -EINVAL, "listenMsQuic");
    }
    if (transport_present && (enable_native_present || enable_http3_present)) {
        node_endpoint_config_free(&storage);
        return node_rejected_native_promise(env, -EINVAL, "listenMsQuic");
    }
    if (!transport_present) {
        if (enable_http3) {
            if (enable_native) {
                node_endpoint_config_free(&storage);
                return node_rejected_native_promise(env, -ENOTSUP, "listenMsQuic");
            }
            storage.config.transport = TREVRPC_RPC_MSQUIC_TRANSPORT_HTTP3;
        } else if (!enable_native) {
            node_endpoint_config_free(&storage);
            return node_rejected_native_promise(env, -EINVAL, "listenMsQuic");
        } else {
            storage.config.transport = TREVRPC_RPC_MSQUIC_TRANSPORT_NATIVE;
        }
    }
    char* cert_file = NULL;
    char* key_file = NULL;
    if (node_get_named_value(env, argv[0], "certFile", &value, &present) != 0 || !present ||
        node_copy_js_string(env, value, &cert_file, &(uint32_t){0}) != 0 ||
        node_get_named_value(env, argv[0], "keyFile", &value, &present) != 0 || !present ||
        node_copy_js_string(env, value, &key_file, &(uint32_t){0}) != 0) {
        free(cert_file);
        free(key_file);
        node_endpoint_config_free(&storage);
        return node_rejected_native_promise(env, -EINVAL, "listenMsQuic");
    }
    storage.config.cert_file = cert_file;
    storage.config.cert_file_len = (uint32_t)strlen(cert_file);
    storage.config.key_file = key_file;
    storage.config.key_file_len = (uint32_t)strlen(key_file);
    storage.config.ca_cert_file = NULL;
    storage.config.ca_cert_file_len = 0;
    const bool http3_selected = storage.config.transport == TREVRPC_RPC_MSQUIC_TRANSPORT_HTTP3;
    storage.config.flags = TREVRPC_RPC_MSQUIC_VERIFY_PEER |
                           (http3_selected && has_admission ? TREVRPC_RPC_MSQUIC_ENABLE_ADMISSION_EVENTS : 0);
    napi_deferred deferred;
    napi_value promise = node_new_promise(env, &deferred);
    if (promise == NULL) {
        free(cert_file);
        free(key_file);
        node_endpoint_config_free(&storage);
        return NULL;
    }
    node_server* server = calloc(1, sizeof(*server));
    node_client* endpoint = calloc(1, sizeof(*endpoint));
    node_subject* reserved = node_subject_allocate(TREVRPC_RPC_OBJECT_ENDPOINT, (node_handle_key){0, 0, 0});
    uint64_t id = node_operation_allocate(runtime);
    node_operation* operation = id == TREVRPC_RPC_OPERATION_ID_NONE ? NULL
                                                                    : node_operation_add_id(runtime,
                                                                          id,
                                                                          TREVRPC_RPC_OBJECT_ENDPOINT,
                                                                          (node_handle_key){0, 0, 0},
                                                                          NODE_OPERATION_ENDPOINT_LISTEN,
                                                                          server,
                                                                          deferred,
                                                                          true);
    if (server == NULL || endpoint == NULL || reserved == NULL || operation == NULL) {
        free(server);
        free(endpoint);
        free(reserved);
        if (operation != NULL)
            node_operation_remove(runtime, operation);
        free(cert_file);
        free(key_file);
        node_endpoint_config_free(&storage);
        (void)node_reject_deferred_native(env, deferred, -ENOMEM, "listenMsQuic");
        return promise;
    }
    server->runtime = runtime;
    server->endpoint = endpoint;
    server->enable_http3 = enable_http3;
    endpoint->runtime = runtime;
    endpoint->server = server;
    endpoint->subject = reserved;
    if (has_admission) {
        napi_value admission_callback;
        if (napi_get_named_property(env, argv[0], "http3Admission", &admission_callback) != napi_ok ||
            napi_create_reference(env, admission_callback, 1, &server->admission_ref) != napi_ok) {
            node_operation_remove(runtime, operation);
            free(server);
            free(endpoint);
            free(reserved);
            free(cert_file);
            free(key_file);
            node_endpoint_config_free(&storage);
            (void)node_reject_deferred_native(env, deferred, -ENOMEM, "listenMsQuic");
            return promise;
        }
    }
    int start = trevrpc_rpc_msquic_endpoint_start_v1(runtime->rpc, &storage.config, id, &endpoint->endpoint);
    free(cert_file);
    free(key_file);
    node_endpoint_config_free(&storage);
    if (start != 0) {
        if (server->admission_ref != NULL)
            (void)napi_delete_reference(env, server->admission_ref);
        node_operation_remove(runtime, operation);
        free(server);
        free(endpoint);
        free(reserved);
        (void)node_reject_deferred_native(env, deferred, start, "listenMsQuic");
        return promise;
    }
    reserved->key = node_key_from_endpoint(endpoint->endpoint);
    reserved->core = endpoint;
    node_registry_insert(runtime, reserved);
    operation->subject = reserved->key;
    node_test_trace_key(runtime, "key-rebind", operation->subject_kind, operation->subject);
    operation->subject_bound = true;
    napi_value object = node_make_server_object(env, server);
    if (object == NULL) {
        (void)node_client_request_close(endpoint);
        (void)node_reject_deferred_native(env, deferred, -ENOMEM, "listenMsQuic");
    }
    return promise;
}

static napi_value node_init(napi_env env, napi_value exports) {
#if defined(_WIN32) || defined(WIN32)
    napi_throw_error(env,
        "ERR_PLATFORM_NOT_SUPPORTED",
        "the native addon is unsupported on WIN32: RPC ABI1 exposes POSIX FD wake sources");
    return NULL;
#else
    napi_property_descriptor descriptors[] = {
        {"connectMsQuic", NULL, node_connect_msquic, NULL, NULL, NULL, napi_default, NULL},
        {"createCancellation", NULL, node_create_cancellation, NULL, NULL, NULL, napi_default, NULL},
        {"listenMsQuic", NULL, node_listen_msquic, NULL, NULL, NULL, napi_default, NULL},
    };
    if (napi_define_properties(env, exports, sizeof(descriptors) / sizeof(descriptors[0]), descriptors) != napi_ok) {
        return node_throw_error(env, -EIO, "native addon initialization");
    }
    return exports;
#endif
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, node_init)
