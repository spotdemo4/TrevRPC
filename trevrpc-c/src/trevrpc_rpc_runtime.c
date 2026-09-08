#define _POSIX_C_SOURCE 200809L

#include "trevrpc_rpc_internal.h"

#include "trevrpc_wire_internal.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <stdatomic.h>
#include <sched.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct trevrpc_rpc_operation trevrpc_rpc_operation;
typedef struct trevrpc_rpc_endpoint_record trevrpc_rpc_endpoint_record;
typedef struct trevrpc_rpc_transport_connection_record trevrpc_rpc_transport_connection_record;
typedef struct trevrpc_rpc_call_record trevrpc_rpc_call_record;
typedef struct trevrpc_rpc_cancellation_record trevrpc_rpc_cancellation_record;
typedef struct trevrpc_rpc_retired_handle trevrpc_rpc_retired_handle;
typedef struct trevrpc_rpc_registry_value trevrpc_rpc_registry_value;
typedef struct trevrpc_rpc_registry_entry trevrpc_rpc_registry_entry;
typedef struct trevrpc_rpc_registry trevrpc_rpc_registry;
typedef struct trevrpc_rpc_deadline_entry trevrpc_rpc_deadline_entry;

enum trevrpc_rpc_operation_kind {
    TREVRPC_RPC_OPERATION_ENDPOINT_START = 1,
    TREVRPC_RPC_OPERATION_CALL_OPEN = 2,
    TREVRPC_RPC_OPERATION_CALL_ACCEPT = 3,
    TREVRPC_RPC_OPERATION_SEND = 4,
    TREVRPC_RPC_OPERATION_RESPOND = 5,
    TREVRPC_RPC_OPERATION_STREAM_FINISH = 6,
    TREVRPC_RPC_OPERATION_CALL_FINISH = 7,
    TREVRPC_RPC_OPERATION_CALL_CANCEL = 8,
    TREVRPC_RPC_OPERATION_STREAM_CLOSE = 9,
    TREVRPC_RPC_OPERATION_CALL_CLOSE = 10,
    TREVRPC_RPC_OPERATION_ENDPOINT_CLOSE = 11,
    TREVRPC_RPC_OPERATION_CANCELLATION_CANCEL = 12,
};

struct trevrpc_rpc_receive {
    uint32_t kind;
    uint32_t flags;
    uint32_t rpc_status;
    uint8_t* data;
    uint64_t data_len;
    char* message;
    uint32_t message_len;
    trevrpc_rpc_metadata_entry_v1* metadata;
    uint32_t metadata_count;
};

struct trevrpc_rpc_event {
    struct trevrpc_rpc_event* next;
    struct trevrpc_rpc_runtime* runtime;
    trevrpc_rpc_transport* admission_transport;
    trevrpc_rpc_transport_event* admission_event;
    uint32_t kind;
    uint32_t flags;
    int32_t status;
    uint32_t subject_kind;
    uint64_t sequence;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_cancellation_v1 cancellation;
    uint64_t operation_id;
    uint32_t rpc_status;
    uint32_t rpc_kind;
    uint64_t application_error_code;
    uint64_t provider_error_code;
    char* service;
    uint32_t service_len;
    char* method;
    uint32_t method_len;
    trevrpc_rpc_receive* receive;
    trevrpc_rpc_endpoint_v1 admission_listener;
    uint32_t admission_protocol;
    uint32_t admission_flags;
    uint8_t* admission_path;
    uint64_t admission_path_len;
    uint8_t* admission_authority;
    uint64_t admission_authority_len;
    uint8_t* admission_origin;
    uint64_t admission_origin_len;
    bool incoming_taken;
    bool mandatory;
    bool reserved;
    atomic_uint take_claim;
};

struct trevrpc_rpc_retired_handle {
    trevrpc_rpc_retired_handle* next;
    trevrpc_rpc_transport_handle handle;
    uint32_t kind;
};

struct trevrpc_rpc_registry_value {
    trevrpc_rpc_registry_value* next;
    void* value;
};

struct trevrpc_rpc_registry_entry {
    uint64_t key[6];
    uint64_t hash;
    trevrpc_rpc_registry_value* values;
    uint8_t count;
    uint8_t state;
};

struct trevrpc_rpc_registry {
    trevrpc_rpc_registry_entry* entries;
    size_t capacity;
    size_t size;
    size_t tombstones;
};

enum trevrpc_rpc_timer_kind {
    TREVRPC_RPC_TIMER_NONE,
    TREVRPC_RPC_TIMER_CALL,
    TREVRPC_RPC_TIMER_INITIAL,
    TREVRPC_RPC_TIMER_SEND_IDLE,
    TREVRPC_RPC_TIMER_RECEIVE_IDLE,
};

struct trevrpc_rpc_deadline_entry {
    trevrpc_rpc_call_record* record;
    uint64_t deadline;
    uint32_t kind;
    size_t index;
};

struct trevrpc_rpc_operation {
    trevrpc_rpc_operation* next;
    uint32_t kind;
    uint32_t subject_kind;
    uint64_t subject_owner;
    uint32_t subject_slot;
    uint32_t subject_generation;
    uint32_t scope_kind;
    uint64_t scope_owner;
    uint32_t scope_slot;
    uint32_t scope_generation;
    uint64_t operation_id;
    uint64_t transport_operation_id;
    trevrpc_rpc_event* event;
    uint8_t* continuation_frame;
    size_t continuation_frame_len;
};

struct trevrpc_rpc_endpoint_record {
    trevrpc_rpc_endpoint_record* next;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_transport_handle transport_endpoint;
    uint32_t transport_kind;
    trevrpc_rpc_operation* start_operation;
    trevrpc_rpc_operation* close_operation;
    trevrpc_rpc_event* terminal_event;
    bool caller_owned;
    bool transport_released;
    bool terminal_committed;
    bool terminal_dequeued;
};

struct trevrpc_rpc_transport_connection_record {
    trevrpc_rpc_transport_connection_record* next;
    trevrpc_rpc_transport_handle transport_connection;
    trevrpc_rpc_endpoint_v1 endpoint;
};

struct trevrpc_rpc_call_record {
    trevrpc_rpc_call_record* next;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_transport_handle transport_stream;
    uint32_t kind;
    trevrpc_rpc_operation* open_operation;
    trevrpc_rpc_operation* close_operation;
    trevrpc_rpc_event* stream_terminal_event;
    trevrpc_rpc_event* call_terminal_event;
    trevrpc_rpc_event* deferred_receive_fin;
    trevrpc_rpc_transport_event_info deferred_stream_terminal;
    trevrpc_rpc_transport_receive* pending_transport_receive;
    trevrpc_rpc_cancellation_record* cancellation;
    uint8_t* request_frame;
    size_t request_frame_len;
    uint64_t deadline_nanos;
    uint64_t call_deadline_nanos;
    uint64_t initial_request_deadline_nanos;
    uint64_t send_idle_deadline_nanos;
    uint64_t receive_idle_deadline_nanos;
    uint64_t response_idle_timeout_nanos;
    int64_t max_send_messages;
    int64_t max_send_body_size;
    int64_t max_receive_messages;
    int64_t max_receive_body_size;
    int64_t max_response_messages;
    int64_t max_response_body_size;
    int64_t max_response_stream_body_size;
    uint64_t request_message_count;
    uint64_t response_message_count;
    uint64_t request_body_size;
    uint64_t response_body_size;
    bool has_deadline;
    uint64_t readable_epoch;
    uint64_t readable_drained_epoch;
    uint32_t readable_flags;
    uint32_t readable_events_pending;
    uint32_t receive_in_flight;
    bool deferred_stream_terminal_valid;
    bool request_send_pending;
    bool awaiting_transport_acceptance;
    bool transport_accepted;
    bool waiting_for_request;
    bool local;
    bool accepted;
    bool closing;
    bool deadline_expired;
    bool cancelled;
    bool runtime_close_target;
    bool suppress_terminals;
    bool send_finish_pending;
    bool send_closed;
    bool peer_status_received;
    bool receive_fin_observed;
    bool caller_owns_call;
    bool caller_owns_stream;
    bool transport_stream_released;
    bool stream_terminal_committed;
    bool call_terminal_committed;
    bool stream_terminal_dequeued;
    bool call_terminal_dequeued;
    trevrpc_wire_diagnostic_reason last_receive_diagnostic;
    trevrpc_rpc_deadline_entry deadline_entry;
};

struct trevrpc_rpc_cancellation_record {
    trevrpc_rpc_cancellation_record* next;
    trevrpc_rpc_cancellation_v1 cancellation;
    uint32_t attached_calls;
    bool cancelled;
};

struct trevrpc_rpc_runtime {
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    pthread_mutex_t lifetime_mutex;
    pthread_cond_t lifetime_condition;
    pthread_t driver_thread;
    atomic_size_t references;
    atomic_uint_fast64_t api_gate;
    atomic_bool test_fail_next_receive_copy;
    atomic_int test_call_release_result;
    atomic_bool test_call_release_result_persistent;
    trevrpc_rpc_transport* transport;
    uint64_t owner_cookie;
    uint32_t state;
    int32_t terminal_status;
    uint32_t event_capacity;
    uint32_t endpoint_capacity;
    uint32_t call_capacity;
    uint32_t stream_capacity;
    uint32_t max_metadata_count;
    uint32_t max_status_message_size;
    uint32_t queue_depth;
    uint32_t ordinary_queue_depth;
    uint64_t next_sequence;
    uint64_t next_transport_operation_id;
    uint64_t events_enqueued;
    uint64_t events_dequeued;
    uint64_t events_rejected;
    uint64_t wake_signals;
    uint64_t wake_write_eagain;
    uint64_t wake_failures;
    uint64_t live_endpoints;
    uint64_t live_calls;
    uint64_t live_streams;
    uint64_t endpoint_sequence;
    uint64_t call_sequence;
    uint64_t cancellation_sequence;
    uint64_t max_message_size;
    uint64_t max_metadata_bytes;
    uint64_t initial_request_timeout_nanos;
    int64_t max_stream_messages;
    int64_t max_stream_body_size;
    uint64_t stream_idle_timeout_nanos;
    uint64_t mandatory_reservations;
    bool wake_armed;
    bool driver_started;
    bool driver_stopped;
    bool public_released;
    uint32_t test_malformed_incoming_kind;
    int wake_read_fd;
    int wake_write_fd;
    int driver_wake_read_fd;
    int driver_wake_write_fd;
    trevrpc_rpc_event* close_event;
    uint64_t close_operation_id;
    trevrpc_rpc_event* event_head;
    trevrpc_rpc_event* event_tail;
    trevrpc_rpc_operation* operations;
    trevrpc_rpc_endpoint_record* endpoints;
    trevrpc_rpc_transport_connection_record* transport_connections;
    trevrpc_rpc_registry call_registry;
    trevrpc_rpc_registry endpoint_registry;
    trevrpc_rpc_registry transport_registry;
    trevrpc_rpc_registry cancellation_registry;
    trevrpc_rpc_registry operation_transport_registry;
    trevrpc_rpc_registry operation_scope_registry;
    trevrpc_rpc_deadline_entry** deadline_heap;
    size_t deadline_heap_count;
    size_t deadline_heap_capacity;
    trevrpc_rpc_retired_handle* retired_handles;
    trevrpc_rpc_retired_handle* retired_pool;
    size_t retired_pool_capacity;
    trevrpc_rpc_call_record* calls;
    trevrpc_rpc_cancellation_record* cancellations;
};

#define TREVRPC_RPC_API_GATE_CLOSED 1u
#define TREVRPC_RPC_API_GATE_REFERENCE 2u

static atomic_uint_fast64_t trevrpc_rpc_owner_sequence = ATOMIC_VAR_INIT(1);

static void trevrpc_rpc_runtime_unref(trevrpc_rpc_runtime* runtime);
static void trevrpc_rpc_abort_or_terminal(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_transport_handle stream, uint64_t application_error_code, int32_t status);
static uint32_t trevrpc_rpc_request_failure_status(int result, trevrpc_wire_diagnostic_reason diagnostic);
static uint32_t trevrpc_rpc_receive_failure_status(int result, trevrpc_wire_diagnostic_reason diagnostic);
static void trevrpc_rpc_transition_fatal_locked(trevrpc_rpc_runtime* runtime, int32_t status);
static void trevrpc_rpc_retire_handle_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_transport_handle handle, uint32_t kind);
static void trevrpc_rpc_flush_receive_barrier_locked(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_record* record);

static int trevrpc_rpc_api_enter(trevrpc_rpc_runtime* runtime) {
    uint_fast64_t current;
    if (runtime == NULL) {
        return -EINVAL;
    }
    current = atomic_load_explicit(&runtime->api_gate, memory_order_acquire);
    for (;;) {
        if ((current & TREVRPC_RPC_API_GATE_CLOSED) != 0) {
            return -EPIPE;
        }
        if (current > UINT_FAST64_MAX - TREVRPC_RPC_API_GATE_REFERENCE) {
            return -EOVERFLOW;
        }
        if (atomic_compare_exchange_weak_explicit(&runtime->api_gate,
                &current,
                current + TREVRPC_RPC_API_GATE_REFERENCE,
                memory_order_acq_rel,
                memory_order_acquire)) {
            return 0;
        }
    }
}

static void trevrpc_rpc_api_leave(trevrpc_rpc_runtime* runtime) {
    uint_fast64_t current = atomic_load_explicit(&runtime->api_gate, memory_order_acquire);
    for (;;) {
        if ((current & TREVRPC_RPC_API_GATE_CLOSED) != 0) {
            pthread_mutex_lock(&runtime->lifetime_mutex);
            atomic_fetch_sub_explicit(&runtime->api_gate, TREVRPC_RPC_API_GATE_REFERENCE, memory_order_acq_rel);
            pthread_cond_broadcast(&runtime->lifetime_condition);
            pthread_mutex_unlock(&runtime->lifetime_mutex);
            return;
        }
        if (atomic_compare_exchange_weak_explicit(&runtime->api_gate,
                &current,
                current - TREVRPC_RPC_API_GATE_REFERENCE,
                memory_order_acq_rel,
                memory_order_acquire)) {
            return;
        }
    }
}

static bool trevrpc_rpc_u64_fields_zero(const uint64_t* fields, size_t count) {
    size_t index;
    for (index = 0; index < count; ++index) {
        if (fields[index] != 0) {
            return false;
        }
    }
    return true;
}

static bool trevrpc_rpc_handle_is_zero(uint64_t owner, uint32_t slot, uint32_t generation) {
    return owner == 0 && slot == 0 && generation == 0;
}

int trevrpc_rpc_validate_runtime_config(const trevrpc_rpc_runtime_config_v1* config) {
    if (config == NULL || config->struct_size < sizeof(*config)) {
        return -EINVAL;
    }
    if (config->struct_version != TREVRPC_RPC_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (config->flags != 0 ||
        !trevrpc_rpc_u64_fields_zero(config->reserved, sizeof(config->reserved) / sizeof(config->reserved[0])) ||
        config->event_capacity == 0 || config->event_capacity > TREVRPC_RPC_MAX_EVENT_CAPACITY ||
        config->endpoint_capacity == 0 || config->call_capacity == 0 || config->stream_capacity == 0 ||
        config->max_receive_owned_count == 0 || config->max_receive_owned_bytes == 0 || config->max_message_size == 0 ||
        config->max_metadata_count == 0 || config->max_metadata_bytes == 0 || config->max_status_message_size == 0) {
        return -EINVAL;
    }
    return 0;
}

static int trevrpc_rpc_set_descriptor_flags(int descriptor) {
    int flags = fcntl(descriptor, F_GETFL, 0);
    if (flags < 0 || fcntl(descriptor, F_SETFL, flags | O_NONBLOCK) != 0) {
        return -errno;
    }
    flags = fcntl(descriptor, F_GETFD, 0);
    if (flags < 0 || fcntl(descriptor, F_SETFD, flags | FD_CLOEXEC) != 0) {
        return -errno;
    }
    return 0;
}

static int trevrpc_rpc_create_wake_pipe(int descriptors[2]) {
    int result;
    if (pipe(descriptors) != 0) {
        return -errno;
    }
    result = trevrpc_rpc_set_descriptor_flags(descriptors[0]);
    if (result == 0) {
        result = trevrpc_rpc_set_descriptor_flags(descriptors[1]);
    }
    if (result != 0) {
        close(descriptors[0]);
        close(descriptors[1]);
        descriptors[0] = -1;
        descriptors[1] = -1;
    }
    return result;
}

static int trevrpc_rpc_monotonic_nanos(uint64_t* out_nanos) {
    struct timespec now;
    uint64_t seconds;
    if (out_nanos == NULL) {
        return -EINVAL;
    }
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        return -errno;
    }
    if (now.tv_sec < 0 || (uint64_t)now.tv_sec > UINT64_MAX / UINT64_C(1000000000)) {
        return -EOVERFLOW;
    }
    seconds = (uint64_t)now.tv_sec * UINT64_C(1000000000);
    if ((uint64_t)now.tv_nsec > UINT64_MAX - seconds) {
        return -EOVERFLOW;
    }
    *out_nanos = seconds + (uint64_t)now.tv_nsec;
    return 0;
}

static int trevrpc_rpc_deadline_after(uint64_t timeout_nanos, uint64_t* out_deadline) {
    uint64_t now = 0;
    int result;
    if (out_deadline == NULL) {
        return -EINVAL;
    }
    if (timeout_nanos == 0 || timeout_nanos == TREVRPC_RPC_DEADLINE_INFINITE) {
        *out_deadline = TREVRPC_RPC_DEADLINE_INFINITE;
        return 0;
    }
    result = trevrpc_rpc_monotonic_nanos(&now);
    if (result != 0) {
        return result;
    }
    *out_deadline = timeout_nanos >= UINT64_MAX - now ? UINT64_MAX - 1u : now + timeout_nanos;
    return 0;
}

static uint64_t trevrpc_rpc_saturating_add_u64(uint64_t left, uint64_t right) {
    return right > UINT64_MAX - left ? UINT64_MAX : left + right;
}

static uint64_t trevrpc_rpc_idle_deadline(uint64_t timeout_nanos) {
    uint64_t now = 0;
    if (timeout_nanos == 0) {
        return 0;
    }
    if (trevrpc_rpc_monotonic_nanos(&now) != 0 || timeout_nanos >= UINT64_MAX - now) {
        return UINT64_MAX - 1u;
    }
    return now + timeout_nanos;
}

static uint64_t trevrpc_rpc_next_deadline(const trevrpc_rpc_call_record* record, uint32_t* out_kind) {
    uint64_t earliest = TREVRPC_RPC_DEADLINE_INFINITE;
    uint32_t kind = TREVRPC_RPC_TIMER_NONE;
#define TREVRPC_RPC_PICK_NEXT(value, timer_kind)                                                                       \
    do {                                                                                                               \
        if ((value) != 0 && (value) < earliest) {                                                                      \
            earliest = (value);                                                                                        \
            kind = (timer_kind);                                                                                       \
        }                                                                                                              \
    } while (0)
    TREVRPC_RPC_PICK_NEXT(record->deadline_nanos, TREVRPC_RPC_TIMER_CALL);
    TREVRPC_RPC_PICK_NEXT(record->initial_request_deadline_nanos, TREVRPC_RPC_TIMER_INITIAL);
    TREVRPC_RPC_PICK_NEXT(record->send_idle_deadline_nanos, TREVRPC_RPC_TIMER_SEND_IDLE);
    TREVRPC_RPC_PICK_NEXT(record->receive_idle_deadline_nanos, TREVRPC_RPC_TIMER_RECEIVE_IDLE);
#undef TREVRPC_RPC_PICK_NEXT
    if (out_kind != NULL) {
        *out_kind = kind;
    }
    return earliest;
}

static bool trevrpc_rpc_deadline_before(
    const trevrpc_rpc_deadline_entry* left, const trevrpc_rpc_deadline_entry* right) {
    return left->deadline < right->deadline ||
           (left->deadline == right->deadline && (uintptr_t)left->record < (uintptr_t)right->record);
}

static void trevrpc_rpc_deadline_swap(trevrpc_rpc_runtime* runtime, size_t left, size_t right) {
    trevrpc_rpc_deadline_entry* entry = runtime->deadline_heap[left];
    runtime->deadline_heap[left] = runtime->deadline_heap[right];
    runtime->deadline_heap[right] = entry;
    runtime->deadline_heap[left]->index = left;
    runtime->deadline_heap[right]->index = right;
}

static void trevrpc_rpc_deadline_sift_up(trevrpc_rpc_runtime* runtime, size_t index) {
    while (index != 0) {
        size_t parent = (index - 1) / 2;
        if (!trevrpc_rpc_deadline_before(runtime->deadline_heap[index], runtime->deadline_heap[parent])) {
            break;
        }
        trevrpc_rpc_deadline_swap(runtime, index, parent);
        index = parent;
    }
}

static void trevrpc_rpc_deadline_sift_down(trevrpc_rpc_runtime* runtime, size_t index) {
    for (;;) {
        size_t left = index * 2 + 1;
        size_t right = left + 1;
        size_t smallest = index;
        if (left < runtime->deadline_heap_count &&
            trevrpc_rpc_deadline_before(runtime->deadline_heap[left], runtime->deadline_heap[smallest])) {
            smallest = left;
        }
        if (right < runtime->deadline_heap_count &&
            trevrpc_rpc_deadline_before(runtime->deadline_heap[right], runtime->deadline_heap[smallest])) {
            smallest = right;
        }
        if (smallest == index) {
            return;
        }
        trevrpc_rpc_deadline_swap(runtime, index, smallest);
        index = smallest;
    }
}

static void trevrpc_rpc_deadline_remove_locked(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_record* record) {
    size_t index = record->deadline_entry.index;
    if (index == SIZE_MAX) {
        return;
    }
    record->deadline_entry.index = SIZE_MAX;
    --runtime->deadline_heap_count;
    if (index != runtime->deadline_heap_count) {
        runtime->deadline_heap[index] = runtime->deadline_heap[runtime->deadline_heap_count];
        runtime->deadline_heap[index]->index = index;
        trevrpc_rpc_deadline_sift_down(runtime, index);
        trevrpc_rpc_deadline_sift_up(runtime, index);
    }
}

static int trevrpc_rpc_deadline_refresh_locked(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_record* record) {
    uint32_t kind;
    uint64_t deadline;
    size_t index;
    if (record == NULL) {
        return -EINVAL;
    }
    deadline = record->closing || record->call_terminal_committed ? TREVRPC_RPC_DEADLINE_INFINITE
                                                                  : trevrpc_rpc_next_deadline(record, &kind);
    if (deadline == TREVRPC_RPC_DEADLINE_INFINITE) {
        trevrpc_rpc_deadline_remove_locked(runtime, record);
        return 0;
    }
    record->deadline_entry.record = record;
    record->deadline_entry.deadline = deadline;
    record->deadline_entry.kind = kind;
    if (record->deadline_entry.index == SIZE_MAX) {
        if (runtime->deadline_heap_count == runtime->deadline_heap_capacity) {
            size_t capacity = runtime->deadline_heap_capacity == 0 ? 64 : runtime->deadline_heap_capacity * 2;
            trevrpc_rpc_deadline_entry** heap;
            if (capacity < runtime->deadline_heap_capacity || capacity > SIZE_MAX / sizeof(*heap)) {
                return -EOVERFLOW;
            }
            heap = realloc(runtime->deadline_heap, capacity * sizeof(*heap));
            if (heap == NULL) {
                return -ENOMEM;
            }
            runtime->deadline_heap = heap;
            runtime->deadline_heap_capacity = capacity;
        }
        index = runtime->deadline_heap_count++;
        runtime->deadline_heap[index] = &record->deadline_entry;
        record->deadline_entry.index = index;
        trevrpc_rpc_deadline_sift_up(runtime, index);
    } else {
        index = record->deadline_entry.index;
        trevrpc_rpc_deadline_sift_down(runtime, index);
        trevrpc_rpc_deadline_sift_up(runtime, index);
    }
    return 0;
}

static void trevrpc_rpc_reset_idle_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_record* record, bool send_direction) {
    uint64_t timeout;
    if (record == NULL || record->call_terminal_committed) {
        return;
    }
    timeout = runtime->stream_idle_timeout_nanos;
    if (record->local && !send_direction) {
        timeout = record->response_idle_timeout_nanos;
    }
    if (record->local && send_direction) {
        timeout = 0;
    }
    if (timeout == 0) {
        if (send_direction) {
            record->send_idle_deadline_nanos = 0;
        } else {
            record->receive_idle_deadline_nanos = 0;
        }
        (void)trevrpc_rpc_deadline_refresh_locked(runtime, record);
        return;
    }
    if (send_direction) {
        record->send_idle_deadline_nanos = trevrpc_rpc_idle_deadline(timeout);
    } else {
        record->receive_idle_deadline_nanos = trevrpc_rpc_idle_deadline(timeout);
    }
    (void)trevrpc_rpc_deadline_refresh_locked(runtime, record);
}

static int trevrpc_rpc_reserve_message_locked(
    uint64_t* count, uint64_t* body_size, int64_t message_limit, int64_t body_limit, size_t body_len) {
    uint64_t next_body;
    if (message_limit >= 0 && *count >= (uint64_t)message_limit) {
        return -EMSGSIZE;
    }
    next_body = trevrpc_rpc_saturating_add_u64(*body_size, (uint64_t)body_len);
    if (body_limit >= 0 && next_body > (uint64_t)body_limit) {
        return -EMSGSIZE;
    }
    ++*count;
    *body_size = next_body;
    return 0;
}

static void trevrpc_rpc_signal_driver_locked(trevrpc_rpc_runtime* runtime) {
    uint8_t byte = 1;
    ssize_t written;
    do {
        written = write(runtime->driver_wake_write_fd, &byte, sizeof(byte));
    } while (written < 0 && errno == EINTR);
}

static void trevrpc_rpc_drain_descriptor(int descriptor) {
    uint8_t bytes[64];
    while (read(descriptor, bytes, sizeof(bytes)) > 0) {
    }
}

static void trevrpc_rpc_receive_destroy(trevrpc_rpc_receive* receive) {
    uint32_t index;
    if (receive == NULL) {
        return;
    }
    if (receive->metadata != NULL) {
        for (index = 0; index < receive->metadata_count; ++index) {
            free((void*)receive->metadata[index].key);
            free((void*)receive->metadata[index].value);
        }
    }
    free(receive->metadata);
    free(receive->data);
    free(receive->message);
    free(receive);
}

static bool trevrpc_rpc_call_equal(trevrpc_rpc_call_v1 left, trevrpc_rpc_call_v1 right) {
    return left.owner == right.owner && left.slot == right.slot && left.generation == right.generation;
}

static bool trevrpc_rpc_stream_equal(trevrpc_rpc_stream_v1 left, trevrpc_rpc_stream_v1 right) {
    return left.owner == right.owner && left.slot == right.slot && left.generation == right.generation;
}

static bool trevrpc_rpc_transport_handle_equal(trevrpc_rpc_transport_handle left, trevrpc_rpc_transport_handle right) {
    return left.owner == right.owner && left.slot == right.slot && left.generation == right.generation;
}

static int trevrpc_rpc_allocate_handle_locked(trevrpc_rpc_runtime* runtime,
    uint64_t* sequence,
    uint64_t* out_owner,
    uint32_t* out_slot,
    uint32_t* out_generation) {
    uint64_t value;
    if (*sequence >= (UINT64_C(0xffffffff) << 32u)) {
        return -EOVERFLOW;
    }
    value = (*sequence)++;
    *out_owner = runtime->owner_cookie;
    *out_slot = (uint32_t)value;
    *out_generation = (uint32_t)(value >> 32u) + 1u;
    return 0;
}

static uint64_t trevrpc_rpc_registry_hash(const uint64_t* key, uint8_t count) {
    uint64_t hash = UINT64_C(1469598103934665603);
    uint8_t index;
    for (index = 0; index < count; ++index) {
        uint64_t value = key[index];
        uint8_t byte;
        for (byte = 0; byte < sizeof(value); ++byte) {
            hash ^= (value >> (byte * 8u)) & UINT64_C(0xff);
            hash *= UINT64_C(1099511628211);
        }
    }
    return hash == 0 ? 1 : hash;
}

static bool trevrpc_rpc_registry_key_equal(
    const trevrpc_rpc_registry_entry* entry, const uint64_t* key, uint8_t count, uint64_t hash) {
    return entry->state == 1 && entry->hash == hash && entry->count == count &&
           memcmp(entry->key, key, (size_t)count * sizeof(*key)) == 0;
}

static int trevrpc_rpc_registry_rehash(trevrpc_rpc_registry* registry, size_t capacity) {
    trevrpc_rpc_registry_entry* old_entries = registry->entries;
    size_t old_capacity = registry->capacity;
    trevrpc_rpc_registry_entry* entries;
    size_t index;
    if (capacity < 16 || capacity > SIZE_MAX / 2 || (capacity & (capacity - 1)) != 0) {
        return -EINVAL;
    }
    entries = calloc(capacity, sizeof(*entries));
    if (entries == NULL) {
        return -ENOMEM;
    }
    registry->entries = entries;
    registry->capacity = capacity;
    registry->size = 0;
    registry->tombstones = 0;
    for (index = 0; index < old_capacity; ++index) {
        if (old_entries[index].state == 1) {
            size_t position = old_entries[index].hash & (capacity - 1);
            while (entries[position].state == 1) {
                position = (position + 1) & (capacity - 1);
            }
            entries[position] = old_entries[index];
            ++registry->size;
        }
    }
    free(old_entries);
    return 0;
}

static int trevrpc_rpc_registry_init(trevrpc_rpc_registry* registry) {
    memset(registry, 0, sizeof(*registry));
    return trevrpc_rpc_registry_rehash(registry, 16);
}

static void trevrpc_rpc_registry_destroy(trevrpc_rpc_registry* registry) {
    size_t index;
    if (registry == NULL) {
        return;
    }
    for (index = 0; index < registry->capacity; ++index) {
        trevrpc_rpc_registry_value* node = registry->entries[index].values;
        while (node != NULL) {
            trevrpc_rpc_registry_value* next = node->next;
            free(node);
            node = next;
        }
    }
    free(registry->entries);
    memset(registry, 0, sizeof(*registry));
}

static void* trevrpc_rpc_registry_find(const trevrpc_rpc_registry* registry, const uint64_t* key, uint8_t count) {
    uint64_t hash;
    size_t position;
    size_t probes = 0;
    if (registry->capacity == 0) {
        return NULL;
    }
    hash = trevrpc_rpc_registry_hash(key, count);
    position = hash & (registry->capacity - 1);
    while (probes++ < registry->capacity) {
        const trevrpc_rpc_registry_entry* entry = &registry->entries[position];
        if (entry->state == 0) {
            return NULL;
        }
        if (trevrpc_rpc_registry_key_equal(entry, key, count, hash)) {
            return entry->values == NULL ? NULL : entry->values->value;
        }
        position = (position + 1) & (registry->capacity - 1);
    }
    return NULL;
}

static int trevrpc_rpc_registry_insert(
    trevrpc_rpc_registry* registry, const uint64_t* key, uint8_t count, void* value) {
    uint64_t hash;
    size_t position;
    size_t first_tombstone = SIZE_MAX;
    size_t probes = 0;
    if (count == 0 || count > 6 || value == NULL) {
        return -EINVAL;
    }
    if (registry->capacity == 0) {
        int result = trevrpc_rpc_registry_init(registry);
        if (result != 0) {
            return result;
        }
    }
    if ((registry->size + registry->tombstones + 1) * 10 >= registry->capacity * 7) {
        size_t capacity = registry->tombstones > registry->size ? registry->capacity : registry->capacity * 2;
        int result = trevrpc_rpc_registry_rehash(registry, capacity);
        if (result != 0) {
            return result;
        }
    }
    hash = trevrpc_rpc_registry_hash(key, count);
    position = hash & (registry->capacity - 1);
    while (probes++ < registry->capacity) {
        trevrpc_rpc_registry_entry* entry = &registry->entries[position];
        if (entry->state == 0) {
            if (first_tombstone != SIZE_MAX) {
                position = first_tombstone;
                entry = &registry->entries[position];
                --registry->tombstones;
            }
            trevrpc_rpc_registry_value* node = malloc(sizeof(*node));
            if (node == NULL) {
                return -ENOMEM;
            }
            node->value = value;
            node->next = NULL;
            memcpy(entry->key, key, (size_t)count * sizeof(*key));
            entry->hash = hash;
            entry->values = node;
            entry->count = count;
            entry->state = 1;
            ++registry->size;
            return 0;
        }
        if (entry->state == 2) {
            if (first_tombstone == SIZE_MAX) {
                first_tombstone = position;
            }
        } else if (trevrpc_rpc_registry_key_equal(entry, key, count, hash)) {
            return -EEXIST;
        }
        position = (position + 1) & (registry->capacity - 1);
    }
    return -ENOSPC;
}

static void* trevrpc_rpc_registry_remove(trevrpc_rpc_registry* registry, const uint64_t* key, uint8_t count) {
    uint64_t hash;
    size_t position;
    size_t probes = 0;
    if (registry->capacity == 0) {
        return NULL;
    }
    hash = trevrpc_rpc_registry_hash(key, count);
    position = hash & (registry->capacity - 1);
    while (probes++ < registry->capacity) {
        trevrpc_rpc_registry_entry* entry = &registry->entries[position];
        if (entry->state == 0) {
            return NULL;
        }
        if (trevrpc_rpc_registry_key_equal(entry, key, count, hash)) {
            trevrpc_rpc_registry_value* node = entry->values;
            void* value;
            if (node == NULL) {
                return NULL;
            }
            entry->values = node->next;
            value = node->value;
            free(node);
            if (entry->values == NULL) {
                entry->state = 2;
                --registry->size;
                ++registry->tombstones;
            }
            return value;
        }
        position = (position + 1) & (registry->capacity - 1);
    }
    return NULL;
}

static int trevrpc_rpc_registry_rekey(
    trevrpc_rpc_registry* registry, const uint64_t* old_key, const uint64_t* new_key, uint8_t count, void* value) {
    uint64_t old_hash;
    uint64_t new_hash;
    size_t old_position;
    size_t new_position;
    size_t first_tombstone = SIZE_MAX;
    size_t probes = 0;
    trevrpc_rpc_registry_entry* old_entry = NULL;
    trevrpc_rpc_registry_entry* new_entry;
    if (registry->capacity == 0 || count == 0 || count > 6 || value == NULL) {
        return -EINVAL;
    }
    old_hash = trevrpc_rpc_registry_hash(old_key, count);
    old_position = old_hash & (registry->capacity - 1);
    while (probes++ < registry->capacity) {
        trevrpc_rpc_registry_entry* entry = &registry->entries[old_position];
        if (entry->state == 0) {
            return -ENOENT;
        }
        if (trevrpc_rpc_registry_key_equal(entry, old_key, count, old_hash)) {
            if (entry->values == NULL || entry->values->next != NULL || entry->values->value != value) {
                return -EIO;
            }
            old_entry = entry;
            break;
        }
        old_position = (old_position + 1) & (registry->capacity - 1);
    }
    if (old_entry == NULL) {
        return -ENOENT;
    }
    new_hash = trevrpc_rpc_registry_hash(new_key, count);
    new_position = new_hash & (registry->capacity - 1);
    probes = 0;
    while (probes++ < registry->capacity) {
        new_entry = &registry->entries[new_position];
        if (new_entry->state == 0) {
            if (first_tombstone != SIZE_MAX) {
                new_position = first_tombstone;
                new_entry = &registry->entries[new_position];
                --registry->tombstones;
            }
            break;
        }
        if (new_entry->state == 2) {
            if (first_tombstone == SIZE_MAX) {
                first_tombstone = new_position;
            }
        } else if (trevrpc_rpc_registry_key_equal(new_entry, new_key, count, new_hash)) {
            return -EEXIST;
        }
        new_position = (new_position + 1) & (registry->capacity - 1);
    }
    if (probes > registry->capacity) {
        if (first_tombstone == SIZE_MAX) {
            return -ENOSPC;
        }
        new_entry = &registry->entries[first_tombstone];
        --registry->tombstones;
    } else if (new_entry->state == 2) {
        --registry->tombstones;
    }
    new_entry->values = old_entry->values;
    memcpy(new_entry->key, new_key, (size_t)count * sizeof(*new_key));
    new_entry->hash = new_hash;
    new_entry->count = count;
    new_entry->state = 1;
    old_entry->values = NULL;
    old_entry->state = 2;
    ++registry->tombstones;
    return 0;
}

static int trevrpc_rpc_registry_insert_alias(
    trevrpc_rpc_registry* registry, const uint64_t* key, uint8_t count, void* value) {
    uint64_t hash;
    size_t position;
    size_t first_tombstone = SIZE_MAX;
    size_t probes = 0;
    if (count == 0 || count > 6 || value == NULL) {
        return -EINVAL;
    }
    if (registry->capacity == 0) {
        int result = trevrpc_rpc_registry_init(registry);
        if (result != 0) {
            return result;
        }
    }
    if ((registry->size + registry->tombstones + 1) * 10 >= registry->capacity * 7) {
        size_t capacity = registry->tombstones > registry->size ? registry->capacity : registry->capacity * 2;
        int result = trevrpc_rpc_registry_rehash(registry, capacity);
        if (result != 0) {
            return result;
        }
    }
    hash = trevrpc_rpc_registry_hash(key, count);
    position = hash & (registry->capacity - 1);
    while (probes++ < registry->capacity) {
        trevrpc_rpc_registry_entry* entry = &registry->entries[position];
        if (entry->state == 0) {
            if (first_tombstone != SIZE_MAX) {
                position = first_tombstone;
                entry = &registry->entries[position];
                --registry->tombstones;
            }
            {
                trevrpc_rpc_registry_value* node = malloc(sizeof(*node));
                if (node == NULL) {
                    return -ENOMEM;
                }
                node->value = value;
                node->next = NULL;
                memcpy(entry->key, key, (size_t)count * sizeof(*key));
                entry->hash = hash;
                entry->values = node;
                entry->count = count;
                entry->state = 1;
                ++registry->size;
            }
            return 0;
        }
        if (entry->state == 2) {
            if (first_tombstone == SIZE_MAX) {
                first_tombstone = position;
            }
        } else if (trevrpc_rpc_registry_key_equal(entry, key, count, hash)) {
            trevrpc_rpc_registry_value* existing = entry->values;
            trevrpc_rpc_registry_value* node;
            while (existing != NULL) {
                if (existing->value == value) {
                    return -EEXIST;
                }
                existing = existing->next;
            }
            node = malloc(sizeof(*node));
            if (node == NULL) {
                return -ENOMEM;
            }
            node->value = value;
            node->next = entry->values;
            entry->values = node;
            return 0;
        }
        position = (position + 1) & (registry->capacity - 1);
    }
    return -ENOSPC;
}

static void* trevrpc_rpc_registry_remove_value(
    trevrpc_rpc_registry* registry, const uint64_t* key, uint8_t count, void* value) {
    uint64_t hash;
    size_t position;
    size_t probes = 0;
    if (registry->capacity == 0) {
        return NULL;
    }
    hash = trevrpc_rpc_registry_hash(key, count);
    position = hash & (registry->capacity - 1);
    while (probes++ < registry->capacity) {
        trevrpc_rpc_registry_entry* entry = &registry->entries[position];
        if (entry->state == 0) {
            return NULL;
        }
        if (trevrpc_rpc_registry_key_equal(entry, key, count, hash)) {
            trevrpc_rpc_registry_value** cursor = &entry->values;
            while (*cursor != NULL && (*cursor)->value != value) {
                cursor = &(*cursor)->next;
            }
            if (*cursor == NULL) {
                return NULL;
            }
            {
                trevrpc_rpc_registry_value* node = *cursor;
                *cursor = node->next;
                free(node);
            }
            if (entry->values == NULL) {
                entry->state = 2;
                --registry->size;
                ++registry->tombstones;
            }
            return value;
        }
        position = (position + 1) & (registry->capacity - 1);
    }
    return NULL;
}

static void trevrpc_rpc_handle_key(uint64_t* key, uint8_t* count, uint64_t owner, uint32_t slot, uint32_t generation) {
    key[0] = owner;
    key[1] = slot;
    key[2] = generation;
    *count = 3;
}

enum trevrpc_rpc_transport_registry_role {
    TREVRPC_RPC_TRANSPORT_ROLE_ENDPOINT = 1,
    TREVRPC_RPC_TRANSPORT_ROLE_CONNECTION = 2,
    TREVRPC_RPC_TRANSPORT_ROLE_CALL = 3,
};

static void trevrpc_rpc_transport_key(
    uint64_t* key, uint8_t* count, trevrpc_rpc_transport_handle handle, uint32_t kind, uint32_t role) {
    trevrpc_rpc_handle_key(key, count, handle.owner, handle.slot, handle.generation);
    key[3] = kind;
    key[4] = role;
    *count = 5;
}

static trevrpc_rpc_call_record* trevrpc_rpc_find_call_locked(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call) {
    uint64_t key[6];
    uint8_t count;
    trevrpc_rpc_handle_key(key, &count, call.owner, call.slot, call.generation);
    return trevrpc_rpc_registry_find(&runtime->call_registry, key, count);
}

static trevrpc_rpc_call_record* trevrpc_rpc_find_stream_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream) {
    return trevrpc_rpc_find_call_locked(runtime, (trevrpc_rpc_call_v1){stream.owner, stream.slot, stream.generation});
}

static trevrpc_rpc_endpoint_record* trevrpc_rpc_find_endpoint_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_endpoint_v1 endpoint) {
    uint64_t key[6];
    uint8_t count;
    trevrpc_rpc_handle_key(key, &count, endpoint.owner, endpoint.slot, endpoint.generation);
    return trevrpc_rpc_registry_find(&runtime->endpoint_registry, key, count);
}

static trevrpc_rpc_call_record* trevrpc_rpc_find_call_by_transport_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_transport_handle stream) {
    uint64_t key[6];
    uint8_t count;
    trevrpc_rpc_transport_key(
        key, &count, stream, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM, TREVRPC_RPC_TRANSPORT_ROLE_CALL);
    return trevrpc_rpc_registry_find(&runtime->transport_registry, key, count);
}

static trevrpc_rpc_endpoint_record* trevrpc_rpc_find_endpoint_by_transport_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_transport_handle endpoint) {
    uint64_t key[6];
    uint8_t count;
    trevrpc_rpc_endpoint_record* record;
    trevrpc_rpc_transport_key(
        key, &count, endpoint, TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER, TREVRPC_RPC_TRANSPORT_ROLE_ENDPOINT);
    record = trevrpc_rpc_registry_find(&runtime->transport_registry, key, count);
    if (record == NULL) {
        trevrpc_rpc_transport_key(
            key, &count, endpoint, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION, TREVRPC_RPC_TRANSPORT_ROLE_ENDPOINT);
        record = trevrpc_rpc_registry_find(&runtime->transport_registry, key, count);
    }
    return record;
}

static trevrpc_rpc_transport_connection_record* trevrpc_rpc_find_transport_connection_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_transport_handle connection) {
    uint64_t key[6];
    uint8_t count;
    trevrpc_rpc_transport_key(
        key, &count, connection, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION, TREVRPC_RPC_TRANSPORT_ROLE_CONNECTION);
    return trevrpc_rpc_registry_find(&runtime->transport_registry, key, count);
}

static bool trevrpc_rpc_remove_transport_connection_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_transport_handle connection) {
    trevrpc_rpc_transport_connection_record** cursor = &runtime->transport_connections;
    while (*cursor != NULL && !trevrpc_rpc_transport_handle_equal((*cursor)->transport_connection, connection)) {
        cursor = &(*cursor)->next;
    }
    if (*cursor != NULL) {
        trevrpc_rpc_transport_connection_record* record = *cursor;
        int result = trevrpc_rpc_transport_release_handle(
            runtime->transport, record->transport_connection, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION);
        *cursor = record->next;
        {
            uint64_t key[6];
            uint8_t count;
            trevrpc_rpc_transport_key(key,
                &count,
                record->transport_connection,
                TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION,
                TREVRPC_RPC_TRANSPORT_ROLE_CONNECTION);
            (void)trevrpc_rpc_registry_remove_value(&runtime->transport_registry, key, count, record);
        }
        if (result != 0) {
            trevrpc_rpc_retire_handle_locked(
                runtime, record->transport_connection, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION);
        }
        free(record);
        return true;
    }
    return false;
}

static trevrpc_rpc_cancellation_record* trevrpc_rpc_find_cancellation_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_cancellation_v1 cancellation) {
    uint64_t key[6];
    uint8_t count;
    trevrpc_rpc_handle_key(key, &count, cancellation.owner, cancellation.slot, cancellation.generation);
    return trevrpc_rpc_registry_find(&runtime->cancellation_registry, key, count);
}

static trevrpc_rpc_event* trevrpc_rpc_event_allocate(uint32_t kind) {
    trevrpc_rpc_event* event = calloc(1, sizeof(*event));
    if (event != NULL) {
        event->kind = kind;
        atomic_init(&event->take_claim, 0);
    }
    return event;
}

static void trevrpc_rpc_event_destroy(trevrpc_rpc_event* event) {
    trevrpc_rpc_runtime* runtime;
    trevrpc_rpc_transport* admission_transport;
    trevrpc_rpc_transport_event* admission_event;
    bool reject = false;
    trevrpc_rpc_transport_handle transport_stream = {0};
    if (event == NULL) {
        return;
    }
    runtime = event->runtime;
    admission_transport = event->admission_transport;
    admission_event = event->admission_event;
    event->admission_event = NULL;
    if (admission_event != NULL) {
        trevrpc_rpc_transport_event_release(admission_transport, admission_event);
    }
    if (runtime != NULL && event->kind == TREVRPC_RPC_EVENT_CALL_INCOMING && !event->incoming_taken) {
        pthread_mutex_lock(&runtime->mutex);
        {
            trevrpc_rpc_call_record* record = trevrpc_rpc_find_call_locked(runtime, event->call);
            if (record != NULL && !record->closing) {
                record->closing = true;
                record->cancelled = true;
                transport_stream = record->transport_stream;
                reject = true;
            }
        }
        pthread_mutex_unlock(&runtime->mutex);
        if (reject) {
            trevrpc_rpc_abort_or_terminal(runtime, transport_stream, TREVRPC_RPC_STATUS_UNAVAILABLE, -ECANCELED);
        }
    }
    trevrpc_rpc_receive_destroy(event->receive);
    free(event->service);
    free(event->method);
    free(event->admission_path);
    free(event->admission_authority);
    free(event->admission_origin);
    free(event);
    if (runtime != NULL) {
        trevrpc_rpc_runtime_unref(runtime);
    }
}

static int trevrpc_rpc_validate_metadata_limits(const trevrpc_rpc_runtime* runtime, const trevrpc_metadata* metadata) {
    size_t index;
    uint64_t total = 0;
    if (metadata == NULL || metadata->entries_len == 0) {
        return 0;
    }
    if (metadata->entries == NULL || metadata->entries_len > runtime->max_metadata_count) {
        return -EMSGSIZE;
    }
    for (index = 0; index < metadata->entries_len; ++index) {
        const trevrpc_metadata_entry* entry = &metadata->entries[index];
        if (entry->key == NULL || entry->key_len == 0 || entry->key_len > UINT32_MAX ||
            (entry->value == NULL && entry->value_len != 0) || entry->key_len > runtime->max_metadata_bytes - total ||
            entry->value_len > runtime->max_metadata_bytes - total - entry->key_len || entry->value_len > SIZE_MAX) {
            return -EMSGSIZE;
        }
        total += entry->key_len + entry->value_len;
    }
    return 0;
}

static int trevrpc_rpc_copy_metadata(
    const trevrpc_rpc_runtime* runtime, trevrpc_rpc_receive* receive, const trevrpc_metadata* metadata) {
    size_t index;
    int result;
    if (metadata == NULL || metadata->entries_len == 0) {
        return 0;
    }
    result = trevrpc_rpc_validate_metadata_limits(runtime, metadata);
    if (result != 0) {
        return result;
    }
    if (metadata->entries_len > UINT32_MAX) {
        return -EOVERFLOW;
    }
    receive->metadata = calloc(metadata->entries_len, sizeof(*receive->metadata));
    if (receive->metadata == NULL) {
        return -ENOMEM;
    }
    receive->metadata_count = (uint32_t)metadata->entries_len;
    for (index = 0; index < metadata->entries_len; ++index) {
        const trevrpc_metadata_entry* source = &metadata->entries[index];
        trevrpc_rpc_metadata_entry_v1* destination = &receive->metadata[index];
        char* key = NULL;
        uint8_t* value = NULL;
        if (source->key_len > UINT32_MAX) {
            return -EOVERFLOW;
        }
        if (source->key_len != 0) {
            key = malloc(source->key_len);
            if (key == NULL) {
                return -ENOMEM;
            }
            memcpy(key, source->key, source->key_len);
        }
        if (source->value_len != 0) {
            value = malloc(source->value_len);
            if (value == NULL) {
                free(key);
                return -ENOMEM;
            }
            memcpy(value, source->value, source->value_len);
        }
        destination->key = key;
        destination->key_len = (uint32_t)source->key_len;
        destination->value = value;
        destination->value_len = source->value_len;
    }
    return 0;
}

static int trevrpc_rpc_metadata_view(trevrpc_rpc_runtime* runtime,
    const trevrpc_rpc_metadata_entry_v1* entries,
    uint32_t count,
    trevrpc_metadata* metadata,
    trevrpc_metadata_entry** storage) {
    uint64_t total = 0;
    uint32_t index;
    *metadata = (trevrpc_metadata){0};
    *storage = NULL;
    if (count == 0) {
        return 0;
    }
    if (entries == NULL || count > runtime->max_metadata_count) {
        return -EINVAL;
    }
    *storage = calloc(count, sizeof(**storage));
    if (*storage == NULL) {
        return -ENOMEM;
    }
    for (index = 0; index < count; ++index) {
        const trevrpc_rpc_metadata_entry_v1* source = &entries[index];
        if (source->key == NULL || source->key_len == 0 || (source->value == NULL && source->value_len != 0) ||
            source->reserved0 != 0 || source->key_len > runtime->max_metadata_bytes - total ||
            source->value_len > runtime->max_metadata_bytes - total - source->key_len || source->value_len > SIZE_MAX) {
            free(*storage);
            *storage = NULL;
            return -EINVAL;
        }
        (*storage)[index].key = (char*)source->key;
        (*storage)[index].key_len = source->key_len;
        (*storage)[index].value = (uint8_t*)source->value;
        (*storage)[index].value_len = (size_t)source->value_len;
        total += source->key_len + source->value_len;
    }
    metadata->entries = *storage;
    metadata->entries_len = count;
    if (trevrpc_internal_metadata_validate(metadata) != 0) {
        free(*storage);
        *storage = NULL;
        *metadata = (trevrpc_metadata){0};
        return -EINVAL;
    }
    return 0;
}

static trevrpc_rpc_operation* trevrpc_rpc_operation_allocate(
    uint32_t kind, uint32_t completion_kind, uint64_t operation_id) {
    trevrpc_rpc_operation* operation = calloc(1, sizeof(*operation));
    if (operation == NULL) {
        return NULL;
    }
    operation->event = trevrpc_rpc_event_allocate(completion_kind);
    if (operation->event == NULL) {
        free(operation);
        return NULL;
    }
    operation->kind = kind;
    operation->operation_id = operation_id;
    return operation;
}

static int trevrpc_rpc_operation_admit_locked(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_operation* operation,
    uint32_t subject_kind,
    uint64_t subject_owner,
    uint32_t subject_slot,
    uint32_t subject_generation,
    uint32_t scope_kind,
    uint64_t scope_owner,
    uint32_t scope_slot,
    uint32_t scope_generation) {
    uint64_t scope_key[6];
    uint64_t transport_key[6];
    uint8_t key_count;
    int result;
    if (runtime->state != TREVRPC_RPC_STATE_RUNNING) {
        return -ESHUTDOWN;
    }
    if (runtime->next_transport_operation_id == UINT64_MAX) {
        return -EOVERFLOW;
    }
    trevrpc_rpc_handle_key(scope_key, &key_count, scope_owner, scope_slot, scope_generation);
    scope_key[3] = scope_kind;
    scope_key[4] = operation->operation_id;
    key_count = 5;
    if (trevrpc_rpc_registry_find(&runtime->operation_scope_registry, scope_key, key_count) != NULL) {
        return -EALREADY;
    }
    operation->subject_kind = subject_kind;
    operation->subject_owner = subject_owner;
    operation->subject_slot = subject_slot;
    operation->subject_generation = subject_generation;
    operation->scope_kind = scope_kind;
    operation->scope_owner = scope_owner;
    operation->scope_slot = scope_slot;
    operation->scope_generation = scope_generation;
    operation->transport_operation_id = runtime->next_transport_operation_id++;
    trevrpc_rpc_handle_key(transport_key, &key_count, subject_owner, subject_slot, subject_generation);
    transport_key[3] = subject_kind;
    transport_key[4] = operation->transport_operation_id;
    key_count = 5;
    result = trevrpc_rpc_registry_insert(&runtime->operation_transport_registry, transport_key, key_count, operation);
    if (result != 0) {
        return result;
    }
    result = trevrpc_rpc_registry_insert(&runtime->operation_scope_registry, scope_key, 5, operation);
    if (result != 0) {
        (void)trevrpc_rpc_registry_remove(&runtime->operation_transport_registry, transport_key, key_count);
        return result;
    }
    operation->event->reserved = true;
    operation->next = runtime->operations;
    runtime->operations = operation;
    ++runtime->mandatory_reservations;
    return 0;
}

static int trevrpc_rpc_operation_rekey_transport_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_operation* operation, uint64_t transport_operation_id) {
    uint64_t old_key[6];
    uint64_t new_key[6];
    uint8_t key_count;
    trevrpc_rpc_handle_key(
        old_key, &key_count, operation->subject_owner, operation->subject_slot, operation->subject_generation);
    old_key[3] = operation->subject_kind;
    old_key[4] = operation->transport_operation_id;
    trevrpc_rpc_handle_key(
        new_key, &key_count, operation->subject_owner, operation->subject_slot, operation->subject_generation);
    new_key[3] = operation->subject_kind;
    new_key[4] = transport_operation_id;
    int result = trevrpc_rpc_registry_rekey(&runtime->operation_transport_registry, old_key, new_key, 5, operation);
    if (result == 0) {
        operation->transport_operation_id = transport_operation_id;
    }
    return result;
}

static void trevrpc_rpc_operation_remove_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_operation* operation, bool destroy_event) {
    trevrpc_rpc_operation** cursor = &runtime->operations;
    while (*cursor != NULL && *cursor != operation) {
        cursor = &(*cursor)->next;
    }
    if (*cursor == operation) {
        *cursor = operation->next;
    }
    {
        uint64_t key[6];
        uint8_t count;
        trevrpc_rpc_handle_key(
            key, &count, operation->subject_owner, operation->subject_slot, operation->subject_generation);
        key[3] = operation->subject_kind;
        key[4] = operation->transport_operation_id;
        (void)trevrpc_rpc_registry_remove(&runtime->operation_transport_registry, key, 5);
        trevrpc_rpc_handle_key(key, &count, operation->scope_owner, operation->scope_slot, operation->scope_generation);
        key[3] = operation->scope_kind;
        key[4] = operation->operation_id;
        (void)trevrpc_rpc_registry_remove(&runtime->operation_scope_registry, key, 5);
    }
    if (destroy_event && operation->event != NULL) {
        if (operation->event->reserved) {
            operation->event->reserved = false;
            if (runtime->mandatory_reservations != 0) {
                --runtime->mandatory_reservations;
            }
        }
        trevrpc_rpc_event_destroy(operation->event);
    }
    free(operation->continuation_frame);
    free(operation);
}

static trevrpc_rpc_operation* trevrpc_rpc_find_transport_operation_locked(trevrpc_rpc_runtime* runtime,
    uint32_t subject_kind,
    uint64_t owner,
    uint32_t slot,
    uint32_t generation,
    uint64_t transport_operation_id) {
    uint64_t key[6];
    uint8_t count;
    trevrpc_rpc_handle_key(key, &count, owner, slot, generation);
    key[3] = subject_kind;
    key[4] = transport_operation_id;
    return trevrpc_rpc_registry_find(&runtime->operation_transport_registry, key, 5);
}

static void trevrpc_rpc_signal_locked(trevrpc_rpc_runtime* runtime) {
    uint8_t byte = 1;
    ssize_t written;
    if (runtime->wake_armed) {
        return;
    }
    do {
        written = write(runtime->wake_write_fd, &byte, sizeof(byte));
    } while (written < 0 && errno == EINTR);
    if (written == (ssize_t)sizeof(byte)) {
        runtime->wake_armed = true;
        ++runtime->wake_signals;
    } else if (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        runtime->wake_armed = true;
        ++runtime->wake_write_eagain;
    } else {
        ++runtime->wake_failures;
    }
}

static int trevrpc_rpc_publish_locked(trevrpc_rpc_runtime* runtime, trevrpc_rpc_event* event, bool mandatory) {
    if (event == NULL) {
        ++runtime->events_rejected;
        return -ENOMEM;
    }
    if (!mandatory && runtime->ordinary_queue_depth >= runtime->event_capacity) {
        ++runtime->events_rejected;
        trevrpc_rpc_event_destroy(event);
        return -EAGAIN;
    }
    if (event->reserved) {
        event->reserved = false;
        if (runtime->mandatory_reservations != 0) {
            --runtime->mandatory_reservations;
        }
    }
    event->runtime = runtime;
    atomic_fetch_add_explicit(&runtime->references, 1, memory_order_relaxed);
    event->mandatory = mandatory;
    event->sequence = runtime->next_sequence++;
    if (runtime->event_tail != NULL) {
        runtime->event_tail->next = event;
    } else {
        runtime->event_head = event;
    }
    runtime->event_tail = event;
    ++runtime->queue_depth;
    if (!mandatory) {
        ++runtime->ordinary_queue_depth;
    }
    ++runtime->events_enqueued;
    trevrpc_rpc_signal_locked(runtime);
    pthread_cond_broadcast(&runtime->condition);
    return 0;
}

static int trevrpc_rpc_publish(trevrpc_rpc_runtime* runtime, trevrpc_rpc_event* event, bool mandatory) {
    int result;
    pthread_mutex_lock(&runtime->mutex);
    result = trevrpc_rpc_publish_locked(runtime, event, mandatory);
    pthread_mutex_unlock(&runtime->mutex);
    return result;
}

static int trevrpc_rpc_complete_operation_locked(trevrpc_rpc_runtime* runtime, trevrpc_rpc_operation* operation) {
    trevrpc_rpc_event* event = operation->event;
    int result;
    operation->event = NULL;
    result = trevrpc_rpc_publish_locked(runtime, event, true);
    trevrpc_rpc_operation_remove_locked(runtime, operation, false);
    return result;
}

static uint32_t trevrpc_rpc_map_transport_flags(uint32_t flags) {
    uint32_t mapped = 0;
    if ((flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_FATAL) != 0) {
        mapped |= TREVRPC_RPC_EVENT_FLAG_FATAL;
    }
    if ((flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL) != 0) {
        mapped |= TREVRPC_RPC_EVENT_FLAG_TERMINAL;
    }
    if ((flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT) != 0) {
        mapped |= TREVRPC_RPC_EVENT_FLAG_CLIENT;
    }
    if ((flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER) != 0) {
        mapped |= TREVRPC_RPC_EVENT_FLAG_SERVER;
    }
    if ((flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL) != 0) {
        mapped |= TREVRPC_RPC_EVENT_FLAG_LOCAL;
    }
    if ((flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER) != 0) {
        mapped |= TREVRPC_RPC_EVENT_FLAG_PEER;
    }
    if ((flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN) != 0) {
        mapped |= TREVRPC_RPC_EVENT_FLAG_CLEAN_FIN;
    }
    if ((flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER_RESET) != 0) {
        mapped |= TREVRPC_RPC_EVENT_FLAG_PEER_RESET | TREVRPC_RPC_EVENT_FLAG_PEER_CANCELLED;
    }
    if ((flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TRANSPORT_ERROR) != 0) {
        mapped |= TREVRPC_RPC_EVENT_FLAG_TRANSPORT_ERROR;
    }
    return mapped;
}

static void trevrpc_rpc_fill_event_from_transport(
    trevrpc_rpc_event* event, const trevrpc_rpc_transport_event_info* info) {
    if (event == NULL || info == NULL) {
        abort();
    }
    event->flags = trevrpc_rpc_map_transport_flags(info->flags);
    event->status = info->status;
    event->operation_id = info->operation_id;
    event->application_error_code = info->application_error_code;
    event->provider_error_code = info->provider_error_code;
}

static trevrpc_rpc_event* trevrpc_rpc_event_from_transport(
    const trevrpc_rpc_transport_event_info* info, uint32_t kind) {
    trevrpc_rpc_event* event = trevrpc_rpc_event_allocate(kind);
    if (event != NULL) {
        trevrpc_rpc_fill_event_from_transport(event, info);
    }
    return event;
}

static int trevrpc_rpc_copy_admission_bytes(const uint8_t* source, uint64_t source_len, uint8_t** out_copy) {
    uint8_t* copy;
    *out_copy = NULL;
    if (source_len == 0) {
        return 0;
    }
    if (source == NULL || source_len > SIZE_MAX) {
        return -EINVAL;
    }
    copy = malloc((size_t)source_len);
    if (copy == NULL) {
        return -ENOMEM;
    }
    memcpy(copy, source, (size_t)source_len);
    *out_copy = copy;
    return 0;
}

static void trevrpc_rpc_promote_admission_event(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_transport_event* transport_event,
    const trevrpc_rpc_transport_event_info* transport_info) {
    trevrpc_rpc_transport_admission_info admission = {0};
    trevrpc_rpc_endpoint_record* listener;
    trevrpc_rpc_event* event = NULL;
    uint32_t expected_protocol;
    uint32_t event_kind;
    int result;

    event_kind = transport_info->kind == TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION
                     ? TREVRPC_RPC_EVENT_HTTP3_ADMISSION
                     : TREVRPC_RPC_EVENT_WEBTRANSPORT_ADMISSION;
    expected_protocol = transport_info->kind == TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION
                            ? TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3
                            : TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT;
    result = trevrpc_rpc_transport_event_get_admission_info(runtime->transport, transport_event, &admission);
    if (result != 0 || admission.protocol != expected_protocol || (admission.path == NULL && admission.path_len != 0) ||
        (admission.authority == NULL && admission.authority_len != 0) ||
        (admission.origin == NULL && admission.origin_len != 0)) {
        trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
        return;
    }
    event = trevrpc_rpc_event_allocate(event_kind);
    if (event == NULL) {
        trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
        return;
    }
    event->admission_transport = runtime->transport;
    event->admission_event = transport_event;
    event->admission_protocol = event_kind == TREVRPC_RPC_EVENT_HTTP3_ADMISSION
                                    ? TREVRPC_RPC_ADMISSION_PROTOCOL_HTTP3
                                    : TREVRPC_RPC_ADMISSION_PROTOCOL_WEBTRANSPORT;
    event->admission_flags = TREVRPC_RPC_ADMISSION_FLAG_SECURE;
    event->admission_path_len = admission.path_len;
    event->admission_authority_len = admission.authority_len;
    event->admission_origin_len = admission.origin_len;
    result = trevrpc_rpc_copy_admission_bytes(admission.path, admission.path_len, &event->admission_path);
    if (result == 0) {
        result =
            trevrpc_rpc_copy_admission_bytes(admission.authority, admission.authority_len, &event->admission_authority);
    }
    if (result == 0) {
        result = trevrpc_rpc_copy_admission_bytes(admission.origin, admission.origin_len, &event->admission_origin);
    }
    if (result != 0) {
        trevrpc_rpc_event_destroy(event);
        return;
    }

    pthread_mutex_lock(&runtime->mutex);
    listener = trevrpc_rpc_find_endpoint_by_transport_locked(runtime, admission.listener);
    if (runtime->state != TREVRPC_RPC_STATE_RUNNING || listener == NULL || listener->terminal_committed ||
        listener->close_operation != NULL || listener->transport_kind != TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER) {
        pthread_mutex_unlock(&runtime->mutex);
        trevrpc_rpc_event_destroy(event);
        return;
    }
    event->flags = trevrpc_rpc_map_transport_flags(transport_info->flags);
    event->subject_kind = TREVRPC_RPC_OBJECT_ENDPOINT;
    event->endpoint = listener->endpoint;
    event->admission_listener = listener->endpoint;
    result = trevrpc_rpc_publish_locked(runtime, event, false);
    pthread_mutex_unlock(&runtime->mutex);
    if (result == 0) {
        return;
    }
    /* trevrpc_rpc_publish_locked destroys rejected ordinary events, including
     * their retained fail-closed transport admission capability. */
}

static trevrpc_rpc_call_record* trevrpc_rpc_call_record_allocate(void) {
    trevrpc_rpc_call_record* record = calloc(1, sizeof(*record));
    if (record == NULL) {
        return NULL;
    }
    record->stream_terminal_event = trevrpc_rpc_event_allocate(TREVRPC_RPC_EVENT_STREAM_CLOSED);
    record->call_terminal_event = trevrpc_rpc_event_allocate(TREVRPC_RPC_EVENT_CALL_CLOSED);
    if (record->stream_terminal_event == NULL || record->call_terminal_event == NULL) {
        trevrpc_rpc_event_destroy(record->stream_terminal_event);
        trevrpc_rpc_event_destroy(record->call_terminal_event);
        free(record);
        return NULL;
    }
    record->stream_terminal_event->reserved = true;
    record->call_terminal_event->reserved = true;
    record->deadline_nanos = TREVRPC_RPC_DEADLINE_INFINITE;
    record->call_deadline_nanos = TREVRPC_RPC_DEADLINE_INFINITE;
    record->initial_request_deadline_nanos = 0;
    record->send_idle_deadline_nanos = 0;
    record->receive_idle_deadline_nanos = 0;
    record->deadline_entry.record = record;
    record->deadline_entry.index = SIZE_MAX;
    record->max_send_messages = -1;
    record->max_send_body_size = -1;
    record->max_receive_messages = -1;
    record->max_receive_body_size = -1;
    record->max_response_messages = -1;
    record->max_response_body_size = -1;
    record->max_response_stream_body_size = -1;
    return record;
}

static void trevrpc_rpc_release_reserved_event_locked(trevrpc_rpc_runtime* runtime, trevrpc_rpc_event** event_pointer) {
    trevrpc_rpc_event* event = *event_pointer;
    if (event == NULL) {
        return;
    }
    *event_pointer = NULL;
    if (event->reserved) {
        event->reserved = false;
        if (runtime->mandatory_reservations != 0) {
            --runtime->mandatory_reservations;
        }
    }
    trevrpc_rpc_event_destroy(event);
}

static void trevrpc_rpc_call_release_cancellation_locked(trevrpc_rpc_call_record* record) {
    if (record->cancellation != NULL) {
        if (record->cancellation->attached_calls != 0) {
            --record->cancellation->attached_calls;
        }
        record->cancellation = NULL;
    }
}

static void trevrpc_rpc_drop_pending_receive_locked(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_record* record) {
    if (record->pending_transport_receive != NULL) {
        trevrpc_rpc_transport_receive_release(runtime->transport, record->pending_transport_receive);
        record->pending_transport_receive = NULL;
    }
}

static void trevrpc_rpc_call_record_destroy_locked(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_record* record) {
    trevrpc_rpc_call_record** cursor = &runtime->calls;
    while (*cursor != NULL && *cursor != record) {
        cursor = &(*cursor)->next;
    }
    if (*cursor == record) {
        *cursor = record->next;
    }
    trevrpc_rpc_deadline_remove_locked(runtime, record);
    {
        uint64_t key[6];
        uint8_t count;
        trevrpc_rpc_handle_key(key, &count, record->call.owner, record->call.slot, record->call.generation);
        (void)trevrpc_rpc_registry_remove(&runtime->call_registry, key, count);
        trevrpc_rpc_transport_key(key,
            &count,
            record->transport_stream,
            TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
            TREVRPC_RPC_TRANSPORT_ROLE_CALL);
        (void)trevrpc_rpc_registry_remove_value(&runtime->transport_registry, key, count, record);
    }
    trevrpc_rpc_drop_pending_receive_locked(runtime, record);
    if (!record->transport_stream_released &&
        trevrpc_rpc_transport_release_handle(
            runtime->transport, record->transport_stream, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM) != 0) {
        trevrpc_rpc_retire_handle_locked(runtime, record->transport_stream, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM);
    }
    trevrpc_rpc_call_release_cancellation_locked(record);
    trevrpc_rpc_release_reserved_event_locked(runtime, &record->stream_terminal_event);
    trevrpc_rpc_release_reserved_event_locked(runtime, &record->call_terminal_event);
    trevrpc_rpc_event_destroy(record->deferred_receive_fin);
    free(record->request_frame);
    free(record);
}

static void trevrpc_rpc_call_maybe_reclaim_locked(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_record* record) {
    if (record->stream_terminal_dequeued && record->call_terminal_dequeued && !record->caller_owns_call &&
        !record->caller_owns_stream && record->receive_in_flight == 0) {
        trevrpc_rpc_call_record_destroy_locked(runtime, record);
    }
}

static void trevrpc_rpc_endpoint_record_destroy_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_endpoint_record* record) {
    trevrpc_rpc_endpoint_record** cursor = &runtime->endpoints;
    while (*cursor != NULL && *cursor != record) {
        cursor = &(*cursor)->next;
    }
    if (*cursor == record) {
        *cursor = record->next;
    }
    {
        uint64_t key[6];
        uint8_t count;
        trevrpc_rpc_handle_key(key, &count, record->endpoint.owner, record->endpoint.slot, record->endpoint.generation);
        (void)trevrpc_rpc_registry_remove(&runtime->endpoint_registry, key, count);
        trevrpc_rpc_transport_key(
            key, &count, record->transport_endpoint, record->transport_kind, TREVRPC_RPC_TRANSPORT_ROLE_ENDPOINT);
        (void)trevrpc_rpc_registry_remove_value(&runtime->transport_registry, key, count, record);
    }
    if (!record->transport_released &&
        trevrpc_rpc_transport_release_handle(runtime->transport, record->transport_endpoint, record->transport_kind) !=
            0) {
        trevrpc_rpc_retire_handle_locked(runtime, record->transport_endpoint, record->transport_kind);
    }
    trevrpc_rpc_release_reserved_event_locked(runtime, &record->terminal_event);
    free(record);
}

static void trevrpc_rpc_endpoint_maybe_reclaim_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_endpoint_record* record) {
    if (record->terminal_dequeued && !record->caller_owned) {
        trevrpc_rpc_endpoint_record_destroy_locked(runtime, record);
    }
}

static int trevrpc_rpc_insert_call_locked(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_record* record) {
    uint64_t key[6];
    uint8_t count;
    int result;
    if (runtime->live_calls >= runtime->call_capacity || runtime->live_streams >= runtime->stream_capacity) {
        return -EAGAIN;
    }
    trevrpc_rpc_handle_key(key, &count, record->call.owner, record->call.slot, record->call.generation);
    result = trevrpc_rpc_registry_insert(&runtime->call_registry, key, count, record);
    if (result != 0) {
        return result;
    }
    trevrpc_rpc_transport_key(
        key, &count, record->transport_stream, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM, TREVRPC_RPC_TRANSPORT_ROLE_CALL);
    result = trevrpc_rpc_registry_insert_alias(&runtime->transport_registry, key, count, record);
    if (result != 0) {
        (void)trevrpc_rpc_registry_remove(
            &runtime->call_registry, (uint64_t[3]){record->call.owner, record->call.slot, record->call.generation}, 3);
        return result;
    }
    record->next = runtime->calls;
    runtime->calls = record;
    ++runtime->live_calls;
    ++runtime->live_streams;
    runtime->mandatory_reservations += 2;
    result = trevrpc_rpc_deadline_refresh_locked(runtime, record);
    if (result != 0) {
        trevrpc_rpc_call_record** cursor = &runtime->calls;
        while (*cursor != record)
            cursor = &(*cursor)->next;
        *cursor = record->next;
        (void)trevrpc_rpc_registry_remove(
            &runtime->call_registry, (uint64_t[3]){record->call.owner, record->call.slot, record->call.generation}, 3);
        trevrpc_rpc_transport_key(key,
            &count,
            record->transport_stream,
            TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
            TREVRPC_RPC_TRANSPORT_ROLE_CALL);
        (void)trevrpc_rpc_registry_remove_value(&runtime->transport_registry, key, count, record);
        --runtime->live_calls;
        --runtime->live_streams;
        runtime->mandatory_reservations -= 2;
        return result;
    }
    return 0;
}

static int trevrpc_rpc_find_or_create_peer_call_locked(
    trevrpc_rpc_runtime* runtime, const trevrpc_rpc_transport_event_info* info, trevrpc_rpc_call_record** out_record) {
    trevrpc_rpc_call_record* record = trevrpc_rpc_find_call_by_transport_locked(runtime, info->subject);
    trevrpc_rpc_endpoint_v1 endpoint = {0};
    trevrpc_rpc_endpoint_record* endpoint_record;
    trevrpc_rpc_transport_connection_record* connection_record;
    int result;
    *out_record = record;
    if (record != NULL) {
        return 0;
    }
    if ((info->flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER) == 0) {
        return -ENOENT;
    }
    endpoint_record = trevrpc_rpc_find_endpoint_by_transport_locked(runtime, info->parent);
    connection_record = trevrpc_rpc_find_transport_connection_locked(runtime, info->parent);
    if (endpoint_record != NULL && !endpoint_record->terminal_committed) {
        endpoint = endpoint_record->endpoint;
    } else if (connection_record != NULL) {
        endpoint = connection_record->endpoint;
    }
    if (trevrpc_rpc_handle_is_zero(endpoint.owner, endpoint.slot, endpoint.generation)) {
        return -ENOENT;
    }
    record = trevrpc_rpc_call_record_allocate();
    if (record == NULL) {
        return -ENOMEM;
    }
    result = trevrpc_rpc_allocate_handle_locked(
        runtime, &runtime->call_sequence, &record->call.owner, &record->call.slot, &record->call.generation);
    if (result != 0) {
        trevrpc_rpc_event_destroy(record->stream_terminal_event);
        trevrpc_rpc_event_destroy(record->call_terminal_event);
        free(record);
        return result;
    }
    record->stream = (trevrpc_rpc_stream_v1){record->call.owner, record->call.slot, record->call.generation};
    record->endpoint = endpoint;
    record->transport_stream = info->subject;
    record->kind = TREVRPC_RPC_KIND_UNARY;
    record->waiting_for_request = true;
    record->max_send_messages = runtime->max_stream_messages;
    record->max_send_body_size = runtime->max_stream_body_size;
    record->max_receive_messages = runtime->max_stream_messages;
    record->max_receive_body_size = runtime->max_stream_body_size;
    record->initial_request_deadline_nanos = trevrpc_rpc_idle_deadline(runtime->initial_request_timeout_nanos);
    result = trevrpc_rpc_insert_call_locked(runtime, record);
    if (result != 0) {
        trevrpc_rpc_event_destroy(record->stream_terminal_event);
        trevrpc_rpc_event_destroy(record->call_terminal_event);
        free(record);
        return result;
    }
    *out_record = record;
    return 0;
}

static int trevrpc_rpc_copy_request_event(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_endpoint_v1 endpoint,
    trevrpc_rpc_call_v1 call,
    trevrpc_rpc_stream_v1 stream,
    trevrpc_rpc_transport_handle transport_stream,
    trevrpc_rpc_transport_receive* transport_receive,
    const trevrpc_rpc_transport_receive_info* receive_info) {
    trevrpc_wire_request_diagnostic diagnostic;
    trevrpc_wire_request_values request = {0};
    trevrpc_rpc_event* event = NULL;
    trevrpc_rpc_event* readable_event = NULL;
    trevrpc_rpc_event* deferred_receive_fin = NULL;
    trevrpc_rpc_receive* receive = NULL;
    trevrpc_rpc_call_record* record;
    uint64_t deadline_nanos;
    bool admitted = false;
    int result = trevrpc_wire_decode_request_diagnostic(
        receive_info->data, (size_t)receive_info->data_len, &request, &diagnostic);
    if (result != 0) {
        trevrpc_rpc_transport_receive_release(runtime->transport, transport_receive);
        trevrpc_rpc_abort_or_terminal(
            runtime, transport_stream, trevrpc_rpc_request_failure_status(result, diagnostic.reason), result);
        return result;
    }
    if (request.kind > TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING || request.service_len > UINT32_MAX ||
        request.method_len > UINT32_MAX || request.body_len > runtime->max_message_size ||
        trevrpc_rpc_validate_metadata_limits(runtime, &request.metadata) != 0 ||
        ((request.kind == TREVRPC_RPC_KIND_CLIENT_STREAMING ||
             request.kind == TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) &&
            ((runtime->max_stream_messages >= 0 && runtime->max_stream_messages == 0) ||
                (runtime->max_stream_body_size >= 0 && request.body_len > (uint64_t)runtime->max_stream_body_size)))) {
        trevrpc_internal_request_reset(&request);
        trevrpc_rpc_transport_receive_release(runtime->transport, transport_receive);
        trevrpc_rpc_abort_or_terminal(runtime, transport_stream, TREVRPC_RPC_STATUS_RESOURCE_EXHAUSTED, -EMSGSIZE);
        return -EMSGSIZE;
    }
    result = trevrpc_rpc_deadline_after(request.timeout_nanos, &deadline_nanos);
    if (result != 0) {
        trevrpc_internal_request_reset(&request);
        trevrpc_rpc_transport_receive_release(runtime->transport, transport_receive);
        trevrpc_rpc_abort_or_terminal(
            runtime, transport_stream, TREVRPC_RPC_STATUS_INTERNAL, result != 0 ? result : -EIO);
        return result;
    }
    event = trevrpc_rpc_event_allocate(TREVRPC_RPC_EVENT_CALL_INCOMING);
    if (request.kind == TREVRPC_RPC_KIND_CLIENT_STREAMING || request.kind == TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        readable_event = trevrpc_rpc_event_allocate(TREVRPC_RPC_EVENT_STREAM_READABLE);
    }
    receive = calloc(1, sizeof(*receive));
    if (event == NULL ||
        ((request.kind == TREVRPC_RPC_KIND_CLIENT_STREAMING ||
             request.kind == TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) &&
            readable_event == NULL) ||
        receive == NULL || (request.service_len != 0 && (event->service = malloc(request.service_len)) == NULL) ||
        (request.method_len != 0 && (event->method = malloc(request.method_len)) == NULL) ||
        (request.body_len != 0 && (receive->data = malloc(request.body_len)) == NULL)) {
        trevrpc_rpc_event_destroy(event);
        trevrpc_rpc_event_destroy(readable_event);
        trevrpc_rpc_receive_destroy(receive);
        trevrpc_internal_request_reset(&request);
        trevrpc_rpc_transport_receive_release(runtime->transport, transport_receive);
        trevrpc_rpc_abort_or_terminal(runtime, transport_stream, TREVRPC_RPC_STATUS_RESOURCE_EXHAUSTED, -ENOMEM);
        return -ENOMEM;
    }
    if (request.service_len != 0) {
        memcpy(event->service, request.service, request.service_len);
    }
    if (request.method_len != 0) {
        memcpy(event->method, request.method, request.method_len);
    }
    if (request.body_len != 0) {
        memcpy(receive->data, request.body, request.body_len);
    }
    result = trevrpc_rpc_copy_metadata(runtime, receive, &request.metadata);
    if (result != 0) {
        trevrpc_rpc_event_destroy(event);
        trevrpc_rpc_event_destroy(readable_event);
        trevrpc_rpc_receive_destroy(receive);
        trevrpc_internal_request_reset(&request);
        trevrpc_rpc_transport_receive_release(runtime->transport, transport_receive);
        trevrpc_rpc_abort_or_terminal(runtime, transport_stream, TREVRPC_RPC_STATUS_RESOURCE_EXHAUSTED, -ENOMEM);
        return result;
    }
    event->flags = TREVRPC_RPC_EVENT_FLAG_SERVER | TREVRPC_RPC_EVENT_FLAG_PEER |
                   TREVRPC_RPC_EVENT_FLAG_HAS_INCOMING_CALL | TREVRPC_RPC_EVENT_FLAG_HAS_RECEIVE;
    event->subject_kind = TREVRPC_RPC_OBJECT_CALL;
    event->endpoint = endpoint;
    event->call = call;
    event->stream = stream;
    event->rpc_kind = request.kind;
    event->service_len = (uint32_t)request.service_len;
    event->method_len = (uint32_t)request.method_len;
    receive->kind = TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE;
    receive->data_len = request.body_len;
    event->receive = receive;
    if (readable_event != NULL) {
        readable_event->flags = TREVRPC_RPC_EVENT_FLAG_SERVER | TREVRPC_RPC_EVENT_FLAG_PEER;
        readable_event->subject_kind = TREVRPC_RPC_OBJECT_STREAM;
        readable_event->endpoint = endpoint;
        readable_event->call = call;
        readable_event->stream = stream;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_call_locked(runtime, call);
    if (record != NULL && !record->closing && !record->stream_terminal_committed && !record->call_terminal_committed) {
        record->kind = request.kind;
        record->call_deadline_nanos = deadline_nanos;
        record->has_deadline = deadline_nanos != TREVRPC_RPC_DEADLINE_INFINITE;
        record->deadline_nanos = deadline_nanos;
        record->initial_request_deadline_nanos = 0;
        record->waiting_for_request = false;
        if (request.kind == TREVRPC_RPC_KIND_CLIENT_STREAMING ||
            request.kind == TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
            record->request_message_count = request.body_len == 0 ? 0 : 1;
            record->request_body_size = request.body_len;
        }
        if (request.kind == TREVRPC_RPC_KIND_CLIENT_STREAMING ||
            request.kind == TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
            trevrpc_rpc_reset_idle_locked(runtime, record, false);
        } else {
            (void)trevrpc_rpc_deadline_refresh_locked(runtime, record);
        }
        if (record->deadline_nanos != TREVRPC_RPC_DEADLINE_INFINITE || record->receive_idle_deadline_nanos != 0) {
            trevrpc_rpc_signal_driver_locked(runtime);
        }
        if (readable_event != NULL) {
            record->readable_flags = readable_event->flags;
            ++record->readable_epoch;
        } else {
            deferred_receive_fin = record->deferred_receive_fin;
            record->deferred_receive_fin = NULL;
        }
        admitted = true;

        /* The incoming notification owns the only copied request payload.
         * Once transport input has been consumed, it must not be rejected
         * merely because the ordinary event budget is full. Mandatory
         * publication keeps FIFO ordering while using the already bounded
         * peer-call admission budget rather than dropping the notification and
         * request. Streaming requests also receive one bounded mandatory drain
         * hint because decoding the initial request consumed the transport's
         * level-triggered notice. Publish the bundle under one lock so an
         * application command cannot insert its completion between these
         * causally ordered notifications. */
        result = trevrpc_rpc_publish_locked(runtime, event, true);
        if (result == 0) {
            event = NULL;
        }
        if (result == 0 && readable_event != NULL) {
            result = trevrpc_rpc_publish_locked(runtime, readable_event, true);
            if (result == 0) {
                ++record->readable_events_pending;
                readable_event = NULL;
            }
        }
        if (result == 0 && deferred_receive_fin != NULL) {
            result = trevrpc_rpc_publish_locked(runtime, deferred_receive_fin, true);
            if (result == 0) {
                deferred_receive_fin = NULL;
            }
        }
        if (result == 0) {
            trevrpc_rpc_flush_receive_barrier_locked(runtime, record);
        }
    }
    pthread_mutex_unlock(&runtime->mutex);
    trevrpc_internal_request_reset(&request);
    trevrpc_rpc_transport_receive_release(runtime->transport, transport_receive);
    if (!admitted) {
        trevrpc_rpc_event_destroy(event);
        trevrpc_rpc_event_destroy(readable_event);
        return -ESTALE;
    }
    if (result != 0) {
        trevrpc_rpc_event_destroy(event);
        trevrpc_rpc_event_destroy(readable_event);
        trevrpc_rpc_event_destroy(deferred_receive_fin);
        trevrpc_rpc_abort_or_terminal(runtime, transport_stream, TREVRPC_RPC_STATUS_RESOURCE_EXHAUSTED, -ENOMEM);
    }
    return result;
}

static void trevrpc_rpc_complete_endpoint_start_locked(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_endpoint_record* record,
    const trevrpc_rpc_transport_event_info* info,
    uint32_t kind) {
    trevrpc_rpc_operation* operation = record->start_operation;
    if (operation == NULL) {
        return;
    }
    record->start_operation = NULL;
    operation->event->kind = kind;
    trevrpc_rpc_fill_event_from_transport(operation->event, info);
    operation->event->subject_kind = TREVRPC_RPC_OBJECT_ENDPOINT;
    operation->event->endpoint = record->endpoint;
    operation->event->operation_id = operation->operation_id;
    (void)trevrpc_rpc_complete_operation_locked(runtime, operation);
}

static void trevrpc_rpc_complete_call_open_locked(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_call_record* record,
    const trevrpc_rpc_transport_event_info* info,
    uint32_t kind,
    int32_t status) {
    trevrpc_rpc_operation* operation = record->open_operation;
    if (operation == NULL) {
        return;
    }
    record->open_operation = NULL;
    record->request_send_pending = false;
    free(record->request_frame);
    record->request_frame = NULL;
    record->request_frame_len = 0;
    operation->event->kind = kind;
    trevrpc_rpc_fill_event_from_transport(operation->event, info);
    operation->event->subject_kind = TREVRPC_RPC_OBJECT_CALL;
    operation->event->endpoint = record->endpoint;
    operation->event->call = record->call;
    operation->event->stream = record->stream;
    operation->event->operation_id = operation->operation_id;
    operation->event->status = status;
    (void)trevrpc_rpc_complete_operation_locked(runtime, operation);
}

static uint32_t trevrpc_rpc_operation_completion_subject(const trevrpc_rpc_operation* operation) {
    return operation->kind == TREVRPC_RPC_OPERATION_CALL_ACCEPT ||
                   operation->kind == TREVRPC_RPC_OPERATION_CALL_CANCEL ||
                   operation->kind == TREVRPC_RPC_OPERATION_CALL_FINISH
               ? TREVRPC_RPC_OBJECT_CALL
               : operation->subject_kind;
}

static void trevrpc_rpc_fail_call_operations_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_record* record, const trevrpc_rpc_transport_event_info* info) {
    trevrpc_rpc_operation* operation;
    int32_t failure_status = info->status != 0 ? info->status : -EPIPE;
    for (;;) {
        operation = runtime->operations;
        while (operation != NULL) {
            bool belongs = (operation->subject_kind == TREVRPC_RPC_OBJECT_CALL &&
                               trevrpc_rpc_call_equal((trevrpc_rpc_call_v1){operation->subject_owner,
                                                          operation->subject_slot,
                                                          operation->subject_generation},
                                   record->call)) ||
                           (operation->subject_kind == TREVRPC_RPC_OBJECT_STREAM &&
                               trevrpc_rpc_stream_equal((trevrpc_rpc_stream_v1){operation->subject_owner,
                                                            operation->subject_slot,
                                                            operation->subject_generation},
                                   record->stream));
            if (belongs && operation != record->close_operation && operation->kind != TREVRPC_RPC_OPERATION_CALL_OPEN) {
                trevrpc_rpc_event* event = operation->event;
                event->flags = trevrpc_rpc_map_transport_flags(info->flags);
                event->status = failure_status;
                event->application_error_code = info->application_error_code;
                event->provider_error_code = info->provider_error_code;
                event->subject_kind = trevrpc_rpc_operation_completion_subject(operation);
                event->endpoint = record->endpoint;
                event->call = record->call;
                event->stream = record->stream;
                event->operation_id = operation->operation_id;
                (void)trevrpc_rpc_complete_operation_locked(runtime, operation);
                break;
            }
            operation = operation->next;
        }
        if (operation == NULL) {
            break;
        }
    }
    record->request_send_pending = false;
    record->send_finish_pending = false;
    record->send_closed = true;
    record->send_idle_deadline_nanos = 0;
    (void)trevrpc_rpc_deadline_refresh_locked(runtime, record);
}

static bool trevrpc_rpc_operation_uses_send_direction(const trevrpc_rpc_operation* operation) {
    return operation->kind == TREVRPC_RPC_OPERATION_SEND || operation->kind == TREVRPC_RPC_OPERATION_RESPOND ||
           operation->kind == TREVRPC_RPC_OPERATION_STREAM_FINISH ||
           operation->kind == TREVRPC_RPC_OPERATION_CALL_FINISH;
}

static void trevrpc_rpc_handle_send_stopped_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_record* record, const trevrpc_rpc_transport_event_info* info) {
    trevrpc_rpc_operation* operation;
    int32_t failure_status = info->status != 0 ? info->status : -EPIPE;
    if (record->request_send_pending && record->open_operation != NULL) {
        trevrpc_rpc_transport_event_info ready = *info;
        ready.flags = TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT;
        ready.status = 0;
        ready.application_error_code = 0;
        ready.provider_error_code = 0;
        trevrpc_rpc_complete_call_open_locked(runtime, record, &ready, TREVRPC_RPC_EVENT_CALL_READY, 0);
    }
    for (;;) {
        operation = runtime->operations;
        while (operation != NULL) {
            bool belongs =
                operation->scope_kind == TREVRPC_RPC_OBJECT_CALL &&
                trevrpc_rpc_call_equal(
                    (trevrpc_rpc_call_v1){operation->scope_owner, operation->scope_slot, operation->scope_generation},
                    record->call);
            if (belongs && operation != record->close_operation &&
                trevrpc_rpc_operation_uses_send_direction(operation)) {
                bool peer_settles_local_finish =
                    record->local && operation->kind == TREVRPC_RPC_OPERATION_STREAM_FINISH;
                trevrpc_rpc_fill_event_from_transport(operation->event, info);
                operation->event->status = peer_settles_local_finish ? 0 : failure_status;
                operation->event->subject_kind = trevrpc_rpc_operation_completion_subject(operation);
                operation->event->endpoint = record->endpoint;
                operation->event->call = record->call;
                operation->event->stream = record->stream;
                operation->event->operation_id = operation->operation_id;
                (void)trevrpc_rpc_complete_operation_locked(runtime, operation);
                break;
            }
            operation = operation->next;
        }
        if (operation == NULL) {
            break;
        }
    }
    record->request_send_pending = false;
    record->send_finish_pending = false;
    record->send_closed = true;
    record->send_idle_deadline_nanos = 0;
    (void)trevrpc_rpc_deadline_refresh_locked(runtime, record);
}

static void trevrpc_rpc_handle_send_stopped(
    trevrpc_rpc_runtime* runtime, const trevrpc_rpc_transport_event_info* info) {
    trevrpc_rpc_call_record* record;
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_call_by_transport_locked(runtime, info->subject);
    if (record != NULL) {
        trevrpc_rpc_handle_send_stopped_locked(runtime, record, info);
    }
    pthread_mutex_unlock(&runtime->mutex);
}

static void trevrpc_rpc_publish_call_terminals_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_record* record, const trevrpc_rpc_transport_event_info* info) {
    trevrpc_rpc_event* stream_event;
    trevrpc_rpc_event* call_event;
    trevrpc_rpc_operation* close_operation = record->close_operation;
    uint64_t close_application_error_code =
        close_operation != NULL && close_operation->event != NULL ? close_operation->event->application_error_code : 0;
    if (record->stream_terminal_committed || record->call_terminal_committed) {
        return;
    }
    if (record->suppress_terminals) {
        trevrpc_rpc_event_destroy(record->deferred_receive_fin);
        record->deferred_receive_fin = NULL;
        trevrpc_rpc_drop_pending_receive_locked(runtime, record);
        record->stream_terminal_committed = true;
        record->call_terminal_committed = true;
        record->stream_terminal_dequeued = true;
        record->call_terminal_dequeued = true;
        record->closing = true;
        record->deadline_nanos = TREVRPC_RPC_DEADLINE_INFINITE;
        record->initial_request_deadline_nanos = 0;
        trevrpc_rpc_deadline_remove_locked(runtime, record);
        trevrpc_rpc_release_reserved_event_locked(runtime, &record->stream_terminal_event);
        trevrpc_rpc_release_reserved_event_locked(runtime, &record->call_terminal_event);
        if (runtime->live_streams != 0) {
            --runtime->live_streams;
        }
        if (runtime->live_calls != 0) {
            --runtime->live_calls;
        }
        trevrpc_rpc_call_maybe_reclaim_locked(runtime, record);
        return;
    }
    if (record->deferred_receive_fin != NULL) {
        trevrpc_rpc_event* receive_fin = record->deferred_receive_fin;
        record->deferred_receive_fin = NULL;
        (void)trevrpc_rpc_publish_locked(runtime, receive_fin, true);
    }
    trevrpc_rpc_drop_pending_receive_locked(runtime, record);
    record->readable_drained_epoch = record->readable_epoch;
    if (record->open_operation != NULL) {
        int32_t open_status = record->deadline_expired ? -ETIMEDOUT : info->status != 0 ? info->status : -EPIPE;
        trevrpc_rpc_complete_call_open_locked(runtime, record, info, TREVRPC_RPC_EVENT_CALL_FAILED, open_status);
    }
    trevrpc_rpc_fail_call_operations_locked(runtime, record, info);
    if (close_operation != NULL && close_operation->kind == TREVRPC_RPC_OPERATION_STREAM_CLOSE) {
        stream_event = close_operation->event;
        close_operation->event = NULL;
        trevrpc_rpc_release_reserved_event_locked(runtime, &record->stream_terminal_event);
    } else {
        stream_event = record->stream_terminal_event;
        record->stream_terminal_event = NULL;
    }
    if (close_operation != NULL && close_operation->kind == TREVRPC_RPC_OPERATION_CALL_CLOSE) {
        call_event = close_operation->event;
        close_operation->event = NULL;
        trevrpc_rpc_release_reserved_event_locked(runtime, &record->call_terminal_event);
    } else {
        call_event = record->call_terminal_event;
        record->call_terminal_event = NULL;
    }
    if (stream_event == NULL || call_event == NULL) {
        abort();
    }
    if ((info->flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER_RESET) != 0 ||
        ((info->flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER) != 0 &&
            (info->flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN) == 0)) {
        record->cancelled = true;
    }
    trevrpc_rpc_fill_event_from_transport(stream_event, info);
    stream_event->kind = TREVRPC_RPC_EVENT_STREAM_CLOSED;
    stream_event->subject_kind = TREVRPC_RPC_OBJECT_STREAM;
    stream_event->endpoint = record->endpoint;
    stream_event->call = record->call;
    stream_event->stream = record->stream;
    stream_event->operation_id = close_operation != NULL && close_operation->kind == TREVRPC_RPC_OPERATION_STREAM_CLOSE
                                     ? close_operation->operation_id
                                     : 0;
    if (close_operation != NULL && close_operation->kind == TREVRPC_RPC_OPERATION_STREAM_CLOSE) {
        stream_event->application_error_code = close_application_error_code;
    }
    trevrpc_rpc_fill_event_from_transport(call_event, info);
    if (record->deadline_expired) {
        stream_event->flags |= TREVRPC_RPC_EVENT_FLAG_LOCAL;
        stream_event->status = -ETIMEDOUT;
        stream_event->rpc_status = TREVRPC_RPC_STATUS_DEADLINE_EXCEEDED;
        stream_event->application_error_code = TREVRPC_RPC_STATUS_DEADLINE_EXCEEDED;
        call_event->flags |= TREVRPC_RPC_EVENT_FLAG_LOCAL;
        call_event->status = -ETIMEDOUT;
        call_event->rpc_status = TREVRPC_RPC_STATUS_DEADLINE_EXCEEDED;
        call_event->application_error_code = TREVRPC_RPC_STATUS_DEADLINE_EXCEEDED;
    }
    call_event->kind = TREVRPC_RPC_EVENT_CALL_CLOSED;
    call_event->subject_kind = TREVRPC_RPC_OBJECT_CALL;
    call_event->endpoint = record->endpoint;
    call_event->call = record->call;
    call_event->stream = record->stream;
    call_event->operation_id = close_operation != NULL && close_operation->kind == TREVRPC_RPC_OPERATION_CALL_CLOSE
                                   ? close_operation->operation_id
                                   : 0;
    if (close_operation != NULL && close_operation->kind == TREVRPC_RPC_OPERATION_CALL_CLOSE) {
        call_event->application_error_code = close_application_error_code;
    }
    record->stream_terminal_committed = true;
    record->call_terminal_committed = true;
    record->closing = true;
    record->deadline_nanos = TREVRPC_RPC_DEADLINE_INFINITE;
    trevrpc_rpc_deadline_remove_locked(runtime, record);
    trevrpc_rpc_call_release_cancellation_locked(record);
    if (runtime->live_streams != 0) {
        --runtime->live_streams;
    }
    if (runtime->live_calls != 0) {
        --runtime->live_calls;
    }
    (void)trevrpc_rpc_publish_locked(runtime, stream_event, true);
    (void)trevrpc_rpc_publish_locked(runtime, call_event, true);
    record->deferred_stream_terminal_valid = false;
    if (close_operation != NULL) {
        record->close_operation = NULL;
        trevrpc_rpc_operation_remove_locked(runtime, close_operation, false);
    }
}

static void trevrpc_rpc_flush_receive_barrier_locked(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_record* record) {
    trevrpc_rpc_event* receive_fin;
    trevrpc_rpc_transport_event_info terminal_info;
    bool terminal_valid;
    if (record->waiting_for_request || record->readable_drained_epoch != record->readable_epoch) {
        return;
    }
    receive_fin = record->deferred_receive_fin;
    record->deferred_receive_fin = NULL;
    terminal_valid = record->deferred_stream_terminal_valid;
    terminal_info = record->deferred_stream_terminal;
    record->deferred_stream_terminal_valid = false;
    if (receive_fin != NULL) {
        (void)trevrpc_rpc_publish_locked(runtime, receive_fin, true);
    }
    if (terminal_valid && !record->stream_terminal_committed) {
        trevrpc_rpc_publish_call_terminals_locked(runtime, record, &terminal_info);
    }
}

static void trevrpc_rpc_mark_readable_drained_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_record* record, uint64_t readable_epoch) {
    if (record != NULL && record->readable_epoch == readable_epoch) {
        record->readable_drained_epoch = readable_epoch;
        trevrpc_rpc_flush_receive_barrier_locked(runtime, record);
    }
}

static void trevrpc_rpc_publish_endpoint_terminal_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_endpoint_record* record, const trevrpc_rpc_transport_event_info* info) {
    trevrpc_rpc_event* event;
    trevrpc_rpc_operation* operation = record->close_operation;
    if (record->terminal_committed) {
        return;
    }
    if (record->start_operation != NULL) {
        trevrpc_rpc_complete_endpoint_start_locked(runtime, record, info, TREVRPC_RPC_EVENT_ENDPOINT_FAILED);
    }
    if (operation != NULL) {
        assert(operation->event != NULL);
        event = operation->event;
        operation->event = NULL;
        trevrpc_rpc_release_reserved_event_locked(runtime, &record->terminal_event);
    } else {
        event = record->terminal_event;
        record->terminal_event = NULL;
    }
    assert(event != NULL);
    trevrpc_rpc_fill_event_from_transport(event, info);
    event->kind = TREVRPC_RPC_EVENT_ENDPOINT_CLOSED;
    event->subject_kind = TREVRPC_RPC_OBJECT_ENDPOINT;
    event->endpoint = record->endpoint;
    event->operation_id = operation == NULL ? 0 : operation->operation_id;
    record->terminal_committed = true;
    if (runtime->live_endpoints != 0) {
        --runtime->live_endpoints;
    }
    (void)trevrpc_rpc_publish_locked(runtime, event, true);
    if (operation != NULL) {
        record->close_operation = NULL;
        trevrpc_rpc_operation_remove_locked(runtime, operation, false);
    }
}

static uint32_t trevrpc_rpc_request_failure_status(int result, trevrpc_wire_diagnostic_reason diagnostic) {
    if (result == -EMSGSIZE || result == -ENOMEM || diagnostic == TREVRPC_WIRE_DIAGNOSTIC_ALLOCATION_FAILURE) {
        return TREVRPC_RPC_STATUS_RESOURCE_EXHAUSTED;
    }
    if (result == TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION) {
        return TREVRPC_RPC_STATUS_FAILED_PRECONDITION;
    }
    return TREVRPC_RPC_STATUS_INVALID_ARGUMENT;
}

static uint32_t trevrpc_rpc_receive_failure_status(int result, trevrpc_wire_diagnostic_reason diagnostic) {
    if (result == -EMSGSIZE) {
        return TREVRPC_RPC_STATUS_RESOURCE_EXHAUSTED;
    }
    if (diagnostic == TREVRPC_WIRE_DIAGNOSTIC_UNSUPPORTED_FRAME_KIND) {
        return TREVRPC_RPC_STATUS_INVALID_ARGUMENT;
    }
    return TREVRPC_RPC_STATUS_INTERNAL;
}

static void trevrpc_rpc_abort_or_terminal(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_transport_handle stream,
    uint64_t application_error_code,
    int32_t status) {
    trevrpc_rpc_call_record* record;
    int abort_result;
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_call_by_transport_locked(runtime, stream);
    if (record != NULL && !record->stream_terminal_committed) {
        record->closing = true;
        record->cancelled = true;
    }
    pthread_mutex_unlock(&runtime->mutex);
    abort_result = trevrpc_rpc_transport_stream_abort(runtime->transport, stream, application_error_code);
    if (abort_result != 0 && abort_result != -EALREADY) {
        trevrpc_rpc_transport_event_info info = {0};
        info.flags = TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL;
        info.status = status;
        info.application_error_code = application_error_code;
        info.provider_error_code = (uint64_t)(uint32_t)(-abort_result);
        pthread_mutex_lock(&runtime->mutex);
        record = trevrpc_rpc_find_call_by_transport_locked(runtime, stream);
        if (record != NULL && !record->stream_terminal_committed) {
            trevrpc_rpc_publish_call_terminals_locked(runtime, record, &info);
        }
        pthread_mutex_unlock(&runtime->mutex);
    }
}

static bool trevrpc_rpc_send_direction_stopped_status(int32_t status) {
    return status == -ECANCELED || status == -EPIPE || status == -EALREADY;
}

static void trevrpc_rpc_handle_send_complete(
    trevrpc_rpc_runtime* runtime, const trevrpc_rpc_transport_event_info* info) {
    trevrpc_rpc_call_record* record;
    trevrpc_rpc_operation* operation;
    bool request_failed = false;
    int finish_result = 0;
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_call_by_transport_locked(runtime, info->subject);
    if (record != NULL && record->request_send_pending && record->open_operation != NULL &&
        record->open_operation->transport_operation_id == info->operation_id) {
        int32_t status = info->status;
        record->request_send_pending = false;
        free(record->request_frame);
        record->request_frame = NULL;
        record->request_frame_len = 0;
        if (trevrpc_rpc_send_direction_stopped_status(status)) {
            status = 0;
            record->send_closed = true;
            record->send_idle_deadline_nanos = 0;
        }
        if (status == 0 && !record->send_closed &&
            (record->kind == TREVRPC_RPC_KIND_UNARY || record->kind == TREVRPC_RPC_KIND_SERVER_STREAMING)) {
            status = trevrpc_rpc_transport_stream_finish_send(runtime->transport, info->subject);
            if (status == 0 || trevrpc_rpc_send_direction_stopped_status(status)) {
                status = 0;
                record->send_closed = true;
            } else {
                record->closing = true;
                (void)trevrpc_rpc_transport_stream_abort(runtime->transport, info->subject, 0);
            }
        }
        if (status != 0) {
            trevrpc_rpc_transport_event_info failure = *info;
            failure.status = status;
            record->closing = true;
            trevrpc_rpc_complete_call_open_locked(runtime, record, &failure, TREVRPC_RPC_EVENT_CALL_FAILED, status);
            trevrpc_rpc_publish_call_terminals_locked(runtime, record, &failure);
            request_failed = true;
        } else {
            trevrpc_rpc_reset_idle_locked(runtime, record, false);
            if (record->receive_idle_deadline_nanos != 0) {
                trevrpc_rpc_signal_driver_locked(runtime);
            }
            if (!record->awaiting_transport_acceptance || record->transport_accepted) {
                trevrpc_rpc_complete_call_open_locked(runtime, record, info, TREVRPC_RPC_EVENT_CALL_READY, status);
            }
        }
        pthread_mutex_unlock(&runtime->mutex);
        if (request_failed) {
            (void)trevrpc_rpc_transport_stream_abort(runtime->transport, info->subject, 0);
        }
        return;
    }
    if (record == NULL) {
        pthread_mutex_unlock(&runtime->mutex);
        return;
    }
    operation = trevrpc_rpc_find_transport_operation_locked(runtime,
        TREVRPC_RPC_OBJECT_STREAM,
        record->stream.owner,
        record->stream.slot,
        record->stream.generation,
        info->operation_id);
    if (operation == NULL) {
        pthread_mutex_unlock(&runtime->mutex);
        return;
    }
    if (operation->kind == TREVRPC_RPC_OPERATION_RESPOND && operation->continuation_frame != NULL) {
        int continuation_result = info->status;
        if (continuation_result == 0) {
            uint8_t* continuation_frame = operation->continuation_frame;
            size_t continuation_frame_len = operation->continuation_frame_len;
            operation->continuation_frame = NULL;
            operation->continuation_frame_len = 0;
            if (runtime->next_transport_operation_id == UINT64_MAX) {
                continuation_result = -EOVERFLOW;
            } else {
                uint64_t transport_operation_id = runtime->next_transport_operation_id++;
                continuation_result =
                    trevrpc_rpc_operation_rekey_transport_locked(runtime, operation, transport_operation_id);
                if (continuation_result == 0) {
                    continuation_result = trevrpc_rpc_transport_stream_send(runtime->transport,
                        info->subject,
                        operation->transport_operation_id,
                        continuation_frame + 4,
                        continuation_frame_len - 4);
                }
            }
            free(continuation_frame);
            if (continuation_result == 0) {
                pthread_mutex_unlock(&runtime->mutex);
                return;
            }
        } else {
            free(operation->continuation_frame);
            operation->continuation_frame = NULL;
            operation->continuation_frame_len = 0;
        }
        trevrpc_rpc_fill_event_from_transport(operation->event, info);
        operation->event->status = continuation_result;
    } else {
        trevrpc_rpc_fill_event_from_transport(operation->event, info);
    }
    operation->event->subject_kind = trevrpc_rpc_operation_completion_subject(operation);
    operation->event->endpoint = record->endpoint;
    operation->event->call = record->call;
    operation->event->stream = record->stream;
    operation->event->operation_id = operation->operation_id;
    if (record->local && operation->kind == TREVRPC_RPC_OPERATION_STREAM_FINISH &&
        trevrpc_rpc_send_direction_stopped_status(operation->event->status)) {
        operation->event->status = 0;
    }
    if ((operation->kind == TREVRPC_RPC_OPERATION_RESPOND || operation->kind == TREVRPC_RPC_OPERATION_CALL_FINISH ||
            operation->kind == TREVRPC_RPC_OPERATION_STREAM_FINISH) &&
        operation->event->status != 0) {
        trevrpc_rpc_transport_event_info failure = *info;
        failure.status = operation->event->status;
        record->send_finish_pending = false;
        record->closing = true;
        (void)trevrpc_rpc_complete_operation_locked(runtime, operation);
        trevrpc_rpc_publish_call_terminals_locked(runtime, record, &failure);
        pthread_mutex_unlock(&runtime->mutex);
        (void)trevrpc_rpc_transport_stream_abort(runtime->transport, info->subject, 0);
        return;
    }
    if (operation->kind == TREVRPC_RPC_OPERATION_RESPOND || operation->kind == TREVRPC_RPC_OPERATION_CALL_FINISH ||
        operation->kind == TREVRPC_RPC_OPERATION_STREAM_FINISH) {
        record->send_finish_pending = false;
        record->send_closed = true;
        record->send_idle_deadline_nanos = 0;
        finish_result = trevrpc_rpc_transport_stream_finish_send(runtime->transport, info->subject);
        if (record->local && operation->kind == TREVRPC_RPC_OPERATION_STREAM_FINISH &&
            trevrpc_rpc_send_direction_stopped_status(finish_result)) {
            finish_result = 0;
        }
        if (finish_result != 0) {
            operation->event->status = finish_result;
        }
    }
    if (operation->kind == TREVRPC_RPC_OPERATION_CALL_FINISH) {
        operation->event->subject_kind = TREVRPC_RPC_OBJECT_CALL;
        operation->event->call = record->call;
        operation->event->stream = record->stream;
    }
    (void)trevrpc_rpc_complete_operation_locked(runtime, operation);
    if (finish_result != 0) {
        trevrpc_rpc_transport_event_info failure = *info;
        failure.status = finish_result;
        record->closing = true;
        trevrpc_rpc_publish_call_terminals_locked(runtime, record, &failure);
    }
    pthread_mutex_unlock(&runtime->mutex);
    if (finish_result != 0) {
        (void)trevrpc_rpc_transport_stream_abort(runtime->transport, info->subject, 0);
    }
}

static void trevrpc_rpc_retire_handle_locked(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_transport_handle handle, uint32_t kind) {
    size_t index;
    for (index = 0; index < runtime->retired_pool_capacity; ++index) {
        trevrpc_rpc_retired_handle* retired = &runtime->retired_pool[index];
        if (retired->kind == kind && trevrpc_rpc_transport_handle_equal(retired->handle, handle)) {
            return;
        }
    }
    for (index = 0; index < runtime->retired_pool_capacity; ++index) {
        trevrpc_rpc_retired_handle* retired = &runtime->retired_pool[index];
        if (retired->kind == 0) {
            retired->handle = handle;
            retired->kind = kind;
            retired->next = runtime->retired_handles;
            runtime->retired_handles = retired;
            return;
        }
    }
    abort();
}

static void trevrpc_rpc_retire_handle(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_transport_handle handle, uint32_t kind) {
    pthread_mutex_lock(&runtime->mutex);
    trevrpc_rpc_retire_handle_locked(runtime, handle, kind);
    pthread_mutex_unlock(&runtime->mutex);
}

static void trevrpc_rpc_release_or_retire_handle(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_transport_handle handle, uint32_t kind) {
    if (trevrpc_rpc_transport_release_handle(runtime->transport, handle, kind) != 0) {
        trevrpc_rpc_retire_handle(runtime, handle, kind);
    }
}

static void trevrpc_rpc_retry_retired_handles_locked(trevrpc_rpc_runtime* runtime) {
    trevrpc_rpc_retired_handle** cursor = &runtime->retired_handles;
    while (*cursor != NULL) {
        trevrpc_rpc_retired_handle* retired = *cursor;
        if (trevrpc_rpc_transport_release_handle(runtime->transport, retired->handle, retired->kind) == 0) {
            *cursor = retired->next;
            retired->next = NULL;
            retired->kind = 0;
            retired->handle = (trevrpc_rpc_transport_handle){0};
        } else {
            cursor = &retired->next;
        }
    }
}

static void trevrpc_rpc_reject_unknown_stream(trevrpc_rpc_runtime* runtime, trevrpc_rpc_transport_handle stream) {
    int result = trevrpc_rpc_transport_stream_abort(runtime->transport, stream, TREVRPC_RPC_STATUS_UNAVAILABLE);
    if (result != 0 && result != -EALREADY) {
        trevrpc_rpc_release_or_retire_handle(runtime, stream, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM);
    }
}

static int trevrpc_rpc_handle_transport_event(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_transport_event* transport_event) {
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_event* event = NULL;
    int result;
    result = trevrpc_rpc_transport_event_get_info(runtime->transport, transport_event, &info);
    if (result != 0) {
        trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
        if (result > 0) {
            result = -EIO;
        }
        pthread_mutex_lock(&runtime->mutex);
        trevrpc_rpc_transition_fatal_locked(runtime, result);
        pthread_mutex_unlock(&runtime->mutex);
        return 0;
    }
    switch (info.kind) {
    case TREVRPC_RPC_TRANSPORT_EVENT_DIAGNOSTIC:
        event = trevrpc_rpc_event_from_transport(&info, TREVRPC_RPC_EVENT_DIAGNOSTIC);
        break;
    case TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION:
    case TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION:
        trevrpc_rpc_promote_admission_event(runtime, transport_event, &info);
        return 0;
    case TREVRPC_RPC_TRANSPORT_EVENT_STOPPED:
        pthread_mutex_lock(&runtime->mutex);
        if (runtime->live_calls != 0 || runtime->live_endpoints != 0 || runtime->operations != NULL) {
            trevrpc_rpc_transition_fatal_locked(runtime, info.status != 0 ? info.status : -EIO);
        } else {
            runtime->state = TREVRPC_RPC_STATE_STOPPED;
            runtime->terminal_status = info.status;
            event = runtime->close_event;
            runtime->close_event = NULL;
            if (event != NULL) {
                trevrpc_rpc_fill_event_from_transport(event, &info);
                event->kind = TREVRPC_RPC_EVENT_STOPPED;
                event->operation_id = runtime->close_operation_id;
                (void)trevrpc_rpc_publish_locked(runtime, event, true);
            }
        }
        pthread_mutex_unlock(&runtime->mutex);
        trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
        return 0;
    case TREVRPC_RPC_TRANSPORT_EVENT_LISTENER_STOPPED:
    case TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_FAILED:
    case TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED: {
        trevrpc_rpc_endpoint_record* record;
        bool release_connection = false;
        pthread_mutex_lock(&runtime->mutex);
        record = trevrpc_rpc_find_endpoint_by_transport_locked(runtime, info.subject);
        if (record != NULL) {
            trevrpc_rpc_publish_endpoint_terminal_locked(runtime, record, &info);
        } else if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_FAILED ||
                   info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED) {
            release_connection = !trevrpc_rpc_remove_transport_connection_locked(runtime, info.subject);
        }
        pthread_mutex_unlock(&runtime->mutex);
        trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
        if (release_connection) {
            trevrpc_rpc_release_or_retire_handle(runtime, info.subject, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION);
        }
        return 0;
    }
    case TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY: {
        trevrpc_rpc_endpoint_record* record;
        bool close_connection = false;
        pthread_mutex_lock(&runtime->mutex);
        record = trevrpc_rpc_find_endpoint_by_transport_locked(runtime, info.subject);
        if (record != NULL) {
            trevrpc_rpc_complete_endpoint_start_locked(runtime, record, &info, TREVRPC_RPC_EVENT_ENDPOINT_READY);
        } else if (info.kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY &&
                   (info.flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER) != 0) {
            trevrpc_rpc_endpoint_record* listener = trevrpc_rpc_find_endpoint_by_transport_locked(runtime, info.parent);
            trevrpc_rpc_transport_connection_record* connection =
                trevrpc_rpc_find_transport_connection_locked(runtime, info.subject);
            if (connection == NULL) {
                if (listener != NULL && !listener->terminal_committed) {
                    connection = calloc(1, sizeof(*connection));
                }
                if (connection != NULL) {
                    uint64_t key[6];
                    uint8_t key_count;
                    connection->transport_connection = info.subject;
                    connection->endpoint = listener->endpoint;
                    trevrpc_rpc_transport_key(key,
                        &key_count,
                        info.subject,
                        TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION,
                        TREVRPC_RPC_TRANSPORT_ROLE_CONNECTION);
                    if (trevrpc_rpc_registry_insert_alias(&runtime->transport_registry, key, key_count, connection) ==
                        0) {
                        connection->next = runtime->transport_connections;
                        runtime->transport_connections = connection;
                    } else {
                        free(connection);
                        connection = NULL;
                    }
                }
                if (connection == NULL) {
                    close_connection = true;
                }
            }
        }
        pthread_mutex_unlock(&runtime->mutex);
        if (close_connection) {
            int close_result = trevrpc_rpc_transport_connection_close(runtime->transport, info.subject, 0);
            if (close_result != 0 && close_result != -EALREADY) {
                trevrpc_rpc_retire_handle(runtime, info.subject, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION);
            }
        }
        trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
        return 0;
    }
    case TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY: {
        trevrpc_rpc_call_record* record = NULL;
        trevrpc_rpc_transport_event_protocol_info protocol = {0};
        int protocol_result =
            trevrpc_rpc_transport_event_get_protocol_info(runtime->transport, transport_event, &protocol);
        int32_t send_failure = 0;
        pthread_mutex_lock(&runtime->mutex);
        (void)trevrpc_rpc_find_or_create_peer_call_locked(runtime, &info, &record);
        if ((info.flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER) != 0) {
            pthread_mutex_unlock(&runtime->mutex);
            if (record == NULL) {
                trevrpc_rpc_reject_unknown_stream(runtime, info.subject);
            }
            trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
            return 0;
        }
        if (record != NULL && !record->closing && record->request_frame != NULL && record->open_operation != NULL) {
            record->awaiting_transport_acceptance =
                protocol_result == 0 && protocol.protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3;
            result = trevrpc_rpc_transport_stream_send(runtime->transport,
                info.subject,
                record->open_operation->transport_operation_id,
                record->request_frame + 4,
                record->request_frame_len - 4);
            if (result == 0) {
                record->request_send_pending = true;
            } else {
                send_failure = result;
                trevrpc_rpc_complete_call_open_locked(
                    runtime, record, &info, TREVRPC_RPC_EVENT_CALL_FAILED, send_failure);
                record->closing = true;
            }
        }
        pthread_mutex_unlock(&runtime->mutex);
        if (send_failure != 0) {
            trevrpc_rpc_abort_or_terminal(runtime, info.subject, TREVRPC_RPC_STATUS_INTERNAL, send_failure);
        }
        trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
        return 0;
    }
    case TREVRPC_RPC_TRANSPORT_EVENT_STREAM_ACCEPTED: {
        trevrpc_rpc_call_record* record;
        pthread_mutex_lock(&runtime->mutex);
        record = trevrpc_rpc_find_call_by_transport_locked(runtime, info.subject);
        if (record != NULL && record->local && record->awaiting_transport_acceptance && !record->closing) {
            record->transport_accepted = true;
            if (record->open_operation != NULL && !record->request_send_pending && record->request_frame == NULL) {
                trevrpc_rpc_complete_call_open_locked(runtime, record, &info, TREVRPC_RPC_EVENT_CALL_READY, 0);
            }
        }
        pthread_mutex_unlock(&runtime->mutex);
        trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
        return 0;
    }
    case TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED: {
        trevrpc_rpc_call_record* record;
        bool release_stream;
        pthread_mutex_lock(&runtime->mutex);
        record = trevrpc_rpc_find_call_by_transport_locked(runtime, info.subject);
        release_stream = record == NULL;
        if (record != NULL) {
            trevrpc_rpc_complete_call_open_locked(runtime, record, &info, TREVRPC_RPC_EVENT_CALL_FAILED, info.status);
            if (!record->closing &&
                (record->waiting_for_request || record->readable_drained_epoch != record->readable_epoch)) {
                record->deferred_stream_terminal = info;
                record->deferred_stream_terminal_valid = true;
            } else {
                record->closing = true;
                trevrpc_rpc_publish_call_terminals_locked(runtime, record, &info);
            }
        }
        pthread_mutex_unlock(&runtime->mutex);
        trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
        if (release_stream) {
            trevrpc_rpc_release_or_retire_handle(runtime, info.subject, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM);
        }
        return 0;
    }
    case TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE: {
        trevrpc_rpc_call_record* record = NULL;
        trevrpc_rpc_endpoint_v1 endpoint = {0};
        trevrpc_rpc_call_v1 call = {0};
        trevrpc_rpc_stream_v1 stream = {0};
        trevrpc_rpc_transport_handle transport_stream = {0};
        bool waiting = false;
        int peer_result;
        pthread_mutex_lock(&runtime->mutex);
        peer_result = trevrpc_rpc_find_or_create_peer_call_locked(runtime, &info, &record);
        if (record != NULL && !record->closing) {
            waiting = record->waiting_for_request;
            if (waiting) {
                endpoint = record->endpoint;
                call = record->call;
                stream = record->stream;
                transport_stream = record->transport_stream;
            } else {
                trevrpc_rpc_reset_idle_locked(runtime, record, false);
                if (record->receive_idle_deadline_nanos != 0) {
                    trevrpc_rpc_signal_driver_locked(runtime);
                }
                event = trevrpc_rpc_event_from_transport(&info, TREVRPC_RPC_EVENT_STREAM_READABLE);
                if (event == NULL) {
                    peer_result = -ENOMEM;
                } else {
                    event->subject_kind = TREVRPC_RPC_OBJECT_STREAM;
                    event->endpoint = record->endpoint;
                    event->call = record->call;
                    event->stream = record->stream;
                    record->readable_flags = event->flags;
                    ++record->readable_epoch;
                    result = trevrpc_rpc_publish_locked(runtime, event, false);
                    event = NULL;
                    if (result != 0) {
                        --record->readable_epoch;
                        peer_result = result;
                    } else {
                        ++record->readable_events_pending;
                    }
                }
            }
        }
        pthread_mutex_unlock(&runtime->mutex);
        if (peer_result == -ENOMEM) {
            return peer_result;
        }
        if (record == NULL && (peer_result == -ENOENT || peer_result == -EAGAIN) &&
            (info.flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER) != 0) {
            trevrpc_rpc_reject_unknown_stream(runtime, info.subject);
        }
        if (waiting) {
            trevrpc_rpc_transport_receive* receive = NULL;
            trevrpc_rpc_transport_receive_info receive_info;
            result = trevrpc_rpc_transport_stream_receive(runtime->transport, info.subject, &receive);
            if (result == -ENOMEM) {
                return result;
            }
            if (result != 0) {
                trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
                trevrpc_rpc_abort_or_terminal(
                    runtime, transport_stream, TREVRPC_RPC_STATUS_INTERNAL, result > 0 ? -EIO : result);
                return 0;
            }
            result = trevrpc_rpc_transport_receive_get_info(runtime->transport, receive, &receive_info);
            if (result == 0) {
                (void)trevrpc_rpc_copy_request_event(
                    runtime, endpoint, call, stream, transport_stream, receive, &receive_info);
            } else {
                trevrpc_rpc_transport_receive_release(runtime->transport, receive);
                trevrpc_rpc_abort_or_terminal(
                    runtime, transport_stream, TREVRPC_RPC_STATUS_INTERNAL, result > 0 ? -EIO : result);
            }
            trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
            return 0;
        }
        trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
        return 0;
    }
    case TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN: {
        trevrpc_rpc_call_record* record = NULL;
        int peer_result;
        pthread_mutex_lock(&runtime->mutex);
        peer_result = trevrpc_rpc_find_or_create_peer_call_locked(runtime, &info, &record);
        if (record != NULL && !record->closing) {
            record->receive_idle_deadline_nanos = 0;
            (void)trevrpc_rpc_deadline_refresh_locked(runtime, record);
            event = trevrpc_rpc_event_from_transport(&info, TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN);
            if (event == NULL) {
                peer_result = -ENOMEM;
            } else {
                event->subject_kind = TREVRPC_RPC_OBJECT_STREAM;
                event->endpoint = record->endpoint;
                event->call = record->call;
                event->stream = record->stream;
                record->receive_fin_observed = true;
                if (record->deferred_receive_fin == NULL) {
                    record->deferred_receive_fin = event;
                } else {
                    trevrpc_rpc_event_destroy(event);
                }
                event = NULL;
                trevrpc_rpc_flush_receive_barrier_locked(runtime, record);
            }
        }
        pthread_mutex_unlock(&runtime->mutex);
        if (record == NULL && (peer_result == -ENOENT || peer_result == -EAGAIN) &&
            (info.flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER) != 0) {
            trevrpc_rpc_reject_unknown_stream(runtime, info.subject);
        }
        if (peer_result == -ENOMEM) {
            return peer_result;
        }
        break;
    }
    case TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE:
        trevrpc_rpc_handle_send_complete(runtime, &info);
        trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
        return 0;
    case TREVRPC_RPC_TRANSPORT_EVENT_SEND_STOPPED:
        trevrpc_rpc_handle_send_stopped(runtime, &info);
        trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
        return 0;
    case TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED: {
        trevrpc_rpc_call_record* record;
        bool release_stream;
        pthread_mutex_lock(&runtime->mutex);
        record = trevrpc_rpc_find_call_by_transport_locked(runtime, info.subject);
        release_stream = record == NULL;
        if (record != NULL) {
            bool successful_response_waits_for_fin = !record->closing && info.status == 0 &&
                                                     record->peer_status_received && !record->receive_fin_observed &&
                                                     record->close_operation == NULL && !record->cancelled &&
                                                     !record->deadline_expired && !record->runtime_close_target;
            if (!record->closing &&
                (record->waiting_for_request || record->readable_drained_epoch != record->readable_epoch ||
                    successful_response_waits_for_fin)) {
                record->deferred_stream_terminal = info;
                record->deferred_stream_terminal_valid = true;
            } else {
                trevrpc_rpc_publish_call_terminals_locked(runtime, record, &info);
            }
        }
        pthread_mutex_unlock(&runtime->mutex);
        trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
        if (release_stream) {
            trevrpc_rpc_release_or_retire_handle(runtime, info.subject, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM);
        }
        return 0;
    }
    default:
        break;
    }
    if (event != NULL) {
        result = trevrpc_rpc_publish(runtime, event, event->kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN);
        if (result != 0 && info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE) {
            return result;
        }
    }
    trevrpc_rpc_transport_event_release(runtime->transport, transport_event);
    return 0;
}

static int trevrpc_rpc_deadline_poll_timeout_locked(trevrpc_rpc_runtime* runtime) {
    uint64_t now = 0;
    uint64_t earliest;
    uint64_t remaining;
    uint64_t milliseconds;
    if (runtime->state != TREVRPC_RPC_STATE_RUNNING) {
        return -1;
    }
    while (runtime->deadline_heap_count != 0 &&
           (runtime->deadline_heap[0]->record->closing || runtime->deadline_heap[0]->record->call_terminal_committed)) {
        trevrpc_rpc_deadline_remove_locked(runtime, runtime->deadline_heap[0]->record);
    }
    if (runtime->deadline_heap_count == 0) {
        return -1;
    }
    earliest = runtime->deadline_heap[0]->deadline;
    if (trevrpc_rpc_monotonic_nanos(&now) != 0 || earliest <= now) {
        return 0;
    }
    remaining = earliest - now;
    milliseconds = remaining / UINT64_C(1000000);
    if (remaining % UINT64_C(1000000) != 0) {
        ++milliseconds;
    }
    return milliseconds > (uint64_t)INT_MAX ? INT_MAX : (int)milliseconds;
}

static bool trevrpc_rpc_expire_one_deadline(trevrpc_rpc_runtime* runtime) {
    trevrpc_rpc_transport_event_info info = {0};
    trevrpc_rpc_transport_handle transport_stream = {0};
    trevrpc_rpc_call_record* record;
    uint32_t timer;
    uint64_t now = 0;
    int abort_result;
    pthread_mutex_lock(&runtime->mutex);
    if (runtime->state != TREVRPC_RPC_STATE_RUNNING) {
        pthread_mutex_unlock(&runtime->mutex);
        return false;
    }
    while (runtime->deadline_heap_count != 0 &&
           (runtime->deadline_heap[0]->record->closing || runtime->deadline_heap[0]->record->call_terminal_committed)) {
        trevrpc_rpc_deadline_remove_locked(runtime, runtime->deadline_heap[0]->record);
    }
    if (runtime->deadline_heap_count == 0) {
        pthread_mutex_unlock(&runtime->mutex);
        return false;
    }
    if (trevrpc_rpc_monotonic_nanos(&now) != 0) {
        now = UINT64_MAX;
    }
    if (runtime->deadline_heap[0]->deadline > now) {
        pthread_mutex_unlock(&runtime->mutex);
        return false;
    }
    record = runtime->deadline_heap[0]->record;
    timer = runtime->deadline_heap[0]->kind;
    trevrpc_rpc_deadline_remove_locked(runtime, record);
    if (timer == TREVRPC_RPC_TIMER_CALL) {
        record->deadline_nanos = TREVRPC_RPC_DEADLINE_INFINITE;
    } else if (timer == TREVRPC_RPC_TIMER_INITIAL) {
        record->initial_request_deadline_nanos = 0;
    } else if (timer == TREVRPC_RPC_TIMER_SEND_IDLE) {
        record->send_idle_deadline_nanos = 0;
    } else {
        record->receive_idle_deadline_nanos = 0;
    }
    record->closing = true;
    record->suppress_terminals = timer == TREVRPC_RPC_TIMER_INITIAL && record->waiting_for_request;
    record->deadline_expired = timer == TREVRPC_RPC_TIMER_CALL;
    record->cancelled = true;
    transport_stream = record->transport_stream;
    info.flags = TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL;
    info.status = -ETIMEDOUT;
    info.application_error_code =
        record->deadline_expired ? TREVRPC_RPC_STATUS_DEADLINE_EXCEEDED : TREVRPC_RPC_STATUS_UNAVAILABLE;
    if (record->open_operation != NULL) {
        trevrpc_rpc_complete_call_open_locked(runtime, record, &info, TREVRPC_RPC_EVENT_CALL_FAILED, -ETIMEDOUT);
    }
    if (record->deferred_stream_terminal_valid) {
        trevrpc_rpc_publish_call_terminals_locked(runtime, record, &info);
        pthread_mutex_unlock(&runtime->mutex);
        return true;
    }
    pthread_mutex_unlock(&runtime->mutex);

    abort_result =
        trevrpc_rpc_transport_stream_abort(runtime->transport, transport_stream, info.application_error_code);
    if (abort_result != 0 && abort_result != -EALREADY) {
        pthread_mutex_lock(&runtime->mutex);
        record = trevrpc_rpc_find_call_by_transport_locked(runtime, transport_stream);
        if (record != NULL) {
            trevrpc_rpc_publish_call_terminals_locked(runtime, record, &info);
        }
        pthread_mutex_unlock(&runtime->mutex);
    }
    return true;
}

static void trevrpc_rpc_expire_deadlines(trevrpc_rpc_runtime* runtime) {
    while (trevrpc_rpc_expire_one_deadline(runtime)) {
    }
}

static void trevrpc_rpc_wait_for_queue_capacity_locked(trevrpc_rpc_runtime* runtime) {
    while (runtime->ordinary_queue_depth >= runtime->event_capacity && runtime->state == TREVRPC_RPC_STATE_RUNNING) {
        struct timespec wake_time;
        int timeout = trevrpc_rpc_deadline_poll_timeout_locked(runtime);
        if (timeout < 0) {
            pthread_cond_wait(&runtime->condition, &runtime->mutex);
            continue;
        }
        if (timeout == 0 || clock_gettime(CLOCK_REALTIME, &wake_time) != 0) {
            break;
        }
        wake_time.tv_sec += timeout / 1000;
        wake_time.tv_nsec += (long)(timeout % 1000) * 1000000L;
        if (wake_time.tv_nsec >= 1000000000L) {
            ++wake_time.tv_sec;
            wake_time.tv_nsec -= 1000000000L;
        }
        if (pthread_cond_timedwait(&runtime->condition, &runtime->mutex, &wake_time) == ETIMEDOUT) {
            break;
        }
    }
}

static void trevrpc_rpc_transition_fatal_locked(trevrpc_rpc_runtime* runtime, int32_t status) {
    trevrpc_rpc_transport_event_info info = {0};
    trevrpc_rpc_call_record* call;
    trevrpc_rpc_endpoint_record* endpoint;
    trevrpc_rpc_event* stop_event;
    if (runtime->state == TREVRPC_RPC_STATE_STOPPED || runtime->state == TREVRPC_RPC_STATE_RELEASING) {
        return;
    }
    info.flags = TREVRPC_RPC_TRANSPORT_EVENT_FLAG_FATAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL |
                 TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL;
    info.status = status != 0 ? status : -EIO;
    runtime->state = TREVRPC_RPC_STATE_STOPPED;
    runtime->terminal_status = info.status;
    for (call = runtime->calls; call != NULL; call = call->next) {
        if (!call->stream_terminal_committed) {
            call->closing = true;
            call->cancelled = true;
            trevrpc_rpc_publish_call_terminals_locked(runtime, call, &info);
        }
    }
    for (endpoint = runtime->endpoints; endpoint != NULL; endpoint = endpoint->next) {
        if (!endpoint->terminal_committed) {
            trevrpc_rpc_publish_endpoint_terminal_locked(runtime, endpoint, &info);
        }
    }
    stop_event = runtime->close_event;
    runtime->close_event = NULL;
    if (stop_event != NULL) {
        trevrpc_rpc_fill_event_from_transport(stop_event, &info);
        stop_event->kind = TREVRPC_RPC_EVENT_STOPPED;
        stop_event->operation_id = runtime->close_operation_id;
        (void)trevrpc_rpc_publish_locked(runtime, stop_event, true);
    }
    pthread_cond_broadcast(&runtime->condition);
}

static bool trevrpc_rpc_driver_stop_on_oom(trevrpc_rpc_runtime* runtime) {
    bool stopping;
    pthread_mutex_lock(&runtime->mutex);
    stopping = runtime->state == TREVRPC_RPC_STATE_STOPPING;
    if (stopping) {
        trevrpc_rpc_transition_fatal_locked(runtime, -ENOMEM);
    }
    pthread_mutex_unlock(&runtime->mutex);
    return stopping;
}

static void* trevrpc_rpc_driver_main(void* context) {
    trevrpc_rpc_runtime* runtime = context;
    trevrpc_rpc_transport_event* pending_event = NULL;
    trevrpc_rpc_transport_wake wakes[TREVRPC_RPC_TRANSPORT_MAX_WAKE_SOURCES] = {{0}};
    struct pollfd descriptors[TREVRPC_RPC_TRANSPORT_MAX_WAKE_SOURCES + 1u];
    size_t wake_count = 0;
    size_t wake_index;
    {
        int wake_result = trevrpc_rpc_transport_get_wake_sources(
            runtime->transport, wakes, TREVRPC_RPC_TRANSPORT_MAX_WAKE_SOURCES, &wake_count);
        if (wake_result != 0 || wake_count == 0 || wake_count > TREVRPC_RPC_TRANSPORT_MAX_WAKE_SOURCES) {
            pthread_mutex_lock(&runtime->mutex);
            trevrpc_rpc_transition_fatal_locked(runtime, wake_result != 0 ? wake_result : -ENOTSUP);
            runtime->driver_stopped = true;
            pthread_mutex_unlock(&runtime->mutex);
            return NULL;
        }
    }
    for (wake_index = 0; wake_index < wake_count; ++wake_index) {
        if (wakes[wake_index].kind != TREVRPC_RPC_TRANSPORT_WAKE_SOURCE_POSIX_FD) {
            pthread_mutex_lock(&runtime->mutex);
            trevrpc_rpc_transition_fatal_locked(runtime, -ENOTSUP);
            runtime->driver_stopped = true;
            pthread_mutex_unlock(&runtime->mutex);
            return NULL;
        }
        descriptors[wake_index].fd = (int)wakes[wake_index].native_handle;
        descriptors[wake_index].events = POLLIN;
        descriptors[wake_index].revents = 0;
    }
    descriptors[wake_count].fd = runtime->driver_wake_read_fd;
    descriptors[wake_count].events = POLLIN;
    descriptors[wake_count].revents = 0;
    for (;;) {
        int timeout;
        int poll_result;
        pthread_mutex_lock(&runtime->mutex);
        timeout = trevrpc_rpc_deadline_poll_timeout_locked(runtime);
        pthread_mutex_unlock(&runtime->mutex);
        {
            int transport_timeout = trevrpc_rpc_transport_poll_timeout_ms(runtime->transport);
            if (transport_timeout >= 0 && (timeout < 0 || transport_timeout < timeout)) {
                timeout = transport_timeout;
            }
        }
        poll_result = poll(descriptors, (nfds_t)(wake_count + 1u), timeout);
        if (poll_result < 0) {
            if (errno == EINTR) {
                continue;
            }
            pthread_mutex_lock(&runtime->mutex);
            trevrpc_rpc_transition_fatal_locked(runtime, -errno);
            pthread_mutex_unlock(&runtime->mutex);
            break;
        }
        if ((descriptors[wake_count].revents & POLLIN) != 0) {
            trevrpc_rpc_drain_descriptor(runtime->driver_wake_read_fd);
        }
        {
            unsigned event_batch = 0;
            for (;;) {
                trevrpc_rpc_transport_event* event = pending_event;
                int result;
                if (event == NULL) {
                    result = trevrpc_rpc_transport_next_event(runtime->transport, &event);
                    if (result == -EAGAIN) {
                        break;
                    }
                    if (result == -ENOMEM) {
                        if (trevrpc_rpc_driver_stop_on_oom(runtime)) {
                            goto done;
                        }
                        trevrpc_rpc_expire_deadlines(runtime);
                        sched_yield();
                        continue;
                    }
                    if (result != 0) {
                        pthread_mutex_lock(&runtime->mutex);
                        trevrpc_rpc_transition_fatal_locked(runtime, result);
                        pthread_mutex_unlock(&runtime->mutex);
                        goto done;
                    }
                }
                result = trevrpc_rpc_handle_transport_event(runtime, event);
                if (result == 0) {
                    pending_event = NULL;
                    if (++event_batch == 64u) {
                        trevrpc_rpc_expire_deadlines(runtime);
                        event_batch = 0;
                    }
                    continue;
                }
                pending_event = event;
                if (result == -ENOMEM) {
                    if (trevrpc_rpc_driver_stop_on_oom(runtime)) {
                        goto done;
                    }
                    trevrpc_rpc_expire_deadlines(runtime);
                    sched_yield();
                    continue;
                }
                if (result == -EAGAIN) {
                    bool discard;
                    pthread_mutex_lock(&runtime->mutex);
                    trevrpc_rpc_wait_for_queue_capacity_locked(runtime);
                    discard = runtime->state != TREVRPC_RPC_STATE_RUNNING;
                    pthread_mutex_unlock(&runtime->mutex);
                    if (discard) {
                        trevrpc_rpc_transport_event_release(runtime->transport, pending_event);
                        pending_event = NULL;
                    } else {
                        trevrpc_rpc_expire_deadlines(runtime);
                    }
                    continue;
                }
                pthread_mutex_lock(&runtime->mutex);
                trevrpc_rpc_transition_fatal_locked(runtime, result);
                pthread_mutex_unlock(&runtime->mutex);
                goto done;
            }
        }
        trevrpc_rpc_expire_deadlines(runtime);
        pthread_mutex_lock(&runtime->mutex);
        if (runtime->state == TREVRPC_RPC_STATE_STOPPED) {
            pthread_mutex_unlock(&runtime->mutex);
            break;
        }
        pthread_mutex_unlock(&runtime->mutex);
    }
done:
    trevrpc_rpc_transport_event_release(runtime->transport, pending_event);
    pthread_mutex_lock(&runtime->mutex);
    runtime->driver_stopped = true;
    pthread_cond_broadcast(&runtime->condition);
    pthread_mutex_unlock(&runtime->mutex);
    return NULL;
}

int trevrpc_rpc_runtime_adopt_transport_v1(
    const trevrpc_rpc_runtime_config_v1* config, trevrpc_rpc_transport* transport, trevrpc_rpc_runtime** out_runtime) {
    trevrpc_rpc_runtime* runtime;
    int descriptors[2] = {-1, -1};
    int driver_descriptors[2] = {-1, -1};
    int result;
    if (transport == NULL || out_runtime == NULL) {
        return -EINVAL;
    }
    result = trevrpc_rpc_validate_runtime_config(config);
    if (result != 0) {
        return result;
    }
    runtime = calloc(1, sizeof(*runtime));
    if (runtime == NULL) {
        return -ENOMEM;
    }
    runtime->wake_read_fd = -1;
    runtime->wake_write_fd = -1;
    runtime->driver_wake_read_fd = -1;
    runtime->driver_wake_write_fd = -1;
    runtime->transport = transport;
    runtime->state = TREVRPC_RPC_STATE_RUNNING;
    runtime->event_capacity = config->event_capacity;
    runtime->endpoint_capacity = config->endpoint_capacity;
    runtime->call_capacity = config->call_capacity;
    runtime->stream_capacity = config->stream_capacity;
#if SIZE_MAX == UINT32_MAX
    if (config->endpoint_capacity > UINT32_MAX - config->stream_capacity) {
        free(runtime);
        return -EOVERFLOW;
    }
#endif
    runtime->retired_pool_capacity = (size_t)config->endpoint_capacity + (size_t)config->stream_capacity;
    if (runtime->retired_pool_capacity > SIZE_MAX / sizeof(*runtime->retired_pool)) {
        free(runtime);
        return -EOVERFLOW;
    }
    runtime->retired_pool = calloc(runtime->retired_pool_capacity, sizeof(*runtime->retired_pool));
    if (runtime->retired_pool == NULL) {
        free(runtime);
        return -ENOMEM;
    }
    result = trevrpc_rpc_registry_init(&runtime->call_registry);
    if (result == 0)
        result = trevrpc_rpc_registry_init(&runtime->endpoint_registry);
    if (result == 0)
        result = trevrpc_rpc_registry_init(&runtime->transport_registry);
    if (result == 0)
        result = trevrpc_rpc_registry_init(&runtime->cancellation_registry);
    if (result == 0)
        result = trevrpc_rpc_registry_init(&runtime->operation_transport_registry);
    if (result == 0)
        result = trevrpc_rpc_registry_init(&runtime->operation_scope_registry);
    if (result != 0) {
        trevrpc_rpc_registry_destroy(&runtime->call_registry);
        trevrpc_rpc_registry_destroy(&runtime->endpoint_registry);
        trevrpc_rpc_registry_destroy(&runtime->transport_registry);
        trevrpc_rpc_registry_destroy(&runtime->cancellation_registry);
        trevrpc_rpc_registry_destroy(&runtime->operation_transport_registry);
        trevrpc_rpc_registry_destroy(&runtime->operation_scope_registry);
        free(runtime->retired_pool);
        free(runtime);
        return result;
    }
    runtime->max_metadata_count = config->max_metadata_count;
    runtime->max_status_message_size = config->max_status_message_size;
    runtime->max_message_size = config->max_message_size;
    runtime->max_metadata_bytes = config->max_metadata_bytes;
    runtime->initial_request_timeout_nanos = config->initial_request_timeout_nanos;
    runtime->max_stream_messages = config->max_stream_messages;
    runtime->max_stream_body_size = config->max_stream_body_size;
    runtime->stream_idle_timeout_nanos = config->stream_idle_timeout_nanos;
    runtime->next_sequence = 1;
    runtime->next_transport_operation_id = 1;
    runtime->cancellation_sequence = 1;
    runtime->close_event = trevrpc_rpc_event_allocate(TREVRPC_RPC_EVENT_STOPPED);
    if (runtime->close_event == NULL) {
        trevrpc_rpc_registry_destroy(&runtime->call_registry);
        trevrpc_rpc_registry_destroy(&runtime->endpoint_registry);
        trevrpc_rpc_registry_destroy(&runtime->transport_registry);
        trevrpc_rpc_registry_destroy(&runtime->cancellation_registry);
        trevrpc_rpc_registry_destroy(&runtime->operation_transport_registry);
        trevrpc_rpc_registry_destroy(&runtime->operation_scope_registry);
        free(runtime->retired_pool);
        free(runtime);
        return -ENOMEM;
    }
    runtime->close_event->reserved = true;
    runtime->mandatory_reservations = 1;
    runtime->owner_cookie = atomic_fetch_add_explicit(&trevrpc_rpc_owner_sequence, 1, memory_order_relaxed);
    if (runtime->owner_cookie == 0) {
        runtime->owner_cookie = atomic_fetch_add_explicit(&trevrpc_rpc_owner_sequence, 1, memory_order_relaxed);
    }
    atomic_init(&runtime->references, 1);
    atomic_init(&runtime->api_gate, 0);
    atomic_init(&runtime->test_fail_next_receive_copy, false);
    atomic_init(&runtime->test_call_release_result, 0);
    atomic_init(&runtime->test_call_release_result_persistent, false);
    result = pthread_mutex_init(&runtime->lifetime_mutex, NULL);
    if (result != 0) {
        result = -result;
        goto fail;
    }
    result = pthread_cond_init(&runtime->lifetime_condition, NULL);
    if (result != 0) {
        result = -result;
        pthread_mutex_destroy(&runtime->lifetime_mutex);
        goto fail;
    }
    result = pthread_mutex_init(&runtime->mutex, NULL);
    if (result != 0) {
        result = -result;
        pthread_cond_destroy(&runtime->lifetime_condition);
        pthread_mutex_destroy(&runtime->lifetime_mutex);
        goto fail;
    }
    result = pthread_cond_init(&runtime->condition, NULL);
    if (result != 0) {
        result = -result;
        pthread_mutex_destroy(&runtime->mutex);
        pthread_cond_destroy(&runtime->lifetime_condition);
        pthread_mutex_destroy(&runtime->lifetime_mutex);
        goto fail;
    }
    result = trevrpc_rpc_create_wake_pipe(descriptors);
    if (result != 0) {
        pthread_cond_destroy(&runtime->condition);
        pthread_mutex_destroy(&runtime->mutex);
        pthread_cond_destroy(&runtime->lifetime_condition);
        pthread_mutex_destroy(&runtime->lifetime_mutex);
        goto fail;
    }
    runtime->wake_read_fd = descriptors[0];
    runtime->wake_write_fd = descriptors[1];
    result = trevrpc_rpc_create_wake_pipe(driver_descriptors);
    if (result != 0) {
        close(runtime->wake_read_fd);
        close(runtime->wake_write_fd);
        pthread_cond_destroy(&runtime->condition);
        pthread_mutex_destroy(&runtime->mutex);
        pthread_cond_destroy(&runtime->lifetime_condition);
        pthread_mutex_destroy(&runtime->lifetime_mutex);
        goto fail;
    }
    runtime->driver_wake_read_fd = driver_descriptors[0];
    runtime->driver_wake_write_fd = driver_descriptors[1];
    result = pthread_create(&runtime->driver_thread, NULL, trevrpc_rpc_driver_main, runtime);
    if (result != 0) {
        result = -result;
        close(runtime->driver_wake_read_fd);
        close(runtime->driver_wake_write_fd);
        close(runtime->wake_read_fd);
        close(runtime->wake_write_fd);
        pthread_cond_destroy(&runtime->condition);
        pthread_mutex_destroy(&runtime->mutex);
        pthread_cond_destroy(&runtime->lifetime_condition);
        pthread_mutex_destroy(&runtime->lifetime_mutex);
        goto fail;
    }
    runtime->driver_started = true;
    *out_runtime = runtime;
    return 0;
fail:
    trevrpc_rpc_event_destroy(runtime->close_event);
    trevrpc_rpc_registry_destroy(&runtime->call_registry);
    trevrpc_rpc_registry_destroy(&runtime->endpoint_registry);
    trevrpc_rpc_registry_destroy(&runtime->transport_registry);
    trevrpc_rpc_registry_destroy(&runtime->cancellation_registry);
    trevrpc_rpc_registry_destroy(&runtime->operation_transport_registry);
    trevrpc_rpc_registry_destroy(&runtime->operation_scope_registry);
    free(runtime->retired_pool);
    free(runtime);
    return result;
}

static int trevrpc_rpc_runtime_get_wake_source_v1_impl(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_wake_source_v1* wake_source) {
    if (runtime == NULL || wake_source == NULL || wake_source->struct_size < sizeof(*wake_source)) {
        return -EINVAL;
    }
    if (wake_source->struct_version != TREVRPC_RPC_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    pthread_mutex_lock(&runtime->mutex);
    if (runtime->public_released) {
        pthread_mutex_unlock(&runtime->mutex);
        return -ESHUTDOWN;
    }
    wake_source->kind = TREVRPC_RPC_WAKE_SOURCE_POSIX_FD;
    wake_source->flags = TREVRPC_RPC_WAKE_FLAG_BORROWED | TREVRPC_RPC_WAKE_FLAG_LEVEL_TRIGGERED;
    wake_source->native_handle = runtime->wake_read_fd;
    pthread_mutex_unlock(&runtime->mutex);
    return 0;
}

static void trevrpc_rpc_note_terminal_dequeue_locked(trevrpc_rpc_runtime* runtime, trevrpc_rpc_event* event) {
    if (event->kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED) {
        trevrpc_rpc_endpoint_record* endpoint = trevrpc_rpc_find_endpoint_locked(runtime, event->endpoint);
        if (endpoint != NULL) {
            endpoint->terminal_dequeued = true;
            trevrpc_rpc_endpoint_maybe_reclaim_locked(runtime, endpoint);
        }
    } else if (event->kind == TREVRPC_RPC_EVENT_STREAM_CLOSED) {
        trevrpc_rpc_call_record* call = trevrpc_rpc_find_stream_locked(runtime, event->stream);
        if (call != NULL) {
            call->stream_terminal_dequeued = true;
            trevrpc_rpc_call_maybe_reclaim_locked(runtime, call);
        }
    } else if (event->kind == TREVRPC_RPC_EVENT_CALL_CLOSED) {
        trevrpc_rpc_call_record* call = trevrpc_rpc_find_call_locked(runtime, event->call);
        if (call != NULL) {
            call->call_terminal_dequeued = true;
            trevrpc_rpc_call_maybe_reclaim_locked(runtime, call);
        }
    }
}

static void trevrpc_rpc_internal_test_mutate_incoming(trevrpc_rpc_event* event, uint32_t kind) {
    uint32_t index;
    if (event == NULL || event->kind != TREVRPC_RPC_EVENT_CALL_INCOMING) {
        return;
    }
    switch (kind) {
    case TREVRPC_RPC_INTERNAL_TEST_MALFORMED_SERVICE:
        free(event->service);
        event->service = NULL;
        event->service_len = 1;
        break;
    case TREVRPC_RPC_INTERNAL_TEST_MALFORMED_METHOD:
        free(event->method);
        event->method = NULL;
        event->method_len = 1;
        break;
    case TREVRPC_RPC_INTERNAL_TEST_MALFORMED_DATA:
        if (event->receive != NULL) {
            free(event->receive->data);
            event->receive->data = NULL;
            event->receive->data_len = 1;
        }
        break;
    case TREVRPC_RPC_INTERNAL_TEST_MALFORMED_METADATA:
        if (event->receive != NULL) {
            if (event->receive->metadata != NULL) {
                for (index = 0; index < event->receive->metadata_count; ++index) {
                    free((void*)event->receive->metadata[index].key);
                    free((void*)event->receive->metadata[index].value);
                }
                free(event->receive->metadata);
            }
            event->receive->metadata = NULL;
            event->receive->metadata_count = 1;
        }
        break;
    default:
        break;
    }
}

static int trevrpc_rpc_runtime_next_event_impl(trevrpc_rpc_runtime* runtime, trevrpc_rpc_event** out_event) {
    trevrpc_rpc_event* event;
    if (runtime == NULL || out_event == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&runtime->mutex);
    if (runtime->public_released) {
        pthread_mutex_unlock(&runtime->mutex);
        return -ESHUTDOWN;
    }
    event = runtime->event_head;
    if (event == NULL) {
        pthread_mutex_unlock(&runtime->mutex);
        return -EAGAIN;
    }
    runtime->event_head = event->next;
    if (runtime->event_head == NULL) {
        uint8_t buffer[64];
        runtime->event_tail = NULL;
        runtime->wake_armed = false;
        /* The runtime wake pipe read end is permanently nonblocking. */
        // NOLINTBEGIN(clang-analyzer-unix.BlockInCriticalSection)
        while (read(runtime->wake_read_fd, buffer, sizeof(buffer)) > 0) {
        }
        // NOLINTEND(clang-analyzer-unix.BlockInCriticalSection)
    }
    event->next = NULL;
    --runtime->queue_depth;
    if (!event->mandatory && runtime->ordinary_queue_depth != 0) {
        --runtime->ordinary_queue_depth;
    }
    ++runtime->events_dequeued;
    if (event->kind == TREVRPC_RPC_EVENT_STREAM_READABLE) {
        trevrpc_rpc_call_record* call = trevrpc_rpc_find_stream_locked(runtime, event->stream);
        if (call != NULL && call->readable_events_pending != 0) {
            --call->readable_events_pending;
        }
    }
    trevrpc_rpc_note_terminal_dequeue_locked(runtime, event);
    const uint32_t malformed_kind =
        event->kind == TREVRPC_RPC_EVENT_CALL_INCOMING ? runtime->test_malformed_incoming_kind : 0;
    if (malformed_kind != 0) {
        runtime->test_malformed_incoming_kind = 0;
    }
    pthread_cond_broadcast(&runtime->condition);
    pthread_mutex_unlock(&runtime->mutex);
    trevrpc_rpc_internal_test_mutate_incoming(event, malformed_kind);
    *out_event = event;
    return 0;
}

int trevrpc_rpc_event_get_info_v1(const trevrpc_rpc_event* event, trevrpc_rpc_event_info_v1* info) {
    if (event == NULL || info == NULL || info->struct_size < sizeof(*info)) {
        return -EINVAL;
    }
    if (info->struct_version != TREVRPC_RPC_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    info->kind = event->kind;
    info->flags = event->flags;
    info->status = event->status;
    info->subject_kind = event->subject_kind;
    info->sequence = event->sequence;
    info->endpoint = event->endpoint;
    info->call = event->call;
    info->stream = event->stream;
    info->cancellation = event->cancellation;
    info->operation_id = event->operation_id;
    info->rpc_status = event->rpc_status;
    info->rpc_kind = event->rpc_kind;
    info->service = event->service;
    info->service_len = event->service_len;
    info->method = event->method;
    info->method_len = event->method_len;
    info->application_error_code = event->application_error_code;
    info->provider_error_code = event->provider_error_code;
    return 0;
}

static int trevrpc_rpc_call_get_context_v1_impl(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call, trevrpc_rpc_call_context_info_v1* info) {
    trevrpc_rpc_call_record* record;
    uint64_t now = 0;
    uint64_t remaining = 0;
    int clock_result = 0;
    if (runtime == NULL || info == NULL || info->struct_size < sizeof(*info)) {
        return -EINVAL;
    }
    if (info->struct_version != TREVRPC_RPC_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (info->reserved0 != 0 || !trevrpc_rpc_u64_fields_zero(info->reserved, 4)) {
        return -EINVAL;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_call_locked(runtime, call);
    if (record == NULL) {
        pthread_mutex_unlock(&runtime->mutex);
        return -ESTALE;
    }
    if (!record->caller_owns_call) {
        int result = record->call_terminal_dequeued ? -ESTALE : -EACCES;
        pthread_mutex_unlock(&runtime->mutex);
        return result;
    }
    info->flags = 0;
    if (record->has_deadline) {
        info->flags |= TREVRPC_RPC_CALL_CONTEXT_HAS_DEADLINE;
        clock_result = trevrpc_rpc_monotonic_nanos(&now);
        if (clock_result != 0) {
            pthread_mutex_unlock(&runtime->mutex);
            return clock_result;
        }
        if (record->deadline_expired) {
            info->flags |= TREVRPC_RPC_CALL_CONTEXT_DEADLINE_EXPIRED;
        } else if (record->call_deadline_nanos > now) {
            remaining = record->call_deadline_nanos - now;
        }
    }
    if (record->cancelled) {
        info->flags |= TREVRPC_RPC_CALL_CONTEXT_CANCELLED;
    }
    info->time_remaining_nanos = remaining;
    pthread_mutex_unlock(&runtime->mutex);
    return 0;
}

int trevrpc_rpc_event_get_admission_info_v1(const trevrpc_rpc_event* event_const, trevrpc_rpc_admission_info_v1* info) {
    trevrpc_rpc_event* event = (trevrpc_rpc_event*)event_const;
    int result = 0;
    if (event == NULL || info == NULL || info->struct_size < sizeof(*info)) {
        return -EINVAL;
    }
    if (info->struct_version != TREVRPC_RPC_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (!trevrpc_rpc_u64_fields_zero(info->reserved, 4)) {
        return -EINVAL;
    }
    if (event->admission_event == NULL) {
        result =
            event->kind == TREVRPC_RPC_EVENT_HTTP3_ADMISSION || event->kind == TREVRPC_RPC_EVENT_WEBTRANSPORT_ADMISSION
                ? -ESTALE
                : -ENOTSUP;
    } else if (event->kind != TREVRPC_RPC_EVENT_HTTP3_ADMISSION &&
               event->kind != TREVRPC_RPC_EVENT_WEBTRANSPORT_ADMISSION) {
        result = -ENOTSUP;
    } else {
        info->protocol = event->admission_protocol;
        info->flags = event->admission_flags;
        info->listener = event->admission_listener;
        info->path = event->admission_path;
        info->path_len = event->admission_path_len;
        info->authority = event->admission_authority;
        info->authority_len = event->admission_authority_len;
        info->origin = event->admission_origin;
        info->origin_len = event->admission_origin_len;
    }
    return result;
}

int trevrpc_rpc_admission_respond_v1(const trevrpc_rpc_event* event_const, uint16_t http_status) {
    trevrpc_rpc_event* event = (trevrpc_rpc_event*)event_const;
    int result;
    if (event == NULL || (http_status != 200 && (http_status < 400 || http_status > 599))) {
        return -EINVAL;
    }
    if (event->kind != TREVRPC_RPC_EVENT_HTTP3_ADMISSION && event->kind != TREVRPC_RPC_EVENT_WEBTRANSPORT_ADMISSION) {
        result = -ENOTSUP;
    } else if (event->admission_event == NULL) {
        result = -ESTALE;
    } else {
        result =
            trevrpc_rpc_transport_admission_respond(event->admission_transport, event->admission_event, http_status);
    }
    return result;
}

int trevrpc_rpc_event_take_incoming_call(trevrpc_rpc_event* event,
    trevrpc_rpc_call_v1* out_call,
    trevrpc_rpc_stream_v1* out_stream,
    trevrpc_rpc_receive** out_initial_message) {
    trevrpc_rpc_call_record* record;
    if (event == NULL || out_call == NULL || out_stream == NULL || out_initial_message == NULL) {
        return -EINVAL;
    }
    if (event->kind != TREVRPC_RPC_EVENT_CALL_INCOMING || event->runtime == NULL) {
        return -EINVAL;
    }
    {
        unsigned expected = 0;
        if (!atomic_compare_exchange_strong_explicit(
                &event->take_claim, &expected, 1, memory_order_acq_rel, memory_order_acquire)) {
            return -EALREADY;
        }
    }
    pthread_mutex_lock(&event->runtime->mutex);
    record = trevrpc_rpc_find_call_locked(event->runtime, event->call);
    if (record == NULL || event->runtime->public_released) {
        pthread_mutex_unlock(&event->runtime->mutex);
        atomic_store_explicit(&event->take_claim, 0, memory_order_release);
        return record == NULL ? -ESTALE : -ESHUTDOWN;
    }
    record->caller_owns_call = true;
    record->caller_owns_stream = true;
    pthread_mutex_unlock(&event->runtime->mutex);
    event->incoming_taken = true;
    *out_call = event->call;
    *out_stream = event->stream;
    *out_initial_message = event->receive;
    event->receive = NULL;
    return 0;
}

void trevrpc_rpc_event_release(trevrpc_rpc_event* event) {
    trevrpc_rpc_event_destroy(event);
}

int trevrpc_rpc_receive_get_info_v1(const trevrpc_rpc_receive* receive, trevrpc_rpc_receive_info_v1* info) {
    if (receive == NULL || info == NULL || info->struct_size < sizeof(*info)) {
        return -EINVAL;
    }
    if (info->struct_version != TREVRPC_RPC_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    info->kind = receive->kind;
    info->flags = receive->flags;
    info->rpc_status = receive->rpc_status;
    info->data = receive->data;
    info->data_len = receive->data_len;
    info->message = receive->message;
    info->message_len = receive->message_len;
    info->metadata = receive->metadata;
    info->metadata_count = receive->metadata_count;
    return 0;
}

void trevrpc_rpc_receive_release(trevrpc_rpc_receive* receive) {
    trevrpc_rpc_receive_destroy(receive);
}

static int trevrpc_rpc_runtime_get_diagnostics_v1_impl(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_diagnostics_v1* diagnostics) {
    trevrpc_rpc_transport_diagnostics transport_diagnostics;
    if (runtime == NULL || diagnostics == NULL || diagnostics->struct_size < sizeof(*diagnostics)) {
        return -EINVAL;
    }
    if (diagnostics->struct_version != TREVRPC_RPC_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    memset(&transport_diagnostics, 0, sizeof(transport_diagnostics));
    if (trevrpc_rpc_transport_get_diagnostics(runtime->transport, &transport_diagnostics) != 0) {
        return -EIO;
    }
    pthread_mutex_lock(&runtime->mutex);
    diagnostics->rpc_abi_version = TREVRPC_RPC_ABI_VERSION;
    diagnostics->state = runtime->state;
    diagnostics->terminal_status = runtime->terminal_status;
    diagnostics->event_capacity = runtime->event_capacity;
    diagnostics->queue_depth = runtime->queue_depth;
    diagnostics->ordinary_queue_depth = runtime->ordinary_queue_depth;
    diagnostics->events_enqueued = runtime->events_enqueued;
    diagnostics->events_dequeued = runtime->events_dequeued;
    diagnostics->events_rejected = runtime->events_rejected;
    diagnostics->live_endpoints = runtime->live_endpoints;
    diagnostics->live_calls = runtime->live_calls;
    diagnostics->live_streams = runtime->live_streams;
    diagnostics->wake_signals = runtime->wake_signals;
    diagnostics->wake_write_eagain = runtime->wake_write_eagain;
    diagnostics->wake_failures = runtime->wake_failures;
    diagnostics->mandatory_reservations =
        runtime->mandatory_reservations + transport_diagnostics.mandatory_reservations;
    pthread_mutex_unlock(&runtime->mutex);
    diagnostics->receive_owned_count = transport_diagnostics.receive_owned_count;
    diagnostics->peak_receive_owned_count = transport_diagnostics.peak_receive_owned_count;
    diagnostics->receive_owned_bytes = transport_diagnostics.receive_owned_bytes;
    diagnostics->peak_receive_owned_bytes = transport_diagnostics.peak_receive_owned_bytes;
    diagnostics->pending_send_bytes = transport_diagnostics.pending_send_bytes;
    diagnostics->pending_send_count = transport_diagnostics.pending_send_count;
    diagnostics->active_api_calls = atomic_load_explicit(&runtime->api_gate, memory_order_acquire) >> 1u;
    diagnostics->active_callbacks = transport_diagnostics.active_callbacks;
    diagnostics->provider_error_code = transport_diagnostics.provider_error_code;
    return 0;
}

static int trevrpc_rpc_runtime_start_transport_endpoint_v1_impl(trevrpc_rpc_runtime* runtime,
    const trevrpc_rpc_transport_endpoint_config* config,
    uint32_t mode,
    uint64_t operation_id,
    trevrpc_rpc_endpoint_v1* out_endpoint) {
    trevrpc_rpc_transport_handle transport_endpoint;
    trevrpc_rpc_endpoint_record* record;
    trevrpc_rpc_operation* operation;
    uint32_t transport_kind;
    int result;
    if (runtime == NULL || config == NULL || out_endpoint == NULL || operation_id == 0 ||
        (mode != TREVRPC_RPC_ENDPOINT_CLIENT && mode != TREVRPC_RPC_ENDPOINT_SERVER)) {
        return -EINVAL;
    }
    record = calloc(1, sizeof(*record));
    operation = trevrpc_rpc_operation_allocate(
        TREVRPC_RPC_OPERATION_ENDPOINT_START, TREVRPC_RPC_EVENT_ENDPOINT_READY, operation_id);
    if (record == NULL || operation == NULL) {
        free(record);
        if (operation != NULL) {
            trevrpc_rpc_event_destroy(operation->event);
            free(operation);
        }
        return -ENOMEM;
    }
    record->terminal_event = trevrpc_rpc_event_allocate(TREVRPC_RPC_EVENT_ENDPOINT_CLOSED);
    if (record->terminal_event == NULL) {
        trevrpc_rpc_event_destroy(operation->event);
        free(operation);
        free(record);
        return -ENOMEM;
    }
    record->terminal_event->reserved = true;
    transport_kind = mode == TREVRPC_RPC_ENDPOINT_SERVER ? TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER
                                                         : TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION;
    pthread_mutex_lock(&runtime->mutex);
    if (runtime->live_endpoints >= runtime->endpoint_capacity) {
        result = -EAGAIN;
    } else {
        result = trevrpc_rpc_allocate_handle_locked(runtime,
            &runtime->endpoint_sequence,
            &record->endpoint.owner,
            &record->endpoint.slot,
            &record->endpoint.generation);
    }
    if (result == 0) {
        result = trevrpc_rpc_operation_admit_locked(runtime,
            operation,
            TREVRPC_RPC_OBJECT_ENDPOINT,
            record->endpoint.owner,
            record->endpoint.slot,
            record->endpoint.generation,
            TREVRPC_RPC_OBJECT_ENDPOINT,
            record->endpoint.owner,
            record->endpoint.slot,
            record->endpoint.generation);
    }
    if (result == 0) {
        result =
            mode == TREVRPC_RPC_ENDPOINT_SERVER
                ? trevrpc_rpc_transport_endpoint_listen(runtime->transport, config, &transport_endpoint)
                : trevrpc_rpc_transport_endpoint_dial(runtime->transport, config, operation_id, &transport_endpoint);
    }
    if (result == 0) {
        uint64_t key[6];
        uint8_t key_count;
        record->transport_endpoint = transport_endpoint;
        record->transport_kind = transport_kind;
        trevrpc_rpc_handle_key(
            key, &key_count, record->endpoint.owner, record->endpoint.slot, record->endpoint.generation);
        result = trevrpc_rpc_registry_insert(&runtime->endpoint_registry, key, key_count, record);
        if (result == 0) {
            trevrpc_rpc_transport_key(
                key, &key_count, transport_endpoint, transport_kind, TREVRPC_RPC_TRANSPORT_ROLE_ENDPOINT);
            result = trevrpc_rpc_registry_insert_alias(&runtime->transport_registry, key, key_count, record);
            if (result != 0) {
                trevrpc_rpc_handle_key(
                    key, &key_count, record->endpoint.owner, record->endpoint.slot, record->endpoint.generation);
                (void)trevrpc_rpc_registry_remove(&runtime->endpoint_registry, key, key_count);
            }
        }
    }
    if (result == 0) {
        record->start_operation = operation;
        record->caller_owned = true;
        record->next = runtime->endpoints;
        runtime->endpoints = record;
        ++runtime->live_endpoints;
        ++runtime->mandatory_reservations;
        if (mode == TREVRPC_RPC_ENDPOINT_SERVER) {
            operation->event->flags = TREVRPC_RPC_EVENT_FLAG_SERVER | TREVRPC_RPC_EVENT_FLAG_LOCAL;
            operation->event->subject_kind = TREVRPC_RPC_OBJECT_ENDPOINT;
            operation->event->endpoint = record->endpoint;
            operation->event->operation_id = operation_id;
            record->start_operation = NULL;
            (void)trevrpc_rpc_complete_operation_locked(runtime, operation);
        }
    } else if (operation->subject_owner != 0) {
        trevrpc_rpc_operation_remove_locked(runtime, operation, true);
        operation = NULL;
    }
    pthread_mutex_unlock(&runtime->mutex);
    if (result != 0) {
        if (record->transport_endpoint.owner != 0) {
            (void)trevrpc_rpc_transport_release_handle(runtime->transport, record->transport_endpoint, transport_kind);
        }
        if (operation != NULL) {
            trevrpc_rpc_event_destroy(operation->event);
            free(operation);
        }
        trevrpc_rpc_event_destroy(record->terminal_event);
        free(record);
        return result;
    }
    *out_endpoint = record->endpoint;
    return 0;
}

static int trevrpc_rpc_endpoint_get_port_v1_impl(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_endpoint_v1 endpoint, uint16_t* out_port) {
    trevrpc_rpc_endpoint_record* record;
    trevrpc_rpc_transport_handle transport_endpoint = {0};
    int result;
    if (runtime == NULL || out_port == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_endpoint_locked(runtime, endpoint);
    result = record == NULL ? -ESTALE : record->transport_kind == TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER ? 0 : -EINVAL;
    if (result == 0) {
        transport_endpoint = record->transport_endpoint;
    }
    pthread_mutex_unlock(&runtime->mutex);
    if (result != 0) {
        return result;
    }
    return trevrpc_rpc_transport_endpoint_get_port(runtime->transport, transport_endpoint, out_port);
}

static int trevrpc_rpc_call_open_v1_impl(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_endpoint_v1 endpoint,
    const trevrpc_rpc_call_config_v1* config,
    uint64_t operation_id,
    trevrpc_rpc_call_v1* out_call,
    trevrpc_rpc_stream_v1* out_stream) {
    trevrpc_rpc_transport_handle stream;
    trevrpc_rpc_endpoint_record* endpoint_record = NULL;
    trevrpc_rpc_call_record* record;
    trevrpc_rpc_operation* operation;
    trevrpc_rpc_cancellation_record* cancellation = NULL;
    trevrpc_metadata metadata;
    trevrpc_metadata_entry* metadata_storage;
    uint8_t* request_frame = NULL;
    size_t request_frame_len = 0;
    uint64_t deadline_nanos;
    int result;
    if (runtime == NULL || config == NULL || out_call == NULL || out_stream == NULL || operation_id == 0 ||
        config->struct_size < sizeof(*config) || config->struct_version != TREVRPC_RPC_STRUCT_VERSION_1 ||
        config->kind > TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING || config->flags != 0 ||
        !trevrpc_rpc_u64_fields_zero(config->reserved, 3) || config->service == NULL || config->service_len == 0 ||
        config->method == NULL || config->method_len == 0 ||
        (config->initial_message == NULL && config->initial_message_len != 0) ||
        config->initial_message_len > runtime->max_message_size) {
        return -EINVAL;
    }
    result = trevrpc_rpc_deadline_after(config->timeout_nanos, &deadline_nanos);
    if (result != 0) {
        return result;
    }
    result = trevrpc_rpc_metadata_view(runtime, config->metadata, config->metadata_count, &metadata, &metadata_storage);
    if (result != 0) {
        return result;
    }
    result = trevrpc_wire_encode_request_view(config->service,
        config->service_len,
        config->method,
        config->method_len,
        config->kind,
        TREVRPC_RPC_ABI_VERSION,
        config->initial_message,
        (size_t)config->initial_message_len,
        &metadata,
        config->timeout_nanos == TREVRPC_RPC_DEADLINE_INFINITE ? 0 : config->timeout_nanos,
        (size_t)runtime->max_message_size,
        &request_frame,
        &request_frame_len);
    free(metadata_storage);
    if (result != 0) {
        return result;
    }
    record = trevrpc_rpc_call_record_allocate();
    operation =
        trevrpc_rpc_operation_allocate(TREVRPC_RPC_OPERATION_CALL_OPEN, TREVRPC_RPC_EVENT_CALL_READY, operation_id);
    if (record == NULL || operation == NULL) {
        if (record != NULL) {
            trevrpc_rpc_event_destroy(record->stream_terminal_event);
            trevrpc_rpc_event_destroy(record->call_terminal_event);
            free(record);
        }
        if (operation != NULL) {
            trevrpc_rpc_event_destroy(operation->event);
            free(operation);
        }
        free(request_frame);
        return -ENOMEM;
    }
    pthread_mutex_lock(&runtime->mutex);
    if (!trevrpc_rpc_handle_is_zero(
            config->cancellation.owner, config->cancellation.slot, config->cancellation.generation)) {
        cancellation = trevrpc_rpc_find_cancellation_locked(runtime, config->cancellation);
        if (cancellation == NULL) {
            result = -ESTALE;
        } else if (cancellation->cancelled) {
            result = -ECANCELED;
        } else {
            result = 0;
        }
    } else {
        result = 0;
    }
    if (result == 0) {
        endpoint_record = trevrpc_rpc_find_endpoint_locked(runtime, endpoint);
        if (endpoint_record == NULL) {
            result = -ESTALE;
        } else if (endpoint_record->transport_kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION ||
                   endpoint_record->terminal_committed) {
            result = -EINVAL;
        }
    }
    if (result == 0 &&
        (runtime->live_calls >= runtime->call_capacity || runtime->live_streams >= runtime->stream_capacity)) {
        result = -EAGAIN;
    }
    if (result == 0) {
        result = trevrpc_rpc_allocate_handle_locked(
            runtime, &runtime->call_sequence, &record->call.owner, &record->call.slot, &record->call.generation);
        record->stream = (trevrpc_rpc_stream_v1){record->call.owner, record->call.slot, record->call.generation};
    }
    if (result == 0) {
        result = trevrpc_rpc_operation_admit_locked(runtime,
            operation,
            TREVRPC_RPC_OBJECT_CALL,
            record->call.owner,
            record->call.slot,
            record->call.generation,
            TREVRPC_RPC_OBJECT_CALL,
            record->call.owner,
            record->call.slot,
            record->call.generation);
    }
    if (result == 0) {
        result = trevrpc_rpc_transport_stream_open(
            runtime->transport, endpoint_record->transport_endpoint, operation_id, &stream);
    }
    if (result == 0) {
        record->endpoint = endpoint;
        record->transport_stream = stream;
        record->kind = config->kind;
        record->open_operation = operation;
        record->request_frame = request_frame;
        record->request_frame_len = request_frame_len;
        record->call_deadline_nanos = deadline_nanos;
        record->has_deadline = deadline_nanos != TREVRPC_RPC_DEADLINE_INFINITE;
        record->deadline_nanos = deadline_nanos;
        record->max_response_messages = config->max_response_messages;
        record->max_response_body_size = config->max_response_body_size;
        record->max_response_stream_body_size = config->max_response_stream_body_size;
        record->response_idle_timeout_nanos = config->response_idle_timeout_nanos;
        record->local = true;
        record->caller_owns_call = true;
        record->caller_owns_stream = true;
        record->cancellation = cancellation;
        if (cancellation != NULL) {
            ++cancellation->attached_calls;
        }
        result = trevrpc_rpc_insert_call_locked(runtime, record);
        if (result == 0 && record->deadline_nanos != TREVRPC_RPC_DEADLINE_INFINITE) {
            trevrpc_rpc_signal_driver_locked(runtime);
        }
    }
    if (result != 0 && operation->subject_owner != 0) {
        trevrpc_rpc_operation_remove_locked(runtime, operation, true);
        operation = NULL;
    }
    pthread_mutex_unlock(&runtime->mutex);
    if (result != 0) {
        if (operation != NULL) {
            trevrpc_rpc_event_destroy(operation->event);
            free(operation);
        }
        trevrpc_rpc_event_destroy(record->stream_terminal_event);
        trevrpc_rpc_event_destroy(record->call_terminal_event);
        free(record);
        free(request_frame);
        return result;
    }
    *out_call = record->call;
    *out_stream = record->stream;
    return 0;
}

static int trevrpc_rpc_call_accept_impl(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call, uint64_t operation_id) {
    trevrpc_rpc_call_record* record;
    trevrpc_rpc_operation* operation;
    int result;
    if (runtime == NULL || operation_id == 0) {
        return -EINVAL;
    }
    operation = trevrpc_rpc_operation_allocate(
        TREVRPC_RPC_OPERATION_CALL_ACCEPT, TREVRPC_RPC_EVENT_CALL_ACCEPTED, operation_id);
    if (operation == NULL) {
        return -ENOMEM;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_call_locked(runtime, call);
    if (record == NULL) {
        result = -ESTALE;
    } else if (record->local || record->accepted || record->closing) {
        result = record->accepted ? -EALREADY : -EINVAL;
    } else if (!record->caller_owns_call || !record->caller_owns_stream) {
        result = -EACCES;
    } else {
        result = trevrpc_rpc_operation_admit_locked(runtime,
            operation,
            TREVRPC_RPC_OBJECT_CALL,
            call.owner,
            call.slot,
            call.generation,
            TREVRPC_RPC_OBJECT_CALL,
            call.owner,
            call.slot,
            call.generation);
    }
    if (result == 0) {
        record->accepted = true;
        trevrpc_rpc_reset_idle_locked(runtime, record, true);
        if (record->send_idle_deadline_nanos != 0) {
            trevrpc_rpc_signal_driver_locked(runtime);
        }
        operation->event->subject_kind = TREVRPC_RPC_OBJECT_CALL;
        operation->event->endpoint = record->endpoint;
        operation->event->call = call;
        operation->event->stream = record->stream;
        operation->event->operation_id = operation_id;
        (void)trevrpc_rpc_complete_operation_locked(runtime, operation);
    }
    pthread_mutex_unlock(&runtime->mutex);
    if (result != 0) {
        trevrpc_rpc_event_destroy(operation->event);
        free(operation);
    }
    return result;
}

static int trevrpc_rpc_stream_send_copy_v1_impl(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_stream_v1 stream,
    uint64_t operation_id,
    const uint8_t* message,
    size_t message_len,
    uint32_t flags) {
    trevrpc_rpc_call_record* record;
    trevrpc_rpc_operation* operation;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    uint64_t previous_message_count = 0;
    uint64_t previous_body_size = 0;
    bool reserved_limit = false;
    int result;
    if (runtime == NULL || operation_id == 0 || flags != 0 || (message == NULL && message_len != 0) ||
        message_len > runtime->max_message_size || runtime->max_message_size > SIZE_MAX - (64u * 1024u)) {
        return -EINVAL;
    }
    result = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_MESSAGE,
        TREVRPC_RPC_STATUS_OK,
        NULL,
        0,
        message,
        message_len,
        NULL,
        (size_t)runtime->max_message_size + (64u * 1024u),
        &frame,
        &frame_len);
    if (result != 0) {
        return result;
    }
    operation =
        trevrpc_rpc_operation_allocate(TREVRPC_RPC_OPERATION_SEND, TREVRPC_RPC_EVENT_SEND_COMPLETE, operation_id);
    if (operation == NULL) {
        free(frame);
        return -ENOMEM;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_stream_locked(runtime, stream);
    if (record == NULL) {
        result = -ESTALE;
    } else if (record->closing || record->send_finish_pending || record->send_closed) {
        result = -EPIPE;
    } else if (!record->local && !record->accepted) {
        result = -EACCES;
    } else {
        previous_message_count = record->response_message_count;
        previous_body_size = record->response_body_size;
        if (!record->local && record->kind != TREVRPC_RPC_KIND_UNARY) {
            result = trevrpc_rpc_reserve_message_locked(&record->response_message_count,
                &record->response_body_size,
                record->max_send_messages,
                record->max_send_body_size,
                message_len);
            reserved_limit = result == 0;
        } else {
            result = 0;
        }
        if (result == 0) {
            result = trevrpc_rpc_operation_admit_locked(runtime,
                operation,
                TREVRPC_RPC_OBJECT_STREAM,
                stream.owner,
                stream.slot,
                stream.generation,
                TREVRPC_RPC_OBJECT_CALL,
                record->call.owner,
                record->call.slot,
                record->call.generation);
        }
    }
    if (result == 0) {
        result = trevrpc_rpc_transport_stream_send(
            runtime->transport, record->transport_stream, operation->transport_operation_id, frame + 4, frame_len - 4);
    }
    if (result == 0) {
        trevrpc_rpc_reset_idle_locked(runtime, record, true);
        if (record->send_idle_deadline_nanos != 0) {
            trevrpc_rpc_signal_driver_locked(runtime);
        }
    } else if (reserved_limit && record != NULL) {
        record->response_message_count = previous_message_count;
        record->response_body_size = previous_body_size;
    }
    if (result != 0 && operation->subject_owner != 0) {
        trevrpc_rpc_operation_remove_locked(runtime, operation, true);
        operation = NULL;
    }
    pthread_mutex_unlock(&runtime->mutex);
    free(frame);
    if (result != 0 && operation != NULL) {
        trevrpc_rpc_event_destroy(operation->event);
        free(operation);
    }
    return result;
}

static int trevrpc_rpc_copy_decoded_receive(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_receive* receive,
    uint32_t kind,
    uint32_t status,
    const char* message,
    size_t message_len,
    const uint8_t* body,
    size_t body_len,
    const trevrpc_metadata* metadata) {
    int result;
    if (atomic_exchange_explicit(&runtime->test_fail_next_receive_copy, false, memory_order_acq_rel)) {
        return -ENOMEM;
    }
    if (message_len > UINT32_MAX || message_len > runtime->max_status_message_size ||
        body_len > runtime->max_message_size) {
        return -EMSGSIZE;
    }
    result = trevrpc_rpc_validate_metadata_limits(runtime, metadata);
    if (result != 0) {
        return result;
    }
    receive->kind = kind;
    receive->rpc_status = status;
    receive->data_len = body_len;
    receive->message_len = (uint32_t)message_len;
    if ((body_len != 0 && (receive->data = malloc(body_len)) == NULL) ||
        (message_len != 0 && (receive->message = malloc(message_len)) == NULL)) {
        return -ENOMEM;
    }
    if (body_len != 0) {
        memcpy(receive->data, body, body_len);
    }
    if (message_len != 0) {
        memcpy(receive->message, message, message_len);
    }
    result = trevrpc_rpc_copy_metadata(runtime, receive, metadata);
    return result;
}

static void trevrpc_rpc_stream_receive_leave(trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream) {
    trevrpc_rpc_call_record* record;
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_stream_locked(runtime, stream);
    if (record != NULL && record->receive_in_flight != 0) {
        --record->receive_in_flight;
        trevrpc_rpc_call_maybe_reclaim_locked(runtime, record);
    }
    pthread_mutex_unlock(&runtime->mutex);
}

static int trevrpc_rpc_stream_receive_restore(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_stream_v1 stream,
    trevrpc_rpc_transport_receive* transport_receive,
    uint64_t readable_epoch) {
    trevrpc_rpc_event* event = trevrpc_rpc_event_allocate(TREVRPC_RPC_EVENT_STREAM_READABLE);
    trevrpc_rpc_call_record* record;
    int result = -ESTALE;
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_stream_locked(runtime, stream);
    if (record != NULL && !record->closing && !record->stream_terminal_committed) {
        if (record->pending_transport_receive != NULL) {
            result = -EALREADY;
        } else if (record->readable_events_pending != 0) {
            record->pending_transport_receive = transport_receive;
            transport_receive = NULL;
            result = 0;
        } else if (event == NULL) {
            result = -ENOMEM;
        } else {
            event->flags = record->readable_flags;
            event->subject_kind = TREVRPC_RPC_OBJECT_STREAM;
            event->endpoint = record->endpoint;
            event->call = record->call;
            event->stream = record->stream;
            record->pending_transport_receive = transport_receive;
            transport_receive = NULL;
            ++record->readable_epoch;
            result = trevrpc_rpc_publish_locked(runtime, event, true);
            if (result == 0) {
                ++record->readable_events_pending;
                event = NULL;
            } else {
                --record->readable_epoch;
                transport_receive = record->pending_transport_receive;
                record->pending_transport_receive = NULL;
            }
        }
    }
    if (result != 0) {
        trevrpc_rpc_mark_readable_drained_locked(runtime, record, readable_epoch);
    }
    pthread_mutex_unlock(&runtime->mutex);
    trevrpc_rpc_event_destroy(event);
    if (transport_receive != NULL) {
        trevrpc_rpc_transport_receive_release(runtime->transport, transport_receive);
    }
    return result;
}

static int trevrpc_rpc_stream_receive_impl(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream, trevrpc_rpc_receive** out_receive) {
    trevrpc_rpc_transport_receive* transport_receive = NULL;
    trevrpc_rpc_transport_receive_info transport_info;
    trevrpc_rpc_transport_handle transport_stream;
    trevrpc_rpc_call_record* record;
    trevrpc_rpc_receive* receive;
    trevrpc_wire_response_values* response = NULL;
    trevrpc_wire_stream_frame_values* frame = NULL;
    bool decode_unary_response;
    bool consume_peer_status = false;
    bool mark_peer_status = false;
    uint64_t readable_epoch;
    trevrpc_wire_diagnostic diagnostic = {0};
    bool abort_stream = false;
    bool receive_limit_reserved = false;
    uint64_t previous_receive_message_count = 0;
    uint64_t previous_receive_body_size = 0;
    int result;
    if (runtime == NULL || out_receive == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_stream_locked(runtime, stream);
    if (record == NULL) {
        pthread_mutex_unlock(&runtime->mutex);
        return -ESTALE;
    }
    if (record->receive_in_flight != 0) {
        pthread_mutex_unlock(&runtime->mutex);
        return -EBUSY;
    }
    ++record->receive_in_flight;
    decode_unary_response = record->local && record->kind == TREVRPC_RPC_KIND_UNARY;
    readable_epoch = record->readable_epoch;
    transport_stream = record->transport_stream;
    transport_receive = record->pending_transport_receive;
    record->pending_transport_receive = NULL;
    pthread_mutex_unlock(&runtime->mutex);
receive_next:
    result = transport_receive != NULL
                 ? 0
                 : trevrpc_rpc_transport_stream_receive(runtime->transport, transport_stream, &transport_receive);
    if (result != 0) {
        if (result == -ENOMEM) {
            int restore_result = trevrpc_rpc_stream_receive_restore(runtime, stream, NULL, readable_epoch);
            if (restore_result != 0 && restore_result != -ESTALE) {
                trevrpc_rpc_abort_or_terminal(
                    runtime, transport_stream, TREVRPC_RPC_STATUS_RESOURCE_EXHAUSTED, restore_result);
            }
        } else if (result == -EAGAIN || result == -ESTALE) {
            pthread_mutex_lock(&runtime->mutex);
            record = trevrpc_rpc_find_stream_locked(runtime, stream);
            trevrpc_rpc_mark_readable_drained_locked(runtime, record, readable_epoch);
            if (result == -ESTALE && record != NULL) {
                result = -EAGAIN;
            }
            pthread_mutex_unlock(&runtime->mutex);
        }
        trevrpc_rpc_stream_receive_leave(runtime, stream);
        return result;
    }
    result = trevrpc_rpc_transport_receive_get_info(runtime->transport, transport_receive, &transport_info);
    if (result != 0) {
        if (result > 0) {
            result = -EIO;
        }
        trevrpc_rpc_transport_receive_release(runtime->transport, transport_receive);
        pthread_mutex_lock(&runtime->mutex);
        record = trevrpc_rpc_find_stream_locked(runtime, stream);
        trevrpc_rpc_mark_readable_drained_locked(runtime, record, readable_epoch);
        pthread_mutex_unlock(&runtime->mutex);
        trevrpc_rpc_abort_or_terminal(runtime, transport_stream, TREVRPC_RPC_STATUS_INTERNAL, result);
        trevrpc_rpc_stream_receive_leave(runtime, stream);
        return result;
    }
    if (decode_unary_response) {
        result = trevrpc_wire_decode_response_diagnostic(
            transport_info.data, (size_t)transport_info.data_len, &response, &diagnostic);
    } else {
        result = trevrpc_wire_decode_stream_frame_diagnostic(
            transport_info.data, (size_t)transport_info.data_len, &frame, &diagnostic);
    }
    if (!decode_unary_response && result == 0 && frame->kind == TREVRPC_STREAM_FRAME_KIND_STATUS &&
        frame->body.len != 0) {
        diagnostic.reason = TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF;
        result = -EPROTO;
    }
    if (!decode_unary_response && result == 0) {
        pthread_mutex_lock(&runtime->mutex);
        record = trevrpc_rpc_find_stream_locked(runtime, stream);
        if (record == NULL) {
            result = -ESTALE;
        } else if (record->peer_status_received ||
                   (!record->local && record->kind != TREVRPC_RPC_KIND_CLIENT_STREAMING &&
                       record->kind != TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING)) {
            diagnostic.reason = TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF;
            result = -EPROTO;
        } else if (frame->kind == TREVRPC_STREAM_FRAME_KIND_STATUS) {
            if (!record->local && frame->status == TREVRPC_RPC_STATUS_OK) {
                record->peer_status_received = true;
                consume_peer_status = true;
            } else {
                mark_peer_status = true;
            }
        }
        pthread_mutex_unlock(&runtime->mutex);
    }
    /* A consumed frame that fails wire decoding cannot be retried: terminalize
     * the stream for every malformed decode result, not just size failures.
     * Allocation failure is local resource pressure, rather than peer input
     * corruption, and remains retryable. */
    abort_stream = result != 0 && diagnostic.reason != TREVRPC_WIRE_DIAGNOSTIC_ALLOCATION_FAILURE;
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_stream_locked(runtime, stream);
    if (record != NULL) {
        record->last_receive_diagnostic = diagnostic.reason;
    }
    pthread_mutex_unlock(&runtime->mutex);
    if (result == 0) {
        pthread_mutex_lock(&runtime->mutex);
        record = trevrpc_rpc_find_stream_locked(runtime, stream);
        if (record == NULL) {
            result = -ESTALE;
        } else if (decode_unary_response) {
            if (record->max_response_body_size >= 0 && response->body.len > (uint64_t)record->max_response_body_size) {
                result = -EMSGSIZE;
            }
        } else if (frame->kind == TREVRPC_STREAM_FRAME_KIND_MESSAGE) {
            previous_receive_message_count =
                record->local ? record->response_message_count : record->request_message_count;
            previous_receive_body_size = record->local ? record->response_body_size : record->request_body_size;
            result = trevrpc_rpc_reserve_message_locked(
                record->local ? &record->response_message_count : &record->request_message_count,
                record->local ? &record->response_body_size : &record->request_body_size,
                record->local ? record->max_response_messages : record->max_receive_messages,
                record->local ? record->max_response_stream_body_size : record->max_receive_body_size,
                frame->body.len);
            receive_limit_reserved = result == 0;
        }
        pthread_mutex_unlock(&runtime->mutex);
        if (result == -EMSGSIZE) {
            abort_stream = true;
        }
    }
    if (consume_peer_status) {
        trevrpc_internal_stream_frame_free(frame);
        frame = NULL;
        trevrpc_rpc_transport_receive_release(runtime->transport, transport_receive);
        transport_receive = NULL;
        pthread_mutex_lock(&runtime->mutex);
        record = trevrpc_rpc_find_stream_locked(runtime, stream);
        if (record != NULL) {
            /* A successful peer status closes the request direction; a
             * delayed transport FIN must not rearm an already-complete idle timer. */
            record->receive_idle_deadline_nanos = 0;
            (void)trevrpc_rpc_deadline_refresh_locked(runtime, record);
        }
        pthread_mutex_unlock(&runtime->mutex);
        consume_peer_status = false;
        goto receive_next;
    }
    receive = calloc(1, sizeof(*receive));
    if (receive == NULL) {
        int restore_result;
        trevrpc_internal_response_free(response);
        trevrpc_internal_stream_frame_free(frame);
        restore_result = trevrpc_rpc_stream_receive_restore(runtime, stream, transport_receive, readable_epoch);
        if (restore_result != 0 && restore_result != -ESTALE) {
            trevrpc_rpc_abort_or_terminal(
                runtime, transport_stream, TREVRPC_RPC_STATUS_RESOURCE_EXHAUSTED, restore_result);
        }
        trevrpc_rpc_stream_receive_leave(runtime, stream);
        return -ENOMEM;
    }
    if (decode_unary_response) {
        if (result == 0 && (response->status > TREVRPC_RPC_STATUS_UNAUTHENTICATED ||
                               (response->status != TREVRPC_RPC_STATUS_OK && response->body.len != 0))) {
            result = -EPROTO;
            abort_stream = true;
        }
        if (result == 0) {
            result = trevrpc_rpc_copy_decoded_receive(runtime,
                receive,
                response->status == TREVRPC_RPC_STATUS_OK ? TREVRPC_RPC_RECEIVE_MESSAGE : TREVRPC_RPC_RECEIVE_STATUS,
                response->status,
                response->message,
                response->message_len,
                response->body.data,
                response->body.len,
                &response->metadata);
        }
        trevrpc_internal_response_free(response);
    } else {
        if (result == 0) {
            result = trevrpc_rpc_copy_decoded_receive(runtime,
                receive,
                frame->kind == TREVRPC_STREAM_FRAME_KIND_STATUS ? TREVRPC_RPC_RECEIVE_STATUS
                                                                : TREVRPC_RPC_RECEIVE_MESSAGE,
                frame->status,
                frame->message,
                frame->message_len,
                frame->body.data,
                frame->body.len,
                &frame->metadata);
        }
        trevrpc_internal_stream_frame_free(frame);
    }
    if (result != 0 && receive_limit_reserved) {
        pthread_mutex_lock(&runtime->mutex);
        record = trevrpc_rpc_find_stream_locked(runtime, stream);
        if (record != NULL) {
            if (record->local) {
                record->response_message_count = previous_receive_message_count;
                record->response_body_size = previous_receive_body_size;
            } else {
                record->request_message_count = previous_receive_message_count;
                record->request_body_size = previous_receive_body_size;
            }
        }
        pthread_mutex_unlock(&runtime->mutex);
    }
    if (result == -ENOMEM) {
        int restore_result = trevrpc_rpc_stream_receive_restore(runtime, stream, transport_receive, readable_epoch);
        if (restore_result != 0 && restore_result != -ESTALE) {
            trevrpc_rpc_abort_or_terminal(
                runtime, transport_stream, TREVRPC_RPC_STATUS_RESOURCE_EXHAUSTED, restore_result);
        }
    } else {
        trevrpc_rpc_transport_receive_release(runtime->transport, transport_receive);
    }
    abort_stream = abort_stream || result == -EMSGSIZE;
    if (result != 0) {
        trevrpc_rpc_receive_destroy(receive);
        if (abort_stream) {
            pthread_mutex_lock(&runtime->mutex);
            record = trevrpc_rpc_find_stream_locked(runtime, stream);
            trevrpc_rpc_mark_readable_drained_locked(runtime, record, readable_epoch);
            pthread_mutex_unlock(&runtime->mutex);
            trevrpc_rpc_abort_or_terminal(
                runtime, transport_stream, trevrpc_rpc_receive_failure_status(result, diagnostic.reason), result);
        }
        trevrpc_rpc_stream_receive_leave(runtime, stream);
        return result;
    }
    if (mark_peer_status) {
        pthread_mutex_lock(&runtime->mutex);
        record = trevrpc_rpc_find_stream_locked(runtime, stream);
        if (record != NULL) {
            record->peer_status_received = true;
            record->receive_idle_deadline_nanos = 0;
            (void)trevrpc_rpc_deadline_refresh_locked(runtime, record);
        }
        pthread_mutex_unlock(&runtime->mutex);
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_stream_locked(runtime, stream);
    if (record != NULL) {
        if (decode_unary_response || receive->kind == TREVRPC_RPC_RECEIVE_STATUS) {
            record->receive_idle_deadline_nanos = 0;
            (void)trevrpc_rpc_deadline_refresh_locked(runtime, record);
        } else {
            trevrpc_rpc_reset_idle_locked(runtime, record, false);
            if (record->receive_idle_deadline_nanos != 0) {
                trevrpc_rpc_signal_driver_locked(runtime);
            }
        }
    }
    pthread_mutex_unlock(&runtime->mutex);
    *out_receive = receive;
    trevrpc_rpc_stream_receive_leave(runtime, stream);
    return 0;
}

void trevrpc_rpc_internal_test_fail_next_receive_copy(trevrpc_rpc_runtime* runtime) {
    if (runtime != NULL) {
        atomic_store_explicit(&runtime->test_fail_next_receive_copy, true, memory_order_release);
    }
}

void trevrpc_rpc_internal_test_set_call_release_result(trevrpc_rpc_runtime* runtime, int result, bool persistent) {
    if (runtime != NULL) {
        atomic_store_explicit(&runtime->test_call_release_result, result, memory_order_release);
        atomic_store_explicit(&runtime->test_call_release_result_persistent, persistent, memory_order_release);
    }
}

void trevrpc_rpc_internal_test_malformed_next_incoming(trevrpc_rpc_runtime* runtime, uint32_t kind) {
    if (runtime != NULL) {
        pthread_mutex_lock(&runtime->mutex);
        runtime->test_malformed_incoming_kind = kind;
        pthread_mutex_unlock(&runtime->mutex);
    }
}

trevrpc_wire_diagnostic_reason trevrpc_rpc_stream_last_receive_diagnostic(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream) {
    trevrpc_wire_diagnostic_reason reason = TREVRPC_WIRE_DIAGNOSTIC_NONE;
    if (runtime == NULL) {
        return reason;
    }
    pthread_mutex_lock(&runtime->mutex);
    {
        trevrpc_rpc_call_record* record = trevrpc_rpc_find_stream_locked(runtime, stream);
        if (record != NULL) {
            reason = record->last_receive_diagnostic;
        }
    }
    pthread_mutex_unlock(&runtime->mutex);
    return reason;
}

static int trevrpc_rpc_stream_finish_send_impl(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream, uint64_t operation_id) {
    trevrpc_rpc_call_record* record;
    trevrpc_rpc_operation* operation;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    int result;
    if (runtime == NULL || operation_id == 0) {
        return -EINVAL;
    }
    result = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_STATUS,
        TREVRPC_RPC_STATUS_OK,
        NULL,
        0,
        NULL,
        0,
        NULL,
        (size_t)runtime->max_message_size + (64u * 1024u),
        &frame,
        &frame_len);
    if (result != 0) {
        return result;
    }
    operation = trevrpc_rpc_operation_allocate(
        TREVRPC_RPC_OPERATION_STREAM_FINISH, TREVRPC_RPC_EVENT_SEND_FINISHED, operation_id);
    if (operation == NULL) {
        free(frame);
        return -ENOMEM;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_stream_locked(runtime, stream);
    if (record == NULL) {
        result = -ESTALE;
    } else if (record->closing || record->send_finish_pending || record->send_closed) {
        result = -EALREADY;
    } else {
        result = trevrpc_rpc_operation_admit_locked(runtime,
            operation,
            TREVRPC_RPC_OBJECT_STREAM,
            stream.owner,
            stream.slot,
            stream.generation,
            TREVRPC_RPC_OBJECT_CALL,
            record->call.owner,
            record->call.slot,
            record->call.generation);
    }
    if (result == 0) {
        record->send_finish_pending = true;
        trevrpc_rpc_reset_idle_locked(runtime, record, true);
        result = trevrpc_rpc_transport_stream_send(
            runtime->transport, record->transport_stream, operation->transport_operation_id, frame + 4, frame_len - 4);
    }
    if (trevrpc_rpc_send_direction_stopped_status(result) && operation->subject_owner != 0 && record != NULL &&
        record->local) {
        operation->event->flags = TREVRPC_RPC_EVENT_FLAG_LOCAL | TREVRPC_RPC_EVENT_FLAG_CLIENT;
        operation->event->status = 0;
        operation->event->subject_kind = TREVRPC_RPC_OBJECT_STREAM;
        operation->event->endpoint = record->endpoint;
        operation->event->call = record->call;
        operation->event->stream = record->stream;
        operation->event->operation_id = operation->operation_id;
        record->send_finish_pending = false;
        record->send_closed = true;
        (void)trevrpc_rpc_complete_operation_locked(runtime, operation);
        operation = NULL;
        result = 0;
    } else if (result != 0 && operation->subject_owner != 0) {
        if (record != NULL) {
            record->send_finish_pending = false;
        }
        trevrpc_rpc_operation_remove_locked(runtime, operation, true);
        operation = NULL;
    }
    pthread_mutex_unlock(&runtime->mutex);
    free(frame);
    if (result != 0 && operation != NULL) {
        trevrpc_rpc_event_destroy(operation->event);
        free(operation);
    }
    return result;
}

static int trevrpc_rpc_call_respond_copy_v1_impl(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_call_v1 call,
    uint64_t operation_id,
    const trevrpc_rpc_status_v1* status,
    const uint8_t* message,
    size_t message_len) {
    trevrpc_rpc_call_record* record;
    trevrpc_rpc_operation* operation;
    trevrpc_wire_response_values response = {0};
    trevrpc_metadata metadata;
    trevrpc_metadata_entry* metadata_storage;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    uint8_t* continuation_frame = NULL;
    size_t continuation_frame_len = 0;
    uint32_t call_kind;
    uint64_t previous_message_count = 0;
    uint64_t previous_body_size = 0;
    bool reserved_limit = false;
    int result;
    if (runtime == NULL || status == NULL || operation_id == 0 || status->struct_size < sizeof(*status) ||
        status->struct_version != TREVRPC_RPC_STRUCT_VERSION_1 || status->code > TREVRPC_RPC_STATUS_UNAUTHENTICATED ||
        status->flags != 0 || !trevrpc_rpc_u64_fields_zero(status->reserved, 4) ||
        (status->message == NULL && status->message_len != 0) ||
        status->message_len > runtime->max_status_message_size || (message == NULL && message_len != 0) ||
        (status->code == TREVRPC_RPC_STATUS_OK && message == NULL) ||
        (status->code != TREVRPC_RPC_STATUS_OK && message != NULL) || message_len > runtime->max_message_size ||
        runtime->max_message_size > SIZE_MAX - (64u * 1024u)) {
        return -EINVAL;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_call_locked(runtime, call);
    if (record == NULL) {
        result = -ESTALE;
    } else {
        call_kind = record->kind;
        result = 0;
    }
    pthread_mutex_unlock(&runtime->mutex);
    if (result != 0) {
        return result;
    }
    result = trevrpc_rpc_metadata_view(runtime, status->metadata, status->metadata_count, &metadata, &metadata_storage);
    if (result != 0) {
        return result;
    }
    if (call_kind == TREVRPC_RPC_KIND_CLIENT_STREAMING) {
        if (status->code == TREVRPC_RPC_STATUS_OK) {
            result = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_MESSAGE,
                TREVRPC_RPC_STATUS_OK,
                NULL,
                0,
                message,
                message_len,
                NULL,
                (size_t)runtime->max_message_size + (64u * 1024u),
                &frame,
                &frame_len);
            if (result == 0) {
                result = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_STATUS,
                    status->code,
                    status->message,
                    status->message_len,
                    NULL,
                    0,
                    &metadata,
                    (size_t)runtime->max_message_size + (64u * 1024u),
                    &continuation_frame,
                    &continuation_frame_len);
            }
        } else {
            result = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_STATUS,
                status->code,
                status->message,
                status->message_len,
                NULL,
                0,
                &metadata,
                (size_t)runtime->max_message_size + (64u * 1024u),
                &frame,
                &frame_len);
        }
    } else {
        response.status = status->code;
        response.message = (char*)status->message;
        response.message_len = status->message_len;
        response.body.data = message;
        response.body.len = message_len;
        response.metadata = metadata;
        result = trevrpc_wire_encode_response(
            &response, (size_t)runtime->max_message_size + (64u * 1024u), &frame, &frame_len);
    }
    free(metadata_storage);
    if (result != 0) {
        free(frame);
        free(continuation_frame);
        return result;
    }
    operation =
        trevrpc_rpc_operation_allocate(TREVRPC_RPC_OPERATION_RESPOND, TREVRPC_RPC_EVENT_SEND_COMPLETE, operation_id);
    if (operation == NULL) {
        free(frame);
        free(continuation_frame);
        return -ENOMEM;
    }
    operation->continuation_frame = continuation_frame;
    operation->continuation_frame_len = continuation_frame_len;
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_call_locked(runtime, call);
    if (record == NULL) {
        result = -ESTALE;
    } else if (record->local || !record->accepted ||
               (record->kind != TREVRPC_RPC_KIND_UNARY && record->kind != TREVRPC_RPC_KIND_CLIENT_STREAMING)) {
        result = -EINVAL;
    } else if (record->closing || record->send_finish_pending || record->send_closed) {
        result = -EPIPE;
    } else {
        previous_message_count = record->response_message_count;
        previous_body_size = record->response_body_size;
        if (status->code == TREVRPC_RPC_STATUS_OK && record->kind == TREVRPC_RPC_KIND_CLIENT_STREAMING) {
            result = trevrpc_rpc_reserve_message_locked(&record->response_message_count,
                &record->response_body_size,
                record->max_send_messages,
                record->max_send_body_size,
                message_len);
            reserved_limit = result == 0;
        } else {
            result = 0;
        }
        if (result == 0) {
            result = trevrpc_rpc_operation_admit_locked(runtime,
                operation,
                TREVRPC_RPC_OBJECT_STREAM,
                record->stream.owner,
                record->stream.slot,
                record->stream.generation,
                TREVRPC_RPC_OBJECT_CALL,
                call.owner,
                call.slot,
                call.generation);
        }
    }
    if (result == 0) {
        record->send_finish_pending = true;
        trevrpc_rpc_reset_idle_locked(runtime, record, true);
        result = trevrpc_rpc_transport_stream_send(
            runtime->transport, record->transport_stream, operation->transport_operation_id, frame + 4, frame_len - 4);
    }
    if (result != 0) {
        if (record != NULL) {
            record->send_finish_pending = false;
            if (reserved_limit) {
                record->response_message_count = previous_message_count;
                record->response_body_size = previous_body_size;
            }
        }
        if (operation->subject_owner != 0) {
            trevrpc_rpc_operation_remove_locked(runtime, operation, true);
            operation = NULL;
        }
    }
    pthread_mutex_unlock(&runtime->mutex);
    free(frame);
    if (result != 0 && operation != NULL) {
        trevrpc_rpc_event_destroy(operation->event);
        free(operation->continuation_frame);
        free(operation);
    }
    return result;
}

static int trevrpc_rpc_call_finish_v1_impl(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_call_v1 call,
    uint64_t operation_id,
    const trevrpc_rpc_status_v1* status) {
    trevrpc_rpc_call_record* record;
    trevrpc_rpc_operation* operation;
    trevrpc_metadata metadata;
    trevrpc_metadata_entry* metadata_storage;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    int result;
    if (runtime == NULL || status == NULL || operation_id == 0 || status->struct_size < sizeof(*status) ||
        status->struct_version != TREVRPC_RPC_STRUCT_VERSION_1 || status->code > TREVRPC_RPC_STATUS_UNAUTHENTICATED ||
        status->flags != 0 || !trevrpc_rpc_u64_fields_zero(status->reserved, 4) ||
        (status->message == NULL && status->message_len != 0) ||
        status->message_len > runtime->max_status_message_size ||
        runtime->max_message_size > SIZE_MAX - (64u * 1024u)) {
        return -EINVAL;
    }
    result = trevrpc_rpc_metadata_view(runtime, status->metadata, status->metadata_count, &metadata, &metadata_storage);
    if (result != 0) {
        return result;
    }
    result = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_STATUS,
        status->code,
        status->message,
        status->message_len,
        NULL,
        0,
        &metadata,
        (size_t)runtime->max_message_size + (64u * 1024u),
        &frame,
        &frame_len);
    free(metadata_storage);
    if (result != 0) {
        return result;
    }
    operation = trevrpc_rpc_operation_allocate(
        TREVRPC_RPC_OPERATION_CALL_FINISH, TREVRPC_RPC_EVENT_CALL_FINISHED, operation_id);
    if (operation == NULL) {
        free(frame);
        return -ENOMEM;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_call_locked(runtime, call);
    if (record == NULL) {
        result = -ESTALE;
    } else if (record->local || !record->accepted || record->kind == TREVRPC_RPC_KIND_UNARY ||
               record->kind == TREVRPC_RPC_KIND_CLIENT_STREAMING) {
        result = -EINVAL;
    } else if (record->closing || record->send_finish_pending || record->send_closed) {
        result = -EPIPE;
    } else {
        result = trevrpc_rpc_operation_admit_locked(runtime,
            operation,
            TREVRPC_RPC_OBJECT_STREAM,
            record->stream.owner,
            record->stream.slot,
            record->stream.generation,
            TREVRPC_RPC_OBJECT_CALL,
            call.owner,
            call.slot,
            call.generation);
    }
    if (result == 0) {
        record->send_finish_pending = true;
        trevrpc_rpc_reset_idle_locked(runtime, record, true);
        if (record->send_idle_deadline_nanos != 0) {
            trevrpc_rpc_signal_driver_locked(runtime);
        }
        operation->event->rpc_status = status->code;
        result = trevrpc_rpc_transport_stream_send(
            runtime->transport, record->transport_stream, operation->transport_operation_id, frame + 4, frame_len - 4);
    }
    if (result != 0 && operation->subject_owner != 0) {
        record->send_finish_pending = false;
        trevrpc_rpc_operation_remove_locked(runtime, operation, true);
        operation = NULL;
    }
    pthread_mutex_unlock(&runtime->mutex);
    free(frame);
    if (result != 0 && operation != NULL) {
        trevrpc_rpc_event_destroy(operation->event);
        free(operation);
    }
    return result;
}

static int trevrpc_rpc_call_cancel_impl(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call, uint64_t operation_id, uint64_t application_error_code) {
    trevrpc_rpc_call_record* record;
    trevrpc_rpc_operation* operation;
    trevrpc_rpc_transport_event_info deferred_terminal = {0};
    bool publish_deferred_terminal = false;
    bool was_cancelled = false;
    int result;
    if (runtime == NULL || operation_id == 0) {
        return -EINVAL;
    }
    operation =
        trevrpc_rpc_operation_allocate(TREVRPC_RPC_OPERATION_CALL_CANCEL, TREVRPC_RPC_EVENT_CANCELLED, operation_id);
    if (operation == NULL) {
        return -ENOMEM;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_call_locked(runtime, call);
    if (record == NULL) {
        result = -ESTALE;
    } else if (record->closing) {
        result = -EALREADY;
    } else {
        result = trevrpc_rpc_operation_admit_locked(runtime,
            operation,
            TREVRPC_RPC_OBJECT_CALL,
            call.owner,
            call.slot,
            call.generation,
            TREVRPC_RPC_OBJECT_CALL,
            call.owner,
            call.slot,
            call.generation);
    }
    if (result == 0) {
        record->closing = true;
        was_cancelled = record->cancelled;
        record->cancelled = true;
        if (record->deferred_stream_terminal_valid) {
            deferred_terminal = record->deferred_stream_terminal;
            publish_deferred_terminal = true;
        } else {
            result = trevrpc_rpc_transport_stream_abort(
                runtime->transport, record->transport_stream, application_error_code);
        }
    }
    if (result == 0) {
        operation->event->subject_kind = TREVRPC_RPC_OBJECT_CALL;
        operation->event->endpoint = record->endpoint;
        operation->event->call = call;
        operation->event->stream = record->stream;
        operation->event->operation_id = operation_id;
        operation->event->application_error_code = application_error_code;
        (void)trevrpc_rpc_complete_operation_locked(runtime, operation);
        if (publish_deferred_terminal) {
            trevrpc_rpc_publish_call_terminals_locked(runtime, record, &deferred_terminal);
        }
    } else if (operation->subject_owner != 0) {
        record->closing = false;
        record->cancelled = was_cancelled;
        trevrpc_rpc_operation_remove_locked(runtime, operation, true);
        operation = NULL;
    }
    pthread_mutex_unlock(&runtime->mutex);
    if (result != 0 && operation != NULL) {
        trevrpc_rpc_event_destroy(operation->event);
        free(operation);
    }
    return result;
}

static int trevrpc_rpc_close_call(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_call_v1 call,
    uint64_t operation_id,
    uint32_t flags,
    uint64_t application_error_code,
    uint32_t operation_kind,
    uint32_t completion_kind) {
    trevrpc_rpc_call_record* record;
    trevrpc_rpc_operation* operation;
    uint32_t subject_kind =
        operation_kind == TREVRPC_RPC_OPERATION_STREAM_CLOSE ? TREVRPC_RPC_OBJECT_STREAM : TREVRPC_RPC_OBJECT_CALL;
    bool was_cancelled = false;
    int result;
    if (runtime == NULL || operation_id == 0 || (flags & ~TREVRPC_RPC_CLOSE_FLAG_ABORT) != 0) {
        return -EINVAL;
    }
    operation = trevrpc_rpc_operation_allocate(operation_kind, completion_kind, operation_id);
    if (operation == NULL) {
        return -ENOMEM;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_call_locked(runtime, call);
    if (record == NULL) {
        result = -ESTALE;
    } else if (record->closing || record->stream_terminal_committed) {
        result = -EALREADY;
    } else {
        result = trevrpc_rpc_operation_admit_locked(runtime,
            operation,
            subject_kind,
            call.owner,
            call.slot,
            call.generation,
            TREVRPC_RPC_OBJECT_CALL,
            call.owner,
            call.slot,
            call.generation);
    }
    if (result == 0) {
        record->closing = true;
        was_cancelled = record->cancelled;
        if ((flags & TREVRPC_RPC_CLOSE_FLAG_ABORT) != 0) {
            record->cancelled = true;
        }
        record->close_operation = operation;
        operation->event->application_error_code = application_error_code;
        if (record->deferred_stream_terminal_valid) {
            trevrpc_rpc_transport_event_info terminal_info = record->deferred_stream_terminal;
            trevrpc_rpc_publish_call_terminals_locked(runtime, record, &terminal_info);
        } else {
            result = (flags & TREVRPC_RPC_CLOSE_FLAG_ABORT) != 0
                         ? trevrpc_rpc_transport_stream_abort(
                               runtime->transport, record->transport_stream, application_error_code)
                         : trevrpc_rpc_transport_stream_close(runtime->transport, record->transport_stream);
        }
    }
    if (result != 0 && operation->subject_owner != 0) {
        record->closing = false;
        record->cancelled = was_cancelled;
        record->close_operation = NULL;
        trevrpc_rpc_operation_remove_locked(runtime, operation, true);
        operation = NULL;
    }
    pthread_mutex_unlock(&runtime->mutex);
    if (result != 0 && operation != NULL) {
        trevrpc_rpc_event_destroy(operation->event);
        free(operation);
    }
    return result;
}

static int trevrpc_rpc_stream_close_impl(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_stream_v1 stream,
    uint64_t operation_id,
    uint32_t flags,
    uint64_t application_error_code) {
    return trevrpc_rpc_close_call(runtime,
        (trevrpc_rpc_call_v1){stream.owner, stream.slot, stream.generation},
        operation_id,
        flags,
        application_error_code,
        TREVRPC_RPC_OPERATION_STREAM_CLOSE,
        TREVRPC_RPC_EVENT_STREAM_CLOSED);
}

static int trevrpc_rpc_call_close_impl(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_call_v1 call,
    uint64_t operation_id,
    uint32_t flags,
    uint64_t application_error_code) {
    return trevrpc_rpc_close_call(runtime,
        call,
        operation_id,
        flags,
        application_error_code,
        TREVRPC_RPC_OPERATION_CALL_CLOSE,
        TREVRPC_RPC_EVENT_CALL_CLOSED);
}

static int trevrpc_rpc_endpoint_close_impl(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_endpoint_v1 endpoint, uint64_t operation_id) {
    trevrpc_rpc_endpoint_record* record;
    trevrpc_rpc_operation* operation;
    int result;
    if (runtime == NULL || operation_id == 0) {
        return -EINVAL;
    }
    operation = trevrpc_rpc_operation_allocate(
        TREVRPC_RPC_OPERATION_ENDPOINT_CLOSE, TREVRPC_RPC_EVENT_ENDPOINT_CLOSED, operation_id);
    if (operation == NULL) {
        return -ENOMEM;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_endpoint_locked(runtime, endpoint);
    if (record == NULL) {
        result = -ESTALE;
    } else if (record->close_operation != NULL || record->terminal_committed) {
        result = -EALREADY;
    } else {
        result = trevrpc_rpc_operation_admit_locked(runtime,
            operation,
            TREVRPC_RPC_OBJECT_ENDPOINT,
            endpoint.owner,
            endpoint.slot,
            endpoint.generation,
            TREVRPC_RPC_OBJECT_ENDPOINT,
            endpoint.owner,
            endpoint.slot,
            endpoint.generation);
    }
    if (result == 0) {
        record->close_operation = operation;
        result = record->transport_kind == TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER
                     ? trevrpc_rpc_transport_listener_close(runtime->transport, record->transport_endpoint)
                     : trevrpc_rpc_transport_connection_close(runtime->transport, record->transport_endpoint, 0);
    }
    if (result != 0 && operation->subject_owner != 0) {
        record->close_operation = NULL;
        trevrpc_rpc_operation_remove_locked(runtime, operation, true);
        operation = NULL;
    }
    pthread_mutex_unlock(&runtime->mutex);
    if (result != 0 && operation != NULL) {
        trevrpc_rpc_event_destroy(operation->event);
        free(operation);
    }
    return result;
}

static int trevrpc_rpc_stream_release_impl(trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream) {
    trevrpc_rpc_call_record* record;
    if (runtime == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_stream_locked(runtime, stream);
    if (record == NULL) {
        pthread_mutex_unlock(&runtime->mutex);
        return -ESTALE;
    }
    if (!record->stream_terminal_dequeued) {
        pthread_mutex_unlock(&runtime->mutex);
        return -EBUSY;
    }
    if (!record->caller_owns_stream) {
        pthread_mutex_unlock(&runtime->mutex);
        return -ESTALE;
    }
    if (record->receive_in_flight != 0) {
        pthread_mutex_unlock(&runtime->mutex);
        return -EBUSY;
    }
    trevrpc_rpc_drop_pending_receive_locked(runtime, record);
    {
        int release_result = trevrpc_rpc_transport_release_handle(
            runtime->transport, record->transport_stream, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM);
        if (release_result != 0) {
            pthread_mutex_unlock(&runtime->mutex);
            return release_result;
        }
    }
    record->transport_stream_released = true;
    record->caller_owns_stream = false;
    trevrpc_rpc_call_maybe_reclaim_locked(runtime, record);
    pthread_mutex_unlock(&runtime->mutex);
    return 0;
}

static int trevrpc_rpc_call_release_impl(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call) {
    trevrpc_rpc_call_record* record;
    if (runtime == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_call_locked(runtime, call);
    if (record == NULL) {
        pthread_mutex_unlock(&runtime->mutex);
        return -ESTALE;
    }
    if (!record->call_terminal_dequeued) {
        pthread_mutex_unlock(&runtime->mutex);
        return -EBUSY;
    }
    if (!record->caller_owns_call) {
        pthread_mutex_unlock(&runtime->mutex);
        return -ESTALE;
    }
    {
        int injected = atomic_load_explicit(&runtime->test_call_release_result, memory_order_acquire);
        if (atomic_load_explicit(&runtime->test_call_release_result_persistent, memory_order_acquire)) {
            if (injected != 0) {
                pthread_mutex_unlock(&runtime->mutex);
                return injected;
            }
        } else {
            injected = atomic_exchange_explicit(&runtime->test_call_release_result, 0, memory_order_acq_rel);
            if (injected != 0) {
                pthread_mutex_unlock(&runtime->mutex);
                return injected;
            }
        }
    }
    record->caller_owns_call = false;
    trevrpc_rpc_call_maybe_reclaim_locked(runtime, record);
    pthread_mutex_unlock(&runtime->mutex);
    return 0;
}

static int trevrpc_rpc_endpoint_release_impl(trevrpc_rpc_runtime* runtime, trevrpc_rpc_endpoint_v1 endpoint) {
    trevrpc_rpc_endpoint_record* record;
    if (runtime == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_endpoint_locked(runtime, endpoint);
    if (record == NULL) {
        pthread_mutex_unlock(&runtime->mutex);
        return -ESTALE;
    }
    if (!record->terminal_dequeued) {
        pthread_mutex_unlock(&runtime->mutex);
        return -EBUSY;
    }
    if (!record->caller_owned) {
        pthread_mutex_unlock(&runtime->mutex);
        return -ESTALE;
    }
    {
        int release_result = trevrpc_rpc_transport_release_handle(
            runtime->transport, record->transport_endpoint, record->transport_kind);
        if (release_result != 0) {
            pthread_mutex_unlock(&runtime->mutex);
            return release_result;
        }
    }
    record->transport_released = true;
    record->caller_owned = false;
    trevrpc_rpc_endpoint_maybe_reclaim_locked(runtime, record);
    pthread_mutex_unlock(&runtime->mutex);
    return 0;
}

static int trevrpc_rpc_cancellation_create_impl(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_cancellation_v1* out_cancellation) {
    trevrpc_rpc_cancellation_record* record;
    if (runtime == NULL || out_cancellation == NULL) {
        return -EINVAL;
    }
    record = calloc(1, sizeof(*record));
    if (record == NULL) {
        return -ENOMEM;
    }
    pthread_mutex_lock(&runtime->mutex);
    if (runtime->state != TREVRPC_RPC_STATE_RUNNING) {
        pthread_mutex_unlock(&runtime->mutex);
        free(record);
        return -ESHUTDOWN;
    }
    if (trevrpc_rpc_allocate_handle_locked(runtime,
            &runtime->cancellation_sequence,
            &record->cancellation.owner,
            &record->cancellation.slot,
            &record->cancellation.generation) != 0) {
        pthread_mutex_unlock(&runtime->mutex);
        free(record);
        return -EOVERFLOW;
    }
    {
        uint64_t key[6];
        uint8_t count;
        int result;
        trevrpc_rpc_handle_key(
            key, &count, record->cancellation.owner, record->cancellation.slot, record->cancellation.generation);
        result = trevrpc_rpc_registry_insert(&runtime->cancellation_registry, key, count, record);
        if (result != 0) {
            pthread_mutex_unlock(&runtime->mutex);
            free(record);
            return result;
        }
    }
    record->next = runtime->cancellations;
    runtime->cancellations = record;
    pthread_mutex_unlock(&runtime->mutex);
    *out_cancellation = record->cancellation;
    return 0;
}

static int trevrpc_rpc_cancellation_cancel_impl(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_cancellation_v1 cancellation, uint64_t operation_id) {
    trevrpc_rpc_cancellation_record* record;
    trevrpc_rpc_call_record* call;
    trevrpc_rpc_operation* operation;
    trevrpc_rpc_transport_handle* streams = NULL;
    size_t stream_capacity = 0;
    size_t stream_count = 0;
    size_t index = 0;
    int result;
    if (runtime == NULL || operation_id == 0) {
        return -EINVAL;
    }
    operation = trevrpc_rpc_operation_allocate(
        TREVRPC_RPC_OPERATION_CANCELLATION_CANCEL, TREVRPC_RPC_EVENT_CANCELLED, operation_id);
    if (operation == NULL) {
        return -ENOMEM;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_cancellation_locked(runtime, cancellation);
    if (record == NULL) {
        result = -ESTALE;
    } else if (record->cancelled) {
        result = -EALREADY;
    } else {
        result = trevrpc_rpc_operation_admit_locked(runtime,
            operation,
            TREVRPC_RPC_OBJECT_CANCELLATION,
            cancellation.owner,
            cancellation.slot,
            cancellation.generation,
            TREVRPC_RPC_OBJECT_CANCELLATION,
            cancellation.owner,
            cancellation.slot,
            cancellation.generation);
    }
    if (result == 0) {
        for (call = runtime->calls; call != NULL; call = call->next) {
            if (call->cancellation == record && !call->closing) {
                ++stream_capacity;
            }
        }
        if (stream_capacity != 0) {
            streams = calloc(stream_capacity, sizeof(*streams));
            if (streams == NULL) {
                result = -ENOMEM;
            }
        }
    }
    if (result == 0) {
        record->cancelled = true;
        for (call = runtime->calls; call != NULL; call = call->next) {
            if (call->cancellation == record && !call->closing) {
                if (stream_count == stream_capacity) {
                    abort();
                }
                call->closing = true;
                call->cancelled = true;
                streams[stream_count++] = call->transport_stream;
            }
        }
        operation->event->subject_kind = TREVRPC_RPC_OBJECT_CANCELLATION;
        operation->event->cancellation = cancellation;
        operation->event->operation_id = operation_id;
        (void)trevrpc_rpc_complete_operation_locked(runtime, operation);
    } else if (operation->subject_owner != 0) {
        trevrpc_rpc_operation_remove_locked(runtime, operation, true);
        operation = NULL;
    }
    pthread_mutex_unlock(&runtime->mutex);
    if (result != 0) {
        if (operation != NULL) {
            trevrpc_rpc_event_destroy(operation->event);
            free(operation);
        }
        free(streams);
        return result;
    }
    for (index = 0; index < stream_count; ++index) {
        int abort_result =
            trevrpc_rpc_transport_stream_abort(runtime->transport, streams[index], TREVRPC_RPC_STATUS_CANCELLED);
        if (abort_result != 0 && abort_result != -EALREADY) {
            trevrpc_rpc_transport_event_info info = {0};
            trevrpc_rpc_call_record* failed_call;
            info.flags = TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL;
            info.status = -ECANCELED;
            info.application_error_code = TREVRPC_RPC_STATUS_CANCELLED;
            info.provider_error_code = (uint64_t)(uint32_t)(-abort_result);
            pthread_mutex_lock(&runtime->mutex);
            failed_call = trevrpc_rpc_find_call_by_transport_locked(runtime, streams[index]);
            if (failed_call != NULL && !failed_call->stream_terminal_committed) {
                trevrpc_rpc_publish_call_terminals_locked(runtime, failed_call, &info);
            }
            pthread_mutex_unlock(&runtime->mutex);
        }
    }
    free(streams);
    return 0;
}

static int trevrpc_rpc_cancellation_release_impl(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_cancellation_v1 cancellation) {
    trevrpc_rpc_cancellation_record** cursor;
    trevrpc_rpc_cancellation_record* record;
    uint64_t key[6];
    uint8_t count;
    if (runtime == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&runtime->mutex);
    record = trevrpc_rpc_find_cancellation_locked(runtime, cancellation);
    if (record == NULL) {
        pthread_mutex_unlock(&runtime->mutex);
        return -ESTALE;
    }
    if (record->attached_calls != 0) {
        pthread_mutex_unlock(&runtime->mutex);
        return -EBUSY;
    }
    cursor = &runtime->cancellations;
    while (*cursor != record) {
        cursor = &(*cursor)->next;
    }
    *cursor = record->next;
    trevrpc_rpc_handle_key(key, &count, cancellation.owner, cancellation.slot, cancellation.generation);
    (void)trevrpc_rpc_registry_remove(&runtime->cancellation_registry, key, count);
    pthread_mutex_unlock(&runtime->mutex);
    free(record);
    return 0;
}

static int trevrpc_rpc_runtime_close_impl(trevrpc_rpc_runtime* runtime, uint64_t operation_id) {
    trevrpc_rpc_call_record* call;
    trevrpc_rpc_event* event;
    int result;
    if (runtime == NULL || operation_id == 0) {
        return -EINVAL;
    }
    pthread_mutex_lock(&runtime->mutex);
    if (runtime->state != TREVRPC_RPC_STATE_RUNNING || runtime->close_event == NULL) {
        pthread_mutex_unlock(&runtime->mutex);
        return -EALREADY;
    }
    event = runtime->close_event;
    runtime->state = TREVRPC_RPC_STATE_STOPPING;
    runtime->close_operation_id = operation_id;
    for (call = runtime->calls; call != NULL; call = call->next) {
        call->runtime_close_target = !call->stream_terminal_committed;
    }
    pthread_cond_broadcast(&runtime->condition);
    pthread_mutex_unlock(&runtime->mutex);
    result = trevrpc_rpc_transport_close(runtime->transport);
    pthread_mutex_lock(&runtime->mutex);
    if (result != 0 && runtime->state == TREVRPC_RPC_STATE_STOPPING && runtime->close_event == event) {
        runtime->state = TREVRPC_RPC_STATE_RUNNING;
        runtime->close_operation_id = 0;
    }
    for (call = runtime->calls; call != NULL; call = call->next) {
        if (call->runtime_close_target) {
            if (result == 0) {
                call->cancelled = true;
            }
            call->runtime_close_target = false;
        }
    }
    pthread_mutex_unlock(&runtime->mutex);
    return result;
}

static int trevrpc_rpc_runtime_drain_impl(trevrpc_rpc_runtime* runtime) {
    if (runtime == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&runtime->mutex);
    if (runtime->state != TREVRPC_RPC_STATE_STOPPED) {
        pthread_mutex_unlock(&runtime->mutex);
        return -EBUSY;
    }
    while (!runtime->driver_stopped || runtime->event_head != NULL) {
        pthread_cond_wait(&runtime->condition, &runtime->mutex);
    }
    trevrpc_rpc_retry_retired_handles_locked(runtime);
    pthread_mutex_unlock(&runtime->mutex);
    return 0;
}

#define TREVRPC_RPC_API_FORWARD(runtime_value, call_expression)                                                        \
    do {                                                                                                               \
        int api_result = trevrpc_rpc_api_enter(runtime_value);                                                         \
        if (api_result != 0) {                                                                                         \
            return api_result;                                                                                         \
        }                                                                                                              \
        api_result = (call_expression);                                                                                \
        trevrpc_rpc_api_leave(runtime_value);                                                                          \
        return api_result;                                                                                             \
    } while (0)

int trevrpc_rpc_runtime_get_wake_source_v1(trevrpc_rpc_runtime* runtime, trevrpc_rpc_wake_source_v1* wake_source) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_runtime_get_wake_source_v1_impl(runtime, wake_source));
}

int trevrpc_rpc_runtime_next_event(trevrpc_rpc_runtime* runtime, trevrpc_rpc_event** out_event) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_runtime_next_event_impl(runtime, out_event));
}

int trevrpc_rpc_runtime_get_diagnostics_v1(trevrpc_rpc_runtime* runtime, trevrpc_rpc_diagnostics_v1* diagnostics) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_runtime_get_diagnostics_v1_impl(runtime, diagnostics));
}

int trevrpc_rpc_call_get_context_v1(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call, trevrpc_rpc_call_context_info_v1* info) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_call_get_context_v1_impl(runtime, call, info));
}

int trevrpc_rpc_runtime_start_transport_endpoint_v1(trevrpc_rpc_runtime* runtime,
    const trevrpc_rpc_transport_endpoint_config* config,
    uint32_t mode,
    uint64_t operation_id,
    trevrpc_rpc_endpoint_v1* out_endpoint) {
    TREVRPC_RPC_API_FORWARD(runtime,
        trevrpc_rpc_runtime_start_transport_endpoint_v1_impl(runtime, config, mode, operation_id, out_endpoint));
}

int trevrpc_rpc_endpoint_get_port_v1(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_endpoint_v1 endpoint, uint16_t* out_port) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_endpoint_get_port_v1_impl(runtime, endpoint, out_port));
}

int trevrpc_rpc_call_open_v1(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_endpoint_v1 endpoint,
    const trevrpc_rpc_call_config_v1* config,
    uint64_t operation_id,
    trevrpc_rpc_call_v1* out_call,
    trevrpc_rpc_stream_v1* out_stream) {
    TREVRPC_RPC_API_FORWARD(
        runtime, trevrpc_rpc_call_open_v1_impl(runtime, endpoint, config, operation_id, out_call, out_stream));
}

int trevrpc_rpc_call_accept(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call, uint64_t operation_id) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_call_accept_impl(runtime, call, operation_id));
}

int trevrpc_rpc_stream_send_copy_v1(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_stream_v1 stream,
    uint64_t operation_id,
    const uint8_t* message,
    size_t message_len,
    uint32_t flags) {
    TREVRPC_RPC_API_FORWARD(
        runtime, trevrpc_rpc_stream_send_copy_v1_impl(runtime, stream, operation_id, message, message_len, flags));
}

int trevrpc_rpc_stream_receive(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream, trevrpc_rpc_receive** out_receive) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_stream_receive_impl(runtime, stream, out_receive));
}

int trevrpc_rpc_stream_finish_send(trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream, uint64_t operation_id) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_stream_finish_send_impl(runtime, stream, operation_id));
}

int trevrpc_rpc_call_respond_copy_v1(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_call_v1 call,
    uint64_t operation_id,
    const trevrpc_rpc_status_v1* status,
    const uint8_t* message,
    size_t message_len) {
    TREVRPC_RPC_API_FORWARD(
        runtime, trevrpc_rpc_call_respond_copy_v1_impl(runtime, call, operation_id, status, message, message_len));
}

int trevrpc_rpc_call_finish_v1(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_call_v1 call,
    uint64_t operation_id,
    const trevrpc_rpc_status_v1* status) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_call_finish_v1_impl(runtime, call, operation_id, status));
}

int trevrpc_rpc_call_cancel(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call, uint64_t operation_id, uint64_t application_error_code) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_call_cancel_impl(runtime, call, operation_id, application_error_code));
}

int trevrpc_rpc_stream_close(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_stream_v1 stream,
    uint64_t operation_id,
    uint32_t flags,
    uint64_t application_error_code) {
    TREVRPC_RPC_API_FORWARD(
        runtime, trevrpc_rpc_stream_close_impl(runtime, stream, operation_id, flags, application_error_code));
}

int trevrpc_rpc_call_close(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_call_v1 call,
    uint64_t operation_id,
    uint32_t flags,
    uint64_t application_error_code) {
    TREVRPC_RPC_API_FORWARD(
        runtime, trevrpc_rpc_call_close_impl(runtime, call, operation_id, flags, application_error_code));
}

int trevrpc_rpc_endpoint_close(trevrpc_rpc_runtime* runtime, trevrpc_rpc_endpoint_v1 endpoint, uint64_t operation_id) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_endpoint_close_impl(runtime, endpoint, operation_id));
}

int trevrpc_rpc_stream_release(trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_stream_release_impl(runtime, stream));
}

int trevrpc_rpc_call_release(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_call_release_impl(runtime, call));
}

int trevrpc_rpc_endpoint_release(trevrpc_rpc_runtime* runtime, trevrpc_rpc_endpoint_v1 endpoint) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_endpoint_release_impl(runtime, endpoint));
}

int trevrpc_rpc_cancellation_create(trevrpc_rpc_runtime* runtime, trevrpc_rpc_cancellation_v1* out_cancellation) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_cancellation_create_impl(runtime, out_cancellation));
}

int trevrpc_rpc_cancellation_cancel(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_cancellation_v1 cancellation, uint64_t operation_id) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_cancellation_cancel_impl(runtime, cancellation, operation_id));
}

int trevrpc_rpc_cancellation_release(trevrpc_rpc_runtime* runtime, trevrpc_rpc_cancellation_v1 cancellation) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_cancellation_release_impl(runtime, cancellation));
}

int trevrpc_rpc_runtime_close(trevrpc_rpc_runtime* runtime, uint64_t operation_id) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_runtime_close_impl(runtime, operation_id));
}

int trevrpc_rpc_runtime_drain(trevrpc_rpc_runtime* runtime) {
    TREVRPC_RPC_API_FORWARD(runtime, trevrpc_rpc_runtime_drain_impl(runtime));
}

#undef TREVRPC_RPC_API_FORWARD

static void trevrpc_rpc_runtime_destroy(trevrpc_rpc_runtime* runtime) {
    trevrpc_rpc_endpoint_record* endpoint;
    trevrpc_rpc_transport_connection_record* transport_connection;
    trevrpc_rpc_call_record* call;
    trevrpc_rpc_cancellation_record* cancellation;
    trevrpc_rpc_operation* operation;
    while ((operation = runtime->operations) != NULL) {
        runtime->operations = operation->next;
        trevrpc_rpc_event_destroy(operation->event);
        free(operation->continuation_frame);
        free(operation);
    }
    while ((endpoint = runtime->endpoints) != NULL) {
        runtime->endpoints = endpoint->next;
        if (!endpoint->transport_released) {
            (void)trevrpc_rpc_transport_release_handle(
                runtime->transport, endpoint->transport_endpoint, endpoint->transport_kind);
        }
        trevrpc_rpc_event_destroy(endpoint->terminal_event);
        free(endpoint);
    }
    while ((transport_connection = runtime->transport_connections) != NULL) {
        runtime->transport_connections = transport_connection->next;
        (void)trevrpc_rpc_transport_release_handle(
            runtime->transport, transport_connection->transport_connection, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION);
        free(transport_connection);
    }
    while (runtime->retired_handles != NULL) {
        trevrpc_rpc_retired_handle* retired = runtime->retired_handles;
        runtime->retired_handles = retired->next;
        (void)trevrpc_rpc_transport_release_handle(runtime->transport, retired->handle, retired->kind);
        retired->kind = 0;
    }
    free(runtime->retired_pool);
    runtime->retired_pool = NULL;
    while ((call = runtime->calls) != NULL) {
        runtime->calls = call->next;
        trevrpc_rpc_drop_pending_receive_locked(runtime, call);
        if (!call->transport_stream_released) {
            (void)trevrpc_rpc_transport_release_handle(
                runtime->transport, call->transport_stream, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM);
        }
        trevrpc_rpc_event_destroy(call->stream_terminal_event);
        trevrpc_rpc_event_destroy(call->call_terminal_event);
        trevrpc_rpc_event_destroy(call->deferred_receive_fin);
        free(call->request_frame);
        free(call);
    }
    while ((cancellation = runtime->cancellations) != NULL) {
        runtime->cancellations = cancellation->next;
        free(cancellation);
    }
    trevrpc_rpc_event_destroy(runtime->close_event);
    (void)trevrpc_rpc_transport_destroy(runtime->transport);
    close(runtime->wake_read_fd);
    close(runtime->wake_write_fd);
    close(runtime->driver_wake_read_fd);
    close(runtime->driver_wake_write_fd);
    pthread_cond_destroy(&runtime->condition);
    pthread_mutex_destroy(&runtime->mutex);
    pthread_cond_destroy(&runtime->lifetime_condition);
    pthread_mutex_destroy(&runtime->lifetime_mutex);
    free(runtime->deadline_heap);
    trevrpc_rpc_registry_destroy(&runtime->call_registry);
    trevrpc_rpc_registry_destroy(&runtime->endpoint_registry);
    trevrpc_rpc_registry_destroy(&runtime->transport_registry);
    trevrpc_rpc_registry_destroy(&runtime->cancellation_registry);
    trevrpc_rpc_registry_destroy(&runtime->operation_transport_registry);
    trevrpc_rpc_registry_destroy(&runtime->operation_scope_registry);
    free(runtime);
}

static void trevrpc_rpc_runtime_unref(trevrpc_rpc_runtime* runtime) {
    if (atomic_fetch_sub_explicit(&runtime->references, 1, memory_order_acq_rel) == 1) {
        trevrpc_rpc_runtime_destroy(runtime);
    }
}

int trevrpc_rpc_runtime_release(trevrpc_rpc_runtime* runtime) {
    uint_fast64_t gate;
    if (runtime == NULL) {
        return -EINVAL;
    }
    gate = atomic_fetch_or_explicit(&runtime->api_gate, TREVRPC_RPC_API_GATE_CLOSED, memory_order_acq_rel);
    if ((gate & TREVRPC_RPC_API_GATE_CLOSED) != 0) {
        return -EPIPE;
    }
    pthread_mutex_lock(&runtime->mutex);
    if (runtime->state != TREVRPC_RPC_STATE_STOPPED || !runtime->driver_stopped || runtime->event_head != NULL) {
        pthread_mutex_unlock(&runtime->mutex);
        atomic_fetch_and_explicit(&runtime->api_gate, ~TREVRPC_RPC_API_GATE_CLOSED, memory_order_release);
        return -EBUSY;
    }
    if (runtime->public_released) {
        pthread_mutex_unlock(&runtime->mutex);
        atomic_fetch_and_explicit(&runtime->api_gate, ~TREVRPC_RPC_API_GATE_CLOSED, memory_order_release);
        return -EALREADY;
    }
    runtime->state = TREVRPC_RPC_STATE_RELEASING;
    runtime->public_released = true;
    pthread_mutex_unlock(&runtime->mutex);
    pthread_mutex_lock(&runtime->lifetime_mutex);
    while ((atomic_load_explicit(&runtime->api_gate, memory_order_acquire) >> 1u) != 0) {
        pthread_cond_wait(&runtime->lifetime_condition, &runtime->lifetime_mutex);
    }
    pthread_mutex_unlock(&runtime->lifetime_mutex);
    if (runtime->driver_started) {
        pthread_join(runtime->driver_thread, NULL);
        runtime->driver_started = false;
    }
    trevrpc_rpc_runtime_unref(runtime);
    return 0;
}
