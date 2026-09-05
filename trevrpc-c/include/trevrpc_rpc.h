#ifndef TREVRPC_RPC_H
#define TREVRPC_RPC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define TREVRPC_RPC_ABI_VERSION 1u
#define TREVRPC_RPC_STRUCT_VERSION_1 1u

#define TREVRPC_RPC_DEFAULT_EVENT_CAPACITY 256u
#define TREVRPC_RPC_DEFAULT_ENDPOINT_CAPACITY 16u
#define TREVRPC_RPC_DEFAULT_CALL_CAPACITY 4096u
#define TREVRPC_RPC_DEFAULT_STREAM_CAPACITY 4096u
#define TREVRPC_RPC_DEFAULT_RECEIVE_OWNED_COUNT 4096u
#define TREVRPC_RPC_DEFAULT_RECEIVE_OWNED_BYTES (64ull * 1024ull * 1024ull)
#define TREVRPC_RPC_DEFAULT_MAX_MESSAGE_SIZE (4ull * 1024ull * 1024ull)
#define TREVRPC_RPC_DEFAULT_MAX_METADATA_COUNT 128u
#define TREVRPC_RPC_DEFAULT_MAX_METADATA_BYTES (64ull * 1024ull)
#define TREVRPC_RPC_DEFAULT_MAX_STATUS_MESSAGE_SIZE (16u * 1024u)
#define TREVRPC_RPC_MAX_EVENT_CAPACITY 65536u
#define TREVRPC_RPC_DEADLINE_INFINITE UINT64_MAX
#define TREVRPC_RPC_OPERATION_ID_NONE 0ull

#define TREVRPC_RPC_STATE_RUNNING 0u
#define TREVRPC_RPC_STATE_STOPPING 1u
#define TREVRPC_RPC_STATE_STOPPED 2u
#define TREVRPC_RPC_STATE_RELEASING 3u

#define TREVRPC_RPC_WAKE_SOURCE_POSIX_FD 1u
#define TREVRPC_RPC_WAKE_FLAG_BORROWED 0x00000001u
#define TREVRPC_RPC_WAKE_FLAG_LEVEL_TRIGGERED 0x00000002u

#define TREVRPC_RPC_OBJECT_NONE 0u
#define TREVRPC_RPC_OBJECT_ENDPOINT 1u
#define TREVRPC_RPC_OBJECT_CALL 2u
#define TREVRPC_RPC_OBJECT_STREAM 3u
#define TREVRPC_RPC_OBJECT_CANCELLATION 4u

#define TREVRPC_RPC_ENDPOINT_CLIENT 1u
#define TREVRPC_RPC_ENDPOINT_SERVER 2u

#define TREVRPC_RPC_KIND_UNARY 0u
#define TREVRPC_RPC_KIND_CLIENT_STREAMING 1u
#define TREVRPC_RPC_KIND_SERVER_STREAMING 2u
#define TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING 3u

#define TREVRPC_RPC_EVENT_DIAGNOSTIC 1u
#define TREVRPC_RPC_EVENT_STOPPED 2u
#define TREVRPC_RPC_EVENT_ENDPOINT_READY 3u
#define TREVRPC_RPC_EVENT_ENDPOINT_FAILED 4u
#define TREVRPC_RPC_EVENT_ENDPOINT_CLOSED 5u
#define TREVRPC_RPC_EVENT_CALL_INCOMING 6u
#define TREVRPC_RPC_EVENT_CALL_READY 7u
#define TREVRPC_RPC_EVENT_CALL_FAILED 8u
#define TREVRPC_RPC_EVENT_CALL_ACCEPTED 9u
#define TREVRPC_RPC_EVENT_STREAM_READABLE 10u
#define TREVRPC_RPC_EVENT_SEND_COMPLETE 11u
#define TREVRPC_RPC_EVENT_SEND_FINISHED 12u
#define TREVRPC_RPC_EVENT_CALL_FINISHED 13u
#define TREVRPC_RPC_EVENT_CALL_CLOSED 14u
#define TREVRPC_RPC_EVENT_CANCELLED 15u
#define TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN 16u
#define TREVRPC_RPC_EVENT_STREAM_CLOSED 17u

#define TREVRPC_RPC_EVENT_FLAG_FATAL 0x00000001u
#define TREVRPC_RPC_EVENT_FLAG_TERMINAL 0x00000002u
#define TREVRPC_RPC_EVENT_FLAG_CLIENT 0x00000004u
#define TREVRPC_RPC_EVENT_FLAG_SERVER 0x00000008u
#define TREVRPC_RPC_EVENT_FLAG_LOCAL 0x00000010u
#define TREVRPC_RPC_EVENT_FLAG_PEER 0x00000020u
#define TREVRPC_RPC_EVENT_FLAG_CLEAN_FIN 0x00000040u
#define TREVRPC_RPC_EVENT_FLAG_PEER_CANCELLED 0x00000080u
#define TREVRPC_RPC_EVENT_FLAG_TRANSPORT_ERROR 0x00000100u
#define TREVRPC_RPC_EVENT_FLAG_PEER_RESET 0x00000200u
#define TREVRPC_RPC_EVENT_FLAG_HAS_RECEIVE 0x00000400u
#define TREVRPC_RPC_EVENT_FLAG_HAS_INCOMING_CALL 0x00000800u

#define TREVRPC_RPC_RECEIVE_INITIAL_MESSAGE 1u
#define TREVRPC_RPC_RECEIVE_MESSAGE 2u
#define TREVRPC_RPC_RECEIVE_STATUS 3u
#define TREVRPC_RPC_RECEIVE_METADATA 4u

#define TREVRPC_RPC_STATUS_OK 0u
#define TREVRPC_RPC_STATUS_CANCELLED 1u
#define TREVRPC_RPC_STATUS_UNKNOWN 2u
#define TREVRPC_RPC_STATUS_INVALID_ARGUMENT 3u
#define TREVRPC_RPC_STATUS_DEADLINE_EXCEEDED 4u
#define TREVRPC_RPC_STATUS_NOT_FOUND 5u
#define TREVRPC_RPC_STATUS_ALREADY_EXISTS 6u
#define TREVRPC_RPC_STATUS_PERMISSION_DENIED 7u
#define TREVRPC_RPC_STATUS_RESOURCE_EXHAUSTED 8u
#define TREVRPC_RPC_STATUS_FAILED_PRECONDITION 9u
#define TREVRPC_RPC_STATUS_ABORTED 10u
#define TREVRPC_RPC_STATUS_OUT_OF_RANGE 11u
#define TREVRPC_RPC_STATUS_UNIMPLEMENTED 12u
#define TREVRPC_RPC_STATUS_INTERNAL 13u
#define TREVRPC_RPC_STATUS_UNAVAILABLE 14u
#define TREVRPC_RPC_STATUS_DATA_LOSS 15u
#define TREVRPC_RPC_STATUS_UNAUTHENTICATED 16u

#define TREVRPC_RPC_SEND_FLAG_NONE 0u

#define TREVRPC_RPC_CLOSE_FLAG_NONE 0u
#define TREVRPC_RPC_CLOSE_FLAG_ABORT 0x00000001u

typedef struct trevrpc_rpc_runtime trevrpc_rpc_runtime;
typedef struct trevrpc_rpc_event trevrpc_rpc_event;
typedef struct trevrpc_rpc_receive trevrpc_rpc_receive;

typedef struct trevrpc_rpc_endpoint_v1 {
    uint64_t owner;
    uint32_t slot;
    uint32_t generation;
} trevrpc_rpc_endpoint_v1;

typedef struct trevrpc_rpc_call_v1 {
    uint64_t owner;
    uint32_t slot;
    uint32_t generation;
} trevrpc_rpc_call_v1;

typedef struct trevrpc_rpc_stream_v1 {
    uint64_t owner;
    uint32_t slot;
    uint32_t generation;
} trevrpc_rpc_stream_v1;

typedef struct trevrpc_rpc_cancellation_v1 {
    uint64_t owner;
    uint32_t slot;
    uint32_t generation;
} trevrpc_rpc_cancellation_v1;

typedef struct trevrpc_rpc_runtime_config_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t event_capacity;
    uint32_t endpoint_capacity;
    uint32_t call_capacity;
    uint32_t stream_capacity;
    uint32_t max_receive_owned_count;
    uint32_t flags;
    uint32_t max_metadata_count;
    uint32_t max_status_message_size;
    uint64_t max_receive_owned_bytes;
    uint64_t max_message_size;
    uint64_t max_metadata_bytes;
    uint64_t reserved[4];
} trevrpc_rpc_runtime_config_v1;

typedef struct trevrpc_rpc_wake_source_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t kind;
    uint32_t flags;
    intptr_t native_handle;
    uint64_t reserved[3];
} trevrpc_rpc_wake_source_v1;

typedef struct trevrpc_rpc_metadata_entry_v1 {
    const char* key;
    uint32_t key_len;
    uint32_t reserved0;
    const uint8_t* value;
    uint64_t value_len;
} trevrpc_rpc_metadata_entry_v1;

typedef struct trevrpc_rpc_call_config_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t kind;
    uint32_t flags;
    const char* service;
    uint32_t service_len;
    uint32_t reserved0;
    const char* method;
    uint32_t method_len;
    uint32_t metadata_count;
    const trevrpc_rpc_metadata_entry_v1* metadata;
    /* Relative call lifetime; zero or TREVRPC_RPC_DEADLINE_INFINITE disables the deadline. */
    uint64_t timeout_nanos;
    trevrpc_rpc_cancellation_v1 cancellation;
    const uint8_t* initial_message;
    uint64_t initial_message_len;
    uint64_t reserved[3];
} trevrpc_rpc_call_config_v1;

typedef struct trevrpc_rpc_status_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t code;
    uint32_t flags;
    const char* message;
    uint32_t message_len;
    uint32_t metadata_count;
    const trevrpc_rpc_metadata_entry_v1* metadata;
    uint64_t reserved[4];
} trevrpc_rpc_status_v1;

typedef struct trevrpc_rpc_event_info_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
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
    const char* service;
    uint32_t service_len;
    uint32_t reserved0;
    const char* method;
    uint32_t method_len;
    uint32_t reserved1;
    uint64_t application_error_code;
    uint64_t provider_error_code;
    uint64_t reserved[4];
} trevrpc_rpc_event_info_v1;

typedef struct trevrpc_rpc_receive_info_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t kind;
    uint32_t flags;
    uint32_t rpc_status;
    uint32_t metadata_count;
    const uint8_t* data;
    uint64_t data_len;
    const char* message;
    uint32_t message_len;
    uint32_t reserved0;
    const trevrpc_rpc_metadata_entry_v1* metadata;
    uint64_t reserved[4];
} trevrpc_rpc_receive_info_v1;

typedef struct trevrpc_rpc_diagnostics_v1 {
    uint32_t struct_size;
    uint32_t struct_version;
    uint32_t rpc_abi_version;
    uint32_t state;
    int32_t terminal_status;
    uint32_t event_capacity;
    uint32_t queue_depth;
    uint32_t ordinary_queue_depth;
    uint64_t events_enqueued;
    uint64_t events_dequeued;
    uint64_t events_rejected;
    uint64_t receive_owned_count;
    uint64_t peak_receive_owned_count;
    uint64_t receive_owned_bytes;
    uint64_t peak_receive_owned_bytes;
    uint64_t pending_send_bytes;
    uint64_t pending_send_count;
    uint64_t live_endpoints;
    uint64_t live_calls;
    uint64_t live_streams;
    uint64_t active_api_calls;
    uint64_t active_callbacks;
    uint64_t wake_signals;
    uint64_t wake_write_eagain;
    uint64_t wake_failures;
    uint64_t provider_error_code;
    uint64_t mandatory_reservations;
    uint64_t reserved[3];
} trevrpc_rpc_diagnostics_v1;

uint32_t trevrpc_rpc_abi_version(void);
void trevrpc_rpc_abi_1_anchor(void);

int trevrpc_rpc_runtime_config_v1_init(trevrpc_rpc_runtime_config_v1* config, size_t struct_size);
int trevrpc_rpc_wake_source_v1_init(trevrpc_rpc_wake_source_v1* wake_source, size_t struct_size);
int trevrpc_rpc_call_config_v1_init(trevrpc_rpc_call_config_v1* config, size_t struct_size);
int trevrpc_rpc_status_v1_init(trevrpc_rpc_status_v1* status, size_t struct_size);
int trevrpc_rpc_event_info_v1_init(trevrpc_rpc_event_info_v1* info, size_t struct_size);
int trevrpc_rpc_receive_info_v1_init(trevrpc_rpc_receive_info_v1* info, size_t struct_size);
int trevrpc_rpc_diagnostics_v1_init(trevrpc_rpc_diagnostics_v1* diagnostics, size_t struct_size);

/*
 * Commands carrying an operation_id require a nonzero value. A synchronous
 * error rejects the operation and produces no completion. A zero return
 * admits the operation and guarantees exactly one event with the same ID.
 * Operation IDs remain unavailable for reuse on the same object until that
 * completion is committed to the event queue.
 *
 * All pointer-bearing command inputs are copied before an admitted command
 * returns. Event-info pointers are borrowed until event release. Receive-info
 * pointers are immutable and borrowed until receive release. Detached events
 * and receives remain valid after their originating object and runtime are
 * released. Event and receive release are NULL-safe, but release must not race
 * another operation on the same detached pointer. A successful incoming-call
 * take transfers the call, stream, and initial receive atomically; a second take
 * returns -EALREADY. Releasing an untaken incoming event releases its provisional
 * ownership. Output objects and handles are unchanged on synchronous failure.
 *
 * A CALL_INCOMING event owns its provisional call, stream, and initial receive.
 * event_take_incoming_call transfers all three atomically. Releasing the event
 * without taking them rejects the call. A taken call must be explicitly
 * accepted, closed, and released. Response-side STATUS receives and non-OK
 * request-side STATUS receives are delivered by stream_receive before terminal
 * events. On incoming client-streaming and bidirectional calls, request-side
 * STATUS OK is consumed as a graceful end-of-input marker; the later
 * STREAM_RECEIVE_FIN event reports transport completion. Terminal events do not
 * attach a receive.
 *
 * All-zero typed handles are the only null handle representation. A null
 * cancellation handle means no cancellation source. Admitted calls retain an
 * attached cancellation source until their terminal event is committed.
 *
 * The wake source is borrowed, level-triggered, and owned by the runtime.
 * Consumers drain events until -EAGAIN before waiting again. The first
 * runtime_close admission owns the sole STOPPED completion; later close calls
 * return -EALREADY and produce no event. Typed release drops caller ownership
 * only after the matching terminal event has been dequeued; it never initiates
 * close and returns -EBUSY while the object is still live.
 */
int trevrpc_rpc_runtime_get_wake_source_v1(trevrpc_rpc_runtime* runtime, trevrpc_rpc_wake_source_v1* wake_source);
int trevrpc_rpc_runtime_next_event(trevrpc_rpc_runtime* runtime, trevrpc_rpc_event** out_event);
int trevrpc_rpc_event_get_info_v1(const trevrpc_rpc_event* event, trevrpc_rpc_event_info_v1* info);
int trevrpc_rpc_event_take_incoming_call(trevrpc_rpc_event* event,
    trevrpc_rpc_call_v1* out_call,
    trevrpc_rpc_stream_v1* out_stream,
    trevrpc_rpc_receive** out_initial_message);
void trevrpc_rpc_event_release(trevrpc_rpc_event* event);
int trevrpc_rpc_receive_get_info_v1(const trevrpc_rpc_receive* receive, trevrpc_rpc_receive_info_v1* info);
void trevrpc_rpc_receive_release(trevrpc_rpc_receive* receive);
int trevrpc_rpc_runtime_get_diagnostics_v1(trevrpc_rpc_runtime* runtime, trevrpc_rpc_diagnostics_v1* diagnostics);

int trevrpc_rpc_endpoint_get_port_v1(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_endpoint_v1 endpoint, uint16_t* out_port);
int trevrpc_rpc_call_open_v1(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_endpoint_v1 endpoint,
    const trevrpc_rpc_call_config_v1* config,
    uint64_t operation_id,
    trevrpc_rpc_call_v1* out_call,
    trevrpc_rpc_stream_v1* out_stream);
int trevrpc_rpc_call_accept(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call, uint64_t operation_id);
int trevrpc_rpc_stream_send_copy_v1(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_stream_v1 stream,
    uint64_t operation_id,
    const uint8_t* message,
    size_t message_len,
    uint32_t flags);
int trevrpc_rpc_stream_receive(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream, trevrpc_rpc_receive** out_receive);
int trevrpc_rpc_stream_finish_send(trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream, uint64_t operation_id);
/* OK requires a message pointer (including an encoded zero-byte message); non-OK requires NULL. */
int trevrpc_rpc_call_respond_copy_v1(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_call_v1 call,
    uint64_t operation_id,
    const trevrpc_rpc_status_v1* status,
    const uint8_t* message,
    size_t message_len);
int trevrpc_rpc_call_finish_v1(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call, uint64_t operation_id, const trevrpc_rpc_status_v1* status);
int trevrpc_rpc_call_cancel(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call, uint64_t operation_id, uint64_t application_error_code);
int trevrpc_rpc_stream_close(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_stream_v1 stream,
    uint64_t operation_id,
    uint32_t flags,
    uint64_t application_error_code);
int trevrpc_rpc_call_close(trevrpc_rpc_runtime* runtime,
    trevrpc_rpc_call_v1 call,
    uint64_t operation_id,
    uint32_t flags,
    uint64_t application_error_code);
int trevrpc_rpc_endpoint_close(trevrpc_rpc_runtime* runtime, trevrpc_rpc_endpoint_v1 endpoint, uint64_t operation_id);
int trevrpc_rpc_stream_release(trevrpc_rpc_runtime* runtime, trevrpc_rpc_stream_v1 stream);
int trevrpc_rpc_call_release(trevrpc_rpc_runtime* runtime, trevrpc_rpc_call_v1 call);
int trevrpc_rpc_endpoint_release(trevrpc_rpc_runtime* runtime, trevrpc_rpc_endpoint_v1 endpoint);

int trevrpc_rpc_cancellation_create(trevrpc_rpc_runtime* runtime, trevrpc_rpc_cancellation_v1* out_cancellation);
int trevrpc_rpc_cancellation_cancel(
    trevrpc_rpc_runtime* runtime, trevrpc_rpc_cancellation_v1 cancellation, uint64_t operation_id);
int trevrpc_rpc_cancellation_release(trevrpc_rpc_runtime* runtime, trevrpc_rpc_cancellation_v1 cancellation);

int trevrpc_rpc_runtime_close(trevrpc_rpc_runtime* runtime, uint64_t operation_id);
/* Requires the STOPPED event to have been dequeued; otherwise returns -EBUSY. */
int trevrpc_rpc_runtime_drain(trevrpc_rpc_runtime* runtime);
/*
 * Final release closes API admission and waits for already-admitted calls.
 * Callers must not attempt raw-pointer API entry after release returns.
 */
int trevrpc_rpc_runtime_release(trevrpc_rpc_runtime* runtime);

#ifdef __cplusplus
}
#endif

#endif
