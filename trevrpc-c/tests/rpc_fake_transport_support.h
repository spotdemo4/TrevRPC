#ifndef TREVRPC_RPC_FAKE_TRANSPORT_SUPPORT_H
#define TREVRPC_RPC_FAKE_TRANSPORT_SUPPORT_H

#include "trevrpc_rpc_transport_internal.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <pthread.h>

#define FAKE_EVENT_CAPACITY 32u

typedef struct fake_transport fake_transport;

typedef struct fake_event fake_event;
typedef struct fake_receive fake_receive;

struct fake_transport {
    trevrpc_rpc_transport base;
    pthread_mutex_t mutex;
    int wake_read_fd;
    int wake_write_fd;
    int alternate_wake_read_fd;
    int alternate_wake_write_fd;
    bool signal_alternate_wake;
    fake_event* events[FAKE_EVENT_CAPACITY];
    size_t event_head;
    size_t event_tail;
    size_t event_count;
    fake_receive* stream_receive_head;
    fake_receive* stream_receive_tail;
    bool listener_closed;
    bool connection_closed;
    bool stream_closed;
    bool close_requested;
    bool close_blocked;
    bool close_entered;
    bool close_release;
    bool diagnostics_blocked;
    bool diagnostics_entered;
    bool diagnostics_release;
    bool dial_blocked;
    bool dial_entered;
    bool dial_release;
    bool next_event_blocked;
    bool next_event_entered;
    bool next_event_release;
    bool admission_info_blocked;
    bool admission_info_entered;
    bool admission_info_release;
    bool listener_close_blocked;
    bool listener_close_entered;
    bool listener_close_release;
    bool receive_blocked;
    bool receive_entered;
    bool receive_release;
    pthread_mutex_t next_event_probe_mutex;
    pthread_cond_t next_event_probe_condition;
    bool next_event_attempted;
    int wake_sources_result;
    uint32_t wake_source_kind;
    atomic_int next_event_result;
    atomic_int close_result;
    atomic_int close_status;
    atomic_int poll_timeout_ms;
    atomic_uint_fast64_t poll_deadline_nanos;
    atomic_uint poll_timeout_fires;
    atomic_int stream_receive_result;
    atomic_int event_get_info_result;
    atomic_int admission_get_info_result;
    atomic_int admission_respond_result;
    atomic_int receive_get_info_result;
    atomic_int stream_send_result;
    int stream_abort_result;
    pthread_cond_t condition;
    atomic_uint readable_info_calls;
    atomic_uint readable_release_calls;
    atomic_uint receive_fin_release_calls;
    atomic_uint receive_release_calls;
    atomic_uint stream_terminal_release_calls;
    atomic_uint stream_abort_receive_calls;
    atomic_uint stream_abort_send_calls;
    atomic_uint stream_abort_calls;
    atomic_uint stream_close_calls;
    atomic_uint stream_open_calls;
    atomic_uint stream_send_calls;
    atomic_uint stream_finish_send_calls;
    atomic_uint admission_response_calls;
    atomic_uint admission_undecided_release_calls;
    atomic_uint admission_last_status;
    atomic_uint release_handle_calls;
    atomic_int release_handle_result;
    atomic_uint_fast64_t last_abort_error;
    atomic_uint_fast64_t last_send_operation_id;
    uint8_t* last_send_body;
    size_t last_send_body_len;
};

extern const trevrpc_rpc_transport_handle fake_listener_handle;
extern const trevrpc_rpc_transport_handle fake_connection_handle;
extern const trevrpc_rpc_transport_handle fake_stream_handle;
extern const trevrpc_rpc_transport_handle fake_second_stream_handle;

fake_transport* fake_create(void);
void fake_destroy(trevrpc_rpc_transport* transport);
int fake_push_event(fake_transport* transport,
    uint32_t kind,
    uint32_t flags,
    trevrpc_rpc_transport_handle subject,
    trevrpc_rpc_transport_handle parent);
int fake_push_status_event(fake_transport* transport,
    uint32_t kind,
    uint32_t flags,
    trevrpc_rpc_transport_handle subject,
    trevrpc_rpc_transport_handle parent,
    int32_t status);
int fake_push_operation_event(fake_transport* transport,
    uint32_t kind,
    uint32_t flags,
    trevrpc_rpc_transport_handle subject,
    trevrpc_rpc_transport_handle parent,
    uint64_t operation_id);
int fake_push_operation_status_event(fake_transport* transport,
    uint32_t kind,
    uint32_t flags,
    trevrpc_rpc_transport_handle subject,
    trevrpc_rpc_transport_handle parent,
    uint64_t operation_id,
    int32_t status);
int fake_push_admission_event(fake_transport* transport,
    uint32_t kind,
    const uint8_t* path,
    size_t path_len,
    const uint8_t* authority,
    size_t authority_len,
    const uint8_t* origin,
    size_t origin_len);
int fake_push_receive(fake_transport* transport, const uint8_t* data, size_t data_len);
int fake_push_incoming_stream(fake_transport* transport, const uint8_t* data, size_t data_len);
size_t fake_close_count(const fake_transport* transport);
size_t fake_release_handle_count(const fake_transport* transport);

#endif
