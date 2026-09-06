#define _POSIX_C_SOURCE 200809L

#include "trevrpc_rpc_transport_h3_internal.h"

#include "trevrpc_h3_demux_internal.h"
#include "trevrpc_http3_frame_internal.h"
#include "trevrpc_http3_headers_internal.h"
#include "trevrpc_http3_settings_internal.h"
#include "trevrpc_msquic_internal.h"
#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
#include "trevrpc_msquic_objects_internal.h" // IWYU pragma: keep
#endif
#include "trevrpc_qpack_static_internal.h"
#include "trevrpc_quic_varint_internal.h"
#include "trevrpc_webtransport_capsule_internal.h"
#include "trevrpc_webtransport_profile_internal.h"

#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define H3_APP_INTERNAL_ERROR UINT64_C(0x102)
#define H3_APP_CLOSED_CRITICAL_STREAM UINT64_C(0x104)
#define H3_APP_FRAME_UNEXPECTED UINT64_C(0x105)
#define H3_APP_REQUEST_CANCELLED UINT64_C(0x10c)
#define H3_RPC_STATUS_CANCELLED UINT64_C(1)
#define H3_APP_QPACK_ENCODER_STREAM_ERROR UINT64_C(0x201)
#define H3_APP_QPACK_DECODER_STREAM_ERROR UINT64_C(0x202)
#define H3_QPACK_SET_CAPACITY_ZERO 0x20u
#define H3_DEFAULT_MAX_FRAME (UINT64_C(4) * 1024 * 1024)
#define H3_DEFAULT_MAX_FIELD (UINT64_C(64) * 1024)
#define H3_MAX_PARSE_BUFFER (UINT64_C(16) * 1024 * 1024)
#define H3_SETTINGS_QPACK_MAX_TABLE_CAPACITY UINT64_C(0x01)
#define H3_SETTINGS_MAX_FIELD_SECTION_SIZE UINT64_C(0x06)
#define H3_SETTINGS_QPACK_BLOCKED_STREAMS UINT64_C(0x07)
#define H3_WT_PROFILE_DRAFT_02 0x00000001u
#define H3_WT_PROFILE_DRAFT_07 0x00000002u
#define H3_WT_PROFILE_DRAFT_14 0x00000004u
#define H3_WT_PROFILE_DRAFT_15 0x00000008u
#define H3_WT_PROFILE_ALL_SUPPORTED 0x0000000fu

#define H3_PENDING_CONNECTED 0x00000001u
#define H3_PENDING_SHUTDOWN 0x00000002u
#define H3_PENDING_ACCEPT 0x00000004u
#define H3_PENDING_READABLE 0x00000008u
#define H3_PENDING_TERMINAL 0x00000010u
#define H3_PENDING_SEND_COMPLETE 0x00000020u
#define H3_PENDING_START 0x00000040u
#define H3_PENDING_OPEN_CONNECT 0x00000080u
#define H3_PENDING_ACCEPT_CONNECT 0x00000100u
#define H3_PENDING_REEMIT_READABLE 0x00000200u

typedef struct h3_entry h3_entry;
typedef struct h3_source h3_source;

struct trevrpc_rpc_transport_event {
    struct trevrpc_rpc_transport_event* next;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_admission_info admission;
    trevrpc_rpc_transport_header_field* admission_headers;
    uint8_t* admission_storage;
    h3_source* source;
    trevrpc_rpc_transport_handle admission_stream;
    uint32_t protocol;
    h3_entry* terminal_entry;
    bool admission_decided;
    bool mandatory;
};

struct trevrpc_rpc_transport_receive {
    struct trevrpc_rpc_transport_receive* next;
    trevrpc_rpc_transport_receive_info info;
    uint8_t data[];
};

typedef struct h3_endpoint_copy {
    char* host;
    char* server_name;
    char* cert_file;
    char* key_file;
    char* ca_cert_file;
    char* path;
    char* origin;
    uint8_t* cert_data;
    uint8_t* key_data;
    uint8_t* ca_cert_data;
    trevrpc_rpc_transport_endpoint_config value;
} h3_endpoint_copy;

static _Thread_local h3_entry* h3_processing_entry;

typedef struct h3_capsule_state {
    trevrpc_wt_capsule_parser parser;
    uint8_t close_reason[TREV_WT_CAPSULE_CLOSE_REASON_MAX];
} h3_capsule_state;

typedef struct h3_registry_slot {
    h3_entry* entry;
    uint32_t generation;
    uint32_t next_free;
    bool exhausted;
} h3_registry_slot;

typedef struct h3_stream_state {
    uint8_t* parse;
    size_t parse_len;
    size_t parse_cap;
    uint8_t* rpc;
    size_t rpc_len;
    size_t rpc_cap;
    h3_capsule_state* capsules;
    trevrpc_rpc_transport_receive* receive_head;
    trevrpc_rpc_transport_receive* receive_tail;
    trevrpc_h3_demux_stream classifier;
    size_t classifier_consumed;
    trevrpc_h3_demux_action action;
    bool classifier_initialized;
    bool classified;
    bool recv_fin;
    bool send_fin;
    bool recv_aborted;
    bool send_aborted;
    bool peer_reset;
    bool fin_reported;
    bool send_stop_reported;
    bool control;
    bool settings_received;
    bool headers_received;
    bool headers_sent;
    bool headers_sending;
    bool ready_reported;
    bool connect_control;
    bool connect_accept_pending;
    bool connect_accepting;
    bool capsule_finished;
    bool wt_wait_session;
    bool frame_unexpected;
    bool receive_blocked;
    bool event_blocked;
    bool admission_pending;
    bool admission_rejected;
    trevrpc_msquic_send_completion* pending_completion;
    uint64_t pending_operation_id;
    uint64_t local_abort_error;
    uint64_t peer_reset_error;
    size_t pending_send_bytes;
    uint64_t unresolved_deadline_nanos;
    uint64_t unresolved_abort_error;
    int unresolved_error;
    bool unresolved_counted;
    bool provider_bytes_selected;
#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
    bool ready_bypass;
#endif
} h3_stream_state;

struct h3_entry {
    h3_source* source;
    void* object;
    uint32_t kind;
    uint32_t slot;
    uint32_t generation;
    uint32_t pending;
    uint32_t side_flags;
    uint64_t operation_id;
    trevrpc_rpc_transport_handle parent;
    h3_endpoint_copy endpoint;
    h3_stream_state stream;
    bool live;
    bool connected;
    bool local_control_ready;
    bool peer_settings_received;
    bool peer_settings_ready;
    bool ready_reported;
    bool start_reported;
    bool close_reported;
    bool terminal_requested;
    bool terminal_pending;
    bool detached;
    h3_entry* retired_next;
    uint32_t child_refs;
    bool parent_ref_held;
    bool profile_resolved;
    bool connect_open_started;
    bool observer_installed;
    bool start_observer_installed;
    bool wt_session_ready;
    bool wt_draining;
    trevrpc_rpc_transport_handle resolving_connect;
    uint64_t connect_stream_id;
    uint32_t unresolved_stream_count;
    trevrpc_msquic_feature_snapshot capabilities;
    /* Mandatory event nodes are owned by the entry from admission until they
     * are queued.  This makes completion and terminal publication allocation
     * free. */
    trevrpc_rpc_transport_event* ready_event;
    trevrpc_rpc_transport_event* terminal_event;
    trevrpc_rpc_transport_event* fin_event;
    trevrpc_rpc_transport_event* send_stop_event;
    trevrpc_rpc_transport_event* send_event;
    size_t api_refs;
    size_t process_refs;
    trevrpc_wt_peer_settings peer_settings;
    trevrpc_wt_profile_negotiation negotiation;
    trevrpc_h3_demux_roles peer_roles;
    trevrpc_msquic_stream* control_streams[3];
};

struct h3_source {
    trevrpc_rpc_transport base;
    pthread_mutex_t mutex;
    pthread_cond_t object_cond;
    int wake_read_fd;
    int wake_write_fd;
    uint64_t owner;
    uint32_t state;
    int terminal_status;
    trevrpc_rpc_transport_config config;
    h3_registry_slot* entries;
    size_t entry_capacity;
    uint32_t free_head;
    h3_entry* retired_entries;
    h3_entry* reap_entries;
    trevrpc_rpc_transport_event* event_head;
    trevrpc_rpc_transport_event* event_tail;
    trevrpc_rpc_transport_event* stopped_event;
    size_t event_depth;
    size_t ordinary_depth;
    atomic_size_t mandatory_reservations;
    uint64_t next_sequence;
    uint64_t events_enqueued;
    uint64_t events_dequeued;
    uint64_t events_rejected;
    uint64_t receive_owned_count;
    uint64_t peak_receive_owned_count;
    uint64_t receive_owned_bytes;
    uint64_t peak_receive_owned_bytes;
    uint64_t pending_send_bytes;
    uint64_t pending_send_count;
    uint64_t wake_signals;
    uint64_t wake_write_eagain;
    uint64_t wake_failures;
    uint64_t live_listeners;
    uint64_t live_connections;
    uint64_t live_streams;
    bool wake_armed;
    bool stop_event_published;
#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
    size_t event_alloc_budget;
    uint64_t monotonic_nanos;
    uint16_t last_rejection_status;
    size_t rejection_count;
    size_t acceptance_count;
    bool monotonic_override;
    bool capture_rejections;
    bool capture_acceptances;
#endif
};

static atomic_uint_fast64_t h3_owner_sequence = ATOMIC_VAR_INIT(UINT64_C(0x100000));

static void h3_free_entry(h3_entry* entry);
static void h3_decode_peer_reset_locked(h3_source* source,
    h3_entry* entry,
    uint64_t provider_error,
    uint64_t* application_error,
    uint64_t* diagnostic_error);

static trevrpc_rpc_transport_event* h3_event_node_alloc(h3_source* source) {
    (void)source;
#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
    if (source->event_alloc_budget == 0)
        return NULL;
    if (source->event_alloc_budget != SIZE_MAX)
        --source->event_alloc_budget;
#endif
    return calloc(1, sizeof(trevrpc_rpc_transport_event));
}

static void h3_event_node_free(trevrpc_rpc_transport_event** node) {
    if (node != NULL && *node != NULL) {
        free((*node)->admission_headers);
        free((*node)->admission_storage);
        free(*node);
        *node = NULL;
    }
}

static trevrpc_rpc_transport_event* h3_mandatory_event_alloc(h3_source* source) {
    trevrpc_rpc_transport_event* event = h3_event_node_alloc(source);
    if (event != NULL)
        atomic_fetch_add_explicit(&source->mandatory_reservations, 1, memory_order_relaxed);
    return event;
}

static void h3_mandatory_reservation_consume(h3_source* source) {
    size_t previous = atomic_fetch_sub_explicit(&source->mandatory_reservations, 1, memory_order_relaxed);
    if (previous == 0)
        abort();
}

static void h3_mandatory_event_free(h3_source* source, trevrpc_rpc_transport_event** node) {
    if (node != NULL && *node != NULL) {
        h3_mandatory_reservation_consume(source);
        h3_event_node_free(node);
    }
}

static int h3_pin_object_locked(h3_source* source, h3_entry* entry, void** out) {
    (void)source;
    if (entry == NULL || !entry->live || entry->object == NULL)
        return -ESTALE;
    ++entry->api_refs;
    *out = entry->object;
    return 0;
}

static int h3_pin_entry_locked(h3_entry* entry) {
    if (entry == NULL || !entry->live)
        return -ESTALE;
    ++entry->process_refs;
    return 0;
}

static void h3_unpin_entry_locked(h3_source* source, h3_entry* entry) {
    if (entry != NULL && entry->process_refs != 0) {
        --entry->process_refs;
        if (entry->process_refs == 0)
            pthread_cond_broadcast(&source->object_cond);
    }
}

static void h3_unpin_object_locked(h3_source* source, h3_entry* entry) {
    if (entry != NULL && entry->api_refs != 0) {
        --entry->api_refs;
        if (entry->api_refs == 0)
            pthread_cond_broadcast(&source->object_cond);
    }
}

static void h3_schedule_waiting_wt_locked(h3_source* source, const h3_entry* connection);
static h3_entry* h3_find_retired_locked(h3_source* source, trevrpc_rpc_transport_handle handle);
static void h3_schedule_connection_waiters_locked(h3_source* source, const h3_entry* connection);
static void h3_emit_stream_readable_locked(h3_source* source, h3_entry* entry);
static void h3_maybe_emit_stream_ready_locked(h3_source* source, h3_entry* stream);
static void h3_maybe_publish_stopped_locked(h3_source* source);

static h3_source* h3_from_base(trevrpc_rpc_transport* transport) {
    return (h3_source*)transport;
}

static int h3_set_fd_flags(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0)
        return -errno;
    flags = fcntl(fd, F_GETFD, 0);
    if (flags < 0 || fcntl(fd, F_SETFD, flags | FD_CLOEXEC) != 0)
        return -errno;
    return 0;
}

static int h3_make_pipe(int fds[2]) {
    if (pipe(fds) != 0)
        return -errno;
    if (h3_set_fd_flags(fds[0]) != 0 || h3_set_fd_flags(fds[1]) != 0) {
        int e = errno;
        close(fds[0]);
        close(fds[1]);
        return -e;
    }
    return 0;
}

static void h3_drain_wake(h3_source* source) {
    uint8_t bytes[128];
    /* The H3 wake pipe read end is permanently nonblocking. */
    // NOLINTBEGIN(clang-analyzer-unix.BlockInCriticalSection)
    while (read(source->wake_read_fd, bytes, sizeof(bytes)) > 0) {
    }
    // NOLINTEND(clang-analyzer-unix.BlockInCriticalSection)
}

static void h3_signal_locked(h3_source* source) {
    uint8_t byte = 1;
    ssize_t n;
    if (source->wake_armed)
        return;
    do {
        n = write(source->wake_write_fd, &byte, 1);
    } while (n < 0 && errno == EINTR);
    if (n == 1) {
        source->wake_armed = true;
        ++source->wake_signals;
    } else if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
        source->wake_armed = true;
        ++source->wake_write_eagain;
    } else {
        ++source->wake_failures;
    }
}

static trevrpc_rpc_transport_handle h3_handle(const h3_entry* entry) {
    trevrpc_rpc_transport_handle result = {entry->source->owner, entry->slot, entry->generation};
    return result;
}

static bool h3_handle_equal(trevrpc_rpc_transport_handle left, trevrpc_rpc_transport_handle right) {
    return left.owner == right.owner && left.slot == right.slot && left.generation == right.generation;
}

static h3_entry* h3_find_locked(h3_source* source, trevrpc_rpc_transport_handle handle) {
    h3_registry_slot* slot;
    if (handle.owner != source->owner || handle.slot == 0 || handle.slot > source->entry_capacity)
        return NULL;
    slot = &source->entries[(size_t)handle.slot - 1u];
    if (slot->entry == NULL || slot->generation != handle.generation || !slot->entry->live)
        return NULL;
    return slot->entry;
}

static h3_entry* h3_find_receive_locked(h3_source* source, trevrpc_rpc_transport_handle handle) {
    h3_registry_slot* slot;
    h3_entry* entry;
    if (handle.owner != source->owner || handle.slot == 0 || handle.slot > source->entry_capacity)
        return NULL;
    slot = &source->entries[(size_t)handle.slot - 1u];
    entry = slot->entry;
    if (entry != NULL && slot->generation == handle.generation && entry->live &&
        entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM)
        return entry;
    if (entry != NULL && slot->generation == handle.generation && entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM &&
        entry->stream.receive_head != NULL)
        return entry;
    for (entry = source->retired_entries; entry != NULL; entry = entry->retired_next) {
        if (entry->slot == handle.slot && entry->generation == handle.generation &&
            entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM && entry->stream.receive_head != NULL)
            return entry;
    }
    return NULL;
}

static h3_entry* h3_find_any_locked(h3_source* source, trevrpc_rpc_transport_handle handle) {
    h3_registry_slot* slot;
    if (handle.owner != source->owner || handle.slot == 0 || handle.slot > source->entry_capacity)
        return NULL;
    slot = &source->entries[(size_t)handle.slot - 1u];
    if (slot->entry != NULL && slot->generation == handle.generation)
        return slot->entry;
    return NULL;
}

static h3_entry* h3_parent_connection_locked(h3_source* source, const h3_entry* entry) {
    h3_entry* parent = h3_find_locked(source, entry->parent);
    return parent != NULL && parent->kind == TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION ? parent : NULL;
}

static void h3_endpoint_free(h3_endpoint_copy* copy) {
    if (copy == NULL)
        return;
    free(copy->host);
    free(copy->server_name);
    free(copy->cert_file);
    free(copy->key_file);
    free(copy->ca_cert_file);
    free(copy->path);
    free(copy->origin);
    free(copy->cert_data);
    free(copy->key_data);
    free(copy->ca_cert_data);
    memset(copy, 0, sizeof(*copy));
}

static char* h3_str_copy(const char* value, uint32_t length) {
    char* result;
    if (length == 0)
        return NULL;
    if (value == NULL)
        return NULL;
#if SIZE_MAX <= UINT32_MAX
    if (length == UINT32_MAX)
        return NULL;
#endif
    result = malloc((size_t)length + 1u);
    if (result == NULL)
        return NULL;
    memcpy(result, value, length);
    result[length] = 0;
    return result;
}

static uint8_t* h3_bytes_copy(const uint8_t* value, uint64_t length) {
    uint8_t* result;
    if (length == 0)
        return NULL;
    if (value == NULL || length > SIZE_MAX)
        return NULL;
    result = malloc((size_t)length);
    if (result != NULL)
        memcpy(result, value, (size_t)length);
    return result;
}

static int h3_endpoint_copy_make(const trevrpc_rpc_transport_endpoint_config* source, h3_endpoint_copy* out) {
    memset(out, 0, sizeof(*out));
    out->value = *source;
    out->host = h3_str_copy(source->host, source->host_len);
    out->server_name = h3_str_copy(source->server_name, source->server_name_len);
    out->cert_file = h3_str_copy(source->cert_file, source->cert_file_len);
    out->key_file = h3_str_copy(source->key_file, source->key_file_len);
    out->ca_cert_file = h3_str_copy(source->ca_cert_file, source->ca_cert_file_len);
    out->path = h3_str_copy(source->path, source->path_len);
    out->origin = h3_str_copy(source->origin, source->origin_len);
    out->cert_data = h3_bytes_copy(source->cert_data, source->cert_data_len);
    out->key_data = h3_bytes_copy(source->key_data, source->key_data_len);
    out->ca_cert_data = h3_bytes_copy(source->ca_cert_data, source->ca_cert_data_len);
    if ((source->host_len && !out->host) || (source->server_name_len && !out->server_name) ||
        (source->cert_file_len && !out->cert_file) || (source->key_file_len && !out->key_file) ||
        (source->ca_cert_file_len && !out->ca_cert_file) || (source->path_len && !out->path) ||
        (source->origin_len && !out->origin) || (source->cert_data_len && !out->cert_data) ||
        (source->key_data_len && !out->key_data) || (source->ca_cert_data_len && !out->ca_cert_data)) {
        h3_endpoint_free(out);
        return -ENOMEM;
    }
    out->value.host = out->host;
    out->value.server_name = out->server_name;
    out->value.cert_file = out->cert_file;
    out->value.key_file = out->key_file;
    out->value.ca_cert_file = out->ca_cert_file;
    out->value.path = out->path;
    out->value.origin = out->origin;
    out->value.cert_data = out->cert_data;
    out->value.key_data = out->key_data;
    out->value.ca_cert_data = out->ca_cert_data;
    return 0;
}

static void h3_stream_free(h3_stream_state* stream) {
    trevrpc_rpc_transport_receive* receive;
    while ((receive = stream->receive_head) != NULL) {
        stream->receive_head = receive->next;
        free(receive);
    }
    if (stream->pending_completion != NULL) {
        (void)trevrpc_msquic_send_completion_wait(stream->pending_completion);
        trevrpc_msquic_send_completion_free(stream->pending_completion);
    }
    free(stream->parse);
    free(stream->rpc);
    free(stream->capsules);
    memset(stream, 0, sizeof(*stream));
}

static h3_entry* h3_alloc_entry_locked(
    h3_source* source, uint32_t kind, uint32_t side_flags, void* object, trevrpc_rpc_transport_handle parent) {
    h3_registry_slot* slot;
    h3_entry* entry;
    h3_entry* parent_entry;
    uint32_t slot_number;
    while (source->free_head != 0) {
        slot_number = source->free_head;
        slot = &source->entries[(size_t)slot_number - 1u];
        source->free_head = slot->next_free;
        slot->next_free = 0;
        if (slot->exhausted || slot->generation == 0)
            continue;
        entry = calloc(1, sizeof(*entry));
        if (entry == NULL) {
            slot->next_free = source->free_head;
            source->free_head = slot_number;
            return NULL;
        }
        /* Reserve every mandatory notification which may be needed by this
         * object.  A single ready node covers READY/FAILED; directional nodes
         * are only used by streams, but reserving them here keeps admission
         * uniform. */
        entry->ready_event = h3_mandatory_event_alloc(source);
        entry->terminal_event = h3_mandatory_event_alloc(source);
        entry->fin_event = h3_mandatory_event_alloc(source);
        entry->send_stop_event = h3_mandatory_event_alloc(source);
        if (entry->ready_event == NULL || entry->terminal_event == NULL || entry->fin_event == NULL ||
            entry->send_stop_event == NULL) {
            h3_mandatory_event_free(source, &entry->ready_event);
            h3_mandatory_event_free(source, &entry->terminal_event);
            h3_mandatory_event_free(source, &entry->fin_event);
            h3_mandatory_event_free(source, &entry->send_stop_event);
            free(entry);
            slot->next_free = source->free_head;
            source->free_head = slot_number;
            return NULL;
        }
        entry->source = source;
        entry->object = object;
        entry->kind = kind;
        entry->slot = slot_number;
        entry->generation = slot->generation;
        entry->side_flags = side_flags;
        entry->parent = parent;
        entry->live = true;
        parent_entry = h3_find_any_locked(source, parent);
        if (parent_entry != NULL) {
            ++parent_entry->child_refs;
            entry->parent_ref_held = true;
        }
        slot->entry = entry;
        if (kind == TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER)
            ++source->live_listeners;
        else if (kind == TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION)
            ++source->live_connections;
        else if (kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM)
            ++source->live_streams;
        return entry;
    }
    return NULL;
}

static void h3_resolve_unresolved_stream_locked(h3_entry* entry) {
    h3_entry* parent;
    if (!entry->stream.unresolved_counted)
        return;
    parent = h3_parent_connection_locked(entry->source, entry);
    if (parent != NULL && parent->unresolved_stream_count != 0)
        --parent->unresolved_stream_count;
    entry->stream.unresolved_counted = false;
    entry->stream.unresolved_deadline_nanos = 0;
}

static bool h3_stream_waits_for_resolution_locked(h3_source* source, h3_entry* entry) {
    h3_entry* parent;
    if (!entry->live || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM)
        return false;
    if (entry->stream.admission_pending)
        return true;
    parent = h3_parent_connection_locked(source, entry);
    if (parent == NULL)
        return false;
    if (!entry->stream.classified && entry->stream.classifier_initialized &&
        entry->stream.classifier.phase == TREV_H3_DEMUX_WAIT_NEGOTIATED_PROFILE && !parent->profile_resolved)
        return true;
    return entry->stream.wt_wait_session && !parent->wt_session_ready;
}

static int h3_select_provider_bytes_if_ready(h3_source* source, h3_entry* entry, trevrpc_msquic_stream* stream) {
    bool select;
    int result;
    pthread_mutex_lock(&source->mutex);
    select = entry->live && entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM && entry->object == stream &&
             entry->stream.classified && !entry->stream.unresolved_counted && !entry->stream.provider_bytes_selected;
    pthread_mutex_unlock(&source->mutex);
    if (!select)
        return 0;
    result = trevrpc_msquic_stream_select_protocol_bytes(stream);
    if (result != 0)
        return result;
    pthread_mutex_lock(&source->mutex);
    if (entry->live && entry->object == stream)
        entry->stream.provider_bytes_selected = true;
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

static void h3_mark_dead_locked(h3_entry* entry) {
    h3_source* source = entry->source;
    if (!entry->live)
        return;
    if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM)
        h3_resolve_unresolved_stream_locked(entry);
    entry->live = false;
    if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER && source->live_listeners)
        --source->live_listeners;
    if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION && source->live_connections)
        --source->live_connections;
    if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM && source->live_streams)
        --source->live_streams;
}

static void h3_release_parent_ref_locked(h3_entry* entry);

/* Marking an object dead is not enough to recycle its slot.  The heap entry remains
 * callback-safe until the terminal event is consumed (and until queued receives drain). */
static void h3_detach_entry_locked(h3_entry* entry) {
    h3_source* source = entry->source;
    h3_registry_slot* slot;
    if (entry->detached)
        return;
    h3_mark_dead_locked(entry);
    /* No future wake-only callback is actionable for a detached generation. */
    entry->pending = 0;
    slot = &source->entries[(size_t)entry->slot - 1u];
    if (slot->entry == entry) {
        slot->entry = NULL;
        if (slot->generation == UINT32_MAX) {
            slot->exhausted = true;
            slot->next_free = 0;
        } else {
            ++slot->generation;
            slot->next_free = source->free_head;
            source->free_head = entry->slot;
        }
    }
    entry->detached = true;
    h3_release_parent_ref_locked(entry);
    entry->retired_next = source->retired_entries;
    source->retired_entries = entry;
}

static h3_entry* h3_find_retired_locked(h3_source* source, trevrpc_rpc_transport_handle handle) {
    h3_entry* entry;
    for (entry = source->retired_entries; entry != NULL; entry = entry->retired_next) {
        if (entry->slot == handle.slot && entry->generation == handle.generation)
            return entry;
    }
    return NULL;
}

static void h3_release_parent_ref_locked(h3_entry* entry) {
    h3_source* source = entry->source;
    h3_entry* parent;
    if (!entry->parent_ref_held)
        return;
    parent = h3_find_any_locked(source, entry->parent);
    if (parent == NULL)
        parent = h3_find_retired_locked(source, entry->parent);
    if (parent != NULL && parent->child_refs != 0)
        --parent->child_refs;
    entry->parent_ref_held = false;
}

static bool h3_retired_reclaimable(const h3_entry* entry) {
    return entry->detached && entry->stream.receive_head == NULL && entry->stream.pending_completion == NULL &&
           entry->pending == 0 && entry->child_refs == 0 && entry->api_refs == 0 && entry->process_refs == 0;
}

static void h3_collect_reclaimable_locked(h3_source* source) {
    h3_entry** link = &source->retired_entries;
    while (*link != NULL) {
        h3_entry* entry = *link;
        if (h3_retired_reclaimable(entry)) {
            *link = entry->retired_next;
            entry->retired_next = source->reap_entries;
            source->reap_entries = entry;
        } else {
            link = &entry->retired_next;
        }
    }
}

static void h3_reap_pending(h3_source* source) {
    h3_entry* entry;
    for (;;) {
        pthread_mutex_lock(&source->mutex);
        entry = source->reap_entries;
        if (entry != NULL)
            source->reap_entries = entry->retired_next;
        pthread_mutex_unlock(&source->mutex);
        if (entry == NULL)
            return;
        entry->retired_next = NULL;
        h3_free_entry(entry);
    }
}

static trevrpc_msquic_send_completion* h3_detach_pending_send_locked(h3_entry* entry) {
    h3_source* source = entry->source;
    trevrpc_msquic_send_completion* completion = entry->stream.pending_completion;
    if (completion == NULL)
        return NULL;
    entry->stream.pending_completion = NULL;
    entry->stream.pending_operation_id = 0;
    if (source->pending_send_count != 0)
        --source->pending_send_count;
    if (source->pending_send_bytes >= entry->stream.pending_send_bytes)
        source->pending_send_bytes -= entry->stream.pending_send_bytes;
    entry->stream.pending_send_bytes = 0;
    entry->pending &= ~H3_PENDING_SEND_COMPLETE;
    h3_mandatory_event_free(source, &entry->send_event);
    return completion;
}

static void h3_release_detached_pending_send(trevrpc_msquic_send_completion* completion) {
    if (completion == NULL)
        return;
    (void)trevrpc_msquic_send_completion_wait(completion);
    trevrpc_msquic_send_completion_free(completion);
}

static void h3_close_entry_object_mode(h3_entry* entry, bool force_stream_close) {
    h3_source* source = entry->source;
    void* object;
    uint32_t kind;
    bool observer_installed;
    bool start_observer_installed;
    trevrpc_msquic_send_completion* completion;
    pthread_mutex_lock(&source->mutex);
    while (entry->api_refs != 0 || entry->process_refs > (h3_processing_entry == entry ? 1u : 0u))
        pthread_cond_wait(&source->object_cond, &source->mutex);
    object = entry->object;
    entry->object = NULL;
    kind = entry->kind;
    completion = kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM ? h3_detach_pending_send_locked(entry) : NULL;
    observer_installed = entry->observer_installed;
    start_observer_installed = entry->start_observer_installed;
    entry->observer_installed = false;
    entry->start_observer_installed = false;
    pthread_mutex_unlock(&source->mutex);
    if (object == NULL) {
        h3_release_detached_pending_send(completion);
        return;
    }
    if (kind == TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER) {
        if (observer_installed) {
            trevrpc_msquic_listener_clear_observer(object);
            trevrpc_msquic_listener_drain_observer(object);
        }
        trevrpc_msquic_listener_close_deferred_owned(object);
    } else if (kind == TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION) {
        if (observer_installed) {
            trevrpc_msquic_conn_clear_observer(object);
            trevrpc_msquic_conn_drain_observer(object);
        }
        trevrpc_msquic_conn_close_deferred_owned(object);
    } else {
        if (observer_installed) {
            trevrpc_msquic_stream_clear_observer(object);
            trevrpc_msquic_stream_drain_observer(object);
        }
        if (start_observer_installed) {
            trevrpc_msquic_stream_clear_start_observer(object);
            trevrpc_msquic_stream_drain_start_observer(object);
        }
        trevrpc_msquic_stream_close_deferred_owned(object, force_stream_close);
        h3_release_detached_pending_send(completion);
    }
}

static void h3_close_entry_object(h3_entry* entry) {
    h3_close_entry_object_mode(entry, false);
}

static void h3_close_entry_object_owned(h3_entry* entry) {
    h3_source* source = entry->source;
    void* object;
    uint32_t kind;
    bool observer_installed;
    bool start_observer_installed;
    trevrpc_msquic_send_completion* completion;
    pthread_mutex_lock(&source->mutex);
    assert(entry->api_refs != 0);
    --entry->api_refs;
    while (entry->api_refs != 0 || entry->process_refs > (h3_processing_entry == entry ? 1u : 0u))
        pthread_cond_wait(&source->object_cond, &source->mutex);
    object = entry->object;
    entry->object = NULL;
    kind = entry->kind;
    completion = kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM ? h3_detach_pending_send_locked(entry) : NULL;
    observer_installed = entry->observer_installed;
    start_observer_installed = entry->start_observer_installed;
    entry->observer_installed = false;
    entry->start_observer_installed = false;
    pthread_mutex_unlock(&source->mutex);
    if (object == NULL) {
        h3_release_detached_pending_send(completion);
        return;
    }
    if (kind == TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER) {
        if (observer_installed) {
            trevrpc_msquic_listener_clear_observer(object);
            trevrpc_msquic_listener_drain_observer(object);
        }
        trevrpc_msquic_listener_close_deferred_owned(object);
    } else if (kind == TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION) {
        if (observer_installed) {
            trevrpc_msquic_conn_clear_observer(object);
            trevrpc_msquic_conn_drain_observer(object);
        }
        trevrpc_msquic_conn_close_deferred_owned(object);
    } else {
        if (observer_installed) {
            trevrpc_msquic_stream_clear_observer(object);
            trevrpc_msquic_stream_drain_observer(object);
        }
        if (start_observer_installed) {
            trevrpc_msquic_stream_clear_start_observer(object);
            trevrpc_msquic_stream_drain_start_observer(object);
        }
        trevrpc_msquic_stream_close_deferred_owned(object, false);
        h3_release_detached_pending_send(completion);
    }
}

static bool h3_object_terminal_kind(uint32_t kind) {
    return kind == TREVRPC_RPC_TRANSPORT_EVENT_LISTENER_STOPPED ||
           kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_FAILED ||
           kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED || kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED ||
           kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED;
}

static int h3_publish_locked(h3_source* source,
    const trevrpc_rpc_transport_event_info* input,
    trevrpc_rpc_transport_event* event,
    bool mandatory,
    h3_entry* terminal_entry) {
    if (!mandatory && source->ordinary_depth >= source->config.event_capacity) {
        ++source->events_rejected;
        return -EAGAIN;
    }
    if (event == NULL) {
        ++source->events_rejected;
        return -ENOMEM;
    }
    event->info = *input;
    event->mandatory = mandatory;
    event->next = NULL;
    event->info.sequence = source->next_sequence++;
    event->terminal_entry = terminal_entry;
    if (mandatory)
        h3_mandatory_reservation_consume(source);
    if (source->event_tail)
        source->event_tail->next = event;
    else
        source->event_head = event;
    source->event_tail = event;
    ++source->event_depth;
    if (!mandatory)
        ++source->ordinary_depth;
    ++source->events_enqueued;
    h3_signal_locked(source);
    return 0;
}

static int h3_emit_locked(h3_source* source,
    uint32_t kind,
    uint32_t flags,
    int status,
    uint32_t subject_kind,
    trevrpc_rpc_transport_handle subject,
    trevrpc_rpc_transport_handle parent,
    uint64_t operation_id,
    uint64_t app_error) {
    trevrpc_rpc_transport_event_info info = {0};
    h3_entry* terminal_entry = NULL;
    h3_entry* owner = NULL;
    trevrpc_rpc_transport_event* reserved_event = NULL;
    bool object_terminal = h3_object_terminal_kind(kind);
    bool mandatory =
        (flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL) != 0 || kind == TREVRPC_RPC_TRANSPORT_EVENT_STOPPED ||
        kind == TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE || kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN ||
        kind == TREVRPC_RPC_TRANSPORT_EVENT_SEND_STOPPED || kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY ||
        kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_FAILED || kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY ||
        kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED;
    info.kind = kind;
    info.flags = flags;
    info.status = status;
    info.subject_kind = subject_kind;
    info.subject = subject;
    info.parent = parent;
    info.operation_id = operation_id;
    info.application_error_code = app_error;
    if (kind == TREVRPC_RPC_TRANSPORT_EVENT_STOPPED) {
        reserved_event = source->stop_event_published ? NULL : source->stopped_event;
    } else if (kind == TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE) {
        owner = h3_find_any_locked(source, subject);
        if (owner == NULL)
            owner = h3_find_retired_locked(source, subject);
        reserved_event = owner != NULL ? owner->send_event : NULL;
    } else if (kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN) {
        owner = h3_find_any_locked(source, subject);
        if (owner == NULL)
            owner = h3_find_retired_locked(source, subject);
        reserved_event = owner != NULL ? owner->fin_event : NULL;
    } else if (kind == TREVRPC_RPC_TRANSPORT_EVENT_SEND_STOPPED) {
        owner = h3_find_any_locked(source, subject);
        if (owner == NULL)
            owner = h3_find_retired_locked(source, subject);
        reserved_event = owner != NULL ? owner->send_stop_event : NULL;
    } else if (object_terminal) {
        terminal_entry = h3_find_any_locked(source, subject);
        if (terminal_entry == NULL)
            terminal_entry = h3_find_retired_locked(source, subject);
        owner = terminal_entry;
        reserved_event = terminal_entry != NULL ? terminal_entry->terminal_event : NULL;
    } else if (mandatory) {
        owner = h3_find_any_locked(source, subject);
        if (owner == NULL)
            owner = h3_find_retired_locked(source, subject);
        reserved_event = owner != NULL ? owner->ready_event : NULL;
    }
    if (object_terminal && terminal_entry != NULL) {
        if (terminal_entry->terminal_pending)
            return 0;
        terminal_entry->terminal_pending = true;
    }
    if (!mandatory) {
        if (source->ordinary_depth >= source->config.event_capacity) {
            ++source->events_rejected;
            return -EAGAIN;
        }
        reserved_event = h3_event_node_alloc(source);
    }
    if (reserved_event != NULL) {
        h3_entry* protocol_entry = owner;
        if (protocol_entry == NULL && !h3_handle_equal(parent, (trevrpc_rpc_transport_handle){0})) {
            protocol_entry = h3_find_any_locked(source, parent);
            if (protocol_entry == NULL)
                protocol_entry = h3_find_retired_locked(source, parent);
        }
        reserved_event->source = source;
        if (protocol_entry != NULL)
            reserved_event->protocol = protocol_entry->endpoint.value.protocol;
    }
    {
        int result = h3_publish_locked(source, &info, reserved_event, mandatory, terminal_entry);
        if (result == 0) {
            if (kind == TREVRPC_RPC_TRANSPORT_EVENT_STOPPED)
                source->stopped_event = NULL;
            else if (owner != NULL) {
                if (kind == TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE)
                    owner->send_event = NULL;
                else if (kind == TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN)
                    owner->fin_event = NULL;
                else if (kind == TREVRPC_RPC_TRANSPORT_EVENT_SEND_STOPPED)
                    owner->send_stop_event = NULL;
                else if (object_terminal)
                    owner->terminal_event = NULL;
                else if (owner->ready_event == reserved_event)
                    owner->ready_event = NULL;
            }
        } else {
            if (!mandatory)
                h3_event_node_free(&reserved_event);
            if (object_terminal && terminal_entry != NULL)
                terminal_entry->terminal_pending = false;
        }
        return result;
    }
}

static int h3_emit_with_provider_locked(h3_source* source,
    uint32_t kind,
    uint32_t flags,
    int status,
    uint32_t subject_kind,
    trevrpc_rpc_transport_handle subject,
    trevrpc_rpc_transport_handle parent,
    uint64_t operation_id,
    uint64_t app_error,
    uint64_t provider_error) {
    uint64_t previous_enqueued = source->events_enqueued;
    int result = h3_emit_locked(source, kind, flags, status, subject_kind, subject, parent, operation_id, app_error);
    if (result == 0 && source->events_enqueued != previous_enqueued && source->event_tail != NULL)
        source->event_tail->info.provider_error_code = provider_error;
    return result;
}

static void h3_maybe_emit_stream_closed_locked(h3_entry* entry) {
    h3_source* source = entry->source;
    if (!entry->live || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM || entry->close_reported ||
        entry->stream.connect_control || !entry->stream.recv_fin || !entry->stream.send_fin ||
        (entry->stream.send_aborted && entry->stream.peer_reset && !entry->stream.send_stop_reported) ||
        entry->send_event != NULL || entry->stream.pending_completion != NULL ||
        (entry->stream.action != TREV_H3_DEMUX_ACTION_REQUEST &&
            entry->stream.action != TREV_H3_DEMUX_ACTION_WEBTRANSPORT))
        return;
    bool aborted = entry->stream.recv_aborted || entry->stream.send_aborted || entry->stream.peer_reset;
    uint64_t application_error = aborted ? entry->stream.local_abort_error : 0;
    uint64_t diagnostic_error = 0;
    if (entry->stream.peer_reset)
        h3_decode_peer_reset_locked(
            source, entry, entry->stream.peer_reset_error, &application_error, &diagnostic_error);
    if (h3_emit_with_provider_locked(source,
            TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
            entry->side_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL |
                (entry->stream.peer_reset ? TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER_RESET : 0) |
                (aborted ? 0 : TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN),
            aborted ? -ECANCELED : 0,
            TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
            h3_handle(entry),
            entry->parent,
            0,
            application_error,
            diagnostic_error) == 0) {
        entry->close_reported = true;
        h3_mark_dead_locked(entry);
    } else {
        entry->pending |= H3_PENDING_TERMINAL;
        h3_signal_locked(source);
    }
}

static bool h3_is_webtransport(const h3_entry* entry) {
    return entry->endpoint.value.protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT;
}

static bool h3_is_multiplexed(const h3_entry* entry) {
    return entry->endpoint.value.protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED;
}

static bool h3_allows_webtransport(const h3_entry* entry) {
    return h3_is_webtransport(entry) || (h3_is_multiplexed(entry) && entry->endpoint.value.webtransport_profiles != 0 &&
                                            entry->endpoint.value.max_sessions != 0);
}

static int h3_monotonic_nanos(uint64_t* out) {
    struct timespec now;
    if (out == NULL)
        return -EINVAL;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0)
        return -errno;
    if ((uint64_t)now.tv_sec > UINT64_MAX / UINT64_C(1000000000))
        return -EOVERFLOW;
    *out = (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
    return 0;
}

static int h3_now_nanos(h3_source* source, uint64_t* out) {
#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
    if (source->monotonic_override) {
        *out = source->monotonic_nanos;
        return 0;
    }
#else
    (void)source;
#endif
    return h3_monotonic_nanos(out);
}

static uint64_t h3_deadline_after_milliseconds(uint64_t now, uint64_t milliseconds) {
    if (milliseconds > (UINT64_MAX - now) / UINT64_C(1000000))
        return UINT64_MAX;
    return now + milliseconds * UINT64_C(1000000);
}

static void h3_expire_unresolved_streams_locked(h3_source* source, uint64_t now) {
    size_t i;
    for (i = 0; i < source->entry_capacity; ++i) {
        h3_entry* entry = source->entries[i].entry;
        if (entry == NULL || !entry->live || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM ||
            !entry->stream.unresolved_counted || entry->stream.unresolved_deadline_nanos == 0 ||
            entry->stream.unresolved_deadline_nanos > now)
            continue;
        entry->stream.unresolved_abort_error = entry->stream.classifier.phase == TREV_H3_DEMUX_READ_SESSION_ID
                                                   ? TREV_H3_DEMUX_APP_ID_ERROR
                                                   : TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR;
        h3_resolve_unresolved_stream_locked(entry);
        entry->stream.unresolved_error = -ETIMEDOUT;
        entry->pending |= H3_PENDING_TERMINAL;
    }
}

static int h3_next_unresolved_timeout_locked(const h3_source* source, uint64_t now) {
    uint64_t earliest = UINT64_MAX;
    size_t i;
    for (i = 0; i < source->entry_capacity; ++i) {
        const h3_entry* entry = source->entries[i].entry;
        if (entry == NULL || !entry->live || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM ||
            !entry->stream.unresolved_counted || entry->stream.unresolved_deadline_nanos == 0)
            continue;
        if (entry->stream.unresolved_deadline_nanos <= now)
            return 0;
        if (entry->stream.unresolved_deadline_nanos < earliest)
            earliest = entry->stream.unresolved_deadline_nanos;
    }
    if (earliest == UINT64_MAX)
        return -1;
    {
        uint64_t remaining = earliest - now;
        uint64_t milliseconds = remaining / UINT64_C(1000000) + (remaining % UINT64_C(1000000) != 0);
        return milliseconds > (uint64_t)INT_MAX ? INT_MAX : (int)milliseconds;
    }
}

static void h3_decode_peer_reset_locked(h3_source* source,
    h3_entry* entry,
    uint64_t provider_error,
    uint64_t* application_error,
    uint64_t* diagnostic_error) {
    h3_entry* parent;
    trevrpc_wt_profile_id profile;
    uint64_t decoded;
    *application_error = 0;
    *diagnostic_error = 0;
    if (entry->stream.action == TREV_H3_DEMUX_ACTION_REQUEST) {
        if (provider_error == H3_APP_REQUEST_CANCELLED)
            *application_error = H3_RPC_STATUS_CANCELLED;
        else
            *diagnostic_error = provider_error;
        return;
    }
    if (entry->stream.action != TREV_H3_DEMUX_ACTION_WEBTRANSPORT) {
        *diagnostic_error = provider_error;
        return;
    }
    parent = h3_parent_connection_locked(source, entry);
    if (parent != NULL && parent->profile_resolved) {
        profile = parent->negotiation.profile;
        if (trevrpc_wt_profile_decode_application_error(profile, provider_error, &decoded) == 0) {
            *application_error = decoded;
            return;
        }
    }
    /* Keep an invalid or unexpected WT code raw; in particular, H3 0x10c is
     * not a WebTransport application code. */
    *diagnostic_error = provider_error;
}

static void h3_schedule_receive_retries_locked(h3_source* source) {
    size_t i;
    bool signaled = false;
    for (i = 0; i < source->entry_capacity; ++i) {
        h3_entry* entry = source->entries[i].entry;
        if (entry == NULL || !entry->live || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM ||
            !entry->stream.receive_blocked || (entry->stream.parse_len == 0 && entry->stream.rpc_len == 0))
            continue;
        entry->pending |= H3_PENDING_READABLE;
        signaled = true;
    }
    if (signaled)
        h3_signal_locked(source);
}

static void h3_schedule_event_retries_locked(h3_source* source) {
    size_t i;
    bool signaled = false;
    for (i = 0; i < source->entry_capacity; ++i) {
        h3_entry* entry = source->entries[i].entry;
        if (entry == NULL || !entry->live || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM ||
            !entry->stream.event_blocked || entry->stream.parse_len == 0)
            continue;
        entry->pending |= H3_PENDING_READABLE;
        signaled = true;
    }
    if (signaled)
        h3_signal_locked(source);
}

static void h3_schedule_connection_waiters_locked(h3_source* source, const h3_entry* connection) {
    trevrpc_rpc_transport_handle parent = h3_handle(connection);
    size_t i;
    for (i = 0; i < source->entry_capacity; ++i) {
        h3_entry* entry = source->entries[i].entry;
        if (entry == NULL)
            continue;
        if (entry->live && entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM && entry->parent.owner == parent.owner &&
            entry->parent.slot == parent.slot && entry->parent.generation == parent.generation &&
            entry->stream.headers_received) {
            h3_maybe_emit_stream_ready_locked(source, entry);
            entry->pending |= H3_PENDING_READABLE;
        }
    }
    h3_signal_locked(source);
}

static void h3_maybe_emit_stream_ready_locked(h3_source* source, h3_entry* stream) {
    h3_entry* connection;
    if (!stream->live || stream->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM || stream->stream.ready_reported ||
        stream->stream.admission_pending || stream->stream.admission_rejected || stream->stream.headers_sending ||
        !stream->stream.headers_received || stream->stream.connect_control ||
        (stream->side_flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER) == 0 ||
        (stream->stream.action != TREV_H3_DEMUX_ACTION_REQUEST &&
            stream->stream.action != TREV_H3_DEMUX_ACTION_WEBTRANSPORT))
        return;
    connection = h3_parent_connection_locked(source, stream);
    if (connection == NULL || !connection->ready_reported)
        return;
    if (h3_emit_locked(source,
            TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
            stream->side_flags,
            0,
            TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
            h3_handle(stream),
            stream->parent,
            stream->operation_id,
            0) == 0) {
        stream->stream.ready_reported = true;
        h3_emit_stream_readable_locked(source, stream);
    } else {
        h3_signal_locked(source);
    }
}

static void h3_schedule_profile_waiters_locked(h3_source* source, const h3_entry* connection) {
    trevrpc_rpc_transport_handle parent = h3_handle(connection);
    size_t i;
    for (i = 0; i < source->entry_capacity; ++i) {
        h3_entry* entry = source->entries[i].entry;
        if (entry == NULL)
            continue;
        if (entry->live && entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM &&
            h3_handle_equal(entry->parent, parent) &&
            ((!entry->stream.classified && entry->stream.classifier_initialized) || entry->stream.parse_len != 0))
            entry->pending |= H3_PENDING_READABLE;
    }
    h3_signal_locked(source);
}

static bool h3_profile_enabled(uint32_t profiles, trevrpc_wt_profile_id profile) {
    if (profiles == 0)
        profiles = H3_WT_PROFILE_ALL_SUPPORTED;
    switch (profile) {
    case TREV_WT_PROFILE_DRAFT_02:
        return (profiles & H3_WT_PROFILE_DRAFT_02) != 0;
    case TREV_WT_PROFILE_DRAFT_07:
        return (profiles & H3_WT_PROFILE_DRAFT_07) != 0;
    case TREV_WT_PROFILE_DRAFT_14:
        return (profiles & H3_WT_PROFILE_DRAFT_14) != 0;
    case TREV_WT_PROFILE_DRAFT_15:
        return (profiles & H3_WT_PROFILE_DRAFT_15) != 0;
    default:
        return false;
    }
}

static void h3_maybe_emit_connection_ready_locked(h3_entry* entry) {
    if (!entry->live || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION || entry->ready_reported ||
        h3_is_multiplexed(entry) || !entry->connected || !entry->local_control_ready || !entry->peer_settings_ready ||
        (h3_is_webtransport(entry) && !entry->wt_session_ready))
        return;
    if (h3_emit_locked(entry->source,
            TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
            entry->side_flags,
            0,
            TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION,
            h3_handle(entry),
            entry->parent,
            entry->operation_id,
            0) == 0) {
        entry->ready_reported = true;
        h3_schedule_connection_waiters_locked(entry->source, entry);
    } else {
        h3_signal_locked(entry->source);
    }
}

static void h3_conn_observer(void* context, const trevrpc_msquic_conn_event* event) {
    h3_entry* entry = context;
    h3_source* source;
    if (entry == NULL || (source = entry->source) == NULL || event == NULL)
        return;
    pthread_mutex_lock(&source->mutex);
    if (entry->live) {
        if (event->kind == TREV_MSQUIC_CONN_EVENT_CONNECTED)
            entry->pending |= H3_PENDING_CONNECTED;
        else if (event->kind == TREV_MSQUIC_CONN_EVENT_PEER_STREAM_AVAILABLE)
            entry->pending |= H3_PENDING_ACCEPT;
        else if (event->kind == TREV_MSQUIC_CONN_EVENT_SHUTDOWN_COMPLETE)
            entry->pending |= H3_PENDING_SHUTDOWN;
    }
    h3_signal_locked(source);
    pthread_mutex_unlock(&source->mutex);
}

static void h3_listener_observer(void* context) {
    h3_entry* entry = context;
    h3_source* source;
    if (entry == NULL || (source = entry->source) == NULL)
        return;
    pthread_mutex_lock(&source->mutex);
    if (entry->live)
        entry->pending |= H3_PENDING_ACCEPT;
    h3_signal_locked(source);
    pthread_mutex_unlock(&source->mutex);
}

static void h3_reschedule_accept(h3_source* source, h3_entry* entry) {
    pthread_mutex_lock(&source->mutex);
    if (entry->live) {
        entry->pending |= H3_PENDING_ACCEPT;
        h3_signal_locked(source);
    }
    pthread_mutex_unlock(&source->mutex);
}

static void h3_stream_observer(void* context, uint32_t flags) {
    h3_entry* entry = context;
    h3_source* source;
    if (entry == NULL || (source = entry->source) == NULL)
        return;
    pthread_mutex_lock(&source->mutex);
    if (entry->live) {
        if (flags & TREV_MSQUIC_STREAM_OBSERVER_READABLE)
            entry->pending |= H3_PENDING_READABLE;
        if (flags & TREV_MSQUIC_STREAM_OBSERVER_TERMINAL)
            entry->pending |= H3_PENDING_TERMINAL;
        if (flags & TREV_MSQUIC_STREAM_OBSERVER_SEND_COMPLETE)
            entry->pending |= H3_PENDING_SEND_COMPLETE;
    }
    h3_signal_locked(source);
    pthread_mutex_unlock(&source->mutex);
}

static void h3_stream_start_observer(void* context) {
    h3_entry* entry = context;
    h3_source* source;
    if (entry == NULL || (source = entry->source) == NULL)
        return;
    pthread_mutex_lock(&source->mutex);
    if (entry->live)
        entry->pending |= H3_PENDING_START;
    h3_signal_locked(source);
    pthread_mutex_unlock(&source->mutex);
}

static int h3_install_conn(h3_entry* entry) {
    h3_source* source = entry->source;
    int result = trevrpc_msquic_conn_set_observer(entry->object, h3_conn_observer, entry);
    if (result != 0)
        return result;
    pthread_mutex_lock(&source->mutex);
    entry->observer_installed = true;
    pthread_mutex_unlock(&source->mutex);
    result = trevrpc_msquic_conn_ready(entry->object);
    pthread_mutex_lock(&source->mutex);
    if (entry->live) {
        if (result == 0)
            entry->pending |= H3_PENDING_CONNECTED;
        else if (result != -EAGAIN)
            entry->pending |= H3_PENDING_SHUTDOWN;
        entry->pending |= H3_PENDING_ACCEPT;
        h3_signal_locked(source);
    }
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

static int h3_install_stream(h3_source* source, h3_entry* entry) {
    uint64_t stream_id = 0;
    int start_status = -EAGAIN;
    int result = trevrpc_msquic_stream_set_observer(entry->object, h3_stream_observer, entry);
    if (result != 0)
        return result;
    pthread_mutex_lock(&source->mutex);
    entry->observer_installed = true;
    pthread_mutex_unlock(&source->mutex);
    result = trevrpc_msquic_stream_set_start_observer(entry->object, h3_stream_start_observer, entry);
    if (result != 0)
        return result;
    pthread_mutex_lock(&source->mutex);
    entry->start_observer_installed = true;
    pthread_mutex_unlock(&source->mutex);
    if ((entry->side_flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL) != 0)
        start_status = trevrpc_msquic_stream_start_status(entry->object, &stream_id);
    pthread_mutex_lock(&source->mutex);
    if (entry->live) {
        entry->pending |= H3_PENDING_READABLE;
        if (start_status != -EAGAIN)
            entry->pending |= H3_PENDING_START;
        h3_signal_locked(source);
    }
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

static int h3_admit_peer_stream_locked(h3_source* source,
    h3_entry* parent,
    trevrpc_msquic_stream* object,
    trevrpc_h3_demux_direction direction,
    h3_entry** out_entry,
    bool* out_quota_rejected) {
    bool unresolved = h3_allows_webtransport(parent) && !parent->wt_session_ready;
    uint64_t now = 0;
    h3_entry* entry;
    int result;
    *out_entry = NULL;
    *out_quota_rejected = false;
    if (unresolved && parent->endpoint.value.unresolved_stream_count != 0 &&
        parent->unresolved_stream_count >= parent->endpoint.value.unresolved_stream_count) {
        *out_quota_rejected = true;
        return -EAGAIN;
    }
    if (unresolved && parent->endpoint.value.unresolved_stream_timeout_ms != 0) {
        result = h3_now_nanos(source, &now);
        if (result != 0)
            return result;
    }
    entry = h3_alloc_entry_locked(source,
        TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
        parent->side_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
        object,
        h3_handle(parent));
    if (entry == NULL)
        return -EAGAIN;
    *out_entry = entry;
    result = trevrpc_h3_demux_stream_init(&entry->stream.classifier, direction);
    if (result != 0) {
        h3_detach_entry_locked(entry);
        return result;
    }
    entry->stream.classifier_initialized = true;
    if (unresolved) {
        entry->stream.unresolved_counted = true;
        ++parent->unresolved_stream_count;
        if (parent->endpoint.value.unresolved_stream_timeout_ms != 0) {
            entry->stream.unresolved_deadline_nanos =
                h3_deadline_after_milliseconds(now, parent->endpoint.value.unresolved_stream_timeout_ms);
        }
    }
    return 0;
}

static int h3_write_all(trevrpc_msquic_stream* stream, const uint8_t* data, size_t len) {
    intptr_t result = trevrpc_msquic_stream_write(stream, data, len);
    if (result < 0)
        return (int)result;
    return (size_t)result == len ? 0 : -EPIPE;
}

static int h3_send_control(h3_entry* entry) {
    static const trevrpc_wt_profile_id profiles[] = {
        TREV_WT_PROFILE_DRAFT_02,
        TREV_WT_PROFILE_DRAFT_07,
        TREV_WT_PROFILE_DRAFT_14,
        TREV_WT_PROFILE_DRAFT_15,
    };
    trevrpc_msquic_conn* conn = entry->object;
    trevrpc_msquic_stream* stream = NULL;
    trevrpc_h3_settings_pair settings[16] = {
        {H3_SETTINGS_QPACK_MAX_TABLE_CAPACITY, 0},
        {H3_SETTINGS_MAX_FIELD_SECTION_SIZE,
            entry->endpoint.value.max_field_section_size != 0 ? entry->endpoint.value.max_field_section_size
                                                              : H3_DEFAULT_MAX_FIELD},
        {H3_SETTINGS_QPACK_BLOCKED_STREAMS, 0},
    };
    size_t settings_count = 3;
    uint8_t settings_payload[256];
    uint8_t settings_prefix[16];
    uint8_t control_wire[1 + sizeof(settings_prefix) + sizeof(settings_payload)];
    uint8_t qpack_encoder[] = {TREV_H3_DEMUX_STREAM_TYPE_QPACK_ENCODER};
    uint8_t qpack_decoder[] = {TREV_H3_DEMUX_STREAM_TYPE_QPACK_DECODER};
    const uint8_t* values[3];
    size_t lengths[3];
    size_t payload_len = 0;
    size_t prefix_len = 0;
    size_t i;
    int result;
    if (h3_allows_webtransport(entry)) {
        bool advertised = false;
        size_t webtransport_settings_start = settings_count;
        settings[settings_count++] = (trevrpc_h3_settings_pair){TREV_WT_PROFILE_SETTINGS_ENABLE_CONNECT_PROTOCOL, 1};
        if (trevrpc_msquic_feature_snapshot_negotiated_datagrams(&entry->capabilities))
            settings[settings_count++] = (trevrpc_h3_settings_pair){TREV_WT_PROFILE_SETTINGS_H3_DATAGRAM, 1};
        for (i = 0; i < sizeof(profiles) / sizeof(profiles[0]); ++i) {
            trevrpc_wt_profile_setting_pair materialized[4];
            size_t materialized_count = 0;
            size_t setting_index;
            if (!h3_profile_enabled(entry->endpoint.value.webtransport_profiles, profiles[i]) ||
                !trevrpc_wt_profile_is_usable(profiles[i], &entry->capabilities))
                continue;
            result = trevrpc_wt_profile_materialize_settings(TREV_WT_PROFILE_ADVERTISEMENT_SELECTED,
                profiles[i],
                &entry->capabilities,
                entry->endpoint.value.max_sessions,
                materialized,
                sizeof(materialized) / sizeof(materialized[0]),
                &materialized_count);
            if (result != 0)
                return result;
            if (materialized_count > sizeof(settings) / sizeof(settings[0]) - settings_count)
                return -EOVERFLOW;
            for (setting_index = 0; setting_index < materialized_count; ++setting_index) {
                settings[settings_count++] = (trevrpc_h3_settings_pair){
                    materialized[setting_index].id,
                    materialized[setting_index].value,
                };
            }
            advertised = true;
        }
        if (!advertised) {
            /*
             * A multiplexed listener must still accept an ordinary HTTP/3
             * peer that did not negotiate the QUIC features required by any
             * enabled WebTransport profile. Keep this connection's SETTINGS
             * truthful and let a separate capable connection select WT.
             */
            settings_count = webtransport_settings_start;
        }
    }
    if (trevrpc_h3_settings_build(settings, settings_count, settings_payload, sizeof(settings_payload), &payload_len) !=
        TREV_H3_SETTINGS_OK)
        return -EINVAL;
    if (trevrpc_h3_frame_prefix_build(
            TREV_H3_FRAME_SETTINGS, payload_len, settings_prefix, sizeof(settings_prefix), &prefix_len) !=
        TREV_H3_FRAME_OK)
        return -EINVAL;
    control_wire[0] = TREV_H3_DEMUX_STREAM_TYPE_CONTROL;
    memcpy(control_wire + 1, settings_prefix, prefix_len);
    memcpy(control_wire + 1 + prefix_len, settings_payload, payload_len);
    values[0] = control_wire;
    lengths[0] = 1 + prefix_len + payload_len;
    values[1] = qpack_encoder;
    lengths[1] = sizeof(qpack_encoder);
    values[2] = qpack_decoder;
    lengths[2] = sizeof(qpack_decoder);
    for (i = 0; i < 3; ++i) {
        result = trevrpc_msquic_conn_open_uni_stream(conn, &stream);
        if (result != 0)
            break;
        result = h3_write_all(stream, values[i], lengths[i]);
        if (result != 0) {
            trevrpc_msquic_stream_close(stream);
            stream = NULL;
            break;
        }
        entry->control_streams[i] = stream;
        stream = NULL;
    }
    if (i == 3)
        return 0;
    while (i != 0) {
        --i;
        trevrpc_msquic_stream_close(entry->control_streams[i]);
        entry->control_streams[i] = NULL;
    }
    return result;
}

static int h3_write_headers_frame(trevrpc_msquic_stream* stream, const uint8_t* block, size_t block_len) {
    uint8_t prefix[16];
    uint8_t* frame;
    size_t prefix_len = 0;
    size_t frame_len;
    int result;
    if (trevrpc_h3_frame_prefix_build(TREV_H3_FRAME_HEADERS, block_len, prefix, sizeof(prefix), &prefix_len) !=
        TREV_H3_FRAME_OK)
        return -EINVAL;
    if (block_len > SIZE_MAX - prefix_len)
        return -EOVERFLOW;
    frame_len = prefix_len + block_len;
    frame = malloc(frame_len);
    if (frame == NULL)
        return -ENOMEM;
    memcpy(frame, prefix, prefix_len);
    if (block_len != 0)
        memcpy(frame + prefix_len, block, block_len);
    result = h3_write_all(stream, frame, frame_len);
    free(frame);
    return result;
}

static int h3_build_request_headers(
    const h3_endpoint_copy* endpoint, uint8_t* output, size_t capacity, size_t* out_len) {
    static const uint8_t content_type[] = "application/trevrpc";
    static const uint8_t root_path[] = "/";
    const uint8_t* authority = (const uint8_t*)endpoint->value.host;
    size_t authority_len = endpoint->value.host_len;
    const uint8_t* path = endpoint->value.path != NULL ? (const uint8_t*)endpoint->value.path : root_path;
    size_t path_len = endpoint->value.path != NULL ? endpoint->value.path_len : sizeof(root_path) - 1u;
    trevrpc_qpack_static_encoder encoder;
    int result = trevrpc_qpack_static_encoder_init(&encoder, output, capacity);
    if (result == 0)
        result = trevrpc_qpack_static_encoder_put_indexed(&encoder, 20);
    if (result == 0)
        result = trevrpc_qpack_static_encoder_put_indexed(&encoder, 23);
    if (result == 0)
        result = trevrpc_qpack_static_encoder_put_literal_name_reference(&encoder, 0, authority, authority_len);
    if (result == 0 && path_len == 1 && path[0] == '/')
        result = trevrpc_qpack_static_encoder_put_indexed(&encoder, 1);
    else if (result == 0)
        result = trevrpc_qpack_static_encoder_put_literal_name_reference(&encoder, 1, path, path_len);
    if (result == 0)
        result = trevrpc_qpack_static_encoder_put_literal_name_reference(
            &encoder, 44, content_type, sizeof(content_type) - 1u);
    if (result == 0)
        *out_len = encoder.length;
    return result;
}

static int h3_build_status_headers(uint16_t status, uint8_t* output, size_t capacity, size_t* out_len) {
    uint8_t digits[3] = {
        (uint8_t)('0' + status / 100u),
        (uint8_t)('0' + (status / 10u) % 10u),
        (uint8_t)('0' + status % 10u),
    };
    trevrpc_qpack_static_encoder encoder;
    int result = trevrpc_qpack_static_encoder_init(&encoder, output, capacity);
    if (result == 0)
        result = trevrpc_qpack_static_encoder_put_literal_name_reference(&encoder, 24, digits, sizeof(digits));
    if (result == 0)
        *out_len = encoder.length;
    return result;
}

static int h3_send_rejection_response(trevrpc_msquic_stream* stream, uint16_t status) {
    uint8_t block[64];
    uint8_t prefix[16];
    uint8_t frame[sizeof(prefix) + sizeof(block)];
    size_t block_len = 0;
    size_t prefix_len = 0;
    intptr_t written;
    int result = h3_build_status_headers(status, block, sizeof(block), &block_len);
    if (result != 0)
        return result;
    if (trevrpc_h3_frame_prefix_build(TREV_H3_FRAME_HEADERS, block_len, prefix, sizeof(prefix), &prefix_len) !=
        TREV_H3_FRAME_OK)
        return -EINVAL;
    memcpy(frame, prefix, prefix_len);
    memcpy(frame + prefix_len, block, block_len);
    written = trevrpc_msquic_stream_write_fin(stream, frame, prefix_len + block_len);
    if (written < 0)
        return (int)written;
    return (size_t)written == prefix_len + block_len ? 0 : -EPIPE;
}

static int h3_build_response_headers(uint8_t* output, size_t capacity, size_t* out_len) {
    static const uint8_t content_type[] = "application/trevrpc";
    trevrpc_qpack_static_encoder encoder;
    int result = trevrpc_qpack_static_encoder_init(&encoder, output, capacity);
    if (result == 0)
        result = trevrpc_qpack_static_encoder_put_indexed(&encoder, 25);
    if (result == 0)
        result = trevrpc_qpack_static_encoder_put_literal_name_reference(
            &encoder, 44, content_type, sizeof(content_type) - 1u);
    if (result == 0)
        *out_len = encoder.length;
    return result;
}

static int h3_send_request_headers(h3_entry* entry) {
    h3_source* source = entry->source;
    h3_entry* parent;
    uint8_t block[1024];
    size_t block_len = 0;
    int result;
    pthread_mutex_lock(&source->mutex);
    parent = h3_parent_connection_locked(source, entry);
    pthread_mutex_unlock(&source->mutex);
    if (parent == NULL)
        return -ESTALE;
    result = h3_build_request_headers(&parent->endpoint, block, sizeof(block), &block_len);
    return result == 0 ? h3_write_headers_frame(entry->object, block, block_len) : result;
}

static int h3_send_response_headers(h3_entry* entry) {
    uint8_t block[128];
    size_t block_len = 0;
    int result = h3_build_response_headers(block, sizeof(block), &block_len);
    return result == 0 ? h3_write_headers_frame(entry->object, block, block_len) : result;
}

static int h3_build_connect_request_headers(
    const h3_entry* connection, uint8_t* output, size_t capacity, size_t* out_len) {
    static const uint8_t protocol_name[] = ":protocol";
    static const uint8_t draft02_name[] = "sec-webtransport-http3-draft02";
    static const uint8_t one[] = "1";
    static const uint8_t root_path[] = "/";
    const char* protocol = trevrpc_wt_profile_connect_protocol(connection->negotiation.profile);
    const uint8_t* authority = (const uint8_t*)connection->endpoint.value.host;
    size_t authority_len = connection->endpoint.value.host_len;
    const uint8_t* path =
        connection->endpoint.value.path != NULL ? (const uint8_t*)connection->endpoint.value.path : root_path;
    size_t path_len =
        connection->endpoint.value.path != NULL ? connection->endpoint.value.path_len : sizeof(root_path) - 1u;
    trevrpc_qpack_static_encoder encoder;
    int result;
    if (protocol == NULL)
        return -EPROTO;
    result = trevrpc_qpack_static_encoder_init(&encoder, output, capacity);
    if (result == 0)
        result = trevrpc_qpack_static_encoder_put_indexed(&encoder, 15);
    if (result == 0)
        result = trevrpc_qpack_static_encoder_put_literal(
            &encoder, protocol_name, sizeof(protocol_name) - 1u, (const uint8_t*)protocol, strlen(protocol));
    if (result == 0)
        result = trevrpc_qpack_static_encoder_put_indexed(&encoder, 23);
    if (result == 0)
        result = trevrpc_qpack_static_encoder_put_literal_name_reference(&encoder, 0, authority, authority_len);
    if (result == 0 && path_len == 1 && path[0] == '/')
        result = trevrpc_qpack_static_encoder_put_indexed(&encoder, 1);
    else if (result == 0)
        result = trevrpc_qpack_static_encoder_put_literal_name_reference(&encoder, 1, path, path_len);
    if (result == 0 && connection->endpoint.value.origin != NULL)
        result = trevrpc_qpack_static_encoder_put_literal_name_reference(
            &encoder, 90, (const uint8_t*)connection->endpoint.value.origin, connection->endpoint.value.origin_len);
    if (result == 0 && trevrpc_wt_profile_requires_draft02_request_marker(connection->negotiation.profile))
        result = trevrpc_qpack_static_encoder_put_literal(
            &encoder, draft02_name, sizeof(draft02_name) - 1u, one, sizeof(one) - 1u);
    if (result == 0)
        *out_len = encoder.length;
    return result;
}

static int h3_build_connect_response_headers(
    trevrpc_wt_profile_id profile, uint8_t* output, size_t capacity, size_t* out_len) {
    static const uint8_t marker_name[] = "sec-webtransport-http3-draft";
    static const uint8_t marker_value[] = "draft02";
    trevrpc_qpack_static_encoder encoder;
    int result = trevrpc_qpack_static_encoder_init(&encoder, output, capacity);
    if (result == 0)
        result = trevrpc_qpack_static_encoder_put_indexed(&encoder, 25);
    if (result == 0 && trevrpc_wt_profile_requires_draft02_response_marker(profile))
        result = trevrpc_qpack_static_encoder_put_literal(
            &encoder, marker_name, sizeof(marker_name) - 1u, marker_value, sizeof(marker_value) - 1u);
    if (result == 0)
        *out_len = encoder.length;
    return result;
}

static int h3_send_connect_request(h3_entry* stream, const h3_entry* connection) {
    uint8_t block[1024];
    size_t block_len = 0;
    int result = h3_build_connect_request_headers(connection, block, sizeof(block), &block_len);
    return result == 0 ? h3_write_headers_frame(stream->object, block, block_len) : result;
}

static int h3_send_connect_response(h3_entry* stream, trevrpc_wt_profile_id profile) {
    uint8_t block[128];
    size_t block_len = 0;
    int result = h3_build_connect_response_headers(profile, block, sizeof(block), &block_len);
    return result == 0 ? h3_write_headers_frame(stream->object, block, block_len) : result;
}

static int h3_send_wt_stream_prefix(h3_entry* stream, uint64_t session_id) {
    uint8_t prefix[16];
    size_t first_len = 0;
    size_t second_len = 0;
    if (trevrpc_quic_varint_write(prefix, sizeof(prefix), TREV_H3_DEMUX_WEBTRANSPORT_BIDI, &first_len) != 0 ||
        trevrpc_quic_varint_write(prefix + first_len, sizeof(prefix) - first_len, session_id, &second_len) != 0)
        return -EOVERFLOW;
    return h3_write_all(stream->object, prefix, first_len + second_len);
}

static int h3_append_bytes(uint8_t** target, size_t* length, size_t* capacity, const uint8_t* data, size_t len) {
    size_t needed, capacity_new;
    if (len == 0)
        return 0;
    if (data == NULL || len > H3_MAX_PARSE_BUFFER || *length > H3_MAX_PARSE_BUFFER - len)
        return -EMSGSIZE;
    needed = *length + len;
    if (needed > *capacity) {
        capacity_new = *capacity ? *capacity : 4096;
        while (capacity_new < needed) {
            if (capacity_new > H3_MAX_PARSE_BUFFER / 2) {
                capacity_new = H3_MAX_PARSE_BUFFER;
                break;
            }
            capacity_new *= 2;
        }
        {
            uint8_t* replacement = realloc(*target, capacity_new);
            if (replacement == NULL)
                return -ENOMEM;
            *target = replacement;
        }
        *capacity = capacity_new;
    }
    memcpy(*target + *length, data, len);
    *length += len;
    return 0;
}

static int h3_apply_peer_setting(void* context, uint64_t id, uint64_t value) {
    trevrpc_wt_peer_settings* settings = context;
    bool recognized = false;
    return trevrpc_wt_profile_apply_setting(settings, id, value, &recognized);
}

static void h3_filter_peer_profiles(trevrpc_wt_peer_settings* settings, uint32_t profiles) {
    if (!h3_profile_enabled(profiles, TREV_WT_PROFILE_DRAFT_02)) {
        settings->enable_webtransport_draft02 = false;
        settings->seen_enable_webtransport_draft02 = false;
    }
    if (!h3_profile_enabled(profiles, TREV_WT_PROFILE_DRAFT_07)) {
        settings->enable_webtransport_draft07 = false;
        settings->seen_enable_webtransport_draft07 = false;
    }
    if (!h3_profile_enabled(profiles, TREV_WT_PROFILE_DRAFT_14)) {
        settings->enable_webtransport_draft14 = false;
        settings->seen_enable_webtransport_draft14 = false;
    }
    if (!h3_profile_enabled(profiles, TREV_WT_PROFILE_DRAFT_15)) {
        settings->enable_webtransport_draft15 = false;
        settings->seen_enable_webtransport_draft15 = false;
    }
}

static int h3_negotiate_peer_locked(h3_source* source, h3_entry* connection) {
    trevrpc_wt_peer_settings settings;
    trevrpc_wt_role role;
    int result;
    if (!connection->peer_settings_received || !connection->local_control_ready)
        return 0;
    if (!h3_allows_webtransport(connection)) {
        connection->peer_settings_ready = true;
        h3_schedule_profile_waiters_locked(source, connection);
        h3_maybe_emit_connection_ready_locked(connection);
        return 0;
    }
    settings = connection->peer_settings;
    h3_filter_peer_profiles(&settings, connection->endpoint.value.webtransport_profiles);
    role = (connection->side_flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT) != 0 ? TREV_WT_ROLE_CLIENT
                                                                                   : TREV_WT_ROLE_SERVER;
    result = trevrpc_wt_profile_negotiate(
        role, &settings, &connection->capabilities, h3_is_webtransport(connection), &connection->negotiation);
    if (result != 0)
        return -EPROTO;
    connection->peer_settings = settings;
    connection->profile_resolved = connection->negotiation.profile != TREV_WT_PROFILE_NONE;
    connection->peer_settings_ready = true;
    h3_schedule_profile_waiters_locked(source, connection);
    if (h3_is_webtransport(connection) && role == TREV_WT_ROLE_CLIENT && !connection->connect_open_started)
        connection->pending |= H3_PENDING_OPEN_CONNECT;
    h3_maybe_emit_connection_ready_locked(connection);
    h3_signal_locked(source);
    return 0;
}

static size_t h3_max_field_section_size(const h3_entry* connection);

static int h3_parse_control_locked(h3_source* source, h3_entry* entry, bool fin) {
    h3_entry* connection = h3_parent_connection_locked(source, entry);
    size_t max_field_section_size;
    size_t offset = 0;
    if (connection == NULL)
        return -ESTALE;
    max_field_section_size = h3_max_field_section_size(connection);
    while (offset < entry->stream.parse_len) {
        size_t consumed = offset;
        uint64_t type = 0;
        uint64_t payload_len = 0;
        uint64_t* seen_ids = NULL;
        size_t seen_capacity;
        trevrpc_h3_settings_report report;
        trevrpc_h3_settings_status status;
        if (trevrpc_quic_varint_read(entry->stream.parse, entry->stream.parse_len, &consumed, &type) != 0)
            break;
        if (trevrpc_quic_varint_read(entry->stream.parse, entry->stream.parse_len, &consumed, &payload_len) != 0)
            break;
        if (payload_len > max_field_section_size || payload_len > SIZE_MAX)
            return -EMSGSIZE;
        if (payload_len > entry->stream.parse_len - consumed)
            break;
        if (!entry->stream.settings_received && type != TREV_H3_FRAME_SETTINGS)
            return -EPROTO;
        if (entry->stream.settings_received && (type == TREV_H3_FRAME_DATA || type == TREV_H3_FRAME_HEADERS)) {
            entry->stream.frame_unexpected = true;
            return -EPROTO;
        }
        if (type == TREV_H3_FRAME_SETTINGS) {
            if (entry->stream.settings_received)
                return -EPROTO;
            seen_capacity = (size_t)payload_len / 2u + 1u;
            if (seen_capacity > SIZE_MAX / sizeof(*seen_ids))
                return -EOVERFLOW;
            seen_ids = calloc(seen_capacity, sizeof(*seen_ids));
            if (seen_ids == NULL)
                return -ENOMEM;
            memset(&report, 0, sizeof(report));
            {
                trevrpc_wt_peer_settings parsed_settings = {0};
                status = trevrpc_h3_settings_parse(entry->stream.parse + consumed,
                    (size_t)payload_len,
                    max_field_section_size,
                    seen_ids,
                    seen_capacity,
                    h3_allows_webtransport(connection) ? h3_apply_peer_setting : NULL,
                    h3_allows_webtransport(connection) ? &parsed_settings : NULL,
                    &report);
                free(seen_ids);
                if (status != TREV_H3_SETTINGS_OK)
                    return -EPROTO;
                entry->stream.settings_received = true;
                connection->peer_settings = parsed_settings;
                connection->peer_settings_received = true;
                if (h3_negotiate_peer_locked(source, connection) != 0)
                    return -EPROTO;
                h3_signal_locked(source);
            }
        }
        offset = consumed + (size_t)payload_len;
    }
    if (offset != 0) {
        memmove(entry->stream.parse, entry->stream.parse + offset, entry->stream.parse_len - offset);
        entry->stream.parse_len -= offset;
    }
    if (fin)
        return -EPROTO;
    return 0;
}

static bool h3_ascii_equal_case(
    const uint8_t* actual, size_t actual_len, const uint8_t* expected, size_t expected_len) {
    size_t i;
    if (actual_len != expected_len)
        return false;
    for (i = 0; i < actual_len; ++i) {
        uint8_t left = actual[i];
        uint8_t right = expected[i];
        if (left >= 'A' && left <= 'Z')
            left = (uint8_t)(left + ('a' - 'A'));
        if (right >= 'A' && right <= 'Z')
            right = (uint8_t)(right + ('a' - 'A'));
        if (left != right)
            return false;
    }
    return true;
}

static const trevrpc_qpack_field* h3_find_header(const trevrpc_qpack_field_section* section, const char* name) {
    size_t index = 0;
    return trevrpc_qpack_find_first(section, (const uint8_t*)name, strlen(name), 0, &index)
               ? trevrpc_qpack_field_at(section, index)
               : NULL;
}

static bool h3_content_type_valid(trevrpc_qpack_bytes value) {
    static const uint8_t expected[] = "application/trevrpc";
    if (value.len < sizeof(expected) - 1u ||
        !h3_ascii_equal_case(value.data, sizeof(expected) - 1u, expected, sizeof(expected) - 1u))
        return false;
    return value.len == sizeof(expected) - 1u || value.data[sizeof(expected) - 1u] == ';';
}

static bool h3_wt_protocol_valid(trevrpc_qpack_bytes value, trevrpc_wt_profile_id profile) {
    const char* protocol = trevrpc_wt_profile_connect_protocol(profile);
    if (protocol != NULL && trevrpc_qpack_bytes_equal(value, (const uint8_t*)protocol, strlen(protocol)))
        return true;
    return profile == TREV_WT_PROFILE_DRAFT_15 &&
           trevrpc_qpack_bytes_equal(value, (const uint8_t*)"webtransport", sizeof("webtransport") - 1u);
}

static size_t h3_max_field_section_size(const h3_entry* connection) {
    uint64_t configured = connection != NULL ? connection->endpoint.value.max_field_section_size : 0;
    if (configured == 0)
        configured = H3_DEFAULT_MAX_FIELD;
    return configured > SIZE_MAX ? SIZE_MAX : (size_t)configured;
}

static int h3_copy_admission_headers(trevrpc_rpc_transport_event* event, const trevrpc_qpack_field_section* section) {
    size_t bytes = 0;
    size_t index;
    uint8_t* cursor;
    if (event == NULL)
        return 0;
    for (index = 0; index < section->field_count; ++index) {
        const trevrpc_qpack_field* field = &section->fields[index];
        if (field->name.len > SIZE_MAX - bytes || field->value.len > SIZE_MAX - bytes - field->name.len)
            return -EOVERFLOW;
        bytes += field->name.len + field->value.len;
    }
    if (section->field_count > SIZE_MAX / sizeof(*event->admission_headers))
        return -EOVERFLOW;
    if (section->field_count != 0)
        event->admission_headers = calloc(section->field_count, sizeof(*event->admission_headers));
    event->admission_storage = malloc(bytes != 0 ? bytes : 1u);
    if ((section->field_count != 0 && event->admission_headers == NULL) || event->admission_storage == NULL)
        return -ENOMEM;
    cursor = event->admission_storage;
    for (index = 0; index < section->field_count; ++index) {
        const trevrpc_qpack_field* field = &section->fields[index];
        trevrpc_rpc_transport_header_field* copy = &event->admission_headers[index];
        copy->name = cursor;
        copy->name_len = field->name.len;
        memcpy(cursor, field->name.data, field->name.len);
        cursor += field->name.len;
        copy->value = cursor;
        copy->value_len = field->value.len;
        memcpy(cursor, field->value.data, field->value.len);
        cursor += field->value.len;
        if (trevrpc_qpack_field_name_equal(field, (const uint8_t*)":method", 7)) {
            event->admission.method = copy->value;
            event->admission.method_len = copy->value_len;
        } else if (trevrpc_qpack_field_name_equal(field, (const uint8_t*)":path", 5)) {
            event->admission.path = copy->value;
            event->admission.path_len = copy->value_len;
        } else if (trevrpc_qpack_field_name_equal(field, (const uint8_t*)":authority", 10)) {
            event->admission.authority = copy->value;
            event->admission.authority_len = copy->value_len;
        } else if (trevrpc_qpack_field_name_equal(field, (const uint8_t*)"origin", 6)) {
            event->admission.origin = copy->value;
            event->admission.origin_len = copy->value_len;
        }
    }
    event->admission.headers = event->admission_headers;
    event->admission.header_count = section->field_count;
    return 0;
}

static int h3_validate_headers(const uint8_t* encoded,
    size_t encoded_len,
    trevrpc_http3_header_block_kind kind,
    const h3_entry* connection,
    bool connect_control,
    bool* out_connect,
    trevrpc_rpc_transport_event* admission_event) {
    static const uint8_t root_path[] = "/";
    size_t max_field_section_size = h3_max_field_section_size(connection);
    trevrpc_qpack_field_section section;
    trevrpc_qpack_limits limits = {
        .max_encoded_size = max_field_section_size,
        .max_decoded_size = max_field_section_size,
        .max_field_count = max_field_section_size / 32u,
    };
    trevrpc_http3_headers_report report;
    const trevrpc_qpack_field* field;
    trevrpc_qpack_status qpack_status;
    trevrpc_http3_headers_status headers_status;
    int result = -EPROTO;
    if (out_connect != NULL)
        *out_connect = false;
    trevrpc_qpack_field_section_init(&section);
    qpack_status = trevrpc_qpack_static_decode(encoded, encoded_len, &limits, &section);
    if (qpack_status != TREV_QPACK_OK)
        goto done;
    headers_status = trevrpc_http3_headers_validate(&section, kind, &report);
    if (headers_status != TREV_HTTP3_HEADERS_OK)
        goto done;
    if (kind == TREV_HTTP3_HEADERS_TRAILERS) {
        result = connect_control ? -EPROTO : 0;
        goto done;
    }
    if (kind == TREV_HTTP3_HEADERS_RESPONSE) {
        if (report.response_status != 200)
            goto done;
        if (connect_control) {
            if (connection == NULL || !h3_is_webtransport(connection))
                goto done;
            if (trevrpc_wt_profile_requires_draft02_response_marker(connection->negotiation.profile)) {
                field = h3_find_header(&section, "sec-webtransport-http3-draft");
                if (field == NULL ||
                    !trevrpc_qpack_bytes_equal(field->value, (const uint8_t*)"draft02", sizeof("draft02") - 1u))
                    goto done;
            }
            result = 0;
            goto done;
        }
        field = h3_find_header(&section, "content-type");
        if (field != NULL && h3_content_type_valid(field->value))
            result = 0;
        goto done;
    }
    field = h3_find_header(&section, ":method");
    if (field == NULL)
        goto done;
    if (trevrpc_qpack_bytes_equal(field->value, (const uint8_t*)"CONNECT", sizeof("CONNECT") - 1u)) {
        const trevrpc_qpack_field* path;
        if (out_connect != NULL)
            *out_connect = true;
        if (connection == NULL || !h3_allows_webtransport(connection))
            goto done;
        if (!connection->profile_resolved) {
            result = connection->peer_settings_ready ? -EPROTO : -EAGAIN;
            goto done;
        }
        field = h3_find_header(&section, ":protocol");
        if (field == NULL || !h3_wt_protocol_valid(field->value, connection->negotiation.profile))
            goto done;
        field = h3_find_header(&section, ":scheme");
        if (field == NULL || !trevrpc_qpack_bytes_equal(field->value, (const uint8_t*)"https", 5))
            goto done;
        field = h3_find_header(&section, ":authority");
        if (field == NULL || field->value.len == 0)
            goto done;
        path = h3_find_header(&section, ":path");
        if (path == NULL || path->value.len == 0 || path->value.data[0] != '/')
            goto done;
        if (connection->endpoint.value.path != NULL) {
            if (!trevrpc_qpack_bytes_equal(
                    path->value, (const uint8_t*)connection->endpoint.value.path, connection->endpoint.value.path_len))
                goto done;
        } else if (!trevrpc_qpack_bytes_equal(path->value, root_path, sizeof(root_path) - 1u)) {
            goto done;
        }
        if (connection->endpoint.value.origin != NULL) {
            field = h3_find_header(&section, "origin");
            if (field == NULL || !trevrpc_qpack_bytes_equal(field->value,
                                     (const uint8_t*)connection->endpoint.value.origin,
                                     connection->endpoint.value.origin_len))
                goto done;
        }
        if (trevrpc_wt_profile_requires_draft02_request_marker(connection->negotiation.profile)) {
            field = h3_find_header(&section, "sec-webtransport-http3-draft02");
            if (field == NULL || !trevrpc_qpack_bytes_equal(field->value, (const uint8_t*)"1", sizeof("1") - 1u))
                goto done;
        }
        if (out_connect != NULL)
            *out_connect = true;
        result = 0;
        goto done;
    }
    if (!trevrpc_qpack_bytes_equal(field->value, (const uint8_t*)"POST", 4))
        goto done;
    field = h3_find_header(&section, ":scheme");
    if (field == NULL || !trevrpc_qpack_bytes_equal(field->value, (const uint8_t*)"https", 5))
        goto done;
    field = h3_find_header(&section, ":authority");
    if (field == NULL || field->value.len == 0)
        goto done;
    field = h3_find_header(&section, ":path");
    if (field == NULL || field->value.len == 0 || field->value.data[0] != '/')
        goto done;
    if (connection == NULL)
        goto done;
    if (connection->endpoint.value.path != NULL) {
        if (!trevrpc_qpack_bytes_equal(
                field->value, (const uint8_t*)connection->endpoint.value.path, connection->endpoint.value.path_len))
            goto done;
    } else if (!trevrpc_qpack_bytes_equal(field->value, root_path, sizeof(root_path) - 1u)) {
        goto done;
    }
    field = h3_find_header(&section, "content-type");
    if (field == NULL || !h3_content_type_valid(field->value))
        goto done;
    result = 0;
done:
    if (result == 0 && kind == TREV_HTTP3_HEADERS_REQUEST && admission_event != NULL)
        result = h3_copy_admission_headers(admission_event, &section);
    trevrpc_qpack_field_section_release(&section);
    return result;
}

static bool h3_stream_can_emit_readable_locked(const h3_entry* entry) {
#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
    if (entry->stream.ready_bypass)
        return true;
#endif
    return entry->stream.ready_reported;
}

static void h3_emit_stream_readable_locked(h3_source* source, h3_entry* entry) {
    if (!entry->live || !h3_stream_can_emit_readable_locked(entry) || entry->stream.admission_pending ||
        entry->stream.admission_rejected || entry->stream.receive_head == NULL)
        return;
    if (h3_emit_locked(source,
            TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READABLE,
            entry->side_flags,
            0,
            TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
            h3_handle(entry),
            entry->parent,
            0,
            0) != 0) {
        entry->pending |= H3_PENDING_REEMIT_READABLE;
        h3_signal_locked(source);
    }
}

static int h3_queue_receive_locked(h3_source* source, h3_entry* entry, const uint8_t* data, size_t len) {
    trevrpc_rpc_transport_receive* receive;
    trevrpc_rpc_transport_receive* previous_tail;
    size_t alloc_len;
    int result;
    if (source->receive_owned_count >= source->config.max_receive_owned_count ||
        len > source->config.max_receive_owned_bytes - source->receive_owned_bytes)
        return -EAGAIN;
    if (len > SIZE_MAX - sizeof(*receive))
        return -EOVERFLOW;
    alloc_len = sizeof(*receive) + len;
    receive = calloc(1, alloc_len);
    if (receive == NULL)
        return -ENOMEM;
    receive->info.data = receive->data;
    receive->info.data_len = len;
    receive->info.flags = 0;
    if (len)
        memcpy(receive->data, data, len);
    previous_tail = entry->stream.receive_tail;
    if (previous_tail)
        previous_tail->next = receive;
    else
        entry->stream.receive_head = receive;
    entry->stream.receive_tail = receive;
    ++source->receive_owned_count;
    source->receive_owned_bytes += len;
    if (source->receive_owned_count > source->peak_receive_owned_count)
        source->peak_receive_owned_count = source->receive_owned_count;
    if (source->receive_owned_bytes > source->peak_receive_owned_bytes)
        source->peak_receive_owned_bytes = source->receive_owned_bytes;
    h3_emit_stream_readable_locked(source, entry);
    result = 0;
    (void)previous_tail;
    return result;
}

static int h3_parse_rpc_locked(h3_source* source, h3_entry* entry, const uint8_t* data, size_t len) {
    size_t original_len = entry->stream.rpc_len;
    size_t cursor = 0;
    size_t required_count = 0;
    size_t required_bytes = 0;
    size_t available_count;
    size_t available_bytes;
    size_t body_len;
    uint64_t max_frame_size =
        entry->endpoint.value.max_frame_size != 0 ? entry->endpoint.value.max_frame_size : H3_DEFAULT_MAX_FRAME;
    int result = h3_append_bytes(&entry->stream.rpc, &entry->stream.rpc_len, &entry->stream.rpc_cap, data, len);
    if (result != 0)
        return result;

    /* Admission must be all-or-nothing.  If the receive-owned budget is full,
     * leave both the RPC accumulator and its source parser bytes untouched so
     * the same input can be retried after a receive is released. */
    while (entry->stream.rpc_len - cursor >= 4u) {
        body_len = ((size_t)entry->stream.rpc[cursor] << 24) | ((size_t)entry->stream.rpc[cursor + 1] << 16) |
                   ((size_t)entry->stream.rpc[cursor + 2] << 8) | entry->stream.rpc[cursor + 3];
        if ((uint64_t)body_len > max_frame_size || body_len > source->config.max_receive_owned_bytes ||
            body_len > SIZE_MAX - 4u) {
            entry->stream.rpc_len = original_len;
            return -EMSGSIZE;
        }
        if (entry->stream.rpc_len - cursor < body_len + 4u)
            break;
        if (required_count == SIZE_MAX || required_bytes > SIZE_MAX - body_len) {
            entry->stream.rpc_len = original_len;
            return -EOVERFLOW;
        }
        ++required_count;
        required_bytes += body_len;
        cursor += body_len + 4u;
    }
    available_count = source->config.max_receive_owned_count - source->receive_owned_count;
    available_bytes = source->config.max_receive_owned_bytes - source->receive_owned_bytes;
    if (required_count > available_count || required_bytes > available_bytes) {
        entry->stream.rpc_len = original_len;
        return -EAGAIN;
    }

    cursor = 0;
    while (entry->stream.rpc_len - cursor >= 4u) {
        body_len = ((size_t)entry->stream.rpc[cursor] << 24) | ((size_t)entry->stream.rpc[cursor + 1] << 16) |
                   ((size_t)entry->stream.rpc[cursor + 2] << 8) | entry->stream.rpc[cursor + 3];
        if (entry->stream.rpc_len - cursor < body_len + 4u)
            break;
        result = h3_queue_receive_locked(source, entry, entry->stream.rpc + cursor + 4, body_len);
        if (result != 0)
            break;
        cursor += body_len + 4u;
    }
    if (cursor != 0) {
        memmove(entry->stream.rpc, entry->stream.rpc + cursor, entry->stream.rpc_len - cursor);
        entry->stream.rpc_len -= cursor;
    }
    return result;
}

static int h3_init_connect_capsules_locked(h3_entry* entry, const h3_entry* connection) {
    trevrpc_wt_capsule_parser_config config;
    trevrpc_wt_capsule_parse_result result;
    h3_capsule_state* state;
    if (entry->stream.capsules != NULL)
        return 0;
    state = calloc(1, sizeof(*state));
    if (state == NULL)
        return -ENOMEM;
    memset(&config, 0, sizeof(config));
    config.profile = connection->negotiation.profile;
    config.compatibility_flags = connection->negotiation.compatibility_flags;
    config.modern_flow_control_enabled = connection->negotiation.peer_modern_flow_control_intent;
    config.max_unknown_capsule_payload = connection->endpoint.value.max_frame_size != 0
                                             ? connection->endpoint.value.max_frame_size
                                             : H3_DEFAULT_MAX_FRAME;
    config.close_reason_workspace = state->close_reason;
    config.close_reason_capacity = sizeof(state->close_reason);
    result = trevrpc_wt_capsule_parser_init(&state->parser, &config);
    if (result != TREV_WT_CAPSULE_PARSE_NEED_INPUT) {
        free(state);
        return -EPROTO;
    }
    entry->stream.capsules = state;
    return 0;
}

static int h3_handle_connect_capsule_locked(h3_source* source, h3_entry* entry, const trevrpc_wt_capsule_event* event) {
    h3_entry* connection = h3_parent_connection_locked(source, entry);
    if (connection == NULL)
        return -ESTALE;
    switch (event->type) {
    case TREV_WT_CAPSULE_EVENT_CLOSE:
        connection->wt_draining = true;
        connection->pending |= H3_PENDING_SHUTDOWN;
        h3_signal_locked(source);
        return 0;
    case TREV_WT_CAPSULE_EVENT_DRAIN:
        connection->wt_draining = true;
        return 0;
    case TREV_WT_CAPSULE_EVENT_MAX_DATA:
    case TREV_WT_CAPSULE_EVENT_MAX_STREAMS_BIDI:
    case TREV_WT_CAPSULE_EVENT_MAX_STREAMS_UNI:
        return -ENOTSUP;
    }
    return -EPROTO;
}

static int h3_parse_connect_capsules_locked(h3_source* source, h3_entry* entry, const uint8_t* data, size_t len) {
    size_t offset = 0;
    if (entry->stream.capsules == NULL)
        return -EPROTO;
    while (offset < len) {
        trevrpc_wt_capsule_event event;
        size_t consumed = 0;
        trevrpc_wt_capsule_parse_result result = trevrpc_wt_capsule_parser_feed(
            &entry->stream.capsules->parser, data + offset, len - offset, &consumed, &event);
        if (consumed > len - offset)
            return -EPROTO;
        offset += consumed;
        if (result == TREV_WT_CAPSULE_PARSE_EVENT) {
            int event_result = h3_handle_connect_capsule_locked(source, entry, &event);
            if (event_result != 0)
                return event_result;
            continue;
        }
        if (result == TREV_WT_CAPSULE_PARSE_NEED_INPUT) {
            if (offset == len)
                return 0;
            return -EPROTO;
        }
        if (result == TREV_WT_CAPSULE_PARSE_EXCESSIVE_LOAD)
            return -EMSGSIZE;
        if (result == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT)
            return -EFAULT;
        return -EPROTO;
    }
    return 0;
}

static int h3_finish_connect_capsules_locked(h3_source* source, h3_entry* entry) {
    trevrpc_wt_capsule_event event;
    trevrpc_wt_capsule_parse_result result;
    if (entry->stream.capsules == NULL || entry->stream.capsule_finished)
        return entry->stream.capsules != NULL ? 0 : -EPROTO;
    result = trevrpc_wt_capsule_parser_finish(&entry->stream.capsules->parser, &event);
    if (result == TREV_WT_CAPSULE_PARSE_EVENT) {
        int event_result = h3_handle_connect_capsule_locked(source, entry, &event);
        if (event_result != 0)
            return event_result;
        entry->stream.capsule_finished = true;
        return 0;
    }
    if (result == TREV_WT_CAPSULE_PARSE_COMPLETE) {
        entry->stream.capsule_finished = true;
        return 0;
    }
    if (result == TREV_WT_CAPSULE_PARSE_INVALID_ARGUMENT)
        return -EFAULT;
    return -EPROTO;
}

static int h3_select_connection_protocol_locked(
    h3_source* source, h3_entry* connection, h3_entry* stream, bool is_connect) {
    uint32_t selected;
    trevrpc_rpc_transport_handle parent;
    size_t i;
    if (connection == NULL || stream == NULL)
        return -ESTALE;
    if (!h3_is_multiplexed(connection))
        return is_connect == h3_is_webtransport(connection) ? 0 : -EPROTO;
    if (!connection->peer_settings_ready || (is_connect && !connection->profile_resolved))
        return -EAGAIN;
    selected = is_connect ? TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT : TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3;
    connection->endpoint.value.protocol = selected;
    parent = h3_handle(connection);
    for (i = 0; i < source->entry_capacity; ++i) {
        h3_entry* child = source->entries[i].entry;
        if (child != NULL && child->live && child->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM &&
            h3_handle_equal(child->parent, parent))
            child->endpoint.value.protocol = selected;
    }
    stream->endpoint.value.protocol = selected;
    h3_maybe_emit_connection_ready_locked(connection);
    return 0;
}

static int h3_publish_admission_locked(h3_source* source,
    h3_entry* stream,
    const h3_entry* connection,
    trevrpc_rpc_transport_event* event,
    bool is_connect) {
    trevrpc_rpc_transport_event_info info = {0};
    int result;
    event->source = source;
    event->admission_stream = h3_handle(stream);
    event->protocol = is_connect ? TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT : TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3;
    event->admission.protocol = event->protocol;
    event->admission.listener = connection->parent;
    info.kind =
        is_connect ? TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION : TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION;
    info.flags = stream->side_flags;
    info.subject_kind = TREVRPC_RPC_TRANSPORT_OBJECT_NONE;
    info.parent = stream->parent;
    result = h3_publish_locked(source, &info, event, false, NULL);
    if (result == 0)
        stream->stream.admission_pending = true;
    return result;
}

static int h3_parse_stream_locked(h3_source* source, h3_entry* entry, const uint8_t* data, size_t len, bool fin) {
    size_t offset = 0, consumed;
    uint64_t type, payload_len;
    int result = h3_append_bytes(&entry->stream.parse, &entry->stream.parse_len, &entry->stream.parse_cap, data, len);
    if (result != 0)
        return result;
    while (offset < entry->stream.parse_len) {
        consumed = offset;
        if (trevrpc_quic_varint_read(entry->stream.parse, entry->stream.parse_len, &consumed, &type) != 0)
            break;
        if (trevrpc_quic_varint_read(entry->stream.parse, entry->stream.parse_len, &consumed, &payload_len) != 0)
            break;
        if (payload_len > source->config.max_receive_owned_bytes || payload_len > SIZE_MAX - 16u) {
            result = -EMSGSIZE;
            goto parsed;
        }
        if (payload_len > entry->stream.parse_len - consumed)
            break;
        if (type == TREV_H3_FRAME_HEADERS) {
            trevrpc_http3_header_block_kind kind;
            h3_entry* connection = h3_parent_connection_locked(source, entry);
            trevrpc_rpc_transport_event* admission_event = NULL;
            bool is_connect = false;
            bool defer_admission;
            if (connection == NULL) {
                result = -ESTALE;
                goto parsed;
            }
            if (!entry->stream.headers_received) {
                kind = (entry->side_flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER) != 0 ? TREV_HTTP3_HEADERS_REQUEST
                                                                                        : TREV_HTTP3_HEADERS_RESPONSE;
            } else {
                kind = TREV_HTTP3_HEADERS_TRAILERS;
            }
            defer_admission = kind == TREV_HTTP3_HEADERS_REQUEST &&
                              (connection->endpoint.value.flags & TREVRPC_RPC_TRANSPORT_ENDPOINT_DEFER_ADMISSION) != 0;
            if (defer_admission) {
                if (source->ordinary_depth >= source->config.event_capacity) {
                    entry->stream.event_blocked = true;
                    result = -EAGAIN;
                    goto parsed;
                }
                entry->stream.event_blocked = false;
                admission_event = h3_event_node_alloc(source);
                if (admission_event == NULL) {
                    result = -ENOMEM;
                    goto parsed;
                }
            }
            result = h3_validate_headers(entry->stream.parse + consumed,
                (size_t)payload_len,
                kind,
                connection,
                entry->stream.connect_control,
                &is_connect,
                admission_event);
            if (result == 0 && kind == TREV_HTTP3_HEADERS_REQUEST)
                result = h3_select_connection_protocol_locked(source, connection, entry, is_connect);
            if (result != 0) {
                h3_event_node_free(&admission_event);
                goto parsed;
            }
            if (!entry->stream.headers_received) {
                entry->stream.headers_received = true;
                if (is_connect) {
                    /* A validated CONNECT stream owns the hidden session
                     * lifecycle even when duplicate/reservation/capsule setup
                     * rejects it.  Its terminal path must not expose a normal
                     * RPC STREAM_FAILED event. */
                    entry->stream.connect_control = true;
                    trevrpc_rpc_transport_handle stream_handle = h3_handle(entry);
                    if (connection->wt_session_ready ||
                        (connection->resolving_connect.owner != 0 &&
                            !h3_handle_equal(connection->resolving_connect, stream_handle))) {
                        result = -EPROTO;
                        h3_event_node_free(&admission_event);
                        goto parsed;
                    }
                    result = h3_init_connect_capsules_locked(entry, connection);
                    if (result != 0) {
                        h3_event_node_free(&admission_event);
                        goto parsed;
                    }
                    connection->resolving_connect = stream_handle;
                    entry->stream.connect_accept_pending = !defer_admission;
                    if (!defer_admission) {
                        entry->pending |= H3_PENDING_ACCEPT_CONNECT;
                        h3_signal_locked(source);
                    }
                } else if (entry->stream.connect_control) {
                    connection->wt_session_ready = true;
                    h3_maybe_emit_connection_ready_locked(connection);
                    h3_schedule_waiting_wt_locked(source, connection);
                } else if ((entry->side_flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER) != 0 && !defer_admission) {
                    h3_maybe_emit_stream_ready_locked(source, entry);
                }
                if (defer_admission) {
                    result = h3_publish_admission_locked(source, entry, connection, admission_event, is_connect);
                    if (result != 0) {
                        h3_event_node_free(&admission_event);
                        goto parsed;
                    }
                    admission_event = NULL;
                }
            }
        } else if (type == TREV_H3_FRAME_DATA) {
            if (entry->stream.connect_control) {
                result = h3_parse_connect_capsules_locked(
                    source, entry, entry->stream.parse + consumed, (size_t)payload_len);
            } else {
                if (!entry->stream.headers_received) {
                    result = -EPROTO;
                    goto parsed;
                }
                result = h3_parse_rpc_locked(source, entry, entry->stream.parse + consumed, (size_t)payload_len);
            }
            if (result != 0)
                goto parsed;
        } else if (trevrpc_h3_frame_type_is_request_prohibited(type)) {
            result = -EPROTO;
            goto parsed;
        }
        offset = consumed + (size_t)payload_len;
        if (entry->stream.admission_pending)
            break;
    }
parsed:
    if (offset != 0) {
        memmove(entry->stream.parse, entry->stream.parse + offset, entry->stream.parse_len - offset);
        entry->stream.parse_len -= offset;
    }
    if (result != 0)
        return result;
    if (entry->stream.admission_pending)
        return 0;
    if (fin) {
        entry->stream.recv_fin = true;
        if (!entry->stream.headers_received || entry->stream.rpc_len != 0 || entry->stream.parse_len != 0)
            return -EPROTO;
        if (entry->stream.connect_control) {
            result = h3_finish_connect_capsules_locked(source, entry);
            if (result != 0)
                return result;
        } else if (!entry->stream.fin_reported) {
            if (h3_emit_locked(source,
                    TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN,
                    entry->side_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL |
                        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN,
                    0,
                    TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
                    h3_handle(entry),
                    entry->parent,
                    0,
                    0) == 0)
                entry->stream.fin_reported = true;
            else {
                entry->pending |= H3_PENDING_READABLE;
                h3_signal_locked(source);
            }
        }
        h3_maybe_emit_stream_closed_locked(entry);
    }
    return 0;
}

static int h3_remove_classified_uni_prefix_locked(h3_entry* entry, uint64_t expected_type) {
    size_t consumed = 0;
    uint64_t type = 0;
    if (trevrpc_quic_varint_read(entry->stream.parse, entry->stream.parse_len, &consumed, &type) != 0 ||
        type != expected_type)
        return -EPROTO;
    memmove(entry->stream.parse, entry->stream.parse + consumed, entry->stream.parse_len - consumed);
    entry->stream.parse_len -= consumed;
    return 0;
}

static int h3_validate_qpack_encoder_bytes(const uint8_t* data, size_t len, bool fin) {
    size_t i;
    for (i = 0; i < len; ++i) {
        if (data[i] != H3_QPACK_SET_CAPACITY_ZERO)
            return -EPROTO;
    }
    return fin ? -EPROTO : 0;
}

static int h3_classify_stream_locked(h3_source* source, h3_entry* entry, const uint8_t* data, size_t len, bool fin) {
    trevrpc_h3_demux_profile_capabilities profile = {0};
    trevrpc_h3_demux_result classification = {0};
    trevrpc_h3_demux_status status;
    h3_entry* parent;
    const uint8_t* pending_data;
    size_t pending_len;
    uint64_t role_error = 0;
    int result = h3_append_bytes(&entry->stream.parse, &entry->stream.parse_len, &entry->stream.parse_cap, data, len);
    if (result != 0)
        return result;
    parent = h3_parent_connection_locked(source, entry);
    if (parent == NULL)
        return -ESTALE;
    profile.resolved = !h3_is_webtransport(parent) || parent->profile_resolved;
    profile.selected_profile =
        h3_is_webtransport(parent) && parent->profile_resolved ? parent->negotiation.profile : TREV_WT_PROFILE_NONE;
    if (entry->stream.classifier_consumed > entry->stream.parse_len)
        return -EPROTO;
    pending_len = entry->stream.parse_len - entry->stream.classifier_consumed;
    pending_data = pending_len != 0 ? entry->stream.parse + entry->stream.classifier_consumed : NULL;
    status =
        trevrpc_h3_demux_stream_feed(&entry->stream.classifier, &profile, pending_data, pending_len, &classification);
    if (classification.consumed > pending_len)
        return -EPROTO;
    entry->stream.classifier_consumed += classification.consumed;
    if (status == TREV_H3_DEMUX_NEED_MORE && fin)
        status = trevrpc_h3_demux_stream_terminal(&entry->stream.classifier, &profile, &classification);
    if (status == TREV_H3_DEMUX_NEED_MORE || status == TREV_H3_DEMUX_WAIT_PROFILE)
        return 0;
    if (status != TREV_H3_DEMUX_ACTION_READY)
        return -EPROTO;
    if (trevrpc_h3_demux_roles_claim(&parent->peer_roles, classification.action, &role_error) != 0)
        return -EPROTO;
    entry->stream.action = classification.action;
    entry->stream.classified = true;
    if (classification.action != TREV_H3_DEMUX_ACTION_WEBTRANSPORT || parent->wt_session_ready)
        h3_resolve_unresolved_stream_locked(entry);
    switch (classification.action) {
    case TREV_H3_DEMUX_ACTION_CONTROL:
        result = h3_remove_classified_uni_prefix_locked(entry, TREV_H3_DEMUX_STREAM_TYPE_CONTROL);
        if (result == 0) {
            entry->stream.control = true;
            result = h3_parse_control_locked(source, entry, fin);
        }
        return result;
    case TREV_H3_DEMUX_ACTION_QPACK_ENCODER:
        result = h3_remove_classified_uni_prefix_locked(entry, TREV_H3_DEMUX_STREAM_TYPE_QPACK_ENCODER);
        if (result == 0)
            result = h3_validate_qpack_encoder_bytes(entry->stream.parse, entry->stream.parse_len, fin);
        entry->stream.parse_len = 0;
        return result;
    case TREV_H3_DEMUX_ACTION_QPACK_DECODER:
        result = h3_remove_classified_uni_prefix_locked(entry, TREV_H3_DEMUX_STREAM_TYPE_QPACK_DECODER);
        if (result == 0 && entry->stream.parse_len != 0)
            result = -EPROTO;
        entry->stream.parse_len = 0;
        return result == 0 && fin ? -EPROTO : result;
    case TREV_H3_DEMUX_ACTION_REQUEST:
        return h3_parse_stream_locked(source, entry, NULL, 0, fin);
    case TREV_H3_DEMUX_ACTION_UNKNOWN_UNIDIRECTIONAL:
    case TREV_H3_DEMUX_ACTION_RECLAIM_UNCLASSIFIED:
        return -ENOTSUP;
    case TREV_H3_DEMUX_ACTION_WEBTRANSPORT: {
        size_t prefix_len = 0;
        uint64_t stream_type = 0;
        uint64_t session_id = 0;
        uint64_t expected_type = entry->stream.classifier.direction == TREV_H3_DEMUX_UNIDIRECTIONAL
                                     ? TREV_H3_DEMUX_WEBTRANSPORT_UNI
                                     : TREV_H3_DEMUX_WEBTRANSPORT_BIDI;
        if (trevrpc_quic_varint_read(entry->stream.parse, entry->stream.parse_len, &prefix_len, &stream_type) != 0 ||
            trevrpc_quic_varint_read(entry->stream.parse, entry->stream.parse_len, &prefix_len, &session_id) != 0 ||
            stream_type != expected_type || (parent->wt_session_ready && session_id != parent->connect_stream_id))
            return -EPROTO;
        memmove(entry->stream.parse, entry->stream.parse + prefix_len, entry->stream.parse_len - prefix_len);
        entry->stream.parse_len -= prefix_len;
        if (!parent->wt_session_ready) {
            entry->stream.wt_wait_session = true;
            return fin ? -EPROTO : 0;
        }
        entry->stream.headers_received = true;
        h3_maybe_emit_stream_ready_locked(source, entry);
        result = h3_parse_rpc_locked(source, entry, entry->stream.parse, entry->stream.parse_len);
        if (result == 0)
            entry->stream.parse_len = 0;
        return result;
    }
    case TREV_H3_DEMUX_ACTION_NONE:
    default:
        return -EPROTO;
    }
}

static int h3_process_classified_stream_locked(
    h3_source* source, h3_entry* entry, const uint8_t* data, size_t len, bool fin) {
    if (entry->stream.action == TREV_H3_DEMUX_ACTION_CONTROL) {
        int result =
            h3_append_bytes(&entry->stream.parse, &entry->stream.parse_len, &entry->stream.parse_cap, data, len);
        return result == 0 ? h3_parse_control_locked(source, entry, fin) : result;
    }
    if (entry->stream.action == TREV_H3_DEMUX_ACTION_QPACK_ENCODER)
        return h3_validate_qpack_encoder_bytes(data, len, fin);
    if (entry->stream.action == TREV_H3_DEMUX_ACTION_QPACK_DECODER)
        return fin || len != 0 ? -EPROTO : 0;
    if (entry->stream.action == TREV_H3_DEMUX_ACTION_WEBTRANSPORT) {
        h3_entry* parent = h3_parent_connection_locked(source, entry);
        int result;
        if (parent == NULL)
            return -ESTALE;
        if (!parent->wt_session_ready) {
            result =
                h3_append_bytes(&entry->stream.parse, &entry->stream.parse_len, &entry->stream.parse_cap, data, len);
            return result == 0 && fin ? -EPROTO : result;
        }
        if (entry->stream.wt_wait_session) {
            if (entry->stream.classifier.session_id != parent->connect_stream_id)
                return -EPROTO;
            entry->stream.headers_received = true;
            h3_maybe_emit_stream_ready_locked(source, entry);
            result = h3_parse_rpc_locked(source, entry, entry->stream.parse, entry->stream.parse_len);
            if (result != 0)
                return result;
            entry->stream.parse_len = 0;
            entry->stream.wt_wait_session = false;
        } else if (entry->stream.parse_len != 0) {
            result = h3_parse_rpc_locked(source, entry, entry->stream.parse, entry->stream.parse_len);
            if (result != 0)
                return result;
            entry->stream.parse_len = 0;
        }
        result = h3_parse_rpc_locked(source, entry, data, len);
        if (result == -EAGAIN && len != 0) {
            int preserve_result =
                h3_append_bytes(&entry->stream.parse, &entry->stream.parse_len, &entry->stream.parse_cap, data, len);
            if (preserve_result != 0)
                return preserve_result;
        }
        if (result != 0)
            return result;
        if (fin) {
            entry->stream.recv_fin = true;
            if (entry->stream.rpc_len != 0)
                return -EPROTO;
            if (!entry->stream.fin_reported) {
                entry->stream.fin_reported = true;
                (void)h3_emit_locked(source,
                    TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN,
                    entry->side_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL |
                        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLEAN_FIN,
                    0,
                    TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
                    h3_handle(entry),
                    entry->parent,
                    0,
                    0);
            }
            h3_maybe_emit_stream_closed_locked(entry);
        }
        return 0;
    }
    return h3_parse_stream_locked(source, entry, data, len, fin);
}

static void h3_schedule_waiting_wt_locked(h3_source* source, const h3_entry* connection) {
    trevrpc_rpc_transport_handle parent = h3_handle(connection);
    size_t i;
    for (i = 0; i < source->entry_capacity; ++i) {
        h3_entry* entry = source->entries[i].entry;
        if (entry == NULL)
            continue;
        if (entry->live && entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM && entry->parent.owner == parent.owner &&
            entry->parent.slot == parent.slot && entry->parent.generation == parent.generation &&
            entry->stream.wt_wait_session) {
            h3_resolve_unresolved_stream_locked(entry);
            entry->pending |= H3_PENDING_READABLE;
        }
    }
    h3_signal_locked(source);
}

static int h3_accept_connect(h3_source* source, h3_entry* stream) {
    h3_entry* connection;
    trevrpc_msquic_stream* stream_object = NULL;
    trevrpc_rpc_transport_handle stream_handle;
    trevrpc_wt_profile_id profile;
    uint64_t stream_id = 0;
    int result;
    pthread_mutex_lock(&source->mutex);
    stream_handle = h3_handle(stream);
    connection = h3_parent_connection_locked(source, stream);
    if (!stream->live || !stream->stream.connect_accept_pending || stream->stream.connect_accepting ||
        connection == NULL || !h3_is_webtransport(connection) || connection->wt_session_ready ||
        !h3_handle_equal(connection->resolving_connect, stream_handle) ||
        h3_pin_object_locked(source, stream, (void**)&stream_object) != 0) {
        pthread_mutex_unlock(&source->mutex);
        return -EPROTO;
    }
    stream->stream.connect_accepting = true;
    profile = connection->negotiation.profile;
    pthread_mutex_unlock(&source->mutex);

    result = trevrpc_msquic_stream_id(stream_object, &stream_id);
    if (result == 0 && !trevrpc_wt_profile_valid_session_id(stream_id))
        result = -EPROTO;
    if (result == 0)
        result = h3_send_connect_response(stream, profile);

    pthread_mutex_lock(&source->mutex);
    connection = h3_parent_connection_locked(source, stream);
    h3_unpin_object_locked(source, stream);
    stream->stream.connect_accepting = false;
    if (result == -EAGAIN && stream->live && connection != NULL && connection->live &&
        h3_handle_equal(connection->resolving_connect, stream_handle)) {
        stream->stream.connect_accept_pending = true;
        stream->pending |= H3_PENDING_ACCEPT_CONNECT;
        h3_signal_locked(source);
        pthread_mutex_unlock(&source->mutex);
        return 0;
    }
    stream->stream.connect_accept_pending = false;
    if (connection != NULL && h3_handle_equal(connection->resolving_connect, stream_handle))
        connection->resolving_connect = (trevrpc_rpc_transport_handle){0};
    if (result == 0 && stream->live && connection != NULL && connection->live && !connection->wt_session_ready) {
        stream->stream.admission_pending = false;
        connection->connect_stream_id = stream_id;
        connection->wt_session_ready = true;
        stream->stream.headers_sent = true;
        h3_maybe_emit_connection_ready_locked(connection);
        h3_schedule_waiting_wt_locked(source, connection);
    } else if (result == 0) {
        result = -ESTALE;
    }
    pthread_mutex_unlock(&source->mutex);
    return result;
}

static int h3_process_stream_readable(h3_source* source, h3_entry* entry) {
    uint8_t buffer[16384];
    trevrpc_msquic_stream* stream = NULL;
    int result = 0;

    /* Keep the provider stream alive across every read and protocol operation.
     * h3_stream_close can run concurrently with this function and waits for
     * this reference before destroying the provider object. */
    pthread_mutex_lock(&source->mutex);
    if (h3_pin_object_locked(source, entry, (void**)&stream) != 0) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    if (entry->stream.recv_aborted) {
        h3_unpin_object_locked(source, entry);
        pthread_mutex_unlock(&source->mutex);
        return 0;
    }
    h3_maybe_emit_stream_ready_locked(source, entry);
    if (h3_stream_waits_for_resolution_locked(source, entry)) {
        pthread_mutex_unlock(&source->mutex);
        goto done;
    }
    pthread_mutex_unlock(&source->mutex);
    pthread_mutex_lock(&source->mutex);
    if (entry->live && !entry->stream.recv_aborted &&
        ((!entry->stream.classified && (entry->stream.parse_len != entry->stream.classifier_consumed ||
                                           entry->stream.classifier.phase == TREV_H3_DEMUX_WAIT_NEGOTIATED_PROFILE)) ||
            (entry->stream.classified && (entry->stream.parse_len != 0 || entry->stream.wt_wait_session)))) {
        int buffered_result = entry->stream.classified
                                  ? h3_process_classified_stream_locked(source, entry, NULL, 0, false)
                                  : h3_classify_stream_locked(source, entry, NULL, 0, false);
        if (buffered_result == -EAGAIN)
            entry->stream.receive_blocked = true;
        else if (buffered_result == 0)
            entry->stream.receive_blocked = false;
        pthread_mutex_unlock(&source->mutex);
        if (buffered_result != 0) {
            result = buffered_result == -EAGAIN ? 0 : buffered_result;
            goto done;
        }
    } else {
        pthread_mutex_unlock(&source->mutex);
    }
    pthread_mutex_lock(&source->mutex);
    if (h3_stream_waits_for_resolution_locked(source, entry)) {
        pthread_mutex_unlock(&source->mutex);
        goto done;
    }
    pthread_mutex_unlock(&source->mutex);
    result = h3_select_provider_bytes_if_ready(source, entry, stream);
    if (result != 0)
        goto done;
    for (;;) {
        size_t read_capacity = sizeof(buffer);
        bool wait_for_profile = false;
        bool wait_for_session = false;
        bool wait_for_resolution = false;
        pthread_mutex_lock(&source->mutex);
        if (!entry->live || entry->stream.recv_aborted) {
            pthread_mutex_unlock(&source->mutex);
            goto done;
        }
        if (entry->stream.unresolved_counted && !entry->stream.classified)
            read_capacity = 1;
        pthread_mutex_unlock(&source->mutex);
        intptr_t n = trevrpc_msquic_stream_read_protocol_ready(stream, buffer, read_capacity);
        bool fin = n == 0;
        if (n < 0) {
            result = n == TREV_MSQUIC_ERR_TIMEOUT || n == -EAGAIN ? 0 : (int)n;
            goto done;
        }
        pthread_mutex_lock(&source->mutex);
        if (entry->live && !entry->stream.recv_aborted) {
            h3_maybe_emit_stream_ready_locked(source, entry);
            result = entry->stream.classified
                         ? h3_process_classified_stream_locked(source, entry, buffer, n > 0 ? (size_t)n : 0, fin)
                         : h3_classify_stream_locked(source, entry, buffer, n > 0 ? (size_t)n : 0, fin);
            if (!entry->stream.classified && entry->stream.classifier.phase == TREV_H3_DEMUX_WAIT_NEGOTIATED_PROFILE) {
                h3_entry* parent = h3_parent_connection_locked(source, entry);
                wait_for_profile = parent != NULL && !parent->profile_resolved;
            }
            wait_for_session = entry->stream.wt_wait_session;
            wait_for_resolution = h3_stream_waits_for_resolution_locked(source, entry);
        }
        pthread_mutex_unlock(&source->mutex);
        if (result == 0 && (wait_for_profile || wait_for_session || wait_for_resolution))
            goto done;
        if (result == 0)
            result = h3_select_provider_bytes_if_ready(source, entry, stream);
        if (result == -EAGAIN) {
            pthread_mutex_lock(&source->mutex);
            if (entry->live && !entry->stream.recv_aborted)
                entry->stream.receive_blocked = true;
            h3_unpin_object_locked(source, entry);
            pthread_mutex_unlock(&source->mutex);
            return 0;
        }
        pthread_mutex_lock(&source->mutex);
        if (entry->live && !entry->stream.recv_aborted)
            entry->stream.receive_blocked = false;
        pthread_mutex_unlock(&source->mutex);
        if (result == -ENOTSUP)
            (void)trevrpc_msquic_stream_abort_with_error(stream, TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR);
        if (n == 0 || result != 0)
            goto done;
    }

done:
    pthread_mutex_lock(&source->mutex);
    h3_unpin_object_locked(source, entry);
    pthread_mutex_unlock(&source->mutex);
    return result;
}

static int h3_open_connect(h3_source* source, h3_entry* connection) {
    trevrpc_msquic_conn* conn;
    trevrpc_msquic_stream* stream = NULL;
    trevrpc_rpc_transport_handle parent;
    uint32_t side_flags;
    h3_entry* child;
    int result;
    pthread_mutex_lock(&source->mutex);
    if (!connection->live || !h3_is_webtransport(connection) || connection->connect_open_started) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    connection->connect_open_started = true;
    conn = connection->object;
    parent = h3_handle(connection);
    side_flags = connection->side_flags;
    pthread_mutex_unlock(&source->mutex);
    result = trevrpc_msquic_conn_open_stream(conn, &stream);
    if (result != 0)
        return result;
    pthread_mutex_lock(&source->mutex);
    connection = h3_find_locked(source, parent);
    if (connection == NULL || connection->object != conn || connection->wt_session_ready) {
        pthread_mutex_unlock(&source->mutex);
        trevrpc_msquic_stream_close(stream);
        return -ESTALE;
    }
    child = h3_alloc_entry_locked(source, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM, side_flags, stream, parent);
    if (child != NULL) {
        child->stream.action = TREV_H3_DEMUX_ACTION_REQUEST;
        child->stream.classified = true;
        child->stream.connect_control = true;
        result = h3_init_connect_capsules_locked(child, connection);
        if (result != 0)
            h3_detach_entry_locked(child);
    }
    pthread_mutex_unlock(&source->mutex);
    if (child == NULL) {
        trevrpc_msquic_stream_close(stream);
        return -EAGAIN;
    }
    if (result != 0) {
        h3_close_entry_object(child);
        return result;
    }
    result = h3_install_stream(source, child);
    if (result != 0) {
        pthread_mutex_lock(&source->mutex);
        h3_detach_entry_locked(child);
        pthread_mutex_unlock(&source->mutex);
        h3_close_entry_object(child);
    }
    return result;
}

static void h3_process_entry(h3_source* source, h3_entry* entry) {
    uint32_t pending;
    bool connection_shutdown;
    int stream_error = 0;
    uint64_t stream_abort_error = TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR;
    bool stream_object_terminal = false;
    bool peer_reset = false;
    uint64_t peer_error = 0;
    bool live;
    pthread_mutex_lock(&source->mutex);
    pending = entry->pending;
    entry->pending = 0;
    live = entry->live;
    if (live && entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM) {
        stream_error = entry->stream.unresolved_error;
        if (entry->stream.unresolved_abort_error != 0)
            stream_abort_error = entry->stream.unresolved_abort_error;
        entry->stream.unresolved_error = 0;
        entry->stream.unresolved_abort_error = 0;
        if (stream_error != 0)
            pending |= H3_PENDING_TERMINAL;
    }
    pthread_mutex_unlock(&source->mutex);
    if (!live)
        return;
    if (stream_error == -ETIMEDOUT)
        (void)trevrpc_msquic_stream_abort_with_error(entry->object, stream_abort_error);
    connection_shutdown = (pending & H3_PENDING_SHUTDOWN) != 0;
    if (connection_shutdown)
        pending |= H3_PENDING_TERMINAL;
    if (pending & H3_PENDING_CONNECTED) {
        bool send_control;
        int result = 0;
        pthread_mutex_lock(&source->mutex);
        entry->connected = true;
        send_control = !entry->local_control_ready;
        pthread_mutex_unlock(&source->mutex);
        if (send_control)
            result = trevrpc_msquic_conn_feature_snapshot(entry->object, &entry->capabilities);
        if (result == 0 && send_control)
            result = h3_send_control(entry);
        pthread_mutex_lock(&source->mutex);
        if (entry->live && result == 0) {
            entry->local_control_ready = true;
            result = h3_negotiate_peer_locked(source, entry);
            if (result == 0)
                h3_maybe_emit_connection_ready_locked(entry);
            else
                entry->pending |= H3_PENDING_TERMINAL;
        } else if (entry->live) {
            entry->pending |= H3_PENDING_TERMINAL;
        }
        pthread_mutex_unlock(&source->mutex);
        if (result != 0)
            trevrpc_msquic_conn_shutdown_error(entry->object, H3_APP_INTERNAL_ERROR);
    }
    if (pending & H3_PENDING_OPEN_CONNECT) {
        int result = h3_open_connect(source, entry);
        if (result != 0) {
            pthread_mutex_lock(&source->mutex);
            if (entry->live)
                entry->pending |= H3_PENDING_TERMINAL;
            pthread_mutex_unlock(&source->mutex);
            trevrpc_msquic_conn_shutdown_error(entry->object, H3_APP_INTERNAL_ERROR);
        }
    }
    if ((pending & H3_PENDING_ACCEPT_CONNECT) != 0 && entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM)
        stream_error = h3_accept_connect(source, entry);
    if (pending & H3_PENDING_START) {
        bool start_needed;
        pthread_mutex_lock(&source->mutex);
        start_needed = entry->live && !entry->start_reported;
        pthread_mutex_unlock(&source->mutex);
        if (start_needed) {
            uint64_t id = 0;
            h3_entry* connection = NULL;
            uint64_t session_id = 0;
            bool publish_ready = true;
            int result = trevrpc_msquic_stream_start_status(entry->object, &id);
            bool native_started = result == 0;
            if (result == 0) {
                pthread_mutex_lock(&source->mutex);
                connection = h3_parent_connection_locked(source, entry);
                if (connection != NULL)
                    session_id = connection->connect_stream_id;
                pthread_mutex_unlock(&source->mutex);
                if (connection == NULL) {
                    result = -ESTALE;
                } else if (entry->stream.connect_control) {
                    if (!trevrpc_wt_profile_valid_session_id(id)) {
                        result = -EPROTO;
                    } else {
                        pthread_mutex_lock(&source->mutex);
                        connection->connect_stream_id = id;
                        pthread_mutex_unlock(&source->mutex);
                        result = h3_send_connect_request(entry, connection);
                        publish_ready = false;
                    }
                } else if (entry->stream.action == TREV_H3_DEMUX_ACTION_WEBTRANSPORT) {
                    result = h3_send_wt_stream_prefix(entry, session_id);
                } else {
                    result = h3_send_request_headers(entry);
                }
            }
            pthread_mutex_lock(&source->mutex);
            if (!entry->start_reported && result == -EAGAIN && native_started) {
                entry->pending |= H3_PENDING_START;
                h3_signal_locked(source);
            } else if (!entry->start_reported && result != -EAGAIN) {
                entry->start_reported = true;
                if (result == 0) {
                    entry->stream.headers_sent = true;
                    if (publish_ready) {
                        entry->stream.ready_reported = true;
                        (void)h3_emit_locked(source,
                            TREVRPC_RPC_TRANSPORT_EVENT_STREAM_READY,
                            entry->side_flags,
                            0,
                            TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
                            h3_handle(entry),
                            entry->parent,
                            entry->operation_id,
                            0);
                    }
                } else {
                    (void)h3_emit_locked(source,
                        TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED,
                        entry->side_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL,
                        result,
                        TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
                        h3_handle(entry),
                        entry->parent,
                        entry->operation_id,
                        0);
                    h3_mark_dead_locked(entry);
                }
            }
            pthread_mutex_unlock(&source->mutex);
            if (result != 0 && result != -EAGAIN)
                trevrpc_msquic_stream_abort_with_error(entry->object, H3_APP_INTERNAL_ERROR);
        }
    }
    if (pending & H3_PENDING_ACCEPT) {
        for (;;) {
            trevrpc_msquic_stream* stream = NULL;
            trevrpc_msquic_conn* conn = NULL;
            int result;
            if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER) {
                result = trevrpc_msquic_listener_accept_ready(entry->object, &conn);
                if (result != 0)
                    break;
                pthread_mutex_lock(&source->mutex);
                h3_entry* child = h3_alloc_entry_locked(source,
                    TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION,
                    TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
                    conn,
                    h3_handle(entry));
                if (child == NULL) {
                    pthread_mutex_unlock(&source->mutex);
                    trevrpc_msquic_conn_shutdown_error(conn, H3_APP_INTERNAL_ERROR);
                    h3_reschedule_accept(source, entry);
                    break;
                }
                result = h3_endpoint_copy_make(&entry->endpoint.value, &child->endpoint);
                if (result != 0)
                    h3_detach_entry_locked(child);
                pthread_mutex_unlock(&source->mutex);
                if (result == 0)
                    result = h3_install_conn(child);
                if (result != 0) {
                    trevrpc_msquic_conn_shutdown_error(conn, H3_APP_INTERNAL_ERROR);
                    pthread_mutex_lock(&source->mutex);
                    h3_detach_entry_locked(child);
                    pthread_mutex_unlock(&source->mutex);
                    h3_close_entry_object(child);
                    h3_reschedule_accept(source, entry);
                    break;
                }
            } else {
                uint64_t stream_id = 0;
                trevrpc_h3_demux_direction direction;
                result = trevrpc_msquic_conn_accept_stream_ready(entry->object, &stream);
                if (result != 0)
                    break;
                result = trevrpc_msquic_stream_id(stream, &stream_id);
                if (result != 0) {
                    trevrpc_msquic_stream_close(stream);
                    h3_reschedule_accept(source, entry);
                    break;
                }
                direction = trevrpc_h3_demux_stream_id_is_unidirectional(stream_id) ? TREV_H3_DEMUX_UNIDIRECTIONAL
                                                                                    : TREV_H3_DEMUX_BIDIRECTIONAL;
                h3_entry* child = NULL;
                bool quota_rejected = false;
                pthread_mutex_lock(&source->mutex);
                result = h3_admit_peer_stream_locked(source, entry, stream, direction, &child, &quota_rejected);
                pthread_mutex_unlock(&source->mutex);
                if (result != 0) {
                    if (quota_rejected) {
                        (void)trevrpc_msquic_stream_abort_with_error(stream, TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR);
                    }
                    if (child != NULL)
                        h3_close_entry_object(child);
                    else
                        trevrpc_msquic_stream_close(stream);
                    if (quota_rejected)
                        continue;
                    h3_reschedule_accept(source, entry);
                    break;
                }
                result = h3_install_stream(source, child);
                if (result != 0) {
                    pthread_mutex_lock(&source->mutex);
                    h3_detach_entry_locked(child);
                    pthread_mutex_unlock(&source->mutex);
                    h3_close_entry_object(child);
                    h3_reschedule_accept(source, entry);
                    break;
                }
            }
        }
    }
    if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM && (pending & H3_PENDING_REEMIT_READABLE) != 0) {
        pthread_mutex_lock(&source->mutex);
        if (entry->live)
            h3_emit_stream_readable_locked(source, entry);
        pthread_mutex_unlock(&source->mutex);
    }
    if (stream_error == 0 && entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM &&
        (pending & (H3_PENDING_READABLE | H3_PENDING_TERMINAL)) != 0)
        stream_error = h3_process_stream_readable(source, entry);
    if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM && (pending & H3_PENDING_TERMINAL) != 0) {
        trevrpc_msquic_stream_state_snapshot state;
        int state_result = trevrpc_msquic_stream_state(entry->object, &state);
        if (state_result != 0) {
            stream_object_terminal = true;
        } else {
            bool explained_receive_abort = state.peer_send_aborted && state.error_code == TREV_MSQUIC_ERR_CLOSED;
            if (state.peer_send_aborted) {
                uint64_t application_error = 0;
                uint64_t diagnostic_error = 0;
                peer_reset = true;
                peer_error = state.peer_send_error;
                if (stream_error == TREV_MSQUIC_ERR_CLOSED)
                    stream_error = 0;
                pthread_mutex_lock(&source->mutex);
                if (entry->live) {
                    entry->stream.peer_reset = true;
                    entry->stream.peer_reset_error = state.peer_send_error;
                }
                if (entry->live && !entry->stream.fin_reported) {
                    entry->stream.recv_fin = true;
                    h3_decode_peer_reset_locked(
                        source, entry, state.peer_send_error, &application_error, &diagnostic_error);
                    if (h3_emit_with_provider_locked(source,
                            TREVRPC_RPC_TRANSPORT_EVENT_RECEIVE_FIN,
                            entry->side_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL |
                                TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER_RESET,
                            -ECANCELED,
                            TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
                            h3_handle(entry),
                            entry->parent,
                            0,
                            application_error,
                            diagnostic_error) == 0)
                        entry->stream.fin_reported = true;
                    else {
                        entry->pending |= H3_PENDING_TERMINAL;
                        h3_signal_locked(source);
                    }
                    h3_maybe_emit_stream_closed_locked(entry);
                }
                pthread_mutex_unlock(&source->mutex);
            }
            if (state.peer_receive_aborted) {
                uint64_t application_error = 0;
                uint64_t diagnostic_error = 0;
                peer_reset = true;
                peer_error = state.peer_receive_error;
                pthread_mutex_lock(&source->mutex);
                if (entry->live) {
                    entry->stream.send_fin = true;
                    entry->stream.send_aborted = true;
                    entry->stream.peer_reset = true;
                    if (!state.peer_send_aborted)
                        entry->stream.peer_reset_error = state.peer_receive_error;
                    if (!entry->stream.send_stop_reported) {
                        h3_decode_peer_reset_locked(
                            source, entry, state.peer_receive_error, &application_error, &diagnostic_error);
                        uint32_t event_flags = entry->side_flags & ~(TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL |
                                                                       TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER);
                        if (h3_emit_with_provider_locked(source,
                                TREVRPC_RPC_TRANSPORT_EVENT_SEND_STOPPED,
                                event_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL |
                                    TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER_RESET,
                                -ECANCELED,
                                TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
                                h3_handle(entry),
                                entry->parent,
                                0,
                                application_error,
                                diagnostic_error) == 0)
                            entry->stream.send_stop_reported = true;
                        else {
                            entry->pending |= H3_PENDING_TERMINAL;
                            h3_signal_locked(source);
                        }
                    }
                    h3_maybe_emit_stream_closed_locked(entry);
                }
                pthread_mutex_unlock(&source->mutex);
            }
            if (state.shutdown_complete || (state.error_code != 0 && !explained_receive_abort))
                stream_object_terminal = true;
        }
    }
    if (pending & H3_PENDING_SEND_COMPLETE) {
        trevrpc_msquic_send_completion* completion;
        uint64_t operation_id;
        int result;
        pthread_mutex_lock(&source->mutex);
        completion = entry->stream.pending_completion;
        operation_id = entry->stream.pending_operation_id;
        pthread_mutex_unlock(&source->mutex);
        if (completion != NULL) {
            result = trevrpc_msquic_send_completion_status(completion);
            if (result != -EAGAIN) {
                pthread_mutex_lock(&source->mutex);
                if (entry->stream.pending_completion == completion) {
                    int event_result = h3_emit_locked(source,
                        TREVRPC_RPC_TRANSPORT_EVENT_SEND_COMPLETE,
                        entry->side_flags,
                        result,
                        TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
                        h3_handle(entry),
                        entry->parent,
                        operation_id,
                        0);
                    if (event_result == 0) {
                        entry->stream.pending_completion = NULL;
                        entry->stream.pending_operation_id = 0;
                        if (source->pending_send_count != 0)
                            --source->pending_send_count;
                        if (source->pending_send_bytes >= entry->stream.pending_send_bytes)
                            source->pending_send_bytes -= entry->stream.pending_send_bytes;
                        entry->stream.pending_send_bytes = 0;
                        /* A clean terminal event is published only after this
                         * accepted completion, preserving queue order. */
                        h3_maybe_emit_stream_closed_locked(entry);
                    } else {
                        entry->pending |= H3_PENDING_SEND_COMPLETE;
                        h3_signal_locked(source);
                    }
                }
                bool release_completion = entry->stream.pending_completion != completion;
                pthread_mutex_unlock(&source->mutex);
                if (release_completion)
                    trevrpc_msquic_send_completion_free(completion);
            }
        }
    }
    if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM && entry->stream.admission_rejected &&
        ((pending & H3_PENDING_TERMINAL) != 0 || stream_error != 0 || stream_object_terminal)) {
        pthread_mutex_lock(&source->mutex);
        h3_entry* parent = h3_parent_connection_locked(source, entry);
        trevrpc_rpc_transport_handle stream_handle = h3_handle(entry);
        if (parent != NULL && h3_handle_equal(parent->resolving_connect, stream_handle))
            parent->resolving_connect = (trevrpc_rpc_transport_handle){0};
        h3_detach_entry_locked(entry);
        pthread_mutex_unlock(&source->mutex);
        h3_close_entry_object(entry);
        return;
    }
    if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM && entry->stream.connect_control &&
        (stream_error != 0 || stream_object_terminal)) {
        h3_entry* parent;
        trevrpc_msquic_conn* conn = NULL;
        trevrpc_rpc_transport_handle stream_handle = h3_handle(entry);
        pthread_mutex_lock(&source->mutex);
        parent = h3_parent_connection_locked(source, entry);
        if (parent != NULL && h3_handle_equal(parent->resolving_connect, stream_handle))
            parent->resolving_connect = (trevrpc_rpc_transport_handle){0};
        if (parent != NULL && parent->live) {
            parent->pending |= H3_PENDING_SHUTDOWN;
            if (stream_error != 0)
                conn = parent->object;
            h3_signal_locked(source);
        }
        h3_detach_entry_locked(entry);
        pthread_mutex_unlock(&source->mutex);
        if (conn != NULL)
            trevrpc_msquic_conn_shutdown_error(conn, H3_APP_INTERNAL_ERROR);
        h3_close_entry_object(entry);
        return;
    }
    if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM && stream_error != -ETIMEDOUT &&
        entry->stream.action != TREV_H3_DEMUX_ACTION_REQUEST &&
        entry->stream.action != TREV_H3_DEMUX_ACTION_WEBTRANSPORT && (stream_error != 0 || stream_object_terminal)) {
        h3_entry* parent;
        trevrpc_msquic_conn* conn = NULL;
        uint64_t app_error = 0;
        pthread_mutex_lock(&source->mutex);
        parent = h3_parent_connection_locked(source, entry);
        if (entry->stream.action == TREV_H3_DEMUX_ACTION_CONTROL || entry->stream.action == TREV_H3_DEMUX_ACTION_NONE)
            app_error = entry->stream.frame_unexpected ? H3_APP_FRAME_UNEXPECTED : H3_APP_CLOSED_CRITICAL_STREAM;
        else if (entry->stream.action == TREV_H3_DEMUX_ACTION_QPACK_ENCODER)
            app_error = stream_error != 0 ? H3_APP_QPACK_ENCODER_STREAM_ERROR : H3_APP_CLOSED_CRITICAL_STREAM;
        else if (entry->stream.action == TREV_H3_DEMUX_ACTION_QPACK_DECODER)
            app_error = stream_error != 0 ? H3_APP_QPACK_DECODER_STREAM_ERROR : H3_APP_CLOSED_CRITICAL_STREAM;
        h3_detach_entry_locked(entry);
        if (app_error != 0 && parent != NULL && parent->live) {
            conn = parent->object;
            parent->pending |= H3_PENDING_TERMINAL;
            h3_signal_locked(source);
        }
        pthread_mutex_unlock(&source->mutex);
        if (conn != NULL)
            trevrpc_msquic_conn_shutdown_error(conn, app_error);
        h3_close_entry_object(entry);
        return;
    }
    if (stream_error != 0 || stream_object_terminal ||
        ((pending & H3_PENDING_TERMINAL) != 0 &&
            (entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM || entry->terminal_requested))) {
        pthread_mutex_lock(&source->mutex);
        if (stream_error == 0 && stream_object_terminal && !peer_reset &&
            entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM) {
            h3_maybe_emit_stream_closed_locked(entry);
            if (entry->close_reported) {
                pthread_mutex_unlock(&source->mutex);
                return;
            }
        }
        if (entry->send_event != NULL || entry->stream.pending_completion != NULL) {
            entry->pending |= H3_PENDING_TERMINAL;
            pthread_mutex_unlock(&source->mutex);
            return;
        }
        if (!entry->close_reported) {
            uint32_t terminal_flags = entry->side_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL;
            int terminal_status = stream_error != 0 ? stream_error : -EPIPE;
            uint64_t application_error = 0;
            uint64_t diagnostic_error = 0;
            bool failure_before_ready =
                !entry->terminal_requested && (stream_error != 0 || stream_object_terminal || connection_shutdown);
            entry->terminal_requested = true;
            entry->close_reported = true;
            if (peer_reset) {
                terminal_flags |= TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER_RESET;
                terminal_status = -ECANCELED;
                h3_decode_peer_reset_locked(source, entry, peer_error, &application_error, &diagnostic_error);
            }
            if (h3_emit_with_provider_locked(source,
                    entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION
                        ? (entry->ready_reported || !failure_before_ready
                                  ? TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED
                                  : TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_FAILED)
                        : (entry->stream.ready_reported || !failure_before_ready
                                  ? TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED
                                  : TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED),
                    terminal_flags,
                    terminal_status,
                    entry->kind,
                    h3_handle(entry),
                    entry->parent,
                    entry->operation_id,
                    application_error,
                    diagnostic_error) == 0)
                h3_mark_dead_locked(entry);
            else {
                entry->close_reported = false;
                entry->pending |= H3_PENDING_TERMINAL;
                h3_signal_locked(source);
            }
        }
        pthread_mutex_unlock(&source->mutex);
    }
}

static bool h3_has_pending_locked(const h3_source* source) {
    size_t i;
    for (i = 0; i < source->entry_capacity; ++i) {
        if (source->entries[i].entry != NULL && source->entries[i].entry->live &&
            source->entries[i].entry->pending != 0)
            return true;
    }
    return false;
}

static bool h3_has_multiplexed_request_retry_locked(h3_source* source) {
    size_t i;
    for (i = 0; i < source->entry_capacity; ++i) {
        h3_entry* entry = source->entries[i].entry;
        h3_entry* parent;
        if (entry == NULL || !entry->live || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM ||
            (entry->pending & H3_PENDING_READABLE) == 0 || !entry->stream.classified || entry->stream.parse_len == 0)
            continue;
        parent = h3_parent_connection_locked(source, entry);
        if (parent != NULL && h3_is_multiplexed(parent) && parent->peer_settings_ready)
            return true;
    }
    return false;
}

static void h3_process_pending(h3_source* source) {
    size_t i, count;
    pthread_mutex_lock(&source->mutex);
    count = source->entry_capacity;
    pthread_mutex_unlock(&source->mutex);
    for (i = 0; i < count; ++i) {
        h3_entry* entry;
        pthread_mutex_lock(&source->mutex);
        entry = source->entries[i].entry;
        if (entry == NULL || entry->pending == 0 || h3_pin_entry_locked(entry) != 0)
            entry = NULL;
        pthread_mutex_unlock(&source->mutex);
        if (entry == NULL)
            continue;
        h3_entry* previous_processing_entry = h3_processing_entry;
        h3_processing_entry = entry;
        h3_process_entry(source, entry);
        h3_processing_entry = previous_processing_entry;
        pthread_mutex_lock(&source->mutex);
        h3_unpin_entry_locked(source, entry);
        h3_collect_reclaimable_locked(source);
        pthread_mutex_unlock(&source->mutex);
        h3_reap_pending(source);
    }
    pthread_mutex_lock(&source->mutex);
    h3_maybe_publish_stopped_locked(source);
    pthread_mutex_unlock(&source->mutex);
}

static int h3_get_wake_source(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_wake* wake) {
    h3_source* source = h3_from_base(transport);
    if (wake == NULL)
        return -EINVAL;
    wake->kind = TREVRPC_RPC_TRANSPORT_WAKE_SOURCE_POSIX_FD;
    wake->flags = TREVRPC_RPC_TRANSPORT_WAKE_FLAG_BORROWED | TREVRPC_RPC_TRANSPORT_WAKE_FLAG_LEVEL_TRIGGERED;
    wake->native_handle = source->wake_read_fd;
    return 0;
}

static int h3_poll_timeout_ms(trevrpc_rpc_transport* transport) {
    h3_source* source = h3_from_base(transport);
    uint64_t now = 0;
    int timeout = -1;
    pthread_mutex_lock(&source->mutex);
    if (h3_now_nanos(source, &now) == 0)
        timeout = h3_next_unresolved_timeout_locked(source, now);
    pthread_mutex_unlock(&source->mutex);
    return timeout;
}

static int h3_next_event(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_event** out_event) {
    h3_source* source = h3_from_base(transport);
    trevrpc_rpc_transport_event* event;
    if (out_event == NULL)
        return -EINVAL;
    *out_event = NULL;
    {
        uint64_t now = 0;
        pthread_mutex_lock(&source->mutex);
        if (h3_now_nanos(source, &now) == 0)
            h3_expire_unresolved_streams_locked(source, now);
        pthread_mutex_unlock(&source->mutex);
    }
    h3_process_pending(source);
    pthread_mutex_lock(&source->mutex);
    bool retry_multiplexed_request = source->event_head == NULL && h3_has_multiplexed_request_retry_locked(source);
    pthread_mutex_unlock(&source->mutex);
    if (retry_multiplexed_request)
        h3_process_pending(source);
    pthread_mutex_lock(&source->mutex);
    event = source->event_head;
    if (event == NULL) {
        if (!h3_has_pending_locked(source) &&
            !(source->state == TREVRPC_RPC_TRANSPORT_STATE_STOPPING && !source->stop_event_published)) {
            source->wake_armed = false;
            h3_drain_wake(source);
        }
        pthread_mutex_unlock(&source->mutex);
        return -EAGAIN;
    }
    source->event_head = event->next;
    if (source->event_head == NULL) {
        source->event_tail = NULL;
        if (!h3_has_pending_locked(source) &&
            !(source->state == TREVRPC_RPC_TRANSPORT_STATE_STOPPING && !source->stop_event_published)) {
            source->wake_armed = false;
            h3_drain_wake(source);
        }
    }
    event->next = NULL;
    --source->event_depth;
    if (!event->mandatory) {
        assert(source->ordinary_depth != 0);
        --source->ordinary_depth;
        h3_schedule_event_retries_locked(source);
    }
    ++source->events_dequeued;
    if (event->terminal_entry != NULL) {
        h3_detach_entry_locked(event->terminal_entry);
        h3_collect_reclaimable_locked(source);
    }
    pthread_mutex_unlock(&source->mutex);
    h3_reap_pending(source);
    *out_event = event;
    return 0;
}

static int h3_event_get_info(const trevrpc_rpc_transport_event* event, trevrpc_rpc_transport_event_info* info) {
    if (event == NULL || info == NULL)
        return -EINVAL;
    *info = event->info;
    return 0;
}

static int h3_event_get_admission_info(
    const trevrpc_rpc_transport_event* event, trevrpc_rpc_transport_admission_info* info) {
    if (event == NULL || info == NULL)
        return -EINVAL;
    if (event->info.kind != TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION &&
        event->info.kind != TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION)
        return -ENOTSUP;
    *info = event->admission;
    return 0;
}

static int h3_event_get_protocol_info(
    const trevrpc_rpc_transport_event* event, trevrpc_rpc_transport_event_protocol_info* info) {
    if (event == NULL || info == NULL)
        return -EINVAL;
    if (event->protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_AUTO)
        return -ENOTSUP;
    info->protocol = event->protocol;
    return 0;
}

static int h3_admission_respond(const trevrpc_rpc_transport_event* event_const, uint16_t status) {
    trevrpc_rpc_transport_event* event = (trevrpc_rpc_transport_event*)event_const;
    h3_source* source;
    h3_entry* stream;
    h3_entry* pinned_stream = NULL;
    trevrpc_msquic_stream* object = NULL;
    bool accept;
    bool connect;
    bool response_attempted = false;
    int result = 0;
    if (event == NULL || (status != 200 && (status < 400 || status > 599)))
        return -EINVAL;
    if (event->info.kind != TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION &&
        event->info.kind != TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION)
        return -ENOTSUP;
    source = event->source;
    if (source == NULL)
        return -ESTALE;
    pthread_mutex_lock(&source->mutex);
    if (event->admission_decided) {
        pthread_mutex_unlock(&source->mutex);
        return -EALREADY;
    }
    event->admission_decided = true;
    stream = h3_find_locked(source, event->admission_stream);
    if (stream == NULL || stream->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM || !stream->stream.admission_pending) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    accept = status == 200;
    connect = event->info.kind == TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION;
    if (accept) {
        if (connect) {
            stream->stream.connect_accept_pending = true;
            stream->pending |= H3_PENDING_ACCEPT_CONNECT | H3_PENDING_READABLE;
            h3_signal_locked(source);
            pthread_mutex_unlock(&source->mutex);
            return 0;
        }
#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
        if (source->capture_acceptances) {
            ++source->acceptance_count;
            stream->stream.admission_pending = false;
            stream->stream.headers_sent = true;
            h3_maybe_emit_stream_ready_locked(source, stream);
            stream->pending |= H3_PENDING_READABLE;
            h3_signal_locked(source);
            pthread_mutex_unlock(&source->mutex);
            return 0;
        }
#endif
        if (h3_pin_object_locked(source, stream, (void**)&object) != 0) {
#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
            if (stream->object == NULL) {
                stream->stream.admission_pending = false;
                stream->stream.headers_sent = true;
                h3_maybe_emit_stream_ready_locked(source, stream);
                stream->pending |= H3_PENDING_READABLE;
                h3_signal_locked(source);
                pthread_mutex_unlock(&source->mutex);
                return 0;
            }
#endif
            stream->stream.admission_rejected = true;
            stream->pending |= H3_PENDING_TERMINAL;
            h3_signal_locked(source);
            pthread_mutex_unlock(&source->mutex);
            return -ESTALE;
        }
        pinned_stream = stream;
        stream->stream.headers_sending = true;
        pthread_mutex_unlock(&source->mutex);

        result = h3_send_response_headers(stream);
        if (result != 0)
            (void)trevrpc_msquic_stream_abort_with_error(object, H3_APP_INTERNAL_ERROR);

        pthread_mutex_lock(&source->mutex);
        stream = h3_find_locked(source, event->admission_stream);
        h3_unpin_object_locked(source, pinned_stream);
        if (stream != NULL)
            stream->stream.headers_sending = false;
        if (result == 0 && stream != NULL) {
            stream->stream.admission_pending = false;
            stream->stream.headers_sent = true;
            h3_maybe_emit_stream_ready_locked(source, stream);
            stream->pending |= H3_PENDING_READABLE;
            h3_signal_locked(source);
        } else if (stream != NULL) {
            stream->stream.admission_rejected = true;
            stream->pending |= H3_PENDING_TERMINAL;
            h3_signal_locked(source);
        }
        pthread_mutex_unlock(&source->mutex);
        return result;
    }
    stream->stream.admission_rejected = true;
    if (h3_pin_object_locked(source, stream, (void**)&object) != 0)
        object = NULL;
    else
        pinned_stream = stream;
    pthread_mutex_unlock(&source->mutex);
#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
    pthread_mutex_lock(&source->mutex);
    if (source->capture_rejections) {
        source->last_rejection_status = status;
        ++source->rejection_count;
        response_attempted = true;
    }
    pthread_mutex_unlock(&source->mutex);
#endif
    if (!response_attempted && object != NULL) {
        response_attempted = true;
        result = h3_send_rejection_response(object, status);
    }
    pthread_mutex_lock(&source->mutex);
    stream = h3_find_locked(source, event->admission_stream);
    if (pinned_stream != NULL)
        h3_unpin_object_locked(source, pinned_stream);
    if (stream != NULL) {
        stream->stream.send_fin = result == 0;
        stream->pending |= H3_PENDING_TERMINAL;
        h3_signal_locked(source);
    }
    pthread_mutex_unlock(&source->mutex);
    return response_attempted ? result : -ESTALE;
}

static void h3_event_release(trevrpc_rpc_transport_event* event) {
    if (event == NULL)
        return;
    if (!event->admission_decided && (event->info.kind == TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION ||
                                         event->info.kind == TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION))
        (void)h3_admission_respond(event, 500);
    h3_event_node_free(&event);
}
static int h3_receive_get_info(const trevrpc_rpc_transport_receive* receive, trevrpc_rpc_transport_receive_info* info) {
    if (receive == NULL || info == NULL)
        return -EINVAL;
    *info = receive->info;
    return 0;
}
static void h3_receive_release(trevrpc_rpc_transport_receive* receive) {
    free(receive);
}

static int h3_get_diagnostics(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_diagnostics* d) {
    h3_source* source = h3_from_base(transport);
    if (d == NULL)
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    memset(d, 0, sizeof(*d));
    d->state = source->state;
    d->terminal_status = source->terminal_status;
    d->event_capacity = source->config.event_capacity;
    d->queue_depth = (uint32_t)source->event_depth;
    d->ordinary_queue_depth = (uint32_t)source->ordinary_depth;
    d->events_enqueued = source->events_enqueued;
    d->events_dequeued = source->events_dequeued;
    d->events_rejected = source->events_rejected;
    d->receive_owned_count = source->receive_owned_count;
    d->peak_receive_owned_count = source->peak_receive_owned_count;
    d->receive_owned_bytes = source->receive_owned_bytes;
    d->peak_receive_owned_bytes = source->peak_receive_owned_bytes;
    d->pending_send_bytes = source->pending_send_bytes;
    d->pending_send_count = source->pending_send_count;
    d->live_listeners = source->live_listeners;
    d->live_connections = source->live_connections;
    d->live_streams = source->live_streams;
    d->wake_signals = source->wake_signals;
    d->wake_write_eagain = source->wake_write_eagain;
    d->wake_failures = source->wake_failures;
    d->mandatory_reservations = atomic_load_explicit(&source->mandatory_reservations, memory_order_relaxed);
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

static int h3_validate_endpoint_sizes(const trevrpc_rpc_transport_endpoint_config* config) {
    if (config->max_pending_send_bytes > SIZE_MAX || config->max_pending_receive_bytes > SIZE_MAX ||
        config->max_frame_size > SIZE_MAX || config->unresolved_stream_bytes > SIZE_MAX)
        return -EOVERFLOW;
    return 0;
}

static void h3_msquic_config(const trevrpc_rpc_transport_endpoint_config* c, trevrpc_msquic_config* out) {
    memset(out, 0, sizeof(*out));
    out->alpn = (const char*)(c->alpn ? c->alpn : (const uint8_t*)"h3");
    out->alpn_len = c->alpn ? c->alpn_len : 2;
    out->cert_file = c->cert_file;
    out->key_file = c->key_file;
    out->ca_cert_file = c->ca_cert_file;
    out->skip_certificate_validation = (c->flags & TREVRPC_RPC_TRANSPORT_ENDPOINT_SKIP_CERTIFICATE_VALIDATION) != 0;
    out->max_idle_timeout_ms = c->max_idle_timeout_ms;
    out->keep_alive_ms = c->keep_alive_ms;
    out->peer_bidi_stream_count = c->peer_bidi_stream_count;
    out->peer_unidi_stream_count = 3;
    out->max_pending_send_bytes = c->max_pending_send_bytes;
    out->max_pending_send_count = c->max_pending_send_count;
    out->max_frame_size = c->max_frame_size ? (size_t)c->max_frame_size : (size_t)H3_DEFAULT_MAX_FRAME;
    out->stream_recv_window = c->stream_recv_window;
    out->conn_flow_control_window = c->conn_flow_control_window;
    out->send_buffering_enabled = 1;
}

static int h3_endpoint_listen_impl(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_endpoint_config* config,
    trevrpc_msquic_accept_dispatch dispatch,
    void* dispatch_context,
    trevrpc_msquic_context_destroy dispatch_context_destroy,
    trevrpc_rpc_transport_handle* out) {
    static const trevrpc_msquic_alpn shared_alpns[] = {
        {.alpn = "trevrpc/1", .alpn_len = sizeof("trevrpc/1") - 1u},
        {.alpn = "h3", .alpn_len = sizeof("h3") - 1u},
    };
    h3_source* source = h3_from_base(transport);
    trevrpc_msquic_config ms;
    trevrpc_msquic_alpn alpn;
    const trevrpc_msquic_alpn* alpns;
    size_t alpn_count;
    trevrpc_msquic_listener* listener = NULL;
    h3_endpoint_copy endpoint;
    h3_entry* entry;
    int result;
    trevrpc_msquic_feature_request features;
    trevrpc_msquic_receive_policy policy = {0};
    if (config == NULL || out == NULL ||
        (config->protocol != TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3 &&
            config->protocol != TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT &&
            config->protocol != TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED)) {
        if (dispatch_context_destroy != NULL)
            dispatch_context_destroy(dispatch_context);
        return -ENOTSUP;
    }
    if ((config->protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED) != (dispatch != NULL) ||
        (dispatch == NULL) != (dispatch_context_destroy == NULL)) {
        if (dispatch_context_destroy != NULL)
            dispatch_context_destroy(dispatch_context);
        return -EINVAL;
    }
    if ((config->protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT ||
            config->protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED) &&
        config->unresolved_stream_bytes != 0 &&
        config->unresolved_stream_bytes < trevrpc_msquic_receive_minimum_raw_bytes()) {
        if (dispatch_context_destroy != NULL)
            dispatch_context_destroy(dispatch_context);
        return -EINVAL;
    }
    result = h3_validate_endpoint_sizes(config);
    if (result != 0) {
        if (dispatch_context_destroy != NULL)
            dispatch_context_destroy(dispatch_context);
        return result;
    }
    result = h3_endpoint_copy_make(config, &endpoint);
    if (result != 0) {
        if (dispatch_context_destroy != NULL)
            dispatch_context_destroy(dispatch_context);
        return result;
    }
    h3_msquic_config(&endpoint.value, &ms);
    alpn.alpn = ms.alpn;
    alpn.alpn_len = ms.alpn_len;
    features = trevrpc_msquic_default_h3_features();
    policy.max_stream_owned_bytes = config->max_pending_receive_bytes;
    policy.max_stream_owned_count = config->max_pending_receive_count;
    policy.max_connection_owned_bytes = config->max_pending_receive_bytes;
    policy.max_connection_owned_count = config->max_pending_receive_count;
    if (config->protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT ||
        config->protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED) {
        policy.max_undecided_owned_bytes = config->unresolved_stream_bytes;
    }
    alpns = config->protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED ? shared_alpns : &alpn;
    alpn_count = config->protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED ? 2u : 1u;
    pthread_mutex_lock(&source->mutex);
    entry = h3_alloc_entry_locked(source,
        TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL,
        NULL,
        (trevrpc_rpc_transport_handle){0});
    pthread_mutex_unlock(&source->mutex);
    if (entry == NULL) {
        h3_endpoint_free(&endpoint);
        if (dispatch_context_destroy != NULL)
            dispatch_context_destroy(dispatch_context);
        return -EAGAIN;
    }
    result = trevrpc_msquic_listen_alpns_features_with_dispatch(endpoint.value.host,
        endpoint.value.port,
        &ms,
        alpns,
        alpn_count,
        &features,
        &policy,
        dispatch,
        dispatch_context,
        dispatch_context_destroy,
        &listener);
    if (result != 0) {
        pthread_mutex_lock(&source->mutex);
        h3_detach_entry_locked(entry);
        pthread_mutex_unlock(&source->mutex);
        h3_close_entry_object(entry);
        h3_endpoint_free(&endpoint);
        return result;
    }
    pthread_mutex_lock(&source->mutex);
    entry->object = listener;
    entry->endpoint = endpoint;
    memset(&endpoint, 0, sizeof(endpoint));
    pthread_mutex_unlock(&source->mutex);
    result = trevrpc_msquic_listener_set_observer(listener, h3_listener_observer, entry);
    if (result == 0) {
        pthread_mutex_lock(&source->mutex);
        if (entry->live) {
            entry->observer_installed = true;
            entry->pending |= H3_PENDING_ACCEPT;
            h3_signal_locked(source);
        }
        pthread_mutex_unlock(&source->mutex);
    }
    if (result != 0) {
        pthread_mutex_lock(&source->mutex);
        h3_detach_entry_locked(entry);
        pthread_mutex_unlock(&source->mutex);
        h3_close_entry_object(entry);
        return result;
    }
    *out = h3_handle(entry);
    return 0;
}

static int h3_endpoint_listen(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_endpoint_config* config,
    trevrpc_rpc_transport_handle* out) {
    return h3_endpoint_listen_impl(transport, config, NULL, NULL, NULL, out);
}

int trevrpc_rpc_transport_h3_listen_shared(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_endpoint_config* config,
    trevrpc_msquic_accept_dispatch dispatch,
    void* dispatch_context,
    trevrpc_msquic_context_destroy dispatch_context_destroy,
    trevrpc_rpc_transport_handle* out_listener) {
    return h3_endpoint_listen_impl(
        transport, config, dispatch, dispatch_context, dispatch_context_destroy, out_listener);
}

static int h3_endpoint_get_port(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle listener, uint16_t* port) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    void* object = NULL;
    int result;
    if (port == NULL)
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, listener);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER ||
        h3_pin_object_locked(source, entry, &object) != 0) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    pthread_mutex_unlock(&source->mutex);
    result = trevrpc_msquic_listener_port(object, port);
    pthread_mutex_lock(&source->mutex);
    h3_unpin_object_locked(source, entry);
    pthread_mutex_unlock(&source->mutex);
    return result;
}

static int h3_endpoint_dial(trevrpc_rpc_transport* transport,
    const trevrpc_rpc_transport_endpoint_config* config,
    uint64_t operation_id,
    trevrpc_rpc_transport_handle* out) {
    h3_source* source = h3_from_base(transport);
    trevrpc_msquic_config ms;
    trevrpc_msquic_feature_request features;
    trevrpc_msquic_receive_policy policy = {0};
    trevrpc_msquic_conn* conn = NULL;
    h3_endpoint_copy endpoint;
    h3_entry* entry;
    int result;
    if (config == NULL || out == NULL || operation_id == 0 ||
        (config->protocol != TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3 &&
            config->protocol != TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT))
        return -ENOTSUP;
    if (config->protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT && config->unresolved_stream_bytes != 0 &&
        config->unresolved_stream_bytes < trevrpc_msquic_receive_minimum_raw_bytes())
        return -EINVAL;
    result = h3_validate_endpoint_sizes(config);
    if (result != 0)
        return result;
    result = h3_endpoint_copy_make(config, &endpoint);
    if (result != 0)
        return result;
    h3_msquic_config(&endpoint.value, &ms);
    features = trevrpc_msquic_default_h3_features();
    policy.max_stream_owned_bytes = config->max_pending_receive_bytes;
    policy.max_stream_owned_count = config->max_pending_receive_count;
    policy.max_connection_owned_bytes = config->max_pending_receive_bytes;
    policy.max_connection_owned_count = config->max_pending_receive_count;
    if (config->protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT) {
        policy.max_undecided_owned_bytes = config->unresolved_stream_bytes;
    }
    result = trevrpc_msquic_dial_start_observed_features_with_receive_policy(endpoint.value.host,
        endpoint.value.port,
        &ms,
        endpoint.value.server_name_len != 0 ? endpoint.value.server_name : NULL,
        &features,
        NULL,
        NULL,
        NULL,
        0,
        NULL,
        NULL,
        &policy,
        &conn);
    if (result != 0) {
        h3_endpoint_free(&endpoint);
        return result;
    }
    pthread_mutex_lock(&source->mutex);
    entry = h3_alloc_entry_locked(source,
        TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL,
        conn,
        (trevrpc_rpc_transport_handle){0});
    if (entry == NULL) {
        pthread_mutex_unlock(&source->mutex);
        trevrpc_msquic_conn_close(conn);
        h3_endpoint_free(&endpoint);
        return -EAGAIN;
    }
    entry->operation_id = operation_id;
    entry->endpoint = endpoint;
    memset(&endpoint, 0, sizeof(endpoint));
    pthread_mutex_unlock(&source->mutex);
    /* The callback context is installed after the entry has a stable address. */
    result = h3_install_conn(entry);
    if (result != 0) {
        pthread_mutex_lock(&source->mutex);
        h3_detach_entry_locked(entry);
        pthread_mutex_unlock(&source->mutex);
        h3_close_entry_object(entry);
        return result;
    }
    *out = h3_handle(entry);
    return 0;
}

static int h3_dial_cancel(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    void* object = NULL;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, handle);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION ||
        h3_pin_object_locked(source, entry, &object) != 0) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    pthread_mutex_unlock(&source->mutex);
    trevrpc_msquic_conn_shutdown_error(object, H3_APP_REQUEST_CANCELLED);
    pthread_mutex_lock(&source->mutex);
    h3_unpin_object_locked(source, entry);
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

static int h3_stream_open(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    uint64_t operation_id,
    trevrpc_rpc_transport_handle* out) {
    h3_source* source = h3_from_base(transport);
    h3_entry* parent;
    h3_entry* entry;
    trevrpc_msquic_conn* conn;
    trevrpc_msquic_stream* stream = NULL;
    uint32_t side_flags;
    bool webtransport;
    int result;
    if (out == NULL || operation_id == 0)
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    parent = h3_find_locked(source, connection);
    if (parent != NULL && parent->kind == TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION && parent->wt_draining) {
        pthread_mutex_unlock(&source->mutex);
        return -ESHUTDOWN;
    }
    if (parent == NULL || parent->kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION || !parent->ready_reported ||
        h3_pin_object_locked(source, parent, (void**)&conn) != 0) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    side_flags = parent->side_flags;
    webtransport = h3_is_webtransport(parent);
    pthread_mutex_unlock(&source->mutex);
    result = trevrpc_msquic_conn_open_stream(conn, &stream);
    pthread_mutex_lock(&source->mutex);
    h3_unpin_object_locked(source, parent);
    if (result != 0) {
        pthread_mutex_unlock(&source->mutex);
        return result;
    }
    parent = h3_find_locked(source, connection);
    if (parent == NULL || parent->object != conn || !parent->ready_reported ||
        webtransport != h3_is_webtransport(parent)) {
        pthread_mutex_unlock(&source->mutex);
        trevrpc_msquic_stream_close(stream);
        return -ESTALE;
    }
    entry = h3_alloc_entry_locked(source, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM, side_flags, stream, connection);
    if (entry != NULL) {
        result = h3_endpoint_copy_make(&parent->endpoint.value, &entry->endpoint);
        if (result == 0) {
            entry->operation_id = operation_id;
            entry->stream.action = webtransport ? TREV_H3_DEMUX_ACTION_WEBTRANSPORT : TREV_H3_DEMUX_ACTION_REQUEST;
            entry->stream.classified = true;
        } else {
            h3_detach_entry_locked(entry);
        }
    }
    pthread_mutex_unlock(&source->mutex);
    if (entry == NULL) {
        trevrpc_msquic_stream_close(stream);
        return -EAGAIN;
    }
    if (result != 0) {
        h3_close_entry_object(entry);
        return result;
    }
    result = h3_install_stream(source, entry);
    if (result != 0) {
        pthread_mutex_lock(&source->mutex);
        h3_detach_entry_locked(entry);
        pthread_mutex_unlock(&source->mutex);
        h3_close_entry_object(entry);
        return result;
    }
    *out = h3_handle(entry);
    return 0;
}

static int h3_stream_send(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle handle,
    uint64_t operation_id,
    const uint8_t* body,
    size_t body_len) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    trevrpc_msquic_stream* stream = NULL;
    trevrpc_msquic_send_completion* completion = NULL;
    uint8_t* frame;
    size_t a = 0, b = 0, c;
    bool send_response_headers;
    bool webtransport;
    uint64_t max_frame_size;
    intptr_t result;
    if (operation_id == 0 || (body == NULL && body_len != 0))
        return -EINVAL;
    if (body_len > UINT32_MAX - 4u)
        return -EMSGSIZE;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, handle);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM ||
        h3_pin_object_locked(source, entry, (void**)&stream) != 0) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    max_frame_size =
        entry->endpoint.value.max_frame_size != 0 ? entry->endpoint.value.max_frame_size : H3_DEFAULT_MAX_FRAME;
    if ((uint64_t)body_len > max_frame_size) {
        h3_unpin_object_locked(source, entry);
        pthread_mutex_unlock(&source->mutex);
        return -EMSGSIZE;
    }
    if (entry->stream.pending_completion != NULL || entry->stream.headers_sending || entry->send_event != NULL) {
        h3_unpin_object_locked(source, entry);
        pthread_mutex_unlock(&source->mutex);
        return -EAGAIN;
    }
    entry->send_event = h3_mandatory_event_alloc(source);
    if (entry->send_event == NULL) {
        h3_unpin_object_locked(source, entry);
        pthread_mutex_unlock(&source->mutex);
        return -ENOMEM;
    }
    webtransport = entry->stream.action == TREV_H3_DEMUX_ACTION_WEBTRANSPORT;
    send_response_headers = !webtransport && (entry->side_flags & TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER) != 0 &&
                            !entry->stream.headers_sent;
    entry->stream.headers_sending = send_response_headers;
    pthread_mutex_unlock(&source->mutex);
    if (send_response_headers) {
        result = h3_send_response_headers(entry);
        pthread_mutex_lock(&source->mutex);
        entry->stream.headers_sending = false;
        if (result >= 0)
            entry->stream.headers_sent = true;
        pthread_mutex_unlock(&source->mutex);
        if (result < 0) {
            pthread_mutex_lock(&source->mutex);
            h3_mandatory_event_free(source, &entry->send_event);
            h3_unpin_object_locked(source, entry);
            pthread_mutex_unlock(&source->mutex);
            return (int)result;
        }
    }
    if (!webtransport &&
        (trevrpc_quic_varint_size(TREV_H3_FRAME_DATA, &a) != 0 || trevrpc_quic_varint_size(body_len + 4, &b) != 0)) {
        pthread_mutex_lock(&source->mutex);
        h3_mandatory_event_free(source, &entry->send_event);
        h3_unpin_object_locked(source, entry);
        pthread_mutex_unlock(&source->mutex);
        return -EOVERFLOW;
    }
    if (body_len > SIZE_MAX - a - b - 4u) {
        pthread_mutex_lock(&source->mutex);
        h3_mandatory_event_free(source, &entry->send_event);
        h3_unpin_object_locked(source, entry);
        pthread_mutex_unlock(&source->mutex);
        return -EOVERFLOW;
    }
    c = a + b + 4 + body_len;
    frame = malloc(c);
    if (frame == NULL) {
        pthread_mutex_lock(&source->mutex);
        h3_mandatory_event_free(source, &entry->send_event);
        h3_unpin_object_locked(source, entry);
        pthread_mutex_unlock(&source->mutex);
        return -ENOMEM;
    }
    if (!webtransport) {
        (void)trevrpc_quic_varint_write(frame, c, TREV_H3_FRAME_DATA, &a);
        (void)trevrpc_quic_varint_write(frame + a, c - a, body_len + 4, &b);
    }
    frame[a + b] = (uint8_t)(body_len >> 24);
    frame[a + b + 1] = (uint8_t)(body_len >> 16);
    frame[a + b + 2] = (uint8_t)(body_len >> 8);
    frame[a + b + 3] = (uint8_t)body_len;
    if (body_len)
        memcpy(frame + a + b + 4, body, body_len);
    result = trevrpc_msquic_stream_write_raw_with_completion(stream, frame, c, false, &completion);
    free(frame);
    if (result < 0) {
        pthread_mutex_lock(&source->mutex);
        h3_mandatory_event_free(source, &entry->send_event);
        h3_unpin_object_locked(source, entry);
        pthread_mutex_unlock(&source->mutex);
        return (int)result;
    }
    pthread_mutex_lock(&source->mutex);
    /* Native acceptance commits the operation even if close raced while the
     * source mutex was released. The reserved send event keeps terminal
     * publication deferred until this completion is committed. */
    entry->stream.pending_completion = completion;
    entry->stream.pending_operation_id = operation_id;
    entry->stream.pending_send_bytes = c;
    ++source->pending_send_count;
    source->pending_send_bytes += c;
    if (trevrpc_msquic_send_completion_status(completion) != -EAGAIN)
        entry->pending |= H3_PENDING_SEND_COMPLETE;
    h3_signal_locked(source);
    h3_unpin_object_locked(source, entry);
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

static int h3_stream_receive(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle, trevrpc_rpc_transport_receive** out) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    trevrpc_rpc_transport_receive* receive;
    if (out == NULL)
        return -EINVAL;
    *out = NULL;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_receive_locked(source, handle);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    receive = entry->stream.receive_head;
    if (receive) {
        entry->stream.receive_head = receive->next;
        if (entry->stream.receive_head == NULL)
            entry->stream.receive_tail = NULL;
        if (source->receive_owned_count != 0)
            --source->receive_owned_count;
        if (source->receive_owned_bytes >= receive->info.data_len)
            source->receive_owned_bytes -= receive->info.data_len;
    }
    if (receive != NULL)
        h3_schedule_receive_retries_locked(source);
    h3_collect_reclaimable_locked(source);
    pthread_mutex_unlock(&source->mutex);
    h3_reap_pending(source);
    if (receive == NULL)
        return -EAGAIN;
    receive->next = NULL;
    *out = receive;
    return 0;
}

static int h3_stream_finish_send(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    trevrpc_msquic_stream* stream;
    int result;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, handle);
    if (entry == NULL || h3_pin_object_locked(source, entry, (void**)&stream) != 0) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    /* Publish the local-FIN intent before the callback-capable shutdown call so
     * terminal processing cannot misclassify its synchronous completion. */
    entry->stream.send_fin = true;
    pthread_mutex_unlock(&source->mutex);
    result = trevrpc_msquic_stream_shutdown_send(stream);
    pthread_mutex_lock(&source->mutex);
    if (result != 0 && entry->live)
        entry->stream.send_fin = false;
    else if (result == 0 && entry->live)
        h3_maybe_emit_stream_closed_locked(entry);
    h3_unpin_object_locked(source, entry);
    pthread_mutex_unlock(&source->mutex);
    return result;
}
static bool h3_request_stream_terminal_locked(h3_entry* entry) {
    h3_source* source = entry->source;
    if (entry->terminal_requested)
        return !entry->close_reported;
    entry->terminal_requested = true;
    if (entry->send_event != NULL || entry->stream.pending_completion != NULL) {
        entry->pending |= H3_PENDING_TERMINAL;
        h3_signal_locked(source);
        return true;
    }
    if (h3_emit_locked(source,
            TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED,
            entry->side_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL,
            0,
            TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
            h3_handle(entry),
            entry->parent,
            0,
            0) == 0) {
        entry->close_reported = true;
        h3_mark_dead_locked(entry);
        return false;
    }
    entry->pending |= H3_PENDING_TERMINAL;
    h3_signal_locked(source);
    return true;
}

static void h3_discard_stream_receives_locked(h3_source* source, h3_entry* entry) {
    trevrpc_rpc_transport_receive* receive;
    while ((receive = entry->stream.receive_head) != NULL) {
        entry->stream.receive_head = receive->next;
        if (source->receive_owned_count != 0)
            --source->receive_owned_count;
        if (source->receive_owned_bytes >= receive->info.data_len)
            source->receive_owned_bytes -= receive->info.data_len;
        free(receive);
    }
    entry->stream.receive_tail = NULL;
    free(entry->stream.parse);
    entry->stream.parse = NULL;
    entry->stream.parse_len = 0;
    entry->stream.parse_cap = 0;
    free(entry->stream.rpc);
    entry->stream.rpc = NULL;
    entry->stream.rpc_len = 0;
    entry->stream.rpc_cap = 0;
    entry->stream.receive_blocked = false;
    entry->pending &= ~(H3_PENDING_READABLE | H3_PENDING_REEMIT_READABLE);
    h3_schedule_receive_retries_locked(source);
}

static int h3_stream_abort_half(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle, uint64_t code, bool receive) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    h3_entry* parent;
    trevrpc_msquic_stream* stream = NULL;
    uint64_t wire_code = H3_APP_REQUEST_CANCELLED;
    uint64_t previous_abort_error;
    bool previous_fin;
    int result;

    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, handle);
    if (entry == NULL || h3_pin_object_locked(source, entry, (void**)&stream) != 0) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    if ((receive && entry->stream.recv_aborted) || (!receive && entry->stream.send_aborted)) {
        h3_unpin_object_locked(source, entry);
        pthread_mutex_unlock(&source->mutex);
        return 0;
    }
    parent = h3_parent_connection_locked(source, entry);
    if (parent != NULL && h3_is_webtransport(parent)) {
        if (!parent->profile_resolved) {
            h3_unpin_object_locked(source, entry);
            pthread_mutex_unlock(&source->mutex);
            return -EAGAIN;
        }
        result = trevrpc_wt_profile_encode_application_error(parent->negotiation.profile, code, &wire_code);
        if (result != 0) {
            h3_unpin_object_locked(source, entry);
            pthread_mutex_unlock(&source->mutex);
            return result;
        }
    }

    previous_abort_error = entry->stream.local_abort_error;
    if (receive) {
        previous_fin = entry->stream.recv_fin;
        entry->stream.recv_fin = true;
        entry->stream.recv_aborted = true;
        h3_discard_stream_receives_locked(source, entry);
    } else {
        previous_fin = entry->stream.send_fin;
        entry->stream.send_fin = true;
        entry->stream.send_aborted = true;
    }
    entry->stream.local_abort_error = code;
    pthread_mutex_unlock(&source->mutex);

    result = receive ? trevrpc_msquic_stream_abort_receive_with_error(stream, wire_code)
                     : trevrpc_msquic_stream_abort_send_with_error(stream, wire_code);

    pthread_mutex_lock(&source->mutex);
    if (result != 0 && entry->live) {
        if (receive) {
            entry->stream.recv_fin = previous_fin;
            entry->stream.recv_aborted = false;
        } else {
            entry->stream.send_fin = previous_fin;
            entry->stream.send_aborted = false;
        }
        entry->stream.local_abort_error = previous_abort_error;
    } else if (result == 0 && entry->live) {
        h3_maybe_emit_stream_closed_locked(entry);
    }
    h3_unpin_object_locked(source, entry);
    pthread_mutex_unlock(&source->mutex);
    return result;
}

static int h3_stream_abort_receive(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle, uint64_t code) {
    return h3_stream_abort_half(transport, handle, code, true);
}

static int h3_stream_abort_send(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle, uint64_t code) {
    return h3_stream_abort_half(transport, handle, code, false);
}

static int h3_stream_abort(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle, uint64_t code) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    h3_entry* parent = NULL;
    trevrpc_msquic_stream* stream = NULL;
    trevrpc_wt_profile_id profile = TREV_WT_PROFILE_NONE;
    uint64_t wire_code = H3_APP_REQUEST_CANCELLED;
    bool webtransport = false;
    bool defer = false;
    int result;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, handle);
    if (entry != NULL) {
        if (h3_pin_object_locked(source, entry, (void**)&stream) != 0)
            entry = NULL;
        parent = entry != NULL ? h3_parent_connection_locked(source, entry) : NULL;
        webtransport = parent != NULL && h3_is_webtransport(parent);
        if (webtransport) {
            if (!parent->profile_resolved) {
                h3_unpin_object_locked(source, entry);
                pthread_mutex_unlock(&source->mutex);
                return -EAGAIN;
            }
            profile = parent->negotiation.profile;
        }
    }
    pthread_mutex_unlock(&source->mutex);
    if (entry == NULL)
        return -ESTALE;
    if (webtransport) {
        result = trevrpc_wt_profile_encode_application_error(profile, code, &wire_code);
        if (result != 0) {
            pthread_mutex_lock(&source->mutex);
            h3_unpin_object_locked(source, entry);
            pthread_mutex_unlock(&source->mutex);
            return result;
        }
    }
    result = trevrpc_msquic_stream_abort_with_error(stream, wire_code);
    pthread_mutex_lock(&source->mutex);
    if (result == 0 && entry->live)
        defer = h3_request_stream_terminal_locked(entry);
    if (result != 0 || defer)
        h3_unpin_object_locked(source, entry);
    pthread_mutex_unlock(&source->mutex);
    if (result == 0 && !defer)
        h3_close_entry_object_owned(entry);
    return result;
}
static int h3_stream_close(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    bool defer = false;
    bool pinned = false;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, handle);
    if (entry != NULL && entry->object != NULL) {
        void* ignored = NULL;
        pinned = h3_pin_object_locked(source, entry, &ignored) == 0;
    }
    if (entry != NULL)
        defer = h3_request_stream_terminal_locked(entry);
    pthread_mutex_unlock(&source->mutex);
    if (entry == NULL)
        return -ESTALE;
    if (!defer) {
        if (pinned)
            h3_close_entry_object_owned(entry);
        else
            h3_close_entry_object(entry);
    } else if (pinned) {
        pthread_mutex_lock(&source->mutex);
        h3_unpin_object_locked(source, entry);
        pthread_mutex_unlock(&source->mutex);
    }
    return 0;
}
static int h3_connection_close(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle, uint64_t code) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    void* object = NULL;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, handle);
    if (entry != NULL && h3_pin_object_locked(source, entry, &object) != 0)
        entry = NULL;
    if (entry != NULL) {
        if (!entry->close_reported) {
            if (h3_emit_locked(source,
                    TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED,
                    entry->side_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL,
                    0,
                    TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION,
                    h3_handle(entry),
                    entry->parent,
                    0,
                    code) == 0)
                entry->close_reported = true;
            else {
                entry->pending |= H3_PENDING_TERMINAL;
                h3_signal_locked(source);
            }
        }
        if (entry->close_reported)
            h3_mark_dead_locked(entry);
    }
    pthread_mutex_unlock(&source->mutex);
    if (entry == NULL)
        return -ESTALE;
    trevrpc_msquic_conn_shutdown_error(object, code);
    pthread_mutex_lock(&source->mutex);
    h3_unpin_object_locked(source, entry);
    pthread_mutex_unlock(&source->mutex);
    return 0;
}
static int h3_listener_close(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, handle);
    if (entry != NULL && entry->object != NULL) {
        /* Stop native admission before the terminal can be dequeued and final
         * listener ownership can transfer to the asynchronous finalizer. */
        trevrpc_msquic_listener_shutdown_deferred(entry->object);
        if (!entry->close_reported) {
            if (h3_emit_locked(source,
                    TREVRPC_RPC_TRANSPORT_EVENT_LISTENER_STOPPED,
                    entry->side_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL,
                    0,
                    TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER,
                    h3_handle(entry),
                    entry->parent,
                    0,
                    0) == 0)
                entry->close_reported = true;
            else {
                entry->pending |= H3_PENDING_TERMINAL;
                h3_signal_locked(source);
            }
        }
        if (entry->close_reported)
            h3_mark_dead_locked(entry);
    } else {
        entry = NULL;
    }
    pthread_mutex_unlock(&source->mutex);
    return entry == NULL ? -ESTALE : 0;
}

static void h3_maybe_publish_stopped_locked(h3_source* source) {
    if (source->state != TREVRPC_RPC_TRANSPORT_STATE_STOPPING || source->stop_event_published ||
        source->live_listeners != 0 || source->live_connections != 0 || source->live_streams != 0)
        return;
    {
        int result = h3_emit_locked(source,
            TREVRPC_RPC_TRANSPORT_EVENT_STOPPED,
            TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL,
            0,
            TREVRPC_RPC_TRANSPORT_OBJECT_NONE,
            (trevrpc_rpc_transport_handle){0},
            (trevrpc_rpc_transport_handle){0},
            0,
            0);
        if (result != 0) {
            /* Keep STOPPING so a later next_event/drain call retries the
             * mandatory STOPPED allocation instead of losing it forever. */
            h3_signal_locked(source);
            return;
        }
    }
    source->stop_event_published = true;
    source->state = TREVRPC_RPC_TRANSPORT_STATE_STOPPED;
}

static int h3_close(trevrpc_rpc_transport* transport) {
    h3_source* source = h3_from_base(transport);
    h3_entry** shutdown_entries;
    size_t shutdown_count = 0;
    size_t i;
    shutdown_entries = calloc(source->entry_capacity, sizeof(*shutdown_entries));
    if (shutdown_entries == NULL)
        return -ENOMEM;
    pthread_mutex_lock(&source->mutex);
    if (source->state != TREVRPC_RPC_TRANSPORT_STATE_RUNNING) {
        pthread_mutex_unlock(&source->mutex);
        free(shutdown_entries);
        return 0;
    }
    source->state = TREVRPC_RPC_TRANSPORT_STATE_STOPPING;
    for (i = 0; i < source->entry_capacity; ++i) {
        h3_entry* entry = source->entries[i].entry;
        void* ignored = NULL;
        uint32_t kind;
        if (entry == NULL || !entry->live)
            continue;
        if (h3_pin_object_locked(source, entry, &ignored) == 0)
            shutdown_entries[shutdown_count++] = entry;
        if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER && entry->object != NULL)
            trevrpc_msquic_listener_shutdown_deferred(entry->object);
        if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM &&
            (entry->stream.connect_control || (entry->stream.action != TREV_H3_DEMUX_ACTION_REQUEST &&
                                                  entry->stream.action != TREV_H3_DEMUX_ACTION_WEBTRANSPORT))) {
            entry->terminal_requested = true;
            entry->close_reported = true;
            h3_mark_dead_locked(entry);
            continue;
        }
        if (entry->terminal_requested)
            continue;
        entry->terminal_requested = true;
        if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM && entry->stream.pending_completion != NULL) {
            entry->pending |= H3_PENDING_TERMINAL;
            h3_signal_locked(source);
            continue;
        }
        entry->close_reported = true;
        kind = entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER     ? TREVRPC_RPC_TRANSPORT_EVENT_LISTENER_STOPPED
               : entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION ? TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED
                                                                        : TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED;
        if (h3_emit_locked(source,
                kind,
                entry->side_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_TERMINAL,
                -ESHUTDOWN,
                entry->kind,
                h3_handle(entry),
                entry->parent,
                0,
                0) == 0)
            h3_mark_dead_locked(entry);
        else {
            entry->close_reported = false;
            entry->pending |= H3_PENDING_TERMINAL;
            h3_signal_locked(source);
        }
    }
    /* STOPPED is published only after every admitted send completion has
     * either completed or been rejected, and every object terminal has been
     * published (or explicitly dropped on allocation failure). */
    h3_maybe_publish_stopped_locked(source);
    pthread_mutex_unlock(&source->mutex);

    for (i = 0; i < shutdown_count; ++i) {
        h3_entry* entry = shutdown_entries[i];
        if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM)
            (void)trevrpc_msquic_stream_abort_with_error(entry->object, H3_APP_REQUEST_CANCELLED);
    }
    for (i = 0; i < shutdown_count; ++i) {
        h3_entry* entry = shutdown_entries[i];
        if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION)
            trevrpc_msquic_conn_shutdown_error(entry->object, 0);
    }
    pthread_mutex_lock(&source->mutex);
    for (i = 0; i < shutdown_count; ++i)
        h3_unpin_object_locked(source, shutdown_entries[i]);
    h3_collect_reclaimable_locked(source);
    pthread_mutex_unlock(&source->mutex);
    h3_reap_pending(source);
    free(shutdown_entries);
    return 0;
}
static int h3_drain(trevrpc_rpc_transport* transport) {
    h3_source* source = h3_from_base(transport);
    h3_process_pending(source);
    pthread_mutex_lock(&source->mutex);
    int result = source->event_depth || source->state != TREVRPC_RPC_TRANSPORT_STATE_STOPPED ? -EAGAIN : 0;
    pthread_mutex_unlock(&source->mutex);
    return result;
}
static void h3_free_entry_mode(h3_entry* entry, bool force_stream_close) {
    size_t control_index;
    if (entry == NULL)
        return;
    if (entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION) {
        /* Closing the parent connection first drives SHUTDOWN_COMPLETE on its
         * long-lived H3 control streams. Queueing graceful stream closes first
         * can otherwise block the single native finalizer ahead of the parent
         * operation needed to complete them. */
        h3_close_entry_object_mode(entry, force_stream_close);
        for (control_index = 0; control_index < 3; ++control_index) {
            trevrpc_msquic_stream* stream = entry->control_streams[control_index];
            entry->control_streams[control_index] = NULL;
            if (stream != NULL)
                trevrpc_msquic_stream_close_deferred_owned(stream, force_stream_close);
        }
    } else {
        h3_close_entry_object_mode(entry, force_stream_close);
    }
    h3_mandatory_event_free(entry->source, &entry->ready_event);
    h3_mandatory_event_free(entry->source, &entry->terminal_event);
    h3_mandatory_event_free(entry->source, &entry->fin_event);
    h3_mandatory_event_free(entry->source, &entry->send_stop_event);
    h3_mandatory_event_free(entry->source, &entry->send_event);
    h3_stream_free(&entry->stream);
    h3_endpoint_free(&entry->endpoint);
    free(entry);
}

static void h3_free_entry(h3_entry* entry) {
    h3_free_entry_mode(entry, false);
}

static void h3_free_entries_of_kind(h3_source* source, uint32_t kind) {
    h3_entry** link;
    size_t i;

    for (i = 0; i < source->entry_capacity; ++i) {
        h3_entry* entry = source->entries[i].entry;
        if (entry != NULL && entry->kind == kind) {
            source->entries[i].entry = NULL;
            h3_free_entry_mode(entry, true);
        }
    }

    link = &source->retired_entries;
    while (*link != NULL) {
        h3_entry* entry = *link;
        if (entry->kind == kind) {
            *link = entry->retired_next;
            entry->retired_next = NULL;
            h3_free_entry_mode(entry, true);
        } else {
            link = &entry->retired_next;
        }
    }

    link = &source->reap_entries;
    while (*link != NULL) {
        h3_entry* entry = *link;
        if (entry->kind == kind) {
            *link = entry->retired_next;
            entry->retired_next = NULL;
            h3_free_entry_mode(entry, true);
        } else {
            link = &entry->retired_next;
        }
    }
}

static void h3_destroy(trevrpc_rpc_transport* transport) {
    h3_source* source = h3_from_base(transport);
    trevrpc_rpc_transport_event* event;
    (void)h3_close(transport);
    /* Destruction drops queued events. Terminal events must perform the same
     * detach/advance transition as normal dequeue before their entries go away. */
    pthread_mutex_lock(&source->mutex);
    while ((event = source->event_head) != NULL) {
        source->event_head = event->next;
        if (event->terminal_entry != NULL)
            h3_detach_entry_locked(event->terminal_entry);
        h3_event_node_free(&event);
    }
    source->event_tail = NULL;
    pthread_mutex_unlock(&source->mutex);
    h3_free_entries_of_kind(source, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM);
    h3_free_entries_of_kind(source, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION);
    h3_free_entries_of_kind(source, TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER);
    trevrpc_msquic_finalizer_drain();
    assert(source->retired_entries == NULL);
    assert(source->reap_entries == NULL);
    h3_mandatory_event_free(source, &source->stopped_event);
    close(source->wake_read_fd);
    close(source->wake_write_fd);
    pthread_cond_destroy(&source->object_cond);
    pthread_mutex_destroy(&source->mutex);
    free(source->entries);
    free(source);
}

static const trevrpc_rpc_transport_ops h3_ops = {
    .get_wake_source = h3_get_wake_source,
    .next_event = h3_next_event,
    .event_get_info = h3_event_get_info,
    .event_release = h3_event_release,
    .receive_get_info = h3_receive_get_info,
    .receive_release = h3_receive_release,
    .get_diagnostics = h3_get_diagnostics,
    .poll_timeout_ms = h3_poll_timeout_ms,
    .endpoint_listen = h3_endpoint_listen,
    .endpoint_get_port = h3_endpoint_get_port,
    .endpoint_dial = h3_endpoint_dial,
    .dial_cancel = h3_dial_cancel,
    .stream_open = h3_stream_open,
    .stream_send = h3_stream_send,
    .stream_receive = h3_stream_receive,
    .stream_finish_send = h3_stream_finish_send,
    .stream_abort_receive = h3_stream_abort_receive,
    .stream_abort_send = h3_stream_abort_send,
    .stream_abort = h3_stream_abort,
    .stream_close = h3_stream_close,
    .connection_close = h3_connection_close,
    .listener_close = h3_listener_close,
    .close = h3_close,
    .drain = h3_drain,
    .destroy = h3_destroy,
    .get_wake_sources = NULL,
    .event_get_admission_info = h3_event_get_admission_info,
    .event_get_protocol_info = h3_event_get_protocol_info,
    .admission_respond = h3_admission_respond,
};

int trevrpc_rpc_transport_h3_create(const trevrpc_rpc_transport_config* config, trevrpc_rpc_transport** out_transport) {
    h3_source* source;
    int fds[2] = {-1, -1};
    int result;
    uint64_t entry_capacity;
    bool mutex_initialized = false;
    if (config == NULL || out_transport == NULL || config->event_capacity == 0 || config->listener_capacity == 0 ||
        config->connection_capacity == 0 || config->stream_capacity == 0)
        return -EINVAL;
    entry_capacity = (uint64_t)config->listener_capacity + config->connection_capacity + config->stream_capacity + 8u;
    if (entry_capacity > UINT32_MAX || entry_capacity > SIZE_MAX)
        return -EOVERFLOW;
    source = calloc(1, sizeof(*source));
    if (source == NULL)
        return -ENOMEM;
    atomic_init(&source->mandatory_reservations, 0);
    source->config = *config;
    if (source->config.max_receive_owned_bytes == 0)
        source->config.max_receive_owned_bytes = SIZE_MAX;
    if (source->config.max_receive_owned_count == 0)
        source->config.max_receive_owned_count = UINT32_MAX;
    source->entry_capacity = (size_t)entry_capacity;
    source->entries = calloc(source->entry_capacity, sizeof(*source->entries));
    if (source->entries == NULL) {
        free(source);
        return -ENOMEM;
    }
    {
        size_t i;
        for (i = source->entry_capacity; i != 0; --i) {
            h3_registry_slot* slot = &source->entries[i - 1u];
            slot->generation = 1;
            slot->next_free = source->free_head;
            source->free_head = (uint32_t)i;
        }
    }
    result = h3_make_pipe(fds);
    if (result != 0) {
        free(source->entries);
        free(source);
        return result;
    }
    source->wake_read_fd = fds[0];
    source->wake_write_fd = fds[1];
    source->owner = atomic_fetch_add(&h3_owner_sequence, 1);
    source->next_sequence = 1;
    source->state = TREVRPC_RPC_TRANSPORT_STATE_RUNNING;
    result = pthread_mutex_init(&source->mutex, NULL);
    if (result != 0)
        goto synchronization_init_failed;
    mutex_initialized = true;
    result = pthread_cond_init(&source->object_cond, NULL);
    if (result != 0)
        goto synchronization_init_failed;
#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
    source->event_alloc_budget = SIZE_MAX;
#endif
    source->stopped_event = h3_mandatory_event_alloc(source);
    if (source->stopped_event == NULL) {
        pthread_cond_destroy(&source->object_cond);
        pthread_mutex_destroy(&source->mutex);
        close(source->wake_read_fd);
        close(source->wake_write_fd);
        free(source->entries);
        free(source);
        return -ENOMEM;
    }
    source->base.ops = &h3_ops;
    *out_transport = &source->base;
    return 0;

synchronization_init_failed:
    if (mutex_initialized)
        pthread_mutex_destroy(&source->mutex);
    close(source->wake_read_fd);
    close(source->wake_write_fd);
    free(source->entries);
    free(source);
    return -ENOMEM;
}

#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
int trevrpc_rpc_transport_h3_test_make_connection(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle* out_connection) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    if (out_connection == NULL)
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    entry = h3_alloc_entry_locked(source,
        TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_CLIENT | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL,
        NULL,
        (trevrpc_rpc_transport_handle){0});
    if (entry != NULL) {
        entry->connected = true;
        entry->start_reported = true;
    }
    pthread_mutex_unlock(&source->mutex);
    if (entry == NULL)
        return -EAGAIN;
    *out_connection = h3_handle(entry);
    return 0;
}

int trevrpc_rpc_transport_h3_test_make_server_connection(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle* out_listener,
    trevrpc_rpc_transport_handle* out_connection) {
    h3_source* source = h3_from_base(transport);
    h3_entry* listener;
    h3_entry* connection;
    if (out_listener == NULL || out_connection == NULL)
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    listener = h3_alloc_entry_locked(source,
        TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_LOCAL,
        NULL,
        (trevrpc_rpc_transport_handle){0});
    connection = listener != NULL ? h3_alloc_entry_locked(source,
                                        TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION,
                                        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
                                        NULL,
                                        h3_handle(listener))
                                  : NULL;
    if (connection != NULL) {
        connection->endpoint.value.protocol = TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3;
        connection->connected = true;
        connection->local_control_ready = true;
        connection->peer_settings_received = true;
        connection->peer_settings_ready = true;
        connection->start_reported = true;
        connection->ready_reported = true;
    } else if (listener != NULL) {
        h3_detach_entry_locked(listener);
    }
    pthread_mutex_unlock(&source->mutex);
    if (connection == NULL)
        return -EAGAIN;
    *out_listener = h3_handle(listener);
    *out_connection = h3_handle(connection);
    return 0;
}

int trevrpc_rpc_transport_h3_test_set_deferred_admission(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, bool enabled) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, connection);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    if (enabled)
        entry->endpoint.value.flags |= TREVRPC_RPC_TRANSPORT_ENDPOINT_DEFER_ADMISSION;
    else
        entry->endpoint.value.flags &= ~TREVRPC_RPC_TRANSPORT_ENDPOINT_DEFER_ADMISSION;
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

int trevrpc_rpc_transport_h3_test_set_connection_protocol(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, uint32_t protocol) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    if (protocol != TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3 && protocol != TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT &&
        protocol != TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED)
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, connection);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    entry->endpoint.value.protocol = protocol;
    if (protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED) {
        entry->endpoint.value.webtransport_profiles = H3_WT_PROFILE_ALL_SUPPORTED;
        entry->endpoint.value.max_sessions = 1;
        entry->ready_reported = false;
    }
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

int trevrpc_rpc_transport_h3_test_set_multiplexed_webtransport(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, bool enabled) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, connection);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION || !h3_is_multiplexed(entry)) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    entry->endpoint.value.webtransport_profiles = enabled ? H3_WT_PROFILE_ALL_SUPPORTED : 0;
    entry->endpoint.value.max_sessions = enabled ? 1 : 0;
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

int trevrpc_rpc_transport_h3_test_set_peer_settings_ready(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, bool ready) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, connection);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    entry->peer_settings_received = ready;
    entry->peer_settings_ready = ready;
    if (!ready) {
        entry->ready_reported = false;
        entry->profile_resolved = false;
        memset(&entry->negotiation, 0, sizeof(entry->negotiation));
    } else {
        h3_maybe_emit_connection_ready_locked(entry);
        h3_schedule_profile_waiters_locked(source, entry);
    }
    h3_signal_locked(source);
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

int trevrpc_rpc_transport_h3_test_make_unresolved_webtransport_connection(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle* out_connection) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    if (out_connection == NULL)
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    entry = h3_alloc_entry_locked(source,
        TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION,
        TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
        NULL,
        (trevrpc_rpc_transport_handle){0});
    if (entry != NULL) {
        entry->endpoint.value.protocol = TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT;
        entry->connected = true;
        entry->local_control_ready = true;
        entry->start_reported = true;
    }
    pthread_mutex_unlock(&source->mutex);
    if (entry == NULL)
        return -EAGAIN;
    *out_connection = h3_handle(entry);
    return 0;
}

int trevrpc_rpc_transport_h3_test_adopt_peer_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    trevrpc_msquic_stream* object,
    bool unidirectional,
    trevrpc_rpc_transport_handle* out_stream) {
    h3_source* source = h3_from_base(transport);
    h3_entry* parent;
    h3_entry* entry = NULL;
    bool quota_rejected = false;
    int result;
    if (object == NULL || out_stream == NULL)
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    parent = h3_find_locked(source, connection);
    if (parent == NULL || parent->kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    if (!h3_is_webtransport(parent)) {
        entry = h3_alloc_entry_locked(source,
            TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
            parent->side_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
            object,
            connection);
        if (entry != NULL) {
            entry->start_reported = true;
            entry->stream.action = TREV_H3_DEMUX_ACTION_REQUEST;
            entry->stream.classified = true;
            entry->stream.headers_received = true;
            entry->stream.headers_sent = true;
            entry->stream.ready_bypass = true;
            result = 0;
        } else {
            result = -EAGAIN;
        }
    } else {
        result = h3_admit_peer_stream_locked(source,
            parent,
            object,
            unidirectional ? TREV_H3_DEMUX_UNIDIRECTIONAL : TREV_H3_DEMUX_BIDIRECTIONAL,
            &entry,
            &quota_rejected);
    }
    pthread_mutex_unlock(&source->mutex);
    if (result != 0) {
        if (quota_rejected)
            (void)trevrpc_msquic_stream_abort_with_error(object, TREV_H3_DEMUX_APP_STREAM_CREATION_ERROR);
        if (entry != NULL)
            h3_close_entry_object(entry);
        else if (quota_rejected)
            trevrpc_msquic_stream_close(object);
        return result;
    }
    result = h3_install_stream(source, entry);
    if (result != 0) {
        pthread_mutex_lock(&source->mutex);
        h3_detach_entry_locked(entry);
        pthread_mutex_unlock(&source->mutex);
        h3_close_entry_object(entry);
        return result;
    }
    *out_stream = h3_handle(entry);
    return 0;
}

int trevrpc_rpc_transport_h3_test_adopt_peer_bidi_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    trevrpc_msquic_stream* object,
    trevrpc_rpc_transport_handle* out_stream) {
    return trevrpc_rpc_transport_h3_test_adopt_peer_stream(transport, connection, object, false, out_stream);
}

int trevrpc_rpc_transport_h3_test_set_unresolved_limits(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    uint32_t stream_count,
    uint64_t timeout_ms) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, connection);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION || !h3_is_webtransport(entry)) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    entry->endpoint.value.unresolved_stream_count = stream_count;
    entry->endpoint.value.unresolved_stream_timeout_ms = timeout_ms;
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

void trevrpc_rpc_transport_h3_test_set_now_nanos(trevrpc_rpc_transport* transport, uint64_t now_nanos) {
    h3_source* source = h3_from_base(transport);
    pthread_mutex_lock(&source->mutex);
    source->monotonic_nanos = now_nanos;
    source->monotonic_override = true;
    pthread_mutex_unlock(&source->mutex);
}

int trevrpc_rpc_transport_h3_test_resolve_webtransport_profile(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, uint32_t profile) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    if (!trevrpc_wt_profile_is_supported((trevrpc_wt_profile_id)profile))
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, connection);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION || !h3_allows_webtransport(entry) ||
        entry->profile_resolved) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    entry->negotiation.profile = (trevrpc_wt_profile_id)profile;
    entry->profile_resolved = true;
    entry->peer_settings_ready = true;
    h3_schedule_profile_waiters_locked(source, entry);
    h3_signal_locked(source);
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

int trevrpc_rpc_transport_h3_test_resolve_webtransport_session(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, uint64_t session_id) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    if (!trevrpc_wt_profile_valid_session_id(session_id))
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, connection);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION || !entry->profile_resolved ||
        entry->wt_session_ready) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    entry->connect_stream_id = session_id;
    entry->wt_session_ready = true;
    h3_maybe_emit_connection_ready_locked(entry);
    h3_schedule_waiting_wt_locked(source, entry);
    h3_signal_locked(source);
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

int trevrpc_rpc_transport_h3_test_make_peer_request_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    const char* configured_path,
    size_t configured_path_len,
    trevrpc_rpc_transport_handle* out_stream) {
    h3_source* source = h3_from_base(transport);
    h3_entry* parent;
    h3_entry* entry = NULL;
    h3_endpoint_copy endpoint = {0};
    trevrpc_rpc_transport_endpoint_config config;
    int result = 0;
    if (configured_path == NULL || configured_path_len == 0 || out_stream == NULL)
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    parent = h3_find_locked(source, connection);
    if (parent == NULL || parent->kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION) {
        result = -ESTALE;
    } else {
        config = parent->endpoint.value;
        config.path = configured_path;
        config.path_len = (uint32_t)configured_path_len;
        result = configured_path_len > UINT32_MAX ? -EOVERFLOW : h3_endpoint_copy_make(&config, &endpoint);
    }
    if (result == 0) {
        h3_endpoint_free(&parent->endpoint);
        parent->endpoint = endpoint;
        memset(&endpoint, 0, sizeof(endpoint));
        entry = h3_alloc_entry_locked(source,
            TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
            TREVRPC_RPC_TRANSPORT_EVENT_FLAG_SERVER | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
            NULL,
            connection);
        if (entry == NULL) {
            result = -EAGAIN;
        } else {
            entry->start_reported = true;
            entry->stream.action = TREV_H3_DEMUX_ACTION_REQUEST;
            entry->stream.classified = true;
        }
    }
    pthread_mutex_unlock(&source->mutex);
    h3_endpoint_free(&endpoint);
    if (result == 0)
        *out_stream = h3_handle(entry);
    return result;
}

int trevrpc_rpc_transport_h3_test_attach_stream_object(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, trevrpc_msquic_stream* object) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    int result;
    if (object == NULL)
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, stream);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    if (entry->object != NULL) {
        pthread_mutex_unlock(&source->mutex);
        return -EALREADY;
    }
    entry->object = object;
    pthread_mutex_unlock(&source->mutex);
    result = h3_install_stream(source, entry);
    if (result != 0) {
        pthread_mutex_lock(&source->mutex);
        if (entry->object == object)
            entry->object = NULL;
        pthread_mutex_unlock(&source->mutex);
    }
    return result;
}

int trevrpc_rpc_transport_h3_test_make_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    trevrpc_rpc_transport_handle* out_stream) {
    h3_source* source = h3_from_base(transport);
    h3_entry* parent;
    h3_entry* entry;
    if (out_stream == NULL)
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    parent = h3_find_locked(source, connection);
    if (parent == NULL) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    entry = h3_alloc_entry_locked(source,
        TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
        parent->side_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
        NULL,
        connection);
    if (entry) {
        entry->start_reported = true;
        entry->stream.action = TREV_H3_DEMUX_ACTION_REQUEST;
        entry->stream.classified = true;
        entry->stream.headers_received = true;
        entry->stream.headers_sent = true;
        entry->stream.ready_bypass = true;
    }
    pthread_mutex_unlock(&source->mutex);
    if (!entry)
        return -EAGAIN;
    *out_stream = h3_handle(entry);
    return 0;
}

static int h3_test_make_protocol_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    trevrpc_h3_demux_action action,
    trevrpc_rpc_transport_handle* out_stream) {
    h3_source* source = h3_from_base(transport);
    h3_entry* parent;
    h3_entry* entry;
    if (out_stream == NULL)
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    parent = h3_find_locked(source, connection);
    if (parent == NULL) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    entry = h3_alloc_entry_locked(source,
        TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
        parent->side_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
        NULL,
        connection);
    if (entry != NULL) {
        entry->start_reported = true;
        entry->stream.action = action;
        entry->stream.classified = true;
        entry->stream.control = action == TREV_H3_DEMUX_ACTION_CONTROL;
    }
    pthread_mutex_unlock(&source->mutex);
    if (entry == NULL)
        return -EAGAIN;
    *out_stream = h3_handle(entry);
    return 0;
}

int trevrpc_rpc_transport_h3_test_make_control_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    trevrpc_rpc_transport_handle* out_stream) {
    return h3_test_make_protocol_stream(transport, connection, TREV_H3_DEMUX_ACTION_CONTROL, out_stream);
}

int trevrpc_rpc_transport_h3_test_make_qpack_encoder_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    trevrpc_rpc_transport_handle* out_stream) {
    return h3_test_make_protocol_stream(transport, connection, TREV_H3_DEMUX_ACTION_QPACK_ENCODER, out_stream);
}

int trevrpc_rpc_transport_h3_test_fail_connection(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, connection);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    entry->pending |= H3_PENDING_SHUTDOWN;
    h3_signal_locked(source);
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

int trevrpc_rpc_transport_h3_test_make_connect_stream(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle connection,
    trevrpc_rpc_transport_handle* out_stream) {
    h3_source* source = h3_from_base(transport);
    h3_entry* parent;
    h3_entry* entry;
    if (out_stream == NULL)
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    parent = h3_find_locked(source, connection);
    if (parent == NULL) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    entry = h3_alloc_entry_locked(source,
        TREVRPC_RPC_TRANSPORT_OBJECT_STREAM,
        parent->side_flags | TREVRPC_RPC_TRANSPORT_EVENT_FLAG_PEER,
        NULL,
        connection);
    if (entry != NULL) {
        entry->start_reported = true;
        entry->stream.action = TREV_H3_DEMUX_ACTION_REQUEST;
        entry->stream.classified = true;
        entry->stream.headers_received = true;
        entry->stream.connect_control = true;
        if (h3_init_connect_capsules_locked(entry, parent) != 0) {
            h3_detach_entry_locked(entry);
            entry = NULL;
        }
    }
    pthread_mutex_unlock(&source->mutex);
    if (entry == NULL)
        return -EAGAIN;
    *out_stream = h3_handle(entry);
    return 0;
}

int trevrpc_rpc_transport_h3_test_mark_connect_accepted(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle connection, trevrpc_rpc_transport_handle stream) {
    h3_source* source = h3_from_base(transport);
    h3_entry* parent;
    h3_entry* entry;
    pthread_mutex_lock(&source->mutex);
    parent = h3_find_locked(source, connection);
    entry = h3_find_locked(source, stream);
    if (parent == NULL || entry == NULL || parent->kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION ||
        entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM || !h3_handle_equal(entry->parent, connection) ||
        !h3_is_webtransport(parent) || !entry->stream.connect_control || entry->stream.capsules == NULL) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    parent->wt_session_ready = true;
    parent->connect_stream_id = 4;
    entry->stream.connect_accept_pending = false;
    entry->stream.connect_accepting = false;
    entry->stream.headers_sent = true;
    if (!parent->ready_reported) {
        if (h3_emit_locked(source,
                TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_READY,
                parent->side_flags,
                0,
                TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION,
                h3_handle(parent),
                parent->parent,
                parent->operation_id,
                0) != 0) {
            pthread_mutex_unlock(&source->mutex);
            return -EAGAIN;
        }
        parent->ready_reported = true;
    }
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

int trevrpc_rpc_transport_h3_test_inject_data(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle stream,
    const uint8_t* data,
    size_t data_len,
    bool fin) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    int result;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, stream);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    if (entry->stream.control) {
        result =
            h3_append_bytes(&entry->stream.parse, &entry->stream.parse_len, &entry->stream.parse_cap, data, data_len);
        if (result == 0)
            result = h3_parse_control_locked(source, entry, fin);
    } else if (entry->stream.action == TREV_H3_DEMUX_ACTION_QPACK_ENCODER) {
        result = h3_validate_qpack_encoder_bytes(data, data_len, fin);
    } else {
        result = h3_parse_stream_locked(source, entry, data, data_len, fin);
        /* Direct test injection bypasses the provider FIN callback. Mirror its
         * terminal wake for accepted CONNECT streams so implicit session
         * shutdown is exercised through the normal driver path. */
        if (result == 0 && fin && entry->stream.connect_control) {
            entry->pending |= H3_PENDING_TERMINAL;
            h3_signal_locked(source);
        }
    }
    pthread_mutex_unlock(&source->mutex);
    return result;
}

int trevrpc_rpc_transport_h3_test_retry_buffered_stream(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    int result;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, stream);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM || !entry->stream.classified) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    entry->pending &= ~H3_PENDING_READABLE;
    result = h3_process_classified_stream_locked(source, entry, NULL, 0, false);
    if (result != 0 && result != -EAGAIN) {
        entry->pending |= H3_PENDING_TERMINAL;
        h3_signal_locked(source);
    }
    pthread_mutex_unlock(&source->mutex);
    return result;
}

int trevrpc_rpc_transport_h3_test_emit(trevrpc_rpc_transport* transport,
    uint32_t kind,
    uint32_t flags,
    int32_t status,
    trevrpc_rpc_transport_handle subject,
    trevrpc_rpc_transport_handle parent,
    uint64_t operation_id) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    uint32_t subject_kind = TREVRPC_RPC_TRANSPORT_OBJECT_NONE;
    int result;
    trevrpc_rpc_transport_event* previous_tail;
    uint64_t previous_enqueued;
    pthread_mutex_lock(&source->mutex);
    previous_tail = source->event_tail;
    previous_enqueued = source->events_enqueued;
    entry = h3_find_any_locked(source, subject);
    if (entry != NULL)
        subject_kind = entry->kind;
    result = h3_emit_locked(source, kind, flags, status, subject_kind, subject, parent, operation_id, 0);
    /* This hook injects an observation only; it does not model the provider's
     * object-terminal transition. Keep the synthetic object live for tests that
     * inspect receive exhaustion after the injected event. */
    if (result == 0 && h3_object_terminal_kind(kind) && source->events_enqueued != previous_enqueued &&
        source->event_tail != previous_tail && source->event_tail != NULL) {
        source->event_tail->terminal_entry = NULL;
        if (entry != NULL)
            entry->terminal_pending = false;
    }
    pthread_mutex_unlock(&source->mutex);
    return result;
}
int trevrpc_rpc_transport_h3_test_stage_pending_send(trevrpc_rpc_transport* transport,
    trevrpc_rpc_transport_handle stream,
    uint64_t operation_id,
    size_t bytes,
    trevrpc_msquic_send_completion** out_completion) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    trevrpc_msquic_send_completion* completion;
    if (out_completion != NULL)
        *out_completion = NULL;
    if (operation_id == 0)
        return -EINVAL;
    completion = calloc(1, sizeof(*completion));
    if (completion == NULL)
        return -ENOMEM;
    if (pthread_mutex_init(&completion->mutex, NULL) != 0) {
        free(completion);
        return -ENOMEM;
    }
    if (pthread_cond_init(&completion->cond, NULL) != 0) {
        pthread_mutex_destroy(&completion->mutex);
        free(completion);
        return -ENOMEM;
    }
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, stream);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM ||
        entry->stream.pending_completion != NULL) {
        pthread_mutex_unlock(&source->mutex);
        trevrpc_msquic_send_completion_free(completion);
        return -ESTALE;
    }
    entry->send_event = h3_mandatory_event_alloc(source);
    if (entry->send_event == NULL) {
        pthread_mutex_unlock(&source->mutex);
        trevrpc_msquic_send_completion_free(completion);
        return -ENOMEM;
    }
    entry->stream.pending_completion = completion;
    entry->stream.pending_operation_id = operation_id;
    entry->stream.pending_send_bytes = bytes;
    ++source->pending_send_count;
    source->pending_send_bytes += bytes;
    if (out_completion != NULL)
        *out_completion = completion;
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

void trevrpc_rpc_transport_h3_test_signal_send_completion(trevrpc_msquic_send_completion* completion, int status) {
    assert(completion != NULL);
    pthread_mutex_lock(&completion->mutex);
    completion->completed = true;
    completion->result = status;
    pthread_cond_broadcast(&completion->cond);
    pthread_mutex_unlock(&completion->mutex);
}

int trevrpc_rpc_transport_h3_test_complete_pending_send(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, int status) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    trevrpc_msquic_send_completion* completion;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, stream);
    completion =
        entry != NULL && entry->kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM ? entry->stream.pending_completion : NULL;
    if (completion == NULL) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    trevrpc_rpc_transport_h3_test_signal_send_completion(completion, status);
    entry->pending |= H3_PENDING_SEND_COMPLETE;
    h3_signal_locked(source);
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

int trevrpc_rpc_transport_h3_test_stream_send_fin(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle stream, bool* out_send_fin) {
    h3_source* source = h3_from_base(transport);
    h3_entry* entry;
    if (out_send_fin == NULL)
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    entry = h3_find_locked(source, stream);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM) {
        pthread_mutex_unlock(&source->mutex);
        return -ESTALE;
    }
    *out_send_fin = entry->stream.send_fin;
    pthread_mutex_unlock(&source->mutex);
    return 0;
}

size_t trevrpc_rpc_transport_h3_test_pending_events(trevrpc_rpc_transport* transport) {
    h3_source* source = h3_from_base(transport);
    pthread_mutex_lock(&source->mutex);
    size_t n = source->event_depth;
    pthread_mutex_unlock(&source->mutex);
    return n;
}

void trevrpc_rpc_transport_h3_test_fail_event_alloc_after(
    trevrpc_rpc_transport* transport, size_t successful_allocations) {
    h3_source* source = h3_from_base(transport);
    pthread_mutex_lock(&source->mutex);
    source->event_alloc_budget = successful_allocations;
    pthread_mutex_unlock(&source->mutex);
}

void trevrpc_rpc_transport_h3_test_capture_acceptances(trevrpc_rpc_transport* transport, bool enabled) {
    h3_source* source = h3_from_base(transport);
    pthread_mutex_lock(&source->mutex);
    source->capture_acceptances = enabled;
    pthread_mutex_unlock(&source->mutex);
}

size_t trevrpc_rpc_transport_h3_test_acceptance_count(trevrpc_rpc_transport* transport) {
    h3_source* source = h3_from_base(transport);
    pthread_mutex_lock(&source->mutex);
    size_t count = source->acceptance_count;
    pthread_mutex_unlock(&source->mutex);
    return count;
}

void trevrpc_rpc_transport_h3_test_capture_rejections(trevrpc_rpc_transport* transport, bool enabled) {
    h3_source* source = h3_from_base(transport);
    pthread_mutex_lock(&source->mutex);
    source->capture_rejections = enabled;
    pthread_mutex_unlock(&source->mutex);
}

int trevrpc_rpc_transport_h3_test_last_rejection(
    trevrpc_rpc_transport* transport, uint16_t* out_status, size_t* out_count) {
    h3_source* source = h3_from_base(transport);
    if (out_status == NULL || out_count == NULL)
        return -EINVAL;
    pthread_mutex_lock(&source->mutex);
    *out_status = source->last_rejection_status;
    *out_count = source->rejection_count;
    pthread_mutex_unlock(&source->mutex);
    return 0;
}
#endif
