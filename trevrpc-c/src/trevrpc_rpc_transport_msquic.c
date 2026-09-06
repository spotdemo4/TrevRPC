#include "trevrpc_rpc_transport_msquic_internal.h"
#include "trevrpc_rpc_transport_h3_internal.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#define COMPOSITE_HASH_EMPTY 0u
#define COMPOSITE_HASH_TOMBSTONE UINT32_MAX

static atomic_uint_fast64_t composite_owner_sequence = ATOMIC_VAR_INIT(UINT64_C(0x200000));

typedef struct composite_shared_listener composite_shared_listener;

typedef struct composite_entry {
    trevrpc_rpc_transport* source;
    trevrpc_rpc_transport_handle local;
    uint32_t kind;
    uint32_t generation;
    uint32_t next_free;
    uint32_t parent_slot;
    uint32_t parent_generation;
    uint32_t child_refs;
    uint32_t event_refs;
    uint32_t receive_refs;
    uint32_t release_refs;
    uint32_t protocol_override;
    bool occupied;
    bool close_requested;
    bool terminal_seen;
    bool semantic_released;
} composite_entry;

typedef struct trevrpc_rpc_transport_msquic {
    trevrpc_rpc_transport base;
    trevrpc_rpc_transport* native_transport;
    trevrpc_rpc_transport* h3_transport;
    pthread_mutex_t mutex;
    pthread_cond_t condition;
    uint64_t owner;
    composite_entry* entries;
    uint32_t* local_hash;
    size_t entry_capacity;
    size_t local_hash_capacity;
    uint32_t free_head;
    uint64_t next_sequence;
    unsigned turn;
    trevrpc_rpc_transport_event* pending_events[2];
    int capacity_wake_read_fd;
    int capacity_wake_write_fd;
    bool capacity_wake_armed;
    bool capacity_waiting;
    int stop_status;
    bool native_stopped;
    bool h3_stopped;
    bool closing;
    size_t shared_listens_in_progress;
} trevrpc_rpc_transport_msquic;

struct composite_shared_listener {
    trevrpc_rpc_transport_msquic* composite;
    uint32_t slot;
    uint32_t generation;
    trevrpc_rpc_transport_endpoint_config native_config;
};

typedef struct composite_pin {
    uint32_t slot;
    uint32_t generation;
} composite_pin;

typedef struct composite_event {
    trevrpc_rpc_transport_msquic* composite;
    trevrpc_rpc_transport* source;
    trevrpc_rpc_transport_event* inner;
    trevrpc_rpc_transport_event_info info;
    trevrpc_rpc_transport_admission_info admission;
    trevrpc_rpc_transport_event_protocol_info protocol;
    composite_pin pins[3];
    size_t pin_count;
    bool has_admission;
    bool has_protocol;
} composite_event;

typedef struct composite_receive {
    trevrpc_rpc_transport_msquic* composite;
    trevrpc_rpc_transport* source;
    trevrpc_rpc_transport_receive* inner;
    composite_pin pin;
} composite_receive;

typedef struct composite_route {
    trevrpc_rpc_transport* source;
    trevrpc_rpc_transport_handle local;
    uint32_t kind;
    bool terminal_seen;
} composite_route;

static int composite_set_fd_flags(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0)
        return -errno;
    flags = fcntl(fd, F_GETFD, 0);
    if (flags < 0 || fcntl(fd, F_SETFD, flags | FD_CLOEXEC) != 0)
        return -errno;
    return 0;
}

static int composite_make_pipe(int fds[2]) {
    int result;
    if (pipe(fds) != 0)
        return -errno;
    result = composite_set_fd_flags(fds[0]);
    if (result == 0)
        result = composite_set_fd_flags(fds[1]);
    if (result != 0) {
        close(fds[0]);
        close(fds[1]);
        return result;
    }
    return 0;
}

static void composite_drain_capacity_wake_locked(trevrpc_rpc_transport_msquic* c) {
    uint8_t bytes[64];
    ssize_t read_count;
    /* The capacity pipe read end is permanently nonblocking. */
    // NOLINTBEGIN(clang-analyzer-unix.BlockInCriticalSection)
    do {
        read_count = read(c->capacity_wake_read_fd, bytes, sizeof(bytes));
    } while (read_count < 0 && errno == EINTR);
    while (read_count > 0) {
        do {
            read_count = read(c->capacity_wake_read_fd, bytes, sizeof(bytes));
        } while (read_count < 0 && errno == EINTR);
    }
    // NOLINTEND(clang-analyzer-unix.BlockInCriticalSection)
    c->capacity_wake_armed = false;
}

static void composite_signal_capacity_locked(trevrpc_rpc_transport_msquic* c) {
    uint8_t byte = 1;
    ssize_t written;
    if (!c->capacity_waiting || c->capacity_wake_armed)
        return;
    do {
        written = write(c->capacity_wake_write_fd, &byte, sizeof(byte));
    } while (written < 0 && errno == EINTR);
    if (written == (ssize_t)sizeof(byte) || (written < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)))
        c->capacity_wake_armed = true;
}

static void composite_clear_capacity_wait_locked(trevrpc_rpc_transport_msquic* c) {
    c->capacity_waiting = false;
    composite_drain_capacity_wake_locked(c);
}

static void composite_wait_for_capacity_locked(trevrpc_rpc_transport_msquic* c) {
    c->capacity_waiting = true;
    /* A rollback may have retired the temporary parent before the failure was
     * reported.  In that case the event can make progress immediately after
     * this call, so publish the already-available slot as a wake. */
    if (c->free_head != 0)
        composite_signal_capacity_locked(c);
}

static trevrpc_rpc_transport_msquic* composite_from_base(trevrpc_rpc_transport* t) {
    return (trevrpc_rpc_transport_msquic*)t;
}

static bool handle_equal(trevrpc_rpc_transport_handle a, trevrpc_rpc_transport_handle b) {
    return a.owner == b.owner && a.slot == b.slot && a.generation == b.generation;
}

static bool handle_zero(trevrpc_rpc_transport_handle a) {
    return a.owner == 0 && a.slot == 0 && a.generation == 0;
}

static trevrpc_rpc_transport_handle composite_external(
    const trevrpc_rpc_transport_msquic* c, uint32_t slot, const composite_entry* entry) {
    trevrpc_rpc_transport_handle result = {c->owner, slot, entry->generation};
    return result;
}

static uint64_t composite_local_hash_value(trevrpc_rpc_transport* source, trevrpc_rpc_transport_handle handle) {
    uint64_t value = (uint64_t)(uintptr_t)source;
    value ^= handle.owner + UINT64_C(0x9e3779b97f4a7c15) + (value << 6) + (value >> 2);
    value ^= ((uint64_t)handle.slot << 32) | handle.generation;
    value ^= value >> 30;
    value *= UINT64_C(0xbf58476d1ce4e5b9);
    value ^= value >> 27;
    value *= UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}

static composite_entry* find_external_locked(trevrpc_rpc_transport_msquic* c, trevrpc_rpc_transport_handle handle) {
    composite_entry* entry;
    if (handle.owner != c->owner || handle.slot == 0 || handle.slot > c->entry_capacity)
        return NULL;
    entry = &c->entries[(size_t)handle.slot - 1u];
    return entry->occupied && entry->generation == handle.generation ? entry : NULL;
}

static composite_entry* find_local_locked(trevrpc_rpc_transport_msquic* c,
    trevrpc_rpc_transport* source,
    trevrpc_rpc_transport_handle handle,
    uint32_t* out_slot) {
    size_t mask = c->local_hash_capacity - 1u;
    size_t index = (size_t)composite_local_hash_value(source, handle) & mask;
    size_t probes;
    for (probes = 0; probes < c->local_hash_capacity; ++probes) {
        uint32_t slot = c->local_hash[index];
        if (slot == COMPOSITE_HASH_EMPTY)
            return NULL;
        if (slot != COMPOSITE_HASH_TOMBSTONE) {
            composite_entry* entry = &c->entries[(size_t)slot - 1u];
            if (entry->occupied && entry->source == source && handle_equal(entry->local, handle)) {
                if (out_slot != NULL)
                    *out_slot = slot;
                return entry;
            }
        }
        index = (index + 1u) & mask;
    }
    return NULL;
}

static int composite_hash_insert_locked(trevrpc_rpc_transport_msquic* c,
    trevrpc_rpc_transport* source,
    trevrpc_rpc_transport_handle handle,
    uint32_t slot) {
    size_t mask = c->local_hash_capacity - 1u;
    size_t index = (size_t)composite_local_hash_value(source, handle) & mask;
    size_t tombstone = SIZE_MAX;
    size_t probes;
    for (probes = 0; probes < c->local_hash_capacity; ++probes) {
        uint32_t value = c->local_hash[index];
        if (value == COMPOSITE_HASH_EMPTY) {
            c->local_hash[tombstone == SIZE_MAX ? index : tombstone] = slot;
            return 0;
        }
        if (value == COMPOSITE_HASH_TOMBSTONE && tombstone == SIZE_MAX)
            tombstone = index;
        index = (index + 1u) & mask;
    }
    if (tombstone != SIZE_MAX) {
        c->local_hash[tombstone] = slot;
        return 0;
    }
    return -EAGAIN;
}

static void composite_hash_remove_locked(
    trevrpc_rpc_transport_msquic* c, trevrpc_rpc_transport* source, trevrpc_rpc_transport_handle handle) {
    size_t mask = c->local_hash_capacity - 1u;
    size_t index = (size_t)composite_local_hash_value(source, handle) & mask;
    size_t probes;
    for (probes = 0; probes < c->local_hash_capacity; ++probes) {
        uint32_t slot = c->local_hash[index];
        if (slot == COMPOSITE_HASH_EMPTY)
            return;
        if (slot != COMPOSITE_HASH_TOMBSTONE) {
            composite_entry* entry = &c->entries[(size_t)slot - 1u];
            if (entry->occupied && entry->source == source && handle_equal(entry->local, handle)) {
                c->local_hash[index] = COMPOSITE_HASH_TOMBSTONE;
                return;
            }
        }
        index = (index + 1u) & mask;
    }
}

static void composite_try_retire_locked(trevrpc_rpc_transport_msquic* c, uint32_t slot);

static void composite_release_parent_locked(trevrpc_rpc_transport_msquic* c, const composite_entry* child) {
    composite_entry* parent;
    if (child->parent_slot == 0 || child->parent_slot > c->entry_capacity)
        return;
    parent = &c->entries[(size_t)child->parent_slot - 1u];
    if (!parent->occupied || parent->generation != child->parent_generation)
        return;
    if (parent->child_refs != 0)
        --parent->child_refs;
    composite_try_retire_locked(c, child->parent_slot);
}

static void composite_try_retire_locked(trevrpc_rpc_transport_msquic* c, uint32_t slot) {
    composite_entry snapshot;
    composite_entry* entry;
    if (slot == 0 || slot > c->entry_capacity)
        return;
    entry = &c->entries[(size_t)slot - 1u];
    if (!entry->occupied || !entry->terminal_seen || !entry->semantic_released || entry->child_refs != 0 ||
        entry->event_refs != 0 || entry->receive_refs != 0 || entry->release_refs != 0)
        return;
    snapshot = *entry;
    composite_hash_remove_locked(c, entry->source, entry->local);
    memset(entry, 0, sizeof(*entry));
    if (snapshot.generation == UINT32_MAX) {
        /* A wrapped generation would make an old external handle valid again. */
        entry->generation = UINT32_MAX;
        composite_release_parent_locked(c, &snapshot);
        return;
    }
    entry->generation = snapshot.generation + 1u;
    entry->next_free = c->free_head;
    c->free_head = slot;
    composite_signal_capacity_locked(c);
    composite_release_parent_locked(c, &snapshot);
}

static composite_entry* register_locked(trevrpc_rpc_transport_msquic* c,
    trevrpc_rpc_transport* source,
    trevrpc_rpc_transport_handle local,
    uint32_t kind,
    composite_entry* parent,
    uint32_t* out_slot) {
    composite_entry* entry;
    uint32_t slot;
    if (handle_zero(local) || c->free_head == 0)
        return NULL;
    slot = c->free_head;
    entry = &c->entries[(size_t)slot - 1u];
    c->free_head = entry->next_free;
    entry->source = source;
    entry->local = local;
    entry->kind = kind;
    entry->next_free = 0;
    entry->parent_slot = 0;
    entry->parent_generation = 0;
    entry->child_refs = 0;
    entry->event_refs = 0;
    entry->receive_refs = 0;
    entry->release_refs = 0;
    entry->protocol_override = 0;
    entry->occupied = true;
    entry->close_requested = false;
    entry->terminal_seen = false;
    entry->semantic_released = false;
    if (parent != NULL) {
        entry->parent_slot = (uint32_t)(parent - c->entries) + 1u;
        entry->parent_generation = parent->generation;
        ++parent->child_refs;
    }
    if (composite_hash_insert_locked(c, source, local, slot) != 0) {
        if (parent != NULL)
            --parent->child_refs;
        entry->occupied = false;
        entry->next_free = c->free_head;
        c->free_head = slot;
        return NULL;
    }
    if (out_slot != NULL)
        *out_slot = slot;
    return entry;
}

static composite_entry* translate_locked(trevrpc_rpc_transport_msquic* c,
    trevrpc_rpc_transport* source,
    trevrpc_rpc_transport_handle local,
    uint32_t kind,
    composite_entry* parent,
    uint32_t* out_slot,
    bool* out_created,
    bool* out_capacity_blocked) {
    composite_entry* entry = find_local_locked(c, source, local, out_slot);
    if (out_created != NULL)
        *out_created = false;
    if (entry == NULL) {
        if (c->free_head == 0) {
            if (out_capacity_blocked != NULL)
                *out_capacity_blocked = true;
            return NULL;
        }
        entry = register_locked(c, source, local, kind, parent, out_slot);
        if (entry != NULL && out_created != NULL)
            *out_created = true;
        else if (entry == NULL && c->free_head == 0 && out_capacity_blocked != NULL)
            *out_capacity_blocked = true;
    } else if (parent != NULL && entry->parent_slot == 0) {
        entry->parent_slot = (uint32_t)(parent - c->entries) + 1u;
        entry->parent_generation = parent->generation;
        ++parent->child_refs;
    }
    return entry;
}

static void composite_discard_entry_locked(trevrpc_rpc_transport_msquic* c, uint32_t slot) {
    composite_entry snapshot;
    composite_entry* entry;
    if (slot == 0 || slot > c->entry_capacity)
        return;
    entry = &c->entries[(size_t)slot - 1u];
    if (!entry->occupied || entry->event_refs != 0 || entry->receive_refs != 0 || entry->release_refs != 0 ||
        entry->child_refs != 0)
        return;
    snapshot = *entry;
    composite_hash_remove_locked(c, entry->source, entry->local);
    memset(entry, 0, sizeof(*entry));
    if (snapshot.generation == UINT32_MAX) {
        entry->generation = UINT32_MAX;
        composite_release_parent_locked(c, &snapshot);
        return;
    }
    entry->generation = snapshot.generation + 1u;
    entry->next_free = c->free_head;
    c->free_head = slot;
    composite_signal_capacity_locked(c);
    composite_release_parent_locked(c, &snapshot);
}

static trevrpc_rpc_transport_handle route_locked(
    trevrpc_rpc_transport_msquic* c, trevrpc_rpc_transport_handle external, composite_entry** out) {
    composite_entry* entry = find_external_locked(c, external);
    if (out != NULL)
        *out = entry;
    return entry != NULL ? entry->local : (trevrpc_rpc_transport_handle){0};
}

static bool route_snapshot_locked(
    trevrpc_rpc_transport_msquic* c, trevrpc_rpc_transport_handle external, composite_route* out) {
    composite_entry* entry = find_external_locked(c, external);
    if (entry == NULL || entry->semantic_released || entry->release_refs != 0)
        return false;
    out->source = entry->source;
    out->local = entry->local;
    out->kind = entry->kind;
    out->terminal_seen = entry->close_requested || entry->terminal_seen;
    return true;
}

static bool composite_event_is_object_terminal(const trevrpc_rpc_transport_event_info* info) {
    return info->kind == TREVRPC_RPC_TRANSPORT_EVENT_LISTENER_STOPPED ||
           info->kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_FAILED ||
           info->kind == TREVRPC_RPC_TRANSPORT_EVENT_CONNECTION_CLOSED ||
           info->kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_FAILED ||
           info->kind == TREVRPC_RPC_TRANSPORT_EVENT_STREAM_CLOSED;
}

static uint32_t composite_parent_kind(const trevrpc_rpc_transport_event_info* info) {
    if (info->subject_kind == TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION)
        return TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER;
    if (info->subject_kind == TREVRPC_RPC_TRANSPORT_OBJECT_STREAM)
        return TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION;
    return TREVRPC_RPC_TRANSPORT_OBJECT_NONE;
}

static void composite_event_pin_locked(composite_event* event, uint32_t slot, composite_entry* entry) {
    size_t index;
    if (entry == NULL || slot == 0)
        return;
    for (index = 0; index < event->pin_count; ++index) {
        if (event->pins[index].slot == slot && event->pins[index].generation == entry->generation)
            return;
    }
    if (event->pin_count < sizeof(event->pins) / sizeof(event->pins[0])) {
        event->pins[event->pin_count].slot = slot;
        event->pins[event->pin_count].generation = entry->generation;
        ++event->pin_count;
        ++entry->event_refs;
    }
}

static int composite_get_wake_sources(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_wake* wakes, size_t cap, size_t* out_count) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(transport);
    size_t n = 0;
    int result;
    if (wakes == NULL || out_count == NULL || cap < 3)
        return -ENOSPC;
    result = trevrpc_rpc_transport_get_wake_sources(c->native_transport, wakes, cap - 2, &n);
    if (result != 0)
        return result;
    result = trevrpc_rpc_transport_get_wake_source(c->h3_transport, &wakes[n]);
    if (result != 0)
        return result;
    wakes[n + 1].kind = TREVRPC_RPC_TRANSPORT_WAKE_SOURCE_POSIX_FD;
    wakes[n + 1].flags = TREVRPC_RPC_TRANSPORT_WAKE_FLAG_BORROWED | TREVRPC_RPC_TRANSPORT_WAKE_FLAG_LEVEL_TRIGGERED;
    wakes[n + 1].native_handle = c->capacity_wake_read_fd;
    *out_count = n + 2;
    return 0;
}

static int composite_get_wake_source(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_wake* wake) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(transport);
    return trevrpc_rpc_transport_get_wake_source(c->native_transport, wake);
}

static int composite_next_event(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_event** out_event) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(transport);
    trevrpc_rpc_transport* sources[2] = {c->native_transport, c->h3_transport};
    size_t i;
    unsigned start_turn;
    int deferred_result = 0;
    bool capacity_blocked = false;
    if (out_event == NULL)
        return -EINVAL;
    *out_event = NULL;
    pthread_mutex_lock(&c->mutex);
    composite_drain_capacity_wake_locked(c);
    pthread_mutex_unlock(&c->mutex);
    pthread_mutex_lock(&c->mutex);
    start_turn = c->turn;
    pthread_mutex_unlock(&c->mutex);
    for (i = 0; i < 2; ++i) {
        size_t index = (start_turn + i) & 1u;
        trevrpc_rpc_transport_event* inner;
        int result;

        /* Endpoint admission holds this mutex until its local handle is
         * registered.  Keep it held through source dequeue and translation:
         * otherwise a synchronously completed dial can have its READY or
         * FAILED event consumed before the composite mapping exists. */
        pthread_mutex_lock(&c->mutex);
        if (index == 1u && c->shared_listens_in_progress) {
            pthread_mutex_unlock(&c->mutex);
            deferred_result = -EAGAIN;
            continue;
        }
        inner = c->pending_events[index];
        if (inner == NULL) {
            result = trevrpc_rpc_transport_next_event(sources[index], &inner);
            if (result == 0)
                c->pending_events[index] = inner;
        } else {
            result = 0;
        }
        if (result != 0) {
            pthread_mutex_unlock(&c->mutex);
            if (result != -EAGAIN)
                return result;
            continue;
        }
        {
            trevrpc_rpc_transport_event_info source_info;
            composite_event* event;
            composite_entry* parent = NULL;
            composite_entry* subject = NULL;
            bool parent_created = false;
            uint32_t parent_slot = 0;
            uint32_t subject_slot = 0;

            result = trevrpc_rpc_transport_event_get_info(sources[index], inner, &source_info);
            if (result != 0) {
                if (result == -ENOMEM || result == -EAGAIN) {
                    pthread_mutex_unlock(&c->mutex);
                    deferred_result = result;
                    continue;
                }
                c->pending_events[index] = NULL;
                c->turn = (unsigned)(index + 1u) & 1u;
                pthread_mutex_unlock(&c->mutex);
                trevrpc_rpc_transport_event_release(sources[index], inner);
                return result;
            }
            event = calloc(1, sizeof(*event));
            if (event == NULL) {
                pthread_mutex_unlock(&c->mutex);
                deferred_result = -ENOMEM;
                continue;
            }
            event->composite = c;
            event->source = sources[index];
            event->inner = inner;
            event->info = source_info;
            if (source_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_STOPPED) {
                bool all_stopped;
                if (sources[index] == c->native_transport)
                    c->native_stopped = true;
                else
                    c->h3_stopped = true;
                if (c->stop_status == 0 && source_info.status != 0)
                    c->stop_status = source_info.status;
                all_stopped = c->native_stopped && c->h3_stopped;
                event->info.status = c->stop_status;
                if (!all_stopped) {
                    c->pending_events[index] = NULL;
                    c->turn = (unsigned)(index + 1u) & 1u;
                    pthread_mutex_unlock(&c->mutex);
                    trevrpc_rpc_transport_event_release(sources[index], inner);
                    free(event);
                    continue;
                }
            }
            if (!handle_zero(source_info.parent)) {
                /* A terminal child may arrive after its parent mapping retired.  In
                 * that case retain the zero parent rather than resurrecting a
                 * nonterminal mapping that no API caller can release. */
                bool parent_known = find_local_locked(c, event->source, source_info.parent, &parent_slot) != NULL;
                if (!parent_known && composite_event_is_object_terminal(&source_info)) {
                    event->info.parent = (trevrpc_rpc_transport_handle){0};
                } else {
                    parent = translate_locked(c,
                        event->source,
                        source_info.parent,
                        composite_parent_kind(&source_info),
                        NULL,
                        &parent_slot,
                        &parent_created,
                        &capacity_blocked);
                    if (parent == NULL)
                        result = -EAGAIN;
                    else
                        event->info.parent = composite_external(c, parent_slot, parent);
                }
            }
            if (result == 0 && !handle_zero(source_info.subject)) {
                subject = translate_locked(c,
                    event->source,
                    source_info.subject,
                    source_info.subject_kind,
                    parent,
                    &subject_slot,
                    NULL,
                    &capacity_blocked);
                if (subject == NULL)
                    result = -EAGAIN;
                else {
                    event->info.subject = composite_external(c, subject_slot, subject);
                    if (subject->parent_slot != 0 && subject->parent_slot <= c->entry_capacity) {
                        composite_entry* mapped_parent = &c->entries[(size_t)subject->parent_slot - 1u];
                        if (mapped_parent->occupied && mapped_parent->generation == subject->parent_generation) {
                            parent = mapped_parent;
                            parent_slot = subject->parent_slot;
                            event->info.parent = composite_external(c, parent_slot, parent);
                        }
                    }
                    if (composite_event_is_object_terminal(&source_info))
                        subject->terminal_seen = true;
                }
            }
            if (result == 0 && (source_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_HTTP3_ADMISSION ||
                                   source_info.kind == TREVRPC_RPC_TRANSPORT_EVENT_WEBTRANSPORT_ADMISSION)) {
                composite_entry* listener;
                uint32_t listener_slot = 0;
                result = trevrpc_rpc_transport_event_get_admission_info(event->source, event->inner, &event->admission);
                if (result == 0) {
                    listener = find_local_locked(c, event->source, event->admission.listener, &listener_slot);
                    if (listener == NULL || listener->kind != TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER) {
                        result = -ESTALE;
                    } else {
                        event->admission.listener = composite_external(c, listener_slot, listener);
                        event->has_admission = true;
                        composite_event_pin_locked(event, listener_slot, listener);
                    }
                }
            }
            if (result == 0) {
                int protocol_result =
                    trevrpc_rpc_transport_event_get_protocol_info(event->source, event->inner, &event->protocol);
                if (protocol_result == 0) {
                    if (subject != NULL && subject->protocol_override != 0)
                        event->protocol.protocol = subject->protocol_override;
                    event->has_protocol = true;
                } else if (protocol_result != -ENOTSUP)
                    result = protocol_result;
            }
            if (result == 0) {
                event->info.sequence = c->next_sequence++;
                composite_event_pin_locked(event, parent_slot, parent);
                composite_event_pin_locked(event, subject_slot, subject);
                c->pending_events[index] = NULL;
                if (capacity_blocked)
                    composite_wait_for_capacity_locked(c);
                else
                    composite_clear_capacity_wait_locked(c);
                c->turn = (unsigned)(index + 1u) & 1u;
            } else if (parent_created) {
                composite_discard_entry_locked(c, parent_slot);
                if (capacity_blocked)
                    composite_wait_for_capacity_locked(c);
                else
                    composite_clear_capacity_wait_locked(c);
            } else if (capacity_blocked) {
                composite_wait_for_capacity_locked(c);
            } else {
                composite_clear_capacity_wait_locked(c);
            }
            if (result != 0 && event->pin_count != 0) {
                size_t pin_index;
                for (pin_index = 0; pin_index < event->pin_count; ++pin_index) {
                    composite_pin pin = event->pins[pin_index];
                    composite_entry* pinned =
                        pin.slot != 0 && pin.slot <= c->entry_capacity ? &c->entries[(size_t)pin.slot - 1u] : NULL;
                    if (pinned != NULL && pinned->occupied && pinned->generation == pin.generation &&
                        pinned->event_refs != 0)
                        --pinned->event_refs;
                }
                event->pin_count = 0;
            }
            pthread_mutex_unlock(&c->mutex);
            if (result != 0) {
                free(event);
                if (result == -ENOMEM || result == -EAGAIN) {
                    deferred_result = result;
                    continue;
                }
                return result;
            }
            *out_event = (trevrpc_rpc_transport_event*)event;
            return 0;
        }
    }
    pthread_mutex_lock(&c->mutex);
    if (capacity_blocked) {
        composite_wait_for_capacity_locked(c);
    } else if (c->pending_events[0] != NULL || c->pending_events[1] != NULL) {
        c->capacity_waiting = true;
        composite_signal_capacity_locked(c);
    } else {
        composite_clear_capacity_wait_locked(c);
    }
    pthread_mutex_unlock(&c->mutex);
    return deferred_result != 0 ? deferred_result : -EAGAIN;
}

static int composite_event_get_info(
    const trevrpc_rpc_transport_event* event_base, trevrpc_rpc_transport_event_info* info) {
    const composite_event* event = (const composite_event*)event_base;
    if (event == NULL || info == NULL)
        return -EINVAL;
    *info = event->info;
    return 0;
}

static int composite_event_get_admission_info(
    const trevrpc_rpc_transport_event* event_base, trevrpc_rpc_transport_admission_info* info) {
    const composite_event* event = (const composite_event*)event_base;
    if (event == NULL || info == NULL)
        return -EINVAL;
    if (!event->has_admission)
        return -ENOTSUP;
    *info = event->admission;
    return 0;
}

static int composite_event_get_protocol_info(
    const trevrpc_rpc_transport_event* event_base, trevrpc_rpc_transport_event_protocol_info* info) {
    const composite_event* event = (const composite_event*)event_base;
    if (event == NULL || info == NULL)
        return -EINVAL;
    if (!event->has_protocol)
        return -ENOTSUP;
    *info = event->protocol;
    return 0;
}

static int composite_admission_respond(const trevrpc_rpc_transport_event* event_base, uint16_t status) {
    const composite_event* event = (const composite_event*)event_base;
    if (event == NULL)
        return -EINVAL;
    return trevrpc_rpc_transport_admission_respond(event->source, event->inner, status);
}

static void composite_event_release(trevrpc_rpc_transport_event* event_base) {
    composite_event* event = (composite_event*)event_base;
    size_t index;
    if (event == NULL)
        return;
    trevrpc_rpc_transport_event_release(event->source, event->inner);
    pthread_mutex_lock(&event->composite->mutex);
    for (index = 0; index < event->pin_count; ++index) {
        composite_pin pin = event->pins[index];
        composite_entry* entry;
        if (pin.slot == 0 || pin.slot > event->composite->entry_capacity)
            continue;
        entry = &event->composite->entries[(size_t)pin.slot - 1u];
        if (!entry->occupied || entry->generation != pin.generation)
            continue;
        if (entry->event_refs != 0)
            --entry->event_refs;
        composite_try_retire_locked(event->composite, pin.slot);
    }
    pthread_mutex_unlock(&event->composite->mutex);
    free(event);
}

static int composite_receive_get_info(
    const trevrpc_rpc_transport_receive* receive_base, trevrpc_rpc_transport_receive_info* info) {
    composite_receive* receive = (composite_receive*)receive_base;
    return trevrpc_rpc_transport_receive_get_info(receive->source, receive->inner, info);
}

static void composite_receive_release(trevrpc_rpc_transport_receive* receive_base) {
    composite_receive* receive = (composite_receive*)receive_base;
    composite_entry* entry;
    if (receive == NULL)
        return;
    trevrpc_rpc_transport_receive_release(receive->source, receive->inner);
    pthread_mutex_lock(&receive->composite->mutex);
    if (receive->pin.slot != 0 && receive->pin.slot <= receive->composite->entry_capacity) {
        entry = &receive->composite->entries[(size_t)receive->pin.slot - 1u];
        if (entry->occupied && entry->generation == receive->pin.generation) {
            if (entry->receive_refs != 0)
                --entry->receive_refs;
            pthread_cond_broadcast(&receive->composite->condition);
            composite_try_retire_locked(receive->composite, receive->pin.slot);
        }
    }
    pthread_mutex_unlock(&receive->composite->mutex);
    free(receive);
}

static int composite_poll_timeout_ms(trevrpc_rpc_transport* transport) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(transport);
    int native_timeout = trevrpc_rpc_transport_poll_timeout_ms(c->native_transport);
    int h3_timeout = trevrpc_rpc_transport_poll_timeout_ms(c->h3_transport);
    if (native_timeout < 0)
        return h3_timeout;
    if (h3_timeout < 0)
        return native_timeout;
    return native_timeout < h3_timeout ? native_timeout : h3_timeout;
}

static int composite_get_diagnostics(trevrpc_rpc_transport* transport, trevrpc_rpc_transport_diagnostics* d) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(transport);
    trevrpc_rpc_transport_diagnostics a = {0}, b = {0};
    int r;
    if (d == NULL)
        return -EINVAL;
    r = trevrpc_rpc_transport_get_diagnostics(c->native_transport, &a);
    if (r != 0)
        return r;
    r = trevrpc_rpc_transport_get_diagnostics(c->h3_transport, &b);
    if (r != 0)
        return r;
    *d = a;
    if (a.state == TREVRPC_RPC_TRANSPORT_STATE_STOPPED && b.state == TREVRPC_RPC_TRANSPORT_STATE_STOPPED)
        d->state = TREVRPC_RPC_TRANSPORT_STATE_STOPPED;
    else if (a.state != TREVRPC_RPC_TRANSPORT_STATE_RUNNING || b.state != TREVRPC_RPC_TRANSPORT_STATE_RUNNING)
        d->state = TREVRPC_RPC_TRANSPORT_STATE_STOPPING;
    else
        d->state = TREVRPC_RPC_TRANSPORT_STATE_RUNNING;
    d->terminal_status = a.terminal_status ? a.terminal_status : b.terminal_status;
    d->event_capacity = a.event_capacity + b.event_capacity;
    d->provider_error_code = a.provider_error_code ? a.provider_error_code : b.provider_error_code;
    d->queue_depth = a.queue_depth + b.queue_depth;
    d->ordinary_queue_depth = a.ordinary_queue_depth + b.ordinary_queue_depth;
#define SUM(field) d->field = a.field + b.field
    SUM(events_enqueued);
    SUM(events_dequeued);
    SUM(events_rejected);
    SUM(receive_owned_count);
    SUM(peak_receive_owned_count);
    SUM(receive_owned_bytes);
    SUM(peak_receive_owned_bytes);
    SUM(pending_send_bytes);
    SUM(pending_send_count);
    SUM(live_listeners);
    SUM(live_connections);
    SUM(live_streams);
    SUM(active_callbacks);
    SUM(active_api_calls);
    SUM(wake_signals);
    SUM(wake_write_eagain);
    SUM(wake_failures);
    SUM(mandatory_reservations);
#undef SUM
    return 0;
}

static composite_entry* composite_reserve_listener_locked(trevrpc_rpc_transport_msquic* c, uint32_t* out_slot) {
    composite_entry* entry;
    uint32_t slot;
    if (c->free_head == 0)
        return NULL;
    slot = c->free_head;
    entry = &c->entries[(size_t)slot - 1u];
    c->free_head = entry->next_free;
    entry->source = NULL;
    entry->local = (trevrpc_rpc_transport_handle){0};
    entry->kind = TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER;
    entry->next_free = 0;
    entry->parent_slot = 0;
    entry->parent_generation = 0;
    entry->child_refs = 0;
    entry->event_refs = 0;
    entry->receive_refs = 0;
    entry->release_refs = 0;
    entry->protocol_override = TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED;
    entry->occupied = true;
    entry->close_requested = false;
    entry->terminal_seen = false;
    entry->semantic_released = false;
    *out_slot = slot;
    return entry;
}

static void composite_bind_reserved_listener_locked(trevrpc_rpc_transport_msquic* c,
    composite_entry* entry,
    uint32_t slot,
    trevrpc_rpc_transport* source,
    trevrpc_rpc_transport_handle local) {
    if (entry == NULL || !entry->occupied || !handle_zero(entry->local) || handle_zero(local))
        abort();
    entry->source = source;
    entry->local = local;
    if (composite_hash_insert_locked(c, source, local, slot) != 0)
        abort();
}

static trevrpc_msquic_accept_disposition composite_shared_accept_dispatch(
    void* context, trevrpc_msquic_accepted_connection* accepted) {
    static const uint8_t native_alpn[] = "trevrpc/1";
    static const uint8_t h3_alpn[] = "h3";
    composite_shared_listener* shared = context;
    trevrpc_rpc_transport_msquic* c;
    const uint8_t* alpn = NULL;
    size_t alpn_len = 0;
    trevrpc_rpc_transport_handle local;
    composite_entry* parent;
    composite_entry* child;
    uint32_t child_slot;
    bool adopted = false;
    int result;
    if (shared == NULL || accepted == NULL ||
        trevrpc_msquic_accepted_connection_get_alpn(accepted, &alpn, &alpn_len) != 0)
        return TREV_MSQUIC_ACCEPT_REJECTED;
    if (alpn_len == sizeof(h3_alpn) - 1u && memcmp(alpn, h3_alpn, alpn_len) == 0)
        return TREV_MSQUIC_ACCEPT_FALLTHROUGH;
    if (alpn_len != sizeof(native_alpn) - 1u || memcmp(alpn, native_alpn, alpn_len) != 0)
        return TREV_MSQUIC_ACCEPT_REJECTED;
    c = shared->composite;
    pthread_mutex_lock(&c->mutex);
    parent = shared->slot != 0 && shared->slot <= c->entry_capacity ? &c->entries[(size_t)shared->slot - 1u] : NULL;
    if (parent == NULL || !parent->occupied || parent->generation != shared->generation || c->closing ||
        parent->close_requested || parent->terminal_seen || c->free_head == 0) {
        pthread_mutex_unlock(&c->mutex);
        return TREV_MSQUIC_ACCEPT_REJECTED;
    }
    child_slot = c->free_head;
    child = &c->entries[(size_t)child_slot - 1u];
    c->free_head = child->next_free;
    child->source = c->native_transport;
    child->local = (trevrpc_rpc_transport_handle){0};
    child->kind = TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION;
    child->next_free = 0;
    child->parent_slot = shared->slot;
    child->parent_generation = parent->generation;
    child->child_refs = 0;
    child->event_refs = 0;
    child->receive_refs = 0;
    child->release_refs = 0;
    child->protocol_override = 0;
    child->occupied = true;
    child->close_requested = false;
    child->terminal_seen = false;
    child->semantic_released = false;
    ++parent->child_refs;
    result = c->native_transport->ops->adopt_accepted_connection != NULL
                 ? c->native_transport->ops->adopt_accepted_connection(
                       c->native_transport, &shared->native_config, accepted, &local)
                 : -ENOTSUP;
    if (result == 0) {
        adopted = true;
        child->local = local;
        if (composite_hash_insert_locked(c, c->native_transport, local, child_slot) != 0)
            abort();
    } else {
        composite_discard_entry_locked(c, child_slot);
    }
    pthread_mutex_unlock(&c->mutex);
    return adopted ? TREV_MSQUIC_ACCEPT_ADOPTED : TREV_MSQUIC_ACCEPT_REJECTED;
}

static trevrpc_rpc_transport* select_endpoint(trevrpc_rpc_transport_msquic* c, uint32_t protocol) {
    return protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_HTTP3 || protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_WEBTRANSPORT
               ? c->h3_transport
               : c->native_transport;
}
static int composite_endpoint_listen(
    trevrpc_rpc_transport* t, const trevrpc_rpc_transport_endpoint_config* cfg, trevrpc_rpc_transport_handle* out) {
    static const char shared_host[] = "shared";
    static const uint8_t native_alpn[] = "trevrpc/1";
    trevrpc_rpc_transport_msquic* c = composite_from_base(t);
    trevrpc_rpc_transport* source = select_endpoint(c, cfg->protocol);
    trevrpc_rpc_transport_handle local = {0};
    composite_entry* entry;
    composite_shared_listener* shared = NULL;
    uint32_t slot = 0;
    int r;
    if (cfg->protocol == TREVRPC_RPC_TRANSPORT_PROTOCOL_MULTIPLEXED) {
        shared = calloc(1, sizeof(*shared));
        if (shared == NULL)
            return -ENOMEM;
        pthread_mutex_lock(&c->mutex);
        if (c->closing) {
            pthread_mutex_unlock(&c->mutex);
            free(shared);
            return -ESHUTDOWN;
        }
        entry = composite_reserve_listener_locked(c, &slot);
        if (entry == NULL) {
            composite_wait_for_capacity_locked(c);
            pthread_mutex_unlock(&c->mutex);
            free(shared);
            return -EAGAIN;
        }
        shared->composite = c;
        shared->slot = slot;
        shared->generation = entry->generation;
        shared->native_config = *cfg;
        shared->native_config.protocol = TREVRPC_RPC_TRANSPORT_PROTOCOL_NATIVE;
        shared->native_config.host = shared_host;
        shared->native_config.host_len = sizeof(shared_host) - 1u;
        shared->native_config.alpn = native_alpn;
        shared->native_config.alpn_len = sizeof(native_alpn) - 1u;
        shared->native_config.server_name = NULL;
        shared->native_config.server_name_len = 0;
        shared->native_config.cert_file = NULL;
        shared->native_config.cert_file_len = 0;
        shared->native_config.key_file = NULL;
        shared->native_config.key_file_len = 0;
        shared->native_config.ca_cert_file = NULL;
        shared->native_config.ca_cert_file_len = 0;
        shared->native_config.cert_data = NULL;
        shared->native_config.cert_data_len = 0;
        shared->native_config.key_data = NULL;
        shared->native_config.key_data_len = 0;
        shared->native_config.ca_cert_data = NULL;
        shared->native_config.ca_cert_data_len = 0;
        shared->native_config.path = NULL;
        shared->native_config.path_len = 0;
        shared->native_config.origin = NULL;
        shared->native_config.origin_len = 0;
        ++c->shared_listens_in_progress;
        pthread_mutex_unlock(&c->mutex);
        r = trevrpc_rpc_transport_h3_listen_shared(
            c->h3_transport, cfg, composite_shared_accept_dispatch, shared, free, &local);
        pthread_mutex_lock(&c->mutex);
        entry = &c->entries[(size_t)slot - 1u];
        if (r == 0 && c->closing)
            r = -ESHUTDOWN;
        if (r == 0)
            composite_bind_reserved_listener_locked(c, entry, slot, c->h3_transport, local);
        if (c->shared_listens_in_progress != 0)
            --c->shared_listens_in_progress;
        pthread_cond_broadcast(&c->condition);
        composite_signal_capacity_locked(c);
        if (r == 0) {
            *out = composite_external(c, slot, entry);
        } else if (entry->child_refs == 0) {
            composite_discard_entry_locked(c, slot);
        } else {
            entry->terminal_seen = true;
            entry->semantic_released = true;
            composite_try_retire_locked(c, slot);
        }
        pthread_mutex_unlock(&c->mutex);
        if (r != 0 && !handle_zero(local))
            (void)trevrpc_rpc_transport_listener_close(c->h3_transport, local);
        return r;
    }
    pthread_mutex_lock(&c->mutex);
    if (c->closing) {
        pthread_mutex_unlock(&c->mutex);
        return -ESHUTDOWN;
    }
    if (c->free_head == 0) {
        composite_wait_for_capacity_locked(c);
        pthread_mutex_unlock(&c->mutex);
        return -EAGAIN;
    }
    r = trevrpc_rpc_transport_endpoint_listen(source, cfg, &local);
    if (r == 0) {
        entry = register_locked(c, source, local, TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER, NULL, &slot);
        if (entry != NULL)
            *out = composite_external(c, slot, entry);
    } else {
        entry = NULL;
    }
    pthread_mutex_unlock(&c->mutex);
    if (r != 0)
        return r;
    if (entry == NULL) {
        trevrpc_rpc_transport_listener_close(source, local);
        return -EAGAIN;
    }
    return 0;
}
static int composite_endpoint_get_port(trevrpc_rpc_transport* t, trevrpc_rpc_transport_handle h, uint16_t* p) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(t);
    composite_route route;
    bool found;
    pthread_mutex_lock(&c->mutex);
    found = route_snapshot_locked(c, h, &route);
    pthread_mutex_unlock(&c->mutex);
    if (!found || route.kind != TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER || route.terminal_seen)
        return -ESTALE;
    return trevrpc_rpc_transport_endpoint_get_port(route.source, route.local, p);
}
static int composite_endpoint_dial(trevrpc_rpc_transport* t,
    const trevrpc_rpc_transport_endpoint_config* cfg,
    uint64_t op,
    trevrpc_rpc_transport_handle* out) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(t);
    trevrpc_rpc_transport* source = select_endpoint(c, cfg->protocol);
    trevrpc_rpc_transport_handle local;
    composite_entry* entry;
    uint32_t slot = 0;
    int r;
    pthread_mutex_lock(&c->mutex);
    if (c->free_head == 0) {
        composite_wait_for_capacity_locked(c);
        pthread_mutex_unlock(&c->mutex);
        return -EAGAIN;
    }
    r = trevrpc_rpc_transport_endpoint_dial(source, cfg, op, &local);
    if (r == 0) {
        entry = register_locked(c, source, local, TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION, NULL, &slot);
        if (entry != NULL)
            *out = composite_external(c, slot, entry);
    } else {
        entry = NULL;
    }
    pthread_mutex_unlock(&c->mutex);
    if (r != 0)
        return r;
    if (entry == NULL) {
        trevrpc_rpc_transport_connection_close(source, local, 0);
        return -EAGAIN;
    }
    return 0;
}
static int composite_dial_cancel(trevrpc_rpc_transport* t, trevrpc_rpc_transport_handle h) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(t);
    composite_route route;
    bool found;
    pthread_mutex_lock(&c->mutex);
    found = route_snapshot_locked(c, h, &route);
    pthread_mutex_unlock(&c->mutex);
    if (!found || route.kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION || route.terminal_seen)
        return -ESTALE;
    return trevrpc_rpc_transport_dial_cancel(route.source, route.local);
}

static int composite_connection_close(trevrpc_rpc_transport* t, trevrpc_rpc_transport_handle h, uint64_t code) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(t);
    composite_route route;
    bool found;
    pthread_mutex_lock(&c->mutex);
    found = route_snapshot_locked(c, h, &route);
    pthread_mutex_unlock(&c->mutex);
    if (!found || route.kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION || route.terminal_seen)
        return -ESTALE;
    return trevrpc_rpc_transport_connection_close(route.source, route.local, code);
}

static int composite_listener_close(trevrpc_rpc_transport* t, trevrpc_rpc_transport_handle h) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(t);
    composite_route route;
    composite_entry* entry;
    int result;
    pthread_mutex_lock(&c->mutex);
    entry = find_external_locked(c, h);
    if (entry == NULL || entry->semantic_released || entry->release_refs != 0 ||
        entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_LISTENER || entry->close_requested || entry->terminal_seen) {
        pthread_mutex_unlock(&c->mutex);
        return -ESTALE;
    }
    entry->close_requested = true;
    route.source = entry->source;
    route.local = entry->local;
    route.kind = entry->kind;
    route.terminal_seen = false;
    pthread_mutex_unlock(&c->mutex);

    result = trevrpc_rpc_transport_listener_close(route.source, route.local);
    if (result != 0) {
        pthread_mutex_lock(&c->mutex);
        entry = find_external_locked(c, h);
        if (entry != NULL && !entry->terminal_seen)
            entry->close_requested = false;
        pthread_mutex_unlock(&c->mutex);
    }
    return result;
}

static int composite_stream_open(
    trevrpc_rpc_transport* t, trevrpc_rpc_transport_handle h, uint64_t op, trevrpc_rpc_transport_handle* out) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(t);
    composite_route route;
    trevrpc_rpc_transport_handle child;
    composite_entry* entry;
    uint32_t slot = 0;
    bool found;
    int failure = -EAGAIN;
    int result;
    pthread_mutex_lock(&c->mutex);
    found = route_snapshot_locked(c, h, &route);
    if (!found || route.kind != TREVRPC_RPC_TRANSPORT_OBJECT_CONNECTION || route.terminal_seen) {
        pthread_mutex_unlock(&c->mutex);
        return -ESTALE;
    }
    if (c->free_head == 0) {
        composite_wait_for_capacity_locked(c);
        pthread_mutex_unlock(&c->mutex);
        return -EAGAIN;
    }
    result = trevrpc_rpc_transport_stream_open(route.source, route.local, op, &child);
    if (result != 0) {
        pthread_mutex_unlock(&c->mutex);
        return result;
    }
    {
        composite_entry* current_parent = find_external_locked(c, h);
        if (current_parent == NULL || current_parent->terminal_seen || current_parent->source != route.source ||
            !handle_equal(current_parent->local, route.local)) {
            entry = NULL;
            failure = -ESTALE;
        } else {
            entry = register_locked(c, route.source, child, TREVRPC_RPC_TRANSPORT_OBJECT_STREAM, current_parent, &slot);
        }
        if (entry != NULL)
            *out = composite_external(c, slot, entry);
    }
    pthread_mutex_unlock(&c->mutex);
    if (entry == NULL) {
        trevrpc_rpc_transport_stream_close(route.source, child);
        return failure;
    }
    return 0;
}
static int composite_stream_send(
    trevrpc_rpc_transport* t, trevrpc_rpc_transport_handle h, uint64_t op, const uint8_t* data, size_t len) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(t);
    composite_route route;
    bool found;
    pthread_mutex_lock(&c->mutex);
    found = route_snapshot_locked(c, h, &route);
    pthread_mutex_unlock(&c->mutex);
    if (!found || route.kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM || route.terminal_seen)
        return -ESTALE;
    return trevrpc_rpc_transport_stream_send(route.source, route.local, op, data, len);
}
static int composite_stream_receive(
    trevrpc_rpc_transport* t, trevrpc_rpc_transport_handle h, trevrpc_rpc_transport_receive** out) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(t);
    composite_entry* entry;
    trevrpc_rpc_transport* source;
    trevrpc_rpc_transport_handle local;
    trevrpc_rpc_transport_receive* inner = NULL;
    composite_receive* wrapper;
    int result;
    if (out == NULL)
        return -EINVAL;
    *out = NULL;
    pthread_mutex_lock(&c->mutex);
    local = route_locked(c, h, &entry);
    if (entry == NULL || entry->kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM || entry->semantic_released ||
        entry->release_refs != 0) {
        pthread_mutex_unlock(&c->mutex);
        return -ESTALE;
    }
    source = entry->source;
    ++entry->receive_refs;
    pthread_mutex_unlock(&c->mutex);

    /* Reserve the wrapper before consuming the source receive.  If allocation
     * fails, the source payload remains available for a later retry. */
    wrapper = calloc(1, sizeof(*wrapper));
    if (wrapper == NULL) {
        pthread_mutex_lock(&c->mutex);
        entry = find_external_locked(c, h);
        if (entry != NULL && entry->receive_refs != 0) {
            --entry->receive_refs;
            pthread_cond_broadcast(&c->condition);
            composite_try_retire_locked(c, h.slot);
        }
        pthread_mutex_unlock(&c->mutex);
        return -ENOMEM;
    }
    result = trevrpc_rpc_transport_stream_receive(source, local, &inner);
    if (result != 0) {
        free(wrapper);
        pthread_mutex_lock(&c->mutex);
        entry = find_external_locked(c, h);
        if (entry != NULL && entry->receive_refs != 0) {
            --entry->receive_refs;
            pthread_cond_broadcast(&c->condition);
            composite_try_retire_locked(c, h.slot);
        }
        pthread_mutex_unlock(&c->mutex);
        return result;
    }
    wrapper->composite = c;
    wrapper->source = source;
    wrapper->inner = inner;
    wrapper->pin.slot = h.slot;
    wrapper->pin.generation = h.generation;
    *out = (trevrpc_rpc_transport_receive*)wrapper;
    return 0;
}
#define STREAM_ROUTED(name)                                                                                            \
    static int composite_stream_##name(trevrpc_rpc_transport* t, trevrpc_rpc_transport_handle h) {                     \
        trevrpc_rpc_transport_msquic* c = composite_from_base(t);                                                      \
        composite_route route;                                                                                         \
        bool found;                                                                                                    \
        pthread_mutex_lock(&c->mutex);                                                                                 \
        found = route_snapshot_locked(c, h, &route);                                                                   \
        pthread_mutex_unlock(&c->mutex);                                                                               \
        if (!found || route.kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM || route.terminal_seen)                        \
            return -ESTALE;                                                                                            \
        return trevrpc_rpc_transport_stream_##name(route.source, route.local);                                         \
    }
STREAM_ROUTED(finish_send)
STREAM_ROUTED(close)
#define STREAM_ABORT_ROUTED(name)                                                                                      \
    static int composite_stream_abort_##name(                                                                          \
        trevrpc_rpc_transport* t, trevrpc_rpc_transport_handle h, uint64_t code) {                                     \
        trevrpc_rpc_transport_msquic* c = composite_from_base(t);                                                      \
        composite_route route;                                                                                         \
        bool found;                                                                                                    \
        pthread_mutex_lock(&c->mutex);                                                                                 \
        found = route_snapshot_locked(c, h, &route);                                                                   \
        pthread_mutex_unlock(&c->mutex);                                                                               \
        if (!found || route.kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM || route.terminal_seen)                        \
            return -ESTALE;                                                                                            \
        return trevrpc_rpc_transport_stream_abort_##name(route.source, route.local, code);                             \
    }
STREAM_ABORT_ROUTED(receive)
STREAM_ABORT_ROUTED(send)
static int composite_stream_abort(trevrpc_rpc_transport* t, trevrpc_rpc_transport_handle h, uint64_t code) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(t);
    composite_route route;
    bool found;
    pthread_mutex_lock(&c->mutex);
    found = route_snapshot_locked(c, h, &route);
    pthread_mutex_unlock(&c->mutex);
    if (!found || route.kind != TREVRPC_RPC_TRANSPORT_OBJECT_STREAM || route.terminal_seen)
        return -ESTALE;
    return trevrpc_rpc_transport_stream_abort(route.source, route.local, code);
}
static int composite_release_handle(trevrpc_rpc_transport* t, trevrpc_rpc_transport_handle handle, uint32_t kind) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(t);
    composite_entry* entry;
    trevrpc_rpc_transport* source;
    trevrpc_rpc_transport_handle local;
    int result;
    pthread_mutex_lock(&c->mutex);
    entry = find_external_locked(c, handle);
    if (entry == NULL) {
        pthread_mutex_unlock(&c->mutex);
        return 0;
    }
    if (entry->kind != kind) {
        pthread_mutex_unlock(&c->mutex);
        return -ESTALE;
    }
    if (entry->semantic_released) {
        pthread_mutex_unlock(&c->mutex);
        return 0;
    }
    while (entry->release_refs != 0) {
        pthread_cond_wait(&c->condition, &c->mutex);
        entry = find_external_locked(c, handle);
        if (entry == NULL || entry->semantic_released) {
            pthread_mutex_unlock(&c->mutex);
            return 0;
        }
        if (entry->kind != kind) {
            pthread_mutex_unlock(&c->mutex);
            return -ESTALE;
        }
    }
    /* Block new receives before waiting for already-admitted receives.  A
     * receive wrapper owns the child source handle until it is released. */
    ++entry->release_refs;
    while (entry->receive_refs != 0) {
        pthread_cond_wait(&c->condition, &c->mutex);
        entry = find_external_locked(c, handle);
        if (entry == NULL) {
            pthread_mutex_unlock(&c->mutex);
            return 0;
        }
    }
    source = entry->source;
    local = entry->local;
    pthread_mutex_unlock(&c->mutex);

    result = trevrpc_rpc_transport_release_handle(source, local, kind);

    pthread_mutex_lock(&c->mutex);
    entry = find_external_locked(c, handle);
    if (entry != NULL && entry->kind == kind && entry->release_refs != 0) {
        --entry->release_refs;
        if (result == 0 || result == -ESTALE)
            entry->semantic_released = true;
        pthread_cond_broadcast(&c->condition);
        composite_try_retire_locked(c, handle.slot);
    }
    pthread_mutex_unlock(&c->mutex);
    return result == -ESTALE ? 0 : result;
}

static int composite_close(trevrpc_rpc_transport* t) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(t);
    int a;
    int b;
    pthread_mutex_lock(&c->mutex);
    c->closing = true;
    pthread_cond_broadcast(&c->condition);
    pthread_mutex_unlock(&c->mutex);
    a = trevrpc_rpc_transport_close(c->native_transport);
    b = trevrpc_rpc_transport_close(c->h3_transport);
    return a ? a : b;
}
static int composite_drain(trevrpc_rpc_transport* t) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(t);
    bool listen_in_progress;
    int a = trevrpc_rpc_transport_drain(c->native_transport);
    int b = trevrpc_rpc_transport_drain(c->h3_transport);
    pthread_mutex_lock(&c->mutex);
    listen_in_progress = c->shared_listens_in_progress != 0;
    pthread_mutex_unlock(&c->mutex);
    return a && a != -EAGAIN
               ? a
               : (b && b != -EAGAIN ? b : (listen_in_progress || a == -EAGAIN || b == -EAGAIN ? -EAGAIN : 0));
}
static void composite_destroy(trevrpc_rpc_transport* t) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(t);
    size_t index;
    pthread_mutex_lock(&c->mutex);
    c->closing = true;
    while (c->shared_listens_in_progress != 0)
        pthread_cond_wait(&c->condition, &c->mutex);
    pthread_mutex_unlock(&c->mutex);
    for (index = 0; index < sizeof(c->pending_events) / sizeof(c->pending_events[0]); ++index) {
        if (c->pending_events[index] != NULL) {
            trevrpc_rpc_transport_event_release(
                index == 0 ? c->native_transport : c->h3_transport, c->pending_events[index]);
            c->pending_events[index] = NULL;
        }
    }
    /* Shared H3 listener callbacks retain the composite dispatch context and may
     * adopt into the native transport. Destroy H3 first so its listener callback
     * barrier has completed before the native child source or composite state is
     * reclaimed. */
    trevrpc_rpc_transport_destroy(c->h3_transport);
    trevrpc_rpc_transport_destroy(c->native_transport);
    close(c->capacity_wake_read_fd);
    close(c->capacity_wake_write_fd);
    pthread_cond_destroy(&c->condition);
    pthread_mutex_destroy(&c->mutex);
    free(c->local_hash);
    free(c->entries);
    free(c);
}

static const trevrpc_rpc_transport_ops composite_ops = {
    .get_wake_source = composite_get_wake_source,
    .next_event = composite_next_event,
    .event_get_info = composite_event_get_info,
    .event_release = composite_event_release,
    .receive_get_info = composite_receive_get_info,
    .receive_release = composite_receive_release,
    .get_diagnostics = composite_get_diagnostics,
    .poll_timeout_ms = composite_poll_timeout_ms,
    .endpoint_listen = composite_endpoint_listen,
    .endpoint_get_port = composite_endpoint_get_port,
    .endpoint_dial = composite_endpoint_dial,
    .dial_cancel = composite_dial_cancel,
    .stream_open = composite_stream_open,
    .stream_send = composite_stream_send,
    .stream_receive = composite_stream_receive,
    .stream_finish_send = composite_stream_finish_send,
    .stream_abort_receive = composite_stream_abort_receive,
    .stream_abort_send = composite_stream_abort_send,
    .stream_abort = composite_stream_abort,
    .stream_close = composite_stream_close,
    .connection_close = composite_connection_close,
    .listener_close = composite_listener_close,
    .close = composite_close,
    .drain = composite_drain,
    .destroy = composite_destroy,
    .get_wake_sources = composite_get_wake_sources,
    .release_handle = composite_release_handle,
    .event_get_admission_info = composite_event_get_admission_info,
    .event_get_protocol_info = composite_event_get_protocol_info,
    .admission_respond = composite_admission_respond,
};

int trevrpc_rpc_transport_msquic_adopt(trevrpc_rpc_transport* native_transport,
    trevrpc_rpc_transport* h3_transport,
    const trevrpc_rpc_transport_config* config,
    trevrpc_rpc_transport** out_transport) {
    trevrpc_rpc_transport_msquic* c;
    uint64_t total_capacity;
    size_t per_source_capacity;
    size_t hash_capacity = 1;
    size_t index;
    int result;
    int capacity_wakes[2] = {-1, -1};
    if (native_transport == NULL || h3_transport == NULL || config == NULL || out_transport == NULL)
        return -EINVAL;
    total_capacity = (uint64_t)config->listener_capacity + config->connection_capacity + config->stream_capacity;
    if (total_capacity == 0 || total_capacity > UINT32_MAX / 2u || total_capacity > SIZE_MAX / 4u)
        return -EOVERFLOW;
    per_source_capacity = (size_t)total_capacity;
    c = calloc(1, sizeof(*c));
    if (c == NULL)
        return -ENOMEM;
    c->entry_capacity = per_source_capacity * 2u;
    while (hash_capacity < c->entry_capacity * 2u) {
        if (hash_capacity > SIZE_MAX / 2u) {
            free(c);
            return -EOVERFLOW;
        }
        hash_capacity *= 2u;
    }
    c->local_hash_capacity = hash_capacity;
    c->entries = calloc(c->entry_capacity, sizeof(*c->entries));
    c->local_hash = calloc(c->local_hash_capacity, sizeof(*c->local_hash));
    if (c->entries == NULL || c->local_hash == NULL) {
        free(c->local_hash);
        free(c->entries);
        free(c);
        return -ENOMEM;
    }
    for (index = 0; index < c->entry_capacity; ++index) {
        c->entries[index].generation = 1;
        c->entries[index].next_free = index + 1u < c->entry_capacity ? (uint32_t)index + 2u : 0;
    }
    c->free_head = 1;
    c->next_sequence = 1;
    if (pthread_mutex_init(&c->mutex, NULL) != 0) {
        free(c->local_hash);
        free(c->entries);
        free(c);
        return -ENOMEM;
    }
    if (pthread_cond_init(&c->condition, NULL) != 0) {
        pthread_mutex_destroy(&c->mutex);
        free(c->local_hash);
        free(c->entries);
        free(c);
        return -ENOMEM;
    }
    result = composite_make_pipe(capacity_wakes);
    if (result != 0) {
        pthread_cond_destroy(&c->condition);
        pthread_mutex_destroy(&c->mutex);
        free(c->local_hash);
        free(c->entries);
        free(c);
        return result;
    }
    c->capacity_wake_read_fd = capacity_wakes[0];
    c->capacity_wake_write_fd = capacity_wakes[1];
    do {
        c->owner = atomic_fetch_add_explicit(&composite_owner_sequence, 1, memory_order_relaxed);
    } while (c->owner == 0);
    c->native_transport = native_transport;
    c->h3_transport = h3_transport;
    c->base.ops = &composite_ops;
    *out_transport = &c->base;
    return 0;
}

#ifdef TREVRPC_RPC_TRANSPORT_H3_TESTING
int trevrpc_rpc_transport_msquic_test_event_refs(
    trevrpc_rpc_transport* transport, trevrpc_rpc_transport_handle handle, uint32_t* out_refs) {
    trevrpc_rpc_transport_msquic* c = composite_from_base(transport);
    composite_entry* entry;
    if (out_refs == NULL)
        return -EINVAL;
    pthread_mutex_lock(&c->mutex);
    entry = find_external_locked(c, handle);
    if (entry == NULL) {
        pthread_mutex_unlock(&c->mutex);
        return -ESTALE;
    }
    *out_refs = entry->event_refs;
    pthread_mutex_unlock(&c->mutex);
    return 0;
}
#endif
