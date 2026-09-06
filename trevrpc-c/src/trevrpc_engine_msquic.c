#if defined(__linux__) && !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif
#if defined(__APPLE__) && !defined(_DARWIN_C_SOURCE)
#define _DARWIN_C_SOURCE
#endif
#define _POSIX_C_SOURCE 200809L
#define QUIC_API_ENABLE_VERSIONED_FEATURES 1

#include "trevrpc_engine_msquic.h"
#include "trevrpc_engine_msquic_internal.h"
#include "trevrpc_engine_internal.h"
#include "trevrpc_frame_internal.h"
#include "trevrpc_msquic_api_owner.h"

#include <arpa/inet.h>
#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#define ADAPTER_ENDPOINT_FLAGS                                                                                         \
    (TREVRPC_ENGINE_ENDPOINT_SKIP_CERTIFICATE_VALIDATION | TREVRPC_ENGINE_ENDPOINT_DISABLE_SEND_BUFFERING)

typedef struct msquic_provider msquic_provider;
typedef struct msquic_event msquic_event;
typedef struct msquic_receive msquic_receive;
typedef struct adapter_object adapter_object;
typedef struct adapter_endpoint adapter_endpoint;
typedef struct adapter_stream adapter_stream;
typedef struct adapter_connection adapter_connection;
typedef struct adapter_listener adapter_listener;
typedef struct adapter_send adapter_send;
typedef struct adapter_receive_node adapter_receive_node;
typedef struct adapter_pending_connection adapter_pending_connection;
typedef struct adapter_pending_stream adapter_pending_stream;

typedef struct adapter_slot {
    adapter_object* object;
    uint32_t generation;
    uint32_t kind;
    bool retired;
} adapter_slot;

struct msquic_event {
    trevrpc_engine_event_spec spec;
    msquic_provider* adapter;
    trevrpc_engine_reservation* reservation;
    uint64_t readable_epoch;
    adapter_object* reclaim_object;
};

struct msquic_receive {
    size_t len;
    uint8_t data[];
};

struct adapter_endpoint {
    atomic_uint refs;
    const QUIC_API_TABLE* api;
    HQUIC registration;
    HQUIC configuration;
    char* host;
    uint8_t* alpn;
    char* cert_file;
    char* key_file;
    char* ca_cert_file;
    uint32_t host_len;
    uint32_t alpn_len;
    uint16_t port;
    uint16_t peer_bidi_stream_count;
    uint32_t flags;
    uint32_t max_pending_send_count;
    uint64_t max_pending_send_bytes;
    uint64_t max_frame_size;
    uint64_t max_idle_timeout_ms;
    uint32_t keep_alive_ms;
    uint32_t stream_recv_window;
    uint32_t conn_flow_control_window;
    void* endpoint_lease;
    trevrpc_msquic_endpoint_lease_release endpoint_lease_release;
    bool owns_msquic_endpoint;
};

struct adapter_object {
    msquic_provider* adapter;
    adapter_endpoint* endpoint;
    adapter_object* retired_next;
    bool on_retired_list;
    HQUIC handle;
    trevrpc_engine_handle_v1 token;
    uint32_t kind;
    bool ready;
    atomic_bool closing;
    bool cancel_requested;
    bool shutdown_complete;
    bool terminal_published;
    bool live_counted;
    bool terminal_deferred;
    bool terminal_failed;
    int terminal_deferred_status;
    uint32_t active_operations;
    bool operation_pending;
    uint32_t event_flags;
    uint64_t operation_id;
    bool owns_endpoint;
    trevrpc_engine_reservation* ready_reservation;
    trevrpc_engine_reservation* terminal_reservation;
};

struct adapter_listener {
    adapter_object base;
};

struct adapter_connection {
    adapter_object base;
    trevrpc_engine_handle_v1 parent;
    uint64_t application_error;
    uint64_t transport_error;
    size_t live_streams;
    adapter_pending_connection* pending_accept_node;
    adapter_pending_stream* pending_peer_stream_head;
    adapter_pending_stream* pending_peer_stream_tail;
    uint64_t pending_peer_stream_count;
    uint64_t pending_send_count;
    uint64_t pending_send_bytes;
    uint64_t max_pending_send_count;
    uint64_t max_pending_send_bytes;
    bool on_accept_queue;
    bool accept_queue_ref_held;
    bool handshake_in_flight;
    bool admission_granted;
    bool construction_ref_held;
};

struct adapter_receive_node {
    adapter_receive_node* next;
    msquic_receive* receive;
};

/* Accept queues own one provider lifetime pin until their node is unlinked. */
struct adapter_pending_connection {
    adapter_pending_connection* next;
    adapter_connection* connection;
    bool release_queue_ref;
};

struct adapter_pending_stream {
    adapter_pending_stream* next;
    adapter_stream* stream;
    bool release_queue_ref;
};

struct adapter_stream {
    adapter_object base;
    trevrpc_engine_handle_v1 parent;
    adapter_pending_stream* pending_peer_node;
    bool on_peer_queue;
    bool peer_queue_ref_held;
    bool stream_admission_granted;
    bool construction_ref_held;
    pthread_mutex_t mutex;
    pthread_mutex_t send_gate;
    trevrpc_frame_parser parser;
    adapter_receive_node* receive_head;
    adapter_receive_node* receive_tail;
    adapter_send* pending_sends;
    adapter_send* pending_send_tail;
    adapter_stream* send_scheduler_next;
    bool send_scheduler_queued;
    atomic_uint_fast64_t* pending_operation_ids;
    msquic_receive* parser_receive_allocation;
    bool readable_pending;
    bool readable_retry_pending;
    uint64_t readable_epoch;
    uint64_t readable_published_epoch;
    bool ready_event_committed;
    bool receive_fin_published;
    bool receive_paused;
    bool receive_aborted;
    bool send_finished;
    bool send_aborted;
    int receive_alloc_error;
    uint64_t application_error;
    uint64_t pending_send_bytes;
    uint32_t pending_send_count;
    trevrpc_engine_reservation* receive_fin_reservation;
};

struct adapter_send {
    adapter_send* next;
    adapter_stream* stream;
    uint64_t operation_id;
    size_t len;
    bool submitted;
    atomic_bool completed;
    trevrpc_engine_reservation* completion_reservation;
    QUIC_BUFFER buffer;
    uint8_t* data;
};

struct msquic_provider {
    pthread_mutex_t mutex;
    pthread_mutex_t budget_mutex;
    trevrpc_engine* engine;
    const QUIC_API_TABLE* api;
    uint32_t state;
    int terminal_status;
    uint64_t owner;
    adapter_slot* slots;
    uint32_t slot_count;
    uint32_t listener_begin;
    uint32_t connection_begin;
    uint32_t stream_begin;
    uint32_t max_receive_owned_count;
    uint64_t max_receive_owned_bytes;
    uint64_t connection_capacity;
    uint64_t stream_capacity;
    uint64_t connection_budget_used;
    uint64_t stream_budget_used;
    uint64_t pending_connection_count;
    uint64_t pending_stream_count;
    uint64_t connection_handshakes_in_flight;
    adapter_pending_connection* pending_connection_head;
    adapter_pending_connection* pending_connection_tail;
    bool accept_admission_open;
    pthread_cond_t scheduler_condition;
    pthread_t scheduler_thread;
    bool scheduler_condition_initialized;
    bool scheduler_thread_started;
    bool scheduler_requested;
    bool scheduler_stop;
    uint64_t receive_owned_count;
    uint64_t peak_receive_owned_count;
    uint64_t receive_owned_bytes;
    uint64_t peak_receive_owned_bytes;
    uint64_t pending_send_bytes;
    uint64_t pending_send_count;
    uint64_t max_pending_send_count;
    uint64_t max_pending_send_bytes;
    atomic_uint_fast64_t readable_retry_count;
    adapter_stream* send_ready_head;
    adapter_stream* send_ready_tail;
    uint64_t live_listeners;
    uint64_t live_connections;
    uint64_t live_streams;
    uint64_t native_closes_in_flight;
    bool close_initiated;
    bool stopped_reported;
    adapter_object* retired_objects;
};

static pthread_mutex_t AdapterOwnerMutex = PTHREAD_MUTEX_INITIALIZER;
static uint64_t AdapterNextOwner = 1;

static QUIC_STATUS QUIC_API adapter_listener_callback(HQUIC handle, void* context, QUIC_LISTENER_EVENT* event);
static QUIC_STATUS QUIC_API adapter_connection_callback(HQUIC handle, void* context, QUIC_CONNECTION_EVENT* event);
static QUIC_STATUS QUIC_API adapter_stream_callback(HQUIC handle, void* context, QUIC_STREAM_EVENT* event);
static void publish_listener_terminal(adapter_listener* listener);
static void publish_connection_terminal(adapter_connection* connection);
static void publish_stream_terminal(adapter_stream* stream, bool failed, int status);
static void promote_peer_streams(adapter_connection* connection);
static void adapter_schedule(msquic_provider* adapter);
static void* adapter_scheduler_main(void* context);
static void adapter_scheduler_drain(msquic_provider* adapter);
static bool reserve_connection_budget(msquic_provider* adapter);
static void release_connection_admission(msquic_provider* adapter);
static void release_connection_admission_if_granted(adapter_connection* connection);
static void release_peer_stream_pending(msquic_provider* adapter, adapter_connection* connection);
static void release_stream_budget(msquic_provider* adapter);
static adapter_pending_connection* remove_connection_queue_locked(
    msquic_provider* adapter, adapter_connection* connection, bool* release_queue_ref);
static adapter_pending_stream* remove_peer_stream_queue_locked(
    msquic_provider* adapter, adapter_connection* connection, adapter_stream* stream, bool* release_queue_ref);
static adapter_pending_connection* detach_pending_connections_locked(
    msquic_provider* adapter, trevrpc_engine_handle_v1 parent, bool filter_parent);
static adapter_pending_stream* detach_pending_streams_locked(msquic_provider* adapter, adapter_connection* connection);
static void drain_detached_connection(
    msquic_provider* adapter, adapter_connection* connection, bool release_queue_ref, uint64_t application_error_code);
static void drain_detached_connections(msquic_provider* adapter, adapter_pending_connection* nodes);
static void discard_detached_connections(msquic_provider* adapter, adapter_pending_connection* nodes);
static void adapter_object_pin(adapter_object* object);
static void adapter_object_unpin(adapter_object* object);
static void adapter_fail_stop(msquic_provider* adapter, int status, const char* message);
static msquic_event* event_new(uint32_t kind,
    uint32_t flags,
    int status,
    uint32_t subject_kind,
    trevrpc_engine_handle_v1 subject,
    trevrpc_engine_handle_v1 parent,
    uint64_t operation_id,
    uint64_t application_error,
    uint64_t transport_error,
    const void* data,
    size_t data_len);
static void* adapter_receive_alloc(size_t size, void* context);
static void adapter_receive_free(void* ptr, void* context);
static bool release_receive_credit(adapter_stream* stream, size_t len);
static int reserve_send_admission(adapter_stream* stream, uint64_t operation_id, size_t len);
static void release_send_admission(adapter_stream* stream, uint64_t operation_id, size_t len);
static void cancel_unsent_sends(adapter_stream* stream, int status);
static bool stream_shutdown_drained(adapter_stream* stream);
static adapter_object* registry_find_retired_locked(
    msquic_provider* adapter, trevrpc_engine_handle_v1 handle, uint32_t expected_kind);
static adapter_object* object_reclaim_locked(msquic_provider* adapter, adapter_object* object);
static void adapter_object_free(adapter_object* object);
static uint64_t limit_product(uint64_t left, uint64_t right);

static int provider_attach(void* provider_context, trevrpc_engine* engine) {
    msquic_provider* adapter = provider_context;
    adapter->engine = engine;
    return 0;
}

static void adapter_schedule(msquic_provider* adapter) {
    if (adapter == NULL || !adapter->scheduler_condition_initialized) {
        return;
    }
    pthread_mutex_lock(&adapter->mutex);
    adapter->scheduler_requested = true;
    if (adapter->scheduler_thread_started) {
        pthread_cond_signal(&adapter->scheduler_condition);
    }
    pthread_mutex_unlock(&adapter->mutex);
}

static void* adapter_scheduler_main(void* context) {
    msquic_provider* adapter = context;
    for (;;) {
        pthread_mutex_lock(&adapter->mutex);
        while (!adapter->scheduler_requested && !adapter->scheduler_stop) {
            pthread_cond_wait(&adapter->scheduler_condition, &adapter->mutex);
        }
        bool stop = adapter->scheduler_stop;
        adapter->scheduler_requested = false;
        pthread_mutex_unlock(&adapter->mutex);
        if (stop) {
            return NULL;
        }
        adapter_scheduler_drain(adapter);
    }
}

static int init_versioned(void* storage, size_t supplied, size_t required) {
    if (storage == NULL || supplied < required) {
        return -EINVAL;
    }
    if (supplied > UINT32_MAX) {
        return -EOVERFLOW;
    }
    memset(storage, 0, supplied);
    uint32_t* prefix = storage;
    prefix[0] = (uint32_t)supplied;
    prefix[1] = TREVRPC_ENGINE_MSQUIC_STRUCT_VERSION_1;
    return 0;
}

uint32_t trevrpc_engine_msquic_abi_version(void) {
    return TREVRPC_ENGINE_MSQUIC_ABI_VERSION;
}

void trevrpc_engine_msquic_abi_1_anchor(void) {
}

int trevrpc_engine_msquic_config_v1_init(trevrpc_engine_msquic_config_v1* config, size_t size) {
    return init_versioned(config, size, sizeof(*config));
}

static bool all_zero(const uint64_t* values, size_t count) {
    for (size_t index = 0; index < count; index++) {
        if (values[index] != 0) {
            return false;
        }
    }
    return true;
}

static int validate_provider_config(const trevrpc_engine_msquic_config_v1* config) {
    if (config == NULL || config->struct_size < sizeof(*config)) {
        return -EINVAL;
    }
    if (config->struct_version != TREVRPC_ENGINE_MSQUIC_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    return config->flags != 0 || config->reserved0 != 0 || !all_zero(config->reserved, 6) ? -EINVAL : 0;
}

static msquic_event* event_new(uint32_t kind,
    uint32_t flags,
    int status,
    uint32_t subject_kind,
    trevrpc_engine_handle_v1 subject,
    trevrpc_engine_handle_v1 parent,
    uint64_t operation_id,
    uint64_t application_error,
    uint64_t provider_error,
    const void* data,
    size_t data_len) {
    msquic_event* event = calloc(1, sizeof(*event));
    if (event == NULL) {
        return NULL;
    }
    event->spec.kind = kind;
    event->spec.flags = flags;
    event->spec.status = status;
    event->spec.subject_kind = subject_kind;
    event->spec.subject = subject;
    event->spec.parent = parent;
    event->spec.operation_id = operation_id;
    event->spec.application_error_code = application_error;
    event->spec.provider_error_code = provider_error;
    event->spec.data = data;
    event->spec.data_len = data_len;
    return event;
}

static void event_detach(void* provider_context, void* hook_context);
static void event_drop(void* provider_context, void* hook_context);
static void object_event_detach(void* provider_context, void* hook_context);

static int publish_reserved_event(msquic_provider* adapter,
    trevrpc_engine_reservation* reservation,
    uint32_t kind,
    uint32_t flags,
    int status,
    uint32_t subject_kind,
    trevrpc_engine_handle_v1 subject,
    trevrpc_engine_handle_v1 parent,
    uint64_t operation_id,
    uint64_t application_error,
    uint64_t provider_error,
    adapter_object* reclaim_object) {
    trevrpc_engine_event_spec spec;
    memset(&spec, 0, sizeof(spec));
    spec.kind = kind;
    spec.flags = flags;
    spec.status = status;
    spec.subject_kind = subject_kind;
    spec.subject = subject;
    spec.parent = parent;
    spec.operation_id = operation_id;
    spec.application_error_code = application_error;
    spec.provider_error_code = provider_error;
    if (reclaim_object != NULL) {
        spec.dequeue_hook = object_event_detach;
        spec.drop_hook = object_event_detach;
        spec.hook_context = reclaim_object;
    }
    return trevrpc_engine_provider_publish_reserved(adapter->engine, reservation, &spec);
}

static int enqueue_event(msquic_provider* adapter, msquic_event* event, bool mandatory) {
    (void)mandatory;
    if (event == NULL) {
        return -ENOMEM;
    }
    event->adapter = adapter;
    event->spec.dequeue_hook = event_detach;
    event->spec.drop_hook = event_drop;
    event->spec.hook_context = event;
    int result;
    if (event->reservation != NULL) {
        trevrpc_engine_reservation* reservation = event->reservation;
        event->reservation = NULL;
        result = trevrpc_engine_provider_publish_reserved(adapter->engine, reservation, &event->spec);
    } else {
        result = trevrpc_engine_provider_publish_event(adapter->engine, &event->spec);
    }
    return result;
}

static char* copy_text(const char* source, uint32_t len) {
    if (source == NULL && len == 0) {
        return NULL;
    }
    if (source == NULL || memchr(source, '\0', len) != NULL) {
        return NULL;
    }
    char* copy = malloc((size_t)len + 1);
    if (copy != NULL) {
        memcpy(copy, source, len);
        copy[len] = '\0';
    }
    return copy;
}

static uint8_t* copy_bytes(const uint8_t* source, uint32_t len) {
    if (source == NULL || len == 0) {
        return NULL;
    }
    uint8_t* copy = malloc(len);
    if (copy != NULL) {
        memcpy(copy, source, len);
    }
    return copy;
}

static void endpoint_retain(adapter_endpoint* endpoint) {
    if (endpoint != NULL) {
        (void)atomic_fetch_add_explicit(&endpoint->refs, 1, memory_order_relaxed);
    }
}

static void endpoint_free(adapter_endpoint* endpoint) {
    if (endpoint == NULL || atomic_fetch_sub_explicit(&endpoint->refs, 1, memory_order_acq_rel) != 1) {
        return;
    }
    if (endpoint->owns_msquic_endpoint) {
        if (endpoint->configuration != NULL)
            endpoint->api->ConfigurationClose(endpoint->configuration);
        if (endpoint->registration != NULL)
            endpoint->api->RegistrationClose(endpoint->registration);
    }
    if (endpoint->endpoint_lease_release != NULL)
        endpoint->endpoint_lease_release(endpoint->endpoint_lease);
    free(endpoint->host);
    free(endpoint->alpn);
    free(endpoint->cert_file);
    free(endpoint->key_file);
    free(endpoint->ca_cert_file);
    free(endpoint);
}

static int validate_endpoint_config(
    const trevrpc_engine_endpoint_config_v1* config, bool server, uint64_t receive_limit) {
    if (config == NULL || config->struct_size < sizeof(*config)) {
        return -EINVAL;
    }
    if (config->struct_version != TREVRPC_ENGINE_STRUCT_VERSION_1) {
        return -ENOTSUP;
    }
    if (config->host == NULL || config->host_len == 0 || memchr(config->host, '\0', config->host_len) != NULL ||
        config->alpn == NULL || config->alpn_len == 0 || config->alpn_len > UINT8_MAX ||
        config->flags & ~ADAPTER_ENDPOINT_FLAGS || config->reserved0 != 0 || config->reserved1 != 0 ||
        config->reserved2 != 0 || !all_zero(config->reserved, 4) || config->max_pending_send_count == 0 ||
        config->max_pending_send_bytes == 0 || config->max_frame_size == 0 || config->max_frame_size > receive_limit) {
        return -EINVAL;
    }
    if (server) {
        if (config->cert_file == NULL || config->cert_file_len == 0 || config->key_file == NULL ||
            config->key_file_len == 0 || (config->flags & TREVRPC_ENGINE_ENDPOINT_SKIP_CERTIFICATE_VALIDATION) != 0) {
            return -EINVAL;
        }
    } else if (config->port == 0 || config->cert_file != NULL || config->cert_file_len != 0 ||
               config->key_file != NULL || config->key_file_len != 0) {
        return -EINVAL;
    }
    if ((config->cert_file == NULL) != (config->cert_file_len == 0) ||
        (config->key_file == NULL) != (config->key_file_len == 0) ||
        (config->ca_cert_file == NULL) != (config->ca_cert_file_len == 0)) {
        return -EINVAL;
    }
    if ((config->cert_file_len > 0 && memchr(config->cert_file, '\0', config->cert_file_len) != NULL) ||
        (config->key_file_len > 0 && memchr(config->key_file, '\0', config->key_file_len) != NULL) ||
        (config->ca_cert_file_len > 0 && memchr(config->ca_cert_file, '\0', config->ca_cert_file_len) != NULL)) {
        return -EINVAL;
    }
    return 0;
}

static int endpoint_create(msquic_provider* adapter,
    const trevrpc_engine_endpoint_config_v1* config,
    bool server,
    uint64_t receive_limit,
    adapter_endpoint** out_endpoint) {
    int result = validate_endpoint_config(config, server, receive_limit);
    if (result != 0) {
        return result;
    }
    adapter_endpoint* endpoint = calloc(1, sizeof(*endpoint));
    if (endpoint == NULL) {
        return -ENOMEM;
    }
    atomic_init(&endpoint->refs, 1);
    endpoint->api = adapter->api;
    endpoint->owns_msquic_endpoint = true;
    endpoint->host = copy_text(config->host, config->host_len);
    endpoint->alpn = copy_bytes(config->alpn, config->alpn_len);
    endpoint->cert_file = copy_text(config->cert_file, config->cert_file_len);
    endpoint->key_file = copy_text(config->key_file, config->key_file_len);
    endpoint->ca_cert_file = copy_text(config->ca_cert_file, config->ca_cert_file_len);
    if (endpoint->host == NULL || endpoint->alpn == NULL ||
        (config->cert_file_len > 0 && endpoint->cert_file == NULL) ||
        (config->key_file_len > 0 && endpoint->key_file == NULL) ||
        (config->ca_cert_file_len > 0 && endpoint->ca_cert_file == NULL)) {
        endpoint_free(endpoint);
        return -ENOMEM;
    }
    endpoint->host_len = config->host_len;
    endpoint->alpn_len = config->alpn_len;
    endpoint->port = config->port;
    endpoint->peer_bidi_stream_count = config->peer_bidi_stream_count;
    endpoint->flags = config->flags;
    endpoint->max_pending_send_count = config->max_pending_send_count;
    endpoint->max_pending_send_bytes = config->max_pending_send_bytes;
    endpoint->max_frame_size = config->max_frame_size;
    endpoint->max_idle_timeout_ms = config->max_idle_timeout_ms;
    endpoint->keep_alive_ms = config->keep_alive_ms;
    endpoint->stream_recv_window = config->stream_recv_window;
    endpoint->conn_flow_control_window = config->conn_flow_control_window;

    QUIC_REGISTRATION_CONFIG registration_config = {
        .AppName = server ? "trevrpc-engine-msquic-server" : "trevrpc-engine-msquic-client",
        .ExecutionProfile = QUIC_EXECUTION_PROFILE_LOW_LATENCY,
    };
    QUIC_STATUS status = adapter->api->RegistrationOpen(&registration_config, &endpoint->registration);
    if (QUIC_FAILED(status)) {
        endpoint_free(endpoint);
        return -EIO;
    }
    QUIC_BUFFER alpn = {.Length = endpoint->alpn_len, .Buffer = endpoint->alpn};
    QUIC_SETTINGS settings = {0};
    if (endpoint->max_idle_timeout_ms > 0) {
        settings.IsSet.IdleTimeoutMs = TRUE;
        settings.IdleTimeoutMs = endpoint->max_idle_timeout_ms;
    }
    if (endpoint->keep_alive_ms > 0) {
        settings.IsSet.KeepAliveIntervalMs = TRUE;
        settings.KeepAliveIntervalMs = endpoint->keep_alive_ms;
    }
    if (endpoint->peer_bidi_stream_count > 0) {
        settings.IsSet.PeerBidiStreamCount = TRUE;
        settings.PeerBidiStreamCount = endpoint->peer_bidi_stream_count;
    }
    if (endpoint->stream_recv_window > 0) {
        settings.IsSet.StreamRecvWindowDefault = TRUE;
        settings.StreamRecvWindowDefault = endpoint->stream_recv_window;
    }
    if (endpoint->conn_flow_control_window > 0) {
        settings.IsSet.ConnFlowControlWindow = TRUE;
        settings.ConnFlowControlWindow = endpoint->conn_flow_control_window;
    }
    settings.IsSet.SendBufferingEnabled = TRUE;
    settings.SendBufferingEnabled = (endpoint->flags & TREVRPC_ENGINE_ENDPOINT_DISABLE_SEND_BUFFERING) == 0;
    settings.IsSet.DatagramReceiveEnabled = TRUE;
    settings.DatagramReceiveEnabled = FALSE;
    settings.IsSet.ReliableResetEnabled = TRUE;
    settings.ReliableResetEnabled = FALSE;
    status = adapter->api->ConfigurationOpen(
        endpoint->registration, &alpn, 1, &settings, sizeof(settings), NULL, &endpoint->configuration);
    if (QUIC_FAILED(status)) {
        endpoint_free(endpoint);
        return -EIO;
    }
    QUIC_CREDENTIAL_CONFIG credential = {0};
    QUIC_CERTIFICATE_FILE certificate = {0};
    if (server) {
        certificate.CertificateFile = endpoint->cert_file;
        certificate.PrivateKeyFile = endpoint->key_file;
        credential.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE;
        credential.CertificateFile = &certificate;
    } else {
        credential.Type = QUIC_CREDENTIAL_TYPE_NONE;
        credential.Flags = QUIC_CREDENTIAL_FLAG_CLIENT;
        if ((endpoint->flags & TREVRPC_ENGINE_ENDPOINT_SKIP_CERTIFICATE_VALIDATION) != 0) {
            credential.Flags |= QUIC_CREDENTIAL_FLAG_NO_CERTIFICATE_VALIDATION;
        }
        if (endpoint->ca_cert_file != NULL) {
            credential.Flags |= QUIC_CREDENTIAL_FLAG_SET_CA_CERTIFICATE_FILE;
            credential.CaCertificateFile = endpoint->ca_cert_file;
        }
    }
    status = adapter->api->ConfigurationLoadCredential(endpoint->configuration, &credential);
    if (QUIC_FAILED(status)) {
        endpoint_free(endpoint);
        return -EIO;
    }
    *out_endpoint = endpoint;
    return 0;
}

static int endpoint_create_borrowed(msquic_provider* adapter,
    const trevrpc_engine_endpoint_config_v1* config,
    trevrpc_msquic_accepted_connection* accepted,
    HQUIC registration,
    HQUIC configuration,
    adapter_endpoint** out_endpoint) {
    adapter_endpoint* endpoint;
    if (config == NULL || config->struct_size < sizeof(*config) ||
        config->struct_version != TREVRPC_ENGINE_STRUCT_VERSION_1 || config->alpn == NULL || config->alpn_len == 0 ||
        config->alpn_len > UINT8_MAX || config->max_pending_send_count == 0 || config->max_pending_send_bytes == 0 ||
        config->max_frame_size == 0 || config->max_frame_size > adapter->max_receive_owned_bytes || accepted == NULL ||
        registration == NULL || configuration == NULL || out_endpoint == NULL)
        return -EINVAL;
    endpoint = calloc(1, sizeof(*endpoint));
    if (endpoint == NULL)
        return -ENOMEM;
    atomic_init(&endpoint->refs, 1);
    endpoint->api = adapter->api;
    endpoint->registration = registration;
    endpoint->configuration = configuration;
    endpoint->alpn = copy_bytes(config->alpn, config->alpn_len);
    if (endpoint->alpn == NULL) {
        endpoint_free(endpoint);
        return -ENOMEM;
    }
    if (trevrpc_msquic_accepted_connection_take_endpoint_lease(
            accepted, &endpoint->endpoint_lease, &endpoint->endpoint_lease_release) != 0) {
        endpoint_free(endpoint);
        return -EINVAL;
    }
    endpoint->alpn_len = config->alpn_len;
    endpoint->peer_bidi_stream_count = config->peer_bidi_stream_count;
    endpoint->flags = config->flags;
    endpoint->max_pending_send_count = config->max_pending_send_count;
    endpoint->max_pending_send_bytes = config->max_pending_send_bytes;
    endpoint->max_frame_size = config->max_frame_size;
    endpoint->max_idle_timeout_ms = config->max_idle_timeout_ms;
    endpoint->keep_alive_ms = config->keep_alive_ms;
    endpoint->stream_recv_window = config->stream_recv_window;
    endpoint->conn_flow_control_window = config->conn_flow_control_window;
    *out_endpoint = endpoint;
    return 0;
}

static int registry_add(
    msquic_provider* adapter, adapter_object* object, uint32_t kind, trevrpc_engine_handle_v1* out_handle) {
    uint32_t begin = kind == TREVRPC_ENGINE_OBJECT_LISTENER     ? adapter->listener_begin
                     : kind == TREVRPC_ENGINE_OBJECT_CONNECTION ? adapter->connection_begin
                                                                : adapter->stream_begin;
    uint32_t end = kind == TREVRPC_ENGINE_OBJECT_LISTENER     ? adapter->connection_begin
                   : kind == TREVRPC_ENGINE_OBJECT_CONNECTION ? adapter->stream_begin
                                                              : adapter->slot_count;
    pthread_mutex_lock(&adapter->mutex);
    if (adapter->state != TREVRPC_ENGINE_STATE_RUNNING) {
        pthread_mutex_unlock(&adapter->mutex);
        return -EPIPE;
    }
    if (object->operation_id != 0) {
        for (uint32_t slot = begin; slot < end; slot++) {
            adapter_object* pending = adapter->slots[slot].object;
            if (pending == NULL || pending->operation_id != object->operation_id || !pending->operation_pending) {
                continue;
            }
            if (kind != TREVRPC_ENGINE_OBJECT_STREAM ||
                (((adapter_stream*)pending)->parent.owner == ((adapter_stream*)object)->parent.owner &&
                    ((adapter_stream*)pending)->parent.slot == ((adapter_stream*)object)->parent.slot &&
                    ((adapter_stream*)pending)->parent.generation == ((adapter_stream*)object)->parent.generation)) {
                pthread_mutex_unlock(&adapter->mutex);
                return -EALREADY;
            }
        }
    }
    for (uint32_t slot = begin; slot < end; slot++) {
        adapter_slot* entry = &adapter->slots[slot];
        if (entry->object == NULL && !entry->retired) {
            if (entry->generation == 0) {
                entry->generation = 1;
            }
            entry->object = object;
            entry->kind = kind;
            object->adapter = adapter;
            object->kind = kind;
            object->token.owner = adapter->owner;
            object->token.slot = slot;
            object->token.generation = entry->generation;
            object->live_counted = true;
            *out_handle = object->token;
            if (kind == TREVRPC_ENGINE_OBJECT_LISTENER) {
                adapter->live_listeners++;
            } else if (kind == TREVRPC_ENGINE_OBJECT_CONNECTION) {
                adapter->live_connections++;
            } else {
                adapter->live_streams++;
            }
            pthread_mutex_unlock(&adapter->mutex);
            return 0;
        }
    }
    pthread_mutex_unlock(&adapter->mutex);
    return -ENOSPC;
}

static adapter_object* registry_find_retired_locked(
    msquic_provider* adapter, trevrpc_engine_handle_v1 handle, uint32_t expected_kind) {
    for (adapter_object* object = adapter->retired_objects; object != NULL; object = object->retired_next) {
        if (object->kind == expected_kind && object->token.owner == handle.owner && object->token.slot == handle.slot &&
            object->token.generation == handle.generation) {
            return object;
        }
    }
    return NULL;
}

static bool object_publish_handle(adapter_object* object, HQUIC handle) {
    msquic_provider* adapter = object->adapter;
    pthread_mutex_lock(&adapter->mutex);
    object->handle = handle;
    bool closing = atomic_load_explicit(&object->closing, memory_order_acquire);
    pthread_mutex_unlock(&adapter->mutex);
    return closing;
}

static HQUIC object_take_handle_locked(adapter_object* object) {
    HQUIC handle = object->handle;
    object->handle = NULL;
    return handle;
}

typedef enum registry_state_policy {
    REGISTRY_REQUIRE_OPEN,
    REGISTRY_ALLOW_UNAVAILABLE,
} registry_state_policy;

static int registry_get_with_policy(msquic_provider* adapter,
    trevrpc_engine_handle_v1 handle,
    uint32_t expected_kind,
    adapter_object** out_object,
    bool require_ready,
    registry_state_policy state_policy) {
    if (handle.owner == 0 || handle.generation == 0) {
        return -EINVAL;
    }
    if (handle.owner != adapter->owner || handle.slot >= adapter->slot_count) {
        return -ESTALE;
    }
    pthread_mutex_lock(&adapter->mutex);
    adapter_slot* slot = &adapter->slots[handle.slot];
    if (slot->object == NULL || slot->generation != handle.generation) {
        pthread_mutex_unlock(&adapter->mutex);
        return -ESTALE;
    }
    if (slot->kind != expected_kind) {
        pthread_mutex_unlock(&adapter->mutex);
        return -EINVAL;
    }
    adapter_object* object = slot->object;
    bool unavailable = atomic_load_explicit(&object->closing, memory_order_acquire) || object->terminal_published;
    int result = state_policy == REGISTRY_REQUIRE_OPEN && unavailable          ? -EPIPE
                 : require_ready && (!object->ready || object->handle == NULL) ? -EAGAIN
                                                                               : 0;
    if (result == 0) {
        object->active_operations++;
        *out_object = object;
    }
    pthread_mutex_unlock(&adapter->mutex);
    return result;
}

static int registry_get(msquic_provider* adapter,
    trevrpc_engine_handle_v1 handle,
    uint32_t expected_kind,
    adapter_object** out_object,
    bool require_ready) {
    return registry_get_with_policy(adapter, handle, expected_kind, out_object, require_ready, REGISTRY_REQUIRE_OPEN);
}

typedef struct adapter_object_scope {
    adapter_object* object;
} adapter_object_scope;

static void adapter_object_scope_cleanup(adapter_object_scope* scope) {
    if (scope->object != NULL) {
        adapter_object_unpin(scope->object);
    }
}

#define ADAPTER_OBJECT_SCOPE(object_value)                                                                             \
    adapter_object_scope object_scope __attribute__((cleanup(adapter_object_scope_cleanup))) = {                       \
        .object = (object_value),                                                                                      \
    }

static void adapter_maybe_stopped_locked(msquic_provider* adapter) {
    if (adapter->state == TREVRPC_ENGINE_STATE_STOPPING && !adapter->stopped_reported && adapter->live_listeners == 0 &&
        adapter->live_connections == 0 && adapter->live_streams == 0 && adapter->native_closes_in_flight == 0) {
        adapter->stopped_reported = true;
        adapter->state = TREVRPC_ENGINE_STATE_STOPPED;
        trevrpc_engine_provider_stopped(adapter->engine, adapter->terminal_status, 0);
    }
}

static void adapter_maybe_stopped(msquic_provider* adapter) {
    pthread_mutex_lock(&adapter->mutex);
    adapter_maybe_stopped_locked(adapter);
    pthread_mutex_unlock(&adapter->mutex);
}

static void adapter_native_close_begin_locked(msquic_provider* adapter, HQUIC handle) {
    if (handle != NULL) {
        adapter->native_closes_in_flight++;
    }
}

static void adapter_native_close_complete(msquic_provider* adapter, HQUIC handle) {
    if (handle == NULL) {
        return;
    }
    pthread_mutex_lock(&adapter->mutex);
    if (adapter->native_closes_in_flight > 0) {
        adapter->native_closes_in_flight--;
    }
    adapter_maybe_stopped_locked(adapter);
    pthread_mutex_unlock(&adapter->mutex);
}

static void adapter_object_pin(adapter_object* object) {
    msquic_provider* adapter = object->adapter;
    pthread_mutex_lock(&adapter->mutex);
    object->active_operations++;
    pthread_mutex_unlock(&adapter->mutex);
}

static bool callback_enter(msquic_provider* adapter, adapter_object* object) {
    if (trevrpc_engine_provider_callback_enter(adapter->engine) != 0) {
        return false;
    }
    adapter_object_pin(object);
    return true;
}

static void callback_leave(msquic_provider* adapter, adapter_object* object) {
    adapter_object_unpin(object);
    trevrpc_engine_provider_callback_leave(adapter->engine);
}

static void object_release_live_count_locked(adapter_object* object) {
    msquic_provider* adapter = object->adapter;
    if (!object->live_counted) {
        return;
    }
    object->live_counted = false;
    if (object->kind == TREVRPC_ENGINE_OBJECT_LISTENER && adapter->live_listeners > 0) {
        adapter->live_listeners--;
    } else if (object->kind == TREVRPC_ENGINE_OBJECT_CONNECTION && adapter->live_connections > 0) {
        adapter->live_connections--;
    } else if (object->kind == TREVRPC_ENGINE_OBJECT_STREAM && adapter->live_streams > 0) {
        adapter->live_streams--;
    }
}

static bool object_terminal_locked(adapter_object* object) {
    if (object->terminal_published) {
        return false;
    }
    object->terminal_published = true;
    object->operation_pending = false;
    object_release_live_count_locked(object);
    return true;
}

static bool object_retains_tombstone_locked(adapter_object* object) {
    if (object->active_operations != 0) {
        return true;
    }
    if (object->kind == TREVRPC_ENGINE_OBJECT_CONNECTION) {
        adapter_connection* connection = (adapter_connection*)object;
        return connection->on_accept_queue || connection->pending_accept_node != NULL ||
               connection->pending_peer_stream_head != NULL || connection->pending_peer_stream_count != 0;
    }
    if (object->kind != TREVRPC_ENGINE_OBJECT_STREAM) {
        return false;
    }
    adapter_stream* stream = (adapter_stream*)object;
    if (stream->on_peer_queue || stream->pending_peer_node != NULL || stream->peer_queue_ref_held ||
        stream->send_scheduler_queued) {
        return true;
    }
    pthread_mutex_lock(&stream->mutex);
    bool retain = stream->receive_head != NULL || stream->readable_pending || stream->pending_send_count != 0;
    pthread_mutex_unlock(&stream->mutex);
    return retain;
}

static adapter_object* object_reclaim_locked(msquic_provider* adapter, adapter_object* object) {
    if (object == NULL || !object->terminal_published) {
        return NULL;
    }
    if (!object->on_retired_list) {
        if (object->token.slot >= adapter->slot_count) {
            return NULL;
        }
        adapter_slot* slot = &adapter->slots[object->token.slot];
        if (slot->object != object || slot->generation != object->token.generation) {
            return NULL;
        }
        slot->object = NULL;
        slot->kind = 0;
        slot->generation++;
        if (slot->generation == 0) {
            slot->retired = true;
        }
    }
    if (object_retains_tombstone_locked(object)) {
        if (!object->on_retired_list) {
            object->retired_next = adapter->retired_objects;
            object->on_retired_list = true;
            adapter->retired_objects = object;
        }
        return NULL;
    }
    if (object->on_retired_list) {
        adapter_object** link = &adapter->retired_objects;
        while (*link != NULL && *link != object) {
            link = &(*link)->retired_next;
        }
        if (*link != object) {
            return NULL;
        }
        *link = object->retired_next;
        object->retired_next = NULL;
        object->on_retired_list = false;
    }
    return object;
}

static void set_readable_retry_locked(adapter_stream* stream, bool pending) {
    if (stream->readable_retry_pending == pending) {
        return;
    }
    stream->readable_retry_pending = pending;
    if (pending) {
        (void)atomic_fetch_add_explicit(&stream->base.adapter->readable_retry_count, 1, memory_order_relaxed);
    } else {
        (void)atomic_fetch_sub_explicit(&stream->base.adapter->readable_retry_count, 1, memory_order_relaxed);
    }
}

static void event_finish(msquic_event* event, bool dequeued) {
    msquic_provider* adapter = event->adapter;
    adapter_object* readable_object = NULL;
    adapter_object* reclaimed = NULL;
    pthread_mutex_lock(&adapter->mutex);
    if (event->readable_epoch != 0 && event->spec.subject.slot < adapter->slot_count) {
        readable_object = adapter->slots[event->spec.subject.slot].object;
        if (readable_object == NULL || readable_object->token.generation != event->spec.subject.generation) {
            readable_object = registry_find_retired_locked(adapter, event->spec.subject, TREVRPC_ENGINE_OBJECT_STREAM);
        }
        if (readable_object != NULL && readable_object->kind == TREVRPC_ENGINE_OBJECT_STREAM) {
            adapter_stream* stream = (adapter_stream*)readable_object;
            pthread_mutex_lock(&stream->mutex);
            if (stream->readable_epoch == event->readable_epoch) {
                if (stream->readable_published_epoch == event->readable_epoch) {
                    stream->readable_published_epoch = 0;
                }
                if (dequeued) {
                    stream->readable_pending = false;
                    set_readable_retry_locked(stream, false);
                } else if (stream->receive_head != NULL && !stream->base.terminal_published) {
                    stream->readable_pending = true;
                    set_readable_retry_locked(stream, true);
                } else {
                    stream->readable_pending = false;
                    set_readable_retry_locked(stream, false);
                }
            }
            pthread_mutex_unlock(&stream->mutex);
        }
    }
    adapter_object* reclaim_candidate = event->reclaim_object;
    if (reclaim_candidate == NULL && readable_object != NULL && readable_object->on_retired_list) {
        reclaim_candidate = readable_object;
    }
    reclaimed = object_reclaim_locked(adapter, reclaim_candidate);
    pthread_mutex_unlock(&adapter->mutex);
    adapter_object_free(reclaimed);
    free(event);
    if (dequeued && atomic_load_explicit(&adapter->readable_retry_count, memory_order_relaxed) != 0) {
        adapter_schedule(adapter);
    }
}

static void event_detach(void* provider_context, void* hook_context) {
    (void)provider_context;
    event_finish(hook_context, true);
}

static void event_drop(void* provider_context, void* hook_context) {
    (void)provider_context;
    event_finish(hook_context, false);
}

static void object_event_detach(void* provider_context, void* hook_context) {
    msquic_provider* adapter = provider_context;
    adapter_object* object = hook_context;
    pthread_mutex_lock(&adapter->mutex);
    adapter_object* reclaimed = object_reclaim_locked(adapter, object);
    pthread_mutex_unlock(&adapter->mutex);
    adapter_object_free(reclaimed);
}

static void registry_remove_unstarted(adapter_object* object) {
    msquic_provider* adapter = object->adapter;
    adapter_object* reclaim = NULL;
    HQUIC closing_handle = NULL;
    pthread_mutex_lock(&adapter->mutex);
    adapter_slot* slot = &adapter->slots[object->token.slot];
    if (slot->object == object) {
        slot->object = NULL;
        slot->kind = 0;
        slot->generation++;
        if (slot->generation == 0) {
            slot->retired = true;
        }
        atomic_store_explicit(&object->closing, true, memory_order_release);
        object->terminal_published = true;
        object->operation_pending = false;
        if (object->active_operations == 0) {
            closing_handle = object->handle;
            adapter_native_close_begin_locked(adapter, closing_handle);
            object_release_live_count_locked(object);
            reclaim = object;
        } else {
            object->on_retired_list = true;
            object->retired_next = adapter->retired_objects;
            adapter->retired_objects = object;
        }
    }
    pthread_mutex_unlock(&adapter->mutex);
    if (reclaim != NULL) {
        adapter_object_free(reclaim);
        adapter_native_close_complete(adapter, closing_handle);
        adapter_maybe_stopped(adapter);
    }
}

static int host_address(const char* host, uint16_t port, QUIC_ADDR* address) {
    memset(address, 0, sizeof(*address));
    if (strchr(host, ':') != NULL) {
        address->Ipv6.sin6_family = QUIC_ADDRESS_FAMILY_INET6;
        address->Ipv6.sin6_port = htons(port);
        return inet_pton(AF_INET6, host, &address->Ipv6.sin6_addr) == 1 ? 0 : -EINVAL;
    }
    address->Ipv4.sin_family = QUIC_ADDRESS_FAMILY_INET;
    address->Ipv4.sin_port = htons(port);
    return inet_pton(AF_INET, host, &address->Ipv4.sin_addr) == 1 ? 0 : -EINVAL;
}

static int provider_listen(msquic_provider* adapter,
    const trevrpc_engine_endpoint_config_v1* config,
    trevrpc_engine_reservation* terminal_reservation,
    trevrpc_engine_handle_v1* out_listener) {
    if (adapter == NULL || out_listener == NULL) {
        return -EINVAL;
    }
    adapter_endpoint* endpoint = NULL;
    int result = endpoint_create(adapter, config, true, adapter->max_receive_owned_bytes, &endpoint);
    if (result != 0) {
        return result;
    }
    adapter_listener* listener = calloc(1, sizeof(*listener));
    if (listener == NULL) {
        endpoint_free(endpoint);
        return -ENOMEM;
    }
    atomic_init(&listener->base.closing, false);
    listener->base.endpoint = endpoint;
    listener->base.owns_endpoint = true;
    listener->base.ready = true;
    listener->base.event_flags = TREVRPC_ENGINE_EVENT_FLAG_SERVER;
    trevrpc_engine_handle_v1 token;
    result = registry_add(adapter, &listener->base, TREVRPC_ENGINE_OBJECT_LISTENER, &token);
    if (result != 0) {
        endpoint_free(endpoint);
        free(listener);
        return result;
    }
    listener->base.terminal_reservation = terminal_reservation;
    HQUIC listener_handle = NULL;
    QUIC_STATUS status =
        adapter->api->ListenerOpen(endpoint->registration, adapter_listener_callback, listener, &listener_handle);
    if (QUIC_FAILED(status)) {
        listener->base.terminal_reservation = NULL;
        registry_remove_unstarted(&listener->base);
        return -EIO;
    }
    bool close_requested = object_publish_handle(&listener->base, listener_handle);
    QUIC_ADDR address;
    result = host_address(endpoint->host, endpoint->port, &address);
    if (result == 0) {
        QUIC_BUFFER alpn = {.Length = endpoint->alpn_len, .Buffer = endpoint->alpn};
        status = adapter->api->ListenerStart(listener_handle, &alpn, 1, &address);
        if (QUIC_FAILED(status)) {
            result = -EIO;
        }
    }
    if (result != 0) {
        listener->base.terminal_reservation = NULL;
        registry_remove_unstarted(&listener->base);
        return result;
    }
    if (close_requested) {
        adapter->api->ListenerStop(listener_handle);
    }
    *out_listener = token;
    return 0;
}

static int provider_listener_get_port(
    msquic_provider* adapter, trevrpc_engine_handle_v1 listener_handle, uint16_t* out_port) {
    if (adapter == NULL || out_port == NULL) {
        return -EINVAL;
    }
    adapter_object* object = NULL;
    int result = registry_get(adapter, listener_handle, TREVRPC_ENGINE_OBJECT_LISTENER, &object, true);
    if (result != 0) {
        return result;
    }
    ADAPTER_OBJECT_SCOPE(object);
    QUIC_ADDR address = {0};
    uint32_t size = sizeof(address);
    QUIC_STATUS status = adapter->api->GetParam(object->handle, QUIC_PARAM_LISTENER_LOCAL_ADDRESS, &size, &address);
    if (QUIC_FAILED(status)) {
        return -EIO;
    }
    uint16_t port = ntohs(address.Ipv4.sin_port);
    *out_port = port;
    return 0;
}

static int provider_dial(msquic_provider* adapter,
    const trevrpc_engine_endpoint_config_v1* config,
    uint64_t operation_id,
    trevrpc_engine_reservation* completion_reservation,
    trevrpc_engine_reservation* terminal_reservation,
    trevrpc_engine_handle_v1* out_connection) {
    if (adapter == NULL || out_connection == NULL || operation_id == 0) {
        return -EINVAL;
    }
    if (!reserve_connection_budget(adapter)) {
        return -EAGAIN;
    }
    adapter_endpoint* endpoint = NULL;
    int result = endpoint_create(adapter, config, false, adapter->max_receive_owned_bytes, &endpoint);
    if (result != 0) {
        release_connection_admission(adapter);
        return result;
    }
    adapter_connection* connection = calloc(1, sizeof(*connection));
    if (connection == NULL) {
        endpoint_free(endpoint);
        release_connection_admission(adapter);
        return -ENOMEM;
    }
    atomic_init(&connection->base.closing, false);
    connection->base.endpoint = endpoint;
    connection->base.owns_endpoint = true;
    connection->max_pending_send_count = limit_product(endpoint->max_pending_send_count, adapter->stream_capacity);
    connection->max_pending_send_bytes = limit_product(endpoint->max_pending_send_bytes, adapter->stream_capacity);
    connection->base.operation_id = operation_id;
    connection->base.operation_pending = true;
    connection->base.event_flags = TREVRPC_ENGINE_EVENT_FLAG_CLIENT;
    connection->admission_granted = true;
    trevrpc_engine_handle_v1 token;
    result = registry_add(adapter, &connection->base, TREVRPC_ENGINE_OBJECT_CONNECTION, &token);
    if (result != 0) {
        endpoint_free(endpoint);
        free(connection);
        release_connection_admission(adapter);
        return result;
    }
    connection->base.ready_reservation = completion_reservation;
    connection->base.terminal_reservation = terminal_reservation;
    HQUIC connection_handle = NULL;
    QUIC_STATUS status = adapter->api->ConnectionOpen(
        endpoint->registration, adapter_connection_callback, connection, &connection_handle);
    if (QUIC_FAILED(status)) {
        connection->base.terminal_reservation = NULL;
        connection->base.ready_reservation = NULL;
        release_connection_admission_if_granted(connection);
        registry_remove_unstarted(&connection->base);
        return -EIO;
    }
    bool close_requested = object_publish_handle(&connection->base, connection_handle);
    status = adapter->api->ConnectionStart(
        connection_handle, endpoint->configuration, QUIC_ADDRESS_FAMILY_UNSPEC, endpoint->host, endpoint->port);
    if (QUIC_FAILED(status)) {
        connection->base.ready_reservation = NULL;
        connection->base.terminal_reservation = NULL;
        release_connection_admission_if_granted(connection);
        registry_remove_unstarted(&connection->base);
        return -EIO;
    }
    if (close_requested) {
        adapter->api->ConnectionShutdown(connection_handle, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
    }
    *out_connection = token;
    return 0;
}

static int provider_dial_cancel(msquic_provider* adapter, trevrpc_engine_handle_v1 connection_handle) {
    if (adapter == NULL) {
        return -EINVAL;
    }
    adapter_object* object = NULL;
    int result = registry_get_with_policy(
        adapter, connection_handle, TREVRPC_ENGINE_OBJECT_CONNECTION, &object, false, REGISTRY_ALLOW_UNAVAILABLE);
    if (result != 0) {
        return result;
    }
    ADAPTER_OBJECT_SCOPE(object);
    HQUIC handle = NULL;
    pthread_mutex_lock(&adapter->mutex);
    if (object->ready) {
        result = -EALREADY;
    } else if (object->cancel_requested) {
        result = 0;
    } else if (object->terminal_published || atomic_load_explicit(&object->closing, memory_order_acquire)) {
        result = -EPIPE;
    } else {
        atomic_store_explicit(&object->closing, true, memory_order_release);
        object->cancel_requested = true;
        handle = object->handle;
        result = 0;
    }
    pthread_mutex_unlock(&adapter->mutex);
    if (handle != NULL) {
        adapter->api->ConnectionShutdown(handle, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
    }
    return result;
}

static adapter_stream* stream_alloc(adapter_connection* connection,
    HQUIC handle,
    uint64_t operation_id,
    uint32_t flags,
    bool peer_pending,
    trevrpc_engine_reservation* ready_reservation,
    trevrpc_engine_reservation* terminal_reservation,
    trevrpc_engine_handle_v1* out_token,
    int* out_result) {
    msquic_provider* adapter = connection->base.adapter;
    adapter_stream* stream = calloc(1, sizeof(*stream));
    if (stream == NULL) {
        if (out_result != NULL) {
            *out_result = -ENOMEM;
        }
        return NULL;
    }
    int pthread_result = pthread_mutex_init(&stream->mutex, NULL);
    if (pthread_result != 0) {
        free(stream);
        if (out_result != NULL) {
            *out_result = -pthread_result;
        }
        return NULL;
    }
    pthread_result = pthread_mutex_init(&stream->send_gate, NULL);
    if (pthread_result != 0) {
        pthread_mutex_destroy(&stream->mutex);
        free(stream);
        if (out_result != NULL) {
            *out_result = -pthread_result;
        }
        return NULL;
    }
    atomic_init(&stream->base.closing, false);
    stream->base.endpoint = connection->base.endpoint;
    endpoint_retain(stream->base.endpoint);
    stream->base.handle = handle;
    stream->base.operation_id = operation_id;
    stream->base.operation_pending = operation_id != 0;
    stream->base.event_flags = flags;
    stream->parent = connection->base.token;
    uint32_t send_id_capacity = connection->base.endpoint->max_pending_send_count;
    if (send_id_capacity != 0) {
        stream->pending_operation_ids = calloc(send_id_capacity, sizeof(*stream->pending_operation_ids));
        if (stream->pending_operation_ids == NULL) {
            pthread_mutex_destroy(&stream->send_gate);
            pthread_mutex_destroy(&stream->mutex);
            endpoint_free(stream->base.endpoint);
            free(stream);
            if (out_result != NULL)
                *out_result = -ENOMEM;
            return NULL;
        }
        for (uint32_t index = 0; index < send_id_capacity; index++) {
            atomic_init(&stream->pending_operation_ids[index], 0);
        }
    }
    if (peer_pending) {
        stream->stream_admission_granted = true;
        stream->peer_queue_ref_held = true;
        stream->base.active_operations++;
    }
    trevrpc_frame_parser_init_with_allocator(&stream->parser,
        connection->base.endpoint->max_frame_size,
        adapter_receive_alloc,
        adapter_receive_free,
        stream);
    trevrpc_frame_parser_set_retain_on_allocation_failure(&stream->parser, true);
    bool owns_ready = ready_reservation == NULL;
    bool owns_terminal = terminal_reservation == NULL;
    int result = 0;
    if (owns_ready) {
        result = trevrpc_engine_provider_reserve_mandatory(adapter->engine, &ready_reservation);
    }
    if (result == 0 && owns_terminal) {
        result = trevrpc_engine_provider_reserve_mandatory(adapter->engine, &terminal_reservation);
    }
    if (result == 0) {
        result = trevrpc_engine_provider_reserve_mandatory(adapter->engine, &stream->receive_fin_reservation);
    }
    if (result == 0) {
        result = registry_add(adapter, &stream->base, TREVRPC_ENGINE_OBJECT_STREAM, out_token);
    }
    if (result == 0) {
        stream->base.ready_reservation = ready_reservation;
        stream->base.terminal_reservation = terminal_reservation;
    } else {
        trevrpc_engine_provider_cancel_reservation(adapter->engine, stream->receive_fin_reservation);
        stream->receive_fin_reservation = NULL;
        if (owns_terminal) {
            trevrpc_engine_provider_cancel_reservation(adapter->engine, terminal_reservation);
        }
        if (owns_ready) {
            trevrpc_engine_provider_cancel_reservation(adapter->engine, ready_reservation);
        }
        if (stream->base.adapter != NULL) {
            registry_remove_unstarted(&stream->base);
        } else {
            trevrpc_frame_parser_reset(&stream->parser);
            free(stream->pending_operation_ids);
            pthread_mutex_destroy(&stream->send_gate);
            pthread_mutex_destroy(&stream->mutex);
            endpoint_free(stream->base.endpoint);
            free(stream);
        }
        if (out_result != NULL) {
            *out_result = result;
        }
        return NULL;
    }
    pthread_mutex_lock(&adapter->mutex);
    connection->live_streams++;
    pthread_mutex_unlock(&adapter->mutex);
    if (out_result != NULL) {
        *out_result = 0;
    }
    return stream;
}

static int provider_connection_open_bidi_stream(msquic_provider* adapter,
    trevrpc_engine_handle_v1 connection_handle,
    uint64_t operation_id,
    trevrpc_engine_reservation* completion_reservation,
    trevrpc_engine_reservation* terminal_reservation,
    trevrpc_engine_handle_v1* out_stream) {
    if (adapter == NULL || out_stream == NULL || operation_id == 0) {
        return -EINVAL;
    }
    adapter_object* object = NULL;
    int result = registry_get(adapter, connection_handle, TREVRPC_ENGINE_OBJECT_CONNECTION, &object, true);
    if (result != 0) {
        return result;
    }
    ADAPTER_OBJECT_SCOPE(object);
    adapter_connection* connection = (adapter_connection*)object;
    trevrpc_engine_handle_v1 token;
    int allocation_result = 0;
    adapter_stream* stream = stream_alloc(connection,
        NULL,
        operation_id,
        connection->base.event_flags | TREVRPC_ENGINE_EVENT_FLAG_LOCAL,
        false,
        completion_reservation,
        terminal_reservation,
        &token,
        &allocation_result);
    if (stream == NULL) {
        return allocation_result;
    }
    HQUIC stream_handle = NULL;
    QUIC_STATUS status = adapter->api->StreamOpen(
        connection->base.handle, QUIC_STREAM_OPEN_FLAG_NONE, adapter_stream_callback, stream, &stream_handle);
    if (QUIC_FAILED(status)) {
        pthread_mutex_lock(&adapter->mutex);
        if (connection->live_streams > 0) {
            connection->live_streams--;
        }
        bool close_parent = connection->base.shutdown_complete && connection->live_streams == 0;
        pthread_mutex_unlock(&adapter->mutex);
        stream->base.terminal_reservation = NULL;
        stream->base.ready_reservation = NULL;
        registry_remove_unstarted(&stream->base);
        if (close_parent) {
            publish_connection_terminal(connection);
        }
        return -EIO;
    }
    bool close_requested = object_publish_handle(&stream->base, stream_handle);
    status = adapter->api->StreamStart(stream_handle, QUIC_STREAM_START_FLAG_IMMEDIATE);
    if (QUIC_FAILED(status)) {
        pthread_mutex_lock(&adapter->mutex);
        if (connection->live_streams > 0) {
            connection->live_streams--;
        }
        bool close_parent = connection->base.shutdown_complete && connection->live_streams == 0;
        pthread_mutex_unlock(&adapter->mutex);
        stream->base.terminal_reservation = NULL;
        stream->base.ready_reservation = NULL;
        registry_remove_unstarted(&stream->base);
        if (close_parent) {
            publish_connection_terminal(connection);
        }
        return -EIO;
    }
    if (close_requested) {
        adapter->api->StreamShutdown(stream_handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
    }
    *out_stream = token;
    return 0;
}

static void* adapter_receive_alloc(size_t size, void* context) {
    adapter_stream* stream = context;
    msquic_provider* adapter = stream->base.adapter;
    stream->receive_alloc_error = 0;
    if (size > SIZE_MAX - sizeof(msquic_receive)) {
        stream->receive_alloc_error = -EOVERFLOW;
        return NULL;
    }
    pthread_mutex_lock(&adapter->budget_mutex);
    bool available = adapter->receive_owned_count < adapter->max_receive_owned_count &&
                     size <= adapter->max_receive_owned_bytes - adapter->receive_owned_bytes;
    if (available) {
        adapter->receive_owned_count++;
        adapter->receive_owned_bytes += size;
        if (adapter->receive_owned_count > adapter->peak_receive_owned_count) {
            adapter->peak_receive_owned_count = adapter->receive_owned_count;
        }
        if (adapter->receive_owned_bytes > adapter->peak_receive_owned_bytes) {
            adapter->peak_receive_owned_bytes = adapter->receive_owned_bytes;
        }
    }
    pthread_mutex_unlock(&adapter->budget_mutex);
    if (!available) {
        stream->receive_alloc_error = -ENOSPC;
        return NULL;
    }
    msquic_receive* receive = malloc(sizeof(*receive) + size);
    if (receive == NULL) {
        stream->receive_alloc_error = -ENOMEM;
        pthread_mutex_lock(&adapter->budget_mutex);
        adapter->receive_owned_count--;
        adapter->receive_owned_bytes -= size;
        pthread_mutex_unlock(&adapter->budget_mutex);
        return NULL;
    }
    receive->len = size;
    stream->parser_receive_allocation = receive;
    return receive->data;
}

static void adapter_receive_destroy(adapter_stream* stream, msquic_receive* receive) {
    if (receive == NULL) {
        return;
    }
    release_receive_credit(stream, receive->len);
    free(receive);
}

static void adapter_receive_free(void* ptr, void* context) {
    if (ptr == NULL) {
        return;
    }
    adapter_stream* stream = context;
    msquic_receive* receive = stream->parser_receive_allocation;
    if (receive == NULL || receive->data != ptr) {
        stream->receive_alloc_error = -EINVAL;
        return;
    }
    stream->parser_receive_allocation = NULL;
    adapter_receive_destroy(stream, receive);
}

static bool reserve_zero_receive(adapter_stream* stream) {
    msquic_provider* adapter = stream->base.adapter;
    pthread_mutex_lock(&adapter->budget_mutex);
    bool available = adapter->receive_owned_count < adapter->max_receive_owned_count;
    if (available) {
        adapter->receive_owned_count++;
        if (adapter->receive_owned_count > adapter->peak_receive_owned_count) {
            adapter->peak_receive_owned_count = adapter->receive_owned_count;
        }
    }
    pthread_mutex_unlock(&adapter->budget_mutex);
    return available;
}

static bool stream_completes_zero_frame_header(const adapter_stream* stream, const uint8_t* data, size_t len) {
    const trevrpc_frame_parser* parser = &stream->parser;
    if (parser->header_len >= sizeof(parser->header) || parser->body != NULL || parser->skip_remaining != 0) {
        return false;
    }
    size_t remaining = sizeof(parser->header) - parser->header_len;
    if (len < remaining) {
        return false;
    }
    for (size_t index = 0; index < parser->header_len; index++) {
        if (parser->header[index] != 0) {
            return false;
        }
    }
    for (size_t index = 0; index < remaining; index++) {
        if (data[index] != 0) {
            return false;
        }
    }
    return true;
}

static bool release_receive_credit(adapter_stream* stream, size_t len) {
    msquic_provider* adapter = stream->base.adapter;
    bool released = false;
    pthread_mutex_lock(&adapter->budget_mutex);
    if (adapter->receive_owned_count > 0) {
        adapter->receive_owned_count--;
        released = true;
    }
    if (adapter->receive_owned_bytes >= len) {
        adapter->receive_owned_bytes -= len;
    }
    pthread_mutex_unlock(&adapter->budget_mutex);
    return released;
}

static void resume_paused_receives(msquic_provider* adapter) {
    for (uint32_t index = adapter->stream_begin; index < adapter->slot_count; index++) {
        adapter_stream* stream = NULL;
        HQUIC handle = NULL;
        pthread_mutex_lock(&adapter->mutex);
        adapter_object* object = adapter->slots[index].object;
        if (adapter->state == TREVRPC_ENGINE_STATE_RUNNING && object != NULL &&
            object->kind == TREVRPC_ENGINE_OBJECT_STREAM && !object->terminal_published && !object->shutdown_complete &&
            !atomic_load_explicit(&object->closing, memory_order_acquire)) {
            stream = (adapter_stream*)object;
            pthread_mutex_lock(&stream->mutex);
            if (stream->receive_paused && !stream->receive_aborted && !stream->receive_fin_published &&
                stream->base.handle != NULL) {
                stream->receive_paused = false;
                handle = stream->base.handle;
                stream->base.active_operations++;
            }
            pthread_mutex_unlock(&stream->mutex);
        }
        pthread_mutex_unlock(&adapter->mutex);
        if (handle != NULL) {
            QUIC_STATUS status = adapter->api->StreamReceiveSetEnabled(handle, TRUE);
            if (QUIC_FAILED(status)) {
                pthread_mutex_lock(&adapter->mutex);
                bool fatal = adapter->state == TREVRPC_ENGINE_STATE_RUNNING && !stream->base.terminal_published &&
                             !stream->base.shutdown_complete && !stream->receive_fin_published &&
                             !atomic_load_explicit(&stream->base.closing, memory_order_acquire);
                pthread_mutex_unlock(&adapter->mutex);
                if (fatal) {
                    adapter_fail_stop(adapter, -EIO, "receive resumption failed");
                }
            }
            adapter_object_unpin(&stream->base);
        }
    }
}

#ifdef TREVRPC_ENGINE_MSQUIC_TESTING
static bool AdapterTestFailNextReadablePublication;
#endif

static bool readable_publication_retryable(int result) {
    return result == -ENOMEM;
}

static int publish_stream_readable(adapter_stream* stream, bool schedule_retry) {
    msquic_provider* adapter = stream->base.adapter;
    pthread_mutex_lock(&adapter->mutex);
    bool ready = stream->base.ready && stream->ready_event_committed && !stream->base.terminal_published;
    pthread_mutex_unlock(&adapter->mutex);
    if (!ready) {
        return 0;
    }
    pthread_mutex_lock(&stream->mutex);
    uint64_t readable_epoch = stream->readable_epoch;
    if (!stream->readable_pending || stream->readable_published_epoch == readable_epoch) {
        pthread_mutex_unlock(&stream->mutex);
        return 0;
    }
    stream->readable_published_epoch = readable_epoch;
    set_readable_retry_locked(stream, true);
    pthread_mutex_unlock(&stream->mutex);
    msquic_event* event = event_new(TREVRPC_ENGINE_EVENT_STREAM_READABLE,
        stream->base.event_flags,
        0,
        TREVRPC_ENGINE_OBJECT_STREAM,
        stream->base.token,
        stream->parent,
        0,
        0,
        0,
        NULL,
        0);
    if (event != NULL) {
        event->readable_epoch = readable_epoch;
    }
#ifdef TREVRPC_ENGINE_MSQUIC_TESTING
    int result;
    if (AdapterTestFailNextReadablePublication) {
        AdapterTestFailNextReadablePublication = false;
        free(event);
        result = -ENOMEM;
    } else {
        result = enqueue_event(stream->base.adapter, event, false);
    }
#else
    int result = enqueue_event(stream->base.adapter, event, false);
#endif
    pthread_mutex_lock(&stream->mutex);
    if (result == 0 && stream->readable_epoch == readable_epoch && stream->readable_published_epoch == readable_epoch) {
        set_readable_retry_locked(stream, false);
    } else if (result != 0 && stream->readable_epoch == readable_epoch) {
        if (stream->readable_published_epoch == readable_epoch) {
            stream->readable_published_epoch = 0;
        }
        if (stream->receive_head != NULL) {
            stream->readable_pending = true;
            set_readable_retry_locked(stream, readable_publication_retryable(result));
        } else {
            stream->readable_pending = false;
            set_readable_retry_locked(stream, false);
        }
    }
    pthread_mutex_unlock(&stream->mutex);
    if (result != 0 && readable_publication_retryable(result) && schedule_retry) {
        adapter_schedule(adapter);
        return 0;
    }
    return result;
}

static int stream_enqueue_receive(adapter_stream* stream, msquic_receive* receive, bool* publish) {
    adapter_receive_node* node = malloc(sizeof(*node));
    if (node == NULL) {
        adapter_receive_destroy(stream, receive);
        return -ENOMEM;
    }
    node->next = NULL;
    node->receive = receive;
    if (stream->receive_tail != NULL) {
        stream->receive_tail->next = node;
    } else {
        stream->receive_head = node;
    }
    stream->receive_tail = node;
    if (!stream->readable_pending) {
        stream->readable_pending = true;
        stream->readable_epoch++;
        *publish = true;
    }
    return 0;
}

static int stream_consume_receive(
    adapter_stream* stream, const uint8_t* data, size_t len, bool* publish_readable, size_t* accepted) {
    size_t offset = 0;
    *accepted = 0;
    while (offset < len) {
        size_t consumed = 0;
        trevrpc_owned_bytes body;
        size_t declared = 0;
        msquic_receive* zero_receive = NULL;
        if (stream_completes_zero_frame_header(stream, data + offset, len - offset)) {
            if (!reserve_zero_receive(stream)) {
                return -ENOSPC;
            }
            zero_receive = malloc(sizeof(*zero_receive));
            if (zero_receive == NULL) {
                release_receive_credit(stream, 0);
                return -ENOMEM;
            }
            zero_receive->len = 0;
        }
        trevrpc_frame_result result = trevrpc_frame_parser_consume_owned(
            &stream->parser, data + offset, len - offset, &consumed, &body, &declared);
        offset += consumed;
        *accepted = offset;
        if (result == TREVRPC_FRAME_READY) {
            msquic_receive* receive = zero_receive;
            if (body.len == 0) {
                if (receive == NULL) {
                    return -EPROTO;
                }
            } else {
                adapter_receive_destroy(stream, zero_receive);
                receive = stream->parser_receive_allocation;
                stream->parser_receive_allocation = NULL;
                if (receive == NULL || receive->data != body.data || receive->len != body.len) {
                    adapter_receive_destroy(stream, receive);
                    trevrpc_owned_bytes_init(&body);
                    return -EPROTO;
                }
            }
            int enqueue_result = stream_enqueue_receive(stream, receive, publish_readable);
            if (enqueue_result != 0) {
                return enqueue_result;
            }
        } else {
            adapter_receive_destroy(stream, zero_receive);
            if (result == TREVRPC_FRAME_TOO_LARGE) {
                return -EMSGSIZE;
            }
            if (result == TREVRPC_FRAME_ALLOCATION_FAILURE) {
                return stream->receive_alloc_error != 0 ? stream->receive_alloc_error : -ENOMEM;
            }
            if (result != TREVRPC_FRAME_NEED_MORE) {
                return -EPROTO;
            }
        }
        if (consumed == 0 && result == TREVRPC_FRAME_NEED_MORE) {
            break;
        }
    }
    return 0;
}

static void adapter_fail_stop(msquic_provider* adapter, int status, const char* message) {
    (void)message;
    pthread_mutex_lock(&adapter->mutex);
    if (adapter->terminal_status == 0) {
        adapter->terminal_status = status;
    }
    pthread_mutex_unlock(&adapter->mutex);
    trevrpc_engine_provider_fail(adapter->engine, status, 0);
}

static void owned_receive_release(void* owner, void* release_context) {
    (void)release_context;
    free(owner);
}

#ifdef TREVRPC_ENGINE_MSQUIC_TESTING
static bool AdapterTestFailNextReceiveCreate;
static bool AdapterTestReceiveDuringReadyPublication;
static bool AdapterTestShutdownAfterSendAdmission;
static bool AdapterTestPromoteBeforePeerHandlePublication;
static bool AdapterTestPeerStreamStayedQueued;
#endif

static int create_owned_receive(msquic_receive* receive, trevrpc_engine_receive** out_receive) {
#ifdef TREVRPC_ENGINE_MSQUIC_TESTING
    if (AdapterTestFailNextReceiveCreate) {
        AdapterTestFailNextReceiveCreate = false;
        return -ENOMEM;
    }
#endif
    return trevrpc_engine_receive_create_owned(receive->data,
        receive->len,
        TREVRPC_ENGINE_RECEIVE_FLAG_NONE,
        receive,
        owned_receive_release,
        NULL,
        out_receive);
}

static int provider_stream_receive_frame(
    msquic_provider* adapter, trevrpc_engine_handle_v1 stream_handle, trevrpc_engine_receive** out_receive) {
    if (adapter == NULL || out_receive == NULL) {
        return -EINVAL;
    }
    adapter_object* object = NULL;
    int result = registry_get(adapter, stream_handle, TREVRPC_ENGINE_OBJECT_STREAM, &object, false);
    if (result != 0) {
        if (result != -EPIPE && result != -ESTALE) {
            return result;
        }
        pthread_mutex_lock(&adapter->mutex);
        adapter_slot* slot = stream_handle.owner == adapter->owner && stream_handle.slot < adapter->slot_count
                                 ? &adapter->slots[stream_handle.slot]
                                 : NULL;
        if (slot != NULL && slot->object != NULL && slot->kind == TREVRPC_ENGINE_OBJECT_STREAM &&
            slot->generation == stream_handle.generation) {
            object = slot->object;
        } else {
            object = registry_find_retired_locked(adapter, stream_handle, TREVRPC_ENGINE_OBJECT_STREAM);
        }
        if (object != NULL) {
            object->active_operations++;
        }
        pthread_mutex_unlock(&adapter->mutex);
        if (object == NULL) {
            return result;
        }
    }
    ADAPTER_OBJECT_SCOPE(object);
    adapter_stream* stream = (adapter_stream*)object;
    trevrpc_engine_receive* detached = NULL;
    bool rearm = false;
    pthread_mutex_lock(&stream->mutex);
    adapter_receive_node* node = stream->receive_head;
    if (node == NULL) {
        pthread_mutex_unlock(&stream->mutex);
        return -EAGAIN;
    }
    result = create_owned_receive(node->receive, &detached);
    if (result != 0) {
        if (!stream->readable_pending) {
            stream->readable_pending = true;
            stream->readable_epoch++;
            rearm = true;
        }
        pthread_mutex_unlock(&stream->mutex);
        if (rearm && publish_stream_readable(stream, true) != 0) {
            adapter_fail_stop(adapter, -ENOSPC, "receive readiness publication failed");
        }
        return result;
    }
    stream->receive_head = node->next;
    if (stream->receive_head == NULL) {
        stream->receive_tail = NULL;
        stream->readable_pending = false;
        set_readable_retry_locked(stream, false);
    } else if (!stream->readable_pending) {
        stream->readable_pending = true;
        stream->readable_epoch++;
        rearm = true;
    }
    pthread_mutex_unlock(&stream->mutex);
    release_receive_credit(stream, node->receive->len);
    free(node);
    resume_paused_receives(adapter);
    if (rearm && publish_stream_readable(stream, true) != 0) {
        adapter_fail_stop(adapter, -ENOSPC, "receive readiness publication failed");
    }
    *out_receive = detached;
    return 0;
}

static uint64_t limit_product(uint64_t left, uint64_t right) {
    if (left == 0 || right == 0) {
        return 0;
    }
    return left > UINT64_MAX / right ? UINT64_MAX : left * right;
}

static adapter_connection* stream_connection_locked(msquic_provider* adapter, adapter_stream* stream) {
    if (stream->parent.owner != adapter->owner || stream->parent.slot >= adapter->slot_count) {
        return NULL;
    }
    adapter_slot* slot = &adapter->slots[stream->parent.slot];
    if (slot->object == NULL || slot->kind != TREVRPC_ENGINE_OBJECT_CONNECTION ||
        slot->generation != stream->parent.generation) {
        return NULL;
    }
    return (adapter_connection*)slot->object;
}

static void queue_stream_locked(msquic_provider* adapter, adapter_stream* stream) {
    if (stream->send_scheduler_queued) {
        return;
    }
    stream->send_scheduler_queued = true;
    stream->send_scheduler_next = NULL;
    if (adapter->send_ready_tail != NULL) {
        adapter->send_ready_tail->send_scheduler_next = stream;
    } else {
        adapter->send_ready_head = stream;
    }
    adapter->send_ready_tail = stream;
}

static void unqueue_stream_locked(msquic_provider* adapter, adapter_stream* stream) {
    if (!stream->send_scheduler_queued) {
        return;
    }
    adapter_stream** link = &adapter->send_ready_head;
    while (*link != NULL && *link != stream) {
        link = &(*link)->send_scheduler_next;
    }
    if (*link == stream) {
        *link = stream->send_scheduler_next;
    }
    adapter->send_ready_tail = NULL;
    for (adapter_stream* queued = adapter->send_ready_head; queued != NULL; queued = queued->send_scheduler_next) {
        adapter->send_ready_tail = queued;
    }
    stream->send_scheduler_next = NULL;
    stream->send_scheduler_queued = false;
}

static bool pending_operation_id_locked(adapter_stream* stream, uint64_t operation_id, uint32_t* slot_out) {
    if (stream->pending_operation_ids != NULL) {
        for (uint32_t index = 0; index < stream->base.endpoint->max_pending_send_count; index++) {
            uint64_t pending = atomic_load_explicit(&stream->pending_operation_ids[index], memory_order_acquire);
            if (pending == operation_id) {
                return true;
            }
            if (slot_out != NULL && *slot_out == UINT32_MAX && pending == 0) {
                *slot_out = index;
            }
        }
        return false;
    }
    for (adapter_send* pending = stream->pending_sends; pending != NULL; pending = pending->next) {
        if (pending->operation_id == operation_id) {
            return true;
        }
    }
    return false;
}

static int reserve_send_admission(adapter_stream* stream, uint64_t operation_id, size_t len) {
    msquic_provider* adapter = stream->base.adapter;
    adapter_endpoint* endpoint = stream->base.endpoint;
    int result = 0;
    uint32_t id_slot = UINT32_MAX;
    pthread_mutex_lock(&stream->send_gate);
    pthread_mutex_lock(&adapter->mutex);
    pthread_mutex_lock(&stream->mutex);
    adapter_connection* connection = stream_connection_locked(adapter, stream);
    if (atomic_load_explicit(&stream->base.closing, memory_order_acquire) || stream->base.shutdown_complete ||
        stream->send_finished || stream->send_aborted) {
        result = -EPIPE;
    } else if (pending_operation_id_locked(stream, operation_id, &id_slot)) {
        result = -EALREADY;
    } else {
        if (adapter->max_pending_send_count == 0) {
            adapter->max_pending_send_count = limit_product(endpoint->max_pending_send_count,
                limit_product(adapter->stream_capacity, adapter->connection_capacity));
        }
        if (adapter->max_pending_send_bytes == 0) {
            adapter->max_pending_send_bytes = limit_product(endpoint->max_pending_send_bytes,
                limit_product(adapter->stream_capacity, adapter->connection_capacity));
        }
        bool stream_full = len > endpoint->max_pending_send_bytes ||
                           stream->pending_send_count >= endpoint->max_pending_send_count ||
                           stream->pending_send_bytes > endpoint->max_pending_send_bytes - len;
        bool connection_full = connection != NULL && connection->max_pending_send_count != 0 &&
                               (connection->pending_send_count >= connection->max_pending_send_count ||
                                   len > connection->max_pending_send_bytes ||
                                   connection->pending_send_bytes > connection->max_pending_send_bytes - len);
        bool provider_full =
            adapter->max_pending_send_count != 0 &&
            (adapter->pending_send_count >= adapter->max_pending_send_count || len > adapter->max_pending_send_bytes ||
                adapter->pending_send_bytes > adapter->max_pending_send_bytes - len);
        if (len > endpoint->max_pending_send_bytes || stream_full || connection_full || provider_full) {
            result = -EAGAIN;
        } else if (stream->pending_operation_ids != NULL && id_slot == UINT32_MAX) {
            result = -EAGAIN;
        } else {
            if (stream->pending_operation_ids != NULL) {
                atomic_store_explicit(&stream->pending_operation_ids[id_slot], operation_id, memory_order_release);
            }
            stream->pending_send_count++;
            stream->pending_send_bytes += len;
            if (connection != NULL) {
                connection->pending_send_count++;
                connection->pending_send_bytes += len;
            }
            adapter->pending_send_count++;
            adapter->pending_send_bytes += len;
        }
    }
    pthread_mutex_unlock(&stream->mutex);
    pthread_mutex_unlock(&adapter->mutex);
    pthread_mutex_unlock(&stream->send_gate);
    return result;
}

static void release_send_admission(adapter_stream* stream, uint64_t operation_id, size_t len) {
    msquic_provider* adapter = stream->base.adapter;
    pthread_mutex_lock(&adapter->mutex);
    pthread_mutex_lock(&stream->mutex);
    adapter_connection* connection = stream_connection_locked(adapter, stream);
    if (stream->pending_send_count > 0) {
        stream->pending_send_count--;
    }
    if (stream->pending_send_bytes >= len) {
        stream->pending_send_bytes -= len;
    } else {
        stream->pending_send_bytes = 0;
    }
    if (stream->pending_operation_ids != NULL) {
        for (uint32_t index = 0; index < stream->base.endpoint->max_pending_send_count; index++) {
            uint64_t pending = atomic_load_explicit(&stream->pending_operation_ids[index], memory_order_acquire);
            if (pending == operation_id) {
                atomic_store_explicit(&stream->pending_operation_ids[index], 0, memory_order_release);
                break;
            }
        }
    }
    if (connection != NULL) {
        if (connection->pending_send_count > 0) {
            connection->pending_send_count--;
        }
        if (connection->pending_send_bytes >= len) {
            connection->pending_send_bytes -= len;
        } else {
            connection->pending_send_bytes = 0;
        }
    }
    if (adapter->pending_send_count > 0) {
        adapter->pending_send_count--;
    }
    if (adapter->pending_send_bytes >= len) {
        adapter->pending_send_bytes -= len;
    } else {
        adapter->pending_send_bytes = 0;
    }
    pthread_mutex_unlock(&stream->mutex);
    pthread_mutex_unlock(&adapter->mutex);
    if (stream_shutdown_drained(stream)) {
        publish_stream_terminal(stream, false, 0);
    } else {
        adapter_schedule(adapter);
    }
}

static bool send_operation_release(adapter_send* send) {
    adapter_stream* stream = send->stream;
    msquic_provider* adapter = stream->base.adapter;
    bool found = false;
    pthread_mutex_lock(&adapter->mutex);
    pthread_mutex_lock(&stream->mutex);
    adapter_send** link = &stream->pending_sends;
    while (*link != NULL && *link != send) {
        link = &(*link)->next;
    }
    if (*link == send) {
        *link = send->next;
        if (stream->pending_send_tail == send) {
            stream->pending_send_tail = NULL;
            for (adapter_send* pending = stream->pending_sends; pending != NULL; pending = pending->next) {
                stream->pending_send_tail = pending;
            }
        }
        found = true;
        adapter_connection* connection = stream_connection_locked(adapter, stream);
        if (stream->pending_send_count > 0)
            stream->pending_send_count--;
        if (stream->pending_send_bytes >= send->len)
            stream->pending_send_bytes -= send->len;
        if (connection != NULL) {
            if (connection->pending_send_count > 0)
                connection->pending_send_count--;
            if (connection->pending_send_bytes >= send->len)
                connection->pending_send_bytes -= send->len;
        }
        if (adapter->pending_send_count > 0)
            adapter->pending_send_count--;
        if (adapter->pending_send_bytes >= send->len)
            adapter->pending_send_bytes -= send->len;
        bool has_unsent = false;
        for (adapter_send* pending = stream->pending_sends; pending != NULL; pending = pending->next) {
            if (!pending->submitted) {
                has_unsent = true;
                break;
            }
        }
        if (has_unsent && adapter->state == TREVRPC_ENGINE_STATE_RUNNING &&
            !atomic_load_explicit(&stream->base.closing, memory_order_acquire) && !stream->base.shutdown_complete) {
            queue_stream_locked(adapter, stream);
        } else if (!has_unsent) {
            unqueue_stream_locked(adapter, stream);
        }
    }
    pthread_mutex_unlock(&stream->mutex);
    pthread_mutex_unlock(&adapter->mutex);
    if (found)
        adapter_schedule(adapter);
    return found;
}

static void release_send_operation_id(adapter_stream* stream, uint64_t operation_id) {
    if (stream->pending_operation_ids != NULL) {
        for (uint32_t index = 0; index < stream->base.endpoint->max_pending_send_count; index++) {
            uint64_t pending = atomic_load_explicit(&stream->pending_operation_ids[index], memory_order_acquire);
            if (pending == operation_id) {
                atomic_store_explicit(&stream->pending_operation_ids[index], 0, memory_order_release);
                break;
            }
        }
    }
}

static void send_completion_commit(void* provider_context, void* hook_context) {
    (void)provider_context;
    adapter_send* send = hook_context;
    release_send_operation_id(send->stream, send->operation_id);
}

static int publish_send_completion(adapter_send* send, int status) {
    adapter_stream* stream = send->stream;
    msquic_provider* adapter = stream->base.adapter;
    trevrpc_engine_event_spec spec;
    memset(&spec, 0, sizeof(spec));
    spec.kind = TREVRPC_ENGINE_EVENT_SEND_COMPLETE;
    spec.flags = stream->base.event_flags;
    spec.status = status;
    spec.subject_kind = TREVRPC_ENGINE_OBJECT_STREAM;
    spec.subject = stream->base.token;
    spec.parent = stream->parent;
    spec.operation_id = send->operation_id;
    spec.mandatory_commit_hook = send_completion_commit;
    spec.mandatory_abort_hook = send_completion_commit;
    spec.mandatory_hook_context = send;
    trevrpc_engine_reservation* reservation = send->completion_reservation;
    send->completion_reservation = NULL;
    int result = trevrpc_engine_provider_publish_reserved(adapter->engine, reservation, &spec);
    free(send->data);
    free(send);
    return result;
}

static void cancel_unsent_sends(adapter_stream* stream, int status) {
    msquic_provider* adapter = stream->base.adapter;
    adapter_send* canceled_head = NULL;
    adapter_send* canceled_tail = NULL;
    pthread_mutex_lock(&stream->send_gate);
    pthread_mutex_lock(&adapter->mutex);
    pthread_mutex_lock(&stream->mutex);
    adapter_connection* connection = stream_connection_locked(adapter, stream);
    adapter_send** link = &stream->pending_sends;
    while (*link != NULL) {
        adapter_send* send = *link;
        if (send->submitted) {
            link = &send->next;
            continue;
        }
        *link = send->next;
        send->next = NULL;
        if (canceled_tail != NULL) {
            canceled_tail->next = send;
        } else {
            canceled_head = send;
        }
        canceled_tail = send;
        atomic_store_explicit(&send->completed, true, memory_order_release);
        if (stream->pending_send_count > 0)
            stream->pending_send_count--;
        if (stream->pending_send_bytes >= send->len)
            stream->pending_send_bytes -= send->len;
        if (connection != NULL) {
            if (connection->pending_send_count > 0)
                connection->pending_send_count--;
            if (connection->pending_send_bytes >= send->len)
                connection->pending_send_bytes -= send->len;
        }
        if (adapter->pending_send_count > 0)
            adapter->pending_send_count--;
        if (adapter->pending_send_bytes >= send->len)
            adapter->pending_send_bytes -= send->len;
    }
    stream->pending_send_tail = NULL;
    for (adapter_send* pending = stream->pending_sends; pending != NULL; pending = pending->next) {
        stream->pending_send_tail = pending;
    }
    unqueue_stream_locked(adapter, stream);
    pthread_mutex_unlock(&stream->mutex);
    pthread_mutex_unlock(&adapter->mutex);
    pthread_mutex_unlock(&stream->send_gate);

    while (canceled_head != NULL) {
        adapter_send* send = canceled_head;
        canceled_head = send->next;
        send->next = NULL;
        (void)publish_send_completion(send, status);
    }
    adapter_schedule(adapter);
}

static bool stream_shutdown_drained(adapter_stream* stream) {
    msquic_provider* adapter = stream->base.adapter;
    pthread_mutex_lock(&adapter->mutex);
    pthread_mutex_lock(&stream->mutex);
    bool ready = stream->base.shutdown_complete && stream->pending_send_count == 0;
    pthread_mutex_unlock(&stream->mutex);
    pthread_mutex_unlock(&adapter->mutex);
    return ready;
}

static int provider_stream_send_frame(msquic_provider* adapter,
    trevrpc_engine_handle_v1 stream_handle,
    uint64_t operation_id,
    const uint8_t* body,
    size_t body_len,
    trevrpc_engine_reservation* completion_reservation) {
    if (adapter == NULL || operation_id == 0 || (body == NULL && body_len != 0)) {
        return -EINVAL;
    }
    adapter_object* object = NULL;
    int result = registry_get(adapter, stream_handle, TREVRPC_ENGINE_OBJECT_STREAM, &object, true);
    if (result != 0) {
        return result;
    }
    ADAPTER_OBJECT_SCOPE(object);
    adapter_stream* stream = (adapter_stream*)object;
    if (body_len > stream->base.endpoint->max_frame_size) {
        return -EMSGSIZE;
    }
    if (body_len > UINT32_MAX - 4u) {
        return -EMSGSIZE;
    }
    size_t wire_len = body_len + 4;
    result = reserve_send_admission(stream, operation_id, wire_len);
    if (result != 0) {
        return result;
    }
#ifdef TREVRPC_ENGINE_MSQUIC_TESTING
    if (AdapterTestShutdownAfterSendAdmission) {
        AdapterTestShutdownAfterSendAdmission = false;
        QUIC_STREAM_EVENT event = {.Type = QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE};
        (void)adapter_stream_callback(stream->base.handle, stream, &event);
    }
#endif
    adapter_send* send = calloc(1, sizeof(*send));
    if (send == NULL) {
        release_send_admission(stream, operation_id, wire_len);
        return -ENOMEM;
    }
    send->data = malloc(wire_len);
    if (send->data == NULL) {
        free(send);
        release_send_admission(stream, operation_id, wire_len);
        return -ENOMEM;
    }
    send->stream = stream;
    send->operation_id = operation_id;
    send->len = wire_len;
    atomic_init(&send->completed, false);
    send->completion_reservation = completion_reservation;
    send->data[0] = (uint8_t)(body_len >> 24);
    send->data[1] = (uint8_t)(body_len >> 16);
    send->data[2] = (uint8_t)(body_len >> 8);
    send->data[3] = (uint8_t)body_len;
    if (body_len > 0) {
        memcpy(send->data + 4, body, body_len);
    }
    send->buffer.Buffer = send->data;
    send->buffer.Length = (uint32_t)wire_len;

    pthread_mutex_lock(&adapter->mutex);
    pthread_mutex_lock(&stream->mutex);
    bool closing = atomic_load_explicit(&stream->base.closing, memory_order_acquire) ||
                   stream->base.shutdown_complete || stream->base.terminal_published || stream->send_finished;
    if (!closing) {
        if (stream->pending_send_tail != NULL) {
            stream->pending_send_tail->next = send;
        } else {
            stream->pending_sends = send;
        }
        stream->pending_send_tail = send;
        queue_stream_locked(adapter, stream);
    }
    pthread_mutex_unlock(&stream->mutex);
    pthread_mutex_unlock(&adapter->mutex);
    if (closing) {
        free(send->data);
        free(send);
        release_send_admission(stream, operation_id, wire_len);
        return -EPIPE;
    }
    adapter_schedule(adapter);
    return 0;
}

static void publish_listener_terminal(adapter_listener* listener) {
    msquic_provider* adapter = listener->base.adapter;
    pthread_mutex_lock(&adapter->mutex);
    if (listener->base.terminal_published) {
        pthread_mutex_unlock(&adapter->mutex);
        return;
    }
    if (listener->base.active_operations != 0) {
        listener->base.terminal_deferred = true;
        pthread_mutex_unlock(&adapter->mutex);
        return;
    }
    listener->base.terminal_deferred = false;
    trevrpc_engine_reservation* reservation = listener->base.terminal_reservation;
    listener->base.terminal_reservation = NULL;
    HQUIC handle = object_take_handle_locked(&listener->base);
    adapter_native_close_begin_locked(adapter, handle);
    (void)object_terminal_locked(&listener->base);
    pthread_mutex_unlock(&adapter->mutex);

    if (handle != NULL) {
        adapter->api->ListenerClose(handle);
    }
    adapter_native_close_complete(adapter, handle);
    (void)publish_reserved_event(adapter,
        reservation,
        TREVRPC_ENGINE_EVENT_LISTENER_STOPPED,
        listener->base.event_flags | TREVRPC_ENGINE_EVENT_FLAG_TERMINAL,
        0,
        TREVRPC_ENGINE_OBJECT_LISTENER,
        listener->base.token,
        (trevrpc_engine_handle_v1){0},
        0,
        0,
        0,
        &listener->base);
    adapter_maybe_stopped(adapter);
}

static void publish_connection_ready(adapter_connection* connection, const uint8_t* alpn, size_t alpn_len) {
    (void)alpn;
    (void)alpn_len;
    msquic_provider* adapter = connection->base.adapter;
    bool cancel = false;
    trevrpc_engine_reservation* reservation = NULL;
    pthread_mutex_lock(&adapter->mutex);
    if (!connection->base.ready && !connection->base.terminal_published) {
        if (connection->base.cancel_requested) {
            cancel = true;
        } else {
            connection->base.ready = true;
            connection->base.operation_pending = false;
            reservation = connection->base.ready_reservation;
            connection->base.ready_reservation = NULL;
        }
    }
    pthread_mutex_unlock(&adapter->mutex);
    if (reservation != NULL && publish_reserved_event(adapter,
                                   reservation,
                                   TREVRPC_ENGINE_EVENT_CONNECTION_READY,
                                   connection->base.event_flags,
                                   0,
                                   TREVRPC_ENGINE_OBJECT_CONNECTION,
                                   connection->base.token,
                                   connection->parent,
                                   connection->base.operation_id,
                                   0,
                                   0,
                                   NULL) != 0) {
        adapter_fail_stop(adapter, -EIO, "connection readiness publication failed");
    }
    if (!cancel) {
        adapter_schedule(adapter);
    } else {
        adapter->api->ConnectionShutdown(connection->base.handle, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
    }
}

static int publish_stream_ready(adapter_stream* stream) {
    msquic_provider* adapter = stream->base.adapter;
    trevrpc_engine_reservation* reservation;
    pthread_mutex_lock(&adapter->mutex);
    if (stream->base.ready || stream->base.terminal_published) {
        pthread_mutex_unlock(&adapter->mutex);
        return 0;
    }
    adapter_connection* connection = NULL;
    if (stream->parent.owner == adapter->owner && stream->parent.slot < adapter->slot_count) {
        adapter_slot* parent_slot = &adapter->slots[stream->parent.slot];
        if (parent_slot->object != NULL && parent_slot->kind == TREVRPC_ENGINE_OBJECT_CONNECTION &&
            parent_slot->generation == stream->parent.generation) {
            connection = (adapter_connection*)parent_slot->object;
        }
    }
    if (adapter->state != TREVRPC_ENGINE_STATE_RUNNING || connection == NULL || !connection->base.ready ||
        connection->base.terminal_published || atomic_load_explicit(&connection->base.closing, memory_order_acquire) ||
        stream->base.handle == NULL || atomic_load_explicit(&stream->base.closing, memory_order_acquire)) {
        pthread_mutex_unlock(&adapter->mutex);
        return -ECANCELED;
    }
    stream->base.ready = true;
    stream->base.operation_pending = false;
    reservation = stream->base.ready_reservation;
    stream->base.ready_reservation = NULL;
    pthread_mutex_unlock(&adapter->mutex);

#ifdef TREVRPC_ENGINE_MSQUIC_TESTING
    if (AdapterTestReceiveDuringReadyPublication) {
        AdapterTestReceiveDuringReadyPublication = false;
        uint8_t frame[4] = {0};
        QUIC_BUFFER buffer = {.Length = sizeof(frame), .Buffer = frame};
        QUIC_STREAM_EVENT event = {.Type = QUIC_STREAM_EVENT_RECEIVE};
        event.RECEIVE.TotalBufferLength = sizeof(frame);
        event.RECEIVE.BufferCount = 1;
        event.RECEIVE.Buffers = &buffer;
        (void)adapter_stream_callback(stream->base.handle, stream, &event);
    }
#endif

    int result = publish_reserved_event(adapter,
        reservation,
        TREVRPC_ENGINE_EVENT_STREAM_READY,
        stream->base.event_flags,
        0,
        TREVRPC_ENGINE_OBJECT_STREAM,
        stream->base.token,
        stream->parent,
        stream->base.operation_id,
        0,
        0,
        NULL);
    if (result == 0) {
        pthread_mutex_lock(&adapter->mutex);
        stream->ready_event_committed = true;
        pthread_mutex_unlock(&adapter->mutex);
        pthread_mutex_lock(&stream->mutex);
        bool readable_pending = stream->readable_pending;
        pthread_mutex_unlock(&stream->mutex);
        if (readable_pending) {
            result = publish_stream_readable(stream, true);
        }
    }
    return result;
}

static void publish_connection_terminal(adapter_connection* connection) {
    msquic_provider* adapter = connection->base.adapter;
    adapter_pending_connection* pending_node = NULL;
    bool release_queue_ref = false;
    trevrpc_engine_reservation* terminal;
    trevrpc_engine_reservation* unused_ready;
    bool failed;
    bool canceled;
    int status;
    uint32_t flags;
    uint64_t transport_error;
    pthread_mutex_lock(&adapter->mutex);
    if (connection->base.terminal_published) {
        pthread_mutex_unlock(&adapter->mutex);
        return;
    }
    if (connection->on_accept_queue) {
        pending_node = remove_connection_queue_locked(adapter, connection, &release_queue_ref);
    }
    if (connection->handshake_in_flight) {
        connection->handshake_in_flight = false;
        if (adapter->connection_handshakes_in_flight > 0) {
            adapter->connection_handshakes_in_flight--;
        }
    }
    if (connection->base.active_operations != 0 || connection->live_streams != 0) {
        connection->base.terminal_deferred = true;
        pthread_mutex_unlock(&adapter->mutex);
        free(pending_node);
        if (release_queue_ref) {
            adapter_object_unpin(&connection->base);
        }
        adapter_schedule(adapter);
        return;
    }
    connection->base.terminal_deferred = false;
    failed = !connection->base.ready;
    canceled = connection->base.cancel_requested;
    transport_error = connection->transport_error;
    status = failed ? (canceled ? -ECANCELED : -EIO) : 0;
    flags = connection->base.event_flags | TREVRPC_ENGINE_EVENT_FLAG_TERMINAL;
    if (transport_error != 0) {
        flags |= TREVRPC_ENGINE_EVENT_FLAG_TRANSPORT_ERROR;
        if (!canceled) {
            status = -EIO;
        }
    }
    terminal = connection->base.terminal_reservation;
    connection->base.terminal_reservation = NULL;
    unused_ready = connection->base.ready_reservation;
    connection->base.ready_reservation = NULL;
    HQUIC handle = object_take_handle_locked(&connection->base);
    adapter_native_close_begin_locked(adapter, handle);
    (void)object_terminal_locked(&connection->base);
    if (connection->admission_granted) {
        connection->admission_granted = false;
        if (adapter->connection_budget_used > 0) {
            adapter->connection_budget_used--;
        }
    }
    pthread_mutex_unlock(&adapter->mutex);
    free(pending_node);

    if (handle != NULL) {
        adapter->api->ConnectionClose(handle);
    }
    adapter_native_close_complete(adapter, handle);
    trevrpc_engine_provider_cancel_reservation(adapter->engine, unused_ready);
    (void)publish_reserved_event(adapter,
        terminal,
        failed ? TREVRPC_ENGINE_EVENT_CONNECTION_FAILED : TREVRPC_ENGINE_EVENT_CONNECTION_CLOSED,
        flags,
        status,
        TREVRPC_ENGINE_OBJECT_CONNECTION,
        connection->base.token,
        connection->parent,
        connection->base.operation_id,
        connection->application_error,
        transport_error,
        &connection->base);
    adapter_maybe_stopped(adapter);
    adapter_schedule(adapter);
}

static void publish_stream_terminal(adapter_stream* stream, bool failed, int status) {
    msquic_provider* adapter = stream->base.adapter;
    adapter_connection* connection = NULL;
    adapter_pending_stream* pending_node = NULL;
    bool release_peer_queue_ref = false;
    bool release_stream_budget_later = false;
    bool promote_peer_queue = false;
    bool close_parent = false;
    trevrpc_engine_reservation* terminal;
    trevrpc_engine_reservation* unused_ready;
    trevrpc_engine_reservation* unused_receive_fin;
    pthread_mutex_lock(&adapter->mutex);
    if (stream->base.terminal_published) {
        pthread_mutex_unlock(&adapter->mutex);
        return;
    }
    adapter_slot* parent_slot = stream->parent.slot < adapter->slot_count ? &adapter->slots[stream->parent.slot] : NULL;
    connection = parent_slot != NULL && parent_slot->object != NULL &&
                         parent_slot->generation == stream->parent.generation &&
                         parent_slot->kind == TREVRPC_ENGINE_OBJECT_CONNECTION
                     ? (adapter_connection*)parent_slot->object
                     : NULL;
    pthread_mutex_lock(&stream->mutex);
    bool shutdown_drained = stream->base.shutdown_complete && stream->pending_send_count == 0;
    if (shutdown_drained) {
        unqueue_stream_locked(adapter, stream);
    }
    pthread_mutex_unlock(&stream->mutex);
    if (connection != NULL && stream->on_peer_queue) {
        pending_node = remove_peer_stream_queue_locked(adapter, connection, stream, &release_peer_queue_ref);
        promote_peer_queue = pending_node != NULL && adapter->state == TREVRPC_ENGINE_STATE_RUNNING &&
                             connection->base.ready && !connection->base.terminal_published &&
                             !atomic_load_explicit(&connection->base.closing, memory_order_acquire);
    }
    if (stream->base.active_operations != 0 || !shutdown_drained) {
        stream->base.terminal_deferred = true;
        if (failed || !stream->base.terminal_failed) {
            stream->base.terminal_failed = failed;
            stream->base.terminal_deferred_status = status;
        }
        pthread_mutex_unlock(&adapter->mutex);
        free(pending_node);
        if (release_peer_queue_ref) {
            adapter_object_unpin(&stream->base);
        }
        if (promote_peer_queue) {
            adapter_schedule(adapter);
        }
        return;
    }
    stream->base.terminal_deferred = false;
    terminal = stream->base.terminal_reservation;
    stream->base.terminal_reservation = NULL;
    unused_ready = stream->base.ready_reservation;
    stream->base.ready_reservation = NULL;
    unused_receive_fin = stream->receive_fin_reservation;
    stream->receive_fin_reservation = NULL;
    if (stream->stream_admission_granted) {
        stream->stream_admission_granted = false;
        release_stream_budget_later = true;
    }
    if (connection != NULL && connection->live_streams > 0) {
        connection->live_streams--;
    }
    close_parent = connection != NULL && connection->base.shutdown_complete && connection->live_streams == 0;
    if (close_parent) {
        connection->base.active_operations++;
        connection->base.terminal_deferred = true;
    }
    HQUIC handle = object_take_handle_locked(&stream->base);
    adapter_native_close_begin_locked(adapter, handle);
    (void)object_terminal_locked(&stream->base);
    pthread_mutex_unlock(&adapter->mutex);

    if (release_stream_budget_later) {
        release_stream_budget(adapter);
    }
    free(pending_node);
    if (handle != NULL) {
        adapter->api->StreamClose(handle);
    }
    adapter_native_close_complete(adapter, handle);
    trevrpc_engine_provider_cancel_reservation(adapter->engine, unused_ready);
    trevrpc_engine_provider_cancel_reservation(adapter->engine, unused_receive_fin);
    (void)publish_reserved_event(adapter,
        terminal,
        failed ? TREVRPC_ENGINE_EVENT_STREAM_FAILED : TREVRPC_ENGINE_EVENT_STREAM_CLOSED,
        stream->base.event_flags | TREVRPC_ENGINE_EVENT_FLAG_TERMINAL,
        status,
        TREVRPC_ENGINE_OBJECT_STREAM,
        stream->base.token,
        stream->parent,
        stream->base.operation_id,
        stream->application_error,
        0,
        &stream->base);
    adapter_maybe_stopped(adapter);
    if (close_parent) {
        adapter_object_unpin(&connection->base);
    }
    if (release_peer_queue_ref) {
        adapter_object_unpin(&stream->base);
    }
    if (promote_peer_queue) {
        adapter_schedule(adapter);
    }
}

static void adapter_object_unpin(adapter_object* object) {
    msquic_provider* adapter = object->adapter;
    adapter_object* reclaim = NULL;
    uint32_t kind = 0;
    bool failed = false;
    bool check_stopped = false;
    HQUIC closing_handle = NULL;
    int status = 0;
    pthread_mutex_lock(&adapter->mutex);
    if (object->active_operations > 0) {
        object->active_operations--;
    }
    if (object->active_operations == 0 && object->terminal_deferred && !object->terminal_published) {
        kind = object->kind;
        failed = object->terminal_failed;
        status = object->terminal_deferred_status;
        object->terminal_deferred = false;
    } else if (object->on_retired_list && !object_retains_tombstone_locked(object)) {
        adapter_object** link = &adapter->retired_objects;
        while (*link != NULL && *link != object) {
            link = &(*link)->retired_next;
        }
        if (*link == object) {
            *link = object->retired_next;
            object->retired_next = NULL;
            object->on_retired_list = false;
            check_stopped = object->live_counted;
            if (check_stopped) {
                closing_handle = object->handle;
                adapter_native_close_begin_locked(adapter, closing_handle);
            }
            object_release_live_count_locked(object);
            reclaim = object;
        }
    }
    pthread_mutex_unlock(&adapter->mutex);
    if (kind == TREVRPC_ENGINE_OBJECT_LISTENER) {
        publish_listener_terminal((adapter_listener*)object);
    } else if (kind == TREVRPC_ENGINE_OBJECT_CONNECTION) {
        publish_connection_terminal((adapter_connection*)object);
    } else if (kind == TREVRPC_ENGINE_OBJECT_STREAM) {
        publish_stream_terminal((adapter_stream*)object, failed, status);
    }
    adapter_object_free(reclaim);
    adapter_native_close_complete(adapter, closing_handle);
    if (check_stopped) {
        adapter_maybe_stopped(adapter);
    }
    adapter_schedule(adapter);
}

static void publish_receive_fin(adapter_stream* stream, uint32_t event_flags, int status) {
    msquic_provider* adapter = stream->base.adapter;
    pthread_mutex_lock(&adapter->mutex);
    if (stream->receive_fin_published || stream->receive_fin_reservation == NULL) {
        pthread_mutex_unlock(&adapter->mutex);
        return;
    }
    stream->receive_fin_published = true;
    pthread_mutex_lock(&stream->mutex);
    stream->receive_paused = false;
    pthread_mutex_unlock(&stream->mutex);
    trevrpc_engine_reservation* reservation = stream->receive_fin_reservation;
    stream->receive_fin_reservation = NULL;
    uint64_t application_error = stream->application_error;
    pthread_mutex_unlock(&adapter->mutex);

    (void)publish_reserved_event(adapter,
        reservation,
        TREVRPC_ENGINE_EVENT_RECEIVE_FIN,
        stream->base.event_flags | TREVRPC_ENGINE_EVENT_FLAG_TERMINAL | event_flags,
        status,
        TREVRPC_ENGINE_OBJECT_STREAM,
        stream->base.token,
        stream->parent,
        0,
        application_error,
        0,
        NULL);
}

static bool reserve_connection_budget(msquic_provider* adapter) {
    pthread_mutex_lock(&adapter->mutex);
    bool available = adapter->state == TREVRPC_ENGINE_STATE_RUNNING &&
                     adapter->connection_budget_used < adapter->connection_capacity;
    if (available) {
        adapter->connection_budget_used++;
    }
    pthread_mutex_unlock(&adapter->mutex);
    return available;
}

static bool reserve_connection_admission(msquic_provider* adapter, adapter_listener* listener) {
    pthread_mutex_lock(&adapter->mutex);
    bool available = adapter->accept_admission_open && adapter->state == TREVRPC_ENGINE_STATE_RUNNING &&
                     (listener == NULL || !atomic_load_explicit(&listener->base.closing, memory_order_acquire)) &&
                     adapter->connection_budget_used < adapter->connection_capacity;
    if (available) {
        adapter->connection_budget_used++;
    }
    pthread_mutex_unlock(&adapter->mutex);
    return available;
}

static void release_connection_admission(msquic_provider* adapter) {
    pthread_mutex_lock(&adapter->mutex);
    if (adapter->connection_budget_used > 0) {
        adapter->connection_budget_used--;
    }
    pthread_mutex_unlock(&adapter->mutex);
    adapter_schedule(adapter);
}

static void release_connection_admission_if_granted(adapter_connection* connection) {
    msquic_provider* adapter = connection->base.adapter;
    bool release = false;
    pthread_mutex_lock(&adapter->mutex);
    if (connection->admission_granted) {
        connection->admission_granted = false;
        release = true;
    }
    pthread_mutex_unlock(&adapter->mutex);
    if (release) {
        release_connection_admission(adapter);
    }
}

static QUIC_STATUS normalize_accept_failure(int status) {
    return status == -ENOMEM ? QUIC_STATUS_OUT_OF_MEMORY : QUIC_STATUS_ABORTED;
}

static adapter_pending_connection* remove_connection_queue_locked(
    msquic_provider* adapter, adapter_connection* connection, bool* release_queue_ref) {
    adapter_pending_connection* node = connection->pending_accept_node;
    if (release_queue_ref != NULL) {
        *release_queue_ref = connection->accept_queue_ref_held;
    }
    connection->pending_accept_node = NULL;
    connection->accept_queue_ref_held = false;
    connection->on_accept_queue = false;
    if (node == NULL) {
        return NULL;
    }
    adapter_pending_connection** link = &adapter->pending_connection_head;
    while (*link != NULL && *link != node) {
        link = &(*link)->next;
    }
    if (*link != node) {
        return NULL;
    }
    *link = node->next;
    if (adapter->pending_connection_tail == node) {
        adapter->pending_connection_tail = NULL;
        for (adapter_pending_connection* pending = adapter->pending_connection_head; pending != NULL;
            pending = pending->next) {
            adapter->pending_connection_tail = pending;
        }
    }
    if (adapter->pending_connection_count > 0) {
        adapter->pending_connection_count--;
    }
    return node;
}

static adapter_pending_connection* detach_pending_connections_locked(
    msquic_provider* adapter, trevrpc_engine_handle_v1 parent, bool filter_parent) {
    adapter_pending_connection* detached_head = NULL;
    adapter_pending_connection* detached_tail = NULL;
    adapter_pending_connection** link = &adapter->pending_connection_head;
    while (*link != NULL) {
        adapter_pending_connection* node = *link;
        adapter_connection* connection = node->connection;
        if (filter_parent &&
            (connection == NULL || connection->parent.owner != parent.owner || connection->parent.slot != parent.slot ||
                connection->parent.generation != parent.generation)) {
            link = &node->next;
            continue;
        }
        *link = node->next;
        node->next = NULL;
        if (detached_tail != NULL) {
            detached_tail->next = node;
        } else {
            detached_head = node;
        }
        detached_tail = node;
        if (adapter->pending_connection_count > 0) {
            adapter->pending_connection_count--;
        }
        if (connection != NULL) {
            node->release_queue_ref = connection->accept_queue_ref_held;
            connection->pending_accept_node = NULL;
            connection->accept_queue_ref_held = false;
            connection->on_accept_queue = false;
            atomic_store_explicit(&connection->base.closing, true, memory_order_release);
            if (!connection->base.ready) {
                connection->base.cancel_requested = true;
            }
            if (connection->base.handle == NULL) {
                connection->base.shutdown_complete = true;
            }
        }
    }
    adapter->pending_connection_tail = NULL;
    for (adapter_pending_connection* node = adapter->pending_connection_head; node != NULL; node = node->next) {
        adapter->pending_connection_tail = node;
    }
    return detached_head;
}

static adapter_pending_stream* detach_pending_streams_locked(msquic_provider* adapter, adapter_connection* connection) {
    adapter_pending_stream* detached_head = connection->pending_peer_stream_head;
    connection->pending_peer_stream_head = NULL;
    connection->pending_peer_stream_tail = NULL;
    uint64_t detached_count = connection->pending_peer_stream_count;
    connection->pending_peer_stream_count = 0;
    if (adapter->pending_stream_count >= detached_count) {
        adapter->pending_stream_count -= detached_count;
    } else {
        adapter->pending_stream_count = 0;
    }
    for (adapter_pending_stream* node = detached_head; node != NULL; node = node->next) {
        adapter_stream* stream = node->stream;
        if (stream == NULL) {
            continue;
        }
        node->release_queue_ref = stream->peer_queue_ref_held;
        stream->pending_peer_node = NULL;
        stream->peer_queue_ref_held = false;
        stream->on_peer_queue = false;
        atomic_store_explicit(&stream->base.closing, true, memory_order_release);
        if (stream->base.handle == NULL) {
            stream->base.shutdown_complete = true;
        }
    }
    return detached_head;
}

static void drain_detached_connection(
    msquic_provider* adapter, adapter_connection* connection, bool release_queue_ref, uint64_t application_error_code) {
    pthread_mutex_lock(&adapter->mutex);
    adapter_pending_stream* streams = detach_pending_streams_locked(adapter, connection);
    HQUIC connection_handle = connection->base.handle;
    bool connection_shutdown_complete = connection->base.shutdown_complete;
    pthread_mutex_unlock(&adapter->mutex);

    while (streams != NULL) {
        adapter_pending_stream* stream_node = streams;
        adapter_stream* stream = stream_node->stream;
        streams = stream_node->next;
        if (stream != NULL) {
            HQUIC stream_handle = stream->base.handle;
            publish_stream_terminal(stream, true, -ECANCELED);
            if (stream_handle != NULL && stream->base.handle == stream_handle) {
                adapter->api->StreamShutdown(stream_handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
            }
            if (stream_node->release_queue_ref) {
                adapter_object_unpin(&stream->base);
            }
        }
        free(stream_node);
    }

    publish_connection_terminal(connection);
    if (!connection_shutdown_complete && connection_handle != NULL && connection->base.handle == connection_handle) {
        adapter->api->ConnectionShutdown(connection_handle, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, application_error_code);
    }
    if (release_queue_ref) {
        adapter_object_unpin(&connection->base);
    }
}

static void drain_detached_connections(msquic_provider* adapter, adapter_pending_connection* nodes) {
    while (nodes != NULL) {
        adapter_pending_connection* node = nodes;
        nodes = node->next;
        if (node->connection != NULL) {
            drain_detached_connection(adapter, node->connection, node->release_queue_ref, 0);
        }
        free(node);
    }
}

static void discard_detached_connections(msquic_provider* adapter, adapter_pending_connection* nodes) {
    while (nodes != NULL) {
        adapter_pending_connection* node = nodes;
        adapter_connection* connection = node->connection;
        nodes = node->next;
        if (connection != NULL) {
            pthread_mutex_lock(&adapter->mutex);
            adapter_pending_stream* streams = detach_pending_streams_locked(adapter, connection);
            if (node->release_queue_ref && connection->base.active_operations > 0) {
                connection->base.active_operations--;
            }
            if (connection->admission_granted) {
                connection->admission_granted = false;
                if (adapter->connection_budget_used > 0) {
                    adapter->connection_budget_used--;
                }
            }
            for (adapter_pending_stream* stream_node = streams; stream_node != NULL; stream_node = stream_node->next) {
                adapter_stream* stream = stream_node->stream;
                if (stream != NULL) {
                    if (stream_node->release_queue_ref && stream->base.active_operations > 0) {
                        stream->base.active_operations--;
                    }
                    if (stream->stream_admission_granted) {
                        stream->stream_admission_granted = false;
                        if (adapter->stream_budget_used > 0) {
                            adapter->stream_budget_used--;
                        }
                    }
                }
            }
            pthread_mutex_unlock(&adapter->mutex);
            while (streams != NULL) {
                adapter_pending_stream* stream_node = streams;
                streams = stream_node->next;
                free(stream_node);
            }
        }
        free(node);
    }
}

static void complete_connection_handshake(adapter_connection* connection) {
    msquic_provider* adapter = connection->base.adapter;
    bool release_queue_ref = false;
    pthread_mutex_lock(&adapter->mutex);
    if (connection->handshake_in_flight) {
        connection->handshake_in_flight = false;
        if (adapter->connection_handshakes_in_flight > 0) {
            adapter->connection_handshakes_in_flight--;
        }
    }
    if (!connection->on_accept_queue) {
        release_queue_ref = connection->accept_queue_ref_held;
        connection->accept_queue_ref_held = false;
    }
    pthread_mutex_unlock(&adapter->mutex);
    if (release_queue_ref) {
        adapter_object_unpin(&connection->base);
    }
    adapter_schedule(adapter);
}

static void adapter_scheduler_drain(msquic_provider* adapter) {
    for (;;) {
        adapter_connection* connection = NULL;
        adapter_pending_connection* pending_node = NULL;
        HQUIC handle = NULL;
        bool release_queue_ref = false;
        bool configure = false;

        pthread_mutex_lock(&adapter->mutex);
        pending_node = adapter->pending_connection_head;
        if (pending_node == NULL) {
            pthread_mutex_unlock(&adapter->mutex);
            break;
        }
        connection = pending_node->connection;
        if (adapter->state == TREVRPC_ENGINE_STATE_RUNNING && adapter->connection_handshakes_in_flight == 0 &&
            connection != NULL && !connection->base.terminal_published &&
            !atomic_load_explicit(&connection->base.closing, memory_order_acquire) && connection->base.handle != NULL) {
            pending_node = remove_connection_queue_locked(adapter, connection, &release_queue_ref);
            connection->accept_queue_ref_held = release_queue_ref;
            connection->handshake_in_flight = true;
            adapter->connection_handshakes_in_flight++;
            handle = connection->base.handle;
            configure = true;
        } else if (adapter->state == TREVRPC_ENGINE_STATE_RUNNING && adapter->connection_handshakes_in_flight != 0) {
            pthread_mutex_unlock(&adapter->mutex);
            break;
        } else {
            pending_node = remove_connection_queue_locked(adapter, connection, &release_queue_ref);
            if (connection != NULL) {
                atomic_store_explicit(&connection->base.closing, true, memory_order_release);
                if (!connection->base.ready) {
                    connection->base.cancel_requested = true;
                }
                handle = connection->base.handle;
            }
        }
        pthread_mutex_unlock(&adapter->mutex);

        free(pending_node);
        if (!configure) {
            if (handle != NULL) {
                adapter->api->ConnectionShutdown(handle, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
            }
            if (release_queue_ref && connection != NULL) {
                adapter_object_unpin(&connection->base);
            }
            continue;
        }

        QUIC_STATUS status = adapter->api->ConnectionSetConfiguration(handle, connection->base.endpoint->configuration);
        if (QUIC_FAILED(status)) {
            pthread_mutex_lock(&adapter->mutex);
            connection->transport_error = (uint64_t)(uint32_t)status;
            atomic_store_explicit(&connection->base.closing, true, memory_order_release);
            pthread_mutex_unlock(&adapter->mutex);
            adapter->api->ConnectionShutdown(handle, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
        }
        complete_connection_handshake(connection);
        return;
    }

    for (uint32_t slot = adapter->connection_begin; slot < adapter->stream_begin; slot++) {
        adapter_connection* connection = NULL;
        pthread_mutex_lock(&adapter->mutex);
        adapter_object* object = adapter->slots[slot].object;
        if (adapter->state == TREVRPC_ENGINE_STATE_RUNNING && object != NULL &&
            object->kind == TREVRPC_ENGINE_OBJECT_CONNECTION) {
            connection = (adapter_connection*)object;
            if (!connection->base.ready || connection->base.terminal_published ||
                atomic_load_explicit(&connection->base.closing, memory_order_acquire) ||
                connection->pending_peer_stream_head == NULL) {
                connection = NULL;
            } else {
                connection->base.active_operations++;
            }
        }
        pthread_mutex_unlock(&adapter->mutex);
        if (connection != NULL) {
            promote_peer_streams(connection);
            adapter_object_unpin(&connection->base);
        }
    }

    if (atomic_load_explicit(&adapter->readable_retry_count, memory_order_relaxed) != 0) {
        for (uint32_t slot = adapter->stream_begin; slot < adapter->slot_count; ++slot) {
            adapter_stream* stream = NULL;
            pthread_mutex_lock(&adapter->mutex);
            adapter_object* object = adapter->slots[slot].object;
            if (adapter->state == TREVRPC_ENGINE_STATE_RUNNING && object != NULL &&
                object->kind == TREVRPC_ENGINE_OBJECT_STREAM && !object->terminal_published &&
                !atomic_load_explicit(&object->closing, memory_order_acquire)) {
                stream = (adapter_stream*)object;
                pthread_mutex_lock(&stream->mutex);
                bool retry = stream->readable_retry_pending && stream->readable_pending &&
                             stream->readable_published_epoch == 0 && stream->receive_head != NULL;
                pthread_mutex_unlock(&stream->mutex);
                if (retry) {
                    object->active_operations++;
                } else {
                    stream = NULL;
                }
            }
            pthread_mutex_unlock(&adapter->mutex);
            if (stream != NULL) {
                int result = publish_stream_readable(stream, false);
                if (result != 0) {
                    adapter_fail_stop(adapter, result, "receive readiness retry failed");
                }
                adapter_object_unpin(&stream->base);
            }
        }
    }

    for (;;) {
        adapter_stream* stream = NULL;
        adapter_send* send = NULL;
        HQUIC handle = NULL;
        pthread_mutex_lock(&adapter->mutex);
        stream = adapter->send_ready_head;
        if (stream != NULL) {
            adapter->send_ready_head = stream->send_scheduler_next;
            if (adapter->send_ready_head == NULL)
                adapter->send_ready_tail = NULL;
            stream->send_scheduler_next = NULL;
            stream->send_scheduler_queued = false;
            stream->base.active_operations++;
            pthread_mutex_lock(&stream->mutex);
            if (adapter->state == TREVRPC_ENGINE_STATE_RUNNING && stream->base.handle != NULL &&
                !stream->base.terminal_published && !stream->base.shutdown_complete && !stream->send_aborted &&
                !atomic_load_explicit(&stream->base.closing, memory_order_acquire)) {
                for (adapter_send* pending = stream->pending_sends; pending != NULL; pending = pending->next) {
                    if (!pending->submitted) {
                        send = pending;
                        pending->submitted = true;
                        handle = stream->base.handle;
                        break;
                    }
                }
                if (send != NULL) {
                    for (adapter_send* pending = send->next; pending != NULL; pending = pending->next) {
                        if (!pending->submitted) {
                            queue_stream_locked(adapter, stream);
                            break;
                        }
                    }
                }
            }
            pthread_mutex_unlock(&stream->mutex);
        }
        pthread_mutex_unlock(&adapter->mutex);
        if (stream == NULL)
            break;
        if (send == NULL) {
            cancel_unsent_sends(stream, -ECANCELED);
            adapter_object_unpin(&stream->base);
            continue;
        }

        int pin_result = trevrpc_engine_provider_operation_pin(adapter->engine);
        if (pin_result != 0) {
            bool complete = false;
            pthread_mutex_lock(&stream->send_gate);
            if (!atomic_exchange_explicit(&send->completed, true, memory_order_acq_rel)) {
                complete = send_operation_release(send);
            }
            pthread_mutex_unlock(&stream->send_gate);
            if (complete) {
                (void)publish_send_completion(send, pin_result);
            }
            if (stream_shutdown_drained(stream))
                publish_stream_terminal(stream, false, 0);
            adapter_object_unpin(&stream->base);
            continue;
        }

        QUIC_STATUS status = adapter->api->StreamSend == NULL
                                 ? QUIC_STATUS_ABORTED
                                 : adapter->api->StreamSend(handle, &send->buffer, 1, QUIC_SEND_FLAG_NONE, send);
        if (QUIC_FAILED(status)) {
            bool complete = false;
            pthread_mutex_lock(&stream->send_gate);
            if (!atomic_exchange_explicit(&send->completed, true, memory_order_acq_rel)) {
                complete = send_operation_release(send);
            }
            pthread_mutex_unlock(&stream->send_gate);
            if (complete) {
                (void)publish_send_completion(send, -EIO);
            }
            trevrpc_engine_provider_operation_unpin(adapter->engine);
            if (stream_shutdown_drained(stream))
                publish_stream_terminal(stream, false, 0);
        }
        adapter_object_unpin(&stream->base);
    }
}

static bool reserve_peer_stream_admission(msquic_provider* adapter, adapter_connection* connection) {
    pthread_mutex_lock(&adapter->mutex);
    bool available = adapter->state == TREVRPC_ENGINE_STATE_RUNNING && !connection->base.terminal_published &&
                     !atomic_load_explicit(&connection->base.closing, memory_order_acquire) &&
                     adapter->stream_budget_used < adapter->stream_capacity &&
                     adapter->pending_stream_count < adapter->stream_capacity &&
                     connection->pending_peer_stream_count < adapter->stream_capacity;
    if (available) {
        adapter->stream_budget_used++;
        adapter->pending_stream_count++;
        connection->pending_peer_stream_count++;
    }
    pthread_mutex_unlock(&adapter->mutex);
    return available;
}

static void release_peer_stream_pending(msquic_provider* adapter, adapter_connection* connection) {
    pthread_mutex_lock(&adapter->mutex);
    if (adapter->pending_stream_count > 0) {
        adapter->pending_stream_count--;
    }
    if (connection->pending_peer_stream_count > 0) {
        connection->pending_peer_stream_count--;
    }
    pthread_mutex_unlock(&adapter->mutex);
}

static void release_stream_budget(msquic_provider* adapter) {
    pthread_mutex_lock(&adapter->mutex);
    if (adapter->stream_budget_used > 0) {
        adapter->stream_budget_used--;
    }
    pthread_mutex_unlock(&adapter->mutex);
    adapter_schedule(adapter);
}

static adapter_pending_stream* remove_peer_stream_queue_locked(
    msquic_provider* adapter, adapter_connection* connection, adapter_stream* stream, bool* release_queue_ref) {
    adapter_pending_stream* node = stream->pending_peer_node;
    if (release_queue_ref != NULL) {
        *release_queue_ref = stream->peer_queue_ref_held;
    }
    stream->peer_queue_ref_held = false;
    stream->pending_peer_node = NULL;
    stream->on_peer_queue = false;
    if (node == NULL) {
        return NULL;
    }
    adapter_pending_stream** link = &connection->pending_peer_stream_head;
    while (*link != NULL && *link != node) {
        link = &(*link)->next;
    }
    if (*link != node) {
        return NULL;
    }
    *link = node->next;
    if (connection->pending_peer_stream_tail == node) {
        connection->pending_peer_stream_tail = NULL;
        for (adapter_pending_stream* pending = connection->pending_peer_stream_head; pending != NULL;
            pending = pending->next) {
            connection->pending_peer_stream_tail = pending;
        }
    }
    if (connection->pending_peer_stream_count > 0) {
        connection->pending_peer_stream_count--;
    }
    if (adapter->pending_stream_count > 0) {
        adapter->pending_stream_count--;
    }
    return node;
}

static void promote_peer_streams(adapter_connection* connection) {
    msquic_provider* adapter = connection->base.adapter;
    for (;;) {
        adapter_stream* stream = NULL;
        adapter_pending_stream* pending_node = NULL;
        bool release_queue_ref = false;
        pthread_mutex_lock(&adapter->mutex);
        if (adapter->state != TREVRPC_ENGINE_STATE_RUNNING || !connection->base.ready ||
            connection->base.terminal_published ||
            atomic_load_explicit(&connection->base.closing, memory_order_acquire)) {
            pthread_mutex_unlock(&adapter->mutex);
            return;
        }
        pending_node = connection->pending_peer_stream_head;
        if (pending_node == NULL || pending_node->stream == NULL) {
            pthread_mutex_unlock(&adapter->mutex);
            return;
        }
        stream = pending_node->stream;
        if (stream->base.handle == NULL) {
            pthread_mutex_unlock(&adapter->mutex);
            return;
        }
        pending_node = remove_peer_stream_queue_locked(adapter, connection, stream, &release_queue_ref);
        pthread_mutex_unlock(&adapter->mutex);
        free(pending_node);

        int ready_result = publish_stream_ready(stream);
        if (ready_result != 0) {
            /* Keep the native handle before terminal publication may take ownership of it. */
            HQUIC stream_handle = stream->base.handle;
            publish_stream_terminal(stream, true, ready_result == -ECANCELED ? -ECANCELED : ready_result);
            if (ready_result != -ECANCELED) {
                adapter_fail_stop(adapter, ready_result, "peer stream readiness publication failed");
            }
            if (stream_handle != NULL) {
                adapter->api->StreamShutdown(stream_handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
            }
        }
        if (release_queue_ref) {
            adapter_object_unpin(&stream->base);
        }
    }
}

static int adapter_adopt_accepted_connection(msquic_provider* adapter,
    adapter_listener* listener,
    adapter_endpoint* endpoint,
    trevrpc_engine_handle_v1 parent,
    HQUIC handle,
    trevrpc_msquic_accepted_connection* accepted,
    trevrpc_engine_handle_v1* out_connection) {
    adapter_connection* connection;
    adapter_pending_connection* node;
    trevrpc_engine_handle_v1 token;
    int add_result;
    if (!reserve_connection_admission(adapter, listener))
        return -ECONNREFUSED;
    connection = calloc(1, sizeof(*connection));
    node = connection == NULL ? NULL : calloc(1, sizeof(*node));
    if (connection == NULL || node == NULL) {
        free(node);
        free(connection);
        release_connection_admission(adapter);
        return -ENOMEM;
    }
    atomic_init(&connection->base.closing, false);
    connection->base.endpoint = endpoint;
    endpoint_retain(endpoint);
    connection->max_pending_send_count = limit_product(endpoint->max_pending_send_count, adapter->stream_capacity);
    connection->max_pending_send_bytes = limit_product(endpoint->max_pending_send_bytes, adapter->stream_capacity);
    connection->base.event_flags = TREVRPC_ENGINE_EVENT_FLAG_SERVER;
    connection->parent = parent;
    connection->admission_granted = true;
    add_result = trevrpc_engine_provider_reserve_mandatory(adapter->engine, &connection->base.ready_reservation);
    if (add_result == 0)
        add_result = trevrpc_engine_provider_reserve_mandatory(adapter->engine, &connection->base.terminal_reservation);
    if (add_result == 0)
        add_result = registry_add(adapter, &connection->base, TREVRPC_ENGINE_OBJECT_CONNECTION, &token);
    if (add_result != 0) {
        trevrpc_engine_provider_cancel_reservation(adapter->engine, connection->base.terminal_reservation);
        trevrpc_engine_provider_cancel_reservation(adapter->engine, connection->base.ready_reservation);
        endpoint_free(connection->base.endpoint);
        free(node);
        free(connection);
        release_connection_admission(adapter);
        return add_result;
    }

    node->connection = connection;
    pthread_mutex_lock(&adapter->mutex);
    if (!adapter->accept_admission_open || adapter->state != TREVRPC_ENGINE_STATE_RUNNING ||
        (listener != NULL && atomic_load_explicit(&listener->base.closing, memory_order_acquire))) {
        pthread_mutex_unlock(&adapter->mutex);
        connection->base.handle = NULL;
        registry_remove_unstarted(&connection->base);
        free(node);
        return -ECONNREFUSED;
    }
    if (accepted != NULL && trevrpc_msquic_accepted_connection_claim_raw(accepted) != 0) {
        pthread_mutex_unlock(&adapter->mutex);
        connection->base.handle = NULL;
        registry_remove_unstarted(&connection->base);
        free(node);
        return -EIO;
    }
    node->next = NULL;
    if (adapter->pending_connection_tail != NULL)
        adapter->pending_connection_tail->next = node;
    else
        adapter->pending_connection_head = node;
    adapter->pending_connection_tail = node;
    adapter->pending_connection_count++;
    connection->pending_accept_node = node;
    connection->on_accept_queue = true;
    connection->accept_queue_ref_held = true;
    connection->base.active_operations += 2;
    connection->construction_ref_held = true;
    pthread_mutex_unlock(&adapter->mutex);

    adapter->api->SetCallbackHandler(handle, (void*)adapter_connection_callback, connection);
    bool close_requested = object_publish_handle(&connection->base, handle);
    if (close_requested)
        adapter->api->ConnectionShutdown(handle, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
    else
        adapter_schedule(adapter);
    pthread_mutex_lock(&adapter->mutex);
    bool release_construction = connection->construction_ref_held;
    connection->construction_ref_held = false;
    pthread_mutex_unlock(&adapter->mutex);
    if (release_construction)
        adapter_object_unpin(&connection->base);
    if (out_connection != NULL)
        *out_connection = token;
    return 0;
}

static QUIC_STATUS QUIC_API adapter_listener_callback(HQUIC handle, void* context, QUIC_LISTENER_EVENT* event) {
    (void)handle;
    adapter_listener* listener = context;
    msquic_provider* adapter = listener->base.adapter;
    if (!callback_enter(adapter, &listener->base)) {
        return QUIC_STATUS_ABORTED;
    }
    QUIC_STATUS result = QUIC_STATUS_SUCCESS;
    switch (event->Type) {
    case QUIC_LISTENER_EVENT_NEW_CONNECTION: {
        int adopt_result = adapter_adopt_accepted_connection(adapter,
            listener,
            listener->base.endpoint,
            listener->base.token,
            event->NEW_CONNECTION.Connection,
            NULL,
            NULL);
        result = adopt_result == 0 ? QUIC_STATUS_SUCCESS : normalize_accept_failure(adopt_result);
        break;
    }
    case QUIC_LISTENER_EVENT_STOP_COMPLETE: {
        pthread_mutex_lock(&adapter->mutex);
        listener->base.shutdown_complete = true;
        adapter_pending_connection* pending = detach_pending_connections_locked(adapter, listener->base.token, true);
        pthread_mutex_unlock(&adapter->mutex);
        drain_detached_connections(adapter, pending);
        publish_listener_terminal(listener);
        break;
    }
    default:
        break;
    }
    callback_leave(adapter, &listener->base);
    return result;
}

static QUIC_STATUS QUIC_API adapter_connection_callback(HQUIC handle, void* context, QUIC_CONNECTION_EVENT* event) {
    adapter_connection* connection = context;
    msquic_provider* adapter = connection->base.adapter;
    if (!callback_enter(adapter, &connection->base)) {
        complete_connection_handshake(connection);
        return QUIC_STATUS_ABORTED;
    }
    QUIC_STATUS result = QUIC_STATUS_SUCCESS;
    switch (event->Type) {
    case QUIC_CONNECTION_EVENT_CONNECTED: {
        complete_connection_handshake(connection);
        pthread_mutex_lock(&adapter->mutex);
        bool canceled = connection->base.cancel_requested && !connection->base.ready;
        pthread_mutex_unlock(&adapter->mutex);
        if (canceled) {
            adapter->api->ConnectionShutdown(handle, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
        } else {
            const uint8_t* alpn = connection->base.endpoint->alpn;
            size_t alpn_len = connection->base.endpoint->alpn_len;
            publish_connection_ready(connection, alpn, alpn_len);
        }
        break;
    }
    case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED: {
        if (!reserve_peer_stream_admission(adapter, connection)) {
            result = QUIC_STATUS_ABORTED;
            break;
        }
        adapter_pending_stream* node = calloc(1, sizeof(*node));
        if (node == NULL) {
            release_stream_budget(adapter);
            release_peer_stream_pending(adapter, connection);
            result = QUIC_STATUS_OUT_OF_MEMORY;
            break;
        }
        trevrpc_engine_handle_v1 token;
        int allocation_result = 0;
        adapter_stream* stream = stream_alloc(connection,
            NULL,
            0,
            connection->base.event_flags | TREVRPC_ENGINE_EVENT_FLAG_PEER,
            true,
            NULL,
            NULL,
            &token,
            &allocation_result);
        if (stream == NULL) {
            release_stream_budget(adapter);
            release_peer_stream_pending(adapter, connection);
            free(node);
            result = normalize_accept_failure(allocation_result);
            break;
        }
        node->stream = stream;
        pthread_mutex_lock(&adapter->mutex);
        stream->base.active_operations++;
        stream->construction_ref_held = true;
        node->next = NULL;
        if (connection->pending_peer_stream_tail != NULL) {
            connection->pending_peer_stream_tail->next = node;
        } else {
            connection->pending_peer_stream_head = node;
        }
        connection->pending_peer_stream_tail = node;
        stream->pending_peer_node = node;
        stream->on_peer_queue = true;
        pthread_mutex_unlock(&adapter->mutex);

#ifdef TREVRPC_ENGINE_MSQUIC_TESTING
        if (AdapterTestPromoteBeforePeerHandlePublication) {
            AdapterTestPromoteBeforePeerHandlePublication = false;
            promote_peer_streams(connection);
            pthread_mutex_lock(&adapter->mutex);
            AdapterTestPeerStreamStayedQueued = stream->on_peer_queue && stream->pending_peer_node == node &&
                                                !stream->base.terminal_published && stream->base.handle == NULL;
            pthread_mutex_unlock(&adapter->mutex);
        }
#endif

        adapter->api->SetCallbackHandler(event->PEER_STREAM_STARTED.Stream, (void*)adapter_stream_callback, stream);
        bool close_requested = object_publish_handle(&stream->base, event->PEER_STREAM_STARTED.Stream);
        if (close_requested) {
            adapter->api->StreamShutdown(event->PEER_STREAM_STARTED.Stream, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
        } else {
            adapter_schedule(adapter);
        }
        pthread_mutex_lock(&adapter->mutex);
        bool release_construction = stream->construction_ref_held;
        stream->construction_ref_held = false;
        pthread_mutex_unlock(&adapter->mutex);
        if (release_construction)
            adapter_object_unpin(&stream->base);
        break;
    }
    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_TRANSPORT:
        pthread_mutex_lock(&adapter->mutex);
        connection->transport_error = (uint64_t)(uint32_t)event->SHUTDOWN_INITIATED_BY_TRANSPORT.Status;
        pthread_mutex_unlock(&adapter->mutex);
        complete_connection_handshake(connection);
        break;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_INITIATED_BY_PEER:
        pthread_mutex_lock(&adapter->mutex);
        connection->application_error = event->SHUTDOWN_INITIATED_BY_PEER.ErrorCode;
        pthread_mutex_unlock(&adapter->mutex);
        complete_connection_handshake(connection);
        break;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE: {
        complete_connection_handshake(connection);
        pthread_mutex_lock(&adapter->mutex);
        connection->base.shutdown_complete = true;
        bool no_children = connection->live_streams == 0;
        bool has_pending_streams = connection->pending_peer_stream_head != NULL;
        pthread_mutex_unlock(&adapter->mutex);
        if (has_pending_streams) {
            drain_detached_connection(adapter, connection, false, 0);
        } else if (no_children) {
            publish_connection_terminal(connection);
        }
        break;
    }
    default:
        break;
    }
    complete_connection_handshake(connection);
    callback_leave(adapter, &connection->base);
    return result;
}

static QUIC_STATUS QUIC_API adapter_stream_callback(HQUIC handle, void* context, QUIC_STREAM_EVENT* event) {
    adapter_stream* stream = context;
    msquic_provider* adapter = stream->base.adapter;
    if (!callback_enter(adapter, &stream->base)) {
        bool abort_stream = false;
        pthread_mutex_lock(&adapter->mutex);
        if (event->Type != QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE && stream->base.live_counted &&
            !stream->base.terminal_published && !stream->base.shutdown_complete && stream->base.handle == handle &&
            !atomic_exchange_explicit(&stream->base.closing, true, memory_order_acq_rel)) {
            abort_stream = true;
        }
        pthread_mutex_unlock(&adapter->mutex);
        if (abort_stream) {
            adapter->api->StreamShutdown(handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
        }
        return QUIC_STATUS_SUCCESS;
    }
    switch (event->Type) {
    case QUIC_STREAM_EVENT_START_COMPLETE: {
        bool failed = QUIC_FAILED(event->START_COMPLETE.Status);
        if (failed) {
            publish_stream_terminal(stream, true, -EIO);
        } else {
            int ready_result = publish_stream_ready(stream);
            if (ready_result != 0) {
                publish_stream_terminal(stream, true, ready_result == -ECANCELED ? -ECANCELED : ready_result);
                if (ready_result != -ECANCELED) {
                    adapter_fail_stop(adapter, ready_result, "local stream readiness publication failed");
                }
                adapter->api->StreamShutdown(handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
            }
        }
        break;
    }
    case QUIC_STREAM_EVENT_RECEIVE: {
        bool publish = false;
        int result = 0;
        uint64_t accepted_total = 0;
        pthread_mutex_lock(&stream->mutex);
        if (stream->receive_aborted) {
            pthread_mutex_unlock(&stream->mutex);
            break;
        }
        for (uint32_t index = 0; index < event->RECEIVE.BufferCount && result == 0; index++) {
            size_t accepted = 0;
            result = stream_consume_receive(stream,
                event->RECEIVE.Buffers[index].Buffer,
                event->RECEIVE.Buffers[index].Length,
                &publish,
                &accepted);
            accepted_total += accepted;
        }
        bool fin = (event->RECEIVE.Flags & QUIC_RECEIVE_FLAG_FIN) != 0;
        bool clean = !fin || trevrpc_frame_parser_finish(&stream->parser) == TREVRPC_FRAME_CLEAN_EOF;
        bool parser_backpressured = result == -ENOSPC;
        if (parser_backpressured && accepted_total < event->RECEIVE.TotalBufferLength) {
            stream->receive_paused = true;
        }
        pthread_mutex_unlock(&stream->mutex);
        if (publish) {
            int publish_result = publish_stream_readable(stream, true);
            if (publish_result != 0) {
                result = publish_result;
                parser_backpressured = false;
            }
        }
        if (parser_backpressured) {
            bool fin_consumed = fin && accepted_total == event->RECEIVE.TotalBufferLength;
            event->RECEIVE.TotalBufferLength = accepted_total;
            if (fin_consumed) {
                publish_receive_fin(stream, clean ? TREVRPC_ENGINE_EVENT_FLAG_CLEAN_FIN : 0, clean ? 0 : -EPROTO);
                if (!clean) {
                    adapter->api->StreamShutdown(handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
                }
            }
        } else if (result != 0) {
            adapter_fail_stop(adapter, result, "receive processing failed");
            adapter->api->StreamShutdown(handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
        } else if (fin) {
            if (clean) {
                publish_receive_fin(stream, TREVRPC_ENGINE_EVENT_FLAG_CLEAN_FIN, 0);
            } else {
                publish_receive_fin(stream, 0, -EPROTO);
                adapter->api->StreamShutdown(handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, 0);
            }
        }
        break;
    }
    case QUIC_STREAM_EVENT_SEND_COMPLETE: {
        adapter_send* send = event->SEND_COMPLETE.ClientContext;
        if (send == NULL) {
            break;
        }
        int status = event->SEND_COMPLETE.Canceled != FALSE ? -ECANCELED : 0;
        pthread_mutex_lock(&stream->send_gate);
        if (atomic_exchange_explicit(&send->completed, true, memory_order_acq_rel)) {
            pthread_mutex_unlock(&stream->send_gate);
            break;
        }
        (void)send_operation_release(send);
        pthread_mutex_unlock(&stream->send_gate);
        (void)publish_send_completion(send, status);
        trevrpc_engine_provider_operation_unpin(adapter->engine);
        if (stream_shutdown_drained(stream)) {
            publish_stream_terminal(stream, false, 0);
        }
        break;
    }
    case QUIC_STREAM_EVENT_PEER_SEND_SHUTDOWN:
        publish_receive_fin(stream, TREVRPC_ENGINE_EVENT_FLAG_CLEAN_FIN, 0);
        break;
    case QUIC_STREAM_EVENT_PEER_SEND_ABORTED:
        pthread_mutex_lock(&adapter->mutex);
        stream->application_error = event->PEER_SEND_ABORTED.ErrorCode;
        pthread_mutex_unlock(&adapter->mutex);
        publish_receive_fin(stream, TREVRPC_ENGINE_EVENT_FLAG_PEER_RESET, -ECANCELED);
        break;
    case QUIC_STREAM_EVENT_PEER_RECEIVE_ABORTED:
        pthread_mutex_lock(&adapter->mutex);
        stream->application_error = event->PEER_RECEIVE_ABORTED.ErrorCode;
        pthread_mutex_unlock(&adapter->mutex);
        pthread_mutex_lock(&stream->mutex);
        stream->send_aborted = true;
        pthread_mutex_unlock(&stream->mutex);
        cancel_unsent_sends(stream, -ECANCELED);
        break;
    case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE: {
        pthread_mutex_lock(&adapter->mutex);
        stream->base.shutdown_complete = true;
        pthread_mutex_unlock(&adapter->mutex);
        cancel_unsent_sends(stream, -ECANCELED);
        if (stream_shutdown_drained(stream)) {
            publish_stream_terminal(stream, false, 0);
        }
        break;
    }
    default:
        break;
    }
    callback_leave(adapter, &stream->base);
    return QUIC_STATUS_SUCCESS;
}

static bool discard_stream_receives(adapter_stream* stream, bool* released_receive_credit) {
    *released_receive_credit = false;
    pthread_mutex_lock(&stream->mutex);
    if (stream->receive_aborted) {
        pthread_mutex_unlock(&stream->mutex);
        return false;
    }
    stream->receive_aborted = true;
    stream->receive_paused = false;
    stream->readable_pending = false;
    stream->readable_published_epoch = 0;
    stream->readable_epoch++;
    set_readable_retry_locked(stream, false);
    while (stream->receive_head != NULL) {
        adapter_receive_node* node = stream->receive_head;
        stream->receive_head = node->next;
        *released_receive_credit |= release_receive_credit(stream, node->receive->len);
        free(node->receive);
        free(node);
    }
    stream->receive_tail = NULL;
    *released_receive_credit |= stream->parser_receive_allocation != NULL;
    trevrpc_frame_parser_reset(&stream->parser);
    pthread_mutex_unlock(&stream->mutex);
    return true;
}

static int provider_stream_finish_send(msquic_provider* adapter, trevrpc_engine_handle_v1 stream_handle) {
    if (adapter == NULL) {
        return -EINVAL;
    }
    adapter_object* object = NULL;
    int result = registry_get(adapter, stream_handle, TREVRPC_ENGINE_OBJECT_STREAM, &object, true);
    if (result == -EPIPE) {
        return 0;
    }
    if (result != 0) {
        return result;
    }
    ADAPTER_OBJECT_SCOPE(object);
    adapter_stream* stream = (adapter_stream*)object;
    pthread_mutex_lock(&stream->mutex);
    if (stream->send_aborted) {
        pthread_mutex_unlock(&stream->mutex);
        return -EPIPE;
    }
    bool invoke = !stream->send_finished;
    stream->send_finished = true;
    pthread_mutex_unlock(&stream->mutex);
    if (!invoke) {
        return 0;
    }
    QUIC_STATUS status = adapter->api->StreamShutdown(object->handle, QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL, 0);
    if (QUIC_FAILED(status)) {
        pthread_mutex_lock(&stream->mutex);
        if (!atomic_load_explicit(&stream->base.closing, memory_order_acquire)) {
            stream->send_finished = false;
        }
        pthread_mutex_unlock(&stream->mutex);
        return -EIO;
    }
    return 0;
}

static int provider_stream_abort_receive(
    msquic_provider* adapter, trevrpc_engine_handle_v1 stream_handle, uint64_t application_error_code) {
    if (adapter == NULL) {
        return -EINVAL;
    }
    adapter_object* object = NULL;
    int result = registry_get(adapter, stream_handle, TREVRPC_ENGINE_OBJECT_STREAM, &object, true);
    if (result == -EPIPE) {
        return 0;
    }
    if (result != 0) {
        return result;
    }
    ADAPTER_OBJECT_SCOPE(object);
    adapter_stream* stream = (adapter_stream*)object;
    bool released_receive_credit = false;
    bool invoke = discard_stream_receives(stream, &released_receive_credit);
    if (released_receive_credit) {
        resume_paused_receives(adapter);
    }
    if (!invoke || object->handle == NULL) {
        return 0;
    }
    QUIC_STATUS status =
        adapter->api->StreamShutdown(object->handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE, application_error_code);
    return QUIC_FAILED(status) ? -EIO : 0;
}

static int provider_stream_abort_send(
    msquic_provider* adapter, trevrpc_engine_handle_v1 stream_handle, uint64_t application_error_code) {
    if (adapter == NULL) {
        return -EINVAL;
    }
    adapter_object* object = NULL;
    int result = registry_get(adapter, stream_handle, TREVRPC_ENGINE_OBJECT_STREAM, &object, true);
    if (result == -EPIPE) {
        return 0;
    }
    if (result != 0) {
        return result;
    }
    ADAPTER_OBJECT_SCOPE(object);
    adapter_stream* stream = (adapter_stream*)object;
    pthread_mutex_lock(&stream->mutex);
    bool invoke = !stream->send_aborted;
    stream->send_aborted = true;
    HQUIC handle = object->handle;
    pthread_mutex_unlock(&stream->mutex);
    cancel_unsent_sends(stream, -ECANCELED);
    if (!invoke || handle == NULL) {
        return 0;
    }
    QUIC_STATUS status =
        adapter->api->StreamShutdown(handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND, application_error_code);
    return QUIC_FAILED(status) ? -EIO : 0;
}

static int provider_stream_abort(
    msquic_provider* adapter, trevrpc_engine_handle_v1 stream_handle, uint64_t application_error_code) {
    if (adapter == NULL) {
        return -EINVAL;
    }
    adapter_object* object = NULL;
    int result = registry_get(adapter, stream_handle, TREVRPC_ENGINE_OBJECT_STREAM, &object, false);
    if (result == -EPIPE) {
        return 0;
    }
    if (result != 0) {
        return result;
    }
    ADAPTER_OBJECT_SCOPE(object);
    adapter_stream* stream = (adapter_stream*)object;
    adapter_pending_stream* pending = NULL;
    bool release_queue_ref = false;
    pthread_mutex_lock(&adapter->mutex);
    bool invoke = !atomic_exchange_explicit(&object->closing, true, memory_order_acq_rel);
    stream->application_error = application_error_code;
    HQUIC handle = object->handle;
    adapter_connection* connection = NULL;
    if (stream->parent.slot < adapter->slot_count) {
        adapter_slot* parent_slot = &adapter->slots[stream->parent.slot];
        if (parent_slot->object != NULL && parent_slot->kind == TREVRPC_ENGINE_OBJECT_CONNECTION &&
            parent_slot->generation == stream->parent.generation) {
            connection = (adapter_connection*)parent_slot->object;
        }
    }
    if (connection != NULL && stream->on_peer_queue) {
        pending = remove_peer_stream_queue_locked(adapter, connection, stream, &release_queue_ref);
    }
    if (pending != NULL && handle == NULL) {
        stream->base.shutdown_complete = true;
    }
    pthread_mutex_unlock(&adapter->mutex);
    cancel_unsent_sends(stream, -ECANCELED);
    if (pending != NULL) {
        free(pending);
        publish_stream_terminal(stream, true, -ECANCELED);
        if (invoke && handle != NULL && stream->base.handle == handle) {
            adapter->api->StreamShutdown(handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, application_error_code);
        }
        if (release_queue_ref) {
            adapter_object_unpin(&stream->base);
        }
    } else if (invoke && handle != NULL) {
        adapter->api->StreamShutdown(handle, QUIC_STREAM_SHUTDOWN_FLAG_ABORT, application_error_code);
    }
    return 0;
}

static int provider_stream_close(msquic_provider* adapter, trevrpc_engine_handle_v1 stream_handle) {
    return provider_stream_abort(adapter, stream_handle, 0);
}

static int provider_connection_close(
    msquic_provider* adapter, trevrpc_engine_handle_v1 connection_handle, uint64_t application_error_code) {
    if (adapter == NULL) {
        return -EINVAL;
    }
    adapter_object* object = NULL;
    int result = registry_get(adapter, connection_handle, TREVRPC_ENGINE_OBJECT_CONNECTION, &object, false);
    if (result == -EPIPE) {
        return 0;
    }
    if (result != 0) {
        return result;
    }
    ADAPTER_OBJECT_SCOPE(object);
    adapter_connection* connection = (adapter_connection*)object;
    adapter_pending_connection* pending = NULL;
    bool release_queue_ref = false;
    pthread_mutex_lock(&adapter->mutex);
    bool invoke = !atomic_exchange_explicit(&object->closing, true, memory_order_acq_rel);
    if (!object->ready) {
        object->cancel_requested = true;
    }
    connection->application_error = application_error_code;
    HQUIC handle = object->handle;
    pending = remove_connection_queue_locked(adapter, connection, &release_queue_ref);
    bool drain_children = pending != NULL || release_queue_ref || connection->pending_peer_stream_head != NULL;
    if (pending != NULL) {
        pending->release_queue_ref = release_queue_ref;
    }
    pthread_mutex_unlock(&adapter->mutex);
    if (drain_children) {
        drain_detached_connection(adapter, connection, release_queue_ref, application_error_code);
        free(pending);
    } else if (invoke && handle != NULL) {
        adapter->api->ConnectionShutdown(handle, QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, application_error_code);
    }
    return 0;
}

static int provider_listener_close(msquic_provider* adapter, trevrpc_engine_handle_v1 listener_handle) {
    if (adapter == NULL) {
        return -EINVAL;
    }
    adapter_object* object = NULL;
    int result = registry_get(adapter, listener_handle, TREVRPC_ENGINE_OBJECT_LISTENER, &object, false);
    if (result == -EPIPE) {
        return 0;
    }
    if (result != 0) {
        return result;
    }
    ADAPTER_OBJECT_SCOPE(object);
    adapter_pending_connection* pending = NULL;
    pthread_mutex_lock(&adapter->mutex);
    bool invoke = !atomic_exchange_explicit(&object->closing, true, memory_order_acq_rel);
    HQUIC handle = object->handle;
    pending = detach_pending_connections_locked(adapter, object->token, true);
    pthread_mutex_unlock(&adapter->mutex);
    if (invoke && handle != NULL) {
        adapter->api->ListenerStop(handle);
    }
    drain_detached_connections(adapter, pending);
    return 0;
}

static int provider_close(msquic_provider* adapter) {
    if (adapter == NULL) {
        return -EINVAL;
    }
    adapter_pending_connection* pending = NULL;
    pthread_mutex_lock(&adapter->mutex);
    if (adapter->state == TREVRPC_ENGINE_STATE_RUNNING) {
        adapter->state = TREVRPC_ENGINE_STATE_STOPPING;
        adapter->accept_admission_open = false;
    }
    if (adapter->close_initiated || adapter->state >= TREVRPC_ENGINE_STATE_STOPPED) {
        pthread_mutex_unlock(&adapter->mutex);
        return 0;
    }
    adapter->close_initiated = true;
    pending = detach_pending_connections_locked(adapter, (trevrpc_engine_handle_v1){0}, false);
    pthread_mutex_unlock(&adapter->mutex);
    drain_detached_connections(adapter, pending);
    adapter_schedule(adapter);

    for (uint32_t index = 0; index < adapter->slot_count; index++) {
        pthread_mutex_lock(&adapter->mutex);
        adapter_object* object = adapter->slots[index].object;
        uint32_t kind = object != NULL ? object->kind : 0;
        trevrpc_engine_handle_v1 token = object != NULL ? object->token : (trevrpc_engine_handle_v1){0};
        pthread_mutex_unlock(&adapter->mutex);
        if (object == NULL) {
            continue;
        }
        if (kind == TREVRPC_ENGINE_OBJECT_LISTENER) {
            (void)provider_listener_close(adapter, token);
        } else if (kind == TREVRPC_ENGINE_OBJECT_STREAM) {
            (void)provider_stream_close(adapter, token);
        }
    }
    for (uint32_t index = adapter->connection_begin; index < adapter->stream_begin; index++) {
        pthread_mutex_lock(&adapter->mutex);
        adapter_object* object = adapter->slots[index].object;
        trevrpc_engine_handle_v1 token = object != NULL ? object->token : (trevrpc_engine_handle_v1){0};
        pthread_mutex_unlock(&adapter->mutex);
        if (object != NULL) {
            (void)provider_connection_close(adapter, token, 0);
        }
    }
    adapter_maybe_stopped(adapter);
    return 0;
}

static void provider_get_diagnostics(msquic_provider* adapter, trevrpc_engine_provider_diagnostics* diagnostics) {
    pthread_mutex_lock(&adapter->mutex);
    diagnostics->pending_send_bytes = adapter->pending_send_bytes;
    diagnostics->pending_send_count = adapter->pending_send_count;
    diagnostics->live_listeners = adapter->live_listeners;
    diagnostics->live_connections = adapter->live_connections;
    diagnostics->live_streams = adapter->live_streams;
    pthread_mutex_unlock(&adapter->mutex);
    pthread_mutex_lock(&adapter->budget_mutex);
    diagnostics->receive_owned_count = adapter->receive_owned_count;
    diagnostics->peak_receive_owned_count = adapter->peak_receive_owned_count;
    diagnostics->receive_owned_bytes = adapter->receive_owned_bytes;
    diagnostics->peak_receive_owned_bytes = adapter->peak_receive_owned_bytes;
    pthread_mutex_unlock(&adapter->budget_mutex);
}

static void adapter_object_free(adapter_object* object) {
    if (object == NULL) {
        return;
    }
    msquic_provider* adapter = object->adapter;
    adapter_pending_connection* pending_connection = NULL;
    adapter_pending_stream* pending_streams = NULL;
    bool schedule = false;
    pthread_mutex_lock(&adapter->mutex);
    if (object->kind == TREVRPC_ENGINE_OBJECT_CONNECTION) {
        adapter_connection* connection = (adapter_connection*)object;
        bool release_queue_ref = false;
        pending_connection = remove_connection_queue_locked(adapter, connection, &release_queue_ref);
        if (pending_connection != NULL) {
            pending_connection->release_queue_ref = release_queue_ref;
        }
        pending_streams = detach_pending_streams_locked(adapter, connection);
        if (release_queue_ref && object->active_operations > 0) {
            object->active_operations--;
        }
        if (connection->admission_granted) {
            connection->admission_granted = false;
            if (adapter->connection_budget_used > 0) {
                adapter->connection_budget_used--;
            }
            schedule = true;
        }
        for (adapter_pending_stream* node = pending_streams; node != NULL; node = node->next) {
            if (node->release_queue_ref != 0 && node->stream != NULL && node->stream->base.active_operations > 0) {
                node->stream->base.active_operations--;
            }
            if (node->stream != NULL && node->stream->stream_admission_granted) {
                node->stream->stream_admission_granted = false;
                if (adapter->stream_budget_used > 0) {
                    adapter->stream_budget_used--;
                }
                schedule = true;
            }
        }
    } else if (object->kind == TREVRPC_ENGINE_OBJECT_STREAM) {
        adapter_stream* stream = (adapter_stream*)object;
        unqueue_stream_locked(adapter, stream);
        if (stream->on_peer_queue || stream->pending_peer_node != NULL) {
            adapter_connection* connection = NULL;
            if (stream->parent.slot < adapter->slot_count) {
                adapter_slot* parent_slot = &adapter->slots[stream->parent.slot];
                if (parent_slot->object != NULL && parent_slot->kind == TREVRPC_ENGINE_OBJECT_CONNECTION &&
                    parent_slot->generation == stream->parent.generation) {
                    connection = (adapter_connection*)parent_slot->object;
                }
            }
            if (connection != NULL) {
                bool release_queue_ref = false;
                adapter_pending_stream* node =
                    remove_peer_stream_queue_locked(adapter, connection, stream, &release_queue_ref);
                free(node);
                if (release_queue_ref && object->active_operations > 0) {
                    object->active_operations--;
                }
            } else if (stream->peer_queue_ref_held && object->active_operations > 0) {
                stream->peer_queue_ref_held = false;
                object->active_operations--;
            }
            stream->on_peer_queue = false;
            stream->pending_peer_node = NULL;
        }
        if (stream->stream_admission_granted) {
            stream->stream_admission_granted = false;
            if (adapter->stream_budget_used > 0) {
                adapter->stream_budget_used--;
            }
            schedule = true;
        }
    }
    pthread_mutex_unlock(&adapter->mutex);
    free(pending_connection);
    while (pending_streams != NULL) {
        adapter_pending_stream* node = pending_streams;
        pending_streams = node->next;
        free(node);
    }
    if (schedule) {
        adapter_schedule(adapter);
    }
    trevrpc_engine_provider_cancel_reservation(adapter->engine, object->ready_reservation);
    trevrpc_engine_provider_cancel_reservation(adapter->engine, object->terminal_reservation);
    if (object->kind == TREVRPC_ENGINE_OBJECT_STREAM) {
        adapter_stream* stream = (adapter_stream*)object;
        if (stream->base.handle != NULL) {
            adapter->api->StreamClose(stream->base.handle);
        }
        bool released_receive_credit = false;
        pthread_mutex_lock(&stream->mutex);
        set_readable_retry_locked(stream, false);
        while (stream->receive_head != NULL) {
            adapter_receive_node* node = stream->receive_head;
            stream->receive_head = node->next;
            released_receive_credit |= release_receive_credit(stream, node->receive->len);
            free(node->receive);
            free(node);
        }
        while (stream->pending_sends != NULL) {
            adapter_send* send = stream->pending_sends;
            stream->pending_sends = send->next;
            trevrpc_engine_provider_cancel_reservation(adapter->engine, send->completion_reservation);
            free(send->data);
            free(send);
        }
        trevrpc_engine_provider_cancel_reservation(adapter->engine, stream->receive_fin_reservation);
        released_receive_credit |= stream->parser_receive_allocation != NULL;
        trevrpc_frame_parser_reset(&stream->parser);
        free(stream->pending_operation_ids);
        stream->pending_operation_ids = NULL;
        pthread_mutex_unlock(&stream->mutex);
        if (released_receive_credit) {
            resume_paused_receives(adapter);
        }
        pthread_mutex_destroy(&stream->send_gate);
        pthread_mutex_destroy(&stream->mutex);
    } else if (object->kind == TREVRPC_ENGINE_OBJECT_CONNECTION) {
        if (object->handle != NULL) {
            adapter->api->ConnectionClose(object->handle);
        }
    } else if (object->kind == TREVRPC_ENGINE_OBJECT_LISTENER && object->handle != NULL) {
        adapter->api->ListenerClose(object->handle);
    }
    endpoint_free(object->endpoint);
    free(object);
}

static void provider_destroy(msquic_provider* adapter) {
    if (adapter == NULL) {
        return;
    }
    pthread_mutex_lock(&adapter->mutex);
    adapter_pending_connection* pending =
        detach_pending_connections_locked(adapter, (trevrpc_engine_handle_v1){0}, false);
    pthread_mutex_unlock(&adapter->mutex);
    discard_detached_connections(adapter, pending);
    if (adapter->scheduler_condition_initialized) {
        pthread_mutex_lock(&adapter->mutex);
        adapter->scheduler_stop = true;
        pthread_cond_signal(&adapter->scheduler_condition);
        pthread_mutex_unlock(&adapter->mutex);
        if (adapter->scheduler_thread_started) {
            (void)pthread_join(adapter->scheduler_thread, NULL);
            adapter->scheduler_thread_started = false;
        }
    }
    pthread_mutex_lock(&adapter->mutex);
    adapter_object* objects = adapter->retired_objects;
    adapter->retired_objects = NULL;
    for (uint32_t index = 0; index < adapter->slot_count; index++) {
        adapter_object* object = adapter->slots[index].object;
        if (object != NULL) {
            adapter->slots[index].object = NULL;
            object->retired_next = objects;
            objects = object;
        }
    }
    pthread_mutex_unlock(&adapter->mutex);

    while (objects != NULL) {
        adapter_object* object = objects;
        objects = object->retired_next;
        adapter_object_free(object);
    }
    const QUIC_API_TABLE* api = adapter->api;
    free(adapter->slots);
    if (adapter->scheduler_condition_initialized) {
        pthread_cond_destroy(&adapter->scheduler_condition);
    }
    pthread_mutex_destroy(&adapter->budget_mutex);
    pthread_mutex_destroy(&adapter->mutex);
    free(adapter);
    trevrpc_msquic_api_owner_release(api);
}

int trevrpc_engine_msquic_adopt_accepted_connection_v1(trevrpc_engine* engine,
    const trevrpc_engine_endpoint_config_v1* config,
    trevrpc_msquic_accepted_connection* accepted,
    trevrpc_engine_handle_v1* out_connection) {
    msquic_provider* adapter;
    adapter_endpoint* endpoint = NULL;
    void* connection_handle = NULL;
    void* registration = NULL;
    void* configuration = NULL;
    int result;
    if (engine == NULL || config == NULL || accepted == NULL || out_connection == NULL)
        return -EINVAL;
    adapter = trevrpc_engine_provider_context(engine);
    if (adapter == NULL)
        return -EINVAL;
    result = trevrpc_engine_provider_callback_enter(engine);
    if (result != 0)
        return result;
    result = trevrpc_msquic_accepted_connection_get_native(accepted, &connection_handle, &registration, &configuration);
    if (result == 0)
        result = endpoint_create_borrowed(adapter, config, accepted, registration, configuration, &endpoint);
    if (result == 0)
        result = adapter_adopt_accepted_connection(
            adapter, NULL, endpoint, (trevrpc_engine_handle_v1){0}, connection_handle, accepted, out_connection);
    endpoint_free(endpoint);
    trevrpc_engine_provider_callback_leave(engine);
    return result;
}

static int ops_listen(void* context,
    const trevrpc_engine_endpoint_config_v1* config,
    trevrpc_engine_reservation* terminal,
    trevrpc_engine_handle_v1* out_listener) {
    return provider_listen(context, config, terminal, out_listener);
}

static int ops_listener_get_port(void* context, trevrpc_engine_handle_v1 listener, uint16_t* out_port) {
    return provider_listener_get_port(context, listener, out_port);
}

static int ops_dial(void* context,
    const trevrpc_engine_endpoint_config_v1* config,
    uint64_t operation_id,
    trevrpc_engine_reservation* completion,
    trevrpc_engine_reservation* terminal,
    trevrpc_engine_handle_v1* out_connection) {
    return provider_dial(context, config, operation_id, completion, terminal, out_connection);
}

static int ops_dial_cancel(void* context, trevrpc_engine_handle_v1 connection) {
    return provider_dial_cancel(context, connection);
}

static int ops_open_stream(void* context,
    trevrpc_engine_handle_v1 connection,
    uint64_t operation_id,
    trevrpc_engine_reservation* completion,
    trevrpc_engine_reservation* terminal,
    trevrpc_engine_handle_v1* out_stream) {
    return provider_connection_open_bidi_stream(context, connection, operation_id, completion, terminal, out_stream);
}

static int ops_send(void* context,
    trevrpc_engine_handle_v1 stream,
    uint64_t operation_id,
    const uint8_t* body,
    size_t body_len,
    trevrpc_engine_reservation* completion) {
    return provider_stream_send_frame(context, stream, operation_id, body, body_len, completion);
}

static int ops_receive(void* context, trevrpc_engine_handle_v1 stream, trevrpc_engine_receive** out_receive) {
    return provider_stream_receive_frame(context, stream, out_receive);
}

static int ops_finish_send(void* context, trevrpc_engine_handle_v1 stream) {
    return provider_stream_finish_send(context, stream);
}

static int ops_stream_abort_receive(void* context, trevrpc_engine_handle_v1 stream, uint64_t error_code) {
    return provider_stream_abort_receive(context, stream, error_code);
}

static int ops_stream_abort_send(void* context, trevrpc_engine_handle_v1 stream, uint64_t error_code) {
    return provider_stream_abort_send(context, stream, error_code);
}

static int ops_stream_abort(void* context, trevrpc_engine_handle_v1 stream, uint64_t error_code) {
    return provider_stream_abort(context, stream, error_code);
}

static int ops_stream_close(void* context, trevrpc_engine_handle_v1 stream) {
    return provider_stream_close(context, stream);
}

static int ops_connection_close(void* context, trevrpc_engine_handle_v1 connection, uint64_t error_code) {
    return provider_connection_close(context, connection, error_code);
}

static int ops_listener_close(void* context, trevrpc_engine_handle_v1 listener) {
    return provider_listener_close(context, listener);
}

static int ops_close(void* context) {
    return provider_close(context);
}

static void ops_get_diagnostics(void* context, trevrpc_engine_provider_diagnostics* diagnostics) {
    provider_get_diagnostics(context, diagnostics);
}

static void ops_destroy(void* context) {
    provider_destroy(context);
}

static const trevrpc_engine_provider_ops MsQuicProviderOps = {
    .attach = provider_attach,
    .listen = ops_listen,
    .listener_get_port = ops_listener_get_port,
    .dial = ops_dial,
    .dial_cancel = ops_dial_cancel,
    .connection_open_bidi_stream = ops_open_stream,
    .stream_send_frame = ops_send,
    .stream_receive_frame = ops_receive,
    .stream_finish_send = ops_finish_send,
    .stream_abort_receive = ops_stream_abort_receive,
    .stream_abort_send = ops_stream_abort_send,
    .stream_abort = ops_stream_abort,
    .stream_close = ops_stream_close,
    .connection_close = ops_connection_close,
    .listener_close = ops_listener_close,
    .close = ops_close,
    .get_diagnostics = ops_get_diagnostics,
    .destroy = ops_destroy,
};

int trevrpc_engine_msquic_create_v1(const trevrpc_engine_config_v1* engine_config,
    const trevrpc_engine_msquic_config_v1* provider_config,
    trevrpc_engine** out_engine) {
    if (out_engine == NULL) {
        return -EINVAL;
    }
    int result = validate_provider_config(provider_config);
    if (result != 0) {
        return result;
    }
    if (engine_config == NULL || engine_config->struct_size < sizeof(*engine_config) ||
        engine_config->struct_version != TREVRPC_ENGINE_STRUCT_VERSION_1 || engine_config->listener_capacity == 0 ||
        engine_config->connection_capacity == 0 || engine_config->stream_capacity == 0 ||
        engine_config->max_receive_owned_count == 0 || engine_config->max_receive_owned_bytes == 0) {
        return -EINVAL;
    }
    uint64_t slot_count64 = (uint64_t)engine_config->listener_capacity + engine_config->connection_capacity +
                            engine_config->stream_capacity;
    if (slot_count64 >= UINT32_MAX) {
        return -EOVERFLOW;
    }
    const QUIC_API_TABLE* api = NULL;
    result = trevrpc_msquic_api_owner_acquire(&api);
    if (result != 0) {
        return result;
    }
    msquic_provider* adapter = calloc(1, sizeof(*adapter));
    if (adapter == NULL) {
        trevrpc_msquic_api_owner_release(api);
        return -ENOMEM;
    }
    adapter->slots = calloc((size_t)slot_count64 + 1u, sizeof(*adapter->slots));
    if (adapter->slots == NULL) {
        free(adapter);
        trevrpc_msquic_api_owner_release(api);
        return -ENOMEM;
    }
    int pthread_result = pthread_mutex_init(&adapter->mutex, NULL);
    if (pthread_result != 0) {
        free(adapter->slots);
        free(adapter);
        trevrpc_msquic_api_owner_release(api);
        return -pthread_result;
    }
    pthread_result = pthread_mutex_init(&adapter->budget_mutex, NULL);
    if (pthread_result != 0) {
        pthread_mutex_destroy(&adapter->mutex);
        free(adapter->slots);
        free(adapter);
        trevrpc_msquic_api_owner_release(api);
        return -pthread_result;
    }
    pthread_result = pthread_cond_init(&adapter->scheduler_condition, NULL);
    if (pthread_result != 0) {
        pthread_mutex_destroy(&adapter->budget_mutex);
        pthread_mutex_destroy(&adapter->mutex);
        free(adapter->slots);
        free(adapter);
        trevrpc_msquic_api_owner_release(api);
        return -pthread_result;
    }
    adapter->scheduler_condition_initialized = true;
    atomic_init(&adapter->readable_retry_count, 0);
    adapter->api = api;
    adapter->state = TREVRPC_ENGINE_STATE_RUNNING;
    adapter->slot_count = (uint32_t)slot_count64 + 1u;
    adapter->listener_begin = 1u;
    adapter->connection_begin = 1u + engine_config->listener_capacity;
    adapter->stream_begin = 1u + engine_config->listener_capacity + engine_config->connection_capacity;
    adapter->connection_capacity = engine_config->connection_capacity;
    adapter->stream_capacity = engine_config->stream_capacity;
    adapter->accept_admission_open = true;
    adapter->max_receive_owned_count = engine_config->max_receive_owned_count;
    adapter->max_receive_owned_bytes = engine_config->max_receive_owned_bytes;
    pthread_mutex_lock(&AdapterOwnerMutex);
    adapter->owner = AdapterNextOwner++;
    if (adapter->owner == 0) {
        adapter->owner = AdapterNextOwner++;
    }
    pthread_mutex_unlock(&AdapterOwnerMutex);
    pthread_result = pthread_create(&adapter->scheduler_thread, NULL, adapter_scheduler_main, adapter);
    if (pthread_result != 0) {
        pthread_cond_destroy(&adapter->scheduler_condition);
        pthread_mutex_destroy(&adapter->budget_mutex);
        pthread_mutex_destroy(&adapter->mutex);
        free(adapter->slots);
        free(adapter);
        trevrpc_msquic_api_owner_release(api);
        return -pthread_result;
    }
    pthread_mutex_lock(&adapter->mutex);
    adapter->scheduler_thread_started = true;
    if (adapter->scheduler_requested) {
        pthread_cond_signal(&adapter->scheduler_condition);
    }
    pthread_mutex_unlock(&adapter->mutex);
    return trevrpc_engine_provider_create_v1(engine_config, &MsQuicProviderOps, adapter, adapter->owner, out_engine);
}
