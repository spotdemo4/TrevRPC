#include "trevrpc_webtransport.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <wtf.h>

/* libwtf draft-15 currently regresses stream credit on sequential streams. */
#define TREV_WT_DRAFT WTF_WEBTRANSPORT_DRAFT_07

typedef struct trevrpc_wt_chunk trevrpc_wt_chunk;
typedef struct trevrpc_wt_stream_node trevrpc_wt_stream_node;
typedef struct trevrpc_wt_session_node trevrpc_wt_session_node;

struct trevrpc_wt_chunk {
    trevrpc_wt_chunk* next;
    size_t len;
    size_t offset;
    uint8_t data[];
};

struct trevrpc_wt_stream_node {
    trevrpc_wt_stream_node* next;
    trevrpc_wt_stream* stream;
};

struct trevrpc_wt_session_node {
    trevrpc_wt_session_node* next;
    trevrpc_wt_session* session;
};

struct trevrpc_wt_stream {
    wtf_stream_t* handle;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_wt_chunk* recv_head;
    trevrpc_wt_chunk* recv_tail;
    bool recv_fin;
    bool send_closed;
    bool closed;
    int err;
};

struct trevrpc_wt_session {
    wtf_session_t* handle;
    wtf_client_t* owner_client;
    wtf_context_t* owner_context;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_wt_stream_node* stream_head;
    trevrpc_wt_stream_node* stream_tail;
    bool connected;
    bool closed;
    int err;
};

struct trevrpc_wt_listener {
    wtf_context_t* context;
    wtf_server_t* server;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_wt_session_node* session_head;
    trevrpc_wt_session_node* session_tail;
    char* path;
    char* origin;
    bool closed;
    int err;
};

typedef struct trevrpc_wt_client_wait {
    trevrpc_wt_session* session;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool done;
    int err;
} trevrpc_wt_client_wait;

static void trevrpc_wt_session_callback(const wtf_session_event_t* event);
static void trevrpc_wt_client_wait_callback(const wtf_session_event_t* event);
static void trevrpc_wt_client_session_callback(const wtf_session_event_t* event);
static void trevrpc_wt_stream_callback(const wtf_stream_event_t* event);

static void trevrpc_wt_log_callback(
    wtf_log_level_t level,
    const char* component,
    const char* file,
    int line,
    const char* message,
    void* user_context) {
    (void)user_context;
    fprintf(stderr, "libwtf[%d] %s %s:%d: %s\n", (int)level, component, file, line, message);
}

static void trevrpc_wt_apply_log_config(wtf_context_config_t* config) {
    const char* log = getenv("TREV_WT_LOG");
    if (log != NULL && log[0] != '\0') {
        config->log_level = WTF_LOG_LEVEL_DEBUG;
        config->log_callback = trevrpc_wt_log_callback;
    } else {
        config->log_level = WTF_LOG_LEVEL_NONE;
    }
}

static bool trevrpc_wt_stream_is_peer_opened(wtf_stream_t* handle, trevrpc_wt_session* session) {
    return handle != NULL && session != NULL && wtf_stream_get_context(handle) == session;
}

static int trevrpc_wt_result(wtf_result_t result) {
    switch (result) {
    case WTF_SUCCESS:
        return 0;
    case WTF_ERROR_INVALID_PARAMETER:
        return -EINVAL;
    case WTF_ERROR_OUT_OF_MEMORY:
        return -ENOMEM;
    case WTF_ERROR_REJECTED:
        return TREV_WT_ERR_REJECTED;
    case WTF_ERROR_CONNECTION_ABORTED:
    case WTF_ERROR_STREAM_ABORTED:
    case WTF_ERROR_INVALID_STATE:
        return TREV_WT_ERR_CLOSED;
    default:
        return -(4000 + (int)result);
    }
}

static char* trevrpc_wt_strdup(const char* value) {
    if (value == NULL) {
        return NULL;
    }
    size_t len = strlen(value);
    char* copy = malloc(len + 1);
    if (copy == NULL) {
        return NULL;
    }
    memcpy(copy, value, len + 1);
    return copy;
}

static trevrpc_wt_stream* trevrpc_wt_stream_alloc(wtf_stream_t* handle) {
    trevrpc_wt_stream* stream = calloc(1, sizeof(*stream));
    if (stream == NULL) {
        return NULL;
    }

    stream->handle = handle;
    pthread_mutex_init(&stream->mutex, NULL);
    pthread_cond_init(&stream->cond, NULL);
    return stream;
}

static trevrpc_wt_session* trevrpc_wt_session_alloc(wtf_session_t* handle) {
    trevrpc_wt_session* session = calloc(1, sizeof(*session));
    if (session == NULL) {
        return NULL;
    }

    session->handle = handle;
    pthread_mutex_init(&session->mutex, NULL);
    pthread_cond_init(&session->cond, NULL);
    return session;
}

static void trevrpc_wt_stream_free_chunks(trevrpc_wt_stream* stream) {
    trevrpc_wt_chunk* chunk = stream->recv_head;
    while (chunk != NULL) {
        trevrpc_wt_chunk* next = chunk->next;
        free(chunk);
        chunk = next;
    }
    stream->recv_head = NULL;
    stream->recv_tail = NULL;
}

static int trevrpc_wt_stream_queue_data(
    trevrpc_wt_stream* stream,
    const wtf_buffer_t* buffers,
    uint32_t buffer_count) {
    size_t total = 0;
    for (uint32_t i = 0; i < buffer_count; i++) {
        if (total > SIZE_MAX - buffers[i].length) {
            return -ENOMEM;
        }
        total += buffers[i].length;
    }
    if (total == 0) {
        return 0;
    }
    if (total > SIZE_MAX - sizeof(trevrpc_wt_chunk)) {
        return -ENOMEM;
    }

    trevrpc_wt_chunk* chunk = malloc(sizeof(*chunk) + total);
    if (chunk == NULL) {
        return -ENOMEM;
    }
    chunk->next = NULL;
    chunk->len = total;
    chunk->offset = 0;

    size_t offset = 0;
    for (uint32_t i = 0; i < buffer_count; i++) {
        memcpy(chunk->data + offset, buffers[i].data, buffers[i].length);
        offset += buffers[i].length;
    }

    pthread_mutex_lock(&stream->mutex);
    if (stream->recv_tail != NULL) {
        stream->recv_tail->next = chunk;
    } else {
        stream->recv_head = chunk;
    }
    stream->recv_tail = chunk;
    pthread_cond_broadcast(&stream->cond);
    pthread_mutex_unlock(&stream->mutex);
    return 0;
}

static int trevrpc_wt_session_queue_stream(trevrpc_wt_session* session, wtf_stream_t* handle) {
    wtf_stream_ref(handle);
    trevrpc_wt_stream* stream = trevrpc_wt_stream_alloc(handle);
    if (stream == NULL) {
        wtf_stream_unref(handle);
        return -ENOMEM;
    }
    wtf_stream_set_context(handle, stream);
    wtf_stream_set_callback(handle, trevrpc_wt_stream_callback);

    trevrpc_wt_stream_node* node = malloc(sizeof(*node));
    if (node == NULL) {
        trevrpc_wt_stream_close(stream);
        return -ENOMEM;
    }
    node->next = NULL;
    node->stream = stream;

    pthread_mutex_lock(&session->mutex);
    if (session->stream_tail != NULL) {
        session->stream_tail->next = node;
    } else {
        session->stream_head = node;
    }
    session->stream_tail = node;
    pthread_cond_broadcast(&session->cond);
    pthread_mutex_unlock(&session->mutex);
    return 0;
}

static void trevrpc_wt_listener_queue_session(trevrpc_wt_listener* listener, wtf_session_t* handle) {
    wtf_session_ref(handle);
    trevrpc_wt_session* session = trevrpc_wt_session_alloc(handle);
    if (session == NULL) {
        wtf_session_unref(handle);
        return;
    }
    session->connected = true;
    wtf_session_set_callback(handle, trevrpc_wt_session_callback, session);

    trevrpc_wt_session_node* node = malloc(sizeof(*node));
    if (node == NULL) {
        trevrpc_wt_session_close(session);
        return;
    }
    node->next = NULL;
    node->session = session;

    pthread_mutex_lock(&listener->mutex);
    if (listener->session_tail != NULL) {
        listener->session_tail->next = node;
    } else {
        listener->session_head = node;
    }
    listener->session_tail = node;
    pthread_cond_broadcast(&listener->cond);
    pthread_mutex_unlock(&listener->mutex);
}

static wtf_connection_decision_t trevrpc_wt_validate_connection(
    const wtf_connection_request_t* request,
    wtf_connection_response_t* response,
    void* user_context) {
    (void)response;
    trevrpc_wt_listener* listener = user_context;
    if (listener == NULL) {
        return WTF_CONNECTION_REJECT;
    }
    if (listener->path != NULL && request->path != NULL && strcmp(listener->path, request->path) != 0) {
        return WTF_CONNECTION_REJECT;
    }
    if (listener->path != NULL && request->path == NULL) {
        return WTF_CONNECTION_REJECT;
    }
    if (listener->origin != NULL && request->origin != NULL && strcmp(listener->origin, request->origin) != 0) {
        return WTF_CONNECTION_REJECT;
    }
    if (listener->origin != NULL && request->origin == NULL) {
        return WTF_CONNECTION_REJECT;
    }

    return WTF_CONNECTION_ACCEPT;
}

static void trevrpc_wt_session_callback(const wtf_session_event_t* event) {
    if (event == NULL) {
        return;
    }
    trevrpc_wt_session* session = NULL;
    switch (event->type) {
    case WTF_SESSION_EVENT_CONNECTED:
        trevrpc_wt_listener_queue_session(event->user_context, event->session);
        return;
    case WTF_SESSION_EVENT_STREAM_OPENED:
        session = event->user_context;
        if (event->stream_opened.stream_type == WTF_STREAM_BIDIRECTIONAL &&
            trevrpc_wt_stream_is_peer_opened(event->stream_opened.stream, session)) {
            (void)trevrpc_wt_session_queue_stream(session, event->stream_opened.stream);
        }
        return;
    case WTF_SESSION_EVENT_DISCONNECTED:
        session = event->user_context;
        if (session != NULL) {
            pthread_mutex_lock(&session->mutex);
            session->closed = true;
            session->err = TREV_WT_ERR_CLOSED;
            pthread_cond_broadcast(&session->cond);
            pthread_mutex_unlock(&session->mutex);
        }
        return;
    default:
        return;
    }
}

static void trevrpc_wt_client_wait_callback(const wtf_session_event_t* event) {
    if (event == NULL) {
        return;
    }
    trevrpc_wt_client_wait* wait = event->user_context;
    trevrpc_wt_session* session = wait == NULL ? NULL : wait->session;
    switch (event->type) {
    case WTF_SESSION_EVENT_CONNECTED:
        if (session != NULL) {
            pthread_mutex_lock(&session->mutex);
            session->connected = true;
            pthread_cond_broadcast(&session->cond);
            pthread_mutex_unlock(&session->mutex);
        }
        if (wait != NULL) {
            pthread_mutex_lock(&wait->mutex);
            wait->done = true;
            pthread_cond_broadcast(&wait->cond);
            pthread_mutex_unlock(&wait->mutex);
        }
        return;
    case WTF_SESSION_EVENT_STREAM_OPENED:
        if (event->stream_opened.stream_type == WTF_STREAM_BIDIRECTIONAL &&
            trevrpc_wt_stream_is_peer_opened(event->stream_opened.stream, session)) {
            (void)trevrpc_wt_session_queue_stream(session, event->stream_opened.stream);
        }
        return;
    case WTF_SESSION_EVENT_DISCONNECTED:
        if (session != NULL) {
            pthread_mutex_lock(&session->mutex);
            session->closed = true;
            session->err = TREV_WT_ERR_CLOSED;
            pthread_cond_broadcast(&session->cond);
            pthread_mutex_unlock(&session->mutex);
        }
        if (wait != NULL) {
            pthread_mutex_lock(&wait->mutex);
            wait->done = true;
            wait->err = TREV_WT_ERR_CLOSED;
            pthread_cond_broadcast(&wait->cond);
            pthread_mutex_unlock(&wait->mutex);
        }
        return;
    default:
        return;
    }
}

static void trevrpc_wt_client_session_callback(const wtf_session_event_t* event) {
    if (event == NULL) {
        return;
    }
    trevrpc_wt_session* session = event->user_context;
    switch (event->type) {
    case WTF_SESSION_EVENT_CONNECTED:
        if (session != NULL) {
            pthread_mutex_lock(&session->mutex);
            session->connected = true;
            pthread_cond_broadcast(&session->cond);
            pthread_mutex_unlock(&session->mutex);
        }
        return;
    case WTF_SESSION_EVENT_STREAM_OPENED:
        if (event->stream_opened.stream_type == WTF_STREAM_BIDIRECTIONAL &&
            trevrpc_wt_stream_is_peer_opened(event->stream_opened.stream, session)) {
            (void)trevrpc_wt_session_queue_stream(session, event->stream_opened.stream);
        }
        return;
    case WTF_SESSION_EVENT_DISCONNECTED:
        if (session != NULL) {
            pthread_mutex_lock(&session->mutex);
            session->closed = true;
            session->err = TREV_WT_ERR_CLOSED;
            pthread_cond_broadcast(&session->cond);
            pthread_mutex_unlock(&session->mutex);
        }
        return;
    default:
        return;
    }
}

static void trevrpc_wt_stream_callback(const wtf_stream_event_t* event) {
    if (event == NULL) {
        return;
    }
    trevrpc_wt_stream* stream = wtf_stream_get_context(event->stream);
    if (stream == NULL) {
        return;
    }

    switch (event->type) {
    case WTF_STREAM_EVENT_DATA_RECEIVED:
        if (trevrpc_wt_stream_queue_data(stream, event->data_received.buffers, event->data_received.buffer_count) != 0) {
            (void)wtf_stream_abort(event->stream, 1);
        }
        return;
    case WTF_STREAM_EVENT_PEER_CLOSED:
        pthread_mutex_lock(&stream->mutex);
        stream->recv_fin = true;
        pthread_cond_broadcast(&stream->cond);
        pthread_mutex_unlock(&stream->mutex);
        return;
    case WTF_STREAM_EVENT_ABORTED:
        pthread_mutex_lock(&stream->mutex);
        stream->err = TREV_WT_ERR_CLOSED;
        stream->closed = true;
        pthread_cond_broadcast(&stream->cond);
        pthread_mutex_unlock(&stream->mutex);
        return;
    case WTF_STREAM_EVENT_CLOSED:
        pthread_mutex_lock(&stream->mutex);
        stream->closed = true;
        pthread_cond_broadcast(&stream->cond);
        pthread_mutex_unlock(&stream->mutex);
        return;
    default:
        return;
    }
}

int trevrpc_wt_listen(const trevrpc_wt_config* config, trevrpc_wt_listener** out_listener) {
    if (config == NULL || out_listener == NULL || config->host == NULL || config->cert_file == NULL ||
        config->key_file == NULL) {
        return -EINVAL;
    }
    *out_listener = NULL;

    trevrpc_wt_listener* listener = calloc(1, sizeof(*listener));
    if (listener == NULL) {
        return -ENOMEM;
    }
    pthread_mutex_init(&listener->mutex, NULL);
    pthread_cond_init(&listener->cond, NULL);
    listener->path = trevrpc_wt_strdup(config->path != NULL ? config->path : "/trevrpc");
    listener->origin = trevrpc_wt_strdup(config->origin);
    if (listener->path == NULL || (config->origin != NULL && listener->origin == NULL)) {
        trevrpc_wt_listener_close(listener);
        return -ENOMEM;
    }

    wtf_context_config_t context_config = {0};
    context_config.execution_profile = WTF_EXECUTION_PROFILE_LOW_LATENCY;
    trevrpc_wt_apply_log_config(&context_config);
    int err = trevrpc_wt_result(wtf_context_create(&context_config, &listener->context));
    if (err != 0) {
        trevrpc_wt_listener_close(listener);
        return err;
    }

    wtf_certificate_config_t cert_config = {0};
    cert_config.cert_type = WTF_CERT_TYPE_FILE;
    cert_config.cert_data.file.cert_path = config->cert_file;
    cert_config.cert_data.file.key_path = config->key_file;
    cert_config.ca_cert_file = config->ca_cert_file;

    wtf_server_config_t server_config = {0};
    server_config.host = config->host;
    server_config.port = config->port;
    server_config.cert_config = &cert_config;
    server_config.draft = TREV_WT_DRAFT;
    server_config.max_sessions_per_connection = config->max_sessions_per_connection;
    server_config.max_streams_per_session = config->max_streams_per_session;
    server_config.max_data_per_session = config->max_data_per_session;
    server_config.stream_recv_window = config->stream_recv_window;
    server_config.conn_flow_control_window = config->conn_flow_control_window;
    server_config.idle_timeout_ms = config->idle_timeout_ms;
    server_config.handshake_timeout_ms = config->handshake_timeout_ms;
    server_config.send_buffering = WTF_SEND_BUFFERING_DISABLED;
    server_config.connection_validator = trevrpc_wt_validate_connection;
    server_config.session_callback = trevrpc_wt_session_callback;
    server_config.user_context = listener;

    err = trevrpc_wt_result(wtf_server_create(listener->context, &server_config, &listener->server));
    if (err != 0) {
        trevrpc_wt_listener_close(listener);
        return err;
    }
    err = trevrpc_wt_result(wtf_server_start(listener->server));
    if (err != 0) {
        trevrpc_wt_listener_close(listener);
        return err;
    }

    *out_listener = listener;
    return 0;
}

int trevrpc_wt_listener_accept_session(trevrpc_wt_listener* listener, trevrpc_wt_session** out_session) {
    if (listener == NULL || out_session == NULL) {
        return -EINVAL;
    }
    *out_session = NULL;

    pthread_mutex_lock(&listener->mutex);
    while (listener->session_head == NULL && !listener->closed && listener->err == 0) {
        pthread_cond_wait(&listener->cond, &listener->mutex);
    }
    if (listener->session_head == NULL) {
        int err = listener->err != 0 ? listener->err : TREV_WT_ERR_CLOSED;
        pthread_mutex_unlock(&listener->mutex);
        return err;
    }

    trevrpc_wt_session_node* node = listener->session_head;
    listener->session_head = node->next;
    if (listener->session_head == NULL) {
        listener->session_tail = NULL;
    }
    pthread_mutex_unlock(&listener->mutex);

    *out_session = node->session;
    free(node);
    return 0;
}

void trevrpc_wt_listener_shutdown(trevrpc_wt_listener* listener) {
    if (listener == NULL) {
        return;
    }

    pthread_mutex_lock(&listener->mutex);
    listener->closed = true;
    pthread_cond_broadcast(&listener->cond);
    pthread_mutex_unlock(&listener->mutex);
    if (listener->server != NULL) {
        (void)wtf_server_stop(listener->server);
    }
}

void trevrpc_wt_listener_close(trevrpc_wt_listener* listener) {
    if (listener == NULL) {
        return;
    }
    trevrpc_wt_listener_shutdown(listener);
    if (listener->server != NULL) {
        wtf_server_destroy(listener->server);
    }
    if (listener->context != NULL) {
        wtf_context_destroy(listener->context);
    }
    while (listener->session_head != NULL) {
        trevrpc_wt_session_node* node = listener->session_head;
        listener->session_head = node->next;
        trevrpc_wt_session_close(node->session);
        free(node);
    }
    free(listener->path);
    free(listener->origin);
    pthread_cond_destroy(&listener->cond);
    pthread_mutex_destroy(&listener->mutex);
    free(listener);
}

int trevrpc_wt_dial(const trevrpc_wt_config* config, trevrpc_wt_session** out_session) {
    if (config == NULL || out_session == NULL || config->url == NULL) {
        return -EINVAL;
    }
    *out_session = NULL;

    wtf_context_t* context = NULL;
    wtf_client_t* client = NULL;
    wtf_session_t* handle = NULL;

    wtf_context_config_t context_config = {0};
    context_config.execution_profile = WTF_EXECUTION_PROFILE_LOW_LATENCY;
    trevrpc_wt_apply_log_config(&context_config);
    int err = trevrpc_wt_result(wtf_context_create(&context_config, &context));
    if (err != 0) {
        return err;
    }

    trevrpc_wt_client_wait wait = {0};
    pthread_mutex_init(&wait.mutex, NULL);
    pthread_cond_init(&wait.cond, NULL);

    wtf_client_config_t client_config = {0};
    client_config.url = config->url;
    client_config.origin = config->origin;
    client_config.draft = TREV_WT_DRAFT;
    client_config.skip_certificate_validation = config->skip_certificate_validation != 0;
    client_config.ca_cert_file = config->ca_cert_file;
    client_config.max_sessions_per_connection = config->max_sessions_per_connection;
    client_config.max_streams_per_session = config->max_streams_per_session;
    client_config.max_data_per_session = config->max_data_per_session;
    client_config.stream_recv_window = config->stream_recv_window;
    client_config.conn_flow_control_window = config->conn_flow_control_window;
    client_config.idle_timeout_ms = config->idle_timeout_ms;
    client_config.handshake_timeout_ms = config->handshake_timeout_ms;
    client_config.send_buffering = WTF_SEND_BUFFERING_DISABLED;
    client_config.session_callback = trevrpc_wt_client_wait_callback;
    client_config.user_context = &wait;

    err = trevrpc_wt_result(wtf_client_create(context, &client_config, &client));
    if (err == 0) {
        err = trevrpc_wt_result(wtf_client_open(client, &handle));
    }
    if (err != 0) {
        if (handle != NULL) {
            wtf_session_unref(handle);
        }
        if (client != NULL) {
            wtf_client_destroy(client);
        }
        wtf_context_destroy(context);
        pthread_cond_destroy(&wait.cond);
        pthread_mutex_destroy(&wait.mutex);
        return err;
    }

    trevrpc_wt_session* session = trevrpc_wt_session_alloc(handle);
    if (session == NULL) {
        wtf_session_unref(handle);
        wtf_client_destroy(client);
        wtf_context_destroy(context);
        pthread_cond_destroy(&wait.cond);
        pthread_mutex_destroy(&wait.mutex);
        return -ENOMEM;
    }
    session->owner_client = client;
    session->owner_context = context;
    wait.session = session;
    wtf_session_set_callback(handle, trevrpc_wt_client_wait_callback, &wait);

    pthread_mutex_lock(&wait.mutex);
    while (!wait.done) {
        pthread_cond_wait(&wait.cond, &wait.mutex);
    }
    err = wait.err;
    pthread_mutex_unlock(&wait.mutex);
    pthread_cond_destroy(&wait.cond);
    pthread_mutex_destroy(&wait.mutex);
    wtf_session_set_callback(handle, trevrpc_wt_client_session_callback, session);

    if (err != 0) {
        trevrpc_wt_session_close(session);
        return err;
    }

    *out_session = session;
    return 0;
}

int trevrpc_wt_session_accept_stream(trevrpc_wt_session* session, trevrpc_wt_stream** out_stream) {
    if (session == NULL || out_stream == NULL) {
        return -EINVAL;
    }
    *out_stream = NULL;

    pthread_mutex_lock(&session->mutex);
    while (session->stream_head == NULL && !session->closed && session->err == 0) {
        pthread_cond_wait(&session->cond, &session->mutex);
    }
    if (session->stream_head == NULL) {
        int err = session->err != 0 ? session->err : TREV_WT_ERR_CLOSED;
        pthread_mutex_unlock(&session->mutex);
        return err;
    }

    trevrpc_wt_stream_node* node = session->stream_head;
    session->stream_head = node->next;
    if (session->stream_head == NULL) {
        session->stream_tail = NULL;
    }
    pthread_mutex_unlock(&session->mutex);

    *out_stream = node->stream;
    free(node);
    return 0;
}

int trevrpc_wt_session_open_stream(trevrpc_wt_session* session, trevrpc_wt_stream** out_stream) {
    if (session == NULL || out_stream == NULL) {
        return -EINVAL;
    }
    *out_stream = NULL;

    wtf_stream_t* handle = NULL;
    int err = trevrpc_wt_result(wtf_session_create_stream(session->handle, WTF_STREAM_BIDIRECTIONAL, &handle));
    if (err != 0) {
        return err;
    }
    trevrpc_wt_stream* stream = trevrpc_wt_stream_alloc(handle);
    if (stream == NULL) {
        wtf_stream_unref(handle);
        return -ENOMEM;
    }
    wtf_stream_set_context(handle, stream);
    wtf_stream_set_callback(handle, trevrpc_wt_stream_callback);
    *out_stream = stream;
    return 0;
}

void trevrpc_wt_session_close(trevrpc_wt_session* session) {
    if (session == NULL) {
        return;
    }
    if (session->handle != NULL) {
        (void)wtf_session_close(session->handle, 1, "closed");
    }
    while (session->stream_head != NULL) {
        trevrpc_wt_stream_node* node = session->stream_head;
        session->stream_head = node->next;
        trevrpc_wt_stream_close(node->stream);
        free(node);
    }
    if (session->handle != NULL) {
        wtf_session_unref(session->handle);
    }
    if (session->owner_client != NULL) {
        (void)wtf_client_disconnect(session->owner_client, 1, "closed");
        wtf_client_destroy(session->owner_client);
    }
    if (session->owner_context != NULL) {
        wtf_context_destroy(session->owner_context);
    }
    pthread_cond_destroy(&session->cond);
    pthread_mutex_destroy(&session->mutex);
    free(session);
}

intptr_t trevrpc_wt_stream_read(trevrpc_wt_stream* stream, uint8_t* data, size_t len) {
    if (stream == NULL || (data == NULL && len > 0)) {
        return -EINVAL;
    }
    if (len == 0) {
        return 0;
    }

    pthread_mutex_lock(&stream->mutex);
    while (stream->recv_head == NULL && !stream->recv_fin && !stream->closed && stream->err == 0) {
        pthread_cond_wait(&stream->cond, &stream->mutex);
    }
    if (stream->recv_head == NULL) {
        intptr_t result = stream->err != 0 ? stream->err : 0;
        pthread_mutex_unlock(&stream->mutex);
        return result;
    }

    trevrpc_wt_chunk* chunk = stream->recv_head;
    size_t available = chunk->len - chunk->offset;
    size_t copied = available < len ? available : len;
    memcpy(data, chunk->data + chunk->offset, copied);
    chunk->offset += copied;
    if (chunk->offset == chunk->len) {
        stream->recv_head = chunk->next;
        if (stream->recv_head == NULL) {
            stream->recv_tail = NULL;
        }
        free(chunk);
    }
    pthread_mutex_unlock(&stream->mutex);
    return (intptr_t)copied;
}

static intptr_t trevrpc_wt_stream_read_exact_locked(trevrpc_wt_stream* stream, uint8_t* data, size_t len) {
    size_t copied = 0;
    while (copied < len) {
        while (stream->recv_head == NULL && !stream->recv_fin && !stream->closed && stream->err == 0) {
            pthread_cond_wait(&stream->cond, &stream->mutex);
        }
        if (stream->recv_head == NULL) {
            if (stream->err != 0) {
                return stream->err;
            }
            return copied == 0 ? 0 : TREV_WT_ERR_CLOSED;
        }

        trevrpc_wt_chunk* chunk = stream->recv_head;
        size_t available = chunk->len - chunk->offset;
        size_t remaining = len - copied;
        size_t chunk_copied = available < remaining ? available : remaining;
        memcpy(data + copied, chunk->data + chunk->offset, chunk_copied);
        copied += chunk_copied;
        chunk->offset += chunk_copied;
        if (chunk->offset == chunk->len) {
            stream->recv_head = chunk->next;
            if (stream->recv_head == NULL) {
                stream->recv_tail = NULL;
            }
            free(chunk);
        }
    }
    return (intptr_t)copied;
}

intptr_t trevrpc_wt_stream_read_frame(
    trevrpc_wt_stream* stream,
    uint8_t** body,
    size_t* len,
    size_t max_len) {
    if (stream == NULL || body == NULL || len == NULL) {
        return -EINVAL;
    }
    *body = NULL;
    *len = 0;

    uint8_t header[4];
    pthread_mutex_lock(&stream->mutex);
    intptr_t read = trevrpc_wt_stream_read_exact_locked(stream, header, sizeof(header));
    if (read <= 0) {
        pthread_mutex_unlock(&stream->mutex);
        return read;
    }

    size_t body_len = ((size_t)header[0] << 24) |
                      ((size_t)header[1] << 16) |
                      ((size_t)header[2] << 8) |
                      (size_t)header[3];
    if (body_len > max_len) {
        *len = body_len;
        pthread_mutex_unlock(&stream->mutex);
        return TREV_WT_ERR_FRAME_TOO_LARGE;
    }
    if (body_len == 0) {
        pthread_mutex_unlock(&stream->mutex);
        return 1;
    }

    uint8_t* buffer = malloc(body_len);
    if (buffer == NULL) {
        pthread_mutex_unlock(&stream->mutex);
        return -ENOMEM;
    }
    read = trevrpc_wt_stream_read_exact_locked(stream, buffer, body_len);
    pthread_mutex_unlock(&stream->mutex);
    if (read <= 0) {
        free(buffer);
        return read == 0 ? TREV_WT_ERR_CLOSED : read;
    }

    *body = buffer;
    *len = body_len;
    return 1;
}

intptr_t trevrpc_wt_stream_write(trevrpc_wt_stream* stream, const uint8_t* data, size_t len) {
    if (stream == NULL || (data == NULL && len > 0)) {
        return -EINVAL;
    }
    if (len == 0) {
        return 0;
    }

    int err = trevrpc_wt_result(wtf_stream_send_copy(stream->handle, data, len, false));
    return err == 0 ? (intptr_t)len : err;
}

int trevrpc_wt_stream_shutdown_send(trevrpc_wt_stream* stream) {
    if (stream == NULL) {
        return -EINVAL;
    }
    pthread_mutex_lock(&stream->mutex);
    bool send_closed = stream->send_closed;
    stream->send_closed = true;
    pthread_mutex_unlock(&stream->mutex);
    if (send_closed) {
        return 0;
    }
    return trevrpc_wt_result(wtf_stream_close(stream->handle));
}

int trevrpc_wt_stream_abort(trevrpc_wt_stream* stream, uint32_t error_code) {
    if (stream == NULL) {
        return -EINVAL;
    }
    return trevrpc_wt_result(wtf_stream_abort(stream->handle, error_code));
}

void trevrpc_wt_stream_close(trevrpc_wt_stream* stream) {
    if (stream == NULL) {
        return;
    }
    if (stream->handle != NULL) {
        if (!stream->send_closed) {
            (void)wtf_stream_abort(stream->handle, 1);
        }
        wtf_stream_unref(stream->handle);
    }
    trevrpc_wt_stream_free_chunks(stream);
    pthread_cond_destroy(&stream->cond);
    pthread_mutex_destroy(&stream->mutex);
    free(stream);
}

void trevrpc_wt_free(void* ptr) {
    free(ptr);
}

const char* trevrpc_wt_error(int code) {
    switch (code) {
    case 0:
        return "ok";
    case TREV_WT_ERR_CLOSED:
        return "closed";
    case TREV_WT_ERR_FRAME_TOO_LARGE:
        return "frame too large";
    case TREV_WT_ERR_REJECTED:
        return "rejected";
    case -ENOMEM:
    case ENOMEM:
        return "out of memory";
    case -EINVAL:
    case EINVAL:
        return "invalid argument";
    default:
        return "WebTransport operation failed";
    }
}
