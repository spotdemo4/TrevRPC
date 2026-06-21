#include "trevrpc.h"

#include "trevrpc_msquic.h"
#include "trevrpc_wire.h"

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

typedef struct trevrpc_method trevrpc_method;
typedef struct trevrpc_server_conn_ref trevrpc_server_conn_ref;

struct trevrpc_client {
    trevrpc_msquic_conn* conn;
    size_t max_frame_size;
};

struct trevrpc_method {
    trevrpc_method* next;
    char* service;
    size_t service_len;
    char* method;
    size_t method_len;
    uint32_t kind;
    trevrpc_unary_handler handler;
    trevrpc_stream_handler stream_handler;
    void* user_data;
};

struct trevrpc_stream {
    trevrpc_msquic_stream* stream;
    size_t max_frame_size;
    bool owns_stream;
    bool sent_status;
};

struct trevrpc_server_conn_ref {
    trevrpc_server_conn_ref* next;
    trevrpc_msquic_conn* conn;
};

struct trevrpc_server {
    trevrpc_msquic_listener* listener;
    size_t max_frame_size;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_method* methods;
    trevrpc_server_conn_ref* conns;
    size_t active_tasks;
    bool shutting_down;
};

typedef struct trevrpc_conn_task {
    trevrpc_server* server;
    trevrpc_msquic_conn* conn;
} trevrpc_conn_task;

typedef struct trevrpc_stream_task {
    trevrpc_server* server;
    trevrpc_msquic_stream* stream;
} trevrpc_stream_task;

static size_t trevrpc_effective_max_frame_size(const trevrpc_config* config) {
    if (config != NULL && config->max_frame_size > 0) {
        return config->max_frame_size;
    }

    return TREVRPC_DEFAULT_MAX_FRAME_SIZE;
}

static trevrpc_msquic_config trevrpc_make_msquic_config(const trevrpc_config* config) {
    trevrpc_config defaults = trevrpc_default_config();
    if (config == NULL) {
        config = &defaults;
    }

    trevrpc_msquic_config msquic_config = {0};
    msquic_config.alpn = TREVRPC_ALPN;
    msquic_config.alpn_len = (uint32_t)(sizeof(TREVRPC_ALPN) - 1);
    msquic_config.cert_file = config->cert_file;
    msquic_config.key_file = config->key_file;
    msquic_config.max_idle_timeout_ms = config->max_idle_timeout_ms;
    msquic_config.keep_alive_ms = config->keep_alive_ms;
    msquic_config.peer_bidi_stream_count = config->peer_bidi_stream_count;
    msquic_config.max_stateless_operations = config->max_stateless_operations;
    msquic_config.max_binding_stateless_operations = config->max_binding_stateless_operations;
    return msquic_config;
}

trevrpc_config trevrpc_default_config(void) {
    trevrpc_config config = {0};
    config.max_idle_timeout_ms = 30000;
    config.keep_alive_ms = 15000;
    config.peer_bidi_stream_count = 100;
    config.max_frame_size = TREVRPC_DEFAULT_MAX_FRAME_SIZE;
    return config;
}

static int trevrpc_write_frame(trevrpc_msquic_stream* stream, const uint8_t* frame, size_t frame_len) {
    intptr_t written = trevrpc_msquic_stream_write(stream, frame, frame_len);
    if (written < 0) {
        return (int)written;
    }
    if ((size_t)written != frame_len) {
        return TREV_MSQUIC_ERR_CLOSED;
    }

    return 0;
}

static trevrpc_stream* trevrpc_stream_alloc(trevrpc_msquic_stream* stream, size_t max_frame_size, bool owns_stream) {
    trevrpc_stream* rpc_stream = calloc(1, sizeof(*rpc_stream));
    if (rpc_stream == NULL) {
        return NULL;
    }

    rpc_stream->stream = stream;
    rpc_stream->max_frame_size = max_frame_size;
    rpc_stream->owns_stream = owns_stream;
    return rpc_stream;
}

int trevrpc_stream_send_message(trevrpc_stream* stream, const uint8_t* body, size_t body_len) {
    if (stream == NULL || stream->stream == NULL || (body == NULL && body_len > 0)) {
        return -EINVAL;
    }

    intptr_t written =
        trevrpc_msquic_stream_write_message_frame(stream->stream, body, body_len, stream->max_frame_size);
    return written < 0 ? (int)written : 0;
}

int trevrpc_stream_send_status(trevrpc_stream* stream, uint32_t status, const char* message, size_t message_len) {
    if (stream == NULL || stream->stream == NULL || (message == NULL && message_len > 0)) {
        return -EINVAL;
    }

    uint8_t* frame = NULL;
    size_t frame_len = 0;
    int err = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_STATUS,
        status,
        message,
        message_len,
        NULL,
        0,
        NULL,
        stream->max_frame_size,
        &frame,
        &frame_len);
    if (err == 0) {
        err = trevrpc_write_frame(stream->stream, frame, frame_len);
    }
    free(frame);
    if (err == 0) {
        stream->sent_status = true;
    }
    return err;
}

int trevrpc_stream_recv(trevrpc_stream* stream, trevrpc_stream_frame** out_frame) {
    if (stream == NULL || stream->stream == NULL || out_frame == NULL) {
        return -EINVAL;
    }
    *out_frame = NULL;

    uint8_t* body = NULL;
    size_t body_len = 0;
    intptr_t read = trevrpc_msquic_stream_read_frame(stream->stream, &body, &body_len, stream->max_frame_size);
    if (read < 0) {
        return (int)read;
    }
    if (read == 0) {
        return 0;
    }

    int err = trevrpc_wire_decode_stream_frame(body, body_len, out_frame);
    trevrpc_msquic_free(body);
    return err;
}

int trevrpc_stream_finish_send(trevrpc_stream* stream) {
    if (stream == NULL || stream->stream == NULL) {
        return -EINVAL;
    }

    return trevrpc_msquic_stream_shutdown_send(stream->stream);
}

void trevrpc_stream_close(trevrpc_stream* stream) {
    if (stream == NULL) {
        return;
    }

    if (stream->owns_stream) {
        trevrpc_msquic_stream_close(stream->stream);
    }
    free(stream);
}

int trevrpc_client_connect(const char* host, uint16_t port, const trevrpc_config* config, trevrpc_client** out_client) {
    if (host == NULL || out_client == NULL) {
        return -EINVAL;
    }
    *out_client = NULL;

    trevrpc_client* client = calloc(1, sizeof(*client));
    if (client == NULL) {
        return -ENOMEM;
    }
    client->max_frame_size = trevrpc_effective_max_frame_size(config);

    trevrpc_msquic_config msquic_config = trevrpc_make_msquic_config(config);
    int err = trevrpc_msquic_dial(host, port, &msquic_config, &client->conn);
    if (err != 0) {
        trevrpc_client_close(client);
        return err;
    }

    *out_client = client;
    return 0;
}

int trevrpc_client_call_unary(trevrpc_client* client,
    const char* service,
    const char* method,
    const uint8_t* body,
    size_t body_len,
    trevrpc_response** out_response) {
    if (client == NULL || out_response == NULL) {
        return -EINVAL;
    }
    *out_response = NULL;

    trevrpc_msquic_stream* stream = NULL;
    int err = trevrpc_msquic_conn_open_stream(client->conn, &stream);
    if (err != 0) {
        return err;
    }

    uint8_t* frame = NULL;
    size_t frame_len = 0;
    err = trevrpc_wire_encode_request(
        service, method, TREVRPC_RPC_KIND_UNARY, body, body_len, NULL, 0, client->max_frame_size, &frame, &frame_len);
    if (err == 0) {
        err = trevrpc_write_frame(stream, frame, frame_len);
    }
    free(frame);
    if (err == 0) {
        err = trevrpc_msquic_stream_shutdown_send(stream);
    }

    uint8_t* response_body = NULL;
    size_t response_body_len = 0;
    if (err == 0) {
        intptr_t read =
            trevrpc_msquic_stream_read_frame(stream, &response_body, &response_body_len, client->max_frame_size);
        if (read < 0) {
            err = (int)read;
        } else if (read == 0) {
            err = TREV_MSQUIC_ERR_CLOSED;
        }
    }
    if (err == 0) {
        err = trevrpc_wire_decode_response(response_body, response_body_len, out_response);
    }

    trevrpc_msquic_free(response_body);
    trevrpc_msquic_stream_close(stream);
    return err;
}

int trevrpc_client_start_stream(trevrpc_client* client,
    const char* service,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len,
    trevrpc_stream** out_stream) {
    if (client == NULL || out_stream == NULL || service == NULL || method == NULL || (body == NULL && body_len > 0)) {
        return -EINVAL;
    }
    *out_stream = NULL;
    if (kind == TREVRPC_RPC_KIND_UNARY || kind > TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        return TREVRPC_ERR_UNSUPPORTED_RPC_KIND;
    }

    trevrpc_msquic_stream* raw_stream = NULL;
    int err = trevrpc_msquic_conn_open_stream(client->conn, &raw_stream);
    if (err != 0) {
        return err;
    }

    uint8_t* frame = NULL;
    size_t frame_len = 0;
    err = trevrpc_wire_encode_request(
        service, method, kind, body, body_len, NULL, 0, client->max_frame_size, &frame, &frame_len);
    if (err == 0) {
        err = trevrpc_write_frame(raw_stream, frame, frame_len);
    }
    free(frame);
    if (err != 0) {
        trevrpc_msquic_stream_close(raw_stream);
        return err;
    }

    trevrpc_stream* stream = trevrpc_stream_alloc(raw_stream, client->max_frame_size, true);
    if (stream == NULL) {
        trevrpc_msquic_stream_close(raw_stream);
        return -ENOMEM;
    }

    *out_stream = stream;
    return 0;
}

void trevrpc_client_close(trevrpc_client* client) {
    if (client == NULL) {
        return;
    }

    trevrpc_msquic_conn_close(client->conn);
    free(client);
}

static bool trevrpc_method_matches(
    const trevrpc_method* method, const char* service, size_t service_len, const char* name, size_t name_len) {
    return method->service_len == service_len && method->method_len == name_len &&
           memcmp(method->service, service, service_len) == 0 && memcmp(method->method, name, name_len) == 0;
}

int trevrpc_server_listen(const char* host, uint16_t port, const trevrpc_config* config, trevrpc_server** out_server) {
    if (host == NULL || out_server == NULL) {
        return -EINVAL;
    }
    *out_server = NULL;

    trevrpc_server* server = calloc(1, sizeof(*server));
    if (server == NULL) {
        return -ENOMEM;
    }
    server->max_frame_size = trevrpc_effective_max_frame_size(config);
    pthread_mutex_init(&server->mutex, NULL);
    pthread_cond_init(&server->cond, NULL);

    trevrpc_msquic_config msquic_config = trevrpc_make_msquic_config(config);
    int err = trevrpc_msquic_listen(host, port, &msquic_config, &server->listener);
    if (err != 0) {
        trevrpc_server_close(server);
        return err;
    }

    *out_server = server;
    return 0;
}

int trevrpc_server_register_unary(
    trevrpc_server* server, const char* service, const char* method, trevrpc_unary_handler handler, void* user_data) {
    if (server == NULL || service == NULL || method == NULL || handler == NULL) {
        return -EINVAL;
    }

    size_t service_len = strlen(service);
    size_t method_len = strlen(method);
    if (service_len == 0 || method_len == 0) {
        return -EINVAL;
    }

    trevrpc_method* registered = calloc(1, sizeof(*registered));
    if (registered == NULL) {
        return -ENOMEM;
    }
    registered->service = malloc(service_len);
    registered->method = malloc(method_len);
    if (registered->service == NULL || registered->method == NULL) {
        free(registered->service);
        free(registered->method);
        free(registered);
        return -ENOMEM;
    }
    memcpy(registered->service, service, service_len);
    memcpy(registered->method, method, method_len);
    registered->service_len = service_len;
    registered->method_len = method_len;
    registered->kind = TREVRPC_RPC_KIND_UNARY;
    registered->handler = handler;
    registered->user_data = user_data;

    pthread_mutex_lock(&server->mutex);
    if (server->shutting_down) {
        pthread_mutex_unlock(&server->mutex);
        free(registered->service);
        free(registered->method);
        free(registered);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    for (trevrpc_method* existing = server->methods; existing != NULL; existing = existing->next) {
        if (trevrpc_method_matches(existing, service, service_len, method, method_len)) {
            pthread_mutex_unlock(&server->mutex);
            free(registered->service);
            free(registered->method);
            free(registered);
            return -EEXIST;
        }
    }
    registered->next = server->methods;
    server->methods = registered;
    pthread_mutex_unlock(&server->mutex);
    return 0;
}

int trevrpc_server_register_streaming(trevrpc_server* server,
    const char* service,
    const char* method,
    uint32_t kind,
    trevrpc_stream_handler handler,
    void* user_data) {
    if (server == NULL || service == NULL || method == NULL || handler == NULL) {
        return -EINVAL;
    }
    if (kind == TREVRPC_RPC_KIND_UNARY || kind > TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        return TREVRPC_ERR_UNSUPPORTED_RPC_KIND;
    }

    size_t service_len = strlen(service);
    size_t method_len = strlen(method);
    if (service_len == 0 || method_len == 0) {
        return -EINVAL;
    }

    trevrpc_method* registered = calloc(1, sizeof(*registered));
    if (registered == NULL) {
        return -ENOMEM;
    }
    registered->service = malloc(service_len);
    registered->method = malloc(method_len);
    if (registered->service == NULL || registered->method == NULL) {
        free(registered->service);
        free(registered->method);
        free(registered);
        return -ENOMEM;
    }
    memcpy(registered->service, service, service_len);
    memcpy(registered->method, method, method_len);
    registered->service_len = service_len;
    registered->method_len = method_len;
    registered->kind = kind;
    registered->stream_handler = handler;
    registered->user_data = user_data;

    pthread_mutex_lock(&server->mutex);
    if (server->shutting_down) {
        pthread_mutex_unlock(&server->mutex);
        free(registered->service);
        free(registered->method);
        free(registered);
        return TREV_MSQUIC_ERR_CLOSED;
    }
    for (trevrpc_method* existing = server->methods; existing != NULL; existing = existing->next) {
        if (trevrpc_method_matches(existing, service, service_len, method, method_len)) {
            pthread_mutex_unlock(&server->mutex);
            free(registered->service);
            free(registered->method);
            free(registered);
            return -EEXIST;
        }
    }
    registered->next = server->methods;
    server->methods = registered;
    pthread_mutex_unlock(&server->mutex);
    return 0;
}

static bool trevrpc_server_task_start(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    bool start = !server->shutting_down;
    if (start) {
        server->active_tasks++;
    }
    pthread_mutex_unlock(&server->mutex);
    return start;
}

static void trevrpc_server_task_finish(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    if (server->active_tasks > 0) {
        server->active_tasks--;
    }
    pthread_cond_broadcast(&server->cond);
    pthread_mutex_unlock(&server->mutex);
}

static bool trevrpc_server_is_shutting_down(trevrpc_server* server) {
    pthread_mutex_lock(&server->mutex);
    bool shutting_down = server->shutting_down;
    pthread_mutex_unlock(&server->mutex);
    return shutting_down;
}

static int trevrpc_server_conn_add(
    trevrpc_server* server, trevrpc_msquic_conn* conn, trevrpc_server_conn_ref** out_ref) {
    *out_ref = NULL;
    trevrpc_server_conn_ref* ref = malloc(sizeof(*ref));
    if (ref == NULL) {
        return -ENOMEM;
    }
    ref->conn = conn;

    pthread_mutex_lock(&server->mutex);
    ref->next = server->conns;
    server->conns = ref;
    bool shutting_down = server->shutting_down;
    pthread_mutex_unlock(&server->mutex);

    if (shutting_down) {
        trevrpc_msquic_conn_shutdown(conn);
    }

    *out_ref = ref;
    return 0;
}

static void trevrpc_server_conn_remove(trevrpc_server* server, trevrpc_server_conn_ref* ref) {
    pthread_mutex_lock(&server->mutex);
    trevrpc_server_conn_ref** link = &server->conns;
    while (*link != NULL) {
        if (*link == ref) {
            *link = ref->next;
            break;
        }
        link = &(*link)->next;
    }
    pthread_mutex_unlock(&server->mutex);
    free(ref);
}

static trevrpc_method* trevrpc_server_find_method(trevrpc_server* server, const trevrpc_request* request) {
    pthread_mutex_lock(&server->mutex);
    for (trevrpc_method* method = server->methods; method != NULL; method = method->next) {
        if (trevrpc_method_matches(
                method, request->service, request->service_len, request->method, request->method_len)) {
            pthread_mutex_unlock(&server->mutex);
            return method;
        }
    }
    pthread_mutex_unlock(&server->mutex);
    return NULL;
}

static void trevrpc_set_status(trevrpc_response* response, uint32_t status, const char* message) {
    response->status = status;
    if (message != NULL) {
        (void)trevrpc_response_set_message(response, message, strlen(message));
    }
}

static void trevrpc_server_write_response(
    trevrpc_msquic_stream* stream, size_t max_frame_size, trevrpc_response* response) {
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    int err = trevrpc_wire_encode_response(response, max_frame_size, &frame, &frame_len);
    if (err != 0 && response->status == TREVRPC_STATUS_OK) {
        trevrpc_response_reset(response);
        trevrpc_set_status(response, TREVRPC_STATUS_RESOURCE_EXHAUSTED, "response frame exceeded maximum size");
        err = trevrpc_wire_encode_response(response, max_frame_size, &frame, &frame_len);
    }
    if (err == 0) {
        (void)trevrpc_write_frame(stream, frame, frame_len);
    }
    free(frame);
    (void)trevrpc_msquic_stream_shutdown_send(stream);
}

static void trevrpc_server_write_status(
    trevrpc_msquic_stream* stream, size_t max_frame_size, uint32_t status, const char* message) {
    trevrpc_response response = {0};
    trevrpc_set_status(&response, status, message);
    trevrpc_server_write_response(stream, max_frame_size, &response);
    trevrpc_response_reset(&response);
}

static void trevrpc_server_write_stream_status(
    trevrpc_msquic_stream* stream, size_t max_frame_size, uint32_t status, const char* message) {
    trevrpc_stream rpc_stream = {
        .stream = stream,
        .max_frame_size = max_frame_size,
    };
    (void)trevrpc_stream_send_status(&rpc_stream, status, message, message == NULL ? 0 : strlen(message));
    (void)trevrpc_msquic_stream_shutdown_send(stream);
}

static void trevrpc_handle_stream(trevrpc_server* server, trevrpc_msquic_stream* stream) {
    uint8_t* body = NULL;
    size_t body_len = 0;
    intptr_t read = trevrpc_msquic_stream_read_frame(stream, &body, &body_len, server->max_frame_size);
    if (read < 0) {
        uint32_t status =
            read == TREV_MSQUIC_ERR_FRAME_TOO_LARGE ? TREVRPC_STATUS_RESOURCE_EXHAUSTED : TREVRPC_STATUS_UNAVAILABLE;
        const char* message = read == TREV_MSQUIC_ERR_FRAME_TOO_LARGE ? "request frame exceeded maximum size"
                                                                      : "failed to read request frame";
        trevrpc_server_write_status(stream, server->max_frame_size, status, message);
        return;
    }
    if (read == 0) {
        return;
    }

    trevrpc_request request;
    int err = trevrpc_wire_decode_request(body, body_len, &request);
    if (err == TREVRPC_ERR_INVALID_FRAME) {
        trevrpc_server_write_status(
            stream, server->max_frame_size, TREVRPC_STATUS_INVALID_ARGUMENT, "invalid request frame");
        trevrpc_msquic_free(body);
        return;
    }
    if (err == TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION) {
        trevrpc_server_write_status(
            stream, server->max_frame_size, TREVRPC_STATUS_FAILED_PRECONDITION, "unsupported TrevRPC wire version");
        trevrpc_msquic_free(body);
        return;
    }
    if (err != 0) {
        trevrpc_server_write_status(stream, server->max_frame_size, TREVRPC_STATUS_INVALID_ARGUMENT, "invalid request");
        trevrpc_msquic_free(body);
        return;
    }
    trevrpc_method* method = trevrpc_server_find_method(server, &request);
    if (method == NULL) {
        if (request.kind == TREVRPC_RPC_KIND_UNARY) {
            trevrpc_server_write_status(
                stream, server->max_frame_size, TREVRPC_STATUS_UNIMPLEMENTED, "method is not implemented");
        } else {
            trevrpc_server_write_stream_status(
                stream, server->max_frame_size, TREVRPC_STATUS_UNIMPLEMENTED, "method is not implemented");
        }
        trevrpc_request_reset(&request);
        trevrpc_msquic_free(body);
        return;
    }
    if (method->kind != request.kind) {
        if (request.kind == TREVRPC_RPC_KIND_UNARY) {
            trevrpc_server_write_status(
                stream, server->max_frame_size, TREVRPC_STATUS_UNIMPLEMENTED, "method RPC kind mismatch");
        } else {
            trevrpc_server_write_stream_status(
                stream, server->max_frame_size, TREVRPC_STATUS_UNIMPLEMENTED, "method RPC kind mismatch");
        }
        trevrpc_request_reset(&request);
        trevrpc_msquic_free(body);
        return;
    }

    if (request.kind != TREVRPC_RPC_KIND_UNARY) {
        trevrpc_stream rpc_stream = {
            .stream = stream,
            .max_frame_size = server->max_frame_size,
        };
        err = method->stream_handler(method->user_data, &request, &rpc_stream);
        if (err != 0 && !rpc_stream.sent_status) {
            (void)trevrpc_stream_send_status(
                &rpc_stream, TREVRPC_STATUS_INTERNAL, "handler failed", strlen("handler failed"));
        } else if (!rpc_stream.sent_status) {
            (void)trevrpc_stream_send_status(&rpc_stream, TREVRPC_STATUS_OK, NULL, 0);
        }
        (void)trevrpc_stream_finish_send(&rpc_stream);
        trevrpc_request_reset(&request);
        trevrpc_msquic_free(body);
        return;
    }

    trevrpc_response response = {0};
    err = method->handler(method->user_data, &request, &response);
    if (err != 0) {
        trevrpc_response_reset(&response);
        trevrpc_set_status(&response, TREVRPC_STATUS_INTERNAL, "handler failed");
    }

    trevrpc_server_write_response(stream, server->max_frame_size, &response);
    trevrpc_response_reset(&response);
    trevrpc_request_reset(&request);
    trevrpc_msquic_free(body);
}

static void* trevrpc_stream_thread(void* arg) {
    trevrpc_stream_task* task = arg;
    trevrpc_handle_stream(task->server, task->stream);
    trevrpc_msquic_stream_close(task->stream);
    trevrpc_server_task_finish(task->server);
    free(task);
    return NULL;
}

static void* trevrpc_conn_thread(void* arg) {
    trevrpc_conn_task* task = arg;
    trevrpc_server* server = task->server;
    trevrpc_msquic_conn* conn = task->conn;
    free(task);

    trevrpc_server_conn_ref* conn_ref = NULL;
    if (trevrpc_server_conn_add(server, conn, &conn_ref) != 0) {
        trevrpc_msquic_conn_close(conn);
        trevrpc_server_task_finish(server);
        return NULL;
    }

    while (!trevrpc_server_is_shutting_down(server)) {
        trevrpc_msquic_stream* stream = NULL;
        int err = trevrpc_msquic_conn_accept_stream(conn, &stream);
        if (err != 0) {
            break;
        }

        if (!trevrpc_server_task_start(server)) {
            trevrpc_msquic_stream_close(stream);
            break;
        }

        trevrpc_stream_task* stream_task = malloc(sizeof(*stream_task));
        if (stream_task == NULL) {
            trevrpc_msquic_stream_close(stream);
            trevrpc_server_task_finish(server);
            continue;
        }
        stream_task->server = server;
        stream_task->stream = stream;

        pthread_t thread;
        err = pthread_create(&thread, NULL, trevrpc_stream_thread, stream_task);
        if (err != 0) {
            trevrpc_handle_stream(server, stream);
            trevrpc_msquic_stream_close(stream);
            trevrpc_server_task_finish(server);
            free(stream_task);
            continue;
        }
        pthread_detach(thread);
    }

    trevrpc_server_conn_remove(server, conn_ref);
    trevrpc_msquic_conn_close(conn);
    trevrpc_server_task_finish(server);
    return NULL;
}

int trevrpc_server_serve(trevrpc_server* server) {
    if (server == NULL) {
        return -EINVAL;
    }
    if (!trevrpc_server_task_start(server)) {
        return TREV_MSQUIC_ERR_CLOSED;
    }

    int result = 0;
    for (;;) {
        trevrpc_msquic_conn* conn = NULL;
        int err = trevrpc_msquic_listener_accept(server->listener, &conn);
        if (err != 0) {
            result = trevrpc_server_is_shutting_down(server) ? 0 : err;
            break;
        }

        if (!trevrpc_server_task_start(server)) {
            trevrpc_msquic_conn_close(conn);
            continue;
        }

        trevrpc_conn_task* task = malloc(sizeof(*task));
        if (task == NULL) {
            trevrpc_msquic_conn_close(conn);
            trevrpc_server_task_finish(server);
            result = -ENOMEM;
            break;
        }
        task->server = server;
        task->conn = conn;

        pthread_t thread;
        err = pthread_create(&thread, NULL, trevrpc_conn_thread, task);
        if (err != 0) {
            free(task);
            trevrpc_msquic_conn_close(conn);
            trevrpc_server_task_finish(server);
            result = -err;
            break;
        }
        pthread_detach(thread);
    }

    trevrpc_server_task_finish(server);
    return result;
}

void trevrpc_server_shutdown(trevrpc_server* server) {
    if (server == NULL) {
        return;
    }

    pthread_mutex_lock(&server->mutex);
    server->shutting_down = true;
    for (trevrpc_server_conn_ref* ref = server->conns; ref != NULL; ref = ref->next) {
        trevrpc_msquic_conn_shutdown(ref->conn);
    }
    pthread_cond_broadcast(&server->cond);
    pthread_mutex_unlock(&server->mutex);

    trevrpc_msquic_listener_shutdown(server->listener);
}

void trevrpc_server_close(trevrpc_server* server) {
    if (server == NULL) {
        return;
    }

    trevrpc_server_shutdown(server);
    pthread_mutex_lock(&server->mutex);
    while (server->active_tasks > 0) {
        pthread_cond_wait(&server->cond, &server->mutex);
    }
    pthread_mutex_unlock(&server->mutex);

    trevrpc_msquic_listener_close(server->listener);
    trevrpc_method* method = server->methods;
    while (method != NULL) {
        trevrpc_method* next = method->next;
        free(method->service);
        free(method->method);
        free(method);
        method = next;
    }

    pthread_cond_destroy(&server->cond);
    pthread_mutex_destroy(&server->mutex);
    free(server);
}

const char* trevrpc_error(int code) {
    switch (code) {
    case TREVRPC_ERR_INVALID_FRAME:
        return "invalid RPC frame";
    case TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION:
        return "unsupported TrevRPC wire version";
    case TREVRPC_ERR_UNSUPPORTED_RPC_KIND:
        return "unsupported TrevRPC RPC kind";
    case TREVRPC_ERR_HANDLER_FAILED:
        return "RPC handler failed";
    case TREVRPC_ERR_FRAME_TOO_LARGE:
        return "frame too large";
    case -ENOMEM:
    case ENOMEM:
        return "out of memory";
    case -EINVAL:
    case EINVAL:
        return "invalid argument";
    case -EEXIST:
    case EEXIST:
        return "method already registered";
    default:
        return trevrpc_msquic_error(code);
    }
}
