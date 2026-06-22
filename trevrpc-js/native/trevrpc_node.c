#define _POSIX_C_SOURCE 200809L

#include "trevrpc.h"
#include "trevrpc_webtransport.h"

#include <errno.h>
#include <node_api.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TREV_NODE_ERR_CLOSED -4001

typedef struct native_client native_client;
typedef struct native_stream native_stream;

struct native_client {
    trevrpc_client* client;
    pthread_mutex_t mutex;
    size_t refs;
    bool closing;
    bool js_alive;
};

struct native_stream {
    trevrpc_stream* stream;
    native_client* owner;
    pthread_mutex_t mutex;
    size_t refs;
    bool closing;
    bool js_alive;
    bool owner_released;
};

typedef struct base_work {
    napi_env env;
    napi_deferred deferred;
    napi_async_work work;
    napi_ref receiver_ref;
    int err;
} base_work;

typedef struct connect_work {
    base_work base;
    trevrpc_client* client;
    char* host;
    char* path;
    char* origin;
    char* cert_file;
    char* key_file;
    char* ca_cert_file;
    uint16_t port;
    int skip_certificate_validation;
    uint32_t max_sessions_per_connection;
    uint32_t max_streams_per_session;
    uint32_t idle_timeout_ms;
    size_t max_frame_size;
} connect_work;

typedef struct call_work {
    base_work base;
    native_client* client;
    bool acquired;
    char* service;
    char* method;
    uint8_t* body;
    size_t body_len;
    trevrpc_response* response;
} call_work;

typedef struct start_stream_work {
    base_work base;
    native_client* client;
    bool acquired;
    char* service;
    char* method;
    uint8_t* body;
    size_t body_len;
    uint32_t kind;
    trevrpc_stream* stream;
} start_stream_work;

typedef struct stream_body_work {
    base_work base;
    native_stream* stream;
    bool acquired;
    uint8_t* body;
    size_t body_len;
} stream_body_work;

typedef struct stream_simple_work {
    base_work base;
    native_stream* stream;
    bool acquired;
} stream_simple_work;

typedef struct stream_recv_work {
    base_work base;
    native_stream* stream;
    bool acquired;
    trevrpc_stream_frame* frame;
} stream_recv_work;

static napi_ref NativeClientConstructor;
static napi_ref NativeStreamConstructor;

static void native_client_release(native_client* client);
static void native_stream_close_request(native_stream* stream);

static void clear_pending_exception(napi_env env) {
    bool pending = false;
    if (napi_is_exception_pending(env, &pending) == napi_ok && pending) {
        napi_value ignored = NULL;
        napi_get_and_clear_last_exception(env, &ignored);
    }
}

static void throw_if_no_pending_exception(napi_env env, const char* message) {
    bool pending = false;
    if (napi_is_exception_pending(env, &pending) != napi_ok || !pending) {
        napi_throw_error(env, NULL, message);
    }
}

static void reject_native_error(napi_env env, napi_deferred deferred, int err, const char* operation) {
    char message[256];
    const char* detail = err == TREV_NODE_ERR_CLOSED ? "native object is closed" : trevrpc_error(err);
    snprintf(message, sizeof(message), "%s failed: %s", operation, detail != NULL ? detail : "native operation failed");

    napi_value message_value = NULL;
    napi_value error = NULL;
    napi_value native_code = NULL;
    napi_create_string_utf8(env, message, NAPI_AUTO_LENGTH, &message_value);
    napi_create_error(env, NULL, message_value, &error);
    napi_create_int32(env, err, &native_code);
    napi_set_named_property(env, error, "nativeCode", native_code);
    napi_reject_deferred(env, deferred, error);
}

static bool get_bool_property(napi_env env, napi_value object, const char* name, bool default_value) {
    bool has_property = false;
    napi_has_named_property(env, object, name, &has_property);
    if (!has_property) {
        return default_value;
    }

    napi_value value = NULL;
    napi_get_named_property(env, object, name, &value);
    bool result = default_value;
    napi_get_value_bool(env, value, &result);
    return result;
}

static uint32_t get_uint32_property(napi_env env, napi_value object, const char* name, uint32_t default_value) {
    bool has_property = false;
    napi_has_named_property(env, object, name, &has_property);
    if (!has_property) {
        return default_value;
    }

    napi_value value = NULL;
    napi_get_named_property(env, object, name, &value);
    uint32_t result = default_value;
    napi_get_value_uint32(env, value, &result);
    return result;
}

static bool get_size_property(napi_env env, napi_value object, const char* name, size_t* out_value) {
    bool has_property = false;
    napi_has_named_property(env, object, name, &has_property);
    if (!has_property) {
        return false;
    }

    napi_value value = NULL;
    napi_get_named_property(env, object, name, &value);
    double number = 0;
    if (napi_get_value_double(env, value, &number) != napi_ok || number < 0) {
        return false;
    }
    *out_value = (size_t)number;
    return true;
}

static char* copy_string_value(napi_env env, napi_value value) {
    size_t len = 0;
    if (napi_get_value_string_utf8(env, value, NULL, 0, &len) != napi_ok) {
        return NULL;
    }
    char* result = malloc(len + 1);
    if (result == NULL) {
        return NULL;
    }
    if (napi_get_value_string_utf8(env, value, result, len + 1, &len) != napi_ok) {
        free(result);
        return NULL;
    }
    return result;
}

static char* get_string_property(napi_env env, napi_value object, const char* name) {
    bool has_property = false;
    napi_has_named_property(env, object, name, &has_property);
    if (!has_property) {
        return NULL;
    }

    napi_value value = NULL;
    napi_get_named_property(env, object, name, &value);
    napi_valuetype type = napi_undefined;
    napi_typeof(env, value, &type);
    if (type == napi_undefined || type == napi_null) {
        return NULL;
    }
    return copy_string_value(env, value);
}

static int copy_string_arg(napi_env env, napi_value value, char** out) {
    *out = copy_string_value(env, value);
    return *out == NULL ? -ENOMEM : 0;
}

static int copy_bytes_arg(napi_env env, napi_value value, uint8_t** out, size_t* out_len) {
    *out = NULL;
    *out_len = 0;

    bool is_typedarray = false;
    napi_is_typedarray(env, value, &is_typedarray);
    if (is_typedarray) {
        napi_typedarray_type type = napi_uint8_array;
        size_t len = 0;
        void* data = NULL;
        napi_value arraybuffer = NULL;
        size_t byte_offset = 0;
        if (napi_get_typedarray_info(env, value, &type, &len, &data, &arraybuffer, &byte_offset) != napi_ok ||
            type != napi_uint8_array) {
            return -EINVAL;
        }
        if (len == 0) {
            return 0;
        }
        *out = malloc(len);
        if (*out == NULL) {
            return -ENOMEM;
        }
        memcpy(*out, data, len);
        *out_len = len;
        return 0;
    }

    bool is_arraybuffer = false;
    napi_is_arraybuffer(env, value, &is_arraybuffer);
    if (is_arraybuffer) {
        void* data = NULL;
        size_t len = 0;
        if (napi_get_arraybuffer_info(env, value, &data, &len) != napi_ok) {
            return -EINVAL;
        }
        if (len == 0) {
            return 0;
        }
        *out = malloc(len);
        if (*out == NULL) {
            return -ENOMEM;
        }
        memcpy(*out, data, len);
        *out_len = len;
        return 0;
    }

    return -EINVAL;
}

static napi_value make_uint8_array(napi_env env, const uint8_t* data, size_t len) {
    napi_value arraybuffer = NULL;
    napi_value typedarray = NULL;
    void* buffer = NULL;
    napi_create_arraybuffer(env, len, &buffer, &arraybuffer);
    if (len > 0 && data != NULL) {
        memcpy(buffer, data, len);
    }
    napi_create_typedarray(env, napi_uint8_array, len, arraybuffer, 0, &typedarray);
    return typedarray;
}

static napi_status set_uint32(napi_env env, napi_value object, const char* name, uint32_t value) {
    napi_value js_value = NULL;
    napi_create_uint32(env, value, &js_value);
    return napi_set_named_property(env, object, name, js_value);
}

static napi_status set_string_bytes(napi_env env, napi_value object, const char* name, const char* value, size_t len) {
    napi_value js_value = NULL;
    napi_create_string_utf8(env, value != NULL ? value : "", len, &js_value);
    return napi_set_named_property(env, object, name, js_value);
}

static napi_status set_bytes(napi_env env, napi_value object, const char* name, const uint8_t* value, size_t len) {
    napi_value bytes = make_uint8_array(env, value, len);
    return napi_set_named_property(env, object, name, bytes);
}

static napi_value metadata_to_js(napi_env env, const trevrpc_metadata* metadata) {
    napi_value object = NULL;
    napi_create_object(env, &object);
    if (metadata == NULL) {
        return object;
    }

    for (size_t i = 0; i < metadata->entries_len; i++) {
        const trevrpc_metadata_entry* entry = &metadata->entries[i];
        napi_value key = NULL;
        napi_value value = make_uint8_array(env, entry->value, entry->value_len);
        napi_create_string_utf8(env, entry->key, entry->key_len, &key);
        napi_set_property(env, object, key, value);
    }
    return object;
}

static napi_value response_to_js(napi_env env, const trevrpc_response* response) {
    napi_value object = NULL;
    napi_create_object(env, &object);
    set_uint32(env, object, "status", response != NULL ? response->status : TREVRPC_STATUS_UNKNOWN);
    set_string_bytes(env,
        object,
        "message",
        response != NULL ? response->message : "",
        response != NULL ? response->message_len : 0);
    set_bytes(env, object, "body", response != NULL ? response->body : NULL, response != NULL ? response->body_len : 0);
    napi_value metadata = metadata_to_js(env, response != NULL ? &response->metadata : NULL);
    napi_set_named_property(env, object, "metadata", metadata);
    return object;
}

static napi_value stream_frame_to_js(napi_env env, const trevrpc_stream_frame* frame) {
    napi_value object = NULL;
    napi_create_object(env, &object);
    set_uint32(env, object, "kind", frame->kind);
    set_uint32(env, object, "status", frame->status);
    set_string_bytes(env, object, "message", frame->message, frame->message_len);
    set_bytes(env, object, "body", frame->body, frame->body_len);
    napi_value metadata = metadata_to_js(env, &frame->metadata);
    napi_set_named_property(env, object, "metadata", metadata);
    return object;
}

static bool unwrap_native_client(napi_env env, napi_value receiver, native_client** out_client) {
    *out_client = NULL;
    if (napi_unwrap(env, receiver, (void**)out_client) != napi_ok || *out_client == NULL) {
        napi_throw_type_error(env, NULL, "invalid native client receiver");
        return false;
    }
    return true;
}

static bool unwrap_native_stream(napi_env env, napi_value receiver, native_stream** out_stream) {
    *out_stream = NULL;
    if (napi_unwrap(env, receiver, (void**)out_stream) != napi_ok || *out_stream == NULL) {
        napi_throw_type_error(env, NULL, "invalid native stream receiver");
        return false;
    }
    return true;
}

static bool create_receiver_ref(napi_env env, napi_value receiver, napi_ref* out_ref) {
    *out_ref = NULL;
    if (napi_create_reference(env, receiver, 1, out_ref) != napi_ok) {
        napi_throw_error(env, NULL, "failed to hold native receiver");
        return false;
    }
    return true;
}

static void native_client_maybe_destroy(native_client* client) {
    bool destroy = false;
    pthread_mutex_lock(&client->mutex);
    destroy = !client->js_alive && client->refs == 0 && client->client == NULL;
    pthread_mutex_unlock(&client->mutex);
    if (destroy) {
        pthread_mutex_destroy(&client->mutex);
        free(client);
    }
}

static int native_client_acquire(native_client* client, trevrpc_client** out_client) {
    if (client == NULL) {
        return TREV_NODE_ERR_CLOSED;
    }
    pthread_mutex_lock(&client->mutex);
    if (client->client == NULL || client->closing) {
        pthread_mutex_unlock(&client->mutex);
        return TREV_NODE_ERR_CLOSED;
    }
    client->refs++;
    *out_client = client->client;
    pthread_mutex_unlock(&client->mutex);
    return 0;
}

static void native_client_release(native_client* client) {
    trevrpc_client* close_client = NULL;
    bool destroy = false;
    pthread_mutex_lock(&client->mutex);
    if (client->refs > 0) {
        client->refs--;
    }
    if (client->closing && client->refs == 0 && client->client != NULL) {
        close_client = client->client;
        client->client = NULL;
    }
    destroy = !client->js_alive && client->refs == 0 && client->client == NULL;
    pthread_mutex_unlock(&client->mutex);

    trevrpc_client_close(close_client);
    if (destroy) {
        pthread_mutex_destroy(&client->mutex);
        free(client);
    }
}

static void native_client_close_request(native_client* client) {
    if (client == NULL) {
        return;
    }
    trevrpc_client* close_client = NULL;
    trevrpc_client* shutdown_client = NULL;
    pthread_mutex_lock(&client->mutex);
    client->closing = true;
    if (client->refs == 0 && client->client != NULL) {
        close_client = client->client;
        client->client = NULL;
    } else if (client->client != NULL) {
        shutdown_client = client->client;
    }
    pthread_mutex_unlock(&client->mutex);

    trevrpc_client_shutdown(shutdown_client);
    trevrpc_client_close(close_client);
    native_client_maybe_destroy(client);
}

static int native_stream_acquire(native_stream* stream, trevrpc_stream** out_stream) {
    if (stream == NULL) {
        return TREV_NODE_ERR_CLOSED;
    }
    pthread_mutex_lock(&stream->mutex);
    if (stream->stream == NULL || stream->closing) {
        pthread_mutex_unlock(&stream->mutex);
        return TREV_NODE_ERR_CLOSED;
    }
    stream->refs++;
    *out_stream = stream->stream;
    pthread_mutex_unlock(&stream->mutex);
    return 0;
}

static void native_stream_maybe_destroy(native_stream* stream) {
    bool destroy = false;
    pthread_mutex_lock(&stream->mutex);
    destroy = !stream->js_alive && stream->refs == 0 && stream->stream == NULL;
    pthread_mutex_unlock(&stream->mutex);
    if (destroy) {
        pthread_mutex_destroy(&stream->mutex);
        free(stream);
    }
}

static void native_stream_release(native_stream* stream) {
    trevrpc_stream* close_stream = NULL;
    bool should_release_owner = false;
    bool destroy = false;
    pthread_mutex_lock(&stream->mutex);
    if (stream->refs > 0) {
        stream->refs--;
    }
    if (stream->closing && stream->refs == 0 && stream->stream != NULL) {
        close_stream = stream->stream;
        stream->stream = NULL;
        should_release_owner = !stream->owner_released;
        stream->owner_released = true;
    }
    destroy = !stream->js_alive && stream->refs == 0 && stream->stream == NULL;
    pthread_mutex_unlock(&stream->mutex);

    trevrpc_stream_close(close_stream);
    if (should_release_owner && stream->owner != NULL) {
        native_client_release(stream->owner);
    }
    if (destroy) {
        pthread_mutex_destroy(&stream->mutex);
        free(stream);
    }
}

static void native_stream_close_request(native_stream* stream) {
    if (stream == NULL) {
        return;
    }
    trevrpc_stream* close_stream = NULL;
    trevrpc_stream* cancel_stream = NULL;
    bool should_release_owner = false;
    pthread_mutex_lock(&stream->mutex);
    stream->closing = true;
    if (stream->refs == 0 && stream->stream != NULL) {
        close_stream = stream->stream;
        stream->stream = NULL;
        should_release_owner = !stream->owner_released;
        stream->owner_released = true;
    } else if (stream->stream != NULL) {
        cancel_stream = stream->stream;
    }
    pthread_mutex_unlock(&stream->mutex);

    trevrpc_stream_cancel(cancel_stream);
    trevrpc_stream_close(close_stream);
    if (should_release_owner && stream->owner != NULL) {
        native_client_release(stream->owner);
    }
    native_stream_maybe_destroy(stream);
}

static napi_value queue_work(napi_env env,
    base_work* work,
    const char* name,
    napi_async_execute_callback execute,
    napi_async_complete_callback complete) {
    napi_value promise = NULL;
    napi_value resource_name = NULL;
    work->env = env;
    napi_create_promise(env, &work->deferred, &promise);
    napi_create_string_utf8(env, name, NAPI_AUTO_LENGTH, &resource_name);
    if (napi_create_async_work(env, NULL, resource_name, execute, complete, work, &work->work) != napi_ok ||
        napi_queue_async_work(env, work->work) != napi_ok) {
        reject_native_error(env, work->deferred, -ENOMEM, name);
    }
    return promise;
}

static void native_client_finalize(napi_env env, void* data, void* hint) {
    (void)env;
    (void)hint;
    native_client* client = data;
    pthread_mutex_lock(&client->mutex);
    client->js_alive = false;
    pthread_mutex_unlock(&client->mutex);
    native_client_close_request(client);
}

static void native_stream_finalize(napi_env env, void* data, void* hint) {
    (void)env;
    (void)hint;
    native_stream* stream = data;
    pthread_mutex_lock(&stream->mutex);
    stream->js_alive = false;
    pthread_mutex_unlock(&stream->mutex);
    native_stream_close_request(stream);
}

static napi_value native_client_constructor(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    if (napi_get_cb_info(env, info, &argc, args, &this_arg, NULL) != napi_ok) {
        return NULL;
    }

    native_client* client = NULL;
    if (argc != 1 || napi_get_value_external(env, args[0], (void**)&client) != napi_ok || client == NULL) {
        napi_throw_type_error(env, NULL, "native clients cannot be constructed directly");
        return NULL;
    }

    if (napi_wrap(env, this_arg, client, native_client_finalize, NULL, NULL) != napi_ok) {
        throw_if_no_pending_exception(env, "failed to wrap native client");
        return NULL;
    }
    client->js_alive = true;
    return this_arg;
}

static napi_value native_stream_constructor(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    if (napi_get_cb_info(env, info, &argc, args, &this_arg, NULL) != napi_ok) {
        return NULL;
    }

    native_stream* stream = NULL;
    if (argc != 1 || napi_get_value_external(env, args[0], (void**)&stream) != napi_ok || stream == NULL) {
        napi_throw_type_error(env, NULL, "native streams cannot be constructed directly");
        return NULL;
    }

    if (napi_wrap(env, this_arg, stream, native_stream_finalize, NULL, NULL) != napi_ok) {
        throw_if_no_pending_exception(env, "failed to wrap native stream");
        return NULL;
    }
    stream->js_alive = true;
    return this_arg;
}

static void connect_execute(napi_env env, void* data) {
    (void)env;
    connect_work* work = data;
    trevrpc_config config = trevrpc_default_config();
    if (work->max_frame_size > 0) {
        config.max_frame_size = work->max_frame_size;
    }
    trevrpc_wt_config wt_config = {
        .host = work->host,
        .port = work->port,
        .path = work->path,
        .origin = work->origin,
        .cert_file = work->cert_file,
        .key_file = work->key_file,
        .ca_cert_file = work->ca_cert_file,
        .skip_certificate_validation = work->skip_certificate_validation,
        .max_sessions_per_connection = work->max_sessions_per_connection,
        .max_streams_per_session = work->max_streams_per_session,
        .idle_timeout_ms = work->idle_timeout_ms,
    };
    work->base.err = trevrpc_client_connect_webtransport(&wt_config, &config, &work->client);
}

static void connect_complete(napi_env env, napi_status status, void* data) {
    connect_work* work = data;
    if (status != napi_ok) {
        reject_native_error(env, work->base.deferred, -ECANCELED, "connectWebTransport");
    } else if (work->base.err != 0) {
        reject_native_error(env, work->base.deferred, work->base.err, "connectWebTransport");
    } else {
        native_client* client = calloc(1, sizeof(*client));
        if (client == NULL) {
            trevrpc_client_close(work->client);
            reject_native_error(env, work->base.deferred, -ENOMEM, "connectWebTransport");
        } else {
            pthread_mutex_init(&client->mutex, NULL);
            client->client = work->client;

            napi_value ctor = NULL;
            napi_value external = NULL;
            napi_value instance = NULL;
            napi_status wrap_status = napi_get_reference_value(env, NativeClientConstructor, &ctor);
            if (wrap_status == napi_ok) {
                wrap_status = napi_create_external(env, client, NULL, NULL, &external);
            }
            if (wrap_status == napi_ok) {
                wrap_status = napi_new_instance(env, ctor, 1, &external, &instance);
            }
            if (wrap_status != napi_ok) {
                clear_pending_exception(env);
                native_client_close_request(client);
                reject_native_error(env, work->base.deferred, -ENOMEM, "connectWebTransport");
            } else {
                napi_resolve_deferred(env, work->base.deferred, instance);
            }
        }
    }

    free(work->host);
    free(work->path);
    free(work->origin);
    free(work->cert_file);
    free(work->key_file);
    free(work->ca_cert_file);
    napi_delete_async_work(env, work->base.work);
    free(work);
}

static napi_value connect_webtransport(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, NULL, NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "connectWebTransport requires an options object");
        return NULL;
    }

    connect_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate connect work");
        return NULL;
    }
    work->host = get_string_property(env, args[0], "host");
    work->path = get_string_property(env, args[0], "path");
    work->origin = get_string_property(env, args[0], "origin");
    work->cert_file = get_string_property(env, args[0], "certFile");
    work->key_file = get_string_property(env, args[0], "keyFile");
    work->ca_cert_file = get_string_property(env, args[0], "caCertFile");
    work->port = (uint16_t)get_uint32_property(env, args[0], "port", 0);
    work->skip_certificate_validation = get_bool_property(env, args[0], "skipCertificateValidation", false) ? 1 : 0;
    work->max_sessions_per_connection = get_uint32_property(env, args[0], "maxSessionsPerConnection", 0);
    work->max_streams_per_session = get_uint32_property(env, args[0], "maxStreamsPerSession", 0);
    work->idle_timeout_ms = get_uint32_property(env, args[0], "idleTimeoutMs", 0);
    get_size_property(env, args[0], "maxFrameSize", &work->max_frame_size);

    if (work->host == NULL || work->port == 0) {
        free(work->host);
        free(work->path);
        free(work->origin);
        free(work->cert_file);
        free(work->key_file);
        free(work->ca_cert_file);
        free(work);
        napi_throw_type_error(env, NULL, "connectWebTransport requires host and port");
        return NULL;
    }

    return queue_work(env, &work->base, "connectWebTransport", connect_execute, connect_complete);
}

static void call_execute(napi_env env, void* data) {
    (void)env;
    call_work* work = data;
    trevrpc_client* client = NULL;
    work->base.err = native_client_acquire(work->client, &client);
    if (work->base.err != 0) {
        return;
    }
    work->acquired = true;
    work->base.err =
        trevrpc_client_call_unary(client, work->service, work->method, work->body, work->body_len, &work->response);
}

static void call_complete(napi_env env, napi_status status, void* data) {
    call_work* work = data;
    if (work->base.err == 0 && status == napi_ok) {
        napi_value response = response_to_js(env, work->response);
        napi_resolve_deferred(env, work->base.deferred, response);
    } else {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "call");
    }
    trevrpc_response_free(work->response);
    if (work->acquired) {
        native_client_release(work->client);
    }
    if (work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    napi_delete_async_work(env, work->base.work);
    free(work->service);
    free(work->method);
    free(work->body);
    free(work);
}

static napi_value native_client_call(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value args[3];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    if (argc != 3) {
        napi_throw_type_error(env, NULL, "call requires service, method, and body");
        return NULL;
    }

    call_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate call work");
        return NULL;
    }
    if (!unwrap_native_client(env, this_arg, &work->client) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    int err = copy_string_arg(env, args[0], &work->service);
    if (err == 0) {
        err = copy_string_arg(env, args[1], &work->method);
    }
    if (err == 0) {
        err = copy_bytes_arg(env, args[2], &work->body, &work->body_len);
    }
    if (err != 0) {
        napi_delete_reference(env, work->base.receiver_ref);
        free(work->service);
        free(work->method);
        free(work->body);
        free(work);
        napi_throw_type_error(env, NULL, "invalid call arguments");
        return NULL;
    }
    return queue_work(env, &work->base, "call", call_execute, call_complete);
}

static void start_stream_execute(napi_env env, void* data) {
    (void)env;
    start_stream_work* work = data;
    trevrpc_client* client = NULL;
    work->base.err = native_client_acquire(work->client, &client);
    if (work->base.err != 0) {
        return;
    }
    work->acquired = true;
    work->base.err = trevrpc_client_start_stream(
        client, work->service, work->method, work->kind, work->body, work->body_len, &work->stream);
}

static void start_stream_complete(napi_env env, napi_status status, void* data) {
    start_stream_work* work = data;
    if (work->base.err == 0 && status == napi_ok) {
        native_stream* stream = calloc(1, sizeof(*stream));
        if (stream == NULL) {
            trevrpc_stream_close(work->stream);
            if (work->acquired) {
                native_client_release(work->client);
                work->acquired = false;
            }
            reject_native_error(env, work->base.deferred, -ENOMEM, "startStream");
        } else {
            pthread_mutex_init(&stream->mutex, NULL);
            stream->stream = work->stream;
            stream->owner = work->client;
            napi_value ctor = NULL;
            napi_value external = NULL;
            napi_value instance = NULL;
            napi_status wrap_status = napi_get_reference_value(env, NativeStreamConstructor, &ctor);
            if (wrap_status == napi_ok) {
                wrap_status = napi_create_external(env, stream, NULL, NULL, &external);
            }
            if (wrap_status == napi_ok) {
                wrap_status = napi_new_instance(env, ctor, 1, &external, &instance);
            }
            if (wrap_status != napi_ok) {
                clear_pending_exception(env);
                native_stream_close_request(stream);
                reject_native_error(env, work->base.deferred, -ENOMEM, "startStream");
            } else {
                napi_resolve_deferred(env, work->base.deferred, instance);
            }
        }
    } else {
        if (work->acquired) {
            native_client_release(work->client);
        }
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "startStream");
    }
    if (work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    napi_delete_async_work(env, work->base.work);
    free(work->service);
    free(work->method);
    free(work->body);
    free(work);
}

static napi_value native_client_start_stream(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value args[4];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    if (argc != 4) {
        napi_throw_type_error(env, NULL, "startStream requires service, method, kind, and body");
        return NULL;
    }

    start_stream_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate startStream work");
        return NULL;
    }
    if (!unwrap_native_client(env, this_arg, &work->client) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    int err = copy_string_arg(env, args[0], &work->service);
    if (err == 0) {
        err = copy_string_arg(env, args[1], &work->method);
    }
    if (err == 0 && napi_get_value_uint32(env, args[2], &work->kind) != napi_ok) {
        err = -EINVAL;
    }
    if (err == 0) {
        err = copy_bytes_arg(env, args[3], &work->body, &work->body_len);
    }
    if (err != 0) {
        napi_delete_reference(env, work->base.receiver_ref);
        free(work->service);
        free(work->method);
        free(work->body);
        free(work);
        napi_throw_type_error(env, NULL, "invalid startStream arguments");
        return NULL;
    }
    return queue_work(env, &work->base, "startStream", start_stream_execute, start_stream_complete);
}

static napi_value native_client_close(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    native_client* client = NULL;
    if (!unwrap_native_client(env, this_arg, &client)) {
        return NULL;
    }
    native_client_close_request(client);
    napi_value undefined = NULL;
    napi_get_undefined(env, &undefined);
    return undefined;
}

static void stream_send_execute(napi_env env, void* data) {
    (void)env;
    stream_body_work* work = data;
    trevrpc_stream* stream = NULL;
    work->base.err = native_stream_acquire(work->stream, &stream);
    if (work->base.err != 0) {
        return;
    }
    work->acquired = true;
    work->base.err = trevrpc_stream_send_message(stream, work->body, work->body_len);
}

static void stream_send_complete(napi_env env, napi_status status, void* data) {
    stream_body_work* work = data;
    if (work->base.err == 0 && status == napi_ok) {
        napi_value undefined = NULL;
        napi_get_undefined(env, &undefined);
        napi_resolve_deferred(env, work->base.deferred, undefined);
    } else {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "sendMessage");
    }
    if (work->acquired) {
        native_stream_release(work->stream);
    }
    if (work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    napi_delete_async_work(env, work->base.work);
    free(work->body);
    free(work);
}

static napi_value native_stream_send_message(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "sendMessage requires a body");
        return NULL;
    }
    stream_body_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate sendMessage work");
        return NULL;
    }
    if (!unwrap_native_stream(env, this_arg, &work->stream) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    if (copy_bytes_arg(env, args[0], &work->body, &work->body_len) != 0) {
        napi_delete_reference(env, work->base.receiver_ref);
        free(work);
        napi_throw_type_error(env, NULL, "invalid sendMessage body");
        return NULL;
    }
    return queue_work(env, &work->base, "sendMessage", stream_send_execute, stream_send_complete);
}

static void stream_finish_execute(napi_env env, void* data) {
    (void)env;
    stream_simple_work* work = data;
    trevrpc_stream* stream = NULL;
    work->base.err = native_stream_acquire(work->stream, &stream);
    if (work->base.err != 0) {
        return;
    }
    work->acquired = true;
    work->base.err = trevrpc_stream_finish_send(stream);
}

static void stream_finish_complete(napi_env env, napi_status status, void* data) {
    stream_simple_work* work = data;
    if (status != napi_ok) {
        reject_native_error(env, work->base.deferred, -ECANCELED, "finishSend");
    } else if (work->base.err != 0) {
        reject_native_error(env, work->base.deferred, work->base.err, "finishSend");
    } else {
        napi_value undefined = NULL;
        napi_get_undefined(env, &undefined);
        napi_resolve_deferred(env, work->base.deferred, undefined);
    }
    if (work->acquired) {
        native_stream_release(work->stream);
    }
    if (work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    napi_delete_async_work(env, work->base.work);
    free(work);
}

static napi_value native_stream_finish_send(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    stream_simple_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate finishSend work");
        return NULL;
    }
    if (!unwrap_native_stream(env, this_arg, &work->stream) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    return queue_work(env, &work->base, "finishSend", stream_finish_execute, stream_finish_complete);
}

static void stream_recv_execute(napi_env env, void* data) {
    (void)env;
    stream_recv_work* work = data;
    trevrpc_stream* stream = NULL;
    work->base.err = native_stream_acquire(work->stream, &stream);
    if (work->base.err != 0) {
        return;
    }
    work->acquired = true;
    work->base.err = trevrpc_stream_recv(stream, &work->frame);
}

static void stream_recv_complete(napi_env env, napi_status status, void* data) {
    stream_recv_work* work = data;
    if (work->base.err == 0 && status == napi_ok) {
        napi_value value = NULL;
        if (work->frame == NULL) {
            napi_get_null(env, &value);
        } else {
            value = stream_frame_to_js(env, work->frame);
        }
        napi_resolve_deferred(env, work->base.deferred, value);
    } else {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "recv");
    }
    trevrpc_stream_frame_free(work->frame);
    if (work->acquired) {
        native_stream_release(work->stream);
    }
    if (work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    napi_delete_async_work(env, work->base.work);
    free(work);
}

static napi_value native_stream_recv(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    stream_recv_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate recv work");
        return NULL;
    }
    if (!unwrap_native_stream(env, this_arg, &work->stream) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    return queue_work(env, &work->base, "recv", stream_recv_execute, stream_recv_complete);
}

static napi_value native_stream_close(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    native_stream* stream = NULL;
    if (!unwrap_native_stream(env, this_arg, &stream)) {
        return NULL;
    }
    native_stream_close_request(stream);
    napi_value undefined = NULL;
    napi_get_undefined(env, &undefined);
    return undefined;
}

static napi_value init(napi_env env, napi_value exports) {
    napi_property_descriptor client_methods[] = {
        {"call", NULL, native_client_call, NULL, NULL, NULL, napi_default, NULL},
        {"startStream", NULL, native_client_start_stream, NULL, NULL, NULL, napi_default, NULL},
        {"close", NULL, native_client_close, NULL, NULL, NULL, napi_default, NULL},
    };
    napi_value client_ctor = NULL;
    napi_define_class(env,
        "NativeClient",
        NAPI_AUTO_LENGTH,
        native_client_constructor,
        NULL,
        sizeof(client_methods) / sizeof(client_methods[0]),
        client_methods,
        &client_ctor);
    napi_create_reference(env, client_ctor, 1, &NativeClientConstructor);

    napi_property_descriptor stream_methods[] = {
        {"sendMessage", NULL, native_stream_send_message, NULL, NULL, NULL, napi_default, NULL},
        {"finishSend", NULL, native_stream_finish_send, NULL, NULL, NULL, napi_default, NULL},
        {"recv", NULL, native_stream_recv, NULL, NULL, NULL, napi_default, NULL},
        {"close", NULL, native_stream_close, NULL, NULL, NULL, napi_default, NULL},
    };
    napi_value stream_ctor = NULL;
    napi_define_class(env,
        "NativeStream",
        NAPI_AUTO_LENGTH,
        native_stream_constructor,
        NULL,
        sizeof(stream_methods) / sizeof(stream_methods[0]),
        stream_methods,
        &stream_ctor);
    napi_create_reference(env, stream_ctor, 1, &NativeStreamConstructor);

    napi_property_descriptor exports_desc[] = {
        {"connectWebTransport", NULL, connect_webtransport, NULL, NULL, NULL, napi_default, NULL},
    };
    napi_define_properties(env, exports, sizeof(exports_desc) / sizeof(exports_desc[0]), exports_desc);
    return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, init)
