#define _POSIX_C_SOURCE 200809L

#include "trevrpc.h"

#include <errno.h> // IWYU pragma: keep
#include <node_api.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TREV_NODE_ERR_CLOSED -4001
#define TREV_NODE_RECV_MANY_DEFAULT 16u
#define TREV_NODE_RECV_MANY_LIMIT 256u

typedef struct native_client native_client;
typedef struct native_stream native_stream;
typedef struct native_server native_server;
typedef struct native_call native_call;
typedef struct native_cancellation native_cancellation;
typedef struct server_route server_route;

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

struct server_route {
    server_route* next;
    native_server* server;
    napi_ref handler_ref;
    char* service;
    char* method;
    uint32_t kind;
};

struct native_server {
    trevrpc_server* server;
    pthread_mutex_t mutex;
    server_route* routes;
    napi_env env;
    napi_threadsafe_function call_tsfn;
    uint16_t port;
    bool closing;
    bool serving;
    bool js_alive;
};

struct native_call {
    trevrpc_call* call;
    pthread_mutex_t mutex;
    size_t refs;
    bool completing;
    bool js_alive;
};

struct native_cancellation {
    trevrpc_cancellation* cancellation;
    bool js_alive;
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
    native_cancellation* cancellation;
    napi_ref cancellation_ref;
    trevrpc_request request;
    char* service;
    char* method;
    uint8_t* body;
    trevrpc_response* response;
} call_work;

typedef struct start_stream_work {
    base_work base;
    native_client* client;
    bool acquired;
    native_cancellation* cancellation;
    napi_ref cancellation_ref;
    trevrpc_request request;
    char* service;
    char* method;
    uint8_t* body;
    trevrpc_stream* stream;
} start_stream_work;

typedef struct stream_recv_work {
    base_work base;
    native_stream* stream;
    bool acquired;
    trevrpc_stream_frame* frame;
} stream_recv_work;

typedef struct stream_recv_many_work {
    base_work base;
    native_stream* stream;
    bool acquired;
    size_t max_frames;
    trevrpc_stream_frame** frames;
    size_t frames_len;
    size_t frames_cap;
    bool eof;
} stream_recv_many_work;

typedef struct listen_work {
    base_work base;
    trevrpc_server* server;
    char* host;
    char* path;
    char* origin;
    char* cert_file;
    char* key_file;
    uint16_t port;
    uint32_t max_sessions_per_connection;
    uint32_t max_streams_per_session;
    uint32_t idle_timeout_ms;
    int64_t max_stream_messages;
    size_t max_frame_size;
    uint16_t bound_port;
    bool has_max_stream_messages;
} listen_work;

typedef struct serve_work {
    base_work base;
    native_server* server;
} serve_work;

typedef struct call_response_work {
    base_work base;
    native_call* call;
    bool acquired;
    trevrpc_response response;
} call_response_work;

typedef struct call_finish_work {
    base_work base;
    native_call* call;
    bool acquired;
    uint32_t status;
    char* message;
    size_t message_len;
    trevrpc_metadata metadata;
} call_finish_work;

typedef struct call_recv_work {
    base_work base;
    native_call* call;
    bool acquired;
    trevrpc_stream_frame* frame;
} call_recv_work;

typedef struct call_recv_many_work {
    base_work base;
    native_call* call;
    bool acquired;
    size_t max_frames;
    trevrpc_stream_frame** frames;
    size_t frames_len;
    size_t frames_cap;
    bool eof;
} call_recv_many_work;

typedef struct server_call_event {
    server_route* route;
    native_call* call;
} server_call_event;

static napi_ref NativeClientConstructor;
static napi_ref NativeStreamConstructor;
static napi_ref NativeServerConstructor;
static napi_ref NativeCallConstructor;
static napi_ref NativeCancellationConstructor;

static void native_client_release(native_client* client);
static void native_stream_close_request(native_stream* stream);
static void native_server_close_request(native_server* server);
static void native_call_close_request(native_call* call);

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

static void throw_native_error(napi_env env, int err, const char* operation) {
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
    napi_throw(env, error);
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

static bool get_int64_property(napi_env env, napi_value object, const char* name, int64_t* out_value) {
    bool has_property = false;
    napi_has_named_property(env, object, name, &has_property);
    if (!has_property) {
        return false;
    }

    napi_value value = NULL;
    napi_get_named_property(env, object, name, &value);
    return napi_get_value_int64(env, value, out_value) == napi_ok;
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

static int bytes_arg_view(napi_env env, napi_value value, const uint8_t** out, size_t* out_len) {
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
        *out = data;
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
        *out = data;
        *out_len = len;
        return 0;
    }

    return -EINVAL;
}

static int copy_bytes_array_arg(napi_env env, napi_value value, uint8_t** out, size_t** out_lens, size_t* out_count) {
    *out = NULL;
    *out_lens = NULL;
    *out_count = 0;

    bool is_array = false;
    if (napi_is_array(env, value, &is_array) != napi_ok || !is_array) {
        return -EINVAL;
    }

    uint32_t length = 0;
    if (napi_get_array_length(env, value, &length) != napi_ok) {
        return -EINVAL;
    }
    if (length == 0) {
        return 0;
    }

    size_t* lens = calloc(length, sizeof(*lens));
    if (lens == NULL) {
        return -ENOMEM;
    }

    size_t total_len = 0;
    for (uint32_t i = 0; i < length; i++) {
        napi_value element = NULL;
        const uint8_t* body = NULL;
        size_t body_len = 0;
        if (napi_get_element(env, value, i, &element) != napi_ok ||
            bytes_arg_view(env, element, &body, &body_len) != 0) {
            free(lens);
            return -EINVAL;
        }
        if (body_len > SIZE_MAX - total_len) {
            free(lens);
            return -EOVERFLOW;
        }
        lens[i] = body_len;
        total_len += body_len;
    }

    uint8_t* bodies = NULL;
    if (total_len > 0) {
        bodies = malloc(total_len);
        if (bodies == NULL) {
            free(lens);
            return -ENOMEM;
        }
    }

    size_t offset = 0;
    for (uint32_t i = 0; i < length; i++) {
        if (lens[i] == 0) {
            continue;
        }
        napi_value element = NULL;
        const uint8_t* body = NULL;
        size_t body_len = 0;
        if (napi_get_element(env, value, i, &element) != napi_ok ||
            bytes_arg_view(env, element, &body, &body_len) != 0 || body_len != lens[i]) {
            free(bodies);
            free(lens);
            return -EINVAL;
        }
        if (bodies == NULL || body == NULL) {
            free(bodies);
            free(lens);
            return -EINVAL;
        }
        memcpy(bodies + offset, body, body_len);
        offset += body_len;
    }

    *out = bodies;
    *out_lens = lens;
    *out_count = length;
    return 0;
}

static napi_value promise_from_void_result(napi_env env, int err, const char* operation) {
    napi_value promise = NULL;
    napi_deferred deferred = NULL;
    napi_create_promise(env, &deferred, &promise);
    if (err == 0) {
        napi_value undefined = NULL;
        napi_get_undefined(env, &undefined);
        napi_resolve_deferred(env, deferred, undefined);
    } else {
        reject_native_error(env, deferred, err, operation);
    }
    return promise;
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

static napi_status set_bool(napi_env env, napi_value object, const char* name, bool value) {
    napi_value js_value = NULL;
    napi_get_boolean(env, value, &js_value);
    return napi_set_named_property(env, object, name, js_value);
}

static napi_status set_uint64_string(napi_env env, napi_value object, const char* name, uint64_t value) {
    char buffer[32];
    snprintf(buffer, sizeof(buffer), "%llu", (unsigned long long)value);
    napi_value js_value = NULL;
    napi_create_string_utf8(env, buffer, NAPI_AUTO_LENGTH, &js_value);
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

static int metadata_from_js(napi_env env, napi_value value, trevrpc_metadata* metadata) {
    napi_valuetype type = napi_undefined;
    napi_typeof(env, value, &type);
    if (type == napi_undefined || type == napi_null) {
        return 0;
    }
    if (type != napi_object) {
        return -EINVAL;
    }

    napi_value names = NULL;
    uint32_t length = 0;
    if (napi_get_property_names(env, value, &names) != napi_ok ||
        napi_get_array_length(env, names, &length) != napi_ok) {
        return -EINVAL;
    }
    for (uint32_t i = 0; i < length; i++) {
        napi_value key_value = NULL;
        napi_value entry_value = NULL;
        char* key = NULL;
        uint8_t* bytes = NULL;
        size_t bytes_len = 0;
        int err = 0;
        if (napi_get_element(env, names, i, &key_value) != napi_ok ||
            napi_get_property(env, value, key_value, &entry_value) != napi_ok) {
            return -EINVAL;
        }
        key = copy_string_value(env, key_value);
        if (key == NULL) {
            return -ENOMEM;
        }
        err = copy_bytes_arg(env, entry_value, &bytes, &bytes_len);
        if (err == 0) {
            err = trevrpc_metadata_set(metadata, key, strlen(key), bytes, bytes_len);
        }
        free(key);
        free(bytes);
        if (err != 0) {
            return err;
        }
    }
    return 0;
}

static int parse_uint64_decimal(const char* value, uint64_t* out) {
    if (value == NULL || value[0] == '\0') {
        return -EINVAL;
    }
    uint64_t result = 0;
    for (const char* cursor = value; *cursor != '\0'; cursor++) {
        if (*cursor < '0' || *cursor > '9') {
            return -EINVAL;
        }
        uint64_t digit = (uint64_t)(*cursor - '0');
        if (result > (UINT64_MAX - digit) / 10u) {
            return -EOVERFLOW;
        }
        result = result * 10u + digit;
    }
    *out = result;
    return 0;
}

static int uint64_from_js(napi_env env, napi_value value, uint64_t* out) {
    napi_valuetype type = napi_undefined;
    napi_typeof(env, value, &type);
    if (type == napi_undefined || type == napi_null) {
        *out = 0;
        return 0;
    }
    if (type == napi_number) {
        double number = 0;
        if (napi_get_value_double(env, value, &number) != napi_ok || !(number >= 0) || number > (double)UINT64_MAX) {
            return -EINVAL;
        }
        *out = (uint64_t)number;
        return 0;
    }

    napi_value string_value = NULL;
    if (napi_coerce_to_string(env, value, &string_value) != napi_ok) {
        return -EINVAL;
    }
    char* string = copy_string_value(env, string_value);
    if (string == NULL) {
        return -ENOMEM;
    }
    int err = parse_uint64_decimal(string, out);
    free(string);
    return err;
}

static int uint32_property_from_js(napi_env env, napi_value object, const char* name, uint32_t* value) {
    bool has_property = false;
    napi_has_named_property(env, object, name, &has_property);
    if (!has_property) {
        return 0;
    }
    napi_value property = NULL;
    napi_get_named_property(env, object, name, &property);
    return napi_get_value_uint32(env, property, value) == napi_ok ? 0 : -EINVAL;
}

static int copy_required_string_property(napi_env env, napi_value object, const char* name, char** out) {
    bool has_property = false;
    napi_has_named_property(env, object, name, &has_property);
    if (!has_property) {
        return -EINVAL;
    }
    napi_value property = NULL;
    napi_get_named_property(env, object, name, &property);
    *out = copy_string_value(env, property);
    return *out == NULL ? -ENOMEM : 0;
}

static int request_body_from_js(napi_env env, napi_value object, uint8_t** body, size_t* body_len) {
    bool has_property = false;
    napi_has_named_property(env, object, "body", &has_property);
    if (!has_property) {
        *body = NULL;
        *body_len = 0;
        return 0;
    }
    napi_value property = NULL;
    napi_get_named_property(env, object, "body", &property);
    return copy_bytes_arg(env, property, body, body_len);
}

static int request_metadata_from_js(napi_env env, napi_value object, trevrpc_metadata* metadata) {
    bool has_property = false;
    napi_has_named_property(env, object, "metadata", &has_property);
    if (!has_property) {
        return 0;
    }
    napi_value property = NULL;
    napi_get_named_property(env, object, "metadata", &property);
    return metadata_from_js(env, property, metadata);
}

static int request_timeout_from_js(napi_env env, napi_value object, uint64_t* timeout_nanos) {
    bool has_property = false;
    napi_has_named_property(env, object, "timeoutNanos", &has_property);
    if (!has_property) {
        *timeout_nanos = 0;
        return 0;
    }
    napi_value property = NULL;
    napi_get_named_property(env, object, "timeoutNanos", &property);
    return uint64_from_js(env, property, timeout_nanos);
}

static int client_request_from_js(napi_env env,
    napi_value value,
    uint32_t default_kind,
    trevrpc_request* request,
    char** service,
    char** method,
    uint8_t** body) {
    napi_valuetype type = napi_undefined;
    napi_typeof(env, value, &type);
    if (type != napi_object) {
        return -EINVAL;
    }

    memset(request, 0, sizeof(*request));
    request->kind = default_kind;
    request->version = TREVRPC_WIRE_VERSION;

    int err = copy_required_string_property(env, value, "service", service);
    if (err == 0) {
        request->service = *service;
        request->service_len = strlen(*service);
        err = copy_required_string_property(env, value, "method", method);
    }
    if (err == 0) {
        request->method = *method;
        request->method_len = strlen(*method);
        err = request_body_from_js(env, value, body, &request->body_len);
    }
    if (err == 0) {
        request->body = *body;
        err = request_metadata_from_js(env, value, &request->metadata);
    }
    if (err == 0) {
        err = uint32_property_from_js(env, value, "kind", &request->kind);
    }
    if (err == 0) {
        err = uint32_property_from_js(env, value, "version", &request->version);
    }
    if (err == 0) {
        err = request_timeout_from_js(env, value, &request->timeout_nanos);
    }
    return err;
}

static int legacy_request_from_args(napi_env env,
    napi_value* args,
    uint32_t kind,
    trevrpc_request* request,
    char** service,
    char** method,
    uint8_t** body) {
    memset(request, 0, sizeof(*request));
    request->kind = kind;
    request->version = TREVRPC_WIRE_VERSION;

    int err = copy_string_arg(env, args[0], service);
    if (err == 0) {
        request->service = *service;
        request->service_len = strlen(*service);
        err = copy_string_arg(env, args[1], method);
    }
    if (err == 0) {
        request->method = *method;
        request->method_len = strlen(*method);
        err = copy_bytes_arg(env, args[2], body, &request->body_len);
    }
    if (err == 0) {
        request->body = *body;
    }
    return err;
}

static napi_value request_to_js(napi_env env, const trevrpc_request* request) {
    napi_value object = NULL;
    napi_create_object(env, &object);
    if (request == NULL) {
        return object;
    }
    set_string_bytes(env, object, "service", request->service, request->service_len);
    set_string_bytes(env, object, "method", request->method, request->method_len);
    set_bytes(env, object, "body", request->body, request->body_len);
    set_uint32(env, object, "kind", request->kind);
    set_uint32(env, object, "version", request->version);
    if (request->timeout_nanos > 0) {
        set_uint64_string(env, object, "timeoutNanos", request->timeout_nanos);
    }
    napi_value metadata = metadata_to_js(env, &request->metadata);
    napi_set_named_property(env, object, "metadata", metadata);
    return object;
}

static int response_from_js(napi_env env, napi_value value, trevrpc_response* response) {
    napi_valuetype type = napi_undefined;
    napi_typeof(env, value, &type);
    if (type == napi_undefined || type == napi_null) {
        return 0;
    }
    if (type != napi_object) {
        return -EINVAL;
    }

    response->status = get_uint32_property(env, value, "status", TREVRPC_STATUS_OK);

    bool has_property = false;
    napi_has_named_property(env, value, "message", &has_property);
    if (has_property) {
        napi_value message = NULL;
        napi_get_named_property(env, value, "message", &message);
        char* copied = copy_string_value(env, message);
        if (copied == NULL) {
            return -ENOMEM;
        }
        int err = trevrpc_response_set_message(response, copied, strlen(copied));
        free(copied);
        if (err != 0) {
            return err;
        }
    }

    napi_has_named_property(env, value, "body", &has_property);
    if (has_property) {
        napi_value body = NULL;
        uint8_t* bytes = NULL;
        size_t bytes_len = 0;
        napi_get_named_property(env, value, "body", &body);
        int err = copy_bytes_arg(env, body, &bytes, &bytes_len);
        if (err == 0) {
            err = trevrpc_response_set_body(response, bytes, bytes_len);
        }
        free(bytes);
        if (err != 0) {
            return err;
        }
    }

    napi_has_named_property(env, value, "metadata", &has_property);
    if (has_property) {
        napi_value metadata = NULL;
        napi_get_named_property(env, value, "metadata", &metadata);
        int err = metadata_from_js(env, metadata, &response->metadata);
        if (err != 0) {
            return err;
        }
    }
    return 0;
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

static void stream_frame_list_reset(trevrpc_stream_frame** frames, size_t frames_len) {
    for (size_t i = 0; i < frames_len; i++) {
        trevrpc_stream_frame_free(frames[i]);
    }
    free(frames);
}

static int stream_frame_list_append(
    trevrpc_stream_frame*** frames, size_t* frames_len, size_t* frames_cap, trevrpc_stream_frame* frame) {
    if (*frames_len == *frames_cap) {
        size_t next_cap = *frames_cap == 0 ? 4 : *frames_cap * 2;
        if (next_cap < *frames_cap || next_cap > TREV_NODE_RECV_MANY_LIMIT) {
            next_cap = TREV_NODE_RECV_MANY_LIMIT;
        }
        if (*frames_len == next_cap) {
            return -ENOMEM;
        }
        trevrpc_stream_frame** next = realloc(*frames, next_cap * sizeof(**frames));
        if (next == NULL) {
            return -ENOMEM;
        }
        *frames = next;
        *frames_cap = next_cap;
    }
    (*frames)[(*frames_len)++] = frame;
    return 0;
}

static int recv_many_from_stream(trevrpc_stream* stream,
    size_t max_frames,
    trevrpc_stream_frame*** frames,
    size_t* frames_len,
    size_t* frames_cap,
    bool* eof) {
    trevrpc_stream_frame* frame = NULL;
    int err = trevrpc_stream_recv(stream, &frame);
    if (err != 0) {
        return err;
    }
    if (frame == NULL) {
        *eof = true;
        return 0;
    }

    err = stream_frame_list_append(frames, frames_len, frames_cap, frame);
    if (err != 0) {
        trevrpc_stream_frame_free(frame);
        return err;
    }
    if (frame->kind == TREVRPC_STREAM_FRAME_KIND_STATUS) {
        return 0;
    }

    while (*frames_len < max_frames) {
        int ready = 0;
        frame = NULL;
        err = trevrpc_stream_recv_ready(stream, &frame, &ready);
        if (err != 0) {
            return err;
        }
        if (!ready) {
            return 0;
        }
        if (frame == NULL) {
            *eof = true;
            return 0;
        }
        err = stream_frame_list_append(frames, frames_len, frames_cap, frame);
        if (err != 0) {
            trevrpc_stream_frame_free(frame);
            return err;
        }
        if (frame->kind == TREVRPC_STREAM_FRAME_KIND_STATUS) {
            return 0;
        }
    }
    return 0;
}

static napi_value stream_frame_list_to_js(napi_env env, trevrpc_stream_frame** frames, size_t frames_len, bool eof) {
    napi_value array = NULL;
    napi_create_array_with_length(env, frames_len + (eof ? 1 : 0), &array);
    for (size_t i = 0; i < frames_len; i++) {
        napi_value value = stream_frame_to_js(env, frames[i]);
        napi_set_element(env, array, (uint32_t)i, value);
    }
    if (eof) {
        napi_value null_value = NULL;
        napi_get_null(env, &null_value);
        napi_set_element(env, array, (uint32_t)frames_len, null_value);
    }
    return array;
}

static napi_value stream_body_batch_to_js(napi_env env, trevrpc_stream_frame** frames, size_t frames_len, bool eof) {
    if (frames_len == 0 && eof) {
        napi_value null_value = NULL;
        napi_get_null(env, &null_value);
        return null_value;
    }

    size_t body_count = 0;
    trevrpc_stream_frame* terminal = NULL;
    for (size_t i = 0; i < frames_len; i++) {
        trevrpc_stream_frame* frame = frames[i];
        if (frame->kind != TREVRPC_STREAM_FRAME_KIND_MESSAGE) {
            terminal = frame;
            break;
        }
        body_count++;
    }

    napi_value object = NULL;
    napi_value bodies = NULL;
    napi_create_object(env, &object);
    napi_create_array_with_length(env, body_count, &bodies);
    for (size_t i = 0; i < body_count; i++) {
        napi_value body = make_uint8_array(env, frames[i]->body, frames[i]->body_len);
        napi_set_element(env, bodies, (uint32_t)i, body);
    }
    napi_set_named_property(env, object, "bodies", bodies);

    if (terminal != NULL && terminal->kind == TREVRPC_STREAM_FRAME_KIND_STATUS) {
        napi_value status = stream_frame_to_js(env, terminal);
        napi_set_named_property(env, object, "status", status);
    } else {
        napi_value null_value = NULL;
        napi_get_null(env, &null_value);
        napi_set_named_property(env, object, "status", null_value);
    }
    if (terminal != NULL && terminal->kind != TREVRPC_STREAM_FRAME_KIND_STATUS) {
        set_uint32(env, object, "unknownFrameKind", terminal->kind);
    }
    set_bool(env, object, "eof", eof);
    return object;
}

static bool recv_batch_max_arg(napi_env env, size_t argc, napi_value* args, size_t* max_frames, const char* operation) {
    *max_frames = TREV_NODE_RECV_MANY_DEFAULT;
    if (argc == 0) {
        return true;
    }
    uint32_t value = 0;
    if (napi_get_value_uint32(env, args[0], &value) != napi_ok || value == 0) {
        char message[96];
        snprintf(message, sizeof(message), "%s requires a positive frame count", operation);
        napi_throw_type_error(env, NULL, message);
        return false;
    }
    *max_frames = value > TREV_NODE_RECV_MANY_LIMIT ? TREV_NODE_RECV_MANY_LIMIT : value;
    return true;
}

static bool recv_many_max_arg(napi_env env, size_t argc, napi_value* args, size_t* max_frames) {
    return recv_batch_max_arg(env, argc, args, max_frames, "recvMany");
}

static bool recv_body_batch_max_arg(napi_env env, size_t argc, napi_value* args, size_t* max_frames) {
    return recv_batch_max_arg(env, argc, args, max_frames, "recvBodyBatch");
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

static bool unwrap_native_server(napi_env env, napi_value receiver, native_server** out_server) {
    *out_server = NULL;
    if (napi_unwrap(env, receiver, (void**)out_server) != napi_ok || *out_server == NULL) {
        napi_throw_type_error(env, NULL, "invalid native server receiver");
        return false;
    }
    return true;
}

static bool unwrap_native_call(napi_env env, napi_value receiver, native_call** out_call) {
    *out_call = NULL;
    if (napi_unwrap(env, receiver, (void**)out_call) != napi_ok || *out_call == NULL) {
        napi_throw_type_error(env, NULL, "invalid native call receiver");
        return false;
    }
    return true;
}

static bool unwrap_native_cancellation(napi_env env, napi_value receiver, native_cancellation** out_cancellation) {
    *out_cancellation = NULL;
    if (napi_unwrap(env, receiver, (void**)out_cancellation) != napi_ok || *out_cancellation == NULL ||
        (*out_cancellation)->cancellation == NULL) {
        napi_throw_type_error(env, NULL, "invalid native cancellation receiver");
        return false;
    }
    return true;
}

static int optional_cancellation_arg(napi_env env,
    size_t argc,
    napi_value* args,
    size_t index,
    native_cancellation** out_cancellation,
    napi_ref* out_ref) {
    *out_cancellation = NULL;
    *out_ref = NULL;
    if (argc <= index) {
        return 0;
    }

    napi_valuetype type = napi_undefined;
    napi_typeof(env, args[index], &type);
    if (type == napi_undefined || type == napi_null) {
        return 0;
    }
    if (!unwrap_native_cancellation(env, args[index], out_cancellation)) {
        return -EINVAL;
    }
    if (napi_create_reference(env, args[index], 1, out_ref) != napi_ok) {
        return -ENOMEM;
    }
    return 0;
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

static int native_call_acquire(native_call* call, trevrpc_call** out_call) {
    if (call == NULL) {
        return TREV_NODE_ERR_CLOSED;
    }
    pthread_mutex_lock(&call->mutex);
    if (call->call == NULL || call->completing) {
        pthread_mutex_unlock(&call->mutex);
        return TREV_NODE_ERR_CLOSED;
    }
    call->refs++;
    *out_call = call->call;
    pthread_mutex_unlock(&call->mutex);
    return 0;
}

static int native_call_acquire_completion(native_call* call, trevrpc_call** out_call) {
    if (call == NULL) {
        return TREV_NODE_ERR_CLOSED;
    }
    pthread_mutex_lock(&call->mutex);
    if (call->call == NULL || call->completing) {
        pthread_mutex_unlock(&call->mutex);
        return TREV_NODE_ERR_CLOSED;
    }
    call->completing = true;
    call->refs++;
    *out_call = call->call;
    pthread_mutex_unlock(&call->mutex);
    return 0;
}

static void native_call_maybe_destroy(native_call* call) {
    bool destroy = false;
    pthread_mutex_lock(&call->mutex);
    destroy = !call->js_alive && call->refs == 0 && call->call == NULL;
    pthread_mutex_unlock(&call->mutex);
    if (destroy) {
        pthread_mutex_destroy(&call->mutex);
        free(call);
    }
}

static void native_call_release(native_call* call) {
    trevrpc_call* close_call = NULL;
    bool destroy = false;
    pthread_mutex_lock(&call->mutex);
    if (call->refs > 0) {
        call->refs--;
    }
    if (!call->js_alive && call->completing && call->refs == 0 && call->call != NULL) {
        close_call = call->call;
        call->call = NULL;
    }
    destroy = !call->js_alive && call->refs == 0 && call->call == NULL;
    pthread_mutex_unlock(&call->mutex);

    trevrpc_call_close(close_call);
    if (destroy) {
        pthread_mutex_destroy(&call->mutex);
        free(call);
    }
}

static void native_call_complete(native_call* call) {
    bool destroy = false;
    pthread_mutex_lock(&call->mutex);
    call->call = NULL;
    if (call->refs > 0) {
        call->refs--;
    }
    destroy = !call->js_alive && call->refs == 0;
    pthread_mutex_unlock(&call->mutex);
    if (destroy) {
        pthread_mutex_destroy(&call->mutex);
        free(call);
    }
}

static void native_call_close_request(native_call* call) {
    if (call == NULL) {
        return;
    }
    trevrpc_call* close_call = NULL;
    pthread_mutex_lock(&call->mutex);
    call->completing = true;
    if (call->refs == 0 && call->call != NULL) {
        close_call = call->call;
        call->call = NULL;
    }
    pthread_mutex_unlock(&call->mutex);

    trevrpc_call_close(close_call);
    native_call_maybe_destroy(call);
}

static void free_server_routes(napi_env env, server_route* route) {
    while (route != NULL) {
        server_route* next = route->next;
        if (route->handler_ref != NULL) {
            napi_delete_reference(env, route->handler_ref);
        }
        free(route->service);
        free(route->method);
        free(route);
        route = next;
    }
}

static void native_server_close_request(native_server* server) {
    if (server == NULL) {
        return;
    }
    trevrpc_server* close_server = NULL;
    trevrpc_server* shutdown_server = NULL;
    pthread_mutex_lock(&server->mutex);
    server->closing = true;
    if (!server->serving && server->server != NULL) {
        close_server = server->server;
        server->server = NULL;
    } else {
        shutdown_server = server->server;
    }
    pthread_mutex_unlock(&server->mutex);

    trevrpc_server_shutdown(shutdown_server);
    trevrpc_server_close(close_server);
    if (close_server != NULL) {
        free_server_routes(server->env, server->routes);
        server->routes = NULL;
        napi_threadsafe_function tsfn = NULL;
        pthread_mutex_lock(&server->mutex);
        tsfn = server->call_tsfn;
        server->call_tsfn = NULL;
        pthread_mutex_unlock(&server->mutex);
        if (tsfn != NULL) {
            napi_release_threadsafe_function(tsfn, napi_tsfn_abort);
        }
    }
}

static void native_server_close_after_serve(native_server* server) {
    trevrpc_server* close_server = NULL;
    napi_threadsafe_function tsfn = NULL;
    pthread_mutex_lock(&server->mutex);
    server->serving = false;
    if (server->closing && server->server != NULL) {
        close_server = server->server;
        server->server = NULL;
        tsfn = server->call_tsfn;
        server->call_tsfn = NULL;
    }
    pthread_mutex_unlock(&server->mutex);
    trevrpc_server_close(close_server);
    if (tsfn != NULL) {
        napi_release_threadsafe_function(tsfn, napi_tsfn_abort);
    }
    if (close_server != NULL) {
        free_server_routes(server->env, server->routes);
        server->routes = NULL;
    }
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

static void native_server_finalize(napi_env env, void* data, void* hint) {
    (void)hint;
    native_server* server = data;
    pthread_mutex_lock(&server->mutex);
    server->js_alive = false;
    pthread_mutex_unlock(&server->mutex);
    native_server_close_request(server);
    if (server->call_tsfn != NULL) {
        napi_release_threadsafe_function(server->call_tsfn, napi_tsfn_abort);
        server->call_tsfn = NULL;
    }
    free_server_routes(env, server->routes);
    pthread_mutex_destroy(&server->mutex);
    free(server);
}

static void native_call_finalize(napi_env env, void* data, void* hint) {
    (void)env;
    (void)hint;
    native_call* call = data;
    pthread_mutex_lock(&call->mutex);
    call->js_alive = false;
    pthread_mutex_unlock(&call->mutex);
    native_call_close_request(call);
}

static void native_cancellation_finalize(napi_env env, void* data, void* hint) {
    (void)env;
    (void)hint;
    native_cancellation* cancellation = data;
    if (cancellation == NULL) {
        return;
    }
    cancellation->js_alive = false;
    trevrpc_cancellation_free(cancellation->cancellation);
    cancellation->cancellation = NULL;
    free(cancellation);
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

static napi_value native_server_constructor(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    if (napi_get_cb_info(env, info, &argc, args, &this_arg, NULL) != napi_ok) {
        return NULL;
    }

    native_server* server = NULL;
    if (argc != 1 || napi_get_value_external(env, args[0], (void**)&server) != napi_ok || server == NULL) {
        napi_throw_type_error(env, NULL, "native servers cannot be constructed directly");
        return NULL;
    }

    if (napi_wrap(env, this_arg, server, native_server_finalize, NULL, NULL) != napi_ok) {
        throw_if_no_pending_exception(env, "failed to wrap native server");
        return NULL;
    }
    server->js_alive = true;

    napi_value port = NULL;
    napi_create_uint32(env, server->port, &port);
    napi_set_named_property(env, this_arg, "port", port);
    return this_arg;
}

static napi_value native_call_constructor(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    if (napi_get_cb_info(env, info, &argc, args, &this_arg, NULL) != napi_ok) {
        return NULL;
    }

    native_call* call = NULL;
    if (argc != 1 || napi_get_value_external(env, args[0], (void**)&call) != napi_ok || call == NULL) {
        napi_throw_type_error(env, NULL, "native calls cannot be constructed directly");
        return NULL;
    }

    if (napi_wrap(env, this_arg, call, native_call_finalize, NULL, NULL) != napi_ok) {
        throw_if_no_pending_exception(env, "failed to wrap native call");
        return NULL;
    }
    call->js_alive = true;

    trevrpc_call* c_call = NULL;
    if (native_call_acquire(call, &c_call) == 0) {
        napi_value request = request_to_js(env, trevrpc_call_request(c_call));
        napi_set_named_property(env, this_arg, "request", request);
        native_call_release(call);
    }
    return this_arg;
}

static napi_value native_cancellation_constructor(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    if (napi_get_cb_info(env, info, &argc, args, &this_arg, NULL) != napi_ok) {
        return NULL;
    }

    native_cancellation* cancellation = NULL;
    if (argc != 1 || napi_get_value_external(env, args[0], (void**)&cancellation) != napi_ok || cancellation == NULL) {
        napi_throw_type_error(env, NULL, "native cancellations cannot be constructed directly");
        return NULL;
    }

    if (napi_wrap(env, this_arg, cancellation, native_cancellation_finalize, NULL, NULL) != napi_ok) {
        throw_if_no_pending_exception(env, "failed to wrap native cancellation");
        return NULL;
    }
    cancellation->js_alive = true;
    return this_arg;
}

static void server_call_event_close(server_call_event* event) {
    if (event == NULL) {
        return;
    }
    native_call_close_request(event->call);
    free(event);
}

static void server_call_js(napi_env env, napi_value js_callback, void* context, void* data) {
    (void)js_callback;
    (void)context;
    server_call_event* event = data;
    if (env == NULL || event == NULL) {
        server_call_event_close(event);
        return;
    }

    napi_value handler = NULL;
    napi_value ctor = NULL;
    napi_value external = NULL;
    napi_value call_object = NULL;
    napi_value global = NULL;
    napi_status status = napi_get_reference_value(env, event->route->handler_ref, &handler);
    if (status == napi_ok) {
        status = napi_get_reference_value(env, NativeCallConstructor, &ctor);
    }
    if (status == napi_ok) {
        status = napi_create_external(env, event->call, NULL, NULL, &external);
    }
    if (status == napi_ok) {
        status = napi_new_instance(env, ctor, 1, &external, &call_object);
    }
    if (status == napi_ok) {
        status = napi_get_global(env, &global);
    }
    if (status == napi_ok) {
        napi_value ignored = NULL;
        status = napi_call_function(env, global, handler, 1, &call_object, &ignored);
    }
    if (status != napi_ok) {
        clear_pending_exception(env);
        native_call_close_request(event->call);
    }
    free(event);
}

static int native_server_call_handler(void* user_data, trevrpc_call* call) {
    server_route* route = user_data;
    if (route == NULL || route->server == NULL || call == NULL) {
        return -EINVAL;
    }
    int err = trevrpc_call_defer(call);
    if (err != 0) {
        return err;
    }

    native_call* native = calloc(1, sizeof(*native));
    server_call_event* event = calloc(1, sizeof(*event));
    if (native == NULL || event == NULL) {
        free(native);
        free(event);
        trevrpc_call_close(call);
        return TREVRPC_CALL_DEFERRED;
    }
    pthread_mutex_init(&native->mutex, NULL);
    native->call = call;
    event->route = route;
    event->call = native;

    pthread_mutex_lock(&route->server->mutex);
    napi_threadsafe_function tsfn = route->server->call_tsfn;
    pthread_mutex_unlock(&route->server->mutex);
    if (tsfn == NULL) {
        native_call_close_request(native);
        free(event);
        return TREVRPC_CALL_DEFERRED;
    }

    napi_status status = napi_acquire_threadsafe_function(tsfn);
    if (status == napi_ok) {
        status = napi_call_threadsafe_function(tsfn, event, napi_tsfn_nonblocking);
        napi_release_threadsafe_function(tsfn, napi_tsfn_release);
    }
    if (status != napi_ok) {
        native_call_close_request(native);
        free(event);
    }
    return TREVRPC_CALL_DEFERRED;
}

static napi_value noop_js_callback(napi_env env, napi_callback_info info) {
    (void)info;
    napi_value undefined = NULL;
    napi_get_undefined(env, &undefined);
    return undefined;
}

static void connect_execute(napi_env env, void* data) {
    (void)env;
    connect_work* work = data;
    trevrpc_config config = trevrpc_default_config();
    if (work->idle_timeout_ms > 0) {
        config.max_idle_timeout_ms = work->idle_timeout_ms;
    }
    if (work->max_streams_per_session > 0) {
        config.peer_bidi_stream_count =
            work->max_streams_per_session > UINT16_MAX ? UINT16_MAX : (uint16_t)work->max_streams_per_session;
    }
    config.max_stateless_operations = 1024;
    config.max_binding_stateless_operations = 256;
    if (work->max_frame_size > 0) {
        config.max_frame_size = work->max_frame_size;
    }
    config.ca_cert_file = work->ca_cert_file;
    config.skip_certificate_validation = work->skip_certificate_validation;
    work->base.err = trevrpc_client_connect(work->host, work->port, &config, &work->client);
}

static void connect_complete(napi_env env, napi_status status, void* data) {
    connect_work* work = data;
    if (status != napi_ok) {
        reject_native_error(env, work->base.deferred, -ECANCELED, "connectMsQuic");
    } else if (work->base.err != 0) {
        reject_native_error(env, work->base.deferred, work->base.err, "connectMsQuic");
    } else {
        native_client* client = calloc(1, sizeof(*client));
        if (client == NULL) {
            trevrpc_client_close(work->client);
            reject_native_error(env, work->base.deferred, -ENOMEM, "connectMsQuic");
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
                reject_native_error(env, work->base.deferred, -ENOMEM, "connectMsQuic");
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

static napi_value connect_msquic(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, NULL, NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "connectMsQuic requires an options object");
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
        napi_throw_type_error(env, NULL, "connectMsQuic requires host and port");
        return NULL;
    }

    return queue_work(env, &work->base, "connectMsQuic", connect_execute, connect_complete);
}

static void listen_execute(napi_env env, void* data) {
    (void)env;
    listen_work* work = data;
    trevrpc_server_config config = trevrpc_default_server_config();
    config.host = work->host;
    config.port = work->port;
    config.cert_file = work->cert_file;
    config.key_file = work->key_file;
    config.keep_alive_ms = 15000;
    config.peer_bidi_stream_count = 128;
    config.max_stateless_operations = 1024;
    config.max_binding_stateless_operations = 256;
    config.webtransport_path = work->path;
    config.webtransport_origin = work->origin;
    config.max_sessions_per_connection = work->max_sessions_per_connection;
    config.max_streams_per_session = work->max_streams_per_session;
    config.max_idle_timeout_ms = work->idle_timeout_ms;
    if (work->max_frame_size > 0) {
        config.max_frame_size = work->max_frame_size;
    }
    work->base.err = trevrpc_server_listen(&config, &work->server);
    if (work->base.err == 0) {
        work->base.err = trevrpc_server_port(work->server, &work->bound_port);
    }
    if (work->base.err == 0 && work->has_max_stream_messages) {
        trevrpc_server_options options = trevrpc_default_server_options();
        options.max_stream_messages = work->max_stream_messages;
        work->base.err = trevrpc_server_set_options(work->server, &options);
    }
}

static void listen_complete(napi_env env, napi_status status, void* data) {
    listen_work* work = data;
    if (status != napi_ok) {
        reject_native_error(env, work->base.deferred, -ECANCELED, "listenMsQuic");
    } else if (work->base.err != 0) {
        trevrpc_server_close(work->server);
        reject_native_error(env, work->base.deferred, work->base.err, "listenMsQuic");
    } else {
        native_server* server = calloc(1, sizeof(*server));
        if (server == NULL) {
            trevrpc_server_close(work->server);
            reject_native_error(env, work->base.deferred, -ENOMEM, "listenMsQuic");
        } else {
            pthread_mutex_init(&server->mutex, NULL);
            server->server = work->server;
            server->env = env;
            server->port = work->bound_port;

            napi_value callback = NULL;
            napi_value resource_name = NULL;
            napi_status wrap_status =
                napi_create_function(env, "serverCall", NAPI_AUTO_LENGTH, noop_js_callback, NULL, &callback);
            if (wrap_status == napi_ok) {
                wrap_status = napi_create_string_utf8(env, "TrevRpcServerCall", NAPI_AUTO_LENGTH, &resource_name);
            }
            if (wrap_status == napi_ok) {
                wrap_status = napi_create_threadsafe_function(
                    env, callback, NULL, resource_name, 0, 1, NULL, NULL, NULL, server_call_js, &server->call_tsfn);
            }

            napi_value ctor = NULL;
            napi_value external = NULL;
            napi_value instance = NULL;
            if (wrap_status == napi_ok) {
                wrap_status = napi_get_reference_value(env, NativeServerConstructor, &ctor);
            }
            if (wrap_status == napi_ok) {
                wrap_status = napi_create_external(env, server, NULL, NULL, &external);
            }
            if (wrap_status == napi_ok) {
                wrap_status = napi_new_instance(env, ctor, 1, &external, &instance);
            }
            if (wrap_status != napi_ok) {
                clear_pending_exception(env);
                if (server->call_tsfn != NULL) {
                    napi_release_threadsafe_function(server->call_tsfn, napi_tsfn_abort);
                }
                trevrpc_server_close(work->server);
                pthread_mutex_destroy(&server->mutex);
                free(server);
                reject_native_error(env, work->base.deferred, -ENOMEM, "listenMsQuic");
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
    napi_delete_async_work(env, work->base.work);
    free(work);
}

static napi_value listen_msquic(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, NULL, NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "listenMsQuic requires an options object");
        return NULL;
    }

    listen_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate listen work");
        return NULL;
    }
    work->host = get_string_property(env, args[0], "host");
    work->path = get_string_property(env, args[0], "path");
    work->origin = get_string_property(env, args[0], "origin");
    work->cert_file = get_string_property(env, args[0], "certFile");
    work->key_file = get_string_property(env, args[0], "keyFile");
    work->port = (uint16_t)get_uint32_property(env, args[0], "port", 0);
    work->max_sessions_per_connection = get_uint32_property(env, args[0], "maxSessionsPerConnection", 16);
    work->max_streams_per_session = get_uint32_property(env, args[0], "maxStreamsPerSession", 128);
    work->idle_timeout_ms = get_uint32_property(env, args[0], "idleTimeoutMs", 30000);
    work->has_max_stream_messages = get_int64_property(env, args[0], "maxStreamMessages", &work->max_stream_messages);
    get_size_property(env, args[0], "maxFrameSize", &work->max_frame_size);

    if (work->host == NULL || work->cert_file == NULL || work->key_file == NULL) {
        free(work->host);
        free(work->path);
        free(work->origin);
        free(work->cert_file);
        free(work->key_file);
        free(work);
        napi_throw_type_error(env, NULL, "listenMsQuic requires host, certFile, and keyFile");
        return NULL;
    }

    return queue_work(env, &work->base, "listenMsQuic", listen_execute, listen_complete);
}

static napi_value native_server_register(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value args[4];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    if (argc != 4) {
        napi_throw_type_error(env, NULL, "register requires service, method, kind, and handler");
        return NULL;
    }

    native_server* server = NULL;
    if (!unwrap_native_server(env, this_arg, &server)) {
        return NULL;
    }
    server_route* route = calloc(1, sizeof(*route));
    if (route == NULL) {
        napi_throw_error(env, NULL, "failed to allocate server route");
        return NULL;
    }
    int err = copy_string_arg(env, args[0], &route->service);
    if (err == 0) {
        err = copy_string_arg(env, args[1], &route->method);
    }
    if (err == 0 && napi_get_value_uint32(env, args[2], &route->kind) != napi_ok) {
        err = -EINVAL;
    }
    napi_valuetype handler_type = napi_undefined;
    if (err == 0) {
        napi_typeof(env, args[3], &handler_type);
        if (handler_type != napi_function) {
            err = -EINVAL;
        }
    }
    if (err == 0 && napi_create_reference(env, args[3], 1, &route->handler_ref) != napi_ok) {
        err = -ENOMEM;
    }
    if (err == 0) {
        route->server = server;
        pthread_mutex_lock(&server->mutex);
        trevrpc_server* c_server = server->server;
        bool closing = server->closing || c_server == NULL;
        pthread_mutex_unlock(&server->mutex);
        err = closing ? TREV_NODE_ERR_CLOSED
                      : trevrpc_server_register_call(
                            c_server, route->service, route->method, route->kind, native_server_call_handler, route);
    }
    if (err != 0) {
        if (route->handler_ref != NULL) {
            napi_delete_reference(env, route->handler_ref);
        }
        free(route->service);
        free(route->method);
        free(route);
        throw_native_error(env, err, "register");
        return NULL;
    }

    pthread_mutex_lock(&server->mutex);
    route->next = server->routes;
    server->routes = route;
    pthread_mutex_unlock(&server->mutex);

    napi_value undefined = NULL;
    napi_get_undefined(env, &undefined);
    return undefined;
}

static void serve_execute(napi_env env, void* data) {
    (void)env;
    serve_work* work = data;
    native_server* server = work->server;
    trevrpc_server* c_server = NULL;
    pthread_mutex_lock(&server->mutex);
    c_server = server->server;
    pthread_mutex_unlock(&server->mutex);
    work->base.err = c_server == NULL ? TREV_NODE_ERR_CLOSED : trevrpc_server_serve(c_server);
}

static void serve_complete(napi_env env, napi_status status, void* data) {
    serve_work* work = data;
    native_server_close_after_serve(work->server);
    if (status != napi_ok) {
        reject_native_error(env, work->base.deferred, -ECANCELED, "serve");
    } else if (work->base.err != 0) {
        reject_native_error(env, work->base.deferred, work->base.err, "serve");
    } else {
        napi_value undefined = NULL;
        napi_get_undefined(env, &undefined);
        napi_resolve_deferred(env, work->base.deferred, undefined);
    }
    if (work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    napi_delete_async_work(env, work->base.work);
    free(work);
}

static napi_value native_server_serve(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    native_server* server = NULL;
    if (!unwrap_native_server(env, this_arg, &server)) {
        return NULL;
    }
    serve_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate serve work");
        return NULL;
    }
    if (!create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    pthread_mutex_lock(&server->mutex);
    bool can_start = !server->closing && !server->serving && server->server != NULL;
    if (can_start) {
        server->serving = true;
    }
    pthread_mutex_unlock(&server->mutex);
    if (!can_start) {
        napi_delete_reference(env, work->base.receiver_ref);
        free(work);
        napi_throw_error(env, NULL, "server is closed or already serving");
        return NULL;
    }
    work->server = server;
    return queue_work(env, &work->base, "serve", serve_execute, serve_complete);
}

static napi_value native_server_close(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    native_server* server = NULL;
    if (!unwrap_native_server(env, this_arg, &server)) {
        return NULL;
    }
    native_server_close_request(server);
    napi_value undefined = NULL;
    napi_get_undefined(env, &undefined);
    return undefined;
}

static napi_value native_client_create_cancellation(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    native_client* client = NULL;
    if (!unwrap_native_client(env, this_arg, &client)) {
        return NULL;
    }

    native_cancellation* cancellation = calloc(1, sizeof(*cancellation));
    if (cancellation == NULL) {
        napi_throw_error(env, NULL, "failed to allocate cancellation");
        return NULL;
    }
    cancellation->cancellation = trevrpc_cancellation_new();
    if (cancellation->cancellation == NULL) {
        free(cancellation);
        napi_throw_error(env, NULL, "failed to allocate cancellation");
        return NULL;
    }

    napi_value ctor = NULL;
    napi_value external = NULL;
    napi_value instance = NULL;
    napi_status status = napi_get_reference_value(env, NativeCancellationConstructor, &ctor);
    if (status == napi_ok) {
        status = napi_create_external(env, cancellation, NULL, NULL, &external);
    }
    if (status == napi_ok) {
        status = napi_new_instance(env, ctor, 1, &external, &instance);
    }
    if (status != napi_ok) {
        clear_pending_exception(env);
        trevrpc_cancellation_free(cancellation->cancellation);
        free(cancellation);
        napi_throw_error(env, NULL, "failed to create cancellation");
        return NULL;
    }
    return instance;
}

static napi_value native_cancellation_cancel(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    native_cancellation* cancellation = NULL;
    if (!unwrap_native_cancellation(env, this_arg, &cancellation)) {
        return NULL;
    }
    trevrpc_cancellation_cancel(cancellation->cancellation);
    napi_value undefined = NULL;
    napi_get_undefined(env, &undefined);
    return undefined;
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
    work->base.err = trevrpc_client_call_request_cancellable(
        client, &work->request, work->cancellation != NULL ? work->cancellation->cancellation : NULL, &work->response);
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
    if (work->cancellation_ref != NULL) {
        napi_delete_reference(env, work->cancellation_ref);
    }
    napi_delete_async_work(env, work->base.work);
    trevrpc_metadata_reset(&work->request.metadata);
    free(work->service);
    free(work->method);
    free(work->body);
    free(work);
}

static napi_value native_client_call(napi_env env, napi_callback_info info) {
    size_t argc = 4;
    napi_value args[4];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    if (argc != 1 && argc != 2 && argc != 3 && argc != 4) {
        napi_throw_type_error(env, NULL, "call requires a request object or service, method, and body");
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
    int err =
        argc <= 2
            ? client_request_from_js(
                  env, args[0], TREVRPC_RPC_KIND_UNARY, &work->request, &work->service, &work->method, &work->body)
            : legacy_request_from_args(
                  env, args, TREVRPC_RPC_KIND_UNARY, &work->request, &work->service, &work->method, &work->body);
    if (err == 0) {
        err =
            optional_cancellation_arg(env, argc, args, argc <= 2 ? 1 : 3, &work->cancellation, &work->cancellation_ref);
    }
    if (err != 0) {
        napi_delete_reference(env, work->base.receiver_ref);
        if (work->cancellation_ref != NULL) {
            napi_delete_reference(env, work->cancellation_ref);
        }
        trevrpc_metadata_reset(&work->request.metadata);
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
    work->base.err = trevrpc_client_start_stream_request_cancellable(
        client, &work->request, work->cancellation != NULL ? work->cancellation->cancellation : NULL, &work->stream);
}

static void start_stream_complete(napi_env env, napi_status status, void* data) {
    start_stream_work* work = data;
    bool owner_transferred = false;
    if (work->base.err == 0 && status == napi_ok) {
        native_stream* stream = calloc(1, sizeof(*stream));
        if (stream == NULL) {
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
                owner_transferred = true;
                work->stream = NULL;
                reject_native_error(env, work->base.deferred, -ENOMEM, "startStream");
            } else {
                owner_transferred = true;
                work->stream = NULL;
                napi_resolve_deferred(env, work->base.deferred, instance);
            }
        }
    } else {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "startStream");
    }

    if (work->stream != NULL) {
        trevrpc_stream_close(work->stream);
        work->stream = NULL;
    }
    if (work->acquired && !owner_transferred) {
        native_client_release(work->client);
    }
    if (work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    if (work->cancellation_ref != NULL) {
        napi_delete_reference(env, work->cancellation_ref);
    }
    napi_delete_async_work(env, work->base.work);
    trevrpc_metadata_reset(&work->request.metadata);
    free(work->service);
    free(work->method);
    free(work->body);
    free(work);
}

static napi_value native_client_start_stream(napi_env env, napi_callback_info info) {
    size_t argc = 5;
    napi_value args[5];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    if (argc != 1 && argc != 2 && argc != 4 && argc != 5) {
        napi_throw_type_error(env, NULL, "startStream requires a request object or service, method, kind, and body");
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
    int err = 0;
    if (argc <= 2) {
        err = client_request_from_js(env,
            args[0],
            TREVRPC_RPC_KIND_SERVER_STREAMING,
            &work->request,
            &work->service,
            &work->method,
            &work->body);
    } else {
        uint32_t kind = 0;
        if (napi_get_value_uint32(env, args[2], &kind) != napi_ok) {
            err = -EINVAL;
        }
        if (err == 0) {
            napi_value legacy_args[3] = {args[0], args[1], args[3]};
            err = legacy_request_from_args(
                env, legacy_args, kind, &work->request, &work->service, &work->method, &work->body);
        }
    }
    if (err == 0) {
        err =
            optional_cancellation_arg(env, argc, args, argc <= 2 ? 1 : 4, &work->cancellation, &work->cancellation_ref);
    }
    if (err != 0) {
        napi_delete_reference(env, work->base.receiver_ref);
        if (work->cancellation_ref != NULL) {
            napi_delete_reference(env, work->cancellation_ref);
        }
        trevrpc_metadata_reset(&work->request.metadata);
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

static napi_value native_stream_send_message(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "sendMessage requires a body");
        return NULL;
    }
    native_stream* stream = NULL;
    if (!unwrap_native_stream(env, this_arg, &stream)) {
        return NULL;
    }
    const uint8_t* body = NULL;
    size_t body_len = 0;
    if (bytes_arg_view(env, args[0], &body, &body_len) != 0) {
        napi_throw_type_error(env, NULL, "invalid sendMessage body");
        return NULL;
    }

    trevrpc_stream* c_stream = NULL;
    int err = native_stream_acquire(stream, &c_stream);
    if (err == 0) {
        err = trevrpc_stream_send_message(c_stream, body, body_len);
        native_stream_release(stream);
    }
    return promise_from_void_result(env, err, "sendMessage");
}

static napi_value native_stream_send_messages(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "sendMessages requires an array of bodies");
        return NULL;
    }
    native_stream* stream = NULL;
    if (!unwrap_native_stream(env, this_arg, &stream)) {
        return NULL;
    }

    uint8_t* bodies = NULL;
    size_t* body_lens = NULL;
    size_t count = 0;
    int err = copy_bytes_array_arg(env, args[0], &bodies, &body_lens, &count);
    if (err != 0) {
        if (err == -ENOMEM) {
            napi_throw_error(env, NULL, "failed to allocate sendMessages bodies");
        } else {
            napi_throw_type_error(env, NULL, "invalid sendMessages bodies");
        }
        return NULL;
    }

    trevrpc_stream* c_stream = NULL;
    err = native_stream_acquire(stream, &c_stream);
    if (err == 0) {
        err = trevrpc_stream_send_messages(c_stream, bodies, body_lens, count);
        native_stream_release(stream);
    }
    free(bodies);
    free(body_lens);
    return promise_from_void_result(env, err, "sendMessages");
}

static napi_value native_stream_finish_send(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    native_stream* stream = NULL;
    if (!unwrap_native_stream(env, this_arg, &stream)) {
        return NULL;
    }

    trevrpc_stream* c_stream = NULL;
    int err = native_stream_acquire(stream, &c_stream);
    if (err == 0) {
        err = trevrpc_stream_finish_send(c_stream);
        native_stream_release(stream);
    }
    return promise_from_void_result(env, err, "finishSend");
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

static void stream_recv_many_execute(napi_env env, void* data) {
    (void)env;
    stream_recv_many_work* work = data;
    trevrpc_stream* stream = NULL;
    work->base.err = native_stream_acquire(work->stream, &stream);
    if (work->base.err != 0) {
        return;
    }
    work->acquired = true;
    work->base.err = recv_many_from_stream(
        stream, work->max_frames, &work->frames, &work->frames_len, &work->frames_cap, &work->eof);
}

static void stream_recv_many_complete(napi_env env, napi_status status, void* data) {
    stream_recv_many_work* work = data;
    if (work->base.err == 0 && status == napi_ok) {
        napi_value frames = stream_frame_list_to_js(env, work->frames, work->frames_len, work->eof);
        napi_resolve_deferred(env, work->base.deferred, frames);
    } else {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "recvMany");
    }
    stream_frame_list_reset(work->frames, work->frames_len);
    if (work->acquired) {
        native_stream_release(work->stream);
    }
    if (work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    napi_delete_async_work(env, work->base.work);
    free(work);
}

static napi_value native_stream_recv_many(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    stream_recv_many_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate recvMany work");
        return NULL;
    }
    if (!recv_many_max_arg(env, argc, args, &work->max_frames) || !unwrap_native_stream(env, this_arg, &work->stream) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    return queue_work(env, &work->base, "recvMany", stream_recv_many_execute, stream_recv_many_complete);
}

static void stream_recv_body_batch_complete(napi_env env, napi_status status, void* data) {
    stream_recv_many_work* work = data;
    if (work->base.err == 0 && status == napi_ok) {
        napi_value batch = stream_body_batch_to_js(env, work->frames, work->frames_len, work->eof);
        napi_resolve_deferred(env, work->base.deferred, batch);
    } else {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "recvBodyBatch");
    }
    stream_frame_list_reset(work->frames, work->frames_len);
    if (work->acquired) {
        native_stream_release(work->stream);
    }
    if (work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    napi_delete_async_work(env, work->base.work);
    free(work);
}

static napi_value native_stream_recv_body_batch(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    stream_recv_many_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate recvBodyBatch work");
        return NULL;
    }
    if (!recv_body_batch_max_arg(env, argc, args, &work->max_frames) ||
        !unwrap_native_stream(env, this_arg, &work->stream) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    return queue_work(env, &work->base, "recvBodyBatch", stream_recv_many_execute, stream_recv_body_batch_complete);
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

static void call_respond_execute(napi_env env, void* data) {
    (void)env;
    call_response_work* work = data;
    trevrpc_call* call = NULL;
    work->base.err = native_call_acquire_completion(work->call, &call);
    if (work->base.err != 0) {
        return;
    }
    work->acquired = true;
    work->base.err = trevrpc_call_respond(call, &work->response);
    if (work->base.err != 0) {
        trevrpc_call_close(call);
    }
}

static void call_respond_complete(napi_env env, napi_status status, void* data) {
    call_response_work* work = data;
    if (status == napi_ok && work->base.err == 0) {
        napi_value undefined = NULL;
        napi_get_undefined(env, &undefined);
        napi_resolve_deferred(env, work->base.deferred, undefined);
    } else {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "respond");
    }
    if (work->acquired) {
        native_call_complete(work->call);
    }
    if (work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    napi_delete_async_work(env, work->base.work);
    trevrpc_response_reset(&work->response);
    free(work);
}

static napi_value native_call_respond(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "respond requires a response object");
        return NULL;
    }
    call_response_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate respond work");
        return NULL;
    }
    if (!unwrap_native_call(env, this_arg, &work->call) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    int err = response_from_js(env, args[0], &work->response);
    if (err != 0) {
        napi_delete_reference(env, work->base.receiver_ref);
        trevrpc_response_reset(&work->response);
        free(work);
        napi_throw_type_error(env, NULL, "invalid response object");
        return NULL;
    }
    return queue_work(env, &work->base, "respond", call_respond_execute, call_respond_complete);
}

static napi_value native_call_send_message(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "sendMessage requires a body");
        return NULL;
    }
    native_call* call = NULL;
    if (!unwrap_native_call(env, this_arg, &call)) {
        return NULL;
    }
    const uint8_t* body = NULL;
    size_t body_len = 0;
    if (bytes_arg_view(env, args[0], &body, &body_len) != 0) {
        napi_throw_type_error(env, NULL, "invalid sendMessage body");
        return NULL;
    }

    trevrpc_call* c_call = NULL;
    int err = native_call_acquire(call, &c_call);
    if (err == 0) {
        trevrpc_stream* stream = trevrpc_call_stream(c_call);
        err = stream == NULL ? TREVRPC_ERR_UNSUPPORTED_RPC_KIND : trevrpc_stream_send_message(stream, body, body_len);
        native_call_release(call);
    }
    return promise_from_void_result(env, err, "sendMessage");
}

static napi_value native_call_send_messages(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "sendMessages requires an array of bodies");
        return NULL;
    }
    native_call* call = NULL;
    if (!unwrap_native_call(env, this_arg, &call)) {
        return NULL;
    }

    uint8_t* bodies = NULL;
    size_t* body_lens = NULL;
    size_t count = 0;
    int err = copy_bytes_array_arg(env, args[0], &bodies, &body_lens, &count);
    if (err != 0) {
        if (err == -ENOMEM) {
            napi_throw_error(env, NULL, "failed to allocate sendMessages bodies");
        } else {
            napi_throw_type_error(env, NULL, "invalid sendMessages bodies");
        }
        return NULL;
    }

    trevrpc_call* c_call = NULL;
    err = native_call_acquire(call, &c_call);
    if (err == 0) {
        trevrpc_stream* stream = trevrpc_call_stream(c_call);
        err = stream == NULL ? TREVRPC_ERR_UNSUPPORTED_RPC_KIND
                             : trevrpc_stream_send_messages(stream, bodies, body_lens, count);
        native_call_release(call);
    }
    free(bodies);
    free(body_lens);
    return promise_from_void_result(env, err, "sendMessages");
}

static void call_finish_execute(napi_env env, void* data) {
    (void)env;
    call_finish_work* work = data;
    trevrpc_call* call = NULL;
    work->base.err = native_call_acquire_completion(work->call, &call);
    if (work->base.err != 0) {
        return;
    }
    work->acquired = true;
    work->base.err =
        trevrpc_call_finish_stream_with_metadata(call, work->status, work->message, work->message_len, &work->metadata);
    if (work->base.err != 0) {
        trevrpc_call_close(call);
    }
}

static void call_finish_complete(napi_env env, napi_status status, void* data) {
    call_finish_work* work = data;
    if (status == napi_ok && work->base.err == 0) {
        napi_value undefined = NULL;
        napi_get_undefined(env, &undefined);
        napi_resolve_deferred(env, work->base.deferred, undefined);
    } else {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "finishStream");
    }
    if (work->acquired) {
        native_call_complete(work->call);
    }
    if (work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    napi_delete_async_work(env, work->base.work);
    trevrpc_metadata_reset(&work->metadata);
    free(work->message);
    free(work);
}

static napi_value native_call_finish_stream(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value args[3];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    call_finish_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate finishStream work");
        return NULL;
    }
    if (!unwrap_native_call(env, this_arg, &work->call) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    work->status = TREVRPC_STATUS_OK;
    if (argc > 0 && napi_get_value_uint32(env, args[0], &work->status) != napi_ok) {
        napi_delete_reference(env, work->base.receiver_ref);
        free(work);
        napi_throw_type_error(env, NULL, "invalid finishStream status");
        return NULL;
    }
    if (argc > 1) {
        work->message = copy_string_value(env, args[1]);
        if (work->message == NULL) {
            napi_delete_reference(env, work->base.receiver_ref);
            free(work);
            napi_throw_type_error(env, NULL, "invalid finishStream message");
            return NULL;
        }
        work->message_len = strlen(work->message);
    }
    if (argc > 2) {
        int err = metadata_from_js(env, args[2], &work->metadata);
        if (err != 0) {
            napi_delete_reference(env, work->base.receiver_ref);
            trevrpc_metadata_reset(&work->metadata);
            free(work->message);
            free(work);
            napi_throw_type_error(env, NULL, "invalid finishStream metadata");
            return NULL;
        }
    }
    return queue_work(env, &work->base, "finishStream", call_finish_execute, call_finish_complete);
}

static void call_recv_execute(napi_env env, void* data) {
    (void)env;
    call_recv_work* work = data;
    trevrpc_call* call = NULL;
    work->base.err = native_call_acquire(work->call, &call);
    if (work->base.err != 0) {
        return;
    }
    work->acquired = true;
    trevrpc_stream* stream = trevrpc_call_stream(call);
    work->base.err = stream == NULL ? TREVRPC_ERR_UNSUPPORTED_RPC_KIND : trevrpc_stream_recv(stream, &work->frame);
}

static void call_recv_complete(napi_env env, napi_status status, void* data) {
    call_recv_work* work = data;
    if (status == napi_ok && work->base.err == 0) {
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
        native_call_release(work->call);
    }
    if (work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    napi_delete_async_work(env, work->base.work);
    free(work);
}

static napi_value native_call_recv(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    call_recv_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate recv work");
        return NULL;
    }
    if (!unwrap_native_call(env, this_arg, &work->call) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    return queue_work(env, &work->base, "recv", call_recv_execute, call_recv_complete);
}

static void call_recv_many_execute(napi_env env, void* data) {
    (void)env;
    call_recv_many_work* work = data;
    trevrpc_call* call = NULL;
    work->base.err = native_call_acquire(work->call, &call);
    if (work->base.err != 0) {
        return;
    }
    work->acquired = true;
    trevrpc_stream* stream = trevrpc_call_stream(call);
    work->base.err =
        stream == NULL ? TREVRPC_ERR_UNSUPPORTED_RPC_KIND
                       : recv_many_from_stream(
                             stream, work->max_frames, &work->frames, &work->frames_len, &work->frames_cap, &work->eof);
}

static void call_recv_many_complete(napi_env env, napi_status status, void* data) {
    call_recv_many_work* work = data;
    if (work->base.err == 0 && status == napi_ok) {
        napi_value frames = stream_frame_list_to_js(env, work->frames, work->frames_len, work->eof);
        napi_resolve_deferred(env, work->base.deferred, frames);
    } else {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "recvMany");
    }
    stream_frame_list_reset(work->frames, work->frames_len);
    if (work->acquired) {
        native_call_release(work->call);
    }
    if (work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    napi_delete_async_work(env, work->base.work);
    free(work);
}

static napi_value native_call_recv_many(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    call_recv_many_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate recvMany work");
        return NULL;
    }
    if (!recv_many_max_arg(env, argc, args, &work->max_frames) || !unwrap_native_call(env, this_arg, &work->call) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    return queue_work(env, &work->base, "recvMany", call_recv_many_execute, call_recv_many_complete);
}

static napi_value native_call_close(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    native_call* call = NULL;
    if (!unwrap_native_call(env, this_arg, &call)) {
        return NULL;
    }
    native_call_close_request(call);
    napi_value undefined = NULL;
    napi_get_undefined(env, &undefined);
    return undefined;
}

static napi_value init(napi_env env, napi_value exports) {
    napi_property_descriptor client_methods[] = {
        {"call", NULL, native_client_call, NULL, NULL, NULL, napi_default, NULL},
        {"startStream", NULL, native_client_start_stream, NULL, NULL, NULL, napi_default, NULL},
        {"createCancellation", NULL, native_client_create_cancellation, NULL, NULL, NULL, napi_default, NULL},
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
        {"sendMessages", NULL, native_stream_send_messages, NULL, NULL, NULL, napi_default, NULL},
        {"finishSend", NULL, native_stream_finish_send, NULL, NULL, NULL, napi_default, NULL},
        {"recv", NULL, native_stream_recv, NULL, NULL, NULL, napi_default, NULL},
        {"recvMany", NULL, native_stream_recv_many, NULL, NULL, NULL, napi_default, NULL},
        {"recvBodyBatch", NULL, native_stream_recv_body_batch, NULL, NULL, NULL, napi_default, NULL},
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

    napi_property_descriptor server_methods[] = {
        {"register", NULL, native_server_register, NULL, NULL, NULL, napi_default, NULL},
        {"serve", NULL, native_server_serve, NULL, NULL, NULL, napi_default, NULL},
        {"close", NULL, native_server_close, NULL, NULL, NULL, napi_default, NULL},
    };
    napi_value server_ctor = NULL;
    napi_define_class(env,
        "NativeServer",
        NAPI_AUTO_LENGTH,
        native_server_constructor,
        NULL,
        sizeof(server_methods) / sizeof(server_methods[0]),
        server_methods,
        &server_ctor);
    napi_create_reference(env, server_ctor, 1, &NativeServerConstructor);

    napi_property_descriptor call_methods[] = {
        {"respond", NULL, native_call_respond, NULL, NULL, NULL, napi_default, NULL},
        {"sendMessage", NULL, native_call_send_message, NULL, NULL, NULL, napi_default, NULL},
        {"sendMessages", NULL, native_call_send_messages, NULL, NULL, NULL, napi_default, NULL},
        {"finishStream", NULL, native_call_finish_stream, NULL, NULL, NULL, napi_default, NULL},
        {"recv", NULL, native_call_recv, NULL, NULL, NULL, napi_default, NULL},
        {"recvMany", NULL, native_call_recv_many, NULL, NULL, NULL, napi_default, NULL},
        {"close", NULL, native_call_close, NULL, NULL, NULL, napi_default, NULL},
    };
    napi_value call_ctor = NULL;
    napi_define_class(env,
        "NativeCall",
        NAPI_AUTO_LENGTH,
        native_call_constructor,
        NULL,
        sizeof(call_methods) / sizeof(call_methods[0]),
        call_methods,
        &call_ctor);
    napi_create_reference(env, call_ctor, 1, &NativeCallConstructor);

    napi_property_descriptor cancellation_methods[] = {
        {"cancel", NULL, native_cancellation_cancel, NULL, NULL, NULL, napi_default, NULL},
    };
    napi_value cancellation_ctor = NULL;
    napi_define_class(env,
        "NativeCancellation",
        NAPI_AUTO_LENGTH,
        native_cancellation_constructor,
        NULL,
        sizeof(cancellation_methods) / sizeof(cancellation_methods[0]),
        cancellation_methods,
        &cancellation_ctor);
    napi_create_reference(env, cancellation_ctor, 1, &NativeCancellationConstructor);

    napi_property_descriptor exports_desc[] = {
        {"connectMsQuic", NULL, connect_msquic, NULL, NULL, NULL, napi_default, NULL},
        {"listenMsQuic", NULL, listen_msquic, NULL, NULL, NULL, napi_default, NULL},
    };
    napi_define_properties(env, exports, sizeof(exports_desc) / sizeof(exports_desc[0]), exports_desc);
    return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, init)
