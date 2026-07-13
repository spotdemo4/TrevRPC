#define _POSIX_C_SOURCE 200809L

#include "trevrpc_binding.h"

#include <errno.h> // IWYU pragma: keep
#include <node_api.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define TREV_NODE_ERR_CLOSED -4001
#define TREV_NODE_COMPLETION_WORKERS 4u
#define TREV_NODE_COMPLETION_POLL_MIN_NANOS (250 * 1000)
#define TREV_NODE_COMPLETION_POLL_MAX_NANOS (8 * 1000 * 1000)
#define TREV_NODE_RECV_MANY_DEFAULT 16u
#define TREV_NODE_RECV_MANY_LIMIT 256u

typedef struct base_work base_work;
typedef struct native_async_work native_async_work;
typedef struct native_completion_runtime native_completion_runtime;
typedef struct native_client native_client;
typedef struct native_client_observer native_client_observer;
typedef struct native_stream native_stream;
typedef struct native_server native_server;
typedef struct native_call native_call;
typedef struct native_cancellation native_cancellation;
typedef struct server_route server_route;
typedef struct node_http3_admission_state node_http3_admission_state;

static void http3_admission_state_shutdown(node_http3_admission_state* state);
static void http3_admission_state_release(node_http3_admission_state* state);

struct native_client {
    trevrpc_raw_client* client;
    native_client_observer* observer;
    pthread_mutex_t mutex;
    size_t refs;
    bool closing;
    bool js_alive;
};

struct native_client_observer {
    napi_env env;
    napi_deferred deferred;
    napi_ref promise_ref;
    napi_threadsafe_function tsfn;
    atomic_bool notified;
    int error_code;
};

struct native_stream {
    trevrpc_stream* stream;
    native_client* owner;
    pthread_mutex_t mutex;
    pthread_mutex_t operation_mutex;
    base_work* outbound_head;
    base_work* outbound_tail;
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
    node_http3_admission_state* http3_admission;
    uint16_t port;
    size_t refs;
    bool closing;
    bool serving;
    bool js_alive;
};

struct native_call {
    trevrpc_call* call;
    pthread_mutex_t mutex;
    pthread_mutex_t operation_mutex;
    base_work* outbound_head;
    base_work* outbound_tail;
    size_t refs;
    bool completing;
    bool js_alive;
};

struct native_cancellation {
    trevrpc_cancellation* cancellation;
    bool js_alive;
};

struct base_work {
    napi_env env;
    napi_deferred deferred;
    native_async_work* work;
    napi_ref receiver_ref;
    int err;
    bool retry;
    bool completion_started;
    trevrpc_call* completion_call;
    uint64_t queued_at_nanos;
    void* owner;
    void (*owner_release)(void* owner);
    base_work* outbound_next;
    bool outbound_registered;
};

struct native_async_work {
    native_completion_runtime* runtime;
    base_work* base;
    napi_async_execute_callback execute;
    napi_async_complete_callback complete;
    napi_async_complete_callback abandon;
    void (*cancel)(void* data);
    native_async_work* next;
    native_async_work* active_next;
    uint64_t retry_delay_nanos;
    uint64_t retry_due_nanos;
};

struct native_completion_runtime {
    napi_env env;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool mutex_initialized;
    bool cond_initialized;
    pthread_t workers[TREV_NODE_COMPLETION_WORKERS];
    size_t worker_count;
    pthread_t scheduler;
    bool scheduler_started;
    native_async_work* head;
    native_async_work* tail;
    native_async_work* active;
    native_async_work** retry_heap;
    size_t retry_heap_len;
    size_t retry_heap_cap;
    napi_threadsafe_function tsfn;
    napi_ref client_constructor;
    napi_ref stream_constructor;
    napi_ref server_constructor;
    napi_ref call_constructor;
    napi_ref cancellation_constructor;
    size_t loop_ref_count;
    bool stopping;
    bool closed;
};

typedef struct connect_work {
    base_work base;
    trevrpc_raw_client* client;
    native_cancellation* cancellation;
    trevrpc_cancellation* owned_cancellation;
    napi_ref cancellation_ref;
    native_client_observer* observer;
    char* host;
    char* ca_cert_file;
    uint16_t port;
    int skip_certificate_validation;
    uint32_t max_streams_per_session;
    uint32_t idle_timeout_ms;
    size_t max_frame_size;
    size_t max_pending_send_bytes;
    size_t max_pending_send_count;
} connect_work;

typedef struct call_work {
    base_work base;
    native_client* client;
    bool acquired;
    native_cancellation* cancellation;
    trevrpc_cancellation* owned_cancellation;
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
    trevrpc_cancellation* owned_cancellation;
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

typedef struct stream_send_work {
    base_work base;
    native_stream* stream;
    bool acquired;
    uint8_t* body;
    size_t body_len;
} stream_send_work;

typedef struct stream_send_many_work {
    base_work base;
    native_stream* stream;
    bool acquired;
    uint8_t* bodies;
    size_t* body_lens;
    size_t count;
} stream_send_many_work;

typedef struct stream_finish_work {
    base_work base;
    native_stream* stream;
    bool acquired;
} stream_finish_work;

typedef struct listen_work {
    base_work base;
    trevrpc_server* server;
    char* host;
    char* path;
    char* origin;
    char* http3_path;
    char* cert_file;
    char* key_file;
    uint16_t port;
    uint32_t max_sessions_per_connection;
    uint32_t max_streams_per_session;
    uint32_t idle_timeout_ms;
    uint32_t stream_idle_timeout_ms;
    uint32_t initial_request_timeout_ms;
    int64_t max_stream_messages;
    size_t max_frame_size;
    size_t max_pending_send_bytes;
    size_t max_pending_send_count;
    uint16_t bound_port;
    bool has_max_stream_messages;
    bool enable_http3;
    node_http3_admission_state* http3_admission;
} listen_work;

typedef struct serve_work {
    base_work base;
    native_server* server;
    bool server_closed;
} serve_work;

typedef struct call_response_work {
    base_work base;
    native_call* call;
    bool acquired;
    trevrpc_call* c_call;
    trevrpc_response response;
    bool terminal_released;
} call_response_work;

typedef struct call_finish_work {
    base_work base;
    native_call* call;
    bool acquired;
    trevrpc_call* c_call;
    uint32_t status;
    char* message;
    size_t message_len;
    trevrpc_metadata metadata;
    bool terminal_released;
} call_finish_work;

typedef struct call_recv_work {
    base_work base;
    native_call* call;
    bool acquired;
    trevrpc_call* c_call;
    trevrpc_stream_frame* frame;
} call_recv_work;

typedef struct call_recv_many_work {
    base_work base;
    native_call* call;
    bool acquired;
    trevrpc_call* c_call;
    size_t max_frames;
    trevrpc_stream_frame** frames;
    size_t frames_len;
    size_t frames_cap;
    bool eof;
} call_recv_many_work;

typedef struct call_send_work {
    base_work base;
    native_call* call;
    bool acquired;
    trevrpc_call* c_call;
    uint8_t* body;
    size_t body_len;
} call_send_work;

typedef struct call_send_many_work {
    base_work base;
    native_call* call;
    bool acquired;
    trevrpc_call* c_call;
    uint8_t* bodies;
    size_t* body_lens;
    size_t count;
} call_send_many_work;

#ifdef TREVRPC_NODE_TEST_HOOKS
typedef struct debug_pending_resource {
    pthread_mutex_t mutex;
    size_t refs;
    bool closing;
    bool js_alive;
    bool closed;
} debug_pending_resource;

typedef struct debug_pending_wait_work {
    base_work base;
    debug_pending_resource* resource;
    bool acquired;
    uint32_t delay_ms;
} debug_pending_wait_work;
#endif

typedef struct server_call_event {
    server_route* route;
    native_call* call;
} server_call_event;

#ifdef TREVRPC_NODE_TEST_HOOKS
static atomic_uint_least64_t ExternalArrayBufferFinalizers = ATOMIC_VAR_INIT(0);
static atomic_uint_least64_t DebugPendingResourceCloses = ATOMIC_VAR_INIT(0);
static atomic_uint_least64_t DebugPendingResourceFinalizers = ATOMIC_VAR_INIT(0);
#endif

static napi_value noop_js_callback(napi_env env, napi_callback_info info);
static void native_client_release(native_client* client);
static void native_stream_close_request(native_stream* stream);
static void native_server_close_request(native_server* server);
static void native_call_close_request(native_call* call);
static void native_call_release(native_call* call, trevrpc_call* acquired_call);
static void native_call_release_keep_wrapper(native_call* call, trevrpc_call* acquired_call);
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
        if (data == NULL) {
            return -EINVAL;
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
        if (data == NULL) {
            return -EINVAL;
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
        if (data == NULL && len > 0) {
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
        if (data == NULL && len > 0) {
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

static void external_arraybuffer_finalize(napi_env env, void* data, void* hint) {
    (void)env;
    (void)data;
    free(hint);
#ifdef TREVRPC_NODE_TEST_HOOKS
    atomic_fetch_add_explicit(&ExternalArrayBufferFinalizers, 1, memory_order_relaxed);
#endif
}

static napi_value make_external_uint8_array(
    napi_env env, uint8_t* data, size_t len, uint8_t** owner_slot, bool* transferred) {
    if (transferred != NULL) {
        *transferred = false;
    }
    if (len == 0 || data == NULL || owner_slot == NULL || *owner_slot == NULL) {
        return make_uint8_array(env, data, len);
    }

    uint8_t* owner = *owner_slot;
    napi_value arraybuffer = NULL;
    napi_value typedarray = NULL;
    if (napi_create_external_arraybuffer(env, data, len, external_arraybuffer_finalize, owner, &arraybuffer) !=
        napi_ok) {
        clear_pending_exception(env);
        return make_uint8_array(env, data, len);
    }

    *owner_slot = NULL;
    if (transferred != NULL) {
        *transferred = true;
    }
    if (napi_create_typedarray(env, napi_uint8_array, len, arraybuffer, 0, &typedarray) != napi_ok) {
        clear_pending_exception(env);
        return make_uint8_array(env, data, len);
    }
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
        napi_get_named_property(env, value, "body", &body);
        uint8_t* bytes = NULL;
        size_t bytes_len = 0;
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

static napi_value response_to_js(napi_env env, trevrpc_response* response) {
    napi_value object = NULL;
    napi_create_object(env, &object);
    set_uint32(env, object, "status", response != NULL ? response->status : TREVRPC_STATUS_UNKNOWN);
    set_string_bytes(env,
        object,
        "message",
        response != NULL ? response->message : "",
        response != NULL ? response->message_len : 0);
    napi_value body = NULL;
    bool body_transferred = false;
    if (response != NULL) {
        body = make_external_uint8_array(
            env, response->body, response->body_len, &response->_body_owner, &body_transferred);
        if (body_transferred) {
            response->body = NULL;
            response->body_len = 0;
        }
    } else {
        body = make_uint8_array(env, NULL, 0);
    }
    napi_set_named_property(env, object, "body", body);
    napi_value metadata = metadata_to_js(env, response != NULL ? &response->metadata : NULL);
    napi_set_named_property(env, object, "metadata", metadata);
    return object;
}

static napi_value stream_frame_to_js(napi_env env, trevrpc_stream_frame* frame) {
    napi_value object = NULL;
    napi_create_object(env, &object);
    set_uint32(env, object, "kind", frame->kind);
    set_uint32(env, object, "status", frame->status);
    set_string_bytes(env, object, "message", frame->message, frame->message_len);
    bool body_transferred = false;
    napi_value body =
        make_external_uint8_array(env, frame->body, frame->body_len, &frame->_body_owner, &body_transferred);
    if (body_transferred) {
        frame->body = NULL;
        frame->body_len = 0;
    }
    napi_set_named_property(env, object, "body", body);
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

static int recv_many_ready_from_stream(trevrpc_stream* stream,
    size_t max_frames,
    trevrpc_stream_frame*** frames,
    size_t* frames_len,
    size_t* frames_cap,
    bool* eof,
    int* ready,
    uint64_t wait_started_nanos) {
    trevrpc_stream_frame* frame = NULL;
    int err = trevrpc_stream_recv_ready_since(stream, &frame, ready, wait_started_nanos);
    if (err != 0) {
        return err;
    }
    if (!*ready) {
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

    while (*frames_len < max_frames) {
        int next_ready = 0;
        frame = NULL;
        err = trevrpc_stream_recv_ready(stream, &frame, &next_ready);
        if (err != 0) {
            return err;
        }
        if (!next_ready) {
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
        bool body_transferred = false;
        napi_value body = make_external_uint8_array(
            env, frames[i]->body, frames[i]->body_len, &frames[i]->_body_owner, &body_transferred);
        if (body_transferred) {
            frames[i]->body = NULL;
            frames[i]->body_len = 0;
        }
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

static void native_client_observer_finalize(napi_env env, void* data, void* hint) {
    (void)hint;
    native_client_observer* observer = data;
    if (env != NULL && observer->promise_ref != NULL) {
        napi_delete_reference(env, observer->promise_ref);
    }
    free(observer);
}

static void native_client_closed_js(napi_env env, napi_value callback, void* context, void* data) {
    (void)callback;
    (void)data;
    native_client_observer* observer = context;
    if (env == NULL || observer->deferred == NULL) {
        return;
    }

    napi_value close_info = NULL;
    napi_value native_code = NULL;
    if (napi_create_object(env, &close_info) == napi_ok &&
        napi_create_int32(env, observer->error_code, &native_code) == napi_ok &&
        napi_set_named_property(env, close_info, "nativeCode", native_code) == napi_ok) {
        napi_resolve_deferred(env, observer->deferred, close_info);
        observer->deferred = NULL;
    } else {
        clear_pending_exception(env);
    }
}

static native_client_observer* native_client_observer_create(napi_env env) {
    native_client_observer* observer = calloc(1, sizeof(*observer));
    if (observer == NULL) {
        return NULL;
    }
    observer->env = env;
    atomic_init(&observer->notified, false);

    napi_value promise = NULL;
    napi_value callback = NULL;
    napi_value resource_name = NULL;
    napi_status status = napi_create_promise(env, &observer->deferred, &promise);
    if (status == napi_ok) {
        status = napi_create_reference(env, promise, 1, &observer->promise_ref);
    }
    if (status == napi_ok) {
        status = napi_create_function(env, "nativeClientClosed", NAPI_AUTO_LENGTH, noop_js_callback, NULL, &callback);
    }
    if (status == napi_ok) {
        status = napi_create_string_utf8(env, "TrevRpcNativeClientClosed", NAPI_AUTO_LENGTH, &resource_name);
    }
    if (status == napi_ok) {
        status = napi_create_threadsafe_function(env,
            callback,
            NULL,
            resource_name,
            0,
            1,
            observer,
            native_client_observer_finalize,
            observer,
            native_client_closed_js,
            &observer->tsfn);
    }
    if (status != napi_ok) {
        if (observer->promise_ref != NULL) {
            napi_delete_reference(env, observer->promise_ref);
        }
        free(observer);
        return NULL;
    }
    (void)napi_unref_threadsafe_function(env, observer->tsfn);
    return observer;
}

static void native_client_observer_notify(native_client_observer* observer, int error_code) {
    if (observer == NULL || atomic_exchange_explicit(&observer->notified, true, memory_order_acq_rel)) {
        return;
    }
    observer->error_code = error_code;
    (void)napi_call_threadsafe_function(observer->tsfn, NULL, napi_tsfn_nonblocking);
}

static void native_client_observer_release(native_client_observer* observer) {
    if (observer != NULL) {
        (void)napi_release_threadsafe_function(observer->tsfn, napi_tsfn_release);
    }
}

static void native_client_connection_shutdown(void* context, int error_code) {
    native_client_observer_notify(context, error_code);
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

static int native_client_acquire(native_client* client, trevrpc_raw_client** out_client) {
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
    trevrpc_raw_client* close_client = NULL;
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

    trevrpc_raw_client_close(close_client);
    if (destroy) {
        pthread_mutex_destroy(&client->mutex);
        free(client);
    }
}

static void native_client_work_release(void* owner) {
    native_client_release(owner);
}

static int native_client_work_reserve(native_client* client, base_work* work) {
    trevrpc_raw_client* ignored = NULL;
    int err = native_client_acquire(client, &ignored);
    if (err == 0) {
        work->owner = client;
        work->owner_release = native_client_work_release;
    }
    return err;
}

static void native_client_close_request(native_client* client) {
    if (client == NULL) {
        return;
    }
    trevrpc_raw_client* close_client = NULL;
    trevrpc_raw_client* shutdown_client = NULL;
    native_client_observer* observer = NULL;
    pthread_mutex_lock(&client->mutex);
    client->closing = true;
    observer = client->observer;
    client->observer = NULL;
    if (client->refs == 0 && client->client != NULL) {
        close_client = client->client;
        client->client = NULL;
    } else if (client->client != NULL) {
        shutdown_client = client->client;
    }
    pthread_mutex_unlock(&client->mutex);

    trevrpc_raw_client_clear_shutdown_callback(close_client != NULL ? close_client : shutdown_client);
    native_client_observer_notify(observer, 0);
    native_client_observer_release(observer);
    trevrpc_raw_client_shutdown(shutdown_client);
    trevrpc_raw_client_close(close_client);
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
        pthread_mutex_destroy(&stream->operation_mutex);
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
        pthread_mutex_destroy(&stream->operation_mutex);
        pthread_mutex_destroy(&stream->mutex);
        free(stream);
    }
}

static void native_stream_work_release(void* owner) {
    native_stream_release(owner);
}

static int native_stream_work_reserve(native_stream* stream, base_work* work) {
    trevrpc_stream* ignored = NULL;
    int err = native_stream_acquire(stream, &ignored);
    if (err == 0) {
        work->owner = stream;
        work->owner_release = native_stream_work_release;
    }
    return err;
}

static int native_stream_outbound_work_reserve(native_stream* stream, base_work* work) {
    pthread_mutex_lock(&stream->mutex);
    if (stream->stream == NULL || stream->closing) {
        pthread_mutex_unlock(&stream->mutex);
        return TREV_NODE_ERR_CLOSED;
    }
    stream->refs++;
    work->owner = stream;
    work->owner_release = native_stream_work_release;
    work->outbound_registered = true;
    if (stream->outbound_tail == NULL) {
        stream->outbound_head = work;
    } else {
        stream->outbound_tail->outbound_next = work;
    }
    stream->outbound_tail = work;
    pthread_mutex_unlock(&stream->mutex);
    return 0;
}

static bool native_stream_outbound_is_head(native_stream* stream, base_work* work) {
    pthread_mutex_lock(&stream->mutex);
    bool is_head = work->outbound_registered && stream->outbound_head == work;
    pthread_mutex_unlock(&stream->mutex);
    return is_head;
}

static void native_stream_outbound_finish(native_stream* stream, base_work* work) {
    pthread_mutex_lock(&stream->mutex);
    if (!work->outbound_registered) {
        pthread_mutex_unlock(&stream->mutex);
        return;
    }
    base_work* previous = NULL;
    base_work** link = &stream->outbound_head;
    while (*link != NULL && *link != work) {
        previous = *link;
        link = &(*link)->outbound_next;
    }
    if (*link == work) {
        *link = work->outbound_next;
        if (stream->outbound_tail == work) {
            stream->outbound_tail = previous;
        }
    }
    work->outbound_next = NULL;
    work->outbound_registered = false;
    pthread_mutex_unlock(&stream->mutex);
}

static int native_stream_operation_acquire(native_stream* stream, trevrpc_stream** out_stream) {
    int err = native_stream_acquire(stream, out_stream);
    if (err != 0) {
        return err;
    }
    err = pthread_mutex_trylock(&stream->operation_mutex);
    if (err == EBUSY) {
        native_stream_release(stream);
        return -EAGAIN;
    }
    if (err != 0) {
        native_stream_release(stream);
        return -err;
    }

    pthread_mutex_lock(&stream->mutex);
    bool closing = stream->closing || stream->stream != *out_stream;
    pthread_mutex_unlock(&stream->mutex);
    if (closing) {
        pthread_mutex_unlock(&stream->operation_mutex);
        native_stream_release(stream);
        return TREV_NODE_ERR_CLOSED;
    }
    return 0;
}

static void native_stream_operation_release(native_stream* stream) {
    pthread_mutex_unlock(&stream->operation_mutex);
    native_stream_release(stream);
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

static int native_call_acquire_internal(native_call* call, bool allow_completing, trevrpc_call** out_call) {
    if (call == NULL) {
        return TREV_NODE_ERR_CLOSED;
    }
    pthread_mutex_lock(&call->mutex);
    if (call->call == NULL || (call->completing && !allow_completing)) {
        pthread_mutex_unlock(&call->mutex);
        return TREV_NODE_ERR_CLOSED;
    }
    int err = trevrpc_call_retain(call->call);
    if (err != 0) {
        pthread_mutex_unlock(&call->mutex);
        return err;
    }
    call->refs++;
    *out_call = call->call;
    pthread_mutex_unlock(&call->mutex);
    return 0;
}

static int native_call_acquire(native_call* call, trevrpc_call** out_call) {
    return native_call_acquire_internal(call, false, out_call);
}

static int native_call_start_completion(native_call* call, trevrpc_call** out_call) {
    if (call == NULL) {
        return TREV_NODE_ERR_CLOSED;
    }
    pthread_mutex_lock(&call->mutex);
    if (call->call == NULL || call->completing) {
        pthread_mutex_unlock(&call->mutex);
        return TREV_NODE_ERR_CLOSED;
    }
    int err = trevrpc_call_retain(call->call);
    if (err != 0) {
        pthread_mutex_unlock(&call->mutex);
        return err;
    }
    call->completing = true;
    call->refs++;
    *out_call = call->call;
    pthread_mutex_unlock(&call->mutex);
    return 0;
}

static int native_call_operation_acquire(native_call* call, bool completion, trevrpc_call** out_call, base_work* work) {
    if (completion && !work->completion_started) {
        int start_err = native_call_start_completion(call, &work->completion_call);
        if (start_err != 0) {
            return start_err;
        }
        work->completion_started = true;
    }
    int err = native_call_acquire_internal(call, completion, out_call);
    if (err != 0) {
        return err;
    }
    err = pthread_mutex_trylock(&call->operation_mutex);
    if (err == EBUSY) {
        trevrpc_call* acquired = *out_call;
        *out_call = NULL;
        native_call_release_keep_wrapper(call, acquired);
        work->retry = true;
        return 0;
    }
    if (err != 0) {
        trevrpc_call* acquired = *out_call;
        *out_call = NULL;
        native_call_release_keep_wrapper(call, acquired);
        return -err;
    }
    return 0;
}

static void native_call_maybe_destroy(native_call* call) {
    bool destroy = false;
    pthread_mutex_lock(&call->mutex);
    destroy = !call->js_alive && call->refs == 0 && call->call == NULL;
    pthread_mutex_unlock(&call->mutex);
    if (destroy) {
        pthread_mutex_destroy(&call->operation_mutex);
        pthread_mutex_destroy(&call->mutex);
        free(call);
    }
}

static void native_call_release(native_call* call, trevrpc_call* acquired_call) {
    trevrpc_call* close_call = NULL;
    bool destroy = false;
    pthread_mutex_lock(&call->mutex);
    if (call->refs > 0) {
        call->refs--;
    }
    if (call->completing && call->refs == 0 && call->call != NULL) {
        close_call = call->call;
        call->call = NULL;
    }
    destroy = !call->js_alive && call->refs == 0 && call->call == NULL;
    pthread_mutex_unlock(&call->mutex);

    trevrpc_call_close(close_call);
    trevrpc_call_release(acquired_call);
    if (destroy) {
        pthread_mutex_destroy(&call->operation_mutex);
        pthread_mutex_destroy(&call->mutex);
        free(call);
    }
}

static void native_call_work_release(void* owner) {
    native_call_release(owner, NULL);
}

static int native_call_work_reserve(native_call* call, base_work* work) {
    pthread_mutex_lock(&call->mutex);
    if (call->call == NULL || call->completing) {
        pthread_mutex_unlock(&call->mutex);
        return TREV_NODE_ERR_CLOSED;
    }
    call->refs++;
    pthread_mutex_unlock(&call->mutex);
    work->owner = call;
    work->owner_release = native_call_work_release;
    return 0;
}

static int native_call_outbound_work_reserve(native_call* call, base_work* work) {
    pthread_mutex_lock(&call->mutex);
    if (call->call == NULL || call->completing) {
        pthread_mutex_unlock(&call->mutex);
        return TREV_NODE_ERR_CLOSED;
    }
    call->refs++;
    work->owner = call;
    work->owner_release = native_call_work_release;
    work->outbound_registered = true;
    if (call->outbound_tail == NULL) {
        call->outbound_head = work;
    } else {
        call->outbound_tail->outbound_next = work;
    }
    call->outbound_tail = work;
    pthread_mutex_unlock(&call->mutex);
    return 0;
}

static bool native_call_outbound_is_head(native_call* call, base_work* work) {
    pthread_mutex_lock(&call->mutex);
    bool is_head = work->outbound_registered && call->outbound_head == work;
    pthread_mutex_unlock(&call->mutex);
    return is_head;
}

static void native_call_outbound_finish(native_call* call, base_work* work) {
    pthread_mutex_lock(&call->mutex);
    if (!work->outbound_registered) {
        pthread_mutex_unlock(&call->mutex);
        return;
    }
    base_work* previous = NULL;
    base_work** link = &call->outbound_head;
    while (*link != NULL && *link != work) {
        previous = *link;
        link = &(*link)->outbound_next;
    }
    if (*link == work) {
        *link = work->outbound_next;
        if (call->outbound_tail == work) {
            call->outbound_tail = previous;
        }
    }
    work->outbound_next = NULL;
    work->outbound_registered = false;
    pthread_mutex_unlock(&call->mutex);
}

static void native_call_release_keep_wrapper(native_call* call, trevrpc_call* acquired_call) {
    trevrpc_call* close_call = NULL;
    pthread_mutex_lock(&call->mutex);
    if (call->refs > 0) {
        call->refs--;
    }
    if (call->completing && call->refs == 0 && call->call != NULL) {
        close_call = call->call;
        call->call = NULL;
    }
    pthread_mutex_unlock(&call->mutex);
    trevrpc_call_close(close_call);
    trevrpc_call_release(acquired_call);
}

static void native_call_work_operation_release(native_call* call, trevrpc_call* acquired_call) {
    pthread_mutex_unlock(&call->operation_mutex);
    native_call_release_keep_wrapper(call, acquired_call);
}

static void native_call_terminal_operation_complete(native_call* call, trevrpc_call* acquired_call, base_work* work) {
    trevrpc_call* completion_call = work->completion_call;
    work->completion_call = NULL;
    pthread_mutex_unlock(&call->operation_mutex);

    pthread_mutex_lock(&call->mutex);
    if (call->call == completion_call) {
        call->call = NULL;
    }
    if (call->refs > 0) {
        call->refs--;
    }
    if (completion_call != NULL && call->refs > 0) {
        call->refs--;
    }
    pthread_mutex_unlock(&call->mutex);

    trevrpc_call_release(acquired_call);
    trevrpc_call_release(completion_call);
}

static void native_call_terminal_abandon(native_call* call, base_work* work) {
    if (!work->completion_started) {
        return;
    }
    trevrpc_call* completion_call = work->completion_call;
    work->completion_call = NULL;
    trevrpc_call* close_call = NULL;

    pthread_mutex_lock(&call->mutex);
    call->completing = true;
    close_call = call->call;
    call->call = NULL;
    if (completion_call != NULL && call->refs > 0) {
        call->refs--;
    }
    pthread_mutex_unlock(&call->mutex);

    trevrpc_call_cancel(close_call);
    trevrpc_call_close(close_call);
    trevrpc_call_release(completion_call);
}

static void native_call_close_request(native_call* call) {
    if (call == NULL) {
        return;
    }
    trevrpc_call* close_call = NULL;
    trevrpc_call* cancel_call = NULL;
    pthread_mutex_lock(&call->mutex);
    call->completing = true;
    if (call->refs == 0 && call->call != NULL) {
        close_call = call->call;
        call->call = NULL;
    } else if (call->call != NULL) {
        cancel_call = call->call;
    }
    pthread_mutex_unlock(&call->mutex);

    trevrpc_call_cancel(cancel_call);
    trevrpc_call_close(close_call);
    native_call_maybe_destroy(call);
}

static void free_server_routes(napi_env env, server_route* route) {
    while (route != NULL) {
        server_route* next = route->next;
        if (env != NULL && route->handler_ref != NULL) {
            napi_delete_reference(env, route->handler_ref);
        }
        free(route->service);
        free(route->method);
        free(route);
        route = next;
    }
}

static void native_server_maybe_destroy(native_server* server) {
    bool destroy = false;
    pthread_mutex_lock(&server->mutex);
    destroy = !server->js_alive && server->refs == 0 && server->server == NULL && server->call_tsfn == NULL &&
              server->http3_admission == NULL && server->routes == NULL;
    pthread_mutex_unlock(&server->mutex);
    if (destroy) {
        pthread_mutex_destroy(&server->mutex);
        free(server);
    }
}

static void native_server_release(native_server* server) {
    pthread_mutex_lock(&server->mutex);
    if (server->refs > 0) {
        server->refs--;
    }
    pthread_mutex_unlock(&server->mutex);
    native_server_maybe_destroy(server);
}

static void native_server_close_request(native_server* server) {
    if (server == NULL) {
        return;
    }
    trevrpc_server* close_server = NULL;
    trevrpc_server* shutdown_server = NULL;
    napi_env env = NULL;
    pthread_mutex_lock(&server->mutex);
    server->closing = true;
    node_http3_admission_state* admission = server->http3_admission;
    if (!server->serving && server->server != NULL) {
        close_server = server->server;
        server->server = NULL;
    } else {
        shutdown_server = server->server;
    }
    env = server->env;
    pthread_mutex_unlock(&server->mutex);

    http3_admission_state_shutdown(admission);
    trevrpc_server_shutdown(shutdown_server);
    trevrpc_server_close(close_server);
    if (close_server != NULL) {
        free_server_routes(env, server->routes);
        server->routes = NULL;
        napi_threadsafe_function tsfn = NULL;
        node_http3_admission_state* admission_state = NULL;
        pthread_mutex_lock(&server->mutex);
        tsfn = server->call_tsfn;
        server->call_tsfn = NULL;
        admission_state = server->http3_admission;
        server->http3_admission = NULL;
        pthread_mutex_unlock(&server->mutex);
        if (tsfn != NULL) {
            napi_release_threadsafe_function(tsfn, napi_tsfn_abort);
        }
        http3_admission_state_release(admission_state);
    }
    native_server_maybe_destroy(server);
}

static void native_server_close_after_serve(native_server* server, napi_env env, bool server_closed) {
    trevrpc_server* close_server = NULL;
    napi_threadsafe_function tsfn = NULL;
    node_http3_admission_state* admission_state = NULL;
    pthread_mutex_lock(&server->mutex);
    server->serving = false;
    if (!server_closed && server->closing && server->server != NULL) {
        close_server = server->server;
        server->server = NULL;
    }
    if (server_closed || close_server != NULL) {
        tsfn = server->call_tsfn;
        server->call_tsfn = NULL;
        admission_state = server->http3_admission;
        server->http3_admission = NULL;
    }
    pthread_mutex_unlock(&server->mutex);
    trevrpc_server_close(close_server);
    if (tsfn != NULL) {
        napi_release_threadsafe_function(tsfn, napi_tsfn_abort);
    }
    http3_admission_state_release(admission_state);
    if (server_closed || close_server != NULL) {
        free_server_routes(env, server->routes);
        server->routes = NULL;
    }
}

static void native_work_release_owner(base_work* work) {
    if (work != NULL && work->owner_release != NULL) {
        void (*release)(void*) = work->owner_release;
        void* owner = work->owner;
        work->owner_release = NULL;
        work->owner = NULL;
        release(owner);
    }
}

static void native_work_delete(native_async_work* work) {
    if (work != NULL) {
        native_work_release_owner(work->base);
    }
    free(work);
}

static void native_completion_runtime_unref_loop(napi_env env, native_completion_runtime* runtime) {
    if (runtime == NULL || runtime->tsfn == NULL) {
        return;
    }

    bool should_unref = false;
    pthread_mutex_lock(&runtime->mutex);
    if (runtime->loop_ref_count > 0) {
        runtime->loop_ref_count--;
        should_unref = runtime->loop_ref_count == 0;
    }
    pthread_mutex_unlock(&runtime->mutex);

    if (should_unref) {
        (void)napi_unref_threadsafe_function(env, runtime->tsfn);
    }
}

static int native_completion_runtime_ref_loop(napi_env env, native_completion_runtime* runtime) {
    if (runtime == NULL || runtime->tsfn == NULL) {
        return -EINVAL;
    }

    bool should_ref = false;
    pthread_mutex_lock(&runtime->mutex);
    if (runtime->stopping) {
        pthread_mutex_unlock(&runtime->mutex);
        return -ECANCELED;
    }
    if (runtime->loop_ref_count == 0) {
        should_ref = true;
    }
    runtime->loop_ref_count++;
    pthread_mutex_unlock(&runtime->mutex);

    if (should_ref && napi_ref_threadsafe_function(env, runtime->tsfn) != napi_ok) {
        pthread_mutex_lock(&runtime->mutex);
        if (runtime->loop_ref_count > 0) {
            runtime->loop_ref_count--;
        }
        pthread_mutex_unlock(&runtime->mutex);
        return -ENOMEM;
    }
    return 0;
}

static void native_completion_enqueue_locked(native_completion_runtime* runtime, native_async_work* work);

static int native_completion_enqueue(native_completion_runtime* runtime, native_async_work* work) {
    pthread_mutex_lock(&runtime->mutex);
    if (runtime->stopping) {
        pthread_mutex_unlock(&runtime->mutex);
        return -ECANCELED;
    }
    native_completion_enqueue_locked(runtime, work);
    pthread_cond_broadcast(&runtime->cond);
    pthread_mutex_unlock(&runtime->mutex);
    return 0;
}

static uint64_t native_completion_now_nanos(void) {
    struct timespec now = {0};
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        return 0;
    }
    return (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec;
}

static struct timespec native_completion_timespec(uint64_t nanos) {
    return (struct timespec){
        .tv_sec = (time_t)(nanos / 1000000000ull),
        .tv_nsec = (long)(nanos % 1000000000ull),
    };
}

static void native_completion_enqueue_locked(native_completion_runtime* runtime, native_async_work* work) {
    work->next = NULL;
    if (runtime->tail == NULL) {
        runtime->head = work;
    } else {
        runtime->tail->next = work;
    }
    runtime->tail = work;
}

static void native_completion_retry_heap_swap(native_async_work** left, native_async_work** right) {
    native_async_work* value = *left;
    *left = *right;
    *right = value;
}

static int native_completion_retry_heap_push_locked(native_completion_runtime* runtime, native_async_work* work) {
    if (runtime->retry_heap_len == runtime->retry_heap_cap) {
        size_t next_cap = runtime->retry_heap_cap == 0 ? 64 : runtime->retry_heap_cap * 2;
        if (next_cap < runtime->retry_heap_cap || next_cap > SIZE_MAX / sizeof(*runtime->retry_heap)) {
            return -ENOMEM;
        }
        native_async_work** next = realloc(runtime->retry_heap, next_cap * sizeof(*next));
        if (next == NULL) {
            return -ENOMEM;
        }
        runtime->retry_heap = next;
        runtime->retry_heap_cap = next_cap;
    }

    size_t index = runtime->retry_heap_len++;
    runtime->retry_heap[index] = work;
    while (index > 0) {
        size_t parent = (index - 1) / 2;
        if (runtime->retry_heap[parent]->retry_due_nanos <= work->retry_due_nanos) {
            break;
        }
        native_completion_retry_heap_swap(&runtime->retry_heap[parent], &runtime->retry_heap[index]);
        index = parent;
    }
    if (runtime->cond_initialized) {
        pthread_cond_broadcast(&runtime->cond);
    }
    return 0;
}

static native_async_work* native_completion_retry_heap_pop_locked(native_completion_runtime* runtime) {
    if (runtime->retry_heap_len == 0) {
        return NULL;
    }
    native_async_work* result = runtime->retry_heap[0];
    runtime->retry_heap_len--;
    if (runtime->retry_heap_len == 0) {
        return result;
    }

    runtime->retry_heap[0] = runtime->retry_heap[runtime->retry_heap_len];
    size_t index = 0;
    for (;;) {
        size_t left = index * 2 + 1;
        size_t right = left + 1;
        if (left >= runtime->retry_heap_len) {
            break;
        }
        size_t next = right < runtime->retry_heap_len &&
                              runtime->retry_heap[right]->retry_due_nanos < runtime->retry_heap[left]->retry_due_nanos
                          ? right
                          : left;
        if (runtime->retry_heap[index]->retry_due_nanos <= runtime->retry_heap[next]->retry_due_nanos) {
            break;
        }
        native_completion_retry_heap_swap(&runtime->retry_heap[index], &runtime->retry_heap[next]);
        index = next;
    }
    return result;
}

static void native_completion_active_remove_locked(native_completion_runtime* runtime, native_async_work* work) {
    native_async_work** link = &runtime->active;
    while (*link != NULL) {
        if (*link == work) {
            *link = work->active_next;
            work->active_next = NULL;
            return;
        }
        link = &(*link)->active_next;
    }
}

static void native_completion_abandon(native_async_work* work) {
    if (work->abandon != NULL) {
        work->abandon(NULL, napi_cancelled, work->base);
    } else {
        native_work_delete(work);
    }
}

static void native_completion_complete_js(napi_env env, napi_value js_callback, void* context, void* data) {
    (void)js_callback;
    (void)context;
    native_async_work* work = data;
    if (work == NULL) {
        return;
    }

    native_completion_runtime* runtime = work->runtime;
    if (env == NULL) {
        native_completion_abandon(work);
        return;
    }
    work->complete(env, napi_ok, work->base);
    native_completion_runtime_unref_loop(env, runtime);
}

static void* native_completion_worker_main(void* data) {
    native_completion_runtime* runtime = data;
    for (;;) {
        pthread_mutex_lock(&runtime->mutex);
        while (runtime->head == NULL && !runtime->stopping) {
            pthread_cond_wait(&runtime->cond, &runtime->mutex);
        }
        if (runtime->head == NULL && runtime->stopping) {
            pthread_mutex_unlock(&runtime->mutex);
            break;
        }

        native_async_work* work = runtime->head;
        runtime->head = work->next;
        if (runtime->head == NULL) {
            runtime->tail = NULL;
        }
        work->next = NULL;
        work->active_next = runtime->active;
        runtime->active = work;
        pthread_mutex_unlock(&runtime->mutex);

        if (work->base->err == 0) {
            work->execute(runtime->env, work->base);
        }
        pthread_mutex_lock(&runtime->mutex);
        native_completion_active_remove_locked(runtime, work);
        bool stopping = runtime->stopping;
        bool queued_for_retry = false;
        int retry_err = 0;
        if (work->base->retry && !stopping) {
            work->base->retry = false;
            work->retry_delay_nanos =
                work->retry_delay_nanos == 0 ? TREV_NODE_COMPLETION_POLL_MIN_NANOS : work->retry_delay_nanos * 2;
            if (work->retry_delay_nanos > TREV_NODE_COMPLETION_POLL_MAX_NANOS) {
                work->retry_delay_nanos = TREV_NODE_COMPLETION_POLL_MAX_NANOS;
            }
            uint64_t now = native_completion_now_nanos();
            work->retry_due_nanos =
                now > UINT64_MAX - work->retry_delay_nanos ? UINT64_MAX : now + work->retry_delay_nanos;
            retry_err = native_completion_retry_heap_push_locked(runtime, work);
            queued_for_retry = retry_err == 0;
        }
        pthread_mutex_unlock(&runtime->mutex);
        if (queued_for_retry) {
            continue;
        }
        work->retry_due_nanos = 0;
        if (retry_err != 0) {
            work->base->err = retry_err;
        }
        if (stopping) {
            native_completion_abandon(work);
            continue;
        }
        if (napi_call_threadsafe_function(runtime->tsfn, work, napi_tsfn_blocking) != napi_ok) {
            native_completion_abandon(work);
        }
    }

    if (runtime->tsfn != NULL) {
        (void)napi_release_threadsafe_function(runtime->tsfn, napi_tsfn_release);
    }
    return NULL;
}

static void* native_completion_scheduler_main(void* data) {
    native_completion_runtime* runtime = data;
    pthread_mutex_lock(&runtime->mutex);
    while (!runtime->stopping) {
        if (runtime->retry_heap_len == 0) {
            pthread_cond_wait(&runtime->cond, &runtime->mutex);
            continue;
        }

        uint64_t now = native_completion_now_nanos();
        uint64_t due = runtime->retry_heap[0]->retry_due_nanos;
        if (due > now) {
            struct timespec deadline = native_completion_timespec(due);
            (void)pthread_cond_timedwait(&runtime->cond, &runtime->mutex, &deadline);
            continue;
        }

        do {
            native_async_work* work = native_completion_retry_heap_pop_locked(runtime);
            work->retry_due_nanos = 0;
            native_completion_enqueue_locked(runtime, work);
            now = native_completion_now_nanos();
        } while (runtime->retry_heap_len > 0 && runtime->retry_heap[0]->retry_due_nanos <= now);
        pthread_cond_broadcast(&runtime->cond);
    }
    pthread_mutex_unlock(&runtime->mutex);
    return NULL;
}

static void native_completion_runtime_close(native_completion_runtime* runtime) {
    if (runtime == NULL || runtime->closed) {
        return;
    }
    if (!runtime->mutex_initialized) {
        runtime->closed = true;
        return;
    }

    pthread_mutex_lock(&runtime->mutex);
    runtime->stopping = true;
    native_async_work* abandoned = runtime->head;
    runtime->head = NULL;
    runtime->tail = NULL;
    while (runtime->retry_heap_len > 0) {
        native_async_work* work = native_completion_retry_heap_pop_locked(runtime);
        work->next = abandoned;
        abandoned = work;
    }
    for (native_async_work* work = runtime->active; work != NULL; work = work->active_next) {
        if (work->cancel != NULL) {
            work->cancel(work->base);
        }
    }
    if (runtime->cond_initialized) {
        pthread_cond_broadcast(&runtime->cond);
    }
    pthread_mutex_unlock(&runtime->mutex);

    while (abandoned != NULL) {
        native_async_work* next = abandoned->next;
        abandoned->next = NULL;
        if (abandoned->cancel != NULL) {
            abandoned->cancel(abandoned->base);
        }
        native_completion_abandon(abandoned);
        abandoned = next;
    }

    if (runtime->scheduler_started) {
        pthread_join(runtime->scheduler, NULL);
        runtime->scheduler_started = false;
    }
    for (size_t i = 0; i < runtime->worker_count; i++) {
        pthread_join(runtime->workers[i], NULL);
    }
    runtime->worker_count = 0;
    if (runtime->tsfn != NULL) {
        (void)napi_release_threadsafe_function(runtime->tsfn, napi_tsfn_abort);
        runtime->tsfn = NULL;
    }
    free(runtime->retry_heap);
    runtime->retry_heap = NULL;
    runtime->retry_heap_cap = 0;
    runtime->closed = true;
    if (runtime->cond_initialized) {
        pthread_cond_destroy(&runtime->cond);
        runtime->cond_initialized = false;
    }
    pthread_mutex_destroy(&runtime->mutex);
    runtime->mutex_initialized = false;
}

static void native_completion_runtime_shutdown(native_completion_runtime* runtime) {
    if (runtime == NULL) {
        return;
    }

    native_completion_runtime_close(runtime);
    free(runtime);
}

static void native_completion_runtime_cleanup(void* data) {
    native_completion_runtime_shutdown(data);
}

static int native_completion_runtime_init(napi_env env, native_completion_runtime* runtime) {
    runtime->env = env;
    if (pthread_mutex_init(&runtime->mutex, NULL) != 0) {
        runtime->closed = true;
        return -ENOMEM;
    }
    runtime->mutex_initialized = true;
    pthread_condattr_t condattr;
    if (pthread_condattr_init(&condattr) != 0) {
        native_completion_runtime_close(runtime);
        return -ENOMEM;
    }
    (void)pthread_condattr_setclock(&condattr, CLOCK_MONOTONIC);
    int cond_err = pthread_cond_init(&runtime->cond, &condattr);
    pthread_condattr_destroy(&condattr);
    if (cond_err != 0) {
        native_completion_runtime_close(runtime);
        return -ENOMEM;
    }
    runtime->cond_initialized = true;

    napi_value callback = NULL;
    napi_value resource_name = NULL;
    if (napi_create_function(env, "nativeCompletion", NAPI_AUTO_LENGTH, noop_js_callback, NULL, &callback) != napi_ok ||
        napi_create_string_utf8(env, "TrevRpcNativeCompletion", NAPI_AUTO_LENGTH, &resource_name) != napi_ok ||
        napi_create_threadsafe_function(env,
            callback,
            NULL,
            resource_name,
            0,
            1,
            NULL,
            NULL,
            NULL,
            native_completion_complete_js,
            &runtime->tsfn) != napi_ok) {
        native_completion_runtime_close(runtime);
        return -ENOMEM;
    }
    (void)napi_unref_threadsafe_function(env, runtime->tsfn);

    if (pthread_create(&runtime->scheduler, NULL, native_completion_scheduler_main, runtime) != 0) {
        native_completion_runtime_close(runtime);
        return -ENOMEM;
    }
    runtime->scheduler_started = true;

    for (size_t i = 0; i < TREV_NODE_COMPLETION_WORKERS; i++) {
        if (napi_acquire_threadsafe_function(runtime->tsfn) != napi_ok) {
            native_completion_runtime_close(runtime);
            return -ENOMEM;
        }
        if (pthread_create(&runtime->workers[runtime->worker_count], NULL, native_completion_worker_main, runtime) !=
            0) {
            (void)napi_release_threadsafe_function(runtime->tsfn, napi_tsfn_release);
            native_completion_runtime_close(runtime);
            return -ENOMEM;
        }
        runtime->worker_count++;
    }

    return 0;
}

static native_completion_runtime* native_completion_runtime_for_env(napi_env env) {
    native_completion_runtime* runtime = NULL;
    if (napi_get_instance_data(env, (void**)&runtime) != napi_ok) {
        return NULL;
    }
    return runtime;
}

static napi_value queue_work_managed(napi_env env,
    base_work* work,
    const char* name,
    napi_async_execute_callback execute,
    napi_async_complete_callback complete,
    napi_async_complete_callback abandon,
    void (*cancel)(void* data)) {
    (void)name;
    napi_value promise = NULL;
    work->env = env;
    work->queued_at_nanos = native_completion_now_nanos();
    napi_create_promise(env, &work->deferred, &promise);

    native_completion_runtime* runtime = native_completion_runtime_for_env(env);
    native_async_work* native_work = calloc(1, sizeof(*native_work));
    if (runtime == NULL || native_work == NULL) {
        free(native_work);
        void* owner = work->owner;
        void (*owner_release)(void*) = work->owner_release;
        work->owner = NULL;
        work->owner_release = NULL;
        work->work = NULL;
        work->err = -ENOMEM;
        complete(env, napi_cancelled, work);
        if (owner_release != NULL) {
            owner_release(owner);
        }
        return promise;
    }

    native_work->runtime = runtime;
    native_work->base = work;
    native_work->execute = execute;
    native_work->complete = complete;
    native_work->abandon = abandon;
    native_work->cancel = cancel;
    work->work = native_work;

    int err = native_completion_runtime_ref_loop(env, runtime);
    if (err == 0) {
        err = native_completion_enqueue(runtime, native_work);
        if (err != 0) {
            native_completion_runtime_unref_loop(env, runtime);
        }
    }
    if (err != 0) {
        work->work = NULL;
        void* owner = work->owner;
        void (*owner_release)(void*) = work->owner_release;
        work->owner = NULL;
        work->owner_release = NULL;
        free(native_work);
        work->err = err;
        complete(env, napi_cancelled, work);
        if (owner_release != NULL) {
            owner_release(owner);
        }
    }
    return promise;
}

static napi_value queue_work(napi_env env,
    base_work* work,
    const char* name,
    napi_async_execute_callback execute,
    napi_async_complete_callback complete) {
    return queue_work_managed(env, work, name, execute, complete, complete, NULL);
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
    (void)env;
    (void)hint;
    native_server* server = data;
    pthread_mutex_lock(&server->mutex);
    server->js_alive = false;
    pthread_mutex_unlock(&server->mutex);
    native_server_close_request(server);
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
        native_call_release(call, c_call);
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
        native_completion_runtime* runtime = native_completion_runtime_for_env(env);
        status =
            runtime == NULL ? napi_generic_failure : napi_get_reference_value(env, runtime->call_constructor, &ctor);
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
    int mutex_err = pthread_mutex_init(&native->mutex, NULL);
    int operation_mutex_err = mutex_err == 0 ? pthread_mutex_init(&native->operation_mutex, NULL) : mutex_err;
    if (mutex_err != 0 || operation_mutex_err != 0) {
        if (mutex_err == 0) {
            pthread_mutex_destroy(&native->mutex);
        }
        free(native);
        free(event);
        trevrpc_call_close(call);
        return TREVRPC_CALL_DEFERRED;
    }
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
    if (work->max_pending_send_bytes > 0) {
        config.max_pending_send_bytes = work->max_pending_send_bytes;
    }
    if (work->max_pending_send_count > 0) {
        config.max_pending_send_count = work->max_pending_send_count;
    }
    config.ca_cert_file = work->ca_cert_file;
    config.skip_certificate_validation = work->skip_certificate_validation;
    trevrpc_cancellation* cancellation =
        work->cancellation != NULL ? work->cancellation->cancellation : work->owned_cancellation;
    work->base.err = trevrpc_raw_client_connect_cancellable_with_shutdown_callback(work->host,
        work->port,
        &config,
        cancellation,
        native_client_connection_shutdown,
        work->observer,
        &work->client);
}

static void connect_cancel(void* data) {
    connect_work* work = data;
    trevrpc_cancellation_cancel(
        work->cancellation != NULL ? work->cancellation->cancellation : work->owned_cancellation);
}

static void connect_complete(napi_env env, napi_status status, void* data) {
    connect_work* work = data;
    trevrpc_cancellation* cancellation =
        work->cancellation != NULL ? work->cancellation->cancellation : work->owned_cancellation;
    if (work->base.err == 0 && trevrpc_cancellation_cancelled(cancellation)) {
        trevrpc_raw_client_clear_shutdown_callback(work->client);
        trevrpc_raw_client_close(work->client);
        work->client = NULL;
        work->base.err = -ECANCELED;
    }
    if (env == NULL) {
        trevrpc_raw_client_clear_shutdown_callback(work->client);
        trevrpc_raw_client_close(work->client);
    } else if (status != napi_ok) {
        trevrpc_raw_client_clear_shutdown_callback(work->client);
        trevrpc_raw_client_close(work->client);
        work->client = NULL;
        reject_native_error(env, work->base.deferred, -ECANCELED, "connectMsQuic");
    } else if (work->base.err != 0) {
        reject_native_error(env, work->base.deferred, work->base.err, "connectMsQuic");
    } else {
        native_client* client = calloc(1, sizeof(*client));
        if (client == NULL) {
            trevrpc_raw_client_clear_shutdown_callback(work->client);
            trevrpc_raw_client_close(work->client);
            reject_native_error(env, work->base.deferred, -ENOMEM, "connectMsQuic");
        } else {
            pthread_mutex_init(&client->mutex, NULL);
            client->client = work->client;
            client->observer = work->observer;
            work->observer = NULL;

            napi_value ctor = NULL;
            napi_value external = NULL;
            napi_value instance = NULL;
            native_completion_runtime* runtime = native_completion_runtime_for_env(env);
            napi_status wrap_status = runtime == NULL
                                          ? napi_generic_failure
                                          : napi_get_reference_value(env, runtime->client_constructor, &ctor);
            if (wrap_status == napi_ok) {
                wrap_status = napi_create_external(env, client, NULL, NULL, &external);
            }
            if (wrap_status == napi_ok) {
                wrap_status = napi_new_instance(env, ctor, 1, &external, &instance);
            }
            napi_value closed = NULL;
            if (wrap_status == napi_ok) {
                wrap_status = napi_get_reference_value(env, client->observer->promise_ref, &closed);
            }
            if (wrap_status == napi_ok) {
                wrap_status = napi_set_named_property(env, instance, "closed", closed);
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
    free(work->ca_cert_file);
    if (env != NULL && work->cancellation_ref != NULL) {
        napi_delete_reference(env, work->cancellation_ref);
    }
    trevrpc_cancellation_free(work->owned_cancellation);
    native_client_observer_release(work->observer);
    native_work_delete(work->base.work);
    free(work);
}

static napi_value connect_msquic(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value args[2];
    napi_get_cb_info(env, info, &argc, args, NULL, NULL);
    if (argc != 1 && argc != 2) {
        napi_throw_type_error(env, NULL, "connectMsQuic requires an options object and optional cancellation");
        return NULL;
    }

    connect_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate connect work");
        return NULL;
    }
    work->observer = native_client_observer_create(env);
    if (work->observer == NULL) {
        free(work);
        napi_throw_error(env, NULL, "failed to create native client close observer");
        return NULL;
    }
    work->host = get_string_property(env, args[0], "host");
    work->ca_cert_file = get_string_property(env, args[0], "caCertFile");
    work->port = (uint16_t)get_uint32_property(env, args[0], "port", 0);
    work->skip_certificate_validation = get_bool_property(env, args[0], "skipCertificateValidation", false) ? 1 : 0;
    work->max_streams_per_session = get_uint32_property(env, args[0], "maxStreamsPerSession", 0);
    work->idle_timeout_ms = get_uint32_property(env, args[0], "idleTimeoutMs", 0);
    get_size_property(env, args[0], "maxFrameSize", &work->max_frame_size);
    get_size_property(env, args[0], "maxPendingSendBytes", &work->max_pending_send_bytes);
    get_size_property(env, args[0], "maxPendingSendCount", &work->max_pending_send_count);
    int cancellation_err = optional_cancellation_arg(env, argc, args, 1, &work->cancellation, &work->cancellation_ref);
    if (cancellation_err == 0 && work->cancellation == NULL) {
        work->owned_cancellation = trevrpc_cancellation_new();
        if (work->owned_cancellation == NULL) {
            cancellation_err = -ENOMEM;
        }
    }

    bool invalid_options = work->host == NULL || work->port == 0;
    if (invalid_options || cancellation_err != 0) {
        free(work->host);
        free(work->ca_cert_file);
        if (work->cancellation_ref != NULL) {
            napi_delete_reference(env, work->cancellation_ref);
        }
        trevrpc_cancellation_free(work->owned_cancellation);
        native_client_observer_release(work->observer);
        free(work);
        if (invalid_options) {
            napi_throw_type_error(env, NULL, "connectMsQuic requires host and port");
        } else if (cancellation_err == -ENOMEM) {
            napi_throw_error(env, NULL, "failed to allocate connect cancellation");
        }
        return NULL;
    }

    return queue_work_managed(
        env, &work->base, "connectMsQuic", connect_execute, connect_complete, connect_complete, connect_cancel);
}

typedef struct http3_admission_call {
    struct http3_admission_call* next;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    atomic_size_t refs;
    char* path;
    size_t path_len;
    char* authority;
    size_t authority_len;
    bool secure;
    clockid_t clock_id;
    bool completed;
    bool admitted;
    bool cancelled;
} http3_admission_call;

struct node_http3_admission_state {
    pthread_mutex_t mutex;
    http3_admission_call* active_calls;
    napi_threadsafe_function tsfn;
    uint64_t timeout_nanos;
    bool shutting_down;
};

static void http3_admission_js(napi_env env, napi_value callback, void* context, void* data);

static void http3_admission_call_release(http3_admission_call* call) {
    if (atomic_fetch_sub_explicit(&call->refs, 1, memory_order_acq_rel) != 1) {
        return;
    }
    pthread_cond_destroy(&call->cond);
    pthread_mutex_destroy(&call->mutex);
    free(call->authority);
    free(call->path);
    free(call);
}

static void http3_admission_state_finalize(napi_env env, void* data, void* hint) {
    (void)env;
    (void)hint;
    node_http3_admission_state* state = data;
    pthread_mutex_destroy(&state->mutex);
    free(state);
}

static void http3_admission_state_shutdown(node_http3_admission_state* state) {
    if (state == NULL) {
        return;
    }
    pthread_mutex_lock(&state->mutex);
    state->shutting_down = true;
    for (http3_admission_call* call = state->active_calls; call != NULL; call = call->next) {
        pthread_mutex_lock(&call->mutex);
        call->cancelled = true;
        call->completed = true;
        pthread_cond_signal(&call->cond);
        pthread_mutex_unlock(&call->mutex);
    }
    pthread_mutex_unlock(&state->mutex);
}

static void http3_admission_state_release(node_http3_admission_state* state) {
    if (state != NULL) {
        napi_release_threadsafe_function(state->tsfn, napi_tsfn_abort);
    }
}

static napi_status http3_admission_state_create(napi_env env,
    napi_value callback,
    napi_value resource_name,
    uint64_t timeout_nanos,
    node_http3_admission_state** out_state) {
    *out_state = NULL;
    node_http3_admission_state* state = calloc(1, sizeof(*state));
    if (state == NULL) {
        return napi_generic_failure;
    }
    pthread_mutex_init(&state->mutex, NULL);
    state->timeout_nanos = timeout_nanos == 0 ? 10000000000ull : timeout_nanos;
    napi_status status = napi_create_threadsafe_function(env,
        callback,
        NULL,
        resource_name,
        0,
        1,
        state,
        http3_admission_state_finalize,
        NULL,
        http3_admission_js,
        &state->tsfn);
    if (status != napi_ok) {
        pthread_mutex_destroy(&state->mutex);
        free(state);
        return status;
    }
    *out_state = state;
    return napi_ok;
}

static void http3_admission_js(napi_env env, napi_value callback, void* context, void* data) {
    (void)context;
    http3_admission_call* call = data;
    bool admitted = false;
    pthread_mutex_lock(&call->mutex);
    bool cancelled = call->cancelled;
    pthread_mutex_unlock(&call->mutex);
    if (!cancelled && env != NULL && callback != NULL) {
        napi_value request = NULL;
        napi_value path = NULL;
        napi_value authority = NULL;
        napi_value secure = NULL;
        napi_value global = NULL;
        napi_value result = NULL;
        napi_status status = napi_create_object(env, &request);
        if (status == napi_ok) {
            status = napi_create_string_utf8(env, call->path, call->path_len, &path);
        }
        if (status == napi_ok) {
            status = napi_create_string_utf8(env, call->authority, call->authority_len, &authority);
        }
        if (status == napi_ok) {
            status = napi_get_boolean(env, call->secure, &secure);
        }
        if (status == napi_ok) {
            status = napi_set_named_property(env, request, "path", path);
        }
        if (status == napi_ok) {
            status = napi_set_named_property(env, request, "authority", authority);
        }
        if (status == napi_ok) {
            status = napi_set_named_property(env, request, "secure", secure);
        }
        if (status == napi_ok) {
            status = napi_get_global(env, &global);
        }
        if (status == napi_ok) {
            status = napi_call_function(env, global, callback, 1, &request, &result);
        }
        if (status == napi_ok) {
            status = napi_get_value_bool(env, result, &admitted);
        }
        if (status != napi_ok) {
            clear_pending_exception(env);
            admitted = false;
        }
    }

    pthread_mutex_lock(&call->mutex);
    if (!call->cancelled) {
        call->admitted = admitted;
    }
    call->completed = true;
    pthread_cond_signal(&call->cond);
    pthread_mutex_unlock(&call->mutex);
    http3_admission_call_release(call);
}

static int node_http3_admission(void* user_data, const trevrpc_http3_admission_request* request) {
    node_http3_admission_state* state = user_data;
    if (state == NULL || request == NULL || request->path_len == SIZE_MAX || request->authority_len == SIZE_MAX) {
        return -1;
    }
    http3_admission_call* call = calloc(1, sizeof(*call));
    if (call == NULL) {
        return -1;
    }
    call->path = malloc(request->path_len + 1);
    call->authority = malloc(request->authority_len + 1);
    if (call->path == NULL || call->authority == NULL) {
        free(call->authority);
        free(call->path);
        free(call);
        return -1;
    }
    memcpy(call->path, request->path, request->path_len);
    call->path[request->path_len] = 0;
    call->path_len = request->path_len;
    memcpy(call->authority, request->authority, request->authority_len);
    call->authority[request->authority_len] = 0;
    call->authority_len = request->authority_len;
    call->secure = request->secure != 0;
    pthread_mutex_init(&call->mutex, NULL);
    pthread_condattr_t cond_attr;
    pthread_condattr_init(&cond_attr);
    call->clock_id = pthread_condattr_setclock(&cond_attr, CLOCK_MONOTONIC) == 0 ? CLOCK_MONOTONIC : CLOCK_REALTIME;
    pthread_cond_init(&call->cond, call->clock_id == CLOCK_MONOTONIC ? &cond_attr : NULL);
    pthread_condattr_destroy(&cond_attr);
    atomic_init(&call->refs, 2);

    pthread_mutex_lock(&state->mutex);
    if (state->shutting_down) {
        pthread_mutex_unlock(&state->mutex);
        http3_admission_call_release(call);
        http3_admission_call_release(call);
        return -1;
    }
    call->next = state->active_calls;
    state->active_calls = call;
    pthread_mutex_unlock(&state->mutex);

    napi_status status = napi_call_threadsafe_function(state->tsfn, call, napi_tsfn_nonblocking);
    if (status != napi_ok) {
        http3_admission_call_release(call);
        pthread_mutex_lock(&call->mutex);
        call->completed = true;
        call->cancelled = true;
        pthread_mutex_unlock(&call->mutex);
    }

    struct timespec deadline = {0};
    bool admitted = false;
    if (clock_gettime(call->clock_id, &deadline) == 0) {
        deadline.tv_sec += (time_t)(state->timeout_nanos / 1000000000ull);
        deadline.tv_nsec += (long)(state->timeout_nanos % 1000000000ull);
        if (deadline.tv_nsec >= 1000000000l) {
            deadline.tv_sec++;
            deadline.tv_nsec -= 1000000000l;
        }
        pthread_mutex_lock(&call->mutex);
        while (!call->completed) {
            int err = pthread_cond_timedwait(&call->cond, &call->mutex, &deadline);
            if (err == ETIMEDOUT) {
                call->cancelled = true;
                break;
            }
            if (err != 0) {
                call->cancelled = true;
                break;
            }
        }
        admitted = call->completed && !call->cancelled && call->admitted;
        pthread_mutex_unlock(&call->mutex);
    } else {
        pthread_mutex_lock(&call->mutex);
        call->cancelled = true;
        pthread_mutex_unlock(&call->mutex);
    }

    pthread_mutex_lock(&state->mutex);
    http3_admission_call** link = &state->active_calls;
    while (*link != NULL) {
        if (*link == call) {
            *link = call->next;
            break;
        }
        link = &(*link)->next;
    }
    pthread_mutex_unlock(&state->mutex);
    http3_admission_call_release(call);
    return admitted ? 0 : -1;
}

#ifdef TREVRPC_NODE_TEST_HOOKS
typedef struct debug_http3_admission_work {
    napi_async_work work;
    napi_deferred deferred;
    node_http3_admission_state* state;
    bool shutdown_first;
    int result;
} debug_http3_admission_work;

static void debug_http3_admission_execute(napi_env env, void* data) {
    (void)env;
    debug_http3_admission_work* work = data;
    if (work->shutdown_first) {
        http3_admission_state_shutdown(work->state);
    }
    const trevrpc_http3_admission_request request = {
        .path = "/rpc",
        .path_len = 4,
        .authority = "localhost",
        .authority_len = 9,
        .secure = 1,
    };
    work->result = node_http3_admission(work->state, &request);
}

static void debug_http3_admission_complete(napi_env env, napi_status status, void* data) {
    debug_http3_admission_work* work = data;
    http3_admission_state_shutdown(work->state);
    http3_admission_state_release(work->state);
    if (status == napi_ok) {
        napi_value admitted = NULL;
        napi_get_boolean(env, work->result == 0, &admitted);
        napi_resolve_deferred(env, work->deferred, admitted);
    } else {
        napi_value message = NULL;
        napi_value error = NULL;
        napi_create_string_utf8(env, "debug HTTP/3 admission work failed", NAPI_AUTO_LENGTH, &message);
        napi_create_error(env, NULL, message, &error);
        napi_reject_deferred(env, work->deferred, error);
    }
    napi_delete_async_work(env, work->work);
    free(work);
}

static napi_value debug_http3_admission(napi_env env, napi_callback_info info) {
    size_t argc = 3;
    napi_value args[3];
    napi_get_cb_info(env, info, &argc, args, NULL, NULL);
    napi_valuetype callback_type = napi_undefined;
    if (argc < 2 || napi_typeof(env, args[0], &callback_type) != napi_ok || callback_type != napi_function) {
        napi_throw_type_error(env, NULL, "_debugHttp3Admission requires a callback and timeout");
        return NULL;
    }
    uint32_t timeout_ms = 0;
    bool shutdown_first = false;
    if (napi_get_value_uint32(env, args[1], &timeout_ms) != napi_ok || timeout_ms == 0 ||
        (argc > 2 && napi_get_value_bool(env, args[2], &shutdown_first) != napi_ok)) {
        napi_throw_type_error(env, NULL, "_debugHttp3Admission requires a positive timeout");
        return NULL;
    }
    debug_http3_admission_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate debug HTTP/3 admission work");
        return NULL;
    }
    napi_value resource_name = NULL;
    napi_value promise = NULL;
    napi_status status = napi_create_string_utf8(env, "TrevRpcDebugHttp3Admission", NAPI_AUTO_LENGTH, &resource_name);
    if (status == napi_ok) {
        status =
            http3_admission_state_create(env, args[0], resource_name, (uint64_t)timeout_ms * 1000000ull, &work->state);
    }
    if (status == napi_ok) {
        status = napi_create_promise(env, &work->deferred, &promise);
    }
    if (status == napi_ok) {
        status = napi_create_async_work(
            env, NULL, resource_name, debug_http3_admission_execute, debug_http3_admission_complete, work, &work->work);
    }
    if (status == napi_ok) {
        work->shutdown_first = shutdown_first;
        status = napi_queue_async_work(env, work->work);
    }
    if (status != napi_ok) {
        if (work->work != NULL) {
            napi_delete_async_work(env, work->work);
        }
        http3_admission_state_shutdown(work->state);
        http3_admission_state_release(work->state);
        free(work);
        napi_throw_error(env, NULL, "failed to queue debug HTTP/3 admission work");
        return NULL;
    }
    return promise;
}
#endif

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
    config.enable_http3 = work->enable_http3 ? 1 : 0;
    config.http3_path = work->http3_path;
    if (work->http3_admission != NULL) {
        config.http3_admission = node_http3_admission;
        config.http3_admission_user_data = work->http3_admission;
    }
    config.max_sessions_per_connection = work->max_sessions_per_connection;
    config.max_streams_per_session = work->max_streams_per_session;
    config.max_idle_timeout_ms = work->idle_timeout_ms;
    if (work->max_frame_size > 0) {
        config.max_frame_size = work->max_frame_size;
    }
    if (work->max_pending_send_bytes > 0) {
        config.max_pending_send_bytes = work->max_pending_send_bytes;
    }
    if (work->max_pending_send_count > 0) {
        config.max_pending_send_count = work->max_pending_send_count;
    }
    work->base.err = trevrpc_server_listen(&config, &work->server);
    if (work->base.err == 0) {
        work->base.err = trevrpc_server_port(work->server, &work->bound_port);
    }
    if (work->base.err == 0 &&
        (work->has_max_stream_messages || work->stream_idle_timeout_ms > 0 || work->initial_request_timeout_ms > 0)) {
        trevrpc_server_options options = trevrpc_default_server_options();
        if (work->has_max_stream_messages) {
            options.max_stream_messages = work->max_stream_messages;
        }
        if (work->stream_idle_timeout_ms > 0) {
            options.stream_idle_timeout_nanos = (uint64_t)work->stream_idle_timeout_ms * 1000000ull;
        }
        if (work->initial_request_timeout_ms > 0) {
            options.initial_request_timeout_nanos = (uint64_t)work->initial_request_timeout_ms * 1000000ull;
        }
        work->base.err = trevrpc_server_set_options(work->server, &options);
    }
}

static void listen_complete(napi_env env, napi_status status, void* data) {
    listen_work* work = data;
    if (env == NULL) {
        trevrpc_server_close(work->server);
    } else if (status != napi_ok) {
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
            server->http3_admission = work->http3_admission;
            work->http3_admission = NULL;

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
                native_completion_runtime* runtime = native_completion_runtime_for_env(env);
                wrap_status = runtime == NULL ? napi_generic_failure
                                              : napi_get_reference_value(env, runtime->server_constructor, &ctor);
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
                http3_admission_state_shutdown(server->http3_admission);
                http3_admission_state_release(server->http3_admission);
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
    free(work->http3_path);
    free(work->cert_file);
    free(work->key_file);
    http3_admission_state_shutdown(work->http3_admission);
    http3_admission_state_release(work->http3_admission);
    native_work_delete(work->base.work);
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
    work->http3_path = get_string_property(env, args[0], "http3Path");
    work->cert_file = get_string_property(env, args[0], "certFile");
    work->key_file = get_string_property(env, args[0], "keyFile");
    work->port = (uint16_t)get_uint32_property(env, args[0], "port", 0);
    work->max_sessions_per_connection = get_uint32_property(env, args[0], "maxSessionsPerConnection", 16);
    work->max_streams_per_session = get_uint32_property(env, args[0], "maxStreamsPerSession", 128);
    work->idle_timeout_ms = get_uint32_property(env, args[0], "idleTimeoutMs", 30000);
    work->stream_idle_timeout_ms = get_uint32_property(env, args[0], "streamIdleTimeoutMs", 0);
    work->initial_request_timeout_ms = get_uint32_property(env, args[0], "initialRequestTimeoutMs", 10000);
    work->enable_http3 = get_bool_property(env, args[0], "enableHttp3", false);
    work->has_max_stream_messages = get_int64_property(env, args[0], "maxStreamMessages", &work->max_stream_messages);
    get_size_property(env, args[0], "maxFrameSize", &work->max_frame_size);
    get_size_property(env, args[0], "maxPendingSendBytes", &work->max_pending_send_bytes);
    get_size_property(env, args[0], "maxPendingSendCount", &work->max_pending_send_count);

    bool has_http3_admission = false;
    napi_has_named_property(env, args[0], "http3Admission", &has_http3_admission);
    bool invalid_http3_admission = false;
    if (has_http3_admission) {
        napi_value callback = NULL;
        napi_value resource_name = NULL;
        napi_valuetype callback_type = napi_undefined;
        napi_status status = napi_get_named_property(env, args[0], "http3Admission", &callback);
        if (status == napi_ok) {
            status = napi_typeof(env, callback, &callback_type);
        }
        if (status == napi_ok && callback_type == napi_function) {
            status = napi_create_string_utf8(env, "TrevRpcHttp3Admission", NAPI_AUTO_LENGTH, &resource_name);
        } else {
            invalid_http3_admission = true;
        }
        if (!invalid_http3_admission && status == napi_ok) {
            status = http3_admission_state_create(env,
                callback,
                resource_name,
                (uint64_t)work->initial_request_timeout_ms * 1000000ull,
                &work->http3_admission);
        }
        invalid_http3_admission = invalid_http3_admission || status != napi_ok;
    }

    if (work->host == NULL || work->cert_file == NULL || work->key_file == NULL || invalid_http3_admission) {
        free(work->host);
        free(work->path);
        free(work->origin);
        free(work->http3_path);
        free(work->cert_file);
        free(work->key_file);
        http3_admission_state_shutdown(work->http3_admission);
        http3_admission_state_release(work->http3_admission);
        free(work);
        napi_throw_type_error(env,
            NULL,
            invalid_http3_admission ? "http3Admission must be a function"
                                    : "listenMsQuic requires host, certFile, and keyFile");
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
    trevrpc_server* close_server = NULL;
    pthread_mutex_lock(&server->mutex);
    if (server->closing && server->server == c_server) {
        close_server = server->server;
        server->server = NULL;
    }
    server->serving = false;
    pthread_mutex_unlock(&server->mutex);
    trevrpc_server_close(close_server);
    work->server_closed = close_server != NULL;
}

static void serve_complete(napi_env env, napi_status status, void* data) {
    serve_work* work = data;
    native_server_close_after_serve(work->server, env, work->server_closed);
    if (env != NULL && status != napi_ok) {
        reject_native_error(env, work->base.deferred, -ECANCELED, "serve");
    } else if (env != NULL && work->base.err != 0) {
        reject_native_error(env, work->base.deferred, work->base.err, "serve");
    } else if (env != NULL) {
        napi_value undefined = NULL;
        napi_get_undefined(env, &undefined);
        napi_resolve_deferred(env, work->base.deferred, undefined);
    }
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    native_server_release(work->server);
    native_work_delete(work->base.work);
    free(work);
}

static void serve_work_cancel(void* data) {
    serve_work* work = data;
    pthread_mutex_lock(&work->server->mutex);
    work->server->env = NULL;
    pthread_mutex_unlock(&work->server->mutex);
    native_server_close_request(work->server);
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
        server->refs++;
    }
    pthread_mutex_unlock(&server->mutex);
    if (!can_start) {
        napi_delete_reference(env, work->base.receiver_ref);
        free(work);
        napi_throw_error(env, NULL, "server is closed or already serving");
        return NULL;
    }
    work->server = server;
    return queue_work_managed(
        env, &work->base, "serve", serve_execute, serve_complete, serve_complete, serve_work_cancel);
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

static napi_value create_native_cancellation(napi_env env) {
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
    native_completion_runtime* runtime = native_completion_runtime_for_env(env);
    napi_status status = runtime == NULL ? napi_generic_failure
                                         : napi_get_reference_value(env, runtime->cancellation_constructor, &ctor);
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

static napi_value native_create_cancellation(napi_env env, napi_callback_info info) {
    (void)info;
    return create_native_cancellation(env);
}

static napi_value native_client_create_cancellation(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    native_client* client = NULL;
    if (!unwrap_native_client(env, this_arg, &client)) {
        return NULL;
    }
    return create_native_cancellation(env);
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
    trevrpc_raw_client* client = NULL;
    work->base.err = native_client_acquire(work->client, &client);
    if (work->base.err != 0) {
        return;
    }
    work->acquired = true;
    trevrpc_cancellation* cancellation =
        work->cancellation != NULL ? work->cancellation->cancellation : work->owned_cancellation;
    work->base.err = trevrpc_raw_client_call_request_cancellable(client, &work->request, cancellation, &work->response);
}

static void call_complete(napi_env env, napi_status status, void* data) {
    call_work* work = data;
    if (env != NULL && work->base.err == 0 && status == napi_ok) {
        napi_value response = response_to_js(env, work->response);
        napi_resolve_deferred(env, work->base.deferred, response);
    } else if (env != NULL) {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "call");
    }
    trevrpc_response_free(work->response);
    if (work->acquired) {
        native_client_release(work->client);
    }
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    if (env != NULL && work->cancellation_ref != NULL) {
        napi_delete_reference(env, work->cancellation_ref);
    }
    trevrpc_cancellation_free(work->owned_cancellation);
    native_work_delete(work->base.work);
    trevrpc_metadata_reset(&work->request.metadata);
    free(work->service);
    free(work->method);
    free(work->body);
    free(work);
}

static void call_work_cancel(void* data) {
    call_work* work = data;
    trevrpc_cancellation_cancel(
        work->cancellation != NULL ? work->cancellation->cancellation : work->owned_cancellation);
}

static napi_value native_client_call(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value args[2];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    if (argc != 1 && argc != 2) {
        napi_throw_type_error(env, NULL, "call requires a request object and optional cancellation");
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
    int err = client_request_from_js(
        env, args[0], TREVRPC_RPC_KIND_UNARY, &work->request, &work->service, &work->method, &work->body);
    if (err == 0) {
        err = optional_cancellation_arg(env, argc, args, 1, &work->cancellation, &work->cancellation_ref);
    }
    if (err == 0 && work->cancellation == NULL) {
        work->owned_cancellation = trevrpc_cancellation_new();
        if (work->owned_cancellation == NULL) {
            err = -ENOMEM;
        }
    }
    if (err != 0) {
        napi_delete_reference(env, work->base.receiver_ref);
        if (work->cancellation_ref != NULL) {
            napi_delete_reference(env, work->cancellation_ref);
        }
        trevrpc_cancellation_free(work->owned_cancellation);
        trevrpc_metadata_reset(&work->request.metadata);
        free(work->service);
        free(work->method);
        free(work->body);
        free(work);
        napi_throw_type_error(env, NULL, "invalid call arguments");
        return NULL;
    }
    work->base.err = native_client_work_reserve(work->client, &work->base);
    return queue_work_managed(env, &work->base, "call", call_execute, call_complete, call_complete, call_work_cancel);
}

static void start_stream_execute(napi_env env, void* data) {
    (void)env;
    start_stream_work* work = data;
    trevrpc_raw_client* client = NULL;
    work->base.err = native_client_acquire(work->client, &client);
    if (work->base.err != 0) {
        return;
    }
    work->acquired = true;
    trevrpc_cancellation* cancellation =
        work->cancellation != NULL ? work->cancellation->cancellation : work->owned_cancellation;
    work->base.err =
        trevrpc_raw_client_start_stream_request_cancellable(client, &work->request, cancellation, &work->stream);
}

static void start_stream_complete(napi_env env, napi_status status, void* data) {
    start_stream_work* work = data;
    bool owner_transferred = false;
    if (env != NULL && work->base.err == 0 && status == napi_ok) {
        native_stream* stream = calloc(1, sizeof(*stream));
        if (stream == NULL) {
            reject_native_error(env, work->base.deferred, -ENOMEM, "startStream");
        } else {
            int mutex_err = pthread_mutex_init(&stream->mutex, NULL);
            int operation_mutex_err = mutex_err == 0 ? pthread_mutex_init(&stream->operation_mutex, NULL) : mutex_err;
            if (mutex_err != 0 || operation_mutex_err != 0) {
                if (mutex_err == 0) {
                    pthread_mutex_destroy(&stream->mutex);
                }
                free(stream);
                reject_native_error(env, work->base.deferred, -ENOMEM, "startStream");
                stream = NULL;
            }
        }
        if (stream != NULL) {
            stream->stream = work->stream;
            stream->owner = work->client;

            napi_value ctor = NULL;
            napi_value external = NULL;
            napi_value instance = NULL;
            native_completion_runtime* runtime = native_completion_runtime_for_env(env);
            napi_status wrap_status = runtime == NULL
                                          ? napi_generic_failure
                                          : napi_get_reference_value(env, runtime->stream_constructor, &ctor);
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
    } else if (env != NULL) {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "startStream");
    }

    if (work->stream != NULL) {
        trevrpc_stream_close(work->stream);
        work->stream = NULL;
    }
    if (work->acquired && !owner_transferred) {
        native_client_release(work->client);
    }
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    if (env != NULL && work->cancellation_ref != NULL) {
        napi_delete_reference(env, work->cancellation_ref);
    }
    trevrpc_cancellation_free(work->owned_cancellation);
    native_work_delete(work->base.work);
    trevrpc_metadata_reset(&work->request.metadata);
    free(work->service);
    free(work->method);
    free(work->body);
    free(work);
}

static void start_stream_work_cancel(void* data) {
    start_stream_work* work = data;
    trevrpc_cancellation_cancel(
        work->cancellation != NULL ? work->cancellation->cancellation : work->owned_cancellation);
}

static napi_value native_client_start_stream(napi_env env, napi_callback_info info) {
    size_t argc = 2;
    napi_value args[2];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    if (argc != 1 && argc != 2) {
        napi_throw_type_error(env, NULL, "startStream requires a request object and optional cancellation");
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
    int err = client_request_from_js(
        env, args[0], TREVRPC_RPC_KIND_SERVER_STREAMING, &work->request, &work->service, &work->method, &work->body);
    if (err == 0) {
        err = optional_cancellation_arg(env, argc, args, 1, &work->cancellation, &work->cancellation_ref);
    }
    if (err == 0 && work->cancellation == NULL) {
        work->owned_cancellation = trevrpc_cancellation_new();
        if (work->owned_cancellation == NULL) {
            err = -ENOMEM;
        }
    }
    if (err != 0) {
        napi_delete_reference(env, work->base.receiver_ref);
        if (work->cancellation_ref != NULL) {
            napi_delete_reference(env, work->cancellation_ref);
        }
        trevrpc_cancellation_free(work->owned_cancellation);
        trevrpc_metadata_reset(&work->request.metadata);
        free(work->service);
        free(work->method);
        free(work->body);
        free(work);
        napi_throw_type_error(env, NULL, "invalid startStream arguments");
        return NULL;
    }
    work->base.err = native_client_work_reserve(work->client, &work->base);
    return queue_work_managed(env,
        &work->base,
        "startStream",
        start_stream_execute,
        start_stream_complete,
        start_stream_complete,
        start_stream_work_cancel);
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

typedef struct native_stream_work_prefix {
    base_work base;
    native_stream* stream;
} native_stream_work_prefix;

static void native_stream_work_cancel(void* data) {
    native_stream_work_prefix* work = data;
    native_stream_close_request(work->stream);
}

static bool native_stream_wait_timed_out(native_stream* stream, uint64_t wait_started_nanos) {
    trevrpc_stream* c_stream = NULL;
    if (native_stream_acquire(stream, &c_stream) != 0) {
        return false;
    }
    bool timed_out = trevrpc_stream_wait_timeout_elapsed(c_stream, wait_started_nanos) == 1;
    native_stream_release(stream);
    return timed_out;
}

static void stream_send_execute(napi_env env, void* data);
static void stream_send_complete(napi_env env, napi_status status, void* data);
static void stream_send_many_execute(napi_env env, void* data);
static void stream_send_many_complete(napi_env env, napi_status status, void* data);

static napi_value native_stream_send_message(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "sendMessage requires a body");
        return NULL;
    }
    stream_send_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate send work");
        return NULL;
    }
    if (!unwrap_native_stream(env, this_arg, &work->stream) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    int err = copy_bytes_arg(env, args[0], &work->body, &work->body_len);
    if (err != 0) {
        napi_delete_reference(env, work->base.receiver_ref);
        free(work->body);
        free(work);
        if (err == -ENOMEM) {
            napi_throw_error(env, NULL, "failed to allocate sendMessage body");
        } else {
            napi_throw_type_error(env, NULL, "invalid sendMessage body");
        }
        return NULL;
    }
    work->base.err = native_stream_outbound_work_reserve(work->stream, &work->base);
    return queue_work_managed(env,
        &work->base,
        "sendMessage",
        stream_send_execute,
        stream_send_complete,
        stream_send_complete,
        native_stream_work_cancel);
}

static void stream_send_execute(napi_env env, void* data) {
    (void)env;
    stream_send_work* work = data;
    if (!native_stream_outbound_is_head(work->stream, &work->base)) {
        work->base.retry = true;
        return;
    }
    trevrpc_stream* stream = NULL;
    work->base.err = native_stream_operation_acquire(work->stream, &stream);
    if (work->base.err == -EAGAIN) {
        work->base.err = 0;
        work->base.retry = true;
        return;
    }
    if (work->base.err != 0) {
        native_stream_outbound_finish(work->stream, &work->base);
        return;
    }
    work->acquired = true;
    work->base.err = trevrpc_stream_send_message(stream, work->body, work->body_len);
    native_stream_operation_release(work->stream);
    work->acquired = false;
    native_stream_outbound_finish(work->stream, &work->base);
}

static void stream_send_complete(napi_env env, napi_status status, void* data) {
    stream_send_work* work = data;
    if (env != NULL && status == napi_ok && work->base.err == 0) {
        napi_value undefined = NULL;
        napi_get_undefined(env, &undefined);
        napi_resolve_deferred(env, work->base.deferred, undefined);
    } else if (env != NULL) {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "sendMessage");
    }
    if (work->acquired) {
        native_stream_operation_release(work->stream);
    }
    native_stream_outbound_finish(work->stream, &work->base);
    free(work->body);
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    native_work_delete(work->base.work);
    free(work);
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
    stream_send_many_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate sendMessages work");
        return NULL;
    }
    if (!unwrap_native_stream(env, this_arg, &work->stream) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    int err = copy_bytes_array_arg(env, args[0], &work->bodies, &work->body_lens, &work->count);
    if (err != 0) {
        napi_delete_reference(env, work->base.receiver_ref);
        free(work->bodies);
        free(work->body_lens);
        free(work);
        if (err == -ENOMEM) {
            napi_throw_error(env, NULL, "failed to allocate sendMessages bodies");
        } else {
            napi_throw_type_error(env, NULL, "invalid sendMessages bodies");
        }
        return NULL;
    }
    work->base.err = native_stream_outbound_work_reserve(work->stream, &work->base);
    return queue_work_managed(env,
        &work->base,
        "sendMessages",
        stream_send_many_execute,
        stream_send_many_complete,
        stream_send_many_complete,
        native_stream_work_cancel);
}

static void stream_send_many_execute(napi_env env, void* data) {
    (void)env;
    stream_send_many_work* work = data;
    if (!native_stream_outbound_is_head(work->stream, &work->base)) {
        work->base.retry = true;
        return;
    }
    trevrpc_stream* stream = NULL;
    work->base.err = native_stream_operation_acquire(work->stream, &stream);
    if (work->base.err == -EAGAIN) {
        work->base.err = 0;
        work->base.retry = true;
        return;
    }
    if (work->base.err != 0) {
        native_stream_outbound_finish(work->stream, &work->base);
        return;
    }
    work->acquired = true;
    work->base.err = trevrpc_stream_send_messages(stream, work->bodies, work->body_lens, work->count);
    native_stream_operation_release(work->stream);
    work->acquired = false;
    native_stream_outbound_finish(work->stream, &work->base);
}

static void stream_send_many_complete(napi_env env, napi_status status, void* data) {
    stream_send_many_work* work = data;
    if (env != NULL && status == napi_ok && work->base.err == 0) {
        napi_value undefined = NULL;
        napi_get_undefined(env, &undefined);
        napi_resolve_deferred(env, work->base.deferred, undefined);
    } else if (env != NULL) {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "sendMessages");
    }
    if (work->acquired) {
        native_stream_operation_release(work->stream);
    }
    native_stream_outbound_finish(work->stream, &work->base);
    free(work->bodies);
    free(work->body_lens);
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    native_work_delete(work->base.work);
    free(work);
}

static void stream_finish_execute(napi_env env, void* data) {
    (void)env;
    stream_finish_work* work = data;
    if (!native_stream_outbound_is_head(work->stream, &work->base)) {
        work->base.retry = true;
        return;
    }
    trevrpc_stream* stream = NULL;
    work->base.err = native_stream_operation_acquire(work->stream, &stream);
    if (work->base.err == -EAGAIN) {
        work->base.err = 0;
        work->base.retry = true;
        return;
    }
    if (work->base.err != 0) {
        native_stream_outbound_finish(work->stream, &work->base);
        return;
    }
    work->acquired = true;
    work->base.err = trevrpc_stream_finish_send(stream);
    native_stream_operation_release(work->stream);
    work->acquired = false;
    native_stream_outbound_finish(work->stream, &work->base);
}

static void stream_finish_complete(napi_env env, napi_status status, void* data) {
    stream_finish_work* work = data;
    if (env != NULL && status == napi_ok && work->base.err == 0) {
        napi_value undefined = NULL;
        napi_get_undefined(env, &undefined);
        napi_resolve_deferred(env, work->base.deferred, undefined);
    } else if (env != NULL) {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "finishSend");
    }
    if (work->acquired) {
        native_stream_operation_release(work->stream);
    }
    native_stream_outbound_finish(work->stream, &work->base);
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    native_work_delete(work->base.work);
    free(work);
}

static napi_value native_stream_finish_send(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    stream_finish_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate finishSend work");
        return NULL;
    }
    if (!unwrap_native_stream(env, this_arg, &work->stream) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    work->base.err = native_stream_outbound_work_reserve(work->stream, &work->base);
    return queue_work_managed(env,
        &work->base,
        "finishSend",
        stream_finish_execute,
        stream_finish_complete,
        stream_finish_complete,
        native_stream_work_cancel);
}

static void stream_recv_execute(napi_env env, void* data) {
    (void)env;
    stream_recv_work* work = data;
    if (native_stream_wait_timed_out(work->stream, work->base.queued_at_nanos)) {
        work->base.err = TREVRPC_ERR_STREAM_IDLE_TIMEOUT;
        native_stream_close_request(work->stream);
        return;
    }
    trevrpc_stream* stream = NULL;
    work->base.err = native_stream_operation_acquire(work->stream, &stream);
    if (work->base.err == -EAGAIN) {
        work->base.err = 0;
        work->base.retry = true;
        return;
    }
    if (work->base.err != 0) {
        return;
    }
    work->acquired = true;
    int ready = 0;
    work->base.err = trevrpc_stream_recv_ready_since(stream, &work->frame, &ready, work->base.queued_at_nanos);
    native_stream_operation_release(work->stream);
    work->acquired = false;
    if (work->base.err == 0 && !ready) {
        work->base.retry = true;
    }
}

static void stream_recv_complete(napi_env env, napi_status status, void* data) {
    stream_recv_work* work = data;
    if (env != NULL && work->base.err == 0 && status == napi_ok) {
        napi_value value = NULL;
        if (work->frame == NULL) {
            napi_get_null(env, &value);
        } else {
            value = stream_frame_to_js(env, work->frame);
        }
        napi_resolve_deferred(env, work->base.deferred, value);
    } else if (env != NULL) {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "recv");
    }
    trevrpc_stream_frame_free(work->frame);
    if (work->acquired) {
        native_stream_operation_release(work->stream);
    }
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    native_work_delete(work->base.work);
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
    work->base.err = native_stream_work_reserve(work->stream, &work->base);
    return queue_work_managed(env,
        &work->base,
        "recv",
        stream_recv_execute,
        stream_recv_complete,
        stream_recv_complete,
        native_stream_work_cancel);
}

static void stream_recv_many_execute(napi_env env, void* data) {
    (void)env;
    stream_recv_many_work* work = data;
    if (native_stream_wait_timed_out(work->stream, work->base.queued_at_nanos)) {
        work->base.err = TREVRPC_ERR_STREAM_IDLE_TIMEOUT;
        native_stream_close_request(work->stream);
        return;
    }
    trevrpc_stream* stream = NULL;
    work->base.err = native_stream_operation_acquire(work->stream, &stream);
    if (work->base.err == -EAGAIN) {
        work->base.err = 0;
        work->base.retry = true;
        return;
    }
    if (work->base.err != 0) {
        return;
    }
    work->acquired = true;
    int ready = 0;
    work->base.err = recv_many_ready_from_stream(stream,
        work->max_frames,
        &work->frames,
        &work->frames_len,
        &work->frames_cap,
        &work->eof,
        &ready,
        work->base.queued_at_nanos);
    native_stream_operation_release(work->stream);
    work->acquired = false;
    if (work->base.err == 0 && !ready) {
        work->base.retry = true;
    }
}

static void stream_recv_many_complete(napi_env env, napi_status status, void* data) {
    stream_recv_many_work* work = data;
    if (env != NULL && work->base.err == 0 && status == napi_ok) {
        napi_value frames = stream_frame_list_to_js(env, work->frames, work->frames_len, work->eof);
        napi_resolve_deferred(env, work->base.deferred, frames);
    } else if (env != NULL) {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "recvMany");
    }
    stream_frame_list_reset(work->frames, work->frames_len);
    if (work->acquired) {
        native_stream_operation_release(work->stream);
    }
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    native_work_delete(work->base.work);
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
    work->base.err = native_stream_work_reserve(work->stream, &work->base);
    return queue_work_managed(env,
        &work->base,
        "recvMany",
        stream_recv_many_execute,
        stream_recv_many_complete,
        stream_recv_many_complete,
        native_stream_work_cancel);
}

static void stream_recv_body_batch_complete(napi_env env, napi_status status, void* data) {
    stream_recv_many_work* work = data;
    if (env != NULL && work->base.err == 0 && status == napi_ok) {
        napi_value batch = stream_body_batch_to_js(env, work->frames, work->frames_len, work->eof);
        napi_resolve_deferred(env, work->base.deferred, batch);
    } else if (env != NULL) {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "recvBodyBatch");
    }
    stream_frame_list_reset(work->frames, work->frames_len);
    if (work->acquired) {
        native_stream_operation_release(work->stream);
    }
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    native_work_delete(work->base.work);
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
    work->base.err = native_stream_work_reserve(work->stream, &work->base);
    return queue_work_managed(env,
        &work->base,
        "recvBodyBatch",
        stream_recv_many_execute,
        stream_recv_body_batch_complete,
        stream_recv_body_batch_complete,
        native_stream_work_cancel);
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

#ifdef TREVRPC_NODE_TEST_HOOKS
static napi_value native_stream_debug_operation_locked(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    native_stream* stream = NULL;
    if (!unwrap_native_stream(env, this_arg, &stream)) {
        return NULL;
    }
    int err = pthread_mutex_trylock(&stream->operation_mutex);
    bool locked = err == EBUSY;
    if (err == 0) {
        pthread_mutex_unlock(&stream->operation_mutex);
    } else if (err != EBUSY) {
        napi_throw_error(env, NULL, "failed to inspect native stream operation lock");
        return NULL;
    }
    napi_value result = NULL;
    napi_get_boolean(env, locked, &result);
    return result;
}
#endif

typedef struct native_call_work_prefix {
    base_work base;
    native_call* call;
} native_call_work_prefix;

static void native_call_work_cancel(void* data) {
    native_call_work_prefix* work = data;
    native_call_close_request(work->call);
}

static bool native_call_wait_timed_out(native_call* call, uint64_t wait_started_nanos) {
    trevrpc_call* c_call = NULL;
    if (native_call_acquire(call, &c_call) != 0) {
        return false;
    }
    trevrpc_stream* stream = trevrpc_call_stream(c_call);
    bool timed_out = stream != NULL && trevrpc_stream_wait_timeout_elapsed(stream, wait_started_nanos) == 1;
    native_call_release_keep_wrapper(call, c_call);
    return timed_out;
}

static void call_send_execute(napi_env env, void* data);
static void call_send_complete(napi_env env, napi_status status, void* data);
static void call_send_many_execute(napi_env env, void* data);
static void call_send_many_complete(napi_env env, napi_status status, void* data);

static void call_respond_execute(napi_env env, void* data) {
    (void)env;
    call_response_work* work = data;
    if (!native_call_outbound_is_head(work->call, &work->base)) {
        work->base.retry = true;
        return;
    }
    work->base.err = native_call_operation_acquire(work->call, true, &work->c_call, &work->base);
    if (work->base.err != 0) {
        native_call_outbound_finish(work->call, &work->base);
        return;
    }
    if (work->base.retry) {
        return;
    }
    work->acquired = true;
    work->base.err = trevrpc_call_respond(work->c_call, &work->response);
    trevrpc_call_close(work->c_call);
    native_call_terminal_operation_complete(work->call, work->c_call, &work->base);
    work->c_call = NULL;
    work->acquired = false;
    work->terminal_released = true;
    native_call_outbound_finish(work->call, &work->base);
}

static void call_respond_complete(napi_env env, napi_status status, void* data) {
    call_response_work* work = data;
    if (env != NULL && status == napi_ok && work->base.err == 0) {
        napi_value undefined = NULL;
        napi_get_undefined(env, &undefined);
        napi_resolve_deferred(env, work->base.deferred, undefined);
    } else if (env != NULL) {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "respond");
    }
    if (work->acquired) {
        native_call_work_operation_release(work->call, work->c_call);
    }
    if (!work->terminal_released) {
        native_call_terminal_abandon(work->call, &work->base);
    }
    native_call_outbound_finish(work->call, &work->base);
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    native_work_delete(work->base.work);
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
        throw_if_no_pending_exception(env, "invalid response object");
        return NULL;
    }
    work->base.err = native_call_outbound_work_reserve(work->call, &work->base);
    return queue_work_managed(env,
        &work->base,
        "respond",
        call_respond_execute,
        call_respond_complete,
        call_respond_complete,
        native_call_work_cancel);
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
    call_send_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate send work");
        return NULL;
    }
    if (!unwrap_native_call(env, this_arg, &work->call) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    int err = copy_bytes_arg(env, args[0], &work->body, &work->body_len);
    if (err != 0) {
        napi_delete_reference(env, work->base.receiver_ref);
        free(work->body);
        free(work);
        if (err == -ENOMEM) {
            napi_throw_error(env, NULL, "failed to allocate sendMessage body");
        } else {
            napi_throw_type_error(env, NULL, "invalid sendMessage body");
        }
        return NULL;
    }
    work->base.err = native_call_outbound_work_reserve(work->call, &work->base);
    return queue_work_managed(env,
        &work->base,
        "sendMessage",
        call_send_execute,
        call_send_complete,
        call_send_complete,
        native_call_work_cancel);
}

static void call_send_execute(napi_env env, void* data) {
    (void)env;
    call_send_work* work = data;
    if (!native_call_outbound_is_head(work->call, &work->base)) {
        work->base.retry = true;
        return;
    }
    work->base.err = native_call_operation_acquire(work->call, false, &work->c_call, &work->base);
    if (work->base.err != 0) {
        native_call_outbound_finish(work->call, &work->base);
        return;
    }
    if (work->base.retry) {
        return;
    }
    work->acquired = true;
    trevrpc_stream* stream = trevrpc_call_stream(work->c_call);
    work->base.err = stream == NULL ? TREVRPC_ERR_UNSUPPORTED_RPC_KIND
                                    : trevrpc_stream_send_message(stream, work->body, work->body_len);
    native_call_work_operation_release(work->call, work->c_call);
    work->c_call = NULL;
    work->acquired = false;
    native_call_outbound_finish(work->call, &work->base);
}

static void call_send_complete(napi_env env, napi_status status, void* data) {
    call_send_work* work = data;
    if (env != NULL && status == napi_ok && work->base.err == 0) {
        napi_value undefined = NULL;
        napi_get_undefined(env, &undefined);
        napi_resolve_deferred(env, work->base.deferred, undefined);
    } else if (env != NULL) {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "sendMessage");
    }
    if (work->acquired) {
        native_call_work_operation_release(work->call, work->c_call);
    }
    native_call_outbound_finish(work->call, &work->base);
    free(work->body);
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    native_work_delete(work->base.work);
    free(work);
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
    call_send_many_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate sendMessages work");
        return NULL;
    }
    if (!unwrap_native_call(env, this_arg, &work->call) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    int err = copy_bytes_array_arg(env, args[0], &work->bodies, &work->body_lens, &work->count);
    if (err != 0) {
        napi_delete_reference(env, work->base.receiver_ref);
        free(work->bodies);
        free(work->body_lens);
        free(work);
        if (err == -ENOMEM) {
            napi_throw_error(env, NULL, "failed to allocate sendMessages bodies");
        } else {
            napi_throw_type_error(env, NULL, "invalid sendMessages bodies");
        }
        return NULL;
    }
    work->base.err = native_call_outbound_work_reserve(work->call, &work->base);
    return queue_work_managed(env,
        &work->base,
        "sendMessages",
        call_send_many_execute,
        call_send_many_complete,
        call_send_many_complete,
        native_call_work_cancel);
}

static void call_send_many_execute(napi_env env, void* data) {
    (void)env;
    call_send_many_work* work = data;
    if (!native_call_outbound_is_head(work->call, &work->base)) {
        work->base.retry = true;
        return;
    }
    work->base.err = native_call_operation_acquire(work->call, false, &work->c_call, &work->base);
    if (work->base.err != 0) {
        native_call_outbound_finish(work->call, &work->base);
        return;
    }
    if (work->base.retry) {
        return;
    }
    work->acquired = true;
    trevrpc_stream* stream = trevrpc_call_stream(work->c_call);
    work->base.err = stream == NULL ? TREVRPC_ERR_UNSUPPORTED_RPC_KIND
                                    : trevrpc_stream_send_messages(stream, work->bodies, work->body_lens, work->count);
    native_call_work_operation_release(work->call, work->c_call);
    work->c_call = NULL;
    work->acquired = false;
    native_call_outbound_finish(work->call, &work->base);
}

static void call_send_many_complete(napi_env env, napi_status status, void* data) {
    call_send_many_work* work = data;
    if (env != NULL && status == napi_ok && work->base.err == 0) {
        napi_value undefined = NULL;
        napi_get_undefined(env, &undefined);
        napi_resolve_deferred(env, work->base.deferred, undefined);
    } else if (env != NULL) {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "sendMessages");
    }
    if (work->acquired) {
        native_call_work_operation_release(work->call, work->c_call);
    }
    native_call_outbound_finish(work->call, &work->base);
    free(work->bodies);
    free(work->body_lens);
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    native_work_delete(work->base.work);
    free(work);
}

static void call_finish_execute(napi_env env, void* data) {
    (void)env;
    call_finish_work* work = data;
    if (!native_call_outbound_is_head(work->call, &work->base)) {
        work->base.retry = true;
        return;
    }
    work->base.err = native_call_operation_acquire(work->call, true, &work->c_call, &work->base);
    if (work->base.err != 0) {
        native_call_outbound_finish(work->call, &work->base);
        return;
    }
    if (work->base.retry) {
        return;
    }
    work->acquired = true;
    work->base.err = trevrpc_call_finish_stream_with_metadata(
        work->c_call, work->status, work->message, work->message_len, &work->metadata);
    trevrpc_call_close(work->c_call);
    native_call_terminal_operation_complete(work->call, work->c_call, &work->base);
    work->c_call = NULL;
    work->acquired = false;
    work->terminal_released = true;
    native_call_outbound_finish(work->call, &work->base);
}

static void call_finish_complete(napi_env env, napi_status status, void* data) {
    call_finish_work* work = data;
    if (env != NULL && status == napi_ok && work->base.err == 0) {
        napi_value undefined = NULL;
        napi_get_undefined(env, &undefined);
        napi_resolve_deferred(env, work->base.deferred, undefined);
    } else if (env != NULL) {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "finishStream");
    }
    if (work->acquired) {
        native_call_work_operation_release(work->call, work->c_call);
    }
    if (!work->terminal_released) {
        native_call_terminal_abandon(work->call, &work->base);
    }
    native_call_outbound_finish(work->call, &work->base);
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    native_work_delete(work->base.work);
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
    work->base.err = native_call_outbound_work_reserve(work->call, &work->base);
    return queue_work_managed(env,
        &work->base,
        "finishStream",
        call_finish_execute,
        call_finish_complete,
        call_finish_complete,
        native_call_work_cancel);
}

static void call_recv_execute(napi_env env, void* data) {
    (void)env;
    call_recv_work* work = data;
    if (native_call_wait_timed_out(work->call, work->base.queued_at_nanos)) {
        work->base.err = TREVRPC_ERR_STREAM_IDLE_TIMEOUT;
        native_call_close_request(work->call);
        return;
    }
    work->base.err = native_call_operation_acquire(work->call, false, &work->c_call, &work->base);
    if (work->base.err != 0) {
        return;
    }
    if (work->base.retry) {
        return;
    }
    work->acquired = true;
    trevrpc_stream* stream = trevrpc_call_stream(work->c_call);
    int ready = 0;
    work->base.err = stream == NULL
                         ? TREVRPC_ERR_UNSUPPORTED_RPC_KIND
                         : trevrpc_stream_recv_ready_since(stream, &work->frame, &ready, work->base.queued_at_nanos);
    native_call_work_operation_release(work->call, work->c_call);
    work->c_call = NULL;
    work->acquired = false;
    if (work->base.err == 0 && !ready) {
        work->base.retry = true;
    }
}

static void call_recv_complete(napi_env env, napi_status status, void* data) {
    call_recv_work* work = data;
    if (env != NULL && status == napi_ok && work->base.err == 0) {
        napi_value value = NULL;
        if (work->frame == NULL) {
            napi_get_null(env, &value);
        } else {
            value = stream_frame_to_js(env, work->frame);
        }
        napi_resolve_deferred(env, work->base.deferred, value);
    } else if (env != NULL) {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "recv");
    }
    trevrpc_stream_frame_free(work->frame);
    if (work->acquired) {
        native_call_work_operation_release(work->call, work->c_call);
    }
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    native_work_delete(work->base.work);
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
    work->base.err = native_call_work_reserve(work->call, &work->base);
    return queue_work_managed(
        env, &work->base, "recv", call_recv_execute, call_recv_complete, call_recv_complete, native_call_work_cancel);
}

static void call_recv_many_execute(napi_env env, void* data) {
    (void)env;
    call_recv_many_work* work = data;
    if (native_call_wait_timed_out(work->call, work->base.queued_at_nanos)) {
        work->base.err = TREVRPC_ERR_STREAM_IDLE_TIMEOUT;
        native_call_close_request(work->call);
        return;
    }
    work->base.err = native_call_operation_acquire(work->call, false, &work->c_call, &work->base);
    if (work->base.err != 0) {
        return;
    }
    if (work->base.retry) {
        return;
    }
    work->acquired = true;
    trevrpc_stream* stream = trevrpc_call_stream(work->c_call);
    int ready = 0;
    work->base.err = stream == NULL ? TREVRPC_ERR_UNSUPPORTED_RPC_KIND
                                    : recv_many_ready_from_stream(stream,
                                          work->max_frames,
                                          &work->frames,
                                          &work->frames_len,
                                          &work->frames_cap,
                                          &work->eof,
                                          &ready,
                                          work->base.queued_at_nanos);
    native_call_work_operation_release(work->call, work->c_call);
    work->c_call = NULL;
    work->acquired = false;
    if (work->base.err == 0 && !ready) {
        work->base.retry = true;
    }
}

static void call_recv_many_complete(napi_env env, napi_status status, void* data) {
    call_recv_many_work* work = data;
    if (env != NULL && work->base.err == 0 && status == napi_ok) {
        napi_value frames = stream_frame_list_to_js(env, work->frames, work->frames_len, work->eof);
        napi_resolve_deferred(env, work->base.deferred, frames);
    } else if (env != NULL) {
        reject_native_error(env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "recvMany");
    }
    stream_frame_list_reset(work->frames, work->frames_len);
    if (work->acquired) {
        native_call_work_operation_release(work->call, work->c_call);
    }
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    native_work_delete(work->base.work);
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
    work->base.err = native_call_work_reserve(work->call, &work->base);
    return queue_work_managed(env,
        &work->base,
        "recvMany",
        call_recv_many_execute,
        call_recv_many_complete,
        call_recv_many_complete,
        native_call_work_cancel);
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

#ifdef TREVRPC_NODE_TEST_HOOKS
static napi_value native_call_debug_operation_locked(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    native_call* call = NULL;
    if (!unwrap_native_call(env, this_arg, &call)) {
        return NULL;
    }
    int err = pthread_mutex_trylock(&call->operation_mutex);
    bool locked = err == EBUSY;
    if (err == 0) {
        pthread_mutex_unlock(&call->operation_mutex);
    } else if (err != EBUSY) {
        napi_throw_error(env, NULL, "failed to inspect native call operation lock");
        return NULL;
    }
    napi_value result = NULL;
    napi_get_boolean(env, locked, &result);
    return result;
}

static void debug_pending_resource_destroy(debug_pending_resource* resource) {
    pthread_mutex_destroy(&resource->mutex);
    free(resource);
}

static void debug_pending_resource_maybe_destroy(debug_pending_resource* resource) {
    bool destroy = false;
    pthread_mutex_lock(&resource->mutex);
    destroy = !resource->js_alive && resource->refs == 0 && resource->closed;
    pthread_mutex_unlock(&resource->mutex);
    if (destroy) {
        debug_pending_resource_destroy(resource);
    }
}

static int debug_pending_resource_acquire(debug_pending_resource* resource) {
    pthread_mutex_lock(&resource->mutex);
    if (resource->closing || resource->closed) {
        pthread_mutex_unlock(&resource->mutex);
        return TREV_NODE_ERR_CLOSED;
    }
    resource->refs++;
    pthread_mutex_unlock(&resource->mutex);
    return 0;
}

static void debug_pending_resource_release(debug_pending_resource* resource) {
    bool closed_now = false;
    bool destroy = false;
    pthread_mutex_lock(&resource->mutex);
    if (resource->refs > 0) {
        resource->refs--;
    }
    if (resource->closing && resource->refs == 0 && !resource->closed) {
        resource->closed = true;
        closed_now = true;
    }
    destroy = !resource->js_alive && resource->refs == 0 && resource->closed;
    pthread_mutex_unlock(&resource->mutex);
    if (closed_now) {
        atomic_fetch_add_explicit(&DebugPendingResourceCloses, 1, memory_order_relaxed);
    }
    if (destroy) {
        debug_pending_resource_destroy(resource);
    }
}

static void debug_pending_resource_close_request(debug_pending_resource* resource) {
    bool closed_now = false;
    pthread_mutex_lock(&resource->mutex);
    resource->closing = true;
    if (resource->refs == 0 && !resource->closed) {
        resource->closed = true;
        closed_now = true;
    }
    pthread_mutex_unlock(&resource->mutex);
    if (closed_now) {
        atomic_fetch_add_explicit(&DebugPendingResourceCloses, 1, memory_order_relaxed);
    }
    debug_pending_resource_maybe_destroy(resource);
}

static void debug_pending_resource_finalize(napi_env env, void* data, void* hint) {
    (void)env;
    (void)hint;
    debug_pending_resource* resource = data;
    atomic_fetch_add_explicit(&DebugPendingResourceFinalizers, 1, memory_order_relaxed);
    pthread_mutex_lock(&resource->mutex);
    resource->js_alive = false;
    pthread_mutex_unlock(&resource->mutex);
    debug_pending_resource_close_request(resource);
}

static bool unwrap_debug_pending_resource(napi_env env, napi_value receiver, debug_pending_resource** out_resource) {
    *out_resource = NULL;
    if (napi_unwrap(env, receiver, (void**)out_resource) != napi_ok || *out_resource == NULL) {
        napi_throw_type_error(env, NULL, "invalid debug pending resource receiver");
        return false;
    }
    return true;
}

static void debug_sleep_ms(uint32_t delay_ms) {
    struct timespec remaining = {
        .tv_sec = delay_ms / 1000u,
        .tv_nsec = (long)(delay_ms % 1000u) * 1000 * 1000,
    };
    while (nanosleep(&remaining, &remaining) != 0 && errno == EINTR) {
    }
}

static void debug_pending_wait_execute(napi_env env, void* data) {
    (void)env;
    debug_pending_wait_work* work = data;
    work->base.err = debug_pending_resource_acquire(work->resource);
    if (work->base.err != 0) {
        return;
    }
    work->acquired = true;
    debug_sleep_ms(work->delay_ms);
    debug_pending_resource_release(work->resource);
    work->acquired = false;
}

static void debug_pending_wait_complete(napi_env env, napi_status status, void* data) {
    debug_pending_wait_work* work = data;
    if (env != NULL && status == napi_ok && work->base.err == 0) {
        napi_value undefined = NULL;
        napi_get_undefined(env, &undefined);
        napi_resolve_deferred(env, work->base.deferred, undefined);
    } else if (env != NULL) {
        reject_native_error(
            env, work->base.deferred, status == napi_ok ? work->base.err : -ECANCELED, "debugPendingWait");
    }
    if (work->acquired) {
        debug_pending_resource_release(work->resource);
    }
    if (env != NULL && work->base.receiver_ref != NULL) {
        napi_delete_reference(env, work->base.receiver_ref);
    }
    native_work_delete(work->base.work);
    free(work);
}

static napi_value debug_pending_resource_wait(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &argc, args, &this_arg, NULL);

    debug_pending_wait_work* work = calloc(1, sizeof(*work));
    if (work == NULL) {
        napi_throw_error(env, NULL, "failed to allocate debug pending wait work");
        return NULL;
    }
    work->delay_ms = 50;
    if (argc > 0) {
        (void)napi_get_value_uint32(env, args[0], &work->delay_ms);
    }
    if (!unwrap_debug_pending_resource(env, this_arg, &work->resource) ||
        !create_receiver_ref(env, this_arg, &work->base.receiver_ref)) {
        free(work);
        return NULL;
    }
    return queue_work(env, &work->base, "debugPendingWait", debug_pending_wait_execute, debug_pending_wait_complete);
}

static napi_value debug_pending_resource_close(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    debug_pending_resource* resource = NULL;
    if (!unwrap_debug_pending_resource(env, this_arg, &resource)) {
        return NULL;
    }
    debug_pending_resource_close_request(resource);
    napi_value undefined = NULL;
    napi_get_undefined(env, &undefined);
    return undefined;
}

static napi_value debug_pending_resource_closed(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    debug_pending_resource* resource = NULL;
    if (!unwrap_debug_pending_resource(env, this_arg, &resource)) {
        return NULL;
    }

    pthread_mutex_lock(&resource->mutex);
    bool closed = resource->closed;
    pthread_mutex_unlock(&resource->mutex);
    napi_value result = NULL;
    napi_get_boolean(env, closed, &result);
    return result;
}

static napi_value debug_pending_resource_refs(napi_env env, napi_callback_info info) {
    napi_value this_arg = NULL;
    napi_get_cb_info(env, info, &(size_t){0}, NULL, &this_arg, NULL);
    debug_pending_resource* resource = NULL;
    if (!unwrap_debug_pending_resource(env, this_arg, &resource)) {
        return NULL;
    }

    pthread_mutex_lock(&resource->mutex);
    uint32_t refs = resource->refs > UINT32_MAX ? UINT32_MAX : (uint32_t)resource->refs;
    pthread_mutex_unlock(&resource->mutex);
    napi_value result = NULL;
    napi_create_uint32(env, refs, &result);
    return result;
}

static napi_value debug_create_pending_resource(napi_env env, napi_callback_info info) {
    (void)info;
    debug_pending_resource* resource = calloc(1, sizeof(*resource));
    if (resource == NULL) {
        napi_throw_error(env, NULL, "failed to allocate debug pending resource");
        return NULL;
    }
    pthread_mutex_init(&resource->mutex, NULL);
    resource->js_alive = true;

    napi_value object = NULL;
    if (napi_create_object(env, &object) != napi_ok ||
        napi_wrap(env, object, resource, debug_pending_resource_finalize, NULL, NULL) != napi_ok) {
        debug_pending_resource_destroy(resource);
        napi_throw_error(env, NULL, "failed to create debug pending resource");
        return NULL;
    }

    napi_property_descriptor methods[] = {
        {"wait", NULL, debug_pending_resource_wait, NULL, NULL, NULL, napi_default, NULL},
        {"close", NULL, debug_pending_resource_close, NULL, NULL, NULL, napi_default, NULL},
        {"closed", NULL, debug_pending_resource_closed, NULL, NULL, NULL, napi_default, NULL},
        {"refs", NULL, debug_pending_resource_refs, NULL, NULL, NULL, napi_default, NULL},
    };
    napi_define_properties(env, object, sizeof(methods) / sizeof(methods[0]), methods);
    return object;
}

static napi_value debug_pending_resource_closes(napi_env env, napi_callback_info info) {
    (void)info;
    uint64_t count = atomic_load_explicit(&DebugPendingResourceCloses, memory_order_relaxed);
    napi_value value = NULL;
    napi_create_double(env, (double)count, &value);
    return value;
}

static napi_value debug_pending_resource_finalizers(napi_env env, napi_callback_info info) {
    (void)info;
    uint64_t count = atomic_load_explicit(&DebugPendingResourceFinalizers, memory_order_relaxed);
    napi_value value = NULL;
    napi_create_double(env, (double)count, &value);
    return value;
}

static int debug_borrowed_body_from_arg(
    napi_env env, napi_value value, uint8_t** out_owner, uint8_t** out_body, size_t* out_body_len) {
    *out_owner = NULL;
    *out_body = NULL;
    *out_body_len = 0;

    const uint8_t* source = NULL;
    size_t source_len = 0;
    int err = bytes_arg_view(env, value, &source, &source_len);
    if (err != 0) {
        return err;
    }
    if (source_len > SIZE_MAX - 16) {
        return -EOVERFLOW;
    }
    uint8_t* owner = malloc(source_len + 16);
    if (owner == NULL) {
        return -ENOMEM;
    }
    memset(owner, 0xa5, source_len + 16);
    if (source_len > 0) {
        memcpy(owner + 8, source, source_len);
        *out_body = owner + 8;
    }
    *out_owner = owner;
    *out_body_len = source_len;
    return 0;
}

static napi_value debug_external_arraybuffer_finalizers(napi_env env, napi_callback_info info) {
    (void)info;
    uint64_t count = atomic_load_explicit(&ExternalArrayBufferFinalizers, memory_order_relaxed);
    napi_value value = NULL;
    napi_create_double(env, (double)count, &value);
    return value;
}

static napi_value debug_make_borrowed_response(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, NULL, NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "_debugMakeBorrowedResponse requires a body");
        return NULL;
    }

    trevrpc_response response = {.status = TREVRPC_STATUS_OK};
    int err = debug_borrowed_body_from_arg(env, args[0], &response._body_owner, &response.body, &response.body_len);
    if (err != 0) {
        throw_native_error(env, err, "_debugMakeBorrowedResponse");
        return NULL;
    }

    napi_value object = response_to_js(env, &response);
    trevrpc_response_reset(&response);
    return object;
}

static napi_value debug_make_borrowed_stream_frame(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, NULL, NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "_debugMakeBorrowedStreamFrame requires a body");
        return NULL;
    }

    trevrpc_stream_frame frame = {.kind = TREVRPC_STREAM_FRAME_KIND_MESSAGE, .status = TREVRPC_STATUS_OK};
    int err = debug_borrowed_body_from_arg(env, args[0], &frame._body_owner, &frame.body, &frame.body_len);
    if (err != 0) {
        throw_native_error(env, err, "_debugMakeBorrowedStreamFrame");
        return NULL;
    }

    napi_value object = stream_frame_to_js(env, &frame);
    trevrpc_stream_frame_reset(&frame);
    return object;
}

static napi_value debug_make_borrowed_stream_body_batch(napi_env env, napi_callback_info info) {
    size_t argc = 1;
    napi_value args[1];
    napi_get_cb_info(env, info, &argc, args, NULL, NULL);
    if (argc != 1) {
        napi_throw_type_error(env, NULL, "_debugMakeBorrowedStreamBodyBatch requires a body");
        return NULL;
    }

    trevrpc_stream_frame frame = {.kind = TREVRPC_STREAM_FRAME_KIND_MESSAGE, .status = TREVRPC_STATUS_OK};
    int err = debug_borrowed_body_from_arg(env, args[0], &frame._body_owner, &frame.body, &frame.body_len);
    if (err != 0) {
        throw_native_error(env, err, "_debugMakeBorrowedStreamBodyBatch");
        return NULL;
    }
    trevrpc_stream_frame* frames[] = {&frame};
    napi_value object = stream_body_batch_to_js(env, frames, 1, false);
    trevrpc_stream_frame_reset(&frame);
    return object;
}

#endif

static napi_value init(napi_env env, napi_value exports) {
    native_completion_runtime* runtime = calloc(1, sizeof(*runtime));
    if (runtime == NULL || native_completion_runtime_init(env, runtime) != 0 ||
        napi_set_instance_data(env, runtime, NULL, NULL) != napi_ok ||
        napi_add_env_cleanup_hook(env, native_completion_runtime_cleanup, runtime) != napi_ok) {
        native_completion_runtime_shutdown(runtime);
        napi_throw_error(env, NULL, "failed to initialize native completion runtime");
        return NULL;
    }

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
    napi_create_reference(env, client_ctor, 1, &runtime->client_constructor);

    napi_property_descriptor stream_methods[] = {
        {"sendMessage", NULL, native_stream_send_message, NULL, NULL, NULL, napi_default, NULL},
        {"sendMessages", NULL, native_stream_send_messages, NULL, NULL, NULL, napi_default, NULL},
        {"finishSend", NULL, native_stream_finish_send, NULL, NULL, NULL, napi_default, NULL},
        {"recv", NULL, native_stream_recv, NULL, NULL, NULL, napi_default, NULL},
        {"recvMany", NULL, native_stream_recv_many, NULL, NULL, NULL, napi_default, NULL},
        {"recvBodyBatch", NULL, native_stream_recv_body_batch, NULL, NULL, NULL, napi_default, NULL},
        {"close", NULL, native_stream_close, NULL, NULL, NULL, napi_default, NULL},
#ifdef TREVRPC_NODE_TEST_HOOKS
        {"_debugOperationLocked", NULL, native_stream_debug_operation_locked, NULL, NULL, NULL, napi_default, NULL},
#endif
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
    napi_create_reference(env, stream_ctor, 1, &runtime->stream_constructor);

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
    napi_create_reference(env, server_ctor, 1, &runtime->server_constructor);

    napi_property_descriptor call_methods[] = {
        {"respond", NULL, native_call_respond, NULL, NULL, NULL, napi_default, NULL},
        {"sendMessage", NULL, native_call_send_message, NULL, NULL, NULL, napi_default, NULL},
        {"sendMessages", NULL, native_call_send_messages, NULL, NULL, NULL, napi_default, NULL},
        {"finishStream", NULL, native_call_finish_stream, NULL, NULL, NULL, napi_default, NULL},
        {"recv", NULL, native_call_recv, NULL, NULL, NULL, napi_default, NULL},
        {"recvMany", NULL, native_call_recv_many, NULL, NULL, NULL, napi_default, NULL},
        {"close", NULL, native_call_close, NULL, NULL, NULL, napi_default, NULL},
#ifdef TREVRPC_NODE_TEST_HOOKS
        {"_debugOperationLocked", NULL, native_call_debug_operation_locked, NULL, NULL, NULL, napi_default, NULL},
#endif
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
    napi_create_reference(env, call_ctor, 1, &runtime->call_constructor);

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
    napi_create_reference(env, cancellation_ctor, 1, &runtime->cancellation_constructor);

    napi_property_descriptor exports_desc[] = {
        {"createCancellation", NULL, native_create_cancellation, NULL, NULL, NULL, napi_default, NULL},
        {"connectMsQuic", NULL, connect_msquic, NULL, NULL, NULL, napi_default, NULL},
        {"listenMsQuic", NULL, listen_msquic, NULL, NULL, NULL, napi_default, NULL},
#ifdef TREVRPC_NODE_TEST_HOOKS
        {"_debugHttp3Admission", NULL, debug_http3_admission, NULL, NULL, NULL, napi_default, NULL},
        {"_debugExternalArrayBufferFinalizers",
            NULL,
            debug_external_arraybuffer_finalizers,
            NULL,
            NULL,
            NULL,
            napi_default,
            NULL},
        {"_debugMakeBorrowedResponse", NULL, debug_make_borrowed_response, NULL, NULL, NULL, napi_default, NULL},
        {"_debugMakeBorrowedStreamFrame", NULL, debug_make_borrowed_stream_frame, NULL, NULL, NULL, napi_default, NULL},
        {"_debugMakeBorrowedStreamBodyBatch",
            NULL,
            debug_make_borrowed_stream_body_batch,
            NULL,
            NULL,
            NULL,
            napi_default,
            NULL},
        {"_debugCreatePendingResource", NULL, debug_create_pending_resource, NULL, NULL, NULL, napi_default, NULL},
        {"_debugPendingResourceCloses", NULL, debug_pending_resource_closes, NULL, NULL, NULL, napi_default, NULL},
        {"_debugPendingResourceFinalizers",
            NULL,
            debug_pending_resource_finalizers,
            NULL,
            NULL,
            NULL,
            napi_default,
            NULL},
#endif
    };
    napi_define_properties(env, exports, sizeof(exports_desc) / sizeof(exports_desc[0]), exports_desc);
    return exports;
}

NAPI_MODULE(NODE_GYP_MODULE_NAME, init)
