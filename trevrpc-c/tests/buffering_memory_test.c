#define _POSIX_C_SOURCE 200809L

#include "trevrpc.h"
#include "trevrpc_raw.h"

#include <errno.h>
#if defined(__APPLE__)
#include <mach/mach.h>
#include <sys/resource.h>
#endif
#if defined(__GLIBC__)
#include <malloc.h>
#endif
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifndef TREVRPC_MSQUIC_TEST_CERT
#define TREVRPC_MSQUIC_TEST_CERT ""
#endif

#ifndef TREVRPC_MSQUIC_TEST_KEY
#define TREVRPC_MSQUIC_TEST_KEY ""
#endif

#define MSQUIC_2_5_8_DOCUMENTED_DEFAULT_STREAM_RECV_WINDOW (64u * 1024u)
#define MSQUIC_2_5_8_DOCUMENTED_DEFAULT_CONN_FLOW_CONTROL_WINDOW (16u * 1024u * 1024u)
#define MSQUIC_2_5_8_DOCUMENTED_DEFAULTS_BASIS "msquic-2.5.8-documented-defaults-not-observed"
#define PROFILE_MAX_FRAME_SIZE TREVRPC_DEFAULT_MAX_FRAME_SIZE
#define PROFILE_MAX_CUMULATIVE_BODY (16u * 1024u * 1024u)
#define PROFILE_MAX_CONCURRENCY 64u
#define PROFILE_IDLE_TIMEOUT_MS 30000u
#define PROFILE_RPC_IDLE_TIMEOUT_NANOS (30ull * 1000000000ull)
#define PEAK_FIXED_ALLOWANCE_BYTES (128u * 1024u * 1024u)
#define CONVERGENCE_FIXED_ALLOWANCE_BYTES (96u * 1024u * 1024u)
#define CONVERGENCE_WAIT_MS 1000u
#define SERVICE_NAME "trevrpc.buffering.Memory"
#define METHOD_NAME "Run"

typedef enum scenario_kind {
    SCENARIO_SLOW_READER,
    SCENARIO_STALLED_HANDLER,
    SCENARIO_RESET,
    SCENARIO_CLOSE,
    SCENARIO_OVERLOAD,
    SCENARIO_BODY_LIMIT,
} scenario_kind;

typedef struct buffering_profile {
    const char* name;
    const char* receive_window_basis;
    uint32_t configured_stream_recv_window;
    uint32_t configured_conn_flow_control_window;
    uint32_t bound_model_stream_recv_window;
    uint32_t bound_model_conn_flow_control_window;
    trevrpc_msquic_execution_profile execution_profile;
    int send_buffering_enabled;
} buffering_profile;

typedef struct call_registry {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    trevrpc_call** calls;
    size_t capacity;
    size_t count;
    size_t duplicate_ids;
    size_t invalid_ids;
    bool closing;
} call_registry;

typedef struct serve_args {
    trevrpc_server* server;
    int result;
} serve_args;

typedef struct rpc_fixture {
    trevrpc_server* server;
    trevrpc_raw_client* client;
    pthread_t serve_thread;
    bool serve_thread_started;
    serve_args serve;
    call_registry registry;
    bool registry_initialized;
} rpc_fixture;

typedef struct scenario_metrics {
    size_t admitted;
    size_t rejected;
    size_t initial_request_bytes;
    size_t submitted_bytes;
    size_t received_bytes;
    size_t message_frames;
    size_t ok_statuses;
    size_t resource_exhausted_statuses;
    size_t reset_closed_results;
    size_t close_eof_results;
    size_t body_limit_results;
    size_t send_failures;
    size_t invariant_failures;
    int observed_reset_result;
} scenario_metrics;

static const buffering_profile SafeProfile = {
    .name = "safe",
    .receive_window_basis = MSQUIC_2_5_8_DOCUMENTED_DEFAULTS_BASIS,
    .bound_model_stream_recv_window = MSQUIC_2_5_8_DOCUMENTED_DEFAULT_STREAM_RECV_WINDOW,
    .bound_model_conn_flow_control_window = MSQUIC_2_5_8_DOCUMENTED_DEFAULT_CONN_FLOW_CONTROL_WINDOW,
    .execution_profile = TREV_MSQUIC_EXECUTION_PROFILE_LOW_LATENCY,
    .send_buffering_enabled = 0,
};

static void usage(const char* program) {
    fprintf(stderr,
        "usage: %s SCENARIO CONCURRENCY REQUEST_FRAME RESPONSE_FRAME CUMULATIVE_BODY READER_PROGRESS "
        "HOLD_MS\n"
        "  SCENARIO: slow-reader | stalled-handler | reset | close | overload | body-limit\n"
        "  REQUEST_FRAME and RESPONSE_FRAME are TrevRPC message body bytes and may differ\n"
        "  READER_PROGRESS: submitted bytes per stream between high-level frame drains; 0 stalls the reader\n",
        program);
}

static uint64_t monotonic_nanos(void) {
    struct timespec now = {0};
    (void)clock_gettime(CLOCK_MONOTONIC, &now);
    return (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec;
}

static void sleep_millis(uint64_t millis) {
    struct timespec delay = {
        .tv_sec = (time_t)(millis / 1000),
        .tv_nsec = (long)(millis % 1000) * 1000000L,
    };
    while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
    }
}

static size_t process_status_kib(const char* key) {
#if defined(__APPLE__)
    if (strcmp(key, "VmRSS") == 0) {
        mach_task_basic_info_data_t info = {0};
        mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
        if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO, (task_info_t)&info, &count) != KERN_SUCCESS) {
            return 0;
        }
        return (size_t)(info.resident_size / 1024u);
    }
    if (strcmp(key, "VmHWM") == 0) {
        struct rusage usage = {0};
        if (getrusage(RUSAGE_SELF, &usage) != 0) {
            return 0;
        }
        return (size_t)((uint64_t)usage.ru_maxrss / 1024u);
    }
    return 0;
#else
    FILE* status = fopen("/proc/self/status", "r");
    if (status == NULL) {
        return 0;
    }
    char line[256];
    size_t value = 0;
    while (fgets(line, sizeof(line), status) != NULL) {
        char name[32];
        size_t parsed = 0;
        if (sscanf(line, "%31[^:]: %zu kB", name, &parsed) == 2 && strcmp(name, key) == 0) {
            value = parsed;
            break;
        }
    }
    fclose(status);
    return value;
#endif
}

static int parse_size(const char* value, size_t* out) {
    char* end = NULL;
    errno = 0;
    unsigned long long parsed = strtoull(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed > SIZE_MAX) {
        return -EINVAL;
    }
    *out = (size_t)parsed;
    return 0;
}

static size_t saturating_add(size_t left, size_t right) {
    return right > SIZE_MAX - left ? SIZE_MAX : left + right;
}

static size_t saturating_multiply(size_t left, size_t right) {
    return left != 0 && right > SIZE_MAX / left ? SIZE_MAX : left * right;
}

static int parse_scenario(const char* name, scenario_kind* scenario) {
    if (strcmp(name, "slow-reader") == 0) {
        *scenario = SCENARIO_SLOW_READER;
    } else if (strcmp(name, "stalled-handler") == 0) {
        *scenario = SCENARIO_STALLED_HANDLER;
    } else if (strcmp(name, "reset") == 0) {
        *scenario = SCENARIO_RESET;
    } else if (strcmp(name, "close") == 0) {
        *scenario = SCENARIO_CLOSE;
    } else if (strcmp(name, "overload") == 0) {
        *scenario = SCENARIO_OVERLOAD;
    } else if (strcmp(name, "body-limit") == 0) {
        *scenario = SCENARIO_BODY_LIMIT;
    } else {
        return -EINVAL;
    }
    return 0;
}

static uint32_t scenario_rpc_kind(scenario_kind scenario) {
    return scenario == SCENARIO_SLOW_READER ? TREVRPC_RPC_KIND_SERVER_STREAMING : TREVRPC_RPC_KIND_CLIENT_STREAMING;
}

static const char* scenario_expected_outcome(scenario_kind scenario) {
    switch (scenario) {
    case SCENARIO_SLOW_READER:
    case SCENARIO_STALLED_HANDLER:
    case SCENARIO_CLOSE:
        return "full-submission-ok";
    case SCENARIO_RESET:
        return "full-submission-reset-closed";
    case SCENARIO_OVERLOAD:
        return "exact-admission-exhaustion";
    case SCENARIO_BODY_LIMIT:
        return "exact-cumulative-exhaustion";
    }
    return "invalid";
}

static int registry_init(call_registry* registry, size_t capacity) {
    memset(registry, 0, sizeof(*registry));
    registry->calls = calloc(capacity, sizeof(*registry->calls));
    if (registry->calls == NULL) {
        return -ENOMEM;
    }
    registry->capacity = capacity;
    int err = pthread_mutex_init(&registry->mutex, NULL);
    if (err != 0) {
        free(registry->calls);
        registry->calls = NULL;
        return -err;
    }
    err = pthread_cond_init(&registry->cond, NULL);
    if (err != 0) {
        pthread_mutex_destroy(&registry->mutex);
        free(registry->calls);
        registry->calls = NULL;
        return -err;
    }
    return 0;
}

static void registry_close_calls(call_registry* registry) {
    if (registry == NULL || registry->calls == NULL) {
        return;
    }
    pthread_mutex_lock(&registry->mutex);
    registry->closing = true;
    pthread_cond_broadcast(&registry->cond);
    pthread_mutex_unlock(&registry->mutex);
    for (size_t i = 0; i < registry->capacity; i++) {
        pthread_mutex_lock(&registry->mutex);
        trevrpc_call* call = registry->calls[i];
        registry->calls[i] = NULL;
        pthread_mutex_unlock(&registry->mutex);
        if (call != NULL) {
            trevrpc_call_close(call);
        }
    }
}

static void registry_destroy(call_registry* registry) {
    if (registry == NULL || registry->calls == NULL) {
        return;
    }
    registry_close_calls(registry);
    pthread_cond_destroy(&registry->cond);
    pthread_mutex_destroy(&registry->mutex);
    free(registry->calls);
    registry->calls = NULL;
}

static size_t request_id(const trevrpc_request* request) {
    if (request == NULL || request->body == NULL || request->body_len < sizeof(uint64_t)) {
        return SIZE_MAX;
    }
    uint64_t value = 0;
    for (size_t i = 0; i < sizeof(value); i++) {
        value |= (uint64_t)request->body[i] << (i * 8);
    }
    return (size_t)value;
}

static int deferred_call_handler(void* user_data, trevrpc_call* call) {
    call_registry* registry = user_data;
    int err = trevrpc_call_defer(call);
    if (err != 0) {
        return err;
    }
    size_t id = request_id(trevrpc_call_request(call));

    pthread_mutex_lock(&registry->mutex);
    if (registry->closing) {
        pthread_mutex_unlock(&registry->mutex);
        trevrpc_call_close(call);
        return TREVRPC_CALL_DEFERRED;
    }
    if (id >= registry->capacity) {
        registry->invalid_ids++;
        pthread_mutex_unlock(&registry->mutex);
        return -EINVAL;
    }
    if (registry->calls[id] != NULL) {
        registry->duplicate_ids++;
        pthread_mutex_unlock(&registry->mutex);
        return -EEXIST;
    }
    registry->calls[id] = call;
    registry->count++;
    pthread_cond_broadcast(&registry->cond);
    pthread_mutex_unlock(&registry->mutex);
    return TREVRPC_CALL_DEFERRED;
}

static int realtime_deadline_after_ms(uint64_t timeout_ms, struct timespec* deadline) {
    if (clock_gettime(CLOCK_REALTIME, deadline) != 0) {
        return -errno;
    }
    deadline->tv_sec += (time_t)(timeout_ms / 1000);
    deadline->tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
    if (deadline->tv_nsec >= 1000000000L) {
        deadline->tv_sec++;
        deadline->tv_nsec -= 1000000000L;
    }
    return 0;
}

static bool registry_wait_count(call_registry* registry, size_t count, uint64_t timeout_ms) {
    struct timespec deadline = {0};
    if (realtime_deadline_after_ms(timeout_ms, &deadline) != 0) {
        return false;
    }
    pthread_mutex_lock(&registry->mutex);
    while (registry->count < count) {
        int err = pthread_cond_timedwait(&registry->cond, &registry->mutex, &deadline);
        if (err == ETIMEDOUT) {
            pthread_mutex_unlock(&registry->mutex);
            return false;
        }
    }
    bool valid = registry->count == count && registry->duplicate_ids == 0 && registry->invalid_ids == 0;
    pthread_mutex_unlock(&registry->mutex);
    return valid;
}

static trevrpc_call* registry_get(call_registry* registry, size_t id) {
    pthread_mutex_lock(&registry->mutex);
    trevrpc_call* call = id < registry->capacity ? registry->calls[id] : NULL;
    pthread_mutex_unlock(&registry->mutex);
    return call;
}

static trevrpc_call* registry_take(call_registry* registry, size_t id) {
    pthread_mutex_lock(&registry->mutex);
    trevrpc_call* call = id < registry->capacity ? registry->calls[id] : NULL;
    if (call != NULL) {
        registry->calls[id] = NULL;
    }
    pthread_mutex_unlock(&registry->mutex);
    return call;
}

static void* serve_main(void* context) {
    serve_args* args = context;
    args->result = trevrpc_server_serve(args->server);
    return NULL;
}

static int fixture_start(
    rpc_fixture* fixture, scenario_kind scenario, size_t concurrency, size_t cumulative_body, size_t min_message_size) {
    memset(fixture, 0, sizeof(*fixture));
    int err = registry_init(&fixture->registry, concurrency);
    if (err != 0) {
        return err;
    }
    fixture->registry_initialized = true;

    trevrpc_server_config_v1 server_config;
    err = trevrpc_server_config_v1_init(&server_config, sizeof(server_config));
    if (err != 0) {
        return err;
    }
    server_config.host = "127.0.0.1";
    server_config.port = 0;
    server_config.cert_file = TREVRPC_MSQUIC_TEST_CERT;
    server_config.key_file = TREVRPC_MSQUIC_TEST_KEY;
    server_config.max_idle_timeout_ms = PROFILE_IDLE_TIMEOUT_MS;
    server_config.keep_alive_ms = 15000;
    server_config.peer_bidi_stream_count = (uint16_t)concurrency;
    server_config.max_streams_per_session = (uint32_t)concurrency;
    server_config.max_frame_size = PROFILE_MAX_FRAME_SIZE;
    err = trevrpc_server_listen_v1(&server_config, &fixture->server);
    if (err != 0) {
        return err;
    }

    size_t admitted = scenario == SCENARIO_OVERLOAD ? concurrency / 2 : concurrency;
    if (admitted == 0) {
        admitted = 1;
    }
    size_t max_messages = cumulative_body / min_message_size + 4;
    trevrpc_server_options_v1 options;
    err = trevrpc_server_options_v1_init(&options, sizeof(options));
    if (err != 0) {
        return err;
    }
    options.max_concurrent_connections = 1;
    options.max_concurrent_streams_per_connection = (int64_t)concurrency;
    options.max_concurrent_requests = (int64_t)admitted;
    options.worker_count = (int64_t)(concurrency < 16 ? concurrency : 16);
    options.worker_queue_capacity = (int64_t)(concurrency * 2);
    options.initial_request_timeout_nanos = 5ull * 1000000000ull;
    options.max_stream_messages = (int64_t)max_messages;
    options.max_stream_body_size = (int64_t)cumulative_body;
    options.stream_idle_timeout_nanos = PROFILE_RPC_IDLE_TIMEOUT_NANOS;
    err = trevrpc_server_set_options_v1(fixture->server, &options);
    if (err == 0) {
        err = trevrpc_server_register_call(fixture->server,
            SERVICE_NAME,
            METHOD_NAME,
            scenario_rpc_kind(scenario),
            deferred_call_handler,
            &fixture->registry);
    }
    if (err == 0) {
        err = trevrpc_server_freeze(fixture->server);
    }
    if (err != 0) {
        return err;
    }

    fixture->serve.server = fixture->server;
    err = pthread_create(&fixture->serve_thread, NULL, serve_main, &fixture->serve);
    if (err != 0) {
        return -err;
    }
    fixture->serve_thread_started = true;

    uint16_t port = 0;
    err = trevrpc_server_port(fixture->server, &port);
    if (err != 0) {
        return err;
    }
    trevrpc_client_config_v1 client_config;
    err = trevrpc_client_config_v1_init(&client_config, sizeof(client_config));
    if (err != 0) {
        return err;
    }
    client_config.skip_certificate_validation = 1;
    client_config.max_idle_timeout_ms = PROFILE_IDLE_TIMEOUT_MS;
    client_config.keep_alive_ms = 15000;
    client_config.peer_bidi_stream_count = (uint16_t)concurrency;
    client_config.max_frame_size = PROFILE_MAX_FRAME_SIZE;
    return trevrpc_raw_client_connect_v1("127.0.0.1", port, &client_config, NULL, &fixture->client);
}

static int fixture_stop(rpc_fixture* fixture) {
    if (fixture->registry_initialized) {
        registry_close_calls(&fixture->registry);
    }
    trevrpc_raw_client_close(fixture->client);
    fixture->client = NULL;
    int err = fixture->server == NULL ? 0 : trevrpc_server_stop(fixture->server);
    if (fixture->serve_thread_started) {
        int join_err = pthread_join(fixture->serve_thread, NULL);
        fixture->serve_thread_started = false;
        if (join_err != 0 && err == 0) {
            err = -join_err;
        } else if (fixture->serve.result != 0 && err == 0) {
            err = fixture->serve.result;
        }
    }
    if (fixture->server != NULL) {
        int wait_err = trevrpc_server_wait_until(fixture->server, TREVRPC_DEADLINE_INFINITE);
        if (wait_err != 0 && err == 0) {
            err = wait_err;
        }
        int release_err = trevrpc_server_release(fixture->server);
        if (release_err != 0 && err == 0) {
            err = release_err;
        }
        fixture->server = NULL;
    }
    if (fixture->registry_initialized) {
        registry_destroy(&fixture->registry);
        fixture->registry_initialized = false;
    }
    return err;
}

static void encode_request_id(uint8_t* body, size_t body_len, size_t id) {
    memset(body, 0x3c, body_len);
    uint64_t value = id;
    for (size_t i = 0; i < sizeof(value); i++) {
        body[i] = (uint8_t)(value >> (i * 8));
    }
}

static int open_client_stream(rpc_fixture* fixture,
    scenario_kind scenario,
    size_t id,
    uint8_t* request_body,
    size_t request_frame_size,
    size_t cumulative_body,
    size_t min_message_size,
    trevrpc_stream** out_stream) {
    encode_request_id(request_body, request_frame_size, id);
    trevrpc_call_options_v1 options;
    int err = trevrpc_call_options_v1_init(&options, sizeof(options));
    if (err != 0) {
        return err;
    }
    options.max_response_messages = (int64_t)(cumulative_body / min_message_size + 4);
    options.max_response_stream_body_size = (int64_t)cumulative_body;
    options.response_idle_timeout_nanos = PROFILE_RPC_IDLE_TIMEOUT_NANOS;
    trevrpc_request request = {
        .service = SERVICE_NAME,
        .service_len = sizeof(SERVICE_NAME) - 1,
        .method = METHOD_NAME,
        .method_len = sizeof(METHOD_NAME) - 1,
        .body = request_body,
        .body_len = request_frame_size,
        .kind = scenario_rpc_kind(scenario),
        .version = TREVRPC_WIRE_VERSION,
    };
    return trevrpc_raw_client_start_stream_request_v1(fixture->client, &request, &options, out_stream);
}

static int send_exact_messages(trevrpc_stream* stream,
    const uint8_t* payload,
    size_t message_size,
    size_t total,
    size_t* submitted,
    scenario_metrics* metrics) {
    while (*submitted < total) {
        size_t remaining = total - *submitted;
        size_t body_len = remaining < message_size ? remaining : message_size;
        int err = trevrpc_stream_send_message(stream, payload, body_len);
        if (err != 0) {
            metrics->send_failures++;
            return err;
        }
        *submitted += body_len;
        metrics->submitted_bytes += body_len;
    }
    return 0;
}

static int recv_messages_to(
    trevrpc_stream* stream, size_t target, size_t* received, scenario_metrics* metrics, bool server_side) {
    while (*received < target) {
        trevrpc_inbound_stream_frame* frame = NULL;
        int err = trevrpc_stream_recv_inbound(stream, &frame);
        uint32_t kind = 0;
        trevrpc_bytes_view body = {0};
        if (err != 0) {
            return err;
        }
        if (frame == NULL || trevrpc_inbound_stream_frame_get_kind(frame, &kind) != 0 ||
            trevrpc_inbound_stream_frame_get_body(frame, &body) != 0 || kind != TREVRPC_STREAM_FRAME_KIND_MESSAGE ||
            body.len > target - *received) {
            trevrpc_inbound_stream_frame_release(frame);
            return -EPROTO;
        }
        *received += body.len;
        metrics->received_bytes += body.len;
        metrics->message_frames++;
        trevrpc_inbound_stream_frame_release(frame);
    }
    (void)server_side;
    return 0;
}

static int recv_terminal_status(trevrpc_stream* stream, uint32_t expected_status, scenario_metrics* metrics) {
    for (;;) {
        trevrpc_inbound_stream_frame* frame = NULL;
        int err = trevrpc_stream_recv_inbound(stream, &frame);
        if (err != 0) {
            return err;
        }
        if (frame == NULL) {
            return -EPROTO;
        }
        uint32_t kind = 0;
        if (trevrpc_inbound_stream_frame_get_kind(frame, &kind) != 0) {
            trevrpc_inbound_stream_frame_release(frame);
            return -EPROTO;
        }
        if (kind == TREVRPC_STREAM_FRAME_KIND_MESSAGE) {
            trevrpc_bytes_view body = {0};
            if (trevrpc_inbound_stream_frame_get_body(frame, &body) != 0) {
                trevrpc_inbound_stream_frame_release(frame);
                return -EPROTO;
            }
            metrics->received_bytes += body.len;
            metrics->message_frames++;
            trevrpc_inbound_stream_frame_release(frame);
            continue;
        }
        uint32_t status = TREVRPC_STATUS_UNKNOWN;
        if (trevrpc_inbound_stream_frame_get_status(frame, &status) != 0) {
            trevrpc_inbound_stream_frame_release(frame);
            return -EPROTO;
        }
        trevrpc_inbound_stream_frame_release(frame);
        if (status != expected_status) {
            return -EPROTO;
        }
        if (status == TREVRPC_STATUS_OK) {
            metrics->ok_statuses++;
        } else if (status == TREVRPC_STATUS_RESOURCE_EXHAUSTED) {
            metrics->resource_exhausted_statuses++;
        }
        return 0;
    }
}

static int finish_call(call_registry* registry, size_t id, uint32_t status) {
    trevrpc_call* call = registry_take(registry, id);
    if (call == NULL) {
        return -ENOENT;
    }
    trevrpc_status_view_v1 status_view;
    int err = trevrpc_status_view_v1_init(&status_view, sizeof(status_view));
    if (err != 0) {
        trevrpc_call_close(call);
        return err;
    }
    status_view.status = status;
    return trevrpc_call_finish_stream_borrowed_v1(call, &status_view);
}

static int run_slow_reader(rpc_fixture* fixture,
    trevrpc_stream** client_streams,
    uint8_t* response_payload,
    size_t concurrency,
    size_t response_frame_size,
    size_t cumulative_body,
    size_t reader_progress,
    size_t hold_ms,
    size_t* load_rss_kib,
    scenario_metrics* metrics) {
    size_t* submitted = calloc(concurrency, sizeof(*submitted));
    size_t* received = calloc(concurrency, sizeof(*received));
    size_t* last_progress = calloc(concurrency, sizeof(*last_progress));
    if (submitted == NULL || received == NULL || last_progress == NULL) {
        free(last_progress);
        free(received);
        free(submitted);
        return -ENOMEM;
    }

    int result = 0;
    for (;;) {
        bool unfinished = false;
        for (size_t i = 0; i < concurrency; i++) {
            if (submitted[i] >= cumulative_body) {
                continue;
            }
            unfinished = true;
            trevrpc_call* call = registry_get(&fixture->registry, i);
            trevrpc_stream* server_stream = trevrpc_call_stream(call);
            size_t next = submitted[i] + response_frame_size;
            if (next > cumulative_body) {
                next = cumulative_body;
            }
            result =
                send_exact_messages(server_stream, response_payload, response_frame_size, next, &submitted[i], metrics);
            if (result != 0) {
                goto cleanup;
            }
            if (reader_progress > 0 && submitted[i] - last_progress[i] >= reader_progress) {
                result = recv_messages_to(client_streams[i], submitted[i], &received[i], metrics, false);
                if (result != 0) {
                    goto cleanup;
                }
                last_progress[i] = submitted[i];
            }
        }
        if (!unfinished) {
            break;
        }
    }

    *load_rss_kib = process_status_kib("VmRSS");
    sleep_millis(hold_ms);
    for (size_t i = 0; i < concurrency; i++) {
        result = finish_call(&fixture->registry, i, TREVRPC_STATUS_OK);
        if (result != 0) {
            goto cleanup;
        }
        result = recv_messages_to(client_streams[i], cumulative_body, &received[i], metrics, false);
        if (result != 0) {
            goto cleanup;
        }
        result = recv_terminal_status(client_streams[i], TREVRPC_STATUS_OK, metrics);
        if (result != 0 || submitted[i] != cumulative_body || received[i] != cumulative_body) {
            result = result != 0 ? result : -EPROTO;
            goto cleanup;
        }
    }

cleanup:
    free(last_progress);
    free(received);
    free(submitted);
    return result;
}

static int run_ingress_success(rpc_fixture* fixture,
    trevrpc_stream** client_streams,
    uint8_t* request_payload,
    size_t concurrency,
    size_t request_frame_size,
    size_t cumulative_body,
    size_t reader_progress,
    size_t hold_ms,
    bool sample_before_finish,
    size_t* load_rss_kib,
    scenario_metrics* metrics) {
    size_t* submitted = calloc(concurrency, sizeof(*submitted));
    size_t* received = calloc(concurrency, sizeof(*received));
    size_t* last_progress = calloc(concurrency, sizeof(*last_progress));
    if (submitted == NULL || received == NULL || last_progress == NULL) {
        free(last_progress);
        free(received);
        free(submitted);
        return -ENOMEM;
    }

    int result = 0;
    for (;;) {
        bool unfinished = false;
        for (size_t i = 0; i < concurrency; i++) {
            if (submitted[i] >= cumulative_body) {
                continue;
            }
            unfinished = true;
            size_t next = submitted[i] + request_frame_size;
            if (next > cumulative_body) {
                next = cumulative_body;
            }
            result = send_exact_messages(
                client_streams[i], request_payload, request_frame_size, next, &submitted[i], metrics);
            if (result != 0) {
                goto cleanup;
            }
            if (reader_progress > 0 && submitted[i] - last_progress[i] >= reader_progress) {
                trevrpc_call* call = registry_get(&fixture->registry, i);
                result = recv_messages_to(trevrpc_call_stream(call), submitted[i], &received[i], metrics, true);
                if (result != 0) {
                    goto cleanup;
                }
                last_progress[i] = submitted[i];
            }
        }
        if (!unfinished) {
            break;
        }
    }

    if (sample_before_finish) {
        *load_rss_kib = process_status_kib("VmRSS");
        sleep_millis(hold_ms);
    }
    for (size_t i = 0; i < concurrency; i++) {
        result = trevrpc_stream_finish_send(client_streams[i]);
        if (result != 0) {
            goto cleanup;
        }
    }
    if (!sample_before_finish) {
        *load_rss_kib = process_status_kib("VmRSS");
        sleep_millis(hold_ms);
    }
    for (size_t i = 0; i < concurrency; i++) {
        trevrpc_call* call = registry_get(&fixture->registry, i);
        result = recv_messages_to(trevrpc_call_stream(call), cumulative_body, &received[i], metrics, true);
        if (result != 0) {
            goto cleanup;
        }
        trevrpc_inbound_stream_frame* frame = NULL;
        result = trevrpc_stream_recv_inbound(trevrpc_call_stream(call), &frame);
        trevrpc_inbound_stream_frame_release(frame);
        if (result != 0 || frame != NULL) {
            result = result != 0 ? result : -EPROTO;
            goto cleanup;
        }
        metrics->close_eof_results++;
        result = finish_call(&fixture->registry, i, TREVRPC_STATUS_OK);
        if (result != 0) {
            goto cleanup;
        }
        result = recv_terminal_status(client_streams[i], TREVRPC_STATUS_OK, metrics);
        if (result != 0 || submitted[i] != cumulative_body || received[i] != cumulative_body) {
            result = result != 0 ? result : -EPROTO;
            goto cleanup;
        }
    }

cleanup:
    free(last_progress);
    free(received);
    free(submitted);
    return result;
}

static int run_reset(rpc_fixture* fixture,
    trevrpc_stream** client_streams,
    uint8_t* request_payload,
    size_t concurrency,
    size_t request_frame_size,
    size_t cumulative_body,
    size_t hold_ms,
    size_t* load_rss_kib,
    scenario_metrics* metrics) {
    size_t* submitted = calloc(concurrency, sizeof(*submitted));
    if (submitted == NULL) {
        return -ENOMEM;
    }
    int result = 0;
    for (size_t i = 0; i < concurrency; i++) {
        result = send_exact_messages(
            client_streams[i], request_payload, request_frame_size, cumulative_body, &submitted[i], metrics);
        if (result != 0 || submitted[i] != cumulative_body) {
            result = result != 0 ? result : -EPROTO;
            goto cleanup;
        }
    }
    *load_rss_kib = process_status_kib("VmRSS");
    sleep_millis(hold_ms);
    for (size_t i = 0; i < concurrency; i++) {
        trevrpc_stream_cancel(client_streams[i]);
    }
    for (size_t i = 0; i < concurrency; i++) {
        trevrpc_call* call = registry_get(&fixture->registry, i);
        trevrpc_inbound_stream_frame* frame = NULL;
        int recv_result = 0;
        do {
            trevrpc_inbound_stream_frame_release(frame);
            frame = NULL;
            recv_result = trevrpc_stream_recv_inbound(trevrpc_call_stream(call), &frame);
            if (frame != NULL) {
                uint32_t kind = 0;
                trevrpc_bytes_view body = {0};
                if (trevrpc_inbound_stream_frame_get_kind(frame, &kind) != 0 ||
                    (kind == TREVRPC_STREAM_FRAME_KIND_MESSAGE &&
                        trevrpc_inbound_stream_frame_get_body(frame, &body) != 0)) {
                    recv_result = -EPROTO;
                } else if (kind == TREVRPC_STREAM_FRAME_KIND_MESSAGE) {
                    metrics->received_bytes += body.len;
                    metrics->message_frames++;
                }
            }
        } while (recv_result == 0 && frame != NULL);
        trevrpc_inbound_stream_frame_release(frame);
        if (recv_result != TREV_MSQUIC_ERR_CLOSED) {
            metrics->observed_reset_result = recv_result;
            result = -EPROTO;
            goto cleanup;
        }
        metrics->observed_reset_result = recv_result;
        metrics->reset_closed_results++;
        call = registry_take(&fixture->registry, i);
        trevrpc_call_close(call);
    }

cleanup:
    free(submitted);
    return result;
}

static int run_body_limit(rpc_fixture* fixture,
    trevrpc_stream** client_streams,
    uint8_t* request_payload,
    size_t concurrency,
    size_t request_frame_size,
    size_t cumulative_body,
    size_t hold_ms,
    size_t* load_rss_kib,
    scenario_metrics* metrics) {
    size_t* submitted = calloc(concurrency, sizeof(*submitted));
    size_t* received = calloc(concurrency, sizeof(*received));
    if (submitted == NULL || received == NULL) {
        free(received);
        free(submitted);
        return -ENOMEM;
    }
    int result = 0;
    for (size_t i = 0; i < concurrency; i++) {
        result = send_exact_messages(
            client_streams[i], request_payload, request_frame_size, cumulative_body, &submitted[i], metrics);
        if (result == 0) {
            result = send_exact_messages(client_streams[i],
                request_payload,
                request_frame_size,
                cumulative_body + request_frame_size,
                &submitted[i],
                metrics);
        }
        if (result != 0 || trevrpc_stream_finish_send(client_streams[i]) != 0) {
            goto cleanup;
        }
    }
    *load_rss_kib = process_status_kib("VmRSS");
    sleep_millis(hold_ms);
    for (size_t i = 0; i < concurrency; i++) {
        trevrpc_call* call = registry_get(&fixture->registry, i);
        result = recv_messages_to(trevrpc_call_stream(call), cumulative_body, &received[i], metrics, true);
        if (result != 0) {
            goto cleanup;
        }
        trevrpc_inbound_stream_frame* frame = NULL;
        result = trevrpc_stream_recv_inbound(trevrpc_call_stream(call), &frame);
        trevrpc_inbound_stream_frame_release(frame);
        if (result != TREVRPC_ERR_STREAM_LIMIT_EXCEEDED || frame != NULL) {
            result = -EPROTO;
            goto cleanup;
        }
        metrics->body_limit_results++;
        result = finish_call(&fixture->registry, i, TREVRPC_STATUS_OK);
        if (result != 0) {
            goto cleanup;
        }
        result = recv_terminal_status(client_streams[i], TREVRPC_STATUS_RESOURCE_EXHAUSTED, metrics);
        if (result != 0 || received[i] != cumulative_body || submitted[i] != cumulative_body + request_frame_size) {
            result = result != 0 ? result : -EPROTO;
            goto cleanup;
        }
    }

cleanup:
    free(received);
    free(submitted);
    return result;
}

static int run_overload(rpc_fixture* fixture,
    trevrpc_stream** client_streams,
    uint8_t* request_body,
    size_t concurrency,
    size_t request_frame_size,
    size_t cumulative_body,
    size_t min_message_size,
    size_t hold_ms,
    size_t* setup_rss_kib,
    size_t* load_rss_kib,
    scenario_metrics* metrics) {
    size_t admitted = concurrency / 2;
    if (admitted == 0) {
        admitted = 1;
    }
    int result = 0;
    for (size_t i = 0; i < admitted; i++) {
        fprintf(stderr, "overload phase=admit id=%zu\n", i);
        result = open_client_stream(fixture,
            SCENARIO_OVERLOAD,
            i,
            request_body,
            request_frame_size,
            cumulative_body,
            min_message_size,
            &client_streams[i]);
        if (result != 0 || !registry_wait_count(&fixture->registry, i + 1, 5000)) {
            return result != 0 ? result : -ETIMEDOUT;
        }
    }
    metrics->admitted = admitted;
    metrics->initial_request_bytes = saturating_multiply(concurrency, request_frame_size);
    *setup_rss_kib = process_status_kib("VmRSS");
    for (size_t i = admitted; i < concurrency; i++) {
        fprintf(stderr, "overload phase=reject-open id=%zu\n", i);
        result = open_client_stream(fixture,
            SCENARIO_OVERLOAD,
            i,
            request_body,
            request_frame_size,
            cumulative_body,
            min_message_size,
            &client_streams[i]);
        if (result != 0) {
            return result;
        }
        fprintf(stderr, "overload phase=reject-recv id=%zu\n", i);
        result = recv_terminal_status(client_streams[i], TREVRPC_STATUS_RESOURCE_EXHAUSTED, metrics);
        if (result != 0) {
            return result;
        }
        metrics->rejected++;
    }
    for (size_t i = 0; i < admitted; i++) {
        result = trevrpc_stream_finish_send(client_streams[i]);
        if (result != 0) {
            return result;
        }
    }
    *load_rss_kib = process_status_kib("VmRSS");
    sleep_millis(hold_ms);
    if (!registry_wait_count(&fixture->registry, admitted, 1)) {
        return -EPROTO;
    }
    for (size_t i = 0; i < admitted; i++) {
        fprintf(stderr, "overload phase=finish id=%zu\n", i);
        result = finish_call(&fixture->registry, i, TREVRPC_STATUS_OK);
        if (result != 0) {
            return result;
        }
        fprintf(stderr, "overload phase=finish-recv id=%zu\n", i);
        result = recv_terminal_status(client_streams[i], TREVRPC_STATUS_OK, metrics);
        if (result != 0) {
            return result;
        }
    }
    return metrics->admitted == admitted && metrics->rejected == concurrency - admitted ? 0 : -EPROTO;
}

static size_t convergence_rss(rpc_fixture* fixture, size_t baseline_rss_kib, size_t convergence_bound_bytes) {
    (void)fixture;
    size_t limit_kib = saturating_add(baseline_rss_kib, convergence_bound_bytes / 1024);
    size_t final_rss_kib = process_status_kib("VmRSS");
    uint64_t started = monotonic_nanos();
    while (final_rss_kib > limit_kib && monotonic_nanos() - started < CONVERGENCE_WAIT_MS * 1000000ull) {
#if defined(__GLIBC__)
        (void)malloc_trim(0);
#endif
        sleep_millis(25);
        final_rss_kib = process_status_kib("VmRSS");
    }
    return final_rss_kib;
}

int main(int argc, char** argv) {
    if (argc != 8) {
        usage(argv[0]);
        return 2;
    }

    scenario_kind scenario;
    size_t concurrency = 0;
    size_t request_frame_size = 0;
    size_t response_frame_size = 0;
    size_t cumulative_body = 0;
    size_t reader_progress = 0;
    size_t hold_ms = 0;
    if (parse_scenario(argv[1], &scenario) != 0 || parse_size(argv[2], &concurrency) != 0 ||
        parse_size(argv[3], &request_frame_size) != 0 || parse_size(argv[4], &response_frame_size) != 0 ||
        parse_size(argv[5], &cumulative_body) != 0 || parse_size(argv[6], &reader_progress) != 0 ||
        parse_size(argv[7], &hold_ms) != 0 || concurrency == 0 || concurrency > PROFILE_MAX_CONCURRENCY ||
        concurrency > UINT16_MAX || request_frame_size < sizeof(uint64_t) || response_frame_size == 0 ||
        request_frame_size > PROFILE_MAX_FRAME_SIZE - 1024 || response_frame_size > PROFILE_MAX_FRAME_SIZE - 1024 ||
        cumulative_body == 0 || cumulative_body > PROFILE_MAX_CUMULATIVE_BODY) {
        usage(argv[0]);
        return 2;
    }

    uint64_t started_at = monotonic_nanos();
    size_t baseline_rss_kib = process_status_kib("VmRSS");
    size_t setup_rss_kib = 0;
    size_t load_rss_kib = 0;
    size_t vmhwm_rss_kib = 0;
    size_t peak_rss_kib = 0;
    size_t final_rss_kib = 0;
    scenario_metrics metrics = {.observed_reset_result = 0};
    rpc_fixture fixture = {0};
    trevrpc_stream** client_streams = calloc(concurrency, sizeof(*client_streams));
    uint8_t* request_body = malloc(request_frame_size);
    uint8_t* request_payload = malloc(request_frame_size);
    uint8_t* response_payload = malloc(response_frame_size);
    size_t min_message_size = request_frame_size < response_frame_size ? request_frame_size : response_frame_size;
    int scenario_result = 0;

    if (baseline_rss_kib == 0 || client_streams == NULL || request_body == NULL || request_payload == NULL ||
        response_payload == NULL) {
        scenario_result = -ENOMEM;
        goto cleanup;
    }
    memset(request_payload, 0xa5, request_frame_size);
    memset(response_payload, 0x5a, response_frame_size);

    scenario_result = fixture_start(&fixture, scenario, concurrency, cumulative_body, min_message_size);
    if (scenario_result != 0) {
        goto cleanup;
    }

    if (scenario == SCENARIO_OVERLOAD) {
        scenario_result = run_overload(&fixture,
            client_streams,
            request_body,
            concurrency,
            request_frame_size,
            cumulative_body,
            min_message_size,
            hold_ms,
            &setup_rss_kib,
            &load_rss_kib,
            &metrics);
        goto cleanup;
    }

    for (size_t i = 0; i < concurrency; i++) {
        scenario_result = open_client_stream(&fixture,
            scenario,
            i,
            request_body,
            request_frame_size,
            cumulative_body,
            min_message_size,
            &client_streams[i]);
        if (scenario_result != 0) {
            goto cleanup;
        }
    }
    if (!registry_wait_count(&fixture.registry, concurrency, 5000)) {
        scenario_result = -ETIMEDOUT;
        goto cleanup;
    }
    metrics.admitted = concurrency;
    metrics.initial_request_bytes = saturating_multiply(concurrency, request_frame_size);
    setup_rss_kib = process_status_kib("VmRSS");

    switch (scenario) {
    case SCENARIO_SLOW_READER:
        scenario_result = run_slow_reader(&fixture,
            client_streams,
            response_payload,
            concurrency,
            response_frame_size,
            cumulative_body,
            reader_progress,
            hold_ms,
            &load_rss_kib,
            &metrics);
        break;
    case SCENARIO_STALLED_HANDLER:
        scenario_result = run_ingress_success(&fixture,
            client_streams,
            request_payload,
            concurrency,
            request_frame_size,
            cumulative_body,
            reader_progress,
            hold_ms,
            true,
            &load_rss_kib,
            &metrics);
        break;
    case SCENARIO_CLOSE:
        scenario_result = run_ingress_success(&fixture,
            client_streams,
            request_payload,
            concurrency,
            request_frame_size,
            cumulative_body,
            reader_progress,
            hold_ms,
            false,
            &load_rss_kib,
            &metrics);
        break;
    case SCENARIO_RESET:
        scenario_result = run_reset(&fixture,
            client_streams,
            request_payload,
            concurrency,
            request_frame_size,
            cumulative_body,
            hold_ms,
            &load_rss_kib,
            &metrics);
        break;
    case SCENARIO_BODY_LIMIT:
        scenario_result = run_body_limit(&fixture,
            client_streams,
            request_payload,
            concurrency,
            request_frame_size,
            cumulative_body,
            hold_ms,
            &load_rss_kib,
            &metrics);
        break;
    case SCENARIO_OVERLOAD:
        break;
    }

cleanup:
    if (scenario_result != 0) {
        metrics.invariant_failures++;
        fprintf(stderr, "scenario failed: %s (%d)\n", trevrpc_error(scenario_result), scenario_result);
    }
    for (size_t i = 0; i < concurrency; i++) {
        if (client_streams != NULL && client_streams[i] != NULL) {
            trevrpc_stream_cancel(client_streams[i]);
            trevrpc_stream_close(client_streams[i]);
            client_streams[i] = NULL;
        }
    }
    int stop_result = fixture_stop(&fixture);
    if (stop_result != 0) {
        metrics.invariant_failures++;
        if (scenario_result == 0) {
            scenario_result = stop_result;
        }
    }
    vmhwm_rss_kib = process_status_kib("VmHWM");

    size_t per_stream_peak = saturating_add(saturating_multiply(cumulative_body, 2),
        saturating_multiply(saturating_add(request_frame_size, response_frame_size), 4));
    size_t peak_bound_bytes = saturating_add(
        PEAK_FIXED_ALLOWANCE_BYTES, saturating_multiply((size_t)SafeProfile.bound_model_conn_flow_control_window, 2));
    peak_bound_bytes = saturating_add(peak_bound_bytes, saturating_multiply(concurrency, per_stream_peak));
    size_t convergence_bound_bytes = saturating_add(CONVERGENCE_FIXED_ALLOWANCE_BYTES,
        saturating_multiply(
            concurrency, saturating_multiply(saturating_add(request_frame_size, response_frame_size), 8)));
    final_rss_kib = convergence_rss(&fixture, baseline_rss_kib, convergence_bound_bytes);
    peak_rss_kib = vmhwm_rss_kib;
    if (baseline_rss_kib > peak_rss_kib) {
        peak_rss_kib = baseline_rss_kib;
    }
    if (setup_rss_kib > peak_rss_kib) {
        peak_rss_kib = setup_rss_kib;
    }
    if (load_rss_kib > peak_rss_kib) {
        peak_rss_kib = load_rss_kib;
    }
    if (final_rss_kib > peak_rss_kib) {
        peak_rss_kib = final_rss_kib;
    }
    size_t peak_delta_kib = peak_rss_kib > baseline_rss_kib ? peak_rss_kib - baseline_rss_kib : 0;
    size_t final_delta_kib = final_rss_kib > baseline_rss_kib ? final_rss_kib - baseline_rss_kib : 0;
    bool within_peak_bound = peak_delta_kib <= peak_bound_bytes / 1024;
    bool converged = final_delta_kib <= convergence_bound_bytes / 1024 && final_rss_kib <= peak_rss_kib;
    if (!within_peak_bound || !converged || metrics.send_failures != 0) {
        metrics.invariant_failures++;
    }
    bool passed = scenario_result == 0 && metrics.invariant_failures == 0;

    uint64_t elapsed_nanos = monotonic_nanos() - started_at;
    printf("profile,scenario,expected_outcome,concurrency,request_frame_size,response_frame_size,cumulative_body,"
           "reader_progress,hold_ms,configured_stream_recv_window,configured_conn_flow_control_window,"
           "bound_model_stream_recv_window,bound_model_conn_flow_control_window,receive_window_basis,"
           "send_buffering,execution_profile,"
           "max_frame_size,max_stream_body_size,max_concurrent_streams,max_concurrent_requests,idle_timeout_ms,"
           "rpc_idle_timeout_nanos,baseline_rss_kib,setup_rss_kib,load_rss_kib,vmhwm_rss_kib,peak_rss_kib,"
           "final_rss_kib,"
           "peak_delta_kib,final_delta_kib,peak_bound_bytes,convergence_bound_bytes,within_peak_bound,converged,"
           "admitted,rejected,initial_request_bytes,submitted_bytes,received_bytes,message_frames,ok_statuses,"
           "resource_exhausted_statuses,"
           "reset_closed_results,close_eof_results,body_limit_results,expected_reset_result,observed_reset_result,"
           "send_failures,invariant_failures,elapsed_ms,passed\n");
    printf("%s,%s,%s,%zu,%zu,%zu,%zu,%zu,%zu,%u,%u,%u,%u,%s,%d,%u,%u,%zu,%zu,%zu,%u,%llu,%zu,%zu,%zu,%zu,"
           "%zu,%zu,"
           "%zu,%zu,%zu,%zu,%d,%d,%zu,%zu,%zu,%zu,%zu,%zu,%zu,%zu,%zu,%zu,%zu,%d,%d,%zu,%zu,%.3f,%d\n",
        SafeProfile.name,
        argv[1],
        scenario_expected_outcome(scenario),
        concurrency,
        request_frame_size,
        response_frame_size,
        cumulative_body,
        reader_progress,
        hold_ms,
        SafeProfile.configured_stream_recv_window,
        SafeProfile.configured_conn_flow_control_window,
        SafeProfile.bound_model_stream_recv_window,
        SafeProfile.bound_model_conn_flow_control_window,
        SafeProfile.receive_window_basis,
        SafeProfile.send_buffering_enabled,
        (unsigned int)SafeProfile.execution_profile,
        PROFILE_MAX_FRAME_SIZE,
        cumulative_body,
        concurrency,
        scenario == SCENARIO_OVERLOAD ? (concurrency / 2 == 0 ? 1 : concurrency / 2) : concurrency,
        PROFILE_IDLE_TIMEOUT_MS,
        (unsigned long long)PROFILE_RPC_IDLE_TIMEOUT_NANOS,
        baseline_rss_kib,
        setup_rss_kib,
        load_rss_kib,
        vmhwm_rss_kib,
        peak_rss_kib,
        final_rss_kib,
        peak_delta_kib,
        final_delta_kib,
        peak_bound_bytes,
        convergence_bound_bytes,
        within_peak_bound ? 1 : 0,
        converged ? 1 : 0,
        metrics.admitted,
        metrics.rejected,
        metrics.initial_request_bytes,
        metrics.submitted_bytes,
        metrics.received_bytes,
        metrics.message_frames,
        metrics.ok_statuses,
        metrics.resource_exhausted_statuses,
        metrics.reset_closed_results,
        metrics.close_eof_results,
        metrics.body_limit_results,
        TREV_MSQUIC_ERR_CLOSED,
        metrics.observed_reset_result,
        metrics.send_failures,
        metrics.invariant_failures,
        (double)elapsed_nanos / 1000000.0,
        passed ? 1 : 0);

    free(response_payload);
    free(request_payload);
    free(request_body);
    free(client_streams);
    return passed ? 0 : 1;
}
