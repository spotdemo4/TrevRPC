#define _POSIX_C_SOURCE 200809L

#include "benchmark.pb-c.h"
#include "benchmark.trevrpc.h"
#include "trevrpc_bench_peer.h"
#include "trevrpc_msquic.h"
#include "trevrpc_raw.h"

#include <errno.h> // IWYU pragma: keep
#include <inttypes.h>
#include <limits.h>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <stdbool.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#define BENCHMARK_SCHEMA_VERSION 4
#define BENCHMARK_WEBTRANSPORT_PATH "/trevrpc"

typedef Trevrpc__Benchmark__V1__BenchmarkRequest BenchmarkRequest;
typedef Trevrpc__Benchmark__V1__BenchmarkResponse BenchmarkResponse;
typedef Trevrpc__Benchmark__V1__BenchmarkSummary BenchmarkSummary;
typedef Trevrpc__Benchmark__V1__StreamRequest StreamRequest;
typedef trevrpc_benchmark_v1_benchmark_service_server BenchmarkService;
typedef trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_receiver BenchmarkRequestReceiver;
typedef trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_event BenchmarkRequestEvent;
typedef trevrpc_benchmark_v1_benchmark_service_benchmark_response_receiver BenchmarkResponseReceiver;
typedef trevrpc_benchmark_v1_benchmark_service_benchmark_response_event BenchmarkResponseEvent;
typedef trevrpc_benchmark_v1_benchmark_service_benchmark_summary_receiver BenchmarkSummaryReceiver;
typedef trevrpc_benchmark_v1_benchmark_service_benchmark_summary_event BenchmarkSummaryEvent;

typedef struct benchmark_client {
    benchmark_stack stack;
    union {
        trevrpc_raw_client* native;
        trevrpc_bench_grpc_client* grpc;
    } impl;
} benchmark_client;

typedef struct histogram_bucket {
    uint64_t upper_bound_ns;
    uint64_t count;
} histogram_bucket;

typedef struct histogram {
    histogram_bucket* buckets;
    size_t count;
    size_t capacity;
} histogram;

typedef struct operation_counts {
    uint64_t request_messages;
    uint64_t response_messages;
} operation_counts;

typedef struct lane_result {
    uint64_t completed;
    uint64_t failed;
    uint64_t request_messages;
    uint64_t response_messages;
    int internal_error;
    histogram latency;
} lane_result;

typedef struct phase_control phase_control;

typedef struct lane_args {
    phase_control* phase;
    size_t lane_index;
    trevrpc_bench_grpc_lane* grpc_lane;
    lane_result result;
} lane_args;

struct phase_control {
    benchmark_client* client;
    const client_options* options;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool started;
    size_t ready_count;
    uint64_t start_ns;
    uint64_t deadline_ns;
    bool record_latency;
    pthread_t* threads;
    lane_args* lanes;
    size_t thread_count;
};

typedef struct server_thread_args {
    trevrpc_server* server;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool ready;
    bool done;
    int result;
} server_thread_args;

typedef struct bidi_sender_args {
    trevrpc_stream* stream;
    const client_options* options;
    int result;
} bidi_sender_args;

static volatile sig_atomic_t server_stop_requested;

static uint64_t monotonic_nanos(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        return 0;
    }
    return (uint64_t)now.tv_sec * 1000000000ull + (uint64_t)now.tv_nsec;
}

static uint64_t saturating_add_u64(uint64_t left, uint64_t right) {
    return UINT64_MAX - left < right ? UINT64_MAX : left + right;
}

static int checked_add_u64(uint64_t* value, uint64_t increment) {
    if (UINT64_MAX - *value < increment) {
        return -EOVERFLOW;
    }
    *value += increment;
    return 0;
}

static void sleep_until(uint64_t deadline_ns) {
    for (;;) {
        uint64_t now = monotonic_nanos();
        if (now == 0 || now >= deadline_ns) {
            return;
        }
        uint64_t remaining = deadline_ns - now;
        struct timespec delay = {
            .tv_sec = (time_t)(remaining / 1000000000ull),
            .tv_nsec = (long)(remaining % 1000000000ull),
        };
        while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
        }
    }
}

static int write_json_string(FILE* output, const char* value) {
    if (value == NULL) {
        return -EINVAL;
    }
    if (fputc('"', output) == EOF) {
        return -EIO;
    }
    const unsigned char* cursor = (const unsigned char*)value;
    for (; *cursor != '\0'; cursor++) {
        switch (*cursor) {
        case '"':
            if (fputs("\\\"", output) == EOF) {
                return -EIO;
            }
            break;
        case '\\':
            if (fputs("\\\\", output) == EOF) {
                return -EIO;
            }
            break;
        case '\b':
            if (fputs("\\b", output) == EOF) {
                return -EIO;
            }
            break;
        case '\f':
            if (fputs("\\f", output) == EOF) {
                return -EIO;
            }
            break;
        case '\n':
            if (fputs("\\n", output) == EOF) {
                return -EIO;
            }
            break;
        case '\r':
            if (fputs("\\r", output) == EOF) {
                return -EIO;
            }
            break;
        case '\t':
            if (fputs("\\t", output) == EOF) {
                return -EIO;
            }
            break;
        default:
            if (*cursor < 0x20) {
                if (fprintf(output, "\\u%04x", *cursor) < 0) {
                    return -EIO;
                }
            } else if (fputc(*cursor, output) == EOF) {
                return -EIO;
            }
            break;
        }
    }
    return fputc('"', output) == EOF ? -EIO : 0;
}

static int flush_event(void) {
    if (fputc('\n', stdout) == EOF || fflush(stdout) != 0) {
        fprintf(stderr, "write protocol event: %s\n", strerror(errno));
        return -EIO;
    }
    return 0;
}

static int emit_capabilities(void) {
    if (fprintf(stdout,
            "{\"schema_version\":%d,\"event\":\"capabilities\",\"peer\":\"c\","
            "\"roles\":{\"client\":[\"trevrpc_native_quic\",\"grpc_http2\"],"
            "\"server\":[\"trevrpc_native_quic\",\"grpc_http2\",\"trevrpc_webtransport\"]},"
            "\"rpc_kinds\":[\"unary\",\"client_stream\",\"server_stream\",\"bidi\"],"
            "\"histogram\":\"log_linear_v1\"}",
            BENCHMARK_SCHEMA_VERSION) < 0) {
        return -EIO;
    }
    return flush_event();
}

static int emit_error(const char* phase, const char* code, const char* message) {
    fprintf(stderr, "%s: %s: %s\n", phase, code, message);
    if (fprintf(
            stdout, "{\"schema_version\":%d,\"event\":\"error\",\"peer\":\"c\",\"phase\":", BENCHMARK_SCHEMA_VERSION) <
            0 ||
        write_json_string(stdout, phase) != 0 || fputs(",\"code\":", stdout) == EOF ||
        write_json_string(stdout, code) != 0 || fputs(",\"message\":", stdout) == EOF ||
        write_json_string(stdout, message) != 0 || fputc('}', stdout) == EOF) {
        return -EIO;
    }
    return flush_event();
}

static int fail_with_error(const char* phase, const char* code, const char* format, ...) {
    char message[512];
    va_list args;
    va_start(args, format);
    (void)vsnprintf(message, sizeof(message), format, args);
    va_end(args);
    (void)emit_error(phase, code, message);
    return 1;
}

static int emit_ready(const char* host, uint16_t port, const char* stack) {
    if (fprintf(stdout,
            "{\"schema_version\":%d,\"event\":\"ready\",\"peer\":\"c\",\"address\":",
            BENCHMARK_SCHEMA_VERSION) < 0) {
        return -EIO;
    }
    char address[512];
    int length = strchr(host, ':') == NULL ? snprintf(address, sizeof(address), "%s:%u", host, port)
                                           : snprintf(address, sizeof(address), "[%s]:%u", host, port);
    if (length < 0 || (size_t)length >= sizeof(address) || write_json_string(stdout, address) != 0 ||
        fputs(",\"stack\":", stdout) == EOF || write_json_string(stdout, stack) != 0 ||
        fprintf(stdout, ",\"pid\":%ld}", (long)getpid()) < 0) {
        return -EIO;
    }
    return flush_event();
}

static int emit_armed(const char* stack) {
    if (fprintf(
            stdout, "{\"schema_version\":%d,\"event\":\"armed\",\"peer\":\"c\",\"stack\":", BENCHMARK_SCHEMA_VERSION) <
            0 ||
        write_json_string(stdout, stack) != 0 || fprintf(stdout, ",\"pid\":%ld}", (long)getpid()) < 0) {
        return -EIO;
    }
    return flush_event();
}

static int emit_stopped(const char* stack) {
    if (fprintf(stdout,
            "{\"schema_version\":%d,\"event\":\"stopped\",\"peer\":\"c\",\"stack\":",
            BENCHMARK_SCHEMA_VERSION) < 0 ||
        write_json_string(stdout, stack) != 0 || fputc('}', stdout) == EOF) {
        return -EIO;
    }
    return flush_event();
}

static int parse_u64(const char* value, uint64_t minimum, uint64_t maximum, uint64_t* result) {
    if (value == NULL || value[0] == '\0' || value[0] == '-') {
        return -EINVAL;
    }
    errno = 0;
    char* end = NULL;
    unsigned long long parsed = strtoull(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed < minimum || parsed > maximum) {
        return -EINVAL;
    }
    *result = (uint64_t)parsed;
    return 0;
}

static int split_address(const char* value, bool allow_zero_port, char** host, uint16_t* port) {
    *host = NULL;
    *port = 0;
    if (value == NULL || value[0] == '\0') {
        return -EINVAL;
    }

    const char* host_start = value;
    const char* host_end = NULL;
    const char* port_start = NULL;
    if (value[0] == '[') {
        host_start++;
        host_end = strchr(host_start, ']');
        if (host_end == NULL || host_end == host_start || host_end[1] != ':' || host_end[2] == '\0') {
            return -EINVAL;
        }
        port_start = host_end + 2;
    } else {
        const char* separator = strrchr(value, ':');
        if (separator == NULL || separator == value || separator[1] == '\0' || strchr(value, ':') != separator) {
            return -EINVAL;
        }
        host_end = separator;
        port_start = separator + 1;
    }

    uint64_t parsed_port = 0;
    if (parse_u64(port_start, allow_zero_port ? 0 : 1, UINT16_MAX, &parsed_port) != 0) {
        return -EINVAL;
    }
    size_t host_len = (size_t)(host_end - host_start);
    char* copy = malloc(host_len + 1);
    if (copy == NULL) {
        return -ENOMEM;
    }
    memcpy(copy, host_start, host_len);
    copy[host_len] = '\0';
    *host = copy;
    *port = (uint16_t)parsed_port;
    return 0;
}

static int set_once(const char** destination, const char* value) {
    if (*destination != NULL) {
        return -EINVAL;
    }
    *destination = value;
    return 0;
}

static int parse_stack(const char* value, benchmark_stack* stack, const char** stack_name) {
    if (strcmp(value, "trevrpc_native_quic") == 0) {
        *stack = BENCHMARK_STACK_TREVRPC_NATIVE_QUIC;
    } else if (strcmp(value, "grpc_http2") == 0) {
        *stack = BENCHMARK_STACK_GRPC_HTTP2;
    } else if (strcmp(value, "trevrpc_webtransport") == 0) {
        *stack = BENCHMARK_STACK_TREVRPC_WEBTRANSPORT;
    } else {
        return -EINVAL;
    }
    *stack_name = value;
    return 0;
}

static int parse_server_options(int argc, char** argv, server_options* options, char* error, size_t error_len) {
    memset(options, 0, sizeof(*options));
    const char* listen = NULL;
    const char* stack = NULL;
    const char* workers = NULL;
    for (int i = 2; i < argc; i += 2) {
        if (i + 1 >= argc) {
            snprintf(error, error_len, "missing value for %s", argv[i]);
            return -EINVAL;
        }
        int err = 0;
        if (strcmp(argv[i], "--listen") == 0) {
            err = set_once(&listen, argv[i + 1]);
        } else if (strcmp(argv[i], "--stack") == 0) {
            err = set_once(&stack, argv[i + 1]);
        } else if (strcmp(argv[i], "--cert") == 0) {
            err = set_once(&options->cert, argv[i + 1]);
        } else if (strcmp(argv[i], "--key") == 0) {
            err = set_once(&options->key, argv[i + 1]);
        } else if (strcmp(argv[i], "--webtransport-origin") == 0) {
            err = set_once(&options->webtransport_origin, argv[i + 1]);
        } else if (strcmp(argv[i], "--workers") == 0) {
            err = set_once(&workers, argv[i + 1]);
        } else {
            snprintf(error, error_len, "unknown server option: %s", argv[i]);
            return -EINVAL;
        }
        if (err != 0) {
            snprintf(error, error_len, "duplicate server option: %s", argv[i]);
            return err;
        }
    }
    if (listen == NULL || stack == NULL || options->cert == NULL || options->key == NULL) {
        snprintf(error, error_len, "server requires --stack, --listen, --cert, and --key");
        return -EINVAL;
    }
    if (parse_stack(stack, &options->stack, &options->stack_name) != 0) {
        snprintf(error, error_len, "invalid --stack value: %s", stack);
        return -EINVAL;
    }
    options->workers = BENCHMARK_SERVER_WORKERS;
    const char* configured_workers = workers != NULL ? workers : getenv("TREVRPC_BENCH_SERVER_WORKERS");
    if (configured_workers != NULL) {
        uint64_t parsed = 0;
        if (parse_u64(configured_workers, 1, BENCHMARK_SERVER_WORKERS, &parsed) != 0) {
            snprintf(error, error_len, "invalid worker count: %s", configured_workers);
            return -EINVAL;
        }
        options->workers = (size_t)parsed;
    }
    if (options->stack == BENCHMARK_STACK_TREVRPC_WEBTRANSPORT) {
        if (options->webtransport_origin == NULL || options->webtransport_origin[0] == '\0') {
            snprintf(error, error_len, "trevrpc_webtransport server requires --webtransport-origin");
            return -EINVAL;
        }
    } else if (options->webtransport_origin != NULL) {
        snprintf(error, error_len, "--webtransport-origin is only valid for a trevrpc_webtransport server");
        return -EINVAL;
    }
    int err = split_address(listen, true, &options->host, &options->port);
    if (err != 0) {
        snprintf(error, error_len, "invalid --listen address: %s", listen);
    }
    return err;
}

static int parse_rpc_kind(const char* value, client_options* options) {
    if (strcmp(value, "unary") == 0) {
        options->rpc_kind = BENCHMARK_RPC_UNARY;
    } else if (strcmp(value, "client_stream") == 0) {
        options->rpc_kind = BENCHMARK_RPC_CLIENT_STREAM;
    } else if (strcmp(value, "server_stream") == 0) {
        options->rpc_kind = BENCHMARK_RPC_SERVER_STREAM;
    } else if (strcmp(value, "bidi") == 0) {
        options->rpc_kind = BENCHMARK_RPC_BIDI;
    } else {
        return -EINVAL;
    }
    options->rpc_name = value;
    return 0;
}

static int parse_client_options(int argc, char** argv, client_options* options, char* error, size_t error_len) {
    memset(options, 0, sizeof(*options));
    const char* address = NULL;
    const char* stack = NULL;
    const char* rpc = NULL;
    const char* concurrency = NULL;
    const char* warmup_ms = NULL;
    const char* measurement_ms = NULL;
    const char* request_bytes = NULL;
    const char* response_bytes = NULL;
    const char* messages_per_stream = NULL;
    for (int i = 2; i < argc; i += 2) {
        if (i + 1 >= argc) {
            snprintf(error, error_len, "missing value for %s", argv[i]);
            return -EINVAL;
        }
        int err = 0;
        if (strcmp(argv[i], "--address") == 0) {
            err = set_once(&address, argv[i + 1]);
        } else if (strcmp(argv[i], "--stack") == 0) {
            err = set_once(&stack, argv[i + 1]);
        } else if (strcmp(argv[i], "--cert") == 0) {
            err = set_once(&options->cert, argv[i + 1]);
        } else if (strcmp(argv[i], "--rpc") == 0) {
            err = set_once(&rpc, argv[i + 1]);
        } else if (strcmp(argv[i], "--concurrency") == 0) {
            err = set_once(&concurrency, argv[i + 1]);
        } else if (strcmp(argv[i], "--warmup-ms") == 0) {
            err = set_once(&warmup_ms, argv[i + 1]);
        } else if (strcmp(argv[i], "--measurement-ms") == 0) {
            err = set_once(&measurement_ms, argv[i + 1]);
        } else if (strcmp(argv[i], "--request-bytes") == 0) {
            err = set_once(&request_bytes, argv[i + 1]);
        } else if (strcmp(argv[i], "--response-bytes") == 0) {
            err = set_once(&response_bytes, argv[i + 1]);
        } else if (strcmp(argv[i], "--messages-per-stream") == 0) {
            err = set_once(&messages_per_stream, argv[i + 1]);
        } else {
            snprintf(error, error_len, "unknown client option: %s", argv[i]);
            return -EINVAL;
        }
        if (err != 0) {
            snprintf(error, error_len, "duplicate client option: %s", argv[i]);
            return err;
        }
    }
    if (address == NULL || stack == NULL || options->cert == NULL || rpc == NULL || concurrency == NULL ||
        warmup_ms == NULL || measurement_ms == NULL || request_bytes == NULL || response_bytes == NULL ||
        messages_per_stream == NULL) {
        snprintf(error, error_len, "client requires all peer protocol options");
        return -EINVAL;
    }
    if (parse_stack(stack, &options->stack, &options->stack_name) != 0) {
        snprintf(error, error_len, "invalid --stack value: %s", stack);
        return -EINVAL;
    }
    if (options->stack == BENCHMARK_STACK_TREVRPC_WEBTRANSPORT) {
        snprintf(error, error_len, "trevrpc_webtransport is server-only");
        return -EINVAL;
    }
    if (parse_rpc_kind(rpc, options) != 0) {
        snprintf(error, error_len, "invalid --rpc value: %s", rpc);
        return -EINVAL;
    }
    int err = split_address(address, false, &options->host, &options->port);
    if (err != 0) {
        snprintf(error, error_len, "invalid --address: %s", address);
        return err;
    }

    uint64_t parsed = 0;
    if (parse_u64(concurrency, 1, BENCHMARK_MAX_CONCURRENCY, &parsed) != 0) {
        snprintf(error, error_len, "invalid --concurrency: %s", concurrency);
        return -EINVAL;
    }
    options->concurrency = (size_t)parsed;
    if (parse_u64(warmup_ms, 0, UINT64_MAX / 1000000ull, &parsed) != 0) {
        snprintf(error, error_len, "invalid --warmup-ms: %s", warmup_ms);
        return -EINVAL;
    }
    options->warmup_ns = parsed * 1000000ull;
    if (parse_u64(measurement_ms, 1, UINT64_MAX / 1000000ull, &parsed) != 0) {
        snprintf(error, error_len, "invalid --measurement-ms: %s", measurement_ms);
        return -EINVAL;
    }
    options->measurement_ns = parsed * 1000000ull;
    if (parse_u64(request_bytes, 0, BENCHMARK_MAX_PAYLOAD_BYTES, &parsed) != 0) {
        snprintf(error, error_len, "invalid --request-bytes: %s", request_bytes);
        return -EINVAL;
    }
    options->request_bytes = (uint32_t)parsed;
    if (parse_u64(response_bytes, 0, BENCHMARK_MAX_PAYLOAD_BYTES, &parsed) != 0) {
        snprintf(error, error_len, "invalid --response-bytes: %s", response_bytes);
        return -EINVAL;
    }
    options->response_bytes = (uint32_t)parsed;
    if (parse_u64(messages_per_stream, 1, BENCHMARK_MAX_MESSAGES_PER_STREAM, &parsed) != 0) {
        snprintf(error, error_len, "invalid --messages-per-stream: %s", messages_per_stream);
        return -EINVAL;
    }
    options->messages_per_stream = (uint32_t)parsed;
    return 0;
}

static uint8_t* new_payload(size_t length, uint8_t value) {
    if (length == 0) {
        return NULL;
    }
    uint8_t* payload = malloc(length);
    if (payload != NULL) {
        memset(payload, value, length);
    }
    return payload;
}

static BenchmarkResponse* new_response(uint64_t sequence, uint32_t payload_len) {
    BenchmarkResponse* response = malloc(sizeof(*response));
    if (response == NULL) {
        return NULL;
    }
    trevrpc__benchmark__v1__benchmark_response__init(response);
    response->sequence = sequence;
    response->payload.data = new_payload(payload_len, 0);
    if (payload_len > 0 && response->payload.data == NULL) {
        free(response);
        return NULL;
    }
    response->payload.len = payload_len;
    return response;
}

static BenchmarkSummary* new_summary(uint64_t message_count, uint64_t payload_bytes) {
    BenchmarkSummary* summary = malloc(sizeof(*summary));
    if (summary == NULL) {
        return NULL;
    }
    trevrpc__benchmark__v1__benchmark_summary__init(summary);
    summary->message_count = message_count;
    summary->payload_bytes = payload_bytes;
    return summary;
}

static int service_unary(void* user_data,
    const trevrpc_call_context* context,
    const BenchmarkRequest* request,
    trevrpc_benchmark_v1_benchmark_service_unary_respond_fn respond,
    void* respond_context) {
    (void)user_data;
    if (request == NULL || respond == NULL || request->payload.len > BENCHMARK_MAX_PAYLOAD_BYTES ||
        request->response_bytes > BENCHMARK_MAX_PAYLOAD_BYTES || trevrpc_call_context_cancelled(context)) {
        return -EINVAL;
    }
    BenchmarkResponse* message = new_response(request->sequence, request->response_bytes);
    if (message == NULL) {
        return -ENOMEM;
    }
    trevrpc_benchmark_v1_benchmark_service_unary_response_view response = {
        .message = message,
        .status = TREVRPC_STATUS_OK,
    };
    int err = respond(respond_context, &response);
    trevrpc__benchmark__v1__benchmark_response__free_unpacked(message, NULL);
    return err;
}

static int send_benchmark_response(trevrpc_stream* stream, const BenchmarkResponse* response) {
    return trevrpc_benchmark_v1_benchmark_service_send_trevrpc_benchmark_v1_benchmark_response(stream, response);
}

static int service_client_stream(void* user_data,
    const trevrpc_call_context* context,
    trevrpc_stream* stream,
    trevrpc_benchmark_v1_benchmark_service_client_stream_respond_fn respond,
    void* respond_context) {
    (void)user_data;
    uint64_t count = 0;
    uint64_t payload_bytes = 0;
    BenchmarkRequestReceiver receiver = TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_REQUEST_REQUEST_RECEIVER_INIT;
    int result = trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_receiver_init(&receiver, stream);
    if (result != 0) {
        return result;
    }
    for (;;) {
        if (trevrpc_call_context_cancelled(context)) {
            result = -ECANCELED;
            break;
        }
        BenchmarkRequestEvent event = TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_REQUEST_REQUEST_EVENT_INIT;
        result = trevrpc_benchmark_v1_benchmark_service_recv_trevrpc_benchmark_v1_benchmark_request_request(
            &receiver, &event);
        if (result != 0) {
            trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_event_reset(&event);
            break;
        }
        if (event.kind == TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_REQUEST_REQUEST_EVENT_END) {
            trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_event_reset(&event);
            break;
        }
        if (event.kind == TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_REQUEST_REQUEST_EVENT_TERMINAL_STATUS) {
            uint32_t status = TREVRPC_STATUS_UNKNOWN;
            result = trevrpc_inbound_stream_frame_get_status(event.frame, &status);
            if (result == 0 && status != TREVRPC_STATUS_OK) {
                result = -EINVAL;
            }
            trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_event_reset(&event);
            break;
        }
        if (event.kind != TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_REQUEST_REQUEST_EVENT_MESSAGE) {
            result = event.error != 0 ? event.error : TREVRPC_ERR_INVALID_FRAME;
            trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_event_reset(&event);
            break;
        }
        if (event.message->payload.len > BENCHMARK_MAX_PAYLOAD_BYTES) {
            result = -EINVAL;
        } else if (count >= BENCHMARK_MAX_MESSAGES_PER_STREAM) {
            result = -E2BIG;
        } else if (checked_add_u64(&payload_bytes, event.message->payload.len) != 0) {
            result = -EINVAL;
        } else if (checked_add_u64(&count, 1) != 0) {
            result = -EOVERFLOW;
        }
        trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_event_reset(&event);
        if (result != 0) {
            break;
        }
    }
    trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_receiver_reset(&receiver);
    if (result != 0) {
        return result;
    }
    BenchmarkSummary* message = new_summary(count, payload_bytes);
    if (message == NULL) {
        return -ENOMEM;
    }
    trevrpc_benchmark_v1_benchmark_service_client_stream_response_view response = {
        .message = message,
        .status = TREVRPC_STATUS_OK,
    };
    result = respond(respond_context, &response);
    trevrpc__benchmark__v1__benchmark_summary__free_unpacked(message, NULL);
    return result;
}

static int service_server_stream(
    void* user_data, const trevrpc_call_context* context, const StreamRequest* request, trevrpc_stream* stream) {
    (void)user_data;
    if (request == NULL || request->payload.len > BENCHMARK_MAX_PAYLOAD_BYTES || request->message_count == 0 ||
        request->message_count > BENCHMARK_MAX_MESSAGES_PER_STREAM ||
        request->response_bytes > BENCHMARK_MAX_PAYLOAD_BYTES) {
        return -EINVAL;
    }
    for (uint64_t sequence = 0; sequence < request->message_count; sequence++) {
        if (trevrpc_call_context_cancelled(context)) {
            return -ECANCELED;
        }
        BenchmarkResponse* response = new_response(sequence, request->response_bytes);
        if (response == NULL) {
            return -ENOMEM;
        }
        int err = send_benchmark_response(stream, response);
        trevrpc__benchmark__v1__benchmark_response__free_unpacked(response, NULL);
        if (err != 0) {
            return err;
        }
    }
    return 0;
}

static int service_bidi(void* user_data, const trevrpc_call_context* context, trevrpc_stream* stream) {
    (void)user_data;
    uint32_t count = 0;
    BenchmarkRequestReceiver receiver = TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_REQUEST_REQUEST_RECEIVER_INIT;
    int result = trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_receiver_init(&receiver, stream);
    if (result != 0) {
        return result;
    }
    for (;;) {
        if (trevrpc_call_context_cancelled(context)) {
            result = -ECANCELED;
            break;
        }
        BenchmarkRequestEvent event = TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_REQUEST_REQUEST_EVENT_INIT;
        result = trevrpc_benchmark_v1_benchmark_service_recv_trevrpc_benchmark_v1_benchmark_request_request(
            &receiver, &event);
        if (result != 0) {
            trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_event_reset(&event);
            break;
        }
        if (event.kind == TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_REQUEST_REQUEST_EVENT_END) {
            trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_event_reset(&event);
            break;
        }
        if (event.kind == TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_REQUEST_REQUEST_EVENT_TERMINAL_STATUS) {
            uint32_t status = TREVRPC_STATUS_UNKNOWN;
            result = trevrpc_inbound_stream_frame_get_status(event.frame, &status);
            if (result == 0 && status != TREVRPC_STATUS_OK) {
                result = -EINVAL;
            }
            trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_event_reset(&event);
            break;
        }
        if (event.kind != TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_REQUEST_REQUEST_EVENT_MESSAGE) {
            result = event.error != 0 ? event.error : TREVRPC_ERR_INVALID_FRAME;
            trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_event_reset(&event);
            break;
        }
        if (event.message->payload.len > BENCHMARK_MAX_PAYLOAD_BYTES || count >= BENCHMARK_MAX_MESSAGES_PER_STREAM ||
            event.message->response_bytes > BENCHMARK_MAX_PAYLOAD_BYTES) {
            result = -EINVAL;
            trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_event_reset(&event);
            break;
        }
        count++;
        BenchmarkResponse* response = new_response(event.message->sequence, event.message->response_bytes);
        trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_event_reset(&event);
        if (response == NULL) {
            result = -ENOMEM;
            break;
        }
        result = send_benchmark_response(stream, response);
        trevrpc__benchmark__v1__benchmark_response__free_unpacked(response, NULL);
        if (result != 0) {
            break;
        }
    }
    trevrpc_benchmark_v1_benchmark_service_benchmark_request_request_receiver_reset(&receiver);
    return result;
}

static const BenchmarkService BenchmarkServiceImplementation = {
    .user_data = NULL,
    .unary = service_unary,
    .client_stream = service_client_stream,
    .server_stream = service_server_stream,
    .bidi = service_bidi,
};

static int pack_benchmark_request(const BenchmarkRequest* request, uint8_t** body, size_t* body_len) {
    *body_len = trevrpc__benchmark__v1__benchmark_request__get_packed_size(request);
    *body = *body_len == 0 ? NULL : malloc(*body_len);
    if (*body_len > 0 && *body == NULL) {
        return -ENOMEM;
    }
    trevrpc__benchmark__v1__benchmark_request__pack(request, *body);
    return 0;
}

static int pack_stream_request(const StreamRequest* request, uint8_t** body, size_t* body_len) {
    *body_len = trevrpc__benchmark__v1__stream_request__get_packed_size(request);
    *body = *body_len == 0 ? NULL : malloc(*body_len);
    if (*body_len > 0 && *body == NULL) {
        return -ENOMEM;
    }
    trevrpc__benchmark__v1__stream_request__pack(request, *body);
    return 0;
}

static int validate_response(const BenchmarkResponse* response, uint64_t sequence, uint32_t payload_len) {
    if (response == NULL || response->sequence != sequence || response->payload.len != payload_len ||
        (payload_len > 0 && response->payload.data == NULL)) {
        return -EINVAL;
    }
    for (size_t i = 0; i < response->payload.len; i++) {
        if (response->payload.data[i] != 0) {
            return -EINVAL;
        }
    }
    return 0;
}

static int require_ok_status_frame(const trevrpc_inbound_stream_frame* frame) {
    uint32_t status = TREVRPC_STATUS_UNKNOWN;
    int err = trevrpc_inbound_stream_frame_get_status(frame, &status);
    return err != 0 ? err : (status == TREVRPC_STATUS_OK ? 0 : -EINVAL);
}

static int send_benchmark_request(trevrpc_stream* stream, const BenchmarkRequest* request) {
    return trevrpc_benchmark_v1_benchmark_service_send_trevrpc_benchmark_v1_benchmark_request(stream, request);
}

static int run_unary(trevrpc_raw_client* client, const client_options* options, uint64_t operation_sequence) {
    BenchmarkRequest request = TREVRPC__BENCHMARK__V1__BENCHMARK_REQUEST__INIT;
    request.sequence = operation_sequence;
    request.payload.len = options->request_bytes;
    request.payload.data = new_payload(options->request_bytes, 0);
    request.response_bytes = options->response_bytes;
    if (options->request_bytes > 0 && request.payload.data == NULL) {
        return -ENOMEM;
    }

    uint8_t* body = NULL;
    size_t body_len = 0;
    int err = pack_benchmark_request(&request, &body, &body_len);
    trevrpc_inbound_response* raw_response = NULL;
    if (err == 0) {
        trevrpc_request rpc_request = {
            .service = "trevrpc.benchmark.v1.BenchmarkService",
            .service_len = sizeof("trevrpc.benchmark.v1.BenchmarkService") - 1,
            .method = "Unary",
            .method_len = sizeof("Unary") - 1,
            .body = body,
            .body_len = body_len,
            .kind = TREVRPC_RPC_KIND_UNARY,
            .version = TREVRPC_WIRE_VERSION,
        };
        err = trevrpc_raw_client_call_request_inbound_v1(client, &rpc_request, NULL, &raw_response);
    }
    free(request.payload.data);
    free(body);
    uint32_t status = TREVRPC_STATUS_UNKNOWN;
    if (err == 0 && (raw_response == NULL || trevrpc_inbound_response_get_status(raw_response, &status) != 0 ||
                        status != TREVRPC_STATUS_OK)) {
        err = -EINVAL;
    }
    BenchmarkResponse* response = NULL;
    if (err == 0) {
        trevrpc_bytes_view response_body = {0};
        err = trevrpc_inbound_response_get_body(raw_response, &response_body);
        if (err == 0) {
            response = trevrpc__benchmark__v1__benchmark_response__unpack(NULL, response_body.len, response_body.data);
            err = validate_response(response, operation_sequence, options->response_bytes);
        }
    }
    if (response != NULL) {
        trevrpc__benchmark__v1__benchmark_response__free_unpacked(response, NULL);
    }
    trevrpc_inbound_response_release(raw_response);
    return err;
}

static int start_raw_stream(trevrpc_raw_client* client,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len,
    trevrpc_stream** stream) {
    trevrpc_request request = {
        .service = "trevrpc.benchmark.v1.BenchmarkService",
        .service_len = sizeof("trevrpc.benchmark.v1.BenchmarkService") - 1,
        .method = method,
        .method_len = strlen(method),
        .body = body,
        .body_len = body_len,
        .kind = kind,
        .version = TREVRPC_WIRE_VERSION,
    };
    return trevrpc_raw_client_start_stream_request_v1(client, &request, NULL, stream);
}

static int run_client_stream(trevrpc_raw_client* client, const client_options* options) {
    trevrpc_stream* stream = NULL;
    int err = start_raw_stream(client, "ClientStream", TREVRPC_RPC_KIND_CLIENT_STREAMING, NULL, 0, &stream);
    uint8_t* payload = NULL;
    if (err == 0) {
        payload = new_payload(options->request_bytes, 0);
        if (options->request_bytes > 0 && payload == NULL) {
            err = -ENOMEM;
        }
    }
    for (uint64_t sequence = 0; err == 0 && sequence < options->messages_per_stream; sequence++) {
        BenchmarkRequest request = TREVRPC__BENCHMARK__V1__BENCHMARK_REQUEST__INIT;
        request.sequence = sequence;
        request.payload.len = options->request_bytes;
        request.payload.data = payload;
        request.response_bytes = options->response_bytes;
        err = send_benchmark_request(stream, &request);
    }
    free(payload);
    if (err == 0) {
        err = trevrpc_stream_finish_send(stream);
    }

    BenchmarkSummaryReceiver receiver = TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_SUMMARY_RECEIVER_INIT;
    if (err == 0) {
        err = trevrpc_benchmark_v1_benchmark_service_benchmark_summary_receiver_init(&receiver, stream);
    }
    if (err == 0) {
        BenchmarkSummaryEvent event = TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_SUMMARY_EVENT_INIT;
        err = trevrpc_benchmark_v1_benchmark_service_recv_trevrpc_benchmark_v1_benchmark_summary(&receiver, &event);
        uint64_t expected_bytes = (uint64_t)options->request_bytes * options->messages_per_stream;
        if (err == 0 && event.kind == TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_SUMMARY_EVENT_MESSAGE) {
            if (event.message->message_count != options->messages_per_stream ||
                event.message->payload_bytes != expected_bytes) {
                err = -EINVAL;
            }
        } else if (err == 0) {
            err = event.error != 0 ? event.error : TREVRPC_ERR_INVALID_FRAME;
        }
        trevrpc_benchmark_v1_benchmark_service_benchmark_summary_event_reset(&event);
    }
    if (err == 0) {
        BenchmarkSummaryEvent event = TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_SUMMARY_EVENT_INIT;
        err = trevrpc_benchmark_v1_benchmark_service_recv_trevrpc_benchmark_v1_benchmark_summary(&receiver, &event);
        if (err == 0 && event.kind == TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_SUMMARY_EVENT_TERMINAL_STATUS) {
            err = require_ok_status_frame(event.frame);
        } else if (err == 0) {
            err = event.error != 0 ? event.error : TREVRPC_ERR_INVALID_FRAME;
        }
        trevrpc_benchmark_v1_benchmark_service_benchmark_summary_event_reset(&event);
    }
    trevrpc_benchmark_v1_benchmark_service_benchmark_summary_receiver_reset(&receiver);
    if (err != 0 && stream != NULL) {
        trevrpc_stream_cancel(stream);
    }
    trevrpc_stream_close(stream);
    return err;
}

static int run_server_stream(trevrpc_raw_client* client, const client_options* options) {
    StreamRequest request = TREVRPC__BENCHMARK__V1__STREAM_REQUEST__INIT;
    request.message_count = options->messages_per_stream;
    request.payload.len = options->request_bytes;
    request.payload.data = new_payload(options->request_bytes, 0);
    request.response_bytes = options->response_bytes;
    if (options->request_bytes > 0 && request.payload.data == NULL) {
        return -ENOMEM;
    }
    uint8_t* body = NULL;
    size_t body_len = 0;
    int err = pack_stream_request(&request, &body, &body_len);
    trevrpc_stream* stream = NULL;
    if (err == 0) {
        err = start_raw_stream(client, "ServerStream", TREVRPC_RPC_KIND_SERVER_STREAMING, body, body_len, &stream);
    }
    free(request.payload.data);
    free(body);
    if (err == 0) {
        err = trevrpc_stream_finish_send(stream);
    }
    BenchmarkResponseReceiver receiver = TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_RESPONSE_RECEIVER_INIT;
    if (err == 0) {
        err = trevrpc_benchmark_v1_benchmark_service_benchmark_response_receiver_init(&receiver, stream);
    }
    for (uint64_t sequence = 0; err == 0 && sequence < options->messages_per_stream; sequence++) {
        BenchmarkResponseEvent event = TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_RESPONSE_EVENT_INIT;
        err = trevrpc_benchmark_v1_benchmark_service_recv_trevrpc_benchmark_v1_benchmark_response(&receiver, &event);
        if (err == 0 && event.kind == TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_RESPONSE_EVENT_MESSAGE) {
            err = validate_response(event.message, sequence, options->response_bytes);
        } else if (err == 0) {
            err = event.error != 0 ? event.error : TREVRPC_ERR_INVALID_FRAME;
        }
        trevrpc_benchmark_v1_benchmark_service_benchmark_response_event_reset(&event);
    }
    if (err == 0) {
        BenchmarkResponseEvent event = TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_RESPONSE_EVENT_INIT;
        err = trevrpc_benchmark_v1_benchmark_service_recv_trevrpc_benchmark_v1_benchmark_response(&receiver, &event);
        if (err == 0 && event.kind == TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_RESPONSE_EVENT_TERMINAL_STATUS) {
            err = require_ok_status_frame(event.frame);
        } else if (err == 0) {
            err = event.error != 0 ? event.error : TREVRPC_ERR_INVALID_FRAME;
        }
        trevrpc_benchmark_v1_benchmark_service_benchmark_response_event_reset(&event);
    }
    trevrpc_benchmark_v1_benchmark_service_benchmark_response_receiver_reset(&receiver);
    if (err != 0 && stream != NULL) {
        trevrpc_stream_cancel(stream);
    }
    trevrpc_stream_close(stream);
    return err;
}

static void* bidi_sender_thread(void* context) {
    bidi_sender_args* args = context;
    uint8_t* payload = new_payload(args->options->request_bytes, 0);
    if (args->options->request_bytes > 0 && payload == NULL) {
        args->result = -ENOMEM;
        return NULL;
    }
    for (uint64_t sequence = 0; sequence < args->options->messages_per_stream; sequence++) {
        BenchmarkRequest request = TREVRPC__BENCHMARK__V1__BENCHMARK_REQUEST__INIT;
        request.sequence = sequence;
        request.payload.len = args->options->request_bytes;
        request.payload.data = payload;
        request.response_bytes = args->options->response_bytes;
        args->result = send_benchmark_request(args->stream, &request);
        if (args->result != 0) {
            break;
        }
    }
    free(payload);
    if (args->result == 0) {
        args->result = trevrpc_stream_finish_send(args->stream);
    }
    return NULL;
}

static int run_bidi(trevrpc_raw_client* client, const client_options* options) {
    trevrpc_stream* stream = NULL;
    int err = start_raw_stream(client, "Bidi", TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING, NULL, 0, &stream);
    if (err != 0) {
        return err;
    }
    BenchmarkResponseReceiver receiver = TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_RESPONSE_RECEIVER_INIT;
    err = trevrpc_benchmark_v1_benchmark_service_benchmark_response_receiver_init(&receiver, stream);
    if (err != 0) {
        trevrpc_stream_cancel(stream);
        trevrpc_stream_close(stream);
        return err;
    }

    bidi_sender_args sender_args = {.stream = stream, .options = options};
    pthread_t sender;
    int thread_err = pthread_create(&sender, NULL, bidi_sender_thread, &sender_args);
    if (thread_err != 0) {
        trevrpc_benchmark_v1_benchmark_service_benchmark_response_receiver_reset(&receiver);
        trevrpc_stream_cancel(stream);
        trevrpc_stream_close(stream);
        return -thread_err;
    }
    for (uint64_t sequence = 0; err == 0 && sequence < options->messages_per_stream; sequence++) {
        BenchmarkResponseEvent event = TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_RESPONSE_EVENT_INIT;
        err = trevrpc_benchmark_v1_benchmark_service_recv_trevrpc_benchmark_v1_benchmark_response(&receiver, &event);
        if (err == 0 && event.kind == TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_RESPONSE_EVENT_MESSAGE) {
            err = validate_response(event.message, sequence, options->response_bytes);
        } else if (err == 0) {
            err = event.error != 0 ? event.error : TREVRPC_ERR_INVALID_FRAME;
        }
        trevrpc_benchmark_v1_benchmark_service_benchmark_response_event_reset(&event);
    }
    if (err != 0) {
        trevrpc_stream_cancel(stream);
    }
    int join_err = pthread_join(sender, NULL);
    if (err == 0) {
        err = join_err == 0 ? sender_args.result : -join_err;
    }
    if (err == 0) {
        BenchmarkResponseEvent event = TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_RESPONSE_EVENT_INIT;
        err = trevrpc_benchmark_v1_benchmark_service_recv_trevrpc_benchmark_v1_benchmark_response(&receiver, &event);
        if (err == 0 && event.kind == TREVRPC_BENCHMARK_V1_BENCHMARK_SERVICE_BENCHMARK_RESPONSE_EVENT_TERMINAL_STATUS) {
            err = require_ok_status_frame(event.frame);
        } else if (err == 0) {
            err = event.error != 0 ? event.error : TREVRPC_ERR_INVALID_FRAME;
        }
        trevrpc_benchmark_v1_benchmark_service_benchmark_response_event_reset(&event);
    }
    trevrpc_benchmark_v1_benchmark_service_benchmark_response_receiver_reset(&receiver);
    if (err != 0) {
        trevrpc_stream_cancel(stream);
    }
    trevrpc_stream_close(stream);
    return err;
}

static int run_native_operation(trevrpc_raw_client* client, const client_options* options, uint64_t sequence) {
    switch (options->rpc_kind) {
    case BENCHMARK_RPC_UNARY:
        return run_unary(client, options, sequence);
    case BENCHMARK_RPC_CLIENT_STREAM:
        return run_client_stream(client, options);
    case BENCHMARK_RPC_SERVER_STREAM:
        return run_server_stream(client, options);
    case BENCHMARK_RPC_BIDI:
        return run_bidi(client, options);
    }
    return -EINVAL;
}

static int run_operation(
    benchmark_client* client, trevrpc_bench_grpc_lane* grpc_lane, const client_options* options, uint64_t sequence) {
    if (client->stack == BENCHMARK_STACK_GRPC_HTTP2) {
        return trevrpc_bench_grpc_run_operation(grpc_lane, options, sequence);
    }
    return run_native_operation(client->impl.native, options, sequence);
}

static operation_counts operation_message_counts(const client_options* options) {
    switch (options->rpc_kind) {
    case BENCHMARK_RPC_UNARY:
        return (operation_counts){.request_messages = 1, .response_messages = 1};
    case BENCHMARK_RPC_CLIENT_STREAM:
        return (operation_counts){.request_messages = options->messages_per_stream, .response_messages = 1};
    case BENCHMARK_RPC_SERVER_STREAM:
        return (operation_counts){.request_messages = 1, .response_messages = options->messages_per_stream};
    case BENCHMARK_RPC_BIDI:
        return (operation_counts){
            .request_messages = options->messages_per_stream,
            .response_messages = options->messages_per_stream,
        };
    }
    return (operation_counts){0};
}

static uint64_t histogram_upper_bound(uint64_t value) {
    value = value == 0 ? 1 : value;
    unsigned int log2 = 0;
    for (uint64_t shifted = value; shifted > 1; shifted >>= 1) {
        log2++;
    }
    unsigned int shift = log2 > 9 ? log2 - 9 : 0;
    return (((value >> shift) + 1) << shift) - 1;
}

static int histogram_add_count(histogram* target, uint64_t upper_bound_ns, uint64_t count) {
    size_t left = 0;
    size_t right = target->count;
    while (left < right) {
        size_t middle = left + (right - left) / 2;
        if (target->buckets[middle].upper_bound_ns < upper_bound_ns) {
            left = middle + 1;
        } else {
            right = middle;
        }
    }
    if (left < target->count && target->buckets[left].upper_bound_ns == upper_bound_ns) {
        return checked_add_u64(&target->buckets[left].count, count);
    }
    if (target->count == target->capacity) {
        size_t capacity = target->capacity == 0 ? 32 : target->capacity * 2;
        if (capacity < target->capacity || capacity > SIZE_MAX / sizeof(*target->buckets)) {
            return -EOVERFLOW;
        }
        histogram_bucket* buckets = realloc(target->buckets, capacity * sizeof(*buckets));
        if (buckets == NULL) {
            return -ENOMEM;
        }
        target->buckets = buckets;
        target->capacity = capacity;
    }
    memmove(&target->buckets[left + 1], &target->buckets[left], (target->count - left) * sizeof(*target->buckets));
    target->buckets[left] = (histogram_bucket){.upper_bound_ns = upper_bound_ns, .count = count};
    target->count++;
    return 0;
}

static int histogram_record(histogram* target, uint64_t value) {
    return histogram_add_count(target, histogram_upper_bound(value), 1);
}

static void histogram_reset(histogram* target) {
    free(target->buckets);
    memset(target, 0, sizeof(*target));
}

static void* lane_thread(void* context) {
    lane_args* lane = context;
    phase_control* phase = lane->phase;
    if (phase->client->stack == BENCHMARK_STACK_GRPC_HTTP2) {
        lane->result.internal_error = trevrpc_bench_grpc_lane_open(phase->client->impl.grpc, &lane->grpc_lane);
    }
    pthread_mutex_lock(&phase->mutex);
    phase->ready_count++;
    pthread_cond_broadcast(&phase->cond);
    while (!phase->started) {
        pthread_cond_wait(&phase->cond, &phase->mutex);
    }
    uint64_t deadline_ns = phase->deadline_ns;
    pthread_mutex_unlock(&phase->mutex);

    if (lane->result.internal_error != 0) {
        return NULL;
    }

    operation_counts counts = operation_message_counts(phase->options);
    uint64_t operation_index = 0;
    for (;;) {
        uint64_t operation_start = monotonic_nanos();
        if (operation_start == 0) {
            lane->result.internal_error = -EIO;
            break;
        }
        if (operation_start >= deadline_ns) {
            break;
        }
        uint64_t sequence = operation_index * phase->options->concurrency + lane->lane_index;
        int err = run_operation(phase->client, lane->grpc_lane, phase->options, sequence);
        uint64_t operation_end = monotonic_nanos();
        if (err != 0) {
            lane->result.failed++;
            fprintf(stderr,
                "lane %zu %s operation failed: %s (%d)\n",
                lane->lane_index,
                phase->options->rpc_name,
                trevrpc_error(err),
                err);
            break;
        }
        if (operation_end == 0 || operation_end < operation_start) {
            lane->result.internal_error = -EIO;
            break;
        }
        if (phase->record_latency) {
            int histogram_err = histogram_record(&lane->result.latency, operation_end - operation_start);
            if (histogram_err != 0) {
                lane->result.internal_error = histogram_err;
                break;
            }
        }
        if (checked_add_u64(&lane->result.completed, 1) != 0 ||
            checked_add_u64(&lane->result.request_messages, counts.request_messages) != 0 ||
            checked_add_u64(&lane->result.response_messages, counts.response_messages) != 0) {
            lane->result.internal_error = -EOVERFLOW;
            break;
        }
        operation_index++;
    }
    trevrpc_bench_grpc_lane_close(lane->grpc_lane);
    lane->grpc_lane = NULL;
    return NULL;
}

static int phase_prepare(
    phase_control* phase, benchmark_client* client, const client_options* options, bool record_latency) {
    memset(phase, 0, sizeof(*phase));
    phase->client = client;
    phase->options = options;
    phase->record_latency = record_latency;
    int err = pthread_mutex_init(&phase->mutex, NULL);
    if (err != 0) {
        return -err;
    }
    err = pthread_cond_init(&phase->cond, NULL);
    if (err != 0) {
        pthread_mutex_destroy(&phase->mutex);
        return -err;
    }
    phase->threads = calloc(options->concurrency, sizeof(*phase->threads));
    phase->lanes = calloc(options->concurrency, sizeof(*phase->lanes));
    if (phase->threads == NULL || phase->lanes == NULL) {
        free(phase->threads);
        free(phase->lanes);
        pthread_cond_destroy(&phase->cond);
        pthread_mutex_destroy(&phase->mutex);
        memset(phase, 0, sizeof(*phase));
        return -ENOMEM;
    }
    for (size_t i = 0; i < options->concurrency; i++) {
        phase->lanes[i].phase = phase;
        phase->lanes[i].lane_index = i;
        err = pthread_create(&phase->threads[i], NULL, lane_thread, &phase->lanes[i]);
        if (err != 0) {
            pthread_mutex_lock(&phase->mutex);
            phase->deadline_ns = monotonic_nanos();
            phase->started = true;
            pthread_cond_broadcast(&phase->cond);
            pthread_mutex_unlock(&phase->mutex);
            for (size_t j = 0; j < phase->thread_count; j++) {
                (void)pthread_join(phase->threads[j], NULL);
            }
            free(phase->threads);
            free(phase->lanes);
            pthread_cond_destroy(&phase->cond);
            pthread_mutex_destroy(&phase->mutex);
            memset(phase, 0, sizeof(*phase));
            return -err;
        }
        phase->thread_count++;
    }
    pthread_mutex_lock(&phase->mutex);
    while (phase->ready_count < options->concurrency) {
        pthread_cond_wait(&phase->cond, &phase->mutex);
    }
    pthread_mutex_unlock(&phase->mutex);
    return 0;
}

static int phase_start(phase_control* phase, uint64_t duration_ns) {
    pthread_mutex_lock(&phase->mutex);
    phase->start_ns = monotonic_nanos();
    if (phase->start_ns == 0) {
        phase->deadline_ns = 0;
    } else {
        phase->deadline_ns = saturating_add_u64(phase->start_ns, duration_ns);
    }
    phase->started = true;
    pthread_cond_broadcast(&phase->cond);
    pthread_mutex_unlock(&phase->mutex);
    return phase->start_ns == 0 ? -EIO : 0;
}

static void phase_abort(phase_control* phase) {
    if (phase->thread_count == 0) {
        return;
    }
    pthread_mutex_lock(&phase->mutex);
    phase->start_ns = monotonic_nanos();
    phase->deadline_ns = phase->start_ns;
    phase->started = true;
    pthread_cond_broadcast(&phase->cond);
    pthread_mutex_unlock(&phase->mutex);
}

static int phase_join(phase_control* phase, lane_result* total, uint64_t* elapsed_ns) {
    int result = 0;
    memset(total, 0, sizeof(*total));
    for (size_t i = 0; i < phase->thread_count; i++) {
        int err = pthread_join(phase->threads[i], NULL);
        if (err != 0 && result == 0) {
            result = -err;
        }
    }
    if (phase->record_latency) {
        sleep_until(phase->deadline_ns);
    }
    uint64_t end_ns = monotonic_nanos();
    if (elapsed_ns != NULL) {
        *elapsed_ns = end_ns >= phase->start_ns ? end_ns - phase->start_ns : 0;
    }
    for (size_t i = 0; i < phase->thread_count; i++) {
        lane_result* lane = &phase->lanes[i].result;
        if (lane->internal_error != 0 && result == 0) {
            result = lane->internal_error;
        }
        if (checked_add_u64(&total->completed, lane->completed) != 0 ||
            checked_add_u64(&total->failed, lane->failed) != 0 ||
            checked_add_u64(&total->request_messages, lane->request_messages) != 0 ||
            checked_add_u64(&total->response_messages, lane->response_messages) != 0) {
            result = -EOVERFLOW;
        }
        for (size_t bucket = 0; bucket < lane->latency.count; bucket++) {
            int err = histogram_add_count(
                &total->latency, lane->latency.buckets[bucket].upper_bound_ns, lane->latency.buckets[bucket].count);
            if (err != 0 && result == 0) {
                result = err;
            }
        }
        histogram_reset(&lane->latency);
    }
    free(phase->threads);
    free(phase->lanes);
    pthread_cond_destroy(&phase->cond);
    pthread_mutex_destroy(&phase->mutex);
    memset(phase, 0, sizeof(*phase));
    return result;
}

static int run_warmup(benchmark_client* client, const client_options* options) {
    if (options->warmup_ns == 0) {
        return 0;
    }
    phase_control phase;
    int err = phase_prepare(&phase, client, options, false);
    if (err != 0) {
        return err;
    }
    err = phase_start(&phase, options->warmup_ns);
    lane_result result;
    uint64_t elapsed_ns = 0;
    int join_err = phase_join(&phase, &result, &elapsed_ns);
    histogram_reset(&result.latency);
    if (err == 0) {
        err = join_err;
    }
    if (err == 0 && (result.failed != 0 || result.internal_error != 0)) {
        err = -EIO;
    }
    return err;
}

static int emit_sample(const client_options* options, uint64_t elapsed_ns, const lane_result* result) {
    uint64_t drain_ns = elapsed_ns > options->measurement_ns ? elapsed_ns - options->measurement_ns : 0;
    if (fprintf(
            stdout, "{\"schema_version\":%d,\"event\":\"sample\",\"peer\":\"c\",\"stack\":", BENCHMARK_SCHEMA_VERSION) <
            0 ||
        write_json_string(stdout, options->stack_name) != 0 || fputs(",\"rpc_kind\":", stdout) == EOF ||
        write_json_string(stdout, options->rpc_name) != 0 ||
        fprintf(stdout,
            ",\"admission_ns\":\"%" PRIu64 "\",\"elapsed_ns\":\"%" PRIu64 "\",\"drain_ns\":\"%" PRIu64
            "\",\"completed\":\"%" PRIu64 "\",\"failed\":\"%" PRIu64 "\",\"request_messages\":\"%" PRIu64
            "\",\"response_messages\":\"%" PRIu64 "\",\"histogram\":[",
            options->measurement_ns,
            elapsed_ns,
            drain_ns,
            result->completed,
            result->failed,
            result->request_messages,
            result->response_messages) < 0) {
        return -EIO;
    }
    for (size_t i = 0; i < result->latency.count; i++) {
        if (i > 0 && fputc(',', stdout) == EOF) {
            return -EIO;
        }
        if (fprintf(stdout,
                "{\"upper_bound_ns\":\"%" PRIu64 "\",\"count\":\"%" PRIu64 "\"}",
                result->latency.buckets[i].upper_bound_ns,
                result->latency.buckets[i].count) < 0) {
            return -EIO;
        }
    }
    if (fputs("]}", stdout) == EOF) {
        return -EIO;
    }
    return flush_event();
}

static int read_control_command(char* command, size_t command_len) {
    if (fgets(command, (int)command_len, stdin) == NULL) {
        return ferror(stdin) ? -EIO : -ENODATA;
    }
    size_t length = strlen(command);
    if (length > 0 && command[length - 1] == '\n') {
        command[--length] = '\0';
    } else if (!feof(stdin)) {
        int byte = 0;
        while ((byte = fgetc(stdin)) != '\n' && byte != EOF) {
        }
        return -EINVAL;
    }
    if (length > 0 && command[length - 1] == '\r') {
        command[length - 1] = '\0';
    }
    return 0;
}

static void* server_thread(void* context) {
    server_thread_args* args = context;
    int result = trevrpc_server_serve(args->server);
    pthread_mutex_lock(&args->mutex);
    args->result = result;
    args->done = true;
    pthread_cond_broadcast(&args->cond);
    pthread_mutex_unlock(&args->mutex);
    return NULL;
}

static void server_transport_event(void* context, const trevrpc_transport_event* event) {
    if (event->kind != TREVRPC_TRANSPORT_EVENT_LISTENER_OPEN) {
        return;
    }
    server_thread_args* args = context;
    pthread_mutex_lock(&args->mutex);
    args->ready = true;
    pthread_cond_broadcast(&args->cond);
    pthread_mutex_unlock(&args->mutex);
}

static int wait_for_server_ready(server_thread_args* args) {
    pthread_mutex_lock(&args->mutex);
    while (!args->ready && !args->done) {
        pthread_cond_wait(&args->cond, &args->mutex);
    }
    int result = args->ready ? 0 : (args->result == 0 ? -ECANCELED : args->result);
    pthread_mutex_unlock(&args->mutex);
    return result;
}

static bool server_thread_done(server_thread_args* args, int* result) {
    pthread_mutex_lock(&args->mutex);
    bool done = args->done;
    if (done) {
        *result = args->result;
    }
    pthread_mutex_unlock(&args->mutex);
    return done;
}

static bool native_server_done(void* context, int* result) {
    return server_thread_done(context, result);
}

static bool grpc_server_done(void* context, int* result) {
    return trevrpc_bench_grpc_server_done(context, result);
}

static void server_signal_handler(int signal_number) {
    (void)signal_number;
    server_stop_requested = 1;
}

static int wait_for_server_shutdown(
    void* server_context, bool (*server_done)(void* context, int* result), bool* graceful) {
    *graceful = false;
    char command[32];
    for (;;) {
        int server_result = 0;
        if (server_done(server_context, &server_result)) {
            return server_result == 0 ? -ECANCELED : server_result;
        }
        if (server_stop_requested) {
            return 0;
        }
        struct pollfd input = {.fd = STDIN_FILENO, .events = POLLIN};
        int ready = poll(&input, 1, 100);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -errno;
        }
        if (ready == 0) {
            continue;
        }
        if ((input.revents & POLLIN) != 0) {
            int err = read_control_command(command, sizeof(command));
            if (err != 0) {
                return err;
            }
            if (strcmp(command, "SHUTDOWN") != 0) {
                return -EINVAL;
            }
            *graceful = true;
            return 0;
        }
        if ((input.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0) {
            return -ENODATA;
        }
    }
}

static int run_grpc_server(const server_options* options) {
    trevrpc_bench_grpc_server* server = NULL;
    uint16_t actual_port = 0;
    int err = trevrpc_bench_grpc_server_start(options, &server, &actual_port);
    if (err != 0) {
        return fail_with_error("listen", "listen_failed", "%s (%d)", trevrpc_error(err), err);
    }

    struct sigaction action = {0};
    action.sa_handler = server_signal_handler;
    sigemptyset(&action.sa_mask);
    (void)sigaction(SIGINT, &action, NULL);
    (void)sigaction(SIGTERM, &action, NULL);
    if (emit_ready(options->host, actual_port, options->stack_name) != 0) {
        (void)trevrpc_bench_grpc_server_close(server);
        return 1;
    }

    bool graceful = false;
    int wait_err = wait_for_server_shutdown(server, grpc_server_done, &graceful);
    int shutdown_err = trevrpc_bench_grpc_server_shutdown(server);
    int close_err = trevrpc_bench_grpc_server_close(server);
    if (wait_err != 0) {
        return fail_with_error("serve", "control_failed", "%s (%d)", trevrpc_error(wait_err), wait_err);
    }
    err = shutdown_err != 0 ? shutdown_err : close_err;
    if (err != 0) {
        return fail_with_error("serve", "shutdown_failed", "%s (%d)", trevrpc_error(err), err);
    }
    if (graceful && emit_stopped(options->stack_name) != 0) {
        return 1;
    }
    return 0;
}

static int run_server(int argc, char** argv) {
    char parse_error[256];
    server_options options;
    int err = parse_server_options(argc, argv, &options, parse_error, sizeof(parse_error));
    if (err != 0) {
        free(options.host);
        return fail_with_error("config", "invalid_argument", "%s", parse_error);
    }
    if (options.stack == BENCHMARK_STACK_GRPC_HTTP2) {
        err = run_grpc_server(&options);
        free(options.host);
        return err;
    }

    trevrpc_server_config_v1 config;
    err = trevrpc_server_config_v1_init(&config, sizeof(config));
    if (err != 0) {
        free(options.host);
        return fail_with_error("config", "config_failed", "%s (%d)", trevrpc_error(err), err);
    }
    config.host = options.host;
    config.port = options.port;
    config.cert_file = options.cert;
    config.key_file = options.key;
    config.max_idle_timeout_ms = BENCHMARK_IDLE_TIMEOUT_MS;
    config.keep_alive_ms = BENCHMARK_KEEP_ALIVE_MS;
    config.peer_bidi_stream_count = BENCHMARK_SERVER_STREAMS;
    config.max_stateless_operations = BENCHMARK_SERVER_REQUESTS;
    config.max_binding_stateless_operations = BENCHMARK_SERVER_STREAMS;
    config.max_frame_size = BENCHMARK_MAX_FRAME_SIZE;
    if (options.stack == BENCHMARK_STACK_TREVRPC_WEBTRANSPORT) {
        config.webtransport_path = BENCHMARK_WEBTRANSPORT_PATH;
        config.webtransport_origin = options.webtransport_origin;
    }
    trevrpc_server* server = NULL;
    err = trevrpc_server_listen_v1(&config, &server);
    if (err != 0) {
        free(options.host);
        return fail_with_error("listen", "listen_failed", "%s (%d)", trevrpc_error(err), err);
    }
    trevrpc_server_options_v1 runtime_options;
    err = trevrpc_server_options_v1_init(&runtime_options, sizeof(runtime_options));
    if (err != 0) {
        (void)trevrpc_server_stop(server);
        (void)trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE);
        (void)trevrpc_server_release(server);
        free(options.host);
        return fail_with_error("config", "config_failed", "%s (%d)", trevrpc_error(err), err);
    }
    runtime_options.max_concurrent_streams_per_connection = BENCHMARK_SERVER_STREAMS;
    runtime_options.max_concurrent_requests = BENCHMARK_SERVER_REQUESTS;
    runtime_options.worker_count = (int64_t)options.workers;
    runtime_options.worker_queue_capacity = BENCHMARK_SERVER_REQUESTS;
    runtime_options.graceful_shutdown_timeout_nanos = BENCHMARK_GRACEFUL_SHUTDOWN_NS;
    runtime_options.max_stream_messages = BENCHMARK_MAX_MESSAGES_PER_STREAM;
    runtime_options.max_stream_body_size = -1;
    err = trevrpc_server_set_options_v1(server, &runtime_options);
    if (err == 0) {
        err = trevrpc_benchmark_v1_benchmark_service_register(server, &BenchmarkServiceImplementation);
    }
    if (err != 0) {
        (void)trevrpc_server_stop(server);
        (void)trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE);
        (void)trevrpc_server_release(server);
        free(options.host);
        return fail_with_error("listen", "service_setup_failed", "%s (%d)", trevrpc_error(err), err);
    }

    uint16_t actual_port = 0;
    err = trevrpc_server_port(server, &actual_port);
    server_thread_args thread_args = {.server = server};
    pthread_t thread;
    bool mutex_initialized = false;
    bool cond_initialized = false;
    bool observer_set = false;
    bool thread_started = false;
    if (err == 0) {
        int mutex_err = pthread_mutex_init(&thread_args.mutex, NULL);
        err = mutex_err == 0 ? 0 : -mutex_err;
        mutex_initialized = mutex_err == 0;
    }
    if (err == 0) {
        int cond_err = pthread_cond_init(&thread_args.cond, NULL);
        err = cond_err == 0 ? 0 : -cond_err;
        cond_initialized = cond_err == 0;
    }
    if (err == 0) {
        trevrpc_transport_observer observer = {
            .transport_event = server_transport_event,
            .user_data = &thread_args,
        };
        err = trevrpc_server_set_transport_observer(server, &observer);
        observer_set = err == 0;
    }
    if (err == 0) {
        err = trevrpc_server_freeze(server);
    }
    if (err == 0) {
        int thread_err = pthread_create(&thread, NULL, server_thread, &thread_args);
        err = thread_err == 0 ? 0 : -thread_err;
        thread_started = thread_err == 0;
    }
    if (err != 0) {
        if (observer_set) {
            trevrpc_server_clear_transport_observer(server);
        }
        if (cond_initialized) {
            pthread_cond_destroy(&thread_args.cond);
        }
        if (mutex_initialized) {
            pthread_mutex_destroy(&thread_args.mutex);
        }
        (void)trevrpc_server_stop(server);
        (void)trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE);
        (void)trevrpc_server_release(server);
        free(options.host);
        return fail_with_error("listen", "serve_failed", "%s (%d)", trevrpc_error(err), err);
    }

    err = wait_for_server_ready(&thread_args);
    if (err != 0) {
        (void)pthread_join(thread, NULL);
        trevrpc_server_clear_transport_observer(server);
        pthread_cond_destroy(&thread_args.cond);
        pthread_mutex_destroy(&thread_args.mutex);
        (void)trevrpc_server_stop(server);
        (void)trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE);
        (void)trevrpc_server_release(server);
        free(options.host);
        return fail_with_error("listen", "serve_failed", "%s (%d)", trevrpc_error(err), err);
    }

    struct sigaction action = {0};
    action.sa_handler = server_signal_handler;
    sigemptyset(&action.sa_mask);
    (void)sigaction(SIGINT, &action, NULL);
    (void)sigaction(SIGTERM, &action, NULL);
    if (emit_ready(options.host, actual_port, options.stack_name) != 0) {
        (void)trevrpc_server_stop(server);
        (void)pthread_join(thread, NULL);
        trevrpc_server_clear_transport_observer(server);
        pthread_cond_destroy(&thread_args.cond);
        pthread_mutex_destroy(&thread_args.mutex);
        (void)trevrpc_server_stop(server);
        (void)trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE);
        (void)trevrpc_server_release(server);
        free(options.host);
        return 1;
    }

    bool graceful = false;
    int wait_err = wait_for_server_shutdown(&thread_args, native_server_done, &graceful);
    (void)trevrpc_server_stop(server);
    int join_err = thread_started ? pthread_join(thread, NULL) : 0;
    int serve_result = 0;
    (void)server_thread_done(&thread_args, &serve_result);
    if (serve_result == TREV_MSQUIC_ERR_CLOSED) {
        serve_result = 0;
    }
    trevrpc_server_clear_transport_observer(server);
    pthread_cond_destroy(&thread_args.cond);
    pthread_mutex_destroy(&thread_args.mutex);
    int server_wait_err = trevrpc_server_wait_until(server, TREVRPC_DEADLINE_INFINITE);
    int server_release_err = server_wait_err == 0 ? trevrpc_server_release(server) : server_wait_err;
    free(options.host);
    if (wait_err != 0) {
        return fail_with_error("serve", "control_failed", "%s (%d)", trevrpc_error(wait_err), wait_err);
    }
    if (join_err != 0 || serve_result != 0 || server_release_err != 0) {
        err = join_err != 0 ? -join_err : (serve_result != 0 ? serve_result : server_release_err);
        return fail_with_error("serve", "shutdown_failed", "%s (%d)", trevrpc_error(err), err);
    }
    if (graceful && emit_stopped(options.stack_name) != 0) {
        return 1;
    }
    return 0;
}

static void benchmark_client_close(benchmark_client* client) {
    if (client->stack == BENCHMARK_STACK_GRPC_HTTP2) {
        trevrpc_bench_grpc_client_close(client->impl.grpc);
    } else {
        trevrpc_raw_client_close(client->impl.native);
    }
    memset(client, 0, sizeof(*client));
}

static int validate_client(benchmark_client* client, const client_options* options) {
    trevrpc_bench_grpc_lane* grpc_lane = NULL;
    int err = 0;
    if (client->stack == BENCHMARK_STACK_GRPC_HTTP2) {
        err = trevrpc_bench_grpc_lane_open(client->impl.grpc, &grpc_lane);
    }
    if (err == 0) {
        err = run_operation(client, grpc_lane, options, 0);
    }
    trevrpc_bench_grpc_lane_close(grpc_lane);
    return err;
}

static int run_client(int argc, char** argv) {
    char parse_error[256];
    client_options options;
    int err = parse_client_options(argc, argv, &options, parse_error, sizeof(parse_error));
    if (err != 0) {
        free(options.host);
        return fail_with_error("config", "invalid_argument", "%s", parse_error);
    }

    benchmark_client client = {.stack = options.stack};
    if (options.stack == BENCHMARK_STACK_GRPC_HTTP2) {
        err = trevrpc_bench_grpc_client_connect(&options, &client.impl.grpc);
    } else {
        trevrpc_client_config_v1 config;
        err = trevrpc_client_config_v1_init(&config, sizeof(config));
        if (err == 0) {
            config.ca_cert_file = options.cert;
            config.skip_certificate_validation = 0;
            config.max_idle_timeout_ms = BENCHMARK_IDLE_TIMEOUT_MS;
            config.keep_alive_ms = BENCHMARK_KEEP_ALIVE_MS;
            config.peer_bidi_stream_count =
                (uint16_t)(options.concurrency > UINT16_MAX ? UINT16_MAX : options.concurrency);
            config.max_frame_size = BENCHMARK_MAX_FRAME_SIZE;
            err = trevrpc_raw_client_connect_v1(options.host, options.port, &config, NULL, &client.impl.native);
        }
    }
    if (err != 0) {
        int result = fail_with_error("connect", "connect_failed", "%s (%d)", trevrpc_error(err), err);
        free(options.host);
        return result;
    }

    err = validate_client(&client, &options);
    if (err != 0) {
        benchmark_client_close(&client);
        free(options.host);
        return fail_with_error("validate", "rpc_failed", "%s (%d)", trevrpc_error(err), err);
    }
    err = run_warmup(&client, &options);
    if (err != 0) {
        benchmark_client_close(&client);
        free(options.host);
        return fail_with_error("warmup", "rpc_failed", "%s (%d)", trevrpc_error(err), err);
    }

    phase_control phase;
    err = phase_prepare(&phase, &client, &options, true);
    if (err != 0) {
        benchmark_client_close(&client);
        free(options.host);
        return fail_with_error("arm", "lane_setup_failed", "%s (%d)", trevrpc_error(err), err);
    }
    if (emit_armed(options.stack_name) != 0) {
        phase_abort(&phase);
        lane_result discarded;
        (void)phase_join(&phase, &discarded, NULL);
        histogram_reset(&discarded.latency);
        benchmark_client_close(&client);
        free(options.host);
        return 1;
    }
    char command[32];
    err = read_control_command(command, sizeof(command));
    if (err == 0 && strcmp(command, "START") != 0) {
        err = -EINVAL;
    }
    if (err != 0) {
        phase_abort(&phase);
        lane_result discarded;
        (void)phase_join(&phase, &discarded, NULL);
        histogram_reset(&discarded.latency);
        benchmark_client_close(&client);
        free(options.host);
        return fail_with_error("control", "invalid_command", "expected START");
    }

    err = phase_start(&phase, options.measurement_ns);
    lane_result result;
    uint64_t elapsed_ns = 0;
    int join_err = phase_join(&phase, &result, &elapsed_ns);
    benchmark_client_close(&client);
    free(options.host);
    if (err == 0) {
        err = join_err;
    }
    if (err != 0) {
        histogram_reset(&result.latency);
        return fail_with_error("measure", "internal_error", "%s (%d)", trevrpc_error(err), err);
    }
    uint64_t histogram_count = 0;
    for (size_t i = 0; i < result.latency.count; i++) {
        if (checked_add_u64(&histogram_count, result.latency.buckets[i].count) != 0) {
            histogram_reset(&result.latency);
            return fail_with_error("measure", "count_overflow", "histogram count overflow");
        }
    }
    if (histogram_count != result.completed) {
        histogram_reset(&result.latency);
        return fail_with_error("measure", "count_mismatch", "histogram count does not match completed operations");
    }
    int emit_err = emit_sample(&options, elapsed_ns, &result);
    histogram_reset(&result.latency);
    if (emit_err != 0) {
        return 1;
    }
    return 0;
}

static void print_usage(const char* program) {
    fprintf(stderr,
        "usage: %s capabilities | server --stack STACK --listen HOST:PORT --cert FILE --key FILE "
        "[--webtransport-origin ORIGIN] | client --stack "
        "STACK [options]\n",
        program);
}

int main(int argc, char** argv) {
    if (argc >= 2 && strcmp(argv[1], "capabilities") == 0) {
        if (argc != 2) {
            return fail_with_error("config", "invalid_argument", "capabilities takes no options");
        }
        return emit_capabilities() == 0 ? 0 : 1;
    }
    if (argc >= 2 && strcmp(argv[1], "server") == 0) {
        return run_server(argc, argv);
    }
    if (argc >= 2 && strcmp(argv[1], "client") == 0) {
        return run_client(argc, argv);
    }
    print_usage(argv[0]);
    return fail_with_error("config", "invalid_command", "expected capabilities, server, or client");
}
