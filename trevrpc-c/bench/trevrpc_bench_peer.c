#define _POSIX_C_SOURCE 200809L

#include "benchmark.pb-c.h"
#include "benchmark.trevrpc.h"
#include "trevrpc_bench_peer.h"
#include "trevrpc_rpc.h"
#include "trevrpc_rpc_msquic.h"

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

#define BENCHMARK_SCHEMA_VERSION 5
#define BENCHMARK_HTTP3_PATH "/trevrpc"
#define BENCHMARK_WEBTRANSPORT_PATH "/trevrpc"
#define BENCHMARK_DISABLED_WEBTRANSPORT_PATH ""
#define BENCHMARK_WEBTRANSPORT_ORIGIN "https://benchmark.invalid"
#define BENCHMARK_SHUTDOWN_REPORT_MARGIN_NS UINT64_C(500000000)

typedef Trevrpc__Benchmark__V1__BenchmarkRequest BenchmarkRequest;
typedef Trevrpc__Benchmark__V1__BenchmarkResponse BenchmarkResponse;
typedef Trevrpc__Benchmark__V1__BenchmarkSummary BenchmarkSummary;
typedef Trevrpc__Benchmark__V1__StreamRequest StreamRequest;

typedef struct pending_event {
    trevrpc_rpc_event* event;
    trevrpc_rpc_event_info_v1 info;
} pending_event;
typedef struct benchmark_client {
    trevrpc_rpc_runtime* runtime;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    pthread_mutex_t event_mutex;
    pending_event* pending;
    size_t pending_count, pending_capacity;
    uint64_t next_operation_id;
} benchmark_client;
typedef struct histogram_bucket {
    uint64_t upper_bound_ns;
    uint64_t count;
} histogram_bucket;
typedef struct histogram {
    histogram_bucket* buckets;
    size_t count, capacity;
} histogram;
typedef struct operation_counts {
    uint64_t request_messages, response_messages;
} operation_counts;
typedef struct lane_result {
    uint64_t completed, failed, request_messages, response_messages;
    int internal_error;
    histogram latency;
} lane_result;
typedef struct phase_control phase_control;
typedef struct lane_args {
    phase_control* phase;
    size_t lane_index;
    lane_result result;
} lane_args;
struct phase_control {
    benchmark_client* client;
    const client_options* options;
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    bool started;
    size_t ready_count;
    uint64_t start_ns, deadline_ns;
    bool record_latency;
    pthread_t* threads;
    lane_args* lanes;
    size_t thread_count;
};
typedef enum server_call_kind {
    SERVER_CALL_UNARY,
    SERVER_CALL_CLIENT_STREAM,
    SERVER_CALL_SERVER_STREAM,
    SERVER_CALL_BIDI
} server_call_kind;
typedef struct server_call {
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    trevrpc_rpc_receive* initial;
    server_call_kind kind;
    uint32_t messages;
    uint64_t payload_bytes;
    uint64_t response_count;
    uint64_t responses_submitted;
    uint64_t* response_sequences;
    size_t response_sequence_capacity;
    uint32_t response_bytes;
    bool response_send_pending;
    bool receive_finished;
    bool finish_submitted;
    bool close_requested;
    bool stream_closed, call_closed;
    bool stream_released, call_released;
} server_call;
typedef struct server_state {
    trevrpc_rpc_runtime* runtime;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    server_call* calls;
    size_t call_count, call_capacity;
    uint64_t next_operation_id;
    int event_error;
    bool stopping;
    bool endpoint_ready;
    bool endpoint_closed;
    bool runtime_stopped;
} server_state;
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
            "\"roles\":{\"client\":[\"trevrpc_native_quic\",\"trevrpc_webtransport\"],"
            "\"server\":[\"trevrpc_native_quic\",\"trevrpc_http3\",\"trevrpc_webtransport\"]},"
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

static int emit_prepared(const char* origin) {
    if (fprintf(stdout,
            "{\"schema_version\":%d,\"event\":\"prepared\",\"peer\":\"c\",\"origin\":",
            BENCHMARK_SCHEMA_VERSION) < 0 ||
        write_json_string(stdout, origin) != 0 || fprintf(stdout, ",\"pid\":%ld}", (long)getpid()) < 0) {
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

static uint32_t transport_for_stack(benchmark_stack stack) {
    if (stack == BENCHMARK_STACK_TREVRPC_HTTP3) {
        return TREVRPC_RPC_MSQUIC_TRANSPORT_HTTP3;
    }
    if (stack == BENCHMARK_STACK_TREVRPC_WEBTRANSPORT) {
        return TREVRPC_RPC_MSQUIC_TRANSPORT_WEBTRANSPORT;
    }
    return TREVRPC_RPC_MSQUIC_TRANSPORT_NATIVE;
}

static int parse_stack(const char* value, benchmark_stack* stack, const char** stack_name) {
    if (strcmp(value, "trevrpc_native_quic") == 0) {
        *stack = BENCHMARK_STACK_TREVRPC_NATIVE_QUIC;
    } else if (strcmp(value, "trevrpc_http3") == 0) {
        *stack = BENCHMARK_STACK_TREVRPC_HTTP3;
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
    if (stack == NULL || options->cert == NULL || rpc == NULL || concurrency == NULL || warmup_ms == NULL ||
        measurement_ms == NULL || request_bytes == NULL || response_bytes == NULL || messages_per_stream == NULL) {
        snprintf(error, error_len, "client requires all peer protocol options");
        return -EINVAL;
    }
    if (parse_stack(stack, &options->stack, &options->stack_name) != 0) {
        snprintf(error, error_len, "invalid --stack value: %s", stack);
        return -EINVAL;
    }
    if (options->stack == BENCHMARK_STACK_TREVRPC_HTTP3) {
        snprintf(error, error_len, "trevrpc_http3 is server-only");
        return -EINVAL;
    }
    if (options->stack == BENCHMARK_STACK_TREVRPC_NATIVE_QUIC && address == NULL) {
        snprintf(error, error_len, "trevrpc_native_quic client requires --address");
        return -EINVAL;
    }
    if (parse_rpc_kind(rpc, options) != 0) {
        snprintf(error, error_len, "invalid --rpc value: %s", rpc);
        return -EINVAL;
    }
    int err = 0;
    if (address != NULL) {
        if (options->stack == BENCHMARK_STACK_TREVRPC_WEBTRANSPORT) {
            snprintf(error, error_len, "--address is only valid for a trevrpc_native_quic client");
            return -EINVAL;
        }
        err = split_address(address, false, &options->host, &options->port);
        if (err != 0) {
            snprintf(error, error_len, "invalid --address: %s", address);
            return err;
        }
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

static const char* rpc_error_string(int error) {
    if (error < 0 && -error > 0 && -error < 4096)
        return strerror(-error);
    return error == 0 ? "success" : "runtime error";
}
static int validate_response(const BenchmarkResponse* response, uint64_t sequence, uint32_t payload_len) {
    if (response == NULL || response->sequence != sequence || response->payload.len != payload_len ||
        (payload_len > 0 && response->payload.data == NULL))
        return -EINVAL;
    for (size_t i = 0; i < response->payload.len; i++)
        if (response->payload.data[i] != 0)
            return -EINVAL;
    return 0;
}
static operation_counts operation_message_counts(const client_options* options) {
    switch (options->rpc_kind) {
    case BENCHMARK_RPC_UNARY:
        return (operation_counts){1, 1};
    case BENCHMARK_RPC_CLIENT_STREAM:
        return (operation_counts){options->messages_per_stream, 1};
    case BENCHMARK_RPC_SERVER_STREAM:
        return (operation_counts){1, options->messages_per_stream};
    case BENCHMARK_RPC_BIDI:
        return (operation_counts){options->messages_per_stream, options->messages_per_stream};
    }
    return (operation_counts){0};
}

static bool handle_equal(uint64_t a, uint32_t b, uint32_t c, uint64_t x, uint32_t y, uint32_t z) {
    return a == x && b == y && c == z;
}
static bool stream_equal(trevrpc_rpc_stream_v1 a, trevrpc_rpc_stream_v1 b) {
    return handle_equal(a.owner, a.slot, a.generation, b.owner, b.slot, b.generation);
}
static bool call_equal(trevrpc_rpc_call_v1 a, trevrpc_rpc_call_v1 b) {
    return handle_equal(a.owner, a.slot, a.generation, b.owner, b.slot, b.generation);
}
static bool endpoint_equal(trevrpc_rpc_endpoint_v1 a, trevrpc_rpc_endpoint_v1 b) {
    return handle_equal(a.owner, a.slot, a.generation, b.owner, b.slot, b.generation);
}

static uint64_t next_client_operation(benchmark_client* c) {
    pthread_mutex_lock(&c->event_mutex);
    uint64_t n = ++c->next_operation_id;
    if (!n)
        n = ++c->next_operation_id;
    pthread_mutex_unlock(&c->event_mutex);
    return n;
}
static int client_append_locked(benchmark_client* c, trevrpc_rpc_event* e, trevrpc_rpc_event_info_v1* i) {
    if (c->pending_count == c->pending_capacity) {
        size_t n = c->pending_capacity ? c->pending_capacity * 2 : 64;
        pending_event* p = realloc(c->pending, n * sizeof(*p));
        if (!p)
            return -ENOMEM;
        c->pending = p;
        c->pending_capacity = n;
    }
    c->pending[c->pending_count++] = (pending_event){e, *i};
    return 0;
}
static int client_collect_locked(benchmark_client* c) {
    for (;;) {
        trevrpc_rpc_event* e = NULL;
        int r = trevrpc_rpc_runtime_next_event(c->runtime, &e);
        if (r == -EAGAIN)
            return 0;
        if (r)
            return r;
        trevrpc_rpc_event_info_v1 i;
        r = trevrpc_rpc_event_info_v1_init(&i, sizeof(i));
        if (!r)
            r = trevrpc_rpc_event_get_info_v1(e, &i);
        if (!r)
            r = client_append_locked(c, e, &i);
        if (r) {
            trevrpc_rpc_event_release(e);
            return r;
        }
    }
}
typedef enum event_subject { SUBJECT_ANY, SUBJECT_CALL, SUBJECT_STREAM, SUBJECT_ENDPOINT } event_subject;
static bool event_matches(pending_event* p, uint32_t kind, uint64_t op, event_subject subject, const void* h) {
    if (p->info.kind != kind || p->info.operation_id != op)
        return false;
    if (subject == SUBJECT_CALL)
        return call_equal(p->info.call, *(const trevrpc_rpc_call_v1*)h);
    if (subject == SUBJECT_STREAM)
        return stream_equal(p->info.stream, *(const trevrpc_rpc_stream_v1*)h);
    if (subject == SUBJECT_ENDPOINT)
        return endpoint_equal(p->info.endpoint, *(const trevrpc_rpc_endpoint_v1*)h);
    return true;
}
static int client_wait(benchmark_client* c,
    uint32_t kind,
    uint64_t op,
    event_subject subject,
    const void* h,
    trevrpc_rpc_event_info_v1* out) {
    for (;;) {
        pthread_mutex_lock(&c->event_mutex);
        int r = client_collect_locked(c);
        if (!r)
            for (size_t i = 0; i < c->pending_count; i++)
                if (event_matches(&c->pending[i], kind, op, subject, h)) {
                    pending_event p = c->pending[i];
                    c->pending[i] = c->pending[--c->pending_count];
                    *out = p.info;
                    trevrpc_rpc_event_release(p.event);
                    pthread_mutex_unlock(&c->event_mutex);
                    return out->status;
                }
        pthread_mutex_unlock(&c->event_mutex);
        if (r)
            return r;
        struct pollfd fd = {(int)c->wake.native_handle, POLLIN, 0};
        r = poll(&fd, 1, 1000);
        if (r < 0 && errno != EINTR)
            return -errno;
    }
}
static int client_wait_ready(benchmark_client* c, trevrpc_rpc_event_info_v1* out) {
    for (;;) {
        pthread_mutex_lock(&c->event_mutex);
        int r = client_collect_locked(c);
        if (!r) {
            for (size_t i = 0; i < c->pending_count; i++) {
                pending_event p = c->pending[i];
                bool endpoint_start = p.info.operation_id == 1 && endpoint_equal(p.info.endpoint, c->endpoint);
                bool endpoint_terminal = p.info.kind == TREVRPC_RPC_EVENT_ENDPOINT_FAILED && endpoint_start;
                bool stopped = p.info.kind == TREVRPC_RPC_EVENT_STOPPED;
                if ((p.info.kind == TREVRPC_RPC_EVENT_ENDPOINT_READY && endpoint_start) || endpoint_terminal ||
                    stopped) {
                    c->pending[i] = c->pending[--c->pending_count];
                    *out = p.info;
                    trevrpc_rpc_event_release(p.event);
                    pthread_mutex_unlock(&c->event_mutex);
                    return p.info.kind == TREVRPC_RPC_EVENT_ENDPOINT_READY ? 0
                           : p.info.status != 0                            ? p.info.status
                                                                           : -EIO;
                }
            }
        }
        pthread_mutex_unlock(&c->event_mutex);
        if (r)
            return r;
        struct pollfd fd = {(int)c->wake.native_handle, POLLIN, 0};
        r = poll(&fd, 1, 1000);
        if (r < 0 && errno != EINTR)
            return -errno;
    }
}
static int client_wait_call(benchmark_client* c, uint64_t op) {
    for (;;) {
        pthread_mutex_lock(&c->event_mutex);
        int r = client_collect_locked(c);
        if (!r) {
            for (size_t i = 0; i < c->pending_count; i++) {
                pending_event p = c->pending[i];
                bool matching_operation = p.info.operation_id == op;
                if (matching_operation &&
                    (p.info.kind == TREVRPC_RPC_EVENT_CALL_READY || p.info.kind == TREVRPC_RPC_EVENT_CALL_FAILED)) {
                    c->pending[i] = c->pending[--c->pending_count];
                    trevrpc_rpc_event_release(p.event);
                    pthread_mutex_unlock(&c->event_mutex);
                    return p.info.kind == TREVRPC_RPC_EVENT_CALL_READY ? p.info.status
                           : p.info.status != 0                        ? p.info.status
                                                                       : -EIO;
                }
            }
        }
        pthread_mutex_unlock(&c->event_mutex);
        if (r)
            return r;
        struct pollfd fd = {(int)c->wake.native_handle, POLLIN, 0};
        r = poll(&fd, 1, 1000);
        if (r < 0 && errno != EINTR)
            return -errno;
    }
}
static int client_wait_stream(benchmark_client* c, uint32_t kind, trevrpc_rpc_stream_v1 s, uint64_t op) {
    trevrpc_rpc_event_info_v1 i;
    return client_wait(c, kind, op, SUBJECT_STREAM, &s, &i);
}

static int client_wait_receive_fin(benchmark_client* c, trevrpc_rpc_stream_v1 s) {
    bool drain_readable = true;
    for (;;) {
        if (drain_readable) {
            trevrpc_rpc_receive* unexpected = NULL;
            int result = trevrpc_rpc_stream_receive(c->runtime, s, &unexpected);
            if (result == 0) {
                trevrpc_rpc_receive_release(unexpected);
                return -EPROTO;
            }
            if (result != -EAGAIN && result != -EPIPE && result != -ESTALE)
                return result;
            drain_readable = false;
        }

        bool readable = false;
        bool receive_fin = false;
        int terminal_status = 0;
        pthread_mutex_lock(&c->event_mutex);
        int result = client_collect_locked(c);
        if (!result) {
            size_t readable_index = c->pending_count;
            size_t receive_fin_index = c->pending_count;
            for (size_t index = 0; index < c->pending_count; ++index) {
                pending_event* pending = &c->pending[index];
                if (!stream_equal(pending->info.stream, s))
                    continue;
                if (pending->info.kind == TREVRPC_RPC_EVENT_STREAM_READABLE && pending->info.operation_id == 0) {
                    readable_index = index;
                    break;
                }
                if (pending->info.kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN && pending->info.operation_id == 0)
                    receive_fin_index = index;
                if (pending->info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED)
                    terminal_status = pending->info.status != 0 ? pending->info.status : -EPIPE;
            }
            size_t event_index = readable_index != c->pending_count ? readable_index : receive_fin_index;
            if (event_index != c->pending_count) {
                pending_event pending = c->pending[event_index];
                c->pending[event_index] = c->pending[--c->pending_count];
                readable = pending.info.kind == TREVRPC_RPC_EVENT_STREAM_READABLE;
                receive_fin = pending.info.kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN;
                trevrpc_rpc_event_release(pending.event);
            }
        }
        pthread_mutex_unlock(&c->event_mutex);
        if (result)
            return result;
        if (readable) {
            drain_readable = true;
            continue;
        }
        if (receive_fin)
            return 0;
        if (terminal_status != 0)
            return terminal_status;

        struct pollfd descriptor = {(int)c->wake.native_handle, POLLIN, 0};
        result = poll(&descriptor, 1, 1000);
        if (result < 0 && errno != EINTR)
            return -errno;
    }
}

static int client_read_message(
    benchmark_client* c, trevrpc_rpc_stream_v1 s, int (*decode)(const trevrpc_rpc_receive*, void**), void** out);

static int decode_status_ok(const trevrpc_rpc_receive* receive, void** out) {
    trevrpc_rpc_receive_info_v1 info;
    (void)out;
    int result = trevrpc_rpc_receive_info_v1_init(&info, sizeof(info));
    if (!result)
        result = trevrpc_rpc_receive_get_info_v1(receive, &info);
    if (!result && (info.kind != TREVRPC_RPC_RECEIVE_STATUS || info.rpc_status != TREVRPC_RPC_STATUS_OK))
        result = -EPROTO;
    return result;
}

static int receive_status_ok(benchmark_client* c, trevrpc_rpc_stream_v1 s) {
    return client_read_message(c, s, decode_status_ok, NULL);
}
static int close_client_call(benchmark_client* c, trevrpc_rpc_call_v1 call, trevrpc_rpc_stream_v1 stream) {
    const char* failure_stage = NULL;
    uint64_t close_operation = next_client_operation(c);
    int e = trevrpc_rpc_call_close(c->runtime, call, close_operation, TREVRPC_RPC_CLOSE_FLAG_NONE, 0);
    bool already_closing = e == -EALREADY || e == -ESTALE || e == -EPIPE;
    if (already_closing)
        e = 0;
    else if (e != 0)
        return e;
    trevrpc_rpc_event_info_v1 terminal_info = {0};
    int r = client_wait(c, TREVRPC_RPC_EVENT_STREAM_CLOSED, 0, SUBJECT_STREAM, &stream, &terminal_info);
    if (!e && terminal_info.kind != TREVRPC_RPC_EVENT_STREAM_CLOSED) {
        e = r;
        failure_stage = "stream_terminal";
    }
    terminal_info = (trevrpc_rpc_event_info_v1){0};
    r = client_wait(
        c, TREVRPC_RPC_EVENT_CALL_CLOSED, already_closing ? 0 : close_operation, SUBJECT_CALL, &call, &terminal_info);
    if (!e && terminal_info.kind != TREVRPC_RPC_EVENT_CALL_CLOSED) {
        e = r;
        failure_stage = "call_terminal";
    }
    r = trevrpc_rpc_stream_release(c->runtime, stream);
    if (!e && r != 0) {
        e = r;
        failure_stage = "stream_release";
    }
    r = trevrpc_rpc_call_release(c->runtime, call);
    if (!e && r != 0) {
        e = r;
        failure_stage = "call_release";
    }
    if (e)
        fprintf(stderr,
            "client close %s failed: %s (%d)\n",
            failure_stage != NULL ? failure_stage : "call_close",
            rpc_error_string(e),
            e);
    return e;
}
static int client_read_message(
    benchmark_client* c, trevrpc_rpc_stream_v1 s, int (*decode)(const trevrpc_rpc_receive*, void**), void** out) {
    for (;;) {
        trevrpc_rpc_receive* receive = NULL;
        int result = trevrpc_rpc_stream_receive(c->runtime, s, &receive);
        if (result == 0) {
            result = decode(receive, out);
            trevrpc_rpc_receive_release(receive);
            return result;
        }
        if (result != -EAGAIN)
            return result;

        bool readable = false;
        int terminal_status = 0;
        pthread_mutex_lock(&c->event_mutex);
        result = client_collect_locked(c);
        if (!result) {
            size_t readable_index = c->pending_count;
            for (size_t index = 0; index < c->pending_count; ++index) {
                pending_event* pending = &c->pending[index];
                if (!stream_equal(pending->info.stream, s))
                    continue;
                if (pending->info.kind == TREVRPC_RPC_EVENT_STREAM_READABLE && pending->info.operation_id == 0) {
                    readable_index = index;
                    break;
                }
                if (pending->info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED)
                    terminal_status = pending->info.status != 0 ? pending->info.status : -EPIPE;
            }
            if (readable_index != c->pending_count) {
                pending_event pending = c->pending[readable_index];
                c->pending[readable_index] = c->pending[--c->pending_count];
                trevrpc_rpc_event_release(pending.event);
                readable = true;
            }
        }
        pthread_mutex_unlock(&c->event_mutex);
        if (result)
            return result;
        if (readable)
            continue;
        if (terminal_status != 0) {
            receive = NULL;
            result = trevrpc_rpc_stream_receive(c->runtime, s, &receive);
            if (result == 0) {
                result = decode(receive, out);
                trevrpc_rpc_receive_release(receive);
                return result;
            }
            return result == -EAGAIN ? terminal_status : result;
        }

        struct pollfd descriptor = {(int)c->wake.native_handle, POLLIN, 0};
        result = poll(&descriptor, 1, 1000);
        if (result < 0 && errno != EINTR)
            return -errno;
    }
}
static int decode_response(const trevrpc_rpc_receive* r, void** out) {
    return trevrpc_benchmark_v1_benchmark_service_unary_decode_response_receive(r, (BenchmarkResponse**)out);
}
static int decode_summary(const trevrpc_rpc_receive* r, void** out) {
    return trevrpc_benchmark_v1_benchmark_service_client_stream_decode_response_receive(r, (BenchmarkSummary**)out);
}
static int decode_server_response(const trevrpc_rpc_receive* r, void** out) {
    return trevrpc_benchmark_v1_benchmark_service_server_stream_decode_response_receive(r, (BenchmarkResponse**)out);
}
static int decode_bidi_response(const trevrpc_rpc_receive* r, void** out) {
    return trevrpc_benchmark_v1_benchmark_service_bidi_decode_response_receive(r, (BenchmarkResponse**)out);
}

static int run_unary(benchmark_client* c, const client_options* o, uint64_t seq) {
    const char* stage = "open";
    BenchmarkRequest q = TREVRPC__BENCHMARK__V1__BENCHMARK_REQUEST__INIT;
    q.sequence = seq;
    q.payload.len = o->request_bytes;
    q.payload.data = new_payload(o->request_bytes, 0);
    q.response_bytes = o->response_bytes;
    if (o->request_bytes && !q.payload.data)
        return -ENOMEM;
    trevrpc_rpc_call_v1 call = {0};
    trevrpc_rpc_stream_v1 s = {0};
    uint64_t op = next_client_operation(c);
    int e = trevrpc_benchmark_v1_benchmark_service_unary_open(c->runtime, c->endpoint, &q, op, &call, &s);
    free(q.payload.data);
    if (!e) {
        stage = "call_ready";
        e = client_wait_call(c, op);
    }
    BenchmarkResponse* r = NULL;
    if (!e) {
        stage = "response";
        e = client_read_message(c, s, decode_response, (void**)&r);
    }
    if (!e) {
        stage = "validate_response";
        e = validate_response(r, seq, o->response_bytes);
    }
    trevrpc__benchmark__v1__benchmark_response__free_unpacked(r, NULL);
    if (!e) {
        stage = "receive_fin";
        e = client_wait_receive_fin(c, s);
    }
    if (call.owner) {
        int x = close_client_call(c, call, s);
        if (!e) {
            stage = "close";
            e = x;
        }
    }
    if (e)
        fprintf(stderr, "unary %s failed: %s (%d)\n", stage, rpc_error_string(e), e);
    return e;
}
static int run_client_stream(benchmark_client* c, const client_options* o) {
    trevrpc_rpc_call_v1 call = {0};
    trevrpc_rpc_stream_v1 s = {0};
    uint64_t op = next_client_operation(c);
    const char* stage = "open";
    int e = trevrpc_benchmark_v1_benchmark_service_client_stream_open(c->runtime, c->endpoint, NULL, op, &call, &s);
    if (!e) {
        stage = "call_ready";
        e = client_wait_call(c, op);
    }
    uint8_t* p = NULL;
    if (!e) {
        p = new_payload(o->request_bytes, 0);
        if (o->request_bytes && !p)
            e = -ENOMEM;
    }
    for (uint64_t n = 0; !e && n < o->messages_per_stream; n++) {
        BenchmarkRequest q = TREVRPC__BENCHMARK__V1__BENCHMARK_REQUEST__INIT;
        q.sequence = n;
        q.payload.len = o->request_bytes;
        q.payload.data = p;
        q.response_bytes = o->response_bytes;
        uint64_t x = next_client_operation(c);
        stage = "send";
        e = trevrpc_benchmark_v1_benchmark_service_client_stream_send(c->runtime, s, &q, x);
        if (!e) {
            stage = "send_complete";
            e = client_wait_stream(c, TREVRPC_RPC_EVENT_SEND_COMPLETE, s, x);
        }
    }
    free(p);
    if (!e) {
        op = next_client_operation(c);
        stage = "finish_send";
        e = trevrpc_benchmark_v1_benchmark_service_client_stream_finish_send(c->runtime, s, op);
        if (!e) {
            stage = "send_finished";
            e = client_wait_stream(c, TREVRPC_RPC_EVENT_SEND_FINISHED, s, op);
        }
    }
    BenchmarkSummary* sum = NULL;
    if (!e) {
        stage = "response";
        e = client_read_message(c, s, decode_summary, (void**)&sum);
    }
    if (!e && (!sum || sum->message_count != o->messages_per_stream ||
                  sum->payload_bytes != (uint64_t)o->request_bytes * o->messages_per_stream)) {
        fprintf(stderr,
            "client_stream summary mismatch: messages=%" PRIu64 "/%" PRIu64 " payload=%" PRIu64 "/%" PRIu64 "\n",
            sum != NULL ? sum->message_count : 0,
            (uint64_t)o->messages_per_stream,
            sum != NULL ? sum->payload_bytes : 0,
            (uint64_t)o->request_bytes * o->messages_per_stream);
        e = -EPROTO;
    }
    trevrpc__benchmark__v1__benchmark_summary__free_unpacked(sum, NULL);
    if (!e) {
        stage = "status";
        e = receive_status_ok(c, s);
    }
    if (!e) {
        stage = "receive_fin";
        e = client_wait_receive_fin(c, s);
    }
    if (call.owner) {
        int x = close_client_call(c, call, s);
        if (!e)
            e = x;
    }
    if (e)
        fprintf(stderr, "client_stream %s failed: %s (%d)\n", stage, rpc_error_string(e), e);
    return e;
}
static int run_server_stream(benchmark_client* c, const client_options* o) {
    StreamRequest q = TREVRPC__BENCHMARK__V1__STREAM_REQUEST__INIT;
    q.message_count = o->messages_per_stream;
    q.payload.len = o->request_bytes;
    q.payload.data = new_payload(o->request_bytes, 0);
    q.response_bytes = o->response_bytes;
    if (o->request_bytes && !q.payload.data)
        return -ENOMEM;
    trevrpc_rpc_call_v1 call = {0};
    trevrpc_rpc_stream_v1 s = {0};
    uint64_t op = next_client_operation(c);
    int e = trevrpc_benchmark_v1_benchmark_service_server_stream_open(c->runtime, c->endpoint, &q, op, &call, &s);
    free(q.payload.data);
    if (!e)
        e = client_wait_call(c, op);
    for (uint64_t n = 0; !e && n < o->messages_per_stream; n++) {
        BenchmarkResponse* r = NULL;
        if (!e)
            e = client_read_message(c, s, decode_server_response, (void**)&r);
        if (!e)
            e = validate_response(r, n, o->response_bytes);
        trevrpc__benchmark__v1__benchmark_response__free_unpacked(r, NULL);
    }
    if (!e)
        e = receive_status_ok(c, s);
    if (!e)
        e = client_wait_receive_fin(c, s);
    if (call.owner) {
        int x = close_client_call(c, call, s);
        if (!e)
            e = x;
    }
    return e;
}
static int run_bidi(benchmark_client* c, const client_options* o) {
    trevrpc_rpc_call_v1 call = {0};
    trevrpc_rpc_stream_v1 s = {0};
    uint64_t op = next_client_operation(c);
    int e = trevrpc_benchmark_v1_benchmark_service_bidi_open(c->runtime, c->endpoint, NULL, op, &call, &s);
    if (!e)
        e = client_wait_call(c, op);
    uint8_t* p = NULL;
    if (!e) {
        p = new_payload(o->request_bytes, 0);
        if (o->request_bytes && !p)
            e = -ENOMEM;
    }
    for (uint64_t n = 0; !e && n < o->messages_per_stream; n++) {
        BenchmarkRequest q = TREVRPC__BENCHMARK__V1__BENCHMARK_REQUEST__INIT;
        q.sequence = n;
        q.payload.len = o->request_bytes;
        q.payload.data = p;
        q.response_bytes = o->response_bytes;
        op = next_client_operation(c);
        e = trevrpc_benchmark_v1_benchmark_service_bidi_send_request(c->runtime, s, &q, op);
        if (!e)
            e = client_wait_stream(c, TREVRPC_RPC_EVENT_SEND_COMPLETE, s, op);
    }
    free(p);
    if (!e) {
        op = next_client_operation(c);
        e = trevrpc_benchmark_v1_benchmark_service_bidi_finish_send(c->runtime, s, op);
        if (!e)
            e = client_wait_stream(c, TREVRPC_RPC_EVENT_SEND_FINISHED, s, op);
    }
    for (uint64_t n = 0; !e && n < o->messages_per_stream; n++) {
        BenchmarkResponse* r = NULL;
        if (!e)
            e = client_read_message(c, s, decode_bidi_response, (void**)&r);
        if (!e)
            e = validate_response(r, n, o->response_bytes);
        trevrpc__benchmark__v1__benchmark_response__free_unpacked(r, NULL);
    }
    if (!e)
        e = receive_status_ok(c, s);
    if (!e)
        e = client_wait_receive_fin(c, s);
    if (call.owner) {
        int x = close_client_call(c, call, s);
        if (!e)
            e = x;
    }
    return e;
}
static int run_native_operation(benchmark_client* c, const client_options* o, uint64_t n) {
    switch (o->rpc_kind) {
    case BENCHMARK_RPC_UNARY:
        return run_unary(c, o, n);
    case BENCHMARK_RPC_CLIENT_STREAM:
        return run_client_stream(c, o);
    case BENCHMARK_RPC_SERVER_STREAM:
        return run_server_stream(c, o);
    case BENCHMARK_RPC_BIDI:
        return run_bidi(c, o);
    }
    return -EINVAL;
}
static int run_operation(benchmark_client* c, const client_options* o, uint64_t n) {
    return run_native_operation(c, o, n);
}

static uint64_t next_server_operation(server_state* s) {
    uint64_t n = ++s->next_operation_id;
    return n ? n : ++s->next_operation_id;
}
static server_call* find_server_call(server_state* s, trevrpc_rpc_call_v1 c) {
    for (size_t i = 0; i < s->call_count; i++)
        if (call_equal(s->calls[i].call, c))
            return &s->calls[i];
    return NULL;
}
static int server_reserve_call(server_state* s) {
    if (s->call_count < s->call_capacity)
        return 0;
    size_t n = s->call_capacity ? s->call_capacity * 2 : 64;
    if (n < s->call_capacity || n > SIZE_MAX / sizeof(*s->calls))
        return -EOVERFLOW;
    server_call* p = realloc(s->calls, n * sizeof(*p));
    if (!p)
        return -ENOMEM;
    s->calls = p;
    s->call_capacity = n;
    return 0;
}
static void server_add_call(server_state* s, const server_call* c) {
    s->calls[s->call_count++] = *c;
}
static int server_status(trevrpc_rpc_status_v1* s, uint32_t code) {
    int e = trevrpc_rpc_status_v1_init(s, sizeof(*s));
    if (!e)
        s->code = code;
    return e;
}
static int server_append_response_sequence(server_call* c, uint64_t sequence) {
    if (c->messages == c->response_sequence_capacity) {
        size_t capacity = c->response_sequence_capacity ? c->response_sequence_capacity * 2 : 8;
        if (capacity < c->response_sequence_capacity || capacity > BENCHMARK_MAX_MESSAGES_PER_STREAM)
            capacity = BENCHMARK_MAX_MESSAGES_PER_STREAM;
        if (capacity <= c->messages)
            return -EOVERFLOW;
        uint64_t* sequences = realloc(c->response_sequences, capacity * sizeof(*sequences));
        if (!sequences)
            return -ENOMEM;
        c->response_sequences = sequences;
        c->response_sequence_capacity = capacity;
    }
    c->response_sequences[c->messages] = sequence;
    return 0;
}
static int server_send(server_state* s, server_call* c, uint64_t seq, uint32_t bytes) {
    BenchmarkResponse* r = new_response(seq, bytes);
    if (!r)
        return -ENOMEM;
    int e;
    if (c->kind == SERVER_CALL_BIDI)
        e = trevrpc_benchmark_v1_benchmark_service_bidi_send_response(
            s->runtime, c->stream, r, next_server_operation(s));
    else
        e = trevrpc_benchmark_v1_benchmark_service_server_stream_send(
            s->runtime, c->stream, r, next_server_operation(s));
    trevrpc__benchmark__v1__benchmark_response__free_unpacked(r, NULL);
    return e;
}
static int server_finish(server_state* s, server_call* c) {
    trevrpc_rpc_status_v1 st;
    int e = server_status(&st, TREVRPC_RPC_STATUS_OK);
    if (!e) {
        uint64_t op = next_server_operation(s);
        e = c->kind == SERVER_CALL_SERVER_STREAM
                ? trevrpc_benchmark_v1_benchmark_service_server_stream_finish(s->runtime, c->call, op, &st)
                : trevrpc_benchmark_v1_benchmark_service_bidi_finish(s->runtime, c->call, op, &st);
    }
    return e;
}
static int server_continue_responses(server_state* s, server_call* c) {
    uint64_t response_count = c->kind == SERVER_CALL_SERVER_STREAM ? c->response_count
                              : c->kind == SERVER_CALL_BIDI        ? c->messages
                                                                   : 0;
    int e;
    if (c->response_send_pending || c->finish_submitted)
        return 0;
    if (c->responses_submitted < response_count) {
        uint64_t sequence = c->responses_submitted;
        if (c->kind == SERVER_CALL_BIDI) {
            if (!c->response_sequences || c->responses_submitted >= c->messages)
                return -EIO;
            sequence = c->response_sequences[c->responses_submitted];
        }
        e = server_send(s, c, sequence, c->response_bytes);
        if (!e) {
            c->responses_submitted++;
            c->response_send_pending = true;
        }
        return e;
    }
    if (c->kind == SERVER_CALL_SERVER_STREAM || (c->kind == SERVER_CALL_BIDI && c->receive_finished)) {
        e = server_finish(s, c);
        if (!e)
            c->finish_submitted = true;
        return e;
    }
    return 0;
}
static int server_initial(server_state* s, server_call* c) {
    if (!c->initial)
        return (c->kind == SERVER_CALL_UNARY || c->kind == SERVER_CALL_SERVER_STREAM) ? -EPROTO : 0;
    int e = 0;
    if (c->kind == SERVER_CALL_UNARY) {
        BenchmarkRequest* q = NULL;
        e = trevrpc_benchmark_v1_benchmark_service_unary_decode_request_receive(c->initial, &q);
        if (!e && q && q->payload.len <= BENCHMARK_MAX_PAYLOAD_BYTES &&
            q->response_bytes <= BENCHMARK_MAX_PAYLOAD_BYTES) {
            BenchmarkResponse* r = new_response(q->sequence, q->response_bytes);
            if (!r)
                e = -ENOMEM;
            else {
                trevrpc_rpc_status_v1 st;
                e = server_status(&st, TREVRPC_RPC_STATUS_OK);
                if (!e)
                    e = trevrpc_benchmark_v1_benchmark_service_unary_respond(
                        s->runtime, c->call, r, &st, next_server_operation(s));
                trevrpc__benchmark__v1__benchmark_response__free_unpacked(r, NULL);
            }
        } else if (!e)
            e = -EINVAL;
        trevrpc__benchmark__v1__benchmark_request__free_unpacked(q, NULL);
    } else if (c->kind == SERVER_CALL_SERVER_STREAM) {
        StreamRequest* q = NULL;
        e = trevrpc_benchmark_v1_benchmark_service_server_stream_decode_request_receive(c->initial, &q);
        if (!e && q && q->message_count && q->message_count <= BENCHMARK_MAX_MESSAGES_PER_STREAM &&
            q->response_bytes <= BENCHMARK_MAX_PAYLOAD_BYTES) {
            c->response_count = q->message_count;
            c->response_bytes = q->response_bytes;
            e = server_continue_responses(s, c);
        } else if (!e)
            e = -EINVAL;
        trevrpc__benchmark__v1__stream_request__free_unpacked(q, NULL);
    }
    trevrpc_rpc_receive_release(c->initial);
    c->initial = NULL;
    return e;
}
static int server_message(server_state* s, server_call* c, trevrpc_rpc_receive* r) {
    BenchmarkRequest* q = NULL;
    int e;
    if (c->kind == SERVER_CALL_CLIENT_STREAM) {
        e = trevrpc_benchmark_v1_benchmark_service_client_stream_decode_request_receive(r, &q);
        if (!e && q && q->payload.len <= BENCHMARK_MAX_PAYLOAD_BYTES &&
            c->messages < BENCHMARK_MAX_MESSAGES_PER_STREAM) {
            c->messages++;
            c->payload_bytes = saturating_add_u64(c->payload_bytes, q->payload.len);
        } else if (!e)
            e = -EINVAL;
    } else {
        e = trevrpc_benchmark_v1_benchmark_service_bidi_decode_request_receive(r, &q);
        if (!e && q && q->payload.len <= BENCHMARK_MAX_PAYLOAD_BYTES &&
            q->response_bytes <= BENCHMARK_MAX_PAYLOAD_BYTES && c->messages < BENCHMARK_MAX_MESSAGES_PER_STREAM &&
            (c->messages == 0 || q->response_bytes == c->response_bytes)) {
            e = server_append_response_sequence(c, q->sequence);
            if (!e) {
                c->response_bytes = q->response_bytes;
                c->messages++;
                e = server_continue_responses(s, c);
            }
        } else if (!e)
            e = -EINVAL;
    }
    trevrpc__benchmark__v1__benchmark_request__free_unpacked(q, NULL);
    return e;
}
static int server_drain_messages(server_state* s, server_call* c) {
    for (;;) {
        trevrpc_rpc_receive* receive = NULL;
        int e = trevrpc_rpc_stream_receive(s->runtime, c->stream, &receive);
        if (e == -EAGAIN)
            return 0;
        if (!e)
            e = server_message(s, c, receive);
        trevrpc_rpc_receive_release(receive);
        if (e)
            return e;
    }
}
static void server_release_ready(server_state* s) {
    for (size_t i = 0; i < s->call_count;) {
        server_call* c = &s->calls[i];
        int result = 0;
        if (c->stream_closed && !c->stream_released) {
            result = trevrpc_rpc_stream_release(s->runtime, c->stream);
            if (result == 0)
                c->stream_released = true;
        }
        if (result == 0 && c->call_closed && !c->call_released) {
            result = trevrpc_rpc_call_release(s->runtime, c->call);
            if (result == 0)
                c->call_released = true;
        }
        if (result != 0) {
            if (s->event_error == 0)
                s->event_error = result;
            i++;
            continue;
        }
        if (c->stream_released && c->call_released) {
            size_t last;
            trevrpc_rpc_receive_release(c->initial);
            free(c->response_sequences);
            last = --s->call_count;
            if (i != last)
                s->calls[i] = s->calls[last];
            continue;
        }
        i++;
    }
}
static void server_event(server_state* s, trevrpc_rpc_event* e) {
    trevrpc_rpc_event_info_v1 i;
    if (trevrpc_rpc_event_info_v1_init(&i, sizeof(i)) || trevrpc_rpc_event_get_info_v1(e, &i)) {
        trevrpc_rpc_event_release(e);
        return;
    }
    if (i.kind == TREVRPC_RPC_EVENT_ENDPOINT_READY && i.operation_id == 1) {
        s->endpoint_ready = true;
    } else if (i.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED && endpoint_equal(i.endpoint, s->endpoint)) {
        s->endpoint_closed = true;
    } else if (i.kind == TREVRPC_RPC_EVENT_STOPPED) {
        s->runtime_stopped = true;
    }
    if (i.kind == TREVRPC_RPC_EVENT_CALL_INCOMING) {
        if (s->stopping) {
            trevrpc_rpc_event_release(e);
            return;
        }
        server_call c = {0};
        int r = server_reserve_call(s);
        if (r) {
            trevrpc_rpc_event_release(e);
            s->event_error = r;
            return;
        }
        if (trevrpc_benchmark_v1_benchmark_service_unary_matches_incoming(&i)) {
            c.kind = SERVER_CALL_UNARY;
            r = trevrpc_benchmark_v1_benchmark_service_unary_take_incoming(e, &c.call, &c.stream, &c.initial);
        } else if (trevrpc_benchmark_v1_benchmark_service_client_stream_matches_incoming(&i)) {
            c.kind = SERVER_CALL_CLIENT_STREAM;
            r = trevrpc_benchmark_v1_benchmark_service_client_stream_take_incoming(e, &c.call, &c.stream, &c.initial);
        } else if (trevrpc_benchmark_v1_benchmark_service_server_stream_matches_incoming(&i)) {
            c.kind = SERVER_CALL_SERVER_STREAM;
            r = trevrpc_benchmark_v1_benchmark_service_server_stream_take_incoming(e, &c.call, &c.stream, &c.initial);
        } else if (trevrpc_benchmark_v1_benchmark_service_bidi_matches_incoming(&i)) {
            c.kind = SERVER_CALL_BIDI;
            r = trevrpc_benchmark_v1_benchmark_service_bidi_take_incoming(e, &c.call, &c.stream, &c.initial);
        } else {
            trevrpc_rpc_event_release(e);
            return;
        }
        trevrpc_rpc_event_release(e);
        if (r) {
            s->event_error = r;
            return;
        }
        server_add_call(s, &c);
        server_call* stored = &s->calls[s->call_count - 1];
        r = trevrpc_rpc_call_accept(s->runtime, stored->call, next_server_operation(s));
        if (r) {
            trevrpc_rpc_receive_release(stored->initial);
            stored->initial = NULL;
            (void)trevrpc_rpc_call_close(s->runtime,
                stored->call,
                next_server_operation(s),
                TREVRPC_RPC_CLOSE_FLAG_ABORT,
                TREVRPC_RPC_STATUS_INTERNAL);
            s->event_error = r;
        }
        return;
    }
    server_call* c = find_server_call(s, i.call);
    if (i.kind == TREVRPC_RPC_EVENT_CALL_ACCEPTED && c) {
        int r = server_initial(s, c);
        if (r)
            trevrpc_rpc_call_cancel(s->runtime, c->call, next_server_operation(s), TREVRPC_RPC_STATUS_INTERNAL);
    } else if (i.kind == TREVRPC_RPC_EVENT_STREAM_READABLE && c) {
        int x = server_drain_messages(s, c);
        if (x)
            trevrpc_rpc_call_cancel(s->runtime, c->call, next_server_operation(s), TREVRPC_RPC_STATUS_INTERNAL);
    } else if (i.kind == TREVRPC_RPC_EVENT_SEND_COMPLETE && c &&
               (c->kind == SERVER_CALL_SERVER_STREAM || c->kind == SERVER_CALL_BIDI)) {
        c->response_send_pending = false;
        int x = server_continue_responses(s, c);
        if (x)
            trevrpc_rpc_call_cancel(s->runtime, c->call, next_server_operation(s), TREVRPC_RPC_STATUS_INTERNAL);
    } else if (i.kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN && c) {
        int x = server_drain_messages(s, c);
        if (!x && c->kind == SERVER_CALL_CLIENT_STREAM) {
            BenchmarkSummary* m = new_summary(c->messages, c->payload_bytes);
            trevrpc_rpc_status_v1 st;
            x = m ? server_status(&st, TREVRPC_RPC_STATUS_OK) : -ENOMEM;
            if (!x)
                x = trevrpc_benchmark_v1_benchmark_service_client_stream_respond(
                    s->runtime, c->call, m, &st, next_server_operation(s));
            trevrpc__benchmark__v1__benchmark_summary__free_unpacked(m, NULL);
        } else if (!x && c->kind == SERVER_CALL_BIDI) {
            c->receive_finished = true;
            x = server_continue_responses(s, c);
        }
        if (x)
            trevrpc_rpc_call_cancel(s->runtime, c->call, next_server_operation(s), TREVRPC_RPC_STATUS_INTERNAL);
    } else if (i.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED && c)
        c->stream_closed = true;
    else if (i.kind == TREVRPC_RPC_EVENT_CALL_CLOSED && c)
        c->call_closed = true;
    trevrpc_rpc_event_release(e);
    server_release_ready(s);
}
static int server_drain(server_state* s, bool* stopped) {
    for (;;) {
        trevrpc_rpc_event* e = NULL;
        int r = trevrpc_rpc_runtime_next_event(s->runtime, &e);
        if (r == -EAGAIN)
            return 0;
        if (r)
            return r;
        trevrpc_rpc_event_info_v1 i;
        trevrpc_rpc_event_info_v1_init(&i, sizeof(i));
        trevrpc_rpc_event_get_info_v1(e, &i);
        if (i.kind == TREVRPC_RPC_EVENT_STOPPED)
            *stopped = true;
        server_event(s, e);
        if (s->event_error) {
            r = s->event_error;
            s->event_error = 0;
            return r;
        }
    }
}

static int init_rpc_runtime(trevrpc_rpc_runtime** out, trevrpc_rpc_wake_source_v1* w) {
    trevrpc_rpc_runtime_config_v1 c;
    trevrpc_rpc_msquic_config_v1 p;
    int e = trevrpc_rpc_runtime_config_v1_init(&c, sizeof(c));
    if (!e)
        e = trevrpc_rpc_msquic_config_v1_init(&p, sizeof(p));
    if (!e) {
        c.event_capacity = TREVRPC_RPC_MAX_EVENT_CAPACITY;
        c.endpoint_capacity = 4;
        c.call_capacity = BENCHMARK_SERVER_REQUESTS;
        c.stream_capacity = BENCHMARK_SERVER_REQUESTS;
        c.max_receive_owned_count = BENCHMARK_SERVER_REQUESTS;
        c.max_receive_owned_bytes = BENCHMARK_MAX_FRAME_SIZE;
        c.max_message_size = BENCHMARK_MAX_FRAME_SIZE;
        e = trevrpc_rpc_msquic_create_v1(&c, &p, out);
    }
    if (!e)
        e = trevrpc_rpc_wake_source_v1_init(w, sizeof(*w));
    if (!e)
        e = trevrpc_rpc_runtime_get_wake_source_v1(*out, w);
    return e;
}

static int run_operation(benchmark_client* client, const client_options* options, uint64_t sequence);

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
        int err = run_operation(phase->client, phase->options, sequence);
        uint64_t operation_end = monotonic_nanos();
        if (err != 0) {
            lane->result.failed++;
            fprintf(stderr,
                "lane %zu %s operation failed: %s (%d)\n",
                lane->lane_index,
                phase->options->rpc_name,
                rpc_error_string(err),
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

static int server_poll(server_state* s, int timeout_ms, bool* stopped) {
    struct pollfd p[2] = {{(int)s->wake.native_handle, POLLIN, 0}, {STDIN_FILENO, POLLIN, 0}};
    int r = poll(p, 2, timeout_ms);
    if (r < 0)
        return errno == EINTR ? 0 : -errno;
    if (r && (p[1].revents & POLLIN))
        return 1;
    if (r && (p[0].revents & (POLLERR | POLLHUP | POLLNVAL)))
        return -EIO;
    return server_drain(s, stopped);
}
static void server_signal_handler(int n) {
    (void)n;
    server_stop_requested = 1;
}

static int server_shutdown_poll(server_state* s, uint64_t deadline_ns) {
    uint64_t now = monotonic_nanos();
    if (now == 0)
        return -EIO;
    if (now >= deadline_ns)
        return -ETIMEDOUT;
    uint64_t remaining_ns = deadline_ns - now;
    int timeout_ms = (int)((remaining_ns + UINT64_C(999999)) / UINT64_C(1000000));
    if (timeout_ms > 100)
        timeout_ms = 100;
    struct pollfd descriptor = {(int)s->wake.native_handle, POLLIN, 0};
    int result = poll(&descriptor, 1, timeout_ms);
    if (result < 0)
        return errno == EINTR ? 0 : -errno;
    if (result != 0 && (descriptor.revents & (POLLERR | POLLHUP | POLLNVAL)))
        return -EIO;
    bool stopped = s->runtime_stopped;
    return server_drain(s, &stopped);
}

static int server_close_calls(server_state* s) {
    for (size_t i = 0; i < s->call_count; i++) {
        server_call* c = &s->calls[i];
        trevrpc_rpc_receive_release(c->initial);
        c->initial = NULL;
        if (c->close_requested || c->call_closed)
            continue;
        int result = trevrpc_rpc_call_close(
            s->runtime, c->call, next_server_operation(s), TREVRPC_RPC_CLOSE_FLAG_ABORT, TREVRPC_RPC_STATUS_CANCELLED);
        if (result == 0 || result == -EALREADY || result == -EPIPE) {
            c->close_requested = true;
            continue;
        }
        return result;
    }
    return 0;
}

static int server_shutdown(server_state* s) {
    uint64_t now = monotonic_nanos();
    if (now == 0 || BENCHMARK_GRACEFUL_SHUTDOWN_NS <= BENCHMARK_SHUTDOWN_REPORT_MARGIN_NS)
        return -EIO;
    uint64_t deadline_ns =
        saturating_add_u64(now, BENCHMARK_GRACEFUL_SHUTDOWN_NS - BENCHMARK_SHUTDOWN_REPORT_MARGIN_NS);
    s->stopping = true;

    int result = server_close_calls(s);
    while (result == 0 && s->call_count != 0) {
        result = server_shutdown_poll(s, deadline_ns);
        if (result == 0)
            result = server_close_calls(s);
    }

    if (result == 0 && s->endpoint.owner != 0 && !s->endpoint_closed) {
        int close_result = trevrpc_rpc_endpoint_close(s->runtime, s->endpoint, next_server_operation(s));
        if (close_result != 0 && close_result != -EALREADY)
            result = close_result;
    }
    while (result == 0 && s->endpoint.owner != 0 && !s->endpoint_closed)
        result = server_shutdown_poll(s, deadline_ns);
    if (result == 0 && s->endpoint.owner != 0) {
        result = trevrpc_rpc_endpoint_release(s->runtime, s->endpoint);
        if (result == 0)
            s->endpoint = (trevrpc_rpc_endpoint_v1){0};
    }

    if (result == 0) {
        int close_result = trevrpc_rpc_runtime_close(s->runtime, next_server_operation(s));
        if (close_result != 0 && close_result != -EALREADY)
            result = close_result;
    }
    while (result == 0 && !s->runtime_stopped)
        result = server_shutdown_poll(s, deadline_ns);
    if (result == 0)
        result = trevrpc_rpc_runtime_drain(s->runtime);
    if (result == 0) {
        result = trevrpc_rpc_runtime_release(s->runtime);
        if (result == 0)
            s->runtime = NULL;
    }
    return result;
}

static int run_server(int argc, char** argv) {
    char pe[256];
    server_options o;
    int e = parse_server_options(argc, argv, &o, pe, sizeof(pe));
    if (e) {
        free(o.host);
        return fail_with_error("config", e == -EOPNOTSUPP ? "unsupported" : "invalid_argument", "%s", pe);
    }
    server_state s = {0};
    e = init_rpc_runtime(&s.runtime, &s.wake);
    trevrpc_rpc_msquic_endpoint_config_v1 c;
    if (!e)
        e = trevrpc_rpc_msquic_endpoint_config_v1_init(&c, sizeof(c));
    if (!e) {
        c.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_LISTENER;
        c.transport = transport_for_stack(o.stack);
        c.host = o.host;
        c.host_len = (uint32_t)strlen(o.host);
        c.port = o.port;
        c.peer_bidi_stream_count = BENCHMARK_SERVER_STREAMS;
        c.cert_file = o.cert;
        c.cert_file_len = (uint32_t)strlen(o.cert);
        c.key_file = o.key;
        c.key_file_len = (uint32_t)strlen(o.key);
        if (o.stack == BENCHMARK_STACK_TREVRPC_WEBTRANSPORT) {
            c.path = BENCHMARK_WEBTRANSPORT_PATH;
            c.path_len = (uint32_t)strlen(BENCHMARK_WEBTRANSPORT_PATH);
            c.origin = o.webtransport_origin;
            c.origin_len = (uint32_t)strlen(o.webtransport_origin);
        } else if (o.stack == BENCHMARK_STACK_TREVRPC_HTTP3) {
            c.path = BENCHMARK_HTTP3_PATH;
            c.path_len = (uint32_t)strlen(BENCHMARK_HTTP3_PATH);
        }
        c.max_frame_size = BENCHMARK_MAX_FRAME_SIZE;
        c.max_pending_receive_bytes = (uint64_t)BENCHMARK_MAX_FRAME_SIZE * 2u + 4096u;
        c.max_idle_timeout_ms = BENCHMARK_IDLE_TIMEOUT_MS;
        c.keep_alive_ms = BENCHMARK_KEEP_ALIVE_MS;
        e = trevrpc_rpc_msquic_endpoint_start_v1(s.runtime, &c, 1, &s.endpoint);
    }
    uint16_t port = 0;
    bool stopped = false;
    while (!e && !s.endpoint_ready && !stopped) {
        e = server_drain(&s, &stopped);
        if (!e && !s.endpoint_ready && !stopped) {
            struct pollfd f = {(int)s.wake.native_handle, POLLIN, 0};
            if (poll(&f, 1, 1000) < 0 && errno != EINTR)
                e = -errno;
        }
    }
    if (!e && !s.endpoint_ready)
        e = -EIO;
    if (!e)
        e = trevrpc_rpc_endpoint_get_port_v1(s.runtime, s.endpoint, &port);
    if (!e)
        e = emit_ready(o.host, port, o.stack_name);
    struct sigaction a = {0};
    a.sa_handler = server_signal_handler;
    sigemptyset(&a.sa_mask);
    sigaction(SIGINT, &a, NULL);
    sigaction(SIGTERM, &a, NULL);
    bool graceful = false;
    char cmd[32];
    while (!e && !server_stop_requested) {
        int r = server_poll(&s, 100, &stopped);
        if (r == 1) {
            e = read_control_command(cmd, sizeof(cmd));
            if (!e && strcmp(cmd, "SHUTDOWN"))
                e = -EINVAL;
            graceful = e == 0;
            break;
        }
        if (r < 0) {
            e = r;
            break;
        }
    }
    if (s.runtime) {
        int shutdown_result = server_shutdown(&s);
        if (!e)
            e = shutdown_result;
    }
    for (size_t i = 0; i < s.call_count; i++) {
        trevrpc_rpc_receive_release(s.calls[i].initial);
        free(s.calls[i].response_sequences);
    }
    free(s.calls);
    free(o.host);
    if (e)
        return fail_with_error("serve", "control_failed", "%s (%d)", rpc_error_string(e), e);
    if (graceful && emit_stopped(o.stack_name))
        return 1;
    return 0;
}
static void benchmark_client_close(benchmark_client* c) {
    if (!c->runtime)
        return;
    (void)trevrpc_rpc_runtime_close(c->runtime, next_client_operation(c));
    bool stopped = false;
    for (int i = 0; !stopped && i < 200; i++) {
        (void)poll(&(struct pollfd){(int)c->wake.native_handle, POLLIN, 0}, 1, 100);
        pthread_mutex_lock(&c->event_mutex);
        (void)client_collect_locked(c);
        for (size_t j = 0; j < c->pending_count; j++) {
            if (c->pending[j].info.kind == TREVRPC_RPC_EVENT_STOPPED)
                stopped = true;
            trevrpc_rpc_event_release(c->pending[j].event);
        }
        c->pending_count = 0;
        pthread_mutex_unlock(&c->event_mutex);
    }
    (void)trevrpc_rpc_endpoint_release(c->runtime, c->endpoint);
    (void)trevrpc_rpc_runtime_drain(c->runtime);
    (void)trevrpc_rpc_runtime_release(c->runtime);
    free(c->pending);
    pthread_mutex_destroy(&c->event_mutex);
    memset(c, 0, sizeof(*c));
}
static int validate_client(benchmark_client* c, const client_options* o) {
    return run_operation(c, o, 0);
}
static int run_client(int argc, char** argv) {
    char pe[256];
    client_options o;
    int e = parse_client_options(argc, argv, &o, pe, sizeof(pe));
    if (e) {
        free(o.host);
        return fail_with_error("config", "invalid_argument", "%s", pe);
    }
    if (o.stack == BENCHMARK_STACK_TREVRPC_WEBTRANSPORT) {
        char command[512];
        e = emit_prepared(BENCHMARK_WEBTRANSPORT_ORIGIN);
        if (!e)
            e = read_control_command(command, sizeof(command));
        if (!e && strncmp(command, "CONNECT ", 8) != 0)
            e = -EINVAL;
        if (!e)
            e = split_address(command + 8, false, &o.host, &o.port);
        if (e) {
            free(o.host);
            return fail_with_error("connect", "connect_failed", "expected CONNECT HOST:PORT (%d)", e);
        }
    }
    benchmark_client c = {0};
    trevrpc_rpc_event_info_v1 endpoint_event = {0};
    if (pthread_mutex_init(&c.event_mutex, NULL)) {
        free(o.host);
        return fail_with_error("connect", "connect_failed", "mutex initialization failed");
    }
    e = init_rpc_runtime(&c.runtime, &c.wake);
    trevrpc_rpc_msquic_endpoint_config_v1 q;
    if (!e)
        e = trevrpc_rpc_msquic_endpoint_config_v1_init(&q, sizeof(q));
    if (!e) {
        q.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_CLIENT;
        q.transport = transport_for_stack(o.stack);
        q.host = o.host;
        q.host_len = (uint32_t)strlen(o.host);
        q.port = o.port;
        q.peer_bidi_stream_count = (uint16_t)(o.concurrency > UINT16_MAX ? UINT16_MAX : o.concurrency);
        q.ca_cert_file = o.cert;
        q.ca_cert_file_len = (uint32_t)strlen(o.cert);
        q.flags = TREVRPC_RPC_MSQUIC_VERIFY_PEER;
        if (o.stack == BENCHMARK_STACK_TREVRPC_WEBTRANSPORT) {
            q.path = BENCHMARK_WEBTRANSPORT_PATH;
            q.path_len = (uint32_t)strlen(BENCHMARK_WEBTRANSPORT_PATH);
            q.origin = BENCHMARK_WEBTRANSPORT_ORIGIN;
            q.origin_len = (uint32_t)strlen(BENCHMARK_WEBTRANSPORT_ORIGIN);
        }
        q.max_frame_size = BENCHMARK_MAX_FRAME_SIZE;
        q.max_pending_receive_bytes = (uint64_t)BENCHMARK_MAX_FRAME_SIZE * 2u + 4096u;
        q.max_idle_timeout_ms = BENCHMARK_IDLE_TIMEOUT_MS;
        q.keep_alive_ms = BENCHMARK_KEEP_ALIVE_MS;
        e = trevrpc_rpc_msquic_endpoint_start_v1(c.runtime, &q, 1, &c.endpoint);
    }
    if (!e)
        e = client_wait_ready(&c, &endpoint_event);
    if (e) {
        if (c.runtime)
            benchmark_client_close(&c);
        free(o.host);
        return fail_with_error("connect",
            "connect_failed",
            "%s (%d), endpoint_event_kind=%u, event_status=%d, application_error_code=%" PRIu64
            ", provider_error_code=%" PRIu64,
            rpc_error_string(e),
            e,
            endpoint_event.kind,
            endpoint_event.status,
            endpoint_event.application_error_code,
            endpoint_event.provider_error_code);
    }
    e = validate_client(&c, &o);
    if (!e)
        e = run_warmup(&c, &o);
    if (e) {
        benchmark_client_close(&c);
        free(o.host);
        return fail_with_error("warmup", "rpc_failed", "%s (%d)", rpc_error_string(e), e);
    }
    phase_control ph;
    e = phase_prepare(&ph, &c, &o, true);
    if (e) {
        benchmark_client_close(&c);
        free(o.host);
        return fail_with_error("arm", "lane_setup_failed", "%s (%d)", rpc_error_string(e), e);
    }
    if (emit_armed(o.stack_name)) {
        phase_abort(&ph);
        lane_result d;
        (void)phase_join(&ph, &d, NULL);
        histogram_reset(&d.latency);
        benchmark_client_close(&c);
        free(o.host);
        return 1;
    }
    char cmd[32];
    e = read_control_command(cmd, sizeof(cmd));
    if (!e && strcmp(cmd, "START"))
        e = -EINVAL;
    if (e) {
        phase_abort(&ph);
        lane_result d;
        (void)phase_join(&ph, &d, NULL);
        histogram_reset(&d.latency);
        benchmark_client_close(&c);
        free(o.host);
        return fail_with_error("control", "invalid_command", "expected START");
    }
    e = phase_start(&ph, o.measurement_ns);
    lane_result r;
    uint64_t elapsed = 0;
    int je = phase_join(&ph, &r, &elapsed);
    benchmark_client_close(&c);
    free(o.host);
    if (!e)
        e = je;
    if (e) {
        histogram_reset(&r.latency);
        return fail_with_error("measure", "internal_error", "%s (%d)", rpc_error_string(e), e);
    }
    uint64_t hc = 0;
    for (size_t i = 0; i < r.latency.count; i++)
        if (checked_add_u64(&hc, r.latency.buckets[i].count)) {
            histogram_reset(&r.latency);
            return fail_with_error("measure", "count_overflow", "histogram count overflow");
        }
    if (hc != r.completed) {
        histogram_reset(&r.latency);
        return fail_with_error("measure", "count_mismatch", "histogram count does not match completed operations");
    }
    int x = emit_sample(&o, elapsed, &r);
    histogram_reset(&r.latency);
    return x ? 1 : 0;
}
static void print_usage(const char* p) {
    fprintf(stderr,
        "usage: %s capabilities | server --stack STACK --listen HOST:PORT --cert FILE --key FILE "
        "[--webtransport-origin ORIGIN] | client --stack STACK [options]\n",
        p);
}
int main(int argc, char** argv) {
    if (argc >= 2 && !strcmp(argv[1], "capabilities")) {
        if (argc != 2)
            return fail_with_error("config", "invalid_argument", "capabilities takes no options");
        return emit_capabilities() ? 1 : 0;
    }
    if (argc >= 2 && !strcmp(argv[1], "server"))
        return run_server(argc, argv);
    if (argc >= 2 && !strcmp(argv[1], "client"))
        return run_client(argc, argv);
    print_usage(argv[0]);
    return fail_with_error("config", "invalid_command", "expected capabilities, server, or client");
}
