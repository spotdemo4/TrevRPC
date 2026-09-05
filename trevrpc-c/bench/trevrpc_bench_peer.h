#ifndef TREVRPC_BENCH_PEER_H
#define TREVRPC_BENCH_PEER_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define BENCHMARK_IDLE_TIMEOUT_MS 600000u
#define BENCHMARK_KEEP_ALIVE_MS 5000u
#define BENCHMARK_MAX_CONCURRENCY 1024u
#define BENCHMARK_MAX_PAYLOAD_BYTES (64u * 1024u * 1024u)
#define BENCHMARK_MAX_MESSAGES_PER_STREAM 1000000u
#define BENCHMARK_MAX_FRAME_SIZE (128u * 1024u * 1024u)
#define BENCHMARK_SERVER_WORKERS 128
#define BENCHMARK_SERVER_STREAMS 1024
#define BENCHMARK_SERVER_REQUESTS 4096
#define BENCHMARK_GRACEFUL_SHUTDOWN_NS 5000000000ull

typedef enum benchmark_stack {
    BENCHMARK_STACK_TREVRPC_NATIVE_QUIC,
    BENCHMARK_STACK_TREVRPC_HTTP3,
    BENCHMARK_STACK_TREVRPC_WEBTRANSPORT,
} benchmark_stack;

typedef enum benchmark_rpc_kind {
    BENCHMARK_RPC_UNARY,
    BENCHMARK_RPC_CLIENT_STREAM,
    BENCHMARK_RPC_SERVER_STREAM,
    BENCHMARK_RPC_BIDI,
} benchmark_rpc_kind;

typedef struct client_options {
    char* host;
    uint16_t port;
    const char* cert;
    benchmark_stack stack;
    const char* stack_name;
    benchmark_rpc_kind rpc_kind;
    const char* rpc_name;
    size_t concurrency;
    uint64_t warmup_ns;
    uint64_t measurement_ns;
    uint32_t request_bytes;
    uint32_t response_bytes;
    uint32_t messages_per_stream;
} client_options;

typedef struct server_options {
    char* host;
    uint16_t port;
    const char* cert;
    const char* key;
    const char* webtransport_origin;
    benchmark_stack stack;
    const char* stack_name;
    size_t workers;
} server_options;

#endif
