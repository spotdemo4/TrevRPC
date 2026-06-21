# TrevRPC C Runtime

`trevrpc-c` provides the C wire/value runtime, transport wrappers, and the high-level RPC client/server runtime.

## Libraries

The CMake build creates these static libraries by default:

| Library                | Purpose                                                           | Public header            |
| ---------------------- | ----------------------------------------------------------------- | ------------------------ |
| `trevrpc_core`         | Wire encoding/decoding and owned value helpers                    | `trevrpc.h`              |
| `trevrpc_msquic`       | Low-level MsQuic listener, connection, and stream wrappers        | `trevrpc_msquic.h`       |
| `trevrpc_webtransport` | Native WebTransport listener, session, and stream wrappers        | `trevrpc_webtransport.h` |
| `trevrpc`              | High-level RPC client/server runtime over MsQuic and WebTransport | `trevrpc.h`              |

`trevrpc.h` is the stable high-level API surface. `trevrpc_msquic.h` and `trevrpc_webtransport.h` expose transport-specific advanced APIs. High-level clients can use MsQuic with `trevrpc_client_connect` or WebTransport with `trevrpc_client_connect_webtransport`; high-level servers can listen on either or both transports.

The supported C artifacts are static libraries. Shared library builds and symbol export annotations are intentionally deferred until the C ABI policy is finalized; do not rely on default compiler symbol visibility as a stable ABI contract.

## ABI Policy

The public C API is source-compatible across patch releases for declarations in installed public headers. Breaking public header changes require a minor-version bump while TrevRPC remains pre-1.0; after 1.0 they require a major-version bump. Structs that are exposed by value may grow only in a minor-version bump and only when callers construct them through documented initializers such as `trevrpc_default_config` or zero-initialization where explicitly allowed. Numeric constants, status codes, and enum-compatible macro values are append-only once documented.

Status, RPC kind, stream-frame kind, transport kind, transport event, and log-level values intentionally remain `uint32_t` macro constants in public structs and function parameters. This keeps the ABI predictable across C compilers, preserves wire-width clarity, and avoids enum underlying-size differences. Helper validators such as `trevrpc_status_code_from_uint32` normalize untrusted numeric input at API boundaries.

`trevrpc_msquic.h` and `trevrpc_webtransport.h` remain public advanced transport headers. They are versioned with the rest of the C package, but only `trevrpc.h` is the stable high-level RPC API. High-level transport support uses explicit transport constructors instead of hiding transport choice behind the existing MsQuic-specific constructors.

Current close/shutdown naming is intentional: `close` releases caller-owned objects, while server `shutdown` asks serving loops and active work to stop before final close. `trevrpc_stream_close` releases the stream handle and aborts further local use; callers that need an orderly streaming half-close must call `trevrpc_stream_finish_send` first.

## Build

```sh
cmake -S trevrpc-c -B build -DTREVRPC_BUILD_TESTS=ON
cmake --build build
ctest --test-dir build --output-on-failure
cmake --install build --prefix /usr/local
```

Installed CMake packages expose imported targets with the `trevrpc::` namespace:

```cmake
find_package(trevrpc CONFIG REQUIRED)
target_link_libraries(app PRIVATE trevrpc::trevrpc)
```

The install also includes pkg-config files for each installed library:

```sh
pkg-config --cflags --libs trevrpc
pkg-config --cflags --libs trevrpc_core trevrpc_msquic trevrpc_webtransport
```

The same build is exercised by Nix:

```sh
nix build .#trevrpc-c
nix flake check
```

CMake options:

| Option                             | Default | Meaning                                                                              |
| ---------------------------------- | ------- | ------------------------------------------------------------------------------------ |
| `TREVRPC_BUILD_TESTS`              | `ON`    | Build CTest executables.                                                             |
| `TREVRPC_BUILD_BENCHMARKS`         | `OFF`   | Build opt-in benchmark executables.                                                  |
| `TREVRPC_BUILD_MSQUIC`             | `ON`    | Build the low-level MsQuic transport library.                                        |
| `TREVRPC_BUILD_WEBTRANSPORT`       | `ON`    | Build the low-level WebTransport transport library.                                  |
| `TREVRPC_BUILD_RUNTIME`            | `ON`    | Build the high-level runtime. This currently requires MsQuic and WebTransport.       |
| `TREVRPC_INSTALL_INTERNAL_HEADERS` | `OFF`   | Install internal headers under `include/trevrpc/internal` for tests and development. |
| `TREVRPC_ENABLE_SANITIZERS`        | `OFF`   | Build C targets with ASan and UBSan when using Clang or GCC.                         |

The Nix `checks.x86_64-linux.c-sanitizers` check runs every CTest executable under ASan/UBSan. On Linux this includes LeakSanitizer coverage from ASan, so CTest leaks fail the sanitizer check.

Opt-in benchmark targets are available when configuring with `-DTREVRPC_BUILD_BENCHMARKS=ON`:

```sh
./build/trevrpc_wire_bench 100000
./build/trevrpc_runtime_bench 10000
./build/trevrpc_rpc_comparison_bench 1000
```

## Allocation Notes

The current C runtime keeps ownership explicit and mostly allocates at API boundaries:

| Path                                      | Expected dynamic allocations                                                                             |
| ----------------------------------------- | -------------------------------------------------------------------------------------------------------- |
| Wire encode request/response/stream frame | One contiguous frame buffer per encode.                                                                  |
| Wire decode request                       | Metadata/message/body copies only when those fields are present; request storage itself is caller-owned. |
| Wire decode response/stream frame         | One result object plus metadata/message/body copies when present.                                        |
| High-level unary dispatch                 | One request decode path, one response encode path, and handler-owned response body copies.               |
| High-level streaming dispatch             | One stream wrapper per accepted stream plus one frame allocation per received stream frame.              |
| MsQuic send path                          | Frame buffers are reused through a bounded per-stream send pool when ownership is unambiguous.           |

Additional pooling is intentionally limited to the MsQuic send pool for now. Reusing decode/request buffers would couple ownership to callback lifetimes and concurrency; add pools only where lifetime and thread ownership are explicit.

Zero-copy decode is intentionally not exposed in the public API yet. Current decode helpers return owned request, response, metadata, and stream-frame values so handlers can safely retain data for the duration of a callback without depending on transport buffer lifetime. A future zero-copy path should be a separate API with explicit borrowed lifetimes, not a silent behavior change to existing decode helpers.

Send batching is currently transport-local. The low-level MsQuic wrapper has `trevrpc_msquic_stream_write_message_frames` for callers that already own a batch and can provide stable buffers. The high-level runtime still sends one framed message per API call to keep ordering, error reporting, and stream-status behavior predictable across MsQuic and WebTransport. Add high-level batching only after both transports expose equivalent completion semantics.

## Ownership

All setters that accept message/body/metadata bytes copy their inputs. Callers may release or mutate their input buffers immediately after a successful setter call.

| API                              | Ownership rule                                                                                                        |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `trevrpc_client_connect`         | On success, `*client` is owned by the caller and must be closed with `trevrpc_client_close`.                          |
| `trevrpc_client_call_unary`      | On success, `*response` is owned by the caller and must be freed with `trevrpc_response_free`.                        |
| `trevrpc_client_start_stream`    | On success, `*stream` is owned by the caller and must be closed with `trevrpc_stream_close`.                          |
| `trevrpc_server_listen`          | On success, `*server` is owned by the caller and must be closed with `trevrpc_server_close`.                          |
| Handler `trevrpc_call_context*`  | Borrowed for the duration of the handler call. Poll it for deadlines/cancellation; do not retain it after return.     |
| Handler `const trevrpc_request*` | Borrowed for the duration of the handler call. Do not free fields or retain field pointers after the handler returns. |
| Handler `trevrpc_response*`      | Borrowed output. The handler may fill it with response helpers, but must not free it.                                 |
| `trevrpc_stream_recv`            | On success with a non-null frame, `*frame` is owned by the caller and must be freed with `trevrpc_stream_frame_free`. |
| `trevrpc_response_reset`         | Releases message, body, and metadata owned by the response, then returns it to an empty OK state.                     |
| `trevrpc_stream_frame_reset`     | Releases message, body, and metadata owned by the frame, then returns it to an empty message-frame state.             |
| `trevrpc_metadata_set`           | Copies the key and value into the metadata list. `trevrpc_metadata_reset` releases all entries.                       |
| `trevrpc_msquic_*_read_frame`    | Returned frame bodies are owned by the caller and must be released with `trevrpc_msquic_free`.                        |
| `trevrpc_wt_*_read_frame`        | Returned frame bodies are owned by the caller and must be released with `trevrpc_wt_free`.                            |

`trevrpc_response_free(NULL)`, `trevrpc_stream_frame_free(NULL)`, `trevrpc_metadata_reset(NULL)`, and `trevrpc_request_reset(NULL)` are allowed no-ops.

## Blocking And Concurrency

The C runtime is synchronous. Functions that perform network I/O can block until data arrives, peer state changes, a transport error occurs, or another thread shuts down/closes the object.

Blocking APIs include `trevrpc_client_connect`, `trevrpc_client_call_unary`, `trevrpc_client_start_stream`, `trevrpc_stream_send_message`, `trevrpc_stream_send_status`, `trevrpc_stream_recv`, `trevrpc_stream_finish_send`, `trevrpc_server_serve`, low-level listener/session/connection accept calls, and low-level stream read/write calls.

`trevrpc_server_shutdown` may be called from another thread while `trevrpc_server_serve` is running. It stops listener acceptance and asks active connections to shut down. `trevrpc_server_close` should be called after serving has stopped and no other thread is using the server.

To serve native QUIC and WebTransport from one handler registry on one UDP port, create a shared listener. The server routes accepted connections by negotiated ALPN: `trevrpc/1` for native TrevRPC and `h3` for WebTransport.

```c
trevrpc_server* server = NULL;
trevrpc_server_listen("127.0.0.1", 5000, &wt_config, &config, &server);
trevrpc_server_register_unary(server, "svc", "method", handler, NULL);
trevrpc_server_serve(server);
```

Public C APIs are not POSIX thread-cancellation-safe and are not async-signal-safe. Do not use `pthread_cancel` to abort runtime calls. Prefer object-specific shutdown or close APIs from another thread.

Concurrent access guarantees are conservative:

| Object             | Concurrent use                                                                                                                                                                   |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `trevrpc_client`   | Protect concurrent calls and close with caller synchronization.                                                                                                                  |
| `trevrpc_server`   | Registration is expected before serving. `trevrpc_server_shutdown` is safe to call while serving.                                                                                |
| `trevrpc_stream`   | Do not call high-level stream APIs concurrently on the same stream without caller synchronization.                                                                               |
| `trevrpc_msquic_*` | Low-level wrappers use internal mutexes for transport callbacks, but callers should still serialize operations on the same object unless the API explicitly documents otherwise. |
| `trevrpc_wt_*`     | Same as MsQuic wrappers. Serialize operations on the same object unless documented otherwise.                                                                                    |

## Handler Safety

C handlers run in-process. A handler crash, invalid memory access, abort, or uncaught signal is a process crash. The runtime does not sandbox handlers and cannot recover from C undefined behavior. If a service needs crash isolation, run handlers in a separate process and communicate over an IPC boundary.

Handlers should return `0` after filling a successful response or streaming status. Non-zero handler returns are converted to internal-error responses by the current high-level runtime.

Request `timeout_nanos` is enforced cooperatively. The runtime rejects timeouts larger than `INT64_MAX`, turns already-expired or post-handler deadlines into `DEADLINE_EXCEEDED`, and passes a `trevrpc_call_context` into every handler. Long-running handlers should poll `trevrpc_call_context_cancelled`, `trevrpc_call_context_deadline_expired`, or `trevrpc_call_context_time_remaining_nanos`; the runtime does not preempt C handler code.

High-level stream send/receive calls return `-ETIMEDOUT` after the request deadline expires and `-ECANCELED` after server shutdown cancellation is observed. Terminal stream statuses are still emitted by the server runtime when a handler exits without sending one.

## Error Boundaries

`TREVRPC_ERR_FRAME_TOO_LARGE` is the transport-agnostic wire/runtime error used when TrevRPC framing exceeds the configured maximum before bytes are handed to a transport. Transport wrappers keep their own frame-too-large error codes, such as `TREV_MSQUIC_ERR_FRAME_TOO_LARGE` and `TREV_WT_ERR_FRAME_TOO_LARGE`. The high-level runtime normalizes all of these to `TREVRPC_STATUS_RESOURCE_EXHAUSTED` responses.

## WebTransport Implementation

The C WebTransport transport uses an internal MsQuic-backed HTTP/3/WebTransport implementation. It supports the TrevRPC runtime path for unary and streaming RPCs, shared native/WebTransport listeners, plus the low-level `trevrpc_wt_*` listener, session, and bidirectional stream APIs. The implementation intentionally covers the TrevRPC subset first; browser-grade HTTP/3 coverage and datagrams remain follow-up work outside the first-cut migration.

## Typed Protobuf Boundary

The C runtime owns transport, framing, metadata, status, stream limits, and generic byte-oriented RPC dispatch. Generated C service code owns protobuf-c packing/unpacking, typed client function names, typed stream send/receive wrappers, and server adapter callbacks that translate between protobuf-c messages and runtime byte bodies. This keeps `trevrpc.h` independent from any generated protobuf schema while still letting generated code provide type-safe service APIs.

## Compatibility Matrix

| Feature                                          | Go runtime | C runtime                                             |
| ------------------------------------------------ | ---------- | ----------------------------------------------------- |
| Wire request/response/stream-frame encode/decode | Yes        | Yes                                                   |
| Shared wire golden vectors                       | Yes        | Yes                                                   |
| Metadata encode/decode and validation            | Yes        | Yes                                                   |
| Metadata-value and bearer authorizers            | Yes        | Yes                                                   |
| Request timeout/deadline enforcement             | Yes        | Yes, cooperative                                      |
| High-level MsQuic client/server RPC              | Yes        | Yes                                                   |
| High-level WebTransport client/server RPC        | Yes        | Yes                                                   |
| Shared native QUIC/WebTransport server listener  | No         | Yes                                                   |
| Low-level WebTransport wrapper                   | Yes        | Yes                                                   |
| Server runtime policy options                    | Yes        | Yes                                                   |
| Server runtime policy enforcement                | Yes        | Partial: stream limits and overload handling          |
| Graceful shutdown timeout                        | Yes        | Yes                                                   |
| Metrics callbacks                                | Yes        | Yes                                                   |
| Transport lifecycle callbacks                    | Yes        | High-level server                                     |
| Structured log callbacks                         | Yes        | Yes                                                   |
| First-class status helpers                       | Yes        | Yes                                                   |
| Generated typed C protobuf helpers               | No         | Partial generator support, runtime helpers incomplete |
| CMake installable libraries                      | N/A        | Yes                                                   |
