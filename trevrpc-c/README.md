# TrevRPC C Runtime

`trevrpc-c` provides the C wire/value runtime, transport wrappers, and the current high-level MsQuic-backed RPC client/server runtime.

## Libraries

The CMake build creates these static libraries by default:

| Library                | Purpose                                                              | Public header            |
| ---------------------- | -------------------------------------------------------------------- | ------------------------ |
| `trevrpc_core`         | Wire encoding/decoding and owned value helpers                       | `trevrpc.h`              |
| `trevrpc_msquic`       | Low-level MsQuic listener, connection, and stream wrappers           | `trevrpc_msquic.h`       |
| `trevrpc_webtransport` | Low-level libwtf WebTransport listener, session, and stream wrappers | `trevrpc_webtransport.h` |
| `trevrpc`              | High-level RPC client/server runtime over MsQuic                     | `trevrpc.h`              |

`trevrpc.h` is the stable high-level API surface. `trevrpc_msquic.h` and `trevrpc_webtransport.h` expose transport-specific advanced APIs and do not provide high-level RPC dispatch by themselves.

## Build

```sh
cmake -S trevrpc-c -B build -DTREVRPC_BUILD_TESTS=ON
cmake --build build
ctest --test-dir build --output-on-failure
cmake --install build --prefix /usr/local
```

The same build is exercised by Nix:

```sh
nix build .#trevrpc-c
nix flake check
```

CMake options:

| Option                       | Default | Meaning                                                       |
| ---------------------------- | ------- | ------------------------------------------------------------- |
| `TREVRPC_BUILD_TESTS`        | `ON`    | Build CTest executables.                                      |
| `TREVRPC_BUILD_MSQUIC`       | `ON`    | Build the low-level MsQuic transport library.                 |
| `TREVRPC_BUILD_WEBTRANSPORT` | `ON`    | Build the low-level WebTransport transport library.           |
| `TREVRPC_BUILD_RUNTIME`      | `ON`    | Build the high-level runtime. This currently requires MsQuic. |

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

## Compatibility Matrix

| Feature                                          | Go runtime | C runtime                                             |
| ------------------------------------------------ | ---------- | ----------------------------------------------------- |
| Wire request/response/stream-frame encode/decode | Yes        | Yes                                                   |
| Shared wire golden vectors                       | Yes        | Yes                                                   |
| Metadata encode/decode and validation            | Yes        | Yes                                                   |
| Metadata-value and bearer authorizers            | Yes        | No                                                    |
| Request timeout/deadline enforcement             | Yes        | Yes, cooperative                                      |
| High-level MsQuic client/server RPC              | Yes        | Yes                                                   |
| High-level WebTransport client/server RPC        | Yes        | No                                                    |
| Low-level WebTransport wrapper                   | Yes        | Yes                                                   |
| Server runtime policy limits                     | Yes        | No                                                    |
| Graceful shutdown timeout                        | Yes        | Partial                                               |
| Metrics callbacks                                | Yes        | No                                                    |
| First-class status helpers                       | Yes        | Yes                                                   |
| Generated typed C protobuf helpers               | No         | Partial generator support, runtime helpers incomplete |
| CMake installable libraries                      | N/A        | Yes                                                   |
