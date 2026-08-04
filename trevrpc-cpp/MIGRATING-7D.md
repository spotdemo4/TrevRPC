# Migrating to C++ Milestone 7D

Milestone 7D keeps TrevRPC C ABI 6 and the existing synchronous generated class and function names, but consumers must rebuild the static C++ library and regenerate every `.trevrpc.hpp` and `.trevrpc.cpp` file.

## Required source changes

Successful unary and client-streaming service handlers now return `Result<trevrpc::Response<T>>` instead of `Result<T>`. Put the protobuf value in `Response::message` and successful response metadata in `Response::metadata`. Server-streaming and bidirectional handlers continue to return `Status`; terminal metadata remains on that status.

`ClientStreamingCall::finish_and_receive()` finishes the request side, requires exactly one response message, consumes the terminal status and clean FIN, and returns the message plus terminal metadata.

`ServerOptions` fields are optional overrides. An empty or partial options object preserves all C runtime defaults. A populated zero still means unlimited or disabled where the C API defines that meaning.

Generated synchronous and asynchronous clients return a runtime `-EINVAL` error when their channel or async runtime is null, including after the generated client has been moved from.

## Asynchronous API

Generated files now also contain `{Service}AsyncService`, `{Service}AsyncClient`, and `Register{Service}Async`. Create an `AsyncRuntime` with an explicit continuation `Executor`; there is no global default executor. The runtime uses a separate bounded native-I/O pool and timer queue behind the C ABI 6 operation seam.

`Task<T>` is lazy and move-only. Await it as an rvalue, use `sync_wait` only outside its continuation executor, or use `spawn` with a mandatory completion sink. Dropping an unstarted task destroys it; started operation state owns execution until final suspension.

Async stream send and receive halves are independently movable and share one operation state. One send lane and one receive lane may progress concurrently. Half-close is explicit through `finish_send`; dropping one half does not finish or invalidate the other.

Send queues are bounded by item count and retained bytes. Fail-fast admission returns runtime `-ENOBUFS`; wait mode also has a bounded waiter count and an optional deadline. Exactly one receive may be pending; concurrent receives return runtime `-EBUSY`.

`Cancellation` is copyable shared ownership of one ABI-6 token, but ABI 6 permits only one simultaneous native stream binding and concurrent reuse may return `-EBUSY`. Use `CancellationSource` and `CancellationToken` for async fan-out; each native operation receives its own cancellation bridge.

Effective async deadlines use the earliest explicit absolute deadline, relative call timeout, parent server-call deadline, and capacity-wait deadline. Requests and messages are never replayed across reconnect, executor rejection, or coroutine restart.

## Lifecycle and callbacks

Use `Server::shutdown(ShutdownOptions)` for bounded graceful shutdown, cancellation fallback, and an explicit `ShutdownReport`. A timeout retains the server handle and route ownership so a later call can resume shutdown. The legacy no-argument `shutdown()` only requests stop and does not wait.

Operational callbacks are configured as owned `shared_ptr` interfaces from `callbacks.hpp`. Their C trampolines are exception-contained. Authorization and transport admission fail closed; observer, logger, and metrics exceptions drop the event. Service exception details are not put on the wire.

## Compatibility boundary

The generated synchronous names and method shapes remain available, and C ABI version 6 is unchanged. The response-envelope migration is an intentional pre-1.0 C++ source break. There is no cross-release binary compatibility promise for `libtrevrpc_cpp.a`; rebuild all consumers.
