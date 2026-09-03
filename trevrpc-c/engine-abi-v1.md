# Engine ABI 1 contract

This document is the normative contract for TrevRPC Engine ABI 1. `trevrpc_engine.h` is the sole public operational transport ABI: every provider exposes the same engine, handle, event, receive, wake, diagnostic, command, and lifecycle types and functions.

Provider companion packages only construct a `trevrpc_engine`. They do not define a second operational API. Applications obtain an engine from a provider factory such as `trevrpc_engine_msquic_create_v1()` and then use only `trevrpc_engine_*` operations; Engine ABI 1 has no provider-neutral construction entry point.

## 1. Scope and versioning

Engine ABI 1 is provider-neutral. It defines:

- provider-neutral listener, connection, and stream handles;
- nonblocking transport commands and asynchronous completion events;
- detached event and receive ownership;
- a bounded ordinary-event queue plus mandatory event reservations;
- a pollable level-triggered wake source;
- close, drain, and final-release coordination;
- provider-neutral and provider-supplied diagnostics.

`TREVRPC_ENGINE_ABI_VERSION` is `1`. Compile-time consumers test that macro, runtime consumers call `trevrpc_engine_abi_version()`, and link-time consumers reference `trevrpc_engine_abi_1_anchor()`. Engine ABI 1 and TrevRPC C ABI 6 are independently versioned; neither version implies the other.

The engine currently requires a POSIX platform with file descriptors and pthreads. It invokes no application or binding callback and creates no dispatcher thread. A provider may receive native transport callbacks, but those callbacks enter the engine through the private provider contract and publish public Engine ABI events.

## 2. Versioned structures and frozen layouts

Every versioned structure starts with `struct_size` and `struct_version`. Its initializer:

- returns `-EINVAL` for null or undersized storage;
- returns `-EOVERFLOW` when the supplied size exceeds `UINT32_MAX`;
- zeroes the complete caller-supplied extent;
- records the supplied size and structure version 1;
- applies the documented defaults.

Consumers require the complete ABI 1 prefix, reject unsupported versions with `-ENOTSUP`, and require reserved fields to remain zero. Larger caller structures are accepted. Getters write only the ABI 1 prefix and leave caller output unchanged on failure.

On the supported 64-bit ABI, the frozen layouts are:

| Type                                | Size | Alignment |
| ----------------------------------- | ---: | --------: |
| `trevrpc_engine_handle_v1`          |   16 |         8 |
| `trevrpc_engine_config_v1`          |   80 |         8 |
| `trevrpc_engine_wake_source_v1`     |   48 |         8 |
| `trevrpc_engine_endpoint_config_v1` |  160 |         8 |
| `trevrpc_engine_event_info_v1`      |  136 |         8 |
| `trevrpc_engine_receive_info_v1`    |   64 |         8 |
| `trevrpc_engine_diagnostics_v1`     |  208 |         8 |

Every field offset is part of ABI 1 and is enforced by the C and C++ ABI fixtures.

`trevrpc_engine_config_v1_init()` sets these defaults:

- `event_capacity`: `TREVRPC_ENGINE_DEFAULT_EVENT_CAPACITY`;
- `listener_capacity`: `TREVRPC_ENGINE_DEFAULT_LISTENER_CAPACITY`;
- `connection_capacity`: `TREVRPC_ENGINE_DEFAULT_CONNECTION_CAPACITY`;
- `stream_capacity`: `TREVRPC_ENGINE_DEFAULT_STREAM_CAPACITY`;
- `max_receive_owned_count`: `TREVRPC_ENGINE_DEFAULT_MAX_RECEIVE_OWNED_COUNT`;
- `max_receive_owned_bytes`: `TREVRPC_ENGINE_DEFAULT_RECEIVE_OWNED_BYTES`.

All capacities and receive-ownership limits must be nonzero. `event_capacity` must not exceed `TREVRPC_ENGINE_MAX_EVENT_CAPACITY`.

`trevrpc_engine_endpoint_config_v1_init()` sets `peer_bidi_stream_count` to 100 and initializes the pending-send count, pending-send byte, and frame-size limits from their `TREVRPC_ENGINE_DEFAULT_*` constants. Endpoint strings and byte sequences are length-delimited. A provider must copy any host, ALPN, certificate, private-key, or CA-certificate data it retains before the command returns.

## 3. Provider-neutral handles

`trevrpc_engine_handle_v1` is an immutable value containing `owner`, `slot`, and `generation`; it is never a public object pointer. The all-zero value and any value with a zero component are malformed and produce `-EINVAL`.

Each engine has one nonzero provider owner cookie. The generic dispatch layer rejects a handle whose owner does not match that engine with `-ESTALE`. The provider then validates slot occupancy, generation, and expected object kind. Reclaimed, foreign, retired, or generation-mismatched handles are stale. Providers advance generation before slot reuse and must not turn a wrapped generation into a valid zero generation.

Handles do not keep the engine alive. Applications must not start an operation with a handle after its terminal event or after engine final release has begun.

## 4. Generic transport operations

After any provider factory succeeds, all transport work uses the following generic functions from `trevrpc_engine.h`:

- listener: `trevrpc_engine_listen_v1()`, `trevrpc_engine_listener_get_port_v1()`, `trevrpc_engine_listener_close()`;
- connection: `trevrpc_engine_dial_v1()`, `trevrpc_engine_dial_cancel()`, `trevrpc_engine_connection_open_bidi_stream_v1()`, `trevrpc_engine_connection_close()`;
- stream: `trevrpc_engine_stream_send_frame_v1()`, `trevrpc_engine_stream_receive_frame()`, `trevrpc_engine_stream_finish_send()`, `trevrpc_engine_stream_abort()`, `trevrpc_engine_stream_close()`;
- engine: wake, event, diagnostics, close, drain, and release functions documented below.

Commands are admitted only while the engine is `RUNNING`. A command returns `-ENOTSUP` when the selected provider does not implement that operation. Generic output arguments remain unchanged on failure.

Listen returns a provider-neutral listener handle after provider admission. Dial and local bidirectional-stream open require nonzero operation IDs and return registered handles after asynchronous admission; completion is reported by `CONNECTION_READY` or `CONNECTION_FAILED`, and `STREAM_READY` or `STREAM_FAILED`. Peer-created connections and streams use operation ID zero in their events.

Send requires a nonzero operation ID and accepts a body of at most `UINT32_MAX - 4` bytes at the generic boundary. An accepted provider copies or otherwise takes independent ownership of all send data before returning. Each accepted send produces exactly one `SEND_COMPLETE` event before the corresponding `STREAM_CLOSED` event. Providers may impose smaller endpoint frame and pending-send budgets and report temporary capacity exhaustion with `-EAGAIN`.

Dial cancellation is idempotent while a dial is pending. Providers serialize cancellation against ready or failed completion so that exactly one terminal dial result commits. Cancellation after readiness returns `-EALREADY`.

Close, abort, finish-send, receive, and port lookup validate the engine owner before dispatch. Providers additionally validate handle kind, generation, readiness, and terminal state. Typical stable results are `-EINVAL` for malformed or wrong-kind input, `-ESTALE` for a reclaimed handle, `-EAGAIN` for an admitted object or receive queue that is not ready, and `-EPIPE` after terminal close or engine command admission closes.

## 5. Events and sequencing

`trevrpc_engine_next_event()` is the only event dequeue operation. Events are immutable snapshots described by `trevrpc_engine_event_info_v1`. A single engine sequence orders all ordinary, mandatory, fatal diagnostic, and stopped publications; it is provider-neutral and never resets during the engine lifetime.

Public event kinds are:

- engine-wide `DIAGNOSTIC` and `STOPPED`;
- `LISTENER_STOPPED`;
- `CONNECTION_READY`, `CONNECTION_FAILED`, and `CONNECTION_CLOSED`;
- `STREAM_READY`, `STREAM_FAILED`, `STREAM_READABLE`, `RECEIVE_FIN`, `SEND_COMPLETE`, and `STREAM_CLOSED`.

Event flags identify terminal and fatal records, client/server and local/peer origin, peer reset, transport error, and clean receive FIN. `status` is a stable negative errno value or zero. `application_error_code` and `provider_error_code` retain the separate peer/application and native-provider codes when applicable. `subject`, `parent`, `subject_kind`, and `operation_id` correlate events with provider-neutral objects and asynchronous commands.

Ordinary event payload bytes are copied into the event before publication. Frame bodies are not carried by `STREAM_READABLE`; consumers obtain them through `trevrpc_engine_stream_receive_frame()`.

## 6. Queue capacity and mandatory reservations

`event_capacity` is the exact capacity for ordinary events. The queue is FIFO by publication linearization order, protected by the engine mutex, safe for multiple producers and consumers, and nonblocking with respect to data and capacity. It never waits for an event or free ordinary slot, overwrites an accepted event, or silently drops one. An empty dequeue returns `-EAGAIN` and leaves `*out_event` unchanged.

The engine separately preallocates one fatal diagnostic event and one stopped event. It also reserves mandatory event storage before admitting operations whose completion or terminal record must not be lost:

- listen reserves its listener terminal event;
- dial reserves one ready-or-failed completion and one connection terminal event;
- local stream open reserves one ready-or-failed completion and one stream terminal event;
- send reserves its completion event.

A failed provider command cancels all reservations that were allocated for that attempted admission. Successful admission transfers the reservations to the provider, which must publish or cancel them exactly once. Reserved publication is not limited by ordinary `event_capacity`, does not allocate event storage at completion time, and carries no payload bytes. `mandatory_reservations` in diagnostics reports reservations not yet settled.

Consequently, total `queue_depth` may exceed `event_capacity`; `ordinary_queue_depth` is the capacity-limited portion. Accepted mandatory completions and terminal records must be committed before the provider reports itself stopped, and `STOPPED` is globally final.

An ordinary publication at capacity returns `-ENOSPC`, increments `events_rejected`, records the first sticky fatal status, closes admission for new commands and provider operations, publishes exactly one fatal diagnostic from its dedicated slot, asks the provider to close, and ultimately publishes exactly one fatal stopped event. Already-admitted callbacks and operations may finish during `STOPPING`; they must settle their mandatory reservations before the provider reports stopped.

### Provider object admission budgets

The MsQuic provider treats the configured `connection_capacity` and `stream_capacity` as hard object budgets. `connection_capacity` covers the sum of queued server connections, handshakes in flight, and admitted/live client or server connections. `stream_capacity` covers the sum of queued peer streams and admitted/live streams. These budgets are provider-private accounting over the existing Engine ABI capacities; the provider does not add MsQuic companion options or silently grow either budget.

A native server connection accepted by MsQuic but not yet promoted to a public connection remains admission-accounted and consumes one connection budget unit. A peer stream accepted by MsQuic but not yet promoted remains admission-accounted and consumes one stream budget unit. Pending server connections use a FIFO owned by the provider, and each connection has a FIFO for pending peer streams. Queue membership owns a provider lifetime reference until the node is unlinked; removing a node and releasing its object are separate steps so a queued object cannot disappear during promotion or rejection.

Handshake admission is bounded by the remaining connection budget. The provider must reserve the connection budget before placing a handshake or accepted connection on a queue, and must release it only when that connection is rejected or reaches terminal cleanup. The same rule applies to stream budget for pending peer streams. Consequently, accepted-but-not-yet-promoted objects count against capacity even when provider diagnostics have not yet counted them as live objects.

MsQuic send admission is also provider-private and bounded at three levels: each stream has its configured pending-send count and byte limit, each connection has a derived aggregate limit covering its admitted streams, and the provider has a derived aggregate limit covering its admitted connections. Count and byte credit are reserved together before allocating or copying the frame payload; a saturated level returns `-EAGAIN` synchronously without allocating. Successful sends retain an independent copy until exactly-once completion or cancellation, and every completion returns all three levels of credit. Queued sends are FIFO within a stream and scheduled by a provider-owned round-robin queue across streams, so one busy stream cannot starve other streams; wakeups signal the single scheduler rather than broadcasting to all writers. Unsubmitted sends are canceled during stream, connection, or provider shutdown, while submitted sends remain owned until their native completion callback.

## 7. Detached event and receive ownership

A successful dequeue transfers one independently allocated `trevrpc_engine_event` to the caller. Before returning it, the engine runs the provider's dequeue-detachment hook and removes all provider hooks and provider-context references from the event. The event and its copied payload therefore remain inspectable and releasable after the subject, provider, or engine is destroyed.

`trevrpc_engine_event_get_info_v1()` returns a borrowed view valid until `trevrpc_engine_event_release()`. Event release accepts `NULL`. A queued event that is destroyed without being dequeued runs its provider drop hook while the provider still exists; a detached event does not.

A successful `trevrpc_engine_stream_receive_frame()` transfers one independently owned `trevrpc_engine_receive` to the caller. A provider may create it by copying bytes or by transferring owned storage with a release function. Removing a receive from a provider queue returns the provider's queued count and byte credit; the detached receive itself is not tied to the engine lifetime. `trevrpc_engine_receive_get_info_v1()` returns a borrowed immutable view valid until `trevrpc_engine_receive_release()`, which accepts `NULL` and invokes the provider-supplied owned-storage release exactly once when present.

Events and receives may outlive `trevrpc_engine_release()`. Drain and release do not wait for callers to release detached objects. Getters and release must not run concurrently on the same event or receive.

For framed stream providers, `STREAM_READABLE` is a coalesced readiness hint. Consumers drain `trevrpc_engine_stream_receive_frame()` until `-EAGAIN`; a stale readiness hint may legitimately find no frame after another consumer popped it. Complete frames preserve wire order, frames completed with FIN remain readable before `RECEIVE_FIN`, zero-length frames are valid, and a partial frame at FIN produces terminal `-EPROTO` rather than a partial receive.

## 8. Wake source

`trevrpc_engine_get_wake_source_v1()` returns a borrowed, nonblocking, close-on-exec, level-triggered POSIX descriptor owned by the engine. The caller must not read, close, reconfigure, or otherwise take ownership of it. It remains valid until successful final release.

Readability means that at least one event may be available. Consumers integrate the descriptor with `poll`, `select`, `kqueue`, or an equivalent descriptor mechanism, then call `trevrpc_engine_next_event()` until `-EAGAIN`.

The engine writes one byte only on an empty-to-nonempty arm transition. `EINTR` is retried and write-side `EAGAIN` counts as a successful arm because the pipe is already readable. The last dequeue drains and disarms the pipe while holding the queue mutex, preventing an enqueue into a drain gap.

If wake draining fails while dequeuing the stopped event, the engine returns a fatal diagnostic first and requeues an updated fatal stopped event with a later sequence. No event is published after that stopped event.

On platforms with `pipe2`, both descriptors are created atomically with `O_NONBLOCK | O_CLOEXEC`. The portable POSIX fallback applies both flags immediately with `fcntl`; applications on those platforms must serialize engine creation with process-wide `fork`/`exec` activity because POSIX has no portable atomic equivalent.

## 9. Admission and lifecycle

The engine states are `RUNNING -> STOPPING -> STOPPED -> RELEASING`.

The public API has an admission gate, while provider callbacks and accepted provider operations use a separate lifetime gate. New commands require `RUNNING`. Provider callbacks may finish during `STOPPING`; accepted operations and callbacks hold lifetime references until they leave. Callback admission is thread-scoped and rejects cross-engine callback nesting with `-EDEADLK`. Operation pins are transferable lifetime references: a provider may acquire one on an API thread and release it on a completion callback thread.

`trevrpc_engine_close()` is concurrent, nonblocking, and idempotent. The first call changes `RUNNING` to `STOPPING` and invokes the provider's close operation exactly once. Close does not wait for provider callbacks, accepted operations, event consumption, or detached receives.

The provider reports final quiescence through the private provider contract. The engine publishes `STOPPED` only after the provider has reported stopped and active provider callbacks and operations have reached zero. The stopped event has `TERMINAL`, also has `FATAL` when the sticky terminal status is nonzero, and carries the sticky provider error code.

`trevrpc_engine_drain()` performs close if needed and blocks until engine state reaches `STOPPED`. It does not consume events, destroy the engine, or wait for detached event or receive release. Operational transport failures are communicated by events and diagnostics; drain returns an immediate close/admission failure, not the sticky terminal status merely because shutdown was fatal.

`trevrpc_engine_release()` accepts `NULL`. It rejects release from the same engine's provider callback context with `-EDEADLK`. Otherwise it closes public API admission, requests close, waits for `STOPPED`, changes state to `RELEASING`, closes provider lifetime admission, waits for already-admitted API calls, callbacks, and transferable operation pins, drops queued events, destroys the provider, closes wake descriptors, and frees the engine. A provider must not call final release while holding an operation pin that cannot be settled by another execution context.

The caller must externally synchronize final release against API entry through the raw engine pointer. Release may begin only after every concurrent caller has either completed gate admission or has been prevented from starting an entry attempt; a thread that has merely entered a C function but has not yet acquired the admission reference is not protected. The gates safely drain calls that acquired admission and reject later admission with `-EPIPE`; they cannot make acquisition of a reference from already-freed raw storage safe.

## 10. Diagnostics and sticky failures

`trevrpc_engine_get_diagnostics_v1()` returns a mutex-consistent engine snapshot combined with a provider snapshot. `engine_abi_version` is always 1. Engine-owned fields include:

- state, sticky terminal status, and sticky provider error code;
- ordinary event capacity, total queue depth, and ordinary queue depth;
- enqueued, dequeued, and rejected event counts;
- active callbacks and admitted API calls;
- wake signals, write-side `EAGAIN`, and wake failures;
- outstanding mandatory reservations.

Provider-supplied fields include current and peak provider-owned receive counts and bytes, pending-send bytes and count, and live listener, connection, and stream counts.

The first negative operational failure is sticky and later failures do not replace it. A nonnegative provider failure report is normalized to `-EIO`. Diagnostic counters saturate at `UINT64_MAX` rather than wrapping.

## 11. Package discovery and dependencies

CMake consumers use:

```cmake
find_package(trevrpc_engine CONFIG REQUIRED)
target_link_libraries(application PRIVATE trevrpc::trevrpc_engine)
```

The package exports `TREVRPC_ENGINE_ABI_VERSION=1` and finds `Threads`. It does not require MsQuic, WebTransport, protobuf, or TrevRPC C ABI 6.

pkg-config consumers use `trevrpc_engine`; the module exports `trevrpc_engine_abi_version=1`, the engine archive and include directory, and private thread flags.

A provider companion is an additional construction-time dependency. For example, the MsQuic companion package depends on this engine package and MsQuic, but all operations on the returned object remain in `trevrpc_engine.h`.

## 12. Stable errors

| Error        | Meaning                                                                                                                                                             |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `-EINVAL`    | Null required pointer, malformed handle, undersized structure, invalid capacity, nonzero reserved field, invalid operation ID or body, or another invalid argument. |
| `-EOVERFLOW` | Initialization size or internal size/reference arithmetic cannot be represented.                                                                                    |
| `-ENOTSUP`   | Structure version or provider operation is not supported.                                                                                                           |
| `-ENOMEM`    | Engine, event, reservation, receive, provider, or payload allocation failed.                                                                                        |
| `-EMSGSIZE`  | A reserved mandatory event attempted to carry payload, or a provider-specific frame limit was exceeded.                                                             |
| `-EAGAIN`    | No event or receive is available, an admitted object is not ready, or provider capacity is temporarily exhausted.                                                   |
| `-EALREADY`  | An operation ID is already pending or cancellation lost to readiness.                                                                                               |
| `-ESTALE`    | A handle belongs to another engine or its slot/generation has been reclaimed.                                                                                       |
| `-ENOSPC`    | Ordinary event capacity was exhausted and fail-stop began, or a provider object registry has no available slot.                                                     |
| `-EPIPE`     | Command, publication, callback, operation, or final-release API admission is closed, or an object is terminally closing.                                            |
| `-ECANCELED` | An accepted asynchronous operation completed by cancellation.                                                                                                       |
| `-EPROTO`    | A framed receive ended with an incomplete or invalid frame.                                                                                                         |
| `-EDEADLK`   | Final release or cross-engine callback admission would deadlock from the current provider callback context.                                                         |
