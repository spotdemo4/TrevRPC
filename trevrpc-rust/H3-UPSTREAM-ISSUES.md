# h3 and h3-WebTransport upstream issue inventory

This document records issues encountered while implementing TrevRPC's Rust HTTP/3 and WebTransport server. It is intended to support upstream reports and dependency-upgrade decisions; it is not a claim that every issue affects every h3 release.

Research and source review were performed on 2026-08-12 against these exact published crates:

- `h3 0.0.8`
- `h3-quinn 0.0.10`
- `h3-webtransport 0.1.2`

The reproductions below omit routine TLS, certificate, endpoint, and h3 connection setup unless those details are relevant. Pseudocode is labeled where it is not a standalone compiling program.

## Summary

|   # | Issue                                                                         | Confirmed in                        | Upstream report                                                                                             | Match                                                           |
| --: | ----------------------------------------------------------------------------- | ----------------------------------- | ----------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------- |
|   1 | `accept_bi()` head-of-line blocks while resolving the first frame             | `h3-webtransport 0.1.2`             | [#293](https://github.com/hyperium/h3/issues/293), [#71](https://github.com/hyperium/h3/issues/71)          | Related umbrella issues; no direct report found                 |
|   2 | No `CloseWebTransportSession` receive or send API                             | `h3-webtransport 0.1.2`             | [#293](https://github.com/hyperium/h3/issues/293), [#71](https://github.com/hyperium/h3/issues/71)          | Related umbrella issues; no direct report found                 |
|   3 | No independently pollable peer `STOP_SENDING` notification                    | `h3 0.0.8`, `h3-webtransport 0.1.2` | [#124](https://github.com/hyperium/h3/issues/124), [#177](https://github.com/hyperium/h3/issues/177)        | Related API issues; no direct report found                      |
|   4 | `RecvStream::stop_sending()` panics during an outstanding read                | `h3-quinn 0.0.10`                   | [#330](https://github.com/hyperium/h3/issues/330), fixed by [#331](https://github.com/hyperium/h3/pull/331) | Exact; closed and fix merged, but still present in `0.0.10`     |
|   5 | `RecvStream::recv_id()` has the same outstanding-read panic window            | `h3-quinn 0.0.10`                   | [#330](https://github.com/hyperium/h3/issues/330), [#331](https://github.com/hyperium/h3/pull/331)          | Closely related; no direct report or fix found                  |
|   6 | Request reset cannot be observed independently while body polling is paused   | `h3 0.0.8`, `h3-quinn 0.0.10`       | [#177](https://github.com/hyperium/h3/issues/177)                                                           | Related QUIC-trait umbrella; no direct report found             |
|   7 | `AsyncWrite` reduces typed stream failures to `io::ErrorKind::Other`          | `h3 0.0.8`, `h3-webtransport 0.1.2` | [#55](https://github.com/hyperium/h3/issues/55)                                                             | Strong historical architectural match; closed                   |
|   8 | Invalid WebTransport CONNECT returns `Ok(WebTransportSession)` after HTTP 400 | `h3-webtransport 0.1.2`             | [#293](https://github.com/hyperium/h3/issues/293)                                                           | Related validation-ownership discussion; no direct report found |
|   9 | `max_webtransport_sessions` is advertised but not enforced                    | `h3 0.0.8`                          | [#71](https://github.com/hyperium/h3/issues/71), [#293](https://github.com/hyperium/h3/issues/293)          | Related umbrella issues; no direct report found                 |
|  10 | Safari/Network.framework hybrid negotiation is unsupported                    | `h3-webtransport 0.1.2`             | [#347](https://github.com/hyperium/h3/issues/347)                                                           | Exact; open                                                     |

GitHub searches also checked the relevant API names and protocol terms, including `accept_bi`, `CloseWebTransportSession`, `STOP_SENDING`, `max_webtransport_sessions`, invalid CONNECT handling, and the h3-quinn stream methods. “No direct report found” means no issue specifically describing the behavior was found; it does not prove that no duplicate exists under different wording.

## 1. `accept_bi()` first-frame head-of-line blocking

**Owner:** `h3-webtransport`

**Confirmed in:** `0.1.2`

### Actual behavior

`WebTransportSession::accept_bi()` first accepts one QUIC bidirectional stream and then synchronously waits for that stream's first protocol frame. It does not return control to the caller until the first stream is classified as a WebTransport stream or an ordinary HTTP/3 request.

A peer can therefore open stream A and send no first-frame bytes, then open a complete stream B. The server remains blocked resolving stream A and never accepts stream B. A single incomplete stream stalls all later bidirectional streams on that HTTP/3/WebTransport connection.

### Expected behavior

Accepting later streams should remain possible while each newly accepted stream independently resolves its first frame. An incomplete stream should consume only its own stream and timeout budget, not the connection's global acceptance path.

### Minimal reproduction

Client-side pseudocode, using a low-level HTTP/3/WebTransport peer so the first stream header can be withheld deliberately:

```rust
let session = establish_webtransport_session().await?;

// Stream A is accepted by QUIC, but never becomes classifiable by h3.
let (_send_a, _recv_a) = session.open_raw_bi().await?;

// Stream B is valid and complete.
let (mut send_b, _recv_b) = session.open_raw_bi().await?;
send_b.write_all(&encode_webtransport_bidi_header(session.id())).await?;
send_b.write_all(b"request").await?;
send_b.finish()?;
```

Server:

```rust
let first = session.accept_bi();
tokio::pin!(first);

assert!(tokio::time::timeout(Duration::from_millis(250), &mut first)
    .await
    .is_err());

// This remains blocked even though stream B is complete, because the only
// accept_bi future is still resolving stream A's first frame.
```

The relevant implementation sequence is effectively:

```rust
let stream = poll_accept_request_stream().await?;
let mut resolver = create_resolver(FrameStream::new(stream));
let frame = poll_fn(|cx| resolver.frame_stream.poll_next(cx)).await;
```

### TrevRPC impact and workaround

TrevRPC's unified server calls `session.accept_bi()` from the session driver. A peer that withholds the first frame can block later RPC acceptance on that session.

Wrapping `accept_bi()` in a timeout is not a safe workaround in `0.1.2`: dropping the pending resolver also drops h3 connection-owned state and was observed to close the session, turning TrevRPC's framed initial-request timeout into abrupt connection loss. TrevRPC therefore documents the limitation and relies on connection-level limits/timeouts rather than cancelling this future.

### Existing upstream issue

No direct issue was found.

- [#293: Create API for http/3 extensions like WebTransport](https://github.com/hyperium/h3/issues/293) is the closest open architectural issue. It asks for lower-level stream and frame access but does not report this head-of-line behavior.
- [#71: Add WebTransport support](https://github.com/hyperium/h3/issues/71) is the broad historical WebTransport tracking issue.

### Suggested upstream fix

Separate QUIC stream acceptance from first-frame resolution. Return an unresolved accepted stream, or maintain a bounded set of per-stream resolver futures and yield whichever resolves first. The API should also define a stream-local cancellation path that does not drop or close the whole h3 connection.

## 2. No WebTransport session-close receive or send API

**Owner:** primarily `h3-webtransport`; extension plumbing may also require `h3`

**Confirmed in:** `h3-webtransport 0.1.2`

### Actual behavior

`WebTransportSession` privately retains the CONNECT request stream as `connect_stream`, but does not poll its body for capsules and exposes no method to:

- receive a peer `CloseWebTransportSession` capsule,
- send a `CloseWebTransportSession` capsule,
- close with a WebTransport close code and reason, or
- await session closure independently of whole HTTP/3 connection closure.

Consequently, graceful server shutdown can only drain RPCs and close the underlying HTTP/3 connection. A peer session-close capsule is not surfaced to application code through `accept_bi()` or another session future.

### Expected behavior

The session API should expose both directions of the WebTransport session lifecycle, for example:

```rust
session.closed().await -> SessionClose
session.close(code, reason).await
```

The implementation should parse and emit the applicable capsule on the CONNECT stream while continuing to drive ordinary session streams.

### Minimal reproduction

Peer pseudocode:

```rust
let mut connect = establish_extended_connect().await?;
connect
    .send_capsule(CloseWebTransportSession {
        code: 42,
        reason: "client shutdown".into(),
    })
    .await?;
connect.finish().await?;
```

Server:

```rust
let session = WebTransportSession::accept(request, stream, h3).await?;

// No public API exists to receive the capsule or its code/reason.
// accept_bi() only accepts and classifies new bidirectional streams.
let result = tokio::time::timeout(Duration::from_millis(250), session.accept_bi()).await;
assert!(result.is_err());

// Likewise, there is no session.close(code, reason) operation to reproduce
// the server-to-client direction without dropping the entire connection.
```

This can also be verified statically: the public `WebTransportSession` methods in `0.1.2` are `accept`, datagram accessors, stream accept/open methods, and `session_id`; none consumes or writes CONNECT-stream capsules.

### TrevRPC impact and workaround

TrevRPC cannot report a browser's WebTransport close code/reason or send its own close reason during graceful shutdown. It drains bounded work and closes the underlying HTTP/3 connection. No protocol-equivalent workaround exists through the public API.

### Existing upstream issue

No direct issue was found.

- [#293](https://github.com/hyperium/h3/issues/293) is a strong umbrella match because it asks how extensions can send and react to custom frames and where CONNECT handling belongs.
- [#71](https://github.com/hyperium/h3/issues/71) broadly tracks WebTransport support.

### Suggested upstream fix

Add a CONNECT-stream capsule driver owned by `WebTransportSession`, plus explicit close and closed APIs. Session close must remain independent of ordinary HTTP/3 connection shutdown and should preserve the close code and reason.

## 3. No independently pollable response-side `STOP_SENDING`

**Owner:** `h3` QUIC abstraction, surfaced through `h3-webtransport`

**Confirmed in:** `h3 0.0.8`, `h3-webtransport 0.1.2`

### Actual behavior

`h3::quic::SendStream` provides send, readiness, finish, reset, and stream-ID operations, but no future or poll method for observing peer `STOP_SENDING` independently of a write. On a bidirectional RPC where the request side has already reached FIN, the server can be waiting for application response data with no transport operation to poll. A client can stop the response direction, but the server learns about it only on a later write/finish, deadline, whole-connection close, or shutdown.

### Expected behavior

A send stream should expose a cancellation/closure signal that can be selected concurrently with application work:

```rust
select! {
    _ = send.stopped() => cancel_handler(),
    item = response.next() => write(item).await?,
}
```

### Minimal reproduction

```rust
// Server has read request FIN and is waiting for the handler to produce a response.
let next_response = std::future::pending::<Bytes>();
tokio::pin!(next_response);

// Client sends STOP_SENDING for the server-to-client half here.
client.stop_response(STREAM_CANCEL_CODE)?;

// There is no h3 SendStream method to poll in this select for that event.
tokio::select! {
    bytes = &mut next_response => send_bytes(&mut send, bytes).await?,
    _ = connection.closed() => return Err(ConnectionLost),
    // Missing: stopped = send.stopped()
}
```

To observe the reset with the current API, force a transport operation after the client's `STOP_SENDING`:

```rust
let error = poll_fn(|cx| SendStreamUnframed::poll_send(&mut send, cx, &mut bytes))
    .await
    .unwrap_err();
assert!(matches!(error, StreamErrorIncoming::StreamTerminated { .. }));
```

The second snippet demonstrates that the error can be classified when a write occurs; the issue is the absence of an independent observer.

### TrevRPC impact and workaround

TrevRPC classifies a typed write failure as `CancellationSource::PeerReset`, but cannot promptly cancel a response handler while no write is pending. It falls back to the next response write, RPC deadline, connection close, or server shutdown.

### Existing upstream issue

No exact report was found.

- [#124: Lacking a way for consumer to reset stream](https://github.com/hyperium/h3/issues/124) is related stream-cancellation API work, but concerns initiating a reset rather than observing peer `STOP_SENDING`.
- [#177: Clarify & simplify quic traits](https://github.com/hyperium/h3/issues/177) discusses ambiguous stream/connection error propagation and is the closest QUIC-trait umbrella.

### Suggested upstream fix

Extend the QUIC send-stream contract with a transport-agnostic `poll_stopped`/`stopped` operation carrying the peer application error code and connection failures. Expose it through request and WebTransport stream wrappers without requiring a dummy write.

## 4. `h3-quinn::RecvStream::stop_sending()` panic during an outstanding read

**Owner:** `h3-quinn`

**Confirmed in:** `0.0.10`

### Actual behavior

`poll_data()` moves the inner `quinn::RecvStream` out of an `Option` and into a reusable read future. If that poll returns `Pending`, `stop_sending()` unwraps the now-empty `Option` and panics. Cancelling a higher-level wait while the reusable future owns the stream can leave the same state behind.

A `Ready` `poll_data()` restores the stream slot before returning, so the direct panic window is specifically an outstanding pending read or a cancelled higher-level operation that leaves the reusable future in that state.

### Expected behavior

`stop_sending()` must be safe in every valid stream state. If Quinn's stream is temporarily owned by an in-flight read future, the stop should be queued and applied once the future returns the stream.

### Minimal reproduction

The exact upstream report contains a standalone client and server. Its essential server-side trigger is:

```rust
poll_fn(|cx| match stream.poll_recv_data(cx) {
    Poll::Pending => {
        stream.stop_sending(Code::H3_NO_ERROR); // panics in h3-quinn 0.0.10
        Poll::Ready(())
    }
    Poll::Ready(_) => Poll::Ready(()),
})
.await;
```

The client opens the HTTP/3 request stream, pauses so the server read becomes pending, and only then sends the body.

### TrevRPC impact and workaround

TrevRPC's request-pump cleanup would normally send `STOP_SENDING` when a handler, timeout, overload path, or response outcome no longer needs the request body. For h3 and h3-WebTransport receive streams, TrevRPC intentionally makes that cleanup operation a no-op and drops the bounded per-RPC stream rather than risk a task panic.

### Existing upstream issue

Exact match:

- [#330: panic in RecvStream::stop_sending when body is dropped](https://github.com/hyperium/h3/issues/330) — closed as completed.
- [#331: Fix stop_sending panic when recv stream is in-flight](https://github.com/hyperium/h3/pull/331) — merged on 2026-01-22.

The fix adds a pending stop code and applies it when the read future returns the Quinn stream. The published `h3-quinn 0.0.10` source reviewed here predates that fix and still contains the unwrap.

### Suggested upstream fix

The merged fix in #331 is appropriate. TrevRPC can remove its no-op workaround after upgrading to a published h3-quinn release containing the fix and adding a regression test for cancellation during a pending request-body read.

## 5. `h3-quinn::RecvStream::recv_id()` panic during an outstanding read

**Owner:** `h3-quinn`

**Confirmed in:** `0.0.10`

### Actual behavior

`recv_id()` reads the same optional inner Quinn stream with `self.stream.as_ref().unwrap()`. Therefore it panics in the same state where `stop_sending()` did: an outstanding `poll_data()` has moved the stream into the reusable future.

The merged #331 fix guards and defers `stop_sending()`, but its patch does not change `recv_id()`.

### Expected behavior

A stable stream ID should be available in every state and should not depend on temporary ownership of the transport object by a read future.

### Minimal reproduction

Pseudocode using a receive half obtained from an h3-quinn bidirectional stream:

```rust
let mut recv = accepted_bidi_stream.split().1;

poll_fn(|cx| {
    assert!(recv.poll_data(cx).is_pending());

    // h3-quinn 0.0.10 calls self.stream.as_ref().unwrap().id().
    let _id = recv.recv_id(); // panics
    Poll::Ready(())
})
.await;
```

The peer must keep the stream open without making readable data available so that `poll_data()` returns `Pending`.

### TrevRPC impact and workaround

TrevRPC does not call `recv_id()` from request-pump cancellation or backpressure paths, so there is no currently observed TrevRPC panic from this method. It remains a sharp edge for new h3 integration code and for any upstream wrapper that queries IDs while reads are outstanding.

### Existing upstream issue

No direct report was found. [#330](https://github.com/hyperium/h3/issues/330) describes the same internal empty-slot state for `stop_sending()`, but [#331](https://github.com/hyperium/h3/pull/331) does not fix `recv_id()`.

### Suggested upstream fix

Store the stream ID as an immutable field when constructing `RecvStream`, or make the reusable future state expose the ID without borrowing the temporarily moved Quinn stream. Add a test that calls `recv_id()` after a pending `poll_data()`.

## 6. Request reset is invisible while body polling is paused by application backpressure

**Owner:** `h3` QUIC abstraction and `h3-quinn` adapter behavior

**Confirmed in:** `h3 0.0.8`, `h3-quinn 0.0.10`

### Actual behavior

A peer `RESET_STREAM` is surfaced as `StreamErrorIncoming::StreamTerminated` only when the receive stream is polled. If an application has paused body polling because its bounded downstream channel is full, h3 exposes no independent per-stream reset future to select alongside that application backpressure.

Polling whole-connection closure does not solve this: a stream reset is intentionally local to one stream and does not close the QUIC connection.

### Expected behavior

Request-stream reset should be independently observable even when body delivery is paused, allowing server work and buffered application state for that RPC to be cancelled promptly.

### Minimal reproduction

```rust
let (body_tx, mut body_rx) = tokio::sync::mpsc::channel::<Bytes>(1);

// Fill the bounded application queue and stop consuming it.
body_tx.send(Bytes::from_static(b"first")).await?;
let reserve = body_tx.reserve();
tokio::pin!(reserve);

// Client now RESET_STREAMs the request body.
client.reset_request(STREAM_CANCEL_CODE)?;

// The server waits for application capacity and intentionally does not call
// recv.poll_data(). The h3 API has no separate reset event to select here.
assert!(tokio::time::timeout(Duration::from_millis(250), &mut reserve)
    .await
    .is_err());
```

Once the queue is drained and `poll_data()` resumes, h3 can return `StreamErrorIncoming::StreamTerminated`; the delayed visibility is the issue.

### TrevRPC impact and workaround

TrevRPC's request pump selects application-channel capacity against a transport event. Native Quinn can observe a stream-local reset separately, but the current h3/h3-quinn implementation can safely provide only whole-connection closure in this branch. Peer-reset cancellation can therefore be delayed until body polling resumes, a response write fails, a deadline expires, or the connection closes.

Attempting to force `stop_sending()` from cleanup is not safe with published `h3-quinn 0.0.10` because of issue 4.

### Existing upstream issue

No direct issue was found. [#177](https://github.com/hyperium/h3/issues/177) is related because it asks the QUIC traits to define where stream and connection errors surface, but it does not request an independently pollable stream-reset signal.

### Suggested upstream fix

Expose stream reset/closure as an independently pollable event, or define a split receive abstraction where body data and terminal/reset state can be driven without competing for the same mutable polling operation.

## 7. `AsyncWrite` loses top-level typed stream-error classification

**Owner:** `h3`; inherited by `h3-webtransport` stream wrappers

**Confirmed in:** `h3 0.0.8`, `h3-webtransport 0.1.2`

### Actual behavior

`BufRecvStream` implements Tokio and futures `AsyncWrite` by converting every `StreamErrorIncoming` into:

```rust
std::io::Error::new(std::io::ErrorKind::Other, error)
```

The source object is not irretrievably discarded: callers can recover it by downcasting `io::Error::get_ref()`. However, the top-level type and meaningful `ErrorKind` classification are lost. Generic code that handles only `io::ErrorKind` cannot distinguish a peer stream reset from whole-connection loss.

### Expected behavior

The primary h3/WebTransport write path should preserve `StreamErrorIncoming` directly, or the I/O adapter should document a stable recovery API and provide useful error-kind mapping where possible.

### Minimal reproduction

```rust
use tokio::io::AsyncWriteExt;

// Arrange for the peer to reset/stop this stream, then write through the
// AsyncWrite adapter.
let err: std::io::Error = send.write_all(b"response").await.unwrap_err();
assert_eq!(err.kind(), std::io::ErrorKind::Other);

let typed = err
    .get_ref()
    .and_then(|source| source.downcast_ref::<h3::quic::StreamErrorIncoming>());
assert!(typed.is_some());
```

In contrast, polling the h3 trait directly returns the typed error:

```rust
let err = poll_fn(|cx| {
    h3::quic::SendStreamUnframed::poll_send(&mut send, cx, &mut bytes)
})
.await
.unwrap_err();

assert!(matches!(
    err,
    h3::quic::StreamErrorIncoming::StreamTerminated { .. }
));
```

### TrevRPC impact and workaround

TrevRPC needs to distinguish a peer reset (`CancellationSource::PeerReset`) from connection loss. Its h3-WebTransport writer therefore polls `SendStreamUnframed::poll_send` and `SendStream::poll_finish` directly instead of using `AsyncWriteExt::write_all` and `shutdown`.

### Existing upstream issue

[#55: Drop AsyncWrite usage in h3-quinn once GATs land](https://github.com/hyperium/h3/issues/55) is a strong architectural match. It explicitly describes performance costs and downcasting transport errors from `io::Error`. It was closed as completed in 2023, but the `h3 0.0.8` `BufRecvStream` I/O adapters still perform the `ErrorKind::Other` wrapping described here. It is not an exact report of this current wrapper path.

### Suggested upstream fix

Prefer typed h3 poll/future methods in public protocol APIs and make `AsyncWrite` an explicitly lossy compatibility adapter. If the adapter remains prominent, document source downcasting and map errors to meaningful `io::ErrorKind` values where semantics are unambiguous.

## 8. Invalid CONNECT returns a successful session object after sending HTTP 400

**Owner:** `h3-webtransport`

**Confirmed in:** `0.1.2`

### Actual behavior

`WebTransportSession::accept()` validates that the request is `CONNECT` with the WebTransport protocol extension. When validation fails, it sends an HTTP `400 Bad Request`, but then continues constructing and returns `Ok(WebTransportSession)`.

This allows application code to enter a session loop for a request the crate itself rejected on the wire.

### Expected behavior

Invalid input should produce a non-session outcome: an error, `Ok(None)`, or an enum distinguishing accepted and rejected requests. `Ok(WebTransportSession)` should imply that a successful 2xx CONNECT response was sent.

### Minimal reproduction

```rust
let request = http::Request::builder()
    .method(http::Method::POST) // or CONNECT without Protocol::WEB_TRANSPORT
    .uri("https://localhost/not-webtransport")
    .body(())?;

let result = WebTransportSession::accept(request, request_stream, h3_connection).await;

// Actual in 0.1.2: the peer receives HTTP 400, but the server gets Ok(session).
assert!(result.is_ok());
```

A complete harness needs an h3 request stream and peer settings with WebTransport and datagrams enabled so execution reaches the request validation branch.

### TrevRPC impact and workaround

TrevRPC validates the method and `Protocol::WEB_TRANSPORT` extension before calling `WebTransportSession::accept()`, so invalid ordinary requests do not currently reach this path. The duplicate validation is required to preserve the invariant that `Ok(session)` represents an accepted session.

### Existing upstream issue

No direct issue was found. [#293](https://github.com/hyperium/h3/issues/293) explicitly asks where CONNECT validation should be handled, making it a related design discussion rather than a duplicate bug report.

### Suggested upstream fix

Return immediately after sending the rejection response and expose a typed rejection result. Consolidate request validation ownership so callers do not need to duplicate the crate's private validation logic.

## 9. `max_webtransport_sessions` is advertised but not enforced

**Owner:** `h3`, with lifecycle implications for `h3-webtransport`

**Confirmed in:** `h3 0.0.8`

### Actual behavior

`server::Builder::max_webtransport_sessions(value)` stores `value` in configuration and serializes it into `SETTINGS_WEBTRANSPORT_MAX_SESSIONS`. No active-session counter or request rejection references the configured field after settings serialization.

The method's Rustdoc says it “Limits the maximum number of WebTransport sessions,” but h3 itself continues delivering extended CONNECT request streams without checking the limit. Moreover, `h3-webtransport 0.1.2` moves the whole h3 connection into one `WebTransportSession`, so its public API cannot naturally operate the configured number of concurrent sessions when the value is greater than one.

### Expected behavior

The API contract should be explicit:

- If the value is only an advertised peer obligation, name and document it as such and expose enough information for the application to enforce violations.
- If it is a server limit, track active sessions and reject excess CONNECT requests with the protocol-appropriate response while allowing up to the configured count.

### Minimal reproduction

Static check:

```sh
rg -n 'max_webtransport_sessions' h3/src
```

For `h3 0.0.8`, references are limited to configuration defaults, settings parsing/serialization, and the builder setter; there is no runtime admission check.

Behavioral peer pseudocode:

```rust
let mut builder = h3::server::builder();
builder
    .enable_extended_connect(true)
    .enable_datagram(true)
    .enable_webtransport(true)
    .max_webtransport_sessions(1);

let mut h3 = builder.build(quic).await?;

// A non-compliant peer opens two valid WebTransport CONNECT requests.
let first = h3.accept().await?.unwrap().resolve_request().await?;
let second = h3.accept().await?.unwrap().resolve_request().await?;

// h3 delivers both to the application; it does not reject the second from
// the configured maximum. Application code must count/reject it itself.
```

### TrevRPC impact and workaround

TrevRPC configures the advertised maximum as one and its h3-WebTransport session architecture handles only one active session per HTTP/3 connection. It also rejects a nested WebTransport CONNECT observed within the active session. The current effective limit therefore comes from application architecture and validation, not h3's builder option.

### Existing upstream issue

No direct issue was found. [#71](https://github.com/hyperium/h3/issues/71) and [#293](https://github.com/hyperium/h3/issues/293) are broad WebTransport and extension-API issues only.

### Suggested upstream fix

Clarify whether the setting is declarative or enforced. Add active-session lifecycle hooks and admission enforcement, or rename the builder operation to make its settings-only behavior explicit. `h3-webtransport` should support multiple session drivers on one h3 connection if values greater than one are intended to be usable.

## 10. Safari/Network.framework hybrid WebTransport negotiation is unsupported

**Owner:** `h3`/`h3-webtransport` WebTransport negotiation and capsule support

**Confirmed in:** `h3-webtransport 0.1.2` on macOS 26 Safari 26.4+ and Cocoa WebKit

### Actual behavior

Safari and Cocoa WebKit builds backed by Apple's Network.framework advertise a hybrid set of draft WebTransport settings and require initial session flow-control capsules on the successful CONNECT stream. `h3-webtransport 0.1.2` does not negotiate that dialect or send the required grants. `WebTransport.ready` remains pending and eventually times out; in partially compatible settings experiments, `createBidirectionalStream()` remains pending instead.

The same TrevRPC browser client and certificates complete all RPC shapes against TrevRPC's C, C++, JavaScript, and Kotlin WebTransport servers, isolating the interoperability gap to the Rust h3-WebTransport path.

### Expected behavior

The server should identify the peer's supported WebTransport dialect, select compatible settings behavior, and send the initial session flow-control capsules required by Network.framework.

### Minimal reproduction

Server:

```rust
let mut builder = h3::server::builder();
builder
    .enable_extended_connect(true)
    .enable_datagram(true)
    .enable_webtransport(true)
    .max_webtransport_sessions(1);
```

Safari 26.4+ or Cocoa WebKit on macOS 26:

```javascript
const transport = new WebTransport(url, {
  serverCertificateHashes: certificateHashes,
});
await transport.ready; // does not resolve against h3-webtransport 0.1.2
```

Use a loopback HTTPS URL, a valid short-lived certificate hash, and an allowed `Origin`. The upstream issue lists the exact hybrid settings and required `WT_MAX_DATA`, `WT_MAX_STREAMS_BIDI`, and `WT_MAX_STREAMS_UNI` capsules.

### TrevRPC impact and workaround

The Rust server is not currently interoperable with this Safari/Network.framework path. Chromium remains the primary browser path for Rust WebTransport. Deployments requiring Safari can use another TrevRPC server runtime with the compatibility path or wait for upstream negotiation and capsule support.

### Existing upstream issue

Exact open report: [#347: Support Safari/Network.framework hybrid WebTransport negotiation and initial flow-control capsules](https://github.com/hyperium/h3/issues/347).

### Suggested upstream fix

Implement dialect-aware settings negotiation and CONNECT-stream capsule support, including the initial flow-control grants described in #347. This overlaps with the general session capsule API required by issue 2.

## Upstream issue relationship notes

### Exact reports

- [#330](https://github.com/hyperium/h3/issues/330) exactly reports the `h3-quinn 0.0.10` `stop_sending()` panic. [#331](https://github.com/hyperium/h3/pull/331) is the merged fix.
- [#347](https://github.com/hyperium/h3/issues/347) exactly reports the Safari/Network.framework interoperability failure.

### Strong architectural match

- [#55](https://github.com/hyperium/h3/issues/55) explicitly discusses avoiding `AsyncWrite` and downcasting transport errors. It is closed and predates the currently reviewed wrapper, so it should be referenced rather than treated as an exact duplicate.

### Related open umbrella issues

- [#293](https://github.com/hyperium/h3/issues/293) covers APIs for HTTP/3 extensions such as WebTransport, including access to lower-level streams/frames and ownership of CONNECT handling.
- [#124](https://github.com/hyperium/h3/issues/124) requests a consumer stream-reset API, but does not cover observing peer `STOP_SENDING`.
- [#177](https://github.com/hyperium/h3/issues/177) covers ambiguity in QUIC trait error/close propagation.
- [#71](https://github.com/hyperium/h3/issues/71) is the broad historical WebTransport support issue.

### Apparently unreported as distinct issues

The GitHub search found no direct report for:

1. `accept_bi()` first-frame head-of-line blocking.
2. Missing `CloseWebTransportSession` receive/send lifecycle APIs.
3. Missing independently pollable response-side peer `STOP_SENDING`.
4. `recv_id()` panicking while the reusable read future owns the stream.
5. Delayed request reset visibility during application backpressure.
6. Invalid CONNECT returning `Ok(WebTransportSession)` after HTTP 400.
7. `max_webtransport_sessions` being settings-only despite limit-oriented Rustdoc.

## Source locations reviewed

Published crate source locations corresponding to the findings:

- `h3-webtransport 0.1.2/src/server.rs`
  - `WebTransportSession` retains a private `connect_stream`.
  - `accept()` sends either 200 or 400 and then unconditionally constructs `Ok(Self)`.
  - `accept_bi()` accepts one stream and awaits its first frame inline.
- `h3-webtransport 0.1.2/src/stream.rs`
  - Stream wrappers expose h3 send/receive traits and Tokio/futures I/O adapters, but no independently pollable stopped event.
- `h3 0.0.8/src/quic.rs`
  - `SendStream` has no peer `STOP_SENDING` observer.
  - `RecvStream` exposes resets only through `poll_data()` and can initiate `stop_sending()`.
- `h3 0.0.8/src/stream.rs`
  - Tokio/futures `AsyncWrite` converts `StreamErrorIncoming` to `io::ErrorKind::Other` while retaining it as the source payload.
- `h3 0.0.8/src/config.rs` and `src/server/builder.rs`
  - `max_webtransport_sessions` is stored and serialized into settings.
- `h3-quinn 0.0.10/src/lib.rs`
  - `poll_data()` moves the Quinn stream into a reusable future.
  - `stop_sending()` and `recv_id()` unwrap the temporarily empty stream slot.

When preparing upstream reports, each apparently unreported item should be searched again immediately before filing, since issue titles and statuses can change after this inventory date.
