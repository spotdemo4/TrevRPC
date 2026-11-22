# @trevrpc/qmux

A JavaScript implementation of [QMux](https://datatracker.ietf.org/doc/draft-ietf-quic-qmux/) (draft-ietf-quic-qmux-02) over WebSocket messages ([draft-lcurley-qmux-websocket-00](https://datatracker.ietf.org/doc/draft-lcurley-qmux-websocket/)), with the API of a [WebTransport](https://developer.mozilla.org/en-US/docs/Web/API/WebTransport) session: QUIC's streams, flow control, stream limits and datagrams for browsers without WebTransport and networks that block UDP.

It has no dependencies and runs in browsers and in server runtimes with a WebSocket, such as Node.js with [`ws`](https://www.npmjs.com/package/ws) or Deno. [TrevRPC](https://trev.zip/llc/TrevRPC)'s browser client uses it for its WebSocket fallback. TrevRPC's interop suite runs its bidirectional streams against the [`qmux`](https://crates.io/crates/qmux) crate and TrevRPC's Go and Erlang implementations, and all of them are checked against the same golden vectors. Unidirectional streams and datagrams are covered by this package's own tests only.

```sh
npm install @trevrpc/qmux
```

> **Versioning:** the package shares the TrevRPC version and is released with it. QMux is still an Internet-Draft, so
> a new draft revision may change the API and the wire format.

## Use

QMux has no identifier of its own. The application defines one per draft version, such as `myapp-qmux-02`, and negotiates it as the WebSocket subprotocol.

```js
import { QMuxSession } from "@trevrpc/qmux";

const session = new QMuxSession("wss://example.com/app", {
  protocols: ["myapp-qmux-02"],
  parameters: { maxDatagramFrameSize: 1200 },
  keepAlivePeriodMs: 10_000,
});
await session.ready;

const stream = await session.createBidirectionalStream();
const writer = stream.writable.getWriter();
await writer.write(new TextEncoder().encode("hello"));
await writer.close();
for await (const chunk of stream.readable) {
  console.log(chunk);
}
session.close({ closeCode: 0, reason: "done" });
```

A server runs a session in the `"server"` role over each WebSocket it accepts, after selecting the subprotocol in the upgrade:

```js
const session = new QMuxSession(socket, { role: "server" });
for await (const { readable, writable } of session.incomingBidirectionalStreams) {
  readable.pipeTo(writable);
}
```

- **Sessions.** `ready` resolves once the peer's transport parameters have arrived, and `closed` resolves with the `{ closeCode, reason }` of an application close by either side. `stage` says how far a session got (`"opening"`, `"handshaking"`, `"ready"` or `"closed"`), and `peerParameters` what the peer advertised.
- **Streams.** `createBidirectionalStream()` returns `{ readable, writable }` and `createUnidirectionalStream()` a send-only writable; both wait for the peer's stream limit unless `waitUntilAvailable` is false. The peer's streams arrive on `incomingBidirectionalStreams` and `incomingUnidirectionalStreams`, and cancelling either refuses the streams the peer opens later. Writes wait for flow control credit and while the WebSocket buffers more than `sendBufferLimit` (1 MiB).
- **Resets.** As over WebTransport, `writable.abort(reason)` sends RESET_STREAM and `readable.cancel(reason)` STOP_SENDING, with the reason's `streamErrorCode`, or 0. A `QMuxError` carries a code of your choice: `new QMuxError("cancelled", { streamErrorCode: 1 })`. A write waiting for credit fails at once when the stream is aborted or stopped.
- **Datagrams** (RFC 9221) are off unless `parameters.maxDatagramFrameSize` enables them; `datagrams` is undefined until then. They go to the peer only if it enabled them too: until it did, `datagrams.maxDatagramSize` is 0. As over QUIC, a datagram may be dropped: one larger than `maxDatagramSize`, one written while the WebSocket buffers more than `sendBufferLimit`, or one that arrives while 128 wait to be read. Unlike over QUIC, one that is sent is delivered reliably and in order.
- **Parameters.** `parameters` overrides `DefaultParameters`: a 1 MiB connection window, 256 KiB per stream, 100 peer streams of each kind, 64 KiB records, no idle timeout and no datagrams. A stream's receive window moves on as the application reads, and the peer's stream limit rises as its streams end.
- **Idle timeout.** Every record received restarts it, and so does the first record sent after each (RFC 9000 §10.1), so keep-alives alone can't keep a session to a vanished peer open. While stream data or datagrams a session sent wait for the peer to be heard from, it sends a QX_PING halfway through the timeout: a live peer answers it, even when it has nothing else to send. Replies such as flow control updates don't count, so a session idle both ways sends no probe.
- **Errors.** Every failure is a `QMuxError`, shaped like `WebTransportError`: `source` is `"session"` or `"stream"`, and `streamErrorCode` is the code of a reset or stop. A session that detects a protocol error closes with a transport CONNECTION_CLOSE, and the `QMuxProtocolError` is the `cause` of the error `closed` rejects with.

Records carry every frame that is ready at once, and stream and flow control state are plain numbers, not BigInts. There are no stream priorities: [`@moq/qmux`](https://www.npmjs.com/package/@moq/qmux) has them, and older QMux drafts.

Not supported: 0-RTT, and the RESET_STREAM_AT extension (draft-02 §9.2). Neither is advertised, so peers do not use them.

## Development

```sh
npm test            # the shared golden vectors in ../../testdata, sessions against a scripted peer, and client and server sessions over in-memory WebSockets
npm run lint
npm run typecheck
```

`nix build .#trevrpc-js-qmux` runs the tests, and the `trevrpc-js-qmux-lint` check runs oxfmt, oxlint and tsc. `@trevrpc/trevrpc-js` depends on this package through the npm workspace in `trevrpc-js/package.json`; flake-release publishes this package to npm first.
