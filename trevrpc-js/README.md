# trevrpc-js

TrevRPC is an RPC framework like gRPC, but uses QUIC (and HTTP/3 / WebTransport) instead of HTTP/2. Define services in protobuf, generate typed clients, and call Go or Rust servers over WebTransport, falling back to QMux over WebSocket where WebTransport is missing or UDP is blocked.

`trevrpc-js` is the **browser client**: a WebTransport runtime with a QMux over WebSocket fallback, plus `protoc-gen-trevrpc-js`, which generates typed clients (no server code). It has no Node.js runtime; servers are written in Go or Rust, native QUIC clients in Go or Rust, and HTTP/3 clients in Kotlin.

Full documentation: https://trev.zip/llc/TrevRPC/wiki

## Protobuf

```proto
syntax = "proto3";

package example.greeter;

service Greeter {
  rpc SayHello(HelloRequest) returns (HelloReply);
  rpc LotsOfReplies(HelloRequest) returns (stream HelloReply);
  rpc LotsOfGreetings(stream HelloRequest) returns (HelloReply);
  rpc BidiHello(stream HelloRequest) returns (stream HelloReply);
}

message HelloRequest { string name = 1; }
message HelloReply { string message = 1; }
```

Generate a client with the protoc plugin (it needs Node.js to run):

```sh
npm install @trevrpc/trevrpc-js
protoc \
  --plugin=protoc-gen-trevrpc-js=node_modules/.bin/protoc-gen-trevrpc-js \
  --trevrpc-js_out=src/gen \
  greeter.proto
```

This writes `greeter.trevrpc.js` and `greeter.trevrpc.d.ts`. Plugin parameters (`--trevrpc-js_out=key=value,key=value:dir`):

| Parameter             | Default                  | Meaning                                                                            |
| --------------------- | ------------------------ | ---------------------------------------------------------------------------------- |
| `runtime_import`      | `@trevrpc/trevrpc-js`    | Module specifier the generated JavaScript imports.                                 |
| `runtime_type_import` | same as `runtime_import` | Module specifier the declarations import; required when `runtime_import` is a URL. |
| `file_suffix`         | `.trevrpc.js`            | Suffix of the generated files.                                                     |

## Client

```js
import { connect } from "@trevrpc/trevrpc-js";

import { GreeterClient } from "./gen/greeter.trevrpc.js";

const channel = await connect("https://example.com:4433/trevrpc");
const client = new GreeterClient(channel, { timeoutMs: 5_000 });

// Unary
const reply = await client.sayHello({ name: "TrevRPC" });

// Server streaming
const replies = await client.lotsOfReplies({ name: "TrevRPC" });
for await (const reply of replies) console.log(reply.message);

// Client streaming
const upload = await client.lotsOfGreetings();
await upload.send({ name: "Alice" });
await upload.send({ name: "Bob" });
const summary = await upload.closeAndRecv();

// Bidirectional streaming
const chat = await client.bidiHello();
await chat.send({ name: "Alice" });
console.log(await chat.recv());
await chat.closeSend();
for await (const reply of chat) console.log(reply.message);

channel.close();
```

### Call options

Every method takes `CallOptions` as its last argument; the same options can be given to the client constructor as defaults, and per-call `metadata` is merged over the defaults per key.

| Option         | Meaning                                                                                       |
| -------------- | --------------------------------------------------------------------------------------------- |
| `timeoutMs`    | Deadline for the whole call, from the request to the final status. Also sent to the server.   |
| `signal`       | `AbortSignal` that cancels the call.                                                          |
| `metadata`     | Request metadata (`{ key: "value" }` or a `Map`); keys are lowercase, `trevrpc-` is reserved. |
| `maxFrameSize` | Largest frame accepted or sent in bytes (default 4 MiB).                                      |

The client enforces no other response limits: there is no cap on the number of streamed
messages, their total size, or idle time between them. Use `timeoutMs` or `signal` to bound a
call.

### Errors, cancellation and streams

- Failed calls throw `TrevRpcError` with a `code` (`Code.NotFound`, ...), `statusMessage` and `metadata`. A deadline surfaces as `Code.DeadlineExceeded`, a cancelled call as `Code.Cancelled` (with the abort reason as `cause`), and a lost connection as `Code.Unavailable`.
- Streaming responses end with the server's final status: iteration finishes after `OK` and throws the `TrevRpcError` otherwise. `stream.status` settles with the same outcome and carries trailing metadata. Leaving a `for await` loop early, or calling `close()`, cancels the call.
- Response headers and trailing metadata are separate. `...WithResponse(request)` (unary) and `closeAndRecvWithResponse()` (client streaming) return `{ message, headers, metadata }`, where `headers` are the server's response headers (empty when it sent none) and `metadata` is the trailing metadata of the final status. Server-streaming and bidirectional calls have a `headers` promise that resolves as soon as the first response frame arrives, so headers are available before the first message, and rejects with the call's error if the call fails first. Reading `headers` is optional.
- Frames follow the [wire format](https://trev.zip/llc/TrevRPC/wiki/Protocol-and-Wire-Format): a type, a length and a payload, with messages sent as raw protobuf. The client skips frame types it does not know, and `maxFrameSize` limits every frame, unknown ones included.
- The server's response decides the outcome of a call, and a call reports it as soon as the final status arrives. If the server ends a call before the upload is done (client-streaming and bidirectional calls), the upload is cancelled and `send()` rejects at once, without waiting for the server to stop reading and whether or not the caller receives:
  - with the server's `TrevRpcError` if its status is not OK, and the same error is the call's outcome (`closeAndRecv()`, `recv()`, `status`);
  - with `Code.Cancelled` ("the server completed the call before the request stream ended") if its status is `OK`, for instance a server that answers after the first messages. The call itself succeeded: `closeAndRecv()` returns the response, and `recv()` and `status` end with `OK`;
  - with the failure of the response stream, for instance `Code.Unavailable` for a lost connection, if the server stopped reading the request and the response then failed. A bare "stream closed" error from the transport is used only when the response gives no other explanation.

  `send()` calls that are blocked on backpressure fail the same way, and so do all sends of a call that is cancelled or whose deadline expires. A deadline or abort that fires after the final status arrived does not change the outcome, even if the server has not finished the response stream yet.

- To find the final status while the caller only sends, a client-streaming or bidirectional call reads the response ahead of `recv()` while its request side is open: at most 32 frames or 1 MiB (plus one frame of at most `maxFrameSize`) are held; beyond that reading pauses and flow control applies until `recv()` takes messages. Responses read ahead are delivered in order, including before an error that ended the stream.

### Connections

`connect()` opens a session and resolves when it is ready; it rejects if no transport can establish one. After that the `Channel` re-establishes the session in the background with exponential backoff whenever it is lost. Calls fail fast with `Unavailable` while `channel.state` is `"reconnecting"` and are never replayed; use `await channel.waitUntilReady()` or the `onStateChange` option to follow the state. `channel.close()` stops reconnecting.

```js
const channel = await connect(url, {
  signal: AbortSignal.timeout(10_000), // cancels the initial connection only
  serverCertificateHashes: [{ algorithm: "sha-256", value: certificateSha256 }],
  onStateChange: ({ state, attempt, error }) => console.log(state, attempt, error),
});
```

`serverCertificateHashes` trusts a self-signed ECDSA certificate that is valid for at most 14 days by its SHA-256 hash, which is how development servers and the interop tests are reached. It applies to WebTransport only: the WebSocket fallback needs a certificate the browser trusts. `allowPooling`, `congestionControl` and `requireUnreliable` are passed to the `WebTransport` constructor.

Any object with `createBidirectionalStream()`, including a plain `WebTransport` session, can be used as the transport of a generated client.

### WebSocket fallback

`connect()` races two transports. WebTransport starts first; QMux over WebSocket (subprotocol `qmux-02.trevrpc-1`, served by Go and Rust on the server's TCP port) starts after `fallbackDelayMs` (default 300) or immediately if WebTransport fails or the browser has no `WebTransport` API. The first session to be ready wins and the other is closed. After the WebSocket wins, reconnects use it directly until `reprobeIntervalMs` (default 5 minutes) has passed since WebTransport was lost, then race again.

```js
const channel = await connect("https://example.com:4433/trevrpc", {
  transports: ["webtransport", "websocket"], // default; the first is preferred, a single entry disables the race
  webSocketUrl: "wss://example.com:4433/trevrpc", // default: `url` with the wss: scheme
  fallbackDelayMs: 300,
  reprobeIntervalMs: 5 * 60_000,
  onStateChange: ({ state, transport }) => console.log(state, transport),
});
console.log(channel.transport); // "webtransport" or "websocket"
```

`channel.transport` and `ChannelStateChange.transport` (set when the channel becomes ready) report which one is in use. `WebSocket` replaces the global WebSocket constructor, like `WebTransport` does for WebTransport. The QMux client is in-tree (`src/qmux/`); WebSocket backpressure polls `bufferedAmount` against a 1 MiB limit.

## Interop client

`trevrpc-interop-browser` runs the cross-runtime interop scenarios from [`interop/README.md`](../interop/README.md) in a real browser over WebTransport, QMux over WebSocket, or the fallback between them. It is built from `interop/proto/interop.proto` with the generator in this package, so it also serves as a complete example of generated client code.

```sh
trevrpc-interop-browser --browser chromium|firefox|safari \
  --url https://127.0.0.1:4433/trevrpc \
  --cert-hash <hex sha256 of the server certificate DER> \
  [--transport webtransport|websocket|auto] [--ws-url wss://127.0.0.1:4433/trevrpc] \
  [--expect-transport webtransport|websocket]
```

It prints `ok <name>` or `fail <name>: <reason>` per scenario and exits 0 only if all scenarios passed. Chromium and Firefox are launched through Playwright from `PLAYWRIGHT_BROWSERS_PATH`, or from `TREVRPC_BROWSER_CHROMIUM` and `TREVRPC_BROWSER_FIREFOX` when those name an executable, and ignore certificate errors because WSS cannot pin a certificate hash. Safari is driven through `safaridriver` (`TREVRPC_BROWSER_SAFARI` overrides its path), which needs `sudo safaridriver --enable` once; Safari cannot ignore certificate errors, so the server certificate must be trusted by the system, as `interop/run.sh` does. Safari passes every lane against the Go server and the WebSocket lanes against the Rust server, but not yet Rust's WebTransport lane ([h3#347](https://github.com/hyperium/h3/issues/347)). The CLI serves the page from a loopback origin; the page fetches its input from there and posts each result back, so where the browser has no WebTransport at all every scenario fails with a clear reason.

## Development

```sh
npm ci
npm run check      # format check, lint, typecheck, tests
npm run build      # generate the Echo client and bundle the interop page into build/interop/
npm run interop -- --browser chromium --url ... --cert-hash ...
```

`nix build .#trevrpc-js` runs the same checks and installs the library, the plugin and the wrapped `trevrpc-interop-browser` with matching Playwright browsers. The pinned `playwright` version must equal nixpkgs' `playwright-driver`; `nix run .#sync-playwright` updates it.

Tests (`node --test`) cover the wire format against `testdata/wire-golden-vectors.txt` (frame bytes, and the frame reader against the shared byte streams), the QMux frames against `testdata/qmux-golden-vectors.txt`, framing, the generator, and the client — deadlines, cancellation, stream termination and reconnection — against in-memory fake servers, including a fake Echo server that runs all interop scenarios.
