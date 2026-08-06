# trevrpc-js

`trevrpc-js` is the ESM-only JavaScript runtime and protobuf.js generator for TrevRPC.

## Generated files

For every protobuf file containing services, `protoc-gen-trevrpc-js` emits an ordinary browser-safe client module and a separate Node server module:

```text
greeter.trevrpc.js
greeter.trevrpc.d.ts
greeter.node.trevrpc.js
greeter.node.trevrpc.d.ts
```

The ordinary module contains the protobuf root, service descriptors, typed clients, and client factories. The Node companion contains typed handler interfaces and registration helpers backed by `@trevrpc/trevrpc-js/node/generated`.

```sh
protoc \
  --plugin=protoc-gen-trevrpc-js=./node_modules/.bin/protoc-gen-trevrpc-js \
  --trevrpc-js_out=. \
  greeter.proto
```

Generator options are `runtime_import`, `runtime_type_import`, `node_runtime_import`, `node_runtime_type_import`, and `file_suffix`. Runtime type imports default to their matching runtime import for bare and relative specifiers. URL specifiers such as `file:` require an explicit type import because TypeScript NodeNext does not resolve them in declarations. A Node import is never inferred by appending `/node` to another specifier.

## Browser clients

Browser declarations accept only `BrowserChannelOptions`.

```js
import { connect } from "@trevrpc/trevrpc-js";
import { GreeterClient } from "./greeter.trevrpc.js";

const channel = await connect("https://localhost:50051/trevrpc", {
  timeoutMs: 10_000,
  congestionControl: "low-latency",
  serverCertificateHashes: [{ algorithm: "sha-256", value: certificateHash }],
});
try {
  const client = new GreeterClient(channel, { timeoutMs: 5_000 });
  const reply = await client.sayHello({ name: "TrevRPC" });
  console.log(reply.message);
} finally {
  channel.close({ closeCode: 0, reason: "client shutdown" });
}
```

Native TLS bypass, certificate validation, MsQuic buffering, and native stream options are intentionally unavailable under browser or Bundler resolution.

## Node clients

Under Node conditions, the root package uses the native channel. The explicit `@trevrpc/trevrpc-js/node` subpath also exports `Channel`, `NodeServer`, and `NodeServerCall`.

```js
import { connect } from "@trevrpc/trevrpc-js";
import { GreeterClient } from "./greeter.trevrpc.js";

const channel = await connect("https://localhost:50051", {
  timeoutMs: 10_000,
  caCertFile: "localhost-cert.pem",
});
try {
  console.log(channel.endpoint); // frozen { host, port }
  console.log(channel.options); // frozen effective NodeChannelOptions
  const client = new GreeterClient(channel, { timeoutMs: 5_000 });
  console.log((await client.sayHello({ name: "TrevRPC" })).message);
} finally {
  channel.close();
}
```

URL and `{ host, port, ...options }` forms normalize to the same public state. Channels reconnect for future calls after native transport loss, but never replay or resume an operation that already failed.

## Streaming clients

Server-streaming methods resolve to `ResponseAsyncIterable<T>`:

```js
const replies = await client.lotsOfReplies({ name: "TrevRPC" });
try {
  for await (const reply of replies) {
    console.log(reply.message);
  }
  const terminal = await replies.status;
  console.log(terminal.code, terminal.message, terminal.metadata);
} finally {
  await replies.close();
}
```

Full clean consumption resolves `status`. A non-OK terminal status rejects iteration and `status` with the same `TrevRpcError`. `close()`, iterator `return()`, and breaking a `for await` loop cancel the stream and reject `status` with `Code.Cancelled`. Consuming a valid terminal frame claims terminal precedence and cancels the absolute deadline, but `status` settles only after trailing EOF is validated; merely buffering a status frame is not completion.

The status/error text distinction is deliberate:

- `StreamStatus.message` is the unmodified peer terminal message.
- `TrevRpcError.statusMessage` is the unmodified peer error message.
- `TrevRpcError.message` is formatted JavaScript text such as `"Unavailable: down"`.

Unary and client-streaming terminal metadata is available through `sayHelloWithResponse()` and `closeAndRecvWithResponse()`.

Client streaming:

```js
const call = await client.lotsOfGreetings();
await call.send({ name: "Alice" });
await call.send({ name: "Bob" });
const { message, metadata } = await call.closeAndRecvWithResponse();
```

Bidirectional streaming:

```js
const call = await client.bidiHello();
try {
  await call.send({ name: "Alice" });
  console.log(await call.recv());
  await call.closeSend();
  while ((await call.recv()) != null) {}
  console.log(await call.status);
} finally {
  await call.close();
}
```

`timeoutMs` is one absolute RPC deadline that begins before transport setup and covers upload, send, receive, and cleanup. The same `DeadlineExceeded` object wins signal and rejection settlement. `streamIdleTimeoutMs` remains an independent per-read inactivity bound.

## Typed Node servers

Import the generated registration helper from the Node companion. Direct protobuf responses are never confused with metadata envelopes, even when they have `message` or `messages` properties.

```js
import { Code, createStreamingResponse, createUnaryResponse } from "@trevrpc/trevrpc-js";
import { NodeServer } from "@trevrpc/trevrpc-js/node";
import { registerGreeterServer } from "./greeter.node.trevrpc.js";

const server = await NodeServer.listen({
  host: "127.0.0.1",
  port: 50051,
  certFile: "localhost-cert.pem",
  keyFile: "localhost-key.pem",
});

registerGreeterServer(server, {
  sayHello(request, call) {
    console.log(call.deadline, call.signal.aborted);
    return createUnaryResponse(
      { message: `Hello, ${request.name || "world"}` },
      { trailer: "unary" },
    );
  },

  async lotsOfGreetings(requests) {
    const names = [];
    for await (const request of requests) names.push(request.name);
    return createUnaryResponse({ message: names.join(", ") }, { trailer: "client-stream" });
  },

  lotsOfReplies(request) {
    return createStreamingResponse([{ message: request.name }], {
      code: Code.Ok,
      message: "complete",
      metadata: { trailer: "server-stream" },
    });
  },

  async *bidiHello(requests) {
    for await (const request of requests) yield { message: request.name };
  },
});

await server.serve();
```

Client-streaming completion uses streaming wire semantics: exactly one response message followed by exactly one terminal status; it never uses unary `respond()`.

`NodeServerCall.signal` covers effective deadlines, server shutdown, local close, and cancellation observed by native I/O. The current native bridge cannot promptly report remote peer cancellation while a handler is idle and performs no call I/O; cancellation becomes observable when the handler next enters native call I/O.

## Native package support

The core tarball is platform-neutral. Native code is supplied by the exact optional package `@trevrpc/trevrpc-js-native-linux-x64-gnu@0.1.1`.

Initial support is Linux x86-64, glibc 2.42 or newer, and Node 24. Browser usage still installs and runs on Darwin, ARM, and musl. Requesting a native Node channel on an unsupported or incomplete installation throws `TrevRpcError(Code.Unavailable)` with the detected target, supported targets, expected optional package, whether loading was missing or failed, and the original loader error as `cause`.

All addon errors crossing public Node transport and server boundaries become `TrevRpcError` unless already normalized. `nativeCode` and `cause` preserve native provenance.

## Package resolution

Conditional ESM exports are authoritative; the package has no legacy top-level `main` or `types`. Use TypeScript NodeNext resolution for Node consumers and Bundler resolution with browser conditions for browser consumers.

## Native development

Installation never builds native code. In a source checkout:

```sh
npm run build:native:test
npm test
npm run build:native
npm run verify:native:production
```

The development fallback is only `build/native/trevrpc_native.node` in a source checkout.
