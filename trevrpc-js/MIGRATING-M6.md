# Migrating to trevrpc-js 0.2.0

Version 0.2.0 is the single breaking JavaScript release for Milestone 6. The invalid 0.1 API shapes have no deprecated aliases.

## Terminal status and errors

Successful streaming completion now returns `StreamStatus` with exactly `code`, `message`, and `metadata`.

- `StreamStatus.message` is the peer's unmodified terminal message.
- `TrevRpcError.statusMessage` is the peer's unmodified non-OK message.
- `TrevRpcError.message` is formatted JavaScript error text, such as `"Unavailable: down"`.

Replace every successful-status use of `statusMessage` with `message`.

Server-streaming results are `ResponseAsyncIterable<T>`. Fully consume the stream before awaiting `status`; consuming a valid terminal frame claims precedence and cancels the absolute deadline, while `status` settles only after trailing EOF is validated. Call `await stream.close()` when abandoning a stream. Breaking a `for await` loop invokes the same cancellation path.

RPC `timeoutMs` is now one absolute deadline covering transport setup, request upload, response receipt, and cleanup. `streamIdleTimeoutMs` remains a separate per-read inactivity bound.

## Browser and Node channels

Browser declarations accept only `BrowserChannelOptions`. Native TLS, certificate validation, MsQuic buffering, and native stream settings are Node-only type errors.

Node channels expose normalized immutable state:

```js
channel.endpoint; // { host, port }
channel.options; // effective NodeChannelOptions
```

`channel.urlOrOptions` was removed. URL and `{ host, port, ...options }` constructor forms produce equivalent state.

The package is ESM-only and conditional exports are authoritative. Legacy top-level `main` and `types` fields were removed. Use a resolver that honors package conditions: NodeNext for Node TypeScript and Bundler/browser conditions for browser builds.

## Generated modules and server responses

Generation now always emits four files:

- `name.trevrpc.js`
- `name.trevrpc.d.ts`
- `name.node.trevrpc.js`
- `name.node.trevrpc.d.ts`

Import clients and descriptors from the ordinary module. Import `register<Service>Server` only from the Node companion. Browser imports of the ordinary module contain no Node or native dependency.

Generated server handlers no longer inspect property names. A protobuf object such as `{ message: "hello" }` is always a protobuf response. Use branded helpers only when attaching metadata or a terminal streaming status:

```js
import { createStreamingResponse, createUnaryResponse } from "trevrpc-js";

return createUnaryResponse({ message: "hello" }, { trailer: "value" });
return createStreamingResponse(messages, {
  code: Code.Ok,
  message: "",
  metadata: { trailer: "value" },
});
```

Client-streaming handlers return one protobuf response or `createUnaryResponse(...)`; the dispatcher sends exactly one message followed by one terminal streaming status.

Custom generator runtime URLs such as `file:` require explicit `runtime_type_import` and `node_runtime_type_import` options. Node runtime imports are never derived by appending `/node` to another specifier.

## Native package and diagnostics

The main `trevrpc-js` tarball is platform-neutral. Native code is supplied by the exact optional dependency `@trev/trevrpc-js-native-linux-x64-gnu@0.2.0`.

Initial native support is Linux x86-64 with glibc 2.42 or newer on Node 24. Darwin, ARM, and musl installation remains usable for browser code; requesting a native Node channel fails with a `TrevRpcError(Code.Unavailable)` that names the detected target, supported target, expected optional package, load state, and original loader cause.

Native failures exposed by Node transport and server APIs are normalized to `TrevRpcError`. `cause` preserves the native error and `nativeCode` preserves its numeric code. A failed operation is never replayed during channel reconnection.

## Node handler cancellation

`NodeServerCall` now exposes `signal` and `deadline`. The signal covers effective deadlines, server shutdown, local close, and cancellation observed by native send or receive operations.

The current native bridge cannot promptly report remote peer cancellation while a handler is idle and performs no call I/O. Cancellation becomes observable when the handler next enters native call I/O.
