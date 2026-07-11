# trevrpc-js

The examples use JavaScript bindings generated from
[`examples/greeter/greeter.proto`](examples/greeter/greeter.proto). Its Greeter service defines
unary, server-streaming, client-streaming, and bidirectional-streaming methods.

## Client

### Browser application

Create a reconnecting transport and generated client once, then reuse them across calls:

```js
import { ReconnectingWebTransportClient } from "trevrpc-js";
import { GreeterClient } from "./greeter.trevrpc.js";

const transport = await ReconnectingWebTransportClient.connect("https://localhost:50051/trevrpc");
try {
  const client = new GreeterClient(transport, { timeoutMs: 5_000 });
  const reply = await client.sayHello({ name: "TrevRPC" });
} finally {
  transport.close({ closeCode: 0, reason: "client shutdown" });
}
```

Close the transport with `transport.close({ closeCode: 0, reason: "done" })` after all calls have
completed, preferably from an application-level `finally` block. Managed transports own persistent
reconnect loops and must be closed during shutdown.

The static `connect` helpers wait indefinitely for the first ready generation while retrying failed
initial connections. Applications that require bounded startup should construct the reconnecting
transport directly, race `waitUntilReady()` against their startup deadline, and call `close()` when
that deadline expires. The complete browser example demonstrates this pattern.

### Unary

```js
const reply = await client.sayHello({ name: "TrevRPC" });
console.log(reply.message);
```

### Server streaming

The generated method returns an async iterable. Completing the loop consumes the terminal RPC
status:

```js
const replies = await client.lotsOfReplies({ name: "TrevRPC" });
for await (const reply of replies) {
  console.log(reply.message);
}
```

### Client streaming

```js
const greetings = await client.lotsOfGreetings();
await greetings.send({ name: "Alice" });
await greetings.send({ name: "Bob" });

const reply = await greetings.closeAndRecv();
console.log(reply.message);
```

`closeAndRecv` half-closes the request side, receives the single response, validates the terminal
status, and releases the call.

### Bidirectional streaming

Requests and responses may be interleaved. `closeSend` half-closes the request side; continue
receiving until `recv` returns `undefined`:

```js
const stream = await client.bidiHello();
try {
  for (const name of ["Alice", "Bob"]) {
    await stream.send({ name });
    const reply = await stream.recv();
    if (reply == null) {
      throw new Error("BidiHello response stream ended early");
    }
    console.log(reply.message);
  }

  await stream.closeSend();
  for (;;) {
    const reply = await stream.recv();
    if (reply == null) {
      break;
    }
    console.log(reply.message);
  }
} finally {
  await stream.close();
}
```

Use independent sender and receiver tasks when a protocol must continuously read and write large
bidi streams.

### Browser transport options

Browsers pass WebTransport constructor options directly to
`ReconnectingWebTransportClient.connect`:

```js
const transport = await ReconnectingWebTransportClient.connect("https://localhost:50051/trevrpc", {
  congestionControl: "low-latency",
  serverCertificateHashes: [{ algorithm: "sha-256", value: certificateHash }],
});
try {
  const client = new GreeterClient(transport);
  const reply = await client.sayHello({ name: "TrevRPC" });
} finally {
  transport.close({ closeCode: 0, reason: "client shutdown" });
}
```

### Node application

Node applications use the reconnecting native QUIC transport:

```js
import { ReconnectingNodeTransport } from "trevrpc-js/node";
import { GreeterClient } from "./greeter.trevrpc.js";

const transport = await ReconnectingNodeTransport.connect("https://localhost:50051");
try {
  const client = new GreeterClient(transport, { timeoutMs: 5_000 });
  const reply = await client.sayHello({ name: "TrevRPC" });
} finally {
  transport.close();
}
```

Call `transport.close()` during application shutdown. The native low-level client does not expose an
idle connection-closed signal, so an idle Node connection loss is first observed by the next RPC.
That RPC fails without replay and starts background reconnection for later calls.

### Low-level transports

`WebTransportClient` wraps an established browser WebTransport session, while `NodeTransport`
opens one native connection. These low-level paths do not reconnect automatically:

```js
import { WebTransportClient } from "trevrpc-js";

const session = new WebTransport("https://localhost:50051/trevrpc");
await session.ready;
const browserTransport = new WebTransportClient(session);
```

```js
import { NodeTransport } from "trevrpc-js/node";

const nodeTransport = await NodeTransport.connect("https://localhost:50051");
```

## Server

Register the generated service descriptor with `NodeServer`. Raw Node handlers decode request
bodies and encode response messages with the generated protobuf root:

```js
import { RpcStreamFrameKind } from "trevrpc-js";
import { NodeServer } from "trevrpc-js/node";
import { GreeterService, root } from "./greeter.trevrpc.js";

const HelloRequest = root.lookupType("example.greeter.HelloRequest");
const HelloReply = root.lookupType("example.greeter.HelloReply");

function encodeReply(message) {
  return HelloReply.encode({ message }).finish();
}
```

### Unary

```js
function sayHello(call) {
  const request = HelloRequest.decode(call.request.body);
  return encodeReply(`Hello, ${request.name || "world"}`);
}
```

### Server streaming

Return an async iterable of encoded response messages:

```js
async function* lotsOfReplies(call) {
  const request = HelloRequest.decode(call.request.body);
  const name = request.name || "world";
  yield encodeReply(`Hello, ${name}`);
  yield encodeReply(`Goodbye, ${name}`);
}
```

### Client streaming

Receive request frames until EOF, then yield the single encoded response:

```js
async function* lotsOfGreetings(call) {
  const names = [];
  for (;;) {
    const frame = await call.recv();
    if (frame == null) {
      break;
    }
    if (frame.kind === RpcStreamFrameKind.Message) {
      names.push(HelloRequest.decode(frame.body).name);
    }
  }
  yield encodeReply(`Hello, ${names.join(", ")}`);
}
```

### Bidirectional streaming

An async generator can consume request frames and emit encoded responses with natural
backpressure:

```js
async function* bidiHello(call) {
  for (;;) {
    const frame = await call.recv();
    if (frame == null) {
      return;
    }
    if (frame.kind === RpcStreamFrameKind.Message) {
      const request = HelloRequest.decode(frame.body);
      yield encodeReply(`Stream hello, ${request.name}`);
    }
  }
}
```

Create the native Node server and register all four handlers. `NodeServer` sends each message from
the returned async iterables and appends an OK terminal status:

```js
const server = await NodeServer.listen({
  host: "127.0.0.1",
  port: 50051,
  certFile: "localhost-cert.pem",
  keyFile: "localhost-key.pem",
  enableHttp3: true,
  http3Path: "/trevrpc",
});

server.registerService(GreeterService, {
  sayHello,
  lotsOfReplies,
  lotsOfGreetings,
  bidiHello,
});

await server.serve();
```

`enableHttp3` adds ordinary HTTP/3 POST serving to the same native MsQuic listener used by native
QUIC and WebTransport. An optional synchronous `http3Admission` callback is bounded by
`initialRequestTimeoutMs`.

See [`examples/greeter/client.js`](examples/greeter/client.js) for a complete browser client.

## Native Development

`npm run build:native` creates the production addon without test-only debug hooks. Build the test
addon before running the native tests:

```sh
npm run build:native:test
npm test
```

The completion-worker profiler is manual and requires the benchmark server binary plus an explicit
case, concurrency, iteration count, and payload size:

```sh
npm run profile:completion-worker:native -- \
  <server-binary> <unary|bidi-duplex> <concurrency> <iterations> <payload-bytes>
```

The profiler reports wall throughput, Node CPU time per operation, RSS, and context switches.
