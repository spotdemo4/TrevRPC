# trevrpc-js

Minimal unary Greeter flow using generated JavaScript bindings.

## Client

Create a transport with `connect`, create the generated client, and send a request:

```js
import { connect } from "trevrpc-js";
import { GreeterClient } from "./greeter.trevrpc.js";

const transport = await connect("https://localhost:50051/trevrpc");
try {
  const client = new GreeterClient(transport, { timeoutMs: 5000 });
  const reply = await client.sayHello({ name: "TrevRPC" });
  console.log(reply.message);
} finally {
  transport.close({ closeCode: 0, reason: "done" });
}
```

Browsers pass WebTransport constructor options directly to `connect`:

```js
const transport = await connect("https://localhost:50051/trevrpc", {
  congestionControl: "low-latency",
  serverCertificateHashes: [{ algorithm: "sha-256", value: certificateHash }],
});
```

Node.js uses the native transport when imported from the package entrypoint.

## Server

Create a Node server, register the generated service descriptor, decode the request body, and return an encoded reply:

```js
import { NodeServer } from "trevrpc-js/node";
import { GreeterService, root } from "./greeter.trevrpc.js";

const HelloRequest = root.lookupType("example.greeter.HelloRequest");
const HelloReply = root.lookupType("example.greeter.HelloReply");

const server = await NodeServer.listen({
  host: "127.0.0.1",
  port: 50051,
  certFile: "localhost-cert.pem",
  keyFile: "localhost-key.pem",
});

server.registerService(GreeterService, {
  sayHello(call) {
    const request = HelloRequest.decode(call.request.body);
    return HelloReply.encode({ message: `Hello, ${request.name || "world"}` }).finish();
  },
});

await server.serve();
```

## Native Development

`npm run build:native` creates the production addon without test-only debug hooks. Build the
test addon before running the native tests:

```sh
npm run build:native:test
npm test
```

The completion-worker profiler is manual and requires the benchmark server binary plus an
explicit case, concurrency, iteration count, and payload size:

```sh
npm run profile:completion-worker:native -- \
  <server-binary> <unary|bidi-duplex> <concurrency> <iterations> <payload-bytes>
```

The profiler reports wall throughput, Node CPU time per operation, RSS, and context switches.
