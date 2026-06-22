# trevrpc-js

JavaScript WebTransport client runtime and `protoc`/buf plugin for TrevRPC.

## Runtime

Clients can use the unified WebTransport connector. It uses the native `trevrpc-c`-backed transport in Node.js and the browser `WebTransport` API in browsers:

```js
import { connectWebTransport } from "trevrpc-js";
import { GreeterClient } from "./hello/v1/greeter.trevrpc.js";

const transport = await connectWebTransport("https://localhost:50051/trevrpc");
const client = new GreeterClient(transport, { timeoutMs: 5000 });

const reply = await client.sayHello({ name: "Trev" });
```

Browser clients can pass native `WebTransport` constructor options directly:

```js
import { connectWebTransport } from "trevrpc-js";
import { GreeterClient } from "./hello/v1/greeter.trevrpc.js";

const transport = await connectWebTransport("https://localhost:50051/trevrpc", {
  serverCertificateHashes: [certificateHash],
});
const client = new GreeterClient(transport);
```

Node.js clients can pass native transport options such as `skipCertificateValidation` as the second argument. The explicit `WebTransportClient` and `NodeTransport` classes remain available when callers need direct transport-class access.

Node.js servers can use the native server wrapper from the same subpath:

```js
import { NodeServer } from "trevrpc-js/node";
import { GreeterService, root } from "./hello/v1/greeter.trevrpc.js";

const HelloRequest = root.lookupType("hello.v1.HelloRequest");
const HelloReply = root.lookupType("hello.v1.HelloReply");

const server = await NodeServer.listenWebTransport({
  host: "127.0.0.1",
  port: 50051,
  path: "/trevrpc",
  certFile: "localhost-cert.pem",
  keyFile: "localhost-key.pem",
});

server.registerService(GreeterService, {
  sayHello(call) {
    const request = HelloRequest.decode(call.request.body);
    return HelloReply.encode({ message: `Hello ${request.name}` }).finish();
  },
});

void server.serve();
```

Build the native addon from a repository checkout with `npm run build:native`. Set `TREVRPC_JS_NATIVE=/path/to/trevrpc_native.node` to load an explicit addon build.

Streaming methods use async iterables:

```js
const replies = await client.lotsOfReplies({ name: "Trev" });
for await (const reply of replies) {
  console.log(reply.message);
}
```

## Code Generation

The package installs `protoc-gen-trevrpc-js`:

```yaml
version: v2
plugins:
  - local: protoc-gen-trevrpc-js
    out: generated
    opt:
      - runtime_import=trevrpc-js
```

Generated files embed a protobuf.js reflection root for the request and response messages used by the service clients, so they can encode plain JavaScript objects directly.

The generator also emits a companion `.d.ts` file for each generated `.js` file. TypeScript projects can import the same generated JavaScript module and get typed service clients:

```ts
import { GreeterClient } from "./hello/v1/greeter.trevrpc.js";
import type { HelloRequest } from "./hello/v1/greeter.trevrpc.js";

const request: HelloRequest = { name: "Trev" };
const reply = await client.sayHello(request);
```
