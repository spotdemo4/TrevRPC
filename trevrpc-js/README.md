# trevrpc-js

JavaScript WebTransport client runtime and `protoc`/buf plugin for TrevRPC.

## Runtime

Browser/WebTransport clients can use the pure JavaScript transport:

```js
import { WebTransportClient } from "trevrpc-js";
import { GreeterClient } from "./hello/v1/greeter.trevrpc.js";

const transport = await WebTransportClient.connect("https://localhost:50051/trevrpc");
const client = new GreeterClient(transport, { timeoutMs: 5000 });

const reply = await client.sayHello({ name: "Trev" });
```

Node.js clients can use the native Node-API transport backed by `trevrpc-c` and MsQuic:

```js
import { NodeTransport } from "trevrpc-js/node";
import { GreeterClient } from "./hello/v1/greeter.trevrpc.js";

const transport = await NodeTransport.connectWebTransport("https://127.0.0.1:50051/trevrpc", {
  skipCertificateValidation: true,
});
const client = new GreeterClient(transport);
```

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
