# trevrpc-js

JavaScript WebTransport client runtime and `protoc`/buf plugin for TrevRPC.

## Runtime

```js
import { WebTransportTransport } from "trevrpc-js";
import { GreeterClient } from "./hello/v1/greeter.trevrpc.js";

const transport = await WebTransportTransport.connect("https://localhost:50051/trevrpc");
const client = new GreeterClient(transport, { timeoutMs: 5000 });

const reply = await client.sayHello({ name: "Trev" });
```

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
