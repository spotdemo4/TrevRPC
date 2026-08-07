# trevrpc-js

TrevRPC is an RPC framework like gRPC, but uses QUIC (and HTTP/3 / WebTransport) instead of HTTP/2. Define services in protobuf, generate typed clients and servers, and run them over QUIC.

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

Generate with `protoc-gen-trevrpc-js` (`--trevrpc-js_out`).

## Client

```js
import { connect } from "@trevrpc/trevrpc-js";
import { GreeterClient } from "./greeter.trevrpc.js";

const channel = await connect("https://127.0.0.1:50051/trevrpc");
const client = new GreeterClient(channel);

// Unary
const reply = await client.sayHello({ name: "TrevRPC" });

// Server streaming
const stream = await client.lotsOfReplies({ name: "TrevRPC" });
for await (const r of stream) console.log(r.message);

// Client streaming
const cs = await client.lotsOfGreetings();
await cs.send({ name: "Alice" });
await cs.send({ name: "Bob" });
const summary = await cs.closeAndRecv();

// Bidirectional streaming
const bidi = await client.bidiHello();
await bidi.send({ name: "Alice" });
console.log(await bidi.recv());
await bidi.closeSend();
for await (const r of bidi) console.log(r.message);

channel.close();
```

Node uses the same API via `@trevrpc/trevrpc-js/node` (native MsQuic).

## Server (Node)

```js
import { NodeServer } from "@trevrpc/trevrpc-js/node";
import { registerGreeterServer } from "./greeter.node.trevrpc.js";
import { createUnaryResponse, createStreamingResponse } from "@trevrpc/trevrpc-js";

const server = await NodeServer.listen({
  host: "127.0.0.1",
  port: 50051,
  certFile: "cert.pem",
  keyFile: "key.pem",
});

registerGreeterServer(server, {
  sayHello(req) {
    return createUnaryResponse({ message: `hello, ${req.name}` });
  },
  lotsOfReplies(req) {
    return createStreamingResponse([
      { message: `hello, ${req.name}` },
      { message: `goodbye, ${req.name}` },
    ]);
  },
  async lotsOfGreetings(reqs) {
    const names = [];
    for await (const r of reqs) names.push(r.name);
    return createUnaryResponse({ message: names.join(", ") });
  },
  async *bidiHello(reqs) {
    for await (const r of reqs) yield { message: `hello, ${r.name}` };
  },
});

await server.serve();
```
