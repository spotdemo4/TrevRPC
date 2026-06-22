import { createRoot, createServiceClient } from "../src/index.js";
import { NodeServer, NodeTransport } from "../src/node.js";

const StreamMessageCount = 16;
const RequestName = "TrevRPC benchmark";
const IdleTimeoutMs = 600_000;

const root = createRoot({
  nested: {
    example: {
      nested: {
        greeter: {
          nested: {
            HelloRequest: {
              fields: {
                name: { type: "string", id: 1 },
              },
            },
            HelloReply: {
              fields: {
                message: { type: "string", id: 1 },
              },
            },
          },
        },
      },
    },
  },
});

const HelloRequest = root.lookupType("example.greeter.HelloRequest");
const HelloReply = root.lookupType("example.greeter.HelloReply");
const GreeterService = Object.freeze({
  name: "Greeter",
  fullName: "example.greeter.Greeter",
  exportName: "GreeterService",
  methods: {
    sayHello: {
      name: "SayHello",
      kind: "unary",
      inputType: "example.greeter.HelloRequest",
      outputType: "example.greeter.HelloReply",
    },
    lotsOfReplies: {
      name: "LotsOfReplies",
      kind: "serverStreaming",
      inputType: "example.greeter.HelloRequest",
      outputType: "example.greeter.HelloReply",
    },
    lotsOfGreetings: {
      name: "LotsOfGreetings",
      kind: "clientStreaming",
      inputType: "example.greeter.HelloRequest",
      outputType: "example.greeter.HelloReply",
    },
    bidiHello: {
      name: "BidiHello",
      kind: "bidirectionalStreaming",
      inputType: "example.greeter.HelloRequest",
      outputType: "example.greeter.HelloReply",
    },
  },
});

const mode = process.argv[2];
if (mode === "client") {
  const host = process.argv[3] ?? "127.0.0.1";
  const port = positiveInteger(process.argv[4]);
  const iterations = positiveInteger(process.argv[5] ?? "1000");
  await runClient(host, port, iterations);
} else if (mode === "server") {
  const certFile = requiredArg(3, "certFile");
  const keyFile = requiredArg(4, "keyFile");
  await runServer(certFile, keyFile);
} else {
  throw new Error(
    "usage: node bench/rpc_split_native.js client <host> <port> <iterations> | server <certFile> <keyFile>",
  );
}

async function runClient(host, port, iterations) {
  const transport = await NodeTransport.connect({
    host,
    port,
    maxStreamsPerSession: 128,
    idleTimeoutMs: IdleTimeoutMs,
  });
  try {
    const client = createServiceClient(transport, GreeterService, root);
    await warmClient(client);
    await runCase("unary_round_trip", iterations, () => unary(client));
    await runCase("server_stream_16_messages", iterations, () => serverStreaming(client));
    await runCase("client_stream_16_messages", iterations, () => clientStreaming(client));
    await runCase("bidi_stream_16_messages", iterations, () => bidiStreaming(client));
  } finally {
    transport.close();
  }
}

async function runServer(certFile, keyFile) {
  const server = await NodeServer.listen({
    host: "127.0.0.1",
    port: 0,
    certFile,
    keyFile,
    maxSessionsPerConnection: 16,
    maxStreamsPerSession: 128,
    idleTimeoutMs: IdleTimeoutMs,
  });
  server.registerService(GreeterService, {
    sayHello: (call) => encodeReply(decodeRequest(call.request.body).name),
    lotsOfReplies: serverStreamReplies,
    lotsOfGreetings: clientStreamReply,
    bidiHello: bidiReplies,
  });

  const serveDone = server.serve();
  console.log(`PORT ${server.port}`);
  console.log(`CERT ${certFile}`);
  const stop = async () => {
    server.close();
    await serveDone;
  };
  process.once("SIGTERM", () => void stop());
  process.once("SIGINT", () => void stop());
  await serveDone;
}

async function warmClient(client) {
  await unary(client);
  await serverStreaming(client);
  await clientStreaming(client);
  await bidiStreaming(client);
}

async function runCase(name, iterations, fn) {
  const start = process.hrtime.bigint();
  for (let i = 0; i < iterations; i++) {
    await fn();
  }
  const elapsedSeconds = Number(process.hrtime.bigint() - start) / 1_000_000_000;
  const opsPerSecond = elapsedSeconds > 0 ? iterations / elapsedSeconds : 0;
  console.log(
    `${name}: ${opsPerSecond.toFixed(0)} ops/s (${iterations} iterations in ${elapsedSeconds.toFixed(3)}s)`,
  );
}

async function unary(client) {
  const reply = await client.sayHello({ name: RequestName });
  if (reply.message !== RequestName) {
    throw new Error(`unexpected unary response: ${JSON.stringify(reply)}`);
  }
}

async function serverStreaming(client) {
  const replies = await client.lotsOfReplies({ name: RequestName });
  let count = 0;
  for await (const reply of replies) {
    if (reply.message !== "server stream") {
      throw new Error(`unexpected server-stream response: ${JSON.stringify(reply)}`);
    }
    count++;
  }
  if (count !== StreamMessageCount) {
    throw new Error(`expected ${StreamMessageCount} server-stream responses, got ${count}`);
  }
}

async function clientStreaming(client) {
  const call = await client.lotsOfGreetings();
  for (let i = 0; i < StreamMessageCount; i++) {
    await call.send({ name: RequestName });
  }
  const reply = await call.closeAndRecv();
  if (reply.message !== `streamed ${StreamMessageCount} greetings`) {
    throw new Error(`unexpected client-stream response: ${JSON.stringify(reply)}`);
  }
}

async function bidiStreaming(client) {
  const call = await client.bidiHello();
  for (let i = 0; i < StreamMessageCount; i++) {
    await call.send({ name: RequestName });
  }
  await call.closeSend();

  let count = 0;
  for (;;) {
    const reply = await call.recv();
    if (reply === undefined) {
      break;
    }
    if (reply.message !== RequestName) {
      throw new Error(`unexpected bidi response: ${JSON.stringify(reply)}`);
    }
    count++;
  }
  if (count !== StreamMessageCount) {
    throw new Error(`expected ${StreamMessageCount} bidi responses, got ${count}`);
  }
}

async function* serverStreamReplies(call) {
  decodeRequest(call.request.body);
  for (let i = 0; i < StreamMessageCount; i++) {
    yield encodeReply("server stream");
  }
}

async function* clientStreamReply(call) {
  let count = 0;
  for (;;) {
    const frame = await call.recv();
    if (frame == null) {
      yield encodeReply(`streamed ${count} greetings`);
      return;
    }
    decodeRequest(frame.body);
    count++;
  }
}

async function* bidiReplies(call) {
  for (;;) {
    const frame = await call.recv();
    if (frame == null) {
      return;
    }
    yield encodeReply(decodeRequest(frame.body).name);
  }
}

function encodeReply(message) {
  return HelloReply.encode({ message }).finish();
}

function decodeRequest(body) {
  return HelloRequest.decode(body);
}

function positiveInteger(value) {
  if (!/^[1-9][0-9]*$/.test(value)) {
    throw new Error(`expected a positive integer, got ${JSON.stringify(value)}`);
  }
  return Number(value);
}

function requiredArg(index, name) {
  const value = process.argv[index];
  if (value == null || value === "") {
    throw new Error(`missing ${name}`);
  }
  return value;
}
