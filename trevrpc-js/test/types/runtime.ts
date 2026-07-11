import { Code, connect, createRoot, normalizeMetadata, unary } from "../../src/index.js";
import type {
  RpcRequestMessage,
  RpcResponseMessage,
  RpcStreamFrameMessage,
  Transport,
} from "../../src/index.js";
import type { NodeListenOptions } from "../../src/node.js";

interface HelloMessage {
  value?: string;
}

const root = createRoot({
  nested: {
    hello: {
      nested: {
        v1: {
          nested: {
            Hello: {
              fields: {
                value: { type: "string", id: 1 },
              },
            },
          },
        },
      },
    },
  },
});
const Hello = root.lookupType("hello.v1.Hello");
const transport: Transport = {
  async call(request: RpcRequestMessage): Promise<RpcResponseMessage> {
    const decoded = Hello.decode(request.body) as HelloMessage;
    return {
      status: Code.Ok,
      body: Hello.encode({ value: `hello ${decoded.value ?? ""}` }).finish(),
      metadata: normalizeMetadata({ checked: "true" }),
    };
  },
  async streamingCall(): Promise<AsyncIterable<RpcStreamFrameMessage>> {
    return emptyFrames();
  },
};

const reply = await unary<HelloMessage, HelloMessage>(
  transport,
  "hello.v1.Greeter",
  "SayHello",
  Hello,
  Hello,
  { value: "Trev" },
);

reply.value?.toUpperCase();

const connected = await connect("https://localhost:50051/trevrpc", {
  serverCertificateHashes: [{ algorithm: "sha-256", value: new Uint8Array(32) }],
  skipCertificateValidation: true,
});
connected.close();

const nodeListenOptions: NodeListenOptions = {
  host: "127.0.0.1",
  port: 0,
  enableHttp3: true,
  http3Path: "/rpc",
  http3Admission: (request) => request.secure && request.path === "/rpc",
  initialRequestTimeoutMs: 5_000,
};
void nodeListenOptions;

async function* emptyFrames(): AsyncIterable<RpcStreamFrameMessage> {}
