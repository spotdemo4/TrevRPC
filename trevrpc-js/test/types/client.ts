import { create, type DescMethod } from "@bufbuild/protobuf";
import {
  Channel,
  Code,
  TrevRpcError,
  bearerMetadata,
  connect,
  createChannel,
  createClient,
  fetchServerCertificateHashes,
  type Client,
  type BidirectionalStreamingCall,
  type CallEnd,
  type CallInterceptor,
  type ChannelStateChange,
  type ClientStreamingCall,
  type Compression,
  type RequestValidator,
  type ResponseStream,
  type StreamStatus,
  type Transport,
} from "@trevrpc/trevrpc-js";
import { createCompression } from "@trevrpc/trevrpc-js/zstd";

import { Echo, EchoRequestSchema, type EchoResponse } from "../../build/interop/gen/interop_pb.js";

const channel = await connect("https://localhost:4433/trevrpc", {
  serverCertificateHashes: [{ algorithm: "sha-256", value: new Uint8Array(32) }],
  fetchServerCertificateHashes: true,
  signal: AbortSignal.timeout(5_000),
  transports: ["webtransport", "websocket"],
  webSocketUrl: "wss://localhost:4433/trevrpc",
  fallbackDelayMs: 300,
  reprobeIntervalMs: 300_000,
  onStateChange(change: ChannelStateChange) {
    change.state satisfies "connecting" | "ready" | "reconnecting" | "closed";
    change.transport satisfies "webtransport" | "websocket" | undefined;
  },
});
channel satisfies Channel satisfies Transport;
createChannel("https://localhost:4433/trevrpc", {
  serverCertificateHashesUrl: new URL("https://localhost:5173/certificate-hashes"),
}).close();
(
  await fetchServerCertificateHashes(new URL("https://localhost:5173/certificate-hashes"), {
    signal: AbortSignal.timeout(5_000),
    fetch,
  })
)[0]?.value satisfies ArrayBuffer | ArrayBufferView | undefined;
channel.state satisfies "connecting" | "ready" | "reconnecting" | "closed";
channel.transport satisfies "webtransport" | "websocket" | undefined;
await channel.waitUntilReady();

const authenticate: CallInterceptor = async (call) => {
  call.method satisfies string;
  call.service satisfies string;
  call.name satisfies string;
  call.kind satisfies "unary" | "serverStreaming" | "clientStreaming" | "bidirectionalStreaming";
  call.descriptor satisfies DescMethod;
  call.timeoutMs satisfies number | undefined;
  call.signal satisfies AbortSignal | undefined;
  call.metadata["authorization"] = "Bearer refreshed";
  return (end: CallEnd) => {
    if (end.code === Code.Unauthenticated) {
      end.error satisfies unknown;
    }
  };
};
const lazy: Channel = createChannel("https://localhost:4433/trevrpc", {
  connectionMetadata: bearerMetadata("token"),
  interceptors: [authenticate, () => {}],
});
lazy.interceptors satisfies readonly CallInterceptor[];
createClient(Echo, lazy);
const wrapper: Transport = {
  createBidirectionalStream: () => lazy.createBidirectionalStream(),
  createBidirectionalStreamWithSession: () => lazy.createBidirectionalStreamWithSession(),
  compression: lazy.compression,
  connectionMetadata: lazy.connectionMetadata,
  streamCompressionBudget: lazy.streamCompressionBudget,
  interceptors: lazy.interceptors,
};
createClient(Echo, wrapper, { interceptors: [authenticate], timeoutMs: 1_000 });
// @ts-expect-error An interceptor returns a finish function or nothing.
createClient(Echo, wrapper, { interceptors: [() => 1] });
const rejectEmpty: RequestValidator = {
  validateFunc(schema) {
    if (schema.typeName !== EchoRequestSchema.typeName) {
      return undefined;
    }
    return (message) => {
      if (Object.keys(message).length === 0) {
        throw new TrevRpcError(Code.InvalidArgument, "empty request");
      }
    };
  },
};
createClient(Echo, wrapper, { validator: rejectEmpty });
// @ts-expect-error A validator's validateFunc returns a function or nothing.
createClient(Echo, wrapper, { validator: { validateFunc: () => 1 } });
({
  createBidirectionalStream: () => lazy.createBidirectionalStream(),
  // @ts-expect-error connection metadata is set through the channel, not built by hand
  connectionMetadata: {},
}) satisfies Transport;
lazy.close();

const echo = createClient(Echo, channel, { timeoutMs: 10_000, metadata: { trace: "abc" } });
echo satisfies Client<typeof Echo>;

// Requests take an init object or a message; responses are messages.
const unary: EchoResponse = await echo.unary({ message: "hi", failCode: Code.NotFound });
unary.message satisfies string;
unary.$typeName satisfies "trevrpc.interop.v1.EchoResponse";
await echo.unary(create(EchoRequestSchema, { message: "hi" }));
await echo.unary(
  { message: "hi" },
  {
    onResponseHeaders: (headers) => headers["x"] satisfies Uint8Array | undefined,
    onResponseMetadata: (metadata) => metadata["x"] satisfies Uint8Array | undefined,
  },
);

const stream = await echo.serverStream({ count: 2 });
stream satisfies ResponseStream<EchoResponse>;
for await (const response of stream) {
  response.index satisfies number;
}
(await stream.status) satisfies StreamStatus;
(await stream.headers)["x"] satisfies Uint8Array;
await stream.close(new Error("done"));

const upload = await echo.clientStream();
upload satisfies ClientStreamingCall<{ message?: string }, EchoResponse>;
await upload.send({ message: "a" });
await upload.sendAll([{ message: "b" }]);
(await upload.closeAndRecv()) satisfies EchoResponse;
(await upload.status) satisfies StreamStatus;

const bidi = await echo.bidi({ signal: new AbortController().signal });
bidi satisfies BidirectionalStreamingCall<{ message?: string }, EchoResponse>;
await bidi.send({ message: "a" });
await bidi.sendAll(
  (async function* () {
    yield { message: "b" };
  })(),
);
(await bidi.recv()) satisfies EchoResponse | undefined;
(await bidi.headers)["x"] satisfies Uint8Array;
await bidi.closeSend();

try {
  await echo.unary({});
} catch (error) {
  if (error instanceof TrevRpcError) {
    error.code satisfies number;
    error.statusMessage satisfies string;
    error.metadata satisfies Record<string, Uint8Array>;
    error._tag satisfies "TrevRpcError";
  }
}

channel.close({ closeCode: 0, reason: "done" });

const compression = await createCompression({
  dictionaries: [{ data: new Uint8Array(), messages: ["trevrpc.interop.v1.EchoRequest"] }],
  level: 3,
});
const compressed = await connect("https://example.test/trevrpc", {
  compression,
  streamCompressionMemory: 16 * 1024 * 1024,
});
// Settings without dictionaries enable stream compression alone.
await connect("https://example.test/trevrpc", { compression: await createCompression() });
compressed.compression satisfies typeof compression | undefined;
// A promise of the settings lets createChannel() return before the WebAssembly compiles.
const pendingCompression = createChannel("https://example.test/trevrpc", {
  compression: createCompression(),
  onStateChange({ compressionError }) {
    compressionError satisfies unknown;
  },
});
pendingCompression.compression satisfies Compression | undefined;
await connect("https://example.test/trevrpc", { compression: createCompression() });
// @ts-expect-error A promise must resolve with compression settings.
createChannel("https://example.test/trevrpc", { compression: Promise.resolve({ query: [] }) });
// @ts-expect-error Compression settings come from createCompression().
await connect("https://example.test/trevrpc", { compression: { query: [] } });

// @ts-expect-error The Channel constructor is internal; use connect().
new Channel();
// @ts-expect-error Clients have the service's methods only.
void echo.registerServer;
// @ts-expect-error Request messages must match the generated message.
await echo.unary({ message: 1 });
// @ts-expect-error Streaming requests are sent on the call, not passed to the method.
await echo.clientStream({ message: "a" });
// @ts-expect-error createClient takes a service descriptor.
createClient(EchoRequestSchema, channel);
