import {
  Channel,
  Code,
  TrevRpcError,
  connect,
  createServiceClient,
  type BidirectionalStreamingCall,
  type ChannelStateChange,
  type ClientStreamingCall,
  type ResponseStream,
  type StreamStatus,
  type Transport,
  type UnaryResponse,
} from "@trevrpc/trevrpc-js";

import {
  EchoClient,
  EchoService,
  createEchoClient,
  root,
  type EchoResponse,
} from "../../build/interop/gen/interop.trevrpc.js";

const channel = await connect("https://localhost:4433/trevrpc", {
  serverCertificateHashes: [{ algorithm: "sha-256", value: new Uint8Array(32) }],
  signal: AbortSignal.timeout(5_000),
  onStateChange(change: ChannelStateChange) {
    change.state satisfies "ready" | "reconnecting" | "closed";
  },
});
channel satisfies Channel satisfies Transport;
channel.state satisfies "ready" | "reconnecting" | "closed";
await channel.waitUntilReady();

const echo = new EchoClient(channel, { timeoutMs: 10_000, metadata: { trace: "abc" } });
createEchoClient(channel) satisfies EchoClient;
createServiceClient(channel, EchoService, root);

const unary: EchoResponse = await echo.unary({ message: "hi", failCode: Code.NotFound });
unary.message satisfies string | undefined;
const withResponse = await echo.unaryWithResponse({ message: "hi" });
withResponse satisfies UnaryResponse<EchoResponse>;
withResponse.metadata["x"] satisfies Uint8Array;

const stream = await echo.serverStream({ count: 2 });
stream satisfies ResponseStream<EchoResponse>;
for await (const response of stream) {
  response.index satisfies number | undefined;
}
(await stream.status) satisfies StreamStatus;
await stream.close(new Error("done"));

const upload = await echo.clientStream();
upload satisfies ClientStreamingCall<{ message?: string }, EchoResponse>;
await upload.send({ message: "a" });
(await upload.closeAndRecv()) satisfies EchoResponse;
(await upload.closeAndRecvWithResponse()) satisfies UnaryResponse<EchoResponse>;

const bidi = await echo.bidi({ signal: new AbortController().signal });
bidi satisfies BidirectionalStreamingCall<{ message?: string }, EchoResponse>;
await bidi.send({ message: "a" });
(await bidi.recv()) satisfies EchoResponse | undefined;
await bidi.closeSend();

try {
  await echo.unary({});
} catch (error) {
  if (error instanceof TrevRpcError) {
    error.code satisfies number;
    error.statusMessage satisfies string;
    error.metadata satisfies Record<string, Uint8Array>;
  }
}

channel.close({ closeCode: 0, reason: "done" });

// @ts-expect-error The Channel constructor is internal; use connect().
new Channel();
// @ts-expect-error Server code generation is gone; there is no server runtime.
void echo.registerServer;
// @ts-expect-error Request messages must match the generated interface.
await echo.unary({ message: 1 });
