import {
  Channel,
  Code,
  FrameTooLargeError,
  RpcStreamFrameKind,
  TrevRpcError,
  connect,
  createRoot,
  serverStreaming,
} from "trevrpc-js";
import type {
  BrowserChannelOptions,
  ResponseAsyncIterable,
  RpcStreamFrameMessage,
  StreamStatus,
} from "trevrpc-js";

const nativeCause = new Error("native");
const statusError = new TrevRpcError(
  Code.Unavailable,
  "down",
  {},
  {
    cause: nativeCause,
    nativeCode: -1,
  },
);
statusError.cause satisfies unknown;
new FrameTooLargeError(2, 1) satisfies TrevRpcError;

const options: BrowserChannelOptions = {
  serverCertificateHashes: [{ algorithm: "sha-256", value: new Uint8Array(32) }],
};
const channel = new Channel("https://localhost:50051/trevrpc", options);
const connected = await connect("https://localhost:50051/trevrpc", options);
channel.close();
connected.close();

const Message = createRoot({
  nested: { test: { nested: { Message: { fields: { value: { type: "string", id: 1 } } } } } },
}).lookupType("test.Message");
const response = await serverStreaming<{ value?: string }, { value?: string }>(
  {
    async call() {
      throw new Error("not used");
    },
    async streamingCall() {
      return emptyFrames();
    },
  },
  "test.Service",
  "Stream",
  Message,
  Message,
  {},
);
response satisfies ResponseAsyncIterable<{ value?: string }>;
const status: StreamStatus = await response.status;
status.code satisfies number;
status.message satisfies string;
await response.close();

// @ts-expect-error Browser declarations do not expose native TLS bypass options.
await connect("https://localhost:50051/trevrpc", { skipCertificateValidation: true });
// @ts-expect-error Successful stream status uses message, not statusMessage.
void status.statusMessage;

async function* emptyFrames(): AsyncIterable<RpcStreamFrameMessage> {
  yield {
    kind: RpcStreamFrameKind.Status,
    status: Code.Ok,
    body: new Uint8Array(),
    metadata: {},
  };
}
