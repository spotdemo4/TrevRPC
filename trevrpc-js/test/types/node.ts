import { Channel as RootChannel, connect as connectRoot } from "@trevrpc/trevrpc-js";
import { Channel, NodeServer } from "@trevrpc/trevrpc-js/node";
import type {
  NodeChannelOptions,
  NodeChannelTarget,
  NodeEndpoint,
  NodeListenOptions,
  NodeServerCall,
} from "@trevrpc/trevrpc-js/node";
import { RawNodeTransport } from "@trevrpc/trevrpc-js/node/advanced";

const target = new URL("https://localhost:50051/trevrpc");
const objectTarget: NodeChannelTarget = {
  host: "localhost",
  port: 50051,
  skipCertificateValidation: true,
};
const options: NodeChannelOptions = {
  skipCertificateValidation: true,
  maxPendingSendBytes: 1_048_576,
  timeoutMs: 5_000,
};
const channel = new Channel(target, options);
channel.endpoint satisfies Readonly<NodeEndpoint>;
channel.options satisfies Readonly<NodeChannelOptions>;
// @ts-expect-error The normalized endpoint is immutable.
channel.endpoint.port = 1;
// @ts-expect-error The effective options are immutable.
channel.options.timeoutMs = 1;
// @ts-expect-error The removed 0.1 input property is not public.
void channel.urlOrOptions;
channel.close();
new Channel(objectTarget).close();

const rootChannel = await connectRoot({ host: "127.0.0.1", port: 50051 }, options);
rootChannel satisfies RootChannel;
rootChannel.close();
void RawNodeTransport.connect({ host: "127.0.0.1", port: 50051 }, options);

const listenOptions: NodeListenOptions = {
  host: "127.0.0.1",
  port: 0,
  certFile: "server.crt",
  keyFile: "server.key",
};
void NodeServer.listen(listenOptions);

function inspectCall(call: NodeServerCall) {
  call.signal satisfies AbortSignal;
  call.deadline?.getTime();
  call.close("stopped");
}
void inspectCall;
