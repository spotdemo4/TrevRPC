import type { RpcResponseMessage, RpcStreamFrameMessage, Transport } from "./index.js";
import type { NodeChannelTarget, NodeConnectOptions } from "./node.js";

export interface RawNodeCloseInfo {
  nativeCode: number;
}

/** Raw single-connection Node transport backed by trevrpc-c and MsQuic. */
export class RawNodeTransport implements Transport {
  maxFrameSize?: number;
  readonly closed: Promise<RawNodeCloseInfo>;

  static connect(
    target: string | URL | NodeChannelTarget,
    options?: NodeConnectOptions,
  ): Promise<RawNodeTransport>;
  call(
    request: Parameters<Transport["call"]>[0],
    options?: Parameters<Transport["call"]>[1],
  ): Promise<RpcResponseMessage>;
  streamingCall(
    request: Parameters<Transport["streamingCall"]>[0],
    requestBody: AsyncIterable<Uint8Array>,
    options?: Parameters<Transport["streamingCall"]>[2],
  ): Promise<AsyncIterableIterator<RpcStreamFrameMessage>>;
  close(): void;
}
