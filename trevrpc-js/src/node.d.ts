import type { RpcResponseMessage, RpcStreamFrameMessage, Transport } from "./index.js";

export interface NodeWebTransportOptions {
  host?: string;
  port?: number;
  path?: string;
  origin?: string;
  certFile?: string;
  keyFile?: string;
  caCertFile?: string;
  skipCertificateValidation?: boolean;
  maxSessionsPerConnection?: number;
  maxStreamsPerSession?: number;
  idleTimeoutMs?: number;
  maxFrameSize?: number;
}

/** Native Node transport backed by trevrpc-c and MsQuic. */
export class NodeTransport implements Transport {
  maxFrameSize?: number;

  /** Opens a WebTransport TrevRPC client backed by the native C runtime. */
  static connectWebTransport(
    urlOrOptions: string | URL | NodeWebTransportOptions,
    options?: NodeWebTransportOptions,
  ): Promise<NodeTransport>;

  /** Sends a unary RPC request and returns its response. */
  call(request: Parameters<Transport["call"]>[0]): Promise<RpcResponseMessage>;

  /** Starts a streaming RPC and returns native response frames. */
  streamingCall(
    request: Parameters<Transport["streamingCall"]>[0],
    requestBody: AsyncIterable<Uint8Array>,
  ): Promise<AsyncIterableIterator<RpcStreamFrameMessage>>;

  /** Closes the underlying native client. */
  close(): void;
}
