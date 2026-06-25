import type {
  NodeConnectOptions as RuntimeNodeConnectOptions,
  RpcKindValue,
  RpcMethodKind,
  RpcResponseMessage,
  RpcServiceDescriptor,
  RpcStreamFrameMessage,
  Transport,
} from "./index.js";

export interface NodeConnectOptions extends RuntimeNodeConnectOptions {}

export interface NodeListenOptions extends RuntimeNodeConnectOptions {}

/** Native Node transport backed by trevrpc-c and MsQuic. */
export class NodeTransport implements Transport {
  maxFrameSize?: number;

  /** Opens a TrevRPC client backed by the native C runtime. */
  static connect(
    urlOrOptions: string | URL | NodeConnectOptions,
    options?: NodeConnectOptions,
  ): Promise<NodeTransport>;

  /** Sends a unary RPC request and returns its response. */
  call(
    request: Parameters<Transport["call"]>[0],
    options?: Parameters<Transport["call"]>[1],
  ): Promise<RpcResponseMessage>;

  /** Starts a streaming RPC and returns native response frames. */
  streamingCall(
    request: Parameters<Transport["streamingCall"]>[0],
    requestBody: AsyncIterable<Uint8Array>,
    options?: Parameters<Transport["streamingCall"]>[2],
  ): Promise<AsyncIterableIterator<RpcStreamFrameMessage>>;

  /** Closes the underlying native client. */
  close(): void;
}

export type NodeServerUnaryResult =
  | RpcResponseMessage
  | Uint8Array
  | ArrayBuffer
  | ArrayBufferView
  | readonly number[];
export type NodeServerStreamingResult = AsyncIterable<
  Uint8Array | ArrayBuffer | ArrayBufferView | readonly number[]
>;

export type NodeServerHandler = (
  call: NodeServerCall,
) =>
  | void
  | NodeServerUnaryResult
  | NodeServerStreamingResult
  | Promise<void | NodeServerUnaryResult | NodeServerStreamingResult>;

/** Raw server call passed to NodeServer handlers. */
export class NodeServerCall {
  request: {
    service: string;
    method: string;
    body: Uint8Array;
    metadata: Record<string, Uint8Array>;
    kind: RpcKindValue;
    version: number;
    timeoutNanos?: string;
  };
  completed: boolean;
  deferred: boolean;

  /** Keeps the call open after the handler returns. */
  defer(): this;
  /** Sends a unary response and completes the call. */
  respond(
    response?: RpcResponseMessage | Uint8Array | ArrayBuffer | ArrayBufferView | readonly number[],
  ): Promise<void>;
  /** Sends one streaming response message. */
  sendMessage(body: Uint8Array | ArrayBuffer | ArrayBufferView | readonly number[]): Promise<void>;
  /** Receives one streaming request frame, or null after EOF. */
  recv(): Promise<RpcStreamFrameMessage | null>;
  /** Sends the terminal streaming status and completes the call. */
  finishStream(status?: number, message?: string): Promise<void>;
  /** Cancels and closes the call. */
  close(): void;
}

/** Native Node server backed by trevrpc-c and MsQuic. */
export class NodeServer {
  port: number;
  maxFrameSize?: number;
  closed: Promise<void> | null;

  /** Creates a native QUIC and WebTransport TrevRPC server backed by trevrpc-c. */
  static listen(
    urlOrOptions: string | URL | NodeListenOptions,
    options?: NodeListenOptions,
  ): Promise<NodeServer>;

  /** Registers one raw RPC handler. */
  register(
    service: string,
    method: string,
    kind: RpcKindValue | RpcMethodKind,
    handler: NodeServerHandler,
  ): this;

  /** Registers handlers for a generated service descriptor. */
  registerService(service: RpcServiceDescriptor, handlers: Record<string, NodeServerHandler>): this;

  /** Starts accepting RPCs. The returned promise resolves after close. */
  serve(): Promise<void>;
  /** Requests server shutdown. */
  close(): void;
}
