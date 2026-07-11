import type {
  ChannelLifecycleEvent,
  ChannelLifecycleOptions,
  ChannelState,
  RpcChannel,
  MetadataInput,
  NodeConnectOptions as RuntimeNodeConnectOptions,
  RpcKindValue,
  RpcMethodKind,
  RpcResponseMessage,
  RpcServiceDescriptor,
  RpcStreamFrameMessage,
  Transport,
} from "./index.js";

export interface NodeConnectOptions extends RuntimeNodeConnectOptions {}

export interface NodeChannelOptions extends NodeConnectOptions, ChannelLifecycleOptions {
  /** Bounds initial readiness only. Later reconnects continue until close(). */
  timeoutMs?: number;
}

export interface NodeRpcStartedEvent {
  service: string;
  method: string;
  requestBodyLength: number;
  kind: RpcKindValue;
}

export interface NodeRpcFinishedEvent extends NodeRpcStartedEvent {
  responseBodyLength: number;
  status: number;
  elapsedMs?: number;
}

export interface NodeLogEvent {
  level: "debug" | "info" | "warn" | "error" | string;
  event: string;
  message?: string;
  service?: string;
  method?: string;
  status?: number;
}

export interface NodeServerMetrics {
  rpcStarted?(event: NodeRpcStartedEvent): void;
  rpcFinished?(event: NodeRpcFinishedEvent): void;
  started?(event: NodeRpcStartedEvent): void;
  finished?(event: NodeRpcFinishedEvent): void;
}

export interface NodeServerLogger {
  log?(event: NodeLogEvent): void;
  debug?(event: NodeLogEvent): void;
  info?(event: NodeLogEvent): void;
  warn?(event: NodeLogEvent): void;
  error?(event: NodeLogEvent): void;
}

export type NodeServerAuthorizationResult =
  | void
  | boolean
  | {
      code?: number;
      status?: number;
      message?: string;
      statusMessage?: string;
      metadata?: MetadataInput;
    };

export type NodeServerAuthorizer = (
  call: NodeServerCall,
) => NodeServerAuthorizationResult | Promise<NodeServerAuthorizationResult>;

export interface NodeHttp3AdmissionRequest {
  path: string;
  authority: string;
  secure: boolean;
}

export type NodeHttp3Admission = (request: NodeHttp3AdmissionRequest) => boolean;

export interface NodeListenOptions extends RuntimeNodeConnectOptions {
  path?: string;
  origin?: string;
  certFile?: string;
  keyFile?: string;
  maxSessionsPerConnection?: number;
  /** Enables ordinary HTTP/3 POST requests. Disabled by default. */
  enableHttp3?: boolean;
  /** HTTP/3 POST path. Defaults to /trevrpc. */
  http3Path?: string;
  /**
   * Synchronous request admission hook run before RPC framing is read. The
   * native wait is bounded by initialRequestTimeoutMs.
   */
  http3Admission?: NodeHttp3Admission;
  authorizer?: NodeServerAuthorizer;
  metrics?: NodeServerMetrics;
  logger?: NodeServerLogger | ((event: NodeLogEvent) => void);
  maxStreamMessages?: number;
  /** Initial HTTP/RPC request timeout. Defaults to 10000 ms. */
  initialRequestTimeoutMs?: number;
  streamIdleTimeoutMs?: number;
}

/** Native Node channel with background connection reconnection. */
export class Channel extends EventTarget implements RpcChannel {
  readonly urlOrOptions: string | URL | Readonly<NodeChannelOptions>;
  readonly options: Readonly<NodeChannelOptions>;
  readonly ready: boolean;
  readonly state: ChannelState;
  readonly generation: number;

  constructor(urlOrOptions: string | URL | NodeChannelOptions, options?: NodeChannelOptions);

  /** Creates a native channel and waits for its first ready generation. */
  static connect(
    urlOrOptions: string | URL | NodeChannelOptions,
    options?: NodeChannelOptions,
  ): Promise<Channel>;
  /** Waits for the current or next connection generation to become ready. */
  waitUntilReady(): Promise<void>;
  /** Sends a unary call on the current generation without replaying it. */
  call(
    request: Parameters<Transport["call"]>[0],
    options?: Parameters<Transport["call"]>[1],
  ): Promise<RpcResponseMessage>;
  /** Starts a streaming call on the current generation without resuming it later. */
  streamingCall(
    request: Parameters<Transport["streamingCall"]>[0],
    requestBody: AsyncIterable<Uint8Array>,
    options?: Parameters<Transport["streamingCall"]>[2],
  ): Promise<AsyncIterableIterator<RpcStreamFrameMessage>>;
  /** Stops reconnecting and closes the current generation. */
  close(): void;
  addEventListener(
    type: "statechange" | ChannelState,
    callback: (event: ChannelLifecycleEvent) => void,
    options?: boolean | AddEventListenerOptions,
  ): void;
  removeEventListener(
    type: "statechange" | ChannelState,
    callback: (event: ChannelLifecycleEvent) => void,
    options?: boolean | EventListenerOptions,
  ): void;
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
  responseBodyLength: number;
  finalStatus: number;
  startedAt: number;
  completedAt: number | null;

  /** Keeps the call open after the handler returns. */
  defer(): this;
  /** Sends a unary response and completes the call. */
  respond(
    response?: RpcResponseMessage | Uint8Array | ArrayBuffer | ArrayBufferView | readonly number[],
  ): Promise<void>;
  /** Sends one streaming response message. */
  sendMessage(body: Uint8Array | ArrayBuffer | ArrayBufferView | readonly number[]): Promise<void>;
  /** Sends multiple streaming response messages. */
  sendMany(
    bodies: Iterable<Uint8Array | ArrayBuffer | ArrayBufferView | readonly number[]>,
  ): Promise<void>;
  /** Receives one streaming request frame, or null after EOF. */
  recv(): Promise<RpcStreamFrameMessage | null>;
  /** Sends the terminal streaming status and completes the call. */
  finishStream(status?: number, message?: string, metadata?: MetadataInput): Promise<void>;
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

  /** Sets an async authorizer that runs before registered handlers. */
  setAuthorizer(authorizer?: NodeServerAuthorizer | null): this;
  /** Clears the server authorizer. */
  clearAuthorizer(): this;
  /** Sets metrics callbacks. */
  setMetrics(metrics?: NodeServerMetrics | null): this;
  /** Clears metrics callbacks. */
  clearMetrics(): this;
  /** Sets a logger function or logger object. */
  setLogger(logger?: NodeServerLogger | ((event: NodeLogEvent) => void) | null): this;
  /** Clears the logger. */
  clearLogger(): this;

  /** Starts accepting RPCs. The returned promise resolves after close. */
  serve(): Promise<void>;
  /** Requests server shutdown. */
  close(): void;
}

/** Creates an authorizer requiring one metadata key/value pair. */
export function metadataValueAuthorizer(
  key: string,
  value: string | Uint8Array | ArrayBuffer | ArrayBufferView | readonly number[],
): NodeServerAuthorizer;

/** Creates an authorizer requiring an Authorization: Bearer token metadata value. */
export function bearerAuthorizer(token: string): NodeServerAuthorizer;
