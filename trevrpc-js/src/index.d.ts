import type { INamespace, Root } from "protobufjs";

export const protobuf: typeof import("protobufjs");

/** Creates a protobuf root from a JSON namespace definition. */
export function createRoot(json: INamespace): Root;

export const Code: Readonly<{
  Ok: 0;
  Cancelled: 1;
  Unknown: 2;
  InvalidArgument: 3;
  DeadlineExceeded: 4;
  NotFound: 5;
  AlreadyExists: 6;
  PermissionDenied: 7;
  ResourceExhausted: 8;
  FailedPrecondition: 9;
  Aborted: 10;
  OutOfRange: 11;
  Unimplemented: 12;
  Internal: 13;
  Unavailable: 14;
  DataLoss: 15;
  Unauthenticated: 16;
}>;

export type StatusCode = (typeof Code)[keyof typeof Code];

/** Returns the canonical status code name, such as "NotFound". */
export function codeName(code: number): string;

export type MetadataValue = string | Uint8Array | ArrayBuffer | ArrayBufferView | readonly number[];
export type MetadataInput = Record<string, MetadataValue> | ReadonlyMap<string, MetadataValue>;
export type Metadata = Record<string, Uint8Array>;

/** Error carrying a TrevRPC status code, message, and metadata. */
export class TrevRpcError extends Error {
  readonly code: StatusCode;
  readonly statusMessage: string;
  readonly metadata: Metadata;

  constructor(code: number, message?: string, metadata?: Metadata, options?: ErrorOptions);
}

export interface CallOptions {
  /** Deadline for the whole call, from the start of the request to its final status. */
  timeoutMs?: number;
  /** Largest frame accepted or sent, in bytes. Defaults to 4 MiB. */
  maxFrameSize?: number;
  /** Request metadata. Keys are lowercase ASCII; the `trevrpc-` prefix is reserved. */
  metadata?: MetadataInput;
  /** Cancels the call. */
  signal?: AbortSignal;
}

export interface TransportStream {
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
}

/** Anything that opens a bidirectional byte stream per call, such as a Channel or WebTransport. */
export interface Transport {
  createBidirectionalStream(): Promise<TransportStream>;
}

export type RpcMethodKind =
  | "unary"
  | "clientStreaming"
  | "serverStreaming"
  | "bidirectionalStreaming";

export interface RpcMethodDescriptor {
  name: string;
  kind: RpcMethodKind;
  inputType: string;
  outputType: string;
}

export interface RpcServiceDescriptor {
  name: string;
  fullName: string;
  exportName: string;
  methods: Record<string, RpcMethodDescriptor>;
}

export interface UnaryResponse<TResponse> {
  readonly message: TResponse;
  /** Response headers; empty when the server sent none. */
  readonly headers: Metadata;
  /** Trailing metadata from the final status. */
  readonly metadata: Metadata;
}

/** The final status of a call that ended with OK. */
export interface StreamStatus {
  readonly code: StatusCode;
  readonly message: string;
  readonly metadata: Metadata;
}

/**
 * Responses of a server-streaming call. Iteration ends after the final OK status and throws a
 * TrevRpcError for any other outcome. Leaving the loop early cancels the call.
 */
export interface ResponseStream<TResponse> extends AsyncIterable<TResponse> {
  /** Settles with the final status, or rejects with the error that ended the call. */
  readonly status: Promise<StreamStatus>;
  /**
   * The response headers, empty when the server sent none. Settles when the first response frame
   * arrives, before the first message is received, or rejects with the error that ended the call.
   */
  readonly headers: Promise<Metadata>;
  /** Cancels the call unless it already finished. */
  close(reason?: unknown): Promise<void>;
}

export interface ClientStreamingCall<TRequest, TResponse> {
  /** Sends one request message. */
  send(request: TRequest): Promise<void>;
  /** Closes the request stream and returns the final response. */
  closeAndRecv(): Promise<TResponse>;
  /** Closes the request stream and returns the final response with its headers and trailing metadata. */
  closeAndRecvWithResponse(): Promise<UnaryResponse<TResponse>>;
  /** Cancels the call unless it already finished. */
  close(reason?: unknown): Promise<void>;
}

export interface BidirectionalStreamingCall<TRequest, TResponse> extends AsyncIterable<TResponse> {
  /** Settles with the final status, or rejects with the error that ended the call. */
  readonly status: Promise<StreamStatus>;
  /**
   * The response headers, empty when the server sent none. Settles when the first response frame
   * arrives, before the first message is received, or rejects with the error that ended the call.
   */
  readonly headers: Promise<Metadata>;
  /** Sends one request message. */
  send(request: TRequest): Promise<void>;
  /** Receives the next response message, or undefined after the final OK status. */
  recv(): Promise<TResponse | undefined>;
  /** Closes the request stream while keeping the response stream readable. */
  closeSend(): Promise<void>;
  /** Cancels the call unless it already finished. */
  close(reason?: unknown): Promise<void>;
}

export type ServiceClient = Record<
  string,
  (requestOrOptions?: unknown, options?: CallOptions) => Promise<unknown>
>;

/** Creates a service client from a generated service descriptor. */
export function createServiceClient<TClient extends ServiceClient = ServiceClient>(
  transport: Transport,
  service: RpcServiceDescriptor,
  root: Root,
  options?: CallOptions,
): TClient;

export interface WebTransportCertificateHash {
  algorithm: string;
  value: ArrayBuffer | ArrayBufferView;
}

export interface WebTransportSessionLike extends Transport {
  readonly ready: Promise<unknown>;
  readonly closed: Promise<unknown>;
  close(closeInfo?: { closeCode?: number; reason?: string }): void;
}

/** How a channel reaches the server: WebTransport, or QMux over WebSocket as the fallback. */
export type TransportKind = "webtransport" | "websocket";

export interface ConnectOptions {
  /**
   * Transports to use, in order of preference. The default, `["webtransport", "websocket"]`,
   * starts QMux over WebSocket once WebTransport fails or has not connected within
   * `fallbackDelayMs`, and uses whichever is ready first.
   */
  transports?: readonly TransportKind[];
  /** The `wss://` URL of the server's TCP listener. Defaults to `url` with a `wss:` scheme. */
  webSocketUrl?: string | URL;
  /** How long WebTransport may take before the WebSocket fallback starts. Defaults to 300. */
  fallbackDelayMs?: number;
  /** How long reconnects keep using WebSocket after it won, before trying WebTransport again. */
  reprobeIntervalMs?: number;
  /** WebSocket implementation to use instead of the global one. */
  WebSocket?: new (url: string | URL, protocols?: string | string[]) => WebSocket;
  allowPooling?: boolean;
  congestionControl?: "default" | "low-latency" | "throughput";
  requireUnreliable?: boolean;
  /**
   * Trusts a self-signed ECDSA certificate (at most 14 days old) by its SHA-256 hash. WebTransport
   * only: the WebSocket fallback needs a certificate the browser trusts.
   */
  serverCertificateHashes?: readonly WebTransportCertificateHash[];
  /** WebTransport implementation to use instead of the global one. */
  WebTransport?: new (url: string | URL, options?: object) => WebTransportSessionLike;
  /** Cancels the initial connection attempt only. */
  signal?: AbortSignal;
  /** Receives channel state transitions. */
  onStateChange?(change: ChannelStateChange): void;
}

export type ChannelState = "ready" | "reconnecting" | "closed";

export interface ChannelStateChange {
  state: ChannelState;
  previousState: ChannelState;
  /** Reconnect attempt number while reconnecting, starting at one. */
  attempt?: number;
  /** Delay before the reconnect attempt. */
  delayMs?: number;
  /** Why the session was lost or the previous attempt failed. */
  error?: unknown;
  /** The transport of the new session, when it becomes ready. */
  transport?: TransportKind;
}

/** Opens a channel to a TrevRPC server and waits for its session to be ready. */
export function connect(url: string | URL, options?: ConnectOptions): Promise<Channel>;

/**
 * A session that reconnects in the background after it is lost: WebTransport, or QMux over
 * WebSocket when WebTransport is unavailable or blocked. Calls fail fast with Unavailable while it
 * reconnects and are never replayed.
 */
export class Channel implements Transport {
  private constructor();

  /** Opens a channel to a TrevRPC server and waits for its session to be ready. */
  static connect(url: string | URL, options?: ConnectOptions): Promise<Channel>;

  readonly state: ChannelState;
  /** The transport of the current or last session. */
  readonly transport: TransportKind;
  createBidirectionalStream(): Promise<TransportStream>;
  /** Resolves once the channel is ready, immediately if it already is. */
  waitUntilReady(): Promise<void>;
  /** Stops reconnecting and closes the current session. */
  close(closeInfo?: { closeCode?: number; reason?: string }): void;
}
