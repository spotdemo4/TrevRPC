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
  /**
   * Called with the response headers, empty when the server sent none, as soon as the first
   * response frame arrives. A callback that throws fails the call.
   */
  onResponseHeaders?(headers: Metadata): void;
  /**
   * Called with the trailing metadata of the final status when the call succeeds, before its
   * result is returned or its response stream ends. A failed call reports its metadata on the
   * TrevRpcError instead. A callback that throws fails the call.
   */
  onResponseMetadata?(metadata: Metadata): void;
}

export interface TransportStream {
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
}

/**
 * The datagram side of a WebTransport session (`WebTransport.datagrams`). TrevRPC needs only
 * `readable`; sending also needs `createWritable()` (or the older `writable`), and
 * `maxDatagramSize` bounds what is sent as a datagram (1100 bytes if it is missing).
 */
export interface DatagramTransport {
  readonly readable: ReadableStream<Uint8Array>;
  readonly maxDatagramSize?: number;
  createWritable?(): WritableStream<Uint8Array>;
  readonly writable?: WritableStream<Uint8Array>;
}

/**
 * Anything that opens a bidirectional byte stream per call, such as a Channel or WebTransport.
 * Calls to methods that mark a direction unreliable use the `datagrams` of the session that opens
 * their stream, when it has them.
 */
export interface Transport {
  createBidirectionalStream(): Promise<TransportStream>;
  /** Datagrams of the session that the next stream opens on; absent without datagrams. */
  readonly datagrams?: DatagramTransport;
  /**
   * Like `createBidirectionalStream()`, but also returns the datagrams of the session that opened
   * the stream. Transports whose session can change, such as a Channel, implement it so a call
   * never mixes the stream of one session with the datagrams of another.
   */
  createBidirectionalStreamWithDatagrams?(): Promise<{
    stream: TransportStream;
    datagrams?: DatagramTransport;
  }>;
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
  /**
   * The method marks its streamed request messages unreliable (`unreliable_requests` in the
   * `trevrpc.method` option). They are sent as WebTransport datagrams when they fit and the
   * session has datagrams, and may be lost or reordered; otherwise they travel on the stream.
   */
  unreliableRequests?: boolean;
  /**
   * The method marks its streamed response messages unreliable (`unreliable_responses`). The
   * client accepts them as datagrams on any session that has them.
   */
  unreliableResponses?: boolean;
}

export interface RpcServiceDescriptor {
  name: string;
  fullName: string;
  exportName: string;
  methods: Record<string, RpcMethodDescriptor>;
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
  /**
   * Sends one request message; the promise settles when the transport accepted it, which applies
   * backpressure.
   *
   * Rejects as soon as the server ends the call before the upload is done: with the server's
   * `TrevRpcError` if its final status is not OK, even if the server never reads the rest of the
   * request, and with `Code.Cancelled` if the status is OK (the call succeeded without the rest
   * of the upload; `closeAndRecv()` returns its response). A response that fails after the
   * upload stopped, such as a lost connection, is reported instead of a generic closed-stream
   * error. A failed send is not retried or reported twice: later sends fail the same way, and
   * `closeAndRecv()` returns the call's outcome.
   *
   * Pending sends are rejected too when the call is cancelled or its deadline expires.
   */
  send(request: TRequest): Promise<void>;
  /**
   * Sends every message of `requests`, then closes the request stream. If the server ends the
   * call first, the rest is not sent and the promise resolves; `closeAndRecv()` reports the
   * outcome. A send that fails otherwise rejects as `send()` does, and requests that throw cancel
   * the call and reject with what they threw.
   */
  sendAll(requests: Iterable<TRequest> | AsyncIterable<TRequest>): Promise<void>;
  /** Closes the request stream and returns the final response. */
  closeAndRecv(): Promise<TResponse>;
  /** Cancels the call unless it already finished. */
  close(reason?: unknown): Promise<void>;
}

/**
 * On a method that marks a direction unreliable (`RpcMethodDescriptor.unreliableRequests` or
 * `unreliableResponses`), a call on a session with datagrams may send its request messages, and
 * receive its response messages, as datagrams: they can be lost or reordered, never duplicated.
 * Response datagrams are delivered after the response headers, merged with the stream's messages
 * in arrival order, and dropped when the call's queue is full (32 messages or 1 MiB) or the call
 * has ended. Without datagrams, such as over WebSocket, everything stays on the stream.
 */
export interface BidirectionalStreamingCall<TRequest, TResponse> extends AsyncIterable<TResponse> {
  /** Settles with the final status, or rejects with the error that ended the call. */
  readonly status: Promise<StreamStatus>;
  /**
   * The response headers, empty when the server sent none. Settles when the first response frame
   * arrives, before the first message is received, or rejects with the error that ended the call.
   */
  readonly headers: Promise<Metadata>;
  /**
   * Sends one request message; the promise settles when the transport accepted it, which applies
   * backpressure.
   *
   * Rejects as soon as the server ends the call before the upload is done, even if nobody is
   * calling `recv()` and the server never reads the rest of the request: with the server's
   * `TrevRpcError` if its final status is not OK, and with `Code.Cancelled` if the status is OK
   * (`recv()` then returns `undefined` after the responses that were sent). A response that
   * fails after the upload stopped, such as a lost connection, is reported instead of a generic
   * closed-stream error. Later sends fail the same way, `closeSend()` resolves, and `recv()` and
   * `status` report the call's outcome.
   *
   * While the request side is open the response is read ahead of `recv()` to find the final
   * status: at most 32 frames or 1 MiB (plus one frame of at most `maxFrameSize`), after which
   * reading pauses until `recv()` takes messages. Responses read ahead are delivered in order.
   *
   * Pending sends are rejected too when the call is cancelled or its deadline expires.
   */
  send(request: TRequest): Promise<void>;
  /**
   * Sends every message of `requests`, then closes the request stream. If the server ends the
   * call first, the rest is not sent and the promise resolves; `recv()` and `status` report the
   * outcome. A send that fails otherwise rejects as `send()` does, and requests that throw cancel
   * the call and reject with what they threw.
   */
  sendAll(requests: Iterable<TRequest> | AsyncIterable<TRequest>): Promise<void>;
  /** Receives the next response message, or undefined after the final OK status. */
  recv(): Promise<TResponse | undefined>;
  /**
   * Closes the request stream while keeping the response stream readable. Resolves if the server
   * already ended the call; its final status is reported by `recv()` and `status`.
   */
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
  /**
   * The WebTransport protocol the server selected. An empty or missing value means wire version 1.
   * Any other value than "trevrpc-1" fails the connection attempt with FailedPrecondition.
   */
  readonly protocol?: string;
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
  /**
   * WebTransport implementation to use instead of the global one. It is constructed with
   * `protocols: ["trevrpc-1"]`, the wire version, which cannot be changed.
   */
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
  /** Opens a stream together with the datagrams of the session that carries it, if any. */
  createBidirectionalStreamWithDatagrams(): Promise<{
    stream: TransportStream;
    datagrams?: DatagramTransport;
  }>;
  /** Resolves once the channel is ready, immediately if it already is. */
  waitUntilReady(): Promise<void>;
  /** Stops reconnecting and closes the current session. */
  close(closeInfo?: { closeCode?: number; reason?: string }): void;
}
