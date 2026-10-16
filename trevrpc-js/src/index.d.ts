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
  /** Cancels the call unless it already finished. */
  close(reason?: unknown): Promise<void>;
}

export interface ClientStreamingCall<TRequest, TResponse> {
  /** Sends one request message. */
  send(request: TRequest): Promise<void>;
  /** Closes the request stream and returns the final response. */
  closeAndRecv(): Promise<TResponse>;
  /** Closes the request stream and returns the final response with its metadata. */
  closeAndRecvWithResponse(): Promise<UnaryResponse<TResponse>>;
  /** Cancels the call unless it already finished. */
  close(reason?: unknown): Promise<void>;
}

export interface BidirectionalStreamingCall<TRequest, TResponse> extends AsyncIterable<TResponse> {
  /** Settles with the final status, or rejects with the error that ended the call. */
  readonly status: Promise<StreamStatus>;
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

export interface ConnectOptions {
  allowPooling?: boolean;
  congestionControl?: "default" | "low-latency" | "throughput";
  requireUnreliable?: boolean;
  /** Trusts a self-signed ECDSA certificate (at most 14 days old) by its SHA-256 hash. */
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
}

/** Opens a channel to a WebTransport server and waits for its session to be ready. */
export function connect(url: string | URL, options?: ConnectOptions): Promise<Channel>;

/**
 * A WebTransport session that reconnects in the background after it is lost. Calls fail fast
 * with Unavailable while it reconnects and are never replayed.
 */
export class Channel implements Transport {
  private constructor();

  /** Opens a channel to a WebTransport server and waits for its session to be ready. */
  static connect(url: string | URL, options?: ConnectOptions): Promise<Channel>;

  readonly state: ChannelState;
  createBidirectionalStream(): Promise<TransportStream>;
  /** Resolves once the channel is ready, immediately if it already is. */
  waitUntilReady(): Promise<void>;
  /** Stops reconnecting and closes the current session. */
  close(closeInfo?: { closeCode?: number; reason?: string }): void;
}
