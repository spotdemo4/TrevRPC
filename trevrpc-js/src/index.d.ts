import type { Long, Message, Root, Type } from "protobufjs";

export const protobuf: typeof import("protobufjs");

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

/** Error carrying a TrevRPC status code, message, and metadata. */
export class TrevRpcError extends Error {
  code: StatusCode;
  statusMessage: string;
  metadata: Metadata;

  /** Creates a TrevRPC status error. */
  constructor(code: number, message?: string, metadata?: Metadata);
}

/** Error reported when a frame exceeds the configured size limit. */
export class FrameTooLargeError extends Error {
  length: number;
  max: number;

  /** Creates a frame size error. */
  constructor(length: number, max: number);
}

/** Converts a number into a known status code, defaulting unknown values to Unknown. */
export function codeFromNumber(code: number): StatusCode;
/** Returns the canonical status code name. */
export function codeName(code: number): string;
/** Creates a TrevRPC status error. */
export function statusError(code: number, message?: string, metadata?: Metadata): TrevRpcError;
/** Builds a status error from an RPC response. */
export function statusFromResponse(response?: RpcResponseMessage | null): TrevRpcError;
/** Reports whether a status-like value is OK. */
export function isOkStatus(status?: { code?: number } | null): boolean;
/** Creates an internal error status. */
export function internal(message: string): TrevRpcError;
/** Creates an invalid-argument status. */
export function invalidArgument(message: string): TrevRpcError;
/** Creates a deadline-exceeded status. */
export function deadlineExceeded(message: string): TrevRpcError;
/** Creates an unavailable status. */
export function unavailable(message: string): TrevRpcError;
/** Creates a resource-exhausted status. */
export function resourceExhausted(message: string): TrevRpcError;
/** Creates a cancelled status. */
export function cancelled(message: string): TrevRpcError;

export const MaxMetadataEntries: 64;
export const MaxMetadataKeyLen: 128;
export const MaxMetadataValueLen: number;
export const MaxMetadataTotalSize: number;
export const ReservedMetadataPrefix: "trevrpc-";

export type MetadataValue = string | Uint8Array | ArrayBuffer | ArrayBufferView | readonly number[];
export type MetadataInput = Record<string, MetadataValue> | ReadonlyMap<string, MetadataValue>;
export type Metadata = Record<string, Uint8Array>;

/** Converts a metadata value into bytes. */
export function metadataValueToBytes(value: MetadataValue): Uint8Array;
/** Normalizes a metadata key to lowercase ASCII. */
export function normalizeMetadataKey(key: string): string;
/** Normalizes metadata keys and converts metadata values to bytes. */
export function normalizeMetadata(metadata?: MetadataInput): Metadata;
/** Validates metadata key syntax, value sizes, and total metadata limits. */
export function validateMetadata(metadata?: MetadataInput): void;

export const WireVersion: 1;
export const RpcKind: Readonly<{
  Unary: 0;
  ClientStreaming: 1;
  ServerStreaming: 2;
  BidirectionalStreaming: 3;
}>;
export const RpcStreamFrameKind: Readonly<{
  Message: 0;
  Status: 1;
}>;

export type RpcKindValue = (typeof RpcKind)[keyof typeof RpcKind];
export type RpcStreamFrameKindValue = (typeof RpcStreamFrameKind)[keyof typeof RpcStreamFrameKind];
export type RpcMethodKind =
  | "unary"
  | "clientStreaming"
  | "serverStreaming"
  | "bidirectionalStreaming";

export interface RpcRequestMessage {
  service: string;
  method: string;
  body: Uint8Array;
  metadata: Metadata;
  kind: RpcKindValue;
  version: number;
  timeoutNanos?: number | string | Long;
}

export interface RpcResponseMessage {
  status?: number;
  message?: string;
  body?: Uint8Array;
  metadata?: Metadata;
}

export interface RpcStreamFrameMessage {
  kind: RpcStreamFrameKindValue;
  status?: number;
  message?: string;
  body?: Uint8Array;
  metadata?: Metadata;
}

export interface RpcMethodDescriptor {
  name: string;
  kind: RpcMethodKind;
  inputType: string;
  outputType: string;
}

export interface RpcServiceDescriptor<
  TMethods extends Record<string, RpcMethodDescriptor> = Record<string, RpcMethodDescriptor>,
> {
  name: string;
  fullName: string;
  exportName: string;
  methods: TMethods;
}

export const wireRoot: Root;
export const RpcRequest: Type;
export const RpcResponse: Type;
export const RpcStreamFrame: Type;

/** Creates a protobuf root from a JSON namespace definition. */
export function createRoot(json: import("protobufjs").INamespace): Root;
/** Creates a stream message frame carrying a protobuf body. */
export function messageFrame(body: Uint8Array): RpcStreamFrameMessage;

export const DefaultMaxFrameSize: number;

/** Encodes a protobuf message body. */
export function marshalMessage<TRequest extends object>(
  messageType: Type,
  message: TRequest,
): Uint8Array;
/** Decodes a protobuf message body. */
export function unmarshalMessage<TResponse = Message<Record<string, unknown>>>(
  messageType: Type,
  body: Uint8Array,
): TResponse;
/** Encodes a protobuf message into a length-prefixed TrevRPC frame. */
export function encodeFrame<TMessage extends object>(
  messageType: Type,
  message: TMessage,
  maxFrameSize?: number,
): Uint8Array;
/** Encodes one streaming message frame carrying an already-encoded protobuf body. */
export function encodeMessageStreamFrame(body: Uint8Array, maxFrameSize?: number): Uint8Array;
/** Encodes multiple streaming message frames into one contiguous write buffer. */
export function encodeMessageStreamFrames(
  bodies: Iterable<Uint8Array>,
  maxFrameSize?: number,
): Uint8Array;
/** Decodes a protobuf message from a TrevRPC frame body. */
export function decodeFrame<TMessage = Message<Record<string, unknown>>>(
  messageType: Type,
  body: Uint8Array,
): TMessage;
/** Decodes a streaming RPC frame body. */
export function decodeStreamFrameBody(body: Uint8Array): RpcStreamFrameMessage;
/** Writes one length-prefixed protobuf frame. */
export function writeFrame<TMessage extends object>(
  writer: WritableStreamDefaultWriter<Uint8Array>,
  messageType: Type,
  message: TMessage,
  maxFrameSize?: number,
): Promise<void>;
/** Writes multiple streaming message frames in one write. */
export function writeMessageStreamFrames(
  writer: WritableStreamDefaultWriter<Uint8Array>,
  bodies: Iterable<Uint8Array>,
  maxFrameSize?: number,
): Promise<void>;
/** Decodes and validates the body length stored in a TrevRPC frame header. */
export function frameBodyLength(header: Uint8Array, maxFrameSize?: number): number;

/** Reads length-prefixed protobuf frames from a byte stream reader. */
export class FrameReader {
  /** Creates a frame reader over a stream reader. */
  constructor(reader: ReadableStreamDefaultReader<Uint8Array>);

  /** Reads and decodes one frame. */
  readFrame<TMessage = Message<Record<string, unknown>>>(
    messageType: Type,
    maxFrameSize?: number,
  ): Promise<TMessage>;
  /** Reads and decodes one frame, or returns null when already at EOF. */
  readFrameOrEOF<TMessage = Message<Record<string, unknown>>>(
    messageType: Type,
    maxFrameSize?: number,
  ): Promise<TMessage | null>;
  /** Reads and decodes one streaming RPC frame, or returns null when already at EOF. */
  readStreamFrameOrEOF(maxFrameSize?: number): Promise<RpcStreamFrameMessage | null>;
  /** Reads one streaming RPC frame, then drains complete frames already buffered. */
  readStreamFrameBatchOrEOF(
    maxBatch?: number,
    maxFrameSize?: number,
  ): Promise<RpcStreamFrameMessage[] | null>;
  /** Reads stream message body batches, with at most one terminal status. */
  readStreamMessageBodyBatchOrEOF(
    maxBatch?: number,
    maxFrameSize?: number,
  ): Promise<{ bodies: Uint8Array[]; status: RpcStreamFrameMessage | null } | null>;
  /** Reads one raw frame body, or returns null when already at EOF. */
  readRawFrameBodyOrEOF(maxFrameSize?: number): Promise<Uint8Array | null>;
  /** Reads exactly size bytes from the underlying reader. */
  readExact(size: number, allowEofAtStart: boolean): Promise<Uint8Array | null>;
  /** Reads a complete buffered frame body without waiting, or undefined. */
  tryReadFrameBody(maxFrameSize?: number): Uint8Array | undefined;
  /** Removes size bytes from the buffered data. */
  consume(size: number): Uint8Array;
  /** Copies size bytes from the buffered data without removing them. */
  peek(size: number): Uint8Array;
}

export interface CallOptions {
  timeoutMs?: number;
  maxFrameSize?: number;
  maxResponseBodySize?: number;
  maxResponseMessages?: number;
  maxResponseStreamBodySize?: number;
  streamIdleTimeoutMs?: number;
  metadata?: MetadataInput;
  signal?: AbortSignal;
}

export interface ResolvedCallOptions {
  timeoutMs?: number;
  maxFrameSize?: number;
  maxResponseBodySize: number;
  maxResponseMessages: number;
  maxResponseStreamBodySize: number;
  streamIdleTimeoutMs: number;
  metadata: Metadata;
  signal?: AbortSignal;
}

export interface Transport {
  /** Sends a unary RPC request and returns its response. */
  call(request: RpcRequestMessage, options?: ResolvedCallOptions): Promise<RpcResponseMessage>;
  /** Sends a streaming RPC request and returns response frames. */
  streamingCall(
    request: RpcRequestMessage,
    requestBody: AsyncIterable<Uint8Array>,
    options?: ResolvedCallOptions,
  ): Promise<AsyncIterable<RpcStreamFrameMessage>>;
}

export interface UnaryResponse<TResponse> {
  message: TResponse;
  metadata: Metadata;
}

export interface StreamStatus {
  code: StatusCode;
  message: string;
  metadata: Metadata;
}

export interface ResponseAsyncIterable<TResponse> extends AsyncIterable<TResponse> {
  status: Promise<StreamStatus>;
}

export interface ClientStreamingCall<TRequest extends object, TResponse> {
  /** Sends one request message. */
  send(request: TRequest): Promise<void>;
  /** Sends multiple request messages. */
  sendMany(requests: Iterable<TRequest>): Promise<void>;
  /** Closes the request stream. */
  closeSend(): Promise<void>;
  /** Closes the request stream and returns the final response. */
  closeAndRecv(): Promise<TResponse>;
  /** Closes the request stream and returns the final response envelope. */
  closeAndRecvWithResponse(): Promise<UnaryResponse<TResponse>>;
  /** Releases call resources without waiting for the response. */
  close(): Promise<void>;
}

export interface BidirectionalStreamingCall<
  TRequest extends object,
  TResponse,
> extends AsyncIterable<TResponse> {
  /** Resolves to the terminal stream status after completion. */
  status: Promise<StreamStatus>;
  /** Sends one request message. */
  send(request: TRequest): Promise<void>;
  /** Sends multiple request messages. */
  sendMany(requests: Iterable<TRequest>): Promise<void>;
  /** Receives one response message, or undefined after the response stream completes. */
  recv(): Promise<TResponse | undefined>;
  /** Closes the request stream while keeping the response stream readable. */
  closeSend(): Promise<void>;
  /** Releases call resources without waiting for more responses. */
  close(): Promise<void>;
}

export type ServiceClient = Record<
  string,
  (requestOrOptions?: unknown, options?: CallOptions) => Promise<unknown>
>;

/** Returns the default client call options. */
export function defaultCallOptions(): ResolvedCallOptions;
/** Merges call options and normalizes metadata. */
export function mergeCallOptions(base?: CallOptions, override?: CallOptions): ResolvedCallOptions;
/** Calls a unary RPC and decodes the protobuf response. */
export function unary<
  TRequest extends object = Record<string, unknown>,
  TResponse = Message<Record<string, unknown>>,
>(
  transport: Transport,
  service: string,
  method: string,
  requestType: Type,
  responseType: Type,
  request: TRequest,
  options?: CallOptions,
): Promise<TResponse>;
/** Calls a unary RPC and returns the decoded response envelope. */
export function unaryWithResponse<
  TRequest extends object = Record<string, unknown>,
  TResponse = Message<Record<string, unknown>>,
>(
  transport: Transport,
  service: string,
  method: string,
  requestType: Type,
  responseType: Type,
  request: TRequest,
  options?: CallOptions,
): Promise<UnaryResponse<TResponse>>;
/** Calls a server-streaming RPC and returns a decoded response stream. */
export function serverStreaming<
  TRequest extends object = Record<string, unknown>,
  TResponse = Message<Record<string, unknown>>,
>(
  transport: Transport,
  service: string,
  method: string,
  requestType: Type,
  responseType: Type,
  request: TRequest,
  options?: CallOptions,
): Promise<ResponseAsyncIterable<TResponse>>;
/** Calls a client-streaming RPC and returns a sendable call object. */
export function clientStreaming<
  TRequest extends object = Record<string, unknown>,
  TResponse = Message<Record<string, unknown>>,
>(
  transport: Transport,
  service: string,
  method: string,
  requestType: Type,
  responseType: Type,
  options?: CallOptions,
): Promise<ClientStreamingCall<TRequest, TResponse>>;
/** Calls a bidirectional-streaming RPC and returns a sendable call object. */
export function bidirectionalStreaming<
  TRequest extends object = Record<string, unknown>,
  TResponse = Message<Record<string, unknown>>,
>(
  transport: Transport,
  service: string,
  method: string,
  requestType: Type,
  responseType: Type,
  options?: CallOptions,
): Promise<BidirectionalStreamingCall<TRequest, TResponse>>;
/** Creates a service client from a generated service descriptor. */
export function createServiceClient<TClient extends ServiceClient = ServiceClient>(
  transport: Transport,
  service: RpcServiceDescriptor,
  root: Root,
  options?: CallOptions,
): TClient;

export interface WebTransportCloseInfoLike {
  closeCode?: number;
  reason?: string;
}

export interface WebTransportBidirectionalStreamLike {
  readable: ReadableStream<Uint8Array>;
  writable: WritableStream<Uint8Array>;
}

export interface WebTransportSessionLike {
  ready: Promise<void>;
  close?(closeInfo?: WebTransportCloseInfoLike): void;
  createBidirectionalStream?(): Promise<WebTransportBidirectionalStreamLike>;
}

export interface WebTransportConstructorLike {
  new (url: string | URL, options?: unknown): WebTransportSessionLike;
}

export interface WebTransportCertificateHash {
  algorithm: string;
  value: BufferSource;
}

export interface BrowserWebTransportOptions {
  allowPooling?: boolean;
  congestionControl?: "default" | "low-latency" | "throughput";
  requireUnreliable?: boolean;
  serverCertificateHashes?: readonly WebTransportCertificateHash[];
}

export interface WebTransportClientOptions extends CallOptions, BrowserWebTransportOptions {
  WebTransport?: WebTransportConstructorLike;
  webTransportOptions?: unknown;
  streamReadBatchMaxMessages?: number;
  streamWriteBatchMaxMessages?: number;
  streamWriteBatchMaxBytes?: number;
}

export interface NodeConnectOptions {
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

export interface ConnectOptions extends WebTransportClientOptions, NodeConnectOptions {}

export interface ConnectedTransport extends Transport {
  /** Closes the underlying WebTransport session or native client. */
  close(closeInfo?: WebTransportCloseInfoLike): void;
}

/** Opens a TrevRPC client for the current JavaScript runtime. */
export function connect(url: string | URL, options?: ConnectOptions): Promise<ConnectedTransport>;

/** Transport implementation for TrevRPC over WebTransport. */
export class WebTransportClient implements Transport {
  session: WebTransportSessionLike;
  maxFrameSize: number;

  /** Creates a client over an established WebTransport session. */
  constructor(session: WebTransportSessionLike, options?: WebTransportClientOptions);

  /** Opens a WebTransport session and wraps it in a TrevRPC client. */
  static connect(
    url: string | URL,
    options?: WebTransportClientOptions,
  ): Promise<WebTransportClient>;
  /** Waits for the underlying WebTransport session to become ready. */
  ready(): Promise<void>;
  /** Closes the underlying WebTransport session. */
  close(closeInfo?: WebTransportCloseInfoLike): void;
  /** Sends a unary RPC request over WebTransport and returns its response. */
  call(request: RpcRequestMessage, options?: ResolvedCallOptions): Promise<RpcResponseMessage>;
  /** Sends a streaming RPC request over WebTransport and returns response frames. */
  streamingCall(
    request: RpcRequestMessage,
    requestBody: AsyncIterable<Uint8Array>,
    options?: ResolvedCallOptions,
  ): Promise<AsyncIterableIterator<RpcStreamFrameMessage>>;
  /** Opens a bidirectional WebTransport stream. */
  openBidirectionalStream(): Promise<WebTransportBidirectionalStreamLike>;
}
