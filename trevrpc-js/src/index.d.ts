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

export class TrevRpcError extends Error {
  code: StatusCode;
  statusMessage: string;
  metadata: Metadata;

  constructor(code: number, message?: string, metadata?: Metadata);
}

export class FrameTooLargeError extends Error {
  length: number;
  max: number;

  constructor(length: number, max: number);
}

export function codeFromNumber(code: number): StatusCode;
export function codeName(code: number): string;
export function statusError(code: number, message?: string, metadata?: Metadata): TrevRpcError;
export function statusFromResponse(response?: RpcResponseMessage | null): TrevRpcError;
export function isOkStatus(status?: { code?: number } | null): boolean;
export function internal(message: string): TrevRpcError;
export function invalidArgument(message: string): TrevRpcError;
export function deadlineExceeded(message: string): TrevRpcError;
export function unavailable(message: string): TrevRpcError;
export function resourceExhausted(message: string): TrevRpcError;
export function cancelled(message: string): TrevRpcError;

export const MaxMetadataEntries: 64;
export const MaxMetadataKeyLen: 128;
export const MaxMetadataValueLen: number;
export const MaxMetadataTotalSize: number;
export const ReservedMetadataPrefix: "trevrpc-";

export type MetadataValue = string | Uint8Array | ArrayBuffer | ArrayBufferView | readonly number[];
export type MetadataInput = Record<string, MetadataValue> | ReadonlyMap<string, MetadataValue>;
export type Metadata = Record<string, Uint8Array>;

export function metadataValueToBytes(value: MetadataValue): Uint8Array;
export function normalizeMetadataKey(key: string): string;
export function normalizeMetadata(metadata?: MetadataInput): Metadata;
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
  deadlineUnixNanos?: number | string | Long;
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

export function createRoot(json: import("protobufjs").INamespace): Root;
export function messageFrame(body: Uint8Array): RpcStreamFrameMessage;

export const DefaultMaxFrameSize: number;

export function marshalMessage<TRequest extends object>(
  messageType: Type,
  message: TRequest,
): Uint8Array;
export function unmarshalMessage<TResponse = Message<Record<string, unknown>>>(
  messageType: Type,
  body: Uint8Array,
): TResponse;
export function encodeFrame<TMessage extends object>(
  messageType: Type,
  message: TMessage,
  maxFrameSize?: number,
): Uint8Array;
export function decodeFrame<TMessage = Message<Record<string, unknown>>>(
  messageType: Type,
  body: Uint8Array,
): TMessage;
export function writeFrame<TMessage extends object>(
  writer: WritableStreamDefaultWriter<Uint8Array>,
  messageType: Type,
  message: TMessage,
  maxFrameSize?: number,
): Promise<void>;
export function frameBodyLength(header: Uint8Array, maxFrameSize?: number): number;

export class FrameReader {
  constructor(reader: ReadableStreamDefaultReader<Uint8Array>);

  readFrame<TMessage = Message<Record<string, unknown>>>(
    messageType: Type,
    maxFrameSize?: number,
  ): Promise<TMessage>;
  readFrameOrEOF<TMessage = Message<Record<string, unknown>>>(
    messageType: Type,
    maxFrameSize?: number,
  ): Promise<TMessage | null>;
  readExact(size: number, allowEofAtStart: boolean): Promise<Uint8Array | null>;
  consume(size: number): Uint8Array;
}

export interface CallOptions {
  timeoutMs?: number;
  maxFrameSize?: number;
  maxResponseBodySize?: number;
  maxResponseMessages?: number;
  maxResponseStreamBodySize?: number;
  streamIdleTimeoutMs?: number;
  metadata?: MetadataInput;
}

export interface ResolvedCallOptions {
  timeoutMs?: number;
  maxFrameSize?: number;
  maxResponseBodySize: number;
  maxResponseMessages: number;
  maxResponseStreamBodySize: number;
  streamIdleTimeoutMs: number;
  metadata: Metadata;
}

export interface Transport {
  call(request: RpcRequestMessage, options?: ResolvedCallOptions): Promise<RpcResponseMessage>;
  streamingCall(
    request: RpcRequestMessage,
    requestBody: AsyncIterable<Uint8Array>,
    options?: ResolvedCallOptions,
  ): Promise<AsyncIterable<RpcStreamFrameMessage>>;
}

export type ServiceClient = Record<
  string,
  (requestOrRequests: unknown, options?: CallOptions) => Promise<unknown>
>;

export function defaultCallOptions(): ResolvedCallOptions;
export function mergeCallOptions(base?: CallOptions, override?: CallOptions): ResolvedCallOptions;
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
): Promise<AsyncIterable<TResponse>>;
export function clientStreaming<
  TRequest extends object = Record<string, unknown>,
  TResponse = Message<Record<string, unknown>>,
>(
  transport: Transport,
  service: string,
  method: string,
  requestType: Type,
  responseType: Type,
  requests: AsyncIterable<TRequest>,
  options?: CallOptions,
): Promise<TResponse>;
export function bidirectionalStreaming<
  TRequest extends object = Record<string, unknown>,
  TResponse = Message<Record<string, unknown>>,
>(
  transport: Transport,
  service: string,
  method: string,
  requestType: Type,
  responseType: Type,
  requests: AsyncIterable<TRequest>,
  options?: CallOptions,
): Promise<AsyncIterable<TResponse>>;
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

export interface WebTransportClientOptions extends CallOptions {
  WebTransport?: WebTransportConstructorLike;
  webTransportOptions?: unknown;
}

export class WebTransportClient implements Transport {
  session: WebTransportSessionLike;
  maxFrameSize: number;

  constructor(session: WebTransportSessionLike, options?: WebTransportClientOptions);

  static connect(
    url: string | URL,
    options?: WebTransportClientOptions,
  ): Promise<WebTransportClient>;
  ready(): Promise<void>;
  close(closeInfo?: WebTransportCloseInfoLike): void;
  call(request: RpcRequestMessage, options?: ResolvedCallOptions): Promise<RpcResponseMessage>;
  streamingCall(
    request: RpcRequestMessage,
    requestBody: AsyncIterable<Uint8Array>,
    options?: ResolvedCallOptions,
  ): Promise<WebTransportResponseFrameStream>;
  openBidirectionalStream(): Promise<WebTransportBidirectionalStreamLike>;
}

export class WebTransportResponseFrameStream implements AsyncIterableIterator<RpcStreamFrameMessage> {
  constructor(
    reader: ReadableStreamDefaultReader<Uint8Array>,
    writer: WritableStreamDefaultWriter<Uint8Array>,
    writerTask: Promise<void>,
    maxFrameSize?: number,
  );

  [Symbol.asyncIterator](): AsyncIterableIterator<RpcStreamFrameMessage>;
  next(): Promise<IteratorResult<RpcStreamFrameMessage>>;
  return(): Promise<IteratorResult<RpcStreamFrameMessage>>;
}
