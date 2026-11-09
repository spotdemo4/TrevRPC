import type { DescMessage, DescService, MessageInitShape, MessageShape } from "@bufbuild/protobuf";

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

/**
 * One detail of a status: a protobuf message, carried as its full name, such as
 * `"buf.validate.Violations"`, and its encoding.
 */
export interface StatusDetail {
  readonly type: string;
  readonly value: Uint8Array;
}

export interface TrevRpcErrorOptions extends ErrorOptions {
  details?: readonly StatusDetail[];
}

/** Error carrying a TrevRPC status code, message, metadata, and details. */
export class TrevRpcError extends Error {
  /** Always `"TrevRpcError"`, for matching errors by tag, as Effect's `catchTag` does. */
  readonly _tag: "TrevRpcError";
  readonly code: StatusCode;
  readonly statusMessage: string;
  readonly metadata: Metadata;
  /** The status details, still encoded; `findDetails` decodes those of one message type. */
  readonly details: readonly StatusDetail[];

  constructor(code: number, message?: string, metadata?: Metadata, options?: TrevRpcErrorOptions);

  /**
   * Decodes the details whose type is the full name of `schema`, such as `ViolationsSchema` for
   * `buf.validate.Violations`.
   */
  findDetails<Desc extends DescMessage>(schema: Desc): MessageShape<Desc>[];
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
   * Called with the response headers, empty when the server sent none, as soon as they, the first
   * message or the final status arrive. A callback that throws fails the call.
   */
  onResponseHeaders?(headers: Metadata): void;
  /**
   * Called with the trailing metadata of the final status when the call succeeds, before its
   * result is returned or its response stream ends. A failed call reports its metadata on the
   * TrevRpcError instead. A callback that throws fails the call.
   */
  onResponseMetadata?(metadata: Metadata): void;
}

/** The kind of an RPC. */
export type CallKind = "unary" | "serverStreaming" | "clientStreaming" | "bidirectionalStreaming";

/** What a call interceptor sees of a call before it starts. */
export interface InterceptedCall {
  /** The method's full path, such as `/example.greeter.Greeter/SayHello`. */
  readonly method: string;
  /** The service's full name, such as `example.greeter.Greeter`. */
  readonly service: string;
  /** The method's name, such as `SayHello`. */
  readonly name: string;
  readonly kind: CallKind;
  /**
   * The call's request metadata. An interceptor may add, replace and delete entries, such as a
   * refreshed token; the result is validated after the last interceptor, and invalid metadata
   * fails the call with InvalidArgument.
   */
  metadata: Record<string, MetadataValue>;
  /** The call's deadline, which starts once the interceptors are done. */
  readonly timeoutMs: number | undefined;
  /** The call's abort signal, which an interceptor that waits can watch. */
  readonly signal: AbortSignal | undefined;
}

/** How an intercepted call ended. */
export interface CallEnd {
  /** `Code.Ok` when the call succeeded; the code of `error` otherwise, or `Code.Unknown`. */
  readonly code: StatusCode;
  /** What failed the call, usually a TrevRpcError; undefined when it succeeded. */
  readonly error: unknown;
}

/** Called once when an intercepted call ends. A finish function that throws is reported, not rethrown. */
export type CallFinish = (end: CallEnd) => void;

/**
 * Runs before each call of a client, in order with the others. It may change the call's metadata,
 * wait (for example for a token refresh), and return a function that is called once when the call
 * ends: when a unary call settles, and when a streaming call receives its final status or fails,
 * including when it is cancelled. Finish functions run innermost first. A streaming call that is
 * neither read to its end nor closed does not end. An interceptor that throws fails the call with
 * what it threw, before it starts; the finish functions of earlier interceptors still run.
 */
export type CallInterceptor = (
  call: InterceptedCall,
) => void | CallFinish | Promise<void | CallFinish>;

/** Options of a service client: defaults for its calls, and interceptors that run around them. */
export interface ClientOptions extends CallOptions {
  /** Interceptors for the client's calls, after those of its transport, such as a Channel's. */
  interceptors?: readonly CallInterceptor[];
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

declare const compressionBrand: unique symbol;
declare const connectionMetadataBrand: unique symbol;
declare const streamCompressionBudgetBrand: unique symbol;

/**
 * Compression settings, made by `createCompression()` from `@trevrpc/trevrpc-js/zstd`: the zstd
 * dictionaries a channel holds and the message types each compresses. See `ConnectOptions`.
 */
export interface Compression {
  readonly [compressionBrand]: true;
}

/**
 * A Channel's connection metadata, which `Channel.setConnectionMetadata()` replaces. See
 * `Transport.connectionMetadata`.
 */
export interface ConnectionMetadataSource {
  readonly [connectionMetadataBrand]: true;
}

/**
 * A Channel's stream compression memory budget, from `ConnectOptions.streamCompressionMemory`.
 * See `Transport.streamCompressionBudget`.
 */
export interface StreamCompressionBudget {
  readonly [streamCompressionBudgetBrand]: true;
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
  /** Compression settings for calls on this transport; absent without compression. */
  readonly compression?: Compression;
  /**
   * The connection metadata calls on this transport carry; a Channel's own. A transport that
   * wraps a Channel forwards it, or its calls carry no connection metadata.
   */
  readonly connectionMetadata?: ConnectionMetadataSource;
  /**
   * The memory budget of the stream compression contexts of calls on this transport; a Channel's
   * own. A transport that wraps a Channel forwards it, or its calls get a default budget of their
   * own.
   */
  readonly streamCompressionBudget?: StreamCompressionBudget;
  /**
   * Interceptors that clients created on this transport run around every call, before their own;
   * a Channel's own. A transport that wraps a Channel forwards them, or its clients skip them.
   */
  readonly interceptors?: readonly CallInterceptor[];
  /**
   * Like `createBidirectionalStream()`, but also returns the session that opened the stream.
   * Calls negotiate dictionaries and open datagram flows per session, so transports whose session
   * can change, such as a Channel, implement it so a call never mixes the stream of one session
   * with the state of another.
   */
  createBidirectionalStreamWithSession?(): Promise<{
    stream: TransportStream;
    session: { readonly datagrams?: DatagramTransport };
  }>;
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
 *
 * Buffered messages are delivered without returning to the event loop, so a page that renders a
 * long stream must yield to let the browser render; see "Rendering long streams" in the guide.
 */
export interface ResponseStream<TResponse> extends AsyncIterable<TResponse> {
  /** Settles with the final status, or rejects with the error that ended the call. */
  readonly status: Promise<StreamStatus>;
  /**
   * The response headers, empty when the server sent none. Settles when they, the first message or
   * the final status arrive, before the first message is received, or rejects with the error that
   * ended the call.
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
  /** Settles with the final status, or rejects with the error that ended the call. */
  readonly status: Promise<StreamStatus>;
  /** Closes the request stream and returns the final response. */
  closeAndRecv(): Promise<TResponse>;
  /** Cancels the call unless it already finished. */
  close(reason?: unknown): Promise<void>;
}

/**
 * On a method whose `trevrpc.method` option marks a direction unreliable (`unreliable_requests` or
 * `unreliable_responses`), a call on a session with datagrams may send its request messages, and
 * receive its response messages, as datagrams: they can be lost or reordered, never duplicated.
 * Response datagrams are delivered after the response headers, merged with the stream's messages
 * in arrival order, and dropped when the call's queue is full (32 messages or 1 MiB) or the call
 * has ended. Without datagrams, such as over WebSocket, everything stays on the stream.
 *
 * Like a `ResponseStream`, iteration delivers buffered messages without returning to the event
 * loop.
 */
export interface BidirectionalStreamingCall<TRequest, TResponse> extends AsyncIterable<TResponse> {
  /** Settles with the final status, or rejects with the error that ended the call. */
  readonly status: Promise<StreamStatus>;
  /**
   * The response headers, empty when the server sent none. Settles when they, the first message or
   * the final status arrive, before the first message is received, or rejects with the error that
   * ended the call.
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

/** A unary method of a client: sends one request and resolves with the response. */
export type UnaryMethod<I extends DescMessage, O extends DescMessage> = (
  request: MessageInitShape<I>,
  options?: CallOptions,
) => Promise<MessageShape<O>>;

/** A server-streaming method of a client: sends one request and resolves with the responses. */
export type ServerStreamingMethod<I extends DescMessage, O extends DescMessage> = (
  request: MessageInitShape<I>,
  options?: CallOptions,
) => Promise<ResponseStream<MessageShape<O>>>;

/** A client-streaming method of a client: starts a call to send requests on. */
export type ClientStreamingMethod<I extends DescMessage, O extends DescMessage> = (
  options?: CallOptions,
) => Promise<ClientStreamingCall<MessageInitShape<I>, MessageShape<O>>>;

/** A bidirectional-streaming method of a client: starts a call to send and receive on. */
export type BidirectionalStreamingMethod<I extends DescMessage, O extends DescMessage> = (
  options?: CallOptions,
) => Promise<BidirectionalStreamingCall<MessageInitShape<I>, MessageShape<O>>>;

/**
 * The client of a protobuf-es service descriptor: one function per method, named by its
 * `localName`. Requests take a message or the init object `create()` takes; responses are
 * messages.
 */
export type Client<S extends DescService> = {
  [K in keyof S["method"]]: S["method"][K] extends {
    methodKind: infer Kind;
    input: infer I extends DescMessage;
    output: infer O extends DescMessage;
  }
    ? Kind extends "unary"
      ? UnaryMethod<I, O>
      : Kind extends "server_streaming"
        ? ServerStreamingMethod<I, O>
        : Kind extends "client_streaming"
          ? ClientStreamingMethod<I, O>
          : Kind extends "bidi_streaming"
            ? BidirectionalStreamingMethod<I, O>
            : never
    : never;
};

/**
 * Creates a client for `service`, a service descriptor that protoc-gen-es generates (such as
 * `GreeterService` from `greeter_pb.js`). The `trevrpc.method` option of each method decides
 * what its calls send as datagrams and compress; one the method's kind cannot use throws
 * InvalidArgument. Calls run the transport's interceptors, then those of `options.interceptors`.
 */
export function createClient<S extends DescService>(
  service: S,
  transport: Transport,
  options?: ClientOptions,
): Client<S>;

export interface WebTransportCertificateHash {
  algorithm: string;
  value: ArrayBuffer | ArrayBufferView;
}

export interface FetchServerCertificateHashesOptions {
  signal?: AbortSignal;
  /** The fetch implementation to use instead of the global one. */
  fetch?: (input: string | URL, init?: RequestInit) => Promise<Response>;
}

/**
 * Fetches the certificate hashes a TrevRPC development server publishes, as JSON such as
 * `[{"algorithm":"sha-256","value":"<hex>"}]`, for `serverCertificateHashes`. A 404 yields no
 * hashes; any other failure rejects with Unavailable.
 */
export function fetchServerCertificateHashes(
  url: string | URL,
  options?: FetchServerCertificateHashesOptions,
): Promise<WebTransportCertificateHash[]>;

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
   * A URL that publishes the server's development certificate hashes, such as a path that the
   * development web server proxies to the TrevRPC server's DevelopmentCertificate. Every
   * WebTransport attempt fetches it, so reconnects follow a rotated certificate, and adds its
   * hashes to `serverCertificateHashes`; a 404 adds none, so the same build works against a server
   * with a trusted certificate. A failed fetch fails the WebTransport attempt.
   */
  serverCertificateHashesUrl?: string | URL;
  /**
   * WebTransport implementation to use instead of the global one. It is constructed with
   * `protocols: ["trevrpc-1"]`, the wire version, which cannot be changed.
   */
  WebTransport?: new (url: string | URL, options?: object) => WebTransportSessionLike;
  /**
   * Compresses messages with the dictionaries these settings hold, from `createCompression()` in
   * `@trevrpc/trevrpc-js/zstd`. Dictionaries are negotiated with the server per session, so a
   * message whose dictionary the server lacks travels uncompressed. Compression settings also
   * enable stream compression on methods that allow it, even without dictionaries.
   */
  compression?: Compression;
  /**
   * The memory, in bytes, the stream compression contexts of this channel's calls may hold.
   * Defaults to 64 MiB; 0 turns stream compression off. Calls beyond it compress per message.
   */
  streamCompressionMemory?: number;
  /**
   * Request metadata for all of the channel's calls, such as a bearer token, which crosses each
   * connection once instead of in every request. A call's own `metadata` wins over an entry with
   * the same key. Keys are lowercase ASCII and the `trevrpc-` prefix is reserved, as for call
   * metadata. Replace it with `Channel.setConnectionMetadata()`.
   */
  connectionMetadata?: MetadataInput;
  /**
   * Interceptors that every client created on the channel runs around its calls, before the
   * client's own; see `CallInterceptor`.
   */
  interceptors?: readonly CallInterceptor[];
  /** Cancels the initial connection attempt of `connect()` only; `createChannel()` ignores it. */
  signal?: AbortSignal;
  /** Receives channel state transitions. */
  onStateChange?(change: ChannelStateChange): void;
}

export type ChannelState = "connecting" | "ready" | "reconnecting" | "closed";

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

/**
 * Opens a channel to a TrevRPC server and waits for its session to be ready. It rejects, leaving
 * nothing behind, if the first attempt fails.
 */
export function connect(url: string | URL, options?: ConnectOptions): Promise<Channel>;

/**
 * Returns a channel to a TrevRPC server that connects in the background, so that clients can be
 * created on it right away. Calls started before its first session is ready wait for that attempt
 * (within their own deadline and signal); a failed attempt fails them, and the channel goes on
 * reconnecting with backoff. Close the channel to give up; the `signal` option does not apply.
 * Throws if an option is invalid.
 */
export function createChannel(url: string | URL, options?: ConnectOptions): Channel;

/**
 * A session that reconnects in the background after it is lost: WebTransport, or QMux over
 * WebSocket when WebTransport is unavailable or blocked. Calls started while it first connects wait
 * for that attempt; afterwards they fail fast with Unavailable while it reconnects and are never
 * replayed.
 */
export class Channel implements Transport {
  private constructor();

  /** Opens a channel to a TrevRPC server and waits for its session to be ready. */
  static connect(url: string | URL, options?: ConnectOptions): Promise<Channel>;

  readonly state: ChannelState;
  /** The transport of the current or last session; undefined before the first. */
  readonly transport: TransportKind | undefined;
  /** The compression settings from the connect options. */
  readonly compression?: Compression;
  /** The connection metadata of the channel's calls, which a wrapping Transport forwards. */
  readonly connectionMetadata: ConnectionMetadataSource;
  /** The stream compression memory budget of the channel's calls, which a wrapping Transport forwards. */
  readonly streamCompressionBudget: StreamCompressionBudget;
  /** The interceptors from the connect options, which a wrapping Transport forwards. */
  readonly interceptors: readonly CallInterceptor[];
  /**
   * Replaces the channel's connection metadata (see `ConnectOptions.connectionMetadata`); empty or
   * missing metadata removes it. Calls started after this use the new metadata, and each
   * connection sends it again before referring to it. Throws if it is invalid, and then keeps the
   * old.
   */
  setConnectionMetadata(metadata?: MetadataInput): void;
  createBidirectionalStream(): Promise<TransportStream>;
  /** Opens a stream together with the session that carries it. */
  createBidirectionalStreamWithSession(): Promise<{
    stream: TransportStream;
    session: { readonly datagrams?: DatagramTransport };
  }>;
  /** Resolves once the channel is ready, immediately if it already is. */
  waitUntilReady(): Promise<void>;
  /** Stops reconnecting and closes the current session. */
  close(closeInfo?: { closeCode?: number; reason?: string }): void;
}
