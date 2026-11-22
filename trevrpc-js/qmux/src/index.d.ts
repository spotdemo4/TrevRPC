/** QUIC transport error codes (RFC 9000 §20.1) that a QMux protocol error carries. */
export declare const TransportError: Readonly<{
  NoError: 0x00;
  InternalError: 0x01;
  FlowControlError: 0x03;
  StreamLimitError: 0x04;
  StreamStateError: 0x05;
  FinalSizeError: 0x06;
  FrameEncodingError: 0x07;
  TransportParameterError: 0x08;
  ProtocolViolation: 0x0a;
}>;

export interface QMuxErrorOptions {
  /** "session" for errors that end the session, "stream" (the default) for stream resets. */
  source?: "session" | "stream";
  /** The application error code of a stream reset or stop. */
  streamErrorCode?: number | null;
  cause?: unknown;
}

/**
 * An error shaped like WebTransportError, which every session and stream failure is. Passing one
 * with a `streamErrorCode` to `abort()` or `cancel()` resets or stops the stream with that code;
 * any other reason sends code 0, as over WebTransport.
 */
export declare class QMuxError extends Error {
  constructor(message?: string, options?: QMuxErrorOptions);
  readonly name: "WebTransportError";
  readonly source: "session" | "stream";
  readonly streamErrorCode: number | null;
}

/**
 * A QMux protocol error, detected locally: it closes the session with a transport
 * CONNECTION_CLOSE, and is the `cause` of the QMuxError that `closed` rejects with.
 */
export declare class QMuxProtocolError extends Error {
  constructor(code: number, message: string, frameType?: number | bigint);
  readonly name: "QMuxProtocolError";
  /** A TransportError code. */
  readonly code: number;
  /** The type of the frame that caused the error, or 0. */
  readonly frameType: number | bigint;
}

/** The max_record_size a peer may assume until told otherwise, and the smallest it may set. */
export declare const DefaultMaxRecordSize: 16382;

/**
 * The transport parameters an endpoint advertises in its QX_TRANSPORT_PARAMETERS frame. A zero
 * leaves the parameter out, so it takes the protocol default: no credit, no streams, no datagrams.
 */
export interface Parameters {
  /**
   * Closes the session after this long without a record either way; 0 disables it. As
   * draft-ietf-quic-qmux-02 §7.1 defines it, every record received or sent restarts it,
   * keep-alives included.
   */
  maxIdleTimeoutMs: number;
  /** The connection-level receive window, in bytes. */
  initialMaxData: number;
  /** The receive window of bidirectional streams this endpoint opens. */
  initialMaxStreamDataBidiLocal: number;
  /** The receive window of bidirectional streams the peer opens. */
  initialMaxStreamDataBidiRemote: number;
  /** The receive window of unidirectional streams the peer opens. */
  initialMaxStreamDataUni: number;
  /** How many bidirectional streams the peer may open at once; the limit rises as they end. */
  initialMaxStreamsBidi: number;
  /** How many unidirectional streams the peer may open at once; the limit rises as they end. */
  initialMaxStreamsUni: number;
  /**
   * The largest DATAGRAM frame this endpoint accepts, including its type and length (RFC 9221).
   * Above 0 it enables `datagrams`. It must also fit in a record.
   */
  maxDatagramFrameSize: number;
  /** The largest record this endpoint accepts; 0 means DefaultMaxRecordSize. */
  maxRecordSize: number;
}

/**
 * What a session advertises unless `options.parameters` overrides it: a 1 MiB connection window,
 * 256 KiB per stream, 100 streams of each kind, 64 KiB records, no idle timeout and no datagrams.
 */
export declare const DefaultParameters: Readonly<Parameters>;

/** A minimal WebSocket: the browser's, or one a server runtime such as `ws` or Deno accepts. */
export interface QMuxWebSocket {
  binaryType: string;
  readonly readyState: number;
  readonly protocol: string;
  readonly bufferedAmount: number;
  send(data: Uint8Array): void;
  close(code?: number, reason?: string): void;
  onopen: ((event: unknown) => void) | null;
  onmessage: ((event: { data: unknown }) => void) | null;
  onerror: ((event: unknown) => void) | null;
  onclose: ((event: { code?: number }) => void) | null;
}

export interface QMuxSessionOptions {
  /**
   * The side of the session: clients open streams with even IDs, servers with odd ones. It
   * defaults to "client"; a session over a socket a server accepted passes "server".
   */
  role?: "client" | "server";
  /**
   * The WebSocket subprotocols a client offers, such as "myapp-qmux-02". QMux has no identifier
   * of its own, so the application defines one per draft version. The server must select one of
   * them. Ignored for a session over an existing socket.
   */
  protocols?: string[];
  /** The transport parameters to advertise, over DefaultParameters. */
  parameters?: Partial<Parameters>;
  /** Replaces the global WebSocket constructor when dialing a URL. */
  WebSocket?: new (url: string, protocols?: string | string[]) => QMuxWebSocket;
  /** Bounds opening the WebSocket and receiving the peer's transport parameters; 10 s by default. */
  handshakeTimeoutMs?: number;
  /**
   * Sends a QX_PING after this long without sending a record, at most half the idle timeout;
   * 0 (the default) disables it.
   */
  keepAlivePeriodMs?: number;
  /** Writes wait, and datagrams are dropped, while the WebSocket buffers more; 1 MiB by default. */
  sendBufferLimit?: number;
  /**
   * Bounds the records this endpoint builds, below the peer's max_record_size; 64 KiB by default,
   * and at least DefaultMaxRecordSize.
   */
  maxSendRecordSize?: number;
}

export interface QMuxStreamOptions {
  /**
   * Whether to wait for the peer's stream limit. It defaults to true; false fails at once with a
   * DOMException named QuotaExceededError when the limit is reached.
   */
  waitUntilAvailable?: boolean;
}

export interface QMuxBidirectionalStream {
  readonly readable: ReadableStream<Uint8Array>;
  readonly writable: WritableStream<Uint8Array | ArrayBufferView | ArrayBuffer>;
}

/** Datagrams (RFC 9221), shaped like WebTransportDatagramDuplexStream. */
export interface QMuxDatagrams {
  /**
   * Datagrams from the peer. While 128 wait to be read, new ones are dropped. Unlike over QUIC,
   * those that arrive come reliably and in order.
   */
  readonly readable: ReadableStream<Uint8Array>;
  /**
   * Datagrams to the peer. One larger than maxDatagramSize, or written while the WebSocket buffers
   * more than the send buffer limit, is dropped.
   */
  readonly writable: WritableStream<Uint8Array | ArrayBufferView | ArrayBuffer>;
  /** The largest payload the peer accepts, or 0 if it accepts no datagrams (or before `ready`). */
  readonly maxDatagramSize: number;
}

export interface QMuxCloseInfo {
  closeCode?: number;
  reason?: string;
}

/**
 * A QMux session over one WebSocket (draft-ietf-quic-qmux-02 and
 * draft-lcurley-qmux-websocket-00), with the API of a WebTransport session.
 */
export declare class QMuxSession {
  /**
   * Dials `url` (`ws:` or `wss:`) as the client, or runs over a WebSocket that is already open or
   * opening, such as one a server accepted, in `options.role`.
   */
  constructor(source: string | URL | QMuxWebSocket, options?: QMuxSessionOptions);
  /** Resolves once the peer's transport parameters have arrived. */
  readonly ready: Promise<void>;
  /**
   * Resolves with the code and reason of an application close, from `close()` on either side.
   * Rejects with a QMuxError when the session ends otherwise: a protocol error, an idle timeout,
   * or a WebSocket that closed without a CONNECTION_CLOSE.
   */
  readonly closed: Promise<Required<QMuxCloseInfo>>;
  /**
   * How far the session got: "opening" until the WebSocket opens, "handshaking" until the peer's
   * transport parameters arrive, "ready" after them, and "closed" once it ended.
   */
  readonly stage: "opening" | "handshaking" | "ready" | "closed";
  /** The negotiated WebSocket subprotocol, or "" if none. */
  readonly protocol: string;
  /** The transport parameters the peer advertised, or null before `ready`. */
  readonly peerParameters: Parameters | null;
  /** The bidirectional streams the peer opens. Cancelling it refuses those it opens later. */
  readonly incomingBidirectionalStreams: ReadableStream<QMuxBidirectionalStream>;
  /** The unidirectional streams the peer opens. Cancelling it refuses those it opens later. */
  readonly incomingUnidirectionalStreams: ReadableStream<ReadableStream<Uint8Array>>;
  /** Datagrams, if `parameters.maxDatagramFrameSize` enables them, and undefined otherwise. */
  readonly datagrams: QMuxDatagrams | undefined;
  createBidirectionalStream(options?: QMuxStreamOptions): Promise<QMuxBidirectionalStream>;
  /** Opens a unidirectional stream, which only sends. */
  createUnidirectionalStream(
    options?: QMuxStreamOptions,
  ): Promise<WritableStream<Uint8Array | ArrayBufferView | ArrayBuffer>>;
  /**
   * Closes the session with an application CONNECTION_CLOSE carrying the code and the first 256
   * bytes of the reason.
   */
  close(closeInfo?: QMuxCloseInfo): void;
}
