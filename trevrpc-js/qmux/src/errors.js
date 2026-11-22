// The errors a session reports: WebTransportError-shaped ones for applications, and the QMux
// protocol errors that close a session with a transport CONNECTION_CLOSE.

/** QUIC transport error codes (RFC 9000 §20.1). */
export const TransportError = Object.freeze({
  NoError: 0x00,
  InternalError: 0x01,
  FlowControlError: 0x03,
  StreamLimitError: 0x04,
  StreamStateError: 0x05,
  FinalSizeError: 0x06,
  FrameEncodingError: 0x07,
  TransportParameterError: 0x08,
  ProtocolViolation: 0x0a,
});

/**
 * An error shaped like WebTransportError, so that applications handle QMux and WebTransport
 * sessions alike: `source` is "session" or "stream", and `streamErrorCode` carries the code of a
 * stream reset or stop. Passing one with a code to `abort()` or `cancel()` sends that code.
 */
export class QMuxError extends Error {
  constructor(message = "", { source = "stream", streamErrorCode = null, cause } = {}) {
    super(message, cause === undefined ? undefined : { cause });
    this.name = "WebTransportError";
    this.source = source;
    this.streamErrorCode = streamErrorCode;
  }
}

/** A QMux protocol error that closes the session with a transport CONNECTION_CLOSE. */
export class QMuxProtocolError extends Error {
  constructor(code, message, frameType = 0) {
    super(message);
    this.name = "QMuxProtocolError";
    this.code = code;
    this.frameType = frameType;
  }
}

/** A session-level QMuxError. */
export function sessionError(message, cause) {
  return new QMuxError(message, { source: "session", cause });
}

/** A stream-level QMuxError carrying the code of a reset or stop. */
export function streamError(message, streamErrorCode) {
  return new QMuxError(message, { source: "stream", streamErrorCode });
}

/**
 * The application error code that a reason passed to `abort()` or `cancel()` sends, as
 * WebTransport derives it: the `streamErrorCode` of an error that carries one, otherwise 0.
 */
export function resetCode(reason) {
  const code = reason?.streamErrorCode;
  return Number.isSafeInteger(code) && code >= 0 ? code : 0;
}
