// TrevRPC over QMux over WebSocket (draft-ietf-quic-qmux-02, draft-lcurley-qmux-websocket-00):
// the browser fallback when WebTransport is unavailable or blocked. A session exposes the part of
// the WebTransport API that the channel uses, so calls run on it unchanged.
import { DefaultMaxFrameSize } from "../framing.js";
import {
  FrameParser,
  FrameType,
  FrameWriter,
  QMuxProtocolError,
  TransportError,
  streamFrameOverhead,
  writeConnectionClose,
  writeMaxData,
  writeMaxStreamData,
  writePing,
  writeResetStream,
  writeStopSending,
  writeStreamFrame,
} from "./frames.js";
import { decodeTransportParameters, writeTransportParameters } from "./params.js";

/** The WebSocket subprotocol of TrevRPC over QMux. */
export const QMuxProtocol = "qmux-02.trevrpc-1";

/** The largest record this endpoint accepts and sends. */
const MaxRecordSize = 64 * 1024;
/** Stream error code for streams abandoned locally. */
const CancelledCode = 1;
/** Writes wait while the WebSocket buffers more than this many bytes. */
const SendBufferLimit = 1024 * 1024;
const SendBufferPollMs = 5;
/** Bounds opening the WebSocket and receiving the server's transport parameters. */
const HandshakeTimeoutMs = 10_000;

/** An error shaped like WebTransportError, so callers treat both transports alike. */
function transportError(message, { source = "session", streamErrorCode = null, cause } = {}) {
  const error = new Error(message, cause === undefined ? undefined : { cause });
  error.name = "WebTransportError";
  error.source = source;
  error.streamErrorCode = streamErrorCode;
  return error;
}

function protocolError(code, message, frameType) {
  return new QMuxProtocolError(code, message, frameType);
}

/** A QMux session over one WebSocket. */
export class QMuxWebSocketSession {
  /** Resolves once the server's transport parameters have arrived. */
  ready;
  /** Resolves with `{ closeCode, reason }` after a clean close and rejects otherwise. */
  closed;

  #ws;
  #window;
  #peer = null;
  #streams = new Map();
  #openedLocal = 0;
  #maxLocal = 0;
  #connSendMax = 0;
  #connSent = 0;
  #connRecvMax;
  #connReceived = 0;
  #connConsumed = 0;
  #pending = new FrameWriter();
  #flushScheduled = false;
  #waiters = new Set();
  /** The highest QX_PING request sequence received, -1 before the first: a peer may start at 0. */
  #pingReceived = -1;
  #closedError = null;
  #lastActivity = Date.now();
  #idleTimer;
  #handshakeTimer;
  #settleReady;
  #settleClosed;

  /**
   * Opens a session to a `wss://` URL. `options.WebSocket` replaces the global WebSocket
   * constructor; `options.maxFrameSize` sizes the receive windows like the native clients.
   */
  constructor(url, options = {}) {
    const WebSocketConstructor = options.WebSocket ?? globalThis.WebSocket;
    if (typeof WebSocketConstructor !== "function") {
      throw transportError("WebSocket is not available in this JavaScript runtime");
    }
    // A frame header is at most 16 bytes, so the window fits one maximum-size frame.
    this.#window = (options.maxFrameSize ?? DefaultMaxFrameSize) + 16;
    this.#connRecvMax = this.#window;
    this.ready = new Promise((resolve, reject) => {
      this.#settleReady = { resolve, reject };
    });
    this.closed = new Promise((resolve, reject) => {
      this.#settleClosed = { resolve, reject };
    });
    this.ready.catch(() => {});
    this.closed.catch(() => {});

    const ws = new WebSocketConstructor(url, [QMuxProtocol]);
    ws.binaryType = "arraybuffer";
    ws.onopen = () => this.#opened();
    ws.onmessage = (event) => this.#received(event.data);
    ws.onerror = () => this.#finish(transportError("WebSocket error"), false);
    ws.onclose = (event) =>
      this.#finish(transportError(`WebSocket closed with code ${event.code}`), false);
    this.#ws = ws;
    this.#handshakeTimer = setTimeout(
      () => this.#finish(transportError("QMux handshake timed out"), false),
      options.handshakeTimeoutMs ?? HandshakeTimeoutMs,
    );
  }

  /** Opens a bidirectional stream, waiting for the server's stream limit. */
  async createBidirectionalStream() {
    await this.ready;
    for (;;) {
      if (this.#closedError) {
        throw this.#closedError;
      }
      if (this.#openedLocal < this.#maxLocal) {
        break;
      }
      await this.#wait();
    }
    const id = this.#openedLocal * 4;
    this.#openedLocal += 1;
    const stream = new QMuxStream(
      this,
      id,
      this.#peer.initialMaxStreamDataBidiRemote,
      this.#window,
    );
    this.#streams.set(id, stream);
    return { readable: stream.readable, writable: stream.writable };
  }

  /** Closes the session with an application CONNECTION_CLOSE. */
  close(closeInfo = {}) {
    if (this.#closedError) {
      return;
    }
    const code = closeInfo.closeCode ?? 0;
    const reason = closeInfo.reason ?? "";
    writeConnectionClose(this.#pending, { code, reason });
    this.#flush();
    this.#finish(transportError("session closed locally"), true, { closeCode: code, reason });
  }

  #opened() {
    if (this.#ws.protocol !== QMuxProtocol) {
      this.#finish(
        transportError(`server selected WebSocket subprotocol "${this.#ws.protocol}"`),
        false,
      );
      return;
    }
    // The transport parameters must be the first frame on the wire.
    writeTransportParameters(this.#pending, {
      initialMaxData: this.#window,
      initialMaxStreamDataBidiLocal: this.#window,
      maxRecordSize: MaxRecordSize,
    });
    this.#flush();
  }

  #received(data) {
    if (this.#closedError) {
      return;
    }
    this.#lastActivity = Date.now();
    try {
      if (typeof data === "string") {
        throw protocolError(TransportError.ProtocolViolation, "text message", 0);
      }
      const record = new Uint8Array(data);
      if (record.length > MaxRecordSize) {
        throw protocolError(TransportError.FrameEncodingError, "record exceeds max_record_size", 0);
      }
      const parser = new FrameParser(record);
      for (let frame = parser.next(); frame !== null && !this.#closedError; frame = parser.next()) {
        if (this.#peer === null && frame.type !== FrameType.TransportParameters) {
          throw protocolError(
            TransportError.ProtocolViolation,
            "first frame is not QX_TRANSPORT_PARAMETERS",
            frame.type,
          );
        }
        this.#handle(frame);
      }
    } catch (error) {
      if (!(error instanceof QMuxProtocolError)) {
        throw error;
      }
      writeConnectionClose(this.#pending, { protocolError: error });
      this.#flush();
      this.#finish(
        transportError(`QMux protocol error: ${error.message}`, { cause: error }),
        false,
      );
    }
  }

  #handle(frame) {
    switch (frame.type) {
      case FrameType.TransportParameters:
        this.#parameters(frame);
        return;
      case FrameType.Padding:
      case FrameType.DataBlocked:
        return;
      case FrameType.ResetStream:
        this.#stream(frame)?.onReset(frame.code, frame.finalSize);
        return;
      case FrameType.StopSending:
        this.#stream(frame)?.onStopSending(frame.code);
        return;
      case FrameType.MaxData:
        if (frame.maximum > this.#connSendMax) {
          this.#connSendMax = frame.maximum;
          this.#wake();
        }
        return;
      case FrameType.MaxStreamData: {
        const stream = this.#stream(frame);
        if (stream && frame.maximum > stream.sendMax) {
          stream.sendMax = frame.maximum;
          this.#wake();
        }
        return;
      }
      case FrameType.MaxStreamsBidi:
      case FrameType.MaxStreamsUni:
      case FrameType.StreamsBlockedBidi:
      case FrameType.StreamsBlockedUni:
        if (frame.maximum > 2 ** 60) {
          throw protocolError(
            TransportError.FrameEncodingError,
            "stream count exceeds 2^60",
            frame.type,
          );
        }
        if (frame.type === FrameType.MaxStreamsBidi && frame.maximum > this.#maxLocal) {
          this.#maxLocal = frame.maximum;
          this.#wake();
        }
        return;
      case FrameType.StreamDataBlocked:
        this.#stream(frame);
        return;
      case FrameType.ConnectionClose:
      case FrameType.ApplicationClose: {
        const clean = frame.type === FrameType.ApplicationClose && frame.code === 0;
        const message = `session closed by peer with code ${frame.code}${frame.reason ? `: ${frame.reason}` : ""}`;
        this.#finish(transportError(message), clean, {
          closeCode: frame.code,
          reason: frame.reason,
        });
        return;
      }
      case FrameType.Ping:
        if (frame.sequence <= this.#pingReceived) {
          throw protocolError(
            TransportError.ProtocolViolation,
            "QX_PING sequence is not increasing",
            frame.type,
          );
        }
        this.#pingReceived = frame.sequence;
        writePing(this.#pending, FrameType.PingResponse, frame.sequence);
        this.#scheduleFlush();
        return;
      case FrameType.PingResponse:
        throw protocolError(
          TransportError.ProtocolViolation,
          "QX_PING response was never requested",
          frame.type,
        );
      default:
        if (frame.data !== undefined) {
          this.#stream(frame)?.onData(frame.offset, frame.data, frame.fin);
        }
    }
  }

  #parameters(frame) {
    if (this.#peer !== null) {
      throw protocolError(
        TransportError.ProtocolViolation,
        "repeated QX_TRANSPORT_PARAMETERS",
        frame.type,
      );
    }
    this.#peer = decodeTransportParameters(frame.params);
    this.#maxLocal = this.#peer.initialMaxStreamsBidi;
    this.#connSendMax = this.#peer.initialMaxData;
    const idleMs = this.#peer.maxIdleTimeoutMs;
    if (idleMs > 0) {
      this.#idleTimer = setInterval(
        () => {
          if (Date.now() - this.#lastActivity >= idleMs) {
            this.#finish(transportError("QMux session idle timeout"), false);
          }
        },
        Math.max(10, idleMs / 4),
      );
    }
    clearTimeout(this.#handshakeTimer);
    this.#settleReady.resolve();
  }

  /** The stream a frame refers to, or undefined once it has completed. */
  #stream(frame) {
    const id = frame.streamId;
    if (id % 4 !== 0) {
      // The server may open no streams of either kind.
      throw protocolError(
        TransportError.StreamLimitError,
        `stream ${id} exceeds the stream limit`,
        frame.type,
      );
    }
    if (id / 4 >= this.#openedLocal) {
      throw protocolError(
        TransportError.StreamStateError,
        `stream ${id} was never opened`,
        frame.type,
      );
    }
    return this.#streams.get(id);
  }

  // Called by streams.

  /** Accounts for stream data received from the peer. */
  received(length, frameType) {
    this.#connReceived += length;
    if (this.#connReceived > this.#connRecvMax) {
      throw protocolError(
        TransportError.FlowControlError,
        "connection flow control limit exceeded",
        frameType,
      );
    }
  }

  /** Returns connection credit for received data that was read or discarded. */
  consumed(length) {
    this.#connConsumed += length;
    if (this.#connRecvMax - this.#connConsumed <= this.#window / 2) {
      this.#connRecvMax = this.#connConsumed + this.#window;
      writeMaxData(this.#pending, this.#connRecvMax);
      this.#scheduleFlush();
    }
  }

  maxStreamData(stream) {
    writeMaxStreamData(this.#pending, stream.id, stream.recvMax);
    this.#scheduleFlush();
  }

  stopSending(stream, code) {
    writeStopSending(this.#pending, stream.id, code);
    this.#scheduleFlush();
  }

  resetStream(stream, code) {
    writeResetStream(this.#pending, stream.id, code, stream.sendOffset);
    this.#scheduleFlush();
  }

  /** Bytes `stream` may send now, waiting for flow control credit and socket buffer space. */
  async sendCredit(stream) {
    for (;;) {
      if (stream.writeError) {
        throw stream.writeError;
      }
      if (this.#closedError) {
        throw this.#closedError;
      }
      const credit = Math.min(
        stream.sendMax - stream.sendOffset,
        this.#connSendMax - this.#connSent,
      );
      if (credit > 0 && this.#ws.bufferedAmount <= SendBufferLimit) {
        return Math.min(credit, this.#recordLimit() - 32);
      }
      if (credit > 0) {
        await new Promise((resolve) => setTimeout(resolve, SendBufferPollMs));
      } else {
        await this.#wait();
      }
    }
  }

  sendStream(stream, data, fin) {
    const overhead = streamFrameOverhead(stream.id, stream.sendOffset, data.length);
    if (this.#pending.length + overhead + data.length > this.#recordLimit()) {
      this.#flush();
    }
    writeStreamFrame(this.#pending, stream.id, stream.sendOffset, data, fin);
    stream.sendOffset += data.length;
    this.#connSent += data.length;
    this.#scheduleFlush();
  }

  completed(stream) {
    this.#streams.delete(stream.id);
  }

  /** Wakes writers waiting for credit so they notice that their stream failed. */
  wakeSenders() {
    this.#wake();
  }

  get closedError() {
    return this.#closedError;
  }

  #recordLimit() {
    return Math.min(this.#peer?.maxRecordSize ?? MaxRecordSize, MaxRecordSize);
  }

  /** Waits for a change in credit or session state. */
  #wait() {
    return new Promise((resolve) => this.#waiters.add(resolve));
  }

  #wake() {
    const waiters = [...this.#waiters];
    this.#waiters.clear();
    for (const resolve of waiters) {
      resolve();
    }
  }

  #scheduleFlush() {
    if (!this.#flushScheduled) {
      this.#flushScheduled = true;
      queueMicrotask(() => {
        this.#flushScheduled = false;
        this.#flush();
      });
    }
  }

  /** Sends the pending frames as one record. */
  #flush() {
    if (this.#pending.length === 0) {
      return;
    }
    const record = this.#pending.take();
    if (this.#ws.readyState === 1) {
      this.#ws.send(record);
      this.#lastActivity = Date.now();
    }
  }

  #finish(error, clean, closeInfo) {
    if (this.#closedError) {
      return;
    }
    this.#closedError = error;
    clearInterval(this.#idleTimer);
    clearTimeout(this.#handshakeTimer);
    for (const stream of this.#streams.values()) {
      stream.onSessionClosed(error);
    }
    this.#streams.clear();
    this.#settleReady.reject(error);
    if (clean) {
      this.#settleClosed.resolve(closeInfo);
    } else {
      this.#settleClosed.reject(error);
    }
    this.#wake();
    try {
      this.#ws.close(1000);
    } catch {
      // Already closing.
    }
  }
}

/** One bidirectional stream: WHATWG streams over STREAM, RESET_STREAM and STOP_SENDING. */
class QMuxStream {
  readable;
  writable;
  sendOffset = 0;
  sendMax;
  recvMax;
  writeError = null;

  #session;
  #window;
  #received = 0;
  #read = 0;
  #chunks = [];
  #finalSize = null;
  #finReceived = false;
  #readError = null;
  #readCancelled = false;
  #sendDone = false;
  #pullWaiter = null;
  #writeController;

  constructor(session, id, sendMax, window) {
    this.id = id;
    this.#session = session;
    this.sendMax = sendMax;
    this.recvMax = window;
    this.#window = window;
    this.readable = new ReadableStream(
      { pull: (controller) => this.#pull(controller), cancel: () => this.#cancelRead() },
      { highWaterMark: 0 },
    );
    this.writable = new WritableStream({
      start: (controller) => {
        this.#writeController = controller;
        // An abort waits for the write in flight, which may be waiting for credit that never
        // comes; the controller's signal fires at once, so the stream is reset and the write fails.
        controller.signal.addEventListener(
          "abort",
          () => this.#abortSend(controller.signal.reason),
          {
            once: true,
          },
        );
      },
      write: (chunk) => this.#write(chunk),
      close: () => this.#finishSend(),
      abort: () => this.#resetSend(CancelledCode),
    });
  }

  onData(offset, data, fin) {
    const end = offset + data.length;
    if (this.#finalSize !== null && (end > this.#finalSize || (fin && end !== this.#finalSize))) {
      throw protocolError(
        TransportError.FinalSizeError,
        `stream ${this.id} data beyond its final size`,
        FrameType.Stream,
      );
    }
    if (offset !== this.#received) {
      throw protocolError(
        TransportError.ProtocolViolation,
        `stream ${this.id} data out of order`,
        FrameType.Stream,
      );
    }
    if (end > this.recvMax) {
      throw protocolError(
        TransportError.FlowControlError,
        `stream ${this.id} exceeds its flow control limit`,
        FrameType.Stream,
      );
    }
    this.#session.received(data.length, FrameType.Stream);
    this.#received = end;
    if (fin) {
      this.#finalSize = end;
      this.#finReceived = true;
    }
    if (this.#readCancelled || this.#readError) {
      this.#read = end;
      this.#session.consumed(data.length);
    } else if (data.length > 0) {
      this.#chunks.push(data);
    }
    this.#wakePull();
    this.#maybeComplete();
  }

  onReset(code, finalSize) {
    if (this.#finReceived && finalSize === this.#finalSize) {
      return;
    }
    if ((this.#finalSize !== null && finalSize !== this.#finalSize) || finalSize < this.#received) {
      throw protocolError(
        TransportError.FinalSizeError,
        `stream ${this.id} reset with an inconsistent final size`,
        FrameType.ResetStream,
      );
    }
    if (finalSize > this.recvMax) {
      throw protocolError(
        TransportError.FlowControlError,
        `stream ${this.id} final size exceeds its flow control limit`,
        FrameType.ResetStream,
      );
    }
    this.#session.received(finalSize - this.#received, FrameType.ResetStream);
    this.#received = finalSize;
    this.#finalSize = finalSize;
    if (!this.#readCancelled && !this.#readError) {
      this.#readError = transportError(`stream reset by peer with code ${code}`, {
        source: "stream",
        streamErrorCode: code,
      });
    }
    this.#session.consumed(finalSize - this.#read);
    this.#read = finalSize;
    this.#chunks = [];
    this.#wakePull();
    this.#maybeComplete();
  }

  onStopSending(code) {
    if (this.#sendDone) {
      return;
    }
    this.writeError = transportError(`stream stopped by peer with code ${code}`, {
      source: "stream",
      streamErrorCode: code,
    });
    this.#resetSend(code);
    this.#writeController.error(this.writeError);
  }

  /** Resets the send side because the writer was aborted, failing a write waiting for credit. */
  #abortSend(reason) {
    if (this.#sendDone) {
      return;
    }
    this.writeError ??= transportError(`stream aborted: ${reason?.message ?? reason}`, {
      source: "stream",
      streamErrorCode: CancelledCode,
    });
    this.#resetSend(CancelledCode);
    this.#session.wakeSenders();
  }

  onSessionClosed(error) {
    this.#readError ??= error;
    this.writeError ??= error;
    this.#sendDone = true;
    this.#wakePull();
    try {
      this.#writeController.error(error);
    } catch {
      // The writable already ended.
    }
  }

  async #pull(controller) {
    for (;;) {
      if (this.#readError) {
        controller.error(this.#readError);
        return;
      }
      const chunk = this.#chunks.shift();
      if (chunk !== undefined) {
        controller.enqueue(chunk);
        this.#consumed(chunk.length);
        return;
      }
      if (this.#finReceived && this.#read === this.#finalSize) {
        controller.close();
        return;
      }
      await new Promise((resolve) => {
        this.#pullWaiter = resolve;
      });
    }
  }

  #wakePull() {
    const resolve = this.#pullWaiter;
    this.#pullWaiter = null;
    resolve?.();
  }

  #consumed(length) {
    this.#read += length;
    if (this.#finalSize === null && this.recvMax - this.#read <= this.#window / 2) {
      this.recvMax = this.#read + this.#window;
      this.#session.maxStreamData(this);
    }
    this.#session.consumed(length);
    this.#maybeComplete();
  }

  #cancelRead() {
    if (
      this.#readCancelled ||
      this.#readError ||
      (this.#finReceived && this.#read === this.#finalSize)
    ) {
      return;
    }
    this.#readCancelled = true;
    const unread = this.#received - this.#read;
    this.#chunks = [];
    this.#read = this.#received;
    this.#session.consumed(unread);
    if (this.#finalSize === null && !this.#session.closedError) {
      this.#session.stopSending(this, CancelledCode);
    }
    this.#maybeComplete();
  }

  async #write(chunk) {
    let data = chunk instanceof Uint8Array ? chunk : new Uint8Array(chunk);
    while (data.length > 0) {
      const allowed = await this.#session.sendCredit(this);
      const piece = data.subarray(0, allowed);
      this.#session.sendStream(this, piece, false);
      data = data.subarray(piece.length);
    }
  }

  #finishSend() {
    if (this.#sendDone) {
      return;
    }
    if (this.#session.closedError) {
      throw this.#session.closedError;
    }
    this.#sendDone = true;
    this.#session.sendStream(this, new Uint8Array(0), true);
    this.#maybeComplete();
  }

  #resetSend(code) {
    if (this.#sendDone) {
      return;
    }
    this.#sendDone = true;
    if (!this.#session.closedError) {
      this.#session.resetStream(this, code);
    }
    this.#maybeComplete();
  }

  #maybeComplete() {
    if (this.#sendDone && this.#finalSize !== null && this.#read === this.#finalSize) {
      this.#session.completed(this);
    }
  }
}
