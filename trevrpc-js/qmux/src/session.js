// A QMux session over one WebSocket (draft-ietf-quic-qmux-02, draft-lcurley-qmux-websocket-00),
// shaped like WebTransport: streams are WHATWG streams, and errors look like WebTransportError.
import {
  QMuxProtocolError,
  TransportError,
  resetCode,
  sessionError,
  streamError,
} from "./errors.js";
import {
  FrameParser,
  FrameType,
  FrameWriter,
  datagramFrameSize,
  streamFrameOverhead,
  writeConnectionClose,
  writeDatagram,
  writeMaxData,
  writeMaxStreamData,
  writeMaxStreams,
  writePing,
  writeResetStream,
  writeStopSending,
  writeStreamFrame,
} from "./frames.js";
import {
  DefaultMaxRecordSize,
  decodeTransportParameters,
  writeTransportParameters,
} from "./params.js";
import { varintLength } from "./varint.js";

/** The transport parameters a session advertises unless `options.parameters` overrides them. */
export const DefaultParameters = Object.freeze({
  maxIdleTimeoutMs: 0,
  initialMaxData: 1024 * 1024,
  initialMaxStreamDataBidiLocal: 256 * 1024,
  initialMaxStreamDataBidiRemote: 256 * 1024,
  initialMaxStreamDataUni: 256 * 1024,
  initialMaxStreamsBidi: 100,
  initialMaxStreamsUni: 100,
  maxDatagramFrameSize: 0,
  maxRecordSize: 64 * 1024,
});

const DefaultSendBufferLimit = 1024 * 1024;
const SendBufferPollMs = 5;
const DefaultHandshakeTimeoutMs = 10_000;
const DefaultMaxSendRecordSize = 64 * 1024;
/** Received datagrams queued for the application; past it, new ones are dropped. */
const DatagramQueueLength = 128;
/** Room a STREAM or DATAGRAM frame header takes at most in a record. */
const FrameHeaderRoom = 32;

const Bidi = 0;
const Uni = 1;
/** What a frame needs of the stream it names: its send side, its receive side, or either. */
const Send = 0;
const Recv = 1;

/**
 * The send side of a stream (RFC 9000 section 3.1): `Send` until the FIN or RESET_STREAM is queued
 * (`DataSent`, `ResetSent`) or the session ends (`SessionClosed`). Only `Send` accepts writes. A
 * receive-only stream starts in `DataSent`.
 */
const SendState = Object.freeze({ Send: 0, DataSent: 1, ResetSent: 2, SessionClosed: 3 });

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

/** The bytes of a chunk written to a stream or received from the WebSocket. */
function toBytes(chunk) {
  if (chunk instanceof Uint8Array) {
    return chunk;
  }
  if (ArrayBuffer.isView(chunk)) {
    return new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength);
  }
  return new Uint8Array(chunk);
}

/** A QMux session over one WebSocket, as a client that dials a URL or over an accepted socket. */
export class QMuxSession {
  /** Resolves once the peer's transport parameters have arrived. */
  ready;
  /**
   * Resolves with `{ closeCode, reason }` after an application close by either side, and rejects
   * otherwise.
   */
  closed;
  /** The bidirectional streams the peer opens, as `{ readable, writable }`. */
  incomingBidirectionalStreams;
  /** The unidirectional streams the peer opens, as readable streams. */
  incomingUnidirectionalStreams;
  /** Datagrams, if `parameters.maxDatagramFrameSize` enables them; undefined otherwise. */
  datagrams;

  #ws;
  #server;
  /** The subprotocols a client offered, which the server's selection must be one of. */
  #protocols;
  #params;
  #peer = null;
  #streams = new Map();
  /** Per direction (Bidi, Uni): streams this endpoint opened, and how many the peer allows. */
  #localOpened = [0, 0];
  #localMax = [0, 0];
  /** Per direction: streams the peer opened, the limit announced to it, and its next value. */
  #remoteOpened = [0, 0];
  #remoteLimit;
  #remoteCredit;
  #incoming;
  #datagrams;
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
  /** The sequence of the next QX_PING request this endpoint sends. */
  #pingNext = 0;
  #closedError = null;
  /** Whether the WebSocket opened (with an accepted subprotocol). */
  #open = false;
  #lastActivity = Date.now();
  #lastSent = Date.now();
  #idleTimer;
  #keepAliveTimer;
  #handshakeTimer;
  #settleReady;
  #settleClosed;
  #sendBufferLimit;
  #maxSendRecordSize;
  #keepAlivePeriodMs;
  /** What the streams and datagrams call on the session. */
  #link;

  /**
   * Opens a session. With a `ws:` or `wss:` URL it dials as the client, offering
   * `options.protocols` as the WebSocket subprotocols; `options.WebSocket` replaces the global
   * WebSocket constructor. With a WebSocket that is already open or opening, such as one a server
   * accepted, it runs over that socket in `options.role`, "client" by default.
   */
  constructor(source, options = {}) {
    const role = options.role ?? "client";
    if (role !== "client" && role !== "server") {
      throw new TypeError(`unknown role "${role}"`);
    }
    this.#server = role === "server";
    const params = { ...DefaultParameters, ...options.parameters };
    params.maxRecordSize ||= DefaultMaxRecordSize;
    if (params.maxRecordSize < DefaultMaxRecordSize) {
      throw new RangeError(`maxRecordSize must be at least ${DefaultMaxRecordSize}`);
    }
    this.#params = params;
    this.#remoteLimit = [params.initialMaxStreamsBidi, params.initialMaxStreamsUni];
    this.#remoteCredit = [...this.#remoteLimit];
    this.#connRecvMax = params.initialMaxData;
    this.#sendBufferLimit = options.sendBufferLimit ?? DefaultSendBufferLimit;
    this.#maxSendRecordSize = Math.max(
      DefaultMaxRecordSize,
      options.maxSendRecordSize ?? DefaultMaxSendRecordSize,
    );
    this.#keepAlivePeriodMs = options.keepAlivePeriodMs ?? 0;
    this.#link = this.#makeLink();

    let ws;
    if (typeof source === "string" || source instanceof URL) {
      const WebSocketConstructor = options.WebSocket ?? globalThis.WebSocket;
      if (typeof WebSocketConstructor !== "function") {
        throw sessionError("WebSocket is not available in this JavaScript runtime");
      }
      this.#protocols = [...(options.protocols ?? [])];
      ws = new WebSocketConstructor(String(source), this.#protocols);
    } else {
      ws = source;
      this.#protocols = [];
    }

    this.#settleReady = deferred();
    this.#settleClosed = deferred();
    this.ready = this.#settleReady.promise;
    this.closed = this.#settleClosed.promise;
    this.ready.catch(() => {});
    this.closed.catch(() => {});
    this.#incoming = [new IncomingStreams(), new IncomingStreams()];
    this.incomingBidirectionalStreams = this.#incoming[Bidi].readable;
    this.incomingUnidirectionalStreams = this.#incoming[Uni].readable;
    if (params.maxDatagramFrameSize > 0) {
      this.#datagrams = new QMuxDatagrams(this.#link);
      this.datagrams = this.#datagrams.api;
    }

    ws.binaryType = "arraybuffer";
    ws.onopen = () => this.#opened();
    ws.onmessage = (event) => this.#received(event.data);
    ws.onerror = () => this.#finish(sessionError("WebSocket error"), false);
    ws.onclose = (event) =>
      this.#finish(sessionError(`WebSocket closed with code ${event?.code ?? 1006}`), false);
    this.#ws = ws;
    this.#handshakeTimer = setTimeout(
      () => this.#finish(sessionError("QMux handshake timed out"), false),
      options.handshakeTimeoutMs ?? DefaultHandshakeTimeoutMs,
    );
    if (ws.readyState === 1) {
      queueMicrotask(() => this.#opened());
    } else if (ws.readyState > 1) {
      queueMicrotask(() => this.#finish(sessionError("WebSocket is already closed"), false));
    }
  }

  /**
   * How far the session got: "opening" until the WebSocket opens, "handshaking" until the peer's
   * transport parameters arrive, "ready" after them, and "closed" once the session ended.
   */
  get stage() {
    if (this.#closedError) {
      return "closed";
    }
    if (this.#peer !== null) {
      return "ready";
    }
    return this.#open ? "handshaking" : "opening";
  }

  /** The WebSocket subprotocol in use, or "" if none was negotiated. */
  get protocol() {
    return this.#ws.protocol ?? "";
  }

  /** The transport parameters the peer advertised, or null before they arrive. */
  get peerParameters() {
    return this.#peer === null ? null : { ...this.#peer };
  }

  /**
   * Opens a bidirectional stream. It waits for the peer's stream limit unless
   * `options.waitUntilAvailable` is false, which fails at once with a QuotaExceededError.
   */
  async createBidirectionalStream(options) {
    const stream = await this.#openLocal(Bidi, options);
    return { readable: stream.readable, writable: stream.writable };
  }

  /** Opens a unidirectional stream, which only sends; it waits as createBidirectionalStream. */
  async createUnidirectionalStream(options) {
    const stream = await this.#openLocal(Uni, options);
    return stream.writable;
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
    this.#finish(sessionError("session closed locally"), true, { closeCode: code, reason });
  }

  #makeLink() {
    return {
      received: (length, frameType) => this.#accountReceived(length, frameType),
      consumed: (length) => this.#consumed(length),
      maxStreamData: (stream) => {
        writeMaxStreamData(this.#pending, stream.id, stream.recvMax);
        this.#scheduleFlush();
      },
      stopSending: (stream, code) => {
        writeStopSending(this.#pending, stream.id, code);
        this.#scheduleFlush();
      },
      resetStream: (stream, code) => {
        writeResetStream(this.#pending, stream.id, code, stream.sendOffset);
        this.#scheduleFlush();
      },
      sendCredit: (stream) => this.#sendCredit(stream),
      sendStream: (stream, data, fin) => this.#sendStream(stream, data, fin),
      completed: (stream) => this.#completed(stream),
      wakeSenders: () => this.#wake(),
      closedError: () => this.#closedError,
      ready: () => this.ready,
      maxDatagramSize: () => this.#maxDatagramSize(),
      sendDatagram: (data) => this.#sendDatagram(data),
    };
  }

  async #openLocal(dir, { waitUntilAvailable = true } = {}) {
    await this.ready;
    for (;;) {
      if (this.#closedError) {
        throw this.#closedError;
      }
      if (this.#localOpened[dir] < this.#localMax[dir]) {
        break;
      }
      if (!waitUntilAvailable) {
        throw new DOMException("the peer's stream limit is reached", "QuotaExceededError");
      }
      await this.#wait();
    }
    const id = this.#localOpened[dir] * 4 + (this.#server ? 1 : 0) + (dir === Uni ? 2 : 0);
    this.#localOpened[dir] += 1;
    const stream = new QMuxStream(this.#link, id, {
      sendMax:
        dir === Bidi
          ? this.#peer.initialMaxStreamDataBidiRemote
          : this.#peer.initialMaxStreamDataUni,
      recvWindow: dir === Bidi ? this.#params.initialMaxStreamDataBidiLocal : null,
      remote: false,
    });
    this.#streams.set(id, stream);
    return stream;
  }

  /** Accepts the stream the peer opened next in direction `dir`. */
  #openRemote(dir) {
    const id = this.#remoteOpened[dir] * 4 + (this.#server ? 0 : 1) + (dir === Uni ? 2 : 0);
    this.#remoteOpened[dir] += 1;
    const stream = new QMuxStream(this.#link, id, {
      sendMax: dir === Bidi ? this.#peer.initialMaxStreamDataBidiLocal : null,
      recvWindow:
        dir === Bidi
          ? this.#params.initialMaxStreamDataBidiRemote
          : this.#params.initialMaxStreamDataUni,
      remote: true,
    });
    this.#streams.set(id, stream);
    if (this.#incoming[dir].refused) {
      stream.refuse();
    } else {
      this.#incoming[dir].push(
        dir === Bidi ? { readable: stream.readable, writable: stream.writable } : stream.readable,
      );
    }
  }

  #opened() {
    if (this.#open || this.#closedError) {
      return;
    }
    const { protocol } = this.#ws;
    if (this.#protocols.length > 0 && !this.#protocols.includes(protocol)) {
      this.#finish(sessionError(`server selected WebSocket subprotocol "${protocol}"`), false);
      return;
    }
    this.#open = true;
    // The transport parameters must be the first frame on the wire.
    writeTransportParameters(this.#pending, this.#params);
    this.#flush();
  }

  #received(data) {
    if (this.#closedError) {
      return;
    }
    this.#lastActivity = Date.now();
    try {
      if (typeof data === "string") {
        throw new QMuxProtocolError(TransportError.ProtocolViolation, "text message", 0);
      }
      const record = toBytes(data);
      if (record.length > this.#params.maxRecordSize) {
        throw new QMuxProtocolError(
          TransportError.FrameEncodingError,
          "record exceeds max_record_size",
          0,
        );
      }
      const parser = new FrameParser(record, {
        maxDatagramFrameSize: this.#params.maxDatagramFrameSize,
      });
      for (let frame = parser.next(); frame !== null && !this.#closedError; frame = parser.next()) {
        if (this.#peer === null && frame.type !== FrameType.TransportParameters) {
          throw new QMuxProtocolError(
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
      this.#finish(sessionError(`QMux protocol error: ${error.message}`, error), false);
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
        this.#stream(frame, Recv)?.onReset(frame.code, frame.finalSize);
        return;
      case FrameType.StopSending:
        this.#stream(frame, Send)?.onStopSending(frame.code);
        return;
      case FrameType.MaxData:
        if (frame.maximum > this.#connSendMax) {
          this.#connSendMax = frame.maximum;
          this.#wake();
        }
        return;
      case FrameType.MaxStreamData: {
        const stream = this.#stream(frame, Send);
        if (stream && frame.maximum > stream.sendMax) {
          stream.sendMax = frame.maximum;
          this.#wake();
        }
        return;
      }
      case FrameType.MaxStreamsBidi:
      case FrameType.MaxStreamsUni:
      case FrameType.StreamsBlockedBidi:
      case FrameType.StreamsBlockedUni: {
        if (frame.maximum > 2 ** 60) {
          throw new QMuxProtocolError(
            TransportError.FrameEncodingError,
            "stream count exceeds 2^60",
            frame.type,
          );
        }
        const dir = frame.type === FrameType.MaxStreamsBidi ? Bidi : Uni;
        if (
          (frame.type === FrameType.MaxStreamsBidi || frame.type === FrameType.MaxStreamsUni) &&
          frame.maximum > this.#localMax[dir]
        ) {
          this.#localMax[dir] = frame.maximum;
          this.#wake();
        }
        return;
      }
      case FrameType.StreamDataBlocked:
        this.#stream(frame, Recv);
        return;
      case FrameType.ConnectionClose:
      case FrameType.ApplicationClose: {
        // As over WebTransport, an application close is a clean end whatever its code; a
        // transport close reports a protocol error the peer detected.
        const clean = frame.type === FrameType.ApplicationClose;
        const message = `session closed by peer with code ${frame.code}${frame.reason ? `: ${frame.reason}` : ""}`;
        this.#finish(sessionError(message), clean, {
          closeCode: frame.code,
          reason: frame.reason,
        });
        return;
      }
      case FrameType.Ping:
        if (frame.sequence <= this.#pingReceived) {
          throw new QMuxProtocolError(
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
        if (frame.sequence >= this.#pingNext) {
          throw new QMuxProtocolError(
            TransportError.ProtocolViolation,
            "QX_PING response was never requested",
            frame.type,
          );
        }
        return;
      case FrameType.Datagram:
      case FrameType.DatagramLen:
        // The parser admits DATAGRAM frames only when this endpoint enabled datagrams.
        this.#datagrams.receive(frame.datagram);
        return;
      default:
        if (frame.data !== undefined) {
          const stream = this.#stream(frame, Recv);
          if (stream) {
            stream.onData(frame.offset, frame.data, frame.fin);
          } else if (frame.data.length > 0) {
            // A released stream received its final size and every byte up to it, so more data
            // lies beyond it. It would also skip the flow control checks, letting a peer send
            // without limit. An empty frame is ignored: it carries nothing to account for.
            throw new QMuxProtocolError(
              TransportError.FinalSizeError,
              `stream ${frame.streamId} data beyond its final size`,
              frame.type,
            );
          }
        }
    }
  }

  #parameters(frame) {
    if (this.#peer !== null) {
      throw new QMuxProtocolError(
        TransportError.ProtocolViolation,
        "repeated QX_TRANSPORT_PARAMETERS",
        frame.type,
      );
    }
    this.#peer = decodeTransportParameters(frame.params);
    this.#localMax = [this.#peer.initialMaxStreamsBidi, this.#peer.initialMaxStreamsUni];
    this.#connSendMax = this.#peer.initialMaxData;
    // The idle timeout is the smaller of the two endpoints' (RFC 9000 section 10.1).
    const idleMs = Math.min(
      this.#params.maxIdleTimeoutMs || Infinity,
      this.#peer.maxIdleTimeoutMs || Infinity,
    );
    if (idleMs !== Infinity) {
      this.#idleTimer = setInterval(
        () => {
          if (Date.now() - this.#lastActivity >= idleMs) {
            this.#finish(sessionError("QMux session idle timeout"), false);
          }
        },
        Math.max(10, idleMs / 4),
      );
    }
    // Keep-alives come at least twice per idle timeout, as in the Go and Erlang packages.
    const keepAliveMs = Math.min(this.#keepAlivePeriodMs, idleMs / 2);
    if (keepAliveMs > 0) {
      this.#keepAliveTimer = setInterval(
        () => {
          if (Date.now() - this.#lastSent >= keepAliveMs) {
            writePing(this.#pending, FrameType.Ping, this.#pingNext);
            this.#pingNext += 1;
            this.#flush();
          }
        },
        Math.max(10, keepAliveMs / 4),
      );
    }
    clearTimeout(this.#handshakeTimer);
    this.#settleReady.resolve();
  }

  /**
   * The stream a frame names, or undefined once it was released. `need` says whether the frame
   * acts on its send side (Send) or its receive side (Recv). A stream the peer opens is accepted
   * here, with every lower-numbered stream of its kind (RFC 9000 section 3.2).
   */
  #stream(frame, need) {
    const id = frame.streamId;
    const type = id % 4;
    const dir = type < 2 ? Bidi : Uni;
    const local = (type % 2 === 1) === this.#server;
    if (dir === Uni && local === (need === Recv)) {
      throw new QMuxProtocolError(
        TransportError.StreamStateError,
        `stream ${id} is ${local ? "send" : "receive"}-only`,
        frame.type,
      );
    }
    const index = Math.floor(id / 4);
    if (local) {
      if (index >= this.#localOpened[dir]) {
        throw new QMuxProtocolError(
          TransportError.StreamStateError,
          `stream ${id} was never opened`,
          frame.type,
        );
      }
      return this.#streams.get(id);
    }
    if (index >= this.#remoteLimit[dir]) {
      throw new QMuxProtocolError(
        TransportError.StreamLimitError,
        `stream ${id} exceeds the stream limit`,
        frame.type,
      );
    }
    while (this.#remoteOpened[dir] <= index) {
      this.#openRemote(dir);
    }
    return this.#streams.get(id);
  }

  /** Accounts for stream data received from the peer. */
  #accountReceived(length, frameType) {
    this.#connReceived += length;
    if (this.#connReceived > this.#connRecvMax) {
      throw new QMuxProtocolError(
        TransportError.FlowControlError,
        "connection flow control limit exceeded",
        frameType,
      );
    }
  }

  /** Returns connection credit for received data that was read or discarded. */
  #consumed(length) {
    this.#connConsumed += length;
    const window = this.#params.initialMaxData;
    if (window > 0 && this.#connRecvMax - this.#connConsumed <= window / 2) {
      this.#connRecvMax = this.#connConsumed + window;
      writeMaxData(this.#pending, this.#connRecvMax);
      this.#scheduleFlush();
    }
  }

  /** Bytes `stream` may send now, waiting for flow control credit and socket buffer space. */
  async #sendCredit(stream) {
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
      if (credit > 0 && this.#ws.bufferedAmount <= this.#sendBufferLimit) {
        return Math.min(credit, this.#recordLimit() - FrameHeaderRoom);
      }
      if (credit > 0) {
        await new Promise((resolve) => setTimeout(resolve, SendBufferPollMs));
      } else {
        await this.#wait();
      }
    }
  }

  #sendStream(stream, data, fin) {
    const overhead = streamFrameOverhead(stream.id, stream.sendOffset, data.length);
    if (this.#pending.length + overhead + data.length > this.#recordLimit()) {
      this.#flush();
    }
    writeStreamFrame(this.#pending, stream.id, stream.sendOffset, data, fin);
    stream.sendOffset += data.length;
    this.#connSent += data.length;
    this.#scheduleFlush();
  }

  /** Releases a stream whose both sides ended; one the peer opened lets it open another. */
  #completed(stream) {
    this.#streams.delete(stream.id);
    if (!stream.remote || this.#closedError) {
      return;
    }
    const dir = stream.id % 4 < 2 ? Bidi : Uni;
    this.#remoteCredit[dir] += 1;
    const initial =
      dir === Bidi ? this.#params.initialMaxStreamsBidi : this.#params.initialMaxStreamsUni;
    if (this.#remoteCredit[dir] - this.#remoteLimit[dir] >= Math.max(1, Math.floor(initial / 2))) {
      this.#remoteLimit[dir] = this.#remoteCredit[dir];
      writeMaxStreams(this.#pending, dir === Bidi, this.#remoteLimit[dir]);
      this.#scheduleFlush();
    }
  }

  /** The largest datagram payload the peer accepts in one record, or 0 if it accepts none. */
  #maxDatagramSize() {
    if (this.#peer === null || this.#peer.maxDatagramFrameSize === 0) {
      return 0;
    }
    const frame = Math.min(this.#peer.maxDatagramFrameSize, this.#recordLimit());
    return Math.max(0, frame - 1 - varintLength(frame));
  }

  /**
   * Queues a datagram, or drops it if the peer accepts none this large or the WebSocket buffers
   * more than the send buffer limit: datagrams may be lost, as over QUIC.
   */
  #sendDatagram(data) {
    if (
      this.#closedError ||
      data.length > this.#maxDatagramSize() ||
      this.#ws.bufferedAmount > this.#sendBufferLimit
    ) {
      return;
    }
    if (this.#pending.length + datagramFrameSize(data.length) > this.#recordLimit()) {
      this.#flush();
    }
    writeDatagram(this.#pending, data);
    this.#scheduleFlush();
  }

  #recordLimit() {
    return Math.min(this.#peer?.maxRecordSize ?? DefaultMaxRecordSize, this.#maxSendRecordSize);
  }

  /** Waits for a change in credit or session state. */
  #wait() {
    return new Promise((resolve) => this.#waiters.add(resolve));
  }

  #wake() {
    if (this.#waiters.size === 0) {
      return;
    }
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
      this.#lastSent = this.#lastActivity;
    }
  }

  #finish(error, clean, closeInfo) {
    if (this.#closedError) {
      return;
    }
    this.#closedError = error;
    clearInterval(this.#idleTimer);
    clearInterval(this.#keepAliveTimer);
    clearTimeout(this.#handshakeTimer);
    for (const stream of this.#streams.values()) {
      stream.onSessionClosed(error);
    }
    this.#streams.clear();
    for (const incoming of this.#incoming) {
      incoming.end(clean ? undefined : error);
    }
    this.#datagrams?.end(error);
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

/** The queue of streams the peer opened in one direction, as a readable stream. */
class IncomingStreams {
  readable;
  /** Set once the application cancels the readable; streams the peer opens then are refused. */
  refused = false;
  #controller;

  constructor() {
    this.readable = new ReadableStream({
      start: (controller) => {
        this.#controller = controller;
      },
      cancel: () => {
        this.refused = true;
      },
    });
  }

  push(stream) {
    this.#controller.enqueue(stream);
  }

  /** Closes the readable, or errors it with `error`. */
  end(error) {
    try {
      if (error === undefined) {
        this.#controller.close();
      } else {
        this.#controller.error(error);
      }
    } catch {
      // Already cancelled.
    }
  }
}

/**
 * The session's datagrams, shaped like WebTransportDatagramDuplexStream. Datagrams that arrive
 * while DatagramQueueLength wait to be read, or after the readable was cancelled, are dropped.
 */
class QMuxDatagrams {
  api;
  #queue = [];
  /** Whether a read waits for the next datagram, which then goes straight to the readable. */
  #reading = false;
  #cancelled = false;
  #readController;
  #writeController;

  constructor(link) {
    const readable = new ReadableStream(
      {
        start: (controller) => {
          this.#readController = controller;
        },
        pull: (controller) => {
          const datagram = this.#queue.shift();
          if (datagram === undefined) {
            this.#reading = true;
          } else {
            controller.enqueue(datagram);
          }
        },
        cancel: () => {
          this.#cancelled = true;
          this.#reading = false;
          this.#queue = [];
        },
      },
      { highWaterMark: 0 },
    );
    const writable = new WritableStream({
      start: (controller) => {
        this.#writeController = controller;
      },
      write: async (chunk) => {
        await link.ready();
        link.sendDatagram(toBytes(chunk).slice());
      },
    });
    this.api = {
      readable,
      writable,
      get maxDatagramSize() {
        return link.maxDatagramSize();
      },
    };
  }

  /**
   * Delivers a datagram from the peer. It must not throw: the rest of the record it came in, which
   * may carry stream data, is still to be handled.
   */
  receive(datagram) {
    if (this.#cancelled) {
      return;
    }
    const copy = datagram.slice();
    if (this.#reading) {
      this.#reading = false;
      try {
        this.#readController.enqueue(copy);
      } catch {
        // The readable ended; nothing reads datagrams any more.
        this.#cancelled = true;
      }
    } else if (this.#queue.length < DatagramQueueLength) {
      this.#queue.push(copy);
    }
  }

  end(error) {
    this.#queue = [];
    for (const controller of [this.#readController, this.#writeController]) {
      try {
        controller.error(error);
      } catch {
        // Already ended.
      }
    }
  }
}

/**
 * One stream: WHATWG streams over STREAM, RESET_STREAM and STOP_SENDING. A send-only stream has
 * no readable and a receive-only one no writable.
 */
class QMuxStream {
  readable;
  writable;
  id;
  /** Whether the peer opened the stream. */
  remote;
  sendOffset = 0;
  sendMax = 0;
  recvMax = 0;
  writeError = null;

  #link;
  #recvWindow = 0;
  #received = 0;
  #read = 0;
  #chunks = [];
  #finalSize = null;
  #finReceived = false;
  #readError = null;
  #readCancelled = false;
  #sendState = SendState.Send;
  #pullWaiter = null;
  #writeController;
  #released = false;

  constructor(link, id, { sendMax, recvWindow, remote }) {
    this.id = id;
    this.remote = remote;
    this.#link = link;
    if (recvWindow === null) {
      // Send-only: the receive side is complete from the start.
      this.#finalSize = 0;
      this.#finReceived = true;
    } else {
      this.#recvWindow = recvWindow;
      this.recvMax = recvWindow;
      this.readable = new ReadableStream(
        {
          pull: (controller) => this.#pull(controller),
          cancel: (reason) => this.#cancelRead(resetCode(reason)),
        },
        { highWaterMark: 0 },
      );
    }
    if (sendMax === null) {
      // Receive-only: there is nothing to send.
      this.#sendState = SendState.DataSent;
    } else {
      this.sendMax = sendMax;
      this.writable = new WritableStream({
        start: (controller) => {
          this.#writeController = controller;
          // An abort waits for the write in flight, which may be waiting for credit that never
          // comes; the controller's signal fires at once, so the stream is reset and the write
          // fails. That is the whole abort, so the sink needs no abort callback.
          controller.signal.addEventListener(
            "abort",
            () => {
              const reason = controller.signal.reason;
              const code = resetCode(reason);
              this.#endSend(
                SendState.ResetSent,
                streamError(`stream aborted: ${reason?.message ?? reason}`, code),
                code,
              );
            },
            { once: true },
          );
        },
        write: (chunk) => this.#write(chunk),
        close: () => this.#endSend(SendState.DataSent),
      });
    }
  }

  onData(offset, data, fin) {
    const end = offset + data.length;
    if (this.#finalSize !== null && (end > this.#finalSize || (fin && end !== this.#finalSize))) {
      throw new QMuxProtocolError(
        TransportError.FinalSizeError,
        `stream ${this.id} data beyond its final size`,
        FrameType.Stream,
      );
    }
    if (offset !== this.#received) {
      throw new QMuxProtocolError(
        TransportError.ProtocolViolation,
        `stream ${this.id} data out of order`,
        FrameType.Stream,
      );
    }
    if (end > this.recvMax) {
      throw new QMuxProtocolError(
        TransportError.FlowControlError,
        `stream ${this.id} exceeds its flow control limit`,
        FrameType.Stream,
      );
    }
    this.#link.received(data.length, FrameType.Stream);
    this.#received = end;
    if (fin) {
      this.#finalSize = end;
      this.#finReceived = true;
    }
    if (this.#readCancelled || this.#readError) {
      this.#read = end;
      this.#link.consumed(data.length);
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
      throw new QMuxProtocolError(
        TransportError.FinalSizeError,
        `stream ${this.id} reset with an inconsistent final size`,
        FrameType.ResetStream,
      );
    }
    if (finalSize > this.recvMax) {
      throw new QMuxProtocolError(
        TransportError.FlowControlError,
        `stream ${this.id} final size exceeds its flow control limit`,
        FrameType.ResetStream,
      );
    }
    this.#link.received(finalSize - this.#received, FrameType.ResetStream);
    this.#received = finalSize;
    this.#finalSize = finalSize;
    if (!this.#readCancelled && !this.#readError) {
      this.#readError = streamError(`stream reset by peer with code ${code}`, code);
    }
    this.#link.consumed(finalSize - this.#read);
    this.#read = finalSize;
    this.#chunks = [];
    this.#wakePull();
    this.#maybeComplete();
  }

  onStopSending(code) {
    if (this.#sendState !== SendState.Send) {
      return;
    }
    const error = streamError(`stream stopped by peer with code ${code}`, code);
    this.#endSend(SendState.ResetSent, error, code);
    this.#writeController.error(error);
  }

  onSessionClosed(error) {
    this.#readError ??= error;
    this.#endSend(SendState.SessionClosed, error);
    this.#wakePull();
    try {
      this.#writeController?.error(error);
    } catch {
      // The writable already ended.
    }
  }

  /** Turns down a stream the application will never see: STOP_SENDING and RESET_STREAM, code 0. */
  refuse() {
    if (this.readable !== undefined) {
      this.#cancelRead(0);
    }
    if (this.#endSend(SendState.ResetSent, streamError("stream refused", 0), 0)) {
      this.#writeController.error(this.writeError);
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
    if (this.#finalSize === null && this.recvMax - this.#read <= this.#recvWindow / 2) {
      this.recvMax = this.#read + this.#recvWindow;
      this.#link.maxStreamData(this);
    }
    this.#link.consumed(length);
    this.#maybeComplete();
  }

  #cancelRead(code) {
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
    this.#link.consumed(unread);
    if (this.#finalSize === null && !this.#link.closedError()) {
      this.#link.stopSending(this, code);
    }
    this.#maybeComplete();
  }

  async #write(chunk) {
    let data = toBytes(chunk);
    while (data.length > 0) {
      const allowed = await this.#link.sendCredit(this);
      // The stream may have been reset or the session closed while this write resumed; STREAM
      // data must not follow a RESET_STREAM, whose final size ends the stream.
      if (this.#sendState !== SendState.Send) {
        throw this.writeError;
      }
      const piece = data.subarray(0, allowed);
      this.#link.sendStream(this, piece, false);
      data = data.subarray(piece.length);
    }
  }

  /**
   * The one transition out of `Send`, to `DataSent` (queue the FIN), `ResetSent` (queue
   * RESET_STREAM with `code`) or `SessionClosed`. The failing states record `error` as the write
   * error and wake writes parked in sendCredit, which see it and fail. Returns false when the send
   * side had already ended.
   */
  #endSend(next, error = null, code = 0) {
    if (this.#sendState !== SendState.Send) {
      return false;
    }
    this.#sendState = next;
    if (next === SendState.DataSent) {
      this.#link.sendStream(this, new Uint8Array(0), true);
    } else {
      this.writeError ??= error;
      if (next === SendState.ResetSent) {
        this.#link.resetStream(this, code);
      }
      this.#link.wakeSenders();
    }
    this.#maybeComplete();
    return true;
  }

  #maybeComplete() {
    if (
      !this.#released &&
      this.#sendState !== SendState.Send &&
      this.#finalSize !== null &&
      this.#read === this.#finalSize
    ) {
      this.#released = true;
      this.#link.completed(this);
    }
  }
}
