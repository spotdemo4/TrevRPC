import {
  encodeMessage,
  receivedMessage,
  sessionCompression,
  transportCompression,
} from "./compression.js";
import { datagramEndpoint, messageFrameSize } from "./datagram.js";
import {
  DefaultMaxFrameSize,
  FrameReader,
  FrameType,
  encodeOutgoingFrame,
  encodeRequestFrame,
} from "./framing.js";
import { normalizeMetadata, validateMetadata } from "./metadata.js";
import {
  Code,
  TrevRpcError,
  cancelled,
  deadlineExceeded,
  failedPrecondition,
  internal,
  invalidArgument,
  statusError,
  statusFromTransportError,
} from "./status.js";
import { CallStreams, streamBudget } from "./stream-compression.js";

/** Longest delay setTimeout can represent; it fires immediately beyond this. */
const MaxTimerDelayMs = 2_147_483_647;
const MaxUint64 = 0xffffffffffffffffn;

/** Returns the default call options. */
export function defaultCallOptions() {
  return {
    timeoutMs: undefined,
    maxFrameSize: DefaultMaxFrameSize,
    metadata: {},
    signal: undefined,
  };
}

/** Merges override options over resolved base options; metadata merges per key. */
export function mergeCallOptions(base, override = {}) {
  const merged = { ...base };
  for (const [key, value] of Object.entries(override)) {
    if (value !== undefined && key !== "metadata") {
      merged[key] = value;
    }
  }
  if (override.metadata != null) {
    merged.metadata = Object.assign(
      Object.create(null),
      base.metadata,
      normalizeMetadata(override.metadata),
    );
    validateMetadata(merged.metadata);
  }
  return merged;
}

/**
 * Cancellation and deadline for one call. The signal aborts with a TrevRpcError reason: the
 * failure that ended the call, the caller's own status error, DeadlineExceeded, or Cancelled
 * wrapping any other abort reason.
 */
class CallScope {
  #controller = new AbortController();
  #parent;
  #timer;

  /** timeoutMs must be validated by the caller. */
  constructor({ signal, timeoutMs }) {
    if (signal?.aborted) {
      this.abort(signal.reason);
      return;
    }
    if (signal != null) {
      this.#parent = { signal, listener: () => this.abort(signal.reason) };
      signal.addEventListener("abort", this.#parent.listener, { once: true });
    }
    if (timeoutMs != null && timeoutMs <= MaxTimerDelayMs) {
      this.#timer = setTimeout(
        () => this.abort(deadlineExceeded("RPC deadline exceeded")),
        timeoutMs,
      );
    }
  }

  get signal() {
    return this.#controller.signal;
  }

  /** Aborts the call; only the first reason counts. Returns the effective reason. */
  abort(reason) {
    if (!this.signal.aborted) {
      this.settle();
      this.#controller.abort(
        reason instanceof TrevRpcError ? reason : cancelled("RPC cancelled", reason),
      );
    }
    return this.signal.reason;
  }

  /** Stops the deadline and detaches from the caller's signal once the outcome is known. */
  settle() {
    clearTimeout(this.#timer);
    this.#parent?.signal.removeEventListener("abort", this.#parent.listener);
    this.#parent = undefined;
  }
}

/**
 * One RPC over one bidirectional stream: the request writer, the response frame reader, and the
 * call's cancellation scope. Transport failures surface as TrevRpcError, and once the call is
 * aborted every operation fails with the abort reason.
 */
export class Call {
  #scope;
  #writer;
  #cancelReader;
  #frames;
  #maxFrameSize;
  #sendClosed = false;
  /** Set once the close of the request side completed. */
  #closeCompleted = false;
  /**
   * The error every request-side operation fails with once the upload was ended, or undefined
   * while it is open.
   */
  #uploadEnded;
  /** Rejectors of the request-side operations waiting for the transport. */
  #pendingUploads = new Set();
  /** The transport error that made the request side fail on its own, or undefined. */
  #uploadFailure;
  /** The call's datagram flow, or undefined if its messages all travel on the stream. */
  #flow;
  /** Whether request messages are sent as datagrams when they fit. */
  #sendDatagrams = false;
  /** The compression state of the session that carries the call, or undefined without one. */
  #compression;
  /** The full protobuf type name of the request messages. */
  #requestType;
  /** The dictionary the call accepts on its response messages, or undefined. */
  #accept;
  /** Whether the REQUEST queried the server's dictionaries, so its HEADERS answer. */
  #queried = false;
  /** The call's stream compression, or undefined if its method or transport has none. */
  #streams;
  /** Request messages of a call that uses datagrams, queued for the flush loop, in order. */
  #unsent = [];
  /** Settles when the flush loop has sent everything queued so far. */
  #sendTail = Promise.resolve();
  #flushing = false;

  /**
   * Opens a stream on the transport and sends the REQUEST frame. The request carries the
   * method path and kind; metadata and the wire timeout come from the options. The request
   * message of a unary or server-streaming call, a marshalled body, goes out in the same write.
   *
   * `rpc` describes the method. `rpc.types` holds the full names of its request and response
   * message types, which pick the dictionaries when the transport has compression. `rpc.datagrams`
   * is `{ send, receive }` for a method that marks a direction unreliable: the call gets a
   * datagram flow if the session that carries it has datagrams. Both depend on that session, which
   * is only known once the stream is open, so then the REQUEST is encoded after that. `send` makes
   * the call send request messages as datagrams, `receive` makes it accept response datagrams.
   * `rpc.compressStream` is `{ requests, responses }`, the directions the call may
   * stream-compress when the transport has compression.
   */
  static async open(transport, request, message, options, rpc = {}) {
    const fields = { ...request, metadata: options.metadata, ...timeoutField(options) };
    const compression = transportCompression(transport);
    const perSession = rpc.datagrams !== undefined || compression !== undefined;
    const header = perSession
      ? null
      : encodeRequestFrame(fields, outgoing(message), options.maxFrameSize);
    const scope = new CallScope(options);
    let call;
    try {
      const session = perSession ? {} : undefined;
      const stream = await openStream(transport, scope.signal, session);
      call = new Call(stream, scope, options.maxFrameSize);
      await call.#write(
        header ?? call.#requestOnSession(fields, message, session, compression, transport, rpc),
      );
      return call;
    } catch (error) {
      if (call !== undefined) {
        call.#releaseSessionState();
      }
      throw scope.abort(failureStatus(error));
    }
  }

  /**
   * Sets the call up on the session that carries it, negotiating compression and opening its
   * datagram flow as the method needs, and encodes the REQUEST.
   */
  #requestOnSession(fields, message, session, compression, transport, rpc) {
    const { types, datagrams, compressStream } = rpc;
    const request = { ...fields };
    if (compression !== undefined) {
      this.#compression = sessionCompression(compression, session.key);
      this.#requestType = types.request;
      const negotiation = this.#compression.prepare(types.response);
      Object.assign(request, negotiation.fields);
      this.#accept = negotiation.accept;
      this.#queried = negotiation.queried;
      if (compressStream?.requests || compressStream?.responses) {
        this.#streams = new CallStreams(compression, streamBudget(transport), compressStream, {
          session: this.#compression,
          requestType: types.request,
          accept: negotiation.accept,
        });
        this.#streams.offer(request);
      }
    }
    if (datagrams !== undefined) {
      this.#flow = datagramEndpoint(session.datagrams)?.openFlow({
        receive: datagrams.receive,
        inflate: (frame) => this.receivedMessage(frame),
      });
      this.#sendDatagrams = this.#flow !== undefined && datagrams.send;
      if (this.#flow !== undefined) {
        request.datagramFlow = this.#flow.id;
      }
    }
    return encodeRequestFrame(
      request,
      message === undefined ? undefined : this.#encode(message),
      this.#maxFrameSize,
    );
  }

  /**
   * Encodes a request message for sending: through the call's stream context once that exists,
   * and otherwise compressed when the session's shared set has the dictionary of the request type.
   * Both are checked per message, so a request stream starts compressing as soon as the server's
   * answer arrives.
   */
  #encode(body) {
    if (body.byteLength > this.#maxFrameSize) {
      return encodeMessage(body);
    }
    const dictionary = this.#compression?.requestDictionary(this.#requestType);
    return this.#streams === undefined
      ? encodeMessage(body, dictionary)
      : this.#streams.encode(body, dictionary);
  }

  /**
   * The message of a received MESSAGE, ZSTD_MESSAGE or ZSTD_STREAM_MESSAGE frame,
   * `{ type, payload }`. A compressed message must use what the call negotiated and fit
   * `maxFrameSize`; it fails with Internal or ResourceExhausted otherwise.
   */
  receivedMessage(frame) {
    if (frame.type === FrameType.ZstdStreamMessage) {
      if (this.#streams === undefined) {
        throw internal("stream-compressed message where stream compression is not enabled");
      }
      return this.#streams.receive(frame.payload, this.#maxFrameSize);
    }
    return receivedMessage(frame, this.#accept, this.#maxFrameSize);
  }

  /** Records the server's answers in the response HEADERS to what the REQUEST asked. */
  learnAnswers(headers) {
    if (this.#queried) {
      this.#compression.learn(headers.dictionaries);
    }
    this.#streams?.learn(headers.streamCompression);
  }

  constructor(stream, scope, maxFrameSize) {
    const reader = stream.readable.getReader();
    this.#scope = scope;
    this.#writer = stream.writable.getWriter();
    this.#cancelReader = (reason) => reader.cancel(reason);
    this.#frames = new FrameReader(reader);
    this.#maxFrameSize = maxFrameSize;
    // The transport errors the request side as soon as the server stops reading it.
    this.#writer.closed.catch((error) => {
      if (this.#uploadEnded === undefined) {
        this.#uploadFailure ??= statusFromTransportError(error);
      }
    });
    scope.signal.addEventListener("abort", () => this.#teardown(), { once: true });
    if (scope.signal.aborted) {
      this.#teardown();
    }
  }

  /** The call's abort signal. */
  get signal() {
    return this.#scope.signal;
  }

  /**
   * The transport error that made the request side fail on its own, for instance because the
   * server stopped reading it; undefined while the request side works or was ended by the call.
   */
  get uploadFailure() {
    return this.#uploadFailure;
  }

  /** Whether the request side can still be written: not ended, not closed. */
  get uploadOpen() {
    return this.#uploadEnded === undefined && !this.#closeCompleted;
  }

  /** The largest frame the call accepts and sends, in bytes. */
  get maxFrameSize() {
    return this.#maxFrameSize;
  }

  /** The call's datagram flow, or undefined if its messages all travel on the stream. */
  get flow() {
    return this.#flow;
  }

  /**
   * Sends one streaming request message; writes are ordered and apply backpressure. Fails with
   * the error that ended the upload, if any, even while the transport itself is still blocked.
   * On a call that sends datagrams the message goes out as one if it fits, and on the stream
   * otherwise.
   */
  sendMessage(body) {
    if (this.#uploadEnded !== undefined) {
      return Promise.reject(this.#uploadEnded);
    }
    if (this.#sendClosed) {
      return Promise.reject(failedPrecondition("request stream is closed"));
    }
    const message = this.#encode(body);
    if (this.#sendDatagrams) {
      return this.#upload(
        new Promise((resolve, reject) => {
          this.#unsent.push({ message, resolve, reject });
          if (!this.#flushing) {
            this.#flushing = true;
            // Messages sent in the same turn are packed together; nothing waits for a timer.
            this.#sendTail = Promise.resolve().then(() => this.#flush());
          }
        }),
      );
    }
    return this.#write(encodeOutgoingFrame(message, this.#maxFrameSize));
  }

  /**
   * Sends the queued request messages in order: as many as fit into one datagram go together, a
   * message that cannot be a datagram goes on the stream, and so does every message of a datagram
   * that failed to send. Messages queued while a datagram waits for send-buffer space join the
   * next one.
   */
  async #flush() {
    let inFlight = [];
    try {
      while (this.#unsent.length > 0) {
        if (this.#uploadEnded !== undefined) {
          throw this.#uploadEnded;
        }
        inFlight = this.#takeBatch();
        if (
          inFlight.length === 0 ||
          !(await this.#flow.send(inFlight.map(messageOf), this.signal))
        ) {
          // Not a datagram: a head message that never fits, or a datagram that failed to send.
          inFlight = inFlight.length > 0 ? inFlight : [this.#unsent.shift()];
          for (const { message } of inFlight) {
            await this.#writer.write(encodeOutgoingFrame(message, this.#maxFrameSize));
          }
        }
        for (const item of inFlight) {
          item.resolve();
        }
        inFlight = [];
      }
    } catch (error) {
      for (const item of [...inFlight, ...this.#unsent.splice(0)]) {
        item.reject(error);
      }
    } finally {
      this.#flushing = false;
    }
  }

  /**
   * Removes the longest run of queued messages from the front that fits one datagram, at least
   * one if the head fits alone; empty if the head can only go on the stream.
   */
  #takeBatch() {
    let room = this.#flow.frameRoom;
    let count = 0;
    while (count < this.#unsent.length) {
      const { message } = this.#unsent[count];
      const size = messageFrameSize(message);
      if (message.size > this.#maxFrameSize || size > room) {
        break;
      }
      room -= size;
      count += 1;
    }
    return this.#unsent.splice(0, count);
  }

  /** Finishes the request stream gracefully. */
  closeSend() {
    if (this.#sendClosed) {
      return Promise.resolve();
    }
    this.#sendClosed = true;
    // Datagram sends in flight go first, so the stream does not end before them.
    const closing = this.#sendDatagrams
      ? this.#sendTail.then(() => this.#writer.close())
      : this.#writer.close();
    return this.#upload(closing).then(() => {
      this.#closeCompleted = true;
    });
  }

  /**
   * Reads the next known response frame as `{ type, payload }`, or null if the stream ended
   * cleanly. Unknown frames are skipped.
   */
  readFrame() {
    return this.#guard(this.#frames.readFrame(this.#maxFrameSize));
  }

  /**
   * Ends the call after its final status was received. The request side stops: pending and later
   * sends fail with `uploadError`, and an unfinished upload is reset. The rest of the response
   * stream is drained in the background: unknown frames are skipped, and a known frame after
   * STATUS cancels the stream without changing the outcome.
   */
  finish(uploadError) {
    this.#scope.settle();
    this.#releaseSessionState();
    this.#endUpload(uploadError, cancelled("RPC completed"));
    this.#frames.expectEnd(this.#maxFrameSize).catch(() => this.#cancelReader().catch(() => {}));
  }

  /** Cancels the call on behalf of the caller; any reason becomes a Cancelled status. */
  cancel(reason) {
    this.#scope.abort(reason);
  }

  /** Fails the call, tearing down both stream directions. Returns the effective error. */
  fail(error) {
    return this.#scope.abort(failureStatus(error));
  }

  #write(bytes) {
    return this.#upload(this.#writer.write(bytes));
  }

  /**
   * Waits for a write or close of the request side. Ending the upload settles it at once: a
   * transport blocked on flow control does not act on an abort while the write is in flight.
   */
  #upload(operation) {
    return new Promise((resolve, reject) => {
      this.#pendingUploads.add(reject);
      operation.then(
        (value) => {
          this.#pendingUploads.delete(reject);
          if (this.signal.aborted) {
            reject(this.signal.reason);
          } else {
            resolve(value);
          }
        },
        (error) => {
          this.#pendingUploads.delete(reject);
          reject(this.#uploadError(error));
        },
      );
    });
  }

  /** Maps a failed request-side operation to the call's error. */
  #uploadError(error) {
    if (this.signal.aborted) {
      return this.signal.reason;
    }
    if (error === this.#uploadEnded) {
      return error;
    }
    this.#uploadFailure ??= statusFromTransportError(error);
    return this.#uploadFailure;
  }

  /** Stops the request side with `error` for everything waiting on it; only the first call counts. */
  #endUpload(error, abortReason = error) {
    if (this.#uploadEnded !== undefined) {
      return;
    }
    this.#uploadEnded = error;
    this.#sendClosed = true;
    this.#writer.abort(abortReason).catch(() => {});
    const pending = [...this.#pendingUploads];
    this.#pendingUploads.clear();
    for (const reject of pending) {
      reject(error);
    }
  }

  async #guard(operation) {
    let result;
    try {
      result = await operation;
    } catch (error) {
      throw this.signal.aborted ? this.signal.reason : statusFromTransportError(error);
    }
    // Cancelling the reader completes pending reads as end-of-stream instead of rejecting them.
    throwIfAborted(this.signal);
    return result;
  }

  /**
   * Ends the datagram flow and frees the stream compression contexts, if any; every way a call
   * ends gets here.
   */
  #releaseSessionState() {
    this.#flow?.release();
    this.#streams?.close();
  }

  #teardown() {
    this.#releaseSessionState();
    const reason = this.signal.reason;
    this.#endUpload(reason);
    this.#cancelReader(reason).catch(() => {});
  }
}

/**
 * Opens a bidirectional stream. A call whose setup depends on its session passes `session`, which
 * receives that session's `key` and `datagrams` object, so the compression state and datagram flow
 * belong to the connection that carries the call and not to one a reconnect swapped in later.
 */
function openStream(transport, signal, session) {
  return new Promise((resolve, reject) => {
    if (signal.aborted) {
      reject(signal.reason);
      return;
    }
    const onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });

    let opening;
    try {
      opening = Promise.resolve(startStream(transport, session));
    } catch (error) {
      opening = Promise.reject(error);
    }
    opening.then(
      (stream) => {
        signal.removeEventListener("abort", onAbort);
        if (signal.aborted) {
          // The caller gave up while the stream was opening; do not leak it.
          stream.writable.abort(signal.reason).catch(() => {});
          stream.readable.cancel(signal.reason).catch(() => {});
        } else {
          resolve(stream);
        }
      },
      (error) => {
        signal.removeEventListener("abort", onAbort);
        reject(statusFromTransportError(error));
      },
    );
  });
}

function messageOf(item) {
  return item.message;
}

/** A request message body as an uncompressed outgoing message, or undefined. */
function outgoing(body) {
  return body === undefined ? undefined : encodeMessage(body);
}

function startStream(transport, session) {
  if (session === undefined) {
    return transport.createBidirectionalStream();
  }
  if (typeof transport.createBidirectionalStreamWithSession === "function") {
    return transport.createBidirectionalStreamWithSession().then(({ stream, session: opened }) => {
      session.key = opened;
      session.datagrams = opened.datagrams;
      return stream;
    });
  }
  // A WebTransport session used directly: the stream opened right now belongs to it.
  session.key = transport;
  session.datagrams = transport.datagrams;
  return transport.createBidirectionalStream();
}

function throwIfAborted(signal) {
  if (signal.aborted) {
    throw signal.reason;
  }
}

/** Status errors describe themselves; anything else reaching the call is an internal bug. */
function failureStatus(error) {
  return error instanceof TrevRpcError
    ? error
    : statusError(Code.Internal, error?.message ?? String(error), {}, error);
}

/**
 * Validates a call timeout and returns the request field for it. Zero means unset on the wire, so
 * a zero timeout is sent as 1ns.
 */
function timeoutField({ timeoutMs }) {
  if (timeoutMs == null) {
    return {};
  }
  if (!(timeoutMs >= 0 && Number.isFinite(timeoutMs))) {
    throw invalidArgument("RPC timeout must be a non-negative finite number of milliseconds");
  }
  const nanos = BigInt(Math.ceil(timeoutMs)) * 1_000_000n;
  if (nanos > MaxUint64) {
    throw invalidArgument("RPC timeout does not fit in 64-bit nanoseconds");
  }
  return { timeoutNanos: String(nanos === 0n ? 1n : nanos) };
}
