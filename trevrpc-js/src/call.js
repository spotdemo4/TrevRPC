import {
  encodeMessage,
  receivedMessage,
  sessionCompression,
  transportCompression,
} from "./compression.js";
import { transportConnectionMetadata } from "./connection-metadata.js";
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
import { CallStreams, ReplayCopies, streamBudget } from "./stream-compression.js";

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
/** Why sends fail once the server completed the call with OK before the upload ended. */
export const EarlyOk = "the server completed the call before the request stream ended";

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
  /** The connection metadata state of the session that carries the call, or undefined without. */
  #connectionMetadata;
  /** The ID of the connection metadata set the REQUEST offered, or zero. */
  #offeredMetadata = 0;
  /** The full protobuf type name of the request messages. */
  #requestType;
  /**
   * What the call accepts on its response messages: a Map from dictionary ID to dictionary, 0 for
   * compression without one; undefined without compression.
   */
  #accept;
  /** Whether request messages may be compressed without a dictionary. */
  #plainRequests = false;
  /** Whether the REQUEST queried the server's dictionaries, so its ANSWER answers the query. */
  #queried = false;
  /** The call's stream compression, or undefined if its method or transport has none. */
  #streams;
  /** Request messages of a call that uses datagrams, queued for the flush loop, in order. */
  #unsent = [];
  /** Settles when the flush loop has sent everything queued so far. */
  #sendTail = Promise.resolve();
  #flushing = false;
  /**
   * What the call needs to open itself again, for the replay of a refused optimistic call:
   * `{ transport, request, rpc, deadline }`, where `deadline` is the `performance.now()` time the
   * call's timeout ends, or undefined.
   */
  #setup;
  /**
   * The requests an optimistic call has sent, kept until the server accepts or refuses it;
   * undefined once it decided, and on calls that are not optimistic.
   */
  #copies;
  /** Whether the call was optimistic, so its sends and close go through #ordered. */
  #optimistic = false;
  /** Settles once the previous send or close of an optimistic call has written, in call order. */
  #ordered = Promise.resolve();
  /** The replay of a refused optimistic call, once it started. */
  #replaying;

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
   * stream-compress when the transport has compression, and `rpc.compress` the same for
   * compression without a dictionary.
   */
  static async open(transport, request, message, options, rpc = {}) {
    const fields = { ...request, metadata: options.metadata, ...timeoutField(options) };
    const compression = transportCompression(transport);
    // The REQUEST depends on the session that carries the call if it negotiates compression, opens
    // a datagram flow or carries connection metadata.
    const perSession =
      rpc.datagrams !== undefined ||
      compression !== undefined ||
      transportConnectionMetadata(transport)?.current !== undefined;
    const header = perSession
      ? null
      : encodeRequestFrame(fields, outgoing(message), options.maxFrameSize);
    const scope = new CallScope(options);
    let call;
    try {
      const session = perSession ? {} : undefined;
      const stream = await openStream(transport, scope.signal, session);
      call = new Call(stream, scope, options.maxFrameSize);
      call.#setup = {
        transport,
        request: { ...request, metadata: options.metadata },
        rpc,
        deadline: options.timeoutMs == null ? undefined : performance.now() + options.timeoutMs,
      };
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
   * datagram flow as the method needs, and encodes the REQUEST. A call whose requests may be
   * stream-compressed offers optimistically while the session allows it, unless `replay` says this
   * is the replay of a refused call or the call has a datagram flow, which a replay would need to
   * open again.
   */
  #requestOnSession(fields, message, session, compression, transport, rpc, replay = false) {
    const { types, datagrams, compressStream, compress } = rpc;
    const request = { ...fields };
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
    if (compression !== undefined) {
      this.#compression = sessionCompression(compression, session.key);
      this.#requestType = types.request;
      const negotiation = this.#compression.prepare(types.response, Boolean(compress?.responses));
      Object.assign(request, negotiation.fields);
      this.#accept = negotiation.accept;
      this.#plainRequests = Boolean(compress?.requests);
      this.#queried = negotiation.queried;
      if (compressStream?.requests || compressStream?.responses) {
        const budget = streamBudget(transport);
        this.#streams = new CallStreams(compression, budget, compressStream, {
          session: this.#compression,
          requestType: types.request,
          accept: negotiation.accept,
        });
        const optimistic =
          !replay && this.#flow === undefined && this.#compression.optimisticStreams;
        if (this.#streams.offer(request, optimistic)) {
          this.#copies = new ReplayCopies(budget);
          this.#optimistic = true;
        }
      }
    }
    this.#connectionMetadata = transportConnectionMetadata(transport)?.session(session.key);
    if (this.#connectionMetadata !== undefined) {
      const { field, offered } = this.#connectionMetadata.prepare();
      this.#offeredMetadata = offered;
      if (field !== undefined) {
        request.connectionMetadata = field;
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
   * and otherwise compressed when the session's shared set has the dictionary of the request type,
   * or compression without a dictionary where the method allows it. Both are checked per message,
   * so a request stream starts compressing as soon as the server's answer arrives.
   */
  #encode(body) {
    if (body.byteLength > this.#maxFrameSize) {
      return encodeMessage(body);
    }
    const dictionary = this.#compression?.requestCompressor(this.#requestType, this.#plainRequests);
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

  /**
   * Records the server's ANSWER to what the REQUEST asked. It answers a dictionary query whole,
   * and must accept an optimistic call; a `stream_compression` answer also lets the session's
   * calls be optimistic again after a refusal. A `connection_metadata` answer confirms the set the
   * REQUEST offered, so the session's calls refer to it from now on.
   */
  learnAnswers(answer) {
    if (this.#copies !== undefined && !answer.streamCompression) {
      throw internal("ANSWER of an optimistic call did not accept stream compression");
    }
    this.#connectionMetadata?.learn(this.#offeredMetadata, answer);
    if (this.#queried) {
      this.#compression.learn(answer.dictionaries);
    }
    if (answer.streamCompression) {
      this.#streams?.accept();
      if (this.#compression !== undefined) {
        this.#compression.optimisticStreams = true;
      }
    }
    this.#settleCopies("accepted");
  }

  /**
   * Whether the server has yet to accept or refuse the call's optimistic stream compression, so a
   * refusing STATUS can still be replayed.
   */
  get replayable() {
    return this.#copies !== undefined;
  }

  /**
   * Replays a call the server refused: opens a new stream for it with the same REQUEST, not
   * optimistic and with the time left before its deadline, and sends the kept requests and, if the
   * caller closed the request side, its end. Sends waiting meanwhile continue on the new stream.
   * The session stops being optimistic until a call is answered. Resolves once the replay was
   * written; the response is read from the new stream after that.
   */
  replay() {
    const copies = this.#copies;
    this.#copies = undefined;
    this.#compression.optimisticStreams = false;
    this.#replaying = this.#reopen(copies.settle("refused"));
    this.#replaying.catch(() => {});
    return this.#replaying;
  }

  async #reopen(bodies) {
    const { transport, request, rpc, deadline } = this.#setup;
    // The old stream's failures belong to the refused call, not to this one.
    this.#writer.abort(cancelled("RPC replayed")).catch(() => {});
    this.#cancelReader(cancelled("RPC replayed")).catch(() => {});
    this.#releaseSessionState();
    const session = {};
    const stream = await openStream(transport, this.signal, session);
    this.#attach(stream);
    this.#uploadFailure = undefined;
    const timeoutMs =
      deadline === undefined ? undefined : Math.max(0, deadline - performance.now());
    const fields = { ...request, ...timeoutField({ timeoutMs }) };
    const compression = transportCompression(transport);
    await this.#write(
      this.#requestOnSession(fields, undefined, session, compression, transport, rpc, true),
    );
    for (const body of bodies) {
      await this.#write(encodeOutgoingFrame(this.#encode(body), this.#maxFrameSize));
    }
    if (this.#sendClosed && this.#uploadEnded === undefined) {
      await this.#upload(this.#writer.close());
      this.#closeCompleted = true;
    }
  }

  /** Ends an optimistic call's wait for the server's decision with `outcome`. */
  #settleCopies(outcome) {
    const copies = this.#copies;
    this.#copies = undefined;
    copies?.settle(outcome);
  }

  constructor(stream, scope, maxFrameSize) {
    this.#scope = scope;
    this.#maxFrameSize = maxFrameSize;
    this.#attach(stream);
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

  /** Makes `stream` the one the call writes and reads. */
  #attach(stream) {
    const reader = stream.readable.getReader();
    const writer = stream.writable.getWriter();
    this.#writer = writer;
    this.#cancelReader = (reason) => reader.cancel(reason);
    this.#frames = new FrameReader(reader);
    // The transport errors the request side as soon as the server stops reading it.
    writer.closed.catch((error) => {
      if (this.#writer === writer && this.#uploadEnded === undefined) {
        this.#uploadFailure ??= statusFromTransportError(error);
      }
    });
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
    if (this.#optimistic) {
      return this.#inOrder((issued) => this.#sendOptimistic(body, issued));
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

  /**
   * Runs `operation(issued)` after the previous send or close of an optimistic call has written,
   * so they reach the stream in call order even when one waits; `operation` calls `issued` once
   * it has written.
   */
  #inOrder(operation) {
    const previous = this.#ordered;
    let issued;
    this.#ordered = new Promise((resolve) => {
      issued = resolve;
    });
    return previous.then(() => operation(issued)).finally(issued);
  }

  /**
   * Sends a request of an optimistic call. Until the server decides, the request is kept for a
   * replay; one that does not fit waits for the decision. A request written to a stream the
   * server then refused does not fail: the replay sends it again.
   */
  async #sendOptimistic(body, issued) {
    let copies;
    for (;;) {
      if (this.#uploadEnded !== undefined) {
        throw this.#uploadEnded;
      }
      if (this.#replaying !== undefined) {
        await this.#replaying;
      }
      if (this.#copies === undefined) {
        break;
      }
      if (this.#copies.keep(body)) {
        copies = this.#copies;
        break;
      }
      await this.#copies.decided;
    }
    const writing = this.#write(encodeOutgoingFrame(this.#encode(body), this.#maxFrameSize));
    issued();
    await this.#survivingRefusal(writing, copies);
  }

  /**
   * Waits for a write of an optimistic call. If it fails while the server's decision was pending
   * and the server then refused the call, the replay covers it: this waits for the replay instead.
   */
  async #survivingRefusal(writing, copies) {
    try {
      return await writing;
    } catch (error) {
      if (copies !== undefined && (await copies.decided) === "refused") {
        await this.#replaying;
        return undefined;
      }
      throw error;
    }
  }

  /** Finishes the request stream gracefully. */
  closeSend() {
    if (this.#sendClosed) {
      return Promise.resolve();
    }
    this.#sendClosed = true;
    if (this.#optimistic) {
      return this.#inOrder(async (issued) => {
        if (this.#replaying !== undefined) {
          // A replay in progress ends the new stream itself; after one, the stream is ended here.
          issued();
          await this.#replaying;
          if (!this.#closeCompleted) {
            await this.#upload(this.#writer.close());
            this.#closeCompleted = true;
          }
          return;
        }
        const copies = this.#copies;
        const closing = this.#upload(this.#writer.close());
        issued();
        await this.#survivingRefusal(closing, copies);
        this.#closeCompleted = true;
      });
    }
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
   * Ends the call after its final status was received: `error` for a failed call, undefined for
   * an OK one. An unfinished upload stops: pending and later sends fail with `error`, or with a
   * Cancelled status saying the server completed the call early, and the request side is reset.
   * A request side the caller already closed is left to finish, since the server either read all
   * of it or stops it after its STATUS (see the wire format): resetting it would only cost the
   * transport, and a browser's WebTransport a round trip to its network service, on every call.
   * The rest of the response stream is drained in the background: unknown frames are skipped,
   * and a known frame after STATUS cancels the stream without changing the outcome.
   */
  finish(error) {
    this.#scope.settle();
    this.#releaseSessionState();
    if (!this.#sendClosed || this.#uploadEnded !== undefined) {
      this.#endUpload(error ?? cancelled(EarlyOk), cancelled("RPC completed"));
    }
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
    const writer = this.#writer;
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
          reject(this.#uploadError(error, writer));
        },
      );
    });
  }

  /**
   * Maps a failed request-side operation on `writer` to the call's error. Only the stream the call
   * still writes records the upload failure; a replayed call's old stream does not.
   */
  #uploadError(error, writer) {
    if (this.signal.aborted) {
      return this.signal.reason;
    }
    if (error === this.#uploadEnded) {
      return error;
    }
    if (writer !== this.#writer) {
      return statusFromTransportError(error);
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
    this.#settleCopies("ended");
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
