import { Call, EarlyOk, defaultCallOptions, mergeCallOptions } from "./call.js";
import { FrameType, marshalMessage, unmarshalMessage } from "./framing.js";
import { validateMetadata } from "./metadata.js";
import { Code, cancelled, internal, statusError } from "./status.js";
import { RpcAnswer, RpcHeaders, RpcKind, RpcStatus } from "./wire.js";

/** Creates a service client from a generated service descriptor. */
export function createServiceClient(transport, service, root, options = {}) {
  const defaults = mergeCallOptions(defaultCallOptions(), options);
  const client = {};

  for (const [jsName, method] of Object.entries(service.methods)) {
    const requestType = root.lookupType(method.inputType);
    const responseType = root.lookupType(method.outputType);
    const rpc = {
      // The wire method path, computed once per method rather than per call.
      path: `/${service.fullName}/${method.name}`,
      requestType,
      responseType,
      // Full protobuf names, which pick the compression dictionaries.
      types: { request: fullName(requestType), response: fullName(responseType) },
      datagrams: datagramUse(method),
      compressStream: streamCompressionUse(method),
      compress: plainCompressionUse(method),
    };
    const resolve = (override) => mergeCallOptions(defaults, override);

    switch (method.kind) {
      case "unary":
        client[jsName] = async (request, override) =>
          unaryCall(transport, rpc, request, resolve(override));
        break;
      case "serverStreaming":
        client[jsName] = (request, override) =>
          serverStreamingCall(transport, rpc, request, resolve(override));
        break;
      case "clientStreaming":
        client[jsName] = (override) => clientStreamingCall(transport, rpc, resolve(override));
        break;
      case "bidirectionalStreaming":
        client[jsName] = (override) =>
          bidirectionalStreamingCall(transport, rpc, resolve(override));
        break;
      default:
        throw internal(`unsupported generated RPC kind ${JSON.stringify(method.kind)}`);
    }
  }

  return client;
}

/** Calls a unary RPC and returns the decoded response. */
export async function unaryCall(transport, rpc, request, options) {
  const body = marshalMessage(rpc.requestType, request);
  const call = await Call.open(transport, requestFields(rpc, RpcKind.Unary), body, options, rpc);
  const stream = new BidirectionalStreamingCall(call, rpc, { options });
  // The response decides the outcome, so the request side finishes in the background.
  stream.closeSend().catch(() => {});
  return await receiveSingleResponse(stream, "unary");
}

/** Calls a server-streaming RPC and returns its response stream. */
export async function serverStreamingCall(transport, rpc, request, options) {
  const body = marshalMessage(rpc.requestType, request);
  const call = await Call.open(
    transport,
    requestFields(rpc, RpcKind.ServerStreaming),
    body,
    options,
    rpc,
  );
  const stream = new BidirectionalStreamingCall(call, rpc, { options });
  stream.closeSend().catch(() => {});
  return new ResponseStream(stream);
}

/** Calls a client-streaming RPC and returns a call for sending requests. */
export async function clientStreamingCall(transport, rpc, options) {
  const call = await Call.open(
    transport,
    requestFields(rpc, RpcKind.ClientStreaming),
    undefined,
    options,
    rpc,
  );
  return new ClientStreamingCall(
    new BidirectionalStreamingCall(call, rpc, { streamsRequest: true, options }),
  );
}

/** Calls a bidirectional-streaming RPC. */
export async function bidirectionalStreamingCall(transport, rpc, options) {
  const call = await Call.open(
    transport,
    requestFields(rpc, RpcKind.BidirectionalStreaming),
    undefined,
    options,
    rpc,
  );
  return new BidirectionalStreamingCall(call, rpc, { streamsRequest: true, options });
}

/**
 * Receives the single response message of a unary or client-streaming call, then its final
 * status. A second message fails the call immediately; none at all is an internal error.
 */
async function receiveSingleResponse(stream, kind) {
  let message;
  let count = 0;
  for (;;) {
    const next = await stream.recv();
    if (next === undefined) {
      break;
    }
    count += 1;
    if (count > 1) {
      const error = internal(`${kind} RPC returned more than one response message`);
      await stream.close(error);
      throw error;
    }
    message = next;
  }
  if (count !== 1) {
    throw internal(`${kind} RPC returned no response message`);
  }
  await stream.status;
  return message;
}

/**
 * How much of a response the call reads ahead of recv() while it is still uploading: at most this
 * many frames or bytes are queued before reading pauses and the transport's flow control applies
 * to a caller that does not receive. The byte bound is checked before each read, so the queue can
 * exceed it by at most one frame, which is at most `maxFrameSize`. Without a bound a caller that
 * only sends could be made to buffer a whole response.
 */
const ReadAheadFrames = 32;
const ReadAheadBytes = 1 << 20;

/**
 * How a method uses datagrams, or undefined if it does not: `send` for request messages, which
 * needs a streaming request and `unreliableRequests`, and `receive` for response messages, which
 * every call with a streaming response accepts. A method needs a flow if it marks either
 * direction unreliable; the generator rejects marks on a direction that does not stream.
 */
function datagramUse({ kind, unreliableRequests, unreliableResponses }) {
  const streamsRequest = kind === "clientStreaming" || kind === "bidirectionalStreaming";
  const streamsResponse = kind === "serverStreaming" || kind === "bidirectionalStreaming";
  if (!(unreliableRequests || unreliableResponses) || !(streamsRequest || streamsResponse)) {
    return undefined;
  }
  return { send: Boolean(unreliableRequests) && streamsRequest, receive: streamsResponse };
}

/**
 * The directions a method may stream-compress, `{ requests, responses }`, or undefined for none:
 * those its descriptor marks (the generator leaves out directions that do not stream and message
 * types with redacted fields), except directions marked unreliable, whose messages mostly travel
 * as datagrams.
 */
function streamCompressionUse({ compressStreamRequests, compressStreamResponses, ...method }) {
  const requests = Boolean(compressStreamRequests) && !method.unreliableRequests;
  const responses = Boolean(compressStreamResponses) && !method.unreliableResponses;
  return requests || responses ? { requests, responses } : undefined;
}

/**
 * The directions whose messages a method may compress without a dictionary, `{ requests,
 * responses }`, or undefined for none: those its descriptor marks (the generator marks both for
 * `COMPRESSION_MESSAGE` and `COMPRESSION_STREAM`, except message types with redacted fields).
 */
function plainCompressionUse({ compressRequests, compressResponses }) {
  const requests = Boolean(compressRequests);
  const responses = Boolean(compressResponses);
  return requests || responses ? { requests, responses } : undefined;
}

/**
 * A bidirectional-streaming RPC. Messages are received with recv() or async iteration. The call
 * ends with the server's final status: a non-OK status makes recv() throw it, and `status`
 * settles with the final status or the error that ended the call. `headers` resolves once the
 * first response frame after any ANSWER arrived; reading it starts that read, so headers are
 * available before the first message.
 *
 * While the request side is open, the response is read ahead of recv() (see ReadAheadFrames), so
 * a final status reaches a caller that is only sending. It stops the upload at once: pending and
 * later sends fail with the non-OK status, or with Cancelled if the status is OK. A call that
 * does not stream its request reads only what recv() or `headers` ask for.
 *
 * The final status settles the outcome as soon as it is read. Responses read before it are still
 * delivered in order, and the call's deadline and abort signal no longer apply to it.
 *
 * The call options' `onResponseHeaders` is called with the headers when the first frame after
 * ANSWER is read, and `onResponseMetadata` with the trailing metadata when the final status is
 * OK. A callback that throws fails the call.
 *
 * A call that stream-compresses its requests optimistically and is refused by the server is
 * replayed without that, transparently: the caller's sends and the response continue on the
 * replay.
 */
export class BidirectionalStreamingCall {
  #call;
  #rpc;
  /** Whether the request side streams, so that responses are read ahead of recv(). */
  #streamsRequest;
  /** Serializes recv() so concurrent callers never interleave frame reads. */
  #reads = Promise.resolve();
  /** Response messages read ahead of recv(), in order. */
  #queue = [];
  #queuedBytes = 0;
  /**
   * What ends the response, delivered by recv() after the queued messages: `{ status, error }`
   * for the final status (error unless OK) or `{ error }` for a failed stream.
   */
  #terminal;
  /** An error that ended the call out of order, such as an abort; queued messages are dropped. */
  #failure;
  #failingInOrder = false;
  #outcomeSettled = false;
  #pumping = false;
  /**
   * The call's datagram flow when its responses may arrive as datagrams, else undefined. The flow
   * connects once HEADERS (or the first frame that implies them) was read; until then it buffers.
   */
  #flow;
  /** Receivers waiting for a frame. */
  #demand = 0;
  #headersWanted = false;
  #sawAnswer = false;
  #sawHeaders = false;
  #sawMessage = false;
  /** Settles on every change a waiter may care about. */
  #changed = deferred();
  #resolveStatus;
  #rejectStatus;
  #resolveHeaders;
  #rejectHeaders;
  #headers;
  #onResponseHeaders;
  #onResponseMetadata;
  /** Whether the headers were handed to onResponseHeaders. */
  #headersReported = false;

  /** The final status, or the error that ended the call. */
  status = new Promise((resolve, reject) => {
    this.#resolveStatus = resolve;
    this.#rejectStatus = reject;
  });

  constructor(call, rpc, { streamsRequest = false, options = {} } = {}) {
    this.#call = call;
    this.#rpc = rpc;
    this.#streamsRequest = streamsRequest;
    this.#onResponseHeaders = options.onResponseHeaders;
    this.#onResponseMetadata = options.onResponseMetadata;
    this.#flow = rpc.datagrams?.receive ? call.flow : undefined;
    this.status.catch(() => {});
    this.#headers = new Promise((resolve, reject) => {
      this.#resolveHeaders = resolve;
      this.#rejectHeaders = reject;
    });
    this.#headers.catch(() => {});
    call.signal.addEventListener(
      "abort",
      () => {
        if (!this.#failingInOrder) {
          this.#settleFailure(call.signal.reason);
        }
      },
      { once: true },
    );
    if (call.signal.aborted) {
      this.#settleFailure(call.signal.reason);
    } else {
      this.#kick();
    }
  }

  /**
   * The response headers, empty when the server sent none. Settles when the first frame after
   * ANSWER arrives, or rejects with the error that ended the call first.
   */
  get headers() {
    this.#headersWanted = true;
    this.#kick();
    return this.#headers;
  }

  /**
   * Sends one request message. Fails with the server's final status if the server ended the
   * call, as soon as it is known; with Cancelled if that status is OK; and with the error that
   * ended the call otherwise.
   */
  async send(message) {
    const body = marshalMessage(this.#rpc.requestType, message);
    try {
      await this.#call.sendMessage(body);
    } catch (error) {
      throw await this.#explainUploadFailure(error);
    }
  }

  /**
   * Closes the request stream while keeping the response stream readable. Resolves if the server
   * already ended the call: its final status is reported by recv().
   */
  async closeSend() {
    try {
      await this.#call.closeSend();
    } catch (error) {
      const reason = await this.#explainUploadFailure(error);
      if (this.#terminal?.status === undefined) {
        throw reason;
      }
    }
  }

  /**
   * Sends every message of requests, an iterable or async iterable, then closes the request
   * stream. If the server ends the call first, the rest is not sent and nothing is thrown: the
   * response side reports the outcome. A send that fails otherwise rejects as send() does, and
   * requests that throw cancel the call with that error and reject with it.
   */
  async sendAll(requests) {
    let sendFailure;
    try {
      for await (const request of requests) {
        try {
          await this.send(request);
        } catch (error) {
          if (this.#terminal?.status !== undefined) {
            return; // the server ended the call; the response side reports how
          }
          sendFailure = error;
          throw error;
        }
      }
    } catch (error) {
      if (error !== sendFailure) {
        await this.close(error);
      }
      throw error;
    }
    await this.closeSend();
  }

  /** Receives the next response message, or undefined after the final OK status. */
  recv() {
    const next = this.#reads.then(() => this.#recv());
    this.#reads = next.catch(() => {});
    return next;
  }

  /** Cancels the call unless it already finished. */
  async close(reason) {
    if (this.#terminal === undefined || this.#queue.length > 0) {
      this.#call.cancel(reason);
      this.#settleFailure(this.#call.signal.reason);
    }
  }

  [Symbol.asyncIterator]() {
    return {
      next: async () => {
        const value = await this.recv();
        return value === undefined ? { done: true, value: undefined } : { done: false, value };
      },
      return: async () => {
        await this.close();
        return { done: true, value: undefined };
      },
    };
  }

  async #recv() {
    const item = await this.#take();
    if (item.payload !== undefined) {
      try {
        return unmarshalMessage(this.#rpc.responseType, item.payload);
      } catch (error) {
        throw this.#settleFailure(this.#call.fail(error));
      }
    }
    if (item.error !== undefined) {
      throw item.error;
    }
    return undefined;
  }

  /** Takes the next queued message, or the end of the response once the queue is empty. */
  async #take() {
    for (;;) {
      if (this.#failure !== undefined) {
        throw this.#failure;
      }
      const payload = this.#queue.shift();
      if (payload !== undefined) {
        this.#queuedBytes -= payload.byteLength;
        // Room for more read-ahead.
        this.#kick();
        return { payload };
      }
      if (this.#terminal !== undefined) {
        // A failure of the stream itself surfaces here, in order, after the responses before it.
        if (this.#terminal.status === undefined) {
          throw this.#settleFailure(this.#terminal.error);
        }
        return this.#terminal;
      }
      const changed = this.#changed.promise;
      this.#demand += 1;
      this.#kick();
      try {
        await changed;
      } finally {
        this.#demand -= 1;
      }
    }
  }

  /** Starts reading the response unless it is running or has nothing to read for. */
  #kick() {
    if (!this.#pumping && this.#wantsRead()) {
      this.#pumping = true;
      // The pump handles the errors of the response itself; anything else is a bug that ends the call.
      this.#pump().catch((error) => this.#settleFailure(this.#call.fail(error)));
    }
  }

  #wantsRead() {
    if (this.#terminal !== undefined || this.#failure !== undefined) {
      return false;
    }
    if (this.#demand > 0 && this.#queue.length === 0) {
      return true;
    }
    if (this.#headersWanted && !this.#sawHeaders && !this.#sawMessage) {
      return true;
    }
    // A call with a datagram flow always reads ahead, so datagrams are released after HEADERS
    // even if nobody receives.
    return (
      ((this.#streamsRequest && this.#call.uploadOpen) || this.#flow !== undefined) &&
      this.#queue.length < ReadAheadFrames &&
      this.#queuedBytes < ReadAheadBytes
    );
  }

  /** Reads response events into the queue for as long as someone wants them. */
  async #pump() {
    try {
      while (this.#wantsRead()) {
        let event;
        try {
          event = await this.#readEvent();
        } catch (error) {
          this.#endWithError(error);
          return;
        }
        if (this.#failure !== undefined) {
          return;
        }
        if (event.type === FrameType.Message) {
          this.#queue.push(event.payload);
          this.#queuedBytes += event.payload.byteLength;
        } else {
          this.#endWithStatus(event.status);
        }
        this.#notify();
      }
    } finally {
      this.#pumping = false;
      this.#notify();
    }
  }

  /** The final status arrived: it is the outcome, and it ends the upload. */
  #endWithStatus({ code, message, metadata }) {
    if (code === Code.Ok && this.#onResponseMetadata !== undefined) {
      try {
        this.#onResponseMetadata(metadata);
      } catch (error) {
        this.#endWithError(error);
        return;
      }
    }
    const error = code === Code.Ok ? undefined : statusError(code, message, metadata);
    this.#terminal = { status: { code, message, metadata }, error };
    this.#call.finish(error);
    if (error === undefined) {
      this.#outcomeSettled = true;
      this.#resolveStatus({ code, message, metadata });
    } else {
      this.#rejectOutcome(error);
    }
  }

  /**
   * The response stream failed or ended without a final status. Responses read before are still
   * delivered; the error surfaces when recv() reaches it. The call is torn down now, so sends
   * fail with this error.
   */
  #endWithError(error) {
    if (this.#call.signal.aborted) {
      return; // an abort already decided the outcome
    }
    this.#failingInOrder = true;
    const reason = this.#call.fail(error);
    this.#failingInOrder = false;
    this.#terminal = { error: reason };
    this.#rejectOutcome(reason);
    this.#notify();
  }

  /** Settles the headers on their first frame, and hands them to onResponseHeaders once. */
  #settleHeaders(metadata) {
    this.#resolveHeaders(metadata);
    if (!this.#headersReported) {
      this.#headersReported = true;
      this.#onResponseHeaders?.(metadata);
    }
  }

  /**
   * Reads the next MESSAGE or STATUS event, consuming ANSWER and HEADERS on the way. Enforces the
   * response grammar `[ANSWER] [HEADERS] MESSAGE* STATUS`, and that an optimistic call's response
   * starts with ANSWER or STATUS. A ZSTD_MESSAGE or ZSTD_STREAM_MESSAGE is a MESSAGE,
   * decompressed. ANSWER is not the headers: they settle with the first frame after it.
   */
  async #readEvent() {
    for (;;) {
      const frame = await this.#call.readFrame();
      if (frame === null) {
        throw this.#call.uploadFailure ?? internal("response stream ended before final status");
      }

      if (
        frame.type !== FrameType.Status &&
        frame.type !== FrameType.Answer &&
        this.#call.replayable
      ) {
        throw internal("optimistic call answered without an ANSWER frame");
      }
      switch (frame.type) {
        case FrameType.Answer: {
          if (this.#sawAnswer || this.#sawHeaders || this.#sawMessage) {
            throw internal(
              this.#sawAnswer
                ? "duplicate ANSWER frame"
                : `ANSWER frame after ${this.#sawMessage ? "MESSAGE" : "HEADERS"}`,
            );
          }
          this.#sawAnswer = true;
          this.#call.learnAnswers(unmarshalMessage(RpcAnswer, frame.payload));
          continue;
        }
        case FrameType.Headers: {
          if (this.#sawHeaders || this.#sawMessage) {
            throw internal(
              this.#sawHeaders ? "duplicate HEADERS frame" : "HEADERS frame after MESSAGE",
            );
          }
          const headers = unmarshalMessage(RpcHeaders, frame.payload);
          validateResponseMetadata(headers.metadata, "headers");
          this.#sawHeaders = true;
          this.#settleHeaders(headers.metadata);
          this.#flow?.connect(this.#onDatagram);
          continue;
        }
        case FrameType.Message:
        case FrameType.ZstdMessage:
        case FrameType.ZstdStreamMessage: {
          const payload = this.#call.receivedMessage(frame);
          this.#sawMessage = true;
          this.#settleHeaders({});
          this.#flow?.connect(this.#onDatagram);
          return { type: FrameType.Message, payload };
        }
        case FrameType.Status: {
          const status = unmarshalMessage(RpcStatus, frame.payload);
          if (status.refusedStreamCompression && this.#call.replayable) {
            // The server refused the optimistic call before running anything for it: the call
            // continues on a replay, and its response is read from there.
            // A call with a datagram flow is never optimistic, but its replay, on a session
            // that may differ after a reconnect, can get one.
            await this.#call.replay();
            this.#flow = this.#rpc.datagrams?.receive ? this.#call.flow : undefined;
            continue;
          }
          validateResponseMetadata(status.metadata, "trailing metadata");
          this.#settleHeaders({});
          this.#flow?.connect(this.#onDatagram);
          return { type: FrameType.Status, status };
        }
        default:
          throw internal(`unexpected frame type ${frame.type} on a response stream`);
      }
    }
  }

  /**
   * Queues a response message that arrived as a datagram, in arrival order with the stream's
   * messages. It never blocks: a message is dropped when the call ended or its queue is full.
   */
  #onDatagram = (payload) => {
    if (
      this.#terminal !== undefined ||
      this.#failure !== undefined ||
      payload.byteLength > this.#call.maxFrameSize ||
      this.#queue.length >= ReadAheadFrames ||
      this.#queuedBytes + payload.byteLength > ReadAheadBytes
    ) {
      return;
    }
    this.#queue.push(payload);
    this.#queuedBytes += payload.byteLength;
    this.#notify();
  };

  /** Ends the call out of order: receivers fail with `error` without draining the queue. */
  #settleFailure(error) {
    this.#failure ??= error;
    this.#rejectOutcome(error);
    this.#notify();
    return this.#failure;
  }

  #rejectOutcome(error) {
    if (!this.#outcomeSettled) {
      this.#outcomeSettled = true;
      this.#rejectStatus(error);
      this.#rejectHeaders(error);
    }
  }

  /** Wakes everything waiting for a change of the response. */
  #notify() {
    const { resolve } = this.#changed;
    this.#changed = deferred();
    resolve();
  }

  /**
   * Resolves when the response outcome is known, the call failed, or no read is in progress, which
   * is when nothing more will be learned without the caller receiving.
   */
  async #outcomeKnown() {
    while (this.#terminal === undefined && this.#failure === undefined && this.#pumping) {
      await this.#changed.promise;
    }
  }

  /**
   * The error a failed request-side operation reports. A request side that the transport failed
   * on its own, typically because the server stopped reading it, says little: the server's
   * final status, or the failure of the response stream, explains it, so those are awaited.
   */
  async #explainUploadFailure(error) {
    if (error !== this.#call.uploadFailure) {
      return error;
    }
    await this.#outcomeKnown();
    if (this.#terminal?.status !== undefined) {
      return this.#terminal.error ?? cancelled(EarlyOk);
    }
    if (this.#call.signal.aborted) {
      return this.#call.signal.reason;
    }
    return this.#terminal?.error ?? error;
  }
}

/** A client-streaming RPC: send requests, then close and receive the single response. */
export class ClientStreamingCall {
  #call;

  constructor(call) {
    this.#call = call;
  }

  /** Sends one request message. */
  send(message) {
    return this.#call.send(message);
  }

  /** Sends every message of requests, then closes the request stream; see the bidirectional call. */
  sendAll(requests) {
    return this.#call.sendAll(requests);
  }

  /** Closes the request stream and returns the final response. */
  async closeAndRecv() {
    this.#call.closeSend().catch(() => {});
    return await receiveSingleResponse(this.#call, "client-streaming");
  }

  /** Cancels the call unless it already finished. */
  close(reason) {
    return this.#call.close(reason);
  }
}

/** The response stream of a server-streaming RPC. */
export class ResponseStream {
  #call;

  constructor(call) {
    this.#call = call;
  }

  /** The final status after the stream ends, or the error that ended it. */
  get status() {
    return this.#call.status;
  }

  /**
   * The response headers, empty when the server sent none. Settles when the first frame after
   * ANSWER arrives, even before the first message is received, or rejects with the call error.
   */
  get headers() {
    return this.#call.headers;
  }

  /** Cancels the call unless it already finished. */
  close(reason) {
    return this.#call.close(reason);
  }

  [Symbol.asyncIterator]() {
    return this.#call[Symbol.asyncIterator]();
  }
}

function deferred() {
  let resolve;
  const promise = new Promise((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function requestFields(rpc, kind) {
  return { method: rpc.path, kind };
}

/** The full name of a protobuf.js message type, without the leading dot. */
function fullName(type) {
  return type.fullName.replace(/^\./u, "");
}

function validateResponseMetadata(metadata, what) {
  try {
    validateMetadata(metadata);
  } catch (error) {
    throw internal(`invalid response ${what}: ${error.statusMessage ?? error.message}`);
  }
}
