import { Call, EarlyOk, defaultCallOptions, mergeCallOptions } from "./call.js";
import { FrameType, decodeWireMessage, marshalMessage, unmarshalMessage } from "./framing.js";
import { interceptCall, interceptorList } from "./interceptor.js";
import { validateMetadata } from "./metadata.js";
import { methodSettings } from "./method-options.js";
import { Code, cancelled, internal, invalidArgument, statusError } from "./status.js";
import { requestValidation } from "./validator.js";
import { RpcAnswer, RpcHeaders, RpcKind, RpcStatus } from "./wire.js";

/**
 * Creates a client for `service`, a service descriptor that protoc-gen-es generates, with one
 * function per method, named by the method's `localName`. Its calls run the interceptors of the
 * transport, then those of `options.interceptors`, `options.validator` checks the request
 * messages they send, and `options.codecs` encode and decode the messages of their types.
 */
export function createClient(service, transport, options = {}) {
  if (service?.kind !== "service" || !Array.isArray(service.methods)) {
    throw invalidArgument("createClient needs a protobuf-es service descriptor");
  }
  const { interceptors: clientInterceptors, validator, codecs, ...callOptions } = options;
  const codecOf = codecsBySchema(codecs);
  const defaults = mergeCallOptions(defaultCallOptions(), callOptions);
  const interceptors = [
    ...interceptorList(transport.interceptors, "transport interceptors"),
    ...interceptorList(clientInterceptors, "interceptors"),
  ];
  const client = {};

  for (const method of service.methods) {
    const settings = methodSettings(method);
    const rpc = {
      // The wire method path, computed once per method rather than per call.
      path: `/${service.typeName}/${method.name}`,
      requestType: method.input,
      responseType: method.output,
      // The generated codecs of the request and response types, or undefined for protobuf-es.
      requestCodec: codecOf.get(method.input),
      responseCodec: codecOf.get(method.output),
      // Checks each request message once it is encoded, or undefined to send them unchecked.
      validate: requestValidation(validator, method.input),
      // Full protobuf names, which pick the compression dictionaries.
      types: { request: method.input.typeName, response: method.output.typeName },
      datagrams: datagramUse(settings),
      compressStream: streamCompressionUse(settings),
      compress: plainCompressionUse(settings),
    };
    const resolve = (override) => mergeCallOptions(defaults, override);
    const shape = MethodShapes[settings.kind];
    const info = {
      method: rpc.path,
      service: service.typeName,
      name: method.name,
      kind: settings.kind,
      descriptor: method,
    };
    const call =
      interceptors.length === 0
        ? async (request, override) => shape.start(transport, rpc, request, resolve(override))
        : async (request, override) =>
            interceptCall(
              interceptors,
              info,
              resolve(override),
              (options) => shape.start(transport, rpc, request, options),
              shape.ended,
            );
    client[method.localName] = shape.takesRequest ? call : (override) => call(undefined, override);
  }

  return client;
}

/**
 * The codecs of the `codecs` client option, by message type. A codec is used only for the very
 * schema it was generated with: the one its `_trevrpc.js` imports from the `_pb.js` the service
 * comes from. Of several codecs of one type, the first is used.
 */
function codecsBySchema(codecs) {
  const bySchema = new Map();
  if (codecs == null) {
    return bySchema;
  }
  if (typeof codecs[Symbol.iterator] !== "function" || typeof codecs === "string") {
    throw invalidArgument("codecs must be an array of message codecs");
  }
  for (const codec of codecs) {
    if (
      codec?.schema?.kind !== "message" ||
      typeof codec.encode !== "function" ||
      typeof codec.decode !== "function"
    ) {
      throw invalidArgument(
        "codecs must hold the message codecs that protoc-gen-trevrpc-js generates",
      );
    }
    if (!bySchema.has(codec.schema)) {
      bySchema.set(codec.schema, codec);
    }
  }
  return bySchema;
}

/**
 * How each kind of RPC is called: whether its client method takes a request message, how it
 * starts, and, for streaming calls, what settles when the call ends after it started.
 */
const MethodShapes = Object.freeze({
  __proto__: null,
  unary: {
    takesRequest: true,
    start: unaryCall,
  },
  serverStreaming: {
    takesRequest: true,
    start: serverStreamingCall,
    ended: (stream) => stream.status,
  },
  clientStreaming: {
    takesRequest: false,
    start: (transport, rpc, _request, options) => clientStreamingCall(transport, rpc, options),
    ended: (call) => call.status,
  },
  bidirectionalStreaming: {
    takesRequest: false,
    start: (transport, rpc, _request, options) =>
      bidirectionalStreamingCall(transport, rpc, options),
    ended: (call) => call.status,
  },
});

/** Calls a unary RPC and returns the decoded response. */
export async function unaryCall(transport, rpc, request, options) {
  const body = marshalMessage(rpc.requestType, request, rpc.validate, rpc.requestCodec);
  const call = await Call.open(transport, requestFields(rpc, RpcKind.Unary), body, options, rpc);
  const stream = new BidirectionalStreamingCall(call, rpc, { options });
  // The response decides the outcome, so the request side finishes in the background.
  stream.closeSend().catch(() => {});
  return await receiveSingleResponse(stream, "unary");
}

/** Calls a server-streaming RPC and returns its response stream. */
export async function serverStreamingCall(transport, rpc, request, options) {
  const body = marshalMessage(rpc.requestType, request, rpc.validate, rpc.requestCodec);
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

/** What acceptFrame() returns for the refusal of an optimistic call, which the caller replays. */
const Replay = Symbol("replay");

/**
 * How a method uses datagrams, or undefined if it does not: `send` for request messages, which
 * needs a streaming request and `unreliableRequests`, and `receive` for response messages, which
 * every call with a streaming response accepts. A method needs a flow if it marks either
 * direction unreliable; methodSettings rejects marks on a direction that does not stream.
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
 * those its settings mark (methodSettings leaves out directions that do not stream and message
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
 * responses }`, or undefined for none: those its settings mark (methodSettings marks both for
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
 * recvAvailable() and batches() receive in batches: every response message that is available
 * without waiting, in one array. They are for callers that process messages in bulk, such as a
 * view that renders a stream, and avoid the promise turns that receiving each message costs.
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
  /** Receivers of a batch waiting for messages, for which the pump reads all it has buffered. */
  #batching = 0;
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
   * ended the call otherwise. A message that cannot be encoded, or that the client's validator
   * rejects, fails with InvalidArgument (or the validator's status) and is not sent; the call
   * goes on.
   */
  async send(message) {
    const body = marshalMessage(
      this.#rpc.requestType,
      message,
      this.#rpc.validate,
      this.#rpc.requestCodec,
    );
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
    return this.#receive(() => this.#recv());
  }

  /**
   * Receives every response message available without waiting as an array, in order, or
   * undefined after the final OK status. It waits for the first message if there is none, so an
   * array is never empty. The array holds the messages queued by read-ahead or as datagrams, and
   * those whose frames the transport has delivered in full by the time the first is there, up to
   * ReadAheadBytes (plus one frame). The messages come out in the order that recv() returns
   * them, and the two may be mixed.
   *
   * The final status, and the error of a stream that failed, are reported by the call after the
   * array of the messages before them, so that no message is lost: a message that cannot be
   * decoded fails the call, and the messages before it come in an array first.
   */
  recvAvailable() {
    return this.#receive(() => this.#recvBatch());
  }

  /**
   * Serializes receives and waits for the one before; a receive that fails does not fail those
   * behind it, which find the same failure themselves.
   */
  #receive(receive) {
    const next = this.#reads.then(receive);
    this.#reads = next.catch(() => {});
    return next;
  }

  /**
   * Cancels the call unless it already finished. A call that finished releases its response
   * stream, if the server has not ended it yet, without changing the outcome.
   */
  async close(reason) {
    if (this.#terminal === undefined || this.#queue.length > 0) {
      this.#call.cancel(reason);
      this.#settleFailure(this.#call.signal.reason);
    } else {
      this.#call.releaseResponse();
    }
  }

  [Symbol.asyncIterator]() {
    return this.#iterator(() => this.recv());
  }

  /**
   * Iterates over the arrays that recvAvailable() returns, until the final OK status. Leaving the
   * loop early cancels the call.
   */
  batches() {
    return this.#iterator(() => this.recvAvailable());
  }

  /** An async iterator over what `receive` returns until it returns undefined. */
  #iterator(receive) {
    return {
      next: async () => {
        const value = await receive();
        return value === undefined ? { done: true, value: undefined } : { done: false, value };
      },
      return: async () => {
        await this.close();
        return { done: true, value: undefined };
      },
      [Symbol.asyncIterator]() {
        return this;
      },
    };
  }

  async #recv() {
    const item = await this.#take();
    if (item.payload !== undefined) {
      return this.#decode(item.payload);
    }
    if (item.error !== undefined) {
      throw item.error;
    }
    return undefined;
  }

  async #recvBatch() {
    this.#batching += 1;
    try {
      if (!this.#pumping) {
        // Nobody is reading the response, so its buffered frames can be read right here.
        this.#readBuffered();
      }
      const item = await this.#take(true);
      if (item.payloads !== undefined) {
        return this.#decodeBatch(item.payloads);
      }
      if (item.error !== undefined) {
        throw item.error;
      }
      return undefined;
    } finally {
      this.#batching -= 1;
    }
  }

  /** Decodes a response message; one that does not decode fails the call. */
  #decode(payload) {
    try {
      return unmarshalMessage(this.#rpc.responseType, payload, this.#rpc.responseCodec);
    } catch (error) {
      throw this.#settleFailure(this.#call.fail(error));
    }
  }

  /**
   * Decodes a batch of response messages. A message that does not decode fails the call, and the
   * messages before it are the batch, as the failure is reported by the next receive.
   */
  #decodeBatch(payloads) {
    const messages = [];
    for (const payload of payloads) {
      try {
        messages.push(this.#decode(payload));
      } catch (error) {
        if (messages.length === 0) {
          throw error;
        }
        break;
      }
    }
    return messages;
  }

  /**
   * Takes the next queued message, or the end of the response once the queue is empty. With
   * `batch`, takes all queued messages at once, as `payloads`.
   */
  async #take(batch = false) {
    for (;;) {
      if (this.#failure !== undefined) {
        throw this.#failure;
      }
      if (batch && this.#queue.length > 0) {
        const payloads = this.#queue;
        this.#queue = [];
        this.#queuedBytes = 0;
        // Room for more read-ahead.
        this.#kick();
        return { payloads };
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
        this.#enqueue(event);
        if (this.#batching > 0) {
          this.#readBuffered();
        }
        this.#notify();
      }
    } finally {
      this.#pumping = false;
      this.#notify();
    }
  }

  /** Queues a message event, or ends the response with a status event. */
  #enqueue(event) {
    if (event.type === FrameType.Message) {
      this.#queue.push(event.payload);
      this.#queuedBytes += event.payload.byteLength;
    } else {
      this.#endWithStatus(event.status);
    }
  }

  /**
   * Reads the events whose frames were received in full already, without waiting, and queues
   * them as the pump does, up to ReadAheadBytes: the bytes are in memory anyway, and a receiver
   * of batches takes them all at once, so the number of frames is not limited. A frame that is
   * not valid ends the response with its error after the events before it, as when the pump
   * reaches it. Must not run while the pump reads.
   */
  #readBuffered() {
    try {
      while (
        this.#terminal === undefined &&
        this.#failure === undefined &&
        this.#queuedBytes < ReadAheadBytes &&
        // The server's decision on an optimistic call may be a replay, which waits.
        !this.#call.replayable
      ) {
        const frame = this.#call.tryReadFrame();
        if (frame === undefined) {
          return;
        }
        const event = this.#acceptFrame(frame);
        if (event !== undefined) {
          this.#enqueue(event);
        }
      }
    } catch (error) {
      this.#endWithError(error);
    }
  }

  /** The final status arrived: it is the outcome, and it ends the upload. */
  #endWithStatus({ code, message, metadata, details }) {
    if (code === Code.Ok && this.#onResponseMetadata !== undefined) {
      try {
        this.#onResponseMetadata(metadata);
      } catch (error) {
        this.#endWithError(error);
        return;
      }
    }
    const error =
      code === Code.Ok ? undefined : statusError(code, message, metadata, undefined, details);
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
   * Reads the next MESSAGE or STATUS event, consuming ANSWER and HEADERS on the way. Frames follow
   * the grammar that acceptFrame() enforces; a refusal of an optimistic call is replayed here,
   * and the response is read from the replay.
   */
  async #readEvent() {
    for (;;) {
      const frame = await this.#call.readFrame();
      if (frame === null) {
        throw this.#call.uploadFailure ?? internal("response stream ended before final status");
      }
      const event = this.#acceptFrame(frame);
      if (event === Replay) {
        await this.#call.replay();
        // A call with a datagram flow is never optimistic, but its replay, on a session that may
        // differ after a reconnect, can get one.
        this.#flow = this.#rpc.datagrams?.receive ? this.#call.flow : undefined;
      } else if (event !== undefined) {
        return event;
      }
    }
  }

  /**
   * Applies a response frame. Enforces the response grammar `[ANSWER] [HEADERS] MESSAGE* STATUS`,
   * and that an optimistic call's response starts with ANSWER or STATUS. A ZSTD_MESSAGE or
   * ZSTD_STREAM_MESSAGE is a MESSAGE, decompressed. ANSWER is not the headers: they settle with
   * the first frame after it. Returns the MESSAGE or STATUS event the frame is, `Replay` for the
   * refusal of an optimistic call, which the caller replays before it reads on, and undefined for
   * frames that carry no event.
   */
  #acceptFrame(frame) {
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
        this.#call.learnAnswers(decodeWireMessage(RpcAnswer, frame.payload));
        return undefined;
      }
      case FrameType.Headers: {
        if (this.#sawHeaders || this.#sawMessage) {
          throw internal(
            this.#sawHeaders ? "duplicate HEADERS frame" : "HEADERS frame after MESSAGE",
          );
        }
        const headers = decodeWireMessage(RpcHeaders, frame.payload);
        validateResponseMetadata(headers.metadata, "headers");
        this.#sawHeaders = true;
        this.#settleHeaders(headers.metadata);
        this.#flow?.connect(this.#onDatagram);
        return undefined;
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
        const status = decodeWireMessage(RpcStatus, frame.payload);
        if (status.refusedStreamCompression && this.#call.replayable) {
          // The server refused the optimistic call before running anything for it: the call
          // continues on a replay, and its response is read from there.
          return Replay;
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

  /** The final status, or the error that ended the call. */
  get status() {
    return this.#call.status;
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

  /**
   * Receives every response message available without waiting as an array, or undefined after
   * the final OK status; see BidirectionalStreamingCall.recvAvailable().
   */
  recvAvailable() {
    return this.#call.recvAvailable();
  }

  /**
   * Iterates over the arrays that recvAvailable() returns, until the final OK status. Leaving the
   * loop early cancels the call.
   */
  batches() {
    return this.#call.batches();
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

function validateResponseMetadata(metadata, what) {
  try {
    validateMetadata(metadata);
  } catch (error) {
    throw internal(`invalid response ${what}: ${error.statusMessage ?? error.message}`);
  }
}
