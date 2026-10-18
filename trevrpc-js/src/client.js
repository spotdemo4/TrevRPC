import { Call, defaultCallOptions, mergeCallOptions } from "./call.js";
import { FrameType, marshalMessage, unmarshalMessage } from "./framing.js";
import { validateMetadata } from "./metadata.js";
import { Code, cancelled, internal, statusError } from "./status.js";
import { RpcHeaders, RpcKind, RpcStatus } from "./wire.js";

/** Creates a service client from a generated service descriptor. */
export function createServiceClient(transport, service, root, options = {}) {
  const defaults = mergeCallOptions(defaultCallOptions(), options);
  const client = {};

  for (const [jsName, method] of Object.entries(service.methods)) {
    const rpc = {
      // The wire method path, computed once per method rather than per call.
      path: `/${service.fullName}/${method.name}`,
      requestType: root.lookupType(method.inputType),
      responseType: root.lookupType(method.outputType),
    };
    const resolve = (override) => mergeCallOptions(defaults, override);

    switch (method.kind) {
      case "unary":
        client[jsName] = async (request, override) =>
          (await unaryCall(transport, rpc, request, resolve(override))).message;
        client[`${jsName}WithResponse`] = (request, override) =>
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

/** Calls a unary RPC and returns the decoded response with its headers and trailing metadata. */
export async function unaryCall(transport, rpc, request, options) {
  const body = marshalMessage(rpc.requestType, request);
  const call = await Call.open(transport, requestFields(rpc, RpcKind.Unary), body, options);
  const stream = new BidirectionalStreamingCall(call, rpc);
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
  );
  const stream = new BidirectionalStreamingCall(call, rpc);
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
  );
  return new ClientStreamingCall(
    new BidirectionalStreamingCall(call, rpc, { streamsRequest: true }),
  );
}

/** Calls a bidirectional-streaming RPC. */
export async function bidirectionalStreamingCall(transport, rpc, options) {
  const call = await Call.open(
    transport,
    requestFields(rpc, RpcKind.BidirectionalStreaming),
    undefined,
    options,
  );
  return new BidirectionalStreamingCall(call, rpc, { streamsRequest: true });
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
  const status = await stream.status;
  return { message, metadata: status.metadata, headers: await stream.headers };
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

const EarlyOk = "the server completed the call before the request stream ended";

/**
 * A bidirectional-streaming RPC. Messages are received with recv() or async iteration. The call
 * ends with the server's final status: a non-OK status makes recv() throw it, and `status`
 * settles with the final status or the error that ended the call. `headers` resolves once the
 * first response frame arrived; reading it starts that read, so headers are available before the
 * first message.
 *
 * While the request side is open, the response is read ahead of recv() (see ReadAheadFrames), so
 * a final status reaches a caller that is only sending. It stops the upload at once: pending and
 * later sends fail with the non-OK status, or with Cancelled if the status is OK. A call that
 * does not stream its request reads only what recv() or `headers` ask for.
 *
 * The final status settles the outcome as soon as it is read. Responses read before it are still
 * delivered in order, and the call's deadline and abort signal no longer apply to it.
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
  /** Receivers waiting for a frame. */
  #demand = 0;
  #headersWanted = false;
  #sawHeaders = false;
  #sawMessage = false;
  /** Settles on every change a waiter may care about. */
  #changed = deferred();
  #resolveStatus;
  #rejectStatus;
  #resolveHeaders;
  #rejectHeaders;
  #headers;

  /** The final status, or the error that ended the call. */
  status = new Promise((resolve, reject) => {
    this.#resolveStatus = resolve;
    this.#rejectStatus = reject;
  });

  constructor(call, rpc, { streamsRequest = false } = {}) {
    this.#call = call;
    this.#rpc = rpc;
    this.#streamsRequest = streamsRequest;
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
   * The response headers, empty when the server sent none. Settles when the first response frame
   * arrives, or rejects with the error that ended the call first.
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
    return (
      this.#streamsRequest &&
      this.#call.uploadOpen &&
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
    const error = code === Code.Ok ? undefined : statusError(code, message, metadata);
    this.#terminal = { status: { code, message, metadata }, error };
    this.#call.finish(error ?? cancelled(EarlyOk));
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

  /**
   * Reads the next MESSAGE or STATUS event, consuming HEADERS on the way. Enforces the response
   * grammar `[HEADERS] MESSAGE* STATUS`.
   */
  async #readEvent() {
    for (;;) {
      const frame = await this.#call.readFrame();
      if (frame === null) {
        throw this.#call.uploadFailure ?? internal("response stream ended before final status");
      }

      switch (frame.type) {
        case FrameType.Headers: {
          if (this.#sawHeaders || this.#sawMessage) {
            throw internal(
              this.#sawHeaders ? "duplicate HEADERS frame" : "HEADERS frame after MESSAGE",
            );
          }
          const headers = unmarshalMessage(RpcHeaders, frame.payload);
          validateResponseMetadata(headers.metadata, "headers");
          this.#sawHeaders = true;
          this.#resolveHeaders(headers.metadata);
          continue;
        }
        case FrameType.Message:
          this.#sawMessage = true;
          this.#resolveHeaders({});
          return frame;
        case FrameType.Status: {
          const status = unmarshalMessage(RpcStatus, frame.payload);
          validateResponseMetadata(status.metadata, "trailing metadata");
          this.#resolveHeaders({});
          return { type: FrameType.Status, status };
        }
        default:
          throw internal(`unexpected frame type ${frame.type} on a response stream`);
      }
    }
  }

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

  /** Closes the request stream and returns the final response. */
  async closeAndRecv() {
    return (await this.closeAndRecvWithResponse()).message;
  }

  /** Closes the request stream and returns the final response with its headers and trailing metadata. */
  async closeAndRecvWithResponse() {
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
   * The response headers, empty when the server sent none. Settles when the first response frame
   * arrives, even before the first message is received, or rejects with the call error.
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

function validateResponseMetadata(metadata, what) {
  try {
    validateMetadata(metadata);
  } catch (error) {
    throw internal(`invalid response ${what}: ${error.statusMessage ?? error.message}`);
  }
}
