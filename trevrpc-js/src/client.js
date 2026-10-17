import { Call, defaultCallOptions, mergeCallOptions } from "./call.js";
import { FrameType, marshalMessage, unmarshalMessage } from "./framing.js";
import { validateMetadata } from "./metadata.js";
import { Code, internal, statusError } from "./status.js";
import { RpcHeaders, RpcKind, RpcStatus } from "./wire.js";

/** Creates a service client from a generated service descriptor. */
export function createServiceClient(transport, service, root, options = {}) {
  const defaults = mergeCallOptions(defaultCallOptions(), options);
  const client = {};

  for (const [jsName, method] of Object.entries(service.methods)) {
    const rpc = {
      service: service.fullName,
      method: method.name,
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
  return new ClientStreamingCall(new BidirectionalStreamingCall(call, rpc));
}

/** Calls a bidirectional-streaming RPC. */
export async function bidirectionalStreamingCall(transport, rpc, options) {
  const call = await Call.open(
    transport,
    requestFields(rpc, RpcKind.BidirectionalStreaming),
    undefined,
    options,
  );
  return new BidirectionalStreamingCall(call, rpc);
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
 * A bidirectional-streaming RPC. Messages are received with recv() or async iteration. The call
 * ends with the server's final status: a non-OK status makes recv() throw it, and `status`
 * settles with the final status or the error that ended the call. `headers` resolves once the
 * first response frame arrived; reading it starts that read, so headers are available before the
 * first message.
 */
export class BidirectionalStreamingCall {
  #call;
  #rpc;
  /** Serializes recv() so concurrent callers never interleave frame reads. */
  #reads = Promise.resolve();
  #settled = false;
  #error = null;
  #uploadError = null;
  /** Response frame already read ahead for `headers`, consumed by the next recv(). */
  #peeked;
  #peekStarted = false;
  #sawHeaders = false;
  #sawMessage = false;
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

  constructor(call, rpc) {
    this.#call = call;
    this.#rpc = rpc;
    this.status.catch(() => {});
    this.#headers = new Promise((resolve, reject) => {
      this.#resolveHeaders = resolve;
      this.#rejectHeaders = reject;
    });
    this.#headers.catch(() => {});
    call.signal.addEventListener("abort", () => this.#settleFailure(call.signal.reason), {
      once: true,
    });
    if (call.signal.aborted) {
      this.#settleFailure(call.signal.reason);
    }
  }

  /**
   * The response headers, empty when the server sent none. Settles when the first response frame
   * arrives, or rejects with the error that ended the call first.
   */
  get headers() {
    this.#peek();
    return this.#headers;
  }

  /** Sends one request message. */
  async send(message) {
    const body = marshalMessage(this.#rpc.requestType, message);
    await this.#recordUploadError(this.#call.sendMessage(body));
  }

  /** Closes the request stream while keeping the response stream readable. */
  async closeSend() {
    await this.#recordUploadError(this.#call.closeSend());
  }

  /** Receives the next response message, or undefined after the final OK status. */
  recv() {
    const next = this.#reads.then(() => this.#recv());
    this.#reads = next.catch(() => {});
    return next;
  }

  /** Cancels the call unless it already finished. */
  async close(reason) {
    if (!this.#settled) {
      this.#call.cancel(reason);
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
    if (this.#error != null) {
      throw this.#error;
    }
    if (this.#settled) {
      return undefined;
    }

    let event;
    try {
      event = await this.#nextEvent();
      if (event.type === FrameType.Message) {
        return unmarshalMessage(this.#rpc.responseType, event.payload);
      }
    } catch (error) {
      throw this.#settleFailure(this.#call.fail(error));
    }

    // A status frame is final: the request side stops and the stream drains in the background.
    this.#call.finish();
    this.#settled = true;
    const { code, message, metadata } = event.status;
    if (code !== Code.Ok) {
      this.#error = statusError(code, message, metadata);
      this.#rejectStatus(this.#error);
      throw this.#error;
    }
    this.#resolveStatus({ code: Code.Ok, message, metadata });
    return undefined;
  }

  /**
   * Reads the first response frames ahead of recv(), so `headers` settles without a receive. Every
   * completed recv() has already settled `headers`, so the read-ahead only matters before one.
   */
  #peek() {
    if (this.#peekStarted) {
      return;
    }
    this.#peekStarted = true;
    const next = this.#reads.then(async () => {
      if (this.#settled || this.#sawMessage) {
        return;
      }
      try {
        this.#peeked = await this.#readEvent();
      } catch (error) {
        this.#settleFailure(this.#call.fail(error));
      }
    });
    this.#reads = next.catch(() => {});
  }

  async #nextEvent() {
    if (this.#peeked !== undefined) {
      const event = this.#peeked;
      this.#peeked = undefined;
      return event;
    }
    return await this.#readEvent();
  }

  /**
   * Reads the next MESSAGE or STATUS event, consuming HEADERS on the way. Enforces the response
   * grammar `[HEADERS] MESSAGE* STATUS`.
   */
  async #readEvent() {
    for (;;) {
      const frame = await this.#call.readFrame();
      if (frame === null) {
        throw this.#uploadError ?? internal("response stream ended before final status");
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

  #settleFailure(error) {
    if (!this.#settled) {
      this.#settled = true;
      this.#error = error;
      this.#rejectStatus(error);
      this.#rejectHeaders(error);
    }
    return this.#error;
  }

  async #recordUploadError(upload) {
    try {
      await upload;
    } catch (error) {
      this.#uploadError ??= error;
      throw error;
    }
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

function requestFields(rpc, kind) {
  return { service: rpc.service, method: rpc.method, kind };
}

function validateResponseMetadata(metadata, what) {
  try {
    validateMetadata(metadata);
  } catch (error) {
    throw internal(`invalid response ${what}: ${error.statusMessage ?? error.message}`);
  }
}
