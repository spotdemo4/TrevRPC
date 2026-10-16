import { Call, defaultCallOptions, mergeCallOptions } from "./call.js";
import { marshalMessage, unmarshalMessage } from "./framing.js";
import { validateMetadata } from "./metadata.js";
import { Code, internal, statusError } from "./status.js";
import { RpcKind, RpcResponse, RpcStreamFrameKind } from "./wire.js";

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

/** Calls a unary RPC and returns the decoded response with its metadata. */
export async function unaryCall(transport, rpc, request, options) {
  const body = marshalMessage(rpc.requestType, request);
  const call = await Call.open(transport, requestHeader(rpc, RpcKind.Unary, body), options);

  let response;
  try {
    // The response decides the outcome, so the request side finishes in the background.
    call.closeSend().catch(() => {});
    response = await call.readFrame(RpcResponse);
    validateResponseMetadata(response.metadata);
  } catch (error) {
    throw call.fail(error);
  }
  call.finish();

  if (response.status !== Code.Ok) {
    throw statusError(response.status, response.message, response.metadata);
  }
  return {
    message: unmarshalMessage(rpc.responseType, response.body),
    metadata: response.metadata,
  };
}

/** Calls a server-streaming RPC and returns its response stream. */
export async function serverStreamingCall(transport, rpc, request, options) {
  const body = marshalMessage(rpc.requestType, request);
  const call = await Call.open(
    transport,
    requestHeader(rpc, RpcKind.ServerStreaming, body),
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
    requestHeader(rpc, RpcKind.ClientStreaming, new Uint8Array(0)),
    options,
  );
  return new ClientStreamingCall(new BidirectionalStreamingCall(call, rpc));
}

/** Calls a bidirectional-streaming RPC. */
export async function bidirectionalStreamingCall(transport, rpc, options) {
  const call = await Call.open(
    transport,
    requestHeader(rpc, RpcKind.BidirectionalStreaming, new Uint8Array(0)),
    options,
  );
  return new BidirectionalStreamingCall(call, rpc);
}

/**
 * A bidirectional-streaming RPC. Messages are received with recv() or async iteration. The call
 * ends with the server's final status: a non-OK status makes recv() throw it, and `status`
 * settles with the final status or the error that ended the call.
 */
export class BidirectionalStreamingCall {
  #call;
  #rpc;
  /** Serializes recv() so concurrent callers never interleave frame reads. */
  #reads = Promise.resolve();
  #settled = false;
  #error = null;
  #uploadError = null;
  #resolveStatus;
  #rejectStatus;

  /** The final status, or the error that ended the call. */
  status = new Promise((resolve, reject) => {
    this.#resolveStatus = resolve;
    this.#rejectStatus = reject;
  });

  constructor(call, rpc) {
    this.#call = call;
    this.#rpc = rpc;
    this.status.catch(() => {});
    call.signal.addEventListener("abort", () => this.#settleFailure(call.signal.reason), {
      once: true,
    });
    if (call.signal.aborted) {
      this.#settleFailure(call.signal.reason);
    }
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

    let frame;
    try {
      frame = await this.#call.readStreamFrame();
      if (frame === null) {
        throw this.#uploadError ?? internal("response stream ended before final status");
      }
      if (frame.kind === RpcStreamFrameKind.Message) {
        return unmarshalMessage(this.#rpc.responseType, frame.body);
      }
      validateResponseMetadata(frame.metadata);
    } catch (error) {
      throw this.#settleFailure(this.#call.fail(error));
    }

    // A status frame is final: the request side stops and the stream drains in the background.
    this.#call.finish();
    this.#settled = true;
    if (frame.status !== Code.Ok) {
      this.#error = statusError(frame.status, frame.message, frame.metadata);
      this.#rejectStatus(this.#error);
      throw this.#error;
    }
    this.#resolveStatus({ code: Code.Ok, message: frame.message, metadata: frame.metadata });
    return undefined;
  }

  #settleFailure(error) {
    if (!this.#settled) {
      this.#settled = true;
      this.#error = error;
      this.#rejectStatus(error);
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

  /** Closes the request stream and returns the final response with its metadata. */
  async closeAndRecvWithResponse() {
    this.#call.closeSend().catch(() => {});

    let message;
    let count = 0;
    for (;;) {
      const next = await this.#call.recv();
      if (next === undefined) {
        break;
      }
      message ??= next;
      count += 1;
    }
    if (count !== 1) {
      throw internal(`client-streaming RPC returned ${count} response messages instead of one`);
    }
    const status = await this.#call.status;
    return { message, metadata: status.metadata };
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

  /** Cancels the call unless it already finished. */
  close(reason) {
    return this.#call.close(reason);
  }

  [Symbol.asyncIterator]() {
    return this.#call[Symbol.asyncIterator]();
  }
}

function requestHeader(rpc, kind, body) {
  return { service: rpc.service, method: rpc.method, kind, body };
}

function validateResponseMetadata(metadata) {
  try {
    validateMetadata(metadata);
  } catch (error) {
    throw internal(`invalid response metadata: ${error.statusMessage ?? error.message}`);
  }
}
