import { marshalMessage, unmarshalMessage } from "./framing.js";
import { isStreamingServerResponse, isUnaryServerResponse } from "./server-response.js";
import { Code, statusError } from "./status.js";
import { RpcStreamFrameKind } from "./wire.js";

/** Registers typed generated handlers on a Node server. */
export function registerTypedService(server, root, service, handlers) {
  for (const [jsName, method] of Object.entries(service.methods)) {
    const handler = handlers[jsName] ?? handlers[method.name];
    if (handler == null) {
      continue;
    }
    const requestType = root.lookupType(method.inputType);
    const responseType = root.lookupType(method.outputType);
    server.register(service.fullName, method.name, method.kind, async (call) => {
      await dispatchTypedNodeHandler(call, handler, method.kind, requestType, responseType);
    });
  }
  return server;
}

/** Dispatches one typed generated Node handler. */
export async function dispatchTypedNodeHandler(call, handler, kind, requestType, responseType) {
  if (kind === "unary") {
    const request = unmarshalMessage(requestType, call.request.body ?? new Uint8Array(0));
    const response = await handler(request, call);
    const envelope = isUnaryServerResponse(response) ? response : null;
    await call.respond({
      body: marshalMessage(responseType, envelope == null ? response : envelope.message),
      metadata: envelope?.metadata ?? {},
    });
    return;
  }

  if (kind === "clientStreaming") {
    const response = await handler(decodeTypedNodeRequests(call, requestType), call);
    const envelope = isUnaryServerResponse(response) ? response : null;
    await call.sendMessage(
      marshalMessage(responseType, envelope == null ? response : envelope.message),
    );
    await call.finishStream(Code.Ok, "", envelope?.metadata ?? {});
    return;
  }

  const request =
    kind === "serverStreaming"
      ? unmarshalMessage(requestType, call.request.body ?? new Uint8Array(0))
      : decodeTypedNodeRequests(call, requestType);
  const response = await handler(request, call);
  const envelope = isStreamingServerResponse(response) ? response : null;
  await writeTypedResponseMessages(
    call,
    envelope == null ? response : envelope.messages,
    responseType,
  );
  const status = envelope?.status;
  await call.finishStream(status?.code ?? Code.Ok, status?.message ?? "", status?.metadata ?? {});
}

async function writeTypedResponseMessages(call, messages, responseType) {
  const iterator = responseIterator(messages ?? []);
  try {
    for (;;) {
      const result = await nextTypedResponseBatch(iterator, call.writeBatchMaxMessages ?? 16);
      if (result.done) {
        return;
      }
      await call.sendMany(result.value.map((message) => marshalMessage(responseType, message)));
    }
  } catch (error) {
    try {
      await iterator.return?.();
    } catch {
      // Preserve the send or iterator error, matching for-await cleanup behavior.
    }
    throw error;
  }
}

function responseIterator(messages) {
  if (typeof messages?.[Symbol.asyncIterator] === "function") {
    return messages[Symbol.asyncIterator]();
  }
  if (typeof messages?.[Symbol.iterator] === "function") {
    const iterator = messages[Symbol.iterator]();
    return {
      next: () => Promise.resolve(iterator.next()),
      return: () =>
        Promise.resolve(
          typeof iterator.return === "function"
            ? iterator.return()
            : { done: true, value: undefined },
        ),
    };
  }
  throw new TypeError("streaming handler must return an iterable or async iterable");
}

async function nextTypedResponseBatch(iterator, maxMessages) {
  if (typeof iterator.nextBatch === "function") {
    return await iterator.nextBatch(maxMessages);
  }
  const result = await iterator.next();
  return result.done ? result : { done: false, value: [result.value] };
}

async function* decodeTypedNodeRequests(call, requestType) {
  for (;;) {
    const frame = await call.recv();
    if (frame == null) {
      return;
    }
    if (frame.kind === RpcStreamFrameKind.Message) {
      yield unmarshalMessage(requestType, frame.body ?? new Uint8Array(0));
      continue;
    }
    if (frame.kind === RpcStreamFrameKind.Status) {
      const code = frame.status ?? Code.Ok;
      if (code !== Code.Ok) {
        throw statusError(code, frame.message ?? "", frame.metadata ?? {});
      }
      return;
    }
    throw statusError(Code.InvalidArgument, "request stream contained an unknown frame kind");
  }
}
