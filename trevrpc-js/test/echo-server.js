// The Echo service of interop/README.md, served over fake streams. Behavior must match what the
// Go and Rust interop servers do.
import { root } from "../build/interop/gen/interop.trevrpc.js";
import { Code } from "../src/status.js";

const EchoRequest = root.lookupType("trevrpc.interop.v1.EchoRequest");
const EchoResponse = root.lookupType("trevrpc.interop.v1.EchoResponse");

/** Serves Echo streams. Tests can replace the unary implementation to simulate a faulty server. */
export function createEchoHandler({ unary = echoUnary } = {}) {
  return async (stream) => {
    const request = await stream.readRequest();
    await serve(stream, request, unary);
  };
}

export const echoHandler = createEchoHandler();

async function serve(stream, request, unary) {
  switch (request.method) {
    case "Unary":
      await unary(stream, request);
      break;
    case "ClientStream":
      await clientStream(stream);
      break;
    case "ServerStream":
      await serverStream(stream, EchoRequest.decode(request.body));
      break;
    case "Bidi":
      await bidi(stream);
      break;
    default:
      await stream.writeStatus(Code.Unimplemented, request.method);
  }
  await stream.finish();
  await stream.stopSending();
}

async function echoUnary(stream, request) {
  const message = EchoRequest.decode(request.body);
  if (message.failCode !== 0) {
    await stream.writeResponse({ status: message.failCode, message: message.failMessage });
    return;
  }
  const suffix = new TextDecoder().decode(request.metadata["interop-suffix"] ?? new Uint8Array());
  const body = EchoResponse.encode({ message: message.message + suffix, index: 0 }).finish();
  await stream.writeResponse({ body });
}

async function clientStream(stream) {
  let message = "";
  let count = 0;
  for (let request; (request = await stream.readMessage(EchoRequest)) !== null; count += 1) {
    if (request.failCode !== 0) {
      await stream.writeStatus(request.failCode, request.failMessage);
      return;
    }
    message += request.message;
  }
  await stream.writeMessage(EchoResponse, { message, index: count });
  await stream.writeStatus();
}

async function serverStream(stream, request) {
  for (let index = 0; index < request.count; index += 1) {
    await stream.writeMessage(EchoResponse, { message: request.message, index });
  }
  await stream.writeStatus(request.failCode, request.failMessage);
}

async function bidi(stream) {
  for (
    let index = 0, request;
    (request = await stream.readMessage(EchoRequest)) !== null;
    index += 1
  ) {
    if (request.failCode !== 0) {
      await stream.writeStatus(request.failCode, request.failMessage);
      return;
    }
    await stream.writeMessage(EchoResponse, { message: request.message, index });
  }
  await stream.writeStatus();
}
