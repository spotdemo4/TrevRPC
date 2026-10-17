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

const text = new TextDecoder();

async function serve(stream, request, unary) {
  // The interop-header rule: echo the value as response header and, on success, as trailer.
  const marker = request.metadata["interop-header"];
  const trailers = marker === undefined ? {} : { "interop-trailer": new Uint8Array(marker) };
  if (marker !== undefined) {
    await stream.writeHeaders({ "interop-header": new Uint8Array(marker) });
  }
  const done = (code = Code.Ok, message = "") =>
    stream.writeStatus(code, message, code === Code.Ok ? trailers : {});

  switch (request.method) {
    case "Unary":
      await unary(stream, request, await stream.readMessage(EchoRequest), done);
      break;
    case "ClientStream":
      await clientStream(stream, done);
      break;
    case "ServerStream":
      await serverStream(stream, await stream.readMessage(EchoRequest), done);
      break;
    case "Bidi":
      await bidi(stream, done);
      break;
    default:
      await stream.writeStatus(Code.Unimplemented, request.method);
  }
  await stream.finish();
  await stream.stopSending();
}

async function echoUnary(stream, request, message, done) {
  if (message.failCode !== 0) {
    await done(message.failCode, message.failMessage);
    return;
  }
  const suffix = text.decode(request.metadata["interop-suffix"] ?? new Uint8Array());
  await stream.writeMessage(EchoResponse, { message: message.message + suffix, index: 0 });
  await done();
}

async function clientStream(stream, done) {
  let message = "";
  let count = 0;
  for (let request; (request = await stream.readMessage(EchoRequest)) !== null; count += 1) {
    if (request.failCode !== 0) {
      await done(request.failCode, request.failMessage);
      return;
    }
    message += request.message;
  }
  await stream.writeMessage(EchoResponse, { message, index: count });
  await done();
}

async function serverStream(stream, request, done) {
  for (let index = 0; index < request.count; index += 1) {
    await stream.writeMessage(EchoResponse, { message: request.message, index });
  }
  await done(request.failCode, request.failMessage);
}

async function bidi(stream, done) {
  for (
    let index = 0, request;
    (request = await stream.readMessage(EchoRequest)) !== null;
    index += 1
  ) {
    if (request.failCode !== 0) {
      await done(request.failCode, request.failMessage);
      return;
    }
    await stream.writeMessage(EchoResponse, { message: request.message, index });
  }
  await done();
}
