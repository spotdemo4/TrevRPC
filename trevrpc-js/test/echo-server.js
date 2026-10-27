// The Echo service of interop/README.md, served over fake streams. Behavior must match what the
// Go and Rust interop servers do.
import { root as compressedRoot } from "../build/interop/gen/compressed.trevrpc.js";
import { root } from "../build/interop/gen/interop.trevrpc.js";
import { encodeMessage, inflateMessage } from "../src/compression.js";
import { decodeDatagram, encodeDatagram } from "../src/datagram.js";
import {
  FrameType,
  encodeHeadersFrame,
  encodeOutgoingFrame,
  marshalMessage,
} from "../src/framing.js";
import { validateMetadata } from "../src/metadata.js";
import { Code, TrevRpcError, invalidArgument } from "../src/status.js";
import { createCompression } from "../src/zstd.js";
import { ServerStreams, parseMethodPath } from "./support.js";

const EchoService = "trevrpc.interop.v1.Echo";
const UnreliableEchoService = "trevrpc.interop.v1.UnreliableEcho";
const CompressedEchoService = "trevrpc.interop.v1.CompressedEcho";
const RefusingEchoService = "trevrpc.interop.v1.RefusingEcho";
/** The server's stream compression; it holds no dictionaries. */
const compression = await createCompression();
/** Responses up to this size are sent as datagrams when the call has a flow. */
const MaxDatagramResponse = 1000;
const EchoRequest = root.lookupType("trevrpc.interop.v1.EchoRequest");
const EchoResponse = root.lookupType("trevrpc.interop.v1.EchoResponse");
const PlainMessage = compressedRoot.lookupType("trevrpc.interop.v1.PlainMessage");

/**
 * Serves Echo streams. Tests can replace the unary implementation to simulate a faulty server.
 * The handler takes the stream and, for UnreliableEcho, the session whose `datagrams` it serves.
 */
export function createEchoHandler({ unary = echoUnary } = {}) {
  return async (stream, session) => {
    const request = await stream.readRequest();
    await serve(stream, request, unary, session);
  };
}

export const echoHandler = createEchoHandler();

const text = new TextDecoder();

/** How many connection metadata sets the server keeps per session. */
const MaxConnectionMetadataSets = 4;
/** The connection metadata sets the server stored, per session, oldest first. */
const connectionMetadataSets = new WeakMap();

/**
 * Merges the connection metadata a REQUEST offers or refers to into its metadata, the call's own
 * entries winning, as real servers do before running the call. An offer is stored for the session
 * and confirmed in the call's HEADERS (see `headersAnswer`); without a session, as over the
 * ordinary HTTP mapping, nothing is stored or confirmed and a reference is unknown. Throws
 * InvalidArgument for an invalid set and for a reference to an unknown one.
 */
function resolveConnectionMetadata(request, session) {
  const info = request.connectionMetadata;
  if (info == null) {
    return;
  }
  let set = info.metadata;
  if (Object.keys(set).length > 0) {
    validateMetadata(set);
    if (session !== undefined) {
      const sets = connectionMetadataSets.get(session) ?? new Map();
      connectionMetadataSets.set(session, sets);
      // Map order is insertion order: re-inserting an ID makes it the newest.
      sets.delete(info.id);
      sets.set(info.id, set);
      if (sets.size > MaxConnectionMetadataSets) {
        sets.delete(sets.keys().next().value);
      }
      request.connectionMetadataConfirmed = info.id;
    }
  } else {
    set = connectionMetadataSets.get(session)?.get(info.id);
    if (set === undefined) {
      throw invalidArgument(`unknown connection metadata set ${info.id}`);
    }
  }
  for (const [key, value] of Object.entries(set)) {
    request.metadata[key] ??= value;
  }
}

/**
 * The HEADERS fields answering the call's dictionary query and confirming the connection metadata
 * it offered. This server holds no dictionaries but decodes messages without one, so it confirms
 * ID 0 when asked, as real servers do in every HEADERS that follows a query. A call that gets any
 * answer gets HEADERS.
 */
function headersAnswer(request) {
  const answer = {};
  const query = request.dictionaryQuery ?? [];
  if (query.length > 0) {
    answer.dictionaries = query.some(({ id }) => id === 0) ? [{}] : [];
  }
  if (request.connectionMetadataConfirmed !== undefined) {
    answer.connectionMetadata = request.connectionMetadataConfirmed;
  }
  return answer;
}

/** Whether the handler of the method writes the call's HEADERS itself, at dispatch. */
function sendsOwnHeaders(method) {
  const path = parseMethodPath(method);
  return (
    path?.service === CompressedEchoService ||
    (path?.service === UnreliableEchoService && path.method === "Bidi")
  );
}

async function serve(stream, request, unary, session) {
  try {
    resolveConnectionMetadata(request, session);
  } catch (error) {
    if (!(error instanceof TrevRpcError)) {
      throw error;
    }
    await stream.writeStatus(error.code, error.statusMessage);
    await stream.finish();
    await stream.stopSending();
    return;
  }
  // The interop-header rule: echo the value as response header and, on success, as trailer.
  const marker = request.metadata["interop-header"];
  const trailers = marker === undefined ? {} : { "interop-trailer": new Uint8Array(marker) };
  const answer = headersAnswer(request);
  if (
    marker !== undefined ||
    (answer.connectionMetadata !== undefined && !sendsOwnHeaders(request.method))
  ) {
    await stream.write(
      encodeHeadersFrame({
        metadata: marker === undefined ? {} : { "interop-header": new Uint8Array(marker) },
        ...answer,
      }),
    );
  }
  const done = (code = Code.Ok, message = "") =>
    stream.writeStatus(code, message, code === Code.Ok ? trailers : {});

  // A malformed path is a protocol error; a well-formed path with no handler is unimplemented.
  const path = parseMethodPath(request.method);
  if (path === null) {
    await stream.writeStatus(Code.InvalidArgument, `malformed method path "${request.method}"`);
    await stream.finish();
    await stream.stopSending();
    return;
  }

  if (path.service === CompressedEchoService && path.method === "Plain") {
    await compressedPlain(stream, request);
    await stream.finish();
    await stream.stopSending();
    return;
  }

  if (path.service === CompressedEchoService) {
    const streams = new ServerStreams(compression, request);
    if (streams.refuses) {
      await stream.refuse();
      return;
    }
    await (path.method === "ClientStream" ? compressedClientStream : compressedBidi)(
      stream,
      streams,
      headersAnswer(request),
    );
    await stream.finish();
    await stream.stopSending();
    return;
  }

  // RefusingEcho is registered without stream compression, as by a server built from a schema
  // without COMPRESSION_STREAM: it never answers, so it refuses every optimistic call.
  if (path.service === RefusingEchoService && path.method === "ClientStream") {
    if (request.streamCompression?.optimistic) {
      await stream.refuse();
      return;
    }
    await clientStream(stream, done);
    await stream.finish();
    await stream.stopSending();
    return;
  }

  if (path.service === UnreliableEchoService && path.method === "Bidi") {
    await unreliableBidi(stream, request, session);
    await stream.finish();
    await stream.stopSending();
    return;
  }

  switch (path.service === EchoService ? path.method : undefined) {
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
      await stream.writeStatus(Code.Unimplemented, `unknown method ${request.method}`);
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

/**
 * CompressedEcho.Bidi as the interop README specifies: Echo.Bidi with stream compression, whose
 * OK status reports how many requests arrived stream-compressed.
 */
async function compressedBidi(stream, streams, answer) {
  try {
    await stream.write(encodeHeadersFrame({ metadata: {}, ...streams.headers, ...answer }));
    for (let index = 0, frame; (frame = await stream.readFrame()) !== null; index += 1) {
      const message = EchoRequest.decode(streams.receive(frame));
      if (message.failCode !== 0) {
        await stream.writeStatus(message.failCode, message.failMessage);
        return;
      }
      const response = marshalMessage(EchoResponse, { message: message.message, index });
      await stream.write(encodeOutgoingFrame(streams.encode(response)));
    }
    await stream.writeStatus(Code.Ok, "", {
      "interop-stream-compressed-received": new TextEncoder().encode(String(streams.received)),
    });
  } finally {
    streams.close();
  }
}

/**
 * CompressedEcho.Plain as the interop README specifies: it echoes the text, decodes messages
 * without a dictionary (answering the query for ID 0), compresses the response so where the call
 * accepts it, and reports whether the request arrived compressed.
 */
async function compressedPlain(stream, request) {
  const answer = headersAnswer(request);
  if (Object.keys(answer).length > 0) {
    await stream.write(encodeHeadersFrame({ metadata: {}, ...answer }));
  }
  const frame = await stream.readFrame();
  const plain = new Map([[0, compression.plain]]);
  const compressed = frame.type === FrameType.ZstdMessage;
  const payload = compressed ? inflateMessage(plain, frame.payload, 1 << 22) : frame.payload;
  const { text: echoed } = PlainMessage.decode(payload);
  const accepted = (request.acceptDictionaries ?? []).includes(0);
  const response = marshalMessage(PlainMessage, { text: echoed });
  await stream.write(
    encodeOutgoingFrame(encodeMessage(response, accepted ? compression.plain : undefined)),
  );
  await stream.writeStatus(Code.Ok, "", {
    "interop-plain-compressed-received": new TextEncoder().encode(compressed ? "1" : "0"),
  });
}

/**
 * CompressedEcho.ClientStream as the interop README specifies: Echo.ClientStream with stream
 * compression. HEADERS go out at dispatch, as servers send them on methods whose requests may be
 * stream-compressed.
 */
async function compressedClientStream(stream, streams, answer) {
  try {
    await stream.write(encodeHeadersFrame({ metadata: {}, ...streams.headers, ...answer }));
    let message = "";
    let count = 0;
    for (let frame; (frame = await stream.readFrame()) !== null; count += 1) {
      message += EchoRequest.decode(streams.receive(frame)).message;
    }
    const response = marshalMessage(EchoResponse, { message, index: count });
    await stream.write(encodeOutgoingFrame(streams.encode(response)));
    await stream.writeStatus(Code.Ok, "", {
      "interop-stream-compressed-received": new TextEncoder().encode(String(streams.received)),
    });
  } finally {
    streams.close();
  }
}

/**
 * UnreliableEcho.Bidi as the interop README specifies. Requests arrive on the stream or, for a call
 * with a datagram flow, as datagrams from the session; responses to small requests go out as
 * datagrams on such a call, after HEADERS.
 */
async function unreliableBidi(stream, request, session) {
  const flow = Number(request.datagramFlow);
  const datagrams = flow === 0 ? undefined : session?.datagrams;
  const counts = { received: 0, datagrams: 0 };
  let work = Promise.resolve();

  const handle = (message, viaDatagram) => {
    counts.received += 1;
    counts.datagrams += viaDatagram ? 1 : 0;
    work = work.then(async () => {
      const responses = Array.from({ length: message.count }, (_, index) => ({
        message: message.message,
        index,
      }));
      if (datagrams !== undefined && message.message.length <= MaxDatagramResponse) {
        // One datagram holds all the responses to a request.
        datagrams.push(
          encodeDatagram(
            flow,
            responses.map((response) => encodeMessage(marshalMessage(EchoResponse, response))),
          ),
        );
        return;
      }
      for (const response of responses) {
        await stream.writeMessage(EchoResponse, response);
      }
    });
  };

  await stream.write(encodeHeadersFrame({ metadata: {}, ...headersAnswer(request) }));
  const previousListener = datagrams?.onSend;
  if (datagrams !== undefined) {
    datagrams.onSend = (datagram) => {
      const decoded = decodeDatagram(datagram);
      if (decoded?.flow === flow) {
        for (const { payload } of decoded.messages) {
          handle(EchoRequest.decode(payload), true);
        }
      }
    };
  }
  for (let message; (message = await stream.readMessage(EchoRequest)) !== null;) {
    handle(message, false);
  }
  if (datagrams !== undefined) {
    datagrams.onSend = previousListener;
  }
  await work;
  await stream.writeStatus(Code.Ok, "", {
    "interop-received": new TextEncoder().encode(String(counts.received)),
    "interop-datagrams-received": new TextEncoder().encode(String(counts.datagrams)),
  });
}
