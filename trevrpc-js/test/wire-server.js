// The server direction of the wire payload codecs: decoding REQUEST and encoding HEADERS, ANSWER
// and STATUS, and the frames of the last three. The browser client never needs it, so it lives
// with the tests, which play the server, on the building blocks of src/wire.js. The codecs here
// have both directions, so tests can round-trip every payload.
import { WireType } from "@bufbuild/protobuf/wire";

import { DefaultMaxFrameSize, FrameType, encodeFrame, encodeWireMessage } from "../src/framing.js";
import * as client from "../src/wire.js";
import {
  WireReader,
  encodeWith,
  writeDictionaries,
  writeMetadata,
  writeString,
  writeUint32,
} from "../src/wire.js";

/** The REQUEST payload; see src/wire.js. Decoded 64-bit fields are bigints. */
export const RpcRequest = Object.freeze({
  ...client.RpcRequest,
  decode: (bytes) => decodeRequest(new WireReader(bytes)),
});

/** The HEADERS payload: `{ metadata }`. */
export const RpcHeaders = Object.freeze({
  ...client.RpcHeaders,
  encode: (headers) => encodeWith(writeHeaders, headers),
});

/** The ANSWER payload: `{ dictionaries, streamCompression, connectionMetadata }`. */
export const RpcAnswer = Object.freeze({
  ...client.RpcAnswer,
  encode: (answer) => encodeWith(writeAnswer, answer),
});

/** The STATUS payload; see src/wire.js. */
export const RpcStatus = Object.freeze({
  ...client.RpcStatus,
  encode: (status) => encodeWith(writeStatus, status),
});

/** Encodes a HEADERS frame from RpcHeaders fields. */
export function encodeHeadersFrame(headers, maxFrameSize = DefaultMaxFrameSize) {
  return encodeFrame(FrameType.Headers, encodeWireMessage(RpcHeaders, headers), maxFrameSize);
}

/** Encodes an ANSWER frame from RpcAnswer fields. */
export function encodeAnswerFrame(answer, maxFrameSize = DefaultMaxFrameSize) {
  return encodeFrame(FrameType.Answer, encodeWireMessage(RpcAnswer, answer), maxFrameSize);
}

/** Encodes a STATUS frame from RpcStatus fields. */
export function encodeStatusFrame(status, maxFrameSize = DefaultMaxFrameSize) {
  return encodeFrame(FrameType.Status, encodeWireMessage(RpcStatus, status), maxFrameSize);
}

function writeHeaders(writer, headers) {
  writeMetadata(writer, 1, headers.metadata);
}

function writeAnswer(writer, answer) {
  writeDictionaries(writer, 1, answer.dictionaries);
  writeBool(writer, 2, answer.streamCompression);
  writeUint32(writer, 3, answer.connectionMetadata);
}

function writeStatus(writer, status) {
  writeUint32(writer, 1, status.code);
  writeString(writer, 2, status.message);
  writeMetadata(writer, 3, status.metadata);
  writeBool(writer, 4, status.refusedStreamCompression);
  for (const detail of status.details ?? []) {
    writer.tag(5, WireType.LengthDelimited).fork();
    writeString(writer, 1, detail.type);
    if (detail.value != null && detail.value.byteLength > 0) {
      writer.tag(2, WireType.LengthDelimited).bytes(detail.value);
    }
    writer.join();
  }
}

function writeBool(writer, field, value) {
  if (value) {
    writer.tag(field, WireType.Varint).bool(true);
  }
}

function decodeRequest(reader) {
  const request = {
    method: "",
    metadata: {},
    kind: 0,
    timeoutNanos: 0n,
    datagramFlow: 0n,
    acceptDictionaries: [],
    dictionaryQuery: [],
    streamCompression: 0,
    connectionMetadata: undefined,
  };
  while (reader.more()) {
    const tag = reader.tag();
    const wireType = tag & 7;
    switch (tag >>> 3) {
      case 1:
        request.method = reader.string(wireType, "method");
        break;
      case 2:
        reader.metadataEntry(wireType, request.metadata, "metadata");
        break;
      case 3:
        request.kind = reader.int32(wireType, "kind");
        break;
      case 4:
        request.timeoutNanos = reader.uint64(wireType, "timeout_nanos");
        break;
      case 5:
        request.datagramFlow = reader.uint64(wireType, "datagram_flow");
        break;
      case 6:
        readRepeatedUint32(reader, wireType, request.acceptDictionaries, "accept_dictionaries");
        break;
      case 7:
        request.dictionaryQuery.push(reader.dictionaryInfo(wireType, "dictionary_query"));
        break;
      case 8:
        request.streamCompression = reader.int32(wireType, "stream_compression");
        break;
      case 9:
        request.connectionMetadata = readConnectionMetadata(
          reader,
          wireType,
          "connection_metadata",
        );
        break;
      default:
        reader.skip(tag);
    }
  }
  return request;
}

/** Appends one value, or a packed run of values, of a repeated uint32 field to `values`. */
function readRepeatedUint32(reader, wireType, values, name) {
  if (wireType === WireType.Varint) {
    values.push(reader.uint32(wireType, name));
    return;
  }
  const packed = reader.nested(wireType, name);
  while (packed.more()) {
    values.push(packed.uint32(WireType.Varint, name));
  }
}

/** A ConnectionMetadata message, `{ id, metadata }`. */
function readConnectionMetadata(reader, wireType, name) {
  const nested = reader.nested(wireType, name);
  const connection = { id: 0, metadata: {} };
  while (nested.more()) {
    const tag = nested.tag();
    const field = tag >>> 3;
    if (field === 1) {
      connection.id = nested.uint32(tag & 7, `${name}.id`);
    } else if (field === 2) {
      nested.metadataEntry(tag & 7, connection.metadata, `${name}.metadata`);
    } else {
      nested.skip(tag);
    }
  }
  return connection;
}
