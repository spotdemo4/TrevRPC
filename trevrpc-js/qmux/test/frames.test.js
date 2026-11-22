import assert from "node:assert/strict";
import test from "node:test";

import { QMuxProtocolError, TransportError } from "../src/errors.js";
import {
  FrameParser,
  FrameType,
  writeConnectionClose,
  writeDatagram,
  writeMaxData,
  writeMaxStreamData,
  writeMaxStreams,
  writePing,
  writeResetStream,
  writeStopSending,
  writeStreamFrame,
} from "../src/frames.js";
import { decodeTransportParameters, writeTransportParameters } from "../src/params.js";
import { readVarint, writeVarint } from "../src/varint.js";
import { frames, goldenVectors } from "./support.js";

function parseAll(bytes, options) {
  const parser = new FrameParser(bytes, options);
  const parsed = [];
  for (let frame = parser.next(); frame !== null; frame = parser.next()) {
    parsed.push(frame);
  }
  return parsed;
}

test("varints match RFC 9000", async () => {
  const vectors = await goldenVectors();
  for (const [name, value] of [
    ["varint.1byte", 37],
    ["varint.2byte", 15293],
    ["varint.4byte", 494878333],
    ["varint.8byte", 151288809941952652n],
  ]) {
    const bytes = new Uint8Array(8);
    const length = writeVarint(bytes, 0, value);
    assert.deepEqual(bytes.subarray(0, length), vectors.get(name), name);
    assert.equal(readVarint(vectors.get(name), 0).value, value, name);
  }
});

test("frames match the shared golden vectors", async () => {
  const vectors = await goldenVectors();
  const encoded = {
    "frame.stream.fin": frames((w) => writeStreamFrame(w, 0, 0, Buffer.from("hi"), true)),
    "frame.stream.offset": frames((w) => writeStreamFrame(w, 4, 16, Buffer.from("hi"), false)),
    "frame.reset_stream": frames((w) => writeResetStream(w, 0, 1, 5)),
    "frame.stop_sending": frames((w) => writeStopSending(w, 0, 1)),
    "frame.max_data": frames((w) => writeMaxData(w, 65)),
    "frame.max_stream_data": frames((w) => writeMaxStreamData(w, 4, 1028)),
    "frame.max_streams_bidi": frames((w) => writeMaxStreams(w, true, 65)),
    "frame.application_close": frames((w) => writeConnectionClose(w, { reason: "bye" })),
    "frame.transport_close": frames((w) =>
      writeConnectionClose(w, {
        protocolError: new QMuxProtocolError(TransportError.ProtocolViolation, "", 0x08),
      }),
    ),
    "frame.ping": frames((w) => writePing(w, FrameType.Ping, 1)),
    "frame.ping_response": frames((w) => writePing(w, FrameType.PingResponse, 1)),
    "message.stream_and_max_data": frames((w) => {
      writeStreamFrame(w, 0, 0, Buffer.from("hi"), true);
      writeMaxData(w, 65);
    }),
    "params.client": frames((w) =>
      writeTransportParameters(w, {
        initialMaxData: 4194308,
        initialMaxStreamDataBidiLocal: 4194308,
        maxRecordSize: 65536,
      }),
    ),
  };
  for (const [name, bytes] of Object.entries(encoded)) {
    assert.deepEqual(bytes, vectors.get(name), name);
    assert.ok(parseAll(bytes).length > 0, `${name} decodes`);
  }
});

test("server transport parameters decode", async () => {
  const vectors = await goldenVectors();
  const [frame] = parseAll(vectors.get("params.server"));
  assert.deepEqual(decodeTransportParameters(frame.params), {
    maxIdleTimeoutMs: 30000,
    initialMaxData: 16777216,
    initialMaxStreamDataBidiLocal: 0,
    initialMaxStreamDataBidiRemote: 4194308,
    initialMaxStreamDataUni: 0,
    initialMaxStreamsBidi: 65,
    initialMaxStreamsUni: 0,
    maxDatagramFrameSize: 0,
    maxRecordSize: 65536,
  });
});

test("invalid frames and parameters are rejected", async () => {
  const vectors = await goldenVectors();
  const codes = {
    frame_encoding: TransportError.FrameEncodingError,
    protocol_violation: TransportError.ProtocolViolation,
    transport_parameter: TransportError.TransportParameterError,
  };
  let tested = 0;
  for (const [name, bytes] of vectors) {
    if (!name.startsWith("invalid.")) {
      continue;
    }
    tested += 1;
    const code = codes[name.split(".")[1]];
    assert.throws(
      () => {
        for (const frame of parseAll(bytes)) {
          if (frame.type === FrameType.TransportParameters) {
            decodeTransportParameters(frame.params);
          }
        }
      },
      (error) => error instanceof QMuxProtocolError && error.code === code,
      name,
    );
  }
  assert.ok(tested > 0);
});

test("every parameter round-trips, including max_datagram_frame_size", () => {
  const params = {
    maxIdleTimeoutMs: 30000,
    initialMaxData: 1 << 20,
    initialMaxStreamDataBidiLocal: 1000,
    initialMaxStreamDataBidiRemote: 2000,
    initialMaxStreamDataUni: 3000,
    initialMaxStreamsBidi: 10,
    initialMaxStreamsUni: 20,
    maxDatagramFrameSize: 1200,
    maxRecordSize: 65536,
  };
  const [frame] = parseAll(frames((w) => writeTransportParameters(w, params)));
  assert.deepEqual(decodeTransportParameters(frame.params), params);
});

test("DATAGRAM frames decode within max_datagram_frame_size", () => {
  const data = Uint8Array.of(1, 2, 3);
  // With a Length field, so that frames may follow it, and without one, to the end of the record.
  const bytes = frames((w) => {
    writeDatagram(w, data);
    w.varint(FrameType.Datagram).bytes(data);
  });
  const parsed = parseAll(bytes, { maxDatagramFrameSize: 5 });
  assert.deepEqual(
    parsed.map((frame) => [frame.type, [...frame.datagram]]),
    [
      [FrameType.DatagramLen, [1, 2, 3]],
      [FrameType.Datagram, [1, 2, 3]],
    ],
  );
  // The Length form takes 5 bytes for 3 of data, the limit, so one more byte is too many.
  assert.throws(
    () =>
      parseAll(
        frames((w) => writeDatagram(w, Uint8Array.of(1, 2, 3, 4))),
        { maxDatagramFrameSize: 5 },
      ),
    (error) =>
      error instanceof QMuxProtocolError && error.code === TransportError.ProtocolViolation,
  );
  assert.throws(
    () => parseAll(frames((w) => writeDatagram(w, data))),
    (error) =>
      error instanceof QMuxProtocolError && error.code === TransportError.ProtocolViolation,
  );
});
