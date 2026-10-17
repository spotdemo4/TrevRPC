import { readVarint, varintLength } from "../varint.js";
// QX_TRANSPORT_PARAMETERS (draft-ietf-quic-qmux-02 §6): the subset of QUIC transport parameters
// that QMux keeps, plus max_record_size.
import { FrameType, FrameWriter, QMuxProtocolError, TransportError } from "./frames.js";

/** The max_record_size a peer may assume until told otherwise, and the smallest it may set. */
export const DefaultMaxRecordSize = 16382;

const Param = Object.freeze({
  MaxIdleTimeout: 0x01,
  InitialMaxData: 0x04,
  InitialMaxStreamDataBidiLocal: 0x05,
  InitialMaxStreamDataBidiRemote: 0x06,
  InitialMaxStreamDataUni: 0x07,
  InitialMaxStreamsBidi: 0x08,
  InitialMaxStreamsUni: 0x09,
  MaxRecordSize: 0x0571_c594_29cd_0845n,
});

/** Parameters that only make sense for QUIC packets. */
const Forbidden = new Set([0x00, 0x02, 0x03, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10]);

const Fields = new Map([
  [Param.MaxIdleTimeout, "maxIdleTimeoutMs"],
  [Param.InitialMaxData, "initialMaxData"],
  [Param.InitialMaxStreamDataBidiLocal, "initialMaxStreamDataBidiLocal"],
  [Param.InitialMaxStreamDataBidiRemote, "initialMaxStreamDataBidiRemote"],
  [Param.InitialMaxStreamDataUni, "initialMaxStreamDataUni"],
  [Param.InitialMaxStreamsBidi, "initialMaxStreamsBidi"],
  [Param.InitialMaxStreamsUni, "initialMaxStreamsUni"],
  [Param.MaxRecordSize, "maxRecordSize"],
]);

/** Appends the QX_TRANSPORT_PARAMETERS frame advertising `params`; zero values are omitted. */
export function writeTransportParameters(writer, params) {
  const body = new FrameWriter();
  for (const [id, field] of Fields) {
    const value = params[field] ?? (id === Param.MaxRecordSize ? DefaultMaxRecordSize : 0);
    if (value !== 0) {
      body.varint(id).varint(varintLength(value)).varint(value);
    }
  }
  const bytes = body.take();
  return writer.varint(FrameType.TransportParameters).varint(bytes.length).bytes(bytes);
}

function parameterError(message) {
  return new QMuxProtocolError(
    TransportError.TransportParameterError,
    message,
    FrameType.TransportParameters,
  );
}

/** Decodes a peer's transport parameters; unknown ones are ignored. */
export function decodeTransportParameters(bytes) {
  const params = {
    maxIdleTimeoutMs: 0,
    initialMaxData: 0,
    initialMaxStreamDataBidiLocal: 0,
    initialMaxStreamDataBidiRemote: 0,
    initialMaxStreamDataUni: 0,
    initialMaxStreamsBidi: 0,
    initialMaxStreamsUni: 0,
    maxRecordSize: DefaultMaxRecordSize,
  };
  const seen = new Set();
  let offset = 0;
  while (offset < bytes.length) {
    const id = readVarint(bytes, offset);
    const length = id && readVarint(bytes, id.next);
    if (!length || typeof length.value === "bigint" || length.next + length.value > bytes.length) {
      throw parameterError("truncated transport parameters");
    }
    const value = bytes.subarray(length.next, length.next + length.value);
    offset = length.next + length.value;
    if (seen.has(id.value)) {
      throw parameterError(`duplicate transport parameter 0x${id.value.toString(16)}`);
    }
    seen.add(id.value);
    if (Forbidden.has(id.value)) {
      throw parameterError(`transport parameter 0x${id.value.toString(16)} is not allowed in QMux`);
    }
    const field = Fields.get(id.value);
    if (field === undefined) {
      continue;
    }
    const parsed = readVarint(value, 0);
    if (!parsed || parsed.next !== value.length) {
      throw parameterError(`malformed transport parameter 0x${id.value.toString(16)}`);
    }
    // Limits beyond 2^53 are as good as unlimited.
    params[field] = typeof parsed.value === "bigint" ? Number.MAX_SAFE_INTEGER : parsed.value;
  }
  if (params.maxRecordSize < DefaultMaxRecordSize) {
    throw parameterError(
      `max_record_size ${params.maxRecordSize} is below ${DefaultMaxRecordSize}`,
    );
  }
  if (params.initialMaxStreamsBidi > 2 ** 60) {
    throw parameterError("initial_max_streams_bidi exceeds 2^60");
  }
  return params;
}
