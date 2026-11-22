// QMux frames (draft-ietf-quic-qmux-02): the QUIC v1 frames QMux carries, RFC 9221 DATAGRAM, and
// QMux's own QX_TRANSPORT_PARAMETERS and QX_PING frames.
import { QMuxProtocolError, TransportError } from "./errors.js";
import { readVarint, varintLength, writeVarint } from "./varint.js";

export const FrameType = Object.freeze({
  Padding: 0x00,
  ResetStream: 0x04,
  StopSending: 0x05,
  Stream: 0x08, // through 0x0f
  MaxData: 0x10,
  MaxStreamData: 0x11,
  MaxStreamsBidi: 0x12,
  MaxStreamsUni: 0x13,
  DataBlocked: 0x14,
  StreamDataBlocked: 0x15,
  StreamsBlockedBidi: 0x16,
  StreamsBlockedUni: 0x17,
  ConnectionClose: 0x1c,
  ApplicationClose: 0x1d,
  /** RFC 9221 DATAGRAM, without and with a Length field. */
  Datagram: 0x30,
  DatagramLen: 0x31,
  TransportParameters: 0x3f51_5330_0d0a_0d0an,
  Ping: 0x348c_6752_9ef8_c7bdn,
  PingResponse: 0x348c_6752_9ef8_c7ben,
});

const StreamFin = 0x01;
const StreamLen = 0x02;
const StreamOff = 0x04;

function truncated(frameType) {
  return new QMuxProtocolError(TransportError.FrameEncodingError, "truncated frame", frameType);
}

/** Builds the frames of one record. */
export class FrameWriter {
  #bytes = new Uint8Array(256);
  #length = 0;

  get length() {
    return this.#length;
  }

  #reserve(extra) {
    if (this.#length + extra > this.#bytes.length) {
      const grown = new Uint8Array(Math.max(this.#bytes.length * 2, this.#length + extra));
      grown.set(this.#bytes.subarray(0, this.#length));
      this.#bytes = grown;
    }
  }

  varint(value) {
    this.#reserve(8);
    this.#length = writeVarint(this.#bytes, this.#length, value);
    return this;
  }

  bytes(data) {
    this.#reserve(data.length);
    this.#bytes.set(data, this.#length);
    this.#length += data.length;
    return this;
  }

  /** Returns the frames written so far and starts over. */
  take() {
    const frames = this.#bytes.slice(0, this.#length);
    this.#length = 0;
    return frames;
  }
}

/** Size of a STREAM frame header carrying `length` bytes at `offset`. */
export function streamFrameOverhead(streamId, offset, length) {
  return (
    1 + varintLength(streamId) + varintLength(length) + (offset > 0 ? varintLength(offset) : 0)
  );
}

/** Appends a STREAM frame. It always carries a length so that frames may follow it. */
export function writeStreamFrame(writer, streamId, offset, data, fin) {
  let type = FrameType.Stream | StreamLen;
  if (offset > 0) {
    type |= StreamOff;
  }
  if (fin) {
    type |= StreamFin;
  }
  writer.varint(type).varint(streamId);
  if (offset > 0) {
    writer.varint(offset);
  }
  return writer.varint(data.length).bytes(data);
}

export function writeResetStream(writer, streamId, code, finalSize) {
  return writer.varint(FrameType.ResetStream).varint(streamId).varint(code).varint(finalSize);
}

export function writeStopSending(writer, streamId, code) {
  return writer.varint(FrameType.StopSending).varint(streamId).varint(code);
}

export function writeMaxData(writer, maximum) {
  return writer.varint(FrameType.MaxData).varint(maximum);
}

export function writeMaxStreamData(writer, streamId, maximum) {
  return writer.varint(FrameType.MaxStreamData).varint(streamId).varint(maximum);
}

export function writeMaxStreams(writer, bidi, maximum) {
  return writer.varint(bidi ? FrameType.MaxStreamsBidi : FrameType.MaxStreamsUni).varint(maximum);
}

/** Size of a DATAGRAM frame carrying `length` bytes, with its Length field. */
export function datagramFrameSize(length) {
  return 1 + varintLength(length) + length;
}

/** Appends a DATAGRAM frame. It always carries a length so that frames may follow it. */
export function writeDatagram(writer, data) {
  return writer.varint(FrameType.DatagramLen).varint(data.length).bytes(data);
}

/** Appends an application CONNECTION_CLOSE, or a transport one for a QMuxProtocolError. */
export function writeConnectionClose(writer, { code = 0, reason = "", protocolError } = {}) {
  const reasonBytes = new TextEncoder().encode(reason).subarray(0, 256);
  if (protocolError) {
    writer
      .varint(FrameType.ConnectionClose)
      .varint(protocolError.code)
      .varint(protocolError.frameType);
  } else {
    writer.varint(FrameType.ApplicationClose).varint(code);
  }
  return writer.varint(reasonBytes.length).bytes(reasonBytes);
}

export function writePing(writer, type, sequence) {
  return writer.varint(type).varint(sequence);
}

/**
 * Decodes the frames of one record; `next()` returns null at its end. DATAGRAM frames are a
 * PROTOCOL_VIOLATION unless `maxDatagramFrameSize`, the local max_datagram_frame_size, admits
 * them (RFC 9221 §3).
 */
export class FrameParser {
  #bytes;
  #offset = 0;
  #maxDatagramFrameSize;

  constructor(bytes, { maxDatagramFrameSize = 0 } = {}) {
    this.#bytes = bytes;
    this.#maxDatagramFrameSize = maxDatagramFrameSize;
  }

  #varint(frameType) {
    const result = readVarint(this.#bytes, this.#offset);
    if (result === null) {
      throw truncated(frameType);
    }
    this.#offset = result.next;
    return result.value;
  }

  /** A varint that must fit a number: offsets, lengths, limits and stream IDs. */
  #number(frameType) {
    const value = this.#varint(frameType);
    if (typeof value === "bigint") {
      throw new QMuxProtocolError(
        TransportError.FrameEncodingError,
        "value exceeds what this endpoint supports",
        frameType,
      );
    }
    return value;
  }

  #take(frameType, length) {
    if (length > this.#bytes.length - this.#offset) {
      throw truncated(frameType);
    }
    const data = this.#bytes.subarray(this.#offset, this.#offset + length);
    this.#offset += length;
    return data;
  }

  next() {
    if (this.#offset >= this.#bytes.length) {
      return null;
    }
    const start = this.#offset;
    const type = this.#varint(0);
    if (typeof type === "bigint") {
      if (type === FrameType.TransportParameters) {
        return { type, params: this.#take(type, this.#number(type)) };
      }
      if (type === FrameType.Ping || type === FrameType.PingResponse) {
        return { type, sequence: this.#number(type) };
      }
      throw new QMuxProtocolError(
        TransportError.FrameEncodingError,
        `frame type 0x${type.toString(16)} is not allowed in QMux`,
        0,
      );
    }
    switch (type) {
      case FrameType.Padding:
        return { type };
      case FrameType.ResetStream:
        return {
          type,
          streamId: this.#number(type),
          code: this.#number(type),
          finalSize: this.#number(type),
        };
      case FrameType.StopSending:
        return { type, streamId: this.#number(type), code: this.#number(type) };
      case FrameType.MaxData:
      case FrameType.DataBlocked:
      case FrameType.MaxStreamsBidi:
      case FrameType.MaxStreamsUni:
      case FrameType.StreamsBlockedBidi:
      case FrameType.StreamsBlockedUni:
        return { type, maximum: this.#number(type) };
      case FrameType.MaxStreamData:
      case FrameType.StreamDataBlocked:
        return { type, streamId: this.#number(type), maximum: this.#number(type) };
      case FrameType.ConnectionClose:
      case FrameType.ApplicationClose: {
        const code = this.#number(type);
        const frameType = type === FrameType.ConnectionClose ? this.#varint(type) : undefined;
        const reason = new TextDecoder().decode(this.#take(type, this.#number(type)));
        return { type, code, frameType, reason };
      }
      case FrameType.Datagram:
      case FrameType.DatagramLen:
        return this.#datagram(type, start);
      default:
        if (type >= FrameType.Stream && type <= (FrameType.Stream | 0x07)) {
          return this.#stream(type);
        }
        throw new QMuxProtocolError(
          TransportError.FrameEncodingError,
          `frame type 0x${type.toString(16)} is not allowed in QMux`,
          type,
        );
    }
  }

  #stream(type) {
    const streamId = this.#number(type);
    const offset = type & StreamOff ? this.#number(type) : 0;
    const length = type & StreamLen ? this.#number(type) : this.#bytes.length - this.#offset;
    return {
      type,
      streamId,
      offset,
      data: this.#take(type, length),
      fin: (type & StreamFin) !== 0,
    };
  }

  #datagram(type, start) {
    if (this.#maxDatagramFrameSize === 0) {
      throw new QMuxProtocolError(
        TransportError.ProtocolViolation,
        "DATAGRAM frame without max_datagram_frame_size",
        type,
      );
    }
    const length =
      type === FrameType.DatagramLen ? this.#number(type) : this.#bytes.length - this.#offset;
    const datagram = this.#take(type, length);
    if (this.#offset - start > this.#maxDatagramFrameSize) {
      throw new QMuxProtocolError(
        TransportError.ProtocolViolation,
        "DATAGRAM frame exceeds max_datagram_frame_size",
        type,
      );
    }
    return { type, datagram };
  }
}
