// Compressed messages: ZSTD_MESSAGE frames, whose payload is a magicless zstd frame compressed with
// a dictionary both peers hold (see the "Compressed Messages" section of the wire format). The
// codec itself lives in the `@trevrpc/trevrpc-js/zstd` subpath, so the runtime never loads it
// unless compression is configured; this module negotiates dictionaries per session, validates
// payloads, and enforces limits before handing a payload to the codec.
import { byteView } from "./bytes.js";
import { FrameType } from "./framing.js";
import { internal, resourceExhausted } from "./status.js";

/**
 * The compression configuration of a transport, or undefined without one. A Channel carries the
 * one from its connect options; a configuration without dictionaries compresses nothing.
 */
export function transportCompression(transport) {
  const compression = transport.compression;
  return compression?.query?.length > 0 ? compression : undefined;
}

const sessions = new WeakMap();

/**
 * The compression state of one session (one connection) for a configuration. The server's answer
 * to a query holds for the whole session, so it is cached here and dropped with the session.
 */
export function sessionCompression(compression, session) {
  let bySession = sessions.get(compression);
  if (bySession === undefined) {
    bySession = new WeakMap();
    sessions.set(compression, bySession);
  }
  let state = bySession.get(session);
  if (state === undefined) {
    state = new SessionCompression(compression);
    bySession.set(session, state);
  }
  return state;
}

class SessionCompression {
  #compression;
  /** The session's shared set, from the server's answer to a query; undefined until one arrives. */
  #shared;

  constructor(compression) {
    this.#compression = compression;
  }

  /**
   * The negotiation of one call whose response messages have type `responseType`: the REQUEST
   * fields it sets, the dictionary it accepts on its responses (or undefined), and whether it
   * queries the server.
   */
  prepare(responseType) {
    const shared = this.#shared;
    const fields = {};
    if (shared === undefined) {
      fields.dictionaryQuery = this.#compression.query;
    }
    // A call that queries may already accept the dictionary it asks about: the server compresses
    // with it only if its answer confirms it.
    const dictionary = this.#compression.dictionaryFor(responseType);
    const accept = dictionary !== undefined && (shared === undefined || shared.has(dictionary));
    if (accept) {
      fields.acceptDictionaries = [dictionary.id];
    }
    return { fields, accept: accept ? dictionary : undefined, queried: shared === undefined };
  }

  /**
   * Records the server's answer to a query as the session's shared set. The first answer wins;
   * they all come from one configuration.
   */
  learn(answer = []) {
    if (this.#shared !== undefined) {
      return;
    }
    const shared = new Set();
    for (const { id, fingerprint } of answer) {
      const dictionary = this.#compression.dictionary(id);
      if (dictionary !== undefined && sameBytes(dictionary.fingerprint, byteView(fingerprint))) {
        shared.add(dictionary);
      }
    }
    this.#shared = shared;
  }

  /** The dictionary request messages of type `requestType` may use now, or undefined. */
  requestDictionary(requestType) {
    const dictionary = this.#compression.dictionaryFor(requestType);
    return dictionary !== undefined && this.#shared?.has(dictionary) ? dictionary : undefined;
  }
}

/**
 * Encodes a marshalled message as an outgoing message, `{ type, payload, size }`: a ZSTD_MESSAGE
 * compressed with `dictionary` when that is shorter, and a MESSAGE otherwise, as without one. `size` is the message size, which
 * limits apply to.
 */
export function encodeMessage(body, dictionary) {
  const compressed = dictionary?.compress(body);
  return compressed !== undefined && compressed.byteLength < body.byteLength
    ? { type: FrameType.ZstdMessage, payload: compressed, size: body.byteLength }
    : { type: FrameType.Message, payload: body, size: body.byteLength };
}

/**
 * The message of a received MESSAGE or ZSTD_MESSAGE frame, `{ type, payload }`, decompressed with
 * `accept` (see inflateMessage).
 */
export function receivedMessage({ type, payload }, accept, maxSize) {
  return type === FrameType.ZstdMessage ? inflateMessage(accept, payload, maxSize) : payload;
}

/**
 * Decompresses the payload of a ZSTD_MESSAGE frame received on a response. `accept` is the
 * dictionary the call accepts, or undefined. A message above maxSize is ResourceExhausted, found
 * before decompressing; every other fault is Internal.
 */
export function inflateMessage(accept, payload, maxSize) {
  if (accept === undefined) {
    throw internal("compressed message where no dictionary is accepted");
  }
  const header = parseZstdHeader(payload);
  if (header.contentSize > maxSize) {
    throw resourceExhausted(
      `compressed message of ${header.contentSize} bytes exceeds maximum ${maxSize}`,
    );
  }
  if (header.dictionaryId !== accept.id) {
    throw internal(
      `compressed message uses dictionary ${header.dictionaryId}, which is not accepted`,
    );
  }
  let message;
  try {
    message = accept.decompress(payload, header.contentSize);
  } catch {
    // Caught below as a failure; the codec's own error says nothing more.
  }
  if (message?.byteLength !== header.contentSize) {
    throw internal("invalid ZSTD_MESSAGE payload");
  }
  return message;
}

const FcsSizes = [0, 2, 4, 8];
const DidSizes = [0, 1, 2, 4];

/**
 * Validates that payload is exactly one magicless zstd frame with a non-zero Dictionary_ID and a
 * Frame_Content_Size, and returns both as `{ dictionaryId, contentSize }`. It walks the block
 * headers to find the frame's end, so trailing bytes, a second frame included, are rejected. Block
 * contents are not checked; decompression does that. A content size beyond 2^53 is approximate,
 * which only matters for limits it exceeds anyway.
 */
export function parseZstdHeader(payload) {
  const bytes = byteView(payload);
  const invalid = () => internal("invalid ZSTD_MESSAGE payload");
  if (bytes.byteLength === 0) {
    throw invalid();
  }
  const descriptor = bytes[0];
  const fcsFlag = descriptor >> 6;
  const singleSegment = (descriptor & 0x20) !== 0;
  const checksum = (descriptor & 0x04) !== 0;
  const didSize = DidSizes[descriptor & 0x03];
  const fcsSize = fcsFlag === 0 && singleSegment ? 1 : FcsSizes[fcsFlag];
  // The reserved bit must be unset.
  if ((descriptor & 0x08) !== 0 || didSize === 0 || fcsSize === 0) {
    throw invalid();
  }
  let position = singleSegment ? 1 : 2; // after the Window_Descriptor, if present
  if (bytes.byteLength < position + didSize + fcsSize) {
    throw invalid();
  }
  const dictionaryId = readLittleEndian(bytes, position, didSize);
  position += didSize;
  let contentSize = readLittleEndian(bytes, position, fcsSize);
  if (fcsSize === 2) {
    contentSize += 256;
  }
  position += fcsSize;
  if (dictionaryId === 0) {
    throw invalid();
  }
  for (;;) {
    if (bytes.byteLength - position < 3) {
      throw invalid();
    }
    const header = bytes[position] | (bytes[position + 1] << 8) | (bytes[position + 2] << 16);
    position += 3;
    let size = header >>> 3;
    switch ((header >>> 1) & 0x03) {
      case 0: // Raw_Block
      case 2: // Compressed_Block
        break;
      case 1: // RLE_Block
        size = 1;
        break;
      default:
        throw invalid();
    }
    if (bytes.byteLength - position < size) {
      throw invalid();
    }
    position += size;
    if ((header & 1) !== 0) {
      break; // Last_Block
    }
  }
  if (checksum) {
    position += 4;
  }
  if (position !== bytes.byteLength) {
    throw invalid();
  }
  return { dictionaryId, contentSize };
}

/** Reads an unsigned little-endian integer of size bytes as a number. */
function readLittleEndian(bytes, offset, size) {
  let value = 0;
  for (let index = size - 1; index >= 0; index -= 1) {
    value = value * 256 + bytes[offset + index];
  }
  return value;
}

function sameBytes(a, b) {
  return a.byteLength === b.byteLength && a.every((byte, index) => byte === b[index]);
}
