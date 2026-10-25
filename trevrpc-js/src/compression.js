// Compressed messages: ZSTD_MESSAGE frames, whose payload is a magicless zstd frame compressed with
// a dictionary both peers hold or, on methods that allow it, without one, and the chunks of
// ZSTD_STREAM_MESSAGE frames (see the "Compressed Messages" section of the wire format). The codec itself lives in the `@trevrpc/trevrpc-js/zstd`
// subpath, so the runtime never loads it unless compression is configured; this module negotiates
// dictionaries per session, validates payloads, and enforces limits before handing a payload to
// the codec. Stream compression's per-call state is in stream-compression.js.
import { byteView } from "./bytes.js";
import { FrameType } from "./framing.js";
import { internal, resourceExhausted } from "./status.js";

/**
 * The compression configuration of a transport, or undefined without one. A Channel carries the
 * one from its connect options. A configuration without dictionaries still enables stream
 * compression.
 */
export function transportCompression(transport) {
  return transport.compression ?? undefined;
}

/**
 * The smallest message compressed without a dictionary: a smaller one rarely has anything to
 * compress against within itself.
 */
export const PlainMinSize = 256;

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
  /**
   * Whether calls on the session stream-compress requests optimistically: from the start, until
   * the server refuses one, and again once a later call's HEADERS answer `stream_compression`.
   */
  optimisticStreams = true;

  constructor(compression) {
    this.#compression = compression;
  }

  /**
   * The negotiation of one call whose response messages have type `responseType`: the REQUEST
   * fields it sets, what it accepts on its responses (a Map from dictionary ID to dictionary, 0
   * for compression without one, which `plainResponses` allows), and whether it queries the
   * server.
   */
  prepare(responseType, plainResponses = false) {
    const shared = this.#shared;
    const fields = {};
    const queried = shared === undefined;
    if (queried) {
      fields.dictionaryQuery = this.#compression.query;
    }
    // A call that queries may already accept the dictionary it asks about: the server compresses
    // with it only if its answer confirms it. Compression without a dictionary needs no
    // confirmation: a server that does not decode it does not send it either.
    const accept = new Map();
    const dictionary = this.#compression.dictionaryFor(responseType);
    if (dictionary !== undefined && (shared === undefined || shared.has(dictionary))) {
      accept.set(dictionary.id, dictionary);
    }
    if (plainResponses) {
      accept.set(0, this.#compression.plain);
    }
    if (accept.size > 0) {
      fields.acceptDictionaries = [...accept.keys()];
    }
    return { fields, accept, queried };
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
    for (const { id = 0, fingerprint } of answer) {
      // ID 0, which an entry without fields decodes to, is compression without a dictionary.
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

  /**
   * What request messages of type `requestType` are compressed with now: their dictionary, or,
   * with `plain`, `Compression.plain` once the server has confirmed it decodes compression
   * without a dictionary; undefined for neither.
   */
  requestCompressor(requestType, plain) {
    const dictionary = this.requestDictionary(requestType);
    if (dictionary !== undefined || !plain) {
      return dictionary;
    }
    const compressor = this.#compression.plain;
    return this.#shared?.has(compressor) ? compressor : undefined;
  }
}

/**
 * Encodes a marshalled message as an outgoing message, `{ type, payload, size }`: a ZSTD_MESSAGE
 * compressed with `dictionary` when that is shorter, and a MESSAGE otherwise, as without one.
 * Compression without a dictionary (ID 0) applies from PlainMinSize. `size` is the message size,
 * which limits apply to.
 */
export function encodeMessage(body, dictionary) {
  const worth =
    dictionary !== undefined && (dictionary.id !== 0 || body.byteLength >= PlainMinSize);
  const compressed = worth ? dictionary.compress(body) : undefined;
  return compressed !== undefined && compressed.byteLength < body.byteLength
    ? { type: FrameType.ZstdMessage, payload: compressed, size: body.byteLength }
    : { type: FrameType.Message, payload: body, size: body.byteLength };
}

/**
 * The message of a received MESSAGE or ZSTD_MESSAGE frame, `{ type, payload }`, decompressed with
 * what `accept` holds (see inflateMessage).
 */
export function receivedMessage({ type, payload }, accept, maxSize) {
  return type === FrameType.ZstdMessage ? inflateMessage(accept, payload, maxSize) : payload;
}

/**
 * Decompresses the payload of a ZSTD_MESSAGE frame received on a response. `accept` maps the
 * dictionary IDs the call accepts to their dictionaries, 0 for compression without one, or is
 * undefined for none. A message above maxSize is ResourceExhausted, found before decompressing;
 * every other fault is Internal.
 */
export function inflateMessage(accept, payload, maxSize) {
  if (accept === undefined || accept.size === 0) {
    throw internal("compressed message where no dictionary is accepted");
  }
  const header = parseZstdHeader(payload);
  if (header.contentSize > maxSize) {
    throw resourceExhausted(
      `compressed message of ${header.contentSize} bytes exceeds maximum ${maxSize}`,
    );
  }
  const dictionary = accept.get(header.dictionaryId);
  if (dictionary === undefined) {
    throw internal(
      header.dictionaryId === 0
        ? "compressed message without a dictionary, which is not accepted"
        : `compressed message uses dictionary ${header.dictionaryId}, which is not accepted`,
    );
  }
  let message;
  try {
    message = dictionary.decompress(payload, header.contentSize);
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
 * Validates that payload is exactly one magicless zstd frame with a Frame_Content_Size, and
 * returns its Dictionary_ID (0 if absent or zero: compressed without a dictionary) and content
 * size as `{ dictionaryId, contentSize }`. It walks the block
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
  if ((descriptor & 0x08) !== 0 || fcsSize === 0) {
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
  for (;;) {
    const block = nextBlock(bytes, position);
    if (block === undefined) {
      throw invalid();
    }
    position = block.next;
    if (block.last) {
      break;
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

/**
 * Validates `chunk`, the zstd data of a ZSTD_STREAM_MESSAGE, and returns
 * `{ startsFrame, dictionaryId, endsFrame }`. `atFrameStart` says that no frame is open, so a
 * non-empty chunk must start with a frame header: one without Single_Segment_flag,
 * Frame_Content_Size_flag, Content_Checksum_flag or the reserved bit, whose window is at most
 * 2^maxWindowLog. It then walks the block headers: a chunk is whole blocks, and a Last_Block ends
 * both the chunk and the frame. An empty chunk is an empty message and changes nothing. Block
 * contents are not checked; decompression does that.
 */
export function parseStreamChunk(chunk, atFrameStart, maxWindowLog) {
  const bytes = byteView(chunk);
  const invalid = () => internal("invalid ZSTD_STREAM_MESSAGE payload");
  const result = { startsFrame: false, dictionaryId: 0, endsFrame: false };
  if (bytes.byteLength === 0) {
    return result;
  }
  let position = 0;
  if (atFrameStart) {
    const descriptor = bytes[0];
    // Frame_Content_Size_flag (bits 7-6), Single_Segment_flag (bit 5), the reserved bit (bit 3) and
    // Content_Checksum_flag (bit 2) must be unset.
    if ((descriptor & 0xec) !== 0 || bytes.byteLength < 2) {
      throw invalid();
    }
    const windowDescriptor = bytes[1];
    const base = 2 ** (10 + (windowDescriptor >> 3));
    if (base + (base / 8) * (windowDescriptor & 7) > 2 ** maxWindowLog) {
      throw invalid();
    }
    const didSize = DidSizes[descriptor & 0x03];
    position = 2 + didSize;
    if (bytes.byteLength < position) {
      throw invalid();
    }
    result.dictionaryId = readLittleEndian(bytes, 2, didSize);
    result.startsFrame = true;
  }
  while (position < bytes.byteLength) {
    const block = nextBlock(bytes, position);
    if (block === undefined || (block.last && block.next !== bytes.byteLength)) {
      throw invalid();
    }
    position = block.next;
    result.endsFrame = block.last;
  }
  return result;
}

/**
 * Walks the zstd block whose header starts at `position` and returns `{ next, last }`: where the
 * next block starts and whether this one is the Last_Block. Returns undefined for a reserved block
 * type or a block that does not fit. Block contents are not checked.
 */
function nextBlock(bytes, position) {
  if (bytes.byteLength - position < 3) {
    return undefined;
  }
  const header = bytes[position] | (bytes[position + 1] << 8) | (bytes[position + 2] << 16);
  let size = header >>> 3;
  switch ((header >>> 1) & 0x03) {
    case 0: // Raw_Block
    case 2: // Compressed_Block
      break;
    case 1: // RLE_Block
      size = 1;
      break;
    default:
      return undefined;
  }
  const next = position + 3 + size;
  return next > bytes.byteLength ? undefined : { next, last: (header & 1) !== 0 };
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
