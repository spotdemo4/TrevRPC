// The zstd codec of compressed messages, the `@trevrpc/trevrpc-js/zstd` entry point. It runs the
// reference libzstd as WebAssembly (about 85 KB gzipped, built from trevrpc-js/zstd-wasm), so it
// is a separate entry point that applications without compression never load.
import { byteView } from "./bytes.js";
import wasmBase64 from "./zstd-wasm.js";

/** The magic number of zstd-format dictionaries (RFC 8878, section 5). */
const DictionaryMagic = 0xec30a437;
const DefaultLevel = 1;
const FullName = /^[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*$/;
/** The smallest scratch buffer the codec allocates. */
const MinScratchSize = 4096;

/**
 * Creates the compression settings of a channel: the dictionaries it holds, each with the
 * protobuf message types it compresses, and the zstd level of outgoing messages. Settings without
 * dictionaries still enable stream compression on the methods that allow it.
 */
export async function createCompression({ dictionaries = [], level = DefaultLevel } = {}) {
  if (!Number.isInteger(level)) {
    throw new TypeError("compression level must be an integer");
  }
  const parsed = dictionaries.map(parseDictionary);
  const ids = new Set();
  const types = new Set();
  for (const { id, messages } of parsed) {
    if (ids.has(id)) {
      throw new TypeError(`compression dictionaries share ID ${id}`);
    }
    ids.add(id);
    for (const name of messages) {
      if (types.has(name)) {
        throw new TypeError(`message type ${name} is in two compression dictionaries`);
      }
      types.add(name);
    }
  }
  const fingerprints = await Promise.all(parsed.map(({ data }) => fingerprintOf(data)));
  const codec = await Codec.create(level === 0 ? DefaultLevel : level);
  return new Compression(
    parsed.map((dictionary, index) => codec.dictionary(dictionary, fingerprints[index])),
    codec,
  );
}

/** Validates one dictionary option as `{ id, data, messages }`. */
function parseDictionary({ data, messages = [] }, index) {
  const bytes = byteView(data);
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (bytes.byteLength < 8 || view.getUint32(0, true) !== DictionaryMagic) {
    throw new TypeError(`compression dictionary ${index} is not a zstd-format dictionary`);
  }
  const id = view.getUint32(4, true);
  if (id === 0) {
    throw new TypeError(`compression dictionary ${index} has dictionary ID 0`);
  }
  const names = [...messages];
  for (const name of names) {
    if (typeof name !== "string" || !FullName.test(name)) {
      throw new TypeError(
        `compression dictionary ${index}: invalid message type ${JSON.stringify(name)}`,
      );
    }
  }
  return { id, data: bytes, messages: names };
}

/** A dictionary's fingerprint: the first 8 bytes of its SHA-256 digest. */
async function fingerprintOf(data) {
  return new Uint8Array(await crypto.subtle.digest("SHA-256", data), 0, 8);
}

/** Compression settings; see createCompression. Channels use them through these methods only. */
class Compression {
  #byId = new Map();
  #byType = new Map();
  #codec;

  constructor(dictionaries, codec) {
    this.#codec = codec;
    for (const dictionary of dictionaries) {
      this.#byId.set(dictionary.id, dictionary);
      for (const name of dictionary.messages) {
        this.#byType.set(name, dictionary);
      }
    }
    /** Every dictionary as `{ id, fingerprint }`, a client's dictionary_query. */
    this.query = Object.freeze(dictionaries.map(({ id, fingerprint }) => ({ id, fingerprint })));
    Object.freeze(this);
  }

  /** The dictionary with Dictionary_ID id, or undefined. */
  dictionary(id) {
    return this.#byId.get(id);
  }

  /** The dictionary that compresses messages of the full protobuf type name, or undefined. */
  dictionaryFor(name) {
    return this.#byType.get(name);
  }

  /**
   * Creates the compressing context of one direction of a call, with a window of 2^windowLog
   * bytes, starting from `dictionary` unless it is undefined. Its `encode(body)` returns the
   * chunk of one message and throws if the context failed; `close()` frees it.
   */
  streamEncoder(dictionary, windowLog) {
    return this.#codec.streamEncoder(dictionary, windowLog);
  }

  /**
   * Creates the decompressing context of one direction of a call, for windows of at most
   * 2^windowLogMax bytes. Its `decode(chunk, size, frameDictionary)` decompresses a validated chunk
   * into exactly size bytes or throws; `frameDictionary` is undefined for a chunk inside a frame,
   * and the frame's dictionary, or null for none, for one that starts a frame. `close()` frees it.
   */
  streamDecoder(windowLogMax) {
    return this.#codec.streamDecoder(windowLogMax);
  }

  /** libzstd's estimate of the memory of one stream compressing context. */
  streamEncoderCost(windowLog) {
    return this.#codec.streamEncoderCost(windowLog);
  }

  /** libzstd's estimate of the memory of one stream decompressing context. */
  streamDecoderCost(windowLogMax) {
    return this.#codec.streamDecoderCost(windowLogMax);
  }
}

let compiled;

/** Compiles the module once per page; every Compression instantiates its own copy. */
function compileModule() {
  compiled ??= WebAssembly.compile(decodeBase64(wasmBase64));
  return compiled;
}

function decodeBase64(text) {
  if (typeof Uint8Array.fromBase64 === "function") {
    return Uint8Array.fromBase64(text);
  }
  const binary = atob(text);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}

/**
 * One instance of the libzstd module: a compression and a decompression context for per-message
 * frames, the digested dictionaries, the stream contexts of open calls, and two scratch buffers in
 * its memory. JavaScript runs one call at a time, so the per-message contexts and the buffers are
 * shared by every message; they and the dictionaries live as long as the instance.
 */
class Codec {
  #exports;
  #level;
  #cctx;
  #dctx;
  /** The libzstd handles of each digested dictionary, `{ cdict, ddict }`. */
  #handles = new WeakMap();
  #input = { pointer: 0, size: 0 };
  #output = { pointer: 0, size: 0 };

  static async create(level) {
    const instance = await WebAssembly.instantiate(await compileModule(), {});
    return new Codec(instance.exports, level);
  }

  constructor(exports, level) {
    this.#exports = exports;
    this.#level = level;
    this.#cctx = exports.zs_cctx_new();
    this.#dctx = exports.zs_dctx_new();
    if (this.#cctx === 0 || this.#dctx === 0) {
      throw new Error("zstd: cannot create contexts");
    }
  }

  /** Digests a parsed dictionary for compression at the codec's level and for decompression. */
  dictionary({ id, data, messages }, fingerprint) {
    const pointer = this.#copyIn(this.#input, data);
    const cdict = this.#exports.zs_cdict_new(pointer, data.byteLength, this.#level);
    const ddict = this.#exports.zs_ddict_new(pointer, data.byteLength);
    if (cdict === 0 || ddict === 0) {
      throw new TypeError(`compression dictionary ${id} cannot be loaded`);
    }
    const dictionary = Object.freeze({
      id,
      fingerprint,
      messages: Object.freeze(messages),
      compress: (body) => this.#compress(cdict, body),
      decompress: (payload, size) => this.#decompress(ddict, payload, size),
    });
    this.#handles.set(dictionary, { cdict, ddict });
    return dictionary;
  }

  /** See Compression.streamEncoder. */
  streamEncoder(dictionary, windowLog) {
    const cdict = dictionary === undefined ? 0 : this.#handles.get(dictionary).cdict;
    const cctx = this.#exports.zs_stream_encoder_new(this.#level, windowLog, cdict);
    if (cctx === 0) {
      throw new Error("zstd: cannot create a stream context");
    }
    let open = true;
    return {
      encode: (body) => this.#streamCompress(cctx, body),
      close: () => {
        if (open) {
          open = false;
          this.#exports.zs_stream_encoder_free(cctx);
        }
      },
    };
  }

  /** See Compression.streamDecoder. */
  streamDecoder(windowLogMax) {
    const dctx = this.#exports.zs_stream_decoder_new(windowLogMax);
    if (dctx === 0) {
      throw new Error("zstd: cannot create a stream context");
    }
    let open = true;
    return {
      decode: (chunk, size, frameDictionary) =>
        this.#streamDecompress(dctx, chunk, size, frameDictionary),
      close: () => {
        if (open) {
          open = false;
          this.#exports.zs_stream_decoder_free(dctx);
        }
      },
    };
  }

  streamEncoderCost(windowLog) {
    return this.#exports.zs_stream_encoder_cost(this.#level, windowLog) >>> 0;
  }

  streamDecoderCost(windowLogMax) {
    return this.#exports.zs_stream_decoder_cost(windowLogMax) >>> 0;
  }

  /** Compresses one message through a stream context into its chunk, or throws. */
  #streamCompress(cctx, body) {
    // Room for the frame header and block headers on top of the bound.
    const capacity = this.#exports.zs_compress_bound(body.byteLength) + 64;
    const output = this.#reserve(this.#output, capacity);
    const input = this.#copyIn(this.#input, body);
    const size = this.#exports.zs_stream_compress(cctx, output, capacity, input, body.byteLength);
    if (size < 0) {
      throw new Error("zstd: stream compression failed");
    }
    return this.#view(output, size).slice();
  }

  /** Decompresses one validated chunk through a stream context into size bytes, or throws. */
  #streamDecompress(dctx, chunk, size, frameDictionary) {
    if (frameDictionary !== undefined) {
      const ddict = frameDictionary === null ? 0 : this.#handles.get(frameDictionary).ddict;
      if (this.#exports.zs_stream_decoder_ref(dctx, ddict) < 0) {
        throw new Error("zstd: cannot reference the dictionary");
      }
    }
    if (chunk.byteLength === 0) {
      if (size !== 0) {
        throw new Error("zstd: empty chunk for a non-empty message");
      }
      return new Uint8Array(0);
    }
    // One spare byte: libzstd holds back output that does not fit, so a message longer than its
    // declared size only shows as that byte.
    const capacity = size + 1;
    const output = this.#reserve(this.#output, capacity);
    const input = this.#copyIn(this.#input, chunk);
    const written = this.#exports.zs_stream_decompress(
      dctx,
      output,
      capacity,
      input,
      chunk.byteLength,
    );
    if (written !== size) {
      throw new Error("zstd: stream decompression failed");
    }
    return this.#view(output, size).slice();
  }

  /**
   * Compresses body into a new payload, or returns undefined if that fails, and the message then
   * travels uncompressed.
   */
  #compress(cdict, body) {
    const bound = this.#exports.zs_compress_bound(body.byteLength);
    let output;
    let input;
    try {
      output = this.#reserve(this.#output, bound);
      input = this.#copyIn(this.#input, body);
    } catch {
      return undefined;
    }
    const size = this.#exports.zs_compress(
      this.#cctx,
      cdict,
      output,
      bound,
      input,
      body.byteLength,
    );
    return size < 0 ? undefined : this.#view(output, size).slice();
  }

  /** Decompresses a validated payload into a new message of exactly size bytes, or throws. */
  #decompress(ddict, payload, size) {
    const output = this.#reserve(this.#output, size);
    const input = this.#copyIn(this.#input, payload);
    const written = this.#exports.zs_decompress(
      this.#dctx,
      ddict,
      output,
      size,
      input,
      payload.byteLength,
    );
    if (written !== size) {
      throw new Error("zstd: decompression failed");
    }
    return this.#view(output, size).slice();
  }

  /** Copies bytes into buffer, growing it as needed, and returns its pointer. */
  #copyIn(buffer, bytes) {
    const pointer = this.#reserve(buffer, bytes.byteLength);
    this.#view(pointer, bytes.byteLength).set(bytes);
    return pointer;
  }

  /** Returns the pointer of buffer after growing it to at least size bytes. */
  #reserve(buffer, size) {
    if (buffer.size < size) {
      if (buffer.size > 0) {
        this.#exports.zs_free(buffer.pointer, buffer.size);
      }
      const grown = Math.max(size, buffer.size * 2, MinScratchSize);
      buffer.pointer = this.#exports.zs_alloc(grown) >>> 0;
      buffer.size = buffer.pointer === 0 ? 0 : grown;
      if (buffer.pointer === 0) {
        throw new RangeError(`zstd: cannot allocate ${grown} bytes`);
      }
    }
    return buffer.pointer;
  }

  /** A view of linear memory; growing the memory detaches earlier views. */
  #view(pointer, size) {
    return new Uint8Array(this.#exports.memory.buffer, pointer, size);
  }
}
