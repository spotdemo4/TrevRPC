// Stream compression: each direction of a call may compress its messages with one zstd context
// that lasts as long as the call, sent as ZSTD_STREAM_MESSAGE frames (see the "Stream Compression"
// section of the wire format). A client offers it in its REQUEST on methods that allow it, and
// may compress its requests once the server's HEADERS answer. Contexts hold memory for the whole
// call, so every channel has a budget for them.
import { encodeMessage, parseStreamChunk } from "./compression.js";
import { FrameType } from "./framing.js";
import { internal, resourceExhausted } from "./status.js";
import { readVarint, varintLength, writeVarint } from "./varint.js";

/** The window, as a base-2 logarithm, the client's contexts use and its decoders accept. */
const StreamWindowLog = 16;
/** A peer advertising a smaller window enables nothing. */
const MinStreamWindowLog = 10;
/** A direction's context starts with this message; earlier ones are compressed per message. */
const StreamCompressionStart = 3;

/** The default memory budget of one channel's stream contexts, in bytes. */
export const DefaultStreamCompressionMemory = 64 * 1024 * 1024;

/** The key of a transport's StreamBudget; a Channel sets it from its connect options. */
export const StreamBudgetKey = Symbol("trevrpc.streamBudget");

/** The memory stream contexts may hold, shared by the calls of one channel. */
export class StreamBudget {
  #available;

  constructor(bytes = DefaultStreamCompressionMemory) {
    if (!(Number.isSafeInteger(bytes) && bytes >= 0)) {
      throw new TypeError("streamCompressionMemory must be a non-negative integer number of bytes");
    }
    this.#available = bytes;
  }

  /** Sets aside `bytes`, returning a function that gives them back, or undefined if they do not fit. */
  reserve(bytes) {
    if (bytes > this.#available) {
      return undefined;
    }
    this.#available -= bytes;
    let held = true;
    return () => {
      if (held) {
        held = false;
        this.#available += bytes;
      }
    };
  }
}

const defaultBudgets = new WeakMap();

/** The stream budget of a transport: a Channel's own, or a default one per other transport. */
export function streamBudget(transport) {
  const budget = transport[StreamBudgetKey];
  if (budget !== undefined) {
    return budget;
  }
  let fallback = defaultBudgets.get(transport);
  if (fallback === undefined) {
    fallback = new StreamBudget();
    defaultBudgets.set(transport, fallback);
  }
  return fallback;
}

/**
 * The stream compression of one call on the client. `directions` is `{ requests, responses }`:
 * the directions the method allows it on. `session` picks dictionaries for request contexts, and
 * `accept` is the dictionary the call accepts on its responses, which response frames may start
 * from.
 */
export class CallStreams {
  #compression;
  #budget;
  #requests;
  #responses;
  #session;
  #requestType;
  #accept;
  /** Whether the REQUEST offered stream compression. */
  #offered = false;
  /** The window the server's answer allows request contexts, or 0 until it arrives. */
  #requestWindowLog = 0;
  #requestsSent = 0;
  /** Whether the request direction stays per-message for the rest of the call. */
  #requestsStopped = false;
  #encoder;
  #decoder;
  /** Whether a response frame is open, so the next non-empty chunk continues it. */
  #inFrame = false;
  #closed = false;
  #releases = [];

  constructor(compression, budget, directions, { session, requestType, accept }) {
    this.#compression = compression;
    this.#budget = budget;
    this.#requests = directions.requests;
    this.#responses = directions.responses;
    this.#session = session;
    this.#requestType = requestType;
    this.#accept = accept;
  }

  /**
   * Sets the REQUEST's `streamCompression` offer. A call whose responses may be compressed first
   * reserves their decompressing context, and offers nothing if that does not fit; a call that
   * only compresses requests offers so the server can answer.
   */
  offer(fields) {
    if (this.#responses) {
      const release = this.#budget.reserve(this.#compression.streamDecoderCost(StreamWindowLog));
      if (release === undefined) {
        return;
      }
      this.#releases.push(release);
    }
    fields.streamCompression = { maxWindowLog: StreamWindowLog };
    this.#offered = true;
  }

  /** Records the server's answer from the response HEADERS. */
  learn(answer) {
    const windowLog = answer?.maxWindowLog ?? 0;
    if (this.#offered && this.#requests && windowLog >= MinStreamWindowLog) {
      this.#requestWindowLog = Math.min(StreamWindowLog, windowLog);
    }
  }

  /**
   * Encodes a request message: through the direction's context once it exists, and per message,
   * with `dictionary`, before. The context starts with the direction's third message if the
   * server's answer allows it and its memory fits the budget.
   */
  encode(body, dictionary) {
    if (!this.#requests) {
      return encodeMessage(body, dictionary);
    }
    this.#requestsSent += 1;
    if (
      this.#encoder === undefined &&
      !this.#requestsStopped &&
      this.#requestWindowLog > 0 &&
      this.#requestsSent >= StreamCompressionStart
    ) {
      this.#startEncoder();
    }
    if (this.#encoder === undefined) {
      return encodeMessage(body, dictionary);
    }
    const chunk = this.#encoder.encode(body);
    const payload = new Uint8Array(varintLength(body.byteLength) + chunk.byteLength);
    payload.set(chunk, writeVarint(payload, 0, body.byteLength));
    return { type: FrameType.ZstdStreamMessage, payload, size: body.byteLength };
  }

  #startEncoder() {
    const release = this.#budget.reserve(
      this.#compression.streamEncoderCost(this.#requestWindowLog),
    );
    if (release === undefined) {
      this.#requestsStopped = true;
      return;
    }
    try {
      this.#encoder = this.#compression.streamEncoder(
        this.#session.requestDictionary(this.#requestType),
        this.#requestWindowLog,
      );
    } catch {
      release();
      this.#requestsStopped = true;
      return;
    }
    this.#releases.push(release);
  }

  /**
   * Decompresses the payload of a ZSTD_STREAM_MESSAGE frame on the response. A message above
   * maxSize is ResourceExhausted, found before decompressing; every other fault is Internal, and
   * the call cannot continue after it.
   */
  receive(payload, maxSize) {
    if (!(this.#offered && this.#responses) || this.#closed) {
      throw internal("stream-compressed message where stream compression is not enabled");
    }
    const size = readVarint(payload, 0);
    if (size === null || typeof size.value !== "number") {
      throw internal("invalid ZSTD_STREAM_MESSAGE payload");
    }
    if (size.value > maxSize) {
      throw resourceExhausted(
        `compressed message of ${size.value} bytes exceeds maximum ${maxSize}`,
      );
    }
    const chunk = payload.subarray(size.next);
    const info = parseStreamChunk(chunk, !this.#inFrame, StreamWindowLog);
    let frameDictionary;
    if (info.startsFrame) {
      frameDictionary = null;
      if (info.dictionaryId !== 0) {
        if (info.dictionaryId !== this.#accept?.id) {
          throw internal(
            `stream-compressed message uses dictionary ${info.dictionaryId}, which is not accepted`,
          );
        }
        frameDictionary = this.#accept;
      }
    }
    let message;
    try {
      this.#decoder ??= this.#compression.streamDecoder(StreamWindowLog);
      message = this.#decoder.decode(chunk, size.value, frameDictionary);
    } catch {
      throw internal("invalid ZSTD_STREAM_MESSAGE payload");
    }
    if (chunk.byteLength > 0) {
      this.#inFrame = !info.endsFrame;
    }
    return message;
  }

  /** Frees the call's contexts and returns their memory; every way a call ends gets here. */
  close() {
    this.#closed = true;
    this.#encoder?.close();
    this.#decoder?.close();
    this.#encoder = undefined;
    this.#decoder = undefined;
    this.#requestsStopped = true;
    for (const release of this.#releases.splice(0)) {
      release();
    }
  }
}
