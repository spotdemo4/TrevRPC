import type { Compression } from "./index.js";

/** A zstd dictionary and the protobuf message types it compresses. */
export interface DictionaryOptions {
  /**
   * The dictionary in zstd's format, as written by `zstd --train`. Its header carries the
   * Dictionary_ID that identifies it to peers, which must never name different dictionaries.
   */
  data: ArrayBuffer | ArrayBufferView;
  /**
   * Full names of the protobuf message types the dictionary compresses, such as
   * "example.v1.Position". A message type may appear in one dictionary only. A dictionary without
   * messages is still offered to servers, which may compress with it, but the channel never does;
   * adding a new dictionary that way first lets clients hold it before servers use it.
   */
  messages?: readonly string[];
}

export interface CompressionOptions {
  /** The dictionaries the channel holds. */
  dictionaries?: readonly DictionaryOptions[];
  /**
   * The zstd level of outgoing messages; defaults to 1. The module keeps only the strategies of
   * levels 1 to 4, and higher levels fall back to them.
   */
  level?: number;
}

/**
 * Creates compression settings for `connect()`. It loads libzstd as WebAssembly (about 85 KB
 * gzipped), digests the dictionaries, and rejects with a TypeError for an invalid dictionary, a
 * dictionary ID used twice, or a message type in two dictionaries. Settings without dictionaries
 * enable stream compression alone.
 */
export function createCompression(options?: CompressionOptions): Promise<Compression>;
