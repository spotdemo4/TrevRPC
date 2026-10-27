// The compression settings of the browser pages' channels, which the interop scenarios and the
// load benchmark share.
import { createCompression } from "../src/zstd.js";

/** The message types the interop dictionary compresses. */
const EchoTypes = ["trevrpc.interop.v1.EchoRequest", "trevrpc.interop.v1.EchoResponse"];

/**
 * Compression settings with dictionary, the interop dictionary in base64, for Echo messages, or
 * without a dictionary when it is undefined. Either way they stream-compress where methods allow.
 */
export function interopCompression(dictionary) {
  return createCompression({
    dictionaries: dictionary ? [{ data: base64Bytes(dictionary), messages: EchoTypes }] : [],
  });
}

function base64Bytes(text) {
  return Uint8Array.from(atob(text), (character) => character.charCodeAt(0));
}
