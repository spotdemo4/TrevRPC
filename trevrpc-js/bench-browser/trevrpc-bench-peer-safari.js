#!/usr/bin/env node

import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  PeerError,
  createPeerMain,
  parseCommandLine as parseCommandLineGeneric,
  parseConnectCommand,
} from "./browser-peer.js";

const Peer = "safari";

export { PeerError, parseConnectCommand };
export function parseCommandLine(argv) {
  return parseCommandLineGeneric(argv, Peer);
}
export const main = createPeerMain(Peer);

function isMainModule() {
  return process.argv[1] != null && resolve(process.argv[1]) === fileURLToPath(import.meta.url);
}

if (isMainModule()) {
  try {
    await main();
  } catch (error) {
    const peerError =
      error instanceof PeerError
        ? error
        : new PeerError("run", "peer_failed", error?.message ?? String(error), { cause: error });
    console.error(`trevrpc-bench-peer-safari: ${peerError.message}`);
    try {
      const { SchemaVersion } = await import("trevrpc-bench-peer-js/common");
      const line = `${JSON.stringify({ schema_version: SchemaVersion, event: "error", phase: peerError.phase, code: peerError.code, message: peerError.message, peer: Peer })}\n`;
      await new Promise((resolveWrite, rejectWrite) => {
        process.stdout.write(line, (err) => (err == null ? resolveWrite() : rejectWrite(err)));
      });
    } catch (writeError) {
      console.error(
        `trevrpc-bench-peer-safari: could not write error event: ${writeError.message}`,
      );
    }
    process.exitCode = 1;
  }
}
