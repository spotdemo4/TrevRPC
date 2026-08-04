#!/usr/bin/env node

import { once } from "node:events";

import { dispatch } from "./operations.js";
import { MaxEventBytes, ProtocolVersion, parseCommand, readCommandLines } from "./protocol.js";

const Peer = "js";
const Capabilities = Object.freeze([
  "codec.decode",
  "codec.encode",
  "framing.decode_stream",
  "framing.encode",
  "state.client_stream",
  "state.server_stream",
]);

if (process.argv.length !== 4 || process.argv[2] !== "--protocol" || process.argv[3] !== "1") {
  process.stderr.write("usage: trevrpc-conformance-js --protocol 1\n");
  process.exitCode = 2;
} else {
  process.exitCode = await runPeer();
}

async function runPeer() {
  await emit({
    schema_version: ProtocolVersion,
    event: "ready",
    peer: Peer,
    pid: process.pid,
    capabilities: Capabilities,
  });

  try {
    for await (const line of readCommandLines(process.stdin)) {
      const command = parseCommand(line);
      if (command.stop) {
        return 0;
      }

      const { payload, error } = await dispatch(command);
      const result = {
        schema_version: ProtocolVersion,
        event: "result",
        peer: Peer,
        sequence: command.sequence,
        case_id: command.caseId,
        operation: command.operation,
        outcome: error == null ? "success" : "error",
        ...payload,
      };
      if (error != null) {
        process.stderr.write(`${command.caseId}: ${error.message}\n`);
      }
      await emit(result);
    }
  } catch (error) {
    const message = error?.message ?? String(error);
    try {
      await emit({
        schema_version: ProtocolVersion,
        event: "fatal",
        peer: Peer,
        message,
      });
    } catch {
      // The original failure remains the useful diagnostic when stdout is unavailable.
    }
    process.stderr.write(`${message}\n`);
    return 2;
  }

  return 2;
}

async function emit(event) {
  const line = `${JSON.stringify(event)}\n`;
  if (Buffer.byteLength(line) > MaxEventBytes) {
    throw new Error("protocol event exceeded limit");
  }
  if (!process.stdout.write(line)) {
    await once(process.stdout, "drain");
  }
}
