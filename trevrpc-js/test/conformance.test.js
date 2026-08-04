import assert from "node:assert/strict";
import { Readable } from "node:stream";
import test from "node:test";

import { dispatch } from "../conformance/operations.js";
import {
  MaxCommandBytes,
  ProtocolError,
  parseCommand,
  readCommandLines,
} from "../conformance/protocol.js";

const encode = (value) => Buffer.from(value, "ascii");

function command(line) {
  return parseCommand(encode(line));
}

async function run(line) {
  const parsed = command(line);
  const result = await dispatch(parsed);
  return result.payload;
}

test("strict command parser accepts canonical fields and rejects protocol drift", () => {
  const parsed = command("RUN\t1\tcase.id\tcodec.decode\trpc_request\t3001");
  assert.equal(parsed.sequence, "1");
  assert.equal(parsed.caseId, "case.id");
  assert.equal(parsed.operation, "codec.decode");
  assert.deepEqual(parsed.body, Buffer.from("3001", "hex"));

  for (const invalid of [
    "RUN\t01\tcase.id\tcodec.decode\trpc_request\t3001",
    "RUN\t1\tCase\tcodec.decode\trpc_request\t3001",
    "RUN\t1\tcase.id\tcodec.decode\trpc_request\tABC0",
    "RUN\t1\tcase.id\tcodec.decode\trpc_request\t3001\textra",
    "RUN\t1\tcase.id\tunknown\trpc_request\t3001",
    "STOP\textra",
  ]) {
    assert.throws(() => command(invalid), ProtocolError, invalid);
  }
  assert.throws(
    () => parseCommand(Buffer.from("RUN\t1\tcase.id\tcodec.decode\trpc_request\t3001\r", "ascii")),
    ProtocolError,
  );
});

test("command reader enforces the exact bounded LF protocol", async () => {
  const exact = Buffer.alloc(MaxCommandBytes, 0x61);
  const accepted = [];
  await assert.rejects(async () => {
    for await (const line of readCommandLines(Readable.from([exact, Buffer.from("\n")]))) {
      accepted.push(line);
    }
  }, /controller input ended without STOP/u);
  assert.equal(accepted.length, 1);
  assert.equal(accepted[0].byteLength, MaxCommandBytes);

  await assert.rejects(async () => {
    for await (const _line of readCommandLines(
      Readable.from([Buffer.alloc(MaxCommandBytes + 1, 0x61), Buffer.from("\n")]),
    )) {
      // The oversized line must fail before it is yielded.
    }
  }, /exceeded limit/u);
  await assert.rejects(async () => {
    for await (const _line of readCommandLines(Readable.from([Buffer.from("STOP")]))) {
      // Missing LF is fatal.
    }
  }, /not LF-terminated/u);
});

test("production wire preflight rejects known wrong types and preserves unknown fields", async () => {
  assert.deepEqual(await run("RUN\t1\trequest.wrong\tcodec.decode\trpc_request\t0801"), {
    outcome: "error",
    category: "malformed_protobuf",
    status_code: 3,
  });
  assert.deepEqual(await run("RUN\t2\tresponse.truncated\tcodec.decode\trpc_response\t80"), {
    outcome: "error",
    category: "malformed_protobuf",
    status_code: 13,
  });
  assert.deepEqual(await run("RUN\t3\tresponse.utf8\tcodec.decode\trpc_response\t1201ff"), {
    outcome: "error",
    category: "malformed_protobuf",
    status_code: 13,
  });

  const unknown = await run(
    "RUN\t4\trequest.unknown\tcodec.decode\trpc_request\t0a0373766312016d1a02686930014007",
  );
  assert.equal(unknown.outcome, undefined);
  assert.equal(unknown.canonical_body_hex, "0a0373766312016d1a0268693001");
});

test("normalization sorts metadata by key bytes rather than object enumeration", async () => {
  const result = await run(
    "RUN\t1\tmetadata.numeric\tcodec.decode\trpc_request\t22060a013212016122070a0231301201623001",
  );
  assert.deepEqual(result.message.metadata, [
    { key_hex: "3130", value_hex: "62" },
    { key_hex: "32", value_hex: "61" },
  ]);
});

test("framing dispatch uses bounded production raw-frame parsing", async () => {
  const decoded = await run(
    "RUN\t1\tframing.fragmented\tframing.decode_stream\trpc_stream_frame\t16\t4\t00\t0000\t0322\t0161",
  );
  assert.deepEqual(decoded, { bodies_hex: ["220161"], eof: true });
  assert.deepEqual(
    await run(
      "RUN\t2\tframing.partial\tframing.decode_stream\trpc_stream_frame\t16\t1\t000000032201",
    ),
    { outcome: "error", category: "incomplete_frame", status_code: 13 },
  );
  assert.deepEqual(
    await run("RUN\t3\tframing.large\tframing.decode_stream\trpc_stream_frame\t16\t1\t00000011"),
    { outcome: "error", category: "frame_too_large", status_code: 8 },
  );
});

test("production stream state verifies FIN, trailing frames, and remote precedence", async () => {
  assert.deepEqual(await run("RUN\t1\tclient.one\tstate.client_stream\t2\t22031a0161\t0801"), {
    response_body_hex: "1a0161",
  });
  assert.deepEqual(await run("RUN\t2\tclient.empty\tstate.client_stream\t0"), {
    outcome: "error",
    category: "missing_terminal_status",
    status_code: 13,
  });
  assert.deepEqual(
    await run("RUN\t3\tclient.two\tstate.client_stream\t3\t22031a0161\t22031a0162\t0801"),
    { outcome: "error", category: "response_cardinality", status_code: 13 },
  );
  assert.deepEqual(
    await run("RUN\t4\tclient.remote\tstate.client_stream\t2\t22031a0161\t0801100e1a04646f776e"),
    { outcome: "error", category: "remote_status", status_code: 14 },
  );
  assert.deepEqual(await run("RUN\t5\tclient.trailing\tstate.client_stream\t2\t0801\t22031a0161"), {
    outcome: "error",
    category: "trailing_frame",
    status_code: 13,
  });

  assert.deepEqual(
    await run("RUN\t6\tclient.trailing-malformed\tstate.client_stream\t3\t22031a0161\t0801\t80"),
    { outcome: "error", category: "trailing_frame", status_code: 13 },
  );

  const server = await run("RUN\t7\tserver.trailing\tstate.server_stream\t2\t0801\t22031a0161");
  assert.deepEqual(server, {
    outcome: "error",
    category: "trailing_frame",
    status_code: 13,
    transport_close_count: "1",
  });
  assert.deepEqual(
    await run("RUN\t8\tserver.trailing-malformed\tstate.server_stream\t2\t0801\t80"),
    {
      outcome: "error",
      category: "trailing_frame",
      status_code: 13,
      transport_close_count: "1",
    },
  );
});

test("unknown terminal statuses normalize at the public status boundary", async () => {
  const decoded = await run(
    "RUN\t1\tstream.unknown\tcodec.decode\trpc_stream_frame\t080110e7071a036f6464",
  );
  assert.equal(decoded.message.status_raw, "999");
  assert.equal(decoded.message.status_code, 2);
  assert.equal(decoded.canonical_body_hex, "080110e7071a036f6464");

  const state = await run("RUN\t2\tserver.unknown\tstate.server_stream\t1\t080110e7071a036f6464");
  assert.deepEqual(state, {
    outcome: "error",
    category: "remote_status",
    status_code: 2,
    transport_close_count: "1",
  });
});
