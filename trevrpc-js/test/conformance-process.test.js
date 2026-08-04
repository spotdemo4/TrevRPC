import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import test from "node:test";

const executable = new URL("../conformance/trevrpc-conformance-js.js", import.meta.url);

function runPeer(input) {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["--no-addons", executable.pathname, "--protocol", "1"], {
      stdio: ["pipe", "pipe", "pipe"],
    });
    const stdout = [];
    const stderr = [];
    child.stdout.on("data", (chunk) => stdout.push(chunk));
    child.stderr.on("data", (chunk) => stderr.push(chunk));
    child.on("error", reject);
    child.on("close", (code, signal) => {
      resolve({
        code,
        signal,
        stdout: Buffer.concat(stdout).toString("utf8"),
        stderr: Buffer.concat(stderr).toString("utf8"),
      });
    });
    child.stdin.on("error", (error) => {
      if (error.code !== "EPIPE") {
        reject(error);
      }
    });
    child.stdin.end(input);
  });
}

function events(output) {
  return output
    .trimEnd()
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line));
}

test("process peer emits strict ready and result events then stops cleanly", async () => {
  const result = await runPeer(
    "RUN\t1\trpc_request.default.decode\tcodec.decode\trpc_request\t3001\nSTOP\n",
  );
  assert.equal(result.code, 0);
  assert.equal(result.signal, null);
  assert.equal(result.stderr, "");
  const output = events(result.stdout);
  assert.deepEqual(Object.keys(output[0]).sort(), [
    "capabilities",
    "event",
    "peer",
    "pid",
    "schema_version",
  ]);
  assert.deepEqual(output[0].capabilities, [
    "codec.decode",
    "codec.encode",
    "framing.decode_stream",
    "framing.encode",
    "state.client_stream",
    "state.server_stream",
  ]);
  assert.equal(output[0].peer, "js");
  assert.equal(output[1].event, "result");
  assert.equal(output[1].outcome, "success");
  assert.equal(output[1].canonical_body_hex, "3001");
});

test("process peer emits exactly one strict fatal event for malformed input", async () => {
  for (const input of [
    "RUN\t01\tcase.id\tcodec.decode\trpc_request\t3001\n",
    "STOP\r\n",
    "STOP",
    Buffer.concat([Buffer.alloc(262_145, 0x61), Buffer.from("\n")]),
  ]) {
    const result = await runPeer(input);
    assert.equal(result.code, 2);
    const output = events(result.stdout);
    assert.equal(output.length, 2);
    assert.equal(output[0].event, "ready");
    assert.deepEqual(Object.keys(output[1]).sort(), ["event", "message", "peer", "schema_version"]);
    assert.equal(output[1].event, "fatal");
    assert.equal(output[1].peer, "js");
    assert.notEqual(result.stderr, "");
  }
});

test("process peer is deterministic across repeated no-addon runs", async () => {
  const input = [
    "RUN\t1\trequest.unknown\tcodec.decode\trpc_request\t0a0373766312016d1a02686930014007",
    "RUN\t2\tclient.one\tstate.client_stream\t2\t22031a0161\t0801",
    "RUN\t3\tserver.trailing\tstate.server_stream\t2\t0801\t22031a0161",
    "STOP",
    "",
  ].join("\n");
  const first = await runPeer(input);
  const second = await runPeer(input);
  assert.equal(first.code, 0);
  assert.equal(second.code, 0);

  const normalize = (result) =>
    events(result.stdout).map((event) => {
      const copy = { ...event };
      delete copy.pid;
      return copy;
    });
  assert.deepEqual(normalize(first), normalize(second));
});
