import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { measure, percentile, summarize } from "../interop-browser/bench.js";
import { Records, newPayload } from "../interop-browser/records.js";

const cli = fileURLToPath(new URL("../interop-browser/cli.js", import.meta.url));
const benchBundle = new URL("../build/interop/bench.js", import.meta.url);
const pageBundle = new URL("../build/interop/page.js", import.meta.url);

/** A clock that only operations advance, and an operation that takes durationMs of it. */
function fakeClock() {
  let time = 0;
  return {
    now: () => time,
    operation: (durationMs, carried, fails = () => false) => ({
      async run() {
        const began = time;
        time += durationMs;
        if (fails(began)) {
          throw new Error("failed");
        }
        return carried;
      },
      close() {},
    }),
  };
}

test("only operations that start and finish inside the window count", async () => {
  const clock = fakeClock();
  // Operations start at 0, 3, 6, 9, 12, 15, 18: those at 12 and 15 fit in [10, 20].
  const measured = await measure({
    operations: [clock.operation(3, 5)],
    start: 10,
    end: 20,
    now: clock.now,
    backoffMs: 0,
  });
  assert.deepEqual(measured, { latencies: [3000, 3000], messages: 10, errors: 0 });
});

test("failures inside the window are counted and the worker goes on", async () => {
  const clock = fakeClock();
  const measured = await measure({
    operations: [clock.operation(2, 1, (began) => began === 12)],
    start: 10,
    end: 20,
    now: clock.now,
    backoffMs: 0,
  });
  // 10 and 14, 16 succeed; 12 fails; 18 ends at 20 and still counts.
  assert.deepEqual(measured, { latencies: [2000, 2000, 2000, 2000], messages: 4, errors: 1 });
});

test("operations close when their worker stops", async () => {
  const clock = fakeClock();
  let closed = 0;
  const operation = { ...clock.operation(1, 1), close: () => closed++ };
  await measure({ operations: [operation, operation], start: 0, end: 5, now: clock.now });
  assert.equal(closed, 2);
});

test("the result line has the native clients' fields", () => {
  const input = {
    scenario: "unary",
    transport: "webtransport",
    size: 64,
    metadataSize: 0,
    payload: "records",
    compression: "stream",
  };
  const latencies = Array.from({ length: 1000 }, (_, i) => 1000 - i);
  const result = summarize(
    { ...input, concurrency: 4, duration: 2 },
    { latencies, messages: 1000, errors: 0 },
  );
  assert.deepEqual(result, {
    scenario: "unary",
    transport: "webtransport",
    size: 64,
    metadata_size: 0,
    metadata_scope: "call",
    concurrency: 4,
    connections: 1,
    duration_s: 2,
    ops: 1000,
    msgs: 1000,
    errors: 0,
    ops_per_sec: 500,
    msgs_per_sec: 500,
    bytes_per_sec: 32000,
    latency_us: { p50: 500, p90: 900, p99: 990, p999: 999 },
    lost: 0,
    payload: "records",
    compression: "stream",
  });
  assert.throws(
    () => summarize({ ...input, duration: 1 }, { latencies: [], messages: 0, errors: 3 }),
    /no operation completed in the 1s measured window \(3 failed\)/,
  );
});

test("records messages are bench/payload.py's, byte for byte", () => {
  // GOLDEN_SHA256 of bench/payload.py: worker 0's messages of these sizes, in turn.
  const records = new Records(0);
  const digest = createHash("sha256");
  for (const size of [64, 256, 1024, 4096]) {
    digest.update(records.message(size));
  }
  assert.equal(
    digest.digest("hex"),
    "1414f2e3835a891eecc8579d6736157e456eee7f4f3547b5a326b48f625a8a98",
  );

  // Each worker has its own messages; each message starts a new record.
  const first = newPayload("records", 300, 0);
  const second = newPayload("records", 300, 1);
  const message = first.next();
  assert.equal(message.length, 300);
  assert.notEqual(message, second.next());
  assert.match(first.next(), /^\{"ts":/);
  assert.equal(newPayload("repeat", 3, 0).next(), "xxx");
});

test("percentiles are nearest-rank", () => {
  assert.equal(percentile(Float64Array.of(7), 0.5), 7);
  assert.equal(percentile(Float64Array.of(1, 2, 3, 4), 0.5), 2);
  assert.equal(percentile(Float64Array.of(1, 2, 3, 4), 0.99), 4);
});

test("the bench command validates its arguments before launching a browser", () => {
  const webtransport = [
    "bench",
    "--browser",
    "chromium",
    "--transport",
    "webtransport",
    "--url",
    "https://127.0.0.1:4433/trevrpc",
    "--cert-hash",
    "ab".repeat(32),
  ];
  const connect = (lane, ...rest) => [
    "bench",
    "--browser",
    "chromium",
    "--transport",
    lane,
    "--url",
    "https://127.0.0.1:4433",
    ...rest,
  ];
  for (const bad of [
    ["bench"],
    [...webtransport, "--unknown"],
    webtransport.map((value) => (value === "chromium" ? "safari" : value)),
    webtransport.map((value) => (value === "webtransport" ? "quic" : value)),
    webtransport.filter((value, i) => i < 7),
    webtransport.map((value) => (value === "https://127.0.0.1:4433/trevrpc" ? "/trevrpc" : value)),
    [...webtransport, "--scenario", "unreliable-paced"],
    [...webtransport, "--compression", "gzip"],
    [...webtransport, "--compression", "dictionary"],
    [...webtransport, "--dictionary", "records.dict"],
    [...webtransport, "--compression", "stream", "--scenario", "server-stream"],
    [...webtransport, "--compression", "dictionary", "--dictionary", "/nonexistent/records.dict"],
    [...webtransport, "--payload", "random"],
    connect("connect-web", "--payload", "records", "--compression", "stream"),
    [...webtransport, "--duration", "0"],
    [...webtransport, "--warmup", "-1"],
    [...webtransport, "--size", "1.5"],
    [...webtransport, "--concurrency", "0"],
    connect("connect-web", "--scenario", "client-stream"),
    connect("grpc-web", "--scenario", "bidi"),
    connect("connect-web-h3"),
    connect("connect-web-h3", "--cert", "cert.pem").map((value) =>
      value === "chromium" ? "firefox" : value,
    ),
  ]) {
    const result = spawnSync(process.execPath, [cli, ...bad], { encoding: "utf8" });
    assert.equal(result.status, 2, bad.join(" "));
    assert.match(result.stderr, /usage: trevrpc-interop-browser bench/);
    assert.equal(result.stdout, "");
  }
});

test("the bench page is browser-only and the interop page leaves connect-web out", async () => {
  // The bench page is minified, which may quote strings with backticks.
  const bench = await readFile(benchBundle, "utf8");
  assert.match(bench, /["'`]\/input["'`]/);
  assert.match(bench, /["'`]\/done["'`]/);
  assert.match(bench, /nextHopProtocol/);
  assert.match(bench, /trevrpc\.interop\.v1\.Echo/);
  assert.doesNotMatch(bench, /from\s*["']node:|require\(["']node:|process\.binding/);
  assert.doesNotMatch(await readFile(pageBundle, "utf8"), /Connect-Protocol-Version/i);
});
