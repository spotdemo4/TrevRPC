import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { createServer } from "node:http";
import { createServer as createNetServer } from "node:net";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { parseBenchArguments } from "../interop-browser/bench-cli.js";
import { measure, percentile, runBench, summarize } from "../interop-browser/bench.js";
import { createJsonClient, jsonLines } from "../interop-browser/json-client.js";
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

/**
 * A loopback port that drops every connection it accepts. Unlike a port that was freed, it stays
 * taken for the whole test, so no test file running at the same time can come to listen on it.
 */
async function droppingPort(t) {
  const server = createNetServer((socket) => socket.destroy());
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => new Promise((resolve) => server.close(resolve)));
  return server.address().port;
}

test("a run that cannot connect says so, for run.py to run it again", async (t) => {
  const port = await droppingPort(t);
  const input = {
    scenario: "unary",
    size: 64,
    metadataSize: 0,
    concurrency: 1,
    duration: 1,
    warmup: 0,
    messages: 1,
    payload: "repeat",
    compression: "none",
  };
  await assert.rejects(
    runBench({
      ...input,
      transport: "websocket",
      url: `https://127.0.0.1:${port}`,
      webSocketUrl: `ws://127.0.0.1:${port}/trevrpc`,
    }),
    /^Error: could not connect: /,
  );
  await assert.rejects(
    runBench({ ...input, transport: "json", url: `http://127.0.0.1:${port}` }),
    /^Error: could not connect: /,
  );
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

test("the bench command validates its arguments before launching a browser", async () => {
  const webtransport = [
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
    "--browser",
    "chromium",
    "--transport",
    lane,
    "--url",
    "https://127.0.0.1:4433",
    ...rest,
  ];

  const parsed = await parseBenchArguments(webtransport);
  assert.equal(parsed.browser.name, "chromium");
  assert.deepEqual(
    parsed.certHash,
    Array.from({ length: 32 }, () => 0xab),
  );
  assert.equal(parsed.dictionary, undefined);
  assert.equal(parsed.duration, 5);

  // The message pins the check each case fails; cases with the same message fail different parts
  // of its condition.
  for (const [bad, message] of [
    [[], /^--browser must be/],
    [[...webtransport, "--unknown"], /Unknown option '--unknown'/],
    [webtransport.map((value) => (value === "chromium" ? "safari" : value)), /^--browser must be/],
    [webtransport.map((value) => (value === "webtransport" ? "quic" : value)), /^--transport must/],
    [webtransport.filter((value, i) => i < 6), /^--cert-hash must be/],
    [
      webtransport.map((value) =>
        value === "https://127.0.0.1:4433/trevrpc" ? "/trevrpc" : value,
      ),
      /^--url must be/,
    ],
    [[...webtransport, "--scenario", "unreliable-paced"], /^the webtransport lane runs/],
    [[...webtransport, "--compression", "gzip"], /^--compression must be/],
    [[...webtransport, "--compression", "dictionary"], /^--dictionary goes with/],
    [[...webtransport, "--dictionary", "records.dict"], /^--dictionary goes with/],
    [
      [...webtransport, "--compression", "stream", "--scenario", "server-stream"],
      /^--compression stream runs/,
    ],
    [
      [...webtransport, "--compression", "dictionary", "--dictionary", "/nonexistent/records.dict"],
      /^--dictionary: ENOENT/,
    ],
    [[...webtransport, "--payload", "random"], /^--payload must be/],
    [connect("connect-web", "--payload", "records", "--compression", "stream"), /not compress/],
    [[...webtransport, "--duration", "0"], /^--duration must be a positive/],
    // "--warmup -1" would fail as an ambiguous option before reaching the check.
    [[...webtransport, "--warmup=-1"], /^--warmup must be a non-negative/],
    [[...webtransport, "--size", "1.5"], /^--size must be an integer/],
    [[...webtransport, "--concurrency", "0"], /^--concurrency must be an integer of at least 1/],
    [connect("connect-web", "--scenario", "client-stream"), /^the connect-web lane runs/],
    [connect("connect-web-h3"), /^the connect-web-h3 lane needs --cert/],
    [
      connect("connect-web-h3", "--cert", "cert.pem").map((value) =>
        value === "chromium" ? "firefox" : value,
      ),
      /^firefox cannot be made to call over HTTP\/3/,
    ],
  ]) {
    await assert.rejects(parseBenchArguments(bad), { name: "UsageError", message }, bad.join(" "));
  }

  // The contract with bench/run.py: status 2, the usage on stderr, nothing on stdout.
  const result = spawnSync(process.execPath, [cli, "bench"], { encoding: "utf8" });
  assert.equal(result.status, 2);
  assert.match(result.stderr, /^trevrpc-interop-browser bench: --browser must be/);
  assert.match(result.stderr, /\nusage: trevrpc-interop-browser bench --browser/);
  assert.equal(result.stdout, "");
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

/** A body that delivers chunks as they are, in turn. */
function chunkedBody(...chunks) {
  return new ReadableStream({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(new TextEncoder().encode(chunk));
      }
      controller.close();
    },
  });
}

test("JSON lines split across and within chunks parse in order", async () => {
  const lines = await Array.fromAsync(
    jsonLines(chunkedBody('{"index":0}\n{"ind', 'ex":1}\n{"index":2}\n{"index":3}')),
  );
  assert.deepEqual(lines, [{ index: 0 }, { index: 1 }, { index: 2 }, { index: 3 }]);
  await assert.rejects(Array.fromAsync(jsonLines(chunkedBody('{"index":'))), SyntaxError);
});

test("the JSON client posts JSON and reads JSON and newline-delimited JSON back", async (t) => {
  const requests = [];
  const server = createServer(async (req, res) => {
    let body = "";
    for await (const chunk of req) {
      body += chunk;
    }
    requests.push({
      url: req.url,
      type: req.headers["content-type"],
      pad: req.headers["bench-pad"],
    });
    const { message, count } = JSON.parse(body);
    if (req.url === "/json/unary") {
      res.end(JSON.stringify({ message, index: 0 }));
    } else if (req.url === "/json/server-stream") {
      for (let index = 0; index < count; index++) {
        res.write(`${JSON.stringify({ message, index })}\n`);
      }
      res.end();
    } else {
      res.writeHead(404).end("no such route");
    }
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  t.after(() => server.close());
  const baseUrl = `http://127.0.0.1:${server.address().port}`;
  const client = createJsonClient(baseUrl);

  const message = '{"ts":"quoted"}';
  assert.equal(
    (await client.unary({ message }, { headers: { "bench-pad": "xx" } })).message,
    message,
  );
  const streamed = await Array.fromAsync(client.serverStream({ message, count: 3 }));
  assert.deepEqual(
    streamed.map((response) => response.index),
    [0, 1, 2],
  );
  assert.deepEqual(requests, [
    { url: "/json/unary", type: "application/json", pad: "xx" },
    { url: "/json/server-stream", type: "application/json", pad: undefined },
  ]);
  await assert.rejects(
    createJsonClient(`${baseUrl}/missing`).unary({ message }),
    /answered 404: no such route/,
  );
});
