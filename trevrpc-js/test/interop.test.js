import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { readFile } from "node:fs/promises";
import test from "node:test";
import { fileURLToPath } from "node:url";

import { ScenarioNames } from "../interop-browser/names.js";
import { runScenarios } from "../interop-browser/scenarios.js";
import { encodeMessageFrame } from "../src/framing.js";
import { Code } from "../src/status.js";
import { createEchoHandler, echoHandler } from "./echo-server.js";
import { fakeDatagrams, fakeTransport } from "./support.js";

const cli = fileURLToPath(new URL("../interop-browser/cli.js", import.meta.url));
const pageBundle = new URL("../build/interop/page.js", import.meta.url);
const url = "https://127.0.0.1:4433/trevrpc";

/** Installs a WebTransport global that serves sessions with handler, as a browser would. */
function withWebTransport(handler, body) {
  const sessions = [];
  class FakeWebTransport {
    constructor(sessionUrl, options) {
      this.url = sessionUrl;
      this.options = options;
      this.ready = Promise.resolve();
      this.closed = new Promise(() => {});
      this.datagrams = fakeDatagrams();
      this.streams = fakeTransport((stream) => handler(stream, this));
      sessions.push(this);
    }

    close() {}

    createBidirectionalStream() {
      return this.streams.createBidirectionalStream();
    }
  }

  globalThis.WebTransport = FakeWebTransport;
  return body(sessions).finally(() => delete globalThis.WebTransport);
}

test("the interop scenarios pass against a spec-conforming Echo server", () =>
  withWebTransport(echoHandler, async (sessions) => {
    const reported = [];
    const certHash = Array.from({ length: 32 }, (_, index) => index);
    const results = await runScenarios({
      url,
      certHash,
      onResult: (result) => reported.push(result.name),
    });

    assert.deepEqual(
      results,
      ScenarioNames.map((name) => ({ name, ok: true })),
    );
    assert.deepEqual(reported, [...ScenarioNames]);
    assert.equal(sessions.length, 1);
    assert.equal(sessions[0].url, url);
    assert.deepEqual(sessions[0].options.serverCertificateHashes, [
      { algorithm: "sha-256", value: Uint8Array.from(certHash) },
    ]);
  }));

test("scenarios fail individually when the server misbehaves", () =>
  withWebTransport(
    // A unary server that ignores request metadata and never reports errors.
    createEchoHandler({
      unary: async (stream) => {
        await stream.write(encodeMessageFrame(Uint8Array.of(0x0a, 5, ...Buffer.from("hello"))));
        await stream.writeStatus();
      },
    }),
    async () => {
      const results = await runScenarios({ url });
      const failed = Object.fromEntries(
        results.filter((result) => !result.ok).map((result) => [result.name, result.reason]),
      );

      assert.deepEqual(Object.keys(failed), [
        "unary-metadata",
        "unary-error",
        "unary-large",
        "unary-headers",
      ]);
      assert.match(failed["unary-metadata"], /expected \(hello!, 0\), got \(hello, 0\)/);
      assert.match(failed["unary-error"], /expected status NotFound "missing", got OK/);
      assert.match(failed["unary-large"], /\[1048576 chars\]/);
      assert.match(failed["unary-headers"], /expected trailing metadata .*, got \{\}/);
      for (const reason of Object.values(failed)) {
        assert.doesNotMatch(reason, /\n/);
      }
    },
  ));

test("the unreliable scenario needs datagrams on WebTransport", async () => {
  class NoDatagramsWebTransport {
    ready = Promise.resolve();
    closed = new Promise(() => {});
    streams = fakeTransport(echoHandler);
    close() {}
    createBidirectionalStream() {
      return this.streams.createBidirectionalStream();
    }
  }
  globalThis.WebTransport = NoDatagramsWebTransport;
  try {
    const results = await runScenarios({ url });
    const failed = results.filter((result) => !result.ok);
    assert.deepEqual(
      failed.map((result) => result.name),
      ["unreliable"],
    );
    assert.match(failed[0].reason, /the server received no request as a datagram/);
  } finally {
    delete globalThis.WebTransport;
  }
});

test("a server that cannot be reached fails every scenario", async () => {
  const results = await runScenarios({ url });
  assert.deepEqual(
    results.map((result) => result.name),
    [...ScenarioNames],
  );
  for (const result of results) {
    assert.equal(result.ok, false);
    assert.match(result.reason, /^connect: Unavailable .*WebTransport is not available/);
  }
});

test("a server that closes the session during connect fails every scenario", async () => {
  globalThis.WebTransport = class {
    ready = Promise.reject(new Error("handshake rejected"));
    closed = Promise.resolve();
    close() {}
  };
  try {
    const results = await runScenarios({ url });
    assert.equal(results.length, ScenarioNames.length);
    assert.match(results[0].reason, /connect: .*handshake rejected/);
  } finally {
    delete globalThis.WebTransport;
  }
});

test("the page bundle is browser-only and reports to the CLI", async () => {
  const bundle = await readFile(pageBundle, "utf8");
  assert.match(bundle, /"\/input"/);
  assert.match(bundle, /"\/result"/);
  assert.doesNotMatch(bundle, /from\s*["']node:|require\(["']node:|process\.binding/);
  assert.equal(Code.Ok, 0);
});

test("the CLI validates its arguments before launching a browser", () => {
  const valid = ["--browser", "chromium", "--url", url, "--cert-hash", "ab".repeat(32)];
  const arguments_ = (replace) => {
    const copy = [...valid];
    for (const [flag, value] of Object.entries(replace)) {
      const index = copy.indexOf(flag);
      if (value === null) {
        copy.splice(index, 2);
      } else {
        copy[index + 1] = value;
      }
    }
    return copy;
  };

  for (const bad of [
    [],
    arguments_({ "--browser": "netscape" }),
    arguments_({ "--browser": "webkit" }),
    arguments_({ "--browser": null }),
    arguments_({ "--url": null }),
    arguments_({ "--cert-hash": "abcd" }),
    arguments_({ "--cert-hash": null }),
    [...valid, "--unknown"],
  ]) {
    const result = spawnSync(process.execPath, [cli, ...bad], { encoding: "utf8" });
    assert.equal(result.status, 2, bad.join(" "));
    assert.match(result.stderr, /usage: trevrpc-interop-browser/);
    assert.equal(result.stdout, "");
  }
});

test("the CLI fails every scenario cleanly when safaridriver cannot start", () => {
  const result = spawnSync(
    process.execPath,
    [cli, "--browser", "safari", "--url", url, "--cert-hash", "ab".repeat(32)],
    {
      encoding: "utf8",
      env: { ...process.env, TREVRPC_BROWSER_SAFARI: "/nonexistent/safaridriver" },
    },
  );
  assert.equal(result.status, 1, result.stderr);
  assert.deepEqual(
    result.stdout.trim().split("\n"),
    ScenarioNames.map(
      (name) => `fail ${name}: cannot start safaridriver: spawn /nonexistent/safaridriver ENOENT`,
    ),
  );
});
