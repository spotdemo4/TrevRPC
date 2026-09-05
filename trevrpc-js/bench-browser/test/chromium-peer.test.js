import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { parseCommandLine, parseConnectCommand } from "../trevrpc-bench-peer-chromium.js";

const execFileAsync = promisify(execFile);
const peer = fileURLToPath(new URL("../trevrpc-bench-peer-chromium.js", import.meta.url));

test("Chromium peer advertises V5 client-only WebTransport capabilities", async () => {
  const { stdout, stderr } = await execFileAsync(process.execPath, [peer, "capabilities"]);
  assert.equal(stderr, "");
  assert.deepEqual(JSON.parse(stdout), {
    schema_version: 5,
    event: "capabilities",
    roles: { client: ["trevrpc_webtransport"] },
    rpc_kinds: ["unary", "client_stream", "server_stream", "bidi"],
    histogram: "log_linear_v1",
    peer: "chromium",
  });
});

test("Chromium client parses workload without an initial address", () => {
  assert.deepEqual(
    parseCommandLine([
      "client",
      "--stack",
      "trevrpc_webtransport",
      "--cert",
      "server.pem",
      "--rpc",
      "bidi",
      "--concurrency",
      "8",
      "--warmup-ms",
      "100",
      "--measurement-ms",
      "1000",
      "--request-bytes",
      "64",
      "--response-bytes",
      "128",
      "--messages-per-stream",
      "4",
    ]),
    {
      command: "client",
      stack: "trevrpc_webtransport",
      cert: "server.pem",
      rpcKind: "bidi",
      concurrency: 8,
      warmupMs: 100,
      measurementMs: 1000,
      requestBytes: 64,
      responseBytes: 128,
      messagesPerStream: 4,
    },
  );
  assert.throws(
    () =>
      parseCommandLine(["client", "--stack", "trevrpc_webtransport", "--address", "127.0.0.1:443"]),
    /unknown option --address/u,
  );
  assert.throws(() => parseCommandLine(["server"]), /usage:/u);
});

test("Chromium peer accepts only a valid CONNECT address", () => {
  assert.equal(parseConnectCommand("CONNECT 127.0.0.1:443"), "127.0.0.1:443");
  assert.equal(parseConnectCommand("CONNECT [::1]:443"), "[::1]:443");
  assert.throws(() => parseConnectCommand("START"), /expected CONNECT HOST:PORT/u);
  assert.throws(() => parseConnectCommand("CONNECT 127.0.0.1:0"), /port from 1 through 65535/u);
});
