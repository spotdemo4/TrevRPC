import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import {
  browserRoles,
  webkitLaunchError,
  webkitLaunchOptions,
  webkitRuntimeDiagnostics,
} from "../browser-peer.js";
import { parseConnectCommand as parseConnectChromium } from "../trevrpc-bench-peer-chromium.js";
import { parseCommandLine as parseChromium } from "../trevrpc-bench-peer-chromium.js";
import {
  parseCommandLine as parseFirefox,
  parseConnectCommand as parseConnectFirefox,
} from "../trevrpc-bench-peer-firefox.js";
import {
  parseCommandLine as parseWebkit,
  parseConnectCommand as parseConnectWebkit,
} from "../trevrpc-bench-peer-webkit.js";

const execFileAsync = promisify(execFile);

test("Linux WPE does not advertise WebTransport support", () => {
  assert.deepEqual(browserRoles("webkit", "linux"), {});
  assert.deepEqual(browserRoles("webkit", "darwin"), {
    client: ["trevrpc_webtransport"],
  });
  assert.deepEqual(browserRoles("chromium", "linux"), {
    client: ["trevrpc_webtransport"],
  });
});

test("WebKit launch options bound startup and preserve an explicit executable", () => {
  assert.deepEqual(webkitLaunchOptions({ TREVRPC_BROWSER_WEBKIT: "/webkit/pw_run.sh" }), {
    headless: true,
    timeout: 20_000,
    executablePath: "/webkit/pw_run.sh",
  });
  assert.deepEqual(webkitLaunchOptions({}), {
    headless: true,
    timeout: 20_000,
  });
});

test("WebKit runtime diagnostics report selected graphics configuration", () => {
  const diagnostics = webkitRuntimeDiagnostics({
    TREVRPC_BROWSER_WEBKIT: "/webkit/pw_run.sh",
    EGL_PLATFORM: "surfaceless",
    WPE_FDO_HEADLESS: "1",
    LIBGL_ALWAYS_SOFTWARE: "true",
    GBM_BACKEND: "dri",
  });
  assert.match(diagnostics, /executable=\/webkit\/pw_run\.sh/u);
  assert.match(diagnostics, /launch_timeout_ms=20000/u);
  assert.match(diagnostics, /EGL_PLATFORM=surfaceless/u);
  assert.match(diagnostics, /LIBGL_DRIVERS_PATH=<unset>/u);
  assert.match(diagnostics, /GBM_BACKEND=dri/u);
});

test("WebKit launch errors preserve the cause and append runtime diagnostics", () => {
  const cause = new Error("launch failed");
  const error = webkitLaunchError(cause, {
    TREVRPC_BROWSER_WEBKIT: "/webkit/pw_run.sh",
    EGL_PLATFORM: "surfaceless",
  });
  assert.equal(error.cause, cause);
  assert.match(error.message, /^launch failed\nWebKit runtime:/u);
  assert.match(error.message, /executable=\/webkit\/pw_run\.sh/u);
  assert.match(error.message, /EGL_PLATFORM=surfaceless/u);
});

const peers = [
  {
    name: "chromium",
    file: "../trevrpc-bench-peer-chromium.js",
    parse: parseChromium,
    connect: parseConnectChromium,
  },
  {
    name: "firefox",
    file: "../trevrpc-bench-peer-firefox.js",
    parse: parseFirefox,
    connect: parseConnectFirefox,
  },
  {
    name: "webkit",
    file: "../trevrpc-bench-peer-webkit.js",
    parse: parseWebkit,
    connect: parseConnectWebkit,
  },
];

for (const { name, file, parse, connect } of peers) {
  const peerPath = fileURLToPath(new URL(file, import.meta.url));

  test(`${name} peer advertises platform-accurate V4 capabilities`, async () => {
    const { stdout, stderr } = await execFileAsync(process.execPath, [peerPath, "capabilities"]);
    assert.equal(stderr, "");
    assert.deepEqual(JSON.parse(stdout), {
      schema_version: 4,
      event: "capabilities",
      roles:
        name === "webkit" && process.platform === "linux"
          ? {}
          : { client: ["trevrpc_webtransport"] },
      rpc_kinds: ["unary", "client_stream", "server_stream", "bidi"],
      histogram: "log_linear_v1",
      peer: name,
    });
  });

  test(`${name} client parses workload without an initial address`, () => {
    assert.deepEqual(
      parse([
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
      () => parse(["client", "--stack", "trevrpc_webtransport", "--address", "127.0.0.1:443"]),
      /unknown option --address/u,
    );
    assert.throws(() => parse(["server"]), /usage:/u);
  });

  test(`${name} peer accepts only a valid CONNECT address`, () => {
    assert.equal(connect("CONNECT 127.0.0.1:443"), "127.0.0.1:443");
    assert.equal(connect("CONNECT [::1]:443"), "[::1]:443");
    assert.throws(() => connect("START"), /expected CONNECT HOST:PORT/u);
    assert.throws(() => connect("CONNECT 127.0.0.1:0"), /port from 1 through 65535/u);
  });

  test(`${name} peer usage mentions its own binary name`, () => {
    try {
      parse(["server"]);
      assert.fail("expected to throw");
    } catch (error) {
      assert.match(error.message, new RegExp(`trevrpc-bench-peer-${name}`, "u"));
    }
  });
}
