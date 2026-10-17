import { EchoClient } from "../build/interop/gen/interop.trevrpc.js";
// The interop scenarios of interop/README.md, run against a WebTransport Echo server. This module
// only needs the web platform, so it runs unchanged in a browser bundle and under Node tests.
import { Code, TrevRpcError, codeName, connect } from "../src/index.js";
import { ScenarioNames } from "./names.js";

const DeadlineMs = 10_000;

const scenarios = {
  async unary(echo) {
    expectEcho(await echo.unary({ message: "hello" }), "hello", 0);
  },

  async "unary-metadata"(echo) {
    const response = await echo.unary(
      { message: "hello" },
      { metadata: { "interop-suffix": "!" } },
    );
    expectEcho(response, "hello!", 0);
  },

  async "unary-error"(echo) {
    await expectStatus(Code.NotFound, "missing", () =>
      echo.unary({ failCode: Code.NotFound, failMessage: "missing" }),
    );
  },

  async "unary-large"(echo) {
    const message = "x".repeat(1 << 20);
    expectEcho(await echo.unary({ message }), message, 0);
  },

  async "client-stream"(echo) {
    const call = await echo.clientStream();
    for (const message of ["a", "b", "c"]) {
      await call.send({ message });
    }
    expectEcho(await call.closeAndRecv(), "abc", 3);
  },

  async "server-stream"(echo) {
    const stream = await echo.serverStream({ message: "s", count: 3 });
    const received = [];
    for await (const response of stream) {
      received.push(response);
    }
    expectEchoes(received, [
      ["s", 0],
      ["s", 1],
      ["s", 2],
    ]);
    expect((await stream.status).code === Code.Ok, "stream did not end with OK");
  },

  async "server-stream-error"(echo) {
    const stream = await echo.serverStream({
      message: "s",
      count: 2,
      failCode: Code.FailedPrecondition,
      failMessage: "stop",
    });
    const received = [];
    await expectStatus(Code.FailedPrecondition, "stop", async () => {
      for await (const response of stream) {
        received.push(response);
      }
    });
    expectEchoes(received, [
      ["s", 0],
      ["s", 1],
    ]);
  },

  async bidi(echo) {
    const call = await echo.bidi();
    for (const [index, message] of ["1", "2", "3"].entries()) {
      await call.send({ message });
      expectEcho(await call.recv(), message, index);
    }
    await call.closeSend();
    expect((await call.recv()) === undefined, "expected the stream to end after closeSend");
    expect((await call.status).code === Code.Ok, "stream did not end with OK");
  },

  async "bidi-error"(echo) {
    const call = await echo.bidi();
    await call.send({ message: "1" });
    expectEcho(await call.recv(), "1", 0);
    await call.send({ failCode: Code.Aborted, failMessage: "abort" });
    await expectStatus(Code.Aborted, "abort", () => call.recv());
  },

  async "unary-headers"(echo) {
    const response = await echo.unaryWithResponse(
      { message: "hello" },
      { metadata: { "interop-header": "h" } },
    );
    expectEcho(response.message, "hello", 0);
    expectMetadata(response.headers, { "interop-header": "h" }, "headers");
    expectMetadata(response.metadata, { "interop-trailer": "h" }, "trailing metadata");
  },

  async "server-stream-headers"(echo) {
    const stream = await echo.serverStream(
      { message: "s", count: 2 },
      { metadata: { "interop-header": "h" } },
    );
    expectMetadata(await stream.headers, { "interop-header": "h" }, "headers");
    const received = [];
    for await (const response of stream) {
      received.push(response);
    }
    expectEchoes(received, [
      ["s", 0],
      ["s", 1],
    ]);
    const status = await stream.status;
    expect(status.code === Code.Ok, "stream did not end with OK");
    expectMetadata(status.metadata, { "interop-trailer": "h" }, "trailing metadata");
  },
};

/** The channel transports each interop lane allows. */
const LaneTransports = {
  webtransport: ["webtransport"],
  websocket: ["websocket"],
  auto: ["webtransport", "websocket"],
};

/**
 * Runs every scenario against the server at url, reporting each result as it completes through
 * onResult({ name, ok, reason }). Resolves with all results. A server that cannot be reached
 * fails every scenario.
 *
 * transport is "webtransport" (the default), "websocket" (QMux over WebSocket at webSocketUrl),
 * or "auto" (WebTransport with the WebSocket fallback). expectTransport fails every scenario
 * unless the channel connected over that transport.
 */
export async function runScenarios({
  url,
  certHash,
  transport = "webtransport",
  webSocketUrl,
  expectTransport,
  onResult = () => {},
}) {
  const results = [];
  const report = (name, error) => {
    const result =
      error === undefined ? { name, ok: true } : { name, ok: false, reason: describe(error) };
    results.push(result);
    onResult(result);
  };

  let channel;
  try {
    channel = await connect(url, {
      signal: AbortSignal.timeout(DeadlineMs),
      transports: LaneTransports[transport],
      webSocketUrl,
      ...(certHash && {
        serverCertificateHashes: [{ algorithm: "sha-256", value: Uint8Array.from(certHash) }],
      }),
    });
    if (expectTransport && channel.transport !== expectTransport) {
      channel.close();
      throw new Error(`connected over ${channel.transport}, want ${expectTransport}`);
    }
  } catch (error) {
    for (const name of ScenarioNames) {
      report(name, new Error(`connect: ${describe(error)}`));
    }
    return results;
  }

  try {
    const echo = new EchoClient(channel, { timeoutMs: DeadlineMs });
    for (const name of ScenarioNames) {
      try {
        await scenarios[name](echo);
        report(name);
      } catch (error) {
        report(name, error);
      }
    }
  } finally {
    channel.close();
  }
  return results;
}

for (const name of ScenarioNames) {
  if (typeof scenarios[name] !== "function") {
    throw new Error(`missing interop scenario ${name}`);
  }
}

function expect(condition, message) {
  if (!condition) {
    throw new Error(message);
  }
}

function expectEcho(response, message, index) {
  expect(
    response?.message === message && response.index === index,
    `expected ${summary(message, index)}, got ${summary(response?.message, response?.index)}`,
  );
}

function expectEchoes(responses, expected) {
  const actual = responses.map((response) => summary(response.message, response.index));
  expect(
    actual.join(",") === expected.map(([message, index]) => summary(message, index)).join(","),
    `expected ${expected.length} responses [${expected.map(([m, i]) => summary(m, i))}], got ${actual.length} [${actual}]`,
  );
}

/** Checks a metadata map holds exactly the expected text values. */
function expectMetadata(metadata, expected, what) {
  const decoder = new TextDecoder();
  const actual = Object.fromEntries(
    Object.entries(metadata ?? {}).map(([key, value]) => [key, decoder.decode(value)]),
  );
  const same =
    Object.keys(actual).length === Object.keys(expected).length &&
    Object.entries(expected).every(([key, value]) => actual[key] === value);
  expect(same, `expected ${what} ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
}

/** Runs an operation that must fail with the given status code and message. */
async function expectStatus(code, message, operation) {
  try {
    await operation();
  } catch (error) {
    expect(
      error instanceof TrevRpcError && error.code === code && error.statusMessage === message,
      `expected status ${codeName(code)} ${JSON.stringify(message)}, got ${describe(error)}`,
    );
    return;
  }
  throw new Error(`expected status ${codeName(code)} ${JSON.stringify(message)}, got OK`);
}

function summary(message, index) {
  const text = String(message);
  return `(${text.length > 16 ? `${text.slice(0, 16)}... [${text.length} chars]` : text}, ${index})`;
}

/** A single-line description of a failure. */
function describe(error) {
  const text =
    error instanceof TrevRpcError
      ? `${codeName(error.code)} ${JSON.stringify(error.statusMessage)}`
      : (error?.message ?? String(error));
  return text.replace(/\s+/g, " ").trim();
}
