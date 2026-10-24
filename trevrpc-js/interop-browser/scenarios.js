import { CompressedEchoClient } from "../build/interop/gen/compressed.trevrpc.js";
import { EchoClient } from "../build/interop/gen/interop.trevrpc.js";
import { UnreliableEchoClient } from "../build/interop/gen/unreliable.trevrpc.js";
// The interop scenarios of interop/README.md, run against a WebTransport Echo server. This module
// only needs the web platform, so it runs unchanged in a browser bundle and under Node tests.
import { Code, TrevRpcError, codeName, connect } from "../src/index.js";
import { createCompression } from "../src/zstd.js";
import { ScenarioNames } from "./names.js";

const DeadlineMs = 10_000;

/** The `unreliable` scenario sends this many short requests, then one that cannot be a datagram. */
const UnreliableShortRequests = 50;
const UnreliableLargeSize = 4096;
/** How many messages the `compressed` scenario streams. */
const CompressedMessages = 3;
/** How many messages the `compressed-stream` scenario echoes. */
const CompressedStreamMessages = 8;
/** The message types the interop dictionary compresses. */
const EchoTypes = ["trevrpc.interop.v1.EchoRequest", "trevrpc.interop.v1.EchoResponse"];

/** The i-th message of the `compressed` scenario: like the dictionary's training data. */
function compressedMessage(i) {
  return `{"service":"search","region":"us-east-1","status":404,"latency_ms":${100 + i}}`;
}

/**
 * Returns n characters that zstd cannot shrink much: base64 digits drawn by the fixed xorshift
 * generator the native clients use. A message of them stays too large for a datagram even when
 * compression is on.
 */
function incompressible(n) {
  const digits = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
  const mask = (1n << 64n) - 1n;
  let state = 0x9e3779b97f4a7c15n;
  let out = "";
  for (let i = 0; i < n; i += 1) {
    state ^= (state << 13n) & mask;
    state ^= state >> 7n;
    state ^= (state << 17n) & mask;
    out += digits[Number(state % 64n)];
  }
  return out;
}

/**
 * Scenarios take the Echo client and a context: `unreliable` is the UnreliableEcho client,
 * `compressed` the CompressedEcho client, and `transport` the transport ("webtransport" or
 * "websocket") the channel connected over.
 */
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
    // The server may end the call before this send settles; the send then rejects with the same
    // status.
    await call
      .send({ failCode: Code.Aborted, failMessage: "abort" })
      .catch((error) => expectStatus(Code.Aborted, "abort", () => Promise.reject(error)));
    await expectStatus(Code.Aborted, "abort", () => call.recv());
  },

  async "unary-headers"(echo) {
    let headers;
    let metadata;
    const response = await echo.unary(
      { message: "hello" },
      {
        metadata: { "interop-header": "h" },
        onResponseHeaders: (received) => {
          headers = received;
        },
        onResponseMetadata: (received) => {
          metadata = received;
        },
      },
    );
    expectEcho(response, "hello", 0);
    expectMetadata(headers, { "interop-header": "h" }, "headers");
    expectMetadata(metadata, { "interop-trailer": "h" }, "trailing metadata");
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

  async unreliable(_echo, { unreliable, transport }) {
    const call = await unreliable.bidi();
    const requests = Array.from({ length: UnreliableShortRequests }, (_, index) => String(index));
    requests.push(incompressible(UnreliableLargeSize));

    // Responses are read while the requests go out, so they are not dropped for a full queue.
    const reading = (async () => {
      const received = [];
      for await (const response of call) {
        received.push(response.message);
      }
      return received;
    })();
    // A failed send is reported through the responses too; do not leave that rejection unhandled.
    reading.catch(() => {});
    for (const message of requests) {
      await call.send({ message, count: 1 });
    }
    await call.closeSend();
    const received = await reading;
    const status = await call.status;
    expect(status.code === Code.Ok, "stream did not end with OK");

    const sent = new Set(requests);
    const seen = new Set();
    for (const message of received) {
      expect(
        sent.has(message),
        `received a response that was never requested: ${summarize(message)}`,
      );
      expect(!seen.has(message), `received the response ${summarize(message)} twice`);
      seen.add(message);
    }
    expect(seen.has(requests.at(-1)), "the large response is missing");

    const metadata = Object.fromEntries(
      ["interop-received", "interop-datagrams-received"].map((key) => [
        key,
        Number(new TextDecoder().decode(status.metadata[key] ?? new Uint8Array())),
      ]),
    );
    const total = requests.length;
    if (transport === "webtransport") {
      expect(
        metadata["interop-received"] >= 1 && metadata["interop-received"] <= total,
        `interop-received ${metadata["interop-received"]}, want 1 to ${total}`,
      );
      expect(
        metadata["interop-datagrams-received"] >= 1,
        "the server received no request as a datagram",
      );
    } else {
      expect(received.length === total, `received ${received.length} responses, want ${total}`);
      expect(
        metadata["interop-received"] === total,
        `interop-received ${metadata["interop-received"]}, want ${total}`,
      );
      expect(
        metadata["interop-datagrams-received"] === 0,
        `interop-datagrams-received ${metadata["interop-datagrams-received"]}, want 0`,
      );
    }
  },

  // With the dictionary on both sides these travel as ZSTD_MESSAGE frames; without, as MESSAGE.
  async compressed(echo) {
    for (let i = 0; i < 2; i += 1) {
      expectEcho(await echo.unary({ message: compressedMessage(i) }), compressedMessage(i), 0);
    }
    const call = await echo.bidi();
    for (let i = 0; i < CompressedMessages; i += 1) {
      await call.send({ message: compressedMessage(i) });
    }
    await call.closeSend();
    const received = [];
    for await (const response of call) {
      received.push(response);
    }
    expectEchoes(
      received,
      Array.from({ length: CompressedMessages }, (_, i) => [compressedMessage(i), i]),
    );
    expect((await call.status).code === Code.Ok, "stream did not end with OK");
  },

  // Each echo is awaited, so the server's HEADERS, which let the client compress its requests,
  // arrive before the third request, when stream compression starts.
  async "compressed-stream"(_echo, { compressed }) {
    const call = await compressed.bidi();
    for (let i = 0; i < CompressedStreamMessages; i += 1) {
      await call.send({ message: compressedMessage(i) });
      expectEcho(await call.recv(), compressedMessage(i), i);
    }
    await call.closeSend();
    expect((await call.recv()) === undefined, "expected the stream to end after closeSend");
    const status = await call.status;
    expect(status.code === Code.Ok, "stream did not end with OK");
    expectStreamCompressed(status.metadata, "bidi");

    // The server sends HEADERS at dispatch on this method, and they carry its answer, so the
    // requests after them compress although the only response comes at the end.
    let headersArrived;
    const headers = new Promise((resolve) => {
      headersArrived = resolve;
    });
    let trailers;
    const upload = await compressed.clientStream({
      onResponseHeaders: headersArrived,
      onResponseMetadata: (metadata) => {
        trailers = metadata;
      },
    });
    await upload.send({ message: compressedMessage(0) });
    await headers;
    for (let i = 1; i < CompressedStreamMessages; i += 1) {
      await upload.send({ message: compressedMessage(i) });
    }
    const all = Array.from({ length: CompressedStreamMessages }, (_, i) => compressedMessage(i));
    expectEcho(await upload.closeAndRecv(), all.join(""), CompressedStreamMessages);
    expectStreamCompressed(trailers, "client-stream");
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
 * unless the channel connected over that transport. The channel always has compression settings,
 * so it stream-compresses where methods allow it; dictionary, the interop dictionary in base64,
 * adds that dictionary for Echo messages.
 */
export async function runScenarios({
  url,
  certHash,
  transport = "webtransport",
  webSocketUrl,
  expectTransport,
  dictionary,
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
    const compression = await createCompression({
      dictionaries: dictionary ? [{ data: base64Bytes(dictionary), messages: EchoTypes }] : [],
    });
    channel = await connect(url, {
      compression,
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
    const context = {
      unreliable: new UnreliableEchoClient(channel, { timeoutMs: DeadlineMs }),
      compressed: new CompressedEchoClient(channel, { timeoutMs: DeadlineMs }),
      get transport() {
        return channel.transport;
      },
    };
    for (const name of ScenarioNames) {
      try {
        await scenarios[name](echo, context);
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

/** Checks that the server received at least one request of the call stream-compressed. */
function expectStreamCompressed(metadata, what) {
  const received = Number(
    new TextDecoder().decode(metadata?.["interop-stream-compressed-received"] ?? new Uint8Array()),
  );
  expect(received >= 1, `${what}: interop-stream-compressed-received ${received}, want at least 1`);
}

function base64Bytes(text) {
  return Uint8Array.from(atob(text), (character) => character.charCodeAt(0));
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
  return `(${summarize(message)}, ${index})`;
}

function summarize(message) {
  const text = String(message);
  return text.length > 16 ? `${text.slice(0, 16)}... [${text.length} chars]` : text;
}

/** A single-line description of a failure. */
function describe(error) {
  const text =
    error instanceof TrevRpcError
      ? `${codeName(error.code)} ${JSON.stringify(error.statusMessage)}`
      : (error?.message ?? String(error));
  return text.replace(/\s+/g, " ").trim();
}
