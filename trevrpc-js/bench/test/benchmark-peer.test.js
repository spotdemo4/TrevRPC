import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { readFileSync } from "node:fs";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { Code, RpcStreamFrameKind, protobuf } from "trevrpc-js";

import {
  LogLinearHistogram,
  createClientOperation,
  logLinearUpperBound,
  prepareFixedAdmissionPhase,
  root,
} from "../common.js";
import { createBenchmarkHandlers, parseCommandLine } from "../trevrpc-bench-peer.js";

const execFileAsync = promisify(execFile);
const BenchmarkRequest = root.lookupType("trevrpc.benchmark.v1.BenchmarkRequest");
const BenchmarkResponse = root.lookupType("trevrpc.benchmark.v1.BenchmarkResponse");
const StreamRequest = root.lookupType("trevrpc.benchmark.v1.StreamRequest");
const BenchmarkSummary = root.lookupType("trevrpc.benchmark.v1.BenchmarkSummary");

test("benchmark peer schema matches the canonical proto", () => {
  const canonical = protobuf.parse(
    readFileSync(new URL("../../../bench/proto/benchmark.proto", import.meta.url), "utf8"),
  ).root;
  for (const typeName of [
    "BenchmarkRequest",
    "BenchmarkResponse",
    "StreamRequest",
    "BenchmarkSummary",
  ]) {
    assert.deepEqual(
      schemaFields(root.lookupType(`trevrpc.benchmark.v1.${typeName}`)),
      schemaFields(canonical.lookupType(`trevrpc.benchmark.v1.${typeName}`)),
    );
  }
  const service = canonical.lookupService("trevrpc.benchmark.v1.BenchmarkService");
  assert.deepEqual(
    Object.values(service.methods).map((method) => ({
      name: method.name,
      requestType: method.requestType,
      responseType: method.responseType,
      requestStream: method.requestStream ?? false,
      responseStream: method.responseStream ?? false,
    })),
    [
      {
        name: "Unary",
        requestType: "BenchmarkRequest",
        responseType: "BenchmarkResponse",
        requestStream: false,
        responseStream: false,
      },
      {
        name: "ClientStream",
        requestType: "BenchmarkRequest",
        responseType: "BenchmarkSummary",
        requestStream: true,
        responseStream: false,
      },
      {
        name: "ServerStream",
        requestType: "StreamRequest",
        responseType: "BenchmarkResponse",
        requestStream: false,
        responseStream: true,
      },
      {
        name: "Bidi",
        requestType: "BenchmarkRequest",
        responseType: "BenchmarkResponse",
        requestStream: true,
        responseStream: true,
      },
    ],
  );
});

function schemaFields(type) {
  return Object.fromEntries(
    Object.entries(type.fields).map(([name, field]) => [
      name,
      {
        id: field.id,
        type: field.type,
        repeated: field.repeated,
        keyType: field.keyType,
      },
    ]),
  );
}

test("benchmark peer capabilities use protocol v4 role-specific stacks", async () => {
  const { stdout, stderr } = await execFileAsync(process.execPath, [
    fileURLToPath(new URL("../trevrpc-bench-peer.js", import.meta.url)),
    "capabilities",
  ]);

  assert.equal(stderr, "");
  assert.deepEqual(JSON.parse(stdout), {
    schema_version: 4,
    event: "capabilities",
    roles: {
      client: ["trevrpc_native_quic"],
      server: ["trevrpc_native_quic", "trevrpc_webtransport"],
    },
    rpc_kinds: ["unary", "client_stream", "server_stream", "bidi"],
    histogram: "log_linear_v1",
    peer: "js",
  });
});

test("benchmark peer parses required client and IPv6 server options", () => {
  const clientArgs = [
    "client",
    "--stack",
    "trevrpc_native_quic",
    "--address",
    "127.0.0.1:43117",
    "--cert",
    "ca.pem",
    "--rpc",
    "bidi",
    "--concurrency",
    "8",
    "--warmup-ms",
    "0",
    "--measurement-ms",
    "1000",
    "--request-bytes",
    "64",
    "--response-bytes",
    "128",
    "--messages-per-stream",
    "4",
  ];
  assert.deepEqual(parseCommandLine(clientArgs), {
    command: "client",
    stack: "trevrpc_native_quic",
    address: { host: "127.0.0.1", port: 43117 },
    cert: "ca.pem",
    rpcKind: "bidi",
    concurrency: 8,
    warmupMs: 0,
    measurementMs: 1000,
    requestBytes: 64,
    responseBytes: 128,
    messagesPerStream: 4,
  });
  assert.deepEqual(
    parseCommandLine([
      "server",
      "--stack",
      "trevrpc_native_quic",
      "--listen",
      "[::1]:0",
      "--cert",
      "server.pem",
      "--key",
      "server-key.pem",
    ]),
    {
      command: "server",
      stack: "trevrpc_native_quic",
      listen: { host: "::1", port: 0 },
      cert: "server.pem",
      key: "server-key.pem",
    },
  );
  assert.deepEqual(
    parseCommandLine([
      "server",
      "--stack",
      "trevrpc_webtransport",
      "--listen",
      "127.0.0.1:0",
      "--cert",
      "server.pem",
      "--key",
      "server-key.pem",
      "--webtransport-origin",
      "http://127.0.0.1:8080",
    ]),
    {
      command: "server",
      stack: "trevrpc_webtransport",
      listen: { host: "127.0.0.1", port: 0 },
      cert: "server.pem",
      key: "server-key.pem",
      webtransportOrigin: "http://127.0.0.1:8080",
    },
  );
  assert.throws(
    () =>
      parseCommandLine([
        "client",
        "--stack",
        "trevrpc_native_quic",
        "--address",
        "127.0.0.1:1",
        "--cert",
        "ca.pem",
        "--rpc",
        "unary",
      ]),
    /missing required option --concurrency/u,
  );
  assert.throws(
    () => parseCommandLine([clientArgs[0], ...clientArgs.slice(3)]),
    /missing required option --stack/u,
  );
  assert.throws(
    () =>
      parseCommandLine([
        "server",
        "--listen",
        "127.0.0.1:0",
        "--cert",
        "server.pem",
        "--key",
        "server-key.pem",
      ]),
    /missing required option --stack/u,
  );
  const oversizedPayload = [...clientArgs];
  oversizedPayload[oversizedPayload.indexOf("--request-bytes") + 1] = "67108865";
  assert.throws(() => parseCommandLine(oversizedPayload), /through 67108864/u);
  const invalidStack = [...clientArgs];
  invalidStack[invalidStack.indexOf("--stack") + 1] = "native_quic";
  assert.throws(() => parseCommandLine(invalidStack), /trevrpc_native_quic/u);
  const webtransportClient = [...clientArgs];
  webtransportClient[webtransportClient.indexOf("--stack") + 1] = "trevrpc_webtransport";
  assert.throws(() => parseCommandLine(webtransportClient), /trevrpc_native_quic/u);
  assert.throws(
    () =>
      parseCommandLine([
        "server",
        "--stack",
        "trevrpc_webtransport",
        "--listen",
        "127.0.0.1:0",
        "--cert",
        "server.pem",
        "--key",
        "server-key.pem",
      ]),
    /missing required option --webtransport-origin/u,
  );
  assert.throws(
    () =>
      parseCommandLine([
        "server",
        "--stack",
        "trevrpc_native_quic",
        "--listen",
        "127.0.0.1:0",
        "--cert",
        "server.pem",
        "--key",
        "server-key.pem",
        "--webtransport-origin",
        "http://127.0.0.1:8080",
      ]),
    /only valid with server stack trevrpc_webtransport/u,
  );
});

test("log_linear_v1 uses exact low buckets and sorted sparse output", () => {
  assert.equal(logLinearUpperBound(1n), 1n);
  assert.equal(logLinearUpperBound(1023n), 1023n);
  assert.equal(logLinearUpperBound(1024n), 1025n);
  assert.equal(logLinearUpperBound(1025n), 1025n);
  assert.equal(logLinearUpperBound(1026n), 1027n);

  const histogram = new LogLinearHistogram();
  histogram.record(1026n);
  histogram.record(1n);
  histogram.record(1024n);
  histogram.record(1025n);
  assert.deepEqual(histogram.toJSON(), [
    { upper_bound_ns: "1", count: "1" },
    { upper_bound_ns: "1025", count: "2" },
    { upper_bound_ns: "1027", count: "1" },
  ]);
});

test("fixed admission does not start work at or after the deadline", async () => {
  let clock = 0n;
  const phase = prepareFixedAdmissionPhase({
    concurrency: 1,
    durationNs: 10n,
    recordLatency: true,
    now: () => clock,
    async operation() {
      clock += 5n;
      return { requestMessages: 2n, responseMessages: 3n };
    },
  });

  const result = await phase.start();

  assert.equal(result.elapsedNs, 10n);
  assert.equal(result.completed, 2n);
  assert.equal(result.failed, 0n);
  assert.equal(result.requestMessages, 4n);
  assert.equal(result.responseMessages, 6n);
  assert.equal(result.histogram.count, 2n);
});

test("fixed admission creates and releases every concurrency lane", async () => {
  let clock = 0n;
  let active = 0;
  let maximumActive = 0;
  const pending = [];
  const phase = prepareFixedAdmissionPhase({
    concurrency: 3,
    durationNs: 10n,
    recordLatency: false,
    now: () => clock,
    operation() {
      active += 1;
      maximumActive = Math.max(maximumActive, active);
      return new Promise((resolveOperation) => {
        pending.push(() => {
          active -= 1;
          resolveOperation({ requestMessages: 1n, responseMessages: 1n });
        });
      });
    },
  });

  const resultPromise = phase.start();
  while (pending.length < 3) {
    await Promise.resolve();
  }
  clock = 10n;
  for (const resolveOperation of pending) {
    resolveOperation();
  }
  const result = await resultPromise;

  assert.equal(maximumActive, 3);
  assert.equal(result.completed, 3n);
  assert.equal(result.requestMessages, 3n);
  assert.equal(result.responseMessages, 3n);
});

test("client operations validate and count all four RPC kinds", async () => {
  const config = {
    requestBytes: 3,
    responseBytes: 4,
    messagesPerStream: 3,
  };

  const unary = createClientOperation(
    {
      async unary(request) {
        assert.equal(request.payload.byteLength, config.requestBytes);
        return { sequence: request.sequence, payload: new Uint8Array(config.responseBytes) };
      },
    },
    { ...config, rpcKind: "unary" },
  );
  assert.deepEqual(await unary({ laneIndex: 2, operationIndex: 7n }), {
    requestMessages: 1n,
    responseMessages: 1n,
  });

  const streamedRequests = [];
  const clientStream = createClientOperation(
    {
      async clientStream() {
        return {
          async sendMany(messages) {
            streamedRequests.push(...messages);
          },
          async closeAndRecv() {
            return {
              messageCount: streamedRequests.length,
              payloadBytes: streamedRequests.reduce(
                (total, request) => total + request.payload.byteLength,
                0,
              ),
            };
          },
        };
      },
    },
    { ...config, rpcKind: "client_stream" },
  );
  assert.deepEqual(await clientStream({ laneIndex: 0, operationIndex: 0n }), {
    requestMessages: 3n,
    responseMessages: 1n,
  });
  assert.deepEqual(
    streamedRequests.map((request) => String(request.sequence)),
    ["0", "1", "2"],
  );

  const individuallySent = [];
  const clientStreamWithoutBatching = createClientOperation(
    {
      async clientStream() {
        return {
          async send(message) {
            individuallySent.push(message);
          },
          async closeAndRecv() {
            return {
              messageCount: individuallySent.length,
              payloadBytes: individuallySent.reduce(
                (total, request) => total + request.payload.byteLength,
                0,
              ),
            };
          },
        };
      },
    },
    { ...config, rpcKind: "client_stream" },
  );
  await clientStreamWithoutBatching({ laneIndex: 0, operationIndex: 0n });
  assert.equal(individuallySent.length, config.messagesPerStream);

  const serverStream = createClientOperation(
    {
      async serverStream(request) {
        assert.equal(request.payload.byteLength, config.requestBytes);
        return responseMessages(request.messageCount, request.responseBytes);
      },
    },
    { ...config, rpcKind: "server_stream" },
  );
  assert.deepEqual(await serverStream({ laneIndex: 0, operationIndex: 0n }), {
    requestMessages: 1n,
    responseMessages: 3n,
  });

  const bidiResponses = [];
  let bidiClosed = false;
  const bidi = createClientOperation(
    {
      async bidi() {
        return {
          async sendMany(messages) {
            bidiResponses.push(
              ...messages.map((request) => ({
                sequence: request.sequence,
                payload: new Uint8Array(request.responseBytes),
              })),
            );
          },
          async closeSend() {
            bidiClosed = true;
          },
          async recv() {
            return (
              bidiResponses.shift() ?? (bidiClosed ? undefined : assert.fail("receive stalled"))
            );
          },
        };
      },
    },
    { ...config, rpcKind: "bidi" },
  );
  assert.deepEqual(await bidi({ laneIndex: 0, operationIndex: 0n }), {
    requestMessages: 3n,
    responseMessages: 3n,
  });
});

test("server handlers implement all four benchmark RPCs", async () => {
  const handlers = createBenchmarkHandlers();
  const unaryBody = await handlers.unary({
    request: {
      body: BenchmarkRequest.encode({
        sequence: "42",
        payload: new Uint8Array(2),
        responseBytes: 3,
      }).finish(),
    },
  });
  const unaryResponse = BenchmarkResponse.decode(unaryBody);
  assert.equal(String(unaryResponse.sequence), "42");
  assert.equal(unaryResponse.payload.byteLength, 3);

  const clientFrames = [
    requestFrame("0", 2, 0),
    requestFrame("1", 2, 0),
    requestFrame("2", 2, 0),
    { kind: RpcStreamFrameKind.Status, status: Code.Ok },
  ];
  const summary = BenchmarkSummary.decode(await handlers.clientStream(receivingCall(clientFrames)));
  assert.equal(String(summary.messageCount), "3");
  assert.equal(String(summary.payloadBytes), "6");

  const serverResponses = [];
  const responseStream = handlers.serverStream({
    request: {
      body: StreamRequest.encode({
        messageCount: 3,
        payload: new Uint8Array(2),
        responseBytes: 4,
      }).finish(),
    },
  });
  for await (const body of responseStream) {
    serverResponses.push(BenchmarkResponse.decode(body));
  }
  assert.deepEqual(
    serverResponses.map((response) => String(response.sequence)),
    ["0", "1", "2"],
  );
  assert.ok(serverResponses.every((response) => response.payload.byteLength === 4));

  const bidiResponses = [];
  for await (const body of handlers.bidi(
    receivingCall([
      requestFrame("7", 2, 4),
      requestFrame("8", 2, 4),
      { kind: RpcStreamFrameKind.Status, status: Code.Ok },
    ]),
  )) {
    bidiResponses.push(BenchmarkResponse.decode(body));
  }
  assert.deepEqual(
    bidiResponses.map((response) => String(response.sequence)),
    ["7", "8"],
  );
  assert.ok(bidiResponses.every((response) => response.payload.byteLength === 4));

  assert.throws(
    () =>
      handlers.unary({
        request: {
          body: BenchmarkRequest.encode({ sequence: "1", responseBytes: 0xffffffff }).finish(),
        },
      }),
    /response_bytes is outside the benchmark peer limit/u,
  );
  assert.throws(
    () =>
      handlers.serverStream({
        request: {
          body: StreamRequest.encode({ messageCount: 0xffffffff, responseBytes: 1 }).finish(),
        },
      }),
    /message_count is outside the benchmark peer limit/u,
  );
});

async function* responseMessages(count, responseBytes) {
  for (let index = 0; index < count; index += 1) {
    yield { sequence: String(index), payload: new Uint8Array(responseBytes) };
  }
}

function requestFrame(sequence, requestBytes, responseBytes) {
  return {
    body: BenchmarkRequest.encode({
      sequence,
      payload: new Uint8Array(requestBytes),
      responseBytes,
    }).finish(),
  };
}

function receivingCall(frames) {
  const queued = [...frames, null];
  return {
    request: {},
    recv() {
      return Promise.resolve(queued.shift());
    },
  };
}
