#!/usr/bin/env node

import { resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

import { createRoot, createServiceClient } from "../src/index.node.js";
import { RawNodeTransport } from "../src/node-advanced.js";
import { NodeServer } from "../src/node-index.js";

const SchemaVersion = 1;
const Peer = "js";
const ServiceName = "trevrpc.benchmark.v1.BenchmarkService";
const IdleTimeoutMs = 600_000;
const StreamBatchSize = 16;
const MaxConcurrency = 4096;
const MaxPayloadBytes = 64 * 1024 * 1024;
const MaxMessagesPerStream = 1_000_000;
const MaxFrameSize = 128 * 1024 * 1024;

export const root = createRoot({
  nested: {
    trevrpc: {
      nested: {
        benchmark: {
          nested: {
            v1: {
              nested: {
                BenchmarkRequest: {
                  fields: {
                    sequence: { type: "uint64", id: 1 },
                    payload: { type: "bytes", id: 2 },
                    responseBytes: { type: "uint32", id: 3 },
                  },
                },
                BenchmarkResponse: {
                  fields: {
                    sequence: { type: "uint64", id: 1 },
                    payload: { type: "bytes", id: 2 },
                  },
                },
                StreamRequest: {
                  fields: {
                    messageCount: { type: "uint32", id: 1 },
                    payload: { type: "bytes", id: 2 },
                    responseBytes: { type: "uint32", id: 3 },
                  },
                },
                BenchmarkSummary: {
                  fields: {
                    messageCount: { type: "uint64", id: 1 },
                    payloadBytes: { type: "uint64", id: 2 },
                  },
                },
              },
            },
          },
        },
      },
    },
  },
});

const BenchmarkRequest = root.lookupType("trevrpc.benchmark.v1.BenchmarkRequest");
const BenchmarkResponse = root.lookupType("trevrpc.benchmark.v1.BenchmarkResponse");
const StreamRequest = root.lookupType("trevrpc.benchmark.v1.StreamRequest");
const BenchmarkSummary = root.lookupType("trevrpc.benchmark.v1.BenchmarkSummary");

export const BenchmarkService = Object.freeze({
  name: "BenchmarkService",
  fullName: ServiceName,
  exportName: "BenchmarkService",
  methods: {
    unary: {
      name: "Unary",
      kind: "unary",
      inputType: "trevrpc.benchmark.v1.BenchmarkRequest",
      outputType: "trevrpc.benchmark.v1.BenchmarkResponse",
    },
    clientStream: {
      name: "ClientStream",
      kind: "clientStreaming",
      inputType: "trevrpc.benchmark.v1.BenchmarkRequest",
      outputType: "trevrpc.benchmark.v1.BenchmarkSummary",
    },
    serverStream: {
      name: "ServerStream",
      kind: "serverStreaming",
      inputType: "trevrpc.benchmark.v1.StreamRequest",
      outputType: "trevrpc.benchmark.v1.BenchmarkResponse",
    },
    bidi: {
      name: "Bidi",
      kind: "bidirectionalStreaming",
      inputType: "trevrpc.benchmark.v1.BenchmarkRequest",
      outputType: "trevrpc.benchmark.v1.BenchmarkResponse",
    },
  },
});

export class PeerError extends Error {
  constructor(phase, code, message, options = {}) {
    super(message, options);
    this.name = "PeerError";
    this.phase = phase;
    this.code = code;
  }
}

export async function main(argv = process.argv.slice(2), io = process) {
  const config = parseCommandLine(argv);
  switch (config.command) {
    case "capabilities":
      await writeEvent(io.stdout, {
        event: "capabilities",
        roles: ["client", "server"],
        rpc_kinds: ["unary", "client_stream", "server_stream", "bidi"],
        transports: ["native_quic"],
        histogram: "log_linear_v1",
      });
      return;
    case "server":
      await runServer(config, io);
      return;
    case "client":
      await runClient(config, io);
      return;
    default:
      throw new PeerError("configure", "invalid_arguments", usage());
  }
}

export function parseCommandLine(argv) {
  const [command, ...args] = argv;
  if (command === "capabilities") {
    if (args.length !== 0) {
      throw new PeerError("configure", "invalid_arguments", "capabilities takes no options");
    }
    return { command };
  }
  if (command === "server") {
    const options = parseOptions(args, ["listen", "cert", "key"]);
    return {
      command,
      listen: parseAddress(requiredOption(options, "listen"), true),
      cert: requiredOption(options, "cert"),
      key: requiredOption(options, "key"),
    };
  }
  if (command === "client") {
    const options = parseOptions(args, [
      "address",
      "cert",
      "rpc",
      "concurrency",
      "warmup-ms",
      "measurement-ms",
      "request-bytes",
      "response-bytes",
      "messages-per-stream",
    ]);
    const rpcKind = requiredOption(options, "rpc");
    if (!new Set(["unary", "client_stream", "server_stream", "bidi"]).has(rpcKind)) {
      throw new PeerError(
        "configure",
        "invalid_arguments",
        `--rpc must be unary, client_stream, server_stream, or bidi; got ${JSON.stringify(rpcKind)}`,
      );
    }
    return {
      command,
      address: parseAddress(requiredOption(options, "address"), false),
      cert: requiredOption(options, "cert"),
      rpcKind,
      concurrency: parseIntegerOption(options, "concurrency", 1, MaxConcurrency),
      warmupMs: parseIntegerOption(options, "warmup-ms", 0, Number.MAX_SAFE_INTEGER),
      measurementMs: parseIntegerOption(options, "measurement-ms", 1, Number.MAX_SAFE_INTEGER),
      requestBytes: parseIntegerOption(options, "request-bytes", 0, MaxPayloadBytes),
      responseBytes: parseIntegerOption(options, "response-bytes", 0, MaxPayloadBytes),
      messagesPerStream: parseIntegerOption(
        options,
        "messages-per-stream",
        1,
        MaxMessagesPerStream,
      ),
    };
  }
  throw new PeerError("configure", "invalid_arguments", usage());
}

export function logLinearUpperBound(value) {
  const latency = typeof value === "bigint" ? value : BigInt(value);
  if (latency <= 0n) {
    throw new RangeError("histogram latency must be positive");
  }
  const shift = Math.max(latency.toString(2).length - 1 - 9, 0);
  const shiftBits = BigInt(shift);
  return (((latency >> shiftBits) + 1n) << shiftBits) - 1n;
}

export class LogLinearHistogram {
  constructor() {
    this.buckets = new Map();
    this.count = 0n;
  }

  record(value) {
    const upperBound = logLinearUpperBound(value);
    this.buckets.set(upperBound, (this.buckets.get(upperBound) ?? 0n) + 1n);
    this.count += 1n;
  }

  add(other) {
    for (const [upperBound, count] of other.buckets) {
      this.buckets.set(upperBound, (this.buckets.get(upperBound) ?? 0n) + count);
    }
    this.count += other.count;
  }

  toJSON() {
    return [...this.buckets]
      .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
      .map(([upperBound, count]) => ({
        upper_bound_ns: String(upperBound),
        count: String(count),
      }));
  }
}

export function createBenchmarkHandlers() {
  return {
    unary(call) {
      const request = BenchmarkRequest.decode(call.request.body);
      return encodeResponse(request.sequence, request.responseBytes);
    },

    async *clientStream(call) {
      let messageCount = 0n;
      let payloadBytes = 0n;
      for (;;) {
        const frame = await call.recv();
        if (frame == null) {
          yield BenchmarkSummary.encode({
            messageCount: String(messageCount),
            payloadBytes: String(payloadBytes),
          }).finish();
          return;
        }
        const request = BenchmarkRequest.decode(frame.body);
        messageCount += 1n;
        payloadBytes += BigInt(request.payload.byteLength);
      }
    },

    serverStream(call) {
      const request = StreamRequest.decode(call.request.body);
      return encodedResponseStream(request.messageCount, request.responseBytes);
    },

    async *bidi(call) {
      for (;;) {
        const frame = await call.recv();
        if (frame == null) {
          return;
        }
        const request = BenchmarkRequest.decode(frame.body);
        yield encodeResponse(request.sequence, request.responseBytes);
      }
    },
  };
}

export function createClientOperation(client, config) {
  const callOptions = {
    maxResponseBodySize: MaxFrameSize,
    maxResponseMessages: -1,
    maxResponseStreamBodySize: -1,
    streamIdleTimeoutMs: IdleTimeoutMs,
  };

  switch (config.rpcKind) {
    case "unary":
      return async ({ laneIndex, operationIndex }) => {
        const sequence = operationSequence(laneIndex, operationIndex);
        const response = await client.unary({
          sequence,
          payload: new Uint8Array(config.requestBytes),
          responseBytes: config.responseBytes,
        });
        validateResponse(response, sequence, config.responseBytes, "unary response");
        return { requestMessages: 1n, responseMessages: 1n };
      };
    case "client_stream":
      return async () => {
        const call = await client.clientStream(callOptions);
        await sendRequestMessages(call, config);
        const summary = await call.closeAndRecv();
        const expectedMessages = BigInt(config.messagesPerStream);
        const expectedBytes = expectedMessages * BigInt(config.requestBytes);
        if (
          uint64Text(summary.messageCount) !== String(expectedMessages) ||
          uint64Text(summary.payloadBytes) !== String(expectedBytes)
        ) {
          throw new Error(
            `client-stream summary was ${uint64Text(summary.messageCount)} messages and ${uint64Text(summary.payloadBytes)} bytes; expected ${expectedMessages} messages and ${expectedBytes} bytes`,
          );
        }
        return { requestMessages: expectedMessages, responseMessages: 1n };
      };
    case "server_stream":
      return async () => {
        const responses = await client.serverStream(
          {
            messageCount: config.messagesPerStream,
            payload: new Uint8Array(config.requestBytes),
            responseBytes: config.responseBytes,
          },
          callOptions,
        );
        let received = 0;
        for await (const response of responses) {
          validateResponse(
            response,
            String(received),
            config.responseBytes,
            "server-stream response",
          );
          received += 1;
        }
        if (received !== config.messagesPerStream) {
          throw new Error(
            `server stream returned ${received} messages; expected ${config.messagesPerStream}`,
          );
        }
        return { requestMessages: 1n, responseMessages: BigInt(received) };
      };
    case "bidi":
      return async () => {
        const call = await client.bidi(callOptions);
        let received = 0;
        await Promise.all([sendAndClose(), receiveAll()]);
        if (received !== config.messagesPerStream) {
          throw new Error(
            `bidi returned ${received} messages; expected ${config.messagesPerStream}`,
          );
        }
        return {
          requestMessages: BigInt(config.messagesPerStream),
          responseMessages: BigInt(received),
        };

        async function sendAndClose() {
          await sendRequestMessages(call, config);
          await call.closeSend();
        }

        async function receiveAll() {
          for (;;) {
            const response = await call.recv();
            if (response === undefined) {
              return;
            }
            validateResponse(response, String(received), config.responseBytes, "bidi response");
            received += 1;
          }
        }
      };
    default:
      throw new PeerError(
        "configure",
        "invalid_arguments",
        `unsupported RPC kind ${JSON.stringify(config.rpcKind)}`,
      );
  }
}

export function prepareFixedAdmissionPhase({
  operation,
  concurrency,
  durationNs,
  recordLatency,
  now = process.hrtime.bigint,
}) {
  let release;
  const gate = new Promise((resolveGate) => {
    release = resolveGate;
  });
  const state = { failed: false, firstError: null };
  const lanes = Array.from({ length: concurrency }, (_, laneIndex) => runLane(laneIndex));
  let started = false;

  return {
    async start() {
      if (started) {
        throw new Error("benchmark phase has already started");
      }
      started = true;
      const start = now();
      release({ deadline: start + durationNs });
      const results = await Promise.all(lanes);
      const elapsedNs = now() - start;
      const result = emptyPhaseResult();
      for (const lane of results) {
        result.completed += lane.completed;
        result.failed += lane.failed;
        result.requestMessages += lane.requestMessages;
        result.responseMessages += lane.responseMessages;
        result.histogram.add(lane.histogram);
      }
      result.elapsedNs = elapsedNs;
      result.error = state.firstError;
      return result;
    },
  };

  async function runLane(laneIndex) {
    const { deadline } = await gate;
    const result = emptyPhaseResult();
    let operationIndex = 0n;
    while (!state.failed) {
      const operationStart = now();
      if (operationStart >= deadline) {
        break;
      }
      try {
        const counts = await operation({ laneIndex, operationIndex });
        result.completed += 1n;
        result.requestMessages += counts.requestMessages;
        result.responseMessages += counts.responseMessages;
        if (recordLatency) {
          const latency = now() - operationStart;
          result.histogram.record(latency > 0n ? latency : 1n);
        }
      } catch (error) {
        result.failed += 1n;
        state.failed = true;
        state.firstError ??= error;
        break;
      }
      operationIndex += 1n;
    }
    return result;
  }
}

async function runServer(config, io) {
  let server;
  try {
    server = await NodeServer.listen({
      host: config.listen.host,
      port: config.listen.port,
      certFile: config.cert,
      keyFile: config.key,
      path: "/trevrpc",
      maxSessionsPerConnection: 16,
      maxStreamsPerSession: MaxConcurrency,
      maxStreamMessages: -1,
      idleTimeoutMs: IdleTimeoutMs,
      streamIdleTimeoutMs: IdleTimeoutMs,
      maxFrameSize: MaxFrameSize,
      maxPendingSendBytes: MaxFrameSize,
    });
  } catch (error) {
    throw wrapError("listen", "listen_failed", error);
  }

  server.registerService(BenchmarkService, createBenchmarkHandlers());
  const serveDone = server.serve();
  const control = createInterface({ input: io.stdin, crlfDelay: Infinity });
  const termination = terminationSignal();
  let reason;
  try {
    await writeEvent(io.stdout, {
      event: "ready",
      address: formatAddress(config.listen.host, server.port),
      pid: process.pid,
    });
    reason = await Promise.race([
      waitForControl(control, new Set(["SHUTDOWN"])),
      termination.promise,
      serveDone.then(
        () => {
          throw new PeerError("serve", "server_stopped", "server stopped before SHUTDOWN");
        },
        (error) => {
          throw wrapError("serve", "serve_failed", error);
        },
      ),
    ]);
    if (reason === "SHUTDOWN") {
      await writeEvent(io.stdout, { event: "stopped" });
    }
  } finally {
    control.close();
    termination.cleanup();
    server.close();
    await serveDone;
  }
}

async function runClient(config, io) {
  let transport;
  try {
    transport = await RawNodeTransport.connect({
      host: config.address.host,
      port: config.address.port,
      caCertFile: config.cert,
      maxStreamsPerSession: Math.max(128, config.concurrency),
      idleTimeoutMs: IdleTimeoutMs,
      maxFrameSize: MaxFrameSize,
      maxPendingSendBytes: MaxFrameSize,
    });
  } catch (error) {
    throw wrapError("connect", "connect_failed", error);
  }

  try {
    const client = createServiceClient(transport, BenchmarkService, root, {
      maxResponseBodySize: MaxFrameSize,
      maxResponseMessages: -1,
      maxResponseStreamBodySize: -1,
      streamIdleTimeoutMs: IdleTimeoutMs,
    });
    const operation = createClientOperation(client, config);
    try {
      await operation({ laneIndex: 0, operationIndex: 0n });
    } catch (error) {
      throw wrapError("validate", "rpc_failed", error);
    }

    if (config.warmupMs > 0) {
      const warmup = prepareFixedAdmissionPhase({
        operation,
        concurrency: config.concurrency,
        durationNs: BigInt(config.warmupMs) * 1_000_000n,
        recordLatency: false,
      });
      const result = await warmup.start();
      if (result.failed !== 0n) {
        throw wrapError("warmup", "rpc_failed", result.error);
      }
    }

    const admissionNs = BigInt(config.measurementMs) * 1_000_000n;
    const measurement = prepareFixedAdmissionPhase({
      operation,
      concurrency: config.concurrency,
      durationNs: admissionNs,
      recordLatency: true,
    });
    await writeEvent(io.stdout, { event: "armed", pid: process.pid });

    const control = createInterface({ input: io.stdin, crlfDelay: Infinity });
    let command;
    try {
      command = await waitForControl(control, new Set(["START", "SHUTDOWN"]));
    } finally {
      control.close();
    }
    if (command === "SHUTDOWN") {
      return;
    }

    const result = await measurement.start();
    if (result.failed !== 0n) {
      throw wrapError("measure", "rpc_failed", result.error);
    }
    if (result.histogram.count !== result.completed) {
      throw new PeerError(
        "measure",
        "histogram_mismatch",
        `histogram contains ${result.histogram.count} samples for ${result.completed} completed operations`,
      );
    }
    const drainNs = result.elapsedNs > admissionNs ? result.elapsedNs - admissionNs : 0n;
    await writeEvent(io.stdout, {
      event: "sample",
      rpc_kind: config.rpcKind,
      admission_ns: String(admissionNs),
      elapsed_ns: String(result.elapsedNs),
      drain_ns: String(drainNs),
      completed: String(result.completed),
      failed: String(result.failed),
      request_messages: String(result.requestMessages),
      response_messages: String(result.responseMessages),
      histogram: result.histogram.toJSON(),
    });
  } finally {
    transport.close();
  }
}

function parseOptions(args, allowed) {
  const allowedOptions = new Set(allowed);
  const options = new Map();
  for (let index = 0; index < args.length; index += 2) {
    const flag = args[index];
    const value = args[index + 1];
    if (typeof flag !== "string" || !flag.startsWith("--") || value == null) {
      throw new PeerError("configure", "invalid_arguments", `invalid option list: ${usage()}`);
    }
    const name = flag.slice(2);
    if (!allowedOptions.has(name)) {
      throw new PeerError("configure", "invalid_arguments", `unknown option ${flag}`);
    }
    if (options.has(name)) {
      throw new PeerError("configure", "invalid_arguments", `duplicate option ${flag}`);
    }
    options.set(name, value);
  }
  return options;
}

function requiredOption(options, name) {
  const value = options.get(name);
  if (value == null || value === "") {
    throw new PeerError("configure", "invalid_arguments", `missing required option --${name}`);
  }
  return value;
}

function parseIntegerOption(options, name, minimum, maximum) {
  const value = requiredOption(options, name);
  if (!/^(0|[1-9][0-9]*)$/u.test(value)) {
    throw new PeerError(
      "configure",
      "invalid_arguments",
      `--${name} must be an integer from ${minimum} through ${maximum}; got ${JSON.stringify(value)}`,
    );
  }
  const number = Number(value);
  if (!Number.isSafeInteger(number) || number < minimum || number > maximum) {
    throw new PeerError(
      "configure",
      "invalid_arguments",
      `--${name} must be an integer from ${minimum} through ${maximum}; got ${JSON.stringify(value)}`,
    );
  }
  return number;
}

function parseAddress(value, allowZeroPort) {
  let host;
  let portText;
  if (value.startsWith("[")) {
    const match = /^\[([^\]]+)\]:(\d+)$/u.exec(value);
    if (match != null) {
      [, host, portText] = match;
    }
  } else {
    const separator = value.lastIndexOf(":");
    if (separator > 0 && value.indexOf(":") === separator) {
      host = value.slice(0, separator);
      portText = value.slice(separator + 1);
    }
  }
  const port = Number(portText);
  const minimum = allowZeroPort ? 0 : 1;
  if (
    host == null ||
    host === "" ||
    !/^(0|[1-9][0-9]*)$/u.test(portText ?? "") ||
    !Number.isInteger(port) ||
    port < minimum ||
    port > 65_535
  ) {
    throw new PeerError(
      "configure",
      "invalid_arguments",
      `address must be HOST:PORT with a port from ${minimum} through 65535; got ${JSON.stringify(value)}`,
    );
  }
  return { host, port };
}

function formatAddress(host, port) {
  return host.includes(":") ? `[${host}]:${port}` : `${host}:${port}`;
}

function operationSequence(laneIndex, operationIndex) {
  return String(BigInt.asUintN(64, (BigInt(laneIndex) << 48n) | operationIndex));
}

async function sendRequestMessages(call, config) {
  const payload = new Uint8Array(config.requestBytes);
  let sent = 0;
  while (sent < config.messagesPerStream) {
    const count = Math.min(streamBatchSize(config.requestBytes), config.messagesPerStream - sent);
    const messages = Array.from({ length: count }, (_, offset) => ({
      sequence: String(sent + offset),
      payload,
      responseBytes: config.responseBytes,
    }));
    if (messages.length === 1 || typeof call.sendMany !== "function") {
      await call.send(messages[0]);
    } else {
      await call.sendMany(messages);
    }
    sent += count;
  }
}

function encodeResponse(sequence, responseBytes) {
  return BenchmarkResponse.encode({
    sequence: uint64Text(sequence),
    payload: new Uint8Array(responseBytes),
  }).finish();
}

function encodedResponseStream(messageCount, responseBytes) {
  let sent = 0;
  return {
    [Symbol.asyncIterator]() {
      return this;
    },
    next() {
      if (sent >= messageCount) {
        return Promise.resolve({ done: true, value: undefined });
      }
      const body = encodeResponse(String(sent), responseBytes);
      sent += 1;
      return Promise.resolve({ done: false, value: body });
    },
    nextBatch(max = StreamBatchSize) {
      if (sent >= messageCount) {
        return Promise.resolve({ done: true, value: undefined });
      }
      const count = Math.min(messageCount - sent, Math.max(1, max), streamBatchSize(responseBytes));
      const bodies = Array.from({ length: count }, () => {
        const body = encodeResponse(String(sent), responseBytes);
        sent += 1;
        return body;
      });
      return Promise.resolve({ done: false, value: bodies });
    },
  };
}

function streamBatchSize(payloadBytes) {
  const encodedSizeEstimate = Math.max(1, payloadBytes + 32);
  return Math.max(1, Math.min(StreamBatchSize, Math.floor(MaxPayloadBytes / encodedSizeEstimate)));
}

function validateResponse(response, expectedSequence, expectedBytes, label) {
  if (uint64Text(response.sequence) !== String(expectedSequence)) {
    throw new Error(
      `${label} sequence was ${uint64Text(response.sequence)}; expected ${expectedSequence}`,
    );
  }
  if (response.payload.byteLength !== expectedBytes) {
    throw new Error(
      `${label} payload was ${response.payload.byteLength} bytes; expected ${expectedBytes}`,
    );
  }
  validateZeroPayload(response.payload, label);
}

function validateZeroPayload(payload, label) {
  for (const byte of payload) {
    if (byte !== 0) {
      throw new Error(`${label} payload contains non-zero data`);
    }
  }
}

function uint64Text(value) {
  return String(value ?? 0);
}

function emptyPhaseResult() {
  return {
    completed: 0n,
    failed: 0n,
    requestMessages: 0n,
    responseMessages: 0n,
    elapsedNs: 0n,
    error: null,
    histogram: new LogLinearHistogram(),
  };
}

async function waitForControl(control, allowed) {
  for await (const line of control) {
    const command = line.trim();
    if (command === "") {
      continue;
    }
    if (!allowed.has(command)) {
      throw new PeerError(
        "control",
        "invalid_command",
        `unexpected control command ${JSON.stringify(command)}`,
      );
    }
    return command;
  }
  throw new PeerError("control", "stdin_closed", "standard input closed before a command");
}

function terminationSignal() {
  let resolveSignal;
  const promise = new Promise((resolvePromise) => {
    resolveSignal = resolvePromise;
  });
  const onSigint = () => resolveSignal("SIGINT");
  const onSigterm = () => resolveSignal("SIGTERM");
  process.once("SIGINT", onSigint);
  process.once("SIGTERM", onSigterm);
  return {
    promise,
    cleanup() {
      process.removeListener("SIGINT", onSigint);
      process.removeListener("SIGTERM", onSigterm);
    },
  };
}

async function writeEvent(stdout, event) {
  const line = `${JSON.stringify({ schema_version: SchemaVersion, ...event, peer: Peer })}\n`;
  await new Promise((resolveWrite, rejectWrite) => {
    stdout.write(line, (error) => (error == null ? resolveWrite() : rejectWrite(error)));
  });
}

function wrapError(phase, code, error) {
  if (error instanceof PeerError) {
    return error;
  }
  return new PeerError(phase, code, error?.message ?? String(error), { cause: error });
}

function usage() {
  return "usage: trevrpc-bench-peer-js capabilities | server --listen HOST:PORT --cert FILE --key FILE | client --address HOST:PORT --cert FILE --rpc KIND --concurrency N --warmup-ms N --measurement-ms N --request-bytes N --response-bytes N --messages-per-stream N";
}

function isMainModule() {
  return process.argv[1] != null && resolve(process.argv[1]) === fileURLToPath(import.meta.url);
}

if (isMainModule()) {
  try {
    await main();
  } catch (error) {
    const peerError = wrapError("run", "peer_failed", error);
    console.error(`trevrpc-bench-peer-js: ${peerError.message}`);
    try {
      await writeEvent(process.stdout, {
        event: "error",
        phase: peerError.phase,
        code: peerError.code,
        message: peerError.message,
      });
    } catch (writeError) {
      console.error(`trevrpc-bench-peer-js: could not write error event: ${writeError.message}`);
    }
    process.exitCode = 1;
  }
}
