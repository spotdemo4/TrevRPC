#!/usr/bin/env node

import { resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

import {
  Code,
  RpcStreamFrameKind,
  createServiceClient,
  invalidArgument,
  resourceExhausted,
  statusError,
} from "trevrpc-js";
import { NodeServer } from "trevrpc-js/node";
import { RawNodeTransport } from "trevrpc-js/node/advanced";

import {
  BenchmarkService,
  IdleTimeoutMs,
  MaxConcurrency,
  MaxFrameSize,
  MaxMessagesPerStream,
  MaxPayloadBytes,
  RpcKinds,
  SchemaVersion,
  StreamBatchSize,
  createClientOperation,
  parseWorkloadOptions,
  prepareFixedAdmissionPhase,
  root,
  sampleForResult,
} from "./common.js";

export {
  BenchmarkService,
  LogLinearHistogram,
  createClientOperation,
  logLinearUpperBound,
  prepareFixedAdmissionPhase,
  root,
} from "./common.js";

const Peer = "js";
const ClientStacks = new Set(["trevrpc_native_quic"]);
const ServerStacks = new Set(["trevrpc_native_quic", "trevrpc_webtransport"]);

const BenchmarkRequest = root.lookupType("trevrpc.benchmark.v1.BenchmarkRequest");
const BenchmarkResponse = root.lookupType("trevrpc.benchmark.v1.BenchmarkResponse");
const StreamRequest = root.lookupType("trevrpc.benchmark.v1.StreamRequest");
const BenchmarkSummary = root.lookupType("trevrpc.benchmark.v1.BenchmarkSummary");

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
        roles: {
          client: [...ClientStacks],
          server: [...ServerStacks],
        },
        rpc_kinds: RpcKinds,
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
    const options = parseOptions(args, ["stack", "listen", "cert", "key", "webtransport-origin"]);
    const stack = parseStack(options, ServerStacks);
    const webtransportOrigin = options.get("webtransport-origin");
    if (
      stack === "trevrpc_webtransport" &&
      (webtransportOrigin == null || webtransportOrigin === "")
    ) {
      throw new PeerError(
        "configure",
        "invalid_arguments",
        "missing required option --webtransport-origin",
      );
    }
    if (stack !== "trevrpc_webtransport" && webtransportOrigin != null) {
      throw new PeerError(
        "configure",
        "invalid_arguments",
        "--webtransport-origin is only valid with server stack trevrpc_webtransport",
      );
    }
    return {
      command,
      stack,
      listen: parseAddress(requiredOption(options, "listen"), true),
      cert: requiredOption(options, "cert"),
      key: requiredOption(options, "key"),
      ...(webtransportOrigin == null
        ? {}
        : { webtransportOrigin: parseOrigin(webtransportOrigin) }),
    };
  }
  if (command === "client") {
    const options = parseOptions(args, [
      "stack",
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
    let workload;
    try {
      workload = parseWorkloadOptions(options, requiredOption, parseIntegerOption);
    } catch (error) {
      throw wrapError("configure", "invalid_arguments", error);
    }
    return {
      command,
      stack: parseStack(options, ClientStacks),
      address: parseAddress(requiredOption(options, "address"), false),
      cert: requiredOption(options, "cert"),
      ...workload,
    };
  }
  throw new PeerError("configure", "invalid_arguments", usage());
}

export function createBenchmarkHandlers() {
  return {
    unary(call) {
      const request = BenchmarkRequest.decode(call.request.body);
      return encodeResponseMessage(responseForRequest(request));
    },

    async clientStream(call) {
      const summary = emptySummary();
      for (;;) {
        const body = requestBody(await call.recv());
        if (body == null) {
          return BenchmarkSummary.encode(summaryResponse(summary)).finish();
        }
        addSummaryRequest(summary, BenchmarkRequest.decode(body));
      }
    },

    serverStream(call) {
      const request = StreamRequest.decode(call.request.body);
      validateMessageCount(request.messageCount);
      validateRequestPayload(request.payload);
      validateResponseBytes(request.responseBytes);
      return encodedResponseStream(request.messageCount, request.responseBytes);
    },

    async *bidi(call) {
      let messageCount = 0;
      for (;;) {
        const body = requestBody(await call.recv());
        if (body == null) {
          return;
        }
        messageCount += 1;
        if (messageCount > MaxMessagesPerStream) {
          throw resourceExhausted("message count exceeded the benchmark peer limit");
        }
        const request = BenchmarkRequest.decode(body);
        yield encodeResponseMessage(responseForRequest(request));
      }
    },
  };
}

function requestBody(frame) {
  if (frame == null) {
    return null;
  }
  const kind = frame.kind ?? RpcStreamFrameKind.Message;
  if (kind === RpcStreamFrameKind.Message) {
    return frame.body ?? new Uint8Array(0);
  }
  if (kind === RpcStreamFrameKind.Status) {
    const code = frame.status ?? Code.Ok;
    if (code !== Code.Ok) {
      throw statusError(code, frame.message ?? "", frame.metadata ?? {});
    }
    return null;
  }
  throw invalidArgument("request stream contained an unknown frame kind");
}

async function runServer(config, io) {
  let server;
  try {
    server = await listenBenchmarkServer(config);
  } catch (error) {
    throw wrapError("listen", "listen_failed", error);
  }

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
      server.done.then(
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
    await server.close();
  }
}

async function listenBenchmarkServer(config) {
  const server = await NodeServer.listen({
    host: config.listen.host,
    port: config.listen.port,
    certFile: config.cert,
    keyFile: config.key,
    ...(config.stack === "trevrpc_webtransport"
      ? { path: "/trevrpc", origin: config.webtransportOrigin }
      : {}),
    maxSessionsPerConnection: 16,
    maxStreamsPerSession: MaxConcurrency,
    maxStreamMessages: -1,
    idleTimeoutMs: IdleTimeoutMs,
    streamIdleTimeoutMs: IdleTimeoutMs,
    maxFrameSize: MaxFrameSize,
    maxPendingSendBytes: MaxFrameSize,
  });
  server.registerService(BenchmarkService, createBenchmarkHandlers());
  const done = server.serve();
  return {
    port: server.port,
    done,
    async close() {
      server.close();
      await done;
    },
  };
}

async function runClient(config, io) {
  let connection;
  try {
    connection = await connectBenchmarkClient(config);
  } catch (error) {
    throw wrapError("connect", "connect_failed", error);
  }

  try {
    const operation = createClientOperation(connection.client, config);
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
    let sample;
    try {
      sample = sampleForResult(config, admissionNs, result);
    } catch (error) {
      throw wrapError("measure", "histogram_mismatch", error);
    }
    await writeEvent(io.stdout, sample);
  } finally {
    connection.close();
  }
}

async function connectBenchmarkClient(config) {
  const transport = await RawNodeTransport.connect({
    host: config.address.host,
    port: config.address.port,
    caCertFile: config.cert,
    maxStreamsPerSession: Math.max(128, config.concurrency),
    idleTimeoutMs: IdleTimeoutMs,
    maxFrameSize: MaxFrameSize,
    maxPendingSendBytes: MaxFrameSize,
  });
  return {
    client: createServiceClient(transport, BenchmarkService, root, {
      maxResponseBodySize: MaxFrameSize,
      maxResponseMessages: -1,
      maxResponseStreamBodySize: -1,
      streamIdleTimeoutMs: IdleTimeoutMs,
    }),
    close() {
      transport.close();
    },
  };
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

function parseStack(options, stacks) {
  const stack = requiredOption(options, "stack");
  if (!stacks.has(stack)) {
    throw new PeerError(
      "configure",
      "invalid_arguments",
      `--stack must be ${[...stacks].join(", ")}; got ${JSON.stringify(stack)}`,
    );
  }
  return stack;
}

function parseOrigin(value) {
  let url;
  try {
    url = new URL(value);
  } catch {
    throw new PeerError(
      "configure",
      "invalid_arguments",
      `--webtransport-origin must be an HTTP origin; got ${JSON.stringify(value)}`,
    );
  }
  if ((url.protocol !== "http:" && url.protocol !== "https:") || url.origin !== value) {
    throw new PeerError(
      "configure",
      "invalid_arguments",
      `--webtransport-origin must be an HTTP origin; got ${JSON.stringify(value)}`,
    );
  }
  return url.origin;
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

function responseForRequest(request) {
  validateRequestPayload(request.payload);
  validateResponseBytes(request.responseBytes);
  return createResponse(request.sequence, request.responseBytes);
}

function createResponse(sequence, responseBytes) {
  return {
    sequence: uint64Text(sequence),
    payload: new Uint8Array(responseBytes),
  };
}

function encodeResponseMessage(response) {
  return BenchmarkResponse.encode(response).finish();
}

function emptySummary() {
  return { messageCount: 0n, payloadBytes: 0n };
}

function addSummaryRequest(summary, request) {
  validateRequestPayload(request.payload);
  summary.messageCount += 1n;
  if (summary.messageCount > BigInt(MaxMessagesPerStream)) {
    throw resourceExhausted("message count exceeded the benchmark peer limit");
  }
  summary.payloadBytes += BigInt(request.payload.byteLength);
}

function summaryResponse(summary) {
  return {
    messageCount: String(summary.messageCount),
    payloadBytes: String(summary.payloadBytes),
  };
}

function validateResponseBytes(responseBytes) {
  if (!Number.isInteger(responseBytes) || responseBytes < 0 || responseBytes > MaxPayloadBytes) {
    throw invalidArgument("response_bytes is outside the benchmark peer limit");
  }
}

function validateRequestPayload(payload) {
  if (payload.byteLength > MaxPayloadBytes) {
    throw invalidArgument("request payload is outside the benchmark peer limit");
  }
}

function validateMessageCount(messageCount) {
  if (!Number.isInteger(messageCount) || messageCount < 1 || messageCount > MaxMessagesPerStream) {
    throw invalidArgument("message_count is outside the benchmark peer limit");
  }
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
      const body = encodeResponseMessage(createResponse(String(sent), responseBytes));
      sent += 1;
      return Promise.resolve({ done: false, value: body });
    },
    nextBatch(max = StreamBatchSize) {
      if (sent >= messageCount) {
        return Promise.resolve({ done: true, value: undefined });
      }
      const count = Math.min(messageCount - sent, Math.max(1, max), streamBatchSize(responseBytes));
      const bodies = Array.from({ length: count }, () => {
        const body = encodeResponseMessage(createResponse(String(sent), responseBytes));
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

function uint64Text(value) {
  return String(value ?? 0);
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
  return "usage: trevrpc-bench-peer-js capabilities | server --stack STACK --listen HOST:PORT --cert FILE --key FILE [--webtransport-origin ORIGIN] | client --stack STACK --address HOST:PORT --cert FILE --rpc KIND --concurrency N --warmup-ms N --measurement-ms N --request-bytes N --response-bytes N --messages-per-stream N";
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
