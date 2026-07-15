#!/usr/bin/env node

import { readFile } from "node:fs/promises";
import { resolve } from "node:path";
import { createInterface } from "node:readline";
import { fileURLToPath } from "node:url";

import * as grpc from "@grpc/grpc-js";
import {
  Code,
  RpcStreamFrameKind,
  createRoot,
  createServiceClient,
  invalidArgument,
  resourceExhausted,
  statusError,
} from "trevrpc-js";
import { NodeServer } from "trevrpc-js/node";
import { RawNodeTransport } from "trevrpc-js/node/advanced";

const SchemaVersion = 2;
const Peer = "js";
const ServiceName = "trevrpc.benchmark.v1.BenchmarkService";
const Stacks = new Set(["trevrpc_native_quic", "grpc_http2"]);
const IdleTimeoutMs = 600_000;
const ConnectTimeoutMs = 30_000;
const ShutdownTimeoutMs = 5_000;
const StreamBatchSize = 16;
const MaxConcurrency = 1024;
const MaxPayloadBytes = 64 * 1024 * 1024;
const MaxMessagesPerStream = 1_000_000;
const MaxFrameSize = MaxPayloadBytes + 1024;

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

export const GrpcBenchmarkService = Object.freeze({
  unary: grpcMethod("Unary", BenchmarkRequest, BenchmarkResponse, false, false),
  clientStream: grpcMethod("ClientStream", BenchmarkRequest, BenchmarkSummary, true, false),
  serverStream: grpcMethod("ServerStream", StreamRequest, BenchmarkResponse, false, true),
  bidi: grpcMethod("Bidi", BenchmarkRequest, BenchmarkResponse, true, true),
});

const GrpcBenchmarkClient = grpc.makeGenericClientConstructor(GrpcBenchmarkService, ServiceName);
const GrpcOptions = Object.freeze({
  "grpc.default_compression_algorithm": grpc.compressionAlgorithms.identity,
  "grpc.max_receive_message_length": MaxFrameSize,
  "grpc.max_send_message_length": MaxFrameSize,
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
        stacks: ["trevrpc_native_quic", "grpc_http2"],
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
    const options = parseOptions(args, ["stack", "listen", "cert", "key"]);
    return {
      command,
      stack: parseStack(options),
      listen: parseAddress(requiredOption(options, "listen"), true),
      cert: requiredOption(options, "cert"),
      key: requiredOption(options, "key"),
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
      stack: parseStack(options),
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
      return encodeResponseMessage(responseForRequest(request));
    },

    async *clientStream(call) {
      const summary = emptySummary();
      for (;;) {
        const body = requestBody(await call.recv());
        if (body == null) {
          yield BenchmarkSummary.encode(summaryResponse(summary)).finish();
          return;
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

export function createGrpcBenchmarkHandlers() {
  return {
    unary(call, callback) {
      try {
        callback(null, responseForRequest(call.request));
      } catch (error) {
        callback(toGrpcError(error));
      }
    },

    clientStream(call, callback) {
      void receiveGrpcClientStream(call, callback);
    },

    serverStream(call) {
      void sendGrpcServerStream(call);
    },

    bidi(call) {
      void runGrpcBidi(call);
    },
  };
}

async function receiveGrpcClientStream(call, callback) {
  const summary = emptySummary();
  try {
    for await (const request of call) {
      addSummaryRequest(summary, request);
    }
    callback(null, summaryResponse(summary));
  } catch (error) {
    callback(toGrpcError(error));
  }
}

async function sendGrpcServerStream(call) {
  try {
    validateMessageCount(call.request.messageCount);
    validateRequestPayload(call.request.payload);
    validateResponseBytes(call.request.responseBytes);
    for (let sequence = 0; sequence < call.request.messageCount; sequence += 1) {
      await writeGrpcMessage(call, createResponse(String(sequence), call.request.responseBytes));
    }
    call.end();
  } catch (error) {
    call.destroy(toGrpcError(error));
  }
}

async function runGrpcBidi(call) {
  let messageCount = 0;
  try {
    for await (const request of call.iterator({ destroyOnReturn: false })) {
      messageCount += 1;
      if (messageCount > MaxMessagesPerStream) {
        throw resourceExhausted("message count exceeded the benchmark peer limit");
      }
      await writeGrpcMessage(call, responseForRequest(request));
    }
    call.end();
  } catch (error) {
    call.destroy(toGrpcError(error));
  }
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

function createGrpcClientAdapter(client) {
  return {
    unary(request) {
      return new Promise((resolveResponse, rejectResponse) => {
        client.unary(request, (error, response) => {
          if (error != null) {
            rejectResponse(error);
          } else if (response == null) {
            rejectResponse(new Error("gRPC unary call returned no response"));
          } else {
            resolveResponse(response);
          }
        });
      });
    },

    clientStream() {
      let resolveResponse;
      const response = new Promise((resolvePromise) => {
        resolveResponse = resolvePromise;
      });
      const call = client.clientStream((error, value) => {
        resolveResponse({ error, value });
      });
      const terminalStatus = grpcTerminalStatus(call);
      return Promise.resolve({
        send(message) {
          return writeGrpcMessage(call, message);
        },
        async sendMany(messages) {
          for (const message of messages) {
            await writeGrpcMessage(call, message);
          }
        },
        async closeAndRecv() {
          call.end();
          const [outcome, status] = await Promise.all([response, terminalStatus]);
          assertGrpcOk(status);
          if (outcome.error != null) {
            throw outcome.error;
          }
          if (outcome.value == null) {
            throw new Error("gRPC client-stream call returned no response");
          }
          return outcome.value;
        },
      });
    },

    serverStream(request) {
      const call = client.serverStream(request);
      const terminalStatus = grpcTerminalStatus(call);
      return Promise.resolve(grpcResponseStream(call, terminalStatus));
    },

    bidi() {
      const call = client.bidi();
      const iterator = call[Symbol.asyncIterator]();
      const terminalStatus = grpcTerminalStatus(call);
      return Promise.resolve({
        send(message) {
          return writeGrpcMessage(call, message);
        },
        async sendMany(messages) {
          for (const message of messages) {
            await writeGrpcMessage(call, message);
          }
        },
        closeSend() {
          return endGrpcWrites(call);
        },
        async recv() {
          let result;
          try {
            result = await iterator.next();
          } catch (error) {
            assertGrpcOk(await terminalStatus);
            throw error;
          }
          if (result.done) {
            assertGrpcOk(await terminalStatus);
            return undefined;
          }
          return result.value;
        },
      });
    },
  };
}

async function* grpcResponseStream(call, terminalStatus) {
  try {
    for await (const response of call) {
      yield response;
    }
  } catch (error) {
    assertGrpcOk(await terminalStatus);
    throw error;
  }
  assertGrpcOk(await terminalStatus);
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
  if (config.stack === "grpc_http2") {
    return await listenGrpcBenchmarkServer(config);
  }

  const server = await NodeServer.listen({
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

export async function listenGrpcBenchmarkServer(config) {
  const [certificate, privateKey] = await Promise.all([
    readFile(config.cert),
    readFile(config.key),
  ]);
  const server = new grpc.Server({
    ...GrpcOptions,
    "grpc.max_concurrent_streams": MaxConcurrency,
  });
  server.addService(GrpcBenchmarkService, createGrpcBenchmarkHandlers());
  const credentials = grpc.ServerCredentials.createSsl(
    null,
    [{ private_key: privateKey, cert_chain: certificate }],
    false,
  );
  let port;
  try {
    port = await new Promise((resolvePort, rejectPort) => {
      server.bindAsync(
        formatAddress(config.listen.host, config.listen.port),
        credentials,
        (error, boundPort) => {
          if (error == null) {
            resolvePort(boundPort);
          } else {
            rejectPort(error);
          }
        },
      );
    });
  } catch (error) {
    server.forceShutdown();
    throw error;
  }
  let closePromise;
  return {
    port,
    done: new Promise(() => {}),
    close() {
      closePromise ??= new Promise((resolveClose, rejectClose) => {
        const timeout = setTimeout(() => {
          server.forceShutdown();
          resolveClose();
        }, ShutdownTimeoutMs);
        server.tryShutdown((error) => {
          clearTimeout(timeout);
          if (error == null) {
            resolveClose();
          } else {
            rejectClose(error);
          }
        });
      });
      return closePromise;
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
    connection.close();
  }
}

async function connectBenchmarkClient(config) {
  if (config.stack === "grpc_http2") {
    return await connectGrpcBenchmarkClient(config);
  }

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

export async function connectGrpcBenchmarkClient(config) {
  const certificate = await readFile(config.cert);
  const credentials = grpc.credentials.createSsl(certificate, null, null, {
    rejectUnauthorized: true,
  });
  const grpcClient = new GrpcBenchmarkClient(
    formatAddress(config.address.host, config.address.port),
    credentials,
    GrpcOptions,
  );
  try {
    await new Promise((resolveReady, rejectReady) => {
      grpcClient.waitForReady(Date.now() + ConnectTimeoutMs, (error) => {
        if (error == null) {
          resolveReady();
        } else {
          rejectReady(error);
        }
      });
    });
  } catch (error) {
    grpcClient.close();
    throw error;
  }
  return {
    client: createGrpcClientAdapter(grpcClient),
    close() {
      grpcClient.close();
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

function parseStack(options) {
  const stack = requiredOption(options, "stack");
  if (!Stacks.has(stack)) {
    throw new PeerError(
      "configure",
      "invalid_arguments",
      `--stack must be trevrpc_native_quic or grpc_http2; got ${JSON.stringify(stack)}`,
    );
  }
  return stack;
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
    if (typeof call.sendMany !== "function") {
      for (const message of messages) {
        await call.send(message);
      }
    } else if (messages.length === 1) {
      await call.send(messages[0]);
    } else {
      await call.sendMany(messages);
    }
    sent += count;
  }
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

function grpcMethod(name, requestType, responseType, requestStream, responseStream) {
  return Object.freeze({
    path: `/${ServiceName}/${name}`,
    requestStream,
    responseStream,
    requestSerialize: protobufSerializer(requestType),
    requestDeserialize: protobufDeserializer(requestType),
    responseSerialize: protobufSerializer(responseType),
    responseDeserialize: protobufDeserializer(responseType),
    originalName: name,
  });
}

function protobufSerializer(type) {
  return (value) => {
    const encoded = type.encode(value).finish();
    return Buffer.from(encoded.buffer, encoded.byteOffset, encoded.byteLength);
  };
}

function protobufDeserializer(type) {
  return (value) => type.decode(value);
}

function writeGrpcMessage(call, message) {
  return new Promise((resolveWrite, rejectWrite) => {
    try {
      call.write(message, (error) => {
        if (error == null) {
          resolveWrite();
        } else {
          rejectWrite(error);
        }
      });
    } catch (error) {
      rejectWrite(error);
    }
  });
}

function endGrpcWrites(call) {
  return new Promise((resolveEnd, rejectEnd) => {
    call.end((error) => {
      if (error == null) {
        resolveEnd();
      } else {
        rejectEnd(error);
      }
    });
  });
}

function grpcTerminalStatus(call) {
  return new Promise((resolveStatus) => {
    call.once("status", resolveStatus);
  });
}

function assertGrpcOk(status) {
  if (status.code === grpc.status.OK) {
    return;
  }
  const error = new Error(`gRPC status ${status.code}: ${status.details}`);
  error.code = status.code;
  error.details = status.details;
  throw error;
}

function toGrpcError(error) {
  const code = Number.isInteger(error?.code) ? error.code : grpc.status.UNKNOWN;
  const details = error?.statusMessage ?? error?.details ?? error?.message ?? String(error);
  return Object.assign(new Error(details), { code, details });
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
  return "usage: trevrpc-bench-peer-js capabilities | server --stack STACK --listen HOST:PORT --cert FILE --key FILE | client --stack STACK --address HOST:PORT --cert FILE --rpc KIND --concurrency N --warmup-ms N --measurement-ms N --request-bytes N --response-bytes N --messages-per-stream N";
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
