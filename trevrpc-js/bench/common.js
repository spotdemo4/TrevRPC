import { createRoot } from "trevrpc-js";

export const SchemaVersion = 4;
export const RpcKinds = Object.freeze(["unary", "client_stream", "server_stream", "bidi"]);
export const IdleTimeoutMs = 600_000;
export const StreamBatchSize = 16;
export const MaxConcurrency = 1024;
export const MaxPayloadBytes = 64 * 1024 * 1024;
export const MaxMessagesPerStream = 1_000_000;
export const MaxFrameSize = MaxPayloadBytes + 1024;

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

export const BenchmarkService = Object.freeze({
  name: "BenchmarkService",
  fullName: "trevrpc.benchmark.v1.BenchmarkService",
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

export function parseWorkloadOptions(options, requiredOption, parseIntegerOption) {
  const rpcKind = requiredOption(options, "rpc");
  if (!RpcKinds.includes(rpcKind)) {
    throw new Error(
      `--rpc must be unary, client_stream, server_stream, or bidi; got ${JSON.stringify(rpcKind)}`,
    );
  }
  return {
    rpcKind,
    concurrency: parseIntegerOption(options, "concurrency", 1, MaxConcurrency),
    warmupMs: parseIntegerOption(options, "warmup-ms", 0, Number.MAX_SAFE_INTEGER),
    measurementMs: parseIntegerOption(options, "measurement-ms", 1, Number.MAX_SAFE_INTEGER),
    requestBytes: parseIntegerOption(options, "request-bytes", 0, MaxPayloadBytes),
    responseBytes: parseIntegerOption(options, "response-bytes", 0, MaxPayloadBytes),
    messagesPerStream: parseIntegerOption(options, "messages-per-stream", 1, MaxMessagesPerStream),
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
      throw new Error(`unsupported RPC kind ${JSON.stringify(config.rpcKind)}`);
  }
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

export function prepareFixedAdmissionPhase({
  operation,
  concurrency,
  durationNs,
  recordLatency,
  now = monotonicNowNs,
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

export function sampleForResult(config, admissionNs, result) {
  if (result.failed !== 0n) {
    throw result.error ?? new Error("benchmark operation failed");
  }
  if (result.histogram.count !== result.completed) {
    throw new Error(
      `histogram contains ${result.histogram.count} samples for ${result.completed} completed operations`,
    );
  }
  const drainNs = result.elapsedNs > admissionNs ? result.elapsedNs - admissionNs : 0n;
  return {
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
  };
}

function monotonicNowNs() {
  return BigInt(Math.floor(performance.now() * 1_000_000));
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
  for (const byte of response.payload) {
    if (byte !== 0) {
      throw new Error(`${label} payload contains non-zero data`);
    }
  }
}

function uint64Text(value) {
  return String(value ?? 0);
}
