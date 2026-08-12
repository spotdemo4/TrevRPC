import { connect, createServiceClient } from "@trevrpc/trevrpc-js";
import {
  BenchmarkService,
  IdleTimeoutMs,
  MaxFrameSize,
  createClientOperation,
  prepareFixedAdmissionPhase,
  root,
  sampleForResult,
} from "trevrpc-bench-peer-js/common";

const ConnectTimeoutMs = 20_000;

let channel;
let measurement;
let config;
let admissionNs;

export async function connectAndPrepare(input) {
  if (channel != null) {
    throw new Error("Benchmark workload is already connected");
  }
  config = input.config;
  const connectOptions = {
    maxFrameSize: MaxFrameSize,
    onStateChange(event) {
      if ((event.state === "connecting" || event.state === "reconnecting") && event.error != null) {
        console.error(
          `WebTransport ${event.state} attempt ${event.attempt}: ${errorMessage(event.error)}`,
        );
      }
    },
    streamIdleTimeoutMs: IdleTimeoutMs,
    timeoutMs: ConnectTimeoutMs,
  };
  if (input.certificateHash != null && input.certificateHash !== "") {
    const hash = base64Bytes(input.certificateHash);
    connectOptions.serverCertificateHashes = [{ algorithm: "sha-256", value: hash }];
  }
  channel = await connect(`https://${input.address}/trevrpc`, connectOptions);

  const client = createServiceClient(channel, BenchmarkService, root, {
    maxResponseBodySize: MaxFrameSize,
    maxResponseMessages: -1,
    maxResponseStreamBodySize: -1,
    streamIdleTimeoutMs: IdleTimeoutMs,
  });
  const operation = createClientOperation(client, config);
  await operation({ laneIndex: 0, operationIndex: 0n });

  if (config.warmupMs > 0) {
    const warmup = prepareFixedAdmissionPhase({
      operation,
      concurrency: config.concurrency,
      durationNs: BigInt(config.warmupMs) * 1_000_000n,
      recordLatency: false,
    });
    const result = await warmup.start();
    if (result.failed !== 0n) {
      throw result.error ?? new Error("Benchmark warmup failed");
    }
  }

  admissionNs = BigInt(config.measurementMs) * 1_000_000n;
  measurement = prepareFixedAdmissionPhase({
    operation,
    concurrency: config.concurrency,
    durationNs: admissionNs,
    recordLatency: true,
  });
}

export async function startMeasurement() {
  if (measurement == null) {
    throw new Error("Benchmark workload is not armed");
  }
  const result = await measurement.start();
  return sampleForResult(config, admissionNs, result);
}

export function close() {
  channel?.close();
  channel = undefined;
  measurement = undefined;
}

function errorMessage(error) {
  if (!(error instanceof Error)) {
    return String(error);
  }
  const parts = [error.name, error.message].filter((part) => part !== "");
  if (error.source != null) {
    parts.push(`source=${error.source}`);
  }
  if (error.streamErrorCode != null) {
    parts.push(`streamErrorCode=${error.streamErrorCode}`);
  }
  return parts.join(": ");
}

function base64Bytes(value) {
  const binary = atob(value);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}
