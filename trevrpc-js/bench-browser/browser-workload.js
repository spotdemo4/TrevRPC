import {
  BenchmarkService,
  IdleTimeoutMs,
  MaxFrameSize,
  createClientOperation,
  prepareFixedAdmissionPhase,
  root,
  sampleForResult,
} from "trevrpc-bench-peer-js/common";
import { connect, createServiceClient } from "trevrpc-js";

let channel;
let measurement;
let config;
let admissionNs;

export async function connectAndPrepare(input) {
  if (channel != null) {
    throw new Error("Chromium benchmark workload is already connected");
  }
  config = input.config;
  const hash = base64Bytes(input.certificateHash);
  channel = await connect(`https://${input.address}/trevrpc`, {
    serverCertificateHashes: [{ algorithm: "sha-256", value: hash }],
    maxFrameSize: MaxFrameSize,
    streamIdleTimeoutMs: IdleTimeoutMs,
  });

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
      throw result.error ?? new Error("Chromium benchmark warmup failed");
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
    throw new Error("Chromium benchmark workload is not armed");
  }
  const result = await measurement.start();
  return sampleForResult(config, admissionNs, result);
}

export function close() {
  channel?.close();
  channel = undefined;
  measurement = undefined;
}

function base64Bytes(value) {
  const binary = atob(value);
  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}
