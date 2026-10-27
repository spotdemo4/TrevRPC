// The load benchmark's browser client: runs one scenario of bench/README.md over one lane for a
// warmup and a measured window, and summarizes it as the native clients do. It only needs the web
// platform, so the measuring also runs under Node tests.
import { createClient } from "@connectrpc/connect";
import { createConnectTransport, createGrpcWebTransport } from "@connectrpc/connect-web";

import { Echo } from "../build/interop/gen-es/interop_pb.js";
import { EchoClient } from "../build/interop/gen/interop.trevrpc.js";
import { BenchLanes } from "./bench-lanes.js";
import { connectLane } from "./lane.js";

const ConnectTimeoutMs = 10_000;
/** How long operations still running at the end of the window get to finish, as natively. */
const GraceMs = 10_000;
/** How long a worker waits after a failed operation before the next. */
const ErrorBackoffMs = 1;
const PadKey = "bench-pad";

/**
 * Runs the benchmark that input describes: { transport (the lane), url, webSocketUrl, certHash,
 * scenario, size, metadataSize, concurrency, duration, warmup (seconds), messages }. Resolves
 * with the result line's fields. onError sees the first operation that fails.
 */
export async function runBench(input, { now = () => performance.now(), onError = () => {} } = {}) {
  const lane = BenchLanes[input.transport];
  const abort = new AbortController();
  const target = lane.transport
    ? await trevrpcTarget(input, abort.signal)
    : await fetchTarget(input, lane, abort.signal);
  try {
    const start = now() + input.warmup * 1000;
    const end = start + input.duration * 1000;
    const stop = setTimeout(
      () => abort.abort(new Error("the measured window ended")),
      end + GraceMs - now(),
    );
    const operations = Array.from({ length: input.concurrency }, () =>
      target.operation(input.scenario, "x".repeat(input.size), input.messages),
    );
    const measured = await measure({ operations, start, end, now, onError });
    clearTimeout(stop);
    return summarize(input, measured);
  } finally {
    abort.abort();
    target.close();
  }
}

/**
 * Runs each operation back to back on its own worker until end. Only the operations that start
 * and finish within [start, end] count: their latencies in microseconds, the messages they
 * carried, and the ones that failed. onError sees the first failure.
 */
export async function measure({
  operations,
  start,
  end,
  now,
  backoffMs = ErrorBackoffMs,
  onError = () => {},
}) {
  const latencies = [];
  let messages = 0;
  let errors = 0;
  let failed = false;
  await Promise.all(
    operations.map(async (operation) => {
      try {
        for (;;) {
          const began = now();
          if (began >= end) {
            return;
          }
          let carried;
          try {
            carried = await operation.run();
          } catch (error) {
            if (!failed) {
              failed = true;
              onError(error);
            }
            if (began >= start && now() <= end) {
              errors++;
            }
            await new Promise((resolve) => setTimeout(resolve, backoffMs));
            continue;
          }
          const finished = now();
          if (began >= start && finished <= end) {
            latencies.push((finished - began) * 1000);
            messages += carried;
          }
        }
      } finally {
        operation.close();
      }
    }),
  );
  return { latencies, messages, errors };
}

/** The result line of what measure found, with input's description of the run. */
export function summarize(input, { latencies, messages, errors }) {
  if (latencies.length === 0) {
    throw new Error(
      `no operation completed in the ${input.duration}s measured window (${errors} failed)`,
    );
  }
  const sorted = Float64Array.from(latencies).sort();
  const seconds = input.duration;
  return {
    scenario: input.scenario,
    transport: input.transport,
    size: input.size,
    metadata_size: input.metadataSize,
    metadata_scope: "call",
    concurrency: input.concurrency,
    connections: 1,
    duration_s: seconds,
    ops: sorted.length,
    msgs: messages,
    errors,
    ops_per_sec: sorted.length / seconds,
    msgs_per_sec: messages / seconds,
    bytes_per_sec: (messages / seconds) * input.size,
    latency_us: {
      p50: percentile(sorted, 0.5),
      p90: percentile(sorted, 0.9),
      p99: percentile(sorted, 0.99),
      p999: percentile(sorted, 0.999),
    },
    lost: 0,
    payload: "repeat",
    compression: "none",
  };
}

/** The nearest-rank percentile p of sorted values, to the nanosecond. */
export function percentile(sorted, p) {
  return Math.round(sorted[Math.max(Math.ceil(p * sorted.length), 1) - 1] * 1000) / 1000;
}

/** The bench-pad entry as a string, or no metadata. */
function padValue(input) {
  return input.metadataSize > 0 ? "x".repeat(input.metadataSize) : undefined;
}

/** A TrevRPC channel over the lane's transport, and the scenarios' operations on it. */
async function trevrpcTarget(input, signal) {
  const channel = await connectLane({
    url: input.url,
    certHash: input.certHash,
    transport: input.transport,
    webSocketUrl: input.webSocketUrl,
    signal: AbortSignal.timeout(ConnectTimeoutMs),
  });
  if (channel.transport !== input.transport) {
    channel.close();
    throw new Error(`connected over ${channel.transport}, want ${input.transport}`);
  }
  const echo = new EchoClient(channel);
  const pad = padValue(input);
  const options = { signal, ...(pad && { metadata: { [PadKey]: pad } }) };
  const operations = {
    unary: (message) =>
      once(async () => {
        expectLength(await echo.unary({ message }, options), message);
        return 1;
      }),
    "server-stream": (message, count) =>
      once(async () => {
        const stream = await echo.serverStream({ message, count }, options);
        let received = 0;
        for await (const response of stream) {
          expectLength(response, message);
          received++;
        }
        return expectCount(received, count);
      }),
    "client-stream": (message, count) =>
      once(async () => {
        const call = await echo.clientStream(options);
        for (let i = 0; i < count; i++) {
          await call.send({ message });
        }
        const response = await call.closeAndRecv();
        expectCount(response.index, count);
        return count;
      }),
    bidi: (message) => {
      // One stream per worker, open across operations and opened again after a failure.
      let call;
      const close = () => {
        call?.close().catch(() => {});
        call = undefined;
      };
      return {
        async run() {
          try {
            call ??= await echo.bidi(options);
            await call.send({ message });
            const response = await call.recv();
            if (response === undefined) {
              throw new Error("the stream ended");
            }
            expectLength(response, message);
            return 1;
          } catch (error) {
            close();
            throw error;
          }
        },
        close,
      };
    },
  };
  return {
    operation: (scenario, message, count) => operations[scenario](message, count),
    close: () => channel.close(),
  };
}

/** A connect-web or gRPC-Web client over the lane's HTTP version, checked with a first call. */
async function fetchTarget(input, lane, signal) {
  const createTransport =
    lane.protocol === "connect" ? createConnectTransport : createGrpcWebTransport;
  const client = createClient(Echo, createTransport({ baseUrl: input.url, useBinaryFormat: true }));
  const pad = padValue(input);
  const options = { signal, ...(pad && { headers: { [PadKey]: pad } }) };

  await client.unary({ message: "probe" }, { signal: AbortSignal.timeout(ConnectTimeoutMs) });
  const probe = performance
    .getEntriesByType("resource")
    .findLast((entry) => entry.name.startsWith(input.url));
  if (probe?.nextHopProtocol !== lane.http) {
    throw new Error(
      `called over ${probe?.nextHopProtocol || "an unknown protocol"}, want ${lane.http}`,
    );
  }
  // Every call would add an entry to the resource timeline.
  performance.clearResourceTimings();
  performance.setResourceTimingBufferSize(0);

  const operations = {
    unary: (message) =>
      once(async () => {
        expectLength(await client.unary({ message }, options), message);
        return 1;
      }),
    "server-stream": (message, count) =>
      once(async () => {
        let received = 0;
        for await (const response of client.serverStream({ message, count }, options)) {
          expectLength(response, message);
          received++;
        }
        return expectCount(received, count);
      }),
  };
  return {
    operation: (scenario, message, count) => operations[scenario](message, count),
    close: () => {},
  };
}

/** An operation that holds nothing across runs. */
function once(run) {
  return { run, close: () => {} };
}

function expectLength(response, message) {
  if (response.message.length !== message.length) {
    throw new Error(`response has ${response.message.length} bytes, want ${message.length}`);
  }
}

function expectCount(received, count) {
  if (received !== count) {
    throw new Error(`received ${received} responses, want ${count}`);
  }
  return received;
}
