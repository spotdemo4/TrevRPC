// The load benchmark's browser client: runs one scenario of bench/README.md over one lane for a
// warmup and a measured window, and summarizes it as the native clients do. It only needs the web
// platform, so the measuring also runs under Node tests.
import { createClient as createConnectClient } from "@connectrpc/connect";
import { createConnectTransport, createGrpcWebTransport } from "@connectrpc/connect-web";

import { CompressedEcho } from "../build/interop/gen/compressed_pb.js";
import { CompressedEchoCodecs } from "../build/interop/gen/compressed_trevrpc.js";
import { Echo } from "../build/interop/gen/interop_pb.js";
import { EchoCodecs } from "../build/interop/gen/interop_trevrpc.js";
import { createClient } from "../src/index.js";
import { BenchLanes, Compressions } from "./bench-lanes.js";
import { interopCompression } from "./compression.js";
import { createJsonClient } from "./json-client.js";
import { connectLane } from "./lane.js";
import { newPayload } from "./records.js";

const ConnectTimeoutMs = 10_000;
/**
 * What the error of a run that could not connect starts with, which bench/run.py runs again, as it
 * does for the native clients: over a lossy link, one burst can hold a TCP handshake for seconds.
 */
const Unconnected = "could not connect";
/** How long operations still running at the end of the window get to finish, as natively. */
const GraceMs = 10_000;
/** How long a worker waits after a failed operation before the next. */
const ErrorBackoffMs = 1;
const PadKey = "bench-pad";

/**
 * Runs the benchmark that input describes: { transport (the lane), url, webSocketUrl, certHash,
 * scenario, size, metadataSize, concurrency, duration, warmup (seconds), messages, payload,
 * compression, dictionary (the trained dictionary in base64, for the modes that use it) }.
 * Resolves with the result line's fields. onError sees the first operation that fails.
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
    const operations = Array.from({ length: input.concurrency }, (_, worker) =>
      target.operation(
        input.scenario,
        newPayload(input.payload, input.size, worker),
        input.messages,
      ),
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
    payload: input.payload,
    compression: input.compression,
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

/**
 * A TrevRPC channel over the lane's transport, and the scenarios' operations on it. A compression
 * mode gives the channel compression settings, and the stream modes call CompressedEcho, as the
 * native clients do.
 */
async function trevrpcTarget(input, signal) {
  const mode = Compressions[input.compression];
  const compression =
    (mode.dictionary || mode.stream) &&
    (await interopCompression(mode.dictionary ? input.dictionary : undefined));
  const channel = await connecting(() =>
    connectLane({
      url: input.url,
      certHash: input.certHash,
      transport: input.transport,
      webSocketUrl: input.webSocketUrl,
      ...(compression && { compression }),
      signal: AbortSignal.timeout(ConnectTimeoutMs),
    }),
  );
  if (channel.transport !== input.transport) {
    channel.close();
    throw new Error(`connected over ${channel.transport}, want ${input.transport}`);
  }
  // With the generated codecs, as an app that cares about speed would call.
  const echo = mode.stream
    ? createClient(CompressedEcho, channel, { codecs: CompressedEchoCodecs })
    : createClient(Echo, channel, { codecs: EchoCodecs });
  const pad = padValue(input);
  const options = { signal, ...(pad && { metadata: { [PadKey]: pad } }) };
  const operations = {
    unary: (payload) =>
      once(async () => {
        const message = payload.next();
        expectLength(await echo.unary({ message }, options), message);
        return 1;
      }),
    "server-stream": (payload, count) =>
      once(async () => {
        const message = payload.next();
        const stream = await echo.serverStream({ message, count }, options);
        let received = 0;
        for await (const response of stream) {
          expectLength(response, message);
          received++;
        }
        return expectCount(received, count);
      }),
    "client-stream": (payload, count) =>
      once(async () => {
        const call = await echo.clientStream(options);
        for (let i = 0; i < count; i++) {
          await call.send({ message: payload.next() });
        }
        const response = await call.closeAndRecv();
        expectCount(response.index, count);
        return count;
      }),
    bidi: (payload) => {
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
            const message = payload.next();
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
    operation: (scenario, payload, count) => operations[scenario](payload, count),
    close: () => channel.close(),
  };
}

/**
 * The resource timing entry of the last request to a URL under url, or undefined after timeoutMs.
 * Chromium can add the entry of a fetch after the fetch has resolved, so it waits for one.
 */
function resourceTiming(url, timeoutMs) {
  const matches = (entry) => entry.name.startsWith(url);
  const queued = performance.getEntriesByType("resource").findLast(matches);
  if (queued) {
    return Promise.resolve(queued);
  }
  return new Promise((resolve) => {
    const finish = (entry) => {
      clearTimeout(timer);
      observer.disconnect();
      resolve(entry);
    };
    const observer = new PerformanceObserver((list) => {
      const entry = list.getEntries().findLast(matches);
      if (entry) {
        finish(entry);
      }
    });
    const timer = setTimeout(() => finish(undefined), timeoutMs);
    observer.observe({ type: "resource", buffered: true });
  });
}

/** The lane's fetch client of Echo: connect-web, gRPC-Web, or plain JSON. */
function fetchClient(input, lane) {
  if (lane.protocol === "json") {
    return createJsonClient(input.url);
  }
  const createTransport =
    lane.protocol === "connect" ? createConnectTransport : createGrpcWebTransport;
  return createConnectClient(Echo, createTransport({ baseUrl: input.url, useBinaryFormat: true }));
}

/**
 * A connect-web, gRPC-Web or JSON client over the lane's HTTP version, checked with a first call.
 */
async function fetchTarget(input, lane, signal) {
  const client = fetchClient(input, lane);
  const pad = padValue(input);
  const options = { signal, ...(pad && { headers: { [PadKey]: pad } }) };

  await connecting(() =>
    client.unary({ message: "probe" }, { signal: AbortSignal.timeout(ConnectTimeoutMs) }),
  );
  const probe = await resourceTiming(input.url, ConnectTimeoutMs);
  if (!probe) {
    throw new Error(`the probe call left no resource timing entry within ${ConnectTimeoutMs} ms`);
  }
  if (probe.nextHopProtocol !== lane.http) {
    throw new Error(
      `called over ${probe.nextHopProtocol || "an unknown protocol"}, want ${lane.http}`,
    );
  }
  // Every call would add an entry to the resource timeline.
  performance.clearResourceTimings();
  performance.setResourceTimingBufferSize(0);

  const operations = {
    unary: (payload) =>
      once(async () => {
        const message = payload.next();
        expectLength(await client.unary({ message }, options), message);
        return 1;
      }),
    "server-stream": (payload, count) =>
      once(async () => {
        const message = payload.next();
        let received = 0;
        for await (const response of client.serverStream({ message, count }, options)) {
          expectLength(response, message);
          received++;
        }
        return expectCount(received, count);
      }),
  };
  return {
    operation: (scenario, payload, count) => operations[scenario](payload, count),
    close: () => {},
  };
}

/** What connect resolves with, or an error that says it could not connect. */
async function connecting(connect) {
  try {
    return await connect();
  } catch (error) {
    throw new Error(`${Unconnected}: ${error?.message ?? String(error)}`, { cause: error });
  }
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
