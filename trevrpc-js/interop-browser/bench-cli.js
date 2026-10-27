// trevrpc-interop-browser bench: the load benchmark's browser client (bench/README.md). It runs one
// scenario in a browser over one lane and prints the result as one JSON line, as the native
// clients' `bench` subcommands do.
//
//   trevrpc-interop-browser bench --browser chromium|firefox --transport <lane> --url <url>
//       [--ws-url <url>] [--cert-hash <hex>] [--cert <pem>] [--scenario S] [--size N]
//       [--metadata-size N] [--concurrency N] [--duration S] [--warmup S] [--messages N]
//       [--payload repeat|records] [--compression MODE] [--dictionary <file>]
//
// The lanes are webtransport and websocket, the TrevRPC transports (--url is the WebTransport URL
// and --cert-hash trusts its certificate; --ws-url is the WebSocket URL), and connect-web,
// connect-web-h3 and grpc-web, which call a Connect server at --url through fetch over HTTP/2 or
// HTTP/3. connect-web-h3 needs --cert to trust the certificate for HTTP/3.
//
// Only the TrevRPC lanes compress: --compression is none, dictionary, stream or
// stream-dictionary, and the dictionary modes compress Echo messages with the zstd dictionary
// in --dictionary, as the native clients do.
//
// The page is cross-origin isolated, so its timers resolve to microseconds; servers reached
// through fetch must answer CORS requests.
import { parseArgs } from "node:util";

import { BenchLanes, Compressions, Payloads } from "./bench-lanes.js";
import { Browsers, describe, pageHtml, readDictionary, runPage } from "./driver.js";

const Usage =
  "usage: trevrpc-interop-browser bench --browser chromium|firefox --transport <lane> --url <url>" +
  " [--ws-url <url>] [--cert-hash <hex>] [--cert <pem>] [--scenario S] [--size N]" +
  " [--metadata-size N] [--concurrency N] [--duration S] [--warmup S] [--messages N]" +
  " [--payload repeat|records] [--compression MODE] [--dictionary <file>]";
const BenchBundle = new URL("../build/interop/bench.js", import.meta.url);
/**
 * How long the page may take to open, and the room on top of the warmup and the window for
 * connecting and for the last operations. Under an emulated lossy link the page loads over it too,
 * and one connection that loses its first packets backs off for seconds. Together they stay
 * within the minute run.py allows on top of the warmup and the window.
 */
const SetupTimeoutMs = 40_000;
const SlackMs = 15_000;

/** Runs the bench subcommand with args, printing its result line; resolves with the exit code. */
export async function runBenchCommand(args) {
  const { browser, extra, ...input } = await parseBenchArguments(args);
  try {
    const { result } = await runPage({
      browser,
      extra,
      html: pageHtml("bench.js"),
      bundles: { "/bench.js": BenchBundle },
      input,
      isolated: true,
      setupTimeoutMs: SetupTimeoutMs,
      runTimeoutMs: (input.warmup + input.duration) * 1000 + SlackMs,
      timeoutMessage: "the benchmark did not finish in time",
    });
    console.log(JSON.stringify(result));
    return 0;
  } catch (error) {
    console.error(`trevrpc-interop-browser bench: ${describe(error)}`);
    return 1;
  }
}

/** The run args describe, or exits with the usage when they do not describe one. */
export async function parseBenchArguments(args) {
  let values;
  try {
    ({ values } = parseArgs({
      args,
      options: {
        browser: { type: "string" },
        transport: { type: "string" },
        url: { type: "string" },
        "ws-url": { type: "string" },
        "cert-hash": { type: "string" },
        cert: { type: "string" },
        scenario: { type: "string", default: "unary" },
        size: { type: "string", default: "64" },
        "metadata-size": { type: "string", default: "0" },
        concurrency: { type: "string", default: "1" },
        duration: { type: "string", default: "5" },
        warmup: { type: "string", default: "1" },
        messages: { type: "string", default: "100" },
        payload: { type: "string", default: "repeat" },
        compression: { type: "string", default: "none" },
        dictionary: { type: "string" },
      },
      strict: true,
    }));
  } catch (error) {
    usageError(error.message);
  }

  const browser = Browsers[values.browser];
  if (browser === undefined || browser.name === "safari") {
    usageError("--browser must be chromium or firefox");
  }
  const lane = BenchLanes[values.transport];
  if (lane === undefined) {
    usageError(`--transport must be one of ${Object.keys(BenchLanes).join(", ")}`);
  }
  if (!values.url || !URL.canParse(values.url)) {
    usageError("--url must be an absolute URL");
  }
  if (!lane.scenarios.includes(values.scenario)) {
    usageError(`the ${values.transport} lane runs ${lane.scenarios.join(", ")}`);
  }
  if (!Payloads.includes(values.payload)) {
    usageError(`--payload must be one of ${Payloads.join(", ")}`);
  }
  const mode = Compressions[values.compression];
  if (mode === undefined) {
    usageError(`--compression must be one of ${Object.keys(Compressions).join(", ")}`);
  }
  if (values.compression !== "none" && lane.transport === undefined) {
    usageError(`the ${values.transport} lane does not compress`);
  }
  if (!mode.scenarios.includes(values.scenario)) {
    usageError(`--compression ${values.compression} runs ${mode.scenarios.join(", ")}`);
  }
  if (Boolean(mode.dictionary) !== (values.dictionary !== undefined)) {
    usageError("--dictionary goes with the dictionary and stream-dictionary compression modes");
  }
  const certHash = values["cert-hash"];
  if (lane.transport === "webtransport" && !/^(?:[0-9a-fA-F]{2}){32}$/.test(certHash ?? "")) {
    usageError("--cert-hash must be the hex SHA-256 of the certificate DER (64 hex digits)");
  }
  let extra = {};
  if (lane.http === "h3") {
    if (browser.forceQuic === undefined) {
      usageError(`${browser.name} cannot be made to call over HTTP/3`);
    }
    if (!values.cert) {
      usageError(`the ${values.transport} lane needs --cert`);
    }
    extra = await browser.forceQuic(new URL(values.url).host, values.cert);
  }

  return {
    browser,
    extra,
    transport: values.transport,
    url: values.url,
    webSocketUrl: values["ws-url"],
    certHash: certHash && [...Buffer.from(certHash, "hex")],
    scenario: values.scenario,
    size: integer(values, "size", 0),
    metadataSize: integer(values, "metadata-size", 0),
    concurrency: integer(values, "concurrency", 1),
    messages: integer(values, "messages", 1),
    duration: seconds(values, "duration", false),
    warmup: seconds(values, "warmup", true),
    payload: values.payload,
    compression: values.compression,
    dictionary: await readDictionary(values.dictionary).catch((error) =>
      usageError(`--dictionary: ${error.message}`),
    ),
  };
}

function integer(values, name, least) {
  const value = Number(values[name]);
  if (!Number.isSafeInteger(value) || value < least) {
    usageError(`--${name} must be an integer of at least ${least}`);
  }
  return value;
}

function seconds(values, name, zero) {
  const value = Number(values[name]);
  if (!Number.isFinite(value) || value < 0 || (!zero && value === 0)) {
    usageError(`--${name} must be a ${zero ? "non-negative" : "positive"} number of seconds`);
  }
  return value;
}

function usageError(message) {
  console.error(`trevrpc-interop-browser bench: ${message}\n${Usage}`);
  process.exit(2);
}
