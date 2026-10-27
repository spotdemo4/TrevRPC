// The browser lanes of the load benchmark (bench/README.md), and the scenarios each runs. The
// TrevRPC lanes run the Echo scenarios; connect-web and gRPC-Web reach their server through
// fetch, which cannot stream a request, so they run the unary and server-streaming ones.
const EchoScenarios = Object.freeze(["unary", "server-stream", "client-stream", "bidi"]);
const FetchScenarios = Object.freeze(["unary", "server-stream"]);
const CompressedScenarios = Object.freeze(["unary", "client-stream", "bidi"]);

/**
 * Each lane: its scenarios, and either the TrevRPC transport it connects over or the fetch
 * protocol (Connect or gRPC-Web) and HTTP version it calls with.
 */
export const BenchLanes = Object.freeze({
  webtransport: { scenarios: EchoScenarios, transport: "webtransport" },
  websocket: { scenarios: EchoScenarios, transport: "websocket" },
  "connect-web": { scenarios: FetchScenarios, protocol: "connect", http: "h2" },
  "connect-web-h3": { scenarios: FetchScenarios, protocol: "connect", http: "h3" },
  "grpc-web": { scenarios: FetchScenarios, protocol: "grpc-web", http: "h2" },
});

/** The payloads: the same letters in every message, or new log records (bench/payload.py). */
export const Payloads = Object.freeze(["repeat", "records"]);

/**
 * The compression modes of bench/README.md, which only the TrevRPC lanes have: whether a mode
 * uses the trained dictionary, and whether it calls CompressedEcho to stream-compress. Modes other
 * than none run the scenarios in which every message differs.
 */
export const Compressions = Object.freeze({
  none: { scenarios: EchoScenarios },
  dictionary: { scenarios: CompressedScenarios, dictionary: true },
  stream: { scenarios: CompressedScenarios, stream: true },
  "stream-dictionary": { scenarios: CompressedScenarios, dictionary: true, stream: true },
});
