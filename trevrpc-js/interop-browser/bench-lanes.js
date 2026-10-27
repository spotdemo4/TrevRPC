// The browser lanes of the load benchmark (bench/README.md), and the scenarios each runs. The
// TrevRPC lanes run the Echo scenarios; connect-web and gRPC-Web reach their server through
// fetch, which cannot stream a request, so they run the unary and server-streaming ones.
const EchoScenarios = Object.freeze(["unary", "server-stream", "client-stream", "bidi"]);
const FetchScenarios = Object.freeze(["unary", "server-stream"]);

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
