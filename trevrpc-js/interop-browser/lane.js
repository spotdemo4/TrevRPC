// Connects a channel over one of the browser lanes: the transports the interop scenarios and the
// load benchmark run over.
import { connect } from "../src/index.js";

/** The channel transports each lane allows. */
export const LaneTransports = {
  webtransport: ["webtransport"],
  websocket: ["websocket"],
  auto: ["webtransport", "websocket"],
};

/**
 * Connects to url over transport: "webtransport", "websocket" (QMux over WebSocket at
 * webSocketUrl), or "auto" (WebTransport with the WebSocket fallback). certHash, the SHA-256 of
 * the server certificate as bytes, is trusted for WebTransport. Other options go to connect.
 */
export function connectLane({
  url,
  certHash,
  transport = "webtransport",
  webSocketUrl,
  ...options
}) {
  return connect(url, {
    transports: LaneTransports[transport],
    webSocketUrl,
    ...(certHash && {
      serverCertificateHashes: [{ algorithm: "sha-256", value: Uint8Array.from(certHash) }],
    }),
    ...options,
  });
}
