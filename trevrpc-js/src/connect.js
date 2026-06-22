import { WebTransportClient } from "./webtransport.js";

/** Opens a WebTransport TrevRPC client for the current JavaScript runtime. */
export async function connectWebTransport(url, options = {}) {
  return await WebTransportClient.connect(url, options);
}
