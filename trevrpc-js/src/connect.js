import { WebTransportClient } from "./webtransport.js";

/** Opens a TrevRPC client for the current JavaScript runtime. */
export async function connect(url, options = {}) {
  return await WebTransportClient.connect(url, options);
}
