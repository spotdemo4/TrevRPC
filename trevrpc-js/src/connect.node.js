import { NodeTransport } from "./node.js";

/** Opens a WebTransport TrevRPC client backed by the native Node runtime. */
export async function connectWebTransport(urlOrOptions, options = {}) {
  return await NodeTransport.connectWebTransport(urlOrOptions, options);
}
