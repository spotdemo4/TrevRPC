import { Channel } from "./node.js";

/** Opens a TrevRPC client backed by the native Node runtime. */
export async function connect(urlOrOptions, options = {}) {
  return await Channel.connect(urlOrOptions, options);
}
