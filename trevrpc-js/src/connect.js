import { Channel } from "./browser-channel.js";

/** Opens a TrevRPC client for the current JavaScript runtime. */
export async function connect(url, options = {}) {
  return await Channel.connect(url, options);
}
