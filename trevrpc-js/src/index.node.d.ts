export * from "./index.js";
export { Channel } from "./node.js";

import type { Channel, NodeChannelOptions, NodeChannelTarget } from "./node.js";

/** Opens a native Node channel and waits for its first ready generation. */
export function connect(
  target: string | URL | NodeChannelTarget,
  options?: NodeChannelOptions,
): Promise<Channel>;
