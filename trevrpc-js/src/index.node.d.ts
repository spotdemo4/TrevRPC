export * from "./index.js";
export { Channel } from "./node.js";

import type { Channel, NodeChannelOptions } from "./node.js";

/** Opens a native Node channel and waits for its first ready generation. */
export function connect(
  urlOrOptions: string | URL | NodeChannelOptions,
  options?: NodeChannelOptions,
): Promise<Channel>;
