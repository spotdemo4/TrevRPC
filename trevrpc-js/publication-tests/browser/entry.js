import { connect } from "trevrpc-js";

import { AllShapesClient } from "./generated/all-shapes.trevrpc.js";

globalThis.trevrpcBrowserSmoke = (async () => {
  const channel = await connect("https://example.test/trevrpc", { timeoutMs: 5_000 });
  try {
    const client = new AllShapesClient(channel, { timeoutMs: 5_000 });
    const reply = await client.unary({ value: "browser" });
    if (reply.message !== "browser") {
      throw new Error(`unexpected browser reply ${JSON.stringify(reply.message)}`);
    }
    return reply.message;
  } finally {
    channel.close({ closeCode: 0, reason: "smoke complete" });
  }
})();
