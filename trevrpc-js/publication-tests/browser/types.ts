import { connect } from "@trevrpc/trevrpc-js";
import type { BrowserChannelOptions } from "@trevrpc/trevrpc-js";

import { AllShapesClient } from "./generated/all-shapes.trevrpc.js";

const options: BrowserChannelOptions = { congestionControl: "low-latency" };
const channel = await connect("https://example.test/trevrpc", options);
const client = new AllShapesClient(channel);
void client.unary({ value: "browser" });

// @ts-expect-error Node native options are excluded by browser conditions.
await connect("https://example.test/trevrpc", { skipCertificateValidation: true });
