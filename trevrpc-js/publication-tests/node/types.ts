import { createUnaryResponse } from "trevrpc-js";
import { Channel, NodeServer } from "trevrpc-js/node";
import type { NodeChannelTarget, NodeServerCall } from "trevrpc-js/node";

import { registerAllShapesServer } from "./generated/all-shapes.node.trevrpc.js";
import { AllShapesClient } from "./generated/all-shapes.trevrpc.js";

const target: NodeChannelTarget = {
  host: "localhost",
  port: 50051,
  skipCertificateValidation: true,
};
const channel = new Channel(target);
const client = new AllShapesClient(channel);
void client.unary({ value: "typed" });

function inspect(call: NodeServerCall) {
  call.signal satisfies AbortSignal;
  call.deadline satisfies Date | null;
}
void inspect;

registerAllShapesServer({} as NodeServer, {
  unary: (request) => createUnaryResponse({ message: request.value }, { trailer: "typed" }),
});
