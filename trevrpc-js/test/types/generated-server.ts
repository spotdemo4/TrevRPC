import { Code, createStreamingResponse, createUnaryResponse } from "@trevrpc/trevrpc-js";
import { NodeServer } from "@trevrpc/trevrpc-js/node";
import { registerTypedService } from "@trevrpc/trevrpc-js/node/generated";

import type { GreeterHandlers } from "../../examples/greeter/greeter.node.trevrpc.js";
import { registerGreeterServer } from "../../examples/greeter/greeter.node.trevrpc.js";

const handlers: GreeterHandlers = {
  sayHello(request, call) {
    call.signal satisfies AbortSignal;
    return createUnaryResponse({ message: `hello ${request.name ?? ""}` }, { trailer: "ok" });
  },
  lotsOfReplies() {
    return createStreamingResponse([{ message: "one" }], {
      code: Code.Ok,
      message: "",
      metadata: { trailer: "done" },
    });
  },
  async lotsOfGreetings(requests) {
    for await (const request of requests) {
      return { message: request.name };
    }
    return { message: "" };
  },
  bidiHello: async () => [{ message: "hello" }],
};

const server = {} as NodeServer;
registerGreeterServer(server, handlers);
void registerTypedService;
