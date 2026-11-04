# @trevrpc/effect

[Effect](https://effect.website) adapter for the TrevRPC browser client, [`@trevrpc/trevrpc-js`](https://www.npmjs.com/package/@trevrpc/trevrpc-js): it wraps a generated client so that its calls are effects and streams.

```sh
npm install @trevrpc/effect @trevrpc/trevrpc-js effect
```

```ts
import { createChannel } from "@trevrpc/trevrpc-js";
import { createEffectClient } from "@trevrpc/effect";
import { Stream } from "effect";

import { GreeterClient, GreeterService } from "./gen/greeter/v1/greeter.trevrpc.js";

const channel = createChannel("https://example.com/trevrpc");
const greeter = createEffectClient(GreeterService, new GreeterClient(channel));

// Effect<HelloReply, TrevRpcError>
const reply = greeter.sayHello({ name: "Ada" });
// Stream<Update, TrevRpcError>
const updates = greeter.watch({ name: "Ada" });
// Effect<UploadReply, TrevRpcError>
const upload = greeter.upload(Stream.make({ chunk: "a" }, { chunk: "b" }));
// Stream<ChatReply, TrevRpcError>
const replies = greeter.chat(
  Stream.fromIterable(["hi", "bye"]).pipe(Stream.map((text) => ({ text }))),
);
```

`createEffectClient(service, client)` takes the generated descriptor and client, and returns a client with the same method names:

| RPC kind                | Effect method                                                                           |
| ----------------------- | --------------------------------------------------------------------------------------- |
| unary                   | `(request, options?) => Effect<Response, TrevRpcError>`                                 |
| server streaming        | `(request, options?) => Stream<Response, TrevRpcError>`                                 |
| client streaming        | `(requests: Stream<Request, E, R>, options?) => Effect<Response, TrevRpcError \| E, R>` |
| bidirectional streaming | `(requests: Stream<Request, E, R>, options?) => Stream<Response, TrevRpcError \| E, R>` |

`options` are the usual [call options](https://trev.zip/llc/TrevRPC/wiki/JavaScript-Guide#call-options).

- **Cancellation.** Interrupting an effect or stream cancels its call, which the server sees as `CANCELLED`; so does taking part of a response stream (`Stream.take`) or a failure elsewhere in the program. A `signal` in the call options also cancels the call.
- **Request streams.** Client-streaming and bidirectional calls pull their requests from the stream as the transport accepts them, so a request stream is only consumed as fast as the server reads it. When the server ends the call first, the rest of the stream is not pulled, and the server's outcome is the result: its response or response stream if it succeeded, or its `TrevRpcError`. A request stream that fails cancels the call, and the call fails with the request stream's error.
- **Errors.** Calls fail with the `TrevRpcError` of the [client](https://trev.zip/llc/TrevRPC/wiki/JavaScript-Guide#errors-cancellation-and-streams), whose `_tag` is `"TrevRpcError"`, so `Effect.catchTag("TrevRpcError", ...)` handles it. Anything else a call rejects with becomes a `TrevRpcError` with `Code.Unknown` and the original as `cause`, as `toTrevRpcError(error)` does.
- **Rendering.** Response streams deliver buffered messages without returning to the event loop, as the client does; see [Rendering long streams](https://trev.zip/llc/TrevRPC/wiki/JavaScript-Guide#rendering-long-streams) before rendering a long stream message by message.

## Development

The package is developed against the trevrpc-js of this checkout, which it links as `file:..`, so install that first:

```sh
npm ci --prefix ..
npm ci
npm run check      # format check, lint, typecheck, tests
```

The tests run the real client against the in-memory server of trevrpc-js's own tests. `nix build .#trevrpc-js-effect` runs them against the built trevrpc-js, and checks the published files.
