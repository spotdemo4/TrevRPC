import type { MessageInitShape } from "@bufbuild/protobuf";
import { TrevRpcError, type CallOptions, type Transport } from "@trevrpc/trevrpc-js";
import { Context, Effect, Stream } from "effect";

import { Echo, EchoRequestSchema, type EchoResponse } from "../../build/test/gen/echo_pb.js";
import { createEffectClient, toTrevRpcError, type EffectClient } from "../../src/index.js";

/** Accepts a value only if its type is exactly `Expected`. */
function exact<Expected>() {
  return <Actual>(
    _value: Actual,
    ..._mismatch: (<T>() => T extends Actual ? 1 : 2) extends <T>() => T extends Expected ? 1 : 2
      ? []
      : [never]
  ) => {};
}

declare const transport: Transport;
type EchoRequestInit = MessageInitShape<typeof EchoRequestSchema>;

const echo = createEffectClient(Echo, transport, { timeoutMs: 1_000 });
echo satisfies EffectClient<typeof Echo>;

const options: CallOptions = { timeoutMs: 1_000 };

// Unary calls are effects that fail with a TrevRpcError.
exact<Effect.Effect<EchoResponse, TrevRpcError>>()(echo.unary({ message: "hi" }));
echo.unary({}, options);
// @ts-expect-error The request is a message, not call options.
echo.unary(options, options);

// Server-streaming calls are streams.
exact<Stream.Stream<EchoResponse, TrevRpcError>>()(echo.serverStream({ message: "hi" }));

class UploadError {
  readonly _tag = "UploadError";
}
class Token extends Context.Service<Token, { readonly value: string }>()("Token") {}

const requests = Stream.make<EchoRequestInit[]>({ message: "a" }, { message: "b" });
const failing: Stream.Stream<EchoRequestInit, UploadError> = Stream.fail(new UploadError());
const needsToken = Stream.mapEffect(requests, (request) =>
  Effect.as(Effect.service(Token), request),
);

// Client-streaming calls take a request stream, whose error and requirements they keep.
exact<Effect.Effect<EchoResponse, TrevRpcError>>()(echo.clientStream(requests));
exact<Effect.Effect<EchoResponse, TrevRpcError | UploadError>>()(
  echo.clientStream(failing, options),
);
exact<Effect.Effect<EchoResponse, TrevRpcError, Token>>()(echo.clientStream(needsToken));
// @ts-expect-error A client-streaming call takes its requests, not call options (#320).
echo.clientStream(options);
// @ts-expect-error The request stream carries request messages.
echo.clientStream(Stream.make(1));

// Bidirectional calls take a request stream and return a response stream.
exact<Stream.Stream<EchoResponse, TrevRpcError>>()(echo.bidi(requests));
exact<Stream.Stream<EchoResponse, TrevRpcError | UploadError>>()(echo.bidi(failing));
exact<Stream.Stream<EchoResponse, TrevRpcError, Token>>()(echo.bidi(needsToken));
// @ts-expect-error A bidirectional call takes its requests, not call options (#320).
echo.bidi(options);

// Clients have the service's methods only.
exact<"unary" | "serverStream" | "clientStream" | "bidi">()("unary" as keyof typeof echo);
// @ts-expect-error createEffectClient takes a service descriptor.
createEffectClient(EchoRequestSchema, transport);

// Errors can be matched by tag.
echo
  .unary({})
  .pipe(
    Effect.catchTag("TrevRpcError", (error) => Effect.succeed(error.code)),
  ) satisfies Effect.Effect<EchoResponse | number>;
echo
  .clientStream(failing)
  .pipe(Effect.catchTag("UploadError", () => Effect.succeed(undefined))) satisfies Effect.Effect<
  EchoResponse | undefined,
  TrevRpcError
>;

toTrevRpcError(new Error("boom")) satisfies TrevRpcError;
