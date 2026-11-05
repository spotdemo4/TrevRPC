import type { DescMessage, DescService, MessageInitShape, MessageShape } from "@bufbuild/protobuf";
import type { CallOptions, ClientOptions, Transport, TrevRpcError } from "@trevrpc/trevrpc-js";
import type { Effect, Stream } from "effect";

/**
 * The Effect form of a method of a protobuf-es service descriptor:
 *
 * - unary: `(request, options?) => Effect<Response, TrevRpcError>`
 * - server streaming: `(request, options?) => Stream<Response, TrevRpcError>`
 * - client streaming: `(requests, options?) => Effect<Response, TrevRpcError | E, R>`
 * - bidirectional streaming: `(requests, options?) => Stream<Response, TrevRpcError | E, R>`
 *
 * where a request is a message or the init object `create()` takes, `requests` is a
 * `Stream<Request, E, R>`, and a response is a message.
 */
export type EffectMethod<Method> = Method extends {
  methodKind: infer Kind;
  input: infer I extends DescMessage;
  output: infer O extends DescMessage;
}
  ? Kind extends "unary"
    ? (
        request: MessageInitShape<I>,
        options?: CallOptions,
      ) => Effect.Effect<MessageShape<O>, TrevRpcError>
    : Kind extends "server_streaming"
      ? (
          request: MessageInitShape<I>,
          options?: CallOptions,
        ) => Stream.Stream<MessageShape<O>, TrevRpcError>
      : Kind extends "client_streaming"
        ? <E = never, R = never>(
            requests: Stream.Stream<MessageInitShape<I>, E, R>,
            options?: CallOptions,
          ) => Effect.Effect<MessageShape<O>, TrevRpcError | E, R>
        : Kind extends "bidi_streaming"
          ? <E = never, R = never>(
              requests: Stream.Stream<MessageInitShape<I>, E, R>,
              options?: CallOptions,
            ) => Stream.Stream<MessageShape<O>, TrevRpcError | E, R>
          : never
  : never;

/** The Effect client of a service: one method per RPC, named by its `localName`. */
export type EffectClient<S extends DescService> = {
  [Name in keyof S["method"]]: EffectMethod<S["method"][Name]>;
};

/**
 * Creates a client for `service`, a service descriptor that protoc-gen-es generates (such as
 * `Greeter` from `greeter_pb.js`), whose calls are Effect values. `transport` and `options` are
 * those of `createClient` in `@trevrpc/trevrpc-js`.
 *
 * Interrupting an effect or stream cancels its call, which the server sees as `Code.Cancelled`.
 * A `signal` in the call options cancels it too. Client-streaming and bidirectional calls pull
 * their requests from the stream as the transport accepts them. When the server ends the call
 * first, the rest of the stream is not pulled, and the server's outcome is the result. A request
 * stream that fails cancels the call, and the effect or stream fails with the request stream's
 * error.
 *
 * Calls fail with a `TrevRpcError`; anything else a call rejects with becomes one with
 * `Code.Unknown`, and with the original as `cause`.
 *
 * @throws TrevRpcError with `Code.InvalidArgument` where `createClient` throws it.
 */
export function createEffectClient<S extends DescService>(
  service: S,
  transport: Transport,
  options?: ClientOptions,
): EffectClient<S>;

/** Returns a `TrevRpcError` as is, and wraps anything else in one with `Code.Unknown`. */
export function toTrevRpcError(error: unknown): TrevRpcError;
