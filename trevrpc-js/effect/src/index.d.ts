import type {
  BidirectionalStreamingCall,
  CallOptions,
  ClientStreamingCall,
  ResponseStream,
  RpcServiceDescriptor,
  TrevRpcError,
} from "@trevrpc/trevrpc-js";
import type { Effect, Stream } from "effect";

/**
 * The Effect form of a generated client method:
 *
 * - unary: `(request, options?) => Effect<Response, TrevRpcError>`
 * - server streaming: `(request, options?) => Stream<Response, TrevRpcError>`
 * - client streaming: `(requests, options?) => Effect<Response, TrevRpcError | E, R>`
 * - bidirectional streaming: `(requests, options?) => Stream<Response, TrevRpcError | E, R>`
 *
 * where `requests` is a `Stream<Request, E, R>`. `never` for anything else.
 */
export type EffectMethod<Method> = Method extends (
  options?: CallOptions,
) => Promise<BidirectionalStreamingCall<infer Request, infer Response>>
  ? <E = never, R = never>(
      requests: Stream.Stream<Request, E, R>,
      options?: CallOptions,
    ) => Stream.Stream<Response, TrevRpcError | E, R>
  : Method extends (
        options?: CallOptions,
      ) => Promise<ClientStreamingCall<infer Request, infer Response>>
    ? <E = never, R = never>(
        requests: Stream.Stream<Request, E, R>,
        options?: CallOptions,
      ) => Effect.Effect<Response, TrevRpcError | E, R>
    : Method extends (
          request: infer Request,
          options?: CallOptions,
        ) => Promise<ResponseStream<infer Response>>
      ? (request: Request, options?: CallOptions) => Stream.Stream<Response, TrevRpcError>
      : Method extends (request: infer Request, options?: CallOptions) => Promise<infer Response>
        ? (request: Request, options?: CallOptions) => Effect.Effect<Response, TrevRpcError>
        : never;

/** A generated client with its RPC methods in Effect form; other members are left out. */
export type EffectClient<Client> = {
  [Name in keyof Client as EffectMethod<Client[Name]> extends never ? never : Name]: EffectMethod<
    Client[Name]
  >;
};

/**
 * Wraps a generated client's methods in Effect. `service` is the client's generated descriptor,
 * such as `GreeterService` for a `GreeterClient`.
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
 * @throws TypeError if `client` lacks a method of `service`.
 */
export function createEffectClient<Client extends object>(
  service: RpcServiceDescriptor,
  client: Client,
): EffectClient<Client>;

/** Returns a `TrevRpcError` as is, and wraps anything else in one with `Code.Unknown`. */
export function toTrevRpcError(error: unknown): TrevRpcError;
