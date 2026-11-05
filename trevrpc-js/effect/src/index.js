import { Code, TrevRpcError, createClient } from "@trevrpc/trevrpc-js";
import { Effect, Stream } from "effect";

/** Returns a `TrevRpcError` as is, and wraps anything else in one with `Code.Unknown`. */
export function toTrevRpcError(error) {
  if (error instanceof TrevRpcError) {
    return error;
  }
  const message = error instanceof Error ? error.message : String(error);
  return new TrevRpcError(Code.Unknown, message, undefined, { cause: error });
}

/**
 * Creates a client for `service`, a service descriptor that protoc-gen-es generates, whose calls
 * are Effect values: unary calls return an `Effect`, server-streaming calls a `Stream`, and
 * client-streaming and bidirectional calls take their requests as a `Stream`. Interrupting the
 * effect or stream cancels the call. `transport` and `options` are those of `createClient`.
 */
export function createEffectClient(service, transport, options) {
  const client = createClient(service, transport, options);
  const result = {};
  for (const method of service.methods) {
    const call = client[method.localName];
    result[method.localName] = Lifts[method.methodKind](call);
  }
  return result;
}

/** How each protobuf-es method kind's calls become Effect values. */
const Lifts = Object.freeze({
  __proto__: null,
  unary: liftUnary,
  server_streaming: liftServerStreaming,
  client_streaming: liftClientStreaming,
  bidi_streaming: liftBidirectionalStreaming,
});

function liftUnary(call) {
  return (request, options) =>
    Effect.tryPromise({
      try: (signal) => call(request, withSignal(options, signal)),
      catch: toTrevRpcError,
    });
}

function liftServerStreaming(call) {
  return (request, options) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const signal = yield* abortOnClose;
        const responses = yield* open(() => call(request, withSignal(options, signal)));
        return Stream.fromAsyncIterable(responses, toTrevRpcError);
      }),
    );
}

function liftClientStreaming(call) {
  return (requests, options) =>
    Effect.scoped(
      Effect.gen(function* () {
        const signal = yield* abortOnClose;
        const stream = yield* open(() => call(withSignal(options, signal)));
        // The server may answer before the upload is done; its status ends the upload, and
        // closeAndRecv() reports the outcome.
        yield* Effect.raceFirst(upload(stream, requests), settled(stream.status));
        return yield* Effect.tryPromise({
          try: () => stream.closeAndRecv(),
          catch: toTrevRpcError,
        });
      }),
    );
}

function liftBidirectionalStreaming(call) {
  return (requests, options) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const signal = yield* abortOnClose;
        const stream = yield* open(() => call(withSignal(options, signal)));
        const sending = upload(stream, requests).pipe(
          // closeSend() resolves when the server already ended the call; any other failure is
          // the response's to report.
          Effect.andThen(Effect.promise(() => stream.closeSend().catch(ignore))),
        );
        // The responses decide when the stream ends: their end stops an unfinished upload,
        // while a failed request stream fails the whole stream.
        return Stream.merge(
          Stream.fromAsyncIterable(stream, toTrevRpcError),
          Stream.drain(Stream.fromEffect(sending)),
          { haltStrategy: "left" },
        );
      }),
    );
}

/**
 * A signal that aborts when the scope closes, cancelling a call that has not finished: on
 * interruption, on failure, and after the call ended, when aborting has no effect.
 */
const abortOnClose = Effect.map(
  Effect.acquireRelease(
    Effect.sync(() => new AbortController()),
    (controller) => Effect.sync(() => controller.abort()),
  ),
  (controller) => controller.signal,
);

/** Starts a call, whose promise settles once the call is open. */
function open(start) {
  return Effect.tryPromise({ try: start, catch: toTrevRpcError });
}

/** The call options with `signal` added to the caller's own, either of which cancels the call. */
function withSignal(options, signal) {
  const own = options?.signal;
  return { ...options, signal: own ? AbortSignal.any([own, signal]) : signal };
}

/**
 * Sends each request of `requests`, waiting for the transport to accept it. Stops without an error
 * at the first rejected send, which means the call has ended; the call reports its outcome. A
 * failing request stream fails with its error, and the caller's scope then cancels the call.
 */
function upload(stream, requests) {
  return Stream.runForEachWhile(requests, (request) =>
    Effect.promise(() => stream.send(request).then(accepted, rejected)),
  );
}

/** Succeeds once `promise` settles, whatever its outcome. */
function settled(promise) {
  return Effect.promise(() => promise.then(ignore, ignore));
}

const accepted = () => true;
const rejected = () => false;
const ignore = () => {};
