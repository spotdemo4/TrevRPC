import { normalizeMetadata } from "./metadata.js";
import { Code, TrevRpcError, invalidArgument } from "./status.js";

/**
 * Checks a list of call interceptors from the options of a channel or client, and returns it as a
 * frozen array; undefined or null is no interceptors. Throws InvalidArgument otherwise.
 */
export function interceptorList(interceptors, what) {
  if (interceptors == null) {
    return Object.freeze([]);
  }
  if (typeof interceptors[Symbol.iterator] !== "function" || typeof interceptors === "string") {
    throw invalidArgument(`${what} must be a list of functions`);
  }
  const list = [...interceptors];
  if (!list.every((interceptor) => typeof interceptor === "function")) {
    throw invalidArgument(`${what} must be a list of functions`);
  }
  return Object.freeze(list);
}

/**
 * Runs `interceptors` in order, then starts the call with `start(options)`, passing the options
 * with the metadata the interceptors left, and returns what `start` returns. The finish functions
 * the interceptors returned run once, innermost first, with `{ code, error }` when the call ends:
 * when an interceptor or `start` fails, when `start` settles if `ended` is undefined, and when
 * `ended(result)` settles otherwise, such as a streaming call's final status.
 */
export async function interceptCall(interceptors, info, options, start, ended) {
  const finishes = [];
  let result;
  try {
    const call = new InterceptedCall(info, options);
    for (const interceptor of interceptors) {
      const finish = await interceptor(call);
      if (finish != null) {
        if (typeof finish !== "function") {
          throw new TypeError("a call interceptor must return a function or nothing");
        }
        finishes.push(finish);
      }
    }
    result = await start({ ...options, metadata: normalizeMetadata(call.metadata) });
  } catch (error) {
    runFinishes(finishes, error);
    throw error;
  }
  if (ended === undefined) {
    runFinishes(finishes, undefined);
  } else if (finishes.length > 0) {
    ended(result).then(
      () => runFinishes(finishes, undefined),
      (error) => runFinishes(finishes, error),
    );
  }
  return result;
}

/** What an interceptor sees of a call before it starts, and may change. */
class InterceptedCall {
  constructor({ method, service, name, kind }, { metadata, timeoutMs, signal }) {
    this.method = method;
    this.service = service;
    this.name = name;
    this.kind = kind;
    /** The request metadata; interceptors may add, replace and delete entries. */
    this.metadata = { ...metadata };
    this.timeoutMs = timeoutMs;
    this.signal = signal;
    Object.seal(this);
  }
}

/**
 * Calls each finish function, innermost first, with how the call ended. A finish function that
 * throws does not stop the others; its error is reported as uncaught.
 */
function runFinishes(finishes, error) {
  if (finishes.length === 0) {
    return;
  }
  const end = Object.freeze(
    error === undefined
      ? { code: Code.Ok, error: undefined }
      : { code: error instanceof TrevRpcError ? error.code : Code.Unknown, error },
  );
  for (let index = finishes.length - 1; index >= 0; index -= 1) {
    try {
      finishes[index](end);
    } catch (failure) {
      reportUncaught(failure);
    }
  }
}

function reportUncaught(error) {
  if (typeof globalThis.reportError === "function") {
    globalThis.reportError(error);
  } else {
    console.error(error);
  }
}
