// Promise and AbortSignal helpers shared across the runtime.

/** A new promise with the functions that settle it: `{ promise, resolve, reject }`. */
export function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

/**
 * Settles like `promise`, or rejects with the abort reason as soon as `signal` aborts, at once if
 * it already has. `promise` keeps running; whatever it settles with after the abort is ignored.
 */
export function abortable(promise, signal) {
  if (signal.aborted) {
    return Promise.reject(signal.reason);
  }
  return new Promise((resolve, reject) => {
    const onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    promise.then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      },
    );
  });
}

/** Resolves once `promise` settled or `signal` aborted, whichever comes first. */
export function settledOrAborted(promise, signal) {
  return abortable(promise, signal).then(ignore, ignore);
}

/** Resolves after `delayMs`, or as soon as `signal` aborts, which stops the timer. */
export function sleep(delayMs, signal) {
  let timer;
  const elapsed = new Promise((resolve) => {
    timer = setTimeout(resolve, delayMs);
  });
  return settledOrAborted(elapsed, signal).then(() => clearTimeout(timer));
}

function ignore() {}
