import { cancelled, statusFromTransportError, unavailable } from "./status.js";

/**
 * Dials `primary`, adds `fallback` once the primary fails or `delayMs` passes, and resolves with
 * `{ session, transport }` from the first to connect. The loser is abandoned and closed if it
 * connects anyway. Each candidate is `{ transport, dial(signal) }`.
 */
export function dialRace(primary, fallback, { delayMs, signal } = {}) {
  return new Promise((resolve, reject) => {
    const attempts = new Map();
    const errors = [];
    let settled = false;
    let fallbackStarted = false;
    let timer;

    const settle = (winner) => {
      settled = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", onAbort);
      for (const [candidate, controller] of attempts) {
        if (candidate !== winner) {
          controller.abort();
        }
      }
    };
    const start = (candidate) => {
      const controller = new AbortController();
      attempts.set(candidate, controller);
      let dialing;
      try {
        // Dialing starts synchronously, so a session exists as soon as the race begins.
        dialing = Promise.resolve(candidate.dial(controller.signal));
      } catch (error) {
        dialing = Promise.reject(error);
      }
      dialing.then(
        (session) => {
          if (settled) {
            closeQuietly(session);
            return;
          }
          settle(candidate);
          resolve({ session, transport: candidate.transport });
        },
        (error) => {
          attempts.delete(candidate);
          if (settled) {
            return;
          }
          errors.push(`${candidate.transport}: ${statusFromTransportError(error).message}`);
          if (candidate === primary) {
            startFallback();
          } else if (attempts.size === 0) {
            settle(null);
            reject(unavailable(`transport unavailable: ${errors.join("; ")}`));
          }
        },
      );
    };
    const startFallback = () => {
      if (!fallbackStarted && !settled) {
        fallbackStarted = true;
        clearTimeout(timer);
        start(fallback);
      }
    };
    const onAbort = () => {
      if (!settled) {
        settle(null);
        reject(cancelled("connection cancelled", signal.reason));
      }
    };

    if (signal?.aborted) {
      onAbort();
      return;
    }
    signal?.addEventListener("abort", onAbort, { once: true });
    start(primary);
    timer = setTimeout(startFallback, delayMs);
  });
}

function closeQuietly(session) {
  try {
    session.close();
  } catch {
    // The session may already be gone.
  }
}
