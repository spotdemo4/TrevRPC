const MaxTimerDelayMs = 2_147_483_647;

/** Schedules a cancellable timeout without overflowing platform timer limits. */
export function scheduleTimeout(callback, delayMs) {
  let remaining = delayMs;
  let timer;
  let cancelled = false;

  const schedule = () => {
    const chunk = Math.min(remaining, MaxTimerDelayMs);
    timer = setTimeout(() => {
      if (cancelled) {
        return;
      }
      remaining -= chunk;
      if (remaining > 0) {
        schedule();
      } else {
        callback();
      }
    }, chunk);
  };

  schedule();
  return () => {
    cancelled = true;
    clearTimeout(timer);
  };
}
