const completedObservations = new WeakSet();
const groupTerminations = new WeakMap();
const observations = new WeakMap();
const terminations = new WeakMap();

export class ChildExitTimeoutError extends Error {}

export function childHasExited(child) {
  return child.exitCode !== null || child.signalCode !== null;
}

function childResourcesClosed(child) {
  return !child.connected && child.stdio.every((stream) => stream == null || stream.closed);
}

export function observeChild(child) {
  let observation = observations.get(child);
  if (observation != null) return observation;

  observation = new Promise((resolve, reject) => {
    let settled = false;
    const cleanup = () => {
      child.off("close", onClose);
      child.off("error", onError);
    };
    const finish = (settle, value) => {
      if (settled) return;
      settled = true;
      completedObservations.add(child);
      cleanup();
      settle(value);
    };
    const onClose = (code, signal) => finish(resolve, { code, signal });
    const onError = (error) => finish(reject, error);

    child.once("close", onClose);
    child.once("error", onError);
    if (childHasExited(child) && childResourcesClosed(child)) {
      finish(resolve, { code: child.exitCode, signal: child.signalCode });
    }
  });
  observation.catch(() => {});
  observations.set(child, observation);
  return observation;
}

export function trackChild(child) {
  observeChild(child);
  return child;
}

export function waitForChild(child, timeoutMs = 10_000, label = "child") {
  const observation = observeChild(child);
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new ChildExitTimeoutError(`${label} did not exit within ${timeoutMs}ms`)),
      timeoutMs,
    );
    timer.unref();
    observation.then(
      (result) => {
        clearTimeout(timer);
        resolve(result);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

export function stopChild(
  child,
  {
    graceMs = 10_000,
    killWaitMs = 10_000,
    label = "child",
    sendSignal = (signal) => child.kill(signal),
  } = {},
) {
  let termination = terminations.get(child);
  if (termination != null) return termination;

  termination = (async () => {
    const observation = observeChild(child);
    if (completedObservations.has(child)) return await observation;
    if (!childHasExited(child)) sendSignal("SIGTERM");
    try {
      return await waitForChild(child, graceMs, label);
    } catch (error) {
      if (!(error instanceof ChildExitTimeoutError)) throw error;
      if (completedObservations.has(child)) return await observation;
      if (!childHasExited(child)) sendSignal("SIGKILL");
      try {
        return await waitForChild(child, killWaitMs, `${label} after SIGKILL`);
      } catch (killError) {
        if (!(killError instanceof ChildExitTimeoutError)) throw killError;
        if (completedObservations.has(child)) return await observation;
        throw new AggregateError([error, killError], `${label} could not be stopped`);
      }
    }
  })();
  terminations.set(child, termination);
  termination.catch(() => {
    if (terminations.get(child) === termination) terminations.delete(child);
  });
  return termination;
}

function processGroupExists(pid) {
  try {
    process.kill(-pid, 0);
    return true;
  } catch (error) {
    if (error?.code === "ESRCH") return false;
    throw error;
  }
}

function signalProcessGroup(pid, signal) {
  try {
    process.kill(-pid, signal);
  } catch (error) {
    if (error?.code !== "ESRCH") throw error;
  }
}

function waitForProcessGroupExit(pid, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  return new Promise((resolve, reject) => {
    const poll = () => {
      try {
        if (!processGroupExists(pid)) {
          resolve();
          return;
        }
      } catch (error) {
        reject(error);
        return;
      }
      if (Date.now() >= deadline) {
        reject(new ChildExitTimeoutError(`${label} did not exit within ${timeoutMs}ms`));
        return;
      }
      setTimeout(poll, 10);
    };
    poll();
  });
}

async function waitForDetachedChildGroup(child, pid, timeoutMs, label) {
  const [, result] = await Promise.all([
    waitForProcessGroupExit(pid, timeoutMs, label),
    waitForChild(child, timeoutMs, `${label} leader`),
  ]);
  return result;
}

export function stopDetachedChildGroup(
  child,
  { graceMs = 10_000, killWaitMs = 10_000, label = "child process group" } = {},
) {
  if (process.platform === "win32" || child.pid == null) {
    return stopChild(child, { graceMs, killWaitMs, label });
  }

  let termination = groupTerminations.get(child);
  if (termination != null) return termination;

  termination = (async () => {
    const observation = observeChild(child);
    if (!processGroupExists(child.pid)) return await observation;

    signalProcessGroup(child.pid, "SIGTERM");
    try {
      return await waitForDetachedChildGroup(child, child.pid, graceMs, label);
    } catch (error) {
      if (!(error instanceof ChildExitTimeoutError)) throw error;
      signalProcessGroup(child.pid, "SIGKILL");
      try {
        return await waitForDetachedChildGroup(
          child,
          child.pid,
          killWaitMs,
          `${label} after SIGKILL`,
        );
      } catch (killError) {
        if (!(killError instanceof ChildExitTimeoutError)) throw killError;
        throw new AggregateError([error, killError], `${label} could not be stopped`);
      }
    }
  })();
  groupTerminations.set(child, termination);
  termination.catch(() => {
    if (groupTerminations.get(child) === termination) groupTerminations.delete(child);
  });
  return termination;
}
