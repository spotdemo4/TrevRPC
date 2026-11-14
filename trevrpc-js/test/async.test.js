// The promise and abort helpers of src/async.js, which the channel, the datagram flow and opening a
// stream build their abort handling on.
import assert from "node:assert/strict";
import test from "node:test";

import { abortable, deferred, settledOrAborted, sleep } from "../src/async.js";

/** Counts the timers that are scheduled and neither fired nor cleared while `run` runs. */
async function liveTimersAfter(t, run) {
  const live = new Set();
  const { setTimeout: schedule, clearTimeout: clear } = globalThis;
  t.mock.method(globalThis, "setTimeout", (callback, delay, ...rest) => {
    const timer = schedule(
      (...args) => {
        live.delete(timer);
        callback(...args);
      },
      delay,
      ...rest,
    );
    live.add(timer);
    return timer;
  });
  t.mock.method(globalThis, "clearTimeout", (timer) => {
    live.delete(timer);
    clear(timer);
  });
  await run();
  return live.size;
}

/** Records the abort listeners added to `signal` that have not been removed. */
function trackListeners(t, signal) {
  const listeners = new Set();
  const add = signal.addEventListener.bind(signal);
  const remove = signal.removeEventListener.bind(signal);
  t.mock.method(signal, "addEventListener", (type, listener, options) => {
    listeners.add(listener);
    add(type, listener, options);
  });
  t.mock.method(signal, "removeEventListener", (type, listener, options) => {
    listeners.delete(listener);
    remove(type, listener, options);
  });
  return listeners;
}

test("deferred settles its promise through the functions it returns", async () => {
  const resolved = deferred();
  resolved.resolve(7);
  assert.equal(await resolved.promise, 7);

  const rejected = deferred();
  const error = new Error("rejected");
  rejected.reject(error);
  await assert.rejects(rejected.promise, error);
});

test("sleep resolves after its delay and leaves no timer", async (t) => {
  const started = performance.now();
  const live = await liveTimersAfter(t, () => sleep(20, new AbortController().signal));
  assert.ok(performance.now() - started >= 15, "sleep returned before its delay");
  assert.equal(live, 0);
});

test("sleep with an already aborted signal resolves at once and leaves no timer", async (t) => {
  const controller = new AbortController();
  controller.abort(new Error("closed"));
  // Before src/async.js, this waited out the whole delay.
  let deadline;
  const waited = new Promise((_, reject) => {
    deadline = setTimeout(
      () => reject(new Error("sleep waited although its signal had aborted")),
      1000,
    );
  });
  try {
    const live = await liveTimersAfter(t, () =>
      Promise.race([sleep(1e6, controller.signal), waited]),
    );
    assert.equal(live, 0);
  } finally {
    clearTimeout(deadline);
  }
});

test("sleep resolves as soon as its signal aborts and clears its timer", async (t) => {
  const controller = new AbortController();
  const live = await liveTimersAfter(t, () => {
    const slept = sleep(1e6, controller.signal);
    controller.abort(new Error("closed"));
    return slept;
  });
  assert.equal(live, 0);
});

test("abortable settles like its promise and removes its abort listener", async (t) => {
  const controller = new AbortController();
  const listeners = trackListeners(t, controller.signal);

  assert.equal(await abortable(Promise.resolve(1), controller.signal), 1);
  assert.equal(listeners.size, 0, "listener left after the promise resolved");

  const error = new Error("failed");
  await assert.rejects(abortable(Promise.reject(error), controller.signal), error);
  assert.equal(listeners.size, 0, "listener left after the promise rejected");
});

test("abortable rejects with the abort reason, at once if the signal has aborted", async () => {
  const reason = new Error("aborted");
  const pending = deferred();

  const controller = new AbortController();
  const waiting = abortable(pending.promise, controller.signal);
  controller.abort(reason);
  await assert.rejects(waiting, reason);

  const aborted = new AbortController();
  aborted.abort(reason);
  await assert.rejects(abortable(pending.promise, aborted.signal), reason);

  // What the promise settles with after the abort is ignored.
  pending.resolve("late");
});

test("settledOrAborted resolves whether the promise settles or the signal aborts", async () => {
  const signal = new AbortController().signal;
  assert.equal(await settledOrAborted(Promise.resolve(1), signal), undefined);
  assert.equal(await settledOrAborted(Promise.reject(new Error("failed")), signal), undefined);

  const controller = new AbortController();
  const waiting = settledOrAborted(new Promise(() => {}), controller.signal);
  controller.abort(new Error("aborted"));
  assert.equal(await waiting, undefined);
});
