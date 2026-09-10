import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import {
  stopChild,
  stopDetachedChildGroup,
  trackChild,
  waitForChild,
} from "./child-process-supervisor.js";

const posixOnly = process.platform === "win32" ? "POSIX signal semantics required" : false;

function waitForOutput(child, pattern, timeoutMs = 2_000) {
  return new Promise((resolve, reject) => {
    let output = "";
    const cleanup = () => {
      clearTimeout(timer);
      child.stdout.off("data", onData);
      child.off("close", onClose);
      child.off("error", onError);
    };
    const finish = (settle, value) => {
      cleanup();
      settle(value);
    };
    const onData = (chunk) => {
      output += chunk;
      if (pattern.test(output)) finish(resolve, output);
    };
    const onClose = (code, signal) =>
      finish(reject, new Error(`child closed before output: ${code}/${signal}: ${output}`));
    const onError = (error) => finish(reject, error);
    const timer = setTimeout(
      () => finish(reject, new Error(`child output timeout: ${output}`)),
      timeoutMs,
    );
    child.stdout.setEncoding("utf8");
    child.stdout.on("data", onData);
    child.once("close", onClose);
    child.once("error", onError);
  });
}

function cleanupChild(t, child) {
  t.after(async () => {
    if (child.exitCode === null && child.signalCode === null) {
      await stopChild(child, { graceMs: 0, label: "test child cleanup" }).catch(() => {});
    }
  });
}

test("reports spawn errors", async () => {
  const child = trackChild(spawn("/trevrpc-missing-child-process"));
  await assert.rejects(
    waitForChild(child, 2_000, "missing child"),
    (error) => error?.code === "ENOENT",
  );
});

test("observes a completed signal exit after close", { skip: posixOnly }, async (t) => {
  const child = spawn(process.execPath, [
    "--input-type=module",
    "-e",
    'process.stdout.write("ready\\n"); setInterval(() => {}, 1000);',
  ]);
  cleanupChild(t, child);
  await waitForOutput(child, /ready/u);
  child.kill("SIGTERM");
  const [code, signal] = await once(child, "close");
  assert.deepEqual({ code, signal }, { code: null, signal: "SIGTERM" });
  assert.deepEqual(await waitForChild(child, 100, "closed child"), {
    code: null,
    signal: "SIGTERM",
  });
});

test("waits for inherited output pipes to close after exit", { skip: posixOnly }, async (t) => {
  const child = spawn(process.execPath, [
    "--input-type=module",
    "-e",
    `
      import { spawn } from "node:child_process";
      const grandchild = spawn(
        process.execPath,
        ["--input-type=module", "-e", "setTimeout(() => {}, 250)"],
        { detached: true, stdio: ["ignore", 1, 2] },
      );
      grandchild.unref();
    `,
  ]);
  cleanupChild(t, child);
  await once(child, "exit");

  let settled = false;
  const observation = waitForChild(child, 2_000, "pipe-holding child").then((result) => {
    settled = true;
    return result;
  });
  await delay(25);
  assert.equal(settled, false);
  assert.deepEqual(await observation, { code: 0, signal: null });
});

test(
  "does not escalate after the child exits during its grace period",
  {
    skip: posixOnly,
  },
  async (t) => {
    const child = trackChild(
      spawn(process.execPath, [
        "--input-type=module",
        "-e",
        `
        import { spawn } from "node:child_process";
        process.on("SIGTERM", () => {
          const grandchild = spawn(
            process.execPath,
            ["--input-type=module", "-e", "setTimeout(() => {}, 200)"],
            { detached: true, stdio: ["ignore", 1, 2] },
          );
          grandchild.unref();
          process.exit(0);
        });
        process.stdout.write("ready\\n");
        setInterval(() => {}, 1000);
      `,
      ]),
    );
    cleanupChild(t, child);
    await waitForOutput(child, /ready/u);

    const signals = [];
    const result = await stopChild(child, {
      graceMs: 50,
      killWaitMs: 2_000,
      label: "graceful child",
      sendSignal(signal) {
        signals.push(signal);
        return child.kill(signal);
      },
    });
    assert.deepEqual(result, { code: 0, signal: null });
    assert.deepEqual(signals, ["SIGTERM"]);
  },
);

test("escalates once and caches repeated stop requests", { skip: posixOnly }, async (t) => {
  const child = trackChild(
    spawn(process.execPath, [
      "--input-type=module",
      "-e",
      'process.on("SIGTERM", () => {}); process.stdout.write("ready\\n"); setInterval(() => {}, 1000);',
    ]),
  );
  cleanupChild(t, child);
  await waitForOutput(child, /ready/u);

  const signals = [];
  const options = {
    graceMs: 50,
    killWaitMs: 2_000,
    label: "stubborn child",
    sendSignal(signal) {
      signals.push(signal);
      return child.kill(signal);
    },
  };
  const first = stopChild(child, options);
  const second = stopChild(child, options);
  assert.strictEqual(second, first);
  assert.deepEqual(await first, { code: null, signal: "SIGKILL" });
  assert.deepEqual(signals, ["SIGTERM", "SIGKILL"]);
  assert.strictEqual(stopChild(child, options), first);
});

test("allows cleanup retry after a failed stop", { skip: posixOnly }, async (t) => {
  const child = trackChild(
    spawn(process.execPath, [
      "--input-type=module",
      "-e",
      'process.on("SIGTERM", () => {}); process.stdout.write("ready\\n"); setInterval(() => {}, 1000);',
    ]),
  );
  cleanupChild(t, child);
  await waitForOutput(child, /ready/u);

  await assert.rejects(
    stopChild(child, {
      graceMs: 10,
      killWaitMs: 10,
      label: "failed stop",
      sendSignal: () => false,
    }),
    AggregateError,
  );
  assert.deepEqual(
    await stopChild(child, {
      graceMs: 0,
      killWaitMs: 2_000,
      label: "retried stop",
    }),
    { code: null, signal: "SIGKILL" },
  );
});

test(
  "waits for and escalates a detached child process group",
  {
    skip: posixOnly,
  },
  async (t) => {
    const child = trackChild(
      spawn(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          `
          import { spawn } from "node:child_process";
          const grandchild = spawn(
            process.execPath,
            [
              "--input-type=module",
              "-e",
              "process.on('SIGTERM', () => {}); setInterval(() => {}, 1000)",
            ],
            { stdio: "ignore" },
          );
          process.on("SIGTERM", () => process.exit(0));
          process.stdout.write(String(grandchild.pid) + "\\n");
          setInterval(() => {}, 1000);
        `,
        ],
        { detached: true, stdio: ["ignore", "pipe", "pipe"] },
      ),
    );
    t.after(async () => {
      await stopDetachedChildGroup(child, {
        graceMs: 0,
        killWaitMs: 2_000,
        label: "test process group cleanup",
      }).catch(() => {});
    });
    const output = await waitForOutput(child, /^\d+\n$/u);
    const grandchildPid = Number(output.trim());

    assert.deepEqual(
      await stopDetachedChildGroup(child, {
        graceMs: 50,
        killWaitMs: 2_000,
        label: "stubborn process group",
      }),
      { code: 0, signal: null },
    );
    assert.throws(
      () => process.kill(grandchildPid, 0),
      (error) => error?.code === "ESRCH",
    );
  },
);
