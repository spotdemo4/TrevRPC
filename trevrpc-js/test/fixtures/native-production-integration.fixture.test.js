import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createSocket } from "node:dgram";
import { existsSync, readFileSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { constants as osConstants, tmpdir } from "node:os";
import { join } from "node:path";
import test, { after } from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { Worker } from "node:worker_threads";

import { Code, RpcKind, RpcStreamFrameKind } from "../../src/index.js";

const require = createRequire(import.meta.url);
const addonPath = join(import.meta.dirname, "..", "..", "build", "native", "trevrpc_native.node");
const nativeTestHooksAvailable =
  existsSync(addonPath) && readFileSync(addonPath).includes(Buffer.from("trevrpc-node-test:"));
const fixturePath =
  process.env.TREVRPC_GREETER_SERVER ??
  join(import.meta.dirname, "..", "..", "..", "trevrpc-c", "build", "trevrpc_greeter_server");
const available = existsSync(addonPath) && existsSync(fixturePath);
let temporaryDirectory;
let server;

if (!available) {
  test(
    "production ABI1 integration fixture",
    { skip: `missing addon or fixture: ${fixturePath}` },
    () => {},
  );
} else {
  after(async () => {
    if (server != null) {
      await stopProcess(server, 10_000, "integration fixture");
      server = null;
    }
    if (temporaryDirectory != null) {
      await rm(temporaryDirectory, { force: true, recursive: true });
    }
  });

  test(
    "production ABI1 WebTransport listener and client negotiate path and origin",
    { timeout: 15_000 },
    async () => {
      const native = require(addonPath);
      const certificate = await makeCertificate();
      const origin = "http://127.0.0.1:8080";
      const webtransportServer = await native.listenMsQuic({
        host: "127.0.0.1",
        port: 0,
        certFile: certificate.cert,
        keyFile: certificate.key,
        transport: "webtransport",
        path: "/trevrpc",
        origin,
      });
      let client;
      try {
        client = await native.connectMsQuic({
          host: "127.0.0.1",
          port: webtransportServer.port,
          transport: "webtransport",
          path: "/trevrpc",
          origin,
          skipCertificateValidation: true,
        });
      } finally {
        if (client != null) {
          client.close();
          await client.closed;
        }
        webtransportServer.close();
        await webtransportServer.serve();
      }
    },
  );

  test("legacy listener selectors map to one canonical transport", async () => {
    const native = require(addonPath);
    const certificate = await makeCertificate();
    const http3Server = await native.listenMsQuic({
      host: "127.0.0.1",
      port: 0,
      certFile: certificate.cert,
      keyFile: certificate.key,
      enableNative: false,
      enableHttp3: true,
      http3Path: "/trevrpc",
    });
    http3Server.close();
    await http3Server.serve();

    await assert.rejects(
      native.listenMsQuic({
        host: "127.0.0.1",
        port: 0,
        certFile: certificate.cert,
        keyFile: certificate.key,
        transport: "http3",
        enableNative: false,
      }),
    );
  });

  test(
    "production ABI1 endpoint, calls, streams, cancellation, and teardown",
    { timeout: 30_000 },
    async () => {
      const native = require(addonPath);
      assert.deepEqual(Object.getOwnPropertyNames(native).sort(), [
        "connectMsQuic",
        "createCancellation",
        "listenMsQuic",
      ]);

      const certificate = await makeCertificate();
      const port = await freeUdpPort();
      server = spawn(
        fixturePath,
        ["127.0.0.1", String(port), certificate.cert, certificate.key, "native"],
        {
          stdio: ["ignore", "pipe", "pipe"],
        },
      );
      await waitForLine(server, /serving native on 127\.0\.0\.1:/u);

      const client = await native.connectMsQuic({
        host: "127.0.0.1",
        port,
        skipCertificateValidation: true,
        idleTimeoutMs: 10_000,
      });
      try {
        const request = helloRequest("production");
        const unary = await client.call({
          service: "hello.v1.Greeter",
          method: "SayHello",
          kind: RpcKind.Unary,
          version: 1,
          body: request,
          metadata: {},
        });
        assert.equal(unary.status, Code.Ok);
        assert.ok(unary.body.byteLength > 0);

        const upload = await client.startStream({
          service: "hello.v1.Greeter",
          method: "LotsOfGreetings",
          kind: RpcKind.ClientStreaming,
          version: 1,
          body: new Uint8Array(),
          metadata: {},
        });
        await upload.sendMessage(helloRequest("one"));
        await upload.sendMessage(helloRequest("two"));
        await upload.finishSend();
        const uploadResponse = await upload.recv();
        assert.equal(uploadResponse.kind, RpcStreamFrameKind.Message);
        assert.ok(uploadResponse.body.byteLength > 0);
        upload.close();

        const tracePath = process.env.TREVRPC_NODE_TRACE_FILE;
        const traceOffset =
          nativeTestHooksAvailable && tracePath != null
            ? readFileSync(tracePath, "utf8").length
            : 0;
        const stream = await client.startStream({
          service: "hello.v1.Greeter",
          method: "LotsOfReplies",
          kind: RpcKind.ServerStreaming,
          version: 1,
          body: helloRequest("non-ok"),
          metadata: {},
        });
        // Let the native poll drain the complete response before JS begins recv;
        // this exercises buffered delivery after STREAM_CLOSED.
        await delay(50);
        const frames = [];
        for (;;) {
          const frame = await stream.recv();
          frames.push(frame);
          if (frame.kind === RpcStreamFrameKind.Status) {
            break;
          }
        }
        assert.equal(frames.filter((frame) => frame.kind === RpcStreamFrameKind.Message).length, 3);
        const terminal = frames.at(-1);
        assert.equal(terminal.status, Code.ResourceExhausted);
        assert.equal(terminal.message, "fixture terminal status");
        if (nativeTestHooksAvailable && tracePath != null) {
          const releaseTrace = readFileSync(tracePath, "utf8").slice(traceOffset);
          const insertedKeys = Array.from(
            releaseTrace.matchAll(
              /^pid=\d+ runtime=\d+ event=key-insert kind=(\d+) owner=(\d+) slot=(\d+) generation=(\d+)$/gmu,
            ),
            ([, kind, owner, slot, generation]) => ({ generation, kind, owner, slot }),
          );
          assert.equal(insertedKeys.length, 2, releaseTrace);
          for (const key of insertedKeys) {
            const event = key.kind === "2" ? "call-release" : "stream-release";
            assert.match(
              releaseTrace,
              new RegExp(
                `^pid=\\d+ runtime=\\d+ event=${event} kind=${key.kind} owner=${key.owner} slot=${key.slot} generation=${key.generation}$`,
                "mu",
              ),
            );
          }
        }
        assert.equal(await stream.recv(), null);
        stream.close();

        const manyStream = await client.startStream({
          service: "hello.v1.Greeter",
          method: "LotsOfReplies",
          kind: RpcKind.ServerStreaming,
          version: 1,
          body: helloRequest("many-eof"),
          metadata: {},
        });
        for (;;) {
          const batch = await manyStream.recvMany();
          const terminalFrame = batch.find((frame) => frame.kind === RpcStreamFrameKind.Status);
          if (terminalFrame != null) {
            break;
          }
        }
        assert.deepEqual(await manyStream.recvMany(), [null]);
        manyStream.close();

        const bodyStream = await client.startStream({
          service: "hello.v1.Greeter",
          method: "LotsOfReplies",
          kind: RpcKind.ServerStreaming,
          version: 1,
          body: helloRequest("body-eof"),
          metadata: {},
        });
        for (;;) {
          const batch = await bodyStream.recvBodyBatch();
          if (batch == null || batch.eof) {
            break;
          }
        }
        assert.equal(await bodyStream.recvBodyBatch(), null);
        bodyStream.close();

        // Closing after a body-batch EOF can synchronously reject the active
        // native receive waiter. Keep the client-stream path exercised across
        // fresh calls, matching the cross-language benchmark workload.
        const { clientStreaming } = await import("../../src/client.js");
        const { RawNodeTransport } = await import("../../src/node.js");
        const messageType = {
          encode(value) {
            return { finish: () => helloRequest(value.value) };
          },
          decode(body) {
            return body;
          },
        };
        const transport = new RawNodeTransport(client);
        for (let index = 0; index < 32; index += 1) {
          const repeatedCall = await clientStreaming(
            transport,
            "hello.v1.Greeter",
            "LotsOfGreetings",
            messageType,
            messageType,
            { streamIdleTimeoutMs: undefined },
          );
          await repeatedCall.sendMany([
            { value: `body-eof-${index}-one` },
            { value: `body-eof-${index}-two` },
          ]);
          const response = await repeatedCall.closeAndRecv();
          assert.ok(response instanceof Uint8Array);
          assert.ok(response.byteLength > 0);
        }

        if (nativeTestHooksAvailable) {
          process.env.TREVRPC_NODE_FAIL_BODY_CONVERSION = "1";
          try {
            const conversionFailure = await client.startStream({
              service: "hello.v1.Greeter",
              method: "LotsOfReplies",
              kind: RpcKind.ServerStreaming,
              version: 1,
              body: helloRequest("conversion-failure"),
              metadata: {},
            });
            await assert.rejects(
              conversionFailure.recv(),
              (error) => error instanceof Error && error.nativeCode === -osConstants.errno.EPROTO,
            );
            conversionFailure.close();
          } finally {
            delete process.env.TREVRPC_NODE_FAIL_BODY_CONVERSION;
          }
        }

        const cancellation = native.createCancellation();
        const cancelledStream = await client.startStream(
          {
            service: "hello.v1.Greeter",
            method: "LotsOfGreetings",
            kind: RpcKind.ClientStreaming,
            version: 1,
            body: new Uint8Array(),
            metadata: {},
          },
          cancellation,
        );
        cancellation.cancel();
        await assert.rejects(
          settlesWithin(cancelledStream.recv(), 3_000, "cancellation"),
          (error) =>
            error?.nativeCode != null ||
            error?.code === Code.Cancelled ||
            error?.code === Code.Unavailable,
        );
        cancelledStream.close();
      } finally {
        client.close();
        await settlesWithin(client.closed, 5_000, "client close");
      }

      const child = spawn(
        process.execPath,
        [
          "--input-type=module",
          "-e",
          `
            import assert from "node:assert/strict";
            import { createRequire } from "node:module";
            const loadAddon = createRequire(import.meta.url);
            const native = loadAddon(${JSON.stringify(addonPath)});
            const request = Uint8Array.from([0x0a, 10, ...new TextEncoder().encode("child-call")]);
            const client = await native.connectMsQuic({
              host: "127.0.0.1",
              port: ${port},
              skipCertificateValidation: true,
              idleTimeoutMs: 10_000,
            });
            const response = await client.call({
              service: "hello.v1.Greeter",
              method: "SayHello",
              kind: ${RpcKind.Unary},
              version: 1,
              body: request,
              metadata: {},
            });
            assert.equal(response.status, ${Code.Ok});
            assert.ok(response.body.byteLength > 0);
            client.close();
            await client.closed;
            process.stdout.write("natural-exit-ok\\n");
          `,
        ],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      let childOutput = "";
      let childError = "";
      child.stdout.on("data", (chunk) => {
        childOutput += chunk;
      });
      child.stderr.on("data", (chunk) => {
        childError += chunk;
      });
      const { code: childCode, signal: childSignal } = await waitForExit(child);
      assert.equal(childCode, 0, childError);
      assert.equal(childSignal, null, childError);
      assert.match(childOutput, /natural-exit-ok/u, childError);

      const worker = new Worker(
        `const { parentPort } = require("node:worker_threads");
       const native = require(${JSON.stringify(addonPath)});
       const pending = native.connectMsQuic({host:"127.0.0.1", port:${port}, skipCertificateValidation:true, idleTimeoutMs:600000});
       parentPort.postMessage("started");
       pending.catch(() => {});`,
        { eval: true },
      );
      let workerTermination;
      const terminateWorker = () => {
        workerTermination ??= worker.terminate();
        return workerTermination;
      };
      try {
        await onceWorkerMessage(worker, "started", 3_000);
        await settlesWithin(terminateWorker(), 10_000, "worker teardown");
      } finally {
        await terminateWorker().catch(() => {});
      }
    },
  );

  test(
    "send and FIN admission failures reject deterministically",
    {
      timeout: 30_000,
      skip: nativeTestHooksAvailable ? false : "native lifecycle test hooks are disabled",
    },
    async () => {
      const certificate = await makeCertificate();
      const port = await freeUdpPort();
      const admissionServer = spawn(
        fixturePath,
        ["127.0.0.1", String(port), certificate.cert, certificate.key, "native"],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      try {
        await waitForLine(admissionServer, /serving native on 127\.0\.0\.1:/u);
        for (const [label, flag, action] of [
          ["send-id", "TREVRPC_NODE_FAIL_SEND_OPERATION_ID", "send"],
          ["send-record", "TREVRPC_NODE_FAIL_SEND_OPERATION_RECORD", "send"],
          ["fin-id", "TREVRPC_NODE_FAIL_SEND_OPERATION_ID", "fin"],
          ["fin-record", "TREVRPC_NODE_FAIL_SEND_OPERATION_RECORD", "fin"],
        ]) {
          const child = spawn(
            process.execPath,
            [
              "--input-type=module",
              "-e",
              `
                import assert from "node:assert/strict";
                import { createRequire } from "node:module";
                const loadAddon = createRequire(import.meta.url);
                const native = loadAddon(${JSON.stringify(addonPath)});
                const client = await native.connectMsQuic({
                  host: "127.0.0.1",
                  port: ${port},
                  skipCertificateValidation: true,
                  idleTimeoutMs: 10_000,
                });
                const stream = await client.startStream({
                  service: "hello.v1.Greeter",
                  method: "LotsOfGreetings",
                  kind: 1,
                  version: 1,
                  body: new Uint8Array(),
                  metadata: {},
                });
                let failed = false;
                try {
                  await ${action === "send" ? "stream.sendMessage(Uint8Array.from([10, 5, 102, 97, 105, 108]))" : "stream.finishSend()"};
                } catch (error) {
                  failed = true;
                  assert.ok(error.nativeCode != null || error.code != null);
                }
                assert.equal(failed, true);
                stream.close();
                client.close();
                await client.closed;
                process.stdout.write(${JSON.stringify(`${label}-ok\\n`)});
              `,
            ],
            {
              env: { ...process.env, [flag]: "1" },
              stdio: ["ignore", "pipe", "pipe"],
            },
          );
          let output = "";
          let errorOutput = "";
          child.stdout.on("data", (chunk) => {
            output += chunk;
          });
          child.stderr.on("data", (chunk) => {
            errorOutput += chunk;
          });
          let childResult;
          try {
            childResult = await waitForExit(child);
          } catch (error) {
            error.message = `${label}: ${error.message}; stderr: ${errorOutput}`;
            throw error;
          }
          const { code, signal } = childResult;
          assert.equal(code, 0, `${label}: ${errorOutput}`);
          assert.equal(signal, null, `${label}: ${errorOutput}`);
          assert.match(output, new RegExp(`${label}-ok`), errorOutput);
        }
      } finally {
        await stopProcess(admissionServer, 10_000, "admission fixture");
      }
    },
  );

  test(
    "connected idle endpoint keeps client.closed live until remote fixture closure",
    { timeout: 30_000 },
    async () => {
      const certificate = await makeCertificate();
      const port = await freeUdpPort();
      const closingServer = spawn(
        fixturePath,
        ["127.0.0.1", String(port), certificate.cert, certificate.key, "native"],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      let child;
      let childOutput = "";
      let childError = "";
      try {
        await waitForLine(closingServer, /serving native on 127\.0\.0\.1:/u);
        child = spawn(
          process.execPath,
          [
            "--input-type=module",
            "-e",
            `
              import { createRequire } from "node:module";
              const loadAddon = createRequire(import.meta.url);
              const native = loadAddon(${JSON.stringify(addonPath)});
              const client = await native.connectMsQuic({
                host: "127.0.0.1",
                port: ${port},
                skipCertificateValidation: true,
                idleTimeoutMs: 10_000,
              });
              process.stdout.write("connected\\n");
              await client.closed;
              process.stdout.write("remote-close-ok\\n");
            `,
          ],
          { stdio: ["ignore", "pipe", "pipe"] },
        );
        let connectedResolve;
        const connected = new Promise((resolve) => {
          connectedResolve = resolve;
        });
        child.stdout.on("data", (chunk) => {
          childOutput += chunk;
          if (childOutput.includes("connected")) {
            connectedResolve();
          }
        });
        child.stderr.on("data", (chunk) => {
          childError += chunk;
        });
        await settlesWithin(connected, 5_000, `client connection: ${childError}`);
        await stopProcess(closingServer, 10_000, "remote-close fixture");
        const { code: childCode, signal: childSignal } = await waitForExit(child);
        assert.equal(childCode, 0, childError);
        assert.equal(childSignal, null, childError);
        assert.match(childOutput, /remote-close-ok/u, childError);
      } finally {
        await stopProcess(closingServer, 10_000, "remote-close fixture");
        if (child != null && child.exitCode == null)
          await stopProcess(child, 10_000, "remote-close client");
      }
    },
  );
}

async function makeCertificate() {
  temporaryDirectory ??= await mkdtemp(join(tmpdir(), "trevrpc-js-native-integration-"));
  const cert = join(temporaryDirectory, "cert.pem");
  const key = join(temporaryDirectory, "key.pem");
  const result = await run("openssl", [
    "req",
    "-x509",
    "-newkey",
    "ec",
    "-pkeyopt",
    "ec_paramgen_curve:prime256v1",
    "-nodes",
    "-days",
    "1",
    "-subj",
    "/CN=localhost",
    "-addext",
    "subjectAltName=DNS:localhost,IP:127.0.0.1",
    "-keyout",
    key,
    "-out",
    cert,
  ]);
  assert.equal(result.code, 0, result.stderr);
  return { cert, key };
}

function helloRequest(name) {
  const bytes = new TextEncoder().encode(name);
  return Uint8Array.from([0x0a, bytes.length, ...bytes]);
}

async function freeUdpPort() {
  const socket = createSocket("udp4");
  await new Promise((resolve, reject) => {
    socket.once("error", reject);
    socket.bind(0, "127.0.0.1", resolve);
  });
  const port = socket.address().port;
  await new Promise((resolve) => socket.close(resolve));
  return port;
}

function waitForLine(child, pattern) {
  return new Promise((resolve, reject) => {
    let output = "";
    const onData = (chunk) => {
      output += chunk.toString();
      if (pattern.test(output)) {
        cleanup();
        resolve();
      }
    };
    const onError = (error) => {
      cleanup();
      reject(error);
    };
    const onExit = (code) => {
      cleanup();
      reject(new Error(`fixture exited ${code}: ${output}`));
    };
    const cleanup = () => {
      child.stdout.off("data", onData);
      child.off("error", onError);
      child.off("exit", onExit);
    };
    child.stdout.on("data", onData);
    child.once("error", onError);
    child.once("exit", onExit);
  });
}

async function stopProcess(child, timeoutMs, label) {
  if (child.exitCode == null) {
    child.kill("SIGTERM");
  }
  try {
    return await waitForExit(child, timeoutMs, label);
  } catch {
    child.kill("SIGKILL");
    return await waitForExit(child, timeoutMs, `${label} after SIGKILL`);
  }
}

function settlesWithin(promise, timeoutMs, label) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`${label} timeout`)), timeoutMs);
    timer.unref();
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      (error) => {
        clearTimeout(timer);
        reject(error);
      },
    );
  });
}

function waitForExit(child, timeoutMs = 10_000, label = "child") {
  return new Promise((resolve, reject) => {
    let settled = false;
    let timeoutError;
    const cleanup = () => {
      clearTimeout(timer);
      child.off("close", onClose);
      child.off("error", onError);
    };
    const finish = (result) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (timeoutError == null) resolve(result);
      else reject(timeoutError);
    };
    const onClose = (code, signal) => finish({ code, signal });
    const onError = (error) => {
      if (settled) return;
      settled = true;
      cleanup();
      reject(error);
    };
    const timer = setTimeout(() => {
      if (settled) return;
      timeoutError = new Error(`${label} did not exit within ${timeoutMs}ms`);
      if (!child.kill("SIGKILL")) {
        settled = true;
        cleanup();
        reject(timeoutError);
      }
    }, timeoutMs);
    child.once("close", onClose);
    child.once("error", onError);
    if (child.exitCode != null || child.signalCode != null) {
      finish({ code: child.exitCode, signal: child.signalCode });
    }
  });
}

function onceWorkerMessage(worker, expected, timeoutMs) {
  return new Promise((resolve, reject) => {
    let settled = false;
    const finish = (error) => {
      if (settled) return;
      settled = true;
      cleanup();
      if (error == null) resolve();
      else reject(error);
    };
    const onMessage = (message) => {
      if (message === expected) finish();
    };
    const onError = (error) => finish(error);
    const onExit = (code) => finish(new Error(`Worker exited before ${expected}: ${code}`));
    const timer = setTimeout(
      () => finish(new Error(`Worker did not post ${expected} within ${timeoutMs}ms`)),
      timeoutMs,
    );
    const cleanup = () => {
      clearTimeout(timer);
      worker.off("message", onMessage);
      worker.off("error", onError);
      worker.off("exit", onExit);
    };
    worker.on("message", onMessage);
    worker.once("error", onError);
    worker.once("exit", onExit);
  });
}

function run(command, args) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "pipe"] });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk;
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk;
    });
    child.once("error", reject);
    child.once("close", (code, signal) => resolve({ code, signal, stdout, stderr }));
  });
}
