import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { constants as osConstants, tmpdir } from "node:os";
import { join } from "node:path";
import test, { after } from "node:test";
import { setImmediate, setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";

import { connect } from "@trevrpc/trevrpc-js";
import { Channel, NodeServer } from "@trevrpc/trevrpc-js/node";
import { RawNodeTransport } from "@trevrpc/trevrpc-js/node/advanced";

import {
  Code,
  RpcKind,
  RpcStreamFrameKind,
  bidirectionalStreaming,
  createRoot,
} from "../src/index.js";

const require = createRequire(import.meta.url);
const nativeAddonPath = join(import.meta.dirname, "..", "build", "native", "trevrpc_native.node");
const thisFile = fileURLToPath(import.meta.url);
const gcChild = process.env.TREVRPC_NATIVE_OWNERSHIP_LIFECYCLE_GC_CHILD === "1";
const gcAvailable = typeof global.gc === "function";
// A black-hole UDP peer can take one MsQuic handshake retransmission window to close.
const stalledConnectShutdownTimeoutMs = 5_000;
let certificateDirectory;

if (gcChild && !gcAvailable) {
  throw new Error("forced-GC lifecycle child started without global.gc");
}

if (!gcAvailable) {
  test(
    "native ownership lifecycle tests run with forced GC",
    { skip: !existsSync(nativeAddonPath) },
    () => {
      const childEnvironment = {
        ...process.env,
        TREVRPC_NATIVE_OWNERSHIP_LIFECYCLE_GC_CHILD: "1",
      };
      delete childEnvironment.NODE_TEST_CONTEXT;
      const result = spawnSync(process.execPath, ["--expose-gc", thisFile], {
        encoding: "utf8",
        env: childEnvironment,
        timeout: 180_000,
      });
      const output = `${result.stdout ?? ""}\n${result.stderr ?? ""}`;

      assert.equal(result.error, undefined, output);
      assert.equal(result.signal, null, output);
      assert.equal(result.status, 0, output);
      assert.match(
        output,
        /native client close transaction pins the wrapper across final release/u,
      );
      assert.match(
        output,
        /native response external body owner survives C cleanup until GC finalizer/u,
      );
    },
  );
} else {
  const native = existsSync(nativeAddonPath) ? require(nativeAddonPath) : null;
  const nativeTestHooks = typeof native?._debugClientCloseReleaseRace === "function";
  after(async () => {
    if (certificateDirectory != null) {
      await rm(certificateDirectory, { force: true, recursive: true });
    }
  });

  test(
    "native client close transaction pins the wrapper across final release",
    { skip: typeof native?._debugClientCloseReleaseRace !== "function" },
    () => {
      const result = native._debugClientCloseReleaseRace();
      assert.equal(result.passed, true, JSON.stringify(result));
      for (const interleaving of [
        result.firstCloseVsFinalRelease,
        result.finalizerAfterExplicitClose,
      ]) {
        assert.equal(interleaving.passed, true, JSON.stringify(interleaving));
        assert.equal(interleaving.pinObserved, true);
        assert.equal(interleaving.stateObserved, true);
        assert.equal(interleaving.noDestroyBeforeResume, true);
        assert.equal(interleaving.destroyAttempts, 1);
        assert.equal(interleaving.destructionCount, 1);
        assert.equal(interleaving.prematureDestructionCount, 0);
        assert.equal(interleaving.barriersOk, true);
      }
    },
  );

  test(
    "native response external body owner survives C cleanup until GC finalizer",
    { skip: native == null, timeout: 15_000 },
    async () => {
      await forceGcCycles();
      const before = finalizerCount(native);
      const releasesBefore = ownerReleaseCount(native);
      const expected = new Uint8Array([1, 2, 3, 4, 5]);

      await withNativePair(
        async (server) => {
          server.nativeServer.register("ownership", "OpaqueUnary", RpcKind.Unary, (call) => {
            call
              .respond({
                status: Code.Unavailable,
                message: "opaque unary",
                body: expected,
                metadata: { trace: new Uint8Array([7, 8, 9]) },
              })
              .catch(() => {});
          });
          server.nativeServer.register("ownership", "EmptyUnary", RpcKind.Unary, (call) => {
            call
              .respond({
                status: Code.Unavailable,
                message: "empty opaque unary",
                body: new Uint8Array(),
              })
              .catch(() => {});
          });
        },
        async (client) => {
          await (async () => {
            let response = await client.nativeClient.call({
              service: "ownership",
              method: "OpaqueUnary",
              kind: RpcKind.Unary,
              version: 1,
              body: new Uint8Array(),
            });
            const body = response.body;

            assert.equal(response.status, Code.Unavailable);
            assert.equal(response.message, "opaque unary");
            assert.deepEqual(Array.from(response.metadata.trace), [7, 8, 9]);
            assert.deepEqual(Array.from(body), Array.from(expected));
            response = null;
            await forceGcCycles();

            assert.deepEqual(Array.from(body), Array.from(expected));
            assert.equal(finalizerCount(native), before);
          })();
          await waitForFinalizers(native, before + 1);
          assert.equal(ownerReleaseCount(native), releasesBefore + 1);

          const beforeEmpty = finalizerCount(native);
          const releasesBeforeEmpty = ownerReleaseCount(native);
          let empty = await client.nativeClient.call({
            service: "ownership",
            method: "EmptyUnary",
            kind: RpcKind.Unary,
            version: 1,
            body: new Uint8Array(),
          });
          assert.equal(empty.status, Code.Unavailable);
          assert.equal(empty.message, "empty opaque unary");
          assert.equal(empty.body instanceof Uint8Array, true);
          assert.equal(empty.body.byteLength, 0);
          assert.equal(ownerReleaseCount(native), releasesBeforeEmpty + 1);
          empty = null;
          await forceGcCycles();
          assert.equal(finalizerCount(native), beforeEmpty);
          assert.equal(ownerReleaseCount(native), releasesBeforeEmpty + 1);
        },
      );
    },
  );

  test(
    "native stream frame external body owner survives C cleanup until GC finalizer",
    { skip: native == null, timeout: 15_000 },
    async () => {
      await forceGcCycles();
      const before = finalizerCount(native);
      const releasesBefore = ownerReleaseCount(native);
      const expected = new Uint8Array([9, 8, 7, 6]);

      await withNativePair(
        async (server) => {
          server.nativeServer.register(
            "ownership",
            "OpaqueStreamFrame",
            RpcKind.ServerStreaming,
            (call) => {
              void (async () => {
                await call.sendMessage(expected);
                await call.finishStream(Code.ResourceExhausted, "opaque terminal", {
                  trailer: new Uint8Array([3, 2, 1]),
                });
              })().catch(() => call.close());
            },
          );
        },
        async (client) => {
          const stream = await client.nativeClient.startStream({
            service: "ownership",
            method: "OpaqueStreamFrame",
            kind: RpcKind.ServerStreaming,
            version: 1,
            body: new Uint8Array(),
          });
          let frame = await stream.recv();
          let body = frame.body;

          assert.equal(frame.kind, RpcStreamFrameKind.Message);
          assert.deepEqual(Array.from(body), Array.from(expected));
          frame = null;
          await forceGcCycles();

          assert.deepEqual(Array.from(body), Array.from(expected));
          assert.equal(finalizerCount(native), before);

          const terminal = await stream.recv();
          assert.equal(terminal.kind, RpcStreamFrameKind.Status);
          assert.equal(terminal.status, Code.ResourceExhausted);
          assert.equal(terminal.message, "opaque terminal");
          assert.deepEqual(Array.from(terminal.metadata.trailer), [3, 2, 1]);
          assert.equal(terminal.body.byteLength, 0);
          assert.equal(ownerReleaseCount(native), releasesBefore + 1);

          body = null;
          await waitForFinalizers(native, before + 1);
          assert.equal(ownerReleaseCount(native), releasesBefore + 2);
          stream.close();
        },
      );
    },
  );

  test(
    "native stream body batches transfer borrowed frame owners",
    { skip: native == null, timeout: 15_000 },
    async () => {
      await forceGcCycles();
      const before = finalizerCount(native);
      const releasesBefore = ownerReleaseCount(native);
      const firstExpected = new Uint8Array([42, 43, 44]);
      const secondExpected = new Uint8Array([51, 52, 53, 54]);
      let messagesSent;
      const messagesSentPromise = new Promise((resolve) => {
        messagesSent = resolve;
      });

      await withNativePair(
        async (server) => {
          server.nativeServer.register(
            "ownership",
            "OpaqueBatch",
            RpcKind.ServerStreaming,
            (call) => {
              void (async () => {
                await call.sendMessage(firstExpected);
                await call.sendMessage(secondExpected);
                messagesSent();
                await call.finishStream(Code.Ok);
              })().catch(() => call.close());
            },
          );
        },
        async (client) => {
          const stream = await client.nativeClient.startStream({
            service: "ownership",
            method: "OpaqueBatch",
            kind: RpcKind.ServerStreaming,
            version: 1,
            body: new Uint8Array(),
          });
          await messagesSentPromise;
          let bodies = [];
          let batch = null;
          while (bodies.length < 2) {
            batch = await stream.recvBodyBatch(2 - bodies.length);
            assert.equal(batch.bodies.length > 0, true);
            bodies.push(...batch.bodies);
          }
          let firstBody = bodies[0];
          let secondBody = bodies[1];
          batch = null;
          bodies = null;
          await forceGcCycles();

          assert.deepEqual(Array.from(firstBody), Array.from(firstExpected));
          assert.deepEqual(Array.from(secondBody), Array.from(secondExpected));
          assert.equal(finalizerCount(native), before);

          firstBody = null;
          await waitForFinalizers(native, before + 1);
          assert.equal(ownerReleaseCount(native), releasesBefore + 1);
          assert.deepEqual(Array.from(secondBody), Array.from(secondExpected));

          secondBody = null;
          await waitForFinalizers(native, before + 2);
          assert.equal(ownerReleaseCount(native), releasesBefore + 2);
          stream.close();
        },
      );
    },
  );

  test(
    "native opaque body conversion failure fallbacks release owners exactly once",
    {
      skip:
        typeof native?._debugSetNextBodyConversionFailure !== "function" ||
        typeof native?._debugBodyOwnerReleases !== "function",
      timeout: 15_000,
    },
    async () => {
      await forceGcCycles();
      const expected = new Uint8Array([61, 62, 63, 64]);

      await withNativePair(
        async (server) => {
          server.nativeServer.register("ownership", "ConversionFailure", RpcKind.Unary, (call) => {
            call.respond({ body: expected }).catch(() => {});
          });
        },
        async (client) => {
          const request = {
            service: "ownership",
            method: "ConversionFailure",
            kind: RpcKind.Unary,
            version: 1,
            body: new Uint8Array(),
          };

          let releases = ownerReleaseCount(native);
          let finalizers = finalizerCount(native);
          native._debugSetNextBodyConversionFailure(1);
          let response = await client.nativeClient.call(request);
          assert.deepEqual(Array.from(response.body), Array.from(expected));
          assert.equal(ownerReleaseCount(native), releases + 1);
          assert.equal(finalizerCount(native), finalizers);
          assert.equal(native._debugBodyConversionFailureStage(), 0);
          response = null;
          await forceGcCycles();
          assert.equal(finalizerCount(native), finalizers);

          releases = ownerReleaseCount(native);
          finalizers = finalizerCount(native);
          native._debugSetNextBodyConversionFailure(2);
          response = await client.nativeClient.call(request);
          assert.deepEqual(Array.from(response.body), Array.from(expected));
          assert.equal(ownerReleaseCount(native), releases);
          assert.equal(native._debugBodyConversionFailureStage(), 0);
          response = null;
          await waitForFinalizers(native, finalizers + 1);
          assert.equal(ownerReleaseCount(native), releases + 1);

          releases = ownerReleaseCount(native);
          finalizers = finalizerCount(native);
          native._debugSetNextBodyConversionFailure(3);
          await assert.rejects(
            client.nativeClient.call(request),
            (error) => error.nativeCode === -osConstants.errno.ENOMEM,
          );
          assert.equal(native._debugBodyConversionFailureStage(), 0);
          await waitForFinalizers(native, finalizers + 1);
          assert.equal(ownerReleaseCount(native), releases + 1);
        },
      );
    },
  );

  test(
    "Worker termination unblocks a live native serve operation",
    { skip: native == null, timeout: 10_000 },
    async () => {
      const certificate = await testCertificate();
      const worker = new Worker(
        `
const { parentPort } = require("node:worker_threads");
const native = require(${JSON.stringify(nativeAddonPath)});
(async () => {
  const server = await native.listenMsQuic({
    host: "127.0.0.1",
    port: 0,
    certFile: ${JSON.stringify(certificate.certFile)},
    keyFile: ${JSON.stringify(certificate.keyFile)},
  });
  server.serve();
  parentPort.postMessage("serving");
})().catch((error) => { throw error; });
`,
        { eval: true },
      );
      await waitForWorkerMessage(worker, (message) => message === "serving");
      await settlesWithin(worker.terminate(), 3_000);
    },
  );

  test(
    "Worker termination force-cancels an admitted native server call",
    { skip: native == null, timeout: 10_000 },
    async () => {
      const certificate = await testCertificate();
      const worker = new Worker(
        `
const { parentPort } = require("node:worker_threads");
const native = require(${JSON.stringify(nativeAddonPath)});
(async () => {
  const server = await native.listenMsQuic({
    host: "127.0.0.1",
    port: 0,
    certFile: ${JSON.stringify(certificate.certFile)},
    keyFile: ${JSON.stringify(certificate.keyFile)},
  });
  server.register("ownership", "StalledUnary", 0, (call) => {
    globalThis.heldCall = call;
    parentPort.postMessage("started");
  });
  server.serve().catch(() => {});
  parentPort.postMessage({ port: server.port });
})().catch((error) => { throw error; });
`,
        { eval: true },
      );
      const listening = await waitForWorkerMessage(
        worker,
        (message) => typeof message === "object" && Number.isInteger(message?.port),
      );
      const client = await RawNodeTransport.connect({
        host: "127.0.0.1",
        port: listening.port,
        skipCertificateValidation: true,
      });
      try {
        const started = waitForWorkerMessage(worker, (message) => message === "started");
        const response = client
          .call({
            service: "ownership",
            method: "StalledUnary",
            body: new Uint8Array(),
            metadata: {},
          })
          .then(
            (value) => ({ value }),
            (error) => ({ error }),
          );
        await started;
        await settlesWithin(worker.terminate(), 3_000);
        const outcome = await settlesWithin(response, 3_000);
        if (outcome.value !== undefined) {
          assert.equal(outcome.error, undefined);
          assert.equal(outcome.value.status, Code.Cancelled);
          assert.deepEqual(Array.from(outcome.value.body), []);
        } else {
          assert.ok(
            outcome.error.code === Code.Cancelled || outcome.error.code === Code.Unavailable,
            `unexpected forced-teardown error status ${outcome.error.code}`,
          );
        }
      } finally {
        client.close();
        await worker.terminate();
      }
    },
  );

  test("native server accepts opt-in HTTP/3 options", { skip: native == null }, async () => {
    const certificate = await testCertificate();
    const server = await native.listenMsQuic({
      host: "127.0.0.1",
      port: 0,
      certFile: certificate.certFile,
      keyFile: certificate.keyFile,
      enableHttp3: true,
      http3Path: "/rpc",
      http3Admission: ({ path, authority, secure }) =>
        secure && path === "/rpc" && authority.length > 0,
    });
    server.close();

    assert.throws(
      () =>
        native.listenMsQuic({
          host: "127.0.0.1",
          port: 0,
          certFile: certificate.certFile,
          keyFile: certificate.keyFile,
          http3Admission: true,
        }),
      /http3Admission must be a function/u,
    );
  });

  test(
    "native HTTP/3 admission timeout owns queued callback data",
    { skip: typeof native?._debugHttp3Admission !== "function" },
    async () => {
      let callbacks = 0;
      const admission = native._debugHttp3Admission(() => {
        callbacks += 1;
        return true;
      }, 20);
      const blockedUntil = Date.now() + 100;
      while (Date.now() < blockedUntil) {
        // Keep the JS thread busy so the native admission wait expires first.
      }
      assert.equal(await admission, false);
      await delay(20);
      assert.equal(callbacks, 0);
    },
  );

  test(
    "native HTTP/3 admission shutdown wakes waiters",
    { skip: typeof native?._debugHttp3Admission !== "function" },
    async () => {
      let callbacks = 0;
      const admitted = await native._debugHttp3Admission(
        () => {
          callbacks += 1;
          return true;
        },
        1_000,
        true,
      );
      assert.equal(admitted, false);
      assert.equal(callbacks, 0);
    },
  );

  test(
    "RawNodeTransport abort cancels a stalled native connect",
    { skip: native == null, timeout: 10_000 },
    async () => {
      const { createSocket } = await import("node:dgram");
      const sink = createSocket("udp4");
      await new Promise((resolve, reject) => {
        sink.once("error", reject);
        sink.bind(0, "127.0.0.1", resolve);
      });
      try {
        const controller = new AbortController();
        const connecting = RawNodeTransport.connect({
          host: "127.0.0.1",
          port: sink.address().port,
          skipCertificateValidation: true,
          idleTimeoutMs: 600_000,
          signal: controller.signal,
        });
        await setImmediate();
        controller.abort();

        await assert.rejects(
          settlesWithin(connecting, stalledConnectShutdownTimeoutMs),
          (error) => error.code === Code.Cancelled,
        );
      } finally {
        sink.close();
      }
    },
  );

  test(
    "Worker termination interrupts an in-progress native connect",
    { skip: native == null, timeout: 10_000 },
    async () => {
      const { createSocket } = await import("node:dgram");
      const sink = createSocket("udp4");
      await new Promise((resolve, reject) => {
        sink.once("error", reject);
        sink.bind(0, "127.0.0.1", resolve);
      });
      const worker = new Worker(
        `
const { parentPort, workerData } = require("node:worker_threads");
const native = require(${JSON.stringify(nativeAddonPath)});
native.connectMsQuic({
  host: "127.0.0.1",
  port: workerData.port,
  skipCertificateValidation: true,
  idleTimeoutMs: 600000,
}).catch(() => {});
parentPort.postMessage("connecting");
`,
        { eval: true, workerData: { port: sink.address().port } },
      );
      try {
        await waitForWorkerMessage(worker, (message) => message === "connecting");
        await settlesWithin(worker.terminate(), stalledConnectShutdownTimeoutMs);
      } finally {
        sink.close();
      }
    },
  );

  test(
    "Node channel reconnect only follows explicit native closed errors",
    { skip: native == null, timeout: 15_000 },
    async () => {
      const certificate = await testCertificate();
      const server = await NodeServer.listen({
        host: "127.0.0.1",
        port: 0,
        certFile: certificate.certFile,
        keyFile: certificate.keyFile,
      });
      server.register("ownership", "Unavailable", RpcKind.Unary, () => ({
        status: Code.Unavailable,
        message: "remote unavailable",
      }));
      const serving = server.serve();
      let client;
      try {
        client = await Channel.connect({
          host: "127.0.0.1",
          port: server.port,
          skipCertificateValidation: true,
          maxPendingSendBytes: 128,
          timeoutMs: 5_000,
        });
        const unavailableResponse = await settlesWithin(
          client.call({
            service: "ownership",
            method: "Unavailable",
            kind: RpcKind.Unary,
            version: 1,
            body: new Uint8Array(),
            metadata: {},
          }),
          5_000,
        );
        assert.equal(unavailableResponse.status, Code.Unavailable);
        assert.equal(client.generation, 1);
        assert.equal(client.state, "ready");

        await assert.rejects(
          settlesWithin(
            client.call({
              service: "ownership",
              method: "Unavailable",
              kind: RpcKind.Unary,
              version: 1,
              body: new Uint8Array(1024),
              metadata: {},
            }),
            5_000,
          ),
          (error) => error.nativeCode === -1004,
        );
        assert.equal(client.generation, 1);
        assert.equal(client.state, "ready");
      } finally {
        server.close();
        client?.close();
        await settlesWithin(serving, 5_000);
      }
    },
  );

  test(
    "Node channel observes an idle native connection shutdown",
    { skip: native == null, timeout: 15_000 },
    async () => {
      const certificate = await testCertificate();
      const server = await NodeServer.listen({
        host: "127.0.0.1",
        port: 0,
        certFile: certificate.certFile,
        keyFile: certificate.keyFile,
      });
      const serving = server.serve();
      const client = await connect({
        host: "127.0.0.1",
        port: server.port,
        skipCertificateValidation: true,
      });
      try {
        assert.equal(client.state, "ready");
        assert.equal(client.generation, 1);

        server.close();
        await serving;
        await waitForCondition(
          () => client.state === "reconnecting",
          "idle channel did not observe native connection shutdown",
        );

        assert.equal(client.generation, 1);
      } finally {
        client.close();
        server.close();
        await serving;
      }
    },
  );

  test(
    "normal Node server close drains an admitted native unary call",
    { skip: native == null, timeout: 15_000 },
    async () => {
      const certificate = await testCertificate();
      const server = await NodeServer.listen({
        host: "127.0.0.1",
        port: 0,
        certFile: certificate.certFile,
        keyFile: certificate.keyFile,
      });
      let releaseHandler;
      const handlerReleased = new Promise((resolve) => {
        releaseHandler = resolve;
      });
      let handlerStarted;
      const handlerStartedPromise = new Promise((resolve) => {
        handlerStarted = resolve;
      });
      server.register("ownership", "DrainUnary", RpcKind.Unary, async () => {
        handlerStarted();
        await handlerReleased;
        return { body: new Uint8Array([7, 8, 9]) };
      });

      const serving = server.serve();
      let serveSettled = false;
      void serving.then(
        () => {
          serveSettled = true;
        },
        () => {
          serveSettled = true;
        },
      );
      const client = await RawNodeTransport.connect({
        host: "127.0.0.1",
        port: server.port,
        skipCertificateValidation: true,
      });
      try {
        const responsePromise = client.call({
          service: "ownership",
          method: "DrainUnary",
          body: new Uint8Array([1]),
          metadata: {},
        });
        await settlesWithin(handlerStartedPromise, 3_000);

        server.close();
        await setImmediate();
        assert.equal(serveSettled, false);

        releaseHandler();
        const response = await settlesWithin(responsePromise, 5_000);
        assert.equal(response.status, Code.Ok);
        assert.deepEqual(Array.from(response.body), [7, 8, 9]);
        await settlesWithin(serving, 5_000);
        assert.equal(serveSettled, true);
      } finally {
        releaseHandler();
        client.close();
        server.close();
        await settlesWithin(serving, 5_000);
      }
    },
  );

  test(
    "Worker termination cancels a real stalled unary call",
    { skip: native == null, timeout: 15_000 },
    async () => {
      let heldCall;
      let callStarted;
      const callStartedPromise = new Promise((resolve) => {
        callStarted = resolve;
      });
      await withNativePair(
        async (server) => {
          server.nativeServer.register("ownership", "WorkerUnary", RpcKind.Unary, (call) => {
            heldCall = call;
            callStarted();
          });
        },
        async (_client, server) => {
          const worker = new Worker(
            `
const { parentPort } = require("node:worker_threads");
const native = require(${JSON.stringify(nativeAddonPath)});
(async () => {
  const client = await native.connectMsQuic({
    host: "127.0.0.1",
    port: ${server.port},
    skipCertificateValidation: true,
  });
  client.call({
    service: "ownership",
    method: "WorkerUnary",
    kind: 0,
    version: 1,
    body: new Uint8Array(1024 * 1024),
  }).catch(() => {});
  parentPort.postMessage("queued");
})().catch((error) => { throw error; });
`,
            { eval: true },
          );
          await waitForWorkerMessage(worker, (message) => message === "queued");
          await settlesWithin(callStartedPromise, 3_000);
          await settlesWithin(worker.terminate(), 3_000);
          heldCall?.close();
          heldCall = null;
        },
      );
    },
  );

  test(
    "Worker termination cancels a pending real MsQuic send",
    { skip: native == null, timeout: 20_000 },
    async () => {
      let heldCall;
      let callStarted;
      const callStartedPromise = new Promise((resolve) => {
        callStarted = resolve;
      });
      await withNativePair(
        async (server) => {
          server.nativeServer.register(
            "ownership",
            "WorkerPendingSend",
            RpcKind.BidirectionalStreaming,
            (call) => {
              heldCall = call;
              callStarted();
            },
          );
        },
        async (_client, server) => {
          const worker = new Worker(
            `
const { parentPort } = require("node:worker_threads");
const native = require(${JSON.stringify(nativeAddonPath)});
(async () => {
  const client = await native.connectMsQuic({
    host: "127.0.0.1",
    port: ${server.port},
    skipCertificateValidation: true,
  });
  const stream = await client.startStream({
    service: "ownership",
    method: "WorkerPendingSend",
    kind: 3,
    version: 1,
    body: new Uint8Array(),
  });
  const bodies = Array.from({ length: 12 }, () => new Uint8Array(3 * 1024 * 1024));
  stream.sendMessages(bodies).catch(() => {});
  stream.sendMessage(new Uint8Array([1])).catch(() => {});
  stream.finishSend().catch(() => {});
  parentPort.postMessage("queued");
})().catch((error) => { throw error; });
`,
            { eval: true },
          );
          await waitForWorkerMessage(worker, (message) => message === "queued");
          await settlesWithin(callStartedPromise, 3_000);
          await settlesWithin(worker.terminate(), stalledConnectShutdownTimeoutMs);
          heldCall?.close();
          heldCall = null;
        },
      );
    },
  );

  test(
    "native unary copies typed slices and ArrayBuffers with metadata prefixes",
    { skip: native == null, timeout: 15_000 },
    async () => {
      let releaseUnary;
      const unaryReleased = new Promise((resolve) => {
        releaseUnary = resolve;
      });
      let unaryStarted;
      const unaryStartedPromise = new Promise((resolve) => {
        unaryStarted = resolve;
      });
      const observed = [];

      await withNativePair(
        async (server) => {
          server.register("ownership", "Unary", RpcKind.Unary, async (call) => {
            observed.push({
              body: Array.from(call.request.body),
              metadata: Array.from(call.request.metadata.authorization),
            });
            unaryStarted();
            await unaryReleased;
            const responseStorage = new Uint8Array([0xff, ...call.request.body, 0xff]);
            return {
              body: responseStorage.subarray(1, responseStorage.byteLength - 1),
              metadata: { response: new Uint8Array([9, 8, 7]) },
            };
          });
        },
        async (client) => {
          const storage = new Uint8Array([0xff, 1, 2, 3, 4, 0xff]);
          const body = storage.subarray(1, 5);
          const metadataBuffer = new Uint8Array([6, 7, 8]).buffer;
          const responsePromise = client.call({
            service: "ownership",
            method: "Unary",
            body,
            metadata: { authorization: metadataBuffer },
          });
          body.fill(99);
          new Uint8Array(metadataBuffer).fill(99);

          await unaryStartedPromise;

          releaseUnary();
          const response = await responsePromise;
          assert.deepEqual(Array.from(response.body), [1, 2, 3, 4]);
          assert.deepEqual(Array.from(response.metadata.response), [9, 8, 7]);

          const arrayBuffer = new Uint8Array([10, 11, 12]).buffer;
          const second = client.call({
            service: "ownership",
            method: "Unary",
            body: arrayBuffer,
            metadata: { authorization: new Uint8Array([6, 7, 8]) },
          });
          new Uint8Array(arrayBuffer).fill(99);
          const secondResponse = await second;
          assert.deepEqual(Array.from(secondResponse.body), [10, 11, 12]);
        },
      );

      assert.deepEqual(observed, [
        { body: [1, 2, 3, 4], metadata: [6, 7, 8] },
        { body: [10, 11, 12], metadata: [6, 7, 8] },
      ]);
    },
  );

  test(
    "native single and batched stream messages preserve typed slices and ArrayBuffers",
    { skip: native == null, timeout: 15_000 },
    async () => {
      await withNativePair(
        async (server) => {
          server.register("ownership", "Bidi", RpcKind.BidirectionalStreaming, async (call) => {
            const bodies = [];
            for (;;) {
              const frame = await call.recv();
              if (frame == null) {
                break;
              }
              if (frame.kind === RpcStreamFrameKind.Message) {
                bodies.push(frame.body);
              }
            }
            await call.sendMessage(bodies[0]);
            await forceGcCycles(2);
            await call.sendMany(bodies.slice(1));
            await call.finishStream(Code.Ok);
          });
        },
        async (client) => {
          const sliceStorage = new Uint8Array([0xff, 1, 2, 0xff]);
          const slice = sliceStorage.subarray(1, 3);
          const arrayBuffer = new Uint8Array([3, 4, 5]).buffer;
          const finalBody = new Uint8Array([6]);
          const frames = await client.streamingCall(
            {
              service: "ownership",
              method: "Bidi",
              kind: RpcKind.BidirectionalStreaming,
              body: new Uint8Array(),
              metadata: { trace: new Uint8Array([42]) },
            },
            bodyBatchIterable([[slice], [arrayBuffer, finalBody]]),
          );

          const received = [];
          for await (const frame of frames) {
            if (frame.kind === RpcStreamFrameKind.Message) {
              received.push(Array.from(frame.body));
            }
          }
          assert.deepEqual(received, [[1, 2], [3, 4, 5], [6]]);
        },
      );
    },
  );

  test(
    "real MsQuic early terminal settles the pending request writer once",
    { skip: native == null, timeout: 15_000 },
    async () => {
      const Hello = createRoot({
        nested: {
          ownership: {
            nested: {
              Hello: {
                fields: { value: { type: "string", id: 1 } },
              },
            },
          },
        },
      }).lookupType("ownership.Hello");
      let requestReturns = 0;
      let requestReadCalls = 0;
      let requestReadSettlements = 0;
      let requestReadDone = false;
      let requestReadSettled;
      const requestReadSettledPromise = new Promise((resolve) => {
        requestReadSettled = resolve;
      });

      await withNativePair(
        async (server) => {
          server.register(
            "ownership",
            "EarlyTerminal",
            RpcKind.BidirectionalStreaming,
            async (call) => {
              await call.finishStream(Code.Ok);
            },
          );
        },
        async (client, server) => {
          const transport = {
            async streamingCall(request, requestBody, options) {
              const iterator = requestBody[Symbol.asyncIterator]();
              const readRequest = async (max) => {
                requestReadCalls += 1;
                const result =
                  max === undefined
                    ? await iterator.next()
                    : typeof iterator.nextBatch === "function"
                      ? await iterator.nextBatch(max)
                      : await iterator.next();
                requestReadSettlements += 1;
                requestReadDone = result.done;
                requestReadSettled();
                return result;
              };
              const wrapped = {
                [Symbol.asyncIterator]() {
                  return this;
                },
                next() {
                  return readRequest();
                },
                nextBatch(max) {
                  return readRequest(max);
                },
                async return() {
                  requestReturns += 1;
                  return typeof iterator.return === "function"
                    ? await iterator.return()
                    : { done: true, value: undefined };
                },
              };
              return await client.streamingCall(request, wrapped, options);
            },
          };
          let call;
          let primaryError;
          let cleanupError;
          try {
            call = await bidirectionalStreaming(
              transport,
              "ownership",
              "EarlyTerminal",
              Hello,
              Hello,
              { streamIdleTimeoutMs: undefined },
            );

            assert.equal(await settlesWithin(call.recv(), 5_000), undefined);
            await settlesWithin(requestReadSettledPromise, 5_000);
            await setImmediate();
            assert.equal(requestReadCalls, 1);
            assert.equal(requestReadSettlements, 1);
            assert.equal(requestReadDone, true);
            assert.ok(requestReturns === 0 || requestReturns === 1);
          } catch (error) {
            primaryError = error;
          } finally {
            try {
              await call?.close();
              await waitForCondition(
                () => server.activeCalls.size === 0,
                "early-terminal server handler did not settle",
              );
            } catch (error) {
              cleanupError = error;
            }
          }
          if (primaryError != null) {
            throw primaryError;
          }
          if (cleanupError != null) {
            throw cleanupError;
          }
        },
      );
    },
  );

  test(
    "native synchronous exhaustion and response failure settle queued operations",
    { skip: native == null, timeout: 15_000 },
    async () => {
      let responseFailure;
      const responseFailurePromise = new Promise((resolve) => {
        responseFailure = resolve;
      });
      let exhaustStarted;
      const exhaustStartedPromise = new Promise((resolve) => {
        exhaustStarted = resolve;
      });
      let exhaustClosed;
      const exhaustClosedPromise = new Promise((resolve) => {
        exhaustClosed = resolve;
      });
      await withNativePair(
        async (server) => {
          server.nativeServer.register(
            "ownership",
            "Exhaust",
            RpcKind.BidirectionalStreaming,
            (call) => {
              exhaustStarted();
              setTimeout(() => {
                call.close();
                exhaustClosed();
              }, 50);
            },
          );
          server.nativeServer.register("ownership", "ResponseFailure", RpcKind.Unary, (call) => {
            call.respond({ body: new Uint8Array(1024) }).then(
              () => responseFailure(new Error("response unexpectedly succeeded")),
              (error) => responseFailure(error),
            );
          });
        },
        async (client) => {
          const stream = await client.nativeClient.startStream({
            service: "ownership",
            method: "Exhaust",
            kind: RpcKind.BidirectionalStreaming,
            body: new Uint8Array(),
          });
          await settlesWithin(exhaustStartedPromise, 3_000);
          await assert.rejects(
            stream.sendMessage(new Uint8Array(1024)),
            (error) => error.nativeCode === -1004,
          );
          stream.close();
          await settlesWithin(exhaustClosedPromise, 3_000);

          const failedResponseCall = client.nativeClient
            .call({
              service: "ownership",
              method: "ResponseFailure",
              kind: RpcKind.Unary,
              version: 1,
              body: new Uint8Array(),
            })
            .catch((error) => error);
          const responseError = await settlesWithin(responseFailurePromise, 3_000);
          assert.equal(responseError.nativeCode, -1004);
          client.close();
          await settlesWithin(failedResponseCall, 5_000);
        },
        () => ({ maxPendingSendBytes: 128 }),
        () => ({ maxPendingSendBytes: 128 }),
      );
    },
  );

  test(
    "native call ownership coordinates overlapping sends, terminal completion, receive, and close",
    { skip: native == null, timeout: 20_000 },
    async () => {
      const outcomes = new Map();
      const outcomeWaiters = new Map();
      const outcome = (name) =>
        new Promise((resolve) => {
          outcomeWaiters.set(name, resolve);
        });
      const record = (name, value) => {
        outcomes.set(name, value);
        outcomeWaiters.get(name)?.(value);
      };

      await withNativePair(
        async (server) => {
          server.nativeServer.register(
            "ownership",
            "SendSend",
            RpcKind.BidirectionalStreaming,
            (call) => {
              Promise.allSettled([
                call.sendMessage(new Uint8Array([1])),
                call.sendMessage(new Uint8Array([2])),
              ]).then(async (results) => {
                const finish = await Promise.allSettled([call.finishStream(Code.Ok)]);
                record("send-send", { results, finish });
              });
            },
          );
          server.nativeServer.register(
            "ownership",
            "SendFinish",
            RpcKind.BidirectionalStreaming,
            (call) => {
              Promise.allSettled([
                call.sendMessage(new Uint8Array(2 * 1024 * 1024)),
                call.finishStream(Code.Ok),
              ]).then((results) => record("send-finish", results));
            },
          );
          server.nativeServer.register(
            "ownership",
            "RecvFinish",
            RpcKind.BidirectionalStreaming,
            (call) => {
              Promise.allSettled([call.recv(), call.finishStream(Code.Ok)]).then((results) =>
                record("recv-finish", results),
              );
            },
          );
          server.nativeServer.register(
            "ownership",
            "Close",
            RpcKind.BidirectionalStreaming,
            (call) => {
              const pending = [call.recv(), call.sendMessage(new Uint8Array(2 * 1024 * 1024))];
              call.close();
              Promise.allSettled(pending).then((results) => record("close", results));
            },
          );
        },
        async (client) => {
          for (const method of ["SendSend", "SendFinish", "RecvFinish", "Close"]) {
            const wait = outcome(
              method.replace(/([A-Z])/gu, (match, letter, offset) =>
                offset === 0 ? letter.toLowerCase() : `-${letter.toLowerCase()}`,
              ),
            );
            const stream = await client.nativeClient.startStream({
              service: "ownership",
              method,
              kind: RpcKind.BidirectionalStreaming,
              body: new Uint8Array(),
            });
            await stream.finishSend().catch(() => {});
            for (;;) {
              const frame = await stream.recv().catch(() => null);
              if (frame == null || frame.kind === RpcStreamFrameKind.Status) {
                break;
              }
            }
            stream.close();
            const result = await settlesWithin(wait, 5_000);
            assert.ok(Array.isArray(result) || result.finish[0].status === "fulfilled");
          }
          assert.equal(
            outcomes.get("send-send").results.filter((result) => result.status === "fulfilled")
              .length,
            2,
          );
          assert.equal(outcomes.get("send-finish")[1].status, "fulfilled");
          assert.equal(outcomes.get("recv-finish")[1].status, "fulfilled");
          assert.equal(outcomes.get("close").length, 2);
        },
      );
    },
  );

  test(
    "rejected double-terminal work cannot abandon the terminal operation owner",
    { skip: !nativeTestHooks, timeout: 15_000 },
    async () => {
      let terminalOutcome;
      const terminalOutcomePromise = new Promise((resolve) => {
        terminalOutcome = resolve;
      });
      let clientReady;
      const clientReadyPromise = new Promise((resolve) => {
        clientReady = resolve;
      });
      await withNativePair(
        async (server) => {
          server.nativeServer.register(
            "ownership",
            "DoubleTerminal",
            RpcKind.BidirectionalStreaming,
            (call) => {
              void (async () => {
                try {
                  await clientReadyPromise;
                  call._debugArmOutboundGate();
                  const heldSend = call.sendMessage(new Uint8Array([7]));
                  await waitForCondition(
                    () => call._debugOutboundGateReached(),
                    "send never reached the outbound gate",
                  );
                  const firstTerminal = call.finishStream(Code.Ok);
                  const secondTerminal = call.finishStream(Code.Ok);
                  call._debugReleaseOutboundGate();
                  const results = await settlesWithin(
                    Promise.allSettled([heldSend, firstTerminal, secondTerminal]),
                    5_000,
                  );
                  terminalOutcome({
                    send: results[0],
                    terminals: results.slice(1),
                  });
                } catch (error) {
                  terminalOutcome(error);
                } finally {
                  try {
                    call._debugReleaseOutboundGate?.();
                  } finally {
                    call.close();
                  }
                }
              })();
            },
          );
        },
        async (client) => {
          let stream;
          try {
            stream = await client.nativeClient.startStream({
              service: "ownership",
              method: "DoubleTerminal",
              kind: RpcKind.BidirectionalStreaming,
              body: new Uint8Array(),
            });
            await stream.finishSend();
            clientReady();
            const outcome = await settlesWithin(terminalOutcomePromise, 5_000);
            if (outcome instanceof Error) {
              throw outcome;
            }
            const frames = [];
            for (;;) {
              const frame = await settlesWithin(stream.recv(), 5_000);
              if (frame == null) {
                break;
              }
              frames.push(frame);
              if (frame.kind === RpcStreamFrameKind.Status) {
                break;
              }
            }
            assert.equal(outcome.send.status, "fulfilled");
            assert.equal(
              outcome.terminals.filter((result) => result.status === "fulfilled").length,
              1,
            );
            assert.equal(
              outcome.terminals.filter((result) => result.status === "rejected").length,
              1,
            );
            assert.deepEqual(
              frames.map((frame) => frame.kind),
              [RpcStreamFrameKind.Message, RpcStreamFrameKind.Status],
            );
          } finally {
            clientReady();
            stream?.close();
          }
        },
      );
    },
  );

  test(
    "outbound FIFO preserves body order before finish",
    { skip: !nativeTestHooks, timeout: 30_000 },
    async () => {
      let requestQueuedResolve;
      const requestQueued = new Promise((resolve) => {
        requestQueuedResolve = resolve;
      });
      let requestResultResolve;
      const requestResult = new Promise((resolve) => {
        requestResultResolve = resolve;
      });
      let requestWritesSettledResolve;
      const requestWritesSettled = new Promise((resolve) => {
        requestWritesSettledResolve = resolve;
      });
      let responseClientReadyResolve;
      const responseClientReady = new Promise((resolve) => {
        responseClientReadyResolve = resolve;
      });
      let responseQueuedResolve;
      const responseQueued = new Promise((resolve) => {
        responseQueuedResolve = resolve;
      });
      let responseResultResolve;
      const responseResult = new Promise((resolve) => {
        responseResultResolve = resolve;
      });

      await withNativePair(
        async (server) => {
          server.nativeServer.register(
            "ownership",
            "RequestOrder",
            RpcKind.BidirectionalStreaming,
            (call) => {
              void (async () => {
                try {
                  await requestQueued;
                  const received = [];
                  for (;;) {
                    const frame = await call.recv();
                    if (frame == null) {
                      break;
                    }
                    received.push({ first: frame.body[0], length: frame.body.byteLength });
                  }
                  await requestWritesSettled;
                  await call.finishStream(Code.Ok);
                  requestResultResolve(received);
                } catch (error) {
                  requestResultResolve(error);
                } finally {
                  call.close();
                }
              })();
            },
          );
          server.nativeServer.register(
            "ownership",
            "ResponseOrder",
            RpcKind.BidirectionalStreaming,
            (call) => {
              void (async () => {
                try {
                  await responseClientReady;
                  call._debugArmOutboundGate();
                  const held = call.sendMessage(new Uint8Array([9]));
                  await waitForCondition(
                    () => call._debugOutboundGateReached(),
                    "response send never reached the outbound gate",
                  );
                  const later = [
                    call.sendMessage(new Uint8Array([1])),
                    call.sendMessages([new Uint8Array([2]), new Uint8Array([3])]),
                    call.finishStream(Code.Ok),
                  ];
                  responseQueuedResolve(null);
                  call._debugReleaseOutboundGate();
                  responseResultResolve(
                    await settlesWithin(Promise.allSettled([held, ...later]), 5_000),
                  );
                } catch (error) {
                  responseQueuedResolve(error);
                  responseResultResolve(error);
                } finally {
                  try {
                    call._debugReleaseOutboundGate?.();
                  } finally {
                    call.close();
                  }
                }
              })();
            },
          );
        },
        async (client) => {
          let requestStream;
          try {
            requestStream = await client.nativeClient.startStream({
              service: "ownership",
              method: "RequestOrder",
              kind: RpcKind.BidirectionalStreaming,
              body: new Uint8Array(),
            });
            requestStream._debugArmOutboundGate();
            const held = requestStream.sendMessage(new Uint8Array([9]));
            await waitForCondition(
              () => requestStream._debugOutboundGateReached(),
              "request send never reached the outbound gate",
            );
            const later = [
              requestStream.sendMessage(new Uint8Array([1])),
              requestStream.sendMessages([new Uint8Array([2]), new Uint8Array([3])]),
              requestStream.finishSend(),
            ];
            requestQueuedResolve();
            requestStream._debugReleaseOutboundGate();
            const requestResults = await settlesWithin(Promise.allSettled([held, ...later]), 5_000);
            requestWritesSettledResolve();
            assert.equal(
              requestResults.filter((result) => result.status === "fulfilled").length,
              requestResults.length,
            );
            const received = await settlesWithin(requestResult, 5_000);
            if (received instanceof Error) {
              throw received;
            }
            assert.deepEqual(received, [
              { first: 9, length: 1 },
              { first: 1, length: 1 },
              { first: 2, length: 1 },
              { first: 3, length: 1 },
            ]);
            const requestStatus = await settlesWithin(requestStream.recv(), 5_000);
            assert.equal(requestStatus.kind, RpcStreamFrameKind.Status);
          } finally {
            requestQueuedResolve();
            requestWritesSettledResolve();
            try {
              requestStream?._debugReleaseOutboundGate?.();
            } finally {
              requestStream?.close();
            }
          }

          let responseStream;
          try {
            responseStream = await client.nativeClient.startStream({
              service: "ownership",
              method: "ResponseOrder",
              kind: RpcKind.BidirectionalStreaming,
              body: new Uint8Array(),
            });
            await responseStream.finishSend();
            responseClientReadyResolve();
            const queued = await settlesWithin(responseQueued, 5_000);
            if (queued instanceof Error) {
              throw queued;
            }
            const responseBodies = [];
            let responseStatus = null;
            for (;;) {
              const frame = await settlesWithin(responseStream.recv(), 5_000);
              if (frame == null) {
                break;
              }
              if (frame.kind === RpcStreamFrameKind.Status) {
                responseStatus = frame;
                break;
              }
              responseBodies.push({ first: frame.body[0], length: frame.body.byteLength });
            }
            assert.deepEqual(responseBodies, [
              { first: 9, length: 1 },
              { first: 1, length: 1 },
              { first: 2, length: 1 },
              { first: 3, length: 1 },
            ]);
            assert.equal(responseStatus.status, Code.Ok);
            const responseResults = await settlesWithin(responseResult, 5_000);
            if (responseResults instanceof Error) {
              throw responseResults;
            }
            assert.equal(
              responseResults.filter((result) => result.status === "fulfilled").length,
              responseResults.length,
            );
          } finally {
            responseClientReadyResolve();
            responseStream?.close();
          }
        },
      );
    },
  );

  test(
    "outbound FIFO advances after submission failure",
    { skip: !nativeTestHooks, timeout: 15_000 },
    async () => {
      let receivedResolve;
      const received = new Promise((resolve) => {
        receivedResolve = resolve;
      });
      await withNativePair(
        async (server) => {
          server.nativeServer.register(
            "ownership",
            "FailureOrder",
            RpcKind.BidirectionalStreaming,
            (call) => {
              void (async () => {
                const bodies = [];
                for (;;) {
                  const frame = await call.recv();
                  if (frame == null) {
                    break;
                  }
                  bodies.push(Array.from(frame.body));
                }
                await call.finishStream(Code.Ok);
                receivedResolve(bodies);
              })().catch(receivedResolve);
            },
          );
        },
        async (client) => {
          let stream;
          try {
            stream = await client.nativeClient.startStream({
              service: "ownership",
              method: "FailureOrder",
              kind: RpcKind.BidirectionalStreaming,
              body: new Uint8Array(),
            });
            stream._debugArmOutboundGate();
            const failed = stream.sendMessage(new Uint8Array(1024));
            await waitForCondition(
              () => stream._debugOutboundGateReached(),
              "failing send never reached the outbound gate",
            );
            const retry = stream.sendMessage(new Uint8Array([7]));
            const finish = stream.finishSend();
            stream._debugReleaseOutboundGate();
            const results = await settlesWithin(Promise.allSettled([failed, retry, finish]), 5_000);
            assert.equal(results[0].status, "rejected");
            assert.equal(results[0].reason.nativeCode, -1004);
            assert.equal(results[1].status, "fulfilled");
            assert.equal(results[2].status, "fulfilled");
            const bodies = await settlesWithin(received, 5_000);
            if (bodies instanceof Error) {
              throw bodies;
            }
            assert.deepEqual(bodies, [[7]]);
            const status = await settlesWithin(stream.recv(), 5_000);
            assert.equal(status.kind, RpcStreamFrameKind.Status);
          } finally {
            try {
              stream?._debugReleaseOutboundGate?.();
            } finally {
              stream?.close();
            }
          }
        },
        () => ({}),
        () => ({ maxPendingSendBytes: 128 }),
      );
    },
  );

  test(
    "native pending stream reset cancels a batched send",
    { skip: native == null, timeout: 15_000 },
    async () => {
      let resetStarted;
      const resetStartedPromise = new Promise((resolve) => {
        resetStarted = resolve;
      });
      let resetFinished;
      const resetFinishedPromise = new Promise((resolve) => {
        resetFinished = resolve;
      });
      await withNativePair(
        async (server) => {
          server.register("ownership", "Reset", RpcKind.BidirectionalStreaming, async (call) => {
            resetStarted();
            await delay(250);
            call.close();
            resetFinished();
          });
        },
        async (client) => {
          const stream = await client.nativeClient.startStream({
            service: "ownership",
            method: "Reset",
            kind: RpcKind.BidirectionalStreaming,
            body: new Uint8Array(),
          });
          const bodies = Array.from({ length: 6 }, (_, index) =>
            new Uint8Array(3 * 1024 * 1024).fill(index + 1),
          );
          const sendResult = stream.sendMessages(bodies).then(
            () => null,
            (error) => error,
          );
          await resetStartedPromise;
          stream.close();
          const sendError = await settlesWithin(sendResult, 5_000);
          assert.equal(sendError.nativeCode, -osConstants.errno.ECANCELED);
          await assert.rejects(
            stream.sendMessage(new Uint8Array([7])),
            (error) => error.nativeCode === -4001,
          );
          await resetFinishedPromise;
        },
      );
    },
  );

  test(
    "serialized native operations include queue wait in stream idle timeouts",
    { skip: native == null, timeout: 15_000 },
    async () => {
      let timeoutObserved;
      const timeoutObservedPromise = new Promise((resolve) => {
        timeoutObserved = resolve;
      });
      await withNativePair(
        async (server) => {
          server.nativeServer.register(
            "ownership",
            "QueuedTimeout",
            RpcKind.BidirectionalStreaming,
            (call) => {
              const pendingSend = call
                .sendMessages(Array.from({ length: 12 }, () => new Uint8Array(3 * 1024 * 1024)))
                .catch(() => {});
              setTimeout(async () => {
                const started = performance.now();
                try {
                  await call.recv();
                  timeoutObserved(new Error("receive unexpectedly completed"));
                } catch (error) {
                  timeoutObserved({ elapsedMs: performance.now() - started, error });
                } finally {
                  call.close();
                  await pendingSend;
                }
              }, 5);
            },
          );
        },
        async (client) => {
          const stream = await client.nativeClient.startStream({
            service: "ownership",
            method: "QueuedTimeout",
            kind: RpcKind.BidirectionalStreaming,
            body: new Uint8Array(),
          });
          const observed = await settlesWithin(timeoutObservedPromise, 3_000);
          assert.equal(observed.error.nativeCode, -2007);
          assert.ok(observed.elapsedMs >= 15, `timeout fired too early: ${observed.elapsedMs}ms`);
          assert.ok(observed.elapsedMs < 1_000, `timeout fired too late: ${observed.elapsedMs}ms`);
          stream.close();
        },
        () => ({ streamIdleTimeoutMs: 25 }),
      );
    },
  );

  test(
    "native completion polling prevents duplex worker starvation",
    { skip: native == null, timeout: 10_000 },
    async () => {
      await withNativePair(
        async (server) => {
          server.register(
            "ownership",
            "ConcurrentBidi",
            RpcKind.BidirectionalStreaming,
            async (call) => {
              const frame = await call.recv();
              if (frame?.kind === RpcStreamFrameKind.Message) {
                await call.sendMessage(frame.body);
              }
              await call.finishStream(Code.Ok);
            },
          );
        },
        async (client) => {
          const concurrency = 64;
          const streams = await Promise.all(
            Array.from({ length: concurrency }, () =>
              client.nativeClient.startStream({
                service: "ownership",
                method: "ConcurrentBidi",
                kind: RpcKind.BidirectionalStreaming,
                body: new Uint8Array(),
              }),
            ),
          );
          try {
            const receives = streams.map((stream) => stream.recv());
            await delay(25);
            await settlesWithin(
              Promise.all(
                streams.map((stream, index) => stream.sendMessage(new Uint8Array([index + 1]))),
              ),
              5_000,
            );
            await Promise.all(streams.map((stream) => stream.finishSend()));
            const frames = await settlesWithin(Promise.all(receives), 5_000);
            assert.deepEqual(
              frames.map((frame) => Array.from(frame.body)),
              Array.from({ length: concurrency }, (_, index) => [index + 1]),
            );
          } finally {
            for (const stream of streams) {
              stream.close();
            }
          }
        },
      );
    },
  );

  test(
    "scheduled copy operations sustain duplex contention without EAGAIN",
    { skip: native == null, timeout: 20_000 },
    async () => {
      const concurrency = 24;
      const rounds = 6;
      await withNativePair(
        async (server) => {
          server.nativeServer.register(
            "ownership",
            "CopyContention",
            RpcKind.BidirectionalStreaming,
            (call) => {
              void (async () => {
                for (let round = 0; round < rounds; round += 1) {
                  const requests = await Promise.all([call.recv(), call.recv(), call.recv()]);
                  await Promise.all([
                    call.sendMessage(requests[0].body),
                    call.sendMessages([requests[1].body, requests[2].body]),
                  ]);
                }
                await call.finishStream(Code.Ok);
              })().catch(() => call.close());
            },
          );
        },
        async (client) => {
          const streams = await Promise.all(
            Array.from({ length: concurrency }, () =>
              client.nativeClient.startStream({
                service: "ownership",
                method: "CopyContention",
                kind: RpcKind.BidirectionalStreaming,
                body: new Uint8Array(),
              }),
            ),
          );
          try {
            await settlesWithin(
              Promise.all(
                streams.map(async (stream, streamIndex) => {
                  const responses = Array.from({ length: rounds * 3 }, () => stream.recv());
                  const sends = [];
                  for (let round = 0; round < rounds; round += 1) {
                    const value = new Uint8Array([streamIndex, round]);
                    sends.push(stream.sendMessage(value));
                    sends.push(stream.sendMessages([value, value]));
                  }
                  await Promise.all(sends);
                  await stream.finishSend();
                  const frames = await Promise.all(responses);
                  assert.equal(
                    frames.filter((frame) => frame?.kind === RpcStreamFrameKind.Message).length,
                    rounds * 3,
                  );
                  const status = await stream.recv();
                  assert.equal(status.kind, RpcStreamFrameKind.Status);
                }),
              ),
              10_000,
            );
          } finally {
            for (const stream of streams) {
              stream.close();
            }
          }
        },
      );
    },
  );

  test(
    "native connection close cancels queued work and settles an acquired send",
    { skip: !nativeTestHooks, timeout: 20_000 },
    async () => {
      let heldCall;
      let callStarted;
      const callStartedPromise = new Promise((resolve) => {
        callStarted = resolve;
      });
      await withNativePair(
        async (server) => {
          server.nativeServer.register(
            "ownership",
            "ConnectionClose",
            RpcKind.BidirectionalStreaming,
            (call) => {
              heldCall = call;
              callStarted();
            },
          );
        },
        async (client) => {
          let stream;
          try {
            stream = await client.nativeClient.startStream({
              service: "ownership",
              method: "ConnectionClose",
              kind: RpcKind.BidirectionalStreaming,
              body: new Uint8Array(),
            });
            stream._debugArmOutboundGate();
            const acquired = stream.sendMessage(new Uint8Array([7])).then(
              () => null,
              (error) => error,
            );
            await waitForCondition(
              () => stream._debugOutboundGateReached(),
              "accepted send never reached the outbound gate",
            );
            const queued = stream.sendMessage(new Uint8Array([8])).then(
              () => null,
              (error) => error,
            );
            await callStartedPromise;
            client.close();
            stream._debugReleaseOutboundGate();
            const [acquiredResult, queuedResult] = await settlesWithin(
              Promise.all([acquired, queued]),
              5_000,
            );
            assert.equal(acquiredResult instanceof Error, true);
            assert.equal(acquiredResult.nativeCode, -osConstants.errno.ECANCELED);
            assert.equal(queuedResult instanceof Error, true);
            assert.equal(queuedResult.nativeCode, -osConstants.errno.ECANCELED);
          } finally {
            try {
              stream?._debugReleaseOutboundGate?.();
            } finally {
              stream?.close();
              heldCall?.close();
              heldCall = null;
            }
          }
        },
      );
    },
  );

  test(
    "native evented completion defers close until pending operation releases native ref",
    {
      skip: native == null,
    },
    async () => {
      const before = resourceCloseCount(native);
      const resource = native._debugCreatePendingResource();
      const pending = resource.wait(75);

      await waitForCondition(() => resource.refs() === 1, "pending resource was not acquired");
      resource.close();

      assert.equal(resource.closed(), false);
      await pending;
      assert.equal(resource.closed(), true);
      assert.equal(resourceCloseCount(native), before + 1);
      await assert.rejects(resource.wait(1), (error) => error.nativeCode === -4001);
    },
  );

  test(
    "native evented completion receiver ref prevents GC finalizer while operation is pending",
    {
      skip: native == null,
    },
    async () => {
      await forceGcCycles();
      const before = resourceFinalizerCount(native);
      let resource = native._debugCreatePendingResource();
      const pending = resource.wait(75);

      await waitForCondition(() => resource.refs() === 1, "pending resource was not acquired");
      resource = null;
      await forceGcCycles(5);

      assert.equal(resourceFinalizerCount(native), before);
      await pending;
      await waitForResourceFinalizers(native, before + 1);
    },
  );
}

function finalizerCount(native) {
  return native._debugExternalArrayBufferFinalizers();
}

function ownerReleaseCount(native) {
  return native._debugBodyOwnerReleases();
}

function resourceCloseCount(native) {
  return native._debugPendingResourceCloses();
}

function resourceFinalizerCount(native) {
  return native._debugPendingResourceFinalizers();
}

async function forceGcCycles(cycles = 4) {
  for (let i = 0; i < cycles; i += 1) {
    global.gc();
    await setImmediate();
  }
}

async function waitForFinalizers(native, target) {
  for (let i = 0; i < 50; i += 1) {
    await forceGcCycles(1);
    if (finalizerCount(native) >= target) {
      return;
    }
  }
  assert.fail(
    `expected at least ${target} external ArrayBuffer finalizers, saw ${finalizerCount(native)}`,
  );
}

async function waitForResourceFinalizers(native, target) {
  for (let i = 0; i < 50; i += 1) {
    await forceGcCycles(1);
    if (resourceFinalizerCount(native) >= target) {
      return;
    }
  }
  assert.fail(
    `expected at least ${target} debug resource finalizers, saw ${resourceFinalizerCount(native)}`,
  );
}

async function waitForCondition(predicate, message, timeoutMs = 5_000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    if (predicate()) {
      return;
    }
    if (Date.now() >= deadline) {
      assert.fail(message);
    }
    await delay(1);
  }
}

function bodyBatchIterable(batches) {
  let index = 0;
  return {
    [Symbol.asyncIterator]() {
      return this;
    },
    nextBatch() {
      if (index >= batches.length) {
        return Promise.resolve({ done: true, value: undefined });
      }
      return Promise.resolve({ done: false, value: batches[index++] });
    },
    return() {
      return Promise.resolve({ done: true, value: undefined });
    },
  };
}

async function withNativePair(
  register,
  run,
  serverOptions = () => ({}),
  clientOptions = () => ({}),
) {
  const certificate = await testCertificate();
  const server = await NodeServer.listen({
    host: "127.0.0.1",
    port: 0,
    certFile: certificate.certFile,
    keyFile: certificate.keyFile,
    ...serverOptions(),
  });
  await register(server);
  const serving = server.serve();
  const client = await RawNodeTransport.connect({
    host: "127.0.0.1",
    port: server.port,
    skipCertificateValidation: true,
    ...clientOptions(),
  });
  try {
    await run(client, server);
  } finally {
    server.close();
    client.close();
    await settlesWithin(serving, 5_000);
  }
}

async function testCertificate() {
  if (certificateDirectory == null) {
    certificateDirectory = await mkdtemp(join(tmpdir(), "trevrpc-js-ownership-lifecycle-cert-"));
    const certFile = join(certificateDirectory, "cert.pem");
    const keyFile = join(certificateDirectory, "key.pem");
    const generated = spawnSync(
      "openssl",
      [
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
        keyFile,
        "-out",
        certFile,
      ],
      { encoding: "utf8" },
    );
    assert.equal(generated.status, 0, generated.stderr);
  }
  return {
    certFile: join(certificateDirectory, "cert.pem"),
    keyFile: join(certificateDirectory, "key.pem"),
  };
}

async function settlesWithin(promise, timeoutMs) {
  return await Promise.race([
    promise,
    delay(timeoutMs, undefined, { ref: false }).then(() => {
      throw new Error(`operation did not settle within ${timeoutMs}ms`);
    }),
  ]);
}

function waitForWorkerMessage(worker, predicate) {
  return new Promise((resolve, reject) => {
    const onMessage = (message) => {
      if (!predicate(message)) {
        return;
      }
      cleanup();
      resolve(message);
    };
    const onError = (error) => {
      cleanup();
      reject(error);
    };
    const onExit = (code) => {
      cleanup();
      reject(new Error(`Worker exited before the expected message: ${code}`));
    };
    const cleanup = () => {
      worker.off("message", onMessage);
      worker.off("error", onError);
      worker.off("exit", onExit);
    };
    worker.on("message", onMessage);
    worker.once("error", onError);
    worker.once("exit", onExit);
  });
}
