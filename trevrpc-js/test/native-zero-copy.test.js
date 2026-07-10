import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { after } from "node:test";
import { setImmediate, setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";

import { Code, RpcKind, RpcStreamFrameKind } from "../src/index.js";
import { NodeServer, NodeTransport } from "../src/node.js";

const require = createRequire(import.meta.url);
const nativeAddonPath = join(import.meta.dirname, "..", "build", "native", "trevrpc_native.node");
const thisFile = fileURLToPath(import.meta.url);
const gcChild = process.env.TREVRPC_NATIVE_ZERO_COPY_GC_CHILD === "1";
let certificateDirectory;

if (typeof global.gc !== "function" && !gcChild) {
  test(
    "native zero-copy ownership tests run with forced GC",
    { skip: !existsSync(nativeAddonPath) },
    () => {
      const result = spawnSync(process.execPath, ["--expose-gc", "--test", thisFile], {
        encoding: "utf8",
        env: { ...process.env, TREVRPC_NATIVE_ZERO_COPY_GC_CHILD: "1" },
      });

      assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
    },
  );
} else {
  const native = existsSync(nativeAddonPath) ? require(nativeAddonPath) : null;
  const hasGc = typeof global.gc === "function";
  after(async () => {
    if (certificateDirectory != null) {
      await rm(certificateDirectory, { force: true, recursive: true });
    }
  });

  test(
    "native response external body owner survives C cleanup until GC finalizer",
    {
      skip: native == null || !hasGc,
    },
    async () => {
      await forceGcCycles();
      const before = finalizerCount(native);
      const expected = new Uint8Array([1, 2, 3, 4, 5]);
      let response = native._debugMakeBorrowedResponse(expected);
      let body = response.body;

      assert.deepEqual(Array.from(body), Array.from(expected));
      response = null;
      await forceGcCycles();

      assert.deepEqual(Array.from(body), Array.from(expected));
      assert.equal(finalizerCount(native), before);

      body = null;
      await waitForFinalizers(native, before + 1);
    },
  );

  test(
    "native stream frame external body owner survives C cleanup until GC finalizer",
    {
      skip: native == null || !hasGc,
    },
    async () => {
      await forceGcCycles();
      const before = finalizerCount(native);
      const expected = new Uint8Array([9, 8, 7, 6]);
      let frame = native._debugMakeBorrowedStreamFrame(expected);
      let body = frame.body;

      assert.deepEqual(Array.from(body), Array.from(expected));
      frame = null;
      await forceGcCycles();

      assert.deepEqual(Array.from(body), Array.from(expected));
      assert.equal(finalizerCount(native), before);

      body = null;
      await waitForFinalizers(native, before + 1);
    },
  );

  test(
    "native stream body batches transfer borrowed frame owners",
    {
      skip: native == null || !hasGc,
    },
    async () => {
      await forceGcCycles();
      const before = finalizerCount(native);
      const expected = new Uint8Array([42, 43, 44]);
      let batch = native._debugMakeBorrowedStreamBodyBatch(expected);
      let body = batch.bodies[0];

      assert.deepEqual(Array.from(body), Array.from(expected));
      batch = null;
      await forceGcCycles();

      assert.deepEqual(Array.from(body), Array.from(expected));
      assert.equal(finalizerCount(native), before);

      body = null;
      await waitForFinalizers(native, before + 1);
    },
  );

  test(
    "native outbound body refs survive forced GC until async completion",
    {
      skip: native == null || !hasGc,
    },
    async () => {
      let body = new Uint8Array([17, 34, 51, 68]);
      const expected = checksum(body);
      const retained = native._debugRetainBodyUntilAsyncComplete(body);

      body = null;
      await forceGcCycles(5);

      assert.equal(await retained, expected);
      await waitForOutboundRefs(native, 0);
    },
  );

  test(
    "native zero-copy rejects detached stores without trusting resizable properties",
    { skip: native == null },
    async () => {
      const detached = new ArrayBuffer(8);
      structuredClone(detached, { transfer: [detached] });
      assert.throws(() => native._debugRetainBodyUntilAsyncComplete(detached), /attached/u);

      const spoofed = new ArrayBuffer(4);
      new Uint8Array(spoofed).set([1, 2, 3, 4]);
      Object.defineProperty(spoofed, "resizable", { value: true });
      assert.equal(
        await native._debugRetainBodyUntilAsyncComplete(spoofed),
        checksum(new Uint8Array(spoofed)),
      );

      const resizable = new ArrayBuffer(8, { maxByteLength: 16 });
      if (resizable.resizable === true) {
        assert.equal(await native._debugRetainBodyUntilAsyncComplete(resizable), 0);
      }
    },
  );

  test(
    "native completion worker releases retained bodies during environment shutdown",
    { skip: native == null || !hasGc },
    () => {
      const script = `
import { Worker } from "node:worker_threads";
const worker = new Worker(
  ${JSON.stringify(`
const { parentPort } = require("node:worker_threads");
const native = require(${JSON.stringify(nativeAddonPath)});
native._debugRetainBodyUntilAsyncComplete(new Uint8Array(1024 * 1024));
parentPort.postMessage("queued");
`)},
  { eval: true, execArgv: [] },
);
await new Promise((resolve, reject) => {
  worker.once("message", resolve);
  worker.once("error", reject);
});
await worker.terminate();
`;
      const result = spawnSync(
        process.execPath,
        ["--expose-gc", "--input-type=module", "-e", script],
        {
          encoding: "utf8",
          timeout: 5_000,
        },
      );
      assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
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
        await settlesWithin(worker.terminate(), 2_000);
      } finally {
        sink.close();
      }
    },
  );

  test(
    "Worker termination cancels a real stalled zero-copy unary call",
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
  client._callZeroCopy({
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
    "Worker termination cancels a pending real MsQuic zero-copy send",
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
  const stream = await client._startStreamZeroCopy({
    service: "ownership",
    method: "WorkerPendingSend",
    kind: 3,
    version: 1,
    body: new Uint8Array(),
  });
  const bodies = Array.from({ length: 12 }, () => new Uint8Array(3 * 1024 * 1024));
  stream._sendMessagesZeroCopy(bodies).catch(() => {});
  stream.sendMessage(new Uint8Array([1])).catch(() => {});
  stream.finishSend().catch(() => {});
  parentPort.postMessage({ refs: native._debugRetainedOutboundBodyRefs() });
})().catch((error) => { throw error; });
`,
            { eval: true },
          );
          const message = await waitForWorkerMessage(worker, (value) => value?.refs > 0);
          assert.equal(message.refs, 12);
          await settlesWithin(callStartedPromise, 3_000);
          await settlesWithin(worker.terminate(), 5_000);
          heldCall?.close();
          heldCall = null;
        },
      );
    },
  );

  test(
    "native zero-copy unary retains typed slices and ArrayBuffers with metadata prefixes",
    { skip: native == null || !hasGc, timeout: 15_000 },
    async () => {
      const baseline = outboundRefCount(native);
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
          let storage = new Uint8Array([0xff, 1, 2, 3, 4, 0xff]);
          let body = storage.subarray(1, 5);
          let metadataBuffer = new Uint8Array([6, 7, 8]).buffer;
          const responsePromise = client.call(
            {
              service: "ownership",
              method: "Unary",
              body,
              metadata: { authorization: metadataBuffer },
            },
            { outboundZeroCopy: true },
          );

          await unaryStartedPromise;
          body = null;
          storage = null;
          metadataBuffer = null;
          await forceGcCycles(5);
          assert.equal(outboundRefCount(native), baseline + 1);

          releaseUnary();
          const response = await responsePromise;
          assert.deepEqual(Array.from(response.body), [1, 2, 3, 4]);
          assert.deepEqual(Array.from(response.metadata.response), [9, 8, 7]);
          await waitForOutboundRefs(native, baseline);

          let arrayBuffer = new Uint8Array([10, 11, 12]).buffer;
          const second = client.call(
            {
              service: "ownership",
              method: "Unary",
              body: arrayBuffer,
              metadata: { authorization: new Uint8Array([6, 7, 8]) },
            },
            { outboundZeroCopy: true },
          );
          arrayBuffer = null;
          await forceGcCycles(3);
          const secondResponse = await second;
          assert.deepEqual(Array.from(secondResponse.body), [10, 11, 12]);
          await waitForOutboundRefs(native, baseline);
        },
        () => ({ outboundZeroCopy: true }),
      );

      assert.deepEqual(observed, [
        { body: [1, 2, 3, 4], metadata: [6, 7, 8] },
        { body: [10, 11, 12], metadata: [6, 7, 8] },
      ]);
    },
  );

  test(
    "native zero-copy single and batched stream messages survive forced GC",
    { skip: native == null || !hasGc, timeout: 15_000 },
    async () => {
      const baseline = outboundRefCount(native);
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
          let sliceStorage = new Uint8Array([0xff, 1, 2, 0xff]);
          let slice = sliceStorage.subarray(1, 3);
          let arrayBuffer = new Uint8Array([3, 4, 5]).buffer;
          let finalBody = new Uint8Array([6]);
          const frames = await client.streamingCall(
            {
              service: "ownership",
              method: "Bidi",
              kind: RpcKind.BidirectionalStreaming,
              body: new Uint8Array(),
              metadata: { trace: new Uint8Array([42]) },
            },
            bodyBatchIterable([[slice], [arrayBuffer, finalBody]]),
            { outboundZeroCopy: true },
          );
          slice = null;
          sliceStorage = null;
          arrayBuffer = null;
          finalBody = null;
          await forceGcCycles(5);

          const received = [];
          for await (const frame of frames) {
            if (frame.kind === RpcStreamFrameKind.Message) {
              received.push(Array.from(frame.body));
            }
          }
          assert.deepEqual(received, [[1, 2], [3, 4, 5], [6]]);
          await waitForOutboundRefs(native, baseline);
        },
        () => ({ outboundZeroCopy: true }),
      );
    },
  );

  test(
    "native zero-copy sync exhaustion and response failure release refs exactly once",
    { skip: native == null || !hasGc, timeout: 15_000 },
    async () => {
      const baseline = outboundRefCount(native);
      let responseFailure;
      const responseFailurePromise = new Promise((resolve) => {
        responseFailure = resolve;
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
              setTimeout(() => {
                call.close();
                exhaustClosed();
              }, 50);
            },
          );
          server.nativeServer.register("ownership", "ResponseFailure", RpcKind.Unary, (call) => {
            call._respondZeroCopy({ body: new Uint8Array(1024) }).then(
              () => responseFailure(new Error("response unexpectedly succeeded")),
              (error) => responseFailure(error),
            );
          });
        },
        async (client) => {
          const stream = await client.nativeClient._startStreamZeroCopy({
            service: "ownership",
            method: "Exhaust",
            kind: RpcKind.BidirectionalStreaming,
            body: new Uint8Array(),
          });
          await assert.rejects(
            stream._sendMessageZeroCopy(new Uint8Array(1024)),
            (error) => error.nativeCode === -1004,
          );
          stream.close();
          await waitForOutboundRefs(native, baseline);
          await exhaustClosedPromise;

          const failedResponseCall = client.nativeClient
            ._callZeroCopy({
              service: "ownership",
              method: "ResponseFailure",
              kind: RpcKind.Unary,
              version: 1,
              body: new Uint8Array(),
            })
            .catch((error) => error);
          const responseError = await settlesWithin(responseFailurePromise, 3_000);
          assert.equal(responseError.nativeCode, -1004);
          await waitForOutboundRefs(native, baseline);
          client.close();
          await failedResponseCall;
        },
        () => ({ outboundZeroCopy: true, maxPendingSendBytes: 128 }),
        () => ({ maxPendingSendBytes: 128 }),
      );
    },
  );

  test(
    "native call ownership coordinates overlapping sends, terminal completion, receive, and close",
    { skip: native == null || !hasGc, timeout: 20_000 },
    async () => {
      const baseline = outboundRefCount(native);
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
                call._sendMessageZeroCopy(new Uint8Array([1])),
                call._sendMessageZeroCopy(new Uint8Array([2])),
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
                call._sendMessageZeroCopy(new Uint8Array(2 * 1024 * 1024)),
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
              const pending = [
                call.recv(),
                call._sendMessageZeroCopy(new Uint8Array(2 * 1024 * 1024)),
              ];
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
            const stream = await client.nativeClient._startStreamZeroCopy({
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
          await waitForOutboundRefs(native, baseline);
        },
        () => ({ outboundZeroCopy: true }),
      );
    },
  );

  test(
    "rejected double-terminal work cannot abandon the terminal operation owner",
    { skip: native == null, timeout: 15_000 },
    async () => {
      let terminalOutcome;
      const terminalOutcomePromise = new Promise((resolve) => {
        terminalOutcome = resolve;
      });
      await withNativePair(
        async (server) => {
          server.nativeServer.register(
            "ownership",
            "DoubleTerminal",
            RpcKind.BidirectionalStreaming,
            (call) => {
              const heldSend = call._sendMessageZeroCopy(new Uint8Array(3 * 1024 * 1024)).then(
                () => ({ status: "fulfilled" }),
                (error) => ({ status: "rejected", error }),
              );
              void (async () => {
                const deadline = Date.now() + 3_000;
                while (!call._debugOperationLocked()) {
                  if (Date.now() >= deadline) {
                    throw new Error("send never acquired the call operation mutex");
                  }
                  await setImmediate();
                }
                const terminals = await Promise.allSettled([
                  call.finishStream(Code.Ok),
                  call.finishStream(Code.Ok),
                ]);
                terminalOutcome({ heldSend: await heldSend, terminals });
              })().catch(terminalOutcome);
            },
          );
        },
        async (client) => {
          const stream = await client.nativeClient.startStream({
            service: "ownership",
            method: "DoubleTerminal",
            kind: RpcKind.BidirectionalStreaming,
            body: new Uint8Array(),
          });
          await stream.finishSend();
          await delay(50);
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
          const outcome = await settlesWithin(terminalOutcomePromise, 5_000);
          assert.equal(outcome.heldSend.status, "fulfilled");
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
          stream.close();
        },
        () => ({ outboundZeroCopy: true }),
      );
    },
  );

  test(
    "outbound FIFO preserves copy and zero-copy body order before finish",
    { skip: native == null, timeout: 30_000 },
    async () => {
      const baseline = outboundRefCount(native);
      const states = new Map();
      for (const mode of ["copy", "zero-copy"]) {
        let requestQueued;
        let requestResult;
        let responseQueued;
        let responseResult;
        states.set(mode, {
          requestQueued: new Promise((resolve) => {
            requestQueued = resolve;
          }),
          requestQueuedResolve: requestQueued,
          requestResult: new Promise((resolve) => {
            requestResult = resolve;
          }),
          requestResultResolve: requestResult,
          responseQueued: new Promise((resolve) => {
            responseQueued = resolve;
          }),
          responseQueuedResolve: responseQueued,
          responseResult: new Promise((resolve) => {
            responseResult = resolve;
          }),
          responseResultResolve: responseResult,
        });
      }

      await withNativePair(
        async (server) => {
          for (const mode of ["copy", "zero-copy"]) {
            const state = states.get(mode);
            const suffix = mode === "copy" ? "Copy" : "ZeroCopy";
            server.nativeServer.register(
              "ownership",
              `RequestOrder${suffix}`,
              RpcKind.BidirectionalStreaming,
              (call) => {
                void (async () => {
                  await state.requestQueued;
                  const received = [];
                  for (;;) {
                    const frame = await call.recv();
                    if (frame == null) {
                      break;
                    }
                    received.push({ first: frame.body[0], length: frame.body.byteLength });
                  }
                  await call.finishStream(Code.Ok);
                  state.requestResultResolve(received);
                })().catch(state.requestResultResolve);
              },
            );
            server.nativeServer.register(
              "ownership",
              `ResponseOrder${suffix}`,
              RpcKind.BidirectionalStreaming,
              (call) => {
                const held = call._sendMessageZeroCopy(new Uint8Array(3 * 1024 * 1024).fill(9));
                void (async () => {
                  await waitForCondition(
                    () => call._debugOperationLocked(),
                    "response send never held the call operation mutex",
                  );
                  const later =
                    mode === "copy"
                      ? [
                          call.sendMessage(new Uint8Array([1])),
                          call.sendMessages([new Uint8Array([2]), new Uint8Array([3])]),
                          call.finishStream(Code.Ok),
                        ]
                      : [
                          call._sendMessageZeroCopy(new Uint8Array([1])),
                          call._sendMessagesZeroCopy([new Uint8Array([2]), new Uint8Array([3])]),
                          call.finishStream(Code.Ok),
                        ];
                  state.responseQueuedResolve();
                  state.responseResultResolve(await Promise.allSettled([held, ...later]));
                })().catch(state.responseResultResolve);
              },
            );
          }
        },
        async (client) => {
          for (const mode of ["copy", "zero-copy"]) {
            const state = states.get(mode);
            const suffix = mode === "copy" ? "Copy" : "ZeroCopy";
            const requestStream = await client.nativeClient.startStream({
              service: "ownership",
              method: `RequestOrder${suffix}`,
              kind: RpcKind.BidirectionalStreaming,
              body: new Uint8Array(),
            });
            const held = requestStream._sendMessageZeroCopy(
              new Uint8Array(3 * 1024 * 1024).fill(9),
            );
            await waitForCondition(
              () => requestStream._debugOperationLocked(),
              "request send never held the stream operation mutex",
            );
            const later =
              mode === "copy"
                ? [
                    requestStream.sendMessage(new Uint8Array([1])),
                    requestStream.sendMessages([new Uint8Array([2]), new Uint8Array([3])]),
                    requestStream.finishSend(),
                  ]
                : [
                    requestStream._sendMessageZeroCopy(new Uint8Array([1])),
                    requestStream._sendMessagesZeroCopy([new Uint8Array([2]), new Uint8Array([3])]),
                    requestStream.finishSend(),
                  ];
            state.requestQueuedResolve();
            const requestResults = await Promise.allSettled([held, ...later]);
            assert.equal(
              requestResults.filter((result) => result.status === "fulfilled").length,
              requestResults.length,
            );
            assert.deepEqual(await settlesWithin(state.requestResult, 5_000), [
              { first: 9, length: 3 * 1024 * 1024 },
              { first: 1, length: 1 },
              { first: 2, length: 1 },
              { first: 3, length: 1 },
            ]);
            const requestStatus = await settlesWithin(requestStream.recv(), 5_000);
            assert.equal(requestStatus.kind, RpcStreamFrameKind.Status);
            requestStream.close();

            const responseStream = await client.nativeClient.startStream({
              service: "ownership",
              method: `ResponseOrder${suffix}`,
              kind: RpcKind.BidirectionalStreaming,
              body: new Uint8Array(),
            });
            await responseStream.finishSend();
            await settlesWithin(state.responseQueued, 5_000);
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
              { first: 9, length: 3 * 1024 * 1024 },
              { first: 1, length: 1 },
              { first: 2, length: 1 },
              { first: 3, length: 1 },
            ]);
            assert.equal(responseStatus.status, Code.Ok);
            const responseResults = await settlesWithin(state.responseResult, 5_000);
            assert.equal(
              responseResults.filter((result) => result.status === "fulfilled").length,
              responseResults.length,
            );
            responseStream.close();
          }
          await waitForOutboundRefs(native, baseline);
        },
        () => ({ outboundZeroCopy: true }),
      );
    },
  );

  test(
    "outbound FIFO advances after copy and zero-copy submission failure",
    { skip: native == null, timeout: 15_000 },
    async () => {
      const received = new Map();
      const waiters = new Map();
      for (const mode of ["copy", "zero-copy"]) {
        let resolve;
        waiters.set(
          mode,
          new Promise((done) => {
            resolve = done;
          }),
        );
        received.set(mode, resolve);
      }
      await withNativePair(
        async (server) => {
          for (const mode of ["copy", "zero-copy"]) {
            const suffix = mode === "copy" ? "Copy" : "ZeroCopy";
            server.nativeServer.register(
              "ownership",
              `FailureOrder${suffix}`,
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
                  received.get(mode)(bodies);
                })().catch(received.get(mode));
              },
            );
          }
        },
        async (client) => {
          for (const mode of ["copy", "zero-copy"]) {
            const suffix = mode === "copy" ? "Copy" : "ZeroCopy";
            const stream = await client.nativeClient.startStream({
              service: "ownership",
              method: `FailureOrder${suffix}`,
              kind: RpcKind.BidirectionalStreaming,
              body: new Uint8Array(),
            });
            const failed =
              mode === "copy"
                ? stream.sendMessage(new Uint8Array(1024))
                : stream._sendMessageZeroCopy(new Uint8Array(1024));
            const retry =
              mode === "copy"
                ? stream.sendMessage(new Uint8Array([7]))
                : stream._sendMessageZeroCopy(new Uint8Array([7]));
            const results = await Promise.allSettled([failed, retry, stream.finishSend()]);
            assert.equal(results[0].status, "rejected");
            assert.equal(results[0].reason.nativeCode, -1004);
            assert.equal(results[1].status, "fulfilled");
            assert.equal(results[2].status, "fulfilled");
            assert.deepEqual(await settlesWithin(waiters.get(mode), 5_000), [[7]]);
            const status = await settlesWithin(stream.recv(), 5_000);
            assert.equal(status.kind, RpcStreamFrameKind.Status);
            stream.close();
          }
        },
        () => ({}),
        () => ({ maxPendingSendBytes: 128 }),
      );
    },
  );

  test(
    "native zero-copy batch releases every ref when a pending stream is reset",
    { skip: native == null || !hasGc, timeout: 15_000 },
    async () => {
      const baseline = outboundRefCount(native);
      let resetFinished;
      const resetFinishedPromise = new Promise((resolve) => {
        resetFinished = resolve;
      });
      await withNativePair(
        async (server) => {
          server.register("ownership", "Reset", RpcKind.BidirectionalStreaming, async (call) => {
            await delay(250);
            call.close();
            resetFinished();
          });
        },
        async (client) => {
          const stream = await client.nativeClient._startStreamZeroCopy({
            service: "ownership",
            method: "Reset",
            kind: RpcKind.BidirectionalStreaming,
            body: new Uint8Array(),
          });
          let bodies = Array.from({ length: 6 }, (_, index) =>
            new Uint8Array(3 * 1024 * 1024).fill(index + 1),
          );
          const sendResult = stream._sendMessagesZeroCopy(bodies).then(
            () => null,
            (error) => error,
          );
          assert.equal(outboundRefCount(native), baseline + bodies.length);
          bodies = null;
          await setImmediate();
          stream.close();
          await forceGcCycles(5);
          const sendError = await settlesWithin(sendResult, 5_000);
          assert.equal(sendError.nativeCode, -125);
          await waitForOutboundRefs(native, baseline);
          await resetFinishedPromise;
        },
      );
    },
  );

  test(
    "serialized native operations include queue wait in stream idle timeouts",
    { skip: native == null || !hasGc, timeout: 15_000 },
    async () => {
      const baseline = outboundRefCount(native);
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
                ._sendMessagesZeroCopy(
                  Array.from({ length: 12 }, () => new Uint8Array(3 * 1024 * 1024)),
                )
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
          const stream = await client.nativeClient._startStreamZeroCopy({
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
          await waitForOutboundRefs(native, baseline);
        },
        () => ({ outboundZeroCopy: true, streamIdleTimeoutMs: 25 }),
      );
    },
  );

  test(
    "native completion polling prevents duplex zero-copy worker starvation",
    { skip: native == null, timeout: 10_000 },
    async () => {
      const baseline = outboundRefCount(native);
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
              client.nativeClient._startStreamZeroCopy({
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
                streams.map((stream, index) =>
                  stream._sendMessageZeroCopy(new Uint8Array([index + 1])),
                ),
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
          await waitForOutboundRefs(native, baseline);
        },
        () => ({ outboundZeroCopy: true }),
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
    "native connection close cancels an accepted zero-copy send and releases refs once",
    { skip: native == null || !hasGc, timeout: 20_000 },
    async () => {
      const baseline = outboundRefCount(native);
      let heldCall;
      await withNativePair(
        async (server) => {
          server.nativeServer.register(
            "ownership",
            "ConnectionClose",
            RpcKind.BidirectionalStreaming,
            (call) => {
              heldCall = call;
            },
          );
        },
        async (client) => {
          const stream = await client.nativeClient._startStreamZeroCopy({
            service: "ownership",
            method: "ConnectionClose",
            kind: RpcKind.BidirectionalStreaming,
            body: new Uint8Array(),
          });
          let bodies = Array.from({ length: 6 }, () => new Uint8Array(3 * 1024 * 1024));
          const pending = stream._sendMessagesZeroCopy(bodies).then(
            () => null,
            (error) => error,
          );
          assert.equal(outboundRefCount(native), baseline + bodies.length);
          bodies = null;
          await delay(10);
          client.close();
          const sendError = await settlesWithin(pending, 5_000);
          assert.equal(sendError.nativeCode, -125);
          await waitForOutboundRefs(native, baseline);
          heldCall?.close();
          heldCall = null;
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
      skip: native == null || !hasGc,
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

function resourceCloseCount(native) {
  return native._debugPendingResourceCloses();
}

function resourceFinalizerCount(native) {
  return native._debugPendingResourceFinalizers();
}

function outboundRefCount(native) {
  return native._debugRetainedOutboundBodyRefs();
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

async function waitForOutboundRefs(native, target) {
  await waitForCondition(
    () => outboundRefCount(native) === target,
    `expected ${target} retained outbound refs, saw ${outboundRefCount(native)}`,
  );
}

async function waitForCondition(predicate, message) {
  for (let i = 0; i < 100; i += 1) {
    if (predicate()) {
      return;
    }
    await delay(1);
  }
  assert.fail(message);
}

function checksum(bytes) {
  let value = 0;
  for (const byte of bytes) {
    value = value * 131 + byte;
  }
  return value;
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
  const client = await NodeTransport.connect({
    host: "127.0.0.1",
    port: server.port,
    skipCertificateValidation: true,
    ...clientOptions(),
  });
  try {
    await run(client, server);
  } finally {
    client.close();
    server.close();
    await serving;
  }
}

async function testCertificate() {
  if (certificateDirectory == null) {
    certificateDirectory = await mkdtemp(join(tmpdir(), "trevrpc-js-zero-copy-cert-"));
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
