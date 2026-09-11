import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync } from "node:fs";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { after } from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import { NodeServer } from "@trevrpc/trevrpc-js/node";
import { RawNodeTransport } from "@trevrpc/trevrpc-js/node/advanced";

import { Code, RpcKind, RpcStreamFrameKind, createServiceClient } from "../src/index.js";

const nativeAddonPath = join(import.meta.dirname, "..", "build", "native", "trevrpc_native.node");
const benchmarkMessageType = {
  decode(body) {
    return body;
  },
  encode(body) {
    return { finish: () => Uint8Array.from(body) };
  },
};
const benchmarkRoot = {
  lookupType() {
    return benchmarkMessageType;
  },
};
const benchmarkService = {
  fullName: "lifecycle.Benchmark",
  methods: {
    unary: { name: "Unary", kind: "unary", inputType: "Bytes", outputType: "Bytes" },
    clientStream: {
      name: "ClientStream",
      kind: "clientStreaming",
      inputType: "Bytes",
      outputType: "Bytes",
    },
    serverStream: {
      name: "ServerStream",
      kind: "serverStreaming",
      inputType: "Bytes",
      outputType: "Bytes",
    },
    bidi: {
      name: "Bidi",
      kind: "bidirectionalStreaming",
      inputType: "Bytes",
      outputType: "Bytes",
    },
  },
};
let certificateDirectory;

after(async () => {
  if (certificateDirectory != null) {
    await rm(certificateDirectory, { force: true, recursive: true });
  }
});

test(
  "successful native client-stream response retires before server shutdown",
  {
    skip: existsSync(nativeAddonPath) ? false : "focused native addon has not been built",
    timeout: 15_000,
  },
  async () => {
    const certificate = await testCertificate();
    const server = await NodeServer.listen({
      host: "127.0.0.1",
      port: 0,
      certFile: certificate.certFile,
      keyFile: certificate.keyFile,
    });
    server.nativeServer.register("lifecycle", "ClientStream", RpcKind.ClientStreaming, (call) => {
      void (async () => {
        let requestBytes = 0;
        for (;;) {
          const frame = await call.recv();
          if (frame == null) {
            break;
          }
          requestBytes += frame.body.byteLength;
        }
        await call.sendMessage(new Uint8Array([requestBytes]));
        await call.finishStream(Code.Ok);
      })().catch(() => call.close());
    });

    const serving = server.serve();
    const client = await RawNodeTransport.connect({
      host: "127.0.0.1",
      port: server.port,
      skipCertificateValidation: true,
    });
    let stream;
    try {
      stream = await client.nativeClient.startStream({
        service: "lifecycle",
        method: "ClientStream",
        kind: RpcKind.ClientStreaming,
        version: 1,
        body: new Uint8Array(),
      });
      await stream.sendMessage(new Uint8Array([1, 2, 3]));
      await stream.finishSend();

      const message = await settlesWithin(stream.recv(), 5_000);
      assert.equal(message.kind, RpcStreamFrameKind.Message);
      assert.deepEqual(Array.from(message.body), [3]);
      const status = await settlesWithin(stream.recv(), 5_000);
      assert.equal(status.kind, RpcStreamFrameKind.Status);
      assert.equal(status.status, Code.Ok);
      assert.equal(await settlesWithin(stream.recv(), 5_000), null);

      server.close();
      await settlesWithin(serving, 5_000);
    } finally {
      stream?.close();
      client.close();
      server.close();
      await settlesWithin(serving, 5_000);
    }
  },
);

test(
  "native server retires repeated benchmark RPC shapes without aborting responses",
  {
    skip: existsSync(nativeAddonPath) ? false : "focused native addon has not been built",
    timeout: 30_000,
  },
  async () => {
    const certificate = await testCertificate();
    const server = await NodeServer.listen({
      host: "127.0.0.1",
      port: 0,
      certFile: certificate.certFile,
      keyFile: certificate.keyFile,
    });
    server.registerService(benchmarkService, benchmarkHandlers());

    const serving = server.serve();
    const transport = await RawNodeTransport.connect({
      host: "127.0.0.1",
      port: server.port,
      skipCertificateValidation: true,
    });
    const client = createServiceClient(transport, benchmarkService, benchmarkRoot, {
      maxResponseMessages: -1,
      maxResponseStreamBodySize: -1,
      streamIdleTimeoutMs: 5_000,
    });
    try {
      for (const rpcKind of ["unary", "client_stream", "server_stream", "bidi"]) {
        for (let operationIndex = 0; operationIndex < 64; operationIndex += 1) {
          try {
            await settlesWithin(runBenchmarkOperation(client, rpcKind, operationIndex), 5_000);
          } catch (error) {
            throw new Error(`${rpcKind} operation ${operationIndex} failed`, { cause: error });
          }
        }
      }
    } finally {
      transport.close();
      await settlesWithin(transport.closed, 5_000);
      server.close();
      await settlesWithin(serving, 5_000);
    }
  },
);

function benchmarkHandlers() {
  return {
    unary(call) {
      return call.request.body;
    },
    async clientStream(call) {
      let total = 0;
      for (;;) {
        const frame = await call.recv();
        if (frame == null || frame.kind === RpcStreamFrameKind.Status) {
          assert.equal(total, 16);
          return new Uint8Array([0x0a, 0x00]);
        }
        total += frame.body.byteLength;
      }
    },
    async *serverStream(call) {
      for (const response of Array(4).fill(call.request.body)) {
        yield response;
      }
    },
    async *bidi(call) {
      for (;;) {
        const frame = await call.recv();
        if (frame == null || frame.kind === RpcStreamFrameKind.Status) {
          return;
        }
        yield frame.body;
      }
    },
  };
}

async function runBenchmarkOperation(client, rpcKind, operationIndex) {
  const body = new Uint8Array([0x0a, 0x02, operationIndex & 0xff, 1]);
  switch (rpcKind) {
    case "unary": {
      const response = await client.unary(body);
      assert.deepEqual(response, body);
      return;
    }
    case "client_stream": {
      const call = await client.clientStream();
      await call.sendMany([body, body, body, body]);
      const response = await call.closeAndRecv();
      assert.deepEqual(response, new Uint8Array([0x0a, 0x00]));
      return;
    }
    case "server_stream": {
      const responses = await client.serverStream(body);
      let count = 0;
      for await (const response of responses) {
        assert.deepEqual(response, body);
        count += 1;
      }
      assert.equal(count, 4);
      return;
    }
    case "bidi": {
      const call = await client.bidi();
      const responses = [];
      await Promise.all([
        (async () => {
          await call.sendMany([body, body, body, body]);
          await call.closeSend();
        })(),
        (async () => {
          for (;;) {
            const response = await call.recv();
            if (response === undefined) {
              return;
            }
            responses.push(response);
          }
        })(),
      ]);
      assert.equal(responses.length, 4);
      for (const response of responses) {
        assert.deepEqual(response, body);
      }
      return;
    }
    default:
      throw new Error(`unsupported benchmark RPC kind ${rpcKind}`);
  }
}

async function testCertificate() {
  if (certificateDirectory == null) {
    certificateDirectory = await mkdtemp(join(tmpdir(), "trevrpc-js-response-lifecycle-cert-"));
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
