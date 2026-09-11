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

import { Code, RpcKind, RpcStreamFrameKind } from "../src/index.js";

const nativeAddonPath = join(import.meta.dirname, "..", "build", "native", "trevrpc_native.node");
let certificateDirectory;

after(async () => {
  if (certificateDirectory != null) {
    await rm(certificateDirectory, { force: true, recursive: true });
  }
});

test(
  "successful native client-stream response closes before server shutdown",
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
