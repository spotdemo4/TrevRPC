// Receiving responses in batches: recvAvailable() and batches() hand over every message that is
// available without waiting, in one array.
import assert from "node:assert/strict";
import test from "node:test";

import { CompressedEcho } from "../build/interop/gen/compressed_pb.js";
import { Echo, EchoRequestSchema, EchoResponseSchema } from "../build/interop/gen/interop_pb.js";
import { UnreliableEcho } from "../build/interop/gen/unreliable_pb.js";
import { createClient } from "../src/client.js";
import { encodeMessage } from "../src/compression.js";
import { encodeDatagram } from "../src/datagram.js";
import {
  FrameType,
  encodeFrame,
  encodeMessageFrame,
  encodeStatusFrame,
  marshalMessage,
} from "../src/framing.js";
import { Code, TrevRpcError } from "../src/status.js";
import { StreamCompression } from "../src/wire.js";
import { createCompression } from "../src/zstd.js";
import { concatBytes, deferred, fakeDatagrams, fakeTransport, isPending, turn } from "./support.js";

const isStatus = (code) => (error) => error instanceof TrevRpcError && error.code === code;

/** The MESSAGE frame of a response whose message is `message`. */
const messageFrame = (message, index = 0) =>
  encodeMessageFrame(marshalMessage(EchoResponseSchema, { message, index }));

/** The MESSAGE frames of responses with the messages `names`, in one buffer. */
const messageFrames = (...names) => concatBytes(names.map((name) => messageFrame(name)));

const okStatus = () => encodeStatusFrame({ code: Code.Ok, message: "", metadata: {} });

/** An Echo client whose streams are served by handler(serverStream) after the REQUEST frame. */
function setup(handler) {
  const transport = fakeTransport(async (stream) => {
    await stream.readRequest();
    await handler(stream);
  });
  return { transport, echo: createClient(Echo, transport) };
}

const names = (batch) => batch.map((response) => response.message);

test("every message of a chunk comes as one batch, then the end", async () => {
  const { echo, transport } = setup(async (stream) => {
    await stream.write(concatBytes([messageFrames("a", "b", "c", "d"), okStatus()]));
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  assert.deepEqual(names(await stream.recvAvailable()), ["a", "b", "c", "d"]);
  assert.equal(await stream.recvAvailable(), undefined);
  assert.equal(await stream.recvAvailable(), undefined);
  assert.equal((await stream.status).code, Code.Ok);
  await transport.settled();
});

test("batches() iterates over the batches until the final status", async () => {
  const second = deferred();
  const { echo, transport } = setup(async (stream) => {
    await stream.write(messageFrames("a", "b"));
    await second.promise;
    await stream.write(concatBytes([messageFrames("c"), okStatus()]));
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  const seen = [];
  for await (const batch of stream.batches()) {
    seen.push(names(batch));
    second.resolve();
  }
  assert.deepEqual(seen, [["a", "b"], ["c"]]);
  await transport.settled();
});

test("a frame split across chunks waits for its rest, and later frames start the next batch", async () => {
  const rest = deferred();
  const frames = messageFrames("a", "b", "c", "d");
  const cut = messageFrame("a").byteLength + messageFrame("b").byteLength + 3;
  const { echo, transport } = setup(async (stream) => {
    // The third frame is cut inside its header and payload.
    await stream.write(frames.subarray(0, cut));
    await rest.promise;
    await stream.write(concatBytes([frames.subarray(cut), okStatus()]));
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  assert.deepEqual(names(await stream.recvAvailable()), ["a", "b"]);
  rest.resolve();
  assert.deepEqual(names(await stream.recvAvailable()), ["c", "d"]);
  assert.equal(await stream.recvAvailable(), undefined);
  await transport.settled();
});

test("a frame header split at every byte still yields every message in order", async () => {
  const wire = concatBytes([messageFrames("a", "b", "c"), okStatus()]);
  const { echo, transport } = setup(async (stream) => {
    for (let offset = 0; offset < wire.byteLength; offset += 1) {
      await stream.write(wire.subarray(offset, offset + 1));
    }
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  const seen = [];
  for await (const batch of stream.batches()) {
    assert.ok(batch.length > 0, "a batch is never empty");
    seen.push(...names(batch));
  }
  assert.deepEqual(seen, ["a", "b", "c"]);
  await transport.settled();
});

test("headers and unknown frames in a chunk belong to no batch", async () => {
  const { echo } = setup(async (stream) => {
    await stream.write(
      concatBytes([
        encodeFrame(0x21, new Uint8Array(5)),
        messageFrame("a"),
        encodeFrame(0x22, new Uint8Array(1)),
        messageFrame("b"),
        okStatus(),
      ]),
    );
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  assert.deepEqual(names(await stream.recvAvailable()), ["a", "b"]);
  assert.equal(await stream.recvAvailable(), undefined);
});

test("an error status comes after the batch of the messages before it", async () => {
  const { echo } = setup(async (stream) => {
    await stream.write(
      concatBytes([
        messageFrames("a", "b"),
        encodeStatusFrame({ code: Code.Aborted, message: "stop", metadata: {} }),
      ]),
    );
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  assert.deepEqual(names(await stream.recvAvailable()), ["a", "b"]);
  await assert.rejects(stream.recvAvailable(), isStatus(Code.Aborted));
  await assert.rejects(stream.recvAvailable(), isStatus(Code.Aborted));
  await assert.rejects(stream.status, isStatus(Code.Aborted));
});

test("batches() throws the error status after the last batch", async () => {
  const { echo } = setup(async (stream) => {
    await stream.write(
      concatBytes([
        messageFrames("a"),
        encodeStatusFrame({ code: Code.NotFound, message: "gone", metadata: {} }),
      ]),
    );
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  const iterator = stream.batches();
  assert.deepEqual(names((await iterator.next()).value), ["a"]);
  await assert.rejects(iterator.next(), isStatus(Code.NotFound));
});

test("a stream that ends without a status fails after the messages before it", async () => {
  const { echo } = setup(async (stream) => {
    await stream.write(messageFrames("a", "b"));
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  assert.deepEqual(names(await stream.recvAvailable()), ["a", "b"]);
  await assert.rejects(stream.recvAvailable(), isStatus(Code.Internal));
});

test("a frame cut off by the end of the stream fails after the messages before it", async () => {
  const { echo } = setup(async (stream) => {
    await stream.write(concatBytes([messageFrames("a", "b"), messageFrame("c").subarray(0, 3)]));
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  assert.deepEqual(names(await stream.recvAvailable()), ["a", "b"]);
  await assert.rejects(stream.recvAvailable(), isStatus(Code.Unavailable));
});

test("an invalid frame in a chunk fails after the messages before it", async () => {
  const invalid = [
    [Code.InvalidArgument, encodeFrame(FrameType.Status, new Uint8Array([0x0a, 0xff]))],
    [Code.Internal, encodeFrame(FrameType.Request, new Uint8Array(0))],
    [Code.Internal, encodeFrame(FrameType.Headers, new Uint8Array(0))],
    // A frame of 2^62 - 1 bytes, which no limit allows.
    [Code.ResourceExhausted, new Uint8Array([FrameType.Message, ...Array(8).fill(0xff)])],
  ];
  for (const [code, frame] of invalid) {
    const { echo } = setup(async (stream) => {
      await stream.write(concatBytes([messageFrames("a", "b"), frame]));
    });
    const stream = await echo.serverStream({});
    assert.deepEqual(names(await stream.recvAvailable()), ["a", "b"]);
    await assert.rejects(stream.recvAvailable(), isStatus(code));
    await assert.rejects(stream.status, isStatus(code));
  }
});

test("a message that does not decode fails the call after the messages before it", async () => {
  const { echo } = setup(async (stream) => {
    await stream.write(
      concatBytes([messageFrames("a", "b"), encodeMessageFrame(new Uint8Array([0x0a, 0xff]))]),
    );
  });
  const stream = await echo.serverStream({});
  assert.deepEqual(names(await stream.recvAvailable()), ["a", "b"]);
  await assert.rejects(stream.recvAvailable(), isStatus(Code.InvalidArgument));
  await assert.rejects(stream.status, isStatus(Code.InvalidArgument));

  // As the first message of a batch, it fails the call at once.
  const first = setup(async (stream) => {
    await stream.write(encodeMessageFrame(new Uint8Array([0x0a, 0xff])));
  });
  const other = await first.echo.serverStream({});
  await assert.rejects(other.recvAvailable(), isStatus(Code.InvalidArgument));
});

test("a receive that waits is answered by the first chunk, with all of its messages", async () => {
  const release = deferred();
  const { echo } = setup(async (stream) => {
    await release.promise;
    await stream.write(messageFrames("a", "b", "c"));
    await stream.write(okStatus());
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  const waiting = stream.recvAvailable();
  assert.ok(await isPending(waiting));
  release.resolve();
  assert.deepEqual(names(await waiting), ["a", "b", "c"]);
  assert.equal(await stream.recvAvailable(), undefined);
});

test("recv() and recvAvailable() may be mixed and keep the order", async () => {
  const { echo } = setup(async (stream) => {
    await stream.write(concatBytes([messageFrames("a", "b", "c", "d", "e"), okStatus()]));
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  const iterator = stream[Symbol.asyncIterator]();
  assert.equal((await iterator.next()).value.message, "a");
  assert.deepEqual(names(await stream.recvAvailable()), ["b", "c", "d", "e"]);
  assert.equal((await iterator.next()).done, true);
});

test("concurrent receives are served in order", async () => {
  const second = deferred();
  const { echo } = setup(async (stream) => {
    await stream.write(messageFrames("a", "b"));
    await second.promise;
    await stream.write(concatBytes([messageFrames("c"), okStatus()]));
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  const first = stream.recvAvailable();
  const next = stream.recvAvailable();
  const last = stream.recvAvailable();
  assert.deepEqual(names(await first), ["a", "b"]);
  second.resolve();
  assert.deepEqual(names(await next), ["c"]);
  assert.equal(await last, undefined);
});

test("leaving a batch loop early cancels the call", async () => {
  let server;
  const { echo } = setup(async (stream) => {
    server = stream;
    await stream.write(messageFrames("a", "b"));
  });
  const stream = await echo.serverStream({});
  for await (const batch of stream.batches()) {
    assert.deepEqual(names(batch), ["a", "b"]);
    break;
  }
  assert.equal(await server.responseCancelled, true);
  await assert.rejects(stream.status, isStatus(Code.Cancelled));
});

test("cancelling the call fails a waiting receive, and closes the batches that follow", async () => {
  const { echo } = setup(async (stream) => {
    await stream.responseCancelled;
  });
  const stream = await echo.serverStream({});
  const waiting = stream.recvAvailable();
  await turn();
  await stream.close(new Error("enough"));
  await assert.rejects(waiting, (error) => {
    assert.ok(isStatus(Code.Cancelled)(error));
    assert.equal(error.cause.message, "enough");
    return true;
  });
  await assert.rejects(stream.recvAvailable(), isStatus(Code.Cancelled));
});

test("aborting the signal after a batch fails the next receive", async () => {
  const controller = new AbortController();
  const { echo } = setup(async (stream) => {
    await stream.write(messageFrames("a", "b"));
    await stream.responseCancelled;
  });
  const stream = await echo.serverStream({}, { signal: controller.signal });
  assert.deepEqual(names(await stream.recvAvailable()), ["a", "b"]);
  controller.abort();
  await assert.rejects(stream.recvAvailable(), isStatus(Code.Cancelled));
});

test("a batch takes the messages a call read ahead and those still in the buffer", async () => {
  const count = 100;
  const written = deferred();
  const { echo, transport } = setup(async (stream) => {
    await stream.write(concatBytes([messageFrames(...Array(count).fill("m")), okStatus()]));
    written.resolve();
    await stream.finish();
  });
  // A call that streams its request reads ahead 32 frames while nobody receives.
  const call = await echo.bidi();
  await written.promise;
  await turn();
  const batch = await call.recvAvailable();
  assert.equal(batch.length, count);
  assert.equal(await call.recvAvailable(), undefined);
  await transport.settled();
});

test("a batch stops at the read-ahead byte limit, and the rest comes in order", async () => {
  const size = 100 * 1024;
  const count = 24;
  const wire = concatBytes([
    ...Array.from({ length: count }, (_, index) => messageFrame("x".repeat(size), index)),
    okStatus(),
  ]);
  const { echo, transport } = setup(async (stream) => {
    // One chunk, so that every frame is buffered at once.
    await stream.write(wire);
    await stream.finish();
  });
  const stream = await echo.serverStream({});
  const indexes = [];
  const sizes = [];
  for await (const batch of stream.batches()) {
    sizes.push(batch.length);
    indexes.push(...batch.map((response) => response.index));
  }
  assert.deepEqual(
    indexes,
    Array.from({ length: count }, (_, index) => index),
  );
  // Up to 1 MiB of messages plus the frame that crosses it: 11 messages of 100 KiB.
  assert.ok(sizes.length >= 2, `one batch took ${sizes.join()} messages`);
  assert.ok(Math.max(...sizes) <= 11, `batches of ${sizes.join()} exceed the byte limit`);
  await transport.settled();
});

test("trailing metadata and headers are reported as with recv()", async () => {
  const { echo } = setup(async (stream) => {
    await stream.write(
      concatBytes([
        encodeFrame(FrameType.Headers, new Uint8Array(0)),
        messageFrames("a"),
        encodeStatusFrame({
          code: Code.Ok,
          message: "done",
          metadata: { checksum: new Uint8Array([7]) },
        }),
      ]),
    );
    await stream.finish();
  });
  const seen = {};
  const stream = await echo.serverStream(
    {},
    {
      onResponseHeaders: (headers) => {
        seen.headers = headers;
      },
      onResponseMetadata: (metadata) => {
        seen.metadata = metadata;
      },
    },
  );
  assert.deepEqual(names(await stream.recvAvailable()), ["a"]);
  assert.equal(await stream.recvAvailable(), undefined);
  assert.deepEqual(seen.headers, {});
  assert.deepEqual([...seen.metadata.checksum], [7]);
  assert.equal((await stream.status).message, "done");
});

test("a call that streams its request batches its responses too", async () => {
  const { echo, transport } = setup(async (stream) => {
    const requests = [];
    for (let request; (request = await stream.readMessage(EchoRequestSchema)) !== null;) {
      requests.push(request.message);
    }
    await stream.write(concatBytes([messageFrames(...requests), okStatus()]));
    await stream.finish();
  });
  const call = await echo.bidi();
  await call.sendAll([{ message: "1" }, { message: "2" }, { message: "3" }]);
  assert.deepEqual(names(await call.recvAvailable()), ["1", "2", "3"]);
  assert.equal(await call.recvAvailable(), undefined);
  await transport.settled();
});

test("datagram messages join a batch in arrival order", async () => {
  const datagrams = fakeDatagrams();
  const transport = fakeTransport(async (stream) => {
    const request = await stream.readRequest();
    await stream.writeHeaders();
    await stream.write(messageFrames("stream-1"));
    await turn();
    datagrams.push(
      encodeDatagram(Number(request.datagramFlow), [
        encodeMessage(marshalMessage(EchoResponseSchema, { message: "dgram-1", index: 0 })),
      ]),
    );
    await turn();
    await stream.write(concatBytes([messageFrames("stream-2"), okStatus()]));
    await stream.finish();
  });
  transport.datagrams = datagrams;
  const call = await createClient(UnreliableEcho, transport).bidi();
  const seen = [];
  for await (const batch of call.batches()) {
    seen.push(...names(batch));
  }
  assert.deepEqual(seen, ["stream-1", "dgram-1", "stream-2"]);
});

test("a refused optimistic call is replayed and its response comes in batches", async () => {
  const compression = await createCompression();
  const requests = [];
  const transport = fakeTransport(async (stream) => {
    const request = await stream.readRequest();
    requests.push(request);
    if (request.streamCompression === StreamCompression.Optimistic) {
      await stream.refuse();
      return;
    }
    await stream.readMessage(EchoRequestSchema);
    await stream.write(concatBytes([messageFrames("a", "b", "c"), okStatus()]));
    await stream.finish();
  });
  transport.compression = compression;
  const call = await createClient(CompressedEcho, transport).bidi();
  await call.send({ message: "go" });
  const batch = await call.recvAvailable();
  assert.deepEqual(names(batch), ["a", "b", "c"]);
  assert.equal(await call.recvAvailable(), undefined);
  assert.equal(requests.length, 2, "the call was replayed once");
  assert.equal(requests[0].streamCompression, StreamCompression.Optimistic);
  assert.notEqual(requests[1].streamCompression, StreamCompression.Optimistic);
});
