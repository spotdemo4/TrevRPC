// A client session and a server session connected by in-memory WebSockets.
import assert from "node:assert/strict";
import test from "node:test";

import { QMuxError } from "../src/errors.js";
import { readAll, sessionPair } from "./support.js";

/** Writes `data` to a writable and closes it. */
async function writeAll(writable, data) {
  const writer = writable.getWriter();
  await writer.write(data);
  await writer.close();
}

/** Echoes every bidirectional stream `session` accepts. */
function echo(session) {
  return (async () => {
    for await (const stream of session.incomingBidirectionalStreams) {
      stream.readable.pipeTo(stream.writable).catch(() => {});
    }
  })().catch(() => {});
}

/** The next item of a readable stream, without releasing others to read on. */
async function next(readable) {
  const reader = readable.getReader();
  try {
    const { value, done } = await reader.read();
    assert.ok(!done, "the stream ended");
    return value;
  } finally {
    reader.releaseLock();
  }
}

const Bytes = Uint8Array.from({ length: 300_000 }, (_, i) => (i * 7) % 256);

test("a server session over an accepted socket echoes the client's streams", async () => {
  const { client, server } = await sessionPair();
  void echo(server);
  const results = await Promise.all(
    [1, 2, 3].map(async (n) => {
      const stream = await client.createBidirectionalStream();
      const payload = Bytes.subarray(0, 100_000 * n);
      const [, echoed] = await Promise.all([
        writeAll(stream.writable, payload),
        readAll(stream.readable),
      ]);
      return echoed.length === payload.length && echoed.every((byte, i) => byte === payload[i]);
    }),
  );
  assert.deepEqual(results, [true, true, true]);
  client.close({ closeCode: 0, reason: "bye" });
  assert.deepEqual(await server.closed, { closeCode: 0, reason: "bye" });
  assert.deepEqual(await client.closed, { closeCode: 0, reason: "bye" });
});

test("the server opens bidirectional streams to the client", async () => {
  const { client, server } = await sessionPair();
  void echo(client);
  const stream = await server.createBidirectionalStream();
  const [, echoed] = await Promise.all([
    writeAll(stream.writable, Bytes),
    readAll(stream.readable),
  ]);
  assert.deepEqual(echoed, Bytes);
  server.close();
});

test("unidirectional streams go both ways", async () => {
  const { client, server } = await sessionPair();
  for (const [from, to] of [
    [client, server],
    [server, client],
  ]) {
    const writable = await from.createUnidirectionalStream();
    const written = writeAll(writable, Bytes);
    const readable = await next(to.incomingUnidirectionalStreams);
    assert.deepEqual(await readAll(readable), Bytes);
    await written;
  }
  client.close();
});

test("a reset unidirectional stream errors the peer's readable with its code", async () => {
  const { client, server } = await sessionPair();
  const writable = await client.createUnidirectionalStream();
  const writer = writable.getWriter();
  await writer.write(Uint8Array.of(1, 2, 3));
  const readable = await next(server.incomingUnidirectionalStreams);
  await writer.abort(new QMuxError("gone", { streamErrorCode: 42 }));
  await assert.rejects(readAll(readable), (error) => error.streamErrorCode === 42);
  client.close();
});

test("the peer's stream limit rises as its streams complete", async () => {
  const { client, server } = await sessionPair(
    {},
    { parameters: { initialMaxStreamsBidi: 2, initialMaxStreamsUni: 2 } },
  );
  void echo(server);
  void (async () => {
    for await (const readable of server.incomingUnidirectionalStreams) {
      readAll(readable).catch(() => {});
    }
  })().catch(() => {});
  // Twenty streams of each kind, five at a time, through limits of two.
  for (let round = 0; round < 4; round += 1) {
    await Promise.all(
      Array.from({ length: 5 }, async () => {
        const stream = await client.createBidirectionalStream();
        const [, echoed] = await Promise.all([
          writeAll(stream.writable, Uint8Array.of(round)),
          readAll(stream.readable),
        ]);
        assert.deepEqual(echoed, Uint8Array.of(round));
        await writeAll(await client.createUnidirectionalStream(), Uint8Array.of(round));
      }),
    );
  }
  client.close();
});

test("cancelling incoming streams refuses those the peer opens later", async () => {
  const { client, server } = await sessionPair();
  await server.incomingBidirectionalStreams.cancel();
  const stream = await client.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  await writer.write(Uint8Array.of(1));
  await assert.rejects(readAll(stream.readable), (error) => error.streamErrorCode === 0);
  await assert.rejects(writer.closed, (error) => error.streamErrorCode === 0);
  assert.equal(server.stage, "ready");
  client.close();
});

test("datagrams go both ways when both endpoints enable them", async () => {
  const parameters = { maxDatagramFrameSize: 1200 };
  const { client, server } = await sessionPair({ parameters }, { parameters });
  // A 1200-byte frame holds its type, a 2-byte length and 1197 bytes.
  assert.equal(client.datagrams.maxDatagramSize, 1197);
  assert.equal(server.datagrams.maxDatagramSize, 1197);
  for (const [from, to] of [
    [client, server],
    [server, client],
  ]) {
    const writer = from.datagrams.writable.getWriter();
    const reader = to.datagrams.readable.getReader();
    await writer.write(new Uint8Array(1198)); // Too large: dropped.
    await writer.write(Uint8Array.of(1, 2, 3));
    await writer.write(new Uint8Array(1197).fill(9));
    assert.deepEqual((await reader.read()).value, Uint8Array.of(1, 2, 3));
    assert.deepEqual((await reader.read()).value, new Uint8Array(1197).fill(9));
    writer.releaseLock();
    reader.releaseLock();
  }
  client.close();
  await assert.rejects(client.datagrams.readable.getReader().read());
});

test("datagrams are off unless enabled, and go only to a peer that enabled them", async () => {
  const { client, server } = await sessionPair({ parameters: { maxDatagramFrameSize: 1200 } });
  assert.equal(server.datagrams, undefined);
  assert.equal(client.datagrams.maxDatagramSize, 0);
  // The server accepts none, so the client drops what it writes instead of sending it: a stream
  // opened after it still reaches the server, which is still up.
  await client.datagrams.writable.getWriter().write(Uint8Array.of(1));
  const stream = await client.createBidirectionalStream();
  await writeAll(stream.writable, Uint8Array.of(2));
  const accepted = await next(server.incomingBidirectionalStreams);
  assert.deepEqual(await readAll(accepted.readable), Uint8Array.of(2));
  assert.equal(server.stage, "ready");
  client.close();
});

test("an application close resolves the peer's closed with its code and reason", async () => {
  const { client, server } = await sessionPair();
  const stream = await client.createBidirectionalStream();
  server.close({ closeCode: 7, reason: "maintenance" });
  assert.deepEqual(await client.closed, { closeCode: 7, reason: "maintenance" });
  await assert.rejects(readAll(stream.readable), (error) => error.source === "session");
  await assert.rejects(client.createBidirectionalStream(), (error) => error.source === "session");
});

test("incoming streams end when the session closes", async () => {
  const { client, server } = await sessionPair();
  const reader = server.incomingBidirectionalStreams.getReader();
  client.close();
  assert.deepEqual(await reader.read(), { value: undefined, done: true });
});
