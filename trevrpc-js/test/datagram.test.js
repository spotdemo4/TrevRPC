import assert from "node:assert/strict";
import test from "node:test";

import { Echo, EchoRequestSchema, EchoResponseSchema } from "../build/interop/gen/interop_pb.js";
import { UnreliableEcho } from "../build/interop/gen/unreliable_pb.js";
import { OneWayUnreliableEcho } from "../build/test/gen/methods_pb.js";
import { Channel } from "../src/channel.js";
import { createClient } from "../src/client.js";
import { encodeMessage } from "../src/compression.js";
import { DatagramCountWaitMs, decodeDatagram, encodeDatagram } from "../src/datagram.js";
import {
  FrameType,
  encodeDatagramCountFrame,
  encodeFrame,
  marshalMessage,
  unmarshalMessage,
} from "../src/framing.js";
import { Code } from "../src/status.js";
import { RpcKind } from "../src/wire.js";
import { concatBytes, deferred, fakeDatagrams, fakeTransport, isPending, turn } from "./support.js";
import { encodeAnswerFrame, encodeHeadersFrame, encodeStatusFrame } from "./wire-server.js";

const ownFlow = (request) => Number(request.datagramFlow);

/** A response datagram for a flow with one message per name, as the server sends it. */
const responseDatagram = (flow, ...messages) =>
  encodeDatagram(
    flow,
    messages.map((message) =>
      encodeMessage(marshalMessage(EchoResponseSchema, { message, index: 0 })),
    ),
  );

/** The request messages carried by a datagram the client sent, and its flow. */
function decodeRequestDatagram(datagram) {
  const { flow, messages } = decodeDatagram(datagram);
  return {
    flow,
    messages: messages.map(({ payload }) => unmarshalMessage(EchoRequestSchema, payload).message),
  };
}

const requestDatagram = (flow, ...messages) => ({ flow, messages });

/** A transport for a session with datagrams; the handler serves each stream. */
function sessionWith(datagrams, handler) {
  const transport = fakeTransport(handler);
  transport.datagrams = datagrams;
  return transport;
}

/** Serves a call: reports its REQUEST, then runs `serve(stream, request)`. */
function serving(requests, serve) {
  return async (stream) => {
    const request = await stream.readRequest();
    requests.push(request);
    await serve?.(stream, request);
  };
}

async function finishCall(stream) {
  await stream.writeStatus();
  await stream.finish();
}

/** Ends the response like a server that sent `count` datagrams: DATAGRAM_COUNT, STATUS, one write. */
async function finishWithCount(stream, count) {
  await stream.write(
    concatBytes([encodeDatagramCountFrame(count), encodeStatusFrame({ code: Code.Ok })]),
  );
  await stream.finish();
}

/** The frames of the request stream after REQUEST, as `[type, payload]`; a reset ends the list. */
async function readRequestFrames(stream) {
  const frames = [];
  try {
    for (let frame; (frame = await stream.readFrame()) !== null;) {
      frames.push([frame.type, frame.payload]);
    }
  } catch {
    // The request stream was reset.
  }
  return frames;
}

test("a call to an unreliable method gets a flow per call, counted per session", async () => {
  const requests = [];
  const first = sessionWith(fakeDatagrams(), serving(requests));
  const calls = [];
  for (let index = 0; index < 3; index += 1) {
    calls.push(await createClient(UnreliableEcho, first).bidi());
  }
  await turn();
  assert.deepEqual(requests.map(ownFlow), [1, 2, 3]);
  assert.ok(requests.every((request) => request.kind === RpcKind.BidirectionalStreaming));
  assert.equal(first.datagrams.writables, 0, "no writer is needed before the first send");

  // Another session has its own allocator.
  const second = sessionWith(fakeDatagrams(), serving(requests));
  calls.push(await createClient(UnreliableEcho, second).bidi());
  await turn();
  assert.equal(ownFlow(requests[3]), 1);
  await Promise.all(calls.map((call) => call.close()));
});

test("methods that are not unreliable, and sessions without datagrams, get no flow", async () => {
  const requests = [];
  const withDatagrams = sessionWith(fakeDatagrams(), serving(requests));
  const echo = createClient(Echo, withDatagrams);
  await (await echo.bidi()).close();
  await (await echo.clientStream()).close();
  await turn();
  assert.deepEqual(requests.map(ownFlow), [0, 0]);

  // A session like QMux over WebSocket, which has no datagrams at all.
  const noDatagrams = fakeTransport(serving(requests));
  await (await createClient(UnreliableEcho, noDatagrams).bidi()).close();
  await turn();
  assert.equal(ownFlow(requests[2]), 0);

  // Datagrams that cannot be read are as good as none.
  const unreadable = sessionWith({ readable: undefined }, serving(requests));
  await (await createClient(UnreliableEcho, unreadable).bidi()).close();
  await turn();
  assert.equal(ownFlow(requests[3]), 0);
});

test("only the directions a method marks unreliable use datagrams", async () => {
  const requests = [];
  const datagrams = fakeDatagrams();
  const transport = sessionWith(datagrams, serving(requests));
  const client = createClient(OneWayUnreliableEcho, transport);

  const upload = await client.upload();
  await upload.send({ message: "u" });
  const watch = await client.watch({ message: "w" });
  await turn();
  assert.deepEqual(requests.map(ownFlow), [1, 2]);
  assert.deepEqual(datagrams.sent.map(decodeRequestDatagram), [requestDatagram(1, "u")]);
  await Promise.all([upload.close(), watch.close()]);
});

test("request messages that fit go out as datagrams after REQUEST, others on the stream", async () => {
  const datagrams = fakeDatagrams({ maxDatagramSize: 40 });
  const frames = [];
  const served = deferred();
  const transport = sessionWith(datagrams, async (stream) => {
    await stream.readRequest();
    for (let frame; (frame = await stream.readFrame()) !== null;) {
      frames.push(frame);
    }
    served.resolve();
  });
  const call = await createClient(UnreliableEcho, transport).bidi();

  await call.send({ message: "small" });
  assert.equal(datagrams.sent.length, 1);
  assert.deepEqual(decodeRequestDatagram(datagrams.sent[0]), requestDatagram(1, "small"));

  // The largest message that fits: flow ID (1 byte), frame header (2 bytes), and 37 bytes of
  // message, which is a 35 character string.
  const fits = "x".repeat(35);
  const exact = marshalMessage(EchoRequestSchema, { message: fits });
  assert.equal(exact.byteLength, 37);
  await call.send({ message: fits });
  assert.equal(datagrams.sent.length, 2);

  // One byte more, and the message goes on the stream.
  await call.send({ message: fits + "x" });
  assert.equal(datagrams.sent.length, 2);

  await call.closeSend();
  await served.promise;
  // The datagrams are announced by DATAGRAM_COUNT right before the end of the request stream.
  assert.deepEqual(
    frames.map((frame) => frame.type),
    [FrameType.Message, FrameType.DatagramCount],
  );
  assert.equal(unmarshalMessage(EchoRequestSchema, frames[0].payload).message, fits + "x");
  assert.deepEqual([...frames[1].payload], [2]);
  assert.equal(datagrams.writables, 1, "all calls share one writer");
  await call.close();
});

test("without maxDatagramSize the limit is 1100 bytes", async () => {
  const datagrams = fakeDatagrams();
  const frames = [];
  const transport = sessionWith(datagrams, async (stream) => {
    await stream.readRequest();
    for (let frame; (frame = await stream.readFrame()) !== null;) {
      frames.push(frame);
    }
  });
  const call = await createClient(UnreliableEcho, transport).bidi();
  // 1100 bytes in total: 1 flow byte, 3 bytes of frame header, 1096 bytes of message, which is a
  // 1093 character string.
  await call.send({ message: "x".repeat(1093) });
  await call.send({ message: "x".repeat(1094) });
  assert.equal(datagrams.sent.length, 1);
  assert.equal(datagrams.sent[0].byteLength, 1100);
  await call.closeSend();
  await turn();
  assert.deepEqual(
    frames.map((frame) => frame.type),
    [FrameType.Message, FrameType.DatagramCount],
  );
  await call.close();
});

test("messages sent together share a datagram, in order, within the size limit", async () => {
  const datagrams = fakeDatagrams({ maxDatagramSize: 40 });
  const transport = sessionWith(datagrams, serving([]));
  const call = await createClient(UnreliableEcho, transport).bidi();

  // Each message is a 15 byte frame payload plus 2 bytes of header: two fit with the flow ID.
  const names = ["a", "b", "c", "d", "e"].map((name) => name.repeat(13));
  await Promise.all(names.map((message) => call.send({ message })));
  assert.deepEqual(datagrams.sent.map(decodeRequestDatagram), [
    requestDatagram(1, names[0], names[1]),
    requestDatagram(1, names[2], names[3]),
    requestDatagram(1, names[4]),
  ]);
  await call.close();
});

test("messages queued while a datagram waits for buffer space join the next datagram", async () => {
  const datagrams = fakeDatagrams();
  const release = deferred();
  datagrams.gate = release.promise;
  const transport = sessionWith(datagrams, serving([]));
  const call = await createClient(UnreliableEcho, transport).bidi();

  const sends = [call.send({ message: "first" })];
  await turn();
  sends.push(call.send({ message: "second" }), call.send({ message: "third" }));
  await turn();
  assert.equal(datagrams.sent.length, 0);
  release.resolve();
  await Promise.all(sends);
  assert.deepEqual(datagrams.sent.map(decodeRequestDatagram), [
    requestDatagram(1, "first"),
    requestDatagram(1, "second", "third"),
  ]);
  await call.close();
});

test("a message that cannot be a datagram goes on the stream without reordering the others", async () => {
  const datagrams = fakeDatagrams({ maxDatagramSize: 40 });
  const messages = [];
  const transport = sessionWith(datagrams, async (stream) => {
    await stream.readRequest();
    for (let message; (message = await stream.readMessage(EchoRequestSchema)) !== null;) {
      messages.push(message.message);
    }
  });
  const call = await createClient(UnreliableEcho, transport).bidi();
  const big = "B".repeat(100);
  await Promise.all(["a", big, "c", "d"].map((message) => call.send({ message })));
  await call.closeSend();
  await turn();
  assert.deepEqual(datagrams.sent.map(decodeRequestDatagram), [
    requestDatagram(1, "a"),
    requestDatagram(1, "c", "d"),
  ]);
  assert.deepEqual(messages, [big]);
  await call.close();
});

test("a failed datagram write sends every message of that datagram on the stream", async () => {
  const datagrams = fakeDatagrams();
  datagrams.failWrites = true;
  const messages = [];
  const transport = sessionWith(datagrams, async (stream) => {
    await stream.readRequest();
    for (let message; (message = await stream.readMessage(EchoRequestSchema)) !== null;) {
      messages.push(message.message);
    }
  });
  const call = await createClient(UnreliableEcho, transport).bidi();
  await Promise.all(["a", "b", "c"].map((message) => call.send({ message })));
  await call.closeSend();
  await turn();
  assert.deepEqual(messages, ["a", "b", "c"]);
  await call.close();
});

test("a failed datagram write sends that message on the stream instead", async () => {
  const datagrams = fakeDatagrams();
  datagrams.failWrites = true;
  const frames = [];
  const transport = sessionWith(datagrams, async (stream) => {
    await stream.readRequest();
    for (let frame; (frame = await stream.readFrame()) !== null;) {
      frames.push(unmarshalMessage(EchoRequestSchema, frame.payload).message);
    }
  });
  const call = await createClient(UnreliableEcho, transport).bidi();
  await call.send({ message: "a" });
  await call.send({ message: "b" });
  await call.closeSend();
  await turn();
  assert.deepEqual(frames, ["a", "b"]);
  await call.close();
});

test("a session without a datagram writer sends everything on the stream", async () => {
  const datagrams = fakeDatagrams();
  delete datagrams.createWritable;
  const frames = [];
  const transport = sessionWith(datagrams, async (stream) => {
    const request = await stream.readRequest();
    assert.equal(ownFlow(request), 1, "the flow still receives");
    for (let frame; (frame = await stream.readFrame()) !== null;) {
      frames.push(frame);
    }
  });
  const call = await createClient(UnreliableEcho, transport).bidi();
  await call.send({ message: "a" });
  await call.closeSend();
  await turn();
  assert.equal(frames.length, 1);
  await call.close();
});

test("waiting for send-buffer space honours the call's cancellation", async () => {
  const datagrams = fakeDatagrams({ outgoingHighWaterMark: 1 });
  datagrams.gate = new Promise(() => {}); // the transport accepts no datagram
  const transport = sessionWith(datagrams, serving([]));
  const controller = new AbortController();
  const call = await createClient(UnreliableEcho, transport).bidi({ signal: controller.signal });

  const sending = call.send({ message: "stuck" });
  sending.catch(() => {});
  assert.equal(await isPending(sending), true);
  controller.abort();
  await assert.rejects(sending, (error) => error.code === Code.Cancelled);

  // Another call of the session is not blocked by the cancelled one: its message waits for the
  // transport too, and it is released by its own deadline.
  const other = await createClient(UnreliableEcho, transport).bidi({ timeoutMs: 20 });
  await assert.rejects(
    other.send({ message: "x" }),
    (error) => error.code === Code.DeadlineExceeded,
  );
});

test("closeSend comes after the datagram sends in flight", async () => {
  const datagrams = fakeDatagrams();
  const release = deferred();
  datagrams.gate = release.promise;
  const requests = [];
  const transport = sessionWith(datagrams, serving(requests));
  const call = await createClient(UnreliableEcho, transport).bidi();

  const sending = call.send({ message: "a" });
  const closing = call.closeSend();
  await assert.rejects(
    call.send({ message: "late" }),
    (error) => error.code === Code.FailedPrecondition,
  );
  assert.equal(await isPending(closing), true);
  release.resolve();
  await Promise.all([sending, closing]);
  assert.equal(datagrams.sent.length, 1);
  await call.close();
});

test("response datagrams merge with stream messages in arrival order", async () => {
  const datagrams = fakeDatagrams();
  const flows = deferred();
  const written = deferred();
  const transport = sessionWith(
    datagrams,
    serving([], async (stream, request) => {
      flows.resolve(ownFlow(request));
      await written.promise;
      await stream.writeHeaders();
      await stream.writeMessage(EchoResponseSchema, { message: "stream-1", index: 0 });
      await turn();
      datagrams.push(responseDatagram(ownFlow(request), "dgram-1"));
      await turn();
      await stream.writeMessage(EchoResponseSchema, { message: "stream-2", index: 1 });
      await finishCall(stream);
    }),
  );
  const call = await createClient(UnreliableEcho, transport).bidi();
  written.resolve();
  await flows.promise;

  const received = [];
  for await (const response of call) {
    received.push(response.message);
  }
  assert.deepEqual(received, ["stream-1", "dgram-1", "stream-2"]);
});

test("a datagram of several messages delivers them in order; malformed ones are dropped whole", async () => {
  const datagrams = fakeDatagrams();
  const transport = sessionWith(
    datagrams,
    serving([], async (stream, request) => {
      const flow = ownFlow(request);
      await stream.writeHeaders();
      datagrams.push(responseDatagram(flow, "one", "two", "three"));
      // A frame that claims more bytes than the datagram holds drops every message in it.
      const truncated = responseDatagram(flow, "lost-1", "lost-2");
      datagrams.push(truncated.subarray(0, truncated.byteLength - 1));
      // Frames of other types are skipped, with any varint form of type and length.
      const hi = marshalMessage(EchoResponseSchema, { message: "four", index: 0 });
      datagrams.push(
        Uint8Array.of(
          flow,
          0x21,
          0,
          0x40,
          0x02,
          0x01,
          0xff,
          0x40,
          0x00,
          0x40,
          hi.byteLength,
          ...hi,
        ),
      );
      datagrams.push(responseDatagram(flow, "five"));
      await turn(5);
      await finishCall(stream);
    }),
  );
  const call = await createClient(UnreliableEcho, transport).bidi();
  const received = [];
  for await (const response of call) {
    received.push(response.message);
  }
  assert.deepEqual(received, ["one", "two", "three", "four", "five"]);
});

test("datagrams wait for HEADERS", async () => {
  const datagrams = fakeDatagrams();
  const release = deferred();
  const transport = sessionWith(
    datagrams,
    serving([], async (stream, request) => {
      datagrams.push(responseDatagram(ownFlow(request), "early"));
      await release.promise;
      await stream.writeHeaders();
      await stream.writeMessage(EchoResponseSchema, { message: "late", index: 0 });
      await finishCall(stream);
    }),
  );
  const call = await createClient(UnreliableEcho, transport).bidi();

  const receiving = call.recv();
  assert.equal(await isPending(receiving), true, "the datagram is held until HEADERS");
  release.resolve();
  assert.equal((await receiving).message, "early");
  assert.equal((await call.recv()).message, "late");
  assert.equal(await call.recv(), undefined);
});

test("a pending recv wakes on a datagram without any stream frame", async () => {
  const datagrams = fakeDatagrams();
  const ready = deferred();
  const end = deferred();
  const transport = sessionWith(
    datagrams,
    serving([], async (stream, request) => {
      await stream.writeHeaders();
      ready.resolve(ownFlow(request));
      await end.promise;
      await finishCall(stream);
    }),
  );
  const call = await createClient(UnreliableEcho, transport).bidi();
  const flow = await ready.promise;
  const receiving = call.recv();
  await turn();
  datagrams.push(responseDatagram(flow, "only-datagram"));
  assert.equal((await receiving).message, "only-datagram");
  end.resolve();
  assert.equal(await call.recv(), undefined);
});

test("datagram messages queued before STATUS are delivered, later ones dropped", async () => {
  const datagrams = fakeDatagrams();
  const flowOf = deferred();
  const status = deferred();
  const transport = sessionWith(
    datagrams,
    serving([], async (stream, request) => {
      await stream.writeHeaders();
      flowOf.resolve(ownFlow(request));
      datagrams.push(responseDatagram(ownFlow(request), "before"));
      await turn();
      await status.promise;
      await finishCall(stream);
    }),
  );
  const call = await createClient(UnreliableEcho, transport).bidi();
  const flow = await flowOf.promise;
  await turn(5);
  status.resolve();
  await call.status;
  datagrams.push(responseDatagram(flow, "after"));
  await turn();
  assert.equal((await call.recv()).message, "before");
  assert.equal(await call.recv(), undefined);
});

test("datagrams for unknown or ended flows are dropped without disturbing other calls", async () => {
  const datagrams = fakeDatagrams();
  const requests = [];
  const transport = sessionWith(
    datagrams,
    serving(requests, async (stream, request) => {
      await stream.writeHeaders();
      if (ownFlow(request) === 1) {
        await finishCall(stream);
      }
    }),
  );
  const client = createClient(UnreliableEcho, transport);
  const first = await client.bidi();
  assert.equal(await first.recv(), undefined);

  const second = await client.bidi();
  datagrams.push(responseDatagram(1, "ended flow"));
  datagrams.push(responseDatagram(99, "unknown flow"));
  datagrams.push(new Uint8Array(0));
  datagrams.push(new Uint8Array([0x40]));
  datagrams.push(responseDatagram(2, "mine"));
  assert.equal((await second.recv()).message, "mine");
  await second.close();
});

test("a datagram that is not a valid message fails the call like a stream message", async () => {
  const datagrams = fakeDatagrams();
  const transport = sessionWith(
    datagrams,
    serving([], async (stream, request) => {
      await stream.writeHeaders();
      datagrams.push(
        encodeDatagram(ownFlow(request), [encodeMessage(new Uint8Array([0x0a, 0x05, 0x61]))]),
      );
    }),
  );
  const call = await createClient(UnreliableEcho, transport).bidi();
  await assert.rejects(call.recv(), (error) => error.code === Code.InvalidArgument);
});

test("a full receive queue drops datagrams instead of blocking the reader", async () => {
  const datagrams = fakeDatagrams();
  const pushed = deferred();
  const transport = sessionWith(
    datagrams,
    serving([], async (stream, request) => {
      await stream.writeHeaders();
      for (let index = 0; index < 100; index += 1) {
        datagrams.push(responseDatagram(ownFlow(request), `m${index}`));
      }
      pushed.resolve();
      await pushed.promise;
      await turn(10);
      datagrams.push(responseDatagram(ownFlow(request), "after-full"));
      await turn(5);
      await finishCall(stream);
    }),
  );
  const call = await createClient(UnreliableEcho, transport).bidi();
  await pushed.promise;
  await call.status;

  const received = [];
  for (let response; (response = await call.recv()) !== undefined;) {
    received.push(response.message);
  }
  // The first 32 fit the call's queue (the oldest are kept), the rest were dropped.
  assert.equal(received.length, 32);
  assert.deepEqual(received.slice(0, 3), ["m0", "m1", "m2"]);
  assert.ok(!received.includes("after-full"));
});

test("an oversized datagram message is dropped", async () => {
  const datagrams = fakeDatagrams();
  const transport = sessionWith(
    datagrams,
    serving([], async (stream, request) => {
      await stream.writeHeaders();
      datagrams.push(responseDatagram(ownFlow(request), "x".repeat(100)));
      datagrams.push(responseDatagram(ownFlow(request), "ok"));
      await turn(5);
      await finishCall(stream);
    }),
  );
  const call = await createClient(UnreliableEcho, transport).bidi({ maxFrameSize: 64 });
  assert.equal((await call.recv()).message, "ok");
  assert.equal(await call.recv(), undefined);
});

test("a cancelled call releases its flow", async () => {
  const datagrams = fakeDatagrams();
  const requests = [];
  const transport = sessionWith(
    datagrams,
    serving(requests, async (stream) => {
      await stream.writeHeaders();
    }),
  );
  const client = createClient(UnreliableEcho, transport);
  const call = await client.bidi();
  await call.headers;
  await call.close();
  datagrams.push(responseDatagram(1, "late"));
  await turn();
  await assert.rejects(call.recv(), (error) => error.code === Code.Cancelled);

  // Flow IDs are never reused on the session.
  await (await client.bidi()).close();
  await turn();
  assert.deepEqual(requests.map(ownFlow), [1, 2]);
});

/** A WebTransport constructor whose sessions carry fake datagrams. */
function datagramWebTransport({ datagrams = true } = {}) {
  const sessions = [];
  class FakeWebTransport {
    constructor() {
      this.ready = Promise.resolve();
      this.closed = new Promise((resolve) => {
        this.lose = resolve;
      });
      this.requests = [];
      this.streams = fakeTransport(serving(this.requests, (stream) => stream.writeHeaders()));
      if (datagrams) {
        this.datagrams = fakeDatagrams();
      }
      sessions.push(this);
    }

    close() {
      this.lose({});
    }

    createBidirectionalStream() {
      return this.streams.createBidirectionalStream();
    }
  }
  return { WebTransport: FakeWebTransport, sessions };
}

test("a channel gives calls the datagrams of the session that carries them", async () => {
  const { WebTransport, sessions } = datagramWebTransport();
  const channel = await Channel.connect("https://example.test/trevrpc", {
    transports: ["webtransport"],
    WebTransport,
  });
  const [first] = sessions;
  const client = createClient(UnreliableEcho, channel);
  await (await client.bidi()).close();
  await turn();
  assert.deepEqual(first.requests.map(ownFlow), [1]);

  // The stream opens slowly on the first session while the channel moves to a second one.
  const original = first.createBidirectionalStream.bind(first);
  const slow = deferred();
  first.createBidirectionalStream = async () => {
    await slow.promise;
    return original();
  };
  const opening = client.bidi();
  first.lose({});
  await new Promise((resolve) => setTimeout(resolve, 150));
  await channel.waitUntilReady();
  assert.equal(sessions.length, 2);
  slow.resolve();
  const call = await opening;
  await turn();

  assert.deepEqual(first.requests.map(ownFlow), [1, 2], "the flow is the first session's");
  assert.equal(sessions[1].requests.length, 0);

  // Datagrams of the first session reach the call; the new session's do not.
  sessions[1].datagrams.push(responseDatagram(2, "wrong session"));
  first.datagrams.push(responseDatagram(2, "right session"));
  first.requests.length = 0;
  assert.equal((await call.recv()).message, "right session");
  await call.close();

  // A call on the new session starts a fresh allocator.
  await (await client.bidi()).close();
  await turn();
  assert.deepEqual(sessions[1].requests.map(ownFlow), [1]);
  channel.close();
});

test("a channel without datagrams, like QMux over WebSocket, sends no flow", async () => {
  const { WebTransport, sessions } = datagramWebTransport({ datagrams: false });
  const channel = await Channel.connect("https://example.test/trevrpc", {
    transports: ["webtransport"],
    WebTransport,
  });
  const call = await createClient(UnreliableEcho, channel).bidi();
  await call.send({ message: "stream" });
  await turn();
  assert.deepEqual(sessions[0].requests.map(ownFlow), [0]);
  await call.close();
  channel.close();
});

// DATAGRAM_COUNT, request direction: what the client sends.

test("closeSend writes DATAGRAM_COUNT with the datagrams sent, right before the end", async () => {
  const datagrams = fakeDatagrams({ maxDatagramSize: 40 });
  const frames = deferred();
  const transport = sessionWith(
    datagrams,
    serving([], async (stream) => frames.resolve(await readRequestFrames(stream))),
  );
  const call = await createClient(UnreliableEcho, transport).bidi();

  // Two datagrams of one message each, then a message that only fits the stream, then a third.
  await call.send({ message: "a" });
  await call.send({ message: "b" });
  await call.send({ message: "B".repeat(100) });
  await call.send({ message: "c" });
  assert.equal(datagrams.sent.length, 3);
  await call.closeSend();

  const read = await frames.promise;
  assert.deepEqual(
    read.map(([type]) => type),
    [FrameType.Message, FrameType.DatagramCount],
    "the count is the last frame, right before the end of the stream",
  );
  assert.deepEqual([...read[1][1]], [3], "a message on the stream is not a datagram");
  await call.close();
});

test("the DATAGRAM_COUNT of a large count is a minimal varint", async () => {
  const datagrams = fakeDatagrams();
  const frames = deferred();
  const transport = sessionWith(
    datagrams,
    serving([], async (stream) => frames.resolve(await readRequestFrames(stream))),
  );
  const call = await createClient(UnreliableEcho, transport).bidi();
  for (let index = 0; index < 64; index += 1) {
    await call.send({ message: "x" });
  }
  await call.closeSend();
  assert.deepEqual(
    (await frames.promise).map(([type, payload]) => [type, [...payload]]),
    [[FrameType.DatagramCount, [0x40, 64]]],
  );
  await call.close();
});

test("closeSend writes no DATAGRAM_COUNT when no datagram was sent", async () => {
  const frames = deferred();
  const handler = serving([], async (stream) => frames.resolve(await readRequestFrames(stream)));

  // Nothing was sent at all.
  const quiet = await createClient(UnreliableEcho, sessionWith(fakeDatagrams(), handler)).bidi();
  await quiet.closeSend();
  assert.deepEqual(await frames.promise, []);
  await quiet.close();

  // Every datagram write failed, so every message went on the stream.
  const failing = fakeDatagrams();
  failing.failWrites = true;
  const stream = deferred();
  const failed = await createClient(
    UnreliableEcho,
    sessionWith(
      failing,
      serving([], async (serverStream) => stream.resolve(await readRequestFrames(serverStream))),
    ),
  ).bidi();
  await failed.send({ message: "a" });
  await failed.send({ message: "b" });
  await failed.closeSend();
  assert.deepEqual(
    (await stream.promise).map(([type]) => type),
    [FrameType.Message, FrameType.Message],
  );
  await failed.close();
});

test("a datagram write that failed does not count, one that succeeded before it does", async () => {
  const datagrams = fakeDatagrams();
  const frames = deferred();
  const transport = sessionWith(
    datagrams,
    serving([], async (stream) => frames.resolve(await readRequestFrames(stream))),
  );
  const call = await createClient(UnreliableEcho, transport).bidi();
  await call.send({ message: "datagram" });
  datagrams.failWrites = true;
  await call.send({ message: "stream" });
  await call.closeSend();
  const read = await frames.promise;
  assert.deepEqual(
    read.map(([type]) => type),
    [FrameType.Message, FrameType.DatagramCount],
  );
  assert.equal(unmarshalMessage(EchoRequestSchema, read[0][1]).message, "stream");
  assert.deepEqual([...read[1][1]], [1]);
  await call.close();
});

test("a call that is cancelled sends no DATAGRAM_COUNT", async () => {
  const datagrams = fakeDatagrams();
  const frames = deferred();
  const transport = sessionWith(
    datagrams,
    serving([], async (stream) => frames.resolve(await readRequestFrames(stream))),
  );
  const call = await createClient(UnreliableEcho, transport).bidi();
  await call.send({ message: "a" });
  assert.equal(datagrams.sent.length, 1);
  await call.close();
  assert.deepEqual(await frames.promise, [], "the request stream was reset instead");
});

test("a call that does not send datagrams as requests sends no DATAGRAM_COUNT", async () => {
  const frames = deferred();
  const transport = sessionWith(
    fakeDatagrams(),
    serving([], async (stream) => frames.resolve(await readRequestFrames(stream))),
  );
  // Only the responses of Watch are unreliable, and Upload sends its requests on the stream.
  const call = await createClient(OneWayUnreliableEcho, transport).upload();
  call.closeAndRecv().catch(() => {});
  assert.deepEqual(await frames.promise, []);
  await call.close();
});

// The request stream as a server reads it: the test server applies the rules of the wire format.

/** A server that reads request messages with ServerStream.readMessage until it fails or ends. */
async function readRequests(frames) {
  const outcome = deferred();
  const transport = fakeTransport(async (stream) => {
    await stream.readRequest();
    const messages = [];
    try {
      for (let message; (message = await stream.readMessage(EchoRequestSchema)) !== null;) {
        messages.push(message.message);
      }
      outcome.resolve({ messages, datagramCount: stream.datagramCount });
    } catch (error) {
      outcome.resolve({ messages, error });
    }
  });
  const { writable } = await transport.createBidirectionalStream();
  const writer = writable.getWriter();
  for (const frame of frames) {
    await writer.write(frame);
  }
  await writer.close();
  return outcome.promise;
}

const requestFrame = encodeFrame(FrameType.Request, new TextEncoder().encode("\n\x06/svc/m"));
const messageFrame = (message) =>
  encodeFrame(FrameType.Message, marshalMessage(EchoRequestSchema, { message }));

test("a request stream may end with one DATAGRAM_COUNT, which a server reads as no message", async () => {
  const outcome = await readRequests([
    requestFrame,
    messageFrame("a"),
    encodeFrame(FrameType.DatagramCount, Uint8Array.of(0x40, 2)),
    encodeFrame(0x21, new Uint8Array(3)),
  ]);
  assert.equal(outcome.error, undefined);
  assert.deepEqual(outcome.messages, ["a"]);
  assert.equal(outcome.datagramCount, 2);
});

test("a request stream rejects a second count, a frame after the count and a malformed count", async () => {
  const count = (...payload) => encodeFrame(FrameType.DatagramCount, Uint8Array.from(payload));
  for (const { name, frames } of [
    { name: "a second count", frames: [count(1), count(1)] },
    { name: "a message after the count", frames: [count(1), messageFrame("late")] },
    { name: "an empty count", frames: [count()] },
    { name: "a count of two varints", frames: [count(1, 1)] },
    { name: "a truncated count", frames: [count(0x40)] },
  ]) {
    const outcome = await readRequests([requestFrame, ...frames]);
    assert.equal(outcome.error?.code, Code.InvalidArgument, name);
  }
});

// DATAGRAM_COUNT, response direction: what the client receives.

test("a call ends only once the datagrams that DATAGRAM_COUNT announces have arrived", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const datagrams = fakeDatagrams();
  const flowOf = deferred();
  const transport = sessionWith(
    datagrams,
    serving([], async (stream, request) => {
      await stream.writeHeaders();
      flowOf.resolve(ownFlow(request));
      datagrams.push(responseDatagram(ownFlow(request), "one"));
      await finishWithCount(stream, 2);
    }),
  );
  const call = await createClient(UnreliableEcho, transport).bidi();
  const flow = await flowOf.promise;
  assert.equal((await call.recv()).message, "one");

  const ending = call.recv();
  assert.equal(await isPending(ending), true, "STATUS waits for the second datagram");
  assert.equal(await isPending(call.status), true);
  datagrams.push(responseDatagram(flow, "two"));
  assert.equal((await ending).message, "two", "delivered before the end of the responses");
  assert.equal(await call.recv(), undefined);
  assert.equal((await call.status).code, Code.Ok);
});

test(
  "a call ends at once when the announced datagrams arrived already",
  { timeout: 5000 },
  async (t) => {
    // The wait would never end: its timer is not ticked.
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const datagrams = fakeDatagrams();
    const written = deferred();
    const release = deferred();
    const transport = sessionWith(
      datagrams,
      serving([], async (stream, request) => {
        const flow = ownFlow(request);
        // Datagrams that overtook HEADERS count as well as those that follow it.
        datagrams.push(responseDatagram(flow, "before-headers"));
        await release.promise;
        await stream.writeHeaders();
        datagrams.push(responseDatagram(flow, "one", "two"));
        await turn(5);
        written.resolve();
        await finishWithCount(stream, 2);
      }),
    );
    const call = await createClient(UnreliableEcho, transport).bidi();
    await turn(5);
    release.resolve();
    await written.promise;

    const received = [];
    for await (const response of call) {
      received.push(response.message);
    }
    assert.deepEqual(received, ["before-headers", "one", "two"]);
    assert.equal((await call.status).code, Code.Ok);
  },
);

test("a call gives up on missing datagrams after the bound", async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const datagrams = fakeDatagrams();
  const flowOf = deferred();
  const transport = sessionWith(
    datagrams,
    serving([], async (stream, request) => {
      await stream.writeHeaders();
      flowOf.resolve(ownFlow(request));
      datagrams.push(responseDatagram(ownFlow(request), "one"));
      await finishWithCount(stream, 3);
    }),
  );
  const call = await createClient(UnreliableEcho, transport).bidi();
  const flow = await flowOf.promise;
  assert.equal((await call.recv()).message, "one");

  const ending = call.recv();
  await turn(5);
  t.mock.timers.tick(DatagramCountWaitMs - 1);
  assert.equal(await isPending(ending), true, "still within the bound");
  t.mock.timers.tick(1);
  assert.equal(await ending, undefined);
  assert.equal((await call.status).code, Code.Ok);

  // A datagram that comes later is dropped.
  datagrams.push(responseDatagram(flow, "late"));
  await turn();
  assert.equal(await call.recv(), undefined);
});

test("datagrams dropped whole count as received", { timeout: 5000 }, async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const datagrams = fakeDatagrams();
  const transport = sessionWith(
    datagrams,
    serving([], async (stream, request) => {
      const flow = ownFlow(request);
      await stream.writeHeaders();
      // 100 datagrams overflow the call's queue of 32 messages, and one is malformed.
      for (let index = 0; index < 100; index += 1) {
        datagrams.push(responseDatagram(flow, `m${index}`));
      }
      const truncated = responseDatagram(flow, "lost");
      datagrams.push(truncated.subarray(0, truncated.byteLength - 1));
      await turn(10);
      await finishWithCount(stream, 101);
    }),
  );
  const call = await createClient(UnreliableEcho, transport).bidi();
  // The status comes at once, though most datagrams found no room; nobody received meanwhile.
  assert.equal((await call.status).code, Code.Ok);
  const received = [];
  for await (const response of call) {
    received.push(response.message);
  }
  assert.equal(received.length, 32);
});

test("closing the call ends the wait for missing datagrams", { timeout: 5000 }, async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const transport = sessionWith(
    fakeDatagrams(),
    serving([], async (stream) => {
      await stream.writeHeaders();
      await finishWithCount(stream, 5);
    }),
  );
  const call = await createClient(UnreliableEcho, transport).bidi();
  const receiving = call.recv();
  receiving.catch(() => {});
  await turn(10);
  assert.equal(await isPending(receiving), true);
  await call.close();
  await assert.rejects(receiving, (error) => error.code === Code.Cancelled);
  await assert.rejects(call.status, (error) => error.code === Code.Cancelled);
});

test("the deadline ends the wait for missing datagrams", { timeout: 5000 }, async (t) => {
  t.mock.timers.enable({ apis: ["setTimeout"] });
  const transport = sessionWith(
    fakeDatagrams(),
    serving([], async (stream) => {
      await stream.writeHeaders();
      await finishWithCount(stream, 5);
    }),
  );
  const call = await createClient(UnreliableEcho, transport).bidi({ timeoutMs: 3 });
  const receiving = call.recv();
  receiving.catch(() => {});
  await turn(10);
  // The deadline falls before the bound does.
  t.mock.timers.tick(DatagramCountWaitMs - 1);
  await assert.rejects(receiving, (error) => error.code === Code.DeadlineExceeded);
  await assert.rejects(call.status, (error) => error.code === Code.DeadlineExceeded);
});

test(
  "the count is ignored without a flow and in a direction that does not stream",
  { timeout: 5000 },
  async (t) => {
    // The wait would never end: its timer is not ticked.
    t.mock.timers.enable({ apis: ["setTimeout"] });
    const answer = async (stream) => {
      await stream.readRequest();
      await stream.write(
        concatBytes([
          encodeFrame(
            FrameType.Message,
            marshalMessage(EchoResponseSchema, { message: "ok", index: 0 }),
          ),
          encodeDatagramCountFrame(5),
          encodeStatusFrame({ code: Code.Ok }),
        ]),
      );
      await stream.finish();
    };

    // A unary response never streams, so it never has a flow.
    const unary = await createClient(Echo, fakeTransport(answer)).unary({ message: "x" });
    assert.equal(unary.message, "ok");

    // The responses of Upload do not stream, though its requests are unreliable and have a flow.
    const upload = await createClient(
      OneWayUnreliableEcho,
      sessionWith(fakeDatagrams(), answer),
    ).upload();
    assert.equal((await upload.closeAndRecv()).message, "ok");

    // A session without datagrams, like QMux over WebSocket, gives the call no flow.
    const bidi = await createClient(UnreliableEcho, fakeTransport(answer)).bidi();
    assert.equal((await bidi.recv()).message, "ok");
    assert.equal(await bidi.recv(), undefined);
  },
);

/** Plays a server that sends `frames` after REQUEST and ends the response. */
function sendingFrames(...frames) {
  return sessionWith(
    fakeDatagrams(),
    serving([], async (stream) => {
      await stream.write(concatBytes(frames));
      await stream.finish();
    }),
  );
}

const message = (text) =>
  encodeFrame(FrameType.Message, marshalMessage(EchoResponseSchema, { message: text, index: 0 }));
const count = (...payload) => encodeFrame(FrameType.DatagramCount, Uint8Array.from(payload));
const ok = encodeStatusFrame({ code: Code.Ok });

test("a response stream accepts one DATAGRAM_COUNT right before STATUS, in any varint form", async () => {
  for (const service of [UnreliableEcho, Echo]) {
    const grease = encodeFrame(0x21, new Uint8Array(2));
    for (const frames of [
      [count(0), ok],
      [encodeHeadersFrame({}), message("a"), count(0x40, 0), grease, ok],
      [encodeAnswerFrame({}), encodeHeadersFrame({}), count(0xc0, 0, 0, 0, 0, 0, 0, 0), ok],
    ]) {
      const call = await createClient(service, sendingFrames(...frames)).bidi();
      for await (const response of call) {
        assert.equal(response.message, "a");
      }
      assert.equal((await call.status).code, Code.Ok);
    }
  }
});

test("a response stream rejects a misplaced or malformed DATAGRAM_COUNT as Internal", async () => {
  const headers = encodeHeadersFrame({});
  for (const { name, frames } of [
    { name: "a second count", frames: [count(0), count(0), ok] },
    {
      name: "a second count after unknown frames",
      frames: [count(0), encodeFrame(0x21, new Uint8Array(1)), count(0), ok],
    },
    { name: "a message after the count", frames: [count(0), message("late"), ok] },
    { name: "HEADERS after the count", frames: [count(0), headers, ok] },
    { name: "ANSWER after the count", frames: [count(0), encodeAnswerFrame({}), ok] },
    { name: "an empty count", frames: [count(), ok] },
    { name: "a count of two varints", frames: [count(1, 1), ok] },
    { name: "a truncated count", frames: [count(0x40), ok] },
  ]) {
    for (const service of [UnreliableEcho, Echo]) {
      const call = await createClient(service, sendingFrames(...frames)).bidi();
      await assert.rejects(
        call.recv(),
        (error) => error.code === Code.Internal,
        `${name} on ${service.typeName}`,
      );
    }
  }
});

test("a DATAGRAM_COUNT after STATUS does not change the outcome", async () => {
  const call = await createClient(
    UnreliableEcho,
    sendingFrames(encodeHeadersFrame({}), ok, count(1)),
  ).bidi();
  assert.equal(await call.recv(), undefined);
  assert.equal((await call.status).code, Code.Ok);
});
