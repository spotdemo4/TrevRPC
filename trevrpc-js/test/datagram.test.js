import assert from "node:assert/strict";
import test from "node:test";

import { EchoClient, root } from "../build/interop/gen/interop.trevrpc.js";
import { UnreliableEchoClient } from "../build/interop/gen/unreliable.trevrpc.js";
import { Channel } from "../src/channel.js";
import { encodeMessage } from "../src/compression.js";
import { decodeDatagram, encodeDatagram } from "../src/datagram.js";
import { FrameType, marshalMessage, unmarshalMessage } from "../src/framing.js";
import { Code } from "../src/status.js";
import { RpcKind } from "../src/wire.js";
import { deferred, fakeDatagrams, fakeTransport, isPending, turn } from "./support.js";

const EchoRequest = root.lookupType("trevrpc.interop.v1.EchoRequest");
const EchoResponse = root.lookupType("trevrpc.interop.v1.EchoResponse");

const ownFlow = (request) => Number(request.datagramFlow);

/** A response datagram for a flow with one message per name, as the server sends it. */
const responseDatagram = (flow, ...messages) =>
  encodeDatagram(
    flow,
    messages.map((message) => encodeMessage(marshalMessage(EchoResponse, { message, index: 0 }))),
  );

/** The request messages carried by a datagram the client sent, and its flow. */
function decodeRequestDatagram(datagram) {
  const { flow, messages } = decodeDatagram(datagram);
  return {
    flow,
    messages: messages.map(({ payload }) => unmarshalMessage(EchoRequest, payload).message),
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

test("a call to an unreliable method gets a flow per call, counted per session", async () => {
  const requests = [];
  const first = sessionWith(fakeDatagrams(), serving(requests));
  const calls = [];
  for (let index = 0; index < 3; index += 1) {
    calls.push(await new UnreliableEchoClient(first).bidi());
  }
  await turn();
  assert.deepEqual(requests.map(ownFlow), [1, 2, 3]);
  assert.ok(requests.every((request) => request.kind === RpcKind.BidirectionalStreaming));
  assert.equal(first.datagrams.writables, 0, "no writer is needed before the first send");

  // Another session has its own allocator.
  const second = sessionWith(fakeDatagrams(), serving(requests));
  calls.push(await new UnreliableEchoClient(second).bidi());
  await turn();
  assert.equal(ownFlow(requests[3]), 1);
  await Promise.all(calls.map((call) => call.close()));
});

test("methods that are not unreliable, and sessions without datagrams, get no flow", async () => {
  const requests = [];
  const withDatagrams = sessionWith(fakeDatagrams(), serving(requests));
  const echo = new EchoClient(withDatagrams);
  await (await echo.bidi()).close();
  await (await echo.clientStream()).close();
  await turn();
  assert.deepEqual(requests.map(ownFlow), [0, 0]);

  // A session like QMux over WebSocket, which has no datagrams at all.
  const noDatagrams = fakeTransport(serving(requests));
  await (await new UnreliableEchoClient(noDatagrams).bidi()).close();
  await turn();
  assert.equal(ownFlow(requests[2]), 0);

  // Datagrams that cannot be read are as good as none.
  const unreadable = sessionWith({ readable: undefined }, serving(requests));
  await (await new UnreliableEchoClient(unreadable).bidi()).close();
  await turn();
  assert.equal(ownFlow(requests[3]), 0);
});

test("only the directions a method marks unreliable use datagrams", async () => {
  const requests = [];
  const datagrams = fakeDatagrams();
  const transport = sessionWith(datagrams, serving(requests));
  const service = {
    name: "S",
    fullName: "trevrpc.interop.v1.S",
    methods: {
      upload: {
        name: "Upload",
        kind: "clientStreaming",
        inputType: "trevrpc.interop.v1.EchoRequest",
        outputType: "trevrpc.interop.v1.EchoResponse",
        unreliableRequests: true,
      },
      watch: {
        name: "Watch",
        kind: "serverStreaming",
        inputType: "trevrpc.interop.v1.EchoRequest",
        outputType: "trevrpc.interop.v1.EchoResponse",
        unreliableResponses: true,
      },
    },
  };
  const { createServiceClient } = await import("../src/client.js");
  const client = createServiceClient(transport, service, root);

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
  const call = await new UnreliableEchoClient(transport).bidi();

  await call.send({ message: "small" });
  assert.equal(datagrams.sent.length, 1);
  assert.deepEqual(decodeRequestDatagram(datagrams.sent[0]), requestDatagram(1, "small"));

  // The largest message that fits: flow ID (1 byte), frame header (2 bytes), and 37 bytes of
  // message, which is a 35 character string.
  const fits = "x".repeat(35);
  const exact = marshalMessage(EchoRequest, { message: fits });
  assert.equal(exact.byteLength, 37);
  await call.send({ message: fits });
  assert.equal(datagrams.sent.length, 2);

  // One byte more, and the message goes on the stream.
  await call.send({ message: fits + "x" });
  assert.equal(datagrams.sent.length, 2);

  await call.closeSend();
  await served.promise;
  assert.deepEqual(
    frames.map((frame) => frame.type),
    [FrameType.Message],
  );
  assert.equal(unmarshalMessage(EchoRequest, frames[0].payload).message, fits + "x");
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
  const call = await new UnreliableEchoClient(transport).bidi();
  // 1100 bytes in total: 1 flow byte, 3 bytes of frame header, 1096 bytes of message, which is a
  // 1093 character string.
  await call.send({ message: "x".repeat(1093) });
  await call.send({ message: "x".repeat(1094) });
  assert.equal(datagrams.sent.length, 1);
  assert.equal(datagrams.sent[0].byteLength, 1100);
  await call.closeSend();
  await turn();
  assert.equal(frames.length, 1);
  await call.close();
});

test("messages sent together share a datagram, in order, within the size limit", async () => {
  const datagrams = fakeDatagrams({ maxDatagramSize: 40 });
  const transport = sessionWith(datagrams, serving([]));
  const call = await new UnreliableEchoClient(transport).bidi();

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
  const call = await new UnreliableEchoClient(transport).bidi();

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
    for (let message; (message = await stream.readMessage(EchoRequest)) !== null;) {
      messages.push(message.message);
    }
  });
  const call = await new UnreliableEchoClient(transport).bidi();
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
    for (let message; (message = await stream.readMessage(EchoRequest)) !== null;) {
      messages.push(message.message);
    }
  });
  const call = await new UnreliableEchoClient(transport).bidi();
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
      frames.push(unmarshalMessage(EchoRequest, frame.payload).message);
    }
  });
  const call = await new UnreliableEchoClient(transport).bidi();
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
  const call = await new UnreliableEchoClient(transport).bidi();
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
  const call = await new UnreliableEchoClient(transport).bidi({ signal: controller.signal });

  const sending = call.send({ message: "stuck" });
  sending.catch(() => {});
  assert.equal(await isPending(sending), true);
  controller.abort();
  await assert.rejects(sending, (error) => error.code === Code.Cancelled);

  // Another call of the session is not blocked by the cancelled one: its message waits for the
  // transport too, and it is released by its own deadline.
  const other = await new UnreliableEchoClient(transport).bidi({ timeoutMs: 20 });
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
  const call = await new UnreliableEchoClient(transport).bidi();

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
      await stream.writeMessage(EchoResponse, { message: "stream-1", index: 0 });
      await turn();
      datagrams.push(responseDatagram(ownFlow(request), "dgram-1"));
      await turn();
      await stream.writeMessage(EchoResponse, { message: "stream-2", index: 1 });
      await finishCall(stream);
    }),
  );
  const call = await new UnreliableEchoClient(transport).bidi();
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
      const hi = marshalMessage(EchoResponse, { message: "four", index: 0 });
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
  const call = await new UnreliableEchoClient(transport).bidi();
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
      await stream.writeMessage(EchoResponse, { message: "late", index: 0 });
      await finishCall(stream);
    }),
  );
  const call = await new UnreliableEchoClient(transport).bidi();

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
  const call = await new UnreliableEchoClient(transport).bidi();
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
  const call = await new UnreliableEchoClient(transport).bidi();
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
  const client = new UnreliableEchoClient(transport);
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
  const call = await new UnreliableEchoClient(transport).bidi();
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
  const call = await new UnreliableEchoClient(transport).bidi();
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
  const call = await new UnreliableEchoClient(transport).bidi({ maxFrameSize: 64 });
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
  const client = new UnreliableEchoClient(transport);
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
  const client = new UnreliableEchoClient(channel);
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
  const call = await new UnreliableEchoClient(channel).bidi();
  await call.send({ message: "stream" });
  await turn();
  assert.deepEqual(sessions[0].requests.map(ownFlow), [0]);
  await call.close();
  channel.close();
});
