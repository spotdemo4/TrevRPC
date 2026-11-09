import assert from "node:assert/strict";
import test from "node:test";

import { Echo, EchoRequestSchema, EchoResponseSchema } from "../build/interop/gen/interop_pb.js";
import { connect, createChannel } from "../src/channel.js";
import { createClient } from "../src/client.js";
import {
  FrameReader,
  FrameType,
  decodeWireMessage,
  encodeAnswerFrame,
  encodeMessageFrame,
  encodeRequestFrame,
} from "../src/framing.js";
import { Code } from "../src/status.js";
import { RpcStatus } from "../src/wire.js";
import { echoHandler } from "./echo-server.js";
import { fakeTransport } from "./support.js";

const url = "https://example.test/trevrpc";
const decoder = new TextDecoder();

/** Metadata as text, for comparisons. */
const texts = (metadata) =>
  Object.fromEntries(Object.entries(metadata).map(([key, value]) => [key, decoder.decode(value)]));

/**
 * A WebTransport constructor whose sessions a server serves with handler(stream, session). Every
 * REQUEST the server reads is recorded in `requests`, as the call's own metadata and the
 * connection metadata it carries, before the server merges anything.
 */
function fakeWebTransport(handler = echoHandler) {
  const sessions = [];
  const requests = [];
  class FakeWebTransport {
    constructor() {
      this.ready = Promise.resolve();
      this.closed = new Promise((resolve) => {
        this.lose = resolve;
      });
      this.streams = fakeTransport((stream) => {
        const readRequest = stream.readRequest.bind(stream);
        stream.readRequest = async () => {
          const request = await readRequest();
          const info = request.connectionMetadata;
          requests.push({
            session: sessions.indexOf(this),
            metadata: texts(request.metadata),
            connection: info && { id: info.id, metadata: texts(info.metadata) },
          });
          return request;
        };
        return handler(stream, this);
      });
      sessions.push(this);
    }

    close() {
      this.lose();
    }

    createBidirectionalStream() {
      return this.streams.createBidirectionalStream();
    }
  }
  return { WebTransport: FakeWebTransport, sessions, requests };
}

async function channelTo(fake, options) {
  return connect(url, {
    transports: ["webtransport"],
    WebTransport: fake.WebTransport,
    ...options,
  });
}

/** Echoes `message` through the unary call and returns the response message. */
async function echo(client, message, metadata) {
  return (await client.unary({ message }, { metadata })).message;
}

test("a session offers the set until an ANSWER confirms it, then refers to it", async () => {
  const fake = fakeWebTransport();
  const channel = await channelTo(fake, { connectionMetadata: { "Interop-Suffix": "!" } });
  const client = createClient(Echo, channel);

  assert.equal(await echo(client, "a"), "a!");
  assert.equal(await echo(client, "b"), "b!");
  assert.equal(await echo(client, "c"), "c!");
  assert.deepEqual(
    fake.requests.map((request) => [request.connection, request.metadata]),
    [
      [{ id: 1, metadata: { "interop-suffix": "!" } }, {}],
      [{ id: 1, metadata: {} }, {}],
      [{ id: 1, metadata: {} }, {}],
    ],
  );
  channel.close();
});

test("the value of a call wins over the connection metadata", async () => {
  const fake = fakeWebTransport();
  const channel = await channelTo(fake, { connectionMetadata: { "interop-suffix": "!" } });
  const client = createClient(Echo, channel);

  assert.equal(await echo(client, "a", { "interop-suffix": "?" }), "a?");
  assert.equal(await echo(client, "b", { "interop-suffix": "?" }), "b?");
  assert.equal(await echo(client, "c"), "c!");
  // Both travel: the call's entries in the metadata, the set in the connection metadata.
  assert.deepEqual(
    fake.requests.map((request) => [request.connection, request.metadata]),
    [
      [{ id: 1, metadata: { "interop-suffix": "!" } }, { "interop-suffix": "?" }],
      [{ id: 1, metadata: {} }, { "interop-suffix": "?" }],
      [{ id: 1, metadata: {} }, {}],
    ],
  );
  channel.close();
});

test("changing the set offers the new one, and removing it sends none", async () => {
  const fake = fakeWebTransport();
  const channel = await channelTo(fake, { connectionMetadata: { "interop-suffix": "!" } });
  const client = createClient(Echo, channel);

  assert.equal(await echo(client, "a"), "a!");
  channel.setConnectionMetadata({ "interop-suffix": "#" });
  assert.equal(await echo(client, "b"), "b#");
  assert.equal(await echo(client, "c"), "c#");
  channel.setConnectionMetadata({});
  assert.equal(await echo(client, "d"), "d");
  channel.setConnectionMetadata({ "interop-suffix": "$" });
  assert.equal(await echo(client, "e"), "e$");
  assert.deepEqual(
    fake.requests.map((request) => request.connection),
    [
      { id: 1, metadata: { "interop-suffix": "!" } },
      { id: 2, metadata: { "interop-suffix": "#" } },
      { id: 2, metadata: {} },
      undefined,
      { id: 3, metadata: { "interop-suffix": "$" } },
    ],
  );
  channel.close();
});

test("a channel without connection metadata can set it later", async () => {
  const fake = fakeWebTransport();
  const channel = await channelTo(fake);
  const client = createClient(Echo, channel);

  assert.equal(await echo(client, "a"), "a");
  channel.setConnectionMetadata({ "interop-suffix": "!" });
  assert.equal(await echo(client, "b"), "b!");
  assert.deepEqual(
    fake.requests.map((request) => request.connection),
    [undefined, { id: 1, metadata: { "interop-suffix": "!" } }],
  );
  channel.close();
});

test("calls started before the confirmation all offer the set", async () => {
  const fake = fakeWebTransport();
  const channel = await channelTo(fake, { connectionMetadata: { "interop-suffix": "!" } });
  const client = createClient(Echo, channel);

  assert.deepEqual(await Promise.all(["a", "b", "c"].map((message) => echo(client, message))), [
    "a!",
    "b!",
    "c!",
  ]);
  assert.deepEqual(
    fake.requests.map((request) => Object.keys(request.connection.metadata)),
    [["interop-suffix"], ["interop-suffix"], ["interop-suffix"]],
  );
  assert.equal(await echo(client, "d"), "d!");
  assert.deepEqual(fake.requests[3].connection, { id: 1, metadata: {} });
  channel.close();
});

test("a new session starts unconfirmed and offers the set again", async () => {
  const fake = fakeWebTransport();
  const channel = await channelTo(fake, { connectionMetadata: { "interop-suffix": "!" } });
  const client = createClient(Echo, channel);

  assert.equal(await echo(client, "a"), "a!");
  assert.equal(await echo(client, "b"), "b!");
  fake.sessions[0].lose();
  await new Promise((resolve) => setTimeout(resolve, 0));
  await channel.waitUntilReady();
  assert.equal(fake.sessions.length, 2);
  assert.equal(await echo(client, "c"), "c!");
  assert.equal(await echo(client, "d"), "d!");
  assert.deepEqual(
    fake.requests.map((request) => [request.session, Object.keys(request.connection.metadata)]),
    [
      [0, ["interop-suffix"]],
      [0, []],
      [1, ["interop-suffix"]],
      [1, []],
    ],
  );
  channel.close();
});

test("only an ANSWER that confirms the offered ID confirms the set", async () => {
  let confirm = 99;
  const fake = fakeWebTransport(async (stream) => {
    const request = await stream.readRequest();
    await stream.write(
      encodeAnswerFrame({ connectionMetadata: confirm ?? request.connectionMetadata.id }),
    );
    await stream.readMessage(EchoRequestSchema);
    await stream.writeMessage(EchoResponseSchema, { message: "x" });
    await stream.writeStatus();
    await stream.finish();
  });
  const channel = await channelTo(fake, { connectionMetadata: { "interop-suffix": "!" } });
  const client = createClient(Echo, channel);

  await echo(client, "a");
  await echo(client, "b");
  confirm = undefined;
  await echo(client, "c");
  await echo(client, "d");
  assert.deepEqual(
    fake.requests.map((request) => Object.keys(request.connection.metadata).length),
    [1, 1, 1, 0],
  );
  channel.close();
});

test("invalid connection metadata is rejected before and after connecting", async () => {
  const fake = fakeWebTransport();
  await assert.rejects(
    channelTo(fake, { connectionMetadata: { "trevrpc-x": "1" } }),
    (error) => error.code === Code.InvalidArgument,
  );
  assert.equal(fake.sessions.length, 0);

  const channel = await channelTo(fake, { connectionMetadata: { "interop-suffix": "!" } });
  assert.throws(
    () => channel.setConnectionMetadata({ "Bad Key": "1" }),
    (error) => error.code === Code.InvalidArgument,
  );
  assert.equal(await echo(createClient(Echo, channel), "a"), "a!");
  channel.close();
});

/** Sends a REQUEST with an empty message to the test Echo server; returns its final status code. */
async function statusOf(transport, fields) {
  const stream = await transport.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  await writer.write(encodeRequestFrame({ method: "/trevrpc.interop.v1.Echo/Unary", ...fields }));
  await writer.write(encodeMessageFrame(new Uint8Array(0)));
  await writer.close();
  const frames = new FrameReader(stream.readable.getReader());
  for (let frame; (frame = await frames.readFrame()) !== null;) {
    if (frame.type === FrameType.Status) {
      return decodeWireMessage(RpcStatus, frame.payload).code;
    }
  }
  throw new Error("the response ended without a STATUS");
}

test("the echo server stores offered sets per session, newest four, and rejects the rest", async () => {
  const session = {};
  const transport = fakeTransport((stream) => echoHandler(stream, session));
  const offer = (id, key = "x-a") => ({
    connectionMetadata: { id, metadata: { [key]: new Uint8Array([49]) } },
  });
  const reference = (id) => ({ connectionMetadata: { id } });

  assert.equal(await statusOf(transport, reference(1)), Code.InvalidArgument);
  assert.equal(await statusOf(transport, offer(1)), Code.Ok);
  assert.equal(await statusOf(transport, reference(1)), Code.Ok);
  assert.equal(await statusOf(transport, offer(2, "trevrpc-x")), Code.InvalidArgument);
  assert.equal(await statusOf(transport, reference(2)), Code.InvalidArgument);
  for (let id = 2; id <= 5; id += 1) {
    assert.equal(await statusOf(transport, offer(id)), Code.Ok);
  }
  assert.equal(await statusOf(transport, reference(1)), Code.InvalidArgument);
  assert.equal(await statusOf(transport, reference(2)), Code.Ok);
});

test("a late confirmation of a replaced set does not undo the current one's", async () => {
  const { ConnectionMetadataSource } = await import("../src/connection-metadata.js");
  const source = new ConnectionMetadataSource({ k: "1" });
  const session = source.session({});
  const first = session.prepare(source.current).offered;
  source.set({ k: "2" });
  const second = session.prepare(source.current).offered;
  session.learn(second, { connectionMetadata: second });
  // The first call's ANSWER arrives last.
  session.learn(first, { connectionMetadata: first });
  const { field, offered } = session.prepare(source.current);
  assert.equal(offered, 0, "the current set is still confirmed");
  assert.deepEqual(field, { id: second });
});

test("a call whose set was replaced offers it in full, even if the session confirmed it", async () => {
  const { ConnectionMetadataSource } = await import("../src/connection-metadata.js");
  const source = new ConnectionMetadataSource({ k: "1" });
  const session = source.session({});
  const set = source.current;
  const { offered } = session.prepare(set);
  session.learn(offered, { connectionMetadata: offered });
  assert.deepEqual(session.prepare(set).field, { id: set.id });
  source.set({ k: "2" });
  // The server may have dropped a replaced set, so a call that started with it offers it again,
  // and its confirmation does not count.
  const stale = session.prepare(set);
  assert.equal(stale.offered, set.id);
  assert.deepEqual(texts(stale.field.metadata), { k: "1" });
  session.learn(stale.offered, { connectionMetadata: stale.offered });
  assert.equal(session.prepare(source.current).offered, source.current.id);
});

test("a call keeps the set that was current when it started", async () => {
  const fake = fakeWebTransport();
  const channel = createChannel(url, {
    transports: ["webtransport"],
    WebTransport: fake.WebTransport,
    connectionMetadata: { "interop-suffix": "!" },
  });
  const client = createClient(Echo, channel);
  // The call waits for the channel to connect, and the set changes meanwhile.
  const first = echo(client, "a");
  channel.setConnectionMetadata({ "interop-suffix": "#" });
  const second = echo(client, "b");
  assert.deepEqual(await Promise.all([first, second]), ["a!", "b#"]);
  assert.deepEqual(
    fake.requests.map((request) => request.connection),
    [
      { id: 1, metadata: { "interop-suffix": "!" } },
      { id: 2, metadata: { "interop-suffix": "#" } },
    ],
  );
  channel.close();
});

test("a call starts, and takes the current set, once its interceptors ran", async () => {
  const fake = fakeWebTransport();
  const channel = await channelTo(fake, { connectionMetadata: { "interop-suffix": "!" } });
  const client = createClient(Echo, channel, {
    interceptors: [
      async () => {
        await new Promise((resolve) => setTimeout(resolve, 0));
        channel.setConnectionMetadata({ "interop-suffix": "#" });
      },
    ],
  });
  assert.equal(await echo(client, "a"), "a#");
  channel.close();
});

test("clients created on a connecting channel carry its connection metadata", async () => {
  const fake = fakeWebTransport();
  const channel = createChannel(url, {
    transports: ["webtransport"],
    WebTransport: fake.WebTransport,
    connectionMetadata: { "interop-suffix": "!" },
  });
  const client = createClient(Echo, channel);
  assert.equal(await echo(client, "a"), "a!");
  assert.deepEqual(fake.requests[0].connection, { id: 1, metadata: { "interop-suffix": "!" } });
  channel.close();
});

test("a transport that wraps a channel forwards its connection metadata", async () => {
  const fake = fakeWebTransport();
  const channel = await channelTo(fake, { connectionMetadata: { "interop-suffix": "!" } });
  const wrapper = {
    createBidirectionalStream: () => channel.createBidirectionalStream(),
    createBidirectionalStreamWithSession: () => channel.createBidirectionalStreamWithSession(),
    get compression() {
      return channel.compression;
    },
    get connectionMetadata() {
      return channel.connectionMetadata;
    },
    get streamCompressionBudget() {
      return channel.streamCompressionBudget;
    },
  };
  const client = createClient(Echo, wrapper);
  assert.equal(await echo(client, "a"), "a!");
  assert.equal(await echo(client, "b"), "b!");
  // The wrapper and the channel share what each session confirmed.
  assert.equal(await echo(createClient(Echo, channel), "c"), "c!");
  assert.deepEqual(
    fake.requests.map((request) => request.connection),
    [
      { id: 1, metadata: { "interop-suffix": "!" } },
      { id: 1, metadata: {} },
      { id: 1, metadata: {} },
    ],
  );
  channel.close();
});
