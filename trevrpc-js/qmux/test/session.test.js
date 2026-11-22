// A client session against a scripted peer, which checks what goes on the wire.
import assert from "node:assert/strict";
import test from "node:test";

import { QMuxError, TransportError } from "../src/errors.js";
import {
  FrameType,
  writeDatagram,
  writeMaxStreamData,
  writeMaxStreams,
  writePing,
  writeResetStream,
  writeStopSending,
  writeStreamFrame,
} from "../src/frames.js";
import { QMuxSession } from "../src/session.js";
import {
  PeerParameters,
  Protocol,
  frames,
  openScripted,
  flushMicrotasks,
  readAll,
  scriptedPeer,
} from "./support.js";

/** A peer that records frames but never echoes stream data. */
function silentPeer(parameters = PeerParameters) {
  return scriptedPeer({ parameters, onFrame: (_socket, frame) => frame.data !== undefined });
}

/** Parameters whose 4-byte stream window leaves a larger write blocked on flow control. */
const BlockedParameters = { ...PeerParameters, initialMaxStreamDataBidiRemote: 4 };

const withCode = (code) => new QMuxError("cancelled", { streamErrorCode: code });

function lastClose(socket) {
  return socket.received.findLast((frame) => frame.type === FrameType.ConnectionClose);
}

const isStream = (end) => (frame) =>
  frame.data !== undefined && frame.offset + frame.data.length === end;
const isReset = (code, finalSize) => (frame) =>
  frame.type === FrameType.ResetStream && frame.code === code && frame.finalSize === finalSize;
const isStop = (code) => (frame) => frame.type === FrameType.StopSending && frame.code === code;

/** The timers the session uses, mocked so that a test moves the clock itself. */
const MockedTimers = { apis: ["setInterval", "setTimeout", "Date"] };

test("a session sends its transport parameters first and echoes streams", async () => {
  const peer = scriptedPeer();
  const session = await openScripted(peer, {
    parameters: { initialMaxStreamsBidi: 0, initialMaxStreamsUni: 0 },
  });
  const [socket] = peer.sockets;
  assert.deepEqual(socket.protocols, [Protocol]);
  assert.equal(socket.binaryType, "arraybuffer");
  assert.equal(socket.received[0].type, FrameType.TransportParameters);
  assert.equal(session.protocol, Protocol);
  assert.equal(session.peerParameters.initialMaxStreamsBidi, 4);

  const payload = Uint8Array.from({ length: 200_000 }, (_, i) => i % 251);
  const stream = await session.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  const written = writer.write(payload).then(() => writer.close());
  assert.deepEqual(await readAll(stream.readable), payload);
  await written;
  session.close();
  assert.deepEqual(await session.closed, { closeCode: 0, reason: "" });
});

test("writes wait for flow control credit", async () => {
  let limit = 1000;
  let received = 0;
  const peer = scriptedPeer({
    parameters: { ...PeerParameters, initialMaxStreamDataBidiRemote: limit },
    onFrame(socket, frame) {
      if (frame.data === undefined) {
        return false;
      }
      assert.ok(frame.offset + frame.data.length <= limit, "data beyond the granted limit");
      received += frame.data.length;
      // The peer reads everything at once and grants another window.
      limit = received + 1000;
      socket.deliver(frames((w) => writeMaxStreamData(w, frame.streamId, limit)));
      return true;
    },
  });
  const session = await openScripted(peer);
  const stream = await session.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  await writer.write(new Uint8Array(10_000));
  await writer.close();
  assert.equal(received, 10_000);
  session.close();
});

test("cancel and abort send STOP_SENDING and RESET_STREAM with the reason's code", async () => {
  for (const [reason, code] of [
    [withCode(7), 7],
    [new Error("no code"), 0],
    [undefined, 0],
  ]) {
    const peer = silentPeer();
    const session = await openScripted(peer);
    const stream = await session.createBidirectionalStream();
    const writer = stream.writable.getWriter();
    await writer.write(Uint8Array.of(1, 2, 3));
    await stream.readable.getReader().cancel(reason);
    await writer.abort(reason);
    await peer.sockets[0].waitFor(isStop(code));
    await peer.sockets[0].waitFor(isReset(code, 3));
    session.close();
  }
});

test("aborting the writer resets a stream whose write is blocked on flow control", async () => {
  const peer = silentPeer({ ...PeerParameters, initialMaxStreamDataBidiRemote: 100 });
  const session = await openScripted(peer);
  const stream = await session.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  const blocked = writer.write(new Uint8Array(1000));
  // The first 100 bytes go out, and the rest waits for credit that never comes.
  await peer.sockets[0].waitFor(isStream(100));
  // The write is in flight while the abort arrives; both must settle anyway.
  await writer.abort(withCode(1));
  await assert.rejects(blocked);
  await peer.sockets[0].waitFor(isReset(1, 100));
  session.close();
});

test("STOP_SENDING fails a write blocked on flow control", async () => {
  const peer = silentPeer(BlockedParameters);
  const session = await openScripted(peer);
  const stream = await session.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  const blocked = writer.write(new Uint8Array(100));
  const [socket] = peer.sockets;
  await socket.waitFor(isStream(4));
  socket.deliver(frames((w) => writeStopSending(w, 0, 7)));
  await assert.rejects(blocked, (error) => error.streamErrorCode === 7);
  await socket.waitFor(isReset(7, 4));
  session.close();
});

test("abort() settles a write that STOP_SENDING left blocked on flow control", async () => {
  const peer = silentPeer(BlockedParameters);
  const session = await openScripted(peer);
  const stream = await session.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  const blocked = writer.write(new Uint8Array(100)).catch(() => {});
  const [socket] = peer.sockets;
  await socket.waitFor(isStream(4));
  socket.deliver(frames((w) => writeStopSending(w, 0, 7)));
  await socket.waitFor(isReset(7, 4));
  // The writable may still be erroring, in which case abort() rejects with the stop.
  await writer.abort(new Error("done")).catch((error) => assert.equal(error.streamErrorCode, 7));
  await blocked;
  session.close();
});

// RESET_STREAM fixes the final size, so no STREAM frame may follow it on that stream.
test("a write followed at once by abort() sends no STREAM data after RESET_STREAM", async () => {
  const peer = silentPeer();
  const session = await openScripted(peer);
  const stream = await session.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  const written = writer.write(Uint8Array.of(1, 2, 3)).catch(() => {});
  const aborted = writer.abort(new Error("done")).catch(() => {});
  await Promise.all([written, aborted]);
  // Whatever the session queued by now is on the wire once the peer's ping is answered.
  await peer.sockets[0].roundTrip();
  const onWire = peer.sockets[0].received.filter(
    (frame) => frame.type === FrameType.ResetStream || frame.data !== undefined,
  );
  const resetAt = onWire.findIndex((frame) => frame.type === FrameType.ResetStream);
  assert.notEqual(resetAt, -1, "RESET_STREAM was sent");
  assert.deepEqual(
    onWire
      .slice(resetAt + 1)
      .filter((frame) => frame.data !== undefined)
      .map((frame) => [frame.offset, frame.data.length]),
    [],
    "STREAM frames after RESET_STREAM",
  );
  session.close();
});

test("a peer cannot move the final size of a stream", async () => {
  for (const second of [
    (w) => writeStreamFrame(w, 0, 3, Uint8Array.of(9), false),
    (w) => writeResetStream(w, 0, 1, 4),
  ]) {
    const peer = silentPeer();
    const session = await openScripted(peer);
    const stream = await session.createBidirectionalStream();
    const reading = stream.readable
      .getReader()
      .read()
      .catch(() => {});
    const [socket] = peer.sockets;
    socket.deliver(frames((w) => writeStreamFrame(w, 0, 0, Uint8Array.of(1, 2, 3), false)));
    socket.deliver(frames((w) => writeResetStream(w, 0, 1, 3)));
    socket.deliver(frames(second));
    await assert.rejects(session.closed, /protocol error/);
    await reading;
    assert.equal(lastClose(socket)?.code, TransportError.FinalSizeError);
  }
});

/** Sends a FIN and reads the peer's, so the stream is complete and its slot released. */
async function releaseStream(session, socket) {
  const stream = await session.createBidirectionalStream();
  await stream.writable.getWriter().close();
  socket.deliver(frames((w) => writeStreamFrame(w, 0, 0, Uint8Array.of(1), true)));
  await readAll(stream.readable);
}

test("STREAM data for a released stream is a FINAL_SIZE_ERROR", async () => {
  const peer = silentPeer();
  const session = await openScripted(peer);
  const [socket] = peer.sockets;
  await releaseStream(session, socket);
  socket.deliver(frames((w) => writeStreamFrame(w, 0, 1, Uint8Array.of(2), false)));
  await assert.rejects(session.closed, /protocol error/);
  assert.equal(lastClose(socket)?.code, TransportError.FinalSizeError);
});

test("an empty frame for a released stream is ignored", async () => {
  const peer = silentPeer();
  const session = await openScripted(peer);
  const [socket] = peer.sockets;
  await releaseStream(session, socket);
  socket.deliver(
    frames((w) => {
      writeStreamFrame(w, 0, 1, new Uint8Array(0), true);
      writeResetStream(w, 0, 7, 1);
      writeStopSending(w, 0, 7);
    }),
  );
  await socket.roundTrip();
  assert.equal(session.stage, "ready");
  session.close();
});

test("a connection that drops fails every pending read, write and stream open", async () => {
  const peer = silentPeer({
    ...PeerParameters,
    initialMaxStreamDataBidiRemote: 100,
    initialMaxStreamsBidi: 1,
  });
  const session = await openScripted(peer);
  const stream = await session.createBidirectionalStream();
  const settled = (promise) =>
    promise.then(
      () => "succeeded",
      (error) => error.message,
    );
  const read = settled(stream.readable.getReader().read());
  const write = settled(stream.writable.getWriter().write(new Uint8Array(1000)));
  const open = settled(session.createBidirectionalStream());
  // The write sent what its window allows and waits for more.
  await peer.sockets[0].waitFor(isStream(100));

  peer.sockets[0].close(1006);

  for (const [name, result] of Object.entries({ read, write, open })) {
    assert.match(await result, /WebSocket closed with code 1006/u, name);
  }
  await assert.rejects(session.closed, (error) => error.source === "session");
});

test("a peer reset errors the readable and STOP_SENDING errors the writable", async () => {
  const peer = scriptedPeer({
    onFrame(socket, frame) {
      if (frame.data !== undefined) {
        socket.deliver(
          frames((w) => {
            writeResetStream(w, frame.streamId, 7, 0);
            writeStopSending(w, frame.streamId, 9);
          }),
        );
        return true;
      }
      return false;
    },
  });
  const session = await openScripted(peer);
  const stream = await session.createBidirectionalStream();
  const writer = stream.writable.getWriter();
  await writer.write(Uint8Array.of(1));
  await assert.rejects(
    readAll(stream.readable),
    (error) =>
      error.name === "WebTransportError" &&
      error.source === "stream" &&
      error.streamErrorCode === 7,
  );
  await assert.rejects(writer.closed, (error) => error.streamErrorCode === 9);
  const resets = peer.sockets[0].received.filter((frame) => frame.type === FrameType.ResetStream);
  assert.deepEqual(
    resets.map((frame) => frame.code),
    [9],
    "STOP_SENDING is answered with a reset",
  );
  session.close();
});

test("a text message is a protocol error", async () => {
  const peer = scriptedPeer();
  const session = await openScripted(peer);
  peer.sockets[0].onmessage({ data: "hello" });
  await assert.rejects(session.closed, /protocol error/);
  assert.equal(lastClose(peer.sockets[0])?.code, TransportError.ProtocolViolation);
});

test("pings are answered", async () => {
  const peer = scriptedPeer();
  const session = await openScripted(peer);
  peer.sockets[0].deliver(frames((w) => writePing(w, FrameType.Ping, 3)));
  await peer.sockets[0].waitFor(
    (frame) => frame.type === FrameType.PingResponse && frame.sequence === 3,
  );
  session.close();
});

test("a first ping may have sequence 0, and later ones must increase", async () => {
  const peer = scriptedPeer();
  const session = await openScripted(peer);
  peer.sockets[0].deliver(frames((w) => writePing(w, FrameType.Ping, 0)));
  await peer.sockets[0].waitFor(
    (frame) => frame.type === FrameType.PingResponse && frame.sequence === 0,
  );

  peer.sockets[0].deliver(frames((w) => writePing(w, FrameType.Ping, 0)));
  await assert.rejects(session.closed, /QX_PING sequence is not increasing/);
  assert.equal(lastClose(peer.sockets[0])?.code, TransportError.ProtocolViolation);
});

test("a ping response that was never requested is a protocol error", async () => {
  const peer = scriptedPeer();
  const session = await openScripted(peer);
  peer.sockets[0].deliver(frames((w) => writePing(w, FrameType.PingResponse, 0)));
  await assert.rejects(session.closed, /never requested/);
});

test("keep-alive pings go out while the session is idle", async (t) => {
  t.mock.timers.enable(MockedTimers);
  const peer = scriptedPeer({
    onFrame(socket, frame) {
      if (frame.type === FrameType.Ping) {
        socket.deliver(frames((w) => writePing(w, FrameType.PingResponse, frame.sequence)));
        return true;
      }
      return false;
    },
  });
  const session = await openScripted(peer, { keepAlivePeriodMs: 20 });
  const pings = () =>
    peer.sockets[0].received
      .filter((frame) => frame.type === FrameType.Ping)
      .map((frame) => frame.sequence);
  t.mock.timers.tick(19);
  assert.deepEqual(pings(), []);
  for (const expected of [[0], [0, 1], [0, 1, 2]]) {
    t.mock.timers.tick(expected.length === 1 ? 1 : 20);
    // The response to each ping arrives, and is no protocol error.
    await flushMicrotasks();
    assert.deepEqual(pings(), expected);
  }
  assert.equal(session.stage, "ready");
  session.close();
});

test("the session closes after the smaller idle timeout", async (t) => {
  t.mock.timers.enable(MockedTimers);
  const peer = scriptedPeer({ parameters: { ...PeerParameters, maxIdleTimeoutMs: 10_000 } });
  const session = await openScripted(peer, { parameters: { maxIdleTimeoutMs: 40 } });
  t.mock.timers.tick(39);
  assert.equal(session.stage, "ready");
  t.mock.timers.tick(1);
  await assert.rejects(session.closed, /idle timeout/);
});

test("a server that selects another subprotocol is refused", async () => {
  const peer = scriptedPeer({ protocol: "chat" });
  const session = new QMuxSession("wss://example.test/qmux", {
    protocols: [Protocol],
    WebSocket: peer.WebSocket,
  });
  await assert.rejects(session.ready, /subprotocol "chat"/);
});

test("the handshake times out when the peer sends no transport parameters", async (t) => {
  t.mock.timers.enable(MockedTimers);
  const peer = scriptedPeer({
    onFrame: (_socket, frame) => frame.type === FrameType.TransportParameters,
  });
  const session = new QMuxSession("wss://example.test/qmux", {
    WebSocket: peer.WebSocket,
    handshakeTimeoutMs: 30,
  });
  await peer.sockets[0].waitFor((frame) => frame.type === FrameType.TransportParameters);
  t.mock.timers.tick(29);
  assert.equal(session.stage, "handshaking");
  t.mock.timers.tick(1);
  await assert.rejects(session.ready, /handshake timed out/);
  assert.equal(session.stage, "closed");
});

test("stream limits: waitUntilAvailable false fails, and MAX_STREAMS wakes waiting opens", async () => {
  const peer = silentPeer({ ...PeerParameters, initialMaxStreamsBidi: 1 });
  const session = await openScripted(peer);
  await session.createBidirectionalStream();
  await assert.rejects(
    session.createBidirectionalStream({ waitUntilAvailable: false }),
    (error) => error.name === "QuotaExceededError",
  );
  await assert.rejects(session.createUnidirectionalStream({ waitUntilAvailable: false }), {
    name: "QuotaExceededError",
  });
  let opened = false;
  const waiting = session.createBidirectionalStream().then(() => {
    opened = true;
  });
  // Once the session answered a later ping, it has seen everything the peer sent: no credit yet.
  await peer.sockets[0].roundTrip();
  assert.equal(opened, false);
  peer.sockets[0].deliver(frames((w) => writeMaxStreams(w, true, 2)));
  await waiting;
  session.close();
});

test("frames on the wrong side of a stream are STREAM_STATE_ERRORs", async () => {
  const parameters = { ...PeerParameters, initialMaxStreamsUni: 1, initialMaxStreamDataUni: 100 };
  for (const [name, write] of [
    // Stream 2 is the client's first unidirectional stream: it only sends.
    ["STREAM on a send-only stream", (w) => writeStreamFrame(w, 2, 0, Uint8Array.of(1), false)],
    ["RESET_STREAM on a send-only stream", (w) => writeResetStream(w, 2, 0, 0)],
    // Stream 3 is the server's first unidirectional stream: the client only receives it.
    ["STOP_SENDING on a receive-only stream", (w) => writeStopSending(w, 3, 0)],
    ["MAX_STREAM_DATA on a receive-only stream", (w) => writeMaxStreamData(w, 3, 10)],
    // Stream 4 is the client's second bidirectional stream, which it never opened.
    ["MAX_STREAM_DATA on an unopened stream", (w) => writeMaxStreamData(w, 4, 10)],
  ]) {
    const peer = silentPeer(parameters);
    const session = await openScripted(peer, { parameters: { initialMaxStreamsUni: 1 } });
    await session.createUnidirectionalStream();
    peer.sockets[0].deliver(frames(write));
    await assert.rejects(session.closed, /protocol error/, name);
    assert.equal(lastClose(peer.sockets[0])?.code, TransportError.StreamStateError, name);
  }
});

test("a peer stream beyond the advertised limit is a STREAM_LIMIT_ERROR", async () => {
  // Stream 9 is the server's third bidirectional stream; stream 3 its first unidirectional one.
  for (const id of [9, 3]) {
    const peer = silentPeer();
    const session = await openScripted(peer, {
      parameters: { initialMaxStreamsBidi: 2, initialMaxStreamsUni: 0 },
    });
    peer.sockets[0].deliver(frames((w) => writeStreamFrame(w, id, 0, Uint8Array.of(1), false)));
    await assert.rejects(session.closed, /exceeds the stream limit/, `stream ${id}`);
    assert.equal(lastClose(peer.sockets[0])?.code, TransportError.StreamLimitError, `stream ${id}`);
  }
});

test("a datagram after the readable was cancelled is dropped, and the rest of its record handled", async () => {
  const peer = silentPeer();
  const session = await openScripted(peer, { parameters: { maxDatagramFrameSize: 1200 } });
  const stream = await session.createBidirectionalStream();
  const reader = session.datagrams.readable.getReader();
  // A read waits for the next datagram when the readable is cancelled.
  const pending = reader.read();
  await reader.cancel();
  assert.deepEqual(await pending, { value: undefined, done: true });
  peer.sockets[0].deliver(
    frames((w) => {
      writeDatagram(w, Uint8Array.of(1));
      writeStreamFrame(w, 0, 0, Uint8Array.of(7, 8), true);
    }),
  );
  assert.deepEqual(await readAll(stream.readable), Uint8Array.of(7, 8));
  assert.equal(session.stage, "ready");
  session.close();
});
