// Compiles against the published declarations; tsc checks it, nothing runs it.
import {
  DefaultMaxRecordSize,
  DefaultParameters,
  QMuxError,
  QMuxProtocolError,
  QMuxSession,
  TransportError,
  type Parameters,
  type QMuxBidirectionalStream,
  type QMuxWebSocket,
} from "@trevrpc/qmux";

export async function client(): Promise<void> {
  const session = new QMuxSession("wss://example.com/app", {
    protocols: ["myapp-qmux-02"],
    parameters: { maxDatagramFrameSize: 1200, initialMaxStreamsUni: 0 },
    keepAlivePeriodMs: 10_000,
  });
  await session.ready;
  const stage: "opening" | "handshaking" | "ready" | "closed" = session.stage;
  const peer: Parameters | null = session.peerParameters;
  void stage;
  void peer;

  const stream: QMuxBidirectionalStream = await session.createBidirectionalStream({
    waitUntilAvailable: false,
  });
  const writer = stream.writable.getWriter();
  await writer.write(new Uint8Array([1, 2, 3]));
  await writer.abort(new QMuxError("cancelled", { streamErrorCode: 1 }));
  const reader = stream.readable.getReader();
  const { value } = await reader.read();
  const bytes: Uint8Array | undefined = value;
  void bytes;

  const uni = await session.createUnidirectionalStream();
  await uni.getWriter().close();

  if (session.datagrams !== undefined) {
    const size: number = session.datagrams.maxDatagramSize;
    await session.datagrams.writable.getWriter().write(new Uint8Array(size));
  }

  session.close({ closeCode: 0, reason: "bye" });
  const info = await session.closed;
  const code: number = info.closeCode;
  const reason: string = info.reason;
  void code;
  void reason;
}

export async function server(socket: QMuxWebSocket): Promise<void> {
  const session = new QMuxSession(socket, { role: "server", parameters: DefaultParameters });
  for await (const stream of session.incomingBidirectionalStreams) {
    await stream.readable.pipeTo(stream.writable);
  }
  for await (const readable of session.incomingUnidirectionalStreams) {
    await readable.cancel(new QMuxError("unwanted", { streamErrorCode: 7 }));
  }
}

export function errors(error: unknown): number | null {
  const smallest: number = DefaultMaxRecordSize;
  void smallest;
  if (error instanceof QMuxError && error.source === "stream") {
    return error.streamErrorCode;
  }
  if (error instanceof QMuxProtocolError && error.code === TransportError.FlowControlError) {
    return error.code;
  }
  return null;
}
