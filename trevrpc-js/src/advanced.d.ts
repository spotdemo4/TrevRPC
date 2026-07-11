import type {
  ResolvedCallOptions,
  RpcRequestMessage,
  RpcResponseMessage,
  RpcStreamFrameMessage,
  Transport,
  WebTransportBidirectionalStreamLike,
  WebTransportCloseInfoLike,
  WebTransportOptions,
  WebTransportSessionLike,
} from "./index.js";

/** Raw single-session TrevRPC transport for browser WebTransport. */
export class RawWebTransport implements Transport {
  session: WebTransportSessionLike;
  maxFrameSize: number;

  constructor(session: WebTransportSessionLike, options?: WebTransportOptions);
  static connect(url: string | URL, options?: WebTransportOptions): Promise<RawWebTransport>;
  ready(): Promise<void>;
  close(closeInfo?: WebTransportCloseInfoLike): void;
  call(request: RpcRequestMessage, options?: ResolvedCallOptions): Promise<RpcResponseMessage>;
  streamingCall(
    request: RpcRequestMessage,
    requestBody: AsyncIterable<Uint8Array>,
    options?: ResolvedCallOptions,
  ): Promise<AsyncIterableIterator<RpcStreamFrameMessage>>;
  openBidirectionalStream(): Promise<WebTransportBidirectionalStreamLike>;
}
