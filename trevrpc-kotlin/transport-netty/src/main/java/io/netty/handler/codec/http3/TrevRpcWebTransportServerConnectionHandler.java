/*
 * This file intentionally uses Netty's package. Netty 4.2.16.Final does not expose
 * bidirectional stream initialization from Http3ConnectionHandler (netty/netty#16992),
 * while WebTransport requires demultiplexing raw 0x41 streams before the H3 codec.
 * Keeping this small adapter in the split package avoids reflection and is pinned by
 * compile-time references and regression tests to the exact Netty version.
 */
package io.netty.handler.codec.http3;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.ChannelInputShutdownEvent;
import io.netty.channel.socket.ChannelInputShutdownReadComplete;
import io.netty.handler.codec.quic.QuicStreamChannel;
import io.netty.util.ReferenceCountUtil;

import java.util.function.LongFunction;

public final class TrevRpcWebTransportServerConnectionHandler extends Http3ConnectionHandler {
    public static final long WEBTRANSPORT_BIDIRECTIONAL_STREAM_TYPE = 0x41;
    public static final long WEBTRANSPORT_DRAFT_02_SETTING = 0x2b603742L;
    public static final long WEBTRANSPORT_MAX_SESSIONS_DRAFT_07_SETTING = 0xc671706aL;
    public static final long WEBTRANSPORT_MAX_SESSIONS_SETTING = 0x2c7cf000L;

    private static final Http3Settings.NonStandardHttp3SettingsValidator WEBTRANSPORT_SETTINGS_VALIDATOR =
            (id, value) -> {
                if (id != WEBTRANSPORT_DRAFT_02_SETTING
                        && id != WEBTRANSPORT_MAX_SESSIONS_DRAFT_07_SETTING
                        && id != WEBTRANSPORT_MAX_SESSIONS_SETTING) {
                    return false;
                }
                if (value == null || value != 1L) {
                    throw new IllegalArgumentException("WebTransport setting 0x" + Long.toHexString(id)
                            + " must be 1");
                }
                return true;
            };

    private final ChannelHandler requestStreamHandler;
    private final ChannelHandler rawWebTransportStreamInitializer;

    public TrevRpcWebTransportServerConnectionHandler(
            ChannelHandler requestStreamHandler, ChannelHandler rawWebTransportStreamInitializer) {
        super(true, null, null, webTransportSettings(), true, WEBTRANSPORT_SETTINGS_VALIDATOR,
                null, Http3CodecUtils.DEFAULT_MAX_UNKNOWN_FRAME_PAYLOAD_LENGTH);
        this.requestStreamHandler = requireNonNull(requestStreamHandler, "requestStreamHandler");
        this.rawWebTransportStreamInitializer =
                requireNonNull(rawWebTransportStreamInitializer, "rawWebTransportStreamInitializer");
    }

    public static Http3SettingsFrame webTransportSettings() {
        Http3Settings settings = new Http3Settings(WEBTRANSPORT_SETTINGS_VALIDATOR)
                .qpackMaxTableCapacity(0)
                .qpackBlockedStreams(0)
                .maxFieldSectionSize(8192)
                .enableConnectProtocol(true)
                .enableH3Datagram(true);
        settings.put(WEBTRANSPORT_DRAFT_02_SETTING, 1L);
        settings.put(WEBTRANSPORT_MAX_SESSIONS_DRAFT_07_SETTING, 1L);
        settings.put(WEBTRANSPORT_MAX_SESSIONS_SETTING, 1L);
        return new DefaultHttp3SettingsFrame(settings);
    }

    public static Http3Settings.NonStandardHttp3SettingsValidator webTransportSettingsValidator() {
        return WEBTRANSPORT_SETTINGS_VALIDATOR;
    }

    @Override
    void initBidirectionalStream(ChannelHandlerContext ctx, QuicStreamChannel streamChannel) {
        streamChannel.pipeline().addLast(new FirstVarintDemultiplexer(
                new ChannelInitializer<QuicStreamChannel>() {
                    @Override
                    protected void initChannel(QuicStreamChannel channel) {
                        initializeHttp3RequestStream(channel);
                    }
                }, rawWebTransportStreamInitializer));
    }

    private void initializeHttp3RequestStream(QuicStreamChannel streamChannel) {
        ChannelPipeline pipeline = streamChannel.pipeline();
        Http3RequestStreamEncodeStateValidator encodeStateValidator =
                new Http3RequestStreamEncodeStateValidator();
        Http3RequestStreamDecodeStateValidator decodeStateValidator =
                new Http3RequestStreamDecodeStateValidator();
        pipeline.addLast(newCodec(encodeStateValidator, decodeStateValidator));
        pipeline.addLast(encodeStateValidator);
        pipeline.addLast(decodeStateValidator);
        pipeline.addLast(newRequestStreamValidationHandler(
                streamChannel, encodeStateValidator, decodeStateValidator));
        pipeline.addLast(requestStreamHandler);
    }

    @Override
    void initUnidirectionalStream(ChannelHandlerContext ctx, QuicStreamChannel streamChannel) {
        final long maxTableCapacity = maxTableCapacity();
        streamChannel.pipeline().addLast(new Http3UnidirectionalStreamInboundServerHandler(
                codecFactory, nonStandardSettingsValidator, localControlStreamHandler, remoteControlStreamHandler,
                (LongFunction<ChannelHandler>) null,
                () -> new QpackEncoderHandler(maxTableCapacity, qpackDecoder),
                () -> new QpackDecoderHandler(qpackEncoder)));
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new NullPointerException(name);
        }
        return value;
    }

    public static final class FirstVarintDemultiplexer extends ChannelInboundHandlerAdapter {
        private final ChannelHandler http3Initializer;
        private final ChannelHandler rawWebTransportInitializer;
        private CompositeByteBuf pending;
        private boolean routed;

        public FirstVarintDemultiplexer(
                ChannelHandler http3Initializer, ChannelHandler rawWebTransportInitializer) {
            this.http3Initializer = requireNonNull(http3Initializer, "http3Initializer");
            this.rawWebTransportInitializer =
                    requireNonNull(rawWebTransportInitializer, "rawWebTransportInitializer");
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object message) {
            if (routed || !(message instanceof ByteBuf)) {
                ctx.fireChannelRead(message);
                return;
            }
            ByteBuf bytes = (ByteBuf) message;
            if (!bytes.isReadable()) {
                ReferenceCountUtil.release(bytes);
                return;
            }
            if (pending == null) {
                pending = ctx.alloc().compositeBuffer(8);
            }
            pending.addComponent(true, bytes);
            Long value = firstVarint(pending);
            if (value == null) {
                return;
            }
            route(ctx, value == WEBTRANSPORT_BIDIRECTIONAL_STREAM_TYPE
                    ? rawWebTransportInitializer : http3Initializer);
        }

        private void route(ChannelHandlerContext ctx, ChannelHandler initializer) {
            routed = true;
            ByteBuf replay = pending;
            pending = null;
            ChannelPipeline pipeline = ctx.pipeline();
            try {
                pipeline.addAfter(ctx.name(), null, initializer);
                pipeline.remove(this);
                pipeline.fireChannelRead(replay);
            } catch (Throwable cause) {
                replay.release();
                ctx.close();
                throw cause;
            }
        }

        private static Long firstVarint(ByteBuf bytes) {
            int readable = bytes.readableBytes();
            if (readable == 0) {
                return null;
            }
            int index = bytes.readerIndex();
            int first = bytes.getUnsignedByte(index);
            int length = 1 << (first >>> 6);
            if (readable < length) {
                return null;
            }
            long value = first & 0x3f;
            for (int offset = 1; offset < length; offset++) {
                value = (value << 8) | bytes.getUnsignedByte(index + offset);
            }
            return value;
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object event) {
            if (!routed && (event == ChannelInputShutdownEvent.INSTANCE
                    || event instanceof ChannelInputShutdownReadComplete)) {
                rejectIncomplete(ctx);
            }
            ctx.fireUserEventTriggered(event);
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            releasePending();
            ctx.fireChannelInactive();
        }

        @Override
        public void handlerRemoved(ChannelHandlerContext ctx) {
            releasePending();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            releasePending();
            ctx.close();
        }

        private void rejectIncomplete(ChannelHandlerContext ctx) {
            releasePending();
            if (ctx.channel() instanceof QuicStreamChannel) {
                QuicStreamChannel stream = (QuicStreamChannel) ctx.channel();
                stream.shutdownInput(1);
                stream.shutdownOutput(1);
            }
            ctx.close();
        }

        private void releasePending() {
            if (pending != null) {
                pending.release();
                pending = null;
            }
        }
    }
}
