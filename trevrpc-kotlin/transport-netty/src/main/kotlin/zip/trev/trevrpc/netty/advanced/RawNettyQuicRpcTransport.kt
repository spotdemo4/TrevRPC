package zip.trev.trevrpc.netty.advanced

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelInboundHandlerAdapter
import io.netty.channel.ChannelInitializer
import io.netty.channel.ChannelOption
import io.netty.channel.EventLoopGroup
import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.nio.NioIoHandler
import io.netty.channel.socket.ChannelInputShutdownReadComplete
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.handler.codec.quic.QuicChannel
import io.netty.handler.codec.quic.QuicClientCodecBuilder
import io.netty.handler.codec.quic.QuicCodecBuilder
import io.netty.handler.codec.quic.QuicStreamChannel
import io.netty.handler.codec.quic.QuicStreamType
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.launch
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcResponse
import zip.trev.trevrpc.RpcStreamFrame
import zip.trev.trevrpc.RpcStreamFrameKind
import zip.trev.trevrpc.RpcTransport
import zip.trev.trevrpc.RpcTransportStream
import zip.trev.trevrpc.WireCodec
import zip.trev.trevrpc.netty.HTTP3_ALPN
import zip.trev.trevrpc.netty.NettyQuicClientConfig
import zip.trev.trevrpc.netty.NettyRpcChannel
import zip.trev.trevrpc.netty.NettyTransportOptions
import zip.trev.trevrpc.netty.TrevRpcFrameDecoder
import zip.trev.trevrpc.netty.TrevRpcFrameWriter
import zip.trev.trevrpc.netty.awaitChannel
import zip.trev.trevrpc.netty.awaitCompletion
import zip.trev.trevrpc.netty.awaitValue
import zip.trev.trevrpc.netty.cancelBoth
import zip.trev.trevrpc.netty.closeApplication
import zip.trev.trevrpc.netty.copyReadableBytes
import zip.trev.trevrpc.netty.transportException
import java.net.InetSocketAddress
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.channels.Channel as CoroutineChannel

/** Advanced single-connection native QUIC transport. Prefer [NettyRpcChannel.nativeQuic]. */
class RawNettyQuicRpcTransport private constructor(
    private val group: EventLoopGroup,
    private val datagramChannel: Channel,
    private val quicChannel: QuicChannel,
    private val options: NettyTransportOptions,
    private val dispatcher: CoroutineDispatcher,
) : RpcTransport,
    AutoCloseable {
    private val scope = CoroutineScope(SupervisorJob() + dispatcher)
    private val closed = AtomicBoolean(false)

    override suspend fun unary(request: RpcRequest): RpcResponse {
        checkOpen()
        val inbox = RawFrameInbox(options)
        val stream = openRawStream(inbox)
        try {
            TrevRpcFrameWriter.write(stream, WireCodec.encode(request), options.maxFrameSize).awaitCompletion()
            stream.shutdownOutput().awaitCompletion()
            val body = inbox.receive() ?: throw transportException("native QUIC unary response ended before a frame")
            val response = WireCodec.decodeResponse(body)
            stream.shutdownInput()
            stream.close()
            return response
        } catch (error: CancellationException) {
            stream.cancelBoth()
            throw error
        } catch (error: Throwable) {
            stream.cancelBoth()
            throw transportException("native QUIC unary RPC failed", error)
        }
    }

    override suspend fun openStream(
        request: RpcRequest,
        requestBody: Flow<ByteArray>,
    ): RpcTransportStream {
        checkOpen()
        val inbox = RawFrameInbox(options)
        val stream = openRawStream(inbox)
        val writer =
            scope.launch {
                try {
                    TrevRpcFrameWriter.write(stream, WireCodec.encode(request), options.maxFrameSize).awaitCompletion()
                    requestBody.collect { body ->
                        val frame = WireCodec.encode(RpcStreamFrame.message(body))
                        TrevRpcFrameWriter.write(stream, frame, options.maxFrameSize).awaitCompletion()
                    }
                    stream.shutdownOutput().awaitCompletion()
                } catch (error: Throwable) {
                    stream.cancelBoth()
                    inbox.fail(error)
                }
            }
        return RawRpcTransportStream(stream, inbox, writer)
    }

    suspend fun shutdown() {
        if (!closed.compareAndSet(false, true)) return
        scope.cancel()
        quicChannel.closeApplication(0, "client closed")
        quicChannel.closeFuture().awaitCompletion()
        datagramChannel.close().awaitCompletion()
        group.shutdownGracefully().awaitValue()
    }

    internal suspend fun awaitClosed() {
        quicChannel.closeFuture().awaitCompletion()
    }

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        scope.cancel()
        quicChannel.closeApplication(0, "client closed")
        quicChannel.closeFuture().syncUninterruptibly()
        datagramChannel.close().syncUninterruptibly()
        group.shutdownGracefully().syncUninterruptibly()
    }

    private suspend fun openRawStream(inbox: RawFrameInbox): QuicStreamChannel =
        quicChannel
            .newStreamBootstrap()
            .type(QuicStreamType.BIDIRECTIONAL)
            .option(ChannelOption.WRITE_BUFFER_WATER_MARK, options.waterMark)
            .handler(inbox.initializer())
            .create()
            .awaitValue()

    private fun checkOpen() {
        if (closed.get()) throw transportException("native QUIC transport is closed")
    }

    companion object {
        suspend fun connect(config: NettyQuicClientConfig): RawNettyQuicRpcTransport {
            val dispatcher = Dispatchers.IO.limitedParallelism(config.options.workerParallelism)
            val endpoint = connectQuic(config, zip.trev.trevrpc.ALPN, ChannelInboundHandlerAdapter())
            return RawNettyQuicRpcTransport(
                endpoint.group,
                endpoint.datagramChannel,
                endpoint.quicChannel,
                config.options,
                dispatcher,
            )
        }
    }
}

internal data class ClientQuicEndpoint(
    val group: EventLoopGroup,
    val datagramChannel: Channel,
    val quicChannel: QuicChannel,
)

internal suspend fun connectQuic(
    config: NettyQuicClientConfig,
    protocol: String,
    connectionHandler: io.netty.channel.ChannelHandler,
): ClientQuicEndpoint {
    val group = MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())
    var datagram: Channel? = null
    try {
        val sslContext = config.tls.context(protocol)
        val receiveWindow = config.options.maxFrameSize.toLong() + 4
        val builder: QuicCodecBuilder<*> =
            if (protocol == HTTP3_ALPN) {
                io.netty.handler.codec.http3.Http3
                    .newQuicClientCodecBuilder()
            } else {
                QuicClientCodecBuilder()
            }
        val codec =
            builder
                .sslEngineProvider { channel ->
                    sslContext.newEngine(
                        channel.alloc(),
                        config.tls.serverName,
                        config.remoteAddress.port,
                    )
                }.maxIdleTimeout(config.options.maxIdleTime.inWholeMilliseconds, TimeUnit.MILLISECONDS)
                .initialMaxData(receiveWindow * 64)
                .initialMaxStreamDataBidirectionalLocal(receiveWindow)
                .build()
        datagram =
            Bootstrap()
                .group(group)
                .channel(NioDatagramChannel::class.java)
                .handler(codec)
                .bind(InetSocketAddress(0))
                .awaitChannel()
        val quic =
            QuicChannel
                .newBootstrap(datagram)
                .handler(connectionHandler)
                .streamHandler(
                    object : ChannelInboundHandlerAdapter() {
                        override fun channelActive(context: ChannelHandlerContext) {
                            (context.channel() as QuicStreamChannel).cancelBoth()
                        }
                    },
                ).remoteAddress(config.remoteAddress)
                .connect()
                .awaitValue()
        return ClientQuicEndpoint(group, datagram, quic)
    } catch (error: CancellationException) {
        runCatching { datagram?.close()?.syncUninterruptibly() }
        runCatching { group.shutdownGracefully().syncUninterruptibly() }
        throw error
    } catch (error: Throwable) {
        datagram?.close()?.syncUninterruptibly()
        group.shutdownGracefully().syncUninterruptibly()
        throw transportException("failed to connect QUIC transport", error)
    }
}

internal class RawFrameInbox(
    private val options: NettyTransportOptions,
) {
    private val frames = CoroutineChannel<ByteArray>(options.inboundQueueCapacity)

    fun initializer(): ChannelInitializer<QuicStreamChannel> =
        object : ChannelInitializer<QuicStreamChannel>() {
            override fun initChannel(channel: QuicStreamChannel) {
                channel.pipeline().addLast(TrevRpcFrameDecoder(options.maxFrameSize), handler())
            }
        }

    fun handler(): ChannelInboundHandlerAdapter =
        object : ChannelInboundHandlerAdapter() {
            override fun channelRead(
                context: ChannelHandlerContext,
                message: Any,
            ) {
                val body = message as ByteBuf
                try {
                    if (frames.trySend(body.copyReadableBytes()).isFailure) {
                        fail(transportException("inbound frame queue is full"))
                        (context.channel() as? QuicStreamChannel)?.cancelBoth()
                    }
                } finally {
                    body.release()
                }
            }

            override fun userEventTriggered(
                context: ChannelHandlerContext,
                event: Any,
            ) {
                if (event is ChannelInputShutdownReadComplete) frames.close()
                context.fireUserEventTriggered(event)
            }

            override fun channelInactive(context: ChannelHandlerContext) {
                frames.close()
            }

            override fun exceptionCaught(
                context: ChannelHandlerContext,
                cause: Throwable,
            ) {
                fail(cause)
                context.close()
            }
        }

    suspend fun receive(): ByteArray? {
        val result = frames.receiveCatching()
        result.exceptionOrNull()?.let { throw it }
        return result.getOrNull()
    }

    fun fail(error: Throwable) {
        frames.close(error)
    }
}

internal class RawRpcTransportStream(
    private val stream: QuicStreamChannel,
    private val inbox: RawFrameInbox,
    private val writer: Job,
) : RpcTransportStream {
    private val closed = AtomicBoolean(false)

    override suspend fun receive(): RpcStreamFrame? {
        val body = inbox.receive() ?: return null
        val frame = WireCodec.decodeStreamFrame(body)
        if (frame.kind == RpcStreamFrameKind.STATUS && closed.compareAndSet(false, true)) {
            writer.cancel()
            stream.shutdownInput()
            stream.close()
        }
        return frame
    }

    override suspend fun close(cause: Throwable?) {
        if (!closed.compareAndSet(false, true)) return
        writer.cancel(CancellationException("RPC stream closed", cause))
        stream.cancelBoth()
        stream.close().awaitCompletion()
    }
}
