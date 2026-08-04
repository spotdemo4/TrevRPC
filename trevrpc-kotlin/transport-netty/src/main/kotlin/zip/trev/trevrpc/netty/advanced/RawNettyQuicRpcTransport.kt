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
import io.netty.channel.socket.ChannelInputShutdownEvent
import io.netty.channel.socket.ChannelInputShutdownReadComplete
import io.netty.channel.socket.SocketProtocolFamily
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.handler.codec.quic.QuicChannel
import io.netty.handler.codec.quic.QuicClientCodecBuilder
import io.netty.handler.codec.quic.QuicCodecBuilder
import io.netty.handler.codec.quic.QuicStreamChannel
import io.netty.handler.codec.quic.QuicStreamType
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeoutOrNull
import zip.trev.trevrpc.RpcClientStream
import zip.trev.trevrpc.RpcKind
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcResponse
import zip.trev.trevrpc.RpcStreamFrame
import zip.trev.trevrpc.RpcStreamFrameKind
import zip.trev.trevrpc.RpcTransport
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.WireCodec
import zip.trev.trevrpc.netty.HTTP3_ALPN
import zip.trev.trevrpc.netty.NettyQuicClientConfig
import zip.trev.trevrpc.netty.NettyRpcChannel
import zip.trev.trevrpc.netty.NettyShutdownCoordinator
import zip.trev.trevrpc.netty.NettyTransportOptions
import zip.trev.trevrpc.netty.TrevRpcFrameDecoder
import zip.trev.trevrpc.netty.TrevRpcFrameWriter
import zip.trev.trevrpc.netty.awaitChannel
import zip.trev.trevrpc.netty.awaitCompletion
import zip.trev.trevrpc.netty.awaitValue
import zip.trev.trevrpc.netty.cancelAndAwaitReset
import zip.trev.trevrpc.netty.cancelAndClose
import zip.trev.trevrpc.netty.cancelBoth
import zip.trev.trevrpc.netty.closeApplication
import zip.trev.trevrpc.netty.copyReadableBytes
import zip.trev.trevrpc.netty.finishAndClose
import zip.trev.trevrpc.netty.isOwnedEventLoopThread
import zip.trev.trevrpc.netty.runBoundedNonCancellableCleanup
import zip.trev.trevrpc.netty.shutdownNow
import zip.trev.trevrpc.netty.shutdownOwnedNettyResources
import zip.trev.trevrpc.netty.transportException
import java.net.Inet6Address
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.time.Duration
import kotlinx.coroutines.channels.Channel as CoroutineChannel

/** Advanced single-connection native QUIC transport. Prefer [NettyRpcChannel.nativeQuic]. */
class RawNettyQuicRpcTransport private constructor(
    private val group: EventLoopGroup,
    private val datagramChannel: Channel,
    private val quicChannel: QuicChannel,
    private val options: NettyTransportOptions,
    private val onTerminalFrame: () -> Unit,
) : RpcTransport,
    AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val streams = ConcurrentHashMap<QuicStreamChannel, (Throwable) -> Unit>()
    private val shutdownCoordinator =
        NettyShutdownCoordinator {
            shutdownOwnedNettyResources(
                group = group,
                timeout = options.shutdownTimeout,
                description = "native QUIC transport",
                forceClose = {
                    streams.keys.forEach { it.close() }
                    quicChannel.close()
                    datagramChannel.close()
                },
            ) {
                closed.set(true)
                val failure = transportException("native QUIC transport is shutting down")
                streams.toMap().forEach { (stream, fail) ->
                    fail(failure)
                    stream.cancelBoth()
                    stream.close()
                }
                quicChannel.closeApplication(0, "client closed")
                quicChannel.closeFuture().awaitCompletion()
                datagramChannel.close().awaitCompletion()
            }
        }

    override suspend fun unary(request: RpcRequest): RpcResponse {
        checkOpen()
        val inbox = RawFrameInbox(options)
        val stream = openRawStream(inbox)
        try {
            writeNativeUnaryRequest(stream, request, options.maxFrameSize)
            val body = inbox.receive() ?: throw transportException("native QUIC unary response ended before a frame")
            val response = WireCodec.decodeResponse(body)
            when (
                val end =
                    inbox.awaitEnd(options.maxIdleTime) {
                        stream.cancelBoth()
                        stream.close()
                    }
            ) {
                TerminalStreamEnd.Clean -> {
                    stream.finishAndClose(options.shutdownTimeout, outputFinSubmitted = true)
                }

                is TerminalStreamEnd.TrailingData -> {
                    throw end.error
                }

                is TerminalStreamEnd.MissingFin -> {
                    stream.cancelAndClose()
                    if (response.status.isOk) throw end.error
                }
            }
            return response
        } catch (error: CancellationException) {
            stream.cancelAndAwaitReset(options.shutdownTimeout)
            throw error
        } catch (error: Throwable) {
            stream.cancelAndClose()
            throw transportException("native QUIC unary RPC failed", error)
        }
    }

    override suspend fun openStream(request: RpcRequest): RpcClientStream {
        checkOpen()
        val inbox = RawFrameInbox(options)
        val stream = openRawStream(inbox)
        val closed = AtomicBoolean(false)
        val requestSendFinished = request.kind == RpcKind.SERVER_STREAMING
        try {
            val encodedRequest = WireCodec.encode(request)
            val write =
                if (requestSendFinished) {
                    TrevRpcFrameWriter.writeFinal(stream, encodedRequest, options.maxFrameSize)
                } else {
                    TrevRpcFrameWriter.write(stream, encodedRequest, options.maxFrameSize)
                }
            write.awaitCompletion()
            return RawRpcTransportStream(
                stream,
                inbox,
                closed,
                options,
                sendRequestEndFrame = !requestSendFinished,
                requestSendFinished = requestSendFinished,
                onTerminalFrame = onTerminalFrame,
            )
        } catch (error: CancellationException) {
            stream.cancelAndAwaitReset(options.shutdownTimeout)
            throw error
        } catch (error: Throwable) {
            stream.cancelAndClose()
            throw transportException("failed to open native QUIC RPC stream", error)
        }
    }

    suspend fun shutdown() {
        closed.set(true)
        shutdownCoordinator.shutdown()
    }

    internal suspend fun awaitClosed() {
        quicChannel.closeFuture().awaitCompletion()
    }

    override fun close() {
        check(!group.isOwnedEventLoopThread()) {
            "RawNettyQuicRpcTransport.close() cannot run on its owned Netty event loop; use shutdown()"
        }
        runBlocking { shutdown() }
    }

    private suspend fun openRawStream(inbox: RawFrameInbox): QuicStreamChannel {
        val stream =
            quicChannel
                .newStreamBootstrap()
                .type(QuicStreamType.BIDIRECTIONAL)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, options.waterMark)
                .handler(inbox.initializer())
                .create()
                .awaitValue { rejectedStream ->
                    rejectedStream.cancelBoth()
                    rejectedStream.close()
                }
        streams[stream] = inbox::fail
        stream.closeFuture().addListener { streams.remove(stream) }
        if (closed.get()) {
            streams.remove(stream)
            inbox.fail(transportException("native QUIC transport is closed"))
            stream.cancelAndClose()
            throw transportException("native QUIC transport is closed")
        }
        return stream
    }

    private fun checkOpen() {
        if (closed.get()) throw transportException("native QUIC transport is closed")
    }

    companion object {
        suspend fun connect(config: NettyQuicClientConfig): RawNettyQuicRpcTransport = connect(config) {}

        internal suspend fun connect(
            config: NettyQuicClientConfig,
            onTerminalFrame: () -> Unit,
        ): RawNettyQuicRpcTransport {
            val endpoint = connectQuic(config, zip.trev.trevrpc.ALPN, ChannelInboundHandlerAdapter())
            return RawNettyQuicRpcTransport(
                endpoint.group,
                endpoint.datagramChannel,
                endpoint.quicChannel,
                config.options,
                onTerminalFrame,
            )
        }
    }
}

internal suspend fun writeNativeUnaryRequest(
    channel: Channel,
    request: RpcRequest,
    maxFrameSize: Int,
) {
    TrevRpcFrameWriter.writeFinal(channel, WireCodec.encode(request), maxFrameSize).awaitCompletion()
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
    val protocolFamily =
        if (config.remoteAddress.address is Inet6Address) SocketProtocolFamily.INET6 else SocketProtocolFamily.INET
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
                .channelFactory { NioDatagramChannel(protocolFamily) }
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
                .awaitValue { rejectedChannel ->
                    rejectedChannel.closeApplication(1, "connection cancelled")
                    rejectedChannel.close()
                }
        return ClientQuicEndpoint(group, datagram, quic)
    } catch (error: CancellationException) {
        cleanupFailedQuicConnect(datagram, group, config.options.shutdownTimeout, error)
        throw error
    } catch (error: Throwable) {
        cleanupFailedQuicConnect(datagram, group, config.options.shutdownTimeout, error)
        throw transportException("failed to connect QUIC transport", error)
    }
}

private suspend fun cleanupFailedQuicConnect(
    datagramChannel: Channel?,
    group: EventLoopGroup,
    timeout: Duration,
    original: Throwable,
) {
    val cleanupFailure =
        runCatching {
            runBoundedNonCancellableCleanup(
                timeout = timeout,
                description = "QUIC connect",
                cleanup = {
                    datagramChannel?.close()?.awaitCompletion()
                    group.shutdownNow(timeout)
                },
                forceCleanup = {
                    datagramChannel?.close()
                    group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS)
                },
            )
        }.exceptionOrNull()
    if (cleanupFailure != null && cleanupFailure !== original) original.addSuppressed(cleanupFailure)
}

internal sealed interface TerminalStreamEnd {
    data object Clean : TerminalStreamEnd

    data class MissingFin(
        val error: Throwable,
    ) : TerminalStreamEnd

    data class TrailingData(
        val error: Throwable,
    ) : TerminalStreamEnd
}

internal suspend fun awaitTerminalStreamEnd(
    timeout: Duration,
    receive: suspend () -> ByteArray?,
    missingFinMessage: String,
    trailingDataMessage: String,
    onTimeout: () -> Unit = {},
    isTrailingDataFailure: (Throwable) -> Boolean = { false },
): TerminalStreamEnd {
    val result =
        withTimeoutOrNull(timeout) {
            try {
                if (receive() == null) {
                    TerminalStreamEnd.Clean
                } else {
                    TerminalStreamEnd.TrailingData(transportException(trailingDataMessage))
                }
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                if (isTrailingDataFailure(error)) {
                    TerminalStreamEnd.TrailingData(error)
                } else {
                    TerminalStreamEnd.MissingFin(error)
                }
            }
        }
    if (result != null) return result
    onTimeout()
    return TerminalStreamEnd.MissingFin(transportException(missingFinMessage))
}

internal class RawFrameInbox(
    private val options: NettyTransportOptions,
) {
    private val frames = CoroutineChannel<ByteArray>(options.inboundQueueCapacity)
    private val inputClosed = AtomicBoolean(false)
    private val protocolFailure = AtomicReference<Throwable?>()

    fun initializer(): ChannelInitializer<QuicStreamChannel> =
        object : ChannelInitializer<QuicStreamChannel>() {
            override fun initChannel(channel: QuicStreamChannel) {
                channel.pipeline().addLast(decoder(), handler())
            }
        }

    fun decoder(): TrevRpcFrameDecoder = TrevRpcFrameDecoder(options.maxFrameSize) { error -> recordProtocolFailure(error) }

    fun handler(): ChannelInboundHandlerAdapter =
        object : ChannelInboundHandlerAdapter() {
            override fun channelRead(
                context: ChannelHandlerContext,
                message: Any,
            ) {
                val body = message as ByteBuf
                try {
                    if (frames.trySend(body.copyReadableBytes()).isFailure) {
                        failProtocol(transportException("inbound frame queue is full"))
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
                if (event is ChannelInputShutdownEvent || event is ChannelInputShutdownReadComplete) {
                    inputClosed.set(true)
                    frames.close()
                }
                context.fireUserEventTriggered(event)
            }

            override fun channelInactive(context: ChannelHandlerContext) {
                val framingFailure = protocolFailure.get()
                when {
                    framingFailure != null -> fail(framingFailure)
                    inputClosed.get() -> frames.close()
                    else -> fail(transportException("native QUIC response stream closed before FIN"))
                }
                context.fireChannelInactive()
            }

            override fun exceptionCaught(
                context: ChannelHandlerContext,
                cause: Throwable,
            ) {
                val framingFailure = cause.findTrevRpcException()
                if (framingFailure == null) {
                    fail(transportException("native QUIC response stream failed", cause))
                } else {
                    failProtocol(framingFailure)
                }
                context.close()
            }
        }

    suspend fun receive(): ByteArray? {
        val result = frames.receiveCatching()
        result.exceptionOrNull()?.let { throw it }
        return result.getOrNull()
    }

    suspend fun receiveBatch(maxFrames: Int): List<ByteArray> {
        require(maxFrames > 0) { "maxFrames must be positive" }
        val first = receive() ?: return emptyList()
        val batch = ArrayList<ByteArray>(maxFrames)
        batch += first
        while (batch.size < maxFrames) {
            val result = frames.tryReceive()
            result.exceptionOrNull()?.let { throw it }
            val frame = result.getOrNull() ?: break
            batch += frame
        }
        return batch
    }

    fun fail(error: Throwable) {
        frames.close(error)
    }

    private fun recordProtocolFailure(error: Throwable) {
        protocolFailure.compareAndSet(null, error)
    }

    private fun failProtocol(error: Throwable) {
        recordProtocolFailure(error)
        fail(checkNotNull(protocolFailure.get()))
    }

    suspend fun awaitEnd(
        timeout: Duration,
        onTimeout: () -> Unit = {},
    ): TerminalStreamEnd =
        awaitTerminalStreamEnd(
            timeout = timeout,
            receive = ::receive,
            missingFinMessage = "native QUIC response stream ended without FIN",
            trailingDataMessage = "native QUIC response contained data after its terminal frame",
            onTimeout = onTimeout,
            isTrailingDataFailure = { it === protocolFailure.get() },
        )

    suspend fun requireEnd(
        timeout: Duration,
        onTimeout: () -> Unit = {},
    ) {
        when (val end = awaitEnd(timeout, onTimeout)) {
            TerminalStreamEnd.Clean -> Unit
            is TerminalStreamEnd.MissingFin -> throw end.error
            is TerminalStreamEnd.TrailingData -> throw end.error
        }
    }
}

internal class RawRpcTransportStream(
    private val stream: QuicStreamChannel,
    private val inbox: RawFrameInbox,
    private val closed: AtomicBoolean,
    private val options: NettyTransportOptions,
    private val sendRequestEndFrame: Boolean,
    requestSendFinished: Boolean,
    private val onTerminalFrame: () -> Unit = {},
) : RpcClientStream {
    private val sendLock = Mutex()
    private val terminalSeen = AtomicBoolean(false)
    private val outputFinSubmitted = AtomicBoolean(requestSendFinished)
    private var sendFinished = requestSendFinished

    override suspend fun send(body: ByteArray) {
        sendLock.withLock {
            checkSendOpen()
            val frame = WireCodec.encodeMessageFrame(body)
            try {
                TrevRpcFrameWriter.write(stream, frame, options.maxFrameSize).awaitCompletion()
            } catch (error: Throwable) {
                failSend(error)
            }
        }
    }

    override suspend fun sendBatch(bodies: List<ByteArray>) {
        if (bodies.isEmpty()) return
        sendLock.withLock {
            checkSendOpen()
            try {
                val batch = ArrayList<ByteArray>(MAX_WRITE_BATCH_MESSAGES)
                var batchBytes = 0
                bodies.forEach { body ->
                    val encoded = WireCodec.encodeMessageFrame(body)
                    val framedBytes = Integer.sum(4, encoded.size)
                    if (
                        batch.isNotEmpty() &&
                        (batch.size == MAX_WRITE_BATCH_MESSAGES || batchBytes + framedBytes > MAX_WRITE_BATCH_BYTES)
                    ) {
                        TrevRpcFrameWriter.writeBatch(stream, batch, options.maxFrameSize).awaitCompletion()
                        batch.clear()
                        batchBytes = 0
                    }
                    batch += encoded
                    batchBytes = Math.addExact(batchBytes, framedBytes)
                }
                if (batch.isNotEmpty()) {
                    TrevRpcFrameWriter.writeBatch(stream, batch, options.maxFrameSize).awaitCompletion()
                }
            } catch (error: Throwable) {
                failSend(error)
            }
        }
    }

    override suspend fun finishSend() {
        sendLock.withLock {
            if (sendFinished) return
            checkSendOpen()
            sendFinished = true
            try {
                if (sendRequestEndFrame) {
                    TrevRpcFrameWriter
                        .writeFinal(
                            stream,
                            WireCodec.encode(RpcStreamFrame.status(Status.ok())),
                            options.maxFrameSize,
                        ).awaitCompletion()
                } else {
                    stream.shutdownOutput().awaitCompletion()
                }
                outputFinSubmitted.set(true)
            } catch (error: Throwable) {
                failSend(error, ignoreIfClosed = true)
            }
        }
    }

    override suspend fun receive(): RpcStreamFrame? {
        val body = inbox.receive() ?: return null
        val frame = WireCodec.decodeStreamFrame(body)
        if (frame.kind == RpcStreamFrameKind.STATUS) finishReceive(frame.status)
        return frame
    }

    override suspend fun receiveBatch(maxFrames: Int): List<RpcStreamFrame> {
        val frames = inbox.receiveBatch(maxFrames).map(WireCodec::decodeStreamFrame)
        val terminalIndex = frames.indexOfFirst { it.kind == RpcStreamFrameKind.STATUS }
        if (terminalIndex >= 0) {
            if (terminalIndex != frames.lastIndex) {
                stream.cancelAndClose()
                throw transportException("native QUIC response contained data after its terminal frame")
            }
            finishReceive(frames[terminalIndex].status)
        }
        return frames
    }

    override suspend fun close(cause: Throwable?) {
        if (!closed.compareAndSet(false, true)) return
        inbox.fail(cause ?: TrevRpcException(Status.cancelled("RPC stream was closed")))
        stream.cancelAndAwaitReset(options.shutdownTimeout)
    }

    private fun checkSendOpen() {
        if (closed.get() || sendFinished) {
            throw TrevRpcException(Status.cancelled("request stream is closed"))
        }
    }

    private suspend fun finishReceive(status: Status) {
        if (!terminalSeen.compareAndSet(false, true)) {
            stream.cancelAndClose()
            throw transportException("native QUIC response contained data after its terminal frame")
        }
        onTerminalFrame()
        when (
            val end =
                inbox.awaitEnd(options.maxIdleTime) {
                    stream.cancelBoth()
                    stream.close()
                }
        ) {
            TerminalStreamEnd.Clean -> {
                if (closed.compareAndSet(false, true)) {
                    stream.finishAndClose(options.shutdownTimeout, outputFinSubmitted.get())
                }
            }

            is TerminalStreamEnd.TrailingData -> {
                if (closed.compareAndSet(false, true)) stream.cancelAndClose()
                throw end.error
            }

            is TerminalStreamEnd.MissingFin -> {
                if (closed.compareAndSet(false, true)) stream.cancelAndClose()
                if (status.isOk) throw end.error
            }
        }
    }

    private suspend fun failSend(
        error: Throwable,
        ignoreIfClosed: Boolean = false,
    ) {
        val ownsFailure = closed.compareAndSet(false, true)
        if (ownsFailure) {
            inbox.fail(error)
            if (error is CancellationException) {
                stream.cancelAndAwaitReset(options.shutdownTimeout)
            } else {
                stream.cancelAndClose()
            }
        }
        if (error is CancellationException) throw error
        if (!ownsFailure && ignoreIfClosed) return
        throw transportException("native QUIC request write failed", error)
    }
}

private fun Throwable.findTrevRpcException(): TrevRpcException? =
    generateSequence(this) { it.cause }.filterIsInstance<TrevRpcException>().firstOrNull()

private const val MAX_WRITE_BATCH_MESSAGES = 16
private const val MAX_WRITE_BATCH_BYTES = 16 * 1024
