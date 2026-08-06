package zip.trev.trevrpc.netty.advanced

import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelOption
import io.netty.handler.codec.http3.DefaultHttp3DataFrame
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame
import io.netty.handler.codec.http3.Http3
import io.netty.handler.codec.http3.Http3ClientConnectionHandler
import io.netty.handler.codec.http3.Http3DataFrame
import io.netty.handler.codec.http3.Http3HeadersFrame
import io.netty.handler.codec.http3.Http3RequestStreamInboundHandler
import io.netty.handler.codec.quic.QuicStreamChannel
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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
import zip.trev.trevrpc.netty.HTTP3_PATH
import zip.trev.trevrpc.netty.IncrementalFrameReader
import zip.trev.trevrpc.netty.NettyQuicClientConfig
import zip.trev.trevrpc.netty.NettyRpcChannel
import zip.trev.trevrpc.netty.NettyShutdownCoordinator
import zip.trev.trevrpc.netty.NettyTransportOptions
import zip.trev.trevrpc.netty.TREV_RPC_MEDIA_TYPE
import zip.trev.trevrpc.netty.TrevRpcFrameWriter
import zip.trev.trevrpc.netty.awaitCompletion
import zip.trev.trevrpc.netty.awaitValue
import zip.trev.trevrpc.netty.cancelAndAwaitResetHttp3
import zip.trev.trevrpc.netty.cancelAndCloseHttp3
import zip.trev.trevrpc.netty.cancelBothHttp3
import zip.trev.trevrpc.netty.closeApplication
import zip.trev.trevrpc.netty.finishAndClose
import zip.trev.trevrpc.netty.finishAndCloseHttp3
import zip.trev.trevrpc.netty.isOwnedEventLoopThread
import zip.trev.trevrpc.netty.isTrevRpcMediaType
import zip.trev.trevrpc.netty.shutdownOwnedNettyResources
import zip.trev.trevrpc.netty.transportException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.channels.Channel as CoroutineChannel

/** Advanced single-connection HTTP/3 transport. Prefer [NettyRpcChannel.http3]. */
class RawNettyHttp3RpcTransport private constructor(
    private val endpoint: ClientQuicEndpoint,
    private val config: NettyQuicClientConfig,
    private val onTerminalFrame: () -> Unit,
) : RpcTransport,
    AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val streams = ConcurrentHashMap<QuicStreamChannel, (Throwable) -> Unit>()
    private val shutdownCoordinator =
        NettyShutdownCoordinator {
            shutdownOwnedNettyResources(
                group = endpoint.group,
                timeout = config.options.shutdownTimeout,
                description = "HTTP/3 transport",
                forceClose = {
                    streams.keys.forEach { it.close() }
                    endpoint.quicChannel.close()
                    endpoint.datagramChannel.close()
                },
            ) {
                closed.set(true)
                val failure = transportException("HTTP/3 transport is shutting down")
                streams.toMap().forEach { (stream, fail) ->
                    fail(failure)
                    stream.cancelBothHttp3()
                    stream.close()
                }
                endpoint.quicChannel.closeApplication(0, "HTTP/3 client closed")
                endpoint.quicChannel.closeFuture().awaitCompletion()
                endpoint.datagramChannel.close().awaitCompletion()
            }
        }

    override suspend fun unary(request: RpcRequest): RpcResponse {
        checkOpen()
        val inbox = Http3FrameInbox(config.options)
        val stream = openRequest(inbox)
        try {
            performHttp3RequestHandshake(
                writeHeaders = { writeRequestHeaders(stream) },
                writeInitialData = {
                    writeDataFrame(stream, WireCodec.encode(request), config.options.maxFrameSize).awaitCompletion()
                },
                finishRequest = { stream.shutdownOutput().awaitCompletion() },
                awaitResponse = inbox::awaitSuccess,
            )
            val body = inbox.receive() ?: throw transportException("HTTP/3 unary response ended before a frame")
            val response = WireCodec.decodeResponse(body)
            when (val end = inbox.awaitEnd()) {
                TerminalStreamEnd.Clean -> {
                    stream.finishAndCloseHttp3(config.options.shutdownTimeout)
                }

                is TerminalStreamEnd.TrailingData -> {
                    throw end.error
                }

                is TerminalStreamEnd.MissingFin -> {
                    stream.cancelAndCloseHttp3()
                    if (response.status.isOk) throw end.error
                }
            }
            return response
        } catch (error: CancellationException) {
            stream.cancelAndAwaitResetHttp3(config.options.shutdownTimeout)
            throw error
        } catch (error: Throwable) {
            stream.cancelAndCloseHttp3()
            throw transportException("HTTP/3 unary RPC failed", error)
        }
    }

    override suspend fun openStream(request: RpcRequest): RpcClientStream {
        checkOpen()
        val inbox = Http3FrameInbox(config.options)
        val stream = openRequest(inbox)
        try {
            val requestEndsAfterInitialData = http3RequestEndsAfterInitialData(request.kind)
            performHttp3RequestHandshake(
                writeHeaders = { writeRequestHeaders(stream) },
                writeInitialData = {
                    writeDataFrame(stream, WireCodec.encode(request), config.options.maxFrameSize).awaitCompletion()
                },
                finishRequest = { stream.shutdownOutput().awaitCompletion() },
                awaitResponse = inbox::awaitSuccess,
                finishInitialRequest = requestEndsAfterInitialData,
            )
            return Http3RpcTransportStream(
                stream,
                inbox,
                config.options,
                sendRequestEndFrame = !requestEndsAfterInitialData,
                requestFinished = requestEndsAfterInitialData,
                onTerminalFrame = onTerminalFrame,
            )
        } catch (error: CancellationException) {
            stream.cancelAndAwaitResetHttp3(config.options.shutdownTimeout)
            throw error
        } catch (error: Throwable) {
            stream.cancelAndCloseHttp3()
            throw transportException("failed to open HTTP/3 RPC stream", error)
        }
    }

    suspend fun shutdown() {
        closed.set(true)
        shutdownCoordinator.shutdown()
    }

    internal suspend fun awaitClosed() {
        endpoint.quicChannel.closeFuture().awaitCompletion()
    }

    override fun close() {
        check(!endpoint.group.isOwnedEventLoopThread()) {
            "RawNettyHttp3RpcTransport.close() cannot run on its owned Netty event loop; use shutdown()"
        }
        runBlocking { shutdown() }
    }

    private suspend fun openRequest(inbox: Http3FrameInbox): QuicStreamChannel {
        val stream =
            Http3
                .newRequestStreamBootstrap(endpoint.quicChannel, inbox)
                .option(ChannelOption.WRITE_BUFFER_WATER_MARK, config.options.waterMark)
                .create()
                .awaitValue { rejectedStream ->
                    rejectedStream.cancelBothHttp3()
                    rejectedStream.close()
                }
        streams[stream] = inbox::fail
        stream.closeFuture().addListener { streams.remove(stream) }
        if (closed.get()) {
            streams.remove(stream)
            inbox.fail(transportException("HTTP/3 transport is closed"))
            stream.cancelAndCloseHttp3()
            throw transportException("HTTP/3 transport is closed")
        }
        return stream
    }

    private suspend fun writeRequestHeaders(stream: QuicStreamChannel) {
        val headers = DefaultHttp3HeadersFrame()
        headers
            .headers()
            .method("POST")
            .path(HTTP3_PATH)
            .scheme("https")
            .authority("${config.tls.serverName}:${config.remoteAddress.port}")
            .set("content-type", TREV_RPC_MEDIA_TYPE)
        stream.writeAndFlush(headers).awaitCompletion()
    }

    private fun checkOpen() {
        if (closed.get()) throw transportException("HTTP/3 transport is closed")
    }

    companion object {
        suspend fun connect(config: NettyQuicClientConfig): RawNettyHttp3RpcTransport = connect(config) {}

        internal suspend fun connect(
            config: NettyQuicClientConfig,
            onTerminalFrame: () -> Unit,
        ): RawNettyHttp3RpcTransport {
            val endpoint = connectQuic(config, HTTP3_ALPN, Http3ClientConnectionHandler())
            return RawNettyHttp3RpcTransport(endpoint, config, onTerminalFrame)
        }

        internal fun fromEndpoint(
            endpoint: ClientQuicEndpoint,
            config: NettyQuicClientConfig,
        ): RawNettyHttp3RpcTransport = RawNettyHttp3RpcTransport(endpoint, config) {}
    }
}

internal fun http3RequestEndsAfterInitialData(kind: RpcKind?): Boolean = kind == RpcKind.UNARY || kind == RpcKind.SERVER_STREAMING

internal suspend fun performHttp3RequestHandshake(
    writeHeaders: suspend () -> Unit,
    writeInitialData: suspend () -> Unit,
    finishRequest: suspend () -> Unit,
    awaitResponse: suspend () -> Unit,
    finishInitialRequest: Boolean = true,
) {
    writeHeaders()
    writeInitialData()
    if (finishInitialRequest) {
        finishRequest()
        awaitResponse()
    }
}

internal suspend fun finishHttp3StreamingRequest(
    writeEndFrame: suspend () -> Unit,
    finishRequest: suspend () -> Unit,
) {
    writeEndFrame()
    finishRequest()
}

private fun writeDataFrame(
    channel: Channel,
    body: ByteArray,
    maxFrameSize: Int,
): io.netty.channel.ChannelFuture {
    val framed = TrevRpcFrameWriter.encode(channel.alloc(), body, maxFrameSize)
    return try {
        channel.writeAndFlush(DefaultHttp3DataFrame(framed))
    } catch (error: Throwable) {
        framed.release()
        throw error
    }
}

internal class Http3FrameInbox(
    private val options: NettyTransportOptions,
) : Http3RequestStreamInboundHandler() {
    private val parser = IncrementalFrameReader(options.maxFrameSize)
    private val frames = CoroutineChannel<ByteArray>(options.inboundQueueCapacity)
    private val responseValidated = CompletableDeferred<Unit>()
    private var headersSeen = false
    private var responseAccepted = false
    private var inputClosed = false
    private var stream: QuicStreamChannel? = null
    private val protocolFailure = AtomicReference<Throwable?>()

    override fun handlerAdded(context: ChannelHandlerContext) {
        stream = context.channel() as? QuicStreamChannel
    }

    override fun channelRead(
        context: ChannelHandlerContext,
        frame: Http3HeadersFrame,
    ) {
        if (headersSeen) {
            failProtocol(TrevRpcException(Status.unavailable("HTTP/3 response contained duplicate headers or trailers")))
            return
        }
        headersSeen = true
        val statuses = frame.headers().getAll(":status").map(CharSequence::toString)
        val contentTypes = frame.headers().getAll("content-type").map(CharSequence::toString)
        when {
            statuses != listOf("200") -> {
                failProtocol(
                    TrevRpcException(
                        Status.unavailable("HTTP/3 response must contain exactly status 200"),
                    ),
                )
            }

            !isTrevRpcMediaType(contentTypes) -> {
                failProtocol(
                    TrevRpcException(
                        Status.unavailable(
                            "HTTP/3 response must contain exactly content-type $TREV_RPC_MEDIA_TYPE",
                        ),
                    ),
                )
            }

            else -> {
                responseAccepted = true
                responseValidated.complete(Unit)
            }
        }
    }

    override fun channelRead(
        context: ChannelHandlerContext,
        frame: Http3DataFrame,
    ) {
        try {
            if (!responseAccepted) {
                failProtocol(TrevRpcException(Status.unavailable("HTTP/3 response data arrived before valid headers")))
                (context.channel() as QuicStreamChannel).cancelBothHttp3()
                return
            }
            parser.feed(frame.content()).forEach { body ->
                if (frames.trySend(body).isFailure) {
                    failProtocol(transportException("HTTP/3 inbound frame queue is full"))
                    (context.channel() as QuicStreamChannel).cancelBothHttp3()
                    return
                }
            }
        } catch (error: Throwable) {
            failProtocol(error)
            (context.channel() as QuicStreamChannel).cancelBothHttp3()
        } finally {
            frame.release()
        }
    }

    override fun channelInputClosed(context: ChannelHandlerContext) {
        inputClosed = true
        if (!responseAccepted) {
            fail(TrevRpcException(Status.unavailable("HTTP/3 response ended before valid headers")))
            return
        }
        try {
            parser.finish()
            frames.close()
        } catch (error: Throwable) {
            failProtocol(error)
        }
    }

    override fun channelInactive(context: ChannelHandlerContext) {
        if (!inputClosed) {
            fail(transportException("HTTP/3 response stream closed before FIN"))
        }
        context.fireChannelInactive()
    }

    override fun exceptionCaught(
        context: ChannelHandlerContext,
        cause: Throwable,
    ) {
        fail(transportException("HTTP/3 response stream failed", cause))
        context.close()
    }

    suspend fun awaitSuccess() {
        responseValidated.await()
    }

    suspend fun receive(): ByteArray? {
        val result = frames.receiveCatching()
        result.exceptionOrNull()?.let { throw it }
        return result.getOrNull()
    }

    fun fail(error: Throwable) {
        responseValidated.completeExceptionally(error)
        frames.close(error)
    }

    private fun failProtocol(error: Throwable) {
        protocolFailure.compareAndSet(null, error)
        fail(error)
    }

    suspend fun awaitEnd(timeout: kotlin.time.Duration = options.maxIdleTime): TerminalStreamEnd =
        awaitTerminalStreamEnd(
            timeout = timeout,
            receive = ::receive,
            missingFinMessage = "HTTP/3 response stream ended without FIN",
            trailingDataMessage = "HTTP/3 response contained data after its terminal frame",
            onTimeout = {
                stream?.cancelBothHttp3()
                stream?.close()
            },
            isTrailingDataFailure = { it === protocolFailure.get() },
        )

    suspend fun requireEnd(timeout: kotlin.time.Duration = options.maxIdleTime) {
        when (val end = awaitEnd(timeout)) {
            TerminalStreamEnd.Clean -> Unit
            is TerminalStreamEnd.MissingFin -> throw end.error
            is TerminalStreamEnd.TrailingData -> throw end.error
        }
    }
}

private class Http3RpcTransportStream(
    private val stream: QuicStreamChannel,
    private val inbox: Http3FrameInbox,
    private val options: NettyTransportOptions,
    private val sendRequestEndFrame: Boolean,
    requestFinished: Boolean,
    private val onTerminalFrame: () -> Unit,
) : RpcClientStream {
    private val closed = AtomicBoolean(false)
    private val terminalSeen = AtomicBoolean(false)
    private val sendLock = Mutex()
    private var sendFinished = requestFinished

    override suspend fun send(body: ByteArray) {
        sendLock.withLock {
            checkSendOpen()
            val frame = WireCodec.encode(RpcStreamFrame.message(body))
            try {
                writeDataFrame(stream, frame, options.maxFrameSize).awaitCompletion()
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
                finishHttp3StreamingRequest(
                    writeEndFrame = {
                        if (sendRequestEndFrame) {
                            writeDataFrame(
                                stream,
                                WireCodec.encode(RpcStreamFrame.status(Status.ok())),
                                options.maxFrameSize,
                            ).awaitCompletion()
                        }
                    },
                    finishRequest = { stream.shutdownOutput().awaitCompletion() },
                )
            } catch (error: Throwable) {
                failSend(error, ignoreIfClosed = true)
            }
        }
    }

    override suspend fun receive(): RpcStreamFrame? {
        inbox.awaitSuccess()
        val body = inbox.receive() ?: return null
        val frame = WireCodec.decodeStreamFrame(body)
        if (frame.kind == RpcStreamFrameKind.STATUS) finishReceive(frame.status)
        return frame
    }

    override suspend fun close(cause: Throwable?) {
        if (!closed.compareAndSet(false, true)) return
        inbox.fail(cause ?: TrevRpcException(Status.cancelled("RPC stream was closed")))
        stream.cancelAndAwaitResetHttp3(options.shutdownTimeout)
    }

    private suspend fun finishReceive(status: Status) {
        if (!terminalSeen.compareAndSet(false, true)) {
            stream.cancelAndCloseHttp3()
            throw transportException("HTTP/3 response contained data after its terminal frame")
        }
        onTerminalFrame()
        when (val end = inbox.awaitEnd()) {
            TerminalStreamEnd.Clean -> {
                if (closed.compareAndSet(false, true)) stream.finishAndCloseHttp3(options.shutdownTimeout)
            }

            is TerminalStreamEnd.TrailingData -> {
                if (closed.compareAndSet(false, true)) stream.cancelAndCloseHttp3()
                throw end.error
            }

            is TerminalStreamEnd.MissingFin -> {
                if (closed.compareAndSet(false, true)) stream.cancelAndCloseHttp3()
                if (status.isOk) throw end.error
            }
        }
    }

    private fun checkSendOpen() {
        if (closed.get() || sendFinished) {
            throw TrevRpcException(Status.cancelled("request stream is closed"))
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
                stream.cancelAndAwaitResetHttp3(options.shutdownTimeout)
            } else {
                stream.cancelAndCloseHttp3()
            }
        }
        if (error is CancellationException) throw error
        if (!ownsFailure && ignoreIfClosed) return
        throw transportException("HTTP/3 request write failed", error)
    }
}
