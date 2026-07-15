package zip.trev.trevrpc.netty

import io.netty.bootstrap.Bootstrap
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
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
import io.netty.channel.socket.nio.NioDatagramChannel
import io.netty.handler.codec.http3.DefaultHttp3DataFrame
import io.netty.handler.codec.http3.DefaultHttp3HeadersFrame
import io.netty.handler.codec.http3.Http3
import io.netty.handler.codec.http3.Http3DataFrame
import io.netty.handler.codec.http3.Http3HeadersFrame
import io.netty.handler.codec.http3.Http3RequestStreamInboundHandler
import io.netty.handler.codec.http3.Http3ServerConnectionHandler
import io.netty.handler.codec.http3.TrevRpcWebTransportServerConnectionHandler
import io.netty.handler.codec.quic.DefaultQuicStreamFrame
import io.netty.handler.codec.quic.QuicChannel
import io.netty.handler.codec.quic.QuicServerCodecBuilder
import io.netty.handler.codec.quic.QuicStreamChannel
import io.netty.handler.ssl.SslHandshakeCompletionEvent
import io.netty.util.AttributeKey
import io.netty.util.ReferenceCountUtil
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import zip.trev.trevrpc.RpcKind
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcResponse
import zip.trev.trevrpc.RpcStreamFrame
import zip.trev.trevrpc.RpcStreamFrameKind
import zip.trev.trevrpc.Server
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.WireCodec
import java.net.InetSocketAddress
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.time.Duration
import kotlinx.coroutines.channels.Channel as CoroutineChannel

private val CONNECTION_STATE = AttributeKey.valueOf<ServerConnectionState>("trevrpc.netty.connection-state")

class NettyRpcServer private constructor(
    private val server: Server,
    val config: NettyRpcServerConfig,
    private val group: EventLoopGroup,
    private var datagramChannel: Channel?,
    private val scope: CoroutineScope,
) : AutoCloseable {
    private val closed = AtomicBoolean(false)
    private val shutdownComplete = CompletableDeferred<Result<Unit>>()
    private val connections = ConcurrentHashMap.newKeySet<QuicChannel>()
    private val activeConnections = AtomicInteger()

    val localAddress: InetSocketAddress
        get() = checkNotNull(datagramChannel) { "server is not bound" }.localAddress() as InetSocketAddress

    internal val bindCount: Int = 1

    suspend fun shutdown() {
        if (!closed.compareAndSet(false, true)) {
            shutdownComplete.await().getOrThrow()
            return
        }
        val result =
            runCatching {
                try {
                    server.shutdown()
                } finally {
                    withContext(NonCancellable) {
                        try {
                            connections.forEach { it.closeApplication(0, "server shutdown") }
                            connections.map { it.closeFuture() }.forEach { it.awaitCompletion() }
                        } finally {
                            try {
                                datagramChannel?.close()?.awaitCompletion()
                            } finally {
                                scope.cancel()
                                group.shutdownGracefully().awaitValue()
                            }
                        }
                    }
                }
            }
        shutdownComplete.complete(result)
        result.getOrThrow()
    }

    override fun close() {
        runBlocking { shutdown() }
    }

    private fun initializeConnection(channel: QuicChannel) {
        channel.pipeline().addLast(
            AlpnDispatchHandler(
                config.protocols().toSet(),
                onAccepted = {
                    val limit = server.options.maxConcurrentConnections
                    if (closed.get()) {
                        false
                    } else {
                        val active = activeConnections.incrementAndGet()
                        if (limit != null && active > limit) {
                            activeConnections.decrementAndGet()
                            false
                        } else {
                            connections += channel
                            true
                        }
                    }
                },
                onNative = {
                    channel.attr(CONNECTION_STATE).set(ServerConnectionState(Protocol.NATIVE))
                },
                onHttp3 = {
                    channel.attr(CONNECTION_STATE).set(ServerConnectionState(Protocol.HTTP3))
                    val handler =
                        if (config.enableWebTransport) {
                            TrevRpcWebTransportServerConnectionHandler(
                                http3StreamInitializer(),
                                webTransportRawStreamInitializer(),
                            )
                        } else {
                            Http3ServerConnectionHandler(http3StreamInitializer())
                        }
                    channel.pipeline().addLast(handler)
                },
                onClosed = {
                    if (connections.remove(channel)) activeConnections.decrementAndGet()
                },
            ),
        )
    }

    private fun initializeStream(channel: QuicStreamChannel) {
        val state = channel.parent().attr(CONNECTION_STATE).get()
        if (state?.protocol == Protocol.NATIVE) initializeNativeStream(channel, state)
    }

    private fun initializeNativeStream(
        channel: QuicStreamChannel,
        connection: ServerConnectionState,
    ) {
        if (!connection.tryAcquire(server.options.maxConcurrentStreamsPerConnection)) {
            rejectRawRpcStream(channel, "too many concurrent streams on connection")
            return
        }
        val input = ServerFrameInput(config.options, channel::cancelBoth)
        channel.pipeline().addLast(TrevRpcFrameDecoder(config.options.maxFrameSize), input.handler())
        channel.closeFuture().addListener { connection.release() }
        scope.launch {
            processRpc(
                input,
                write = { writeRaw(channel, it) },
                writeTerminal = { writeRaw(channel, it, finish = true) },
                finish = { channel.shutdownOutput().awaitCompletion() },
                cancelInput = channel::cancelBoth,
            )
        }
    }

    private fun http3StreamInitializer(): ChannelInitializer<QuicStreamChannel> =
        object : ChannelInitializer<QuicStreamChannel>() {
            override fun initChannel(channel: QuicStreamChannel) {
                channel.pipeline().addLast(Http3ServerStreamHandler(channel))
            }
        }

    private fun webTransportRawStreamInitializer(): ChannelInitializer<QuicStreamChannel> =
        object : ChannelInitializer<QuicStreamChannel>() {
            override fun initChannel(channel: QuicStreamChannel) {
                val connection = channel.parent().attr(CONNECTION_STATE).get()
                if (connection == null) {
                    channel.cancelBoth()
                    channel.close()
                    return
                }
                channel.pipeline().addLast(WebTransportRawStreamHandler(channel, connection))
            }
        }

    private suspend fun processRpc(
        input: ServerFrameInput,
        write: suspend (ByteArray) -> Unit,
        writeTerminal: suspend (ByteArray) -> Unit,
        finish: suspend () -> Unit,
        cancelInput: () -> Unit,
    ) {
        val request =
            try {
                val encoded = receiveInitial(input, server.options.initialRequestTimeout)
                WireCodec.decodeRequest(encoded)
            } catch (error: Throwable) {
                writeTerminal(WireCodec.encode(RpcResponse.fromStatus(error.statusForWire())))
                return
            }
        try {
            if (request.kind == RpcKind.UNARY) {
                writeTerminal(WireCodec.encode(server.handleUnary(request)))
                return
            }
            val responses = server.handleStreaming(request, requestFlow(input))
            var terminalWritten = false
            try {
                while (true) {
                    val response = responses.receive() ?: break
                    val encoded = WireCodec.encode(response)
                    if (response.kind == RpcStreamFrameKind.STATUS) {
                        writeTerminal(encoded)
                        terminalWritten = true
                        break
                    }
                    write(encoded)
                }
            } finally {
                responses.close()
            }
            if (!terminalWritten) finish()
        } catch (error: CancellationException) {
            cancelInput()
            throw error
        } catch (error: Throwable) {
            val status = RpcStreamFrame.status(error.statusForWire())
            runCatching { writeTerminal(WireCodec.encode(status)) }
        }
    }

    private fun requestFlow(input: ServerFrameInput): Flow<ByteArray> =
        flow {
            while (true) {
                val encoded = input.receive() ?: break
                val frame = WireCodec.decodeStreamFrame(encoded)
                when (frame.kind) {
                    RpcStreamFrameKind.MESSAGE -> {
                        emit(frame.body)
                    }

                    RpcStreamFrameKind.STATUS -> {
                        if (!frame.status.isOk) throw TrevRpcException(frame.status)
                        break
                    }

                    null -> {
                        throw TrevRpcException(Status.invalidArgument("unknown request stream frame kind"))
                    }
                }
            }
        }

    private suspend fun receiveInitial(
        input: ServerFrameInput,
        timeout: Duration?,
    ): ByteArray {
        val receive = suspend { input.receive() ?: throw transportException("stream ended before request frame") }
        return if (timeout == null) receive() else withTimeout(timeout) { receive() }
    }

    private fun rejectRawRpcStream(
        channel: QuicStreamChannel,
        message: String,
    ) {
        val input = ServerFrameInput(config.options, channel::cancelBoth)
        channel.pipeline().addLast(TrevRpcFrameDecoder(config.options.maxFrameSize), input.handler())
        scope.launch {
            try {
                val body = receiveInitial(input, server.options.initialRequestTimeout)
                val request = WireCodec.decodeRequest(body)
                val status = Status.unavailable(message)
                val response =
                    if (request.kind == RpcKind.UNARY) {
                        WireCodec.encode(RpcResponse.fromStatus(status))
                    } else {
                        WireCodec.encode(RpcStreamFrame.status(status))
                    }
                writeRaw(channel, response, finish = true)
                channel.shutdownInput(CANCELLED_STREAM_CODE)
            } catch (_: Throwable) {
                channel.cancelBoth()
            }
        }
    }

    private suspend fun writeRaw(
        channel: QuicStreamChannel,
        body: ByteArray,
        finish: Boolean = false,
    ) {
        val framed = TrevRpcFrameWriter.encode(channel.alloc(), body, config.options.maxFrameSize)
        val message: Any = if (finish) DefaultQuicStreamFrame(framed, true) else framed
        val future =
            try {
                channel.writeAndFlush(message)
            } catch (error: Throwable) {
                ReferenceCountUtil.release(message)
                throw error
            }
        future.awaitCompletion()
    }

    private inner class Http3ServerStreamHandler(
        private val channel: QuicStreamChannel,
    ) : Http3RequestStreamInboundHandler() {
        private val input = ServerFrameInput(config.options, channel::cancelBoth)
        private var headersReceived = false

        @Volatile
        private var admitted = false

        @Volatile
        private var rejected = false

        private var webTransportConnect = false

        override fun channelRead(
            context: ChannelHandlerContext,
            frame: Http3HeadersFrame,
        ) {
            if (headersReceived) {
                if (admitted) input.fail(TrevRpcException(Status.invalidArgument("HTTP/3 trailers are not supported")))
                return
            }
            headersReceived = true
            val request = snapshot(frame)
            if (request.method == "CONNECT" && request.protocol.equals("webtransport", ignoreCase = true)) {
                webTransportConnect = true
                scope.launch { handleWebTransportConnect(request) }
                return
            }
            scope.launch {
                val rejection = runCatching { validateHttp3(request) }.getOrElse { 403 }
                if (rejection != null) {
                    rejected = true
                    sendHttpStatus(channel, rejection)
                    return@launch
                }
                val connection = checkNotNull(channel.parent().attr(CONNECTION_STATE).get())
                if (!connection.tryAcquire(server.options.maxConcurrentStreamsPerConnection)) {
                    rejected = true
                    sendHttpStatus(channel, 503)
                    return@launch
                }
                channel.closeFuture().addListener { connection.release() }
                admitted = true
                sendHttpStatus(channel, 200, finish = false)
                processRpc(
                    input,
                    write = { writeHttp3(channel, it) },
                    writeTerminal = {
                        writeHttp3(channel, it)
                        channel.shutdownOutput().awaitCompletion()
                    },
                    finish = { channel.shutdownOutput().awaitCompletion() },
                    cancelInput = channel::cancelBoth,
                )
            }
        }

        override fun channelRead(
            context: ChannelHandlerContext,
            frame: Http3DataFrame,
        ) {
            try {
                if (!rejected && !webTransportConnect) input.feed(frame.content())
            } finally {
                frame.release()
            }
        }

        override fun channelInputClosed(context: ChannelHandlerContext) {
            if (!webTransportConnect) input.finish()
        }

        override fun channelInactive(context: ChannelHandlerContext) {
            if (!webTransportConnect) input.finish()
        }

        override fun exceptionCaught(
            context: ChannelHandlerContext,
            cause: Throwable,
        ) {
            input.fail(cause)
            if (!admitted) context.close()
        }

        private suspend fun handleWebTransportConnect(request: Http3AdmissionRequest) {
            val authority = request.authority
            val admission = config.webTransportAdmission
            val status =
                when {
                    !config.enableWebTransport || request.path != WEBTRANSPORT_PATH -> 404

                    !request.secure || authority.isNullOrBlank() -> 403

                    admission == null -> 403

                    !runCatching {
                        admission.admit(
                            WebTransportAdmissionRequest(
                                request.path,
                                authority,
                                request.headers["origin"]?.singleOrNull(),
                                request.secure,
                                request.headers,
                            ),
                        )
                    }.getOrDefault(false) -> 403

                    else -> null
                }
            if (status != null) {
                rejected = true
                sendHttpStatus(channel, status)
                return
            }
            val connection = checkNotNull(channel.parent().attr(CONNECTION_STATE).get())
            val sessionId = channel.streamId()
            if (!connection.tryOpenSession(sessionId)) {
                rejected = true
                sendHttpStatus(channel, 409)
                return
            }
            channel.closeFuture().addListener { connection.closeSession(sessionId) }
            admitted = true
            val response = DefaultHttp3HeadersFrame()
            response.headers().status("200")
            response.headers().set("sec-webtransport-http3-draft", "draft02")
            try {
                channel.writeAndFlush(response).awaitCompletion()
            } catch (error: Throwable) {
                connection.closeSession(sessionId)
                throw error
            }
        }
    }

    private inner class WebTransportRawStreamHandler(
        private val channel: QuicStreamChannel,
        private val connection: ServerConnectionState,
    ) : ChannelInboundHandlerAdapter() {
        private var decoder: WebTransportPreludeDecoder? = null
        private var routed = false

        override fun channelRead(
            context: ChannelHandlerContext,
            message: Any,
        ) {
            if (routed || message !is ByteBuf) {
                context.fireChannelRead(message)
                return
            }
            val sessionId = connection.activeSessionId()
            if (sessionId == null) {
                message.release()
                reject()
                return
            }
            val parser = decoder ?: WebTransportPreludeDecoder(sessionId).also { decoder = it }
            val result =
                try {
                    parser.feed(message)
                } finally {
                    message.release()
                }
            when (result) {
                WebTransportPreludeResult.NeedMoreData -> Unit
                is WebTransportPreludeResult.Rejected -> reject()
                is WebTransportPreludeResult.Accepted -> accept(result)
            }
        }

        override fun userEventTriggered(
            context: ChannelHandlerContext,
            event: Any,
        ) {
            if (!routed && event is ChannelInputShutdownReadComplete) {
                if (decoder?.finish() !is WebTransportPreludeResult.Accepted) reject()
            }
            context.fireUserEventTriggered(event)
        }

        override fun channelInactive(context: ChannelHandlerContext) {
            if (routed) connection.unregisterRawStream(channel)
            context.fireChannelInactive()
        }

        override fun exceptionCaught(
            context: ChannelHandlerContext,
            cause: Throwable,
        ) {
            reject()
        }

        private fun accept(result: WebTransportPreludeResult.Accepted) {
            if (!connection.registerRawStream(result.sessionId, channel)) {
                reject()
                return
            }
            routed = true
            channel.pipeline().remove(this)
            initializeWebTransportRpcStream(channel, connection)
            if (result.remaining.isNotEmpty()) {
                channel.pipeline().fireChannelRead(Unpooled.wrappedBuffer(result.remaining))
            }
        }

        private fun reject() {
            if (routed) return
            routed = true
            channel.cancelBoth()
            channel.close()
        }
    }

    private fun initializeWebTransportRpcStream(
        channel: QuicStreamChannel,
        connection: ServerConnectionState,
    ) {
        channel.closeFuture().addListener { connection.unregisterRawStream(channel) }
        if (!connection.tryAcquire(server.options.maxConcurrentStreamsPerConnection)) {
            rejectRawRpcStream(channel, "too many concurrent streams on WebTransport session")
            return
        }
        channel.closeFuture().addListener { connection.release() }
        val input = ServerFrameInput(config.options, channel::cancelBoth)
        channel.pipeline().addLast(TrevRpcFrameDecoder(config.options.maxFrameSize), input.handler())
        scope.launch {
            processRpc(
                input,
                write = { writeRaw(channel, it) },
                writeTerminal = { writeRaw(channel, it, finish = true) },
                finish = { channel.shutdownOutput().awaitCompletion() },
                cancelInput = channel::cancelBoth,
            )
        }
    }

    private fun snapshot(frame: Http3HeadersFrame): Http3AdmissionRequest {
        val headers = linkedMapOf<String, MutableList<String>>()
        frame.headers().forEach { entry ->
            if (!entry.key.toString().startsWith(':')) {
                headers.getOrPut(entry.key.toString().lowercase()) { mutableListOf() } += entry.value.toString()
            }
        }
        return Http3AdmissionRequest(
            path =
                frame
                    .headers()
                    .path()
                    ?.toString()
                    .orEmpty(),
            method =
                frame
                    .headers()
                    .method()
                    ?.toString()
                    .orEmpty(),
            authority = frame.headers().authority()?.toString(),
            secure = frame.headers().scheme()?.toString() == "https",
            headers = headers,
            protocol = frame.headers().protocol()?.toString(),
        )
    }

    private fun validateHttp3(request: Http3AdmissionRequest): Int? {
        if (!config.enableHttp3 || request.path != HTTP3_PATH) return 404
        if (request.method != "POST") return 405
        if (!isTrevRpcMediaType(request.headers["content-type"].orEmpty())) return 415
        if (config.http3Admission?.admit(request) == false) return 403
        return null
    }

    private suspend fun sendHttpStatus(
        channel: QuicStreamChannel,
        status: Int,
        finish: Boolean = true,
    ) {
        val frame = DefaultHttp3HeadersFrame()
        frame.headers().status(status.toString())
        if (status == 200) frame.headers().set("content-type", TREV_RPC_MEDIA_TYPE)
        if (status == 405) frame.headers().set("allow", "POST")
        channel.writeAndFlush(frame).awaitCompletion()
        if (finish) channel.shutdownOutput().awaitCompletion()
    }

    private suspend fun writeHttp3(
        channel: QuicStreamChannel,
        body: ByteArray,
    ) {
        val buffer = TrevRpcFrameWriter.encode(channel.alloc(), body, config.options.maxFrameSize)
        try {
            channel.writeAndFlush(DefaultHttp3DataFrame(buffer)).awaitCompletion()
        } catch (error: Throwable) {
            if (buffer.refCnt() > 0) buffer.release()
            throw error
        }
    }

    companion object {
        suspend fun bind(
            server: Server,
            config: NettyRpcServerConfig,
        ): NettyRpcServer {
            val group = MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())
            val scope =
                CoroutineScope(
                    SupervisorJob() + Dispatchers.IO.limitedParallelism(config.options.workerParallelism),
                )
            val instance = NettyRpcServer(server, config, group, null, scope)
            try {
                val ssl = config.tls.context(config.protocols())
                val receiveWindow = config.options.maxFrameSize.toLong() + 4
                val transportStreamLimit =
                    (
                        server.options.maxConcurrentStreamsPerConnection
                            ?.toLong()
                            ?.plus(1) ?: 64L
                    ).coerceAtLeast(64L)
                val builder: QuicServerCodecBuilder =
                    if (config.enableHttp3 || config.enableWebTransport) {
                        Http3.newQuicServerCodecBuilder()
                    } else {
                        QuicServerCodecBuilder()
                    }
                var configuredBuilder =
                    builder
                        .sslContext(ssl)
                        .maxIdleTimeout(config.options.maxIdleTime.inWholeMilliseconds, TimeUnit.MILLISECONDS)
                        .initialMaxData(
                            receiveWindow *
                                (server.options.maxConcurrentStreamsPerConnection ?: 64).toLong(),
                        ).initialMaxStreamDataBidirectionalRemote(receiveWindow)
                        .initialMaxStreamDataBidirectionalLocal(receiveWindow)
                        .initialMaxStreamsBidirectional(transportStreamLimit)
                        .tokenHandler(config.tokenHandler)
                        .streamOption(ChannelOption.WRITE_BUFFER_WATER_MARK, config.options.waterMark)
                        .handler(
                            object : ChannelInitializer<QuicChannel>() {
                                override fun initChannel(channel: QuicChannel) {
                                    instance.initializeConnection(channel)
                                }
                            },
                        ).streamHandler(
                            object : ChannelInitializer<QuicStreamChannel>() {
                                override fun initChannel(channel: QuicStreamChannel) {
                                    instance.initializeStream(channel)
                                }
                            },
                        )
                if (config.enableWebTransport) configuredBuilder = configuredBuilder.datagram(1024, 1024)
                val codec = configuredBuilder.build()
                val datagram =
                    Bootstrap()
                        .group(group)
                        .channel(NioDatagramChannel::class.java)
                        .handler(codec)
                        .bind(config.bindAddress)
                        .awaitChannel()
                instance.datagramChannel = datagram
                return instance
            } catch (error: CancellationException) {
                runCatching { instance.datagramChannel?.close()?.syncUninterruptibly() }
                scope.cancel()
                runCatching { group.shutdownGracefully().syncUninterruptibly() }
                throw error
            } catch (error: Throwable) {
                instance.datagramChannel?.close()?.syncUninterruptibly()
                scope.cancel()
                group.shutdownGracefully().syncUninterruptibly()
                throw transportException("failed to bind Netty RPC server", error)
            }
        }
    }
}

internal enum class AlpnDispatchResult {
    WAITING,
    NATIVE,
    HTTP3,
    REJECT,
}

internal class AlpnDispatchState(
    private val enabledProtocols: Set<String>,
) {
    private var result = AlpnDispatchResult.WAITING

    fun handshake(
        success: Boolean,
        applicationProtocol: String?,
    ): AlpnDispatchResult {
        if (result != AlpnDispatchResult.WAITING) return result
        result =
            when {
                !success || applicationProtocol !in enabledProtocols -> AlpnDispatchResult.REJECT
                applicationProtocol == zip.trev.trevrpc.ALPN -> AlpnDispatchResult.NATIVE
                applicationProtocol == HTTP3_ALPN -> AlpnDispatchResult.HTTP3
                else -> AlpnDispatchResult.REJECT
            }
        return result
    }
}

private class AlpnDispatchHandler(
    enabledProtocols: Set<String>,
    private val onAccepted: () -> Boolean,
    private val onNative: () -> Unit,
    private val onHttp3: () -> Unit,
    private val onClosed: () -> Unit,
) : ChannelInboundHandlerAdapter() {
    private val state = AlpnDispatchState(enabledProtocols)
    private var accepted = false

    override fun userEventTriggered(
        context: ChannelHandlerContext,
        event: Any,
    ) {
        if (event is SslHandshakeCompletionEvent) {
            val channel = context.channel() as QuicChannel
            val protocol = if (event.isSuccess) channel.sslEngine()?.applicationProtocol else null
            when (state.handshake(event.isSuccess, protocol)) {
                AlpnDispatchResult.NATIVE -> dispatch(context, onNative)
                AlpnDispatchResult.HTTP3 -> dispatch(context, onHttp3)
                AlpnDispatchResult.REJECT -> channel.closeApplication(1, "unsupported ALPN")
                AlpnDispatchResult.WAITING -> Unit
            }
        }
        context.fireUserEventTriggered(event)
    }

    override fun channelInactive(context: ChannelHandlerContext) {
        if (accepted) onClosed()
        context.fireChannelInactive()
    }

    private fun dispatch(
        context: ChannelHandlerContext,
        install: () -> Unit,
    ) {
        if (!onAccepted()) {
            (context.channel() as QuicChannel).closeApplication(1, "too many concurrent connections")
            return
        }
        accepted = true
        install()
    }
}

private enum class Protocol {
    NATIVE,
    HTTP3,
}

private class ServerConnectionState(
    val protocol: Protocol,
) {
    private val activeStreams = AtomicInteger()
    private val activeSession = AtomicLong(NO_SESSION)
    private val rawStreams = ConcurrentHashMap.newKeySet<QuicStreamChannel>()

    fun tryAcquire(limit: Int?): Boolean {
        if (limit == null) {
            activeStreams.incrementAndGet()
            return true
        }
        while (true) {
            val current = activeStreams.get()
            if (current >= limit) return false
            if (activeStreams.compareAndSet(current, current + 1)) return true
        }
    }

    fun release() {
        activeStreams.decrementAndGet()
    }

    fun tryOpenSession(sessionId: Long): Boolean = activeSession.compareAndSet(NO_SESSION, sessionId)

    fun activeSessionId(): Long? = activeSession.get().takeUnless { it == NO_SESSION }

    fun registerRawStream(
        sessionId: Long,
        stream: QuicStreamChannel,
    ): Boolean {
        if (activeSession.get() != sessionId) return false
        rawStreams += stream
        if (activeSession.get() == sessionId) return true
        rawStreams -= stream
        return false
    }

    fun unregisterRawStream(stream: QuicStreamChannel) {
        rawStreams -= stream
    }

    fun closeSession(sessionId: Long) {
        if (!activeSession.compareAndSet(sessionId, NO_SESSION)) return
        rawStreams.toList().forEach { stream ->
            stream.cancelBoth()
            stream.close()
        }
    }

    private companion object {
        const val NO_SESSION = -1L
    }
}

internal class ServerFrameInput(
    options: NettyTransportOptions,
    private val onOverflow: () -> Unit = {},
) {
    private val parser = IncrementalFrameReader(options.maxFrameSize)
    private val frames = CoroutineChannel<ByteArray>(options.inboundQueueCapacity)
    private val accepting = AtomicBoolean(true)

    fun handler(): ChannelInboundHandlerAdapter =
        object : ChannelInboundHandlerAdapter() {
            override fun channelRead(
                context: ChannelHandlerContext,
                message: Any,
            ) {
                val body = message as ByteBuf
                try {
                    if (accepting.get()) offer(body.copyReadableBytes())
                } finally {
                    body.release()
                }
            }

            override fun userEventTriggered(
                context: ChannelHandlerContext,
                event: Any,
            ) {
                if (event is ChannelInputShutdownEvent || event is ChannelInputShutdownReadComplete) shutdown()
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
            }
        }

    fun feed(input: ByteBuf) {
        if (!accepting.get()) return
        try {
            for (frame in parser.feed(input)) {
                if (!offer(frame)) break
            }
        } catch (error: Throwable) {
            fail(error)
        }
    }

    fun finish() {
        try {
            parser.finish()
            if (accepting.compareAndSet(true, false)) frames.close()
        } catch (error: Throwable) {
            fail(error)
        }
    }

    fun shutdown() {
        if (accepting.compareAndSet(true, false)) frames.close()
    }

    suspend fun receive(): ByteArray? {
        val result = frames.receiveCatching()
        result.exceptionOrNull()?.let { throw it }
        return result.getOrNull()
    }

    fun fail(error: Throwable) {
        if (accepting.compareAndSet(true, false)) frames.close(error)
    }

    private fun offer(frame: ByteArray): Boolean {
        if (!accepting.get()) return false
        if (frames.trySend(frame).isFailure) {
            if (accepting.compareAndSet(true, false)) {
                frames.close(transportException("server inbound frame queue is full"))
                onOverflow()
            }
            return false
        }
        return true
    }
}

private fun Throwable.statusForWire(): Status =
    when (this) {
        is TrevRpcException -> status
        is kotlinx.coroutines.TimeoutCancellationException -> Status.deadlineExceeded("initial request frame timeout")
        is CancellationException -> Status.cancelled("RPC cancelled")
        else -> Status.invalidArgument(message ?: "invalid RPC transport frame")
    }

internal fun isTrevRpcMediaType(values: List<String>): Boolean =
    values.size == 1 && values.single().equals(TREV_RPC_MEDIA_TYPE, ignoreCase = true)
