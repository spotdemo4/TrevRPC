package zip.trev.trevrpc.netty

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
import zip.trev.trevrpc.RpcTransportConnection
import zip.trev.trevrpc.RpcTransportLifecycle
import zip.trev.trevrpc.RpcTransportStream
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.WireCodec
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.channels.Channel as CoroutineChannel

class NettyHttp3RpcTransport private constructor(
    private val endpoint: ClientQuicEndpoint,
    private val config: NettyQuicClientConfig,
) : RpcTransport,
    RpcTransportLifecycle,
    AutoCloseable {
    private val scope =
        CoroutineScope(
            SupervisorJob() + Dispatchers.IO.limitedParallelism(config.options.workerParallelism),
        )
    private val closed = AtomicBoolean(false)

    override suspend fun unary(request: RpcRequest): RpcResponse {
        checkOpen()
        val inbox = Http3FrameInbox(config.options)
        val stream = openRequest(inbox)
        try {
            writeRequestHeaders(stream)
            writeDataFrame(stream, WireCodec.encode(request), config.options.maxFrameSize).awaitCompletion()
            stream.shutdownOutput().awaitCompletion()
            inbox.awaitSuccess()
            val body = inbox.receive() ?: throw transportException("HTTP/3 unary response ended before a frame")
            val response = WireCodec.decodeResponse(body)
            inbox.requireEnd()
            stream.close()
            return response
        } catch (error: CancellationException) {
            stream.cancelBoth()
            throw error
        } catch (error: Throwable) {
            stream.cancelBoth()
            throw transportException("HTTP/3 unary RPC failed", error)
        }
    }

    override suspend fun openStream(
        request: RpcRequest,
        requestBody: Flow<ByteArray>,
    ): RpcTransportStream {
        checkOpen()
        val inbox = Http3FrameInbox(config.options)
        val stream = openRequest(inbox)
        writeRequestHeaders(stream)
        inbox.awaitSuccess()
        val writer =
            scope.launch {
                try {
                    writeDataFrame(stream, WireCodec.encode(request), config.options.maxFrameSize).awaitCompletion()
                    requestBody.collect { body ->
                        writeDataFrame(
                            stream,
                            WireCodec.encode(RpcStreamFrame.message(body)),
                            config.options.maxFrameSize,
                        ).awaitCompletion()
                    }
                    stream.shutdownOutput().awaitCompletion()
                } catch (error: Throwable) {
                    stream.cancelBoth()
                    inbox.fail(error)
                }
            }
        return Http3RpcTransportStream(stream, inbox, writer)
    }

    suspend fun shutdown() {
        if (!closed.compareAndSet(false, true)) return
        scope.cancel()
        endpoint.quicChannel.closeApplication(0, "HTTP/3 client closed")
        endpoint.quicChannel.closeFuture().awaitCompletion()
        endpoint.datagramChannel.close().awaitCompletion()
        endpoint.group.shutdownGracefully().awaitValue()
    }

    override suspend fun awaitClosed() {
        endpoint.quicChannel.closeFuture().awaitCompletion()
    }

    fun asManagedConnection(): RpcTransportConnection = RpcTransportConnection(this, this, ::shutdown)

    override fun close() {
        if (!closed.compareAndSet(false, true)) return
        scope.cancel()
        endpoint.quicChannel.closeApplication(0, "HTTP/3 client closed")
        endpoint.quicChannel.closeFuture().syncUninterruptibly()
        endpoint.datagramChannel.close().syncUninterruptibly()
        endpoint.group.shutdownGracefully().syncUninterruptibly()
    }

    private suspend fun openRequest(inbox: Http3FrameInbox): QuicStreamChannel =
        Http3
            .newRequestStreamBootstrap(endpoint.quicChannel, inbox)
            .option(ChannelOption.WRITE_BUFFER_WATER_MARK, config.options.waterMark)
            .create()
            .awaitValue()

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
        suspend fun connect(config: NettyQuicClientConfig): NettyHttp3RpcTransport {
            val endpoint = connectQuic(config, HTTP3_ALPN, Http3ClientConnectionHandler())
            return NettyHttp3RpcTransport(endpoint, config)
        }

        internal fun fromEndpoint(
            endpoint: ClientQuicEndpoint,
            config: NettyQuicClientConfig,
        ): NettyHttp3RpcTransport = NettyHttp3RpcTransport(endpoint, config)
    }
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
    options: NettyTransportOptions,
) : Http3RequestStreamInboundHandler() {
    private val parser = IncrementalFrameReader(options.maxFrameSize)
    private val frames = CoroutineChannel<ByteArray>(options.inboundQueueCapacity)
    private val responseValidated = CompletableDeferred<Unit>()
    private var headersSeen = false
    private var responseAccepted = false

    override fun channelRead(
        context: ChannelHandlerContext,
        frame: Http3HeadersFrame,
    ) {
        if (headersSeen) {
            fail(TrevRpcException(Status.unavailable("HTTP/3 response contained duplicate headers or trailers")))
            return
        }
        headersSeen = true
        val statuses = frame.headers().getAll(":status").map(CharSequence::toString)
        val contentTypes = frame.headers().getAll("content-type").map(CharSequence::toString)
        when {
            statuses != listOf("200") -> {
                fail(
                    TrevRpcException(
                        Status.unavailable("HTTP/3 response must contain exactly status 200"),
                    ),
                )
            }

            !isTrevRpcMediaType(contentTypes) -> {
                fail(
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
                fail(TrevRpcException(Status.unavailable("HTTP/3 response data arrived before valid headers")))
                (context.channel() as QuicStreamChannel).cancelBoth()
                return
            }
            parser.feed(frame.content()).forEach { body ->
                if (frames.trySend(body).isFailure) {
                    fail(transportException("HTTP/3 inbound frame queue is full"))
                    (context.channel() as QuicStreamChannel).cancelBoth()
                    return
                }
            }
        } catch (error: Throwable) {
            fail(error)
            (context.channel() as QuicStreamChannel).cancelBoth()
        } finally {
            frame.release()
        }
    }

    override fun channelInputClosed(context: ChannelHandlerContext) {
        try {
            if (!responseAccepted) {
                throw TrevRpcException(Status.unavailable("HTTP/3 response ended before valid headers"))
            }
            parser.finish()
            frames.close()
        } catch (error: Throwable) {
            fail(error)
        }
    }

    override fun exceptionCaught(
        context: ChannelHandlerContext,
        cause: Throwable,
    ) {
        fail(cause)
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

    suspend fun requireEnd() {
        if (receive() != null) {
            throw TrevRpcException(Status.unavailable("HTTP/3 response contained data after its terminal frame"))
        }
    }
}

private class Http3RpcTransportStream(
    private val stream: QuicStreamChannel,
    private val inbox: Http3FrameInbox,
    private val writer: Job,
) : RpcTransportStream {
    private val closed = AtomicBoolean(false)

    override suspend fun receive(): RpcStreamFrame? {
        val body = inbox.receive() ?: return null
        val frame = WireCodec.decodeStreamFrame(body)
        if (frame.kind == RpcStreamFrameKind.STATUS && closed.compareAndSet(false, true)) {
            writer.cancel()
            try {
                inbox.requireEnd()
            } finally {
                stream.close()
            }
        }
        return frame
    }

    override suspend fun close(cause: Throwable?) {
        if (!closed.compareAndSet(false, true)) return
        writer.cancel(CancellationException("HTTP/3 RPC stream closed", cause))
        stream.cancelBoth()
        stream.close().awaitCompletion()
    }
}
