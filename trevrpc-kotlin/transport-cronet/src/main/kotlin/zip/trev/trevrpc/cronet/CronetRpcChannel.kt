package zip.trev.trevrpc.cronet

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import org.chromium.net.BidirectionalStream
import org.chromium.net.CronetEngine
import org.chromium.net.CronetException
import org.chromium.net.UrlResponseInfo
import zip.trev.trevrpc.RpcChannel
import zip.trev.trevrpc.RpcChannelState
import zip.trev.trevrpc.RpcClientStream
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcResponse
import zip.trev.trevrpc.RpcTransport
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import java.net.URI
import java.nio.ByteBuffer
import java.util.concurrent.Executor
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext

/**
 * Android Cronet channel factory. Cronet owns connection pooling and reconnection; the caller owns
 * the engine and callback executor, including shutting them down.
 */
object CronetRpcChannel {
    fun create(
        engine: CronetEngine,
        origin: String,
        callbackExecutor: Executor,
        options: CronetTransportOptions = CronetTransportOptions(),
        coroutineContext: CoroutineContext = Dispatchers.IO,
    ): RpcChannel =
        CronetChannel(
            Http3RpcTransport(
                CronetDuplexStreamFactory(engine, rpcUrl(origin), callbackExecutor),
                options,
                coroutineContext,
            ),
        )
}

internal class CronetChannel(
    private val delegate: RpcTransport,
) : RpcChannel {
    private val closed = AtomicBoolean(false)
    private val mutableState = MutableStateFlow(RpcChannelState.READY)

    override val state: StateFlow<RpcChannelState> = mutableState.asStateFlow()

    override suspend fun unary(request: RpcRequest): RpcResponse {
        checkOpen()
        return delegate.unary(request)
    }

    override suspend fun openStream(request: RpcRequest): RpcClientStream {
        checkOpen()
        return delegate.openStream(request)
    }

    override suspend fun awaitReady() = checkOpen()

    override suspend fun close() {
        if (closed.compareAndSet(false, true)) mutableState.value = RpcChannelState.CLOSED
    }

    private fun checkOpen() {
        if (closed.get()) throw TrevRpcException(Status.unavailable("RPC channel is closed"))
    }
}

private class CronetDuplexStreamFactory(
    private val engine: CronetEngine,
    private val url: String,
    private val executor: Executor,
) : DuplexStreamFactory {
    override fun open(callback: DuplexCallback): DuplexStream {
        val cronetCallback = CronetCallback(callback)
        val stream =
            engine
                .newBidirectionalStreamBuilder(url, cronetCallback, executor)
                .setHttpMethod("POST")
                .addHeader("Content-Type", TREV_RPC_CONTENT_TYPE)
                .build()
        return CronetDuplexStream(stream).also(cronetCallback::attach)
    }
}

private class CronetDuplexStream(
    private val stream: BidirectionalStream,
) : DuplexStream {
    override fun start() = stream.start()

    override fun read(buffer: ByteBuffer) = stream.read(buffer)

    override fun write(
        buffer: ByteBuffer,
        endOfStream: Boolean,
    ) = stream.write(buffer, endOfStream)

    override fun flush() = stream.flush()

    override fun cancel() = stream.cancel()
}

private class CronetCallback(
    private val callback: DuplexCallback,
) : BidirectionalStream.Callback() {
    private lateinit var facade: CronetDuplexStream

    fun attach(stream: CronetDuplexStream) {
        facade = stream
    }

    override fun onStreamReady(stream: BidirectionalStream) = callback.onReady(facade)

    override fun onResponseHeadersReceived(
        stream: BidirectionalStream,
        info: UrlResponseInfo,
    ) = callback.onHeaders(
        facade,
        ResponseHeaders(
            info.httpStatusCode,
            info.allHeadersAsList.map { it.key to it.value },
        ),
    )

    override fun onReadCompleted(
        stream: BidirectionalStream,
        info: UrlResponseInfo,
        buffer: ByteBuffer,
        endOfStream: Boolean,
    ) = callback.onRead(facade, buffer, endOfStream)

    override fun onWriteCompleted(
        stream: BidirectionalStream,
        info: UrlResponseInfo,
        buffer: ByteBuffer,
        endOfStream: Boolean,
    ) = callback.onWrite(facade, buffer, endOfStream)

    override fun onSucceeded(
        stream: BidirectionalStream,
        info: UrlResponseInfo,
    ) = callback.onSucceeded(facade)

    override fun onFailed(
        stream: BidirectionalStream,
        info: UrlResponseInfo?,
        error: CronetException,
    ) = callback.onFailed(facade, error)

    override fun onCanceled(
        stream: BidirectionalStream,
        info: UrlResponseInfo?,
    ) = callback.onCanceled(facade)
}

private fun rpcUrl(origin: String): String {
    val uri = URI(origin)
    require(uri.scheme.equals("https", ignoreCase = true)) { "Cronet HTTP/3 origin must use https" }
    require(uri.rawAuthority != null && uri.host != null) { "Cronet HTTP/3 origin must include a host" }
    require(uri.rawUserInfo == null) { "Cronet HTTP/3 origin must not include user information" }
    require(uri.rawQuery == null && uri.rawFragment == null) { "Cronet HTTP/3 origin must not include a query or fragment" }
    require(uri.rawPath.isNullOrEmpty() || uri.rawPath == "/") { "Cronet HTTP/3 origin must not include a path" }
    return "https://${uri.rawAuthority}/trevrpc"
}
