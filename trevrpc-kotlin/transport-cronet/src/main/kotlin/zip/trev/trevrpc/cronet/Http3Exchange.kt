package zip.trev.trevrpc.cronet

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import zip.trev.trevrpc.DEFAULT_MAX_FRAME_SIZE
import zip.trev.trevrpc.FrameDecoder
import zip.trev.trevrpc.RpcClientStream
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcResponse
import zip.trev.trevrpc.RpcStreamFrame
import zip.trev.trevrpc.RpcStreamFrameKind
import zip.trev.trevrpc.RpcTransport
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.WireCodec
import zip.trev.trevrpc.frame
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

internal const val TREV_RPC_CONTENT_TYPE = "application/trevrpc"

data class CronetTransportOptions(
    val readBufferSize: Int = 64 * 1024,
    val responseChannelCapacity: Int = 8,
    val maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE,
) {
    init {
        require(readBufferSize > 0) { "readBufferSize must be positive" }
        require(responseChannelCapacity > 0) { "responseChannelCapacity must be positive" }
        require(maxFrameSize >= 0) { "maxFrameSize must be non-negative" }
    }
}

internal data class ResponseHeaders(
    val statusCode: Int,
    val fields: List<Pair<String, String>>,
)

internal interface DuplexStreamFactory {
    fun open(callback: DuplexCallback): DuplexStream
}

internal interface DuplexStream {
    fun start()

    fun read(buffer: ByteBuffer)

    fun write(
        buffer: ByteBuffer,
        endOfStream: Boolean,
    )

    fun flush()

    fun cancel()
}

internal interface DuplexCallback {
    fun onReady(stream: DuplexStream)

    fun onHeaders(
        stream: DuplexStream,
        headers: ResponseHeaders,
    )

    fun onRead(
        stream: DuplexStream,
        buffer: ByteBuffer,
        endOfStream: Boolean,
    )

    fun onWrite(
        stream: DuplexStream,
        buffer: ByteBuffer,
        endOfStream: Boolean,
    )

    fun onSucceeded(stream: DuplexStream)

    fun onFailed(
        stream: DuplexStream,
        error: Throwable,
    )

    fun onCanceled(stream: DuplexStream)
}

internal class Http3RpcTransport(
    private val factory: DuplexStreamFactory,
    private val options: CronetTransportOptions = CronetTransportOptions(),
    private val coroutineContext: CoroutineContext = Dispatchers.IO,
) : RpcTransport {
    override suspend fun unary(request: RpcRequest): RpcResponse {
        val exchange = open(request, endOfStream = true)
        try {
            val first = exchange.receiveBody() ?: protocolError("unary response body was empty")
            if (exchange.receiveBody() != null) protocolError("unary response contained more than one frame")
            return WireCodec.decodeResponse(first)
        } catch (error: CancellationException) {
            exchange.close(error)
            throw error
        } catch (error: Throwable) {
            exchange.close(error)
            throw error
        }
    }

    override suspend fun openStream(request: RpcRequest): RpcClientStream = StreamingTransportStream(open(request, endOfStream = false))

    private suspend fun open(
        request: RpcRequest,
        endOfStream: Boolean,
    ): Http3Exchange {
        val parent = coroutineContext[Job]
        val scope = CoroutineScope(SupervisorJob(parent) + coroutineContext.minusKey(Job))
        val exchange = Http3Exchange(factory, options, scope, streaming = !endOfStream)
        exchange.start(request, endOfStream)
        return exchange
    }
}

private class StreamingTransportStream(
    private val exchange: Http3Exchange,
) : RpcClientStream {
    override suspend fun send(body: ByteArray) = exchange.send(body)

    override suspend fun finishSend() = exchange.finishSend()

    override suspend fun receive(): RpcStreamFrame? = exchange.receiveBody()?.let(WireCodec::decodeStreamFrame)

    override suspend fun close(cause: Throwable?) = exchange.close(cause)
}

internal class Http3Exchange(
    private val factory: DuplexStreamFactory,
    private val options: CronetTransportOptions,
    private val scope: CoroutineScope,
    private val streaming: Boolean,
) : DuplexCallback {
    private val completed = AtomicBoolean(false)
    private val canceled = AtomicBoolean(false)
    private val headersReceived = AtomicBoolean(false)
    private val responseEndReceived = AtomicBoolean(false)
    private val remoteTerminalSeen = AtomicBoolean(false)
    private val responseBodies = Channel<ByteArray>(options.responseChannelCapacity)
    private val readChunks = Channel<ReadChunk>(1)
    private val writeReady = CompletableDeferred<Unit>()
    private val sendLock = Mutex()
    private val writeLock = Any()
    private var sendFinished = false
    private var pendingWrite: PendingWrite? = null
    private lateinit var stream: DuplexStream

    suspend fun start(
        request: RpcRequest,
        endOfStream: Boolean,
    ) {
        try {
            stream = factory.open(this)
            stream.start()
        } catch (error: Throwable) {
            if (::stream.isInitialized) {
                fail(error)
            } else if (completeOnce()) {
                responseBodies.close(error)
                readChunks.close(error)
                scope.cancel(cancellationException("Cronet stream creation failed", error))
            }
            throw error
        }
        scope.launch { readResponses() }
        sendFinished = endOfStream
        writeDirect(
            endOfStream,
            "initial request write failed",
        ) { frame(WireCodec.encode(request), options.maxFrameSize) }
    }

    suspend fun send(body: ByteArray) {
        sendLock.withLock {
            checkSendOpen()
            writeDirect(false, "request write failed") {
                frame(WireCodec.encode(RpcStreamFrame.message(body)), options.maxFrameSize)
            }
        }
    }

    suspend fun finishSend() {
        sendLock.withLock {
            if (sendFinished) return
            checkSendOpen()
            sendFinished = true
            writeDirect(true, "request finish failed") { byteArrayOf() }
        }
    }

    suspend fun receiveBody(): ByteArray? {
        val result = responseBodies.receiveCatching()
        return result.getOrNull() ?: result.exceptionOrNull()?.let { throw it }
    }

    suspend fun close(cause: Throwable? = null) {
        if (completeOnce()) {
            cancelNative()
            responseBodies.close(cause)
            readChunks.close(cause)
            cancelPendingWrite(cause ?: CancellationException("RPC stream closed"))
            scope.cancel(cancellationException("RPC stream closed", cause))
        }
    }

    override fun onReady(stream: DuplexStream) {
        if (stream !== this.stream || completed.get() || remoteTerminalSeen.get()) return
        writeReady.complete(Unit)
    }

    override fun onHeaders(
        stream: DuplexStream,
        headers: ResponseHeaders,
    ) {
        if (stream !== this.stream || completed.get() || remoteTerminalSeen.get()) return
        if (!headersReceived.compareAndSet(false, true)) {
            fail(protocolException("received response headers more than once"))
            return
        }
        val error = validateHeaders(headers)
        if (error != null) {
            fail(error)
            return
        }
        requestRead()
    }

    override fun onRead(
        stream: DuplexStream,
        buffer: ByteBuffer,
        endOfStream: Boolean,
    ) {
        if (stream !== this.stream || completed.get() || remoteTerminalSeen.get()) return
        val bytes = ByteArray(buffer.position())
        buffer.flip()
        buffer.get(bytes)
        if (endOfStream) responseEndReceived.set(true)
        if (readChunks.trySend(ReadChunk(bytes, endOfStream)).isFailure) {
            fail(protocolException("Cronet delivered overlapping response reads"))
        }
    }

    override fun onWrite(
        stream: DuplexStream,
        buffer: ByteBuffer,
        endOfStream: Boolean,
    ) {
        if (stream !== this.stream) return
        val continuation =
            synchronized(writeLock) {
                val pending = pendingWrite
                if (pending == null || pending.buffer !== buffer || pending.endOfStream != endOfStream) {
                    null
                } else {
                    pendingWrite = null
                    pending.continuation
                }
            }
        if (continuation == null) {
            if (!completed.get() && !remoteTerminalSeen.get()) {
                fail(protocolException("Cronet completed an unexpected request write"))
            }
        } else {
            continuation.resume(Unit)
        }
    }

    override fun onSucceeded(stream: DuplexStream) {
        if (stream !== this.stream || completed.get() || remoteTerminalSeen.get()) return
        if (!headersReceived.get()) {
            fail(protocolException("Cronet stream succeeded before response headers"))
        } else if (!responseEndReceived.get()) {
            if (readChunks.trySend(ReadChunk(byteArrayOf(), true)).isFailure) {
                fail(protocolException("Cronet succeeded with a response read still pending"))
            }
        }
    }

    override fun onFailed(
        stream: DuplexStream,
        error: Throwable,
    ) {
        if (stream === this.stream && !remoteTerminalSeen.get()) {
            fail(transportException("Cronet stream failed", error))
        }
    }

    override fun onCanceled(stream: DuplexStream) {
        if (stream === this.stream && !completed.get() && !remoteTerminalSeen.get()) {
            fail(transportException("Cronet stream was canceled"))
        }
    }

    private suspend fun writeDirect(
        endOfStream: Boolean,
        failureMessage: String,
        bytes: () -> ByteArray,
    ) {
        try {
            write(bytes(), endOfStream)
        } catch (error: CancellationException) {
            if (!completed.get() && !remoteTerminalSeen.get()) {
                fail(error)
            }
            throw error
        } catch (error: Throwable) {
            val transportError =
                if (error is TrevRpcException) error else transportException(failureMessage, error)
            if (!remoteTerminalSeen.get()) fail(transportError)
            throw transportError
        }
    }

    private suspend fun write(
        bytes: ByteArray,
        endOfStream: Boolean,
    ) {
        writeReady.await()
        if (completed.get()) throw CancellationException("RPC stream completed")
        val buffer = ByteBuffer.allocateDirect(bytes.size)
        buffer.put(bytes)
        buffer.flip()
        suspendCancellableCoroutine { continuation ->
            val pending = PendingWrite(buffer, endOfStream, continuation)
            continuation.invokeOnCancellation {
                val canceledByCaller =
                    synchronized(writeLock) {
                        if (pendingWrite !== pending) {
                            false
                        } else {
                            pendingWrite = null
                            true
                        }
                    }
                if (canceledByCaller) {
                    fail(CancellationException("request write was canceled"))
                }
            }
            val installed =
                synchronized(writeLock) {
                    if (completed.get() || remoteTerminalSeen.get() || !continuation.isActive) {
                        false
                    } else {
                        check(pendingWrite == null) { "only one Cronet write may be in flight" }
                        pendingWrite = pending
                        true
                    }
                }
            if (!installed) {
                if (continuation.isActive) {
                    continuation.resumeWithException(CancellationException("RPC stream completed"))
                }
                return@suspendCancellableCoroutine
            }

            var dispatchError: Throwable? = null
            synchronized(writeLock) {
                if (pendingWrite === pending && !completed.get() && !remoteTerminalSeen.get()) {
                    try {
                        stream.write(buffer, endOfStream)
                        stream.flush()
                    } catch (error: Throwable) {
                        pendingWrite = null
                        dispatchError = error
                    }
                }
            }
            dispatchError?.let { error ->
                if (continuation.isActive) continuation.resumeWithException(error)
            }
        }
    }

    private suspend fun readResponses() {
        val decoder = FrameDecoder(options.maxFrameSize)
        try {
            for (chunk in readChunks) {
                for (body in decoder.feed(chunk.bytes)) {
                    if (streaming) {
                        val decoded = WireCodec.decodeStreamFrame(body)
                        if (decoded.kind == RpcStreamFrameKind.STATUS) beginRemoteStatus()
                        responseBodies.send(body)
                        if (decoded.kind == RpcStreamFrameKind.STATUS) {
                            finishRemoteStatus()
                            return
                        }
                    } else {
                        responseBodies.send(body)
                    }
                }
                if (chunk.endOfStream) {
                    decoder.finish()
                    finishEndOfResponse()
                    return
                }
                requestRead()
            }
        } catch (error: CancellationException) {
            if (!completed.get()) fail(transportException("response processing was canceled", error))
        } catch (error: Throwable) {
            fail(error)
        }
    }

    private fun requestRead() {
        if (completed.get()) return
        try {
            stream.read(ByteBuffer.allocateDirect(options.readBufferSize))
        } catch (error: Throwable) {
            fail(transportException("Cronet response read failed", error))
        }
    }

    private fun finishRemoteStatus() {
        if (completeOnce()) {
            writeReady.completeExceptionally(CancellationException("remote terminal status received"))
            responseBodies.close()
            readChunks.close()
            cancelPendingWrite(CancellationException("remote terminal status received"))
            cancelNative()
            scope.cancel(CancellationException("remote terminal status received"))
        }
    }

    private fun beginRemoteStatus() {
        if (beginRemoteTerminalOnce()) {
            cancelPendingWrite(CancellationException("remote terminal status received"))
            cancelNative()
        }
    }

    private fun finishEndOfResponse() {
        if (completeOnce()) {
            writeReady.completeExceptionally(CancellationException("response ended"))
            responseBodies.close()
            readChunks.close()
            cancelPendingWrite(CancellationException("response ended"))
            cancelNative()
            scope.cancel(CancellationException("response ended"))
        }
    }

    private fun fail(error: Throwable) {
        if (completeOnce()) {
            writeReady.completeExceptionally(error)
            responseBodies.close(error)
            readChunks.close(error)
            cancelPendingWrite(error)
            cancelNative()
            scope.cancel(cancellationException("Cronet exchange failed", error))
        }
    }

    private fun cancelPendingWrite(error: Throwable) {
        val continuation =
            synchronized(writeLock) {
                val pending = pendingWrite
                pendingWrite = null
                pending?.continuation
            }
        continuation?.resumeWithException(error)
    }

    private fun cancelNative() {
        if (canceled.compareAndSet(false, true)) runCatching(stream::cancel)
    }

    private fun completeOnce(): Boolean =
        synchronized(writeLock) {
            completed.compareAndSet(false, true)
        }

    private fun beginRemoteTerminalOnce(): Boolean =
        synchronized(writeLock) {
            remoteTerminalSeen.compareAndSet(false, true)
        }

    private fun checkSendOpen() {
        if (completed.get() || sendFinished) {
            throw TrevRpcException(Status.cancelled("request stream is closed"))
        }
    }

    private data class ReadChunk(
        val bytes: ByteArray,
        val endOfStream: Boolean,
    )

    private data class PendingWrite(
        val buffer: ByteBuffer,
        val endOfStream: Boolean,
        val continuation: CancellableContinuation<Unit>,
    )
}

private fun validateHeaders(headers: ResponseHeaders): TrevRpcException? {
    if (headers.statusCode != 200) {
        return transportException("HTTP/3 response status was ${headers.statusCode}, expected 200")
    }
    val contentTypes =
        headers.fields
            .filter { it.first.equals("content-type", ignoreCase = true) }
            .map { it.second }
    if (contentTypes.size != 1 || !contentTypes.single().equals(TREV_RPC_CONTENT_TYPE, ignoreCase = true)) {
        return protocolException("HTTP/3 response Content-Type must be exactly $TREV_RPC_CONTENT_TYPE")
    }
    return null
}

private fun protocolError(message: String): Nothing = throw protocolException(message)

private fun protocolException(message: String): TrevRpcException = TrevRpcException(Status.internal(message))

private fun transportException(
    message: String,
    cause: Throwable? = null,
): TrevRpcException = TrevRpcException(Status.unavailable(message), cause)

private fun cancellationException(
    message: String,
    cause: Throwable?,
): CancellationException = CancellationException(message).also { if (cause != null) it.initCause(cause) }
