package zip.trev.trevrpc.cronet

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
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
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import java.net.URI
import java.nio.ByteBuffer
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

/**
 * Cronet channel factory.
 *
 * The published transport is a JVM 17 API JAR and does not select or bundle a Cronet provider. The
 * caller selects and owns the provider, [CronetEngine], and callback [Executor]. TrevRPC
 * never shuts down either borrowed resource. A successful [RpcChannel.close] proves that all TrevRPC
 * exchanges settled logically, reached a terminal Cronet callback, returned from every provider call,
 * and drained their serialized callback queue; only then may the caller shut down the engine and
 * executor. A failed or timed-out close does not prove that either borrowed resource is quiescent.
 */
object CronetRpcChannel {
    fun create(
        engine: CronetEngine,
        origin: String,
        callbackExecutor: Executor,
        options: CronetTransportOptions = CronetTransportOptions(),
        coroutineContext: CoroutineContext = Dispatchers.IO,
    ): RpcChannel {
        val containedExecutor = DrainableSerialExecutor(callbackExecutor)
        return CronetChannel(
            Http3RpcTransport(
                CronetDuplexStreamFactory(engine, rpcUrl(origin), containedExecutor),
                options,
                coroutineContext,
                containedExecutor,
            ),
        )
    }
}

internal class CronetChannel(
    private val delegate: CronetTransport,
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
        delegate.close()
    }

    private fun checkOpen() {
        if (closed.get()) throw TrevRpcException(Status.unavailable("RPC channel is closed"))
    }
}

internal interface CallbackDrain {
    suspend fun barrier()
}

internal object ImmediateCallbackDrain : CallbackDrain {
    override suspend fun barrier() = Unit
}

/** Isolates synchronous provider calls so coroutine timeout does not structurally await them. */
internal object DetachedExternalCalls : CoroutineDispatcher() {
    private val threadId = AtomicLong()
    private val executor =
        Executors.newCachedThreadPool { command ->
            Thread(command, "trevrpc-cronet-external-${threadId.incrementAndGet()}").apply { isDaemon = true }
        }

    override fun dispatch(
        context: CoroutineContext,
        block: Runnable,
    ) {
        executor.execute(block)
    }

    fun dispatch(action: () -> Unit) {
        executor.execute(action)
    }

    suspend fun await(action: () -> Unit) {
        val outcome = CompletableDeferred<Result<Unit>>()
        dispatch { outcome.complete(runCatching(action)) }
        outcome.await().getOrThrow()
    }
}

/** Serializes Cronet callbacks without taking ownership of the caller's executor. */
internal class DrainableSerialExecutor(
    private val delegate: Executor,
) : Executor,
    CallbackDrain {
    private val lock = Any()
    private val tasks = ArrayDeque<Runnable>()
    private var workerRunning = false

    override fun execute(command: Runnable) {
        synchronized(lock) {
            tasks.addLast(command)
            if (workerRunning) return
            workerRunning = true
            try {
                // Hold the reentrant monitor until submission is accepted or rejected so another
                // callback cannot return successfully and then be discarded by this submission.
                delegate.execute(::drainTasks)
            } catch (error: Throwable) {
                workerRunning = false
                tasks.clear()
                throw error
            }
        }
    }

    override suspend fun barrier() {
        val reached = CompletableDeferred<Unit>()
        try {
            DetachedExternalCalls.await {
                val submitWorker =
                    synchronized(lock) {
                        tasks.addLast { reached.complete(Unit) }
                        if (workerRunning) {
                            false
                        } else {
                            workerRunning = true
                            true
                        }
                    }
                if (submitWorker) delegate.execute(::drainTasks)
            }
        } catch (error: CancellationException) {
            throw error
        } catch (error: Throwable) {
            DetachedExternalCalls.dispatch {
                synchronized(lock) {
                    workerRunning = false
                    tasks.clear()
                }
            }
            throw TrevRpcException(Status.unavailable("Cronet callback executor rejected the drain barrier"), error)
        }
        reached.await()
    }

    private fun drainTasks() {
        while (true) {
            val task =
                synchronized(lock) {
                    if (tasks.isEmpty()) {
                        workerRunning = false
                        return
                    }
                    tasks.removeFirst()
                }
            runCatching(task::run)
        }
    }
}

internal class CronetDuplexStreamFactory(
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

    override fun onStreamReady(stream: BidirectionalStream) = dispatch { callback.onReady(facade) }

    override fun onResponseHeadersReceived(
        stream: BidirectionalStream,
        info: UrlResponseInfo,
    ) = dispatch {
        callback.onHeaders(
            facade,
            ResponseHeaders(
                info.httpStatusCode,
                info.allHeadersAsList.map { it.key to it.value },
            ),
        )
    }

    override fun onReadCompleted(
        stream: BidirectionalStream,
        info: UrlResponseInfo,
        buffer: ByteBuffer,
        endOfStream: Boolean,
    ) = dispatch { callback.onRead(facade, buffer, endOfStream) }

    override fun onWriteCompleted(
        stream: BidirectionalStream,
        info: UrlResponseInfo,
        buffer: ByteBuffer,
        endOfStream: Boolean,
    ) = dispatch { callback.onWrite(facade, buffer, endOfStream) }

    override fun onSucceeded(
        stream: BidirectionalStream,
        info: UrlResponseInfo,
    ) = dispatch { callback.onSucceeded(facade) }

    override fun onFailed(
        stream: BidirectionalStream,
        info: UrlResponseInfo?,
        error: CronetException,
    ) = dispatch { callback.onFailed(facade, error) }

    override fun onCanceled(
        stream: BidirectionalStream,
        info: UrlResponseInfo?,
    ) = dispatch { callback.onCanceled(facade) }

    private fun dispatch(action: () -> Unit) {
        try {
            action()
        } catch (error: Throwable) {
            if (::facade.isInitialized) runCatching { callback.onCallbackFailure(facade, error) }
        }
    }
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
