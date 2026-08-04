package zip.trev.trevrpc.cronet

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withTimeout
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
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

internal const val TREV_RPC_CONTENT_TYPE = "application/trevrpc"

/**
 * Cronet transport limits. [closeTimeout] strictly bounds TrevRPC's shutdown wait. Provider
 * cancellation and callback-executor submission run on detached daemon workers, and close never
 * waits for a provider call while holding a transport monitor. A non-cooperative borrowed
 * implementation therefore cannot hold the closing coroutine past this bound.
 */
data class CronetTransportOptions(
    val readBufferSize: Int = 64 * 1024,
    val responseChannelCapacity: Int = 8,
    val maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE,
    val closeTimeout: Duration = 5.seconds,
) {
    init {
        require(readBufferSize > 0) { "readBufferSize must be positive" }
        require(responseChannelCapacity > 0) { "responseChannelCapacity must be positive" }
        require(maxFrameSize >= 0) { "maxFrameSize must be non-negative" }
        require(closeTimeout.isFinite() && closeTimeout.isPositive()) {
            "closeTimeout must be positive and finite"
        }
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

    fun onCallbackFailure(
        stream: DuplexStream,
        error: Throwable,
    )
}

internal interface CronetTransport : RpcTransport {
    suspend fun close()
}

internal class Http3RpcTransport(
    private val factory: DuplexStreamFactory,
    private val options: CronetTransportOptions = CronetTransportOptions(),
    coroutineContext: CoroutineContext = Dispatchers.IO,
    private val callbackDrain: CallbackDrain = ImmediateCallbackDrain,
    shutdownContext: CoroutineContext = DetachedExternalCalls,
) : CronetTransport {
    private val transportJob = SupervisorJob()
    private val shutdownJob = SupervisorJob()
    private val shutdownScope = CoroutineScope(shutdownJob + shutdownContext.minusKey(Job))
    private val exchangeContext = coroutineContext.minusKey(Job)
    private val exchanges = ExchangeRegistry()
    private val providerInvocations = ProviderInvocationTracker()
    private val closeLock = Any()
    private var closeResult: CompletableDeferred<Result<Unit>>? = null

    internal val activeExchangeCount: Int
        get() = exchanges.activeCount

    internal val logicallyActiveExchangeCount: Int
        get() = exchanges.logicallyActiveCount

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

    override suspend fun close() {
        beginClose().await().getOrThrow()
    }

    private suspend fun open(
        request: RpcRequest,
        endOfStream: Boolean,
    ): Http3Exchange {
        val lease = exchanges.admit()
        val exchange =
            Http3Exchange(
                factory,
                options,
                transportJob,
                exchangeContext,
                lease,
                providerInvocations,
                streaming = !endOfStream,
            )
        exchange.start(request, endOfStream)
        return exchange
    }

    private fun beginClose(): CompletableDeferred<Result<Unit>> =
        synchronized(closeLock) {
            closeResult?.let { return@synchronized it }
            val result = CompletableDeferred<Result<Unit>>()
            val plan = exchanges.beginClose()
            closeResult = result
            shutdownScope.launch {
                val outcome =
                    try {
                        withTimeout(options.closeTimeout) {
                            val closeCause = TrevRpcException(Status.unavailable("RPC channel is closed"))
                            plan.leases.forEach { it.requestCancel(closeCause) }
                            plan.drained.await()
                            providerInvocations.sealAndAwaitDrain()
                            callbackDrain.barrier()
                            exchanges.finishClose()
                        }
                        Result.success(Unit)
                    } catch (error: TimeoutCancellationException) {
                        exchanges.finishClose()
                        Result.failure(closeTimeoutException(options.closeTimeout, error))
                    } catch (error: Throwable) {
                        exchanges.finishClose()
                        Result.failure(error)
                    }
                result.complete(outcome)
                transportJob.cancel()
                shutdownJob.cancel()
            }
            result
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
    parentJob: Job,
    coroutineContext: CoroutineContext,
    private val lease: ExchangeLease,
    private val providerInvocations: ProviderInvocationTracker,
    private val streaming: Boolean,
) : DuplexCallback {
    private val job =
        CoroutineScope(parentJob + coroutineContext).launch(start = CoroutineStart.LAZY) {
            readResponses()
        }
    private val completed = AtomicBoolean(false)
    private val nativeCancelRequested = AtomicBoolean(false)
    private val headersReceived = AtomicBoolean(false)
    private val responseEndReceived = AtomicBoolean(false)
    private val remoteTerminalSeen = AtomicBoolean(false)
    private val responseBodies = Channel<ByteArray>(options.responseChannelCapacity)
    private val readChunks = Channel<ReadChunk>(1)
    private val writeReady = CompletableDeferred<Unit>()
    private val sendLock = Mutex()
    private val writeLock = Any()
    private val nativeLock = Any()
    private var sendFinished = false
    private var pendingWrite: PendingWrite? = null
    private var completionCause: Throwable? = null
    private var startInProgress = false
    private var nativeStarted = false
    private var nativeTerminalReceived = false
    private var cancelAfterStart = false
    private lateinit var stream: DuplexStream

    init {
        lease.attach(::cancelFromChannelClose)
    }

    suspend fun start(
        request: RpcRequest,
        endOfStream: Boolean,
    ) {
        val opened =
            try {
                providerInvocations.invoke { factory.open(this) }
            } catch (error: Throwable) {
                settleLogical(error, requestNativeCancel = false)
                lease.nativeDone()
                throw error
            }

        val closedBeforeStart =
            synchronized(nativeLock) {
                stream = opened
                if (completed.get()) {
                    completionCause ?: TrevRpcException(Status.unavailable("RPC channel is closed"))
                } else {
                    startInProgress = true
                    null
                }
            }
        if (closedBeforeStart != null) {
            lease.nativeDone()
            throw closedBeforeStart
        }

        try {
            providerInvocations.invoke(opened::start)
        } catch (error: Throwable) {
            synchronized(nativeLock) {
                startInProgress = false
            }
            val transportError = transportException("Cronet stream start failed", error)
            settleLogical(transportError, requestNativeCancel = false)
            lease.nativeDone()
            throw transportError
        }

        val cancelStartedStream: Boolean
        val nativeFinishedDuringStart: Boolean
        synchronized(nativeLock) {
            startInProgress = false
            nativeStarted = true
            nativeFinishedDuringStart = nativeTerminalReceived
            cancelStartedStream =
                !nativeFinishedDuringStart &&
                (cancelAfterStart || completed.get()) &&
                nativeCancelRequested.compareAndSet(false, true)
        }
        if (nativeFinishedDuringStart) lease.nativeDone()
        if (cancelStartedStream) requestProviderCancel(opened)

        job.start()
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
        settleLogical(cause, requestNativeCancel = true)
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
        if (stream !== this.stream) return
        try {
            if (!completed.get() && !remoteTerminalSeen.get()) {
                if (!headersReceived.get()) {
                    fail(protocolException("Cronet stream succeeded before response headers"))
                } else if (!responseEndReceived.get() && readChunks.trySend(ReadChunk(byteArrayOf(), true)).isFailure) {
                    fail(protocolException("Cronet succeeded with a response read still pending"))
                }
            }
        } finally {
            nativeTerminated()
        }
    }

    override fun onFailed(
        stream: DuplexStream,
        error: Throwable,
    ) {
        if (stream !== this.stream) return
        try {
            if (!completed.get() && !remoteTerminalSeen.get()) {
                fail(transportException("Cronet stream failed", error))
            }
        } finally {
            nativeTerminated()
        }
    }

    override fun onCanceled(stream: DuplexStream) {
        if (stream !== this.stream) return
        try {
            if (!completed.get() && !remoteTerminalSeen.get()) {
                fail(transportException("Cronet stream was canceled"))
            }
        } finally {
            nativeTerminated()
        }
    }

    override fun onCallbackFailure(
        stream: DuplexStream,
        error: Throwable,
    ) {
        if (stream === this.stream) fail(transportException("Cronet callback failed", error))
    }

    private suspend fun writeDirect(
        endOfStream: Boolean,
        failureMessage: String,
        bytes: () -> ByteArray,
    ) {
        try {
            write(bytes(), endOfStream)
        } catch (error: CancellationException) {
            if (!completed.get() && !remoteTerminalSeen.get()) fail(error)
            throw error
        } catch (error: Throwable) {
            val transportError = if (error is TrevRpcException) error else transportException(failureMessage, error)
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
                if (canceledByCaller) fail(CancellationException("request write was canceled"))
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

            val shouldDispatch =
                synchronized(writeLock) {
                    pendingWrite === pending && !completed.get() && !remoteTerminalSeen.get()
                }
            if (!shouldDispatch) return@suspendCancellableCoroutine

            val dispatchError =
                runCatching {
                    providerInvocations.invoke { stream.write(buffer, endOfStream) }
                    providerInvocations.invoke(stream::flush)
                }.exceptionOrNull()
            if (dispatchError != null) {
                val failedContinuation =
                    synchronized(writeLock) {
                        if (pendingWrite !== pending) {
                            null
                        } else {
                            pendingWrite = null
                            pending.continuation
                        }
                    }
                if (failedContinuation?.isActive == true) failedContinuation.resumeWithException(dispatchError)
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
            providerInvocations.invoke { stream.read(ByteBuffer.allocateDirect(options.readBufferSize)) }
        } catch (error: Throwable) {
            fail(transportException("Cronet response read failed", error))
        }
    }

    private fun finishRemoteStatus() {
        settleLogical(null, requestNativeCancel = true)
    }

    private fun beginRemoteStatus() {
        if (beginRemoteTerminalOnce()) {
            cancelPendingWrite(CancellationException("remote terminal status received"))
            cancelNative()
        }
    }

    private fun finishEndOfResponse() {
        settleLogical(null, requestNativeCancel = true)
    }

    private fun fail(error: Throwable) {
        settleLogical(error, requestNativeCancel = true)
    }

    private fun cancelFromChannelClose(error: Throwable) {
        settleLogical(error, requestNativeCancel = true)
    }

    private fun settleLogical(
        error: Throwable?,
        requestNativeCancel: Boolean,
    ) {
        if (!completeOnce(error)) return
        val completionError = error ?: CancellationException("RPC stream completed")
        writeReady.completeExceptionally(completionError)
        responseBodies.close(error)
        readChunks.close(error)
        cancelPendingWrite(completionError)
        if (requestNativeCancel) cancelNative()
        job.cancel(cancellationException("Cronet exchange completed", error))
        lease.logicalDone()
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
        val streamToCancel =
            synchronized(nativeLock) {
                if (!::stream.isInitialized || lease.isNativeDone || nativeTerminalReceived) return
                if (startInProgress || !nativeStarted) {
                    cancelAfterStart = true
                    return
                }
                if (nativeCancelRequested.compareAndSet(false, true)) stream else null
            }
        if (streamToCancel != null) requestProviderCancel(streamToCancel)
    }

    private fun requestProviderCancel(stream: DuplexStream) {
        runCatching {
            providerInvocations.dispatch { runCatching(stream::cancel) }
        }
    }

    private fun nativeTerminated() {
        val markNativeDone =
            synchronized(nativeLock) {
                nativeTerminalReceived = true
                !startInProgress
            }
        if (markNativeDone) lease.nativeDone()
    }

    private fun completeOnce(error: Throwable?): Boolean =
        synchronized(writeLock) {
            if (completed.get()) {
                false
            } else {
                completionCause = error
                completed.set(true)
                true
            }
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

internal class ProviderInvocationTracker {
    private val lock = Any()
    private var activeInvocations = 0
    private var sealed = false
    private var drainWaiter: CompletableDeferred<Unit>? = null

    fun <T> invoke(action: () -> T): T {
        beginInvocation()
        try {
            return action()
        } finally {
            finishInvocation()
        }
    }

    fun dispatch(action: () -> Unit) {
        beginInvocation()
        try {
            DetachedExternalCalls.dispatch {
                try {
                    action()
                } finally {
                    finishInvocation()
                }
            }
        } catch (error: Throwable) {
            finishInvocation()
            throw error
        }
    }

    suspend fun sealAndAwaitDrain() {
        val waiter =
            synchronized(lock) {
                sealed = true
                if (activeInvocations == 0) {
                    null
                } else {
                    drainWaiter ?: CompletableDeferred<Unit>().also { drainWaiter = it }
                }
            }
        waiter?.await()
    }

    private fun beginInvocation() {
        synchronized(lock) {
            check(!sealed) { "Cronet provider invocations are sealed" }
            activeInvocations++
        }
    }

    private fun finishInvocation() {
        val waiter =
            synchronized(lock) {
                check(activeInvocations > 0) { "Cronet provider invocation accounting underflow" }
                activeInvocations--
                if (sealed && activeInvocations == 0) drainWaiter else null
            }
        waiter?.complete(Unit)
    }
}

internal class ExchangeRegistry {
    private val lock = Any()
    private val leases = LinkedHashSet<ExchangeLease>()
    private var state = State.OPEN
    private var closingDrain: CompletableDeferred<Unit>? = null

    val activeCount: Int
        get() = synchronized(lock) { leases.size }

    val logicallyActiveCount: Int
        get() = synchronized(lock) { leases.count { !it.isLogicalDone } }

    fun admit(): ExchangeLease =
        synchronized(lock) {
            if (state != State.OPEN) throw TrevRpcException(Status.unavailable("RPC channel is closed"))
            ExchangeLease(this).also(leases::add)
        }

    fun beginClose(): ClosePlan =
        synchronized(lock) {
            check(state == State.OPEN) { "Cronet close may only begin once" }
            state = State.CLOSING
            val drained = CompletableDeferred<Unit>()
            if (leases.isEmpty()) drained.complete(Unit)
            closingDrain = drained
            ClosePlan(leases.toList(), drained)
        }

    fun finishClose() {
        synchronized(lock) {
            state = State.CLOSED
        }
    }

    fun exchangeStateChanged(lease: ExchangeLease) {
        val drained =
            synchronized(lock) {
                if (!lease.isSettled || !leases.remove(lease)) return
                if (state == State.CLOSING && leases.isEmpty()) closingDrain else null
            }
        drained?.complete(Unit)
    }

    private enum class State {
        OPEN,
        CLOSING,
        CLOSED,
    }
}

internal class ExchangeLease(
    private val registry: ExchangeRegistry,
) {
    private val logical = AtomicBoolean(false)
    private val native = AtomicBoolean(false)
    private val cancelLock = Any()
    private var cancelAction: ((Throwable) -> Unit)? = null
    private var requestedCancel: Throwable? = null
    private var cancelDelivered = false

    val isNativeDone: Boolean
        get() = native.get()

    val isLogicalDone: Boolean
        get() = logical.get()

    val isSettled: Boolean
        get() = logical.get() && native.get()

    fun attach(action: (Throwable) -> Unit) {
        val cancellation =
            synchronized(cancelLock) {
                check(cancelAction == null) { "exchange cancellation was already attached" }
                cancelAction = action
                takePendingCancellation()
            }
        if (cancellation != null) action(cancellation)
    }

    fun requestCancel(error: Throwable) {
        val action =
            synchronized(cancelLock) {
                if (requestedCancel == null) requestedCancel = error
                val cancellation = takePendingCancellation()
                val cancel = cancelAction
                if (cancellation == null || cancel == null) null else cancel to cancellation
            }
        action?.let { (cancel, cancellation) -> cancel(cancellation) }
    }

    fun logicalDone() {
        if (logical.compareAndSet(false, true)) registry.exchangeStateChanged(this)
    }

    fun nativeDone() {
        if (native.compareAndSet(false, true)) registry.exchangeStateChanged(this)
    }

    private fun takePendingCancellation(): Throwable? {
        val cancellation = requestedCancel
        if (cancelDelivered || cancellation == null || cancelAction == null) return null
        cancelDelivered = true
        return cancellation
    }
}

internal data class ClosePlan(
    val leases: List<ExchangeLease>,
    val drained: CompletableDeferred<Unit>,
)

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

private fun closeTimeoutException(
    timeout: Duration,
    cause: Throwable,
): TrevRpcException =
    TrevRpcException(
        Status.unavailable(
            "Cronet channel close timed out after $timeout; borrowed Cronet engine and executor are not proven quiescent",
        ),
        cause,
    )

private fun cancellationException(
    message: String,
    cause: Throwable?,
): CancellationException = CancellationException(message).also { if (cause != null) it.initCause(cause) }
