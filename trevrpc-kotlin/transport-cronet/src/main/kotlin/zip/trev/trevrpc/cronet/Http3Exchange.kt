package zip.trev.trevrpc.cronet

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
import zip.trev.trevrpc.Compression
import zip.trev.trevrpc.ConnectionMetadata
import zip.trev.trevrpc.ConnectionMetadataSource
import zip.trev.trevrpc.DEFAULT_MAX_FRAME_SIZE
import zip.trev.trevrpc.DEFAULT_STREAM_COMPRESSION_MEMORY
import zip.trev.trevrpc.FrameDecoder
import zip.trev.trevrpc.Metadata
import zip.trev.trevrpc.ResponseFrame
import zip.trev.trevrpc.RpcChannel
import zip.trev.trevrpc.RpcClientStream
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.StreamCompressionBudget
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.WireCodec
import zip.trev.trevrpc.encodeMessageFrame
import zip.trev.trevrpc.encodeRequestFrames
import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * The media type that names TrevRPC wire version 1. Frames carry no version, so a server that
 * does not serve this media type rejects the request with `415` before any RPC runs.
 */
internal const val TREV_RPC_CONTENT_TYPE = "application/trevrpc"

/**
 * Cronet transport options. [compression] enables compressed messages on the channel's calls, and
 * [streamCompressionMemory] bounds the memory stream compression holds on the channel's calls (64
 * MiB by default; 0 turns stream compression off). [connectionMetadata] is request metadata sent
 * with every call of the channel, such as a bearer token, which the server merges into each call's
 * metadata, the call's own entries winning; [RpcChannel.setConnectionMetadata] replaces it. Cronet
 * does not say which calls share a connection, so every call carries the whole set.
 * [closeTimeout] strictly bounds TrevRPC's shutdown wait. Provider
 * cancellation and callback-executor submission run on detached daemon workers, and close never
 * waits for a provider call while holding a transport monitor. A non-cooperative borrowed
 * implementation therefore cannot hold the closing coroutine past this bound.
 */
data class CronetTransportOptions(
    val readBufferSize: Int = 64 * 1024,
    val responseChannelCapacity: Int = 8,
    val maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE,
    val closeTimeout: Duration = 5.seconds,
    val compression: Compression? = null,
    val streamCompressionMemory: Long = DEFAULT_STREAM_COMPRESSION_MEMORY,
    val connectionMetadata: Metadata = Metadata.EMPTY,
) {
    init {
        require(streamCompressionMemory >= 0) { "streamCompressionMemory must be non-negative" }
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

internal class Http3RpcTransport(
    private val factory: DuplexStreamFactory,
    private val options: CronetTransportOptions = CronetTransportOptions(),
    coroutineContext: CoroutineContext = Dispatchers.IO,
    private val callbackDrain: CallbackDrain = ImmediateCallbackDrain,
    shutdownContext: CoroutineContext = DetachedExternalCalls,
) : RpcChannel {
    private val transportJob = SupervisorJob()
    private val shutdownJob = SupervisorJob()
    private val shutdownScope = CoroutineScope(shutdownJob + shutdownContext.minusKey(Job))
    private val exchangeContext = coroutineContext.minusKey(Job)
    private val exchanges = ExchangeRegistry()
    private val providerInvocations = ProviderInvocationTracker()
    private val closeLock = Any()
    private var closeResult: CompletableDeferred<Result<Unit>>? = null

    override val compression: Compression?
        get() = options.compression

    override val streamCompressionBudget = StreamCompressionBudget(options.streamCompressionMemory)

    private val connectionMetadataSource = ConnectionMetadataSource(options.connectionMetadata)

    override val connectionMetadata: ConnectionMetadata?
        get() = connectionMetadataSource.current()

    override fun setConnectionMetadata(metadata: Metadata) = connectionMetadataSource.set(metadata)

    internal val activeExchangeCount: Int
        get() = exchanges.activeCount

    internal val logicallyActiveExchangeCount: Int
        get() = exchanges.logicallyActiveCount

    override suspend fun openStream(
        request: RpcRequest,
        firstMessage: ByteArray?,
        finishSend: Boolean,
    ): RpcClientStream = StreamingTransportStream(open(request, firstMessage, finishSend))

    override suspend fun close() {
        beginClose().await().getOrThrow()
    }

    private suspend fun open(
        request: RpcRequest,
        firstMessage: ByteArray?,
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
            )
        exchange.start(request, firstMessage, endOfStream)
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
    override suspend fun send(
        body: ByteArray,
        frameType: Long,
    ) = exchange.send(body, frameType)

    override suspend fun finishSend() = exchange.finishSend()

    override suspend fun receive(): ResponseFrame? = exchange.receiveFrame()

    override suspend fun close(cause: Throwable?) = exchange.close(cause)
}

/**
 * One RPC over one Cronet bidirectional stream. Two state machines describe it, kept apart because
 * Cronet's native stream outlives the logical call:
 *
 * - [CallStateMachine] is the logical call: [CallState.Open], optionally [CallState.RemoteTerminal]
 *   once the server's STATUS is decoded, then [CallState.Settled]. Every transition takes the write
 *   in flight in the same critical section, so a Cronet write completion can never observe a write
 *   cleared by a transition that has not yet moved the call out of [CallState.Open].
 * - [NativeStreamLifecycle] is the native stream: start, the single provider cancel and Cronet's
 *   terminal callback, which decides when the exchange's [ExchangeLease] is released.
 *
 * Cronet's callbacks read the call state without a lock and ignore everything once it is not open.
 */
internal class Http3Exchange(
    private val factory: DuplexStreamFactory,
    private val options: CronetTransportOptions,
    parentJob: Job,
    coroutineContext: CoroutineContext,
    private val lease: ExchangeLease,
    private val providerInvocations: ProviderInvocationTracker,
) : DuplexCallback {
    private val job =
        CoroutineScope(parentJob + coroutineContext).launch(start = CoroutineStart.LAZY) {
            readResponses()
        }
    private val call = CallStateMachine()
    private val native = NativeStreamLifecycle()
    private val headersReceived = AtomicBoolean(false)
    private val responseEndReceived = AtomicBoolean(false)
    private val responseFrames = Channel<ResponseFrame>(options.responseChannelCapacity)
    private val readChunks = Channel<ReadChunk>(1)
    private val writeReady = CompletableDeferred<Unit>()
    private val sendLock = Mutex()
    private var sendFinished = false

    /**
     * Assigned before [NativeStreamLifecycle.beginStart], so every provider call and callback that
     * follows sees it: [NativeStreamLifecycle] only lets a cancel through after `start()` returned.
     */
    private lateinit var stream: DuplexStream

    init {
        lease.attach(::cancelFromChannelClose)
    }

    suspend fun start(
        request: RpcRequest,
        firstMessage: ByteArray?,
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

        stream = opened
        if (!native.beginStart()) {
            // The call was settled and its cancel requested before the stream existed.
            lease.nativeDone()
            throw (call.state as? CallState.Settled)?.cause ?: TrevRpcException(Status.unavailable("RPC channel is closed"))
        }

        try {
            providerInvocations.invoke(opened::start)
        } catch (error: Throwable) {
            native.startFailed()
            val transportError = transportException("Cronet stream start failed", error)
            settleLogical(transportError, requestNativeCancel = false)
            lease.nativeDone()
            throw transportError
        }

        when (native.startReturned()) {
            NativeStreamLifecycle.AfterStart.FINISHED -> lease.nativeDone()
            NativeStreamLifecycle.AfterStart.CANCEL -> requestProviderCancel(opened)
            NativeStreamLifecycle.AfterStart.RUNNING -> Unit
        }

        job.start()
        sendFinished = endOfStream
        writeDirect(
            endOfStream,
            "initial request write failed",
        ) { encodeRequestFrames(request, firstMessage, options.maxFrameSize) }
    }

    suspend fun send(
        body: ByteArray,
        frameType: Long,
    ) {
        sendLock.withLock {
            checkSendOpen()
            writeDirect(false, "request write failed") {
                encodeMessageFrame(frameType, body, options.maxFrameSize)
            }
        }
    }

    suspend fun finishSend() {
        sendLock.withLock {
            if (sendFinished || call.state.remoteStatus != null) {
                sendFinished = true
                return
            }
            checkSendOpen()
            sendFinished = true
            writeDirect(true, "request finish failed") { byteArrayOf() }
        }
    }

    suspend fun receiveFrame(): ResponseFrame? {
        val result = responseFrames.receiveCatching()
        return result.getOrNull() ?: result.exceptionOrNull()?.let { throw it }
    }

    suspend fun close(cause: Throwable? = null) {
        settleLogical(cause, requestNativeCancel = true)
    }

    override fun onReady(stream: DuplexStream) {
        if (stream !== this.stream || call.state !is CallState.Open) return
        writeReady.complete(Unit)
    }

    override fun onHeaders(
        stream: DuplexStream,
        headers: ResponseHeaders,
    ) {
        if (stream !== this.stream || call.state !is CallState.Open) return
        if (!headersReceived.compareAndSet(false, true)) {
            failIfOpen(protocolException("received response headers more than once"))
            return
        }
        val error = validateHeaders(headers)
        if (error != null) {
            failIfOpen(error)
            return
        }
        requestRead()
    }

    override fun onRead(
        stream: DuplexStream,
        buffer: ByteBuffer,
        endOfStream: Boolean,
    ) {
        if (stream !== this.stream || call.state !is CallState.Open) return
        val bytes = ByteArray(buffer.position())
        buffer.flip()
        buffer.get(bytes)
        if (endOfStream) responseEndReceived.set(true)
        if (readChunks.trySend(ReadChunk(bytes, endOfStream)).isFailure) {
            failIfOpen(protocolException("Cronet delivered overlapping response reads"))
        }
    }

    override fun onWrite(
        stream: DuplexStream,
        buffer: ByteBuffer,
        endOfStream: Boolean,
    ) {
        if (stream !== this.stream) return
        val completed = call.takeCompletedWrite(buffer, endOfStream)
        if (completed != null) {
            completed.continuation.resume(Unit)
        } else {
            // A transition that took the write also moved the call out of Open, so this is a no-op
            // for a write that was canceled, failed or cut short, and an error only for a write
            // Cronet reports that was never issued.
            failIfOpen(protocolException("Cronet completed an unexpected request write"))
        }
    }

    override fun onSucceeded(stream: DuplexStream) {
        if (stream !== this.stream) return
        try {
            if (call.state is CallState.Open) {
                if (!headersReceived.get()) {
                    failIfOpen(protocolException("Cronet stream succeeded before response headers"))
                } else if (!responseEndReceived.get() && readChunks.trySend(ReadChunk(byteArrayOf(), true)).isFailure) {
                    failIfOpen(protocolException("Cronet succeeded with a response read still pending"))
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
            // Cronet's message names the network error, such as net::ERR_CONNECTION_RESET.
            failIfOpen(transportException("Cronet stream failed: ${error.message}", error))
        } finally {
            nativeTerminated()
        }
    }

    override fun onCanceled(stream: DuplexStream) {
        if (stream !== this.stream) return
        try {
            failIfOpen(transportException("Cronet stream was canceled"))
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
            if (admitWrite(endOfStream)) write(bytes(), endOfStream, failureMessage)
        } catch (error: CancellationException) {
            failIfOpen(error)
            throw error
        } catch (error: Throwable) {
            val transportError = if (error is TrevRpcException) error else transportException(failureMessage, error)
            failIfOpen(transportError)
            throw transportError
        }
    }

    private suspend fun write(
        bytes: ByteArray,
        endOfStream: Boolean,
        failureMessage: String,
    ) {
        writeReady.await()
        if (!admitWrite(endOfStream)) return
        val buffer = ByteBuffer.allocateDirect(bytes.size)
        buffer.put(bytes)
        buffer.flip()
        suspendCancellableCoroutine { continuation ->
            val pending = PendingWrite(buffer, endOfStream, continuation)
            // Taking the write and settling the call are one transition, so a write completion that
            // lands next finds a settled call instead of a missing write on an open one. A write cut
            // short after the server's STATUS was already taken by that transition: nothing to settle.
            continuation.invokeOnCancellation {
                finishSettle(call.settleWrite(pending, CancellationException("request write was canceled")), requestNativeCancel = true)
            }
            val refused = call.installWrite(pending)
            if (refused != null) {
                if (continuation.isActive && refused is CallState.Ended) {
                    if (endOfStream && refused.remoteStatus != null) {
                        continuation.resume(Unit)
                    } else {
                        continuation.resumeWithException(refused.sendError())
                    }
                }
                return@suspendCancellableCoroutine
            }

            if (!call.isPending(pending)) return@suspendCancellableCoroutine

            val dispatchError =
                runCatching {
                    providerInvocations.invoke { stream.write(buffer, endOfStream) }
                    providerInvocations.invoke(stream::flush)
                }.exceptionOrNull()
            if (dispatchError != null) {
                // A write completion that lands after a failed dispatch finds the call settled.
                finishSettle(
                    call.settleWrite(pending, transportException(failureMessage, dispatchError)),
                    requestNativeCancel = true,
                )
            }
        }
    }

    private suspend fun readResponses() {
        val decoder = FrameDecoder(options.maxFrameSize)
        try {
            for (chunk in readChunks) {
                var statusSeen = false
                for (raw in decoder.feed(chunk.bytes)) {
                    // Each frame is decoded exactly once, here; the stream receives the result.
                    val frame = WireCodec.decodeResponseFrame(raw, options.maxFrameSize)
                    // Frames after STATUS in the same read are still forwarded so the reader can
                    // reject a known frame after the terminal status.
                    if (frame is ResponseFrame.Status && !statusSeen) {
                        statusSeen = true
                        beginRemoteStatus(frame.status)
                    }
                    responseFrames.send(frame)
                }
                if (statusSeen) {
                    finishRemoteStatus()
                    return
                }
                if (chunk.endOfStream) {
                    decoder.finish()
                    finishEndOfResponse()
                    return
                }
                requestRead()
            }
        } catch (error: CancellationException) {
            if (call.state !is CallState.Settled) fail(transportException("response processing was canceled", error))
        } catch (error: Throwable) {
            fail(error)
        }
    }

    private fun requestRead() {
        if (call.state is CallState.Settled) return
        try {
            providerInvocations.invoke { stream.read(ByteBuffer.allocateDirect(options.readBufferSize)) }
        } catch (error: Throwable) {
            fail(transportException("Cronet response read failed", error))
        }
    }

    private fun finishRemoteStatus() {
        settleLogical(null, requestNativeCancel = true)
    }

    private fun beginRemoteStatus(status: Status) {
        val transition = call.beginRemoteTerminal(status) ?: return
        transition.pendingWrite?.let { pending ->
            if (pending.endOfStream) {
                pending.continuation.resume(Unit)
            } else {
                pending.continuation.resumeWithException(transition.state.sendError())
            }
        }
        cancelNative()
    }

    private fun finishEndOfResponse() {
        settleLogical(null, requestNativeCancel = true)
    }

    private fun fail(error: Throwable) {
        settleLogical(error, requestNativeCancel = true)
    }

    private fun failIfOpen(error: Throwable) {
        finishSettle(call.settleIfOpen(error), requestNativeCancel = true)
    }

    private fun cancelFromChannelClose(error: Throwable) {
        settleLogical(error, requestNativeCancel = true)
    }

    private fun settleLogical(
        error: Throwable?,
        requestNativeCancel: Boolean,
    ) {
        finishSettle(call.settle(error), requestNativeCancel)
    }

    /** Runs the side effects of a transition into [CallState.Settled]; a `null` transition did not happen. */
    private fun finishSettle(
        transition: Transition<CallState.Settled>?,
        requestNativeCancel: Boolean,
    ) {
        if (transition == null) return
        val error = transition.state.cause
        val completionError = error ?: transition.state.sendError()
        writeReady.completeExceptionally(completionError)
        responseFrames.close(error)
        readChunks.close(error)
        transition.pendingWrite?.continuation?.resumeWithException(completionError)
        if (requestNativeCancel) cancelNative()
        job.cancel(cancellationException("Cronet exchange completed", error))
        lease.logicalDone()
    }

    private fun cancelNative() {
        if (native.requestCancel()) requestProviderCancel(stream)
    }

    private fun requestProviderCancel(stream: DuplexStream) {
        runCatching {
            providerInvocations.dispatch { runCatching(stream::cancel) }
        }
    }

    private fun nativeTerminated() {
        if (native.terminated()) lease.nativeDone()
    }

    /**
     * Whether a write may go out. `false` means it is moot: a half-close after the server's STATUS.
     * Throws why the call can no longer be written to.
     */
    private fun admitWrite(endOfStream: Boolean): Boolean {
        val current = call.state
        if (current !is CallState.Ended) return true
        if (endOfStream && current.remoteStatus != null) return false
        throw current.sendError()
    }

    private fun checkSendOpen() {
        if (sendFinished) throw TrevRpcException(Status.cancelled("request stream is closed"))
        val current = call.state
        if (current is CallState.Ended) throw current.sendError()
    }

    private data class ReadChunk(
        val bytes: ByteArray,
        val endOfStream: Boolean,
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
