package zip.trev.trevrpc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.coroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeMark
import kotlin.time.TimeSource

data class ServerOptions(
    val maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE,
    val maxConcurrentConnections: Int? = DEFAULT_MAX_CONCURRENT_CONNECTIONS,
    val maxConcurrentStreamsPerConnection: Int? = DEFAULT_MAX_CONCURRENT_STREAMS_PER_CONNECTION,
    val maxConcurrentRequests: Int? = DEFAULT_MAX_CONCURRENT_REQUESTS,
    val gracefulShutdownTimeout: Duration = 30.seconds,
    val forceShutdownTimeout: Duration = 10.seconds,
    val initialRequestTimeout: Duration? = 10.seconds,
    val maxStreamMessages: Int? = DEFAULT_MAX_STREAM_MESSAGES,
    val maxStreamBodySize: Long? = DEFAULT_MAX_STREAM_BODY_SIZE,
    val streamIdleTimeout: Duration? = 30.seconds,
) {
    init {
        require(maxFrameSize >= 0) { "maxFrameSize must be non-negative" }
        require(maxConcurrentConnections == null || maxConcurrentConnections > 0)
        require(maxConcurrentStreamsPerConnection == null || maxConcurrentStreamsPerConnection > 0)
        require(maxConcurrentRequests == null || maxConcurrentRequests > 0)
        require(gracefulShutdownTimeout.isFinite() && gracefulShutdownTimeout.isPositive())
        require(forceShutdownTimeout.isFinite() && forceShutdownTimeout.isPositive())
        require(initialRequestTimeout == null || initialRequestTimeout.isPositive())
        require(maxStreamMessages == null || maxStreamMessages >= 0)
        require(maxStreamBodySize == null || maxStreamBodySize >= 0)
        require(streamIdleTimeout == null || streamIdleTimeout.isPositive())
    }
}

class RequestContext internal constructor(
    val service: String,
    val method: String,
    val kind: RpcKind,
    val metadata: Metadata,
    private val deadline: TimeMark?,
    private val job: Job?,
) {
    val deadlineExpired: Boolean
        get() = deadline?.hasPassedNow() == true

    val cancelled: Boolean
        get() = deadlineExpired || job?.isCancelled == true

    fun timeRemaining(): Duration? = deadline?.let { -it.elapsedNow() }?.coerceAtLeast(Duration.ZERO)
}

fun interface Authorizer {
    suspend fun authorize(request: RpcRequest)
}

interface Metrics {
    fun rpcStarted(event: RpcStarted) {}

    fun rpcFinished(event: RpcFinished) {}
}

object NoopMetrics : Metrics

data class RpcStarted(
    val service: String,
    val method: String,
    val requestBodyLength: Int,
)

data class RpcFinished(
    val service: String,
    val method: String,
    val requestBodyLength: Int,
    val responseBodyLength: Long,
    val code: Code,
    val elapsed: Duration,
)

fun interface UnaryHandler {
    suspend fun handle(
        context: RequestContext,
        body: ByteArray,
    ): ResponseEnvelope<ByteArray>
}

fun interface ServerStreamingHandler {
    suspend fun handle(
        context: RequestContext,
        body: ByteArray,
    ): ResponseEnvelope<Flow<ByteArray>>
}

fun interface ClientStreamingHandler {
    suspend fun handle(
        context: RequestContext,
        requests: Flow<ByteArray>,
    ): ResponseEnvelope<ByteArray>
}

fun interface BidirectionalStreamingHandler {
    suspend fun handle(
        context: RequestContext,
        requests: Flow<ByteArray>,
    ): ResponseEnvelope<Flow<ByteArray>>
}

fun interface ServerResponseSink {
    suspend fun send(frame: RpcStreamFrame)
}

interface BatchingServerResponseSink : ServerResponseSink {
    suspend fun sendBatch(frames: List<RpcStreamFrame>)
}

class Server(
    val options: ServerOptions = ServerOptions(),
    private val authorizer: Authorizer? = null,
    private val metrics: Metrics = NoopMetrics,
) {
    private val routes = ConcurrentHashMap<RouteKey, Route>()
    private val requestPermits = options.maxConcurrentRequests?.let(::Semaphore)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val shutdownScope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val accepting = AtomicBoolean(true)
    private val shutdownStarted = AtomicBoolean(false)
    private val shutdownComplete = CompletableDeferred<Result<Unit>>()
    private val stateLock = Any()
    private val activeJobs = linkedMapOf<Job, Int>()
    private var activeWithoutJob = 0
    private var drained = CompletableDeferred(Unit)

    val routeCount: Int
        get() = routes.size

    fun routeUnary(
        service: String,
        method: String,
        handler: UnaryHandler,
    ) {
        routes[RouteKey(service, method)] = Route.Unary(handler)
    }

    fun routeServerStreaming(
        service: String,
        method: String,
        handler: ServerStreamingHandler,
    ) {
        routes[RouteKey(service, method)] = Route.ServerStreaming(handler)
    }

    fun routeClientStreaming(
        service: String,
        method: String,
        handler: ClientStreamingHandler,
    ) {
        routes[RouteKey(service, method)] = Route.ClientStreaming(handler)
    }

    fun routeBidirectionalStreaming(
        service: String,
        method: String,
        handler: BidirectionalStreamingHandler,
    ) {
        routes[RouteKey(service, method)] = Route.BidirectionalStreaming(handler)
    }

    suspend fun handleUnary(request: RpcRequest): RpcResponse {
        val lifecycle = Lifecycle(metrics, request)
        val admission = admit(coroutineContext[Job])
        if (admission == null) {
            val status = rejectionStatus()
            lifecycle.finish(status.code)
            return RpcResponse.fromStatus(status)
        }
        try {
            val deadline = prepare(request)
            if (request.kind != RpcKind.UNARY) {
                throw TrevRpcException(Status.invalidArgument("unary endpoint received ${request.kind} request"))
            }
            val route = routes[RouteKey(request.service, request.method)]
            if (route !is Route.Unary) throw unknownOrMismatched(request, route)
            val context = requestContext(request, deadline)
            val response =
                runWithDeadline(deadline) {
                    invokeHandler { route.handler.handle(context, request.body.copyOf()) }
                }
            checkResponseBody(response.message.size)
            lifecycle.finish(Code.OK, response.message.size.toLong())
            return RpcResponse.ok(response.message, response.metadata)
        } catch (error: CancellationException) {
            lifecycle.finish(Code.CANCELLED)
            throw error
        } catch (error: Throwable) {
            val status = error.toStatus()
            lifecycle.finish(status.code)
            return RpcResponse.fromStatus(status)
        } finally {
            admission.close()
        }
    }

    suspend fun handleStreaming(
        request: RpcRequest,
        requestBody: Flow<ByteArray>,
    ): RpcTransportStream {
        val channel = Channel<RpcStreamFrame>(1)
        val streamJob =
            scope.launch {
                try {
                    handleStreaming(request, requestBody, ServerResponseSink(channel::send))
                } finally {
                    channel.close()
                }
            }
        return ChannelTransportStream(channel, streamJob)
    }

    suspend fun handleStreaming(
        request: RpcRequest,
        requestBody: Flow<ByteArray>,
        sink: ServerResponseSink,
    ) {
        val lifecycle = Lifecycle(metrics, request)
        val admission = admit(currentCoroutineContext()[Job])
        if (admission == null) {
            val status = rejectionStatus()
            lifecycle.finish(status.code)
            sendFrame(sink, RpcStreamFrame.status(status))
            return
        }
        try {
            val deadline = prepare(request)
            val kind =
                request.kind ?: throw TrevRpcException(
                    Status.invalidArgument("unsupported TrevRPC RPC kind ${request.kindValue}"),
                )
            if (kind == RpcKind.UNARY) {
                throw TrevRpcException(Status.invalidArgument("streaming endpoint received unary request"))
            }
            val route = routes[RouteKey(request.service, request.method)]
            val context = requestContext(request, deadline)
            val limitedRequests = limitedFlow(requestBody, "request", deadline)
            runWithDeadline(deadline) {
                when {
                    kind == RpcKind.SERVER_STREAMING && route is Route.ServerStreaming -> {
                        val response = invokeHandler { route.handler.handle(context, request.body.copyOf()) }
                        val batching = response.message.responseBatching()?.takeIf { sink is BatchingServerResponseSink }
                        sendResponses(
                            sink,
                            if (batching == null) limitedFlow(response.message, "response", deadline) else response.message,
                            lifecycle,
                            batching,
                        )
                        sendFrame(sink, RpcStreamFrame.status(Status.ok(response.metadata)))
                        lifecycle.finish(Code.OK)
                    }

                    kind == RpcKind.CLIENT_STREAMING && route is Route.ClientStreaming -> {
                        val response = invokeHandler { route.handler.handle(context, limitedRequests) }
                        checkResponseBody(response.message.size)
                        sendFrame(sink, RpcStreamFrame.message(response.message))
                        lifecycle.addResponseBytes(response.message.size)
                        sendFrame(sink, RpcStreamFrame.status(Status.ok(response.metadata)))
                        lifecycle.finish(Code.OK)
                    }

                    kind == RpcKind.BIDIRECTIONAL_STREAMING && route is Route.BidirectionalStreaming -> {
                        val response = invokeHandler { route.handler.handle(context, limitedRequests) }
                        sendResponses(sink, limitedFlow(response.message, "response", deadline), lifecycle)
                        sendFrame(sink, RpcStreamFrame.status(Status.ok(response.metadata)))
                        lifecycle.finish(Code.OK)
                    }

                    else -> {
                        throw unknownOrMismatched(request, route)
                    }
                }
            }
        } catch (error: ResponseSinkException) {
            lifecycle.finish(Code.CANCELLED)
            throw error.cause ?: error
        } catch (error: TimeoutCancellationException) {
            val status = Status.deadlineExceeded("RPC deadline exceeded")
            lifecycle.finish(status.code)
            sendTerminalStatus(sink, status)
        } catch (error: CancellationException) {
            lifecycle.finish(Code.CANCELLED)
            sendTerminalStatus(sink, Status.cancelled("RPC cancelled"))
            throw error
        } catch (error: Throwable) {
            val status = error.toStatus()
            lifecycle.finish(status.code)
            sendTerminalStatus(sink, status)
        } finally {
            admission.close()
        }
    }

    suspend fun shutdown(
        gracefulTimeout: Duration = options.gracefulShutdownTimeout,
        forceTimeout: Duration = options.forceShutdownTimeout,
    ) {
        require(gracefulTimeout.isFinite() && gracefulTimeout.isPositive())
        require(forceTimeout.isFinite() && forceTimeout.isPositive())
        if (shutdownStarted.compareAndSet(false, true)) {
            accepting.set(false)
            shutdownScope.launch {
                val result =
                    runCatching {
                        withContext(NonCancellable) {
                            val waitForDrain = synchronized(stateLock) { drained }
                            val graceful = withTimeoutOrNull(gracefulTimeout) { waitForDrain.await() } != null
                            if (!graceful) {
                                val jobs = synchronized(stateLock) { activeJobs.keys.toList() }
                                jobs.forEach {
                                    it.cancel(CancellationException("server graceful shutdown timed out"))
                                }
                                val forced = withTimeoutOrNull(forceTimeout) { waitForDrain.await() } != null
                                if (!forced) {
                                    throw IllegalStateException(
                                        "server shutdown timed out waiting for cancelled requests",
                                    )
                                }
                            }
                        }
                    }
                scope.cancel()
                shutdownComplete.complete(result)
                shutdownScope.cancel()
            }
        }
        shutdownComplete.await().getOrThrow()
    }

    private suspend fun prepare(request: RpcRequest): TimeMark? {
        request.validateProtocol()
        if (request.body.size > options.maxFrameSize) {
            throw TrevRpcException(Status.resourceExhausted("request body exceeded maximum frame size"))
        }
        val deadline = requestDeadline(request.timeoutNanos)
        runWithDeadline(deadline) { authorizer?.authorize(request) }
        return deadline
    }

    private suspend fun requestContext(
        request: RpcRequest,
        deadline: TimeMark?,
    ): RequestContext =
        RequestContext(
            request.service,
            request.method,
            checkNotNull(request.kind),
            request.metadata,
            deadline,
            currentCoroutineContext()[Job],
        )

    private fun admit(job: Job?): Admission? =
        synchronized(stateLock) {
            if (!accepting.get()) return@synchronized null
            if (requestPermits?.tryAcquire() == false) return@synchronized null
            if (activeJobs.isEmpty() && activeWithoutJob == 0) drained = CompletableDeferred()
            if (job == null) {
                activeWithoutJob++
            } else {
                activeJobs[job] = (activeJobs[job] ?: 0) + 1
            }
            Admission(job)
        }

    private fun rejectionStatus(): Status =
        if (accepting.get()) {
            Status.resourceExhausted("server request admission limit reached")
        } else {
            Status.unavailable("server is shutting down")
        }

    private fun checkResponseBody(size: Int) {
        if (size > options.maxFrameSize) {
            throw TrevRpcException(Status.resourceExhausted("response body exceeded maximum frame size"))
        }
    }

    private suspend fun sendResponses(
        sink: ServerResponseSink,
        responses: Flow<ByteArray>,
        lifecycle: Lifecycle,
        batching: ResponseBatching? = null,
    ) {
        if (batching != null) {
            sendBatchedResponses(sink as BatchingServerResponseSink, responses, lifecycle, batching)
            return
        }
        responses.collect { body ->
            checkResponseBody(body.size)
            sendFrame(sink, RpcStreamFrame.message(body))
            lifecycle.addResponseBytes(body.size)
        }
    }

    private suspend fun sendBatchedResponses(
        sink: BatchingServerResponseSink,
        responses: Flow<ByteArray>,
        lifecycle: Lifecycle,
        batching: ResponseBatching,
    ) {
        var frames = ArrayList<RpcStreamFrame>(batching.maxMessages)
        var bodyBytes = 0
        var streamMessages = 0
        var streamBytes = 0L

        suspend fun flush() {
            if (frames.isEmpty()) return
            val batch = frames
            val acceptedBodyBytes = bodyBytes
            frames = ArrayList(batching.maxMessages)
            bodyBytes = 0
            sendFrames(sink, batch)
            lifecycle.addResponseBytes(acceptedBodyBytes)
        }

        responses.collect { body ->
            checkResponseBody(body.size)
            val maxMessages = options.maxStreamMessages
            if (maxMessages != null && streamMessages >= maxMessages) {
                flush()
                throw TrevRpcException(
                    Status.resourceExhausted("response stream exceeded maximum of $maxMessages messages"),
                )
            }
            val maxBody = options.maxStreamBodySize
            if (maxBody != null && body.size.toLong() > maxBody - streamBytes) {
                flush()
                throw TrevRpcException(
                    Status.resourceExhausted("response stream exceeded maximum body size of $maxBody bytes"),
                )
            }
            if (
                frames.isNotEmpty() &&
                (frames.size == batching.maxMessages || body.size > batching.maxBytes - bodyBytes)
            ) {
                flush()
            }
            frames += RpcStreamFrame.message(body)
            bodyBytes = Math.addExact(bodyBytes, body.size)
            streamMessages++
            streamBytes += body.size
        }
        flush()
    }

    private suspend fun sendTerminalStatus(
        sink: ServerResponseSink,
        status: Status,
    ) {
        try {
            sendFrame(sink, RpcStreamFrame.status(status))
        } catch (_: CancellationException) {
            // The peer closed the stream, so there is nowhere to deliver the terminal status.
        }
    }

    private suspend fun sendFrame(
        sink: ServerResponseSink,
        frame: RpcStreamFrame,
    ) {
        try {
            sink.send(frame)
        } catch (error: CancellationException) {
            throw error
        } catch (error: Throwable) {
            throw ResponseSinkException(error)
        }
    }

    private suspend fun sendFrames(
        sink: BatchingServerResponseSink,
        frames: List<RpcStreamFrame>,
    ) {
        try {
            sink.sendBatch(frames)
        } catch (error: CancellationException) {
            throw error
        } catch (error: Throwable) {
            throw ResponseSinkException(error)
        }
    }

    private fun limitedFlow(
        source: Flow<ByteArray>,
        direction: String,
        deadline: TimeMark?,
    ): Flow<ByteArray> =
        flow {
            var messages = 0
            var bytes = 0L
            val input = Channel<ByteArray>(1)
            kotlinx.coroutines.coroutineScope {
                val producer =
                    launch {
                        try {
                            source.collect { input.send(it.copyOf()) }
                            input.close()
                        } catch (error: Throwable) {
                            input.close(error)
                        }
                    }
                try {
                    while (true) {
                        val item = receiveWithLimits(input, direction, deadline) ?: break
                        val maxMessages = options.maxStreamMessages
                        if (maxMessages != null && messages >= maxMessages) {
                            throw TrevRpcException(
                                Status.resourceExhausted("$direction stream exceeded maximum of $maxMessages messages"),
                            )
                        }
                        val maxBody = options.maxStreamBodySize
                        if (maxBody != null && item.size.toLong() > maxBody - bytes) {
                            throw TrevRpcException(
                                Status.resourceExhausted(
                                    "$direction stream exceeded maximum body size of $maxBody bytes",
                                ),
                            )
                        }
                        messages++
                        bytes += item.size
                        emit(item)
                    }
                } finally {
                    producer.cancel()
                    input.cancel()
                }
            }
        }

    private suspend fun receiveWithLimits(
        input: Channel<ByteArray>,
        direction: String,
        deadline: TimeMark?,
    ): ByteArray? {
        val deadlineTimeout = deadline?.let { -it.elapsedNow() }
        if (deadlineTimeout != null && !deadlineTimeout.isPositive()) {
            throw TrevRpcException(Status.deadlineExceeded("RPC deadline exceeded"))
        }
        val idleTimeout = options.streamIdleTimeout
        val timeout = listOfNotNull(deadlineTimeout, idleTimeout).minOrNull()
        val result =
            if (timeout == null) {
                input.receiveCatching()
            } else {
                try {
                    withTimeout(timeout) { input.receiveCatching() }
                } catch (error: TimeoutCancellationException) {
                    if (deadlineTimeout != null && deadlineTimeout <= timeout) {
                        throw TrevRpcException(Status.deadlineExceeded("RPC deadline exceeded"), error)
                    }
                    throw TrevRpcException(Status.unavailable("$direction stream idle timeout"), error)
                }
            }
        result.exceptionOrNull()?.let { throw it }
        return result.getOrNull()
    }

    private inner class Admission(
        private val job: Job?,
    ) : AutoCloseable {
        private val closed = AtomicBoolean(false)

        override fun close() {
            if (!closed.compareAndSet(false, true)) return
            requestPermits?.release()
            synchronized(stateLock) {
                if (job == null) {
                    activeWithoutJob--
                } else {
                    val count = checkNotNull(activeJobs[job]) - 1
                    if (count == 0) activeJobs.remove(job) else activeJobs[job] = count
                }
                if (activeJobs.isEmpty() && activeWithoutJob == 0) drained.complete(Unit)
            }
        }
    }
}

private data class RouteKey(
    val service: String,
    val method: String,
)

private sealed interface Route {
    data class Unary(
        val handler: UnaryHandler,
    ) : Route

    data class ServerStreaming(
        val handler: ServerStreamingHandler,
    ) : Route

    data class ClientStreaming(
        val handler: ClientStreamingHandler,
    ) : Route

    data class BidirectionalStreaming(
        val handler: BidirectionalStreamingHandler,
    ) : Route
}

private class ResponseSinkException(
    cause: Throwable,
) : RuntimeException(cause)

private class ChannelTransportStream(
    private val channel: Channel<RpcStreamFrame>,
    private val job: Job,
) : RpcTransportStream {
    private val closed = AtomicBoolean(false)

    override suspend fun receive(): RpcStreamFrame? = channel.receiveCatching().getOrNull()

    override suspend fun close(cause: Throwable?) {
        if (!closed.compareAndSet(false, true)) return
        channel.cancel(CancellationException("RPC response stream closed", cause))
        job.cancel(CancellationException("RPC response stream closed", cause))
        job.join()
    }
}

private class Lifecycle(
    private val metrics: Metrics,
    private val request: RpcRequest,
) {
    private val started = TimeSource.Monotonic.markNow()
    private val finished = AtomicBoolean(false)
    private var responseBodyLength = 0L

    init {
        runCatching {
            metrics.rpcStarted(RpcStarted(request.service, request.method, request.body.size))
        }
    }

    fun addResponseBytes(size: Int) {
        responseBodyLength += size
    }

    fun finish(
        code: Code,
        responseLength: Long = responseBodyLength,
    ) {
        if (!finished.compareAndSet(false, true)) return
        runCatching {
            metrics.rpcFinished(
                RpcFinished(
                    request.service,
                    request.method,
                    request.body.size,
                    responseLength,
                    code,
                    started.elapsedNow(),
                ),
            )
        }
    }
}

private fun requestDeadline(timeoutNanos: ULong): TimeMark? {
    if (timeoutNanos == 0uL) return null
    if (timeoutNanos > Long.MAX_VALUE.toULong()) {
        throw TrevRpcException(Status.invalidArgument("RPC timeout is too large"))
    }
    return TimeSource.Monotonic.markNow() + timeoutNanos.toLong().nanoseconds
}

private suspend fun <T> runWithDeadline(
    deadline: TimeMark?,
    block: suspend () -> T,
): T {
    if (deadline == null) return block()
    val remaining = -deadline.elapsedNow()
    if (!remaining.isPositive()) throw TrevRpcException(Status.deadlineExceeded("RPC deadline exceeded"))
    return try {
        withTimeout(remaining) { block() }
    } catch (error: TimeoutCancellationException) {
        throw TrevRpcException(Status.deadlineExceeded("RPC deadline exceeded"), error)
    }
}

private suspend fun <T> invokeHandler(block: suspend () -> T): T =
    try {
        block()
    } catch (error: CancellationException) {
        throw error
    } catch (error: TrevRpcException) {
        throw error
    } catch (error: Throwable) {
        throw TrevRpcException(Status.internal("RPC handler failed"), error)
    }

private fun unknownOrMismatched(
    request: RpcRequest,
    route: Route?,
): TrevRpcException =
    if (route == null) {
        TrevRpcException(Status.unimplemented("unknown RPC method ${request.service}/${request.method}"))
    } else {
        TrevRpcException(
            Status.invalidArgument("RPC kind mismatch for ${request.service}/${request.method}"),
        )
    }
