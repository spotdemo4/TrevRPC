package zip.trev.trevrpc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeMark
import kotlin.time.TimeSource

class MessageCodec<T>(
    val encode: (T) -> ByteArray,
    val decode: (ByteArray) -> T,
) {
    companion object {
        val BYTE_ARRAY: MessageCodec<ByteArray> = MessageCodec(ByteArray::copyOf, ByteArray::copyOf)
    }
}

data class ResponseEnvelope<T>(
    val message: T,
    val metadata: Metadata = Metadata.EMPTY,
)

data class CallOptions(
    val timeout: Duration? = null,
    val maxResponseBodySize: Int = DEFAULT_MAX_FRAME_SIZE,
    val maxResponseMessages: Int? = DEFAULT_MAX_STREAM_MESSAGES,
    val maxResponseStreamBodySize: Long? = DEFAULT_MAX_STREAM_BODY_SIZE,
    val streamIdleTimeout: Duration? = 30.seconds,
    val metadata: Metadata = Metadata.EMPTY,
) {
    init {
        require(timeout == null || (timeout.isFinite() && timeout.isPositive())) {
            "timeout must be positive and finite"
        }
        require(maxResponseBodySize >= 0) { "maxResponseBodySize must be non-negative" }
        require(maxResponseMessages == null || maxResponseMessages >= 0) {
            "maxResponseMessages must be non-negative"
        }
        require(maxResponseStreamBodySize == null || maxResponseStreamBodySize >= 0) {
            "maxResponseStreamBodySize must be non-negative"
        }
        require(streamIdleTimeout == null || (streamIdleTimeout.isFinite() && streamIdleTimeout.isPositive())) {
            "streamIdleTimeout must be positive and finite"
        }
    }
}

interface RpcTransport {
    suspend fun unary(request: RpcRequest): RpcResponse

    suspend fun openStream(request: RpcRequest): RpcClientStream
}

interface RpcTransportStream {
    suspend fun receive(): RpcStreamFrame?

    /** Receives one frame, then drains up to [maxFrames] frames that are already available. */
    suspend fun receiveBatch(maxFrames: Int): List<RpcStreamFrame> {
        require(maxFrames > 0) { "maxFrames must be positive" }
        return listOfNotNull(receive())
    }

    suspend fun close(cause: Throwable? = null)
}

/**
 * A bidirectional client transport stream. Sends are serialized with [finishSend] and complete only
 * after the transport accepts the write. Sending and receiving may proceed concurrently.
 */
interface RpcClientStream : RpcTransportStream {
    suspend fun send(body: ByteArray)

    /** Sends an ordered batch. Transports may override this to combine immediately ready writes. */
    suspend fun sendBatch(bodies: List<ByteArray>) {
        bodies.forEach { send(it) }
    }

    /** Half-closes the request side after prior sends. Repeated calls have no effect. */
    suspend fun finishSend()
}

class Client(
    private val transport: RpcTransport,
) {
    suspend fun <Req, Res> unary(
        service: String,
        method: String,
        request: Req,
        requestCodec: MessageCodec<Req>,
        responseCodec: MessageCodec<Res>,
        options: CallOptions = CallOptions(),
    ): Res = unaryResponse(service, method, request, requestCodec, responseCodec, options).message

    suspend fun <Req, Res> unaryResponse(
        service: String,
        method: String,
        request: Req,
        requestCodec: MessageCodec<Req>,
        responseCodec: MessageCodec<Res>,
        options: CallOptions = CallOptions(),
    ): ResponseEnvelope<Res> {
        val wireRequest = request(service, method, RpcKind.UNARY, requestCodec.encode(request), options)
        val response = callWithTimeout(options.timeout) { transport.unary(wireRequest) }
        if (!response.status.isOk) throw TrevRpcException(response.status)
        enforceBodyLimit(response.body.size, options.maxResponseBodySize, "unary response")
        return ResponseEnvelope(decodeResponse(responseCodec, response.body), response.metadata)
    }

    suspend fun <Req, Res> serverStreaming(
        service: String,
        method: String,
        request: Req,
        requestCodec: MessageCodec<Req>,
        responseCodec: MessageCodec<Res>,
        options: CallOptions = CallOptions(),
    ): ServerStreamingCall<Res> {
        val state =
            open(
                request(service, method, RpcKind.SERVER_STREAMING, requestCodec.encode(request), options),
                responseCodec,
                options,
                finishSend = true,
            )
        return ServerStreamingCall(state.reader)
    }

    suspend fun <Req, Res> clientStreaming(
        service: String,
        method: String,
        requestCodec: MessageCodec<Req>,
        responseCodec: MessageCodec<Res>,
        options: CallOptions = CallOptions(),
    ): ClientStreamingCall<Req, Res> {
        val state =
            open(
                request(service, method, RpcKind.CLIENT_STREAMING, byteArrayOf(), options),
                responseCodec,
                options,
            )
        return ClientStreamingCall(state.writer, requestCodec, state.reader)
    }

    suspend fun <Req, Res> bidirectionalStreaming(
        service: String,
        method: String,
        requestCodec: MessageCodec<Req>,
        responseCodec: MessageCodec<Res>,
        options: CallOptions = CallOptions(),
    ): BidirectionalStreamingCall<Req, Res> {
        val state =
            open(
                request(service, method, RpcKind.BIDIRECTIONAL_STREAMING, byteArrayOf(), options),
                responseCodec,
                options,
            )
        return BidirectionalStreamingCall(state.writer, requestCodec, state.reader)
    }

    private suspend fun <Res> open(
        request: RpcRequest,
        codec: MessageCodec<Res>,
        options: CallOptions,
        finishSend: Boolean = false,
    ): OpenedStream<Res> {
        val deadline = options.timeout?.let { TimeSource.Monotonic.markNow() + it }
        val stream = callWithDeadline(deadline, null) { transport.openStream(request) }
        var committed = false
        try {
            if (finishSend) callWithDeadline(deadline, null) { stream.finishSend() }
            val writer = RequestWriter(stream, deadline)
            return OpenedStream(writer, ResponseReader(stream, codec, options, deadline)).also { committed = true }
        } finally {
            if (!committed) {
                withContext(NonCancellable) { runCatching { stream.close() } }
            }
        }
    }
}

private data class OpenedStream<T>(
    val writer: RequestWriter,
    val reader: ResponseReader<T>,
)

internal class RequestWriter(
    private val stream: RpcClientStream,
    private val deadline: TimeMark?,
) {
    private val sendFinished = AtomicBoolean(false)

    suspend fun send(body: ByteArray) {
        if (sendFinished.get()) {
            throw TrevRpcException(Status.cancelled("request stream is closed"))
        }
        write { stream.send(body) }
    }

    suspend fun sendBatch(bodies: List<ByteArray>) {
        if (sendFinished.get()) {
            throw TrevRpcException(Status.cancelled("request stream is closed"))
        }
        if (bodies.isNotEmpty()) write { stream.sendBatch(bodies) }
    }

    suspend fun finishSend() {
        if (!sendFinished.compareAndSet(false, true)) return
        write { stream.finishSend() }
    }

    private suspend fun write(operation: suspend () -> Unit) {
        try {
            callWithDeadline(deadline, null, operation)
        } catch (error: Throwable) {
            val closeError =
                withContext(NonCancellable) {
                    runCatching { stream.close(error) }.exceptionOrNull()
                }
            if (closeError != null && closeError !== error) error.addSuppressed(closeError)
            throw error
        }
    }
}

class ServerStreamingCall<T> internal constructor(
    private val reader: ResponseReader<T>,
) {
    val terminalStatus: Status?
        get() = reader.terminalStatus

    val responseMetadata: Metadata
        get() = reader.responseMetadata

    suspend fun receive(): T? = reader.receive()

    suspend fun receiveBatch(maxMessages: Int = 32): List<T> = reader.receiveBatch(maxMessages)

    suspend fun close() = reader.close()
}

class ClientStreamingCall<Req, Res> internal constructor(
    private val writer: RequestWriter,
    private val requestCodec: MessageCodec<Req>,
    private val reader: ResponseReader<Res>,
) {
    private val received = AtomicBoolean(false)

    suspend fun send(request: Req) = writer.send(requestCodec.encode(request))

    suspend fun sendBatch(requests: List<Req>) = writer.sendBatch(requests.map(requestCodec.encode))

    suspend fun closeSend() = writer.finishSend()

    suspend fun receive(): ResponseEnvelope<Res> {
        if (!received.compareAndSet(false, true)) {
            throw TrevRpcException(Status.failedPrecondition("client-streaming response was already received"))
        }
        closeSend()
        var response: ResponseEnvelope<Res>? = null
        var multipleResponses = false
        while (true) {
            val message = reader.receive() ?: break
            if (response == null) {
                response = ResponseEnvelope(message)
            } else {
                multipleResponses = true
            }
        }
        if (response == null) {
            throw TrevRpcException(Status.internal("response stream ended without a response message"))
        }
        if (multipleResponses) {
            throw TrevRpcException(
                Status.internal("client-streaming RPC returned more than one response message"),
            )
        }
        return ResponseEnvelope(response.message, reader.responseMetadata)
    }

    suspend fun close() {
        reader.close()
    }
}

class BidirectionalStreamingCall<Req, Res> internal constructor(
    private val writer: RequestWriter,
    private val requestCodec: MessageCodec<Req>,
    private val reader: ResponseReader<Res>,
) {
    val terminalStatus: Status?
        get() = reader.terminalStatus

    val responseMetadata: Metadata
        get() = reader.responseMetadata

    suspend fun send(request: Req) = writer.send(requestCodec.encode(request))

    suspend fun sendBatch(requests: List<Req>) = writer.sendBatch(requests.map(requestCodec.encode))

    suspend fun closeSend() = writer.finishSend()

    suspend fun receive(): Res? = reader.receive()

    suspend fun close() {
        reader.close()
    }
}

internal class ResponseReader<T>(
    private val stream: RpcTransportStream,
    private val codec: MessageCodec<T>,
    private val options: CallOptions,
    private val deadline: TimeMark?,
) {
    private val closed = AtomicBoolean(false)
    private var messages = 0
    private var bodySize = 0L
    private var done = false
    private var pendingTerminalError: Throwable? = null

    var terminalStatus: Status? = null
        private set

    val responseMetadata: Metadata
        get() = terminalStatus?.metadata ?: Metadata.EMPTY

    suspend fun receive(): T? {
        pendingTerminalError?.let { error ->
            pendingTerminalError = null
            throw error
        }
        if (done) return null
        try {
            val frame = callWithDeadline(deadline, options.streamIdleTimeout) { stream.receive() }
            if (frame == null) {
                done = true
                throw TrevRpcException(Status.internal("response stream ended before final status"))
            }
            return when (frame.kind) {
                RpcStreamFrameKind.MESSAGE -> {
                    receiveMessage(frame)
                }

                RpcStreamFrameKind.STATUS -> {
                    receiveStatus(frame)
                }

                null -> {
                    done = true
                    throw TrevRpcException(Status.invalidArgument("response stream contained an unknown frame kind"))
                }
            }
        } catch (error: CancellationException) {
            done = true
            close(error)
            throw error
        } catch (error: Throwable) {
            done = true
            close(error)
            throw error
        }
    }

    suspend fun receiveBatch(maxMessages: Int): List<T> {
        require(maxMessages > 0) { "maxMessages must be positive" }
        pendingTerminalError?.let { error ->
            pendingTerminalError = null
            throw error
        }
        if (done) return emptyList()
        try {
            val frames = callWithDeadline(deadline, options.streamIdleTimeout) { stream.receiveBatch(maxMessages) }
            if (frames.isEmpty()) {
                done = true
                throw TrevRpcException(Status.internal("response stream ended before final status"))
            }
            val messages = ArrayList<T>(frames.size)
            frames.forEachIndexed { index, frame ->
                when (frame.kind) {
                    RpcStreamFrameKind.MESSAGE -> {
                        messages += receiveMessage(frame)
                    }

                    RpcStreamFrameKind.STATUS -> {
                        if (index != frames.lastIndex) {
                            throw TrevRpcException(Status.internal("response stream contained data after final status"))
                        }
                        try {
                            receiveStatus(frame)
                        } catch (error: CancellationException) {
                            throw error
                        } catch (error: Throwable) {
                            if (messages.isEmpty()) throw error
                            pendingTerminalError = error
                        }
                    }

                    null -> {
                        throw TrevRpcException(Status.invalidArgument("response stream contained an unknown frame kind"))
                    }
                }
            }
            return messages
        } catch (error: CancellationException) {
            done = true
            close(error)
            throw error
        } catch (error: Throwable) {
            done = true
            close(error)
            throw error
        }
    }

    suspend fun close(cause: Throwable? = null) {
        done = true
        if (closed.compareAndSet(false, true)) stream.close(cause)
    }

    private fun receiveMessage(frame: RpcStreamFrame): T {
        val maxMessages = options.maxResponseMessages
        if (maxMessages != null && messages >= maxMessages) {
            throw TrevRpcException(
                Status.resourceExhausted("response stream exceeded maximum of $maxMessages messages"),
            )
        }
        enforceBodyLimit(frame.body.size, options.maxResponseBodySize, "response message")
        val maxBody = options.maxResponseStreamBodySize
        if (maxBody != null && frame.body.size.toLong() > maxBody - bodySize) {
            throw TrevRpcException(
                Status.resourceExhausted("response stream exceeded maximum body size of $maxBody bytes"),
            )
        }
        messages++
        bodySize += frame.body.size
        return decodeResponse(codec, frame.body)
    }

    private suspend fun receiveStatus(frame: RpcStreamFrame): T? {
        val status = frame.status
        var trailingError: Throwable? = null
        val trailing =
            try {
                callWithDeadline(deadline, options.streamIdleTimeout) { stream.receive() }
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                trailingError = error
                null
            }
        done = true
        val closeError =
            try {
                close()
                null
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                error
            }
        if (trailing != null) {
            throw TrevRpcException(Status.internal("response stream contained data after final status"))
        }
        if (!status.isOk) {
            terminalStatus = status
            throw TrevRpcException(status).also { failure ->
                trailingError?.let(failure::addSuppressed)
                closeError?.let(failure::addSuppressed)
            }
        }
        trailingError?.let { error ->
            if (!error.isMalformedTrailingData()) throw error
            throw TrevRpcException(
                Status.internal("response stream contained data after final status"),
                error,
            )
        }
        if (closeError != null) throw closeError
        terminalStatus = status
        return null
    }
}

private fun request(
    service: String,
    method: String,
    kind: RpcKind,
    body: ByteArray,
    options: CallOptions,
): RpcRequest =
    RpcRequest(
        service = service,
        method = method,
        body = body.copyOf(),
        metadata = options.metadata,
        kindValue = kind.value,
        timeoutNanos = timeoutNanos(options.timeout),
    )

private fun timeoutNanos(timeout: Duration?): ULong {
    if (timeout == null) return 0u
    return timeout.inWholeNanoseconds.coerceAtLeast(1).toULong()
}

private fun enforceBodyLimit(
    size: Int,
    maximum: Int,
    description: String,
) {
    if (size > maximum) {
        throw TrevRpcException(
            Status.resourceExhausted("$description is $size bytes, maximum is $maximum"),
        )
    }
}

private fun Throwable.isMalformedTrailingData(): Boolean =
    when (this) {
        is TrevRpcException -> {
            status.code == Code.INTERNAL ||
                status.code == Code.INVALID_ARGUMENT ||
                status.code == Code.RESOURCE_EXHAUSTED ||
                status.code == Code.DATA_LOSS
        }

        else -> {
            true
        }
    }

private fun <T> decodeResponse(
    codec: MessageCodec<T>,
    body: ByteArray,
): T =
    try {
        codec.decode(body.copyOf())
    } catch (error: TrevRpcException) {
        throw error
    } catch (error: Throwable) {
        throw TrevRpcException(Status.internal("failed to decode response body"), error)
    }

private suspend fun <T> callWithTimeout(
    timeout: Duration?,
    operation: suspend () -> T,
): T =
    if (timeout == null) {
        operation()
    } else {
        try {
            withTimeout(timeout) { operation() }
        } catch (error: TimeoutCancellationException) {
            throw TrevRpcException(Status.deadlineExceeded("RPC deadline exceeded"), error)
        }
    }

private suspend fun <T> callWithDeadline(
    deadline: TimeMark?,
    idleTimeout: Duration?,
    operation: suspend () -> T,
): T {
    val deadlineTimeout =
        deadline?.let {
            val remaining = -it.elapsedNow()
            if (!remaining.isPositive()) {
                throw TrevRpcException(Status.deadlineExceeded("RPC deadline exceeded"))
            }
            remaining
        }
    val timeout = listOfNotNull(deadlineTimeout, idleTimeout).minOrNull() ?: return operation()
    val deadlineWins = deadlineTimeout != null && deadlineTimeout <= timeout
    return try {
        withTimeout(timeout) { operation() }
    } catch (error: TimeoutCancellationException) {
        val status =
            if (deadlineWins) {
                Status.deadlineExceeded("RPC deadline exceeded")
            } else {
                Status.unavailable("response stream idle timeout")
            }
        throw TrevRpcException(status, error)
    } catch (error: CancellationException) {
        throw error
    }
}
