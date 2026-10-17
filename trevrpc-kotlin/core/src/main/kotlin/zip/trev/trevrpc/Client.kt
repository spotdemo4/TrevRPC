package zip.trev.trevrpc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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

/**
 * A single response message. [metadata] is the trailing metadata from the terminal status;
 * [headers] is the response header metadata, empty when the server sent no HEADERS frame.
 */
data class ResponseEnvelope<T>(
    val message: T,
    val metadata: Metadata = Metadata.EMPTY,
    val headers: Metadata = Metadata.EMPTY,
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
    /**
     * Opens one call stream and writes REQUEST. When [firstMessage] is present (unary and
     * server-streaming calls) it is written in the same write as REQUEST, and when [finishSend] is
     * also set the request side is half-closed in that write too. Implementations clean up after
     * themselves when this fails.
     */
    suspend fun openStream(
        request: RpcRequest,
        firstMessage: ByteArray? = null,
        finishSend: Boolean = false,
    ): RpcClientStream
}

/**
 * A bidirectional client transport stream. Sends are serialized with [finishSend] and complete only
 * after the transport accepts the write. Sending and receiving may proceed concurrently.
 */
interface RpcClientStream {
    suspend fun send(body: ByteArray)

    /** Half-closes the request side after prior sends. Repeated calls have no effect. */
    suspend fun finishSend()

    /**
     * Receives the next known response frame, or null at transport end of stream. Unknown frames
     * are skipped by the transport and never surface here.
     */
    suspend fun receive(): ResponseFrame?

    suspend fun close(cause: Throwable? = null)
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
        // A unary call has no stream-idle bound, only the call deadline.
        val state =
            open(
                request(service, method, RpcKind.UNARY, options),
                requestCodec.encode(request),
                responseCodec,
                options,
                finishSend = true,
                idleTimeout = null,
            )
        return state.reader.receiveSingle("unary")
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
                request(service, method, RpcKind.SERVER_STREAMING, options),
                requestCodec.encode(request),
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
        val state = open(request(service, method, RpcKind.CLIENT_STREAMING, options), null, responseCodec, options)
        return ClientStreamingCall(state.writer, requestCodec, state.reader)
    }

    suspend fun <Req, Res> bidirectionalStreaming(
        service: String,
        method: String,
        requestCodec: MessageCodec<Req>,
        responseCodec: MessageCodec<Res>,
        options: CallOptions = CallOptions(),
    ): BidirectionalStreamingCall<Req, Res> {
        val state = open(request(service, method, RpcKind.BIDIRECTIONAL_STREAMING, options), null, responseCodec, options)
        return BidirectionalStreamingCall(state.writer, requestCodec, state.reader)
    }

    private suspend fun <Res> open(
        request: RpcRequest,
        firstMessage: ByteArray?,
        codec: MessageCodec<Res>,
        options: CallOptions,
        finishSend: Boolean = false,
        idleTimeout: Duration? = options.streamIdleTimeout,
    ): OpenedStream<Res> {
        val deadline = options.timeout?.let { TimeSource.Monotonic.markNow() + it }
        val stream = callWithDeadline(deadline, null) { transport.openStream(request, firstMessage, finishSend) }
        return OpenedStream(
            RequestWriter(stream, deadline, sendFinished = finishSend),
            ResponseReader(stream, codec, options, deadline, idleTimeout),
        )
    }
}

private data class OpenedStream<T>(
    val writer: RequestWriter,
    val reader: ResponseReader<T>,
)

internal class RequestWriter(
    private val stream: RpcClientStream,
    private val deadline: TimeMark?,
    sendFinished: Boolean = false,
) {
    private val sendFinished = AtomicBoolean(sendFinished)

    suspend fun send(body: ByteArray) {
        if (sendFinished.get()) {
            throw TrevRpcException(Status.cancelled("request stream is closed"))
        }
        write { stream.send(body) }
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

    /** Trailing metadata from the terminal status; empty until the status arrives. */
    val responseMetadata: Metadata
        get() = reader.responseMetadata

    /**
     * Response header metadata. Suspends until the first known response frame arrives and is empty
     * when the server sent no HEADERS frame. No message is lost: a message read ahead is returned
     * by the next [receive].
     */
    suspend fun headers(): Metadata = reader.headers()

    suspend fun receive(): T? = reader.receive()

    suspend fun close() = reader.close()
}

class ClientStreamingCall<Req, Res> internal constructor(
    private val writer: RequestWriter,
    private val requestCodec: MessageCodec<Req>,
    private val reader: ResponseReader<Res>,
) {
    private val received = AtomicBoolean(false)

    suspend fun send(request: Req) = writer.send(requestCodec.encode(request))

    suspend fun closeSend() = writer.finishSend()

    /** Response header metadata; see [ServerStreamingCall.headers]. */
    suspend fun headers(): Metadata = reader.headers()

    suspend fun receive(): ResponseEnvelope<Res> {
        if (!received.compareAndSet(false, true)) {
            throw TrevRpcException(Status.failedPrecondition("client-streaming response was already received"))
        }
        closeSend()
        return reader.receiveSingle("client-streaming")
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

    /** Trailing metadata from the terminal status; empty until the status arrives. */
    val responseMetadata: Metadata
        get() = reader.responseMetadata

    /** Response header metadata; see [ServerStreamingCall.headers]. */
    suspend fun headers(): Metadata = reader.headers()

    suspend fun send(request: Req) = writer.send(requestCodec.encode(request))

    suspend fun closeSend() = writer.finishSend()

    suspend fun receive(): Res? = reader.receive()

    suspend fun close() {
        reader.close()
    }
}

internal class ResponseReader<T>(
    private val stream: RpcClientStream,
    private val codec: MessageCodec<T>,
    private val options: CallOptions,
    private val deadline: TimeMark?,
    private val idleTimeout: Duration? = options.streamIdleTimeout,
) {
    private val closed = AtomicBoolean(false)

    /** Serializes reads from [stream], so [headers] and [receive] never race for frames. */
    private val readLock = Mutex()
    private var messages = 0
    private var bodySize = 0L

    @Volatile
    private var done = false

    /** A message or status that [headers] read ahead and [receive] has not consumed yet. */
    private var readAhead: ResponseFrame? = null
    private var failure: Throwable? = null
    private var failureDelivered = false

    /** Null until the first known response frame arrives; then the header metadata. */
    @Volatile
    var headersMetadata: Metadata? = null
        private set

    @Volatile
    var terminalStatus: Status? = null
        private set

    val responseMetadata: Metadata
        get() = terminalStatus?.metadata ?: Metadata.EMPTY

    suspend fun headers(): Metadata {
        headersMetadata?.let { return it }
        return readLock.withLock {
            headersMetadata?.let { return@withLock it }
            failure?.let { throw it }
            if (done) return@withLock Metadata.EMPTY
            try {
                readAhead = nextFrame()
                // Reading the first known frame resolves the headers: HEADERS carry them, anything
                // else means they are absent. A HEADERS frame is consumed here; other frames are
                // kept for receive().
                if (readAhead is ResponseFrame.Headers) readAhead = null
                checkNotNull(headersMetadata)
            } catch (error: Throwable) {
                fail(error)
                throw error
            }
        }
    }

    suspend fun receive(): T? = readLock.withLock { receiveLocked() }

    private suspend fun receiveLocked(): T? {
        if (!failureDelivered) {
            failure?.let {
                failureDelivered = true
                throw it
            }
        }
        if (done) return null
        try {
            while (true) {
                when (val frame = readAhead?.also { readAhead = null } ?: nextFrame()) {
                    is ResponseFrame.Headers -> {}

                    is ResponseFrame.Message -> {
                        return receiveMessage(frame)
                    }

                    is ResponseFrame.Status -> {
                        return receiveStatus(frame)
                    }
                }
            }
        } catch (error: Throwable) {
            failureDelivered = true
            fail(error)
            throw error
        }
    }

    suspend fun close(cause: Throwable? = null) {
        done = true
        if (closed.compareAndSet(false, true)) stream.close(cause)
    }

    /**
     * Reads one frame from the transport and applies the response grammar: HEADERS is allowed
     * once, and only as the first known frame.
     */
    private suspend fun nextFrame(): ResponseFrame {
        val frame =
            callWithDeadline(deadline, idleTimeout) { stream.receive() }
                ?: throw TrevRpcException(Status.internal("response stream ended before final status"))
        if (frame is ResponseFrame.Headers) {
            if (headersMetadata != null) {
                throw TrevRpcException(
                    Status.internal("response HEADERS must be the first frame and appear at most once"),
                )
            }
            headersMetadata = frame.metadata
        } else if (headersMetadata == null) {
            headersMetadata = Metadata.EMPTY
        }
        return frame
    }

    private suspend fun fail(error: Throwable) {
        done = true
        if (error !is CancellationException) failure = error
        close(error)
    }

    private fun receiveMessage(frame: ResponseFrame.Message): T {
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

    private suspend fun receiveStatus(frame: ResponseFrame.Status): T? {
        val status = frame.status
        var trailingError: Throwable? = null
        val trailing =
            try {
                callWithDeadline(deadline, idleTimeout) { stream.receive() }
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

/**
 * Reads the single response message of a unary or client-streaming call, then the terminal status.
 * The stream is drained to its status first so a remote failure wins over a shape violation.
 */
internal suspend fun <T> ResponseReader<T>.receiveSingle(kind: String): ResponseEnvelope<T> {
    var response: T? = null
    var multipleResponses = false
    while (true) {
        val message = receive() ?: break
        if (response == null) {
            response = message
        } else {
            multipleResponses = true
        }
    }
    if (response == null) {
        throw TrevRpcException(Status.internal("response stream ended without a response message"))
    }
    if (multipleResponses) {
        throw TrevRpcException(Status.internal("$kind RPC returned more than one response message"))
    }
    return ResponseEnvelope(response, responseMetadata, headersMetadata ?: Metadata.EMPTY)
}

private fun request(
    service: String,
    method: String,
    kind: RpcKind,
    options: CallOptions,
): RpcRequest =
    RpcRequest(
        service = service,
        method = method,
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
