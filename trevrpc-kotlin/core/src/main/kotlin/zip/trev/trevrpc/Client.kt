package zip.trev.trevrpc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.receiveAsFlow
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
    val requestChannelCapacity: Int = DEFAULT_CALL_CHANNEL_CAPACITY,
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
        require(requestChannelCapacity > 0) { "requestChannelCapacity must be positive" }
    }
}

interface RpcTransport {
    suspend fun unary(request: RpcRequest): RpcResponse

    suspend fun openStream(
        request: RpcRequest,
        requestBody: Flow<ByteArray>,
    ): RpcTransportStream
}

interface RpcTransportStream {
    suspend fun receive(): RpcStreamFrame?

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
    ): Res = unaryEnvelope(service, method, request, requestCodec, responseCodec, options).message

    suspend fun <Req, Res> unaryEnvelope(
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
                emptyFlow(),
                responseCodec,
                options,
            )
        return ServerStreamingCall(state)
    }

    suspend fun <Req, Res> clientStreaming(
        service: String,
        method: String,
        requestCodec: MessageCodec<Req>,
        responseCodec: MessageCodec<Res>,
        options: CallOptions = CallOptions(),
    ): ClientStreamingCall<Req, Res> {
        val outbound = Channel<ByteArray>(options.requestChannelCapacity)
        val state =
            try {
                open(
                    request(service, method, RpcKind.CLIENT_STREAMING, byteArrayOf(), options),
                    outbound.receiveAsFlow(),
                    responseCodec,
                    options,
                    onTerminal = { outbound.cancel() },
                )
            } catch (error: Throwable) {
                outbound.cancel()
                throw error
            }
        return ClientStreamingCall(outbound, requestCodec, state)
    }

    suspend fun <Req, Res> bidirectionalStreaming(
        service: String,
        method: String,
        requestCodec: MessageCodec<Req>,
        responseCodec: MessageCodec<Res>,
        options: CallOptions = CallOptions(),
    ): BidirectionalStreamingCall<Req, Res> {
        val outbound = Channel<ByteArray>(options.requestChannelCapacity)
        val state =
            try {
                open(
                    request(service, method, RpcKind.BIDIRECTIONAL_STREAMING, byteArrayOf(), options),
                    outbound.receiveAsFlow(),
                    responseCodec,
                    options,
                    onTerminal = { outbound.cancel() },
                )
            } catch (error: Throwable) {
                outbound.cancel()
                throw error
            }
        return BidirectionalStreamingCall(outbound, requestCodec, state)
    }

    private suspend fun <Res> open(
        request: RpcRequest,
        body: Flow<ByteArray>,
        codec: MessageCodec<Res>,
        options: CallOptions,
        onTerminal: () -> Unit = {},
    ): ResponseReader<Res> {
        val deadline = options.timeout?.let { TimeSource.Monotonic.markNow() + it }
        val stream = callWithDeadline(deadline, null) { transport.openStream(request, body) }
        return ResponseReader(stream, codec, options, deadline, onTerminal)
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

    suspend fun close() = reader.close()
}

class ClientStreamingCall<Req, Res> internal constructor(
    private val outbound: Channel<ByteArray>,
    private val requestCodec: MessageCodec<Req>,
    private val reader: ResponseReader<Res>,
) {
    private val received = AtomicBoolean(false)

    suspend fun send(request: Req) = sendOutbound(outbound, requestCodec.encode(request))

    fun closeSend() {
        outbound.close()
    }

    suspend fun receive(): ResponseEnvelope<Res> {
        if (!received.compareAndSet(false, true)) {
            throw TrevRpcException(Status.failedPrecondition("client-streaming response was already received"))
        }
        closeSend()
        val first =
            reader.receive()
                ?: throw TrevRpcException(Status.internal("response stream ended without a response message"))
        val second = reader.receive()
        if (second != null) {
            reader.close()
            throw TrevRpcException(
                Status.internal("client-streaming RPC returned more than one response message"),
            )
        }
        return ResponseEnvelope(first, reader.responseMetadata)
    }

    suspend fun close() {
        outbound.cancel()
        reader.close()
    }
}

class BidirectionalStreamingCall<Req, Res> internal constructor(
    private val outbound: Channel<ByteArray>,
    private val requestCodec: MessageCodec<Req>,
    private val reader: ResponseReader<Res>,
) {
    val terminalStatus: Status?
        get() = reader.terminalStatus

    val responseMetadata: Metadata
        get() = reader.responseMetadata

    suspend fun send(request: Req) = sendOutbound(outbound, requestCodec.encode(request))

    fun closeSend() {
        outbound.close()
    }

    suspend fun receive(): Res? = reader.receive()

    suspend fun close() {
        outbound.cancel()
        reader.close()
    }
}

internal class ResponseReader<T>(
    private val stream: RpcTransportStream,
    private val codec: MessageCodec<T>,
    private val options: CallOptions,
    private val deadline: TimeMark?,
    private val onTerminal: () -> Unit,
) {
    private val closed = AtomicBoolean(false)
    private var messages = 0
    private var bodySize = 0L
    private var done = false

    var terminalStatus: Status? = null
        private set

    val responseMetadata: Metadata
        get() = terminalStatus?.metadata ?: Metadata.EMPTY

    suspend fun receive(): T? {
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
                    throw TrevRpcException(Status.internal("response stream contained an unknown frame kind"))
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

    suspend fun close(cause: Throwable? = null) {
        done = true
        onTerminal()
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
        done = true
        terminalStatus = frame.status
        val closeError =
            try {
                close()
                null
            } catch (error: CancellationException) {
                throw error
            } catch (error: Throwable) {
                error
            }
        if (!frame.status.isOk) throw TrevRpcException(frame.status)
        if (closeError != null) throw closeError
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

private suspend fun sendOutbound(
    outbound: Channel<ByteArray>,
    body: ByteArray,
) {
    try {
        outbound.send(body.copyOf())
    } catch (error: ClosedSendChannelException) {
        throw TrevRpcException(Status.cancelled("request stream is closed"), error)
    }
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
