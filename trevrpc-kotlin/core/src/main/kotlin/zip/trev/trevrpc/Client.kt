package zip.trev.trevrpc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.select
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlin.time.TimeMark
import kotlin.time.TimeSource

/**
 * Encodes and decodes the messages of one type. [messageType] is the full protobuf name of the
 * type, such as `example.v1.Position`, which picks its [Dictionary]; generated codecs set it, and
 * messages of a codec without one are never compressed.
 */
class MessageCodec<T>(
    val encode: (T) -> ByteArray,
    val decode: (ByteArray) -> T,
    val messageType: String? = null,
) {
    companion object {
        val BYTE_ARRAY: MessageCodec<ByteArray> = MessageCodec(ByteArray::copyOf, ByteArray::copyOf)
    }
}

/**
 * Options of one call. By default a call has no deadline and no metadata, and a response stream is
 * bounded only by [maxResponseBodySize] per message: [maxResponseMessages],
 * [maxResponseStreamBodySize], and [streamIdleTimeout] are off until set.
 *
 * [onResponseHeaders] is called with the response headers, empty when the server sent none, as
 * soon as the first response frame arrives, also while a streaming request is still uploading.
 * [onResponseMetadata] is called with the trailing metadata of the final status when the call
 * succeeds, before its result is returned or its response flow completes; a failed call carries
 * its metadata on the [TrevRpcException] instead. Both run in the coroutine that reads the
 * response, and one that throws fails the call.
 */
data class CallOptions(
    val timeout: Duration? = null,
    val maxResponseBodySize: Int = DEFAULT_MAX_FRAME_SIZE,
    val maxResponseMessages: Int? = null,
    val maxResponseStreamBodySize: Long? = null,
    val streamIdleTimeout: Duration? = null,
    val metadata: Metadata = Metadata.EMPTY,
    val onResponseHeaders: ((Metadata) -> Unit)? = null,
    val onResponseMetadata: ((Metadata) -> Unit)? = null,
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
    /** The compressed messages calls on this transport negotiate, or null for none. */
    val compression: Compression?
        get() = null

    /**
     * The memory budget of stream compression on this transport, or null for none, which turns it
     * off. Stream compression also needs [compression].
     */
    val streamCompressionBudget: StreamCompressionBudget?
        get() = null

    /**
     * The channel's connection metadata, which [Client] puts in the REQUEST of every call, or null
     * for none.
     */
    val connectionMetadata: ConnectionMetadata?
        get() = null

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
 * after the transport accepts the write. Sending and receiving may proceed concurrently: for calls
 * that stream their request, [Client] reads the response through [receive] from the moment the
 * call opens, so it learns of the server's terminal status while a [send] is blocked.
 */
interface RpcClientStream {
    /**
     * Writes one message frame: [body] is the message of a MESSAGE frame, or the payload of a
     * ZSTD_MESSAGE or ZSTD_STREAM_MESSAGE frame when [frameType] says so (see
     * [encodeMessageFrame]). [Client] sends compressed frames only on a transport that has
     * [RpcTransport.compression]. [Client] cancels
     * a send in flight when the response ends the call, so implementations must stay cancellable.
     * A transport that stops accepting writes because the server ended the call should fail with a
     * [TrevRpcException]; [Client] reports the server's status or the response error instead when
     * the response explains the failure, so a transport that cannot say why may fail with any
     * exception.
     */
    suspend fun send(
        body: ByteArray,
        frameType: Long = FrameType.MESSAGE,
    )

    /** Half-closes the request side after prior sends. Repeated calls have no effect. */
    suspend fun finishSend()

    /**
     * Receives the next known response frame, or null at transport end of stream. Unknown frames
     * are skipped by the transport and never surface here.
     */
    suspend fun receive(): ResponseFrame?

    suspend fun close(cause: Throwable? = null)
}

/**
 * Runs RPCs over [transport].
 *
 * Client-streaming and bidirectional calls read their response in a coroutine of [responseContext]
 * (a `Job` in it is ignored) from the moment they open until STATUS arrives or the call closes, so
 * the server's terminal status reaches a caller that is only sending. See [ClientStreamingCall.send].
 */
class Client(
    private val transport: RpcTransport,
    private val responseContext: CoroutineContext = Dispatchers.Default,
) {
    /**
     * Calls a unary RPC and returns its response. [compressMessages] is the method's compression
     * without a dictionary, which generated clients pass.
     */
    suspend fun <Req, Res> unary(
        path: String,
        request: Req,
        requestCodec: MessageCodec<Req>,
        responseCodec: MessageCodec<Res>,
        options: CallOptions = CallOptions(),
        compressMessages: CompressMessages = CompressMessages.NONE,
    ): Res {
        // A unary call has no stream-idle bound, only the call deadline.
        val state =
            open(
                path,
                RpcKind.UNARY,
                requestCodec,
                requestCodec.encode(request),
                responseCodec,
                options,
                finishSend = true,
                idleTimeout = null,
                compressMessages = compressMessages,
            )
        return state.reader.receiveSingle("unary")
    }

    /**
     * Calls a server-streaming RPC when the returned flow is collected, and emits its responses.
     * Cancelling the collection cancels the call. [compressStream] is the method's stream
     * compression and [compressMessages] its compression without a dictionary, which generated
     * clients pass.
     */
    fun <Req, Res> serverStreaming(
        path: String,
        request: Req,
        requestCodec: MessageCodec<Req>,
        responseCodec: MessageCodec<Res>,
        options: CallOptions = CallOptions(),
        compressStream: CompressStream = CompressStream.NONE,
        compressMessages: CompressMessages = CompressMessages.NONE,
    ): Flow<Res> =
        flow {
            val call = openServerStreaming(path, request, requestCodec, responseCodec, options, compressStream, compressMessages)
            try {
                while (true) emit(call.receive() ?: break)
            } finally {
                call.close()
            }
        }

    /**
     * Calls a client-streaming RPC that sends every request of [requests], and returns its
     * response. If the server ends the call before [requests] is exhausted, the rest is dropped and
     * the server's outcome is returned or thrown.
     */
    suspend fun <Req, Res> clientStreaming(
        path: String,
        requests: Flow<Req>,
        requestCodec: MessageCodec<Req>,
        responseCodec: MessageCodec<Res>,
        options: CallOptions = CallOptions(),
        compressStream: CompressStream = CompressStream.NONE,
        compressMessages: CompressMessages = CompressMessages.NONE,
    ): Res {
        val call = openClientStreaming(path, requestCodec, responseCodec, options, compressStream, compressMessages)
        return try {
            call.sendAll(requests)
            call.receive()
        } finally {
            call.close()
        }
    }

    /**
     * Calls a bidirectional-streaming RPC when the returned flow is collected: it sends every
     * request of [requests] while it emits the responses. Cancelling the collection cancels the
     * call.
     */
    fun <Req, Res> bidirectionalStreaming(
        path: String,
        requests: Flow<Req>,
        requestCodec: MessageCodec<Req>,
        responseCodec: MessageCodec<Res>,
        options: CallOptions = CallOptions(),
        compressStream: CompressStream = CompressStream.NONE,
        compressMessages: CompressMessages = CompressMessages.NONE,
    ): Flow<Res> =
        flow {
            coroutineScope {
                val call = openBidirectionalStreaming(path, requestCodec, responseCodec, options, compressStream, compressMessages)
                val sender = launch { call.sendAll(requests) }
                try {
                    while (true) emit(call.receive() ?: break)
                } finally {
                    sender.cancel()
                    call.close()
                }
            }
        }

    /**
     * Opens a server-streaming call and returns it, for reading its responses, headers, and final
     * status one at a time.
     */
    suspend fun <Req, Res> openServerStreaming(
        path: String,
        request: Req,
        requestCodec: MessageCodec<Req>,
        responseCodec: MessageCodec<Res>,
        options: CallOptions = CallOptions(),
        compressStream: CompressStream = CompressStream.NONE,
        compressMessages: CompressMessages = CompressMessages.NONE,
    ): ServerStreamingCall<Res> {
        val state =
            open(
                path,
                RpcKind.SERVER_STREAMING,
                requestCodec,
                requestCodec.encode(request),
                responseCodec,
                options,
                finishSend = true,
                compressStream = compressStream,
                compressMessages = compressMessages,
            )
        return ServerStreamingCall(state.reader)
    }

    /** Opens a client-streaming call and returns it, for sending its requests one at a time. */
    suspend fun <Req, Res> openClientStreaming(
        path: String,
        requestCodec: MessageCodec<Req>,
        responseCodec: MessageCodec<Res>,
        options: CallOptions = CallOptions(),
        compressStream: CompressStream = CompressStream.NONE,
        compressMessages: CompressMessages = CompressMessages.NONE,
    ): ClientStreamingCall<Req, Res> {
        val state =
            open(
                path,
                RpcKind.CLIENT_STREAMING,
                requestCodec,
                null,
                responseCodec,
                options,
                compressStream = compressStream,
                compressMessages = compressMessages,
            )
        return ClientStreamingCall(state.writer, requestCodec, state.reader)
    }

    /**
     * Opens a bidirectional-streaming call and returns it, for sending requests and receiving
     * responses one at a time.
     */
    suspend fun <Req, Res> openBidirectionalStreaming(
        path: String,
        requestCodec: MessageCodec<Req>,
        responseCodec: MessageCodec<Res>,
        options: CallOptions = CallOptions(),
        compressStream: CompressStream = CompressStream.NONE,
        compressMessages: CompressMessages = CompressMessages.NONE,
    ): BidirectionalStreamingCall<Req, Res> {
        val state =
            open(
                path,
                RpcKind.BIDIRECTIONAL_STREAMING,
                requestCodec,
                null,
                responseCodec,
                options,
                compressStream = compressStream,
                compressMessages = compressMessages,
            )
        return BidirectionalStreamingCall(state.writer, requestCodec, state.reader)
    }

    private suspend fun <Res> open(
        path: String,
        kind: RpcKind,
        requestCodec: MessageCodec<*>,
        firstMessage: ByteArray?,
        codec: MessageCodec<Res>,
        options: CallOptions,
        finishSend: Boolean = false,
        idleTimeout: Duration? = options.streamIdleTimeout,
        compressStream: CompressStream = CompressStream.NONE,
        compressMessages: CompressMessages = CompressMessages.NONE,
    ): OpenedStream<Res> {
        val configured = transport.compression
        val compression = configured?.call(requestCodec.messageType, codec.messageType, streamsRequest = !finishSend, compressMessages)
        val budget = transport.streamCompressionBudget
        val directions = compressStream.effective(kind)
        val streams = configured?.let { CallStreams.open(it, budget, directions, compression) }
        val request = request(path, kind, options, compression, streams, transport.connectionMetadata)
        val deadline = options.timeout?.let { TimeSource.Monotonic.markNow() + it }
        val stream =
            try {
                callWithDeadline(deadline, null) { transport.openStream(request, firstMessage, finishSend) }
            } catch (error: Throwable) {
                streams?.close()
                throw error
            }
        // An optimistic call replays itself, without optimism, if the server refuses it.
        val replay =
            if (configured != null && budget != null && streams?.optimistic == true) {
                OptimisticReplay(transport, request, deadline, responseContext, budget) {
                    CallStreams.open(configured, budget, directions, compression, optimistic = false)
                }
            } else {
                null
            }
        val call = CallStream(stream, compression, streams, options.maxResponseBodySize, options.onResponseHeaders, replay)
        // A call that streams its request needs to learn of the server's terminal status while it uploads.
        if (!finishSend) call.pump = ResponsePump(responseContext, call, deadline, options)
        return OpenedStream(
            RequestWriter(call, deadline, options.streamIdleTimeout, sendFinished = finishSend),
            ResponseReader(call, codec, options, deadline, idleTimeout),
        )
    }
}

private data class OpenedStream<T>(
    val writer: RequestWriter,
    val reader: ResponseReader<T>,
)

/** Stops [RequestWriter.sendAll] once the response has ended the call. */
private class CallEnded : Exception()

/** How long a failed write waits for the response to explain it. */
internal val EXPLANATION_TIMEOUT = 5.seconds

/**
 * Writes the request of a call. A write races the end of the call: when the response ends it (the
 * server's STATUS, a response failure, or a close), the write in flight is cancelled and the send
 * fails with the reason, so a send never waits on flow control for a server that stopped reading.
 */
internal class RequestWriter(
    private val call: CallStream,
    private val deadline: TimeMark?,
    private val idleTimeout: Duration?,
    sendFinished: Boolean = false,
) {
    private val sendFinished = AtomicBoolean(sendFinished)

    private sealed interface Outcome {
        object Written : Outcome

        /** The response ended the call; [error] is why the send fails. */
        class Ended(
            val error: TrevRpcException,
        ) : Outcome

        /** The write failed and the response did not explain why. */
        class Failed(
            val error: Throwable,
        ) : Outcome
    }

    suspend fun send(body: ByteArray) {
        if (sendFinished.get()) {
            throw TrevRpcException(Status.cancelled("request stream is closed"))
        }
        call.end.sendError()?.let { throw it }
        write(reportEnd = true) { call.send(body) }
    }

    /**
     * Sends every body of [bodies], then half-closes. When the response ends the call first the
     * remaining bodies are dropped and nothing is thrown: [ResponseReader] reports the outcome.
     */
    suspend fun sendAll(bodies: Flow<ByteArray>) {
        try {
            bodies.collect { body ->
                try {
                    send(body)
                } catch (error: TrevRpcException) {
                    if (call.end.isEnded) throw CallEnded()
                    throw error
                }
            }
        } catch (_: CallEnded) {
            // The call ended while uploading; the response says how.
        }
        finishSend()
    }

    suspend fun finishSend() {
        if (!sendFinished.compareAndSet(false, true)) return
        try {
            // Half-closing a call that already ended has nothing left to do; the outcome is
            // reported by receive.
            if (!call.end.isEnded) write(reportEnd = false) { call.finishSend() }
        } finally {
            call.pump?.uploadClosed()
        }
    }

    private suspend fun write(
        reportEnd: Boolean,
        operation: suspend () -> Unit,
    ) {
        val outcome =
            try {
                attempt(operation)
            } catch (error: Throwable) {
                closeAfter(error)
                throw error
            }
        when (outcome) {
            Outcome.Written -> {}

            is Outcome.Ended -> {
                // After the server's STATUS the response is still being read. Any other end (a
                // failure, the call's deadline) leaves a stream that nobody else will close.
                if (!call.end.endedByStatus) closeAfter(outcome.error)
                if (reportEnd) throw outcome.error
            }

            is Outcome.Failed -> {
                closeAfter(outcome.error)
                throw outcome.error
            }
        }
    }

    private suspend fun attempt(operation: suspend () -> Unit): Outcome {
        val failure =
            try {
                if (raceEnd(operation)) return Outcome.Written
                null
            } catch (error: Throwable) {
                error
            }
        if (failure is CancellationException && !currentCoroutineContext().isActive) throw failure
        call.end.sendError()?.let { ended ->
            failure?.let(ended::addSuppressed)
            return Outcome.Ended(ended)
        }
        val cause = checkNotNull(failure) { "the write was abandoned without the call ending" }
        // The write failed. A transport that refuses writes usually does so because the response
        // ended or failed, and that is what the caller needs to hear, not a generic closed stream.
        awaitExplanation(cause)?.let { explanation ->
            explanation.addSuppressed(cause)
            return Outcome.Ended(explanation)
        }
        return Outcome.Failed(
            if (cause is CancellationException) {
                TrevRpcException(Status.cancelled("request stream is closed"), cause)
            } else {
                cause
            },
        )
    }

    /** Runs [operation]; false if the response ended the call first and the write was cancelled. */
    private suspend fun raceEnd(operation: suspend () -> Unit): Boolean =
        coroutineScope {
            val write = async(start = CoroutineStart.UNDISPATCHED) { callWithDeadline(deadline, null, operation) }
            select {
                write.onAwait { true }
                call.end.onEnd {
                    write.cancel()
                    false
                }
            }
        }

    /**
     * Waits, within the call's deadline and idle timeout, for the response to end the call. Null if
     * it does not: the call's own deadline cut the write short, or nothing explains the failure.
     */
    private suspend fun awaitExplanation(failure: Throwable): TrevRpcException? {
        if (failure.isCallDeadline()) return null
        try {
            callWithDeadline(deadline, minOf(idleTimeout ?: EXPLANATION_TIMEOUT, EXPLANATION_TIMEOUT)) { call.end.await() }
        } catch (error: CancellationException) {
            throw error
        } catch (_: Throwable) {
            return null
        }
        return call.end.sendError()
    }

    private suspend fun closeAfter(error: Throwable) {
        val closeError =
            withContext(NonCancellable) {
                runCatching { call.close(error) }.exceptionOrNull()
            }
        if (closeError != null && closeError !== error) error.addSuppressed(closeError)
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

    /**
     * Sends one request message, suspending until the transport accepts it.
     *
     * The call is watched while it uploads: as soon as the server ends it, [send] stops and fails
     * instead of waiting on flow control or the deadline for a server that no longer reads, even
     * when nobody is receiving yet and even when the server did not stop the request side.
     *
     * - A non-OK terminal status is thrown as the [TrevRpcException] [receive] reports too.
     * - An OK status that ends the call before the upload finished is a [TrevRpcException] with
     *   [Code.CANCELLED]; the response is still available from [receive].
     * - A failure of the response side (a lost connection, a protocol violation, an exceeded
     *   response limit) is what [send] throws, rather than a generic "closed" error.
     * - After [closeSend] or [close] it fails with [Code.CANCELLED].
     */
    suspend fun send(request: Req) = writer.send(requestCodec.encode(request))

    /**
     * Sends every request of [requests], then half-closes the request side. If the server ends the
     * call first the remaining requests are dropped and nothing is thrown; [receive] reports the
     * outcome. Other failures are thrown like [send] does.
     */
    suspend fun sendAll(requests: Flow<Req>) = writer.sendAll(requests.map(requestCodec.encode))

    /** Half-closes the request side. Does nothing if the call already ended. */
    suspend fun closeSend() = writer.finishSend()

    /** Response header metadata; see [ServerStreamingCall.headers]. */
    suspend fun headers(): Metadata = reader.headers()

    /** Half-closes the request side and returns the response. */
    suspend fun receive(): Res {
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

    /**
     * Sends one request message, suspending until the transport accepts it. Fails promptly once
     * the server has ended the call; see [ClientStreamingCall.send] for the exact errors. Sends
     * need no concurrent [receive]: the response is watched for the server's status while the call
     * uploads, and responses read ahead for that are returned by later [receive] calls.
     */
    suspend fun send(request: Req) = writer.send(requestCodec.encode(request))

    /** Sends every request of [requests], then half-closes; see [ClientStreamingCall.sendAll]. */
    suspend fun sendAll(requests: Flow<Req>) = writer.sendAll(requests.map(requestCodec.encode))

    /** Half-closes the request side. Does nothing if the call already ended. */
    suspend fun closeSend() = writer.finishSend()

    suspend fun receive(): Res? = reader.receive()

    suspend fun close() {
        reader.close()
    }
}

internal class ResponseReader<T>(
    private val call: CallStream,
    private val codec: MessageCodec<T>,
    private val options: CallOptions,
    private val deadline: TimeMark?,
    private val idleTimeout: Duration? = options.streamIdleTimeout,
) {
    /** Serializes reads from the response, so [headers] and [receive] never race for frames. */
    private val readLock = Mutex()
    private val limits = ResponseLimits(options)

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
        call.close(cause)
    }

    /**
     * Reads the next response frame: from the pump when the call has one, otherwise (and after the
     * pump stopped at STATUS) from the transport. A frame the pump already queued is delivered even
     * if the call's deadline has passed since it arrived. Null at the end of the stream.
     */
    private suspend fun readResponse(): ResponseFrame? {
        val pump = call.pump
        pump?.poll()?.let { return it.unwrap() }
        return callWithDeadline(deadline, idleTimeout) {
            val item = pump?.take()
            if (item != null) item.unwrap() else call.receive()
        }
    }

    private fun ResponsePump.Item.unwrap(): ResponseFrame? {
        error?.let { throw it }
        return frame
    }

    /**
     * Reads one frame from the transport and applies the response grammar: HEADERS is allowed
     * once, and only as the first known frame.
     */
    private suspend fun nextFrame(): ResponseFrame {
        val frame =
            readResponse()
                ?: throw TrevRpcException(Status.internal("response stream ended before final status"))
        if (frame is ResponseFrame.Headers) {
            if (headersMetadata != null) {
                throw TrevRpcException(
                    Status.internal("response HEADERS must be the first frame and appear at most once"),
                )
            }
            headersMetadata = frame.metadata
            call.reportHeaders(frame.metadata)
        } else if (headersMetadata == null) {
            headersMetadata = Metadata.EMPTY
            call.reportHeaders(Metadata.EMPTY)
        }
        return frame
    }

    private suspend fun fail(error: Throwable) {
        done = true
        if (error !is CancellationException) {
            failure = error
            call.end.failure(error)
        }
        close(error)
    }

    private fun receiveMessage(frame: ResponseFrame.Message): T {
        limits.admit(frame.body.size)?.let { throw it }
        return decodeResponse(codec, frame.body)
    }

    private suspend fun receiveStatus(frame: ResponseFrame.Status): T? {
        val status = frame.status
        var trailingError: Throwable? = null
        val trailing =
            try {
                readResponse()
            } catch (error: CancellationException) {
                // The caller was cancelled while waiting for the end of the stream. The server's
                // status already decided the outcome: keep it, release the stream, and let the
                // cancellation propagate.
                terminalStatus = status
                done = true
                withContext(NonCancellable) { runCatching { close() } }
                throw error
            } catch (error: Throwable) {
                trailingError = error
                null
            }
        // The call's own deadline cut the wait short. That says nothing about the response, and the
        // server's status has already decided the outcome.
        if (trailingError?.isCallDeadline() == true) trailingError = null
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
        options.onResponseMetadata?.invoke(status.metadata)
        terminalStatus = status
        return null
    }
}

/**
 * Reads the single response message of a unary or client-streaming call, then the terminal status.
 * The stream is drained to its status first so a remote failure wins over a shape violation.
 */
internal suspend fun <T> ResponseReader<T>.receiveSingle(kind: String): T {
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
    return response
}

private fun request(
    path: String,
    kind: RpcKind,
    options: CallOptions,
    compression: CallCompression?,
    streams: CallStreams?,
    connectionMetadata: ConnectionMetadata?,
): RpcRequest =
    RpcRequest(
        method = path,
        metadata = options.metadata,
        kindValue = kind.value,
        timeoutNanos = timeoutNanos(options.timeout),
        acceptDictionaries = compression?.accept.orEmpty(),
        dictionaryQuery = compression?.query.orEmpty(),
        streamCompression = streams?.offer,
        connectionMetadata = connectionMetadata,
    )

private fun timeoutNanos(timeout: Duration?): ULong {
    if (timeout == null) return 0u
    return timeout.inWholeNanoseconds.coerceAtLeast(1).toULong()
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

/** Marks the errors [callWithDeadline] raises when the call's own deadline passes. */
private class CallDeadlineReached(
    cause: Throwable? = null,
) : Exception("RPC deadline exceeded", cause)

/** Whether this is the error [callWithDeadline] raises when the call's own deadline passes. */
internal fun Throwable.isCallDeadline(): Boolean = this is TrevRpcException && cause is CallDeadlineReached

internal suspend fun <T> callWithDeadline(
    deadline: TimeMark?,
    idleTimeout: Duration?,
    operation: suspend () -> T,
): T {
    val deadlineTimeout =
        deadline?.let {
            val remaining = -it.elapsedNow()
            if (!remaining.isPositive()) {
                throw TrevRpcException(Status.deadlineExceeded("RPC deadline exceeded"), CallDeadlineReached())
            }
            remaining
        }
    val timeout = listOfNotNull(deadlineTimeout, idleTimeout).minOrNull() ?: return operation()
    val deadlineWins = deadlineTimeout != null && deadlineTimeout <= timeout
    return try {
        withTimeout(timeout) { operation() }
    } catch (error: TimeoutCancellationException) {
        if (deadlineWins) {
            throw TrevRpcException(Status.deadlineExceeded("RPC deadline exceeded"), CallDeadlineReached(error))
        }
        throw TrevRpcException(Status.unavailable("response stream idle timeout"), error)
    } catch (error: CancellationException) {
        throw error
    }
}
