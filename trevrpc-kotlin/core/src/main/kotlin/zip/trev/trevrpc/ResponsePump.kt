package zip.trev.trevrpc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.SelectClause0
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext
import kotlin.time.TimeMark

/** Frames the pump keeps queued ahead of the caller once the request side is closed. */
internal const val PUMP_QUEUED_FRAMES = 2

/** The message bound of the pump's read-ahead when the call sets no `maxResponseMessages`. */
internal const val PUMP_FALLBACK_MAX_MESSAGES: Int = 4096

/** The byte bound of the pump's read-ahead when the call sets no `maxResponseStreamBodySize`. */
internal const val PUMP_FALLBACK_MAX_BYTES: Long = 16L * 1024 * 1024

/**
 * How a call ended, as far as its request side is concerned. The response side records the end as
 * soon as it learns of it, whoever happens to read the frame, and sends that are blocked or later
 * attempted fail with it.
 */
internal class CallEnd {
    private sealed interface Reason {
        class ByStatus(
            val status: Status,
        ) : Reason

        class ByFailure(
            val error: Throwable,
        ) : Reason

        object ByClose : Reason
    }

    private val ended = CompletableDeferred<Unit>()

    @Volatile
    private var reason: Reason? = null

    val isEnded: Boolean
        get() = ended.isCompleted

    /**
     * Whether the server's STATUS ended the call. Then the transport settles itself and the
     * response is still being read; any other end leaves the stream to be closed.
     */
    val endedByStatus: Boolean
        get() = reason is Reason.ByStatus

    /** Selectable: completes once the call has ended. */
    val onEnd: SelectClause0
        get() = ended.onJoin

    /** The server's terminal status arrived. */
    fun status(status: Status) = end(Reason.ByStatus(status))

    /** The response failed, or ended without a status. */
    fun failure(error: Throwable) = end(Reason.ByFailure(error))

    /** The caller closed the call. */
    fun closed() = end(Reason.ByClose)

    suspend fun await() = ended.join()

    /**
     * The error a send fails with now that the call has ended, or null while it is running: the
     * server's status when it is not OK, "the server completed the call" for an OK status, the
     * response error, or "closed".
     */
    fun sendError(): TrevRpcException? =
        when (val reason = reason) {
            null -> {
                null
            }

            is Reason.ByStatus -> {
                if (reason.status.isOk) {
                    TrevRpcException(Status.cancelled("the server completed the call before the request stream finished"))
                } else {
                    TrevRpcException(reason.status)
                }
            }

            is Reason.ByFailure -> {
                val error = reason.error
                when {
                    error is TrevRpcException -> TrevRpcException(error.status, error)
                    error is CancellationException -> TrevRpcException(Status.cancelled("request stream is closed"), error)
                    else -> TrevRpcException(Status.unavailable("response stream failed"), error)
                }
            }

            Reason.ByClose -> {
                TrevRpcException(Status.cancelled("request stream is closed"))
            }
        }

    private fun end(reason: Reason) {
        synchronized(this) {
            if (this.reason != null) return
            this.reason = reason
        }
        ended.complete(Unit)
    }
}

/**
 * The transport stream of one call with the state its two halves share. Both halves send and
 * receive through it, so the call's [compression] applies to every message: responses larger than
 * [maxMessageSize] are rejected before they are decompressed.
 */
internal class CallStream(
    val stream: RpcClientStream,
    private val compression: CallCompression? = null,
    private val maxMessageSize: Int = DEFAULT_MAX_FRAME_SIZE,
) {
    val end = CallEnd()

    /** Reads the response while the call uploads; null for calls that do not stream their request. */
    var pump: ResponsePump? = null

    private val closed = AtomicBoolean(false)

    /** Sends one request message, compressed when the call's negotiation allows it. */
    suspend fun send(body: ByteArray) {
        val compressed = compression?.compressRequest(body)
        if (compressed == null) stream.send(body) else stream.send(compressed, FrameType.ZSTD_MESSAGE)
    }

    /**
     * Receives the next response frame from the transport: HEADERS carry the server's answer to
     * the call's dictionary query, and compressed messages are decompressed.
     */
    suspend fun receive(): ResponseFrame? =
        when (val frame = stream.receive()) {
            is ResponseFrame.Headers -> frame.also { compression?.learn(it.dictionaries) }
            is ResponseFrame.Message -> inflateResponse(frame, compression, maxMessageSize)
            else -> frame
        }

    /** Closes the transport stream once, releasing the pump and failing blocked sends. */
    suspend fun close(cause: Throwable? = null) {
        end.closed()
        if (!closed.compareAndSet(false, true)) return
        pump?.cancel()
        stream.close(cause)
    }
}

/**
 * Applies the call's response limits to messages in the order they arrive. The response reader and
 * the pump each hold an instance, so the pump finds the frame that will make the reader fail.
 */
internal class ResponseLimits(
    private val options: CallOptions,
) {
    private var messages = 0
    private var bodySize = 0L

    /** Counts one message of [size] bytes, or returns the error the call fails with instead. */
    fun admit(size: Int): TrevRpcException? {
        val maxMessages = options.maxResponseMessages
        if (maxMessages != null && messages >= maxMessages) {
            return TrevRpcException(
                Status.resourceExhausted("response stream exceeded maximum of $maxMessages messages"),
            )
        }
        if (size > options.maxResponseBodySize) {
            return TrevRpcException(
                Status.resourceExhausted("response message is $size bytes, maximum is ${options.maxResponseBodySize}"),
            )
        }
        val maxBody = options.maxResponseStreamBodySize
        if (maxBody != null && size.toLong() > maxBody - bodySize) {
            return TrevRpcException(
                Status.resourceExhausted("response stream exceeded maximum body size of $maxBody bytes"),
            )
        }
        messages++
        bodySize += size
        return null
    }
}

/**
 * Reads the response of a call that streams its request, so a terminal status reaches a caller that
 * is only sending. A server may end a call at any moment, and the request side must then stop: the
 * pump records the end in [CallEnd] the moment STATUS (or a failure) arrives, which cancels the send
 * in flight and fails later ones, even though nobody has asked to receive yet. Frames wait in a
 * queue until the response reader takes them.
 *
 * Read-ahead is bounded, so a caller that does not receive cannot make the pump buffer without
 * limit. While the request side is open, the pump reads ahead up to the call's response limits:
 * `maxResponseMessages` messages and `maxResponseStreamBodySize` bytes (4096 messages and 16 MiB
 * when unset), plus the one frame that crosses a limit. A frame that crosses a user-set limit ends
 * the call for sends with `ResourceExhausted`, which is also what the reader fails with when it
 * reaches that frame. Reaching the bound with no limit set merely pauses reading until the caller
 * takes frames. Once the request side is closed there is nothing left to stop, so the pump falls
 * back to reading [PUMP_QUEUED_FRAMES] frames ahead and the transport's flow control paces the
 * server.
 *
 * The pump stops at STATUS, leaving the end of the stream to the reader, and ends when the stream
 * fails, when the call closes, or when [cancel] is called. It owns no resources besides its
 * coroutine, which [CallStream.close] cancels.
 */
internal class ResponsePump(
    context: CoroutineContext,
    private val call: CallStream,
    private val deadline: TimeMark?,
    options: CallOptions,
) {
    /** A queued frame, or the error that ended the stream, or (both null) its end without STATUS. */
    class Item(
        val frame: ResponseFrame?,
        val error: Throwable?,
    )

    private val job = Job()
    private val scope = CoroutineScope(context.minusKey(Job) + job)
    private val queue = Channel<Item>(Channel.UNLIMITED)
    private val wake = Channel<Unit>(Channel.CONFLATED)
    private val queuedFrames = AtomicInteger()
    private val queuedMessages = AtomicInteger()
    private val queuedBytes = AtomicLong()
    private val maxMessages = options.maxResponseMessages ?: PUMP_FALLBACK_MAX_MESSAGES
    private val maxBytes = options.maxResponseStreamBodySize ?: PUMP_FALLBACK_MAX_BYTES
    private val limits = ResponseLimits(options)

    @Volatile
    private var uploadOpen = true

    init {
        scope.launch { run() }
        // The scope owns nothing but the reading coroutine: it completes with it.
        job.complete()
    }

    /** The request side is closed: nothing is left to stop, so read-ahead shrinks. */
    fun uploadClosed() {
        uploadOpen = false
    }

    /** Takes a queued item without waiting, or returns null. */
    fun poll(): Item? = queue.tryReceive().getOrNull()?.also(::taken)

    /** Waits for the next item. Null once the pump has finished and everything it read was taken. */
    suspend fun take(): Item? = queue.receiveCatching().getOrNull()?.also(::taken)

    fun cancel() {
        scope.cancel()
    }

    private fun taken(item: Item) {
        item.frame?.let { frame ->
            queuedFrames.decrementAndGet()
            if (frame is ResponseFrame.Message) {
                queuedMessages.decrementAndGet()
                queuedBytes.addAndGet(-frame.body.size.toLong())
            }
        }
        wake.trySend(Unit)
    }

    private fun hasRoom(): Boolean =
        if (uploadOpen) {
            queuedMessages.get() <= maxMessages && queuedBytes.get() <= maxBytes
        } else {
            queuedFrames.get() < PUMP_QUEUED_FRAMES
        }

    private suspend fun run() {
        try {
            while (true) {
                while (!hasRoom()) wake.receive()
                val frame =
                    try {
                        callWithDeadline(deadline, null) { call.receive() }
                    } catch (error: CancellationException) {
                        if (!currentCoroutineContext().isActive) throw error
                        finish(error)
                        return
                    } catch (error: Throwable) {
                        finish(error)
                        return
                    }
                if (frame == null) {
                    queue.trySend(Item(null, null))
                    call.end.failure(TrevRpcException(Status.internal("response stream ended before final status")))
                    return
                }
                val violation = if (frame is ResponseFrame.Message) limits.admit(frame.body.size) else null
                push(frame)
                if (frame is ResponseFrame.Status) {
                    call.end.status(frame.status)
                    return
                }
                if (violation != null) {
                    call.end.failure(violation)
                    return
                }
            }
        } finally {
            queue.close()
        }
    }

    private fun push(frame: ResponseFrame) {
        queuedFrames.incrementAndGet()
        if (frame is ResponseFrame.Message) {
            queuedMessages.incrementAndGet()
            queuedBytes.addAndGet(frame.body.size.toLong())
        }
        queue.trySend(Item(frame, null))
    }

    private fun finish(error: Throwable) {
        // A cancellation that did not come from the pump's own scope is a response failure, not a
        // reason for the caller's coroutine to look cancelled.
        val failure =
            if (error is CancellationException) {
                TrevRpcException(Status.cancelled("response stream was cancelled"), error)
            } else {
                error
            }
        queue.trySend(Item(null, failure))
        call.end.failure(failure)
    }
}
