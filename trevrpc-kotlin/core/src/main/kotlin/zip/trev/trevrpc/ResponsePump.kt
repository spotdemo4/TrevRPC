package zip.trev.trevrpc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.selects.SelectClause0
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
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
 * receive through it, so the call's [compression] and stream compression apply to every message:
 * responses larger than [maxMessageSize] are rejected before they are decompressed.
 *
 * An optimistic call ([replay] set) keeps its requests until its first response frame, which is
 * the server's ANSWER accepting it or a STATUS. If that frame is the server's refusal, the call
 * switches to a new stream that is not optimistic and resends them there, in order, before any later send; a write to the refused stream that fails
 * meanwhile was resent, so it does not fail the send. The caller never sees the refused stream.
 */
internal class CallStream(
    stream: RpcClientStream,
    private val compression: CallCompression? = null,
    streams: CallStreams? = null,
    private val maxMessageSize: Int = DEFAULT_MAX_FRAME_SIZE,
    private val onHeaders: ((Metadata) -> Unit)? = null,
    private val replay: OptimisticReplay? = null,
) {
    val end = CallEnd()

    /** The call's current transport stream: the replayed one after a refusal. */
    @Volatile
    var stream: RpcClientStream = stream
        private set

    @Volatile
    private var streams: CallStreams? = streams

    /** Orders sends and half-closes with a switch to the replayed stream. */
    private val sendLock = Mutex()

    /** Counts switches of [stream]; a write to an earlier stream that fails was resent. */
    @Volatile
    private var generation = 0

    /** Guarded by [sendLock]: the caller half-closed, so the replayed stream is half-closed too. */
    private var sendFinished = false

    /** Completes once the replayed stream has its resent requests; until then sends wait. */
    @Volatile
    private var resent: CompletableDeferred<Unit>? = null
    private var resendJob: Job? = null

    /** Whether the first response frame has yet to arrive on an optimistic call. */
    private var awaitingFirstFrame = replay != null

    /** Whether an ANSWER may still arrive: only before every other known frame, and once. */
    private var answerAllowed = true

    /**
     * Completes once an optimistic call has handled its first response frame, switching to the
     * replayed stream if it was a refusal, or once the call closes. A write that fails before then
     * may have failed because the server refused the call, so it waits for this to tell.
     */
    private val firstFrameHandled = CompletableDeferred<Unit>()

    private val headersReported = AtomicBoolean(false)

    /**
     * Hands the response headers to the call's `onResponseHeaders`, once: from whichever reads them
     * first, the pump while the call uploads or the caller.
     */
    fun reportHeaders(metadata: Metadata) {
        if (headersReported.compareAndSet(false, true)) onHeaders?.invoke(metadata)
    }

    /** Reads the response while the call uploads; null for calls that do not stream their request. */
    var pump: ResponsePump? = null

    private val closed = AtomicBoolean(false)

    private class Target(
        val stream: RpcClientStream,
        val streams: CallStreams?,
        val generation: Int,
    )

    /**
     * Sends one request message: stream-compressed once the call's stream compression has started,
     * otherwise compressed with a dictionary when the call's negotiation allows it. An optimistic
     * call keeps a copy first, waiting for its first response frame when it has no room.
     */
    suspend fun send(body: ByteArray) {
        val copies = replay?.copies
        while (true) {
            val target =
                sendLock.withLock {
                    if (resent?.isCompleted == false || (copies != null && !copies.tryKeep(body))) {
                        null
                    } else {
                        Target(stream, streams, generation)
                    }
                }
            if (target == null) {
                resent?.takeUnless { it.isCompleted }?.join() ?: copies?.awaitSettled()
                continue
            }
            onTarget(target) { write(target.stream, target.streams, body) }
            return
        }
    }

    /** Half-closes the request side, on the replayed stream if the call was refused. */
    suspend fun finishSend() {
        while (true) {
            val target =
                sendLock.withLock {
                    if (resent?.isCompleted == false) {
                        null
                    } else {
                        sendFinished = true
                        Target(stream, streams, generation)
                    }
                }
            if (target == null) {
                resent?.join()
                continue
            }
            onTarget(target) { target.stream.finishSend() }
            return
        }
    }

    /**
     * Runs [operation] on [target]; a failure after the call switched streams was resent. A refused
     * server stops reading, so a write can fail on the refused stream before the response pump has
     * read the refusal: an optimistic call's failed write waits for its first frame to be handled
     * before deciding, or the caller would see the refusal instead of the replayed call.
     */
    private suspend fun onTarget(
        target: Target,
        operation: suspend () -> Unit,
    ) {
        try {
            operation()
        } catch (error: Throwable) {
            if (error is CancellationException && !currentCoroutineContext().isActive) throw error
            if (replay != null && target.generation == generation && !closed.get()) {
                withTimeoutOrNull(EXPLANATION_TIMEOUT) { firstFrameHandled.await() }
            }
            if (target.generation == generation || closed.get()) throw error
        }
    }

    private suspend fun write(
        stream: RpcClientStream,
        streams: CallStreams?,
        body: ByteArray,
    ) {
        streams?.compressRequest(body)?.let { payload ->
            stream.send(payload, FrameType.ZSTD_STREAM_MESSAGE)
            return
        }
        val compressed = compression?.compressRequest(body)
        if (compressed == null) stream.send(body) else stream.send(compressed, FrameType.ZSTD_MESSAGE)
    }

    /**
     * Receives the next response frame from the transport. The server's ANSWER is consumed here:
     * it carries the answers to the call's dictionary query and stream compression offer, and
     * never reaches the caller. Compressed messages are decompressed. A refusal of an optimistic
     * call is replaced by the replayed call's frames.
     */
    suspend fun receive(): ResponseFrame? {
        while (true) {
            val frame =
                try {
                    stream.receive()
                } catch (error: Throwable) {
                    firstFrameHandled.complete(Unit)
                    throw error
                }
            val replay = replay
            if (replay != null && awaitingFirstFrame) {
                awaitingFirstFrame = false
                val refused = frame is ResponseFrame.Status && frame.refusedStreamCompression
                try {
                    if (refused) {
                        replay(replay)
                    } else {
                        replay.copies.settle()
                        // A server accepts an optimistic call with its ANSWER, before anything else.
                        val accepted =
                            when (frame) {
                                null, is ResponseFrame.Status -> true
                                is ResponseFrame.Answer -> frame.streamCompression
                                else -> false
                            }
                        if (!accepted) throw TrevRpcException(Status.internal("the server did not accept an optimistic call first"))
                    }
                } finally {
                    // Before the replayed stream is read, which may wait for its response.
                    firstFrameHandled.complete(Unit)
                }
                if (refused) continue
            }
            if (frame is ResponseFrame.Answer) {
                if (!answerAllowed) {
                    throw TrevRpcException(Status.internal("response ANSWER must be the first frame and appear at most once"))
                }
                answerAllowed = false
                compression?.learn(frame.dictionaries)
                streams?.learn(frame.streamCompression)
                continue
            }
            answerAllowed = false
            return when (frame) {
                is ResponseFrame.Headers -> {
                    frame.also { reportHeaders(it.metadata) }
                }

                is ResponseFrame.Message -> {
                    if (frame.streamSize == null) {
                        inflateResponse(frame, compression, maxMessageSize)
                    } else {
                        streams?.inflate(frame, maxMessageSize)
                            ?: throw TrevRpcException(Status.internal("stream-compressed message where stream compression is not enabled"))
                    }
                }

                else -> {
                    frame
                }
            }
        }
    }

    /**
     * Switches a refused call to a new stream that is not optimistic, and resends its requests
     * there in a coroutine of its own, so the response is read meanwhile. Sends wait for the
     * resend; the refused stream is closed.
     */
    private suspend fun replay(replay: OptimisticReplay) {
        replay.budget.optimistic = false
        val done = CompletableDeferred<Unit>()
        sendLock.withLock {
            if (closed.get()) throw TrevRpcException(Status.cancelled("request stream is closed"))
            val copies = replay.copies.settle()
            generation++
            resent = done
            val refused = stream
            withContext(NonCancellable) {
                runCatching { refused.close(null) }
                streams?.close()
            }
            streams = null
            val replayedStreams = replay.reopen()
            val replayed =
                try {
                    callWithDeadline(replay.deadline, null) {
                        replay.transport.openStream(replay.replayedRequest(replayedStreams))
                    }
                } catch (error: Throwable) {
                    replayedStreams?.close()
                    done.complete(Unit)
                    throw error
                }
            stream = replayed
            streams = replayedStreams
            if (closed.get()) {
                // The call closed while the replayed stream opened, and may have closed the refused one.
                withContext(NonCancellable) {
                    runCatching { replayed.close(null) }
                    replayedStreams?.close()
                }
                done.complete(Unit)
                throw TrevRpcException(Status.cancelled("request stream is closed"))
            }
            val finish = sendFinished
            resendJob =
                CoroutineScope(replay.context.minusKey(Job)).launch {
                    try {
                        for (copy in copies) write(replayed, replayedStreams, copy)
                        if (finish) replayed.finishSend()
                    } catch (error: CancellationException) {
                        throw error
                    } catch (error: Throwable) {
                        resendFailed(error)
                    } finally {
                        done.complete(Unit)
                    }
                }
        }
    }

    /**
     * A resend failed. Usually the response explains why (the server ended the call); if it does
     * not soon, the failure ends the call.
     */
    private suspend fun resendFailed(error: Throwable) {
        withTimeoutOrNull(EXPLANATION_TIMEOUT) { end.await() }
        if (end.isEnded) return
        end.failure(error)
        close(error)
    }

    /** Closes the transport stream once, releasing the pump, failing blocked sends, and freeing compression contexts. */
    suspend fun close(cause: Throwable? = null) {
        end.closed()
        firstFrameHandled.complete(Unit)
        if (!closed.compareAndSet(false, true)) return
        try {
            pump?.cancel()
            resendJob?.cancel()
            stream.close(cause)
        } finally {
            replay?.copies?.settle()
            streams?.close()
        }
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
