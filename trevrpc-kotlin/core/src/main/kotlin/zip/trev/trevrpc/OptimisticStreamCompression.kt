package zip.trev.trevrpc

import kotlinx.coroutines.CompletableDeferred
import kotlin.coroutines.CoroutineContext
import kotlin.time.TimeMark

/** The most request bytes an optimistic call keeps for its replay. */
internal const val OPTIMISTIC_COPY_CAP: Long = 1L shl 20

/**
 * What an optimistic call needs to replay itself when the server refuses it: the [transport] and
 * the [request] it opened with, its [deadline], and [reopen], which sets up the stream compression
 * of the replayed call, which is never optimistic. The replay resends the [copies] in a coroutine
 * of [context], so the response keeps being read meanwhile.
 */
internal class OptimisticReplay(
    val transport: RpcTransport,
    val request: RpcRequest,
    val deadline: TimeMark?,
    val context: CoroutineContext,
    val budget: StreamCompressionBudget,
    val reopen: () -> CallStreams?,
) {
    val copies = RequestCopies(budget)

    /** The REQUEST of the replayed call: not optimistic, with [streams]' offer and the time left. */
    fun replayedRequest(streams: CallStreams?): RpcRequest =
        request.copy(
            streamCompression = streams?.offer ?: StreamCompression.NONE,
            timeoutNanos = deadline?.let { (-it.elapsedNow()).inWholeNanoseconds.coerceAtLeast(1).toULong() } ?: 0uL,
        )
}

/**
 * The requests an optimistic call has sent, kept until the server accepts the call or refuses it
 * ([settle]). They are at most [OPTIMISTIC_COPY_CAP] bytes, reserved in [budget]; a request
 * without room waits for the call to settle.
 */
internal class RequestCopies(
    private val budget: StreamCompressionBudget,
) {
    private val lock = Any()
    private val copies = ArrayList<ByteArray>()
    private val reservations = ArrayList<StreamCompressionBudget.Reservation>()
    private var bytes = 0L
    private val settled = CompletableDeferred<Unit>()

    val isSettled: Boolean
        get() = settled.isCompleted

    /**
     * Keeps [body], or returns false if it must wait for [awaitSettled] first: the cap or the
     * budget has no room. Once the call has settled nothing is kept and this returns true.
     */
    fun tryKeep(body: ByteArray): Boolean =
        synchronized(lock) {
            if (settled.isCompleted) return true
            val size = body.size.toLong()
            if (bytes + size > OPTIMISTIC_COPY_CAP) return false
            reservations += budget.reserve(size) ?: return false
            copies += body
            bytes += size
            true
        }

    suspend fun awaitSettled() = settled.join()

    /** Stops keeping requests and returns their memory; returns the requests kept, in order. */
    fun settle(): List<ByteArray> =
        synchronized(lock) {
            settled.complete(Unit)
            reservations.forEach { it.release() }
            reservations.clear()
            bytes = 0
            copies.toList().also { copies.clear() }
        }
}
