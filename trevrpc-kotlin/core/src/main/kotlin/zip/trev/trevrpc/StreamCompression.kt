package zip.trev.trevrpc

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

/**
 * The streamed directions of a method that may use stream compression: one zstd context per
 * direction of a call, so later messages compress against earlier ones (ZSTD_STREAM_MESSAGE).
 * Generated clients pass it for methods whose schema sets `compress_stream`, leaving out directions
 * that do not stream, that the schema marks unreliable, or whose message type has a field marked
 * `debug_redact`. A direction that does not stream is ignored.
 */
data class CompressStream(
    val requests: Boolean = false,
    val responses: Boolean = false,
) {
    val any: Boolean
        get() = requests || responses

    /** The directions that [kind] streams. */
    internal fun effective(kind: RpcKind): CompressStream =
        CompressStream(
            requests = requests && (kind == RpcKind.CLIENT_STREAMING || kind == RpcKind.BIDIRECTIONAL_STREAMING),
            responses = responses && (kind == RpcKind.SERVER_STREAMING || kind == RpcKind.BIDIRECTIONAL_STREAMING),
        )

    companion object {
        val NONE = CompressStream()
        val REQUESTS = CompressStream(requests = true)
        val RESPONSES = CompressStream(responses = true)
        val BOTH = CompressStream(requests = true, responses = true)
    }
}

/** The default [StreamCompressionBudget] of a channel. */
const val DEFAULT_STREAM_COMPRESSION_MEMORY: Long = 64L * 1024 * 1024

/**
 * The memory a channel's stream-compressed calls may hold in zstd contexts, in [bytes]; 0 turns
 * stream compression off. A call reserves a decompressing context when it offers stream
 * compression on a method whose responses stream, and a compressing context when it starts
 * compressing requests; a call that cannot reserve one falls back to per-message compression.
 * Reservations end with the call.
 */
class StreamCompressionBudget(
    val bytes: Long = DEFAULT_STREAM_COMPRESSION_MEMORY,
) {
    private val available = AtomicLong(bytes)

    init {
        require(bytes >= 0) { "stream compression memory must be non-negative" }
    }

    internal val enabled: Boolean
        get() = bytes > 0

    /** Reserves [cost] bytes, or returns null if the budget does not have them. */
    internal fun reserve(cost: Long): Reservation? {
        while (true) {
            val current = available.get()
            if (current < cost) return null
            if (available.compareAndSet(current, current - cost)) return Reservation(cost)
        }
    }

    /** Memory set aside for one context; [release] returns it once. */
    internal inner class Reservation(
        private val cost: Long,
    ) {
        private val released = AtomicBoolean(false)

        fun release() {
            if (released.compareAndSet(false, true)) available.addAndGet(cost)
        }
    }
}

/** libzstd's estimate of a decompressing context with a 64 KiB window. */
private const val STREAM_DECODER_COST = 358_184L

/**
 * libzstd's estimates of a compressing context with a 64 KiB window, by level from 1 to 22. Level 0
 * is libzstd's default, level 3; negative levels share one cheaper figure.
 */
private val STREAM_ENCODER_COSTS =
    longArrayOf(
        533_825,
        730_433,
        1_254_721,
        2_565_441,
        3_614_017,
        3_614_017,
        6_759_745,
        6_759_745,
        13_051_201,
        25_634_113,
        25_634_113,
        50_799_937,
        34_022_721,
        50_799_937,
        67_577_153,
        34_171_969,
        50_949_185,
        51_271_424,
        84_825_856,
        168_711_936,
        336_484_096,
        672_028_416,
    )
private const val STREAM_NEGATIVE_LEVEL_ENCODER_COST = 501_057L

private fun streamEncoderCost(level: Int): Long =
    when {
        level < 0 -> STREAM_NEGATIVE_LEVEL_ENCODER_COST
        level == 0 -> STREAM_ENCODER_COSTS[2]
        else -> STREAM_ENCODER_COSTS[minOf(level, STREAM_ENCODER_COSTS.size) - 1]
    }

/** Request messages go through the context from this message of the direction on. */
private const val STREAM_COMPRESSION_START = 3

/**
 * The stream compression of one call. Created only when the call offers it: [offer] is the call's
 * `stream_compression`. Responses are decompressed with one context if the response direction is
 * enabled, and requests are compressed with another from their third message once the server's
 * HEADERS answered. The sender and the receiver each use their half; [close] may come from either.
 */
internal class CallStreams private constructor(
    private val compression: Compression,
    private val budget: StreamCompressionBudget,
    private val directions: CompressStream,
    private val dictionaries: CallCompression?,
    private val decoderReservation: StreamCompressionBudget.Reservation?,
) {
    val offer = StreamCompression(STREAM_WINDOW_LOG.toUInt())

    private val lock = Any()
    private var closed = false

    /** The window of compressed requests once the server answered, or 0 until then. */
    @Volatile
    private var requestWindowLog = 0
    private var sent = 0
    private var stopped = false
    private var encoder: ZstdStreamEncoder? = null
    private var encoderReservation: StreamCompressionBudget.Reservation? = null

    private var decoder: ZstdStreamDecoder? = null
    private var inFrame = false
    private var failed = false

    /** Records the server's answer from the call's HEADERS. */
    fun learn(answer: StreamCompression?) {
        val windowLog = answer?.maxWindowLog?.toLong() ?: return
        if (directions.requests && windowLog >= MIN_STREAM_WINDOW_LOG) {
            requestWindowLog = minOf(windowLog, STREAM_WINDOW_LOG.toLong()).toInt()
        }
    }

    /**
     * The ZSTD_STREAM_MESSAGE payload of a request message, or null if it travels in another
     * frame: before the third message, before the answer, or when the budget is spent.
     */
    fun compressRequest(message: ByteArray): ByteArray? =
        synchronized(lock) {
            sent++
            if (closed) return null
            if (encoder == null && !stopped && sent >= STREAM_COMPRESSION_START && requestWindowLog > 0) start()
            val encoder = encoder ?: return null
            val chunk =
                try {
                    encoder.compress(message)
                } catch (error: RuntimeException) {
                    // The context is unusable, and the server's is now ahead of it.
                    throw TrevRpcException(Status.internal("stream compression failed"), error)
                }
            QuicVarint.encode(message.size.toLong()) + chunk
        }

    /** Creates the request context, starting from the request dictionary the server confirmed. */
    private fun start() {
        val reservation = budget.reserve(streamEncoderCost(compression.level))
        if (reservation == null) {
            stopped = true
            return
        }
        try {
            encoder = ZstdStreamEncoder(compression.level, requestWindowLog, dictionaries?.confirmedRequest?.codec)
            encoderReservation = reservation
        } catch (_: RuntimeException) {
            reservation.release()
            stopped = true
        }
    }

    /**
     * Decompresses a response read from a ZSTD_STREAM_MESSAGE frame. A message above [maxSize] is
     * `ResourceExhausted`, found before decompressing; every other fault is `Internal`, after which
     * the direction cannot continue.
     */
    fun inflate(
        message: ResponseFrame.Message,
        maxSize: Int,
    ): ResponseFrame.Message {
        val size = checkNotNull(message.streamSize)
        if (size > maxSize) {
            throw TrevRpcException(Status.resourceExhausted("response message is $size bytes, maximum is $maxSize"))
        }
        synchronized(lock) {
            if (!directions.responses || decoderReservation == null || closed || failed) {
                throw invalidStream("stream-compressed message where stream compression is not enabled")
            }
            try {
                return ResponseFrame.Message(decode(message.body, size))
            } catch (error: TrevRpcException) {
                failed = true
                throw error
            }
        }
    }

    private fun decode(
        chunk: ByteArray,
        size: Int,
    ): ByteArray {
        val info = parseZstdStreamChunk(chunk, atFrameStart = !inFrame) ?: throw invalidStream("invalid ZSTD_STREAM_MESSAGE payload")
        val decoder = decoder ?: ZstdStreamDecoder().also { decoder = it }
        if (info.startsFrame) {
            val dictionary =
                if (info.dictionaryId == 0u) {
                    null
                } else {
                    dictionaries?.acceptedResponse?.takeIf { it.info.id == info.dictionaryId }
                        ?: throw invalidStream("stream-compressed message uses dictionary ${info.dictionaryId}, which is not accepted")
                }
            decoder.startFrame(dictionary?.codec)
        }
        val body = decoder.decompress(chunk, size) ?: throw invalidStream("invalid ZSTD_STREAM_MESSAGE payload")
        if (chunk.isNotEmpty()) inFrame = !info.endsFrame
        return body
    }

    /** Ends the call's stream compression: its contexts are freed and its memory returned. */
    fun close() {
        synchronized(lock) {
            if (closed) return
            closed = true
            encoder?.close()
            encoder = null
            decoder?.close()
            decoder = null
        }
        encoderReservation?.release()
        decoderReservation?.release()
    }

    companion object {
        /**
         * The stream compression of a call to a method whose [directions] may use it, or null if
         * the call does not offer it: no direction applies, the budget is off, or a call whose
         * responses stream cannot reserve their context.
         */
        fun open(
            compression: Compression,
            budget: StreamCompressionBudget?,
            directions: CompressStream,
            dictionaries: CallCompression?,
        ): CallStreams? {
            if (budget == null || !budget.enabled || !directions.any) return null
            val reservation =
                if (directions.responses) budget.reserve(STREAM_DECODER_COST) ?: return null else null
            return CallStreams(compression, budget, directions, dictionaries, reservation)
        }
    }
}

private fun invalidStream(message: String) = TrevRpcException(Status.internal(message))
