package zip.trev.trevrpc

/** Frame type numbers. Every other type is unknown and skipped by receivers. */
object FrameType {
    const val MESSAGE: Long = 0
    const val REQUEST: Long = 1
    const val HEADERS: Long = 2
    const val STATUS: Long = 3

    fun isKnown(type: Long): Boolean = type in MESSAGE..STATUS
}

/** A known frame read from the wire. [type] is one of [FrameType]. */
class Frame(
    val type: Long,
    val payload: ByteArray,
)

/**
 * Encodes one frame: type and length as QUIC varints, then the payload. The payload is limited by
 * [maxFrameSize] before anything is allocated.
 */
fun encodeFrame(
    type: Long,
    payload: ByteArray,
    maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE,
): ByteArray {
    checkFrameSize(payload.size, maxFrameSize)
    return ByteArray(frameSize(type, payload.size)).also { writeFrame(it, 0, type, payload) }
}

/** Encodes a REQUEST frame, followed by the first MESSAGE frame when [firstMessage] is present. */
fun encodeRequestFrames(
    request: RpcRequest,
    firstMessage: ByteArray?,
    maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE,
): ByteArray {
    val requestPayload = WireCodec.encode(request)
    checkFrameSize(requestPayload.size, maxFrameSize)
    if (firstMessage == null) return encodeFrame(FrameType.REQUEST, requestPayload, maxFrameSize)
    checkFrameSize(firstMessage.size, maxFrameSize)
    val requestSize = frameSize(FrameType.REQUEST, requestPayload.size)
    return ByteArray(requestSize + frameSize(FrameType.MESSAGE, firstMessage.size)).also { encoded ->
        val offset = writeFrame(encoded, 0, FrameType.REQUEST, requestPayload)
        writeFrame(encoded, offset, FrameType.MESSAGE, firstMessage)
    }
}

private fun frameSize(
    type: Long,
    payloadSize: Int,
): Int = QuicVarint.size(type) + QuicVarint.size(payloadSize.toLong()) + payloadSize

private fun writeFrame(
    output: ByteArray,
    initialOffset: Int,
    type: Long,
    payload: ByteArray,
): Int {
    var offset = QuicVarint.write(output, initialOffset, type)
    offset = QuicVarint.write(output, offset, payload.size.toLong())
    payload.copyInto(output, offset)
    return offset + payload.size
}

/**
 * Incremental frame reader. Chunk boundaries are arbitrary: the frame header is parsed across
 * chunks, the declared length is checked against [maxFrameSize] before any payload is allocated,
 * and frames of unknown type are discarded without being buffered.
 */
class FrameDecoder(
    val maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE,
) {
    private enum class Phase { TYPE, LENGTH, PAYLOAD, SKIP }

    private val varint = ByteArray(8)
    private var varintBytes = 0
    private var varintLength = 0
    private var phase = Phase.TYPE
    private var type = 0L
    private var payload = EMPTY
    private var payloadBytes = 0
    private var skipRemaining = 0L
    private var failure: TrevRpcException? = null

    init {
        require(maxFrameSize >= 0) { "maxFrameSize must be non-negative" }
    }

    fun feed(chunk: ByteArray): List<Frame> {
        failure?.let { throw it }
        val frames = mutableListOf<Frame>()
        var offset = 0
        try {
            while (offset < chunk.size) {
                when (phase) {
                    Phase.TYPE -> {
                        offset = readVarint(chunk, offset)
                        if (varintLength != 0 && varintBytes == varintLength) {
                            type = takeVarint()
                            phase = Phase.LENGTH
                        }
                    }

                    Phase.LENGTH -> {
                        offset = readVarint(chunk, offset)
                        if (varintLength != 0 && varintBytes == varintLength) {
                            startPayload(takeVarint(), frames)
                        }
                    }

                    Phase.PAYLOAD -> {
                        val copied = minOf(payload.size - payloadBytes, chunk.size - offset)
                        chunk.copyInto(payload, payloadBytes, offset, offset + copied)
                        payloadBytes += copied
                        offset += copied
                        if (payloadBytes == payload.size) {
                            frames += Frame(type, payload)
                            resetFrame()
                        }
                    }

                    Phase.SKIP -> {
                        val skipped = minOf(skipRemaining, (chunk.size - offset).toLong()).toInt()
                        offset += skipped
                        skipRemaining -= skipped
                        if (skipRemaining == 0L) resetFrame()
                    }
                }
            }
        } catch (error: TrevRpcException) {
            failure = error
            throw error
        }
        return frames
    }

    /** Call at transport end of stream; throws when the stream ended inside a frame. */
    fun finish() {
        failure?.let { throw it }
        if (phase != Phase.TYPE || varintBytes != 0) {
            throw TrevRpcException(Status.internal("framed stream ended with a partial frame"))
        }
    }

    private fun startPayload(
        declared: Long,
        frames: MutableList<Frame>,
    ) {
        if (declared > maxFrameSize.toLong()) {
            throw TrevRpcException(
                Status.resourceExhausted("declared frame payload is $declared bytes, maximum is $maxFrameSize"),
            )
        }
        val known = FrameType.isKnown(type)
        if (declared == 0L) {
            if (known) frames += Frame(type, EMPTY)
            resetFrame()
        } else if (known) {
            payload = ByteArray(declared.toInt())
            payloadBytes = 0
            phase = Phase.PAYLOAD
        } else {
            skipRemaining = declared
            phase = Phase.SKIP
        }
    }

    /** Copies header bytes into [varint]; returns the new chunk offset. */
    private fun readVarint(
        chunk: ByteArray,
        initialOffset: Int,
    ): Int {
        var offset = initialOffset
        if (varintLength == 0) {
            varintLength = QuicVarint.encodedLength(chunk[offset])
        }
        val copied = minOf(varintLength - varintBytes, chunk.size - offset)
        chunk.copyInto(varint, varintBytes, offset, offset + copied)
        varintBytes += copied
        offset += copied
        return offset
    }

    private fun takeVarint(): Long {
        val value = QuicVarint.decode(varint)
        varintBytes = 0
        varintLength = 0
        return value
    }

    private fun resetFrame() {
        phase = Phase.TYPE
        payload = EMPTY
        payloadBytes = 0
        skipRemaining = 0
    }

    private companion object {
        val EMPTY = ByteArray(0)
    }
}

private fun checkFrameSize(
    size: Int,
    maxFrameSize: Int,
) {
    require(maxFrameSize >= 0) { "maxFrameSize must be non-negative" }
    if (size > maxFrameSize) {
        throw TrevRpcException(
            Status.resourceExhausted("frame payload is $size bytes, maximum is $maxFrameSize"),
        )
    }
}
