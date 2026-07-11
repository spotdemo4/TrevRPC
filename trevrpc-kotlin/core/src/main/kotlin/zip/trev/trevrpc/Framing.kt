package zip.trev.trevrpc

private const val FRAME_HEADER_SIZE = 4

fun frame(
    body: ByteArray,
    maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE,
): ByteArray {
    checkFrameSize(body.size, maxFrameSize)
    return ByteArray(FRAME_HEADER_SIZE + body.size).also { framed ->
        framed[0] = (body.size ushr 24).toByte()
        framed[1] = (body.size ushr 16).toByte()
        framed[2] = (body.size ushr 8).toByte()
        framed[3] = body.size.toByte()
        body.copyInto(framed, FRAME_HEADER_SIZE)
    }
}

class FrameDecoder(
    val maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE,
) {
    private val header = ByteArray(FRAME_HEADER_SIZE)
    private var headerBytes = 0
    private var body: ByteArray? = null
    private var bodyBytes = 0
    private var failure: TrevRpcException? = null

    init {
        require(maxFrameSize >= 0) { "maxFrameSize must be non-negative" }
    }

    fun feed(chunk: ByteArray): List<ByteArray> {
        failure?.let { throw it }
        val frames = mutableListOf<ByteArray>()
        var offset = 0
        try {
            while (offset < chunk.size || readyEmptyBody()) {
                if (body == null) {
                    val copied = minOf(FRAME_HEADER_SIZE - headerBytes, chunk.size - offset)
                    if (copied > 0) {
                        chunk.copyInto(header, headerBytes, offset, offset + copied)
                        headerBytes += copied
                        offset += copied
                    }
                    if (headerBytes < FRAME_HEADER_SIZE) break
                    val declared =
                        ((header[0].toInt() and 0xff) shl 24) or
                            ((header[1].toInt() and 0xff) shl 16) or
                            ((header[2].toInt() and 0xff) shl 8) or
                            (header[3].toInt() and 0xff)
                    val unsignedDeclared = declared.toUInt().toLong()
                    if (unsignedDeclared > maxFrameSize.toLong()) {
                        throw TrevRpcException(
                            Status.resourceExhausted(
                                "declared frame body is $unsignedDeclared bytes, maximum is $maxFrameSize",
                            ),
                        )
                    }
                    body = ByteArray(unsignedDeclared.toInt())
                }

                val currentBody = checkNotNull(body)
                val copied = minOf(currentBody.size - bodyBytes, chunk.size - offset)
                if (copied > 0) {
                    chunk.copyInto(currentBody, bodyBytes, offset, offset + copied)
                    bodyBytes += copied
                    offset += copied
                }
                if (bodyBytes == currentBody.size) {
                    frames += currentBody
                    headerBytes = 0
                    body = null
                    bodyBytes = 0
                } else {
                    break
                }
            }
        } catch (error: TrevRpcException) {
            failure = error
            throw error
        }
        return frames
    }

    fun finish() {
        failure?.let { throw it }
        if (headerBytes != 0 || body != null) {
            throw TrevRpcException(Status.internal("framed stream ended with a partial frame"))
        }
    }

    private fun readyEmptyBody(): Boolean = body?.isEmpty() == true
}

private fun checkFrameSize(
    size: Int,
    maxFrameSize: Int,
) {
    require(maxFrameSize >= 0) { "maxFrameSize must be non-negative" }
    if (size > maxFrameSize) {
        throw TrevRpcException(
            Status.resourceExhausted("frame body is $size bytes, maximum is $maxFrameSize"),
        )
    }
}
