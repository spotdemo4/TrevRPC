package zip.trev.trevrpc.netty

import io.netty.buffer.ByteBuf

private const val WEBTRANSPORT_BIDI_STREAM_TYPE = 0x41L

sealed interface WebTransportPreludeResult {
    data object NeedMoreData : WebTransportPreludeResult

    data class Accepted(
        val sessionId: Long,
        val remaining: ByteArray,
    ) : WebTransportPreludeResult

    data class Rejected(
        val reason: String,
    ) : WebTransportPreludeResult
}

/** Incrementally parses the RFC 9297 stream type and CONNECT-stream/session ID prelude. */
class WebTransportPreludeDecoder(
    private val expectedSessionId: Long,
) {
    private val type = QuicVarintDecoder()
    private val session = QuicVarintDecoder()
    private var state = State.TYPE
    private var terminal: WebTransportPreludeResult? = null

    init {
        require(expectedSessionId >= 0) { "expectedSessionId must be non-negative" }
    }

    fun feed(input: ByteBuf): WebTransportPreludeResult {
        terminal?.let { return it }
        while (input.isReadable) {
            when (state) {
                State.TYPE -> {
                    when (val result = type.read(input)) {
                        QuicVarintResult.NeedMoreData -> {
                            return WebTransportPreludeResult.NeedMoreData
                        }

                        is QuicVarintResult.Malformed -> {
                            return reject(result.reason)
                        }

                        is QuicVarintResult.Value -> {
                            if (result.value != WEBTRANSPORT_BIDI_STREAM_TYPE) {
                                return reject("unknown WebTransport stream type ${result.value}")
                            }
                            state = State.SESSION
                        }
                    }
                }

                State.SESSION -> {
                    when (val result = session.read(input)) {
                        QuicVarintResult.NeedMoreData -> {
                            return WebTransportPreludeResult.NeedMoreData
                        }

                        is QuicVarintResult.Malformed -> {
                            return reject(result.reason)
                        }

                        is QuicVarintResult.Value -> {
                            if (result.value != expectedSessionId) {
                                return reject(
                                    "unknown WebTransport session ${result.value}; expected $expectedSessionId",
                                )
                            }
                            val accepted =
                                WebTransportPreludeResult.Accepted(
                                    result.value,
                                    input.copyReadableBytes(),
                                )
                            terminal = accepted
                            return accepted
                        }
                    }
                }
            }
        }
        return WebTransportPreludeResult.NeedMoreData
    }

    fun finish(): WebTransportPreludeResult = terminal ?: reject("WebTransport stream ended before its prelude was complete")

    private fun reject(reason: String): WebTransportPreludeResult.Rejected =
        WebTransportPreludeResult.Rejected(reason).also { terminal = it }

    private enum class State {
        TYPE,
        SESSION,
    }
}

private sealed interface QuicVarintResult {
    data object NeedMoreData : QuicVarintResult

    data class Value(
        val value: Long,
    ) : QuicVarintResult

    data class Malformed(
        val reason: String,
    ) : QuicVarintResult
}

private class QuicVarintDecoder {
    private val bytes = ByteArray(8)
    private var expected = 0
    private var read = 0

    fun read(input: ByteBuf): QuicVarintResult {
        if (expected == 0 && input.isReadable) {
            val first = input.readUnsignedByte().toInt()
            bytes[0] = first.toByte()
            read = 1
            expected = 1 shl (first ushr 6)
        }
        while (read < expected && input.isReadable) bytes[read++] = input.readByte()
        if (read < expected) return QuicVarintResult.NeedMoreData
        var value = (bytes[0].toInt() and 0x3f).toLong()
        for (index in 1 until expected) value = (value shl 8) or (bytes[index].toInt() and 0xff).toLong()
        return QuicVarintResult.Value(value)
    }
}
