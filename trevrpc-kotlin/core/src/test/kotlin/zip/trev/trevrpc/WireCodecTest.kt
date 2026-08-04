package zip.trev.trevrpc

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class WireCodecTest {
    @Test
    fun `plain message frame uses exact compact encoding across varint lengths`() {
        listOf(0, 1, 127, 128, 16_384).forEach { size ->
            val body = ByteArray(size) { index -> index.toByte() }
            val encoded = WireCodec.encodeMessageFrame(body)
            assertArrayEquals(encoded, WireCodec.encode(RpcStreamFrame.message(body)))
            assertArrayEquals(body, WireCodec.decodeStreamFrame(encoded).body)
            if (size > 0) assertEquals(0x22, encoded[0].toInt())
        }

        listOf(
            byteArrayOf(0x22, 0x80.toByte()),
            byteArrayOf(0x22, 0x80.toByte(), 0x80.toByte(), 0x80.toByte(), 0x80.toByte(), 0x10),
        ).forEach { malformed ->
            assertEquals(
                Code.INVALID_ARGUMENT,
                assertThrows(TrevRpcException::class.java) { WireCodec.decodeStreamFrame(malformed) }.status.code,
            )
        }
    }

    @Test
    fun `all shared golden vectors encode decode and frame byte for byte`() {
        val vectors = goldenVectors()
        val metadata = Metadata.of("authorization" to "ok".encodeToByteArray())
        val messages =
            mapOf(
                "rpc_request.unary" to
                    WireMessage(
                        RpcRequest("svc", "m", "hi".encodeToByteArray()),
                        WireCodec::encode,
                        WireCodec::decodeRequest,
                    ),
                "rpc_request.timeout" to
                    WireMessage(
                        RpcRequest("svc", "m", "hi".encodeToByteArray(), timeoutNanos = 123_456u),
                        WireCodec::encode,
                        WireCodec::decodeRequest,
                    ),
                "rpc_request.metadata" to
                    WireMessage(
                        RpcRequest("svc", "m", "hi".encodeToByteArray(), metadata),
                        WireCodec::encode,
                        WireCodec::decodeRequest,
                    ),
                "rpc_stream_frame.message" to
                    WireMessage(
                        RpcStreamFrame.message("hi".encodeToByteArray()),
                        WireCodec::encode,
                        WireCodec::decodeStreamFrame,
                    ),
                "rpc_stream_frame.status" to
                    WireMessage(
                        RpcStreamFrame.status(Status.unavailable("down")),
                        WireCodec::encode,
                        WireCodec::decodeStreamFrame,
                    ),
                "rpc_response.ok_body" to
                    WireMessage(
                        RpcResponse.ok("hi".encodeToByteArray()),
                        WireCodec::encode,
                        WireCodec::decodeResponse,
                    ),
                "rpc_response.unavailable" to
                    WireMessage(
                        RpcResponse.fromStatus(Status.unavailable("down")),
                        WireCodec::encode,
                        WireCodec::decodeResponse,
                    ),
            )

        assertEquals(messages.keys.flatMap { listOf("$it.body", "$it.frame") }.toSet(), vectors.keys)
        messages.forEach { (name, message) -> message.assertMatches(name, vectors) }
    }

    @Test
    fun `codec rejects malformed protobuf varints lengths wire types and utf8`() {
        val malformed =
            listOf(
                byteArrayOf(0x80.toByte()),
                ByteArray(10) { 0x80.toByte() },
                byteArrayOf(0x0a, 0x02, 0x01),
                byteArrayOf(0x0f),
                byteArrayOf(0x0a, 0x01, 0xff.toByte()),
            )
        malformed.forEach { bytes ->
            assertEquals(
                Code.INVALID_ARGUMENT,
                assertThrows(TrevRpcException::class.java) { WireCodec.decodeRequest(bytes) }.status.code,
            )
        }
    }

    @Test
    fun `metadata validation remains distinguishable from malformed metadata utf8`() {
        val invalidMetadata =
            assertThrows(TrevRpcException::class.java) {
                WireCodec.decodeResponse(
                    byteArrayOf(
                        0x22,
                        0x13,
                        0x0a,
                        0x0d,
                        *"Authorization".encodeToByteArray(),
                        0x12,
                        0x02,
                        0x6f,
                        0x6b,
                    ),
                )
            }
        assertEquals(Code.INVALID_ARGUMENT, invalidMetadata.status.code)
        assertTrue(invalidMetadata.status.message.startsWith("invalid metadata:"))

        val malformedKey =
            assertThrows(TrevRpcException::class.java) {
                WireCodec.decodeResponse(byteArrayOf(0x22, 0x03, 0x0a, 0x01, 0xff.toByte()))
            }
        assertEquals(Code.INVALID_ARGUMENT, malformedKey.status.code)
        assertFalse(malformedKey.status.message.startsWith("invalid metadata:"))
    }

    @Test
    fun `codec skips supported unknown fields and preserves unknown enums`() {
        val request = WireCodec.decodeRequest(byteArrayOf(0x28, 0x63, 0x30, 0x01, 0x40, 0x07))
        assertEquals(99, request.kindValue)
        assertNull(request.kind)
        assertEquals(WIRE_VERSION, request.version)
        assertEquals(Code.UNKNOWN, Code.fromValue(999u))
        assertNull(RpcStreamFrameKind.fromValue(99))

        val negativeKind = RpcRequest("svc", "method", kindValue = -1)
        assertEquals(-1, WireCodec.decodeRequest(WireCodec.encode(negativeKind)).kindValue)
    }

    private class WireMessage<T>(
        private val message: T,
        private val encode: (T) -> ByteArray,
        private val decode: (ByteArray) -> T,
    ) {
        fun assertMatches(
            name: String,
            vectors: Map<String, ByteArray>,
        ) {
            val body = encode(message)
            val framed = frame(body)
            assertArrayEquals(vectors.getValue("$name.body"), body)
            assertArrayEquals(vectors.getValue("$name.frame"), framed)
            assertArrayEquals(body, encode(decode(body)))
            assertArrayEquals(body, FrameDecoder().feed(framed).single())
        }
    }

    private fun goldenVectors(): Map<String, ByteArray> =
        checkNotNull(javaClass.classLoader.getResourceAsStream("wire-golden-vectors.txt"))
            .bufferedReader()
            .useLines { lines ->
                lines
                    .map(String::trim)
                    .filter { it.isNotEmpty() && !it.startsWith('#') }
                    .associate { line ->
                        val (name, value) = line.split('=', limit = 2).map(String::trim)
                        name to value.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
                    }
            }
}
