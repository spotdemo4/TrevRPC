package zip.trev.trevrpc

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class FramingMetadataTest {
    @Test
    fun `incremental framing handles splits coalescing zero and exact maximum`() {
        val payloads = listOf(byteArrayOf(), byteArrayOf(1), byteArrayOf(2, 3, 4), ByteArray(16) { it.toByte() })
        val types = listOf(FrameType.MESSAGE, FrameType.HEADERS, FrameType.STATUS, FrameType.MESSAGE)
        val encoded =
            types.zip(payloads).fold(byteArrayOf()) { result, (type, payload) -> result + encodeFrame(type, payload, 16) }

        for (split in 0..encoded.size) {
            val decoder = FrameDecoder(16)
            val decoded = decoder.feed(encoded.copyOfRange(0, split)) + decoder.feed(encoded.copyOfRange(split, encoded.size))
            decoder.finish()
            assertEquals(payloads.size, decoded.size)
            payloads.indices.forEach { index ->
                assertEquals(types[index], decoded[index].type)
                assertArrayEquals(payloads[index], decoded[index].payload)
            }
        }
    }

    @Test
    fun `frames carry two four and eight byte headers and non minimal encodings`() {
        // Non-minimal type (4 bytes) and length (8 bytes) for a MESSAGE with payload "ab".
        val wide = byteArrayOf(0x80.toByte(), 0, 0, 0, 0xc0.toByte(), 0, 0, 0, 0, 0, 0, 2, 'a'.code.toByte(), 'b'.code.toByte())
        val frames = FrameDecoder().feed(wide)
        assertEquals(1, frames.size)
        assertEquals(FrameType.MESSAGE, frames.single().type)
        assertArrayEquals("ab".encodeToByteArray(), frames.single().payload)

        val big = ByteArray(300) { it.toByte() }
        val encoded = encodeFrame(FrameType.MESSAGE, big)
        assertEquals(1 + 2 + 300, encoded.size)
        assertEquals(0x41, encoded[1].toInt() and 0xff)
        assertArrayEquals(big, FrameDecoder().feed(encoded).single().payload)
    }

    @Test
    fun `unknown and grease frames are skipped including after status and without buffering`() {
        val unknown =
            encodeFrame(0x40, ByteArray(10)) + encodeFrame(0x21, byteArrayOf(1)) + encodeFrame(QuicVarint.MAX_VALUE, byteArrayOf())
        val stream =
            unknown + encodeFrame(FrameType.HEADERS, byteArrayOf()) + unknown +
                encodeFrame(FrameType.MESSAGE, byteArrayOf(9)) + unknown +
                encodeFrame(FrameType.STATUS, byteArrayOf()) + unknown
        for (split in 0..stream.size) {
            val decoder = FrameDecoder()
            val frames = decoder.feed(stream.copyOfRange(0, split)) + decoder.feed(stream.copyOfRange(split, stream.size))
            decoder.finish()
            assertEquals(listOf(FrameType.HEADERS, FrameType.MESSAGE, FrameType.STATUS), frames.map(Frame::type))
        }
    }

    @Test
    fun `declared oversize is rejected from the header before payload arrives`() {
        val decoder = FrameDecoder(16)
        // MESSAGE with a declared length of 17; only the header is fed.
        val error = assertThrows(TrevRpcException::class.java) { decoder.feed(byteArrayOf(0, 17)) }
        assertEquals(Code.RESOURCE_EXHAUSTED, error.status.code)
        assertThrows(TrevRpcException::class.java) { decoder.feed(byteArrayOf()) }
        assertThrows(TrevRpcException::class.java, decoder::finish)

        // Unknown frames obey the same limit, and a huge declared length never allocates.
        val unknown = FrameDecoder(16)
        val unknownError =
            assertThrows(TrevRpcException::class.java) {
                unknown.feed(byteArrayOf(0x40, 0x40) + QuicVarint.encode(QuicVarint.MAX_VALUE))
            }
        assertEquals(Code.RESOURCE_EXHAUSTED, unknownError.status.code)

        val huge = FrameDecoder()
        val hugeError =
            assertThrows(TrevRpcException::class.java) {
                huge.feed(byteArrayOf(0) + QuicVarint.encode(Int.MAX_VALUE.toLong() + 1))
            }
        assertEquals(Code.RESOURCE_EXHAUSTED, hugeError.status.code)

        assertThrows(TrevRpcException::class.java) { encodeFrame(FrameType.MESSAGE, ByteArray(17), 16) }
        encodeFrame(FrameType.MESSAGE, ByteArray(16), 16)
    }

    @Test
    fun `partial frame at eof is internal in the header payload and skipped payload`() {
        val frame = encodeFrame(FrameType.MESSAGE, byteArrayOf(1, 2, 3))
        val skipped = encodeFrame(0x40, byteArrayOf(1, 2, 3))
        listOf(
            byteArrayOf(0x40), // first byte of a two-byte type
            frame.copyOfRange(0, 1), // type only
            byteArrayOf(0, 0x40), // first byte of a two-byte length
            frame.copyOfRange(0, frame.size - 1), // partial payload
            skipped.copyOfRange(0, skipped.size - 1), // partial unknown payload
        ).forEach { partial ->
            val decoder = FrameDecoder()
            decoder.feed(partial)
            assertEquals(
                Code.INTERNAL,
                assertThrows(TrevRpcException::class.java, decoder::finish).status.code,
            )
        }

        // A clean boundary, with or without frames, finishes normally.
        FrameDecoder().finish()
        FrameDecoder().apply {
            feed(frame + skipped)
            finish()
        }
    }

    @Test
    fun `metadata is normalized immutable and accepts exact boundaries`() {
        val source = byteArrayOf(1, 2)
        val metadata = Metadata.of("Authorization" to source)
        source[0] = 9
        assertArrayEquals(byteArrayOf(1, 2), metadata["authorization"])
        val returned = checkNotNull(metadata["authorization"])
        returned[0] = 8
        assertArrayEquals(byteArrayOf(1, 2), metadata["authorization"])
        assertFalse(metadata.contains("Authorization"))

        Metadata.of("a".repeat(MAX_METADATA_KEY_LENGTH) to byteArrayOf())
        Metadata.of("value" to ByteArray(MAX_METADATA_VALUE_LENGTH))
        val exactTotal = Metadata.builder()
        listOf("a", "b", "c", "d", "e", "f", "g", "h").forEach {
            exactTotal.put(it, ByteArray(8191))
        }
        assertEquals(MAX_METADATA_TOTAL_SIZE, exactTotal.build().sumOf { it.key.length + it.value.size })
    }

    @Test
    fun `metadata rejects syntax count value and total violations`() {
        listOf("", "bad key", "trevrpc-timeout", "ümlaut").forEach { key ->
            assertInvalid { Metadata.of(key to byteArrayOf()) }
        }
        assertInvalid { Metadata.of("key" to ByteArray(MAX_METADATA_VALUE_LENGTH + 1)) }
        assertInvalid {
            val builder = Metadata.builder()
            repeat(MAX_METADATA_ENTRIES + 1) { builder.put("key-$it", byteArrayOf()) }
            builder.build()
        }
        assertInvalid {
            val builder = Metadata.builder()
            listOf("a", "b", "c", "d", "e", "f", "g", "h").forEach {
                builder.put(it, ByteArray(8191))
            }
            builder.put("i", byteArrayOf())
            builder.build()
        }
    }

    @Test
    fun `versions kinds and all status values are stable`() {
        assertEquals(1u, WIRE_VERSION)
        assertEquals(listOf(0, 1, 2, 3), RpcKind.entries.map(RpcKind::value))
        assertEquals((0u..16u).toList(), Code.entries.map(Code::value))
    }

    private fun assertInvalid(block: () -> Unit) {
        val error = assertThrows(TrevRpcException::class.java, block)
        assertEquals(Code.INVALID_ARGUMENT, error.status.code)
        assertTrue(error.status.message.startsWith("invalid metadata:"))
    }
}
