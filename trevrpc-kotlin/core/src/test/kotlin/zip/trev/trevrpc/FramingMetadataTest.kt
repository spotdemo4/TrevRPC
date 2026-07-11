package zip.trev.trevrpc

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class FramingMetadataTest {
    @Test
    fun `incremental framing handles splits coalescing zero and exact maximum`() {
        val bodies = listOf(byteArrayOf(), byteArrayOf(1), byteArrayOf(2, 3, 4), ByteArray(16) { it.toByte() })
        val encoded = bodies.fold(byteArrayOf()) { result, body -> result + frame(body, 16) }

        for (split in 0..encoded.size) {
            val decoder = FrameDecoder(16)
            val decoded = decoder.feed(encoded.copyOfRange(0, split)) + decoder.feed(encoded.copyOfRange(split, encoded.size))
            decoder.finish()
            assertEquals(bodies.size, decoded.size)
            bodies.zip(decoded).forEach { (expected, actual) -> assertArrayEquals(expected, actual) }
        }
    }

    @Test
    fun `declared oversize is rejected from the header before body arrives`() {
        val decoder = FrameDecoder(16)
        val error =
            assertThrows(TrevRpcException::class.java) {
                decoder.feed(byteArrayOf(0, 0, 0, 17))
            }
        assertEquals(Code.RESOURCE_EXHAUSTED, error.status.code)
        assertThrows(TrevRpcException::class.java) { decoder.feed(byteArrayOf()) }
    }

    @Test
    fun `partial frame at eof is internal`() {
        val decoder = FrameDecoder()
        decoder.feed(byteArrayOf(0, 0, 0))
        assertEquals(
            Code.INTERNAL,
            assertThrows(TrevRpcException::class.java, decoder::finish).status.code,
        )
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
        assertEquals("trevrpc/1", ALPN)
        assertEquals(1u, WIRE_VERSION)
        assertEquals(listOf(0, 1, 2, 3), RpcKind.entries.map(RpcKind::value))
        assertEquals((0u..16u).toList(), Code.entries.map(Code::value))

        val badVersion = RpcRequest("svc", "method", version = 2u)
        assertEquals(
            Code.FAILED_PRECONDITION,
            assertThrows(TrevRpcException::class.java, badVersion::validateProtocol).status.code,
        )
        val badKind = RpcRequest("svc", "method", kindValue = 99)
        assertEquals(
            Code.INVALID_ARGUMENT,
            assertThrows(TrevRpcException::class.java, badKind::validateProtocol).status.code,
        )
    }

    private fun assertInvalid(block: () -> Unit) {
        assertEquals(Code.INVALID_ARGUMENT, assertThrows(TrevRpcException::class.java, block).status.code)
    }
}
