package zip.trev.trevrpc

import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class WireCodecTest {
    /** A logical value in the vector file: how to encode it, decode it back, and which frame type carries it. */
    private class Entry<T>(
        val type: Long,
        val value: T,
        val encode: (T) -> ByteArray,
        val decode: (ByteArray) -> T,
    ) {
        fun assertMatches(
            name: String,
            vectors: Map<String, ByteArray>,
        ) {
            val payload = encode(value)
            assertArrayEquals(vectors.getValue("$name.payload"), payload, "$name payload")
            assertEquals(value, decode(payload), "$name decoded value")
            assertArrayEquals(payload, encode(decode(payload)), "$name re-encoded")

            val frame = encodeFrame(type, payload)
            assertArrayEquals(vectors.getValue("$name.frame"), frame, "$name frame")
            val read = FrameDecoder().feed(frame).single()
            assertEquals(type, read.type, "$name frame type")
            assertArrayEquals(payload, read.payload, "$name frame payload")
        }
    }

    @Test
    fun `all shared payload and frame vectors encode and decode byte for byte`() {
        val vectors = goldenVectors()
        val metadata = Metadata.of("authorization" to "ok".encodeToByteArray())
        val entries: Map<String, Entry<*>> =
            mapOf(
                "request.unary" to requestEntry(RpcRequest("/svc/m")),
                "request.bidi_timeout" to
                    requestEntry(
                        RpcRequest("/svc/m", kindValue = RpcKind.BIDIRECTIONAL_STREAMING.value, timeoutNanos = 123_456u),
                    ),
                "request.datagram_flow" to
                    requestEntry(
                        RpcRequest("/svc/m", kindValue = RpcKind.BIDIRECTIONAL_STREAMING.value, datagramFlow = 1uL),
                    ),
                "request.metadata" to requestEntry(RpcRequest("/svc/m", metadata)),
                "request.extensions" to requestEntry(RpcRequest("/svc/m", extensions = listOf(1uL, 300uL))),
                "headers.metadata" to headersEntry(RpcHeaders(Metadata.of("x-a" to "1".encodeToByteArray()))),
                "headers.empty" to headersEntry(RpcHeaders()),
                "headers.extensions" to headersEntry(RpcHeaders(extensions = listOf(1uL))),
                "status.ok" to statusEntry(RpcStatus()),
                "status.unavailable" to statusEntry(RpcStatus.fromStatus(Status.unavailable("down"))),
                "status.metadata" to
                    statusEntry(
                        RpcStatus.fromStatus(
                            Status(Code.FAILED_PRECONDITION, "stop", Metadata.of("x-t" to "2".encodeToByteArray())),
                        ),
                    ),
                "message.hi" to messageEntry("hi".encodeToByteArray()),
                "message.empty" to messageEntry(byteArrayOf()),
            )

        val payloadNames = vectors.keys.filter { it.endsWith(".payload") }.map { it.removeSuffix(".payload") }
        val frameNames = vectors.keys.filter { it.endsWith(".frame") }.map { it.removeSuffix(".frame") }
        assertEquals(payloadNames.toSet() - "grease.two_byte_type", entries.keys)
        assertEquals(payloadNames.toSet(), frameNames.toSet())
        entries.forEach { (name, entry) -> entry.assertMatches(name, vectors) }

        // Grease frames are well formed on the wire but never surface from a decoder.
        val grease = vectors.getValue("grease.two_byte_type.payload")
        val greaseFrame = encodeFrame(0x40, grease)
        assertArrayEquals(vectors.getValue("grease.two_byte_type.frame"), greaseFrame)
        assertTrue(FrameDecoder().feed(greaseFrame).isEmpty())
    }

    @Test
    fun `shared datagram vectors are the flow varint then one MESSAGE frame per message`() {
        val vectors = goldenVectors()
        val raw = rawVectors()
        val names = vectors.keys.filter { it.endsWith(".datagram") }.map { it.removeSuffix(".datagram") }
        assertTrue(names.size >= 3)
        names.forEach { name ->
            val flow = java.math.BigInteger(1, vectors.getValue("$name.flow")).toLong()
            val messages = raw.getValue("$name.messages").let { if (it.isEmpty()) emptyList() else it.split(',') }
            val expected =
                messages.fold(QuicVarint.encode(flow)) { bytes, message ->
                    bytes + encodeFrame(0, message.chunked(2).map { it.toInt(16).toByte() }.toByteArray())
                }
            assertArrayEquals(expected, vectors.getValue("$name.datagram"), "$name datagram")
            // The Cronet client never opens a flow, but a request carrying one still round-trips.
            val request = RpcRequest("/svc/m", datagramFlow = flow.toULong())
            assertEquals(flow.toULong(), WireCodec.decodeRequest(WireCodec.encode(request)).datagramFlow)
        }
    }

    @Test
    fun `shared stream vectors yield the same known frames at every split`() {
        val vectors = goldenVectors()
        val raw = rawVectors()
        val names = vectors.keys.filter { it.endsWith(".stream") }.map { it.removeSuffix(".stream") }
        assertTrue(names.size >= 4)
        assertEquals(
            names.toSet(),
            raw.keys
                .filter { it.endsWith(".frames") }
                .map { it.removeSuffix(".frames") }
                .toSet(),
        )
        names.forEach { name ->
            val stream = vectors.getValue("$name.stream")
            val expected = goldenFrames(raw.getValue("$name.frames"))
            val whole = FrameDecoder()
            assertEquals(expected, describe(whole.feed(stream)))
            whole.finish()
            for (split in 0..stream.size) {
                val decoder = FrameDecoder()
                val frames = decoder.feed(stream.copyOfRange(0, split)) + decoder.feed(stream.copyOfRange(split, stream.size))
                decoder.finish()
                assertEquals(expected, describe(frames), "$name split at $split")
            }
            val byteByByte = FrameDecoder()
            val frames = stream.flatMap { byteByByte.feed(byteArrayOf(it)) }
            byteByByte.finish()
            assertEquals(expected, describe(frames), "$name byte by byte")
        }
    }

    @Test
    fun `response frames decode exactly once into the sealed type`() {
        val headers =
            WireCodec.decodeResponseFrame(
                Frame(FrameType.HEADERS, WireCodec.encode(RpcHeaders(Metadata.of("a" to byteArrayOf(1))))),
            )
        assertEquals(1, (headers as ResponseFrame.Headers).metadata["a"]?.single()?.toInt())
        val message = WireCodec.decodeResponseFrame(Frame(FrameType.MESSAGE, byteArrayOf(5)))
        assertArrayEquals(byteArrayOf(5), (message as ResponseFrame.Message).body)
        val status = WireCodec.decodeResponseFrame(Frame(FrameType.STATUS, WireCodec.encode(RpcStatus.fromStatus(Status.unavailable("x")))))
        assertEquals(Status.unavailable("x"), (status as ResponseFrame.Status).status)

        val unexpected =
            assertThrows(TrevRpcException::class.java) { WireCodec.decodeResponseFrame(Frame(FrameType.REQUEST, byteArrayOf())) }
        assertEquals(Code.INTERNAL, unexpected.status.code)
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
                byteArrayOf(0x45, 0x00),
            )
        malformed.forEach { bytes ->
            assertEquals(
                Code.INVALID_ARGUMENT,
                assertThrows(TrevRpcException::class.java) { WireCodec.decodeRequest(bytes) }.status.code,
            )
        }
        listOf(byteArrayOf(0x08), byteArrayOf(0x12, 0x01, 0xff.toByte())).forEach { bytes ->
            assertEquals(
                Code.INVALID_ARGUMENT,
                assertThrows(TrevRpcException::class.java) { WireCodec.decodeStatus(bytes) }.status.code,
            )
        }
        assertEquals(
            Code.INVALID_ARGUMENT,
            assertThrows(TrevRpcException::class.java) { WireCodec.decodeHeaders(byteArrayOf(0x0a, 0x05, 0x01)) }.status.code,
        )
    }

    @Test
    fun `metadata validation remains distinguishable from malformed metadata utf8`() {
        val invalidMetadata =
            assertThrows(TrevRpcException::class.java) {
                WireCodec.decodeStatus(
                    byteArrayOf(
                        0x1a,
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

        val invalidHeaders =
            assertThrows(TrevRpcException::class.java) {
                WireCodec.decodeResponseFrame(
                    Frame(FrameType.HEADERS, byteArrayOf(0x0a, 0x0d, 0x0a, 0x09, *"trevrpc-x".encodeToByteArray(), 0x12, 0x00)),
                )
            }
        assertEquals(Code.INVALID_ARGUMENT, invalidHeaders.status.code)

        val malformedKey =
            assertThrows(TrevRpcException::class.java) {
                WireCodec.decodeStatus(byteArrayOf(0x1a, 0x03, 0x0a, 0x01, 0xff.toByte()))
            }
        assertEquals(Code.INVALID_ARGUMENT, malformedKey.status.code)
        assertFalse(malformedKey.status.message.startsWith("invalid metadata:"))
    }

    @Test
    fun `codec skips unknown fields including the reserved request body and preserves unknown enums`() {
        val request = WireCodec.decodeRequest(byteArrayOf(0x28, 0x63, 0x30, 0x01, 0x40, 0x07, 0x1a, 0x01, 0x7a))
        assertEquals(99, request.kindValue)
        assertNull(request.kind)
        assertEquals("", request.method)
        assertEquals(Code.UNKNOWN, Code.fromValue(999u))

        val negativeKind = RpcRequest("/svc/method", kindValue = -1)
        assertEquals(-1, WireCodec.decodeRequest(WireCodec.encode(negativeKind)).kindValue)
    }

    @Test
    fun `request codec reads the method path from field 1 and ignores reserved tags 2 3 and 6`() {
        val path = "/example.greeter.Greeter/SayHello"
        val encoded = WireCodec.encode(RpcRequest(path))
        assertEquals(path, WireCodec.decodeRequest(encoded).method)
        // A peer sending the retired method name (2), request body (3) or wire version (6) is skipped.
        val withReserved = encoded + byteArrayOf(0x12, 0x01, 0x6d, 0x1a, 0x01, 0x7a, 0x30, 0x01)
        assertEquals(RpcRequest(path), WireCodec.decodeRequest(withReserved))
        assertEquals(path, WireCodec.decodeRequest(WireCodec.encode(RpcRequest(path))).method)
        assertEquals(
            listOf<Byte>(0x0a, path.length.toByte()),
            encoded.take(2),
        )
    }

    @Test
    fun `extensions decode packed and unpacked and are ignored by the response frame`() {
        assertEquals(listOf(1uL, 300uL), WireCodec.decodeRequest(byteArrayOf(0x42, 0x03, 0x01, 0xac.toByte(), 0x02)).extensions)
        assertEquals(listOf(1uL, 300uL), WireCodec.decodeRequest(byteArrayOf(0x40, 0x01, 0x40, 0xac.toByte(), 0x02)).extensions)
        val frame = WireCodec.decodeResponseFrame(Frame(FrameType.HEADERS, byteArrayOf(0x12, 0x01, 0x01)))
        assertTrue((frame as ResponseFrame.Headers).metadata.isEmpty)
    }

    @Test
    fun `quic varints encode minimally decode non minimal forms and cover the full range`() {
        listOf(
            0L to "00",
            63L to "3f",
            64L to "4040",
            16_383L to "7fff",
            16_384L to "80004000",
            1_073_741_823L to "bfffffff",
            1_073_741_824L to "c000000040000000",
            QuicVarint.MAX_VALUE to "ffffffffffffffff",
        ).forEach { (value, hex) ->
            val encoded = hex.chunked(2).map { it.toInt(16).toByte() }.toByteArray()
            assertArrayEquals(encoded, QuicVarint.encode(value), "encode $value")
            assertEquals(encoded.size, QuicVarint.size(value))
            assertEquals(value, QuicVarint.decode(encoded))
        }
        assertEquals(37L, QuicVarint.decode(byteArrayOf(0x40, 0x25)))
        assertEquals(37L, QuicVarint.decode(byteArrayOf(0x80.toByte(), 0, 0, 0x25)))
        assertEquals(37L, QuicVarint.decode(byteArrayOf(0xc0.toByte(), 0, 0, 0, 0, 0, 0, 0x25)))
        assertThrows(IllegalArgumentException::class.java) { QuicVarint.decode(byteArrayOf(0x40)) }
        assertThrows(IllegalArgumentException::class.java) { QuicVarint.encode(QuicVarint.MAX_VALUE + 1) }
        assertThrows(IllegalArgumentException::class.java) { QuicVarint.encode(-1) }
    }

    private fun requestEntry(value: RpcRequest) = Entry(FrameType.REQUEST, value, WireCodec::encode, WireCodec::decodeRequest)

    private fun headersEntry(value: RpcHeaders) = Entry(FrameType.HEADERS, value, WireCodec::encode, WireCodec::decodeHeaders)

    private fun statusEntry(value: RpcStatus) = Entry(FrameType.STATUS, value, WireCodec::encode, WireCodec::decodeStatus)

    // Message payloads are the application bytes themselves, so the codec is the identity.
    private fun messageEntry(value: ByteArray) = Entry(FrameType.MESSAGE, value.toList(), { it.toByteArray() }, { it.toList() })

    private fun describe(frames: List<Frame>): List<String> = frames.map { "${it.type}:${it.payload.toHex()}" }

    private fun goldenFrames(value: String): List<String> = if (value.isEmpty()) emptyList() else value.split(',')

    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it) }

    private fun rawVectors(): Map<String, String> =
        checkNotNull(javaClass.classLoader.getResourceAsStream("wire-golden-vectors.txt"))
            .bufferedReader()
            .useLines { lines ->
                lines
                    .map(String::trim)
                    .filter { it.isNotEmpty() && !it.startsWith('#') }
                    .associate { line ->
                        val (name, value) = line.split('=', limit = 2).map(String::trim)
                        name to value
                    }
            }

    /**
     * Hex values of every entry as bytes except `.frames` and `.messages`, which are lists. The
     * `datagram.*` entries (`.flow`, `.messages`, `.datagram`, `.received`) are not frames; no frame
     * test reads them because they have no `.payload`.
     */
    private fun goldenVectors(): Map<String, ByteArray> =
        rawVectors()
            .filterKeys { !it.endsWith(".frames") && !it.endsWith(".messages") }
            .mapValues { (_, value) -> value.chunked(2).map { it.toInt(16).toByte() }.toByteArray() }
}
