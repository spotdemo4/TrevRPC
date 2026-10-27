package zip.trev.trevrpc

import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class CompressionTest {
    private val vectors = zstdVectors()
    private val dictionary = vectors.getValue("dict.d1.dictionary")
    private val d1 = DictionaryInfo(1u, vectors.getValue("dict.d1.fingerprint"))
    private val compressible = vectors.getValue("ok.small.message")

    /** A message large enough to compress without a dictionary. */
    private val record = vectors.getValue("plain.records.message")
    private val requests = MessageCodec(ByteArray::copyOf, ByteArray::copyOf, "test.Request")
    private val responses = MessageCodec(ByteArray::copyOf, ByteArray::copyOf, "test.Response")

    @Test
    fun `negotiation fields match the shared vectors byte for byte`() {
        val request = RpcRequest("/svc/m", acceptDictionaries = listOf(1u, 300u), dictionaryQuery = listOf(d1))
        assertArrayEquals(vectors.getValue("request.dictionaries.payload"), WireCodec.encode(request))
        assertEquals(request, WireCodec.decodeRequest(vectors.getValue("request.dictionaries.payload")))

        val answer = RpcAnswer(dictionaries = listOf(d1))
        assertArrayEquals(vectors.getValue("answer.dictionaries.payload"), WireCodec.encode(answer))
        assertEquals(answer, WireCodec.decodeAnswer(vectors.getValue("answer.dictionaries.payload")))
        val frame = WireCodec.decodeResponseFrame(Frame(FrameType.ANSWER, vectors.getValue("answer.dictionaries.payload")))
        assertEquals(listOf(d1), (frame as ResponseFrame.Answer).dictionaries)

        listOf("request.dictionaries", "answer.dictionaries").forEach { name ->
            val type = if (name.startsWith("request")) FrameType.REQUEST else FrameType.ANSWER
            assertArrayEquals(vectors.getValue("$name.frame"), encodeFrame(type, vectors.getValue("$name.payload")), name)
        }
    }

    @Test
    fun `compression without a dictionary is negotiated as ID 0, byte for byte`() {
        val request = RpcRequest("/svc/m", acceptDictionaries = listOf(0u), dictionaryQuery = listOf(PLAIN_DICTIONARY))
        assertArrayEquals(vectors.getValue("request.plain.payload"), WireCodec.encode(request))
        assertEquals(request, WireCodec.decodeRequest(vectors.getValue("request.plain.payload")))

        val frame = WireCodec.decodeResponseFrame(Frame(FrameType.ANSWER, vectors.getValue("answer.plain.payload")))
        assertEquals(listOf(PLAIN_DICTIONARY), (frame as ResponseFrame.Answer).dictionaries)
        assertArrayEquals(vectors.getValue("answer.plain.payload"), WireCodec.encode(RpcAnswer(dictionaries = listOf(PLAIN_DICTIONARY))))

        listOf("request.plain", "answer.plain").forEach { name ->
            val type = if (name.startsWith("request")) FrameType.REQUEST else FrameType.ANSWER
            assertArrayEquals(vectors.getValue("$name.frame"), encodeFrame(type, vectors.getValue("$name.payload")), name)
        }
    }

    @Test
    fun `a call that accepts messages without a dictionary decodes them and still rejects the invalid ones`() {
        val accepting = checkNotNull(compression("test.Response").call(null, "test.Response", false, CompressMessages.RESPONSES))
        val plain = vectors.keys.filter { it.startsWith("plain.") && it.endsWith(".payload") }
        assertTrue(plain.isNotEmpty())
        plain.forEach { name ->
            assertArrayEquals(vectors.getValue(name.replace(".payload", ".message")), inflate(accepting, vectors.getValue(name)), name)
            // A call that listed only dict.d1 rejects it.
            val error = assertThrows(TrevRpcException::class.java, { inflate(callCompression(), vectors.getValue(name)) }, name)
            assertEquals(Code.INTERNAL, error.status.code, name)
        }
        // bad.no_dictionary_id, among the others, needs dict.d1 to decompress, so it fails here too.
        vectors.keys.filter { it.startsWith("bad.") && it.endsWith(".payload") }.forEach { name ->
            val error = assertThrows(TrevRpcException::class.java, { inflate(accepting, vectors.getValue(name)) }, name)
            assertEquals(Code.INTERNAL, error.status.code, name)
        }
    }

    @Test
    fun `a unary call accepts responses without a dictionary but never compresses its request`() =
        runBlocking {
            val transport = ScriptedTransport(Compression(emptyList()))
            transport.respond(plainCompressed(record), ok())
            transport.end()
            val response = Client(transport).unary("/svc/m", record, requests, responses, compressMessages = CompressMessages.BOTH)

            assertArrayEquals(record, response)
            val request = transport.requests.single()
            assertEquals(listOf(0u), request.acceptDictionaries)
            assertEquals(emptyList<DictionaryInfo>(), request.dictionaryQuery)
            assertArrayEquals(record, transport.firstMessages.single())
        }

    @Test
    fun `a request stream compresses without a dictionary from 256 bytes once its answer confirms ID 0`() =
        runBlocking {
            val transport = ScriptedTransport(Compression(emptyList()))
            val call =
                Client(transport).openBidirectionalStreaming(
                    "/svc/m",
                    requests,
                    responses,
                    compressMessages = CompressMessages.BOTH,
                )
            val request = transport.requests.single()
            assertEquals(listOf(PLAIN_DICTIONARY), request.dictionaryQuery)
            assertEquals(listOf(0u), request.acceptDictionaries)

            call.send(record)
            transport.respond(answer(PLAIN_DICTIONARY), ResponseFrame.Headers())
            call.headers()
            call.send(record)
            call.send(record.copyOf(PLAIN_MIN_SIZE - 1))
            // Random bytes do not shrink, so they travel as they are.
            call.send(ByteArray(PLAIN_MIN_SIZE).also(java.util.Random(1)::nextBytes))

            assertEquals(
                listOf(FrameType.MESSAGE, FrameType.ZSTD_MESSAGE, FrameType.MESSAGE, FrameType.MESSAGE),
                transport.sent.map { it.first },
            )
            val payload = transport.sent[1].second
            val header = checkNotNull(parseZstdHeader(payload))
            assertEquals(0u, header.dictionaryId)
            assertArrayEquals(record, ZstdCodec(null, 1).decompress(payload, header))
            call.close()
        }

    @Test
    fun `a confirmed dictionary wins over compression without one`() =
        runBlocking {
            val transport = ScriptedTransport(compression("test.Request"))
            val call =
                Client(transport).openBidirectionalStreaming("/svc/m", requests, responses, compressMessages = CompressMessages.REQUESTS)
            assertEquals(listOf(d1, PLAIN_DICTIONARY), transport.requests.single().dictionaryQuery)
            transport.respond(answer(d1, PLAIN_DICTIONARY), ResponseFrame.Headers())
            call.headers()
            call.send(record)
            assertEquals(1u, parseZstdHeader(transport.sent.single().second)?.dictionaryId)
            call.close()
        }

    @Test
    fun `a request stream stays uncompressed when the answer does not confirm ID 0`() =
        runBlocking {
            val transport = ScriptedTransport(Compression(emptyList()))
            val call =
                Client(transport).openBidirectionalStreaming("/svc/m", requests, responses, compressMessages = CompressMessages.REQUESTS)
            transport.respond(answer(), ResponseFrame.Headers())
            call.headers()
            call.send(record)
            assertEquals(listOf(FrameType.MESSAGE), transport.sent.map { it.first })
            call.close()
        }

    @Test
    fun `responses without a dictionary fail a call that did not accept them`() =
        runBlocking {
            val transport = ScriptedTransport(Compression(emptyList()))
            transport.respond(plainCompressed(record), ok())
            val error =
                assertThrows(TrevRpcException::class.java) {
                    runBlocking { Client(transport).unary("/svc/m", record, requests, responses) }
                }
            assertEquals(Code.INTERNAL, error.status.code)
            assertEquals(RpcRequest("/svc/m"), transport.requests.single())
        }

    @Test
    fun `dictionary ID and fingerprint come from the dictionary`() {
        assertEquals(1u, zstdDictionaryId(dictionary))
        assertArrayEquals(vectors.getValue("dict.d1.fingerprint"), zstdFingerprint(dictionary))
    }

    @Test
    fun `receivers accept the valid payloads and reject the invalid ones`() {
        val accepting = callCompression()
        val ok = vectors.keys.filter { it.startsWith("ok.") && it.endsWith(".payload") }
        assertTrue(ok.size >= 5)
        ok.forEach { name ->
            val message = inflate(accepting, vectors.getValue(name))
            assertArrayEquals(vectors.getValue(name.replace(".payload", ".message")), message, name)
        }

        val bad = vectors.keys.filter { it.startsWith("bad.") && it.endsWith(".payload") }
        assertTrue(bad.size >= 9)
        bad.forEach { name ->
            val error = assertThrows(TrevRpcException::class.java, { inflate(accepting, vectors.getValue(name)) }, name)
            assertEquals(Code.INTERNAL, error.status.code, name)
        }

        val tooLarge = vectors.getValue("too_large.zeros.payload")
        val error = assertThrows(TrevRpcException::class.java) { inflate(accepting, tooLarge) }
        assertEquals(Code.RESOURCE_EXHAUSTED, error.status.code)
    }

    @Test
    fun `a ZSTD_MESSAGE frame carries the payload and counts at the size of its message`() {
        val payload = vectors.getValue("zstd_message.small.payload")
        val frame = encodeMessageFrame(FrameType.ZSTD_MESSAGE, payload)
        assertArrayEquals(vectors.getValue("zstd_message.small.frame"), frame)
        val read = FrameDecoder().feed(frame).single()
        assertEquals(FrameType.ZSTD_MESSAGE, read.type)
        assertTrue((WireCodec.decodeResponseFrame(read) as ResponseFrame.Message).compressed)

        val small = payload.size
        val error = assertThrows(TrevRpcException::class.java) { encodeMessageFrame(FrameType.ZSTD_MESSAGE, payload, small) }
        assertEquals(Code.RESOURCE_EXHAUSTED, error.status.code)
        val read2 = assertThrows(TrevRpcException::class.java) { WireCodec.decodeResponseFrame(read, small) }
        assertEquals(Code.RESOURCE_EXHAUSTED, read2.status.code)
    }

    @Test
    fun `messages compress only when that makes them shorter and round trip`() {
        val codec = ConfiguredDictionary(dictionary, 1).codec
        val payload = checkNotNull(codec.compress(compressible))
        assertTrue(payload.size < compressible.size)
        val header = checkNotNull(parseZstdHeader(payload))
        assertEquals(1u, header.dictionaryId)
        assertArrayEquals(compressible, codec.decompress(payload, header))
        assertNull(codec.compress(vectors.getValue("ok.raw_block.message")))
        assertNull(codec.compress(byteArrayOf()))
    }

    @Test
    fun `invalid configurations are rejected`() {
        val raw = byteArrayOf(1, 2, 3, 4, 5, 6, 7, 8, 9)
        assertThrows(IllegalArgumentException::class.java) { Compression(listOf(Dictionary(raw))) }
        assertThrows(IllegalArgumentException::class.java) {
            Compression(listOf(Dictionary(dictionary), Dictionary(dictionary)))
        }
        assertThrows(IllegalArgumentException::class.java) {
            Compression(listOf(Dictionary(dictionary, listOf("not a type"))))
        }
        val zero = dictionary.copyOf().also { it.fill(0, 4, 8) }
        assertThrows(IllegalArgumentException::class.java) { Compression(listOf(Dictionary(zero))) }
    }

    @Test
    fun `a unary call queries and accepts the response dictionary and decompresses the response`() =
        runBlocking {
            val transport = ScriptedTransport(compression("test.Request", "test.Response"))
            transport.respond(answer(d1), compressed(compressible), ok())
            transport.end()
            val response = Client(transport).unary("/svc/m", compressible, requests, responses)

            assertArrayEquals(compressible, response)
            val request = transport.requests.single()
            // The unary request message is sent before the answer, so only the response dictionary is queried.
            assertEquals(listOf(d1), request.dictionaryQuery)
            assertEquals(listOf(1u), request.acceptDictionaries)
            assertArrayEquals(compressible, transport.firstMessages.single())
        }

    @Test
    fun `a call whose types have no dictionary sends no negotiation fields`() =
        runBlocking {
            val transport = ScriptedTransport(compression("other.Type"))
            transport.respond(message(compressible), ok())
            transport.end()
            Client(transport).unary("/svc/m", compressible, requests, responses)
            assertEquals(RpcRequest("/svc/m"), transport.requests.single())
        }

    @Test
    fun `a request stream compresses once the call's answer confirms the dictionary`() =
        runBlocking {
            val transport = ScriptedTransport(compression("test.Request"))
            val call = Client(transport).openBidirectionalStreaming("/svc/m", requests, responses)
            assertEquals(listOf(d1), transport.requests.single().dictionaryQuery)
            assertEquals(emptyList<UInt>(), transport.requests.single().acceptDictionaries)

            call.send(compressible)
            transport.respond(answer(d1), ResponseFrame.Headers())
            call.headers()
            call.send(compressible)
            call.send(vectors.getValue("ok.raw_block.message"))

            assertEquals(listOf(FrameType.MESSAGE, FrameType.ZSTD_MESSAGE, FrameType.MESSAGE), transport.sent.map { it.first })
            val payload = transport.sent[1].second
            assertArrayEquals(compressible, ConfiguredDictionary(dictionary, 1).codec.decompress(payload, parseZstdHeader(payload)!!))
            call.close()
        }

    @Test
    fun `a request stream stays uncompressed when the answer does not confirm the dictionary`() =
        runBlocking {
            val transport = ScriptedTransport(compression("test.Request"))
            val call = Client(transport).openBidirectionalStreaming("/svc/m", requests, responses)
            transport.respond(answer(DictionaryInfo(1u, ByteArray(8))), ResponseFrame.Headers())
            call.headers()
            call.send(compressible)
            assertEquals(listOf(FrameType.MESSAGE), transport.sent.map { it.first })
            call.close()
        }

    @Test
    fun `compressed responses the call does not accept fail the call`() =
        runBlocking {
            val unaccepting = ScriptedTransport(null)
            unaccepting.respond(compressed(compressible), ok())
            val error =
                assertThrows(TrevRpcException::class.java) {
                    runBlocking { Client(unaccepting).unary("/svc/m", compressible, requests, responses) }
                }
            assertEquals(Code.INTERNAL, error.status.code)

            val small = ScriptedTransport(compression("test.Response"))
            small.respond(answer(d1), compressed(compressible), ok())
            val tooLarge =
                assertThrows(TrevRpcException::class.java) {
                    runBlocking {
                        Client(small).unary("/svc/m", compressible, requests, responses, CallOptions(maxResponseBodySize = 8))
                    }
                }
            assertEquals(Code.RESOURCE_EXHAUSTED, tooLarge.status.code)
        }

    private fun compression(vararg types: String) = Compression(listOf(Dictionary(dictionary, types.toList())))

    private fun callCompression(): CallCompression = checkNotNull(compression("test.Response").call(null, "test.Response", false))

    private fun inflate(
        compression: CallCompression,
        payload: ByteArray,
    ): ByteArray {
        val message = WireCodec.decodeResponseFrame(Frame(FrameType.ZSTD_MESSAGE, payload)) as ResponseFrame.Message
        return inflateResponse(message, compression, DEFAULT_MAX_FRAME_SIZE).body
    }

    private fun plainCompressed(message: ByteArray): ResponseFrame {
        val payload = checkNotNull(ZstdCodec(null, 1).compress(message))
        return WireCodec.decodeResponseFrame(Frame(FrameType.ZSTD_MESSAGE, payload))
    }

    private fun compressed(message: ByteArray): ResponseFrame {
        val payload = checkNotNull(ConfiguredDictionary(dictionary, 1).codec.compress(message))
        return WireCodec.decodeResponseFrame(Frame(FrameType.ZSTD_MESSAGE, payload))
    }

    private fun answer(vararg answer: DictionaryInfo): ResponseFrame = ResponseFrame.Answer(answer.toList())

    private fun message(body: ByteArray): ResponseFrame = ResponseFrame.Message(body)

    private fun ok(): ResponseFrame = ResponseFrame.Status(Status.ok())
}
