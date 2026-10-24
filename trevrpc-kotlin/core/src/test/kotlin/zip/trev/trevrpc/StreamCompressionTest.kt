package zip.trev.trevrpc

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

class StreamCompressionTest {
    private val vectors = zstdVectors()
    private val dictionary = vectors.getValue("dict.d1.dictionary")
    private val requests = MessageCodec(ByteArray::copyOf, ByteArray::copyOf, "test.Request")
    private val responses = MessageCodec(ByteArray::copyOf, ByteArray::copyOf, "test.Response")
    private val compressible = vectors.getValue("stream.plain.0.message")

    @Test
    fun `negotiation fields match the shared vectors byte for byte`() {
        val request = RpcRequest("/svc/m", streamCompression = StreamCompression(16u))
        assertArrayEquals(vectors.getValue("request.stream_compression.payload"), WireCodec.encode(request))
        assertEquals(request, WireCodec.decodeRequest(vectors.getValue("request.stream_compression.payload")))
        val headers = RpcHeaders(streamCompression = StreamCompression(16u))
        assertArrayEquals(vectors.getValue("headers.stream_compression.payload"), WireCodec.encode(headers))
        assertEquals(headers, WireCodec.decodeHeaders(vectors.getValue("headers.stream_compression.payload")))
        assertArrayEquals(
            vectors.getValue("zstd_stream_message.first.frame"),
            encodeMessageFrame(FrameType.ZSTD_STREAM_MESSAGE, vectors.getValue("zstd_stream_message.first.payload")),
        )
    }

    @Test
    fun `receivers decode every valid sequence in order`() {
        for (name in sequences("stream.")) {
            val streams = responseStreams()
            var index = 0
            while (vectors.containsKey("stream.$name.$index.payload")) {
                val received = streams.inflate(streamMessage(vectors.getValue("stream.$name.$index.payload")), DEFAULT_MAX_FRAME_SIZE)
                assertArrayEquals(vectors.getValue("stream.$name.$index.message"), received.body, "$name $index")
                index++
            }
            assertTrue(index >= 2, name)
            streams.close()
        }
    }

    @Test
    fun `receivers reject every invalid first payload`() {
        val names = vectors.keys.filter { it.startsWith("stream_bad.") }
        assertTrue(names.size >= 9)
        for (name in names) {
            val streams = responseStreams()
            val error =
                assertThrows(
                    TrevRpcException::class.java,
                    { streams.inflate(streamMessage(vectors.getValue(name)), DEFAULT_MAX_FRAME_SIZE) },
                    name,
                )
            assertEquals(Code.INTERNAL, error.status.code, name)
            // The direction cannot continue after a failure.
            val next = streamMessage(vectors.getValue("stream.plain.0.payload"))
            assertEquals(
                Code.INTERNAL,
                assertThrows(TrevRpcException::class.java) { streams.inflate(next, DEFAULT_MAX_FRAME_SIZE) }.status.code,
            )
            streams.close()
        }
        val tooLarge =
            assertThrows(TrevRpcException::class.java) {
                streamMessage(vectors.getValue("stream_too_large.zeros.payload"))
            }
        assertEquals(Code.RESOURCE_EXHAUSTED, tooLarge.status.code)
        val overLimit = streamMessage(vectors.getValue("stream.plain.0.payload"))
        val limited = responseStreams()
        assertEquals(Code.RESOURCE_EXHAUSTED, assertThrows(TrevRpcException::class.java) { limited.inflate(overLimit, 8) }.status.code)
        limited.close()
    }

    @Test
    fun `the encoder writes the shared payloads byte for byte`() {
        for ((name, seed) in listOf("plain" to null, "seeded" to ConfiguredDictionary(dictionary, 1).codec)) {
            val encoder = ZstdStreamEncoder(1, STREAM_WINDOW_LOG, seed)
            var index = 0
            while (vectors.containsKey("stream.$name.$index.payload")) {
                val message = vectors.getValue("stream.$name.$index.message")
                val payload = QuicVarint.encode(message.size.toLong()) + encoder.compress(message)
                assertArrayEquals(vectors.getValue("stream.$name.$index.payload"), payload, "$name $index")
                index++
            }
            encoder.close()
        }
        val encoder = ZstdStreamEncoder(1, STREAM_WINDOW_LOG, null)
        assertEquals(0, encoder.compress(ByteArray(0)).size)
        encoder.close()
    }

    @Test
    fun `a call that is not optimistic offers, then compresses requests from the third message after the answer`() =
        runBlocking {
            val budget = StreamCompressionBudget()
            // As after a refusal: the call waits for the answer.
            budget.optimistic = false
            val transport = ScriptedTransport(Compression(emptyList()), budget)
            val call = Client(transport).openBidirectionalStreaming("/svc/m", requests, responses, compressStream = CompressStream.BOTH)
            assertEquals(StreamCompression(16u), transport.requests.single().streamCompression)

            call.send(compressible)
            call.send(compressible)
            transport.respond(ResponseFrame.Headers(streamCompression = StreamCompression(16u)))
            call.headers()
            val more = (1..3).map { vectors.getValue("stream.plain.${it % 3}.message") }
            more.forEach { call.send(it) }
            assertEquals(
                listOf(FrameType.MESSAGE, FrameType.MESSAGE) + List(3) { FrameType.ZSTD_STREAM_MESSAGE },
                transport.sent.map { it.first },
            )
            // The server's side of the request stream decodes them in order.
            val server = responseStreams()
            transport.sent.drop(2).zip(more).forEach { (sent, message) ->
                assertArrayEquals(message, server.inflate(streamMessage(sent.second), DEFAULT_MAX_FRAME_SIZE).body)
            }
            server.close()

            var index = 0
            while (vectors.containsKey("stream.plain.$index.payload")) {
                transport.respond(streamFrame(vectors.getValue("stream.plain.$index.payload")))
                assertArrayEquals(vectors.getValue("stream.plain.$index.message"), call.receive())
                index++
            }
            call.close()
            assertNotNull(budget.reserve(budget.bytes), "every reservation is released when the call ends")
        }

    @Test
    fun `a client-streaming call reports the answer's headers mid-upload, and compresses the requests after them`() =
        runBlocking {
            val transport = ScriptedTransport(Compression(emptyList()), StreamCompressionBudget())
            val headers = CompletableDeferred<Metadata>()
            val more = (0..2).map { vectors.getValue("stream.plain.$it.message") }
            val upload =
                flow {
                    emit(compressible)
                    // The server sends HEADERS at dispatch, before its only response.
                    transport.respond(ResponseFrame.Headers(streamCompression = StreamCompression(16u)))
                    headers.await()
                    emit(compressible)
                    more.forEach { emit(it) }
                    transport.respond(ResponseFrame.Message(compressible), ResponseFrame.Status(Status.ok()))
                }
            val response =
                Client(transport).clientStreaming(
                    "/svc/m",
                    upload,
                    requests,
                    responses,
                    CallOptions(onResponseHeaders = { headers.complete(it) }),
                    CompressStream.REQUESTS,
                )
            assertArrayEquals(compressible, response)
            assertEquals(
                listOf(FrameType.MESSAGE, FrameType.MESSAGE) + List(3) { FrameType.ZSTD_STREAM_MESSAGE },
                transport.sent.map { it.first },
            )
        }

    @Test
    fun `without an answer requests stay per-message`() =
        runBlocking {
            val transport = ScriptedTransport(Compression(emptyList()), StreamCompressionBudget())
            val call = Client(transport).openBidirectionalStreaming("/svc/m", requests, responses, compressStream = CompressStream.BOTH)
            transport.respond(ResponseFrame.Headers())
            call.headers()
            repeat(4) { call.send(compressible) }
            assertEquals(List(4) { FrameType.MESSAGE }, transport.sent.map { it.first })
            call.close()
        }

    @Test
    fun `a spent budget falls back to per-message compression`() =
        runBlocking {
            // Room for the response context only: the call offers, but cannot compress requests.
            val tight = StreamCompressionBudget(400_000)
            val transport = ScriptedTransport(Compression(emptyList()), tight)
            val call = Client(transport).openBidirectionalStreaming("/svc/m", requests, responses, compressStream = CompressStream.BOTH)
            assertEquals(StreamCompression(16u, optimistic = true), transport.requests.single().streamCompression)
            transport.respond(ResponseFrame.Headers(streamCompression = StreamCompression(16u)))
            call.headers()
            repeat(4) { call.send(compressible) }
            assertEquals(List(4) { FrameType.MESSAGE }, transport.sent.map { it.first })

            // Without room for the response context, a call whose responses stream does not offer.
            val second =
                Client(
                    transport,
                ).openServerStreaming("/svc/m", compressible, requests, responses, compressStream = CompressStream.RESPONSES)
            assertNull(transport.requests.last().streamCompression)
            second.close()
            call.close()

            val off = ScriptedTransport(Compression(emptyList()), StreamCompressionBudget(0))
            Client(off).openBidirectionalStreaming("/svc/m", requests, responses, compressStream = CompressStream.BOTH).close()
            assertNull(off.requests.single().streamCompression)
        }

    @Test
    fun `stream compression applies only to the streamed directions it is enabled for`() =
        runBlocking {
            val transport = ScriptedTransport(Compression(emptyList()), StreamCompressionBudget())
            // A client-streaming call offers so the server can answer, but a stream-compressed
            // response is still invalid: its response direction does not stream.
            val upload = Client(transport).openClientStreaming("/svc/m", requests, responses, compressStream = CompressStream.BOTH)
            assertEquals(StreamCompression(16u, optimistic = true), transport.requests.single().streamCompression)
            upload.close()

            val plain = Client(transport).openBidirectionalStreaming("/svc/m", requests, responses)
            assertNull(transport.requests.last().streamCompression)
            transport.respond(streamFrame(vectors.getValue("stream.plain.0.payload")))
            val error = assertThrows(TrevRpcException::class.java) { runBlocking { plain.receive() } }
            assertEquals(Code.INTERNAL, error.status.code)
            plain.close()

            // Without a compression configuration there is no stream compression either.
            val unconfigured = ScriptedTransport(null, StreamCompressionBudget())
            Client(unconfigured).openBidirectionalStreaming("/svc/m", requests, responses, compressStream = CompressStream.BOTH).close()
            assertNull(unconfigured.requests.single().streamCompression)
        }

    @Test
    fun `a request stream starts from the dictionary the answer confirmed`() =
        runBlocking {
            val compression = Compression(listOf(Dictionary(dictionary, listOf("test.Request"))))
            val transport = ScriptedTransport(compression, StreamCompressionBudget())
            val call = Client(transport).openBidirectionalStreaming("/svc/m", requests, responses, compressStream = CompressStream.REQUESTS)
            transport.respond(
                ResponseFrame.Headers(
                    dictionaries = listOf(DictionaryInfo(1u, vectors.getValue("dict.d1.fingerprint"))),
                    streamCompression = StreamCompression(16u),
                ),
            )
            call.headers()
            val messages = (0..2).map { vectors.getValue("stream.seeded.$it.message") }
            messages.forEach { call.send(it) }
            val sent = transport.sent.last()
            assertEquals(FrameType.ZSTD_STREAM_MESSAGE, sent.first)
            // A context seeded with dict.d1 writes its Dictionary_ID into the frame header.
            val chunk = streamMessage(sent.second).body
            assertEquals(1u, parseZstdStreamChunk(chunk, atFrameStart = true)?.dictionaryId)
            call.close()
        }

    /** A response direction that may be stream-compressed, accepting dict.d1. */
    private fun responseStreams(): CallStreams {
        val compression = Compression(listOf(Dictionary(dictionary, listOf("test.Response"))))
        val dictionaries = compression.call(null, "test.Response", streamsRequest = false)
        return checkNotNull(CallStreams.open(compression, StreamCompressionBudget(), CompressStream.RESPONSES, dictionaries))
    }

    private fun sequences(prefix: String): Set<String> =
        vectors.keys
            .filter { it.startsWith(prefix) && it.endsWith(".payload") }
            .map { it.removePrefix(prefix).substringBefore('.') }
            .toSet()

    private fun streamMessage(payload: ByteArray): ResponseFrame.Message =
        WireCodec.decodeResponseFrame(Frame(FrameType.ZSTD_STREAM_MESSAGE, payload)) as ResponseFrame.Message

    private fun streamFrame(payload: ByteArray): ResponseFrame = streamMessage(payload)

    private fun assertTrue(
        condition: Boolean,
        message: String? = null,
    ) = org.junit.jupiter.api.Assertions
        .assertTrue(condition, message)
}
