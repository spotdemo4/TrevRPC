package zip.trev.trevrpc

import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class OptimisticStreamCompressionTest {
    private val vectors = zstdVectors()
    private val requests = MessageCodec(ByteArray::copyOf, ByteArray::copyOf, "test.Request")
    private val responses = MessageCodec(ByteArray::copyOf, ByteArray::copyOf, "test.Response")
    private val messages = (0..4).map { vectors.getValue("stream.plain.${it % 3}.message") }

    @Test
    fun `the optimistic fields match the shared vectors byte for byte`() {
        val request = RpcRequest("/svc/m", streamCompression = StreamCompression.OPTIMISTIC)
        assertArrayEquals(vectors.getValue("request.optimistic_stream_compression.payload"), WireCodec.encode(request))
        assertArrayEquals(vectors.getValue("request.optimistic_stream_compression.frame"), encodeRequestFrames(request, null))
        assertEquals(request, WireCodec.decodeRequest(vectors.getValue("request.optimistic_stream_compression.payload")))
        val status = RpcStatus(Code.RESOURCE_EXHAUSTED.value, refusedStreamCompression = true)
        assertArrayEquals(vectors.getValue("status.refused_stream_compression.payload"), WireCodec.encode(status))
        assertEquals(status, WireCodec.decodeStatus(vectors.getValue("status.refused_stream_compression.payload")))
        val frame =
            WireCodec.decodeResponseFrame(
                Frame(FrameType.STATUS, vectors.getValue("status.refused_stream_compression.payload")),
            ) as ResponseFrame.Status
        assertTrue(frame.refusedStreamCompression)
        assertEquals(Code.RESOURCE_EXHAUSTED, frame.status.code)
    }

    @Test
    fun `an optimistic call compresses from the third message without waiting for the answer`() =
        runBlocking {
            val transport = MultiStreamTransport(Compression(emptyList()), StreamCompressionBudget())
            val client = Client(transport)
            val call = client.openClientStreaming("/svc/m", requests, responses, compressStream = CompressStream.REQUESTS)
            val stream = transport.streams.single()
            assertEquals(StreamCompression.OPTIMISTIC, stream.request.streamCompression)
            messages.forEach { call.send(it) }
            assertEquals(
                listOf(FrameType.MESSAGE, FrameType.MESSAGE) + List(3) { FrameType.ZSTD_STREAM_MESSAGE },
                stream.sent.map { it.first },
            )
            // A server decoding from the third message gets the messages back.
            val server = requestDecoder()
            stream.sent.drop(2).zip(messages.drop(2)).forEach { (sent, message) ->
                assertArrayEquals(message, server.inflate(streamMessage(sent.second), DEFAULT_MAX_FRAME_SIZE).body)
            }
            server.close()
            stream.respond(
                ResponseFrame.Answer(streamCompression = true),
                ResponseFrame.Message(messages[0]),
                ResponseFrame.Status(Status.ok()),
            )
            assertArrayEquals(messages[0], call.receive())
            assertEquals(1, transport.streams.size)

            // Directions whose requests do not stream are never optimistic.
            client.openServerStreaming("/svc/m", messages[0], requests, responses, compressStream = CompressStream.BOTH).close()
            assertEquals(
                StreamCompression.ENABLED,
                transport.streams
                    .last()
                    .request.streamCompression,
            )
        }

    @Test
    fun `a refused call is replayed without optimism after the request side closed`() = refusedCall(closeFirst = true)

    @Test
    fun `a refused call is replayed without optimism while it still uploads`() = refusedCall(closeFirst = false)

    private fun refusedCall(closeFirst: Boolean) =
        runBlocking<Unit> {
            val budget = StreamCompressionBudget()
            val transport = MultiStreamTransport(Compression(emptyList()), budget)
            var headers: Metadata? = null
            var headerReports = 0
            var trailers: Metadata? = null
            val options =
                CallOptions(
                    metadata = Metadata.of("m" to "v".toByteArray()),
                    onResponseHeaders = {
                        headers = it
                        headerReports++
                    },
                    onResponseMetadata = { trailers = it },
                )
            val call = Client(transport).openClientStreaming("/svc/m", requests, responses, options, CompressStream.REQUESTS)
            val refused = transport.opened.receive()
            messages.take(4).forEach { call.send(it) }
            if (closeFirst) call.closeSend()
            refused.respond(ResponseFrame.Status(Status.resourceExhausted("no memory"), refusedStreamCompression = true))

            val replayed = withTimeout(5.seconds) { transport.opened.receive() }
            assertEquals(StreamCompression.ENABLED, replayed.request.streamCompression)
            assertEquals(
                refused.request.copy(streamCompression = StreamCompression.NONE),
                replayed.request.copy(streamCompression = StreamCompression.NONE),
            )
            if (!closeFirst) {
                call.send(messages[4])
                call.closeSend()
            }
            eventually { replayed.finished }
            val sent = if (closeFirst) messages.take(4) else messages
            // The replay waits for the answer, which has not come, so every message travels on its own.
            assertEquals(List(sent.size) { FrameType.MESSAGE }, replayed.sent.map { it.first })
            sent.zip(replayed.sent).forEach { (message, frame) -> assertArrayEquals(message, frame.second) }
            assertTrue(refused.closed)
            assertFalse(budget.optimistic, "a refusal stops optimistic calls")

            replayed.respond(
                ResponseFrame.Headers(Metadata.of("h" to "replayed".toByteArray())),
                ResponseFrame.Message(messages[0]),
                ResponseFrame.Status(Status.ok(Metadata.of("t" to "replayed".toByteArray()))),
            )
            assertArrayEquals(messages[0], call.receive())
            assertEquals("replayed", headers?.get("h")?.decodeToString())
            assertEquals(1, headerReports)
            assertEquals("replayed", trailers?.get("t")?.decodeToString())
            call.close()
            val everything = budget.reserve(budget.bytes)
            assertNotNull(everything, "every reservation is released when the call ends")
            everything?.release()
        }

    @Test
    fun `a write that fails before the refusal is read is resent on the replay`() =
        runBlocking<Unit> {
            val transport = MultiStreamTransport(Compression(emptyList()), StreamCompressionBudget())
            val call = Client(transport).openClientStreaming("/svc/m", requests, responses, compressStream = CompressStream.REQUESTS)
            val refused = transport.opened.receive()
            messages.take(2).forEach { call.send(it) }
            // The server refused and stopped reading: the next write fails with its status before
            // the response pump has read the refusal.
            val refusal = Status.resourceExhausted("stream-compressed requests refused")
            refused.sendFailure = TrevRpcException(refusal)
            val respond =
                async {
                    delay(100.milliseconds)
                    refused.respond(ResponseFrame.Status(refusal, refusedStreamCompression = true))
                }
            withTimeout(5.seconds) { call.send(messages[2]) }
            respond.await()

            val replayed = withTimeout(5.seconds) { transport.opened.receive() }
            call.closeSend()
            eventually { replayed.finished }
            // Every message reaches the replayed stream once, the one whose write failed included.
            messages.take(3).zip(replayed.sent).forEach { (message, frame) -> assertArrayEquals(message, frame.second) }
            assertEquals(3, replayed.sent.size)
            replayed.respond(ResponseFrame.Message(messages[0]), ResponseFrame.Status(Status.ok()))
            assertArrayEquals(messages[0], call.receive())
            call.close()
        }

    @Test
    fun `calls stop being optimistic after a refusal until a call is answered`() =
        runBlocking {
            val budget = StreamCompressionBudget()
            val transport = MultiStreamTransport(Compression(emptyList()), budget)
            val client = Client(transport)
            budget.optimistic = false
            val waiting = client.openClientStreaming("/svc/m", requests, responses, compressStream = CompressStream.REQUESTS)
            assertEquals(
                StreamCompression.ENABLED,
                transport.streams
                    .last()
                    .request.streamCompression,
            )
            transport.streams.last().respond(ResponseFrame.Answer(streamCompression = true), ResponseFrame.Headers())
            waiting.headers()
            assertTrue(budget.optimistic, "an answer resumes optimistic calls")
            waiting.close()

            client.openClientStreaming("/svc/m", requests, responses, compressStream = CompressStream.REQUESTS).close()
            assertEquals(
                StreamCompression.OPTIMISTIC,
                transport.streams
                    .last()
                    .request.streamCompression,
            )

            // HEADERS without an ANSWER neither resume nor stop them.
            budget.optimistic = false
            val unanswered = client.openClientStreaming("/svc/m", requests, responses, compressStream = CompressStream.REQUESTS)
            transport.streams.last().respond(ResponseFrame.Headers())
            unanswered.headers()
            assertFalse(budget.optimistic)
            unanswered.close()
        }

    @Test
    fun `a replayed call carries the time left before the original deadline`() =
        runBlocking {
            val transport = MultiStreamTransport(Compression(emptyList()), StreamCompressionBudget())
            val options = CallOptions(timeout = 10.seconds)
            val call = Client(transport).openClientStreaming("/svc/m", requests, responses, options, CompressStream.REQUESTS)
            val refused = transport.opened.receive()
            delay(50.milliseconds)
            refused.respond(ResponseFrame.Status(Status.resourceExhausted("no memory"), refusedStreamCompression = true))
            val replayed = withTimeout(5.seconds) { transport.opened.receive() }
            val original = refused.request.timeoutNanos
            assertEquals(10.seconds.inWholeNanoseconds.toULong(), original)
            assertTrue(replayed.request.timeoutNanos in 1uL..<original - 50.milliseconds.inWholeNanoseconds.toULong())
            call.close()
        }

    @Test
    fun `closing a call while it replays cancels the replay`() =
        runBlocking<Unit> {
            val budget = StreamCompressionBudget()
            val transport = MultiStreamTransport(Compression(emptyList()), budget, gatedFrom = 1)
            val call = Client(transport).openClientStreaming("/svc/m", requests, responses, compressStream = CompressStream.REQUESTS)
            val refused = transport.opened.receive()
            call.send(messages[0])
            refused.respond(ResponseFrame.Status(Status.resourceExhausted("no memory"), refusedStreamCompression = true))
            eventually { refused.closed }
            call.close()
            val error = assertThrows(TrevRpcException::class.java) { runBlocking { call.send(messages[1]) } }
            assertEquals(Code.CANCELLED, error.status.code)
            transport.gate.complete(Unit)
            // The replayed stream, if it still opened, is closed with the call.
            delay(100.milliseconds)
            transport.streams.drop(1).forEach { assertTrue(it.closed) }
            val everything = budget.reserve(budget.bytes)
            assertNotNull(everything, "every reservation is released when the call ends")
            everything?.release()
        }

    @Test
    fun `a refused bidirectional call is replayed before any response`() =
        runBlocking {
            val transport = MultiStreamTransport(Compression(emptyList()), StreamCompressionBudget())
            val call = Client(transport).openBidirectionalStreaming("/svc/m", requests, responses, compressStream = CompressStream.BOTH)
            val refused = transport.opened.receive()
            assertEquals(StreamCompression.OPTIMISTIC, refused.request.streamCompression)
            messages.take(3).forEach { call.send(it) }
            refused.respond(ResponseFrame.Status(Status.resourceExhausted("no memory"), refusedStreamCompression = true))
            val replayed = withTimeout(5.seconds) { transport.opened.receive() }
            eventually { replayed.sent.size == 3 }
            replayed.respond(ResponseFrame.Headers(), ResponseFrame.Message(messages[1]), ResponseFrame.Status(Status.ok()))
            assertArrayEquals(messages[1], call.receive())
            assertNull(call.receive())
            assertEquals(Code.OK, call.terminalStatus?.code)
        }

    @Test
    fun `a refusal is only a refusal as the first frame of an optimistic call`() =
        runBlocking {
            val transport = MultiStreamTransport(Compression(emptyList()), StreamCompressionBudget())
            val call = Client(transport).openClientStreaming("/svc/m", requests, responses, compressStream = CompressStream.REQUESTS)
            transport.streams.single().respond(
                ResponseFrame.Answer(streamCompression = true),
                ResponseFrame.Status(Status.resourceExhausted("quota"), refusedStreamCompression = true),
            )
            val error = assertThrows(TrevRpcException::class.java) { runBlocking { call.receive() } }
            assertEquals(Code.RESOURCE_EXHAUSTED, error.status.code)
            assertEquals(1, transport.streams.size)
        }

    @Test
    fun `an optimistic call must be accepted by an ANSWER before anything else`() =
        runBlocking {
            listOf(
                listOf(ResponseFrame.Headers(), ResponseFrame.Status(Status.ok())),
                listOf(ResponseFrame.Answer(), ResponseFrame.Status(Status.ok())),
                listOf(ResponseFrame.Message(messages[0]), ResponseFrame.Status(Status.ok())),
            ).forEach { response ->
                val transport = MultiStreamTransport(Compression(emptyList()), StreamCompressionBudget())
                val call = Client(transport).openClientStreaming("/svc/m", requests, responses, compressStream = CompressStream.REQUESTS)
                transport.streams.single().respond(*response.toTypedArray())
                val error = assertThrows(TrevRpcException::class.java) { runBlocking { call.receive() } }
                assertEquals(Code.INTERNAL, error.status.code)
                assertEquals(1, transport.streams.size)
            }
        }

    @Test
    fun `sends wait for the first response frame when the copies have no room`() =
        runBlocking {
            // Room in the budget for one 600-byte copy, and no compressing context.
            val budget = StreamCompressionBudget(1000)
            val transport = MultiStreamTransport(Compression(emptyList()), budget)
            val call = Client(transport).openClientStreaming("/svc/m", requests, responses, compressStream = CompressStream.REQUESTS)
            val stream = transport.streams.single()
            call.send(ByteArray(600))
            val blocked = async(start = CoroutineStart.UNDISPATCHED) { call.send(ByteArray(600)) }
            delay(100.milliseconds)
            assertFalse(blocked.isCompleted)
            assertEquals(1, stream.sent.size)
            stream.respond(ResponseFrame.Answer(streamCompression = true))
            withTimeout(5.seconds) { blocked.await() }
            assertEquals(2, stream.sent.size)
            call.close()

            // A message beyond the copy cap waits too, whatever the budget.
            val large = MultiStreamTransport(Compression(emptyList()), StreamCompressionBudget())
            val bigCall = Client(large).openClientStreaming("/svc/m", requests, responses, compressStream = CompressStream.REQUESTS)
            val bigStream = large.streams.single()
            val big = async(start = CoroutineStart.UNDISPATCHED) { bigCall.send(ByteArray(OPTIMISTIC_COPY_CAP.toInt() + 1)) }
            delay(100.milliseconds)
            assertFalse(big.isCompleted)
            bigStream.respond(ResponseFrame.Answer(streamCompression = true))
            withTimeout(5.seconds) { big.await() }
            assertEquals(1, bigStream.sent.size)
            bigCall.close()
        }

    /** The server's side of a request direction that may be stream-compressed. */
    private fun requestDecoder(): CallStreams {
        val compression = Compression(emptyList())
        return checkNotNull(
            CallStreams.open(compression, StreamCompressionBudget(), CompressStream.RESPONSES, compression.call(null, null, false)),
        )
    }

    private fun streamMessage(payload: ByteArray): ResponseFrame.Message =
        WireCodec.decodeResponseFrame(Frame(FrameType.ZSTD_STREAM_MESSAGE, payload)) as ResponseFrame.Message

    private suspend fun eventually(condition: () -> Boolean) {
        withTimeout(5.seconds) {
            while (!condition()) delay(5.milliseconds)
        }
    }
}
