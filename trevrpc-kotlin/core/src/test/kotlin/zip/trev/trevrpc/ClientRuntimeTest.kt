package zip.trev.trevrpc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.yield
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class ClientRuntimeTest {
    private val strings = MessageCodec<String>(String::encodeToByteArray, ByteArray::decodeToString)

    @Test
    fun `all four shapes encode requests and decode responses`() =
        runBlocking {
            val client = Client(LoopbackTransport())

            val response = ResponseCapture()
            val unary = client.unary("/test.Service/Unary", "x", strings, strings, response.options())
            assertEquals("u:x", unary)
            assertEquals(1, response.metadata["result"]?.single())
            assertEquals(2, response.headers["server"]?.single())
            assertNull(response.headers["result"])
            assertNull(response.metadata["server"])

            val serverStream = client.openServerStreaming("/test.Service/ServerStream", "s", strings, strings)
            assertEquals(2, serverStream.headers()["server"]?.single())
            assertEquals("s-1", serverStream.receive())
            assertEquals("s-2", serverStream.receive())
            assertNull(serverStream.receive())
            assertEquals(Code.OK, serverStream.terminalStatus?.code)

            val upload = ResponseCapture()
            val clientStream =
                client.openClientStreaming("/test.Service/ClientStream", strings, strings, upload.options())
            clientStream.send("a")
            clientStream.send("b")
            clientStream.closeSend()
            assertEquals("a,b", clientStream.receive())
            assertEquals(2, upload.headers["server"]?.single())
            assertEquals(1, upload.metadata["result"]?.single())

            val bidi = client.openBidirectionalStreaming("/test.Service/Bidi", strings, strings)
            bidi.send("a")
            assertEquals(2, bidi.headers()["server"]?.single())
            assertEquals("echo:a", bidi.receive())
            bidi.send("b")
            bidi.closeSend()
            assertEquals("echo:b", bidi.receive())
            assertNull(bidi.receive())
            assertEquals(Code.OK, bidi.terminalStatus?.code)
        }

    @Test
    fun `failed terminal status is thrown and keeps its metadata`() =
        runBlocking {
            val trace = Metadata.of("trace" to byteArrayOf(9))
            val call =
                Client(StaticStreamTransport(status(Status(Code.ABORTED, "stream failed", trace))))
                    .openServerStreaming("/test.Service/Fail", "request", strings, strings)

            val failure = assertThrows(TrevRpcException::class.java) { runBlocking { call.receive() } }

            assertEquals(Code.ABORTED, failure.status.code)
            assertEquals(trace, call.responseMetadata)
        }

    @Test
    fun `streaming calls send directly and finish exactly once`() =
        runTest {
            val stream =
                // Like a client-streaming server, it answers once the request side is closed.
                RecordingClientStream(
                    message("reply"),
                    status(Status.ok()),
                    responseAfterFinish = true,
                )
            val call =
                testClient(SingleStreamTransport(stream))
                    .openClientStreaming<String, String>("/svc/method", strings, strings)

            call.send("request")
            assertEquals(1, stream.sent.size)
            assertArrayEquals("request".encodeToByteArray(), stream.sent.single())
            call.send("second")
            assertEquals(listOf("request", "second"), stream.sent.map(ByteArray::decodeToString))
            call.closeSend()
            call.closeSend()
            assertEquals(1, stream.finishes)
            assertCode(Code.CANCELLED) { call.send("late") }
            assertEquals("reply", call.receive())
            assertEquals(1, stream.closes)

            val serverStream = RecordingClientStream(status(Status.ok()))
            val transport = SingleStreamTransport(serverStream)
            testClient(transport).openServerStreaming("/svc/method", "request", strings, strings)
            // REQUEST, the only MESSAGE, and the half-close travel in the one open; nothing is sent after.
            assertEquals(listOf("request"), transport.firstMessages.map { it?.decodeToString() })
            assertEquals(listOf(true), transport.finishSends)
            assertEquals(RpcKind.SERVER_STREAMING.value, transport.requests.single().kindValue)
            assertEquals(0, serverStream.finishes)
            assertEquals(0, serverStream.sent.size)
        }

    @Test
    fun `unary writes request and message together with half close and no idle timeout`() =
        runTest {
            val stream = RecordingClientStream(message("reply"), status(Status.ok()))
            val transport = SingleStreamTransport(stream)
            val response =
                testClient(
                    transport,
                ).unary("/svc/method", "request", strings, strings, CallOptions(streamIdleTimeout = 1.milliseconds))
            assertEquals("reply", response)
            assertEquals(listOf("request"), transport.firstMessages.map { it?.decodeToString() })
            assertEquals(listOf(true), transport.finishSends)
            assertEquals(RpcKind.UNARY.value, transport.requests.single().kindValue)
            assertEquals(1, stream.closes)
        }

    @Test
    fun `client and bidirectional streaming open without a message or half close`() =
        runTest {
            val transport = SingleStreamTransport(RecordingClientStream())
            val client = testClient(transport)
            client.openClientStreaming<String, String>("/svc/method", strings, strings)
            client.openBidirectionalStreaming<String, String>("/svc/method", strings, strings)
            assertEquals(listOf(null, null), transport.firstMessages)
            assertEquals(listOf(false, false), transport.finishSends)
            assertEquals(
                listOf(RpcKind.CLIENT_STREAMING.value, RpcKind.BIDIRECTIONAL_STREAMING.value),
                transport.requests.map(RpcRequest::kindValue),
            )
        }

    @Test
    fun `request writes and half close obey the RPC deadline`() =
        runTest {
            val sendCloses = AtomicInteger()
            val blockedSend =
                SequenceStream(onClose = { sendCloses.incrementAndGet() }) { awaitCancellation() }
                    .apply { sendOperation = { awaitCancellation() } }
            val upload =
                testClient(SingleStreamTransport(blockedSend))
                    .openClientStreaming<String, String>(
                        "/svc/method",
                        strings,
                        strings,
                        CallOptions(timeout = 1.seconds),
                    )
            assertCode(Code.DEADLINE_EXCEEDED) { upload.send("request") }
            assertEquals(1, sendCloses.get())

            val blockedOpen =
                object : RpcTransport {
                    override suspend fun openStream(
                        request: RpcRequest,
                        firstMessage: ByteArray?,
                        finishSend: Boolean,
                    ): RpcClientStream = awaitCancellation()
                }
            assertCode(Code.DEADLINE_EXCEEDED) {
                testClient(blockedOpen)
                    .openServerStreaming(
                        "/svc/method",
                        "request",
                        strings,
                        strings,
                        CallOptions(timeout = 1.seconds),
                    )
            }
        }

    @Test
    fun `client enforces deadline idle message body and per-message limits`() =
        runTest {
            val pendingTransport = SingleStreamTransport(SequenceStream { awaitCancellation() })
            val client = testClient(pendingTransport)
            assertCode(Code.DEADLINE_EXCEEDED) {
                client.unary(
                    "/svc/method",
                    "request",
                    strings,
                    strings,
                    CallOptions(timeout = 1.seconds),
                )
            }
            val idle =
                client.openServerStreaming(
                    "/svc/method",
                    "request",
                    strings,
                    strings,
                    CallOptions(streamIdleTimeout = 1.seconds),
                )
            assertCode(Code.UNAVAILABLE) { idle.receive() }

            val messageLimited =
                testClient(
                    StaticStreamTransport(
                        message("a"),
                        message("b"),
                        status(Status.ok()),
                    ),
                ).openServerStreaming(
                    "/svc/method",
                    "request",
                    strings,
                    strings,
                    CallOptions(maxResponseMessages = 1),
                )
            assertEquals("a", messageLimited.receive())
            assertCode(Code.RESOURCE_EXHAUSTED) { messageLimited.receive() }

            val bodyLimited =
                testClient(
                    StaticStreamTransport(
                        message("ab"),
                        message("c"),
                    ),
                ).openServerStreaming(
                    "/svc/method",
                    "request",
                    strings,
                    strings,
                    CallOptions(maxResponseStreamBodySize = 2),
                )
            assertEquals("ab", bodyLimited.receive())
            assertCode(Code.RESOURCE_EXHAUSTED) { bodyLimited.receive() }

            val frameLimited =
                testClient(
                    StaticStreamTransport(
                        message("ab"),
                    ),
                ).openServerStreaming(
                    "/svc/method",
                    "request",
                    strings,
                    strings,
                    CallOptions(maxResponseBodySize = 1),
                )
            assertCode(Code.RESOURCE_EXHAUSTED) { frameLimited.receive() }
        }

    @Test
    fun `terminal status metadata eof malformed shape and remote precedence are enforced`() =
        runTest {
            val closes = AtomicInteger()
            val terminalFrames =
                listOf(
                    message("ok"),
                    status(Status.ok(Metadata.of("trace" to byteArrayOf(7)))),
                ).iterator()
            val terminal =
                testClient(
                    SingleStreamTransport(
                        SequenceStream(onClose = { closes.incrementAndGet() }) {
                            if (terminalFrames.hasNext()) terminalFrames.next() else null
                        },
                    ),
                ).openServerStreaming("/svc/method", "request", strings, strings)
            assertEquals("ok", terminal.receive())
            assertNull(terminal.receive())
            assertEquals(7, terminal.responseMetadata["trace"]?.single())
            assertEquals(1, closes.get())

            val eof = testClient(StaticStreamTransport()).openServerStreaming("/svc/method", "request", strings, strings)
            assertCode(Code.INTERNAL) { eof.receive() }

            val trailingCloses = AtomicInteger()
            val trailingFrames =
                listOf(
                    status(Status.unavailable("remote")),
                    message("late"),
                ).iterator()
            val trailing =
                testClient(
                    SingleStreamTransport(
                        SequenceStream(onClose = { trailingCloses.incrementAndGet() }) {
                            if (trailingFrames.hasNext()) trailingFrames.next() else null
                        },
                    ),
                ).openServerStreaming("/svc/method", "request", strings, strings)
            assertCode(Code.INTERNAL) { trailing.receive() }
            assertEquals(1, trailingCloses.get())

            var pendingTerminalReads = 0
            val pendingTerminal =
                testClient(
                    SingleStreamTransport(
                        SequenceStream {
                            if (pendingTerminalReads++ == 0) status(Status.ok()) else awaitCancellation()
                        },
                    ),
                ).openServerStreaming(
                    "/svc/method",
                    "request",
                    strings,
                    strings,
                    CallOptions(streamIdleTimeout = 1.milliseconds),
                )
            assertCode(Code.UNAVAILABLE) { pendingTerminal.receive() }
            assertNull(pendingTerminal.terminalStatus)

            var malformedTrailingReads = 0
            val malformedTrailing =
                testClient(
                    SingleStreamTransport(
                        SequenceStream {
                            if (malformedTrailingReads++ == 0) {
                                status(Status.ok())
                            } else {
                                throw TrevRpcException(Status.internal("malformed trailing protobuf"))
                            }
                        },
                    ),
                ).openServerStreaming("/svc/method", "request", strings, strings)
            assertStatus(Code.INTERNAL, "response stream contained data after final status") {
                malformedTrailing.receive()
            }
            assertNull(malformedTrailing.terminalStatus)

            val multiple =
                testClient(
                    StaticStreamTransport(
                        message("one"),
                        message("two"),
                        status(Status.ok()),
                    ),
                ).openClientStreaming<String, String>("/svc/method", strings, strings)
            assertStatus(
                Code.INTERNAL,
                "client-streaming RPC returned more than one response message",
            ) { multiple.receive() }

            val multipleWithoutStatus =
                testClient(
                    StaticStreamTransport(
                        message("one"),
                        message("two"),
                    ),
                ).openClientStreaming<String, String>("/svc/method", strings, strings)
            assertStatus(Code.INTERNAL, "response stream ended before final status") {
                multipleWithoutStatus.receive()
            }

            val multipleThenRemote =
                testClient(
                    StaticStreamTransport(
                        message("one"),
                        message("two"),
                        status(Status.unavailable("remote")),
                    ),
                ).openClientStreaming<String, String>("/svc/method", strings, strings)
            assertStatus(Code.UNAVAILABLE, "remote") { multipleThenRemote.receive() }

            val multipleThenTrailing =
                testClient(
                    StaticStreamTransport(
                        message("one"),
                        message("two"),
                        status(Status.ok()),
                        message("late"),
                    ),
                ).openClientStreaming<String, String>("/svc/method", strings, strings)
            assertStatus(Code.INTERNAL, "response stream contained data after final status") {
                multipleThenTrailing.receive()
            }

            val oneThenRemote =
                testClient(
                    StaticStreamTransport(
                        message("one"),
                        status(Status.unavailable("remote")),
                    ),
                ).openClientStreaming<String, String>("/svc/method", strings, strings)
            assertCode(Code.UNAVAILABLE) { oneThenRemote.receive() }

            val messageWithoutStatus =
                testClient(StaticStreamTransport(message("one")))
                    .openClientStreaming<String, String>("/svc/method", strings, strings)
            assertCode(Code.INTERNAL) { messageWithoutStatus.receive() }

            val remote = testClient(StaticStreamTransport(status(Status.unavailable("remote down"))))
            assertCode(Code.UNAVAILABLE) {
                remote.unary(
                    "/svc/method",
                    "request",
                    strings,
                    strings,
                    CallOptions(maxResponseBodySize = 1),
                )
            }
        }

    @Test
    fun `early and repeated close cancel transport exactly once`() =
        runTest {
            val closes = AtomicInteger()
            val transport =
                SingleStreamTransport(SequenceStream(onClose = { closes.incrementAndGet() }) { awaitCancellation() })
            val call = testClient(transport).openBidirectionalStreaming<String, String>("/svc/method", strings, strings)
            call.close()
            call.close()
            assertEquals(1, closes.get())
        }

    @Test
    fun `terminal status cleanup preserves coroutine cancellation`() =
        runTest {
            val stream =
                object : RpcClientStream {
                    override suspend fun send(body: ByteArray) = Unit

                    override suspend fun finishSend() = Unit

                    override suspend fun receive(): ResponseFrame = status(Status.unavailable("remote down"))

                    override suspend fun close(cause: Throwable?): Unit = throw CancellationException("close cancelled")
                }
            val call =
                testClient(SingleStreamTransport(stream))
                    .openServerStreaming("/svc/method", "request", strings, strings)
            val error = runCatching { call.receive() }.exceptionOrNull()
            assertTrue(error is CancellationException)
            assertEquals("close cancelled", error?.message)
        }

    @Test
    fun `headers resolve on the first known frame without losing a message`() =
        runTest {
            val md = Metadata.of("x-h" to byteArrayOf(4))
            val trailer = Metadata.of("x-t" to byteArrayOf(5))
            val call =
                testClient(StaticStreamTransport(headers(md), message("a"), message("b"), status(Status.ok(trailer))))
                    .openServerStreaming("/svc/method", "request", strings, strings)

            assertEquals(md, call.headers())
            assertEquals(md, call.headers())
            assertEquals(Metadata.EMPTY, call.responseMetadata)
            assertEquals("a", call.receive())
            assertEquals("b", call.receive())
            assertNull(call.receive())
            assertEquals(md, call.headers())
            assertEquals(trailer, call.responseMetadata)
            assertEquals(trailer, call.terminalStatus?.metadata)
        }

    @Test
    fun `absent headers are empty and a message read ahead is not lost`() =
        runTest {
            val absent =
                testClient(StaticStreamTransport(message("a"), status(Status.ok())))
                    .openServerStreaming("/svc/method", "request", strings, strings)
            assertEquals(Metadata.EMPTY, absent.headers())
            assertEquals("a", absent.receive())
            assertNull(absent.receive())

            val lone =
                testClient(StaticStreamTransport(status(Status.unavailable("down"))))
                    .openServerStreaming("/svc/method", "request", strings, strings)
            assertEquals(Metadata.EMPTY, lone.headers())
            assertCode(Code.UNAVAILABLE) { lone.receive() }

            val afterReceive =
                testClient(StaticStreamTransport(headers(Metadata.of("x-h" to byteArrayOf(1))), message("a"), status(Status.ok())))
                    .openServerStreaming("/svc/method", "request", strings, strings)
            assertEquals("a", afterReceive.receive())
            assertEquals(1, afterReceive.headers()["x-h"]?.single())
        }

    @Test
    fun `headers wait for the first frame while a receive is pending`() =
        runTest {
            val frames = Channel<ResponseFrame>(Channel.UNLIMITED)
            val call =
                testClient(SingleStreamTransport(SequenceStream { frames.receiveCatching().getOrNull() }))
                    .openServerStreaming("/svc/method", "request", strings, strings)
            val receiving = async { call.receive() }
            val reading = async { call.headers() }
            repeat(3) { yield() }
            assertTrue(receiving.isActive)
            assertTrue(reading.isActive)
            frames.send(headers(Metadata.of("x-h" to byteArrayOf(9))))
            frames.send(message("a"))
            assertEquals("a", receiving.await())
            assertEquals(9, reading.await()["x-h"]?.single())
        }

    @Test
    fun `headers surface a stream failure and receive reports it too`() =
        runTest {
            val call = testClient(StaticStreamTransport()).openServerStreaming("/svc/method", "request", strings, strings)
            assertCode(Code.INTERNAL) { call.headers() }
            assertCode(Code.INTERNAL) { call.receive() }
            assertNull(call.receive())

            val idle =
                testClient(SingleStreamTransport(SequenceStream { awaitCancellation() }))
                    .openServerStreaming("/svc/method", "request", strings, strings, CallOptions(streamIdleTimeout = 1.seconds))
            assertCode(Code.UNAVAILABLE) { idle.headers() }
        }

    @Test
    fun `second HEADERS and HEADERS after a message or status are internal errors`() =
        runTest {
            val duplicate =
                testClient(StaticStreamTransport(headers(), headers(), status(Status.ok())))
                    .openServerStreaming("/svc/method", "request", strings, strings)
            assertCode(Code.INTERNAL) { duplicate.receive() }

            val afterMessage =
                testClient(StaticStreamTransport(message("a"), headers(), status(Status.ok())))
                    .openServerStreaming("/svc/method", "request", strings, strings)
            assertEquals("a", afterMessage.receive())
            assertCode(Code.INTERNAL) { afterMessage.receive() }

            val afterStatus =
                testClient(StaticStreamTransport(message("a"), status(Status.ok()), headers()))
                    .openClientStreaming<String, String>("/svc/method", strings, strings)
            assertStatus(Code.INTERNAL, "response stream contained data after final status") { afterStatus.receive() }

            val lateHeadersApi =
                testClient(StaticStreamTransport(message("a"), headers()))
                    .openServerStreaming("/svc/method", "request", strings, strings)
            assertEquals(Metadata.EMPTY, lateHeadersApi.headers())
            assertEquals("a", lateHeadersApi.receive())
            assertCode(Code.INTERNAL) { lateHeadersApi.receive() }

            val unary = testClient(StaticStreamTransport(message("a"), headers(), status(Status.ok())))
            assertCode(Code.INTERNAL) { unary.unary("/svc/method", "request", strings, strings) }
        }

    @Test
    fun `response callbacks get headers separately from trailing metadata`() =
        runTest {
            val md = Metadata.of("x-h" to byteArrayOf(1))
            val trailer = Metadata.of("x-t" to byteArrayOf(2))
            val frames = arrayOf(headers(md), message("r"), status(Status.ok(trailer)))
            val unary = ResponseCapture()
            assertEquals(
                "r",
                testClient(StaticStreamTransport(*frames)).unary("/svc/method", "request", strings, strings, unary.options()),
            )
            assertEquals(md, unary.headers)
            assertEquals(trailer, unary.metadata)

            val upload = ResponseCapture()
            val call =
                testClient(StaticStreamTransport(*frames))
                    .openClientStreaming<String, String>("/svc/method", strings, strings, upload.options())
            assertEquals("r", call.receive())
            assertEquals(md, upload.headers)
            assertEquals(trailer, upload.metadata)

            val noHeaders = ResponseCapture()
            testClient(StaticStreamTransport(message("r"), status(Status.ok(trailer))))
                .unary("/svc/method", "request", strings, strings, noHeaders.options())
            assertEquals(Metadata.EMPTY, noHeaders.headers)
            assertEquals(trailer, noHeaders.metadata)

            val failed = ResponseCapture()
            assertCode(Code.UNAVAILABLE) {
                testClient(StaticStreamTransport(headers(md), status(Status.unavailable("down"))))
                    .unary("/svc/method", "request", strings, strings, failed.options())
            }
            assertEquals(md, failed.headers)
            assertNull(failed.metadataOrNull)
        }

    @Test
    fun `flows report response metadata and a throwing callback fails the call`() =
        runTest {
            val md = Metadata.of("x-h" to byteArrayOf(1))
            val trailer = Metadata.of("x-t" to byteArrayOf(2))
            val frames = arrayOf(headers(md), message("a"), message("b"), status(Status.ok(trailer)))
            val stream = ResponseCapture()
            val replies =
                testClient(StaticStreamTransport(*frames))
                    .serverStreaming("/svc/method", "request", strings, strings, stream.options())
                    .toList()
            assertEquals(listOf("a", "b"), replies)
            assertEquals(md, stream.headers)
            assertEquals(trailer, stream.metadata)

            val throwing = CallOptions(onResponseMetadata = { throw IllegalStateException("rejected") })
            val error =
                runCatching {
                    testClient(StaticStreamTransport(*frames))
                        .serverStreaming("/svc/method", "request", strings, strings, throwing)
                        .toList()
                }.exceptionOrNull()
            assertEquals("rejected", error?.message)
        }

    /**
     * Runs the call's response pump on the test dispatcher, so virtual time cannot expire an idle
     * timeout while a pump on another thread is still reading.
     */
    private fun TestScope.testClient(transport: RpcTransport): Client = Client(transport, coroutineContext)

    private suspend fun assertCode(
        code: Code,
        block: suspend () -> Unit,
    ) {
        assertEquals(code, captureTrevRpcException(code, block).status.code)
    }

    private suspend fun assertStatus(
        code: Code,
        message: String,
        block: suspend () -> Unit,
    ) {
        val error = captureTrevRpcException(code, block)
        assertEquals(code, error.status.code)
        assertEquals(message, error.status.message)
    }

    private suspend fun captureTrevRpcException(
        code: Code,
        block: suspend () -> Unit,
    ): TrevRpcException =
        try {
            block()
            throw AssertionError("expected TrevRpcException with code $code")
        } catch (error: TrevRpcException) {
            error
        }
}

private fun message(text: String): ResponseFrame = ResponseFrame.Message(text.encodeToByteArray())

private fun status(status: Status): ResponseFrame = ResponseFrame.Status(status)

private fun headers(metadata: Metadata = Metadata.EMPTY): ResponseFrame = ResponseFrame.Headers(metadata)

private class LoopbackTransport : RpcTransport {
    override suspend fun openStream(
        request: RpcRequest,
        firstMessage: ByteArray?,
        finishSend: Boolean,
    ): RpcClientStream {
        val frames = Channel<ResponseFrame>(Channel.UNLIMITED)
        val kind = request.kind
        val serverHeader = Metadata.of("server" to byteArrayOf(2))
        if (kind == RpcKind.UNARY) {
            val value = checkNotNull(firstMessage).decodeToString()
            frames.trySend(headers(serverHeader))
            frames.trySend(message("u:$value"))
            frames.trySend(status(Status.ok(Metadata.of("result" to byteArrayOf(1)))))
            frames.close()
        } else if (kind == RpcKind.SERVER_STREAMING) {
            val value = checkNotNull(firstMessage).decodeToString()
            frames.trySend(headers(serverHeader))
            frames.trySend(message("$value-1"))
            frames.trySend(message("$value-2"))
            frames.trySend(status(Status.ok()))
            frames.close()
        } else {
            frames.trySend(headers(serverHeader))
        }
        val uploaded = mutableListOf<String>()
        return object : RpcClientStream {
            override suspend fun send(body: ByteArray) {
                if (kind == RpcKind.BIDIRECTIONAL_STREAMING) {
                    frames.send(message("echo:${body.decodeToString()}"))
                } else {
                    uploaded += body.decodeToString()
                }
            }

            override suspend fun finishSend() {
                if (kind == RpcKind.CLIENT_STREAMING) {
                    frames.send(message(uploaded.joinToString(",")))
                }
                if (kind == RpcKind.CLIENT_STREAMING || kind == RpcKind.BIDIRECTIONAL_STREAMING) {
                    frames.send(status(Status.ok(Metadata.of("result" to byteArrayOf(1)))))
                    frames.close()
                }
            }

            override suspend fun receive(): ResponseFrame? = frames.receiveCatching().getOrNull()

            override suspend fun close(cause: Throwable?) {
                frames.close()
            }
        }
    }
}

private class StaticStreamTransport(
    vararg frames: ResponseFrame,
) : RpcTransport {
    private val frames = frames.toList()

    override suspend fun openStream(
        request: RpcRequest,
        firstMessage: ByteArray?,
        finishSend: Boolean,
    ): RpcClientStream {
        val iterator = frames.iterator()
        return SequenceStream { if (iterator.hasNext()) iterator.next() else null }
    }
}

private class SingleStreamTransport(
    private val stream: RpcClientStream,
) : RpcTransport {
    val requests = mutableListOf<RpcRequest>()
    val firstMessages = mutableListOf<ByteArray?>()
    val finishSends = mutableListOf<Boolean>()

    override suspend fun openStream(
        request: RpcRequest,
        firstMessage: ByteArray?,
        finishSend: Boolean,
    ): RpcClientStream {
        requests += request
        firstMessages += firstMessage
        finishSends += finishSend
        return stream
    }
}

private class SequenceStream(
    private val onClose: () -> Unit = {},
    private val next: suspend () -> ResponseFrame?,
) : RpcClientStream {
    var sendOperation: suspend () -> Unit = {}
    var finishOperation: suspend () -> Unit = {}

    override suspend fun send(body: ByteArray) = sendOperation()

    override suspend fun finishSend() = finishOperation()

    override suspend fun receive(): ResponseFrame? = next()

    override suspend fun close(cause: Throwable?) {
        onClose()
    }
}

private class RecordingClientStream(
    vararg frames: ResponseFrame,
    responseAfterFinish: Boolean = false,
) : RpcClientStream {
    private val frames = frames.iterator()
    private val responseReleased = CompletableDeferred<Unit>().apply { if (!responseAfterFinish) complete(Unit) }
    private var sendFinished = false
    val sent = mutableListOf<ByteArray>()
    var finishes = 0
        private set
    var closes = 0
        private set

    override suspend fun send(body: ByteArray) {
        if (sendFinished) throw TrevRpcException(Status.cancelled("request stream is closed"))
        sent += body.copyOf()
    }

    override suspend fun finishSend() {
        if (sendFinished) return
        sendFinished = true
        finishes++
        responseReleased.complete(Unit)
    }

    override suspend fun receive(): ResponseFrame? {
        responseReleased.await()
        return if (frames.hasNext()) frames.next() else null
    }

    override suspend fun close(cause: Throwable?) {
        closes++
    }
}

/** Records the response headers and trailing metadata a call reports to its callbacks. */
internal class ResponseCapture {
    @Volatile
    var headersOrNull: Metadata? = null

    @Volatile
    var metadataOrNull: Metadata? = null

    val headers: Metadata
        get() = checkNotNull(headersOrNull) { "no response headers were reported" }

    val metadata: Metadata
        get() = checkNotNull(metadataOrNull) { "no trailing metadata was reported" }

    fun options(base: CallOptions = CallOptions()): CallOptions =
        base.copy(onResponseHeaders = { headersOrNull = it }, onResponseMetadata = { metadataOrNull = it })
}
