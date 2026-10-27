package zip.trev.trevrpc.cronet

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.yield
import org.chromium.net.BidirectionalStream
import org.chromium.net.CronetEngine
import org.chromium.net.UrlRequest
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import zip.trev.trevrpc.CallOptions
import zip.trev.trevrpc.Client
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.ConnectionMetadata
import zip.trev.trevrpc.DEFAULT_STREAM_COMPRESSION_MEMORY
import zip.trev.trevrpc.Frame
import zip.trev.trevrpc.FrameDecoder
import zip.trev.trevrpc.FrameType
import zip.trev.trevrpc.MessageCodec
import zip.trev.trevrpc.Metadata
import zip.trev.trevrpc.QuicVarint
import zip.trev.trevrpc.ResponseFrame
import zip.trev.trevrpc.RpcClientStream
import zip.trev.trevrpc.RpcHeaders
import zip.trev.trevrpc.RpcKind
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcStatus
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.WireCodec
import zip.trev.trevrpc.encodeFrame
import java.net.URL
import java.net.URLConnection
import java.net.URLStreamHandlerFactory
import java.nio.ByteBuffer
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime

@OptIn(ExperimentalCoroutinesApi::class)
class Http3RpcTransportTest {
    @Test
    fun `closed channel rejects new calls and close is idempotent`() =
        runTest {
            val channel =
                Http3RpcTransport(
                    FakeFactory(),
                    coroutineContext = coroutineContext,
                    shutdownContext = coroutineContext,
                )

            channel.close()
            channel.close()

            val error = runCatching { channel.openStream(RpcRequest("/svc/method"), byteArrayOf(), true) }.exceptionOrNull()
            assertEquals(Code.UNAVAILABLE, (error as TrevRpcException).status.code)
        }

    @Test
    fun `unary writes one request and decodes one response`() =
        runTest {
            val fake = FakeFactory()
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext)
            val request = RpcRequest("/example.Service/Unary")
            val call = async { transport.openStream(request, byteArrayOf(1, 2), true) }

            runCurrent()
            fake.ready()
            runCurrent()
            val requestWrite = fake.onlyWrite()
            // REQUEST, the only MESSAGE and the half-close share one write.
            assertTrue(requestWrite.endOfStream)
            val written = FrameDecoder().feed(requestWrite.bytes)
            assertEquals(listOf(FrameType.REQUEST, FrameType.MESSAGE), written.map(Frame::type))
            assertRequestEquals(request, WireCodec.decodeRequest(written[0].payload))
            assertArrayEquals(byteArrayOf(1, 2), written[1].payload)
            fake.completeWrite()
            val stream = call.await()

            fake.headers(validHeaders())
            fake.deliverRead(
                headersFrame(Metadata.of("x-h" to byteArrayOf(1))) + messageFrame(byteArrayOf(7, 8, 9)) + statusFrame(Status.ok()),
                endOfStream = true,
            )
            runCurrent()

            assertEquals(1, (stream.receive() as ResponseFrame.Headers).metadata["x-h"]?.single()?.toInt())
            assertArrayEquals(byteArrayOf(7, 8, 9), (stream.receive() as ResponseFrame.Message).body)
            assertTrue((stream.receive() as ResponseFrame.Status).status.isOk)
            assertNull(stream.receive())
            assertTrue(fake.awaitCancel())
            assertEquals(1, fake.cancelCalls)
        }

    @Test
    fun `the channel offers its connection metadata, which can be replaced and removed`() =
        runTest {
            fun set(value: String) = Metadata.of("authorization" to value.encodeToByteArray())
            val options = CronetTransportOptions(connectionMetadata = set("a"))
            val transport = Http3RpcTransport(FakeFactory(), options, coroutineContext, shutdownContext = coroutineContext)

            assertEquals(ConnectionMetadata(1u, set("a")), transport.connectionMetadata)
            transport.setConnectionMetadata(set("b"))
            assertEquals(ConnectionMetadata(2u, set("b")), transport.connectionMetadata)
            transport.setConnectionMetadata(Metadata.EMPTY)
            assertNull(transport.connectionMetadata)
            assertNull(Http3RpcTransport(FakeFactory(), coroutineContext = coroutineContext).connectionMetadata)
            transport.close()
        }

    @Test
    fun `headers may precede ready and reads split and coalesced frames`() =
        runTest {
            val fake = FakeFactory()
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext)
            val request =
                RpcRequest(
                    "/example.Service/Bidi",
                    kindValue = RpcKind.BIDIRECTIONAL_STREAMING.value,
                )
            val opening = async { transport.openStream(request) }
            runCurrent()

            fake.headers(validHeaders("Application/TrevRPC"))
            assertEquals(1, fake.pendingReads)
            fake.ready()
            runCurrent()
            assertRequestEquals(request, decodeRequestFrame(fake.onlyWrite().bytes))
            assertFalse(opening.isCompleted)
            fake.completeWrite()
            val stream = opening.await()

            val responseBytes = messageFrame(byteArrayOf(4, 5)) + statusFrame(Status.ok())
            fake.deliverRead(responseBytes.copyOfRange(0, 3))
            runCurrent()
            assertEquals(1, fake.pendingReads)
            fake.deliverRead(responseBytes.copyOfRange(3, responseBytes.size))
            runCurrent()

            val actualMessage = stream.receive()
            val actualStatus = stream.receive()
            assertArrayEquals(byteArrayOf(4, 5), (actualMessage as ResponseFrame.Message).body)
            assertInstanceOf(ResponseFrame.Status::class.java, actualStatus)
            assertNull(stream.receive())
            assertTrue(fake.awaitCancel())
            assertEquals(1, fake.cancelCalls)
        }

    @Test
    fun `send waits for direct completion and serializes with finish`() =
        runTest {
            val fake = FakeFactory()
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext)
            val stream = openStreaming(transport, fake)

            val first = async { stream.send(byteArrayOf(1)) }
            val second = async { stream.send(byteArrayOf(2)) }
            val finish = async { stream.finishSend() }
            runCurrent()

            assertMessageWrite(fake.onlyWrite().bytes, byteArrayOf(1))
            assertFalse(first.isCompleted)
            assertFalse(second.isCompleted)
            assertFalse(finish.isCompleted)

            fake.completeWrite()
            runCurrent()
            assertTrue(first.isCompleted)
            assertMessageWrite(fake.onlyWrite().bytes, byteArrayOf(2))
            assertFalse(second.isCompleted)
            assertFalse(finish.isCompleted)

            fake.completeWrite()
            runCurrent()
            assertTrue(second.isCompleted)
            assertTrue(fake.onlyWrite().endOfStream)
            assertArrayEquals(byteArrayOf(), fake.onlyWrite().bytes)
            assertFalse(finish.isCompleted)

            fake.completeWrite()
            finish.await()
            stream.finishSend()
            assertEquals(0, fake.pendingWrites)

            val error = runCatching { stream.send(byteArrayOf(3)) }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.CANCELLED, error.status.code)
            stream.close()
        }

    @Test
    fun `remote terminal status makes a later finish idempotent`() =
        runTest {
            val fake = FakeFactory()
            val stream =
                openStreaming(
                    Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext),
                    fake,
                )
            fake.headers(validHeaders())
            fake.deliverRead(statusFrame(Status.ok()))
            runCurrent()

            stream.finishSend()
            assertEquals(0, fake.pendingWrites)
            assertInstanceOf(ResponseFrame.Status::class.java, stream.receive())
            assertNull(stream.receive())
            assertTrue(fake.awaitCancel())
        }

    @Test
    fun `remote terminal status completes an in-flight finish`() =
        runTest {
            val fake = FakeFactory()
            val stream =
                openStreaming(
                    Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext),
                    fake,
                )
            val finish = async { stream.finishSend() }
            runCurrent()
            assertTrue(fake.onlyWrite().endOfStream)
            assertFalse(finish.isCompleted)

            fake.headers(validHeaders())
            fake.deliverRead(statusFrame(Status.ok()))
            runCurrent()

            finish.await()
            fake.completeWrite()
            assertInstanceOf(ResponseFrame.Status::class.java, stream.receive())
            assertNull(stream.receive())
            assertTrue(fake.awaitCancel())
        }

    @Test
    fun `canceling an in-flight send cancels the exchange`() =
        runTest {
            val fake = FakeFactory()
            val stream =
                openStreaming(
                    Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext),
                    fake,
                )
            val send = async { stream.send(byteArrayOf(1)) }
            runCurrent()
            assertEquals(1, fake.pendingWrites)

            send.cancel()
            runCurrent()

            assertTrue(send.isCancelled)
            assertTrue(fake.awaitCancel())
            assertEquals(1, fake.cancelCalls)
            assertInstanceOf(CancellationException::class.java, stream.receiveFailure())
        }

    @Test
    fun `response channel backpressures subsequent Cronet reads`() =
        runTest {
            val fake = FakeFactory()
            val transport =
                Http3RpcTransport(
                    fake,
                    CronetTransportOptions(responseChannelCapacity = 1),
                    coroutineContext,
                )
            val stream = openStreaming(transport, fake)
            fake.headers(validHeaders())
            val frames =
                (1..3).fold(byteArrayOf()) { bytes, value ->
                    bytes + messageFrame(byteArrayOf(value.toByte()))
                }

            fake.deliverRead(frames)
            runCurrent()
            assertEquals(0, fake.pendingReads)
            assertEquals(
                1,
                (stream.receive() as ResponseFrame.Message)
                    .body
                    .single()
                    .toInt(),
            )
            runCurrent()
            assertEquals(0, fake.pendingReads)
            assertEquals(
                2,
                (stream.receive() as ResponseFrame.Message)
                    .body
                    .single()
                    .toInt(),
            )
            runCurrent()
            assertEquals(1, fake.pendingReads)
            assertEquals(
                3,
                (stream.receive() as ResponseFrame.Message)
                    .body
                    .single()
                    .toInt(),
            )
            stream.close()
        }

    @Test
    fun `strict HTTP validation rejects status parameters duplicates and combined values`() =
        runTest {
            val invalid =
                listOf(
                    ResponseHeaders(503, listOf("content-type" to TREV_RPC_CONTENT_TYPE)),
                    ResponseHeaders(200, emptyList()),
                    ResponseHeaders(200, listOf("content-type" to "$TREV_RPC_CONTENT_TYPE; charset=utf-8")),
                    ResponseHeaders(
                        200,
                        listOf("content-type" to TREV_RPC_CONTENT_TYPE, "Content-Type" to TREV_RPC_CONTENT_TYPE),
                    ),
                    ResponseHeaders(200, listOf("content-type" to "$TREV_RPC_CONTENT_TYPE, $TREV_RPC_CONTENT_TYPE")),
                )

            invalid.forEach { headers ->
                val fake = FakeFactory()
                val stream =
                    openStreaming(
                        Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext),
                        fake,
                    )
                fake.headers(headers)
                val error = stream.receiveFailure()
                assertInstanceOf(TrevRpcException::class.java, error)
                assertTrue(fake.awaitCancel())
                assertEquals(1, fake.cancelCalls)
            }
        }

    @Test
    fun `early response close and partial frame are distinct outcomes`() =
        runTest {
            val earlyFake = FakeFactory()
            val early =
                openStreaming(
                    Http3RpcTransport(earlyFake, coroutineContext = coroutineContext),
                    earlyFake,
                )
            earlyFake.headers(validHeaders())
            earlyFake.deliverRead(byteArrayOf(), endOfStream = true)
            runCurrent()
            assertNull(early.receive())

            val partialFake = FakeFactory()
            val partial =
                openStreaming(
                    Http3RpcTransport(partialFake, coroutineContext = coroutineContext),
                    partialFake,
                )
            partialFake.headers(validHeaders())
            partialFake.deliverRead(byteArrayOf(0, 0x40), endOfStream = true)
            runCurrent()
            val error = partial.receiveFailure()
            assertEquals(Code.INTERNAL, (error as TrevRpcException).status.code)
        }

    @Test
    fun `unknown frames are skipped everywhere and frames decode once into response frames`() =
        runTest {
            val fake = FakeFactory()
            val stream =
                openStreaming(Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext), fake)
            fake.headers(validHeaders())
            val unknown =
                encodeFrame(0x40, ByteArray(5)) + encodeFrame(0x21, byteArrayOf()) + encodeFrame(QuicVarint.MAX_VALUE, byteArrayOf(1))
            val bytes = unknown + headersFrame(Metadata.of("x-h" to byteArrayOf(3))) + unknown + messageFrame(byteArrayOf(1)) + unknown
            // One byte at a time: every header is split across reads at every boundary.
            bytes.forEach {
                fake.deliverRead(byteArrayOf(it))
                runCurrent()
            }
            // The exchange stops reading at STATUS, so trailing unknown frames share its read.
            fake.deliverRead(statusFrame(Status.ok()) + unknown)
            runCurrent()
            assertEquals(3, (stream.receive() as ResponseFrame.Headers).metadata["x-h"]?.single()?.toInt())
            assertArrayEquals(byteArrayOf(1), (stream.receive() as ResponseFrame.Message).body)
            assertTrue((stream.receive() as ResponseFrame.Status).status.isOk)
            assertNull(stream.receive())
        }

    @Test
    fun `compressed messages travel as ZSTD_MESSAGE frames and count at the size of their message`() =
        runTest {
            val fake = FakeFactory()
            val stream =
                openStreaming(Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext), fake)
            val send = async { stream.send(COMPRESSED_PAYLOAD, FrameType.ZSTD_MESSAGE) }
            runCurrent()
            val written = FrameDecoder().feed(fake.onlyWrite().bytes).single()
            assertEquals(FrameType.ZSTD_MESSAGE, written.type)
            assertArrayEquals(COMPRESSED_PAYLOAD, written.payload)
            fake.completeWrite()
            send.await()
            fake.headers(validHeaders())
            fake.deliverRead(encodeFrame(FrameType.ZSTD_MESSAGE, COMPRESSED_PAYLOAD) + statusFrame(Status.ok()))
            runCurrent()
            val message = stream.receive() as ResponseFrame.Message
            assertTrue(message.compressed)
            assertArrayEquals(COMPRESSED_PAYLOAD, message.body)

            // The payload fits a 64-byte frame, but the 71-byte message it carries does not.
            val limited = CronetTransportOptions(maxFrameSize = 64)
            val smallFake = FakeFactory()
            val small =
                openStreaming(Http3RpcTransport(smallFake, limited, coroutineContext, shutdownContext = coroutineContext), smallFake)
            val error = runCatching { small.send(COMPRESSED_PAYLOAD, FrameType.ZSTD_MESSAGE) }.exceptionOrNull()
            assertEquals(Code.RESOURCE_EXHAUSTED, (error as TrevRpcException).status.code)
            val readFake = FakeFactory()
            val reading =
                openStreaming(Http3RpcTransport(readFake, limited, coroutineContext, shutdownContext = coroutineContext), readFake)
            readFake.headers(validHeaders())
            readFake.deliverRead(encodeFrame(FrameType.ZSTD_MESSAGE, COMPRESSED_PAYLOAD))
            runCurrent()
            assertEquals(Code.RESOURCE_EXHAUSTED, (reading.receiveFailure() as TrevRpcException).status.code)
        }

    @Test
    fun `stream-compressed messages travel as ZSTD_STREAM_MESSAGE frames and count at their declared size`() =
        runTest {
            val fake = FakeFactory()
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext)
            assertEquals(DEFAULT_STREAM_COMPRESSION_MEMORY, transport.streamCompressionBudget.bytes)
            val stream = openStreaming(transport, fake)
            val send = async { stream.send(STREAM_PAYLOAD, FrameType.ZSTD_STREAM_MESSAGE) }
            runCurrent()
            val written = FrameDecoder().feed(fake.onlyWrite().bytes).single()
            assertEquals(FrameType.ZSTD_STREAM_MESSAGE, written.type)
            assertArrayEquals(STREAM_PAYLOAD, written.payload)
            fake.completeWrite()
            send.await()
            fake.headers(validHeaders())
            fake.deliverRead(encodeFrame(FrameType.ZSTD_STREAM_MESSAGE, STREAM_PAYLOAD) + statusFrame(Status.ok()))
            runCurrent()
            val message = stream.receive() as ResponseFrame.Message
            assertTrue(message.compressed)
            // The body is the chunk after the message size, still compressed.
            assertArrayEquals(STREAM_PAYLOAD.copyOfRange(2, STREAM_PAYLOAD.size), message.body)

            // The payload fits a 64-byte frame, but the 71-byte message it declares does not.
            val limited = CronetTransportOptions(maxFrameSize = 64)
            val smallFake = FakeFactory()
            val small =
                openStreaming(Http3RpcTransport(smallFake, limited, coroutineContext, shutdownContext = coroutineContext), smallFake)
            val error = runCatching { small.send(STREAM_PAYLOAD, FrameType.ZSTD_STREAM_MESSAGE) }.exceptionOrNull()
            assertEquals(Code.RESOURCE_EXHAUSTED, (error as TrevRpcException).status.code)
            val readFake = FakeFactory()
            val reading =
                openStreaming(Http3RpcTransport(readFake, limited, coroutineContext, shutdownContext = coroutineContext), readFake)
            readFake.headers(validHeaders())
            readFake.deliverRead(encodeFrame(FrameType.ZSTD_STREAM_MESSAGE, STREAM_PAYLOAD))
            runCurrent()
            assertEquals(Code.RESOURCE_EXHAUSTED, (reading.receiveFailure() as TrevRpcException).status.code)
            assertEquals(0L, CronetTransportOptions(streamCompressionMemory = 0).streamCompressionMemory)
            assertThrows(IllegalArgumentException::class.java) { CronetTransportOptions(streamCompressionMemory = -1) }
        }

    @Test
    fun `a known frame after status in the same read is forwarded for the reader to reject`() =
        runTest {
            val fake = FakeFactory()
            val stream =
                openStreaming(Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext), fake)
            fake.headers(validHeaders())
            fake.deliverRead(statusFrame(Status.ok()) + encodeFrame(0x40, byteArrayOf()) + messageFrame(byteArrayOf(2)))
            runCurrent()
            assertInstanceOf(ResponseFrame.Status::class.java, stream.receive())
            assertInstanceOf(ResponseFrame.Message::class.java, stream.receive())
            assertNull(stream.receive())
        }

    @Test
    fun `oversized frames are rejected from the header and a request frame in a response is internal`() =
        runTest {
            val oversizedFake = FakeFactory()
            val oversized =
                openStreaming(
                    Http3RpcTransport(
                        oversizedFake,
                        CronetTransportOptions(maxFrameSize = 64),
                        coroutineContext,
                        shutdownContext = coroutineContext,
                    ),
                    oversizedFake,
                )
            oversizedFake.headers(validHeaders())
            // Grease type 0x21 declaring 100 bytes: no payload needs to arrive.
            oversizedFake.deliverRead(byteArrayOf(0x21, 0x40, 100))
            runCurrent()
            assertEquals(Code.RESOURCE_EXHAUSTED, (oversized.receiveFailure() as TrevRpcException).status.code)

            val requestFake = FakeFactory()
            val requestFrame =
                openStreaming(
                    Http3RpcTransport(requestFake, coroutineContext = coroutineContext, shutdownContext = coroutineContext),
                    requestFake,
                )
            requestFake.headers(validHeaders())
            requestFake.deliverRead(encodeFrame(FrameType.REQUEST, byteArrayOf()))
            runCurrent()
            assertEquals(Code.INTERNAL, (requestFrame.receiveFailure() as TrevRpcException).status.code)
        }

    @Test
    fun `remote terminal status wins while direct send is still pending`() =
        runTest {
            val fake = FakeFactory()
            val transport =
                Http3RpcTransport(
                    fake,
                    CronetTransportOptions(responseChannelCapacity = 1),
                    coroutineContext,
                )
            val stream = openStreaming(transport, fake)
            val send = async { runCatching { stream.send(byteArrayOf(1)) } }
            runCurrent()
            assertFalse(fake.onlyWrite().endOfStream)
            fake.headers(validHeaders())
            fake.deliverRead(messageFrame(byteArrayOf(9)) + statusFrame(Status.unavailable("remote result")))
            runCurrent()
            fake.completeWrite()
            fake.fail(IllegalStateException("late request write failure"))
            // The pending send reports the server's status, not a cancellation.
            val sendError = send.await().exceptionOrNull() as TrevRpcException
            assertEquals(Code.UNAVAILABLE, sendError.status.code)
            assertEquals("remote result", sendError.status.message)

            assertArrayEquals(byteArrayOf(9), (stream.receive() as ResponseFrame.Message).body)
            runCurrent()
            val actual = stream.receive() as ResponseFrame.Status
            assertEquals(Code.UNAVAILABLE, actual.status.code)
            assertEquals("remote result", actual.status.message)
            assertNull(stream.receive())
            assertTrue(fake.awaitCancel())
            assertEquals(1, fake.cancelCalls)
        }

    @Test
    fun `sends on the exchange report the server's status, a clean completion, or the transport error`() =
        runTest {
            val failed = FakeFactory()
            val failedStream =
                openStreaming(
                    Http3RpcTransport(failed, coroutineContext = coroutineContext, shutdownContext = coroutineContext),
                    failed,
                )
            failed.headers(validHeaders())
            failed.deliverRead(statusFrame(Status(Code.ABORTED, "stop")))
            runCurrent()
            val abortedSend = runCatching { failedStream.send(byteArrayOf(1)) }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.ABORTED, abortedSend.status.code)
            assertEquals("stop", abortedSend.status.message)
            failedStream.finishSend()

            val completed = FakeFactory()
            val completedStream =
                openStreaming(
                    Http3RpcTransport(completed, coroutineContext = coroutineContext, shutdownContext = coroutineContext),
                    completed,
                )
            completed.headers(validHeaders())
            completed.deliverRead(statusFrame(Status.ok()))
            runCurrent()
            val okSend = runCatching { completedStream.send(byteArrayOf(1)) }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.CANCELLED, okSend.status.code)

            val broken = FakeFactory()
            val brokenStream =
                openStreaming(
                    Http3RpcTransport(broken, coroutineContext = coroutineContext, shutdownContext = coroutineContext),
                    broken,
                )
            broken.fail(IllegalStateException("connection reset"))
            runCurrent()
            val brokenSend = runCatching { brokenStream.send(byteArrayOf(1)) }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.UNAVAILABLE, brokenSend.status.code)
            assertEquals("connection reset", brokenSend.rootCause().message)
        }

    @Test
    fun `a non-OK status fails a blocked client-streaming send with that status while the request stays open`() =
        runTest {
            val fake = FakeFactory()
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext)
            val call =
                openCall(fake) {
                    Client(transport, coroutineContext)
                        .openClientStreaming("/svc/m", strings, strings, CallOptions(timeout = 1.hours))
                }
            val send = async { runCatching { call.send("a") } }
            runCurrent()
            // The server does not read the request: the write never completes.
            assertEquals(1, fake.pendingWrites)

            fake.headers(validHeaders())
            fake.deliverRead(statusFrame(Status(Code.ABORTED, "stop")))
            runCurrent()

            assertTrue(send.isCompleted)
            val error = send.await().exceptionOrNull() as TrevRpcException
            assertEquals(Code.ABORTED, error.status.code)
            assertEquals("stop", error.status.message)
            val later = runCatching { call.send("b") }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.ABORTED, later.status.code)
            val outcome = runCatching { call.receive() }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.ABORTED, outcome.status.code)
            assertTrue(fake.awaitCancel())
        }

    @Test
    fun `a non-OK status behind more unread responses than the channel holds still fails the sends of a bidi call`() =
        runTest {
            val fake = FakeFactory()
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext)
            val call =
                openCall(fake) {
                    Client(transport, coroutineContext)
                        .openBidirectionalStreaming("/svc/m", strings, strings, CallOptions(timeout = 1.hours))
                }
            val send = async { runCatching { call.send("a") } }
            runCurrent()
            assertEquals(1, fake.pendingWrites)

            val responses = (0 until 20).fold(byteArrayOf()) { bytes, i -> bytes + messageFrame("m$i".encodeToByteArray()) }
            fake.headers(validHeaders())
            fake.deliverRead(responses + statusFrame(Status(Code.NOT_FOUND, "gone")))
            runCurrent()

            // Nobody receives, yet the sender learns why the call ended.
            assertTrue(send.isCompleted)
            assertEquals(Code.NOT_FOUND, (send.await().exceptionOrNull() as TrevRpcException).status.code)
            repeat(20) { assertEquals("m$it", call.receive()) }
            assertEquals(Code.NOT_FOUND, (runCatching { call.receive() }.exceptionOrNull() as TrevRpcException).status.code)
        }

    @Test
    fun `an early OK status ends sends with Cancelled and the response is still received`() =
        runTest {
            val fake = FakeFactory()
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext)
            val call =
                openCall(fake) {
                    Client(transport, coroutineContext)
                        .openClientStreaming("/svc/m", strings, strings, CallOptions(timeout = 1.hours))
                }
            val send = async { runCatching { call.send("a") } }
            runCurrent()
            assertEquals(1, fake.pendingWrites)

            fake.headers(validHeaders())
            fake.deliverRead(messageFrame("early".encodeToByteArray()) + statusFrame(Status.ok()))
            runCurrent()

            assertTrue(send.isCompleted)
            assertEquals(Code.CANCELLED, (send.await().exceptionOrNull() as TrevRpcException).status.code)
            assertEquals("early", call.receive())
        }

    @Test
    fun `the server's status wins when the call deadline passes after the status but before the end of the stream`() =
        runTest {
            val fake = FakeFactory()
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext)
            val call =
                openCall(fake) {
                    Client(transport, coroutineContext)
                        .openClientStreaming("/svc/m", strings, strings, CallOptions(timeout = 100.milliseconds))
                }
            fake.headers(validHeaders())
            fake.deliverRead(messageFrame("reply".encodeToByteArray()) + statusFrame(Status.ok()))
            runCurrent()
            // The deadline is measured on the real clock.
            Thread.sleep(250)

            assertEquals("reply", call.receive())
        }

    @Test
    fun `a transport error after the upload stopped is what send reports`() =
        runTest {
            val fake = FakeFactory()
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext)
            val call =
                openCall(fake) {
                    Client(transport, coroutineContext)
                        .openBidirectionalStreaming("/svc/m", strings, strings, CallOptions(timeout = 1.hours))
                }
            fake.headers(validHeaders())
            fake.fail(IllegalStateException("connection reset"))
            runCurrent()

            val error = runCatching { call.send("a") }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.UNAVAILABLE, error.status.code)
            assertEquals("connection reset", error.rootCause().message)
            val outcome = runCatching { call.receive() }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.UNAVAILABLE, outcome.status.code)
        }

    @Test
    fun `close and terminal callbacks race to exactly one completion`() =
        runTest {
            val fake = FakeFactory()
            val stream =
                openStreaming(
                    Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext),
                    fake,
                )
            val race = CyclicBarrier(3)
            val localClose =
                async(Dispatchers.Default) {
                    race.await(5, TimeUnit.SECONDS)
                    stream.close(TrevRpcException(Status.cancelled("local close")))
                }
            val terminalCallback =
                async(Dispatchers.Default) {
                    race.await(5, TimeUnit.SECONDS)
                    fake.cancelCallback()
                }
            race.await(5, TimeUnit.SECONDS)
            localClose.await()
            terminalCallback.await()

            stream.close()
            fake.fail(IllegalStateException("late failure"))
            assertTrue(fake.awaitCancel())
            assertEquals(1, fake.cancelCalls)
            assertInstanceOf(TrevRpcException::class.java, stream.receiveFailure())
        }

    @Test
    fun `first transport error is retained across later callbacks`() =
        runTest {
            val fake = FakeFactory()
            val stream =
                openStreaming(
                    Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext),
                    fake,
                )
            fake.fail(IllegalArgumentException("first"))
            fake.cancelCallback()
            fake.fail(IllegalStateException("second"))

            val error = stream.receiveFailure() as TrevRpcException
            assertEquals(Code.UNAVAILABLE, error.status.code)
            assertEquals("first", error.cause?.message)
            assertTrue(fake.awaitCancel())
            assertEquals(1, fake.cancelCalls)
        }

    @Test
    fun `channel close cancels and drains unary and every streaming exchange shape`() =
        runTest {
            RpcKind.entries.forEach { kind ->
                val fake = FakeFactory()
                val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext)
                val request = RpcRequest("/example.Service/${kind.name}", kindValue = kind.value)
                val call =
                    async {
                        runCatching {
                            if (kind == RpcKind.UNARY) {
                                transport.openStream(request, byteArrayOf(), true)
                            } else {
                                transport.openStream(request)
                            }
                        }.exceptionOrNull()
                    }
                runCurrent()
                assertEquals(1, transport.activeExchangeCount)

                val closing = async { transport.close() }
                runCurrent()
                assertTrue(fake.awaitCancel())
                assertEquals(1, fake.cancelCalls)
                assertFalse(closing.isCompleted)
                fake.cancelCallback()
                runCurrent()

                closing.await()
                assertEquals(0, transport.activeExchangeCount)
                assertInstanceOf(TrevRpcException::class.java, call.await())
            }
        }

    @Test
    fun `logical close remains registered until native termination and callback barrier`() =
        runTest {
            val fake = FakeFactory()
            val callbackDrain = ControlledCallbackDrain()
            val transport =
                Http3RpcTransport(
                    fake,
                    coroutineContext = coroutineContext,
                    callbackDrain = callbackDrain,
                    shutdownContext = coroutineContext,
                )
            val stream = openStreaming(transport, fake)

            stream.close()
            assertTrue(fake.awaitCancel())
            assertEquals(1, fake.cancelCalls)
            assertEquals(1, transport.activeExchangeCount)

            val closing = async { transport.close() }
            runCurrent()
            assertFalse(closing.isCompleted)
            assertTrue(fake.awaitCancel())
            assertEquals(1, fake.cancelCalls)

            fake.cancelCallback()
            runCurrent()
            assertEquals(0, transport.activeExchangeCount)
            assertFalse(closing.isCompleted)
            assertTrue(callbackDrain.entered.isCompleted)

            callbackDrain.release.complete(Unit)
            closing.await()
        }

    @Test
    fun `remote terminal status waits for terminal Cronet callback before deregistration`() =
        runTest {
            val fake = FakeFactory()
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext)
            val stream = openStreaming(transport, fake)
            fake.headers(validHeaders())
            fake.deliverRead(statusFrame(Status.ok()))
            runCurrent()

            assertInstanceOf(ResponseFrame.Status::class.java, stream.receive())
            assertNull(stream.receive())
            assertEquals(1, transport.activeExchangeCount)

            fake.cancelCallback()
            assertEquals(0, transport.activeExchangeCount)
            assertTrue(fake.awaitCancelReturned())
            transport.close()
        }

    @Test
    fun `factory failure settles both logical and native exchange state`() =
        runTest {
            val transport =
                Http3RpcTransport(
                    object : DuplexStreamFactory {
                        override fun open(callback: DuplexCallback): DuplexStream = throw IllegalStateException("factory failed")
                    },
                    coroutineContext = coroutineContext,
                )

            val error = runCatching { transport.openStream(streamRequest()) }.exceptionOrNull()
            assertEquals("factory failed", error?.message)
            assertEquals(0, transport.activeExchangeCount)
            transport.close()
        }

    @Test
    fun `synchronous start failure releases native registration`() =
        runTest {
            val transport =
                Http3RpcTransport(
                    StartFailingFactory(),
                    coroutineContext = coroutineContext,
                    shutdownContext = coroutineContext,
                )

            val error = runCatching { transport.openStream(streamRequest()) }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.UNAVAILABLE, error.status.code)
            assertEquals(0, transport.activeExchangeCount)
            transport.close()
        }

    @Test
    fun `canceling one close caller does not cancel the shared drain`() =
        runTest {
            val fake = FakeFactory()
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext)
            openStreaming(transport, fake)

            val first = async { transport.close() }
            runCurrent()
            first.cancel()
            runCurrent()
            assertTrue(first.isCancelled)
            assertTrue(fake.awaitCancel())
            assertEquals(1, fake.cancelCalls)

            val second = async { transport.close() }
            runCurrent()
            assertFalse(second.isCompleted)
            fake.cancelCallback()
            runCurrent()
            second.await()
            transport.close()
        }

    @Test
    fun `close timeout is bounded and shared by concurrent callers`() =
        runTest {
            val fake = FakeFactory()
            val transport =
                Http3RpcTransport(
                    fake,
                    CronetTransportOptions(closeTimeout = 100.milliseconds),
                    coroutineContext,
                    shutdownContext = coroutineContext,
                )
            openStreaming(transport, fake)

            val first = async { runCatching { transport.close() }.exceptionOrNull() }
            val second = async { runCatching { transport.close() }.exceptionOrNull() }
            runCurrent()
            assertTrue(fake.awaitCancel())
            assertEquals(1, fake.cancelCalls)
            assertFalse(first.isCompleted)
            assertFalse(second.isCompleted)

            advanceTimeBy(100)
            runCurrent()
            val firstError = first.await() as TrevRpcException
            val secondError = second.await() as TrevRpcException
            assertSame(firstError, secondError)
            assertEquals(Code.UNAVAILABLE, firstError.status.code)
            assertTrue(firstError.status.message.contains("not proven quiescent"))
        }

    @Test
    fun `close timeout also bounds a stalled callback barrier`() =
        runTest {
            val fake = FakeFactory()
            val callbackDrain = ControlledCallbackDrain()
            val transport =
                Http3RpcTransport(
                    fake,
                    CronetTransportOptions(closeTimeout = 100.milliseconds),
                    coroutineContext,
                    callbackDrain,
                    shutdownContext = coroutineContext,
                )
            val stream = openStreaming(transport, fake)
            stream.close()

            val closing = async { runCatching { transport.close() }.exceptionOrNull() }
            runCurrent()
            assertTrue(fake.awaitCancel())
            fake.cancelCallback()
            assertTrue(fake.awaitCancelReturned())
            runCurrent()
            assertTrue(callbackDrain.entered.isCompleted)
            assertFalse(closing.isCompleted)

            advanceTimeBy(100)
            runCurrent()
            val error = closing.await() as TrevRpcException
            assertEquals(Code.UNAVAILABLE, error.status.code)
            assertTrue(error.status.message.contains("not proven quiescent"))
        }

    @Test
    fun `close timeout bounds a provider write that blocks synchronously`(): Unit =
        runBlocking {
            val fake = FakeFactory()
            val writeEntered = CountDownLatch(1)
            val releaseWrite = CountDownLatch(1)
            fake.blockWrite(writeEntered, releaseWrite)
            fake.cancelCallbackBeforeReturn()
            val transport =
                Http3RpcTransport(
                    fake,
                    CronetTransportOptions(closeTimeout = 100.milliseconds),
                    Dispatchers.Default,
                    shutdownContext = Dispatchers.Default,
                )
            val opening = async(Dispatchers.Default) { runCatching { transport.openStream(streamRequest()) }.exceptionOrNull() }
            assertTrue(fake.awaitOpen())
            assertTrue(fake.awaitStart())
            fake.ready()
            assertTrue(writeEntered.await(5, TimeUnit.SECONDS))

            try {
                lateinit var failure: Throwable
                val elapsed =
                    measureTime {
                        failure = checkNotNull(runCatching { transport.close() }.exceptionOrNull())
                    }
                assertEquals(Code.UNAVAILABLE, (failure as TrevRpcException).status.code)
                assertTrue(elapsed < 2.seconds)
            } finally {
                releaseWrite.countDown()
            }
            assertInstanceOf(TrevRpcException::class.java, opening.await())
        }

    @Test
    fun `close timeout bounds a provider flush that blocks synchronously`(): Unit =
        runBlocking {
            val fake = FakeFactory()
            val flushEntered = CountDownLatch(1)
            val releaseFlush = CountDownLatch(1)
            fake.blockFlush(flushEntered, releaseFlush)
            fake.cancelCallbackBeforeReturn()
            val transport =
                Http3RpcTransport(
                    fake,
                    CronetTransportOptions(closeTimeout = 100.milliseconds),
                    Dispatchers.Default,
                    shutdownContext = Dispatchers.Default,
                )
            val opening = async(Dispatchers.Default) { runCatching { transport.openStream(streamRequest()) }.exceptionOrNull() }
            assertTrue(fake.awaitOpen())
            assertTrue(fake.awaitStart())
            fake.ready()
            assertTrue(flushEntered.await(5, TimeUnit.SECONDS))

            try {
                lateinit var failure: Throwable
                val elapsed =
                    measureTime {
                        failure = checkNotNull(runCatching { transport.close() }.exceptionOrNull())
                    }
                assertEquals(Code.UNAVAILABLE, (failure as TrevRpcException).status.code)
                assertTrue(elapsed < 2.seconds)
            } finally {
                releaseFlush.countDown()
            }
            assertInstanceOf(TrevRpcException::class.java, opening.await())
        }

    @Test
    fun `callback before provider cancel returns does not report quiescence`(): Unit =
        runBlocking {
            val fake = FakeFactory()
            val cancelEntered = CountDownLatch(1)
            val releaseCancel = CountDownLatch(1)
            fake.cancelCallbackBeforeReturn()
            fake.blockCancel(cancelEntered, releaseCancel)
            val transport = Http3RpcTransport(fake, coroutineContext = Dispatchers.Default)
            val opening = async(Dispatchers.Default) { transport.openStream(streamRequest()) }
            assertTrue(fake.awaitOpen())
            assertTrue(fake.awaitStart())
            fake.ready()
            assertTrue(fake.awaitWrite())
            fake.completeWrite()
            val stream = opening.await()

            val closing = async(Dispatchers.Default) { transport.close() }
            assertTrue(cancelEntered.await(5, TimeUnit.SECONDS))
            withTimeout(5_000) {
                while (transport.activeExchangeCount != 0) yield()
            }
            assertFalse(closing.isCompleted)

            releaseCancel.countDown()
            closing.await()
            assertInstanceOf(TrevRpcException::class.java, stream.receiveFailure())
        }

    @Test
    fun `close timeout bounds a provider cancel that blocks synchronously`() =
        runBlocking {
            val fake = FakeFactory()
            val cancelEntered = CountDownLatch(1)
            val releaseCancel = CountDownLatch(1)
            fake.blockCancel(cancelEntered, releaseCancel)
            val transport =
                Http3RpcTransport(
                    fake,
                    CronetTransportOptions(closeTimeout = 100.milliseconds),
                    Dispatchers.Default,
                    shutdownContext = Dispatchers.Default,
                )
            val opening = async(Dispatchers.Default) { transport.openStream(streamRequest()) }
            assertTrue(fake.awaitOpen())
            assertTrue(fake.awaitStart())
            fake.ready()
            assertTrue(fake.awaitWrite())
            val initial = fake.onlyWrite()
            assertRequestEquals(streamRequest(), decodeRequestFrame(initial.bytes))
            fake.completeWrite()
            opening.await()

            try {
                lateinit var failure: Throwable
                val elapsed =
                    measureTime {
                        failure = checkNotNull(runCatching { transport.close() }.exceptionOrNull())
                    }
                assertTrue(cancelEntered.await(5, TimeUnit.SECONDS))
                assertEquals(Code.UNAVAILABLE, (failure as TrevRpcException).status.code)
                assertTrue(elapsed < 2.seconds)
            } finally {
                releaseCancel.countDown()
            }
        }

    @Test
    fun `close timeout bounds borrowed executor submission that blocks synchronously`() =
        runBlocking {
            val submissionEntered = CountDownLatch(1)
            val releaseSubmission = CountDownLatch(1)
            val callbackDrain =
                DrainableSerialExecutor(
                    Executor {
                        submissionEntered.countDown()
                        check(releaseSubmission.await(5, TimeUnit.SECONDS))
                    },
                )
            val transport =
                Http3RpcTransport(
                    FakeFactory(),
                    CronetTransportOptions(closeTimeout = 100.milliseconds),
                    Dispatchers.Default,
                    callbackDrain,
                    shutdownContext = Dispatchers.Default,
                )

            try {
                lateinit var failure: Throwable
                val elapsed =
                    measureTime {
                        failure = checkNotNull(runCatching { transport.close() }.exceptionOrNull())
                    }
                assertTrue(submissionEntered.await(5, TimeUnit.SECONDS))
                assertEquals(Code.UNAVAILABLE, (failure as TrevRpcException).status.code)
                assertTrue(elapsed < 2.seconds)
            } finally {
                releaseSubmission.countDown()
            }
        }

    @Test
    fun `close timeout does not block on callback executor monitor held by provider submission`() =
        runBlocking {
            val submissionEntered = CountDownLatch(1)
            val releaseSubmission = CountDownLatch(1)
            val callbackDrain =
                DrainableSerialExecutor(
                    Executor { command ->
                        command.run()
                        submissionEntered.countDown()
                        check(releaseSubmission.await(5, TimeUnit.SECONDS))
                    },
                )
            val providerSubmission =
                async(Dispatchers.Default) {
                    runCatching { callbackDrain.execute {} }.exceptionOrNull()
                }
            assertTrue(submissionEntered.await(5, TimeUnit.SECONDS))
            val transport =
                Http3RpcTransport(
                    FakeFactory(),
                    CronetTransportOptions(closeTimeout = 100.milliseconds),
                    Dispatchers.Default,
                    callbackDrain,
                )

            try {
                lateinit var failure: Throwable
                val elapsed =
                    measureTime {
                        failure = checkNotNull(runCatching { transport.close() }.exceptionOrNull())
                    }
                assertEquals(Code.UNAVAILABLE, (failure as TrevRpcException).status.code)
                assertTrue(elapsed < 2.seconds)
            } finally {
                releaseSubmission.countDown()
            }
            assertNull(providerSubmission.await())
        }

    @Test
    fun `close racing factory open cancels the registered exchange and rejects later admission`() =
        runBlocking {
            val factory = BlockingFactory()
            val transport = Http3RpcTransport(factory, coroutineContext = Dispatchers.Default)
            val opening = async(Dispatchers.Default) { runCatching { transport.openStream(streamRequest()) }.exceptionOrNull() }
            assertTrue(factory.entered.await(5, TimeUnit.SECONDS))

            val closing = async { transport.close() }
            yield()
            val rejected = runCatching { transport.openStream(streamRequest()) }.exceptionOrNull() as TrevRpcException
            assertEquals(Code.UNAVAILABLE, rejected.status.code)
            assertEquals(1, factory.openCalls)
            withTimeout(5_000) {
                while (transport.logicallyActiveExchangeCount != 0) yield()
            }

            factory.release.countDown()
            val openingError = opening.await() as TrevRpcException
            assertEquals(Code.UNAVAILABLE, openingError.status.code)
            closing.await()
            assertEquals(0, factory.stream.cancelCalls)
        }

    @Test
    fun `close racing native start cancels only after start returns`() =
        runBlocking {
            val factory = StartBlockingFactory()
            val transport = Http3RpcTransport(factory, coroutineContext = Dispatchers.Default)
            val opening = async(Dispatchers.Default) { runCatching { transport.openStream(streamRequest()) }.exceptionOrNull() }
            assertTrue(factory.stream.startEntered.await(5, TimeUnit.SECONDS))

            val closing = async { transport.close() }
            withTimeout(5_000) {
                while (transport.logicallyActiveExchangeCount != 0) yield()
            }
            assertFalse(closing.isCompleted)
            assertEquals(0, factory.stream.cancelCalls)

            factory.stream.releaseStart.countDown()
            val openingError = opening.await()
            assertInstanceOf(TrevRpcException::class.java, openingError)
            closing.await()
            assertEquals(1, factory.stream.cancelCalls)
        }

    @Test
    fun `close orchestration does not depend on exchange dispatcher progress`() =
        runBlocking {
            val transport = Http3RpcTransport(FakeFactory(), coroutineContext = StalledDispatcher())
            transport.close()
        }

    @Test
    fun `serial callback executor contains task failures and orders its barrier`() =
        runBlocking {
            val borrowed = Executors.newSingleThreadExecutor()
            try {
                val serial = DrainableSerialExecutor(borrowed)
                val events = CopyOnWriteArrayList<String>()
                serial.execute {
                    events += "first"
                    error("callback failure")
                }
                serial.execute { events += "second" }

                withTimeout(5_000) { serial.barrier() }
                events += "barrier"

                assertEquals(listOf("first", "second", "barrier"), events)
            } finally {
                borrowed.shutdown()
                assertTrue(borrowed.awaitTermination(5, TimeUnit.SECONDS))
            }
        }

    @Test
    fun `null write response info reaches streaming send and finish`() =
        runBlocking {
            val engine = BorrowedCronetEngine()
            val executor = Executors.newSingleThreadExecutor()
            val channel = CronetRpcChannel.create(engine, "https://example.com", executor)
            try {
                val stream = withTimeout(5.seconds) { channel.openStream(streamRequest()) }
                withTimeout(5.seconds) {
                    stream.send(byteArrayOf(1, 2, 3))
                    stream.finishSend()
                }
                assertEquals(3, engine.stream.writeCompletions)
            } finally {
                runCatching { channel.close() }
                executor.shutdown()
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS))
            }
        }

    @Test
    fun `channel close never shuts down borrowed engine or executor`() =
        runBlocking {
            val engine = BorrowedCronetEngine()
            val executor = Executors.newSingleThreadExecutor()
            try {
                val channel = CronetRpcChannel.create(engine, "https://example.com", executor)
                val stream = channel.openStream(streamRequest())
                assertEquals(1, engine.openCalls)
                assertInstanceOf(DrainableSerialExecutor::class.java, engine.callbackExecutor)
                assertFalse(engine.callbackExecutor === executor)
                assertSame(engine.callbackExecutor, engine.stream.callbackExecutor)
                channel.close()

                assertEquals(1, engine.stream.cancelCalls)
                assertInstanceOf(TrevRpcException::class.java, stream.receiveFailure())
                assertEquals(0, engine.shutdownCalls)
                assertFalse(executor.isShutdown)
            } finally {
                executor.shutdown()
                assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS))
            }
        }
}

@OptIn(ExperimentalCoroutinesApi::class)
private suspend fun TestScope.openStreaming(
    transport: Http3RpcTransport,
    fake: FakeFactory,
    request: RpcRequest = streamRequest(),
): RpcClientStream {
    val opening = async { transport.openStream(request) }
    runCurrent()
    fake.ready()
    runCurrent()
    assertFalse(opening.isCompleted)
    val initial = fake.onlyWrite()
    assertFalse(initial.endOfStream)
    assertRequestEquals(request, decodeRequestFrame(initial.bytes))
    fake.completeWrite()
    return opening.await()
}

private fun Throwable.rootCause(): Throwable = generateSequence(this) { it.cause }.last()

private val strings = MessageCodec<String>(String::encodeToByteArray, ByteArray::decodeToString)

/** Opens a call through [open], completing the REQUEST write the way a reading server would. */
@OptIn(ExperimentalCoroutinesApi::class)
private suspend fun <T> TestScope.openCall(
    fake: FakeFactory,
    open: suspend () -> T,
): T {
    val opening = async { open() }
    runCurrent()
    fake.ready()
    runCurrent()
    fake.completeWrite()
    return opening.await()
}

private suspend fun RpcClientStream.receiveFailure(): Throwable =
    runCatching { receive() }.exceptionOrNull() ?: error("receive unexpectedly succeeded")

private fun streamRequest(): RpcRequest = RpcRequest("/example.Service/Stream", kindValue = RpcKind.BIDIRECTIONAL_STREAMING.value)

private fun validHeaders(contentType: String = TREV_RPC_CONTENT_TYPE): ResponseHeaders =
    ResponseHeaders(200, listOf("Content-Type" to contentType))

private fun decodeRequestFrame(bytes: ByteArray): RpcRequest {
    val frame = FrameDecoder().feed(bytes).first()
    assertEquals(FrameType.REQUEST, frame.type)
    return WireCodec.decodeRequest(frame.payload)
}

private fun messageFrame(body: ByteArray): ByteArray = encodeFrame(FrameType.MESSAGE, body)

/** `ok.small` of the compression golden vectors: a 71-byte message compressed into 23 bytes. */
private val COMPRESSED_PAYLOAD =
    "2101478d0000203238397d047c00cac4a5bd2f4f413605".chunked(2).map { it.toInt(16).toByte() }.toByteArray()

/** `stream.plain.1` of the compression golden vectors: a 71-byte message in a 12-byte chunk. */
private val STREAM_PAYLOAD = "40474c000010317d0100a2a00108".chunked(2).map { it.toInt(16).toByte() }.toByteArray()

private fun headersFrame(metadata: Metadata = Metadata.EMPTY): ByteArray =
    encodeFrame(FrameType.HEADERS, WireCodec.encode(RpcHeaders(metadata)))

private fun statusFrame(status: Status): ByteArray = encodeFrame(FrameType.STATUS, WireCodec.encode(RpcStatus.fromStatus(status)))

private fun assertMessageWrite(
    bytes: ByteArray,
    expected: ByteArray,
) {
    val frame = FrameDecoder().feed(bytes).single()
    assertEquals(FrameType.MESSAGE, frame.type)
    assertArrayEquals(expected, frame.payload)
}

private fun assertRequestEquals(
    expected: RpcRequest,
    actual: RpcRequest,
) {
    assertEquals(expected.method, actual.method)
    assertEquals(expected.kindValue, actual.kindValue)
    assertEquals(expected.timeoutNanos, actual.timeoutNanos)
    assertEquals(expected.metadata, actual.metadata)
}

private class FakeFactory : DuplexStreamFactory {
    private lateinit var callback: DuplexCallback
    private val stream = FakeStream()
    private val opened = CountDownLatch(1)

    val cancelCalls: Int
        get() = stream.cancelCalls

    fun awaitCancel(): Boolean = stream.cancelObserved.await(5, TimeUnit.SECONDS)

    fun awaitCancelReturned(): Boolean = stream.cancelReturned.await(5, TimeUnit.SECONDS)

    fun awaitOpen(): Boolean = opened.await(5, TimeUnit.SECONDS)

    fun awaitStart(): Boolean = stream.startObserved.await(5, TimeUnit.SECONDS)

    fun awaitWrite(): Boolean = stream.writeObserved.await(5, TimeUnit.SECONDS)

    val pendingReads: Int
        get() = stream.reads.size

    val pendingWrites: Int
        get() = stream.writes.size

    override fun open(callback: DuplexCallback): DuplexStream {
        this.callback = callback
        opened.countDown()
        return stream
    }

    fun ready() = callback.onReady(stream)

    fun headers(headers: ResponseHeaders) = callback.onHeaders(stream, headers)

    fun deliverRead(
        bytes: ByteArray,
        endOfStream: Boolean = false,
    ) {
        val buffer = stream.reads.removeFirst()
        check(bytes.size <= buffer.remaining())
        buffer.put(bytes)
        callback.onRead(stream, buffer, endOfStream)
    }

    fun onlyWrite(): FakeWrite = stream.writes.single()

    fun completeWrite() {
        val write = stream.writes.removeFirst()
        write.buffer.position(write.buffer.limit())
        callback.onWrite(stream, write.buffer, write.endOfStream)
    }

    fun fail(error: Throwable) = callback.onFailed(stream, error)

    fun cancelCallback() = callback.onCanceled(stream)

    fun blockWrite(
        entered: CountDownLatch,
        release: CountDownLatch,
    ) {
        stream.writeEntered = entered
        stream.releaseWrite = release
    }

    fun blockFlush(
        entered: CountDownLatch,
        release: CountDownLatch,
    ) {
        stream.flushEntered = entered
        stream.releaseFlush = release
    }

    fun cancelCallbackBeforeReturn() {
        stream.cancelAction = { callback.onCanceled(stream) }
    }

    fun blockCancel(
        entered: CountDownLatch,
        release: CountDownLatch,
    ) {
        stream.cancelEntered = entered
        stream.releaseCancel = release
    }
}

private class FakeStream : DuplexStream {
    val reads = ArrayDeque<ByteBuffer>()
    val writes = ArrayDeque<FakeWrite>()
    var cancelCalls = 0
    val cancelObserved = CountDownLatch(1)
    val cancelReturned = CountDownLatch(1)
    val startObserved = CountDownLatch(1)
    val writeObserved = CountDownLatch(1)
    var writeEntered: CountDownLatch? = null
    var releaseWrite: CountDownLatch? = null
    var flushEntered: CountDownLatch? = null
    var releaseFlush: CountDownLatch? = null
    var cancelAction: (() -> Unit)? = null
    var cancelEntered: CountDownLatch? = null
    var releaseCancel: CountDownLatch? = null

    override fun start() {
        startObserved.countDown()
    }

    override fun read(buffer: ByteBuffer) {
        check(buffer.isDirect)
        reads.add(buffer)
    }

    override fun write(
        buffer: ByteBuffer,
        endOfStream: Boolean,
    ) {
        check(buffer.isDirect)
        writeEntered?.countDown()
        releaseWrite?.let { check(it.await(5, TimeUnit.SECONDS)) { "write release timed out" } }
        check(writes.isEmpty()) { "only one write may be in flight" }
        val copy = buffer.duplicate()
        val bytes = ByteArray(copy.remaining())
        copy.get(bytes)
        writes.add(FakeWrite(buffer, bytes, endOfStream))
        writeObserved.countDown()
    }

    override fun flush() {
        flushEntered?.countDown()
        releaseFlush?.let { check(it.await(5, TimeUnit.SECONDS)) { "flush release timed out" } }
    }

    override fun cancel() {
        cancelCalls++
        cancelObserved.countDown()
        cancelAction?.invoke()
        cancelEntered?.countDown()
        releaseCancel?.let { check(it.await(5, TimeUnit.SECONDS)) }
        cancelReturned.countDown()
    }
}

private data class FakeWrite(
    val buffer: ByteBuffer,
    val bytes: ByteArray,
    val endOfStream: Boolean,
)

private class ControlledCallbackDrain : CallbackDrain {
    val entered = kotlinx.coroutines.CompletableDeferred<Unit>()
    val release = kotlinx.coroutines.CompletableDeferred<Unit>()

    override suspend fun barrier() {
        entered.complete(Unit)
        release.await()
    }
}

private class BlockingFactory : DuplexStreamFactory {
    val entered = CountDownLatch(1)
    val release = CountDownLatch(1)
    val stream = CancelCompletingStream()
    var openCalls = 0

    override fun open(callback: DuplexCallback): DuplexStream {
        openCalls++
        stream.callback = callback
        entered.countDown()
        check(release.await(5, TimeUnit.SECONDS)) { "factory release timed out" }
        return stream
    }
}

private class CancelCompletingStream : DuplexStream {
    lateinit var callback: DuplexCallback
    var cancelCalls = 0

    override fun start() = Unit

    override fun read(buffer: ByteBuffer) = Unit

    override fun write(
        buffer: ByteBuffer,
        endOfStream: Boolean,
    ) = Unit

    override fun flush() = Unit

    override fun cancel() {
        cancelCalls++
        callback.onCanceled(this)
    }
}

private class StartFailingFactory : DuplexStreamFactory {
    override fun open(callback: DuplexCallback): DuplexStream =
        object : DuplexStream {
            override fun start(): Unit = throw IllegalStateException("start failed")

            override fun read(buffer: ByteBuffer) = Unit

            override fun write(
                buffer: ByteBuffer,
                endOfStream: Boolean,
            ) = Unit

            override fun flush() = Unit

            override fun cancel() = Unit
        }
}

private class StartBlockingFactory : DuplexStreamFactory {
    val stream = StartBlockingStream()

    override fun open(callback: DuplexCallback): DuplexStream = stream.also { it.callback = callback }
}

private class StartBlockingStream : DuplexStream {
    val startEntered = CountDownLatch(1)
    val releaseStart = CountDownLatch(1)
    lateinit var callback: DuplexCallback
    var cancelCalls = 0

    override fun start() {
        startEntered.countDown()
        check(releaseStart.await(5, TimeUnit.SECONDS)) { "start release timed out" }
    }

    override fun read(buffer: ByteBuffer) = Unit

    override fun write(
        buffer: ByteBuffer,
        endOfStream: Boolean,
    ) = Unit

    override fun flush() = Unit

    override fun cancel() {
        cancelCalls++
        callback.onCanceled(this)
    }
}

private class StalledDispatcher : CoroutineDispatcher() {
    override fun dispatch(
        context: CoroutineContext,
        block: Runnable,
    ) = Unit
}

@Suppress("OVERRIDE_DEPRECATION")
private class BorrowedCronetEngine : CronetEngine() {
    var shutdownCalls = 0
    var openCalls = 0
    lateinit var callbackExecutor: Executor
    lateinit var stream: AdapterCronetStream

    override fun getVersionString(): String = "fake"

    override fun shutdown() {
        shutdownCalls++
    }

    override fun newBidirectionalStreamBuilder(
        url: String,
        callback: BidirectionalStream.Callback,
        executor: Executor,
    ): BidirectionalStream.Builder {
        openCalls++
        callbackExecutor = executor
        return AdapterCronetBuilder {
            AdapterCronetStream(callback, executor).also { stream = it }
        }
    }

    override fun startNetLogToFile(
        fileName: String,
        logAll: Boolean,
    ) = Unit

    override fun stopNetLog() = Unit

    override fun getGlobalMetricsDeltas(): ByteArray = byteArrayOf()

    override fun openConnection(url: URL): URLConnection = throw UnsupportedOperationException()

    override fun createURLStreamHandlerFactory(): URLStreamHandlerFactory = throw UnsupportedOperationException()

    override fun newUrlRequestBuilder(
        url: String,
        callback: UrlRequest.Callback,
        executor: Executor,
    ): UrlRequest.Builder = throw UnsupportedOperationException()
}

private class AdapterCronetBuilder(
    private val buildStream: () -> BidirectionalStream,
) : BidirectionalStream.Builder() {
    override fun setHttpMethod(method: String): BidirectionalStream.Builder = this

    override fun addHeader(
        header: String,
        value: String,
    ): BidirectionalStream.Builder = this

    override fun setPriority(priority: Int): BidirectionalStream.Builder = this

    override fun delayRequestHeadersUntilFirstFlush(delay: Boolean): BidirectionalStream.Builder = this

    override fun build(): BidirectionalStream = buildStream()
}

private class AdapterCronetStream(
    private val callback: BidirectionalStream.Callback,
    val callbackExecutor: Executor,
) : BidirectionalStream() {
    var cancelCalls = 0
    var writeCompletions = 0
    private var done = false

    override fun start() {
        callbackExecutor.execute { callback.onStreamReady(this) }
    }

    override fun read(buffer: ByteBuffer) = Unit

    override fun write(
        buffer: ByteBuffer,
        endOfStream: Boolean,
    ) {
        callbackExecutor.execute {
            buffer.position(buffer.limit())
            writeCompletions++
            callback.onWriteCompleted(this, null, buffer, endOfStream)
        }
    }

    override fun flush() = Unit

    override fun cancel() {
        cancelCalls++
        done = true
        callbackExecutor.execute { callback.onCanceled(this, null) }
    }

    override fun isDone(): Boolean = done
}
