package zip.trev.trevrpc.cronet

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.test.TestScope
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.FrameDecoder
import zip.trev.trevrpc.RpcChannelState
import zip.trev.trevrpc.RpcClientStream
import zip.trev.trevrpc.RpcKind
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcResponse
import zip.trev.trevrpc.RpcStreamFrame
import zip.trev.trevrpc.RpcStreamFrameKind
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.WireCodec
import zip.trev.trevrpc.frame
import java.nio.ByteBuffer

@OptIn(ExperimentalCoroutinesApi::class)
class Http3RpcTransportTest {
    @Test
    fun `provider channel is ready until explicitly closed`() =
        runTest {
            val channel = CronetChannel(Http3RpcTransport(FakeFactory(), coroutineContext = coroutineContext))

            assertEquals(RpcChannelState.READY, channel.state.value)
            channel.awaitReady()
            channel.close()
            channel.close()

            assertEquals(RpcChannelState.CLOSED, channel.state.value)
            val error = runCatching { channel.awaitReady() }.exceptionOrNull()
            assertEquals(Code.UNAVAILABLE, (error as TrevRpcException).status.code)
        }

    @Test
    fun `unary writes one request and decodes one response`() =
        runTest {
            val fake = FakeFactory()
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext)
            val request = RpcRequest("example.Service", "Unary", byteArrayOf(1, 2))
            val call = async { transport.unary(request) }

            runCurrent()
            fake.ready()
            runCurrent()
            val requestWrite = fake.onlyWrite()
            assertTrue(requestWrite.endOfStream)
            assertRequestEquals(request, WireCodec.decodeRequest(decodeFrame(requestWrite.bytes)))
            fake.completeWrite()

            fake.headers(validHeaders())
            val response = RpcResponse.ok(byteArrayOf(7, 8, 9))
            fake.deliverRead(frame(WireCodec.encode(response)), endOfStream = true)
            runCurrent()

            val actual = call.await()
            assertEquals(response.statusValue, actual.statusValue)
            assertArrayEquals(response.body, actual.body)
            assertEquals(1, fake.cancelCalls)
        }

    @Test
    fun `headers may precede ready and reads split and coalesced frames`() =
        runTest {
            val fake = FakeFactory()
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext)
            val request =
                RpcRequest(
                    "example.Service",
                    "Bidi",
                    kindValue = RpcKind.BIDIRECTIONAL_STREAMING.value,
                )
            val opening = async { transport.openStream(request) }
            runCurrent()

            fake.headers(validHeaders("Application/TrevRPC"))
            assertEquals(1, fake.pendingReads)
            fake.ready()
            runCurrent()
            assertRequestEquals(request, WireCodec.decodeRequest(decodeFrame(fake.onlyWrite().bytes)))
            assertFalse(opening.isCompleted)
            fake.completeWrite()
            val stream = opening.await()

            val message = RpcStreamFrame.message(byteArrayOf(4, 5))
            val status = RpcStreamFrame.status(Status.ok())
            val responseBytes = frame(WireCodec.encode(message)) + frame(WireCodec.encode(status))
            fake.deliverRead(responseBytes.copyOfRange(0, 3))
            runCurrent()
            assertEquals(1, fake.pendingReads)
            fake.deliverRead(responseBytes.copyOfRange(3, responseBytes.size))
            runCurrent()

            val actualMessage = stream.receive()
            val actualStatus = stream.receive()
            assertArrayEquals(message.body, actualMessage?.body)
            assertEquals(RpcStreamFrameKind.STATUS, actualStatus?.kind)
            assertNull(stream.receive())
            assertEquals(1, fake.cancelCalls)
        }

    @Test
    fun `send waits for direct completion and serializes with finish`() =
        runTest {
            val fake = FakeFactory()
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext)
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
    fun `canceling an in-flight send cancels the exchange`() =
        runTest {
            val fake = FakeFactory()
            val stream =
                openStreaming(
                    Http3RpcTransport(fake, coroutineContext = coroutineContext),
                    fake,
                )
            val send = async { stream.send(byteArrayOf(1)) }
            runCurrent()
            assertEquals(1, fake.pendingWrites)

            send.cancel()
            runCurrent()

            assertTrue(send.isCancelled)
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
                    bytes + frame(WireCodec.encode(RpcStreamFrame.message(byteArrayOf(value.toByte()))))
                }

            fake.deliverRead(frames)
            runCurrent()
            assertEquals(0, fake.pendingReads)
            assertEquals(
                1,
                stream
                    .receive()
                    ?.body
                    ?.single()
                    ?.toInt(),
            )
            runCurrent()
            assertEquals(0, fake.pendingReads)
            assertEquals(
                2,
                stream
                    .receive()
                    ?.body
                    ?.single()
                    ?.toInt(),
            )
            runCurrent()
            assertEquals(1, fake.pendingReads)
            assertEquals(
                3,
                stream
                    .receive()
                    ?.body
                    ?.single()
                    ?.toInt(),
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
                        Http3RpcTransport(fake, coroutineContext = coroutineContext),
                        fake,
                    )
                fake.headers(headers)
                val error = stream.receiveFailure()
                assertInstanceOf(TrevRpcException::class.java, error)
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
            partialFake.deliverRead(byteArrayOf(0, 0), endOfStream = true)
            runCurrent()
            val error = partial.receiveFailure()
            assertEquals(Code.INTERNAL, (error as TrevRpcException).status.code)
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
            val send = async { stream.send(byteArrayOf(1)) }
            runCurrent()
            assertFalse(fake.onlyWrite().endOfStream)
            fake.headers(validHeaders())
            val message = RpcStreamFrame.message(byteArrayOf(9))
            val terminal = RpcStreamFrame.status(Status.unavailable("remote result"))
            fake.deliverRead(frame(WireCodec.encode(message)) + frame(WireCodec.encode(terminal)))
            runCurrent()
            fake.completeWrite()
            fake.fail(IllegalStateException("late request write failure"))
            assertTrue(send.isCancelled)

            assertArrayEquals(message.body, stream.receive()?.body)
            runCurrent()
            val actual = stream.receive()
            assertEquals(Code.UNAVAILABLE, actual?.status?.code)
            assertEquals("remote result", actual?.status?.message)
            assertNull(stream.receive())
            assertEquals(1, fake.cancelCalls)
        }

    @Test
    fun `close and terminal callbacks race to exactly one completion`() =
        runTest {
            val fake = FakeFactory()
            val stream =
                openStreaming(
                    Http3RpcTransport(fake, coroutineContext = coroutineContext),
                    fake,
                )
            stream.close()
            stream.close()
            fake.cancelCallback()
            fake.fail(IllegalStateException("late failure"))
            assertEquals(1, fake.cancelCalls)
            assertNull(stream.receive())
        }

    @Test
    fun `first transport error is retained across later callbacks`() =
        runTest {
            val fake = FakeFactory()
            val stream =
                openStreaming(
                    Http3RpcTransport(fake, coroutineContext = coroutineContext),
                    fake,
                )
            fake.fail(IllegalArgumentException("first"))
            fake.cancelCallback()
            fake.fail(IllegalStateException("second"))

            val error = stream.receiveFailure() as TrevRpcException
            assertEquals(Code.UNAVAILABLE, error.status.code)
            assertEquals("first", error.cause?.message)
            assertEquals(1, fake.cancelCalls)
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
    assertRequestEquals(request, WireCodec.decodeRequest(decodeFrame(initial.bytes)))
    fake.completeWrite()
    return opening.await()
}

private suspend fun RpcClientStream.receiveFailure(): Throwable =
    runCatching { receive() }.exceptionOrNull() ?: error("receive unexpectedly succeeded")

private fun streamRequest(): RpcRequest = RpcRequest("example.Service", "Stream", kindValue = RpcKind.BIDIRECTIONAL_STREAMING.value)

private fun validHeaders(contentType: String = TREV_RPC_CONTENT_TYPE): ResponseHeaders =
    ResponseHeaders(200, listOf("Content-Type" to contentType))

private fun decodeFrame(bytes: ByteArray): ByteArray = FrameDecoder().feed(bytes).single()

private fun assertMessageWrite(
    bytes: ByteArray,
    expected: ByteArray,
) {
    val message = WireCodec.decodeStreamFrame(decodeFrame(bytes))
    assertEquals(RpcStreamFrameKind.MESSAGE, message.kind)
    assertArrayEquals(expected, message.body)
}

private fun assertRequestEquals(
    expected: RpcRequest,
    actual: RpcRequest,
) {
    assertEquals(expected.service, actual.service)
    assertEquals(expected.method, actual.method)
    assertEquals(expected.kindValue, actual.kindValue)
    assertEquals(expected.version, actual.version)
    assertEquals(expected.timeoutNanos, actual.timeoutNanos)
    assertEquals(expected.metadata, actual.metadata)
    assertArrayEquals(expected.body, actual.body)
}

private class FakeFactory : DuplexStreamFactory {
    private lateinit var callback: DuplexCallback
    private val stream = FakeStream()

    val cancelCalls: Int
        get() = stream.cancelCalls

    val pendingReads: Int
        get() = stream.reads.size

    val pendingWrites: Int
        get() = stream.writes.size

    override fun open(callback: DuplexCallback): DuplexStream {
        this.callback = callback
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
}

private class FakeStream : DuplexStream {
    val reads = ArrayDeque<ByteBuffer>()
    val writes = ArrayDeque<FakeWrite>()
    var cancelCalls = 0

    override fun start() = Unit

    override fun read(buffer: ByteBuffer) {
        check(buffer.isDirect)
        reads.add(buffer)
    }

    override fun write(
        buffer: ByteBuffer,
        endOfStream: Boolean,
    ) {
        check(buffer.isDirect)
        check(writes.isEmpty()) { "only one write may be in flight" }
        val copy = buffer.duplicate()
        val bytes = ByteArray(copy.remaining())
        copy.get(bytes)
        writes.add(FakeWrite(buffer, bytes, endOfStream))
    }

    override fun flush() = Unit

    override fun cancel() {
        cancelCalls++
    }
}

private data class FakeWrite(
    val buffer: ByteBuffer,
    val bytes: ByteArray,
    val endOfStream: Boolean,
)
