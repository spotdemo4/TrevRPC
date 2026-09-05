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
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.measureTime

@OptIn(ExperimentalCoroutinesApi::class)
class Http3RpcTransportTest {
    @Test
    fun `provider channel is ready until explicitly closed`() =
        runTest {
            val channel =
                CronetChannel(
                    Http3RpcTransport(
                        FakeFactory(),
                        coroutineContext = coroutineContext,
                        shutdownContext = coroutineContext,
                    ),
                )

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
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext)
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
            assertTrue(fake.awaitCancel())
            assertEquals(1, fake.cancelCalls)
        }

    @Test
    fun `headers may precede ready and reads split and coalesced frames`() =
        runTest {
            val fake = FakeFactory()
            val transport = Http3RpcTransport(fake, coroutineContext = coroutineContext, shutdownContext = coroutineContext)
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
            fake.deliverRead(frame(WireCodec.encode(RpcStreamFrame.status(Status.ok()))))
            runCurrent()

            stream.finishSend()
            assertEquals(0, fake.pendingWrites)
            assertEquals(RpcStreamFrameKind.STATUS, stream.receive()?.kind)
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
            fake.deliverRead(frame(WireCodec.encode(RpcStreamFrame.status(Status.ok()))))
            runCurrent()

            finish.await()
            fake.completeWrite()
            assertEquals(RpcStreamFrameKind.STATUS, stream.receive()?.kind)
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
            assertTrue(fake.awaitCancel())
            assertEquals(1, fake.cancelCalls)
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
                val request = RpcRequest("example.Service", kind.name, kindValue = kind.value)
                val call =
                    async {
                        runCatching {
                            if (kind == RpcKind.UNARY) {
                                transport.unary(request)
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
            fake.deliverRead(frame(WireCodec.encode(RpcStreamFrame.status(Status.ok()))))
            runCurrent()

            assertEquals(RpcStreamFrameKind.STATUS, stream.receive()?.kind)
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
            assertRequestEquals(streamRequest(), WireCodec.decodeRequest(decodeFrame(initial.bytes)))
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
