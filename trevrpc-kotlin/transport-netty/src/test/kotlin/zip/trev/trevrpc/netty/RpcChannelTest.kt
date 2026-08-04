package zip.trev.trevrpc.netty

import io.netty.channel.MultiThreadIoEventLoopGroup
import io.netty.channel.embedded.EmbeddedChannel
import io.netty.channel.nio.NioIoHandler
import io.netty.util.concurrent.ImmediateEventExecutor
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.async
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.RpcChannelState
import zip.trev.trevrpc.RpcClientStream
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcResponse
import zip.trev.trevrpc.RpcStreamFrame
import zip.trev.trevrpc.RpcTransport
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class RpcChannelTest {
    @Test
    fun `calls fail fast during initial connection`() =
        runTest {
            val generation = TestGeneration()
            val allowConnect = CompletableDeferred<Unit>()
            val channel =
                createNettyRpcChannel(backgroundScope, backoff = noJitter()) {
                    allowConnect.await()
                    generation.connection()
                }

            runCurrent()
            assertEquals(RpcChannelState.CONNECTING, channel.state.value)
            assertUnavailable { channel.unary(REQUEST) }

            allowConnect.complete(Unit)
            runCurrent()
            channel.awaitReady()
            channel.close()
        }

    @Test
    fun `failed call is not replayed on the next generation`() =
        runTest {
            val first = TestGeneration()
            val second = TestGeneration(response = "second")
            first.unary = {
                first.closed.complete(Unit)
                throw TrevRpcException(Status.unavailable("connection lost"))
            }
            val connections = ArrayDeque(listOf(first, second))
            val channel =
                createNettyRpcChannel(
                    backgroundScope,
                    connector = { connections.removeFirst().connection() },
                    backoff = noJitter(),
                )

            runCurrent()
            channel.awaitReady()
            assertUnavailable { channel.unary(REQUEST) }
            runCurrent()

            assertEquals(1, first.unaryCalls.get())
            assertEquals(0, second.unaryCalls.get())
            channel.awaitReady()
            assertEquals("second", channel.unary(REQUEST).body.decodeToString())
            assertEquals(1, second.unaryCalls.get())
            channel.close()
        }

    @Test
    fun `calls fail fast while reconnect is in progress`() =
        runTest {
            val first = TestGeneration()
            val second = TestGeneration(response = "ready")
            val allowReconnect = CompletableDeferred<Unit>()
            var connects = 0
            val channel =
                createNettyRpcChannel(
                    backgroundScope,
                    connector = {
                        connects++
                        if (connects == 1) {
                            first.connection()
                        } else {
                            allowReconnect.await()
                            second.connection()
                        }
                    },
                    backoff = noJitter(),
                )

            runCurrent()
            first.closed.complete(Unit)
            runCurrent()
            assertEquals(RpcChannelState.CONNECTING, channel.state.value)

            assertUnavailable { channel.unary(REQUEST) }
            assertEquals(0, second.unaryCalls.get())
            allowReconnect.complete(Unit)
            runCurrent()
            channel.awaitReady()
            channel.close()
        }

    @Test
    fun `closed lifecycle reconnects and advances generation`() =
        runTest {
            val first = TestGeneration(response = "first")
            val second = TestGeneration(response = "second")
            val connections = ArrayDeque(listOf(first, second))
            val channel =
                createNettyRpcChannel(
                    backgroundScope,
                    connector = { connections.removeFirst().connection() },
                    backoff = noJitter(),
                )

            runCurrent()
            assertEquals("first", channel.unary(REQUEST).body.decodeToString())
            first.closed.complete(Unit)
            runCurrent()

            assertEquals(1, connections.size)
            advanceTimeBy(1.milliseconds)
            runCurrent()
            channel.awaitReady()
            assertEquals(0, connections.size)
            assertEquals(1, first.closeCalls.get())
            assertEquals("second", channel.unary(REQUEST).body.decodeToString())
            channel.close()
        }

    @Test
    fun `close is terminal and releases the current generation once`() =
        runTest {
            val generation = TestGeneration()
            var connects = 0
            val channel =
                createNettyRpcChannel(
                    backgroundScope,
                    connector = {
                        connects++
                        generation.connection()
                    },
                    backoff = noJitter(),
                )

            runCurrent()
            channel.close()
            channel.close()
            runCurrent()

            assertEquals(RpcChannelState.CLOSED, channel.state.value)
            assertEquals(1, generation.closeCalls.get())
            assertEquals(1, connects)
            assertUnavailable { channel.unary(REQUEST) }
            assertUnavailable { channel.awaitReady() }
        }

    @Test
    fun `explicit close callers share and surface terminal cleanup failure`() =
        runTest {
            val failure = IllegalStateException("close failed")
            val generation = TestGeneration(closeFailure = failure)
            val channel = createNettyRpcChannel(backgroundScope, backoff = noJitter()) { generation.connection() }
            runCurrent()
            channel.awaitReady()

            val first = async { runCatching { channel.close() }.exceptionOrNull() }
            val second = async { runCatching { channel.close() }.exceptionOrNull() }
            runCurrent()

            val observed = listOf(first.await(), second.await(), runCatching { channel.close() }.exceptionOrNull())
            observed.forEach { error ->
                assertEquals(failure::class, error?.let { it::class })
                assertEquals(failure.message, error?.message)
            }
            assertEquals(1, generation.closeCalls.get())
            assertEquals(RpcChannelState.CLOSED, channel.state.value)
        }

    @Test
    fun `reconnect failures use deterministic capped exponential backoff and jitter`() =
        runTest {
            val generation = TestGeneration()
            var connects = 0
            val channel =
                createNettyRpcChannel(
                    backgroundScope,
                    connector = {
                        connects++
                        if (connects < 4) error("connect $connects failed")
                        generation.connection()
                    },
                    backoff =
                        NettyChannelBackoff(
                            initialDelay = 100.milliseconds,
                            maxDelay = 250.milliseconds,
                            multiplier = 2.0,
                            jitterRatio = 0.0,
                        ),
                )

            runCurrent()
            assertEquals(1, connects)
            assertEquals(RpcChannelState.CONNECTING, channel.state.value)

            advanceTimeBy(100.milliseconds)
            runCurrent()
            assertEquals(2, connects)
            assertEquals(RpcChannelState.CONNECTING, channel.state.value)

            advanceTimeBy(200.milliseconds)
            runCurrent()
            assertEquals(3, connects)
            assertEquals(RpcChannelState.CONNECTING, channel.state.value)

            advanceTimeBy(249.milliseconds)
            runCurrent()
            assertEquals(3, connects)
            advanceTimeBy(1.milliseconds)
            runCurrent()
            assertEquals(4, connects)
            channel.awaitReady()

            val jittered =
                NettyChannelBackoff(
                    initialDelay = 100.milliseconds,
                    maxDelay = 1.seconds,
                    jitterRatio = 0.2,
                    random = { 1.0 },
                ).delayForAttempt(1)
            assertEquals(120.milliseconds, jittered)
            channel.close()
        }

    @Test
    fun `future cancellation closes a value that lost prompt delivery`() =
        runTest {
            val promise = ImmediateEventExecutor.INSTANCE.newPromise<OwnedValue>()
            val value = OwnedValue()
            val delivery =
                async(start = CoroutineStart.UNDISPATCHED) {
                    promise.awaitValue { rejected -> rejected.close() }
                }

            promise.setSuccess(value)
            delivery.cancel()
            runCurrent()

            assertTrue(delivery.isCancelled)
            assertEquals(1, value.closeCalls.get())
        }

    @Test
    fun `bind future cancellation closes a channel that lost prompt delivery`() =
        runTest {
            val channel = EmbeddedChannel()
            val promise = channel.newPromise()
            val delivery = async(start = CoroutineStart.UNDISPATCHED) { promise.awaitChannel() }

            promise.setSuccess()
            delivery.cancel()
            runCurrent()

            assertTrue(delivery.isCancelled)
            assertFalse(channel.isOpen)
            channel.finishAndReleaseAll()
        }

    @Test
    fun `shutdown cleanup survives first caller cancellation and is shared`() =
        runTest {
            val entered = CompletableDeferred<Unit>()
            val release = CompletableDeferred<Unit>()
            val cleanupCalls = AtomicInteger()
            val coordinator =
                NettyShutdownCoordinator {
                    cleanupCalls.incrementAndGet()
                    entered.complete(Unit)
                    release.await()
                }

            val first = async(start = CoroutineStart.UNDISPATCHED) { coordinator.shutdown() }
            entered.await()
            first.cancel()
            val second = async(start = CoroutineStart.UNDISPATCHED) { coordinator.shutdown() }

            assertFalse(second.isCompleted)
            release.complete(Unit)
            second.await()
            assertEquals(1, cleanupCalls.get())
        }

    @Test
    fun `failed startup cleanup is non-cancellable bounded and forces release`() =
        runTest {
            val entered = CompletableDeferred<Unit>()
            val forceCalls = AtomicInteger()
            val cleanup =
                async {
                    runCatching {
                        runBoundedNonCancellableCleanup(
                            timeout = 10.milliseconds,
                            description = "test startup",
                            cleanup = {
                                entered.complete(Unit)
                                kotlinx.coroutines.awaitCancellation()
                            },
                            forceCleanup = { forceCalls.incrementAndGet() },
                        )
                    }.exceptionOrNull()
                }

            entered.await()
            cleanup.cancel()
            advanceTimeBy(10.milliseconds)
            runCurrent()

            assertTrue(cleanup.isCancelled)
            assertEquals(1, forceCalls.get())
        }

    @Test
    fun `owned event loop threads are detected for blocking close guards`() {
        val group = MultiThreadIoEventLoopGroup(1, NioIoHandler.newFactory())
        try {
            assertFalse(group.isOwnedEventLoopThread())
            val detected = CompletableFuture<Boolean>()
            group.next().execute { detected.complete(group.isOwnedEventLoopThread()) }
            assertTrue(detected.get(5, TimeUnit.SECONDS))
        } finally {
            group.shutdownGracefully(0, 0, TimeUnit.MILLISECONDS).syncUninterruptibly()
        }
    }

    private suspend fun assertUnavailable(block: suspend () -> Unit) {
        val error =
            try {
                block()
                throw AssertionError("expected unavailable")
            } catch (error: TrevRpcException) {
                error
            }
        assertEquals(Code.UNAVAILABLE, error.status.code)
    }

    private fun noJitter(): NettyChannelBackoff =
        NettyChannelBackoff(
            initialDelay = 1.milliseconds,
            maxDelay = 1.milliseconds,
            jitterRatio = 0.0,
        )

    companion object {
        private val REQUEST = RpcRequest("test.Service", "Call")
    }
}

private class OwnedValue : AutoCloseable {
    val closeCalls = AtomicInteger()

    override fun close() {
        closeCalls.incrementAndGet()
    }
}

private class TestGeneration(
    response: String = "ok",
    private val closeFailure: Throwable? = null,
) {
    val closed = CompletableDeferred<Unit>()
    val unaryCalls = AtomicInteger()
    val closeCalls = AtomicInteger()
    var unary: suspend () -> RpcResponse = { RpcResponse.ok(response.encodeToByteArray()) }

    private val transport =
        object : RpcTransport {
            override suspend fun unary(request: RpcRequest): RpcResponse {
                unaryCalls.incrementAndGet()
                return unary()
            }

            override suspend fun openStream(request: RpcRequest): RpcClientStream =
                object : RpcClientStream {
                    override suspend fun send(body: ByteArray) = Unit

                    override suspend fun finishSend() = Unit

                    override suspend fun receive(): RpcStreamFrame? = null

                    override suspend fun close(cause: Throwable?) = Unit
                }
        }

    fun connection(): NettyChannelConnection =
        NettyChannelConnection(
            transport = transport,
            awaitClosed = { closed.await() },
            closeTransport = {
                closeCalls.incrementAndGet()
                closed.complete(Unit)
                closeFailure?.let { throw it }
            },
        )
}
