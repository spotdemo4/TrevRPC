package zip.trev.trevrpc

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.test.advanceTimeBy
import kotlinx.coroutines.test.runCurrent
import kotlinx.coroutines.test.runTest
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@OptIn(ExperimentalCoroutinesApi::class)
class ReconnectingRpcTransportTest {
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
            val managed =
                ReconnectingRpcTransport(
                    backgroundScope,
                    connector = { connections.removeFirst().connection() },
                    backoff = noJitter(),
                )

            runCurrent()
            assertEquals(1, managed.waitUntilReady())
            assertUnavailable { managed.unary(REQUEST) }
            runCurrent()

            assertEquals(1, first.unaryCalls.get())
            assertEquals(0, second.unaryCalls.get())
            assertEquals(2, managed.waitUntilReady())
            assertEquals("second", managed.unary(REQUEST).body.decodeToString())
            assertEquals(1, second.unaryCalls.get())
            managed.close()
        }

    @Test
    fun `calls fail fast while reconnect is in progress`() =
        runTest {
            val first = TestGeneration()
            val second = TestGeneration(response = "ready")
            val allowReconnect = CompletableDeferred<Unit>()
            var connects = 0
            val managed =
                ReconnectingRpcTransport(
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
            assertInstanceOf(ReconnectingRpcTransportState.Reconnecting::class.java, managed.state.value)

            assertUnavailable { managed.unary(REQUEST) }
            assertEquals(0, second.unaryCalls.get())
            allowReconnect.complete(Unit)
            runCurrent()
            assertEquals(2, managed.waitUntilReady())
            managed.close()
        }

    @Test
    fun `closed lifecycle reconnects and advances generation`() =
        runTest {
            val first = TestGeneration(response = "first")
            val second = TestGeneration(response = "second")
            val connections = ArrayDeque(listOf(first, second))
            val managed =
                ReconnectingRpcTransport(
                    backgroundScope,
                    connector = { connections.removeFirst().connection() },
                    backoff = noJitter(),
                )

            runCurrent()
            assertEquals("first", managed.unary(REQUEST).body.decodeToString())
            first.closed.complete(Unit)
            runCurrent()

            assertEquals(2, managed.waitUntilReady())
            assertEquals(2, managed.generation)
            assertEquals(1, first.closeCalls.get())
            assertEquals("second", managed.unary(REQUEST).body.decodeToString())
            managed.close()
        }

    @Test
    fun `close is terminal and releases the current generation once`() =
        runTest {
            val generation = TestGeneration()
            var connects = 0
            val managed =
                ReconnectingRpcTransport(
                    backgroundScope,
                    connector = {
                        connects++
                        generation.connection()
                    },
                    backoff = noJitter(),
                )

            runCurrent()
            managed.close()
            managed.close()
            runCurrent()

            assertInstanceOf(ReconnectingRpcTransportState.Closed::class.java, managed.state.value)
            assertEquals(1, generation.closeCalls.get())
            assertEquals(1, connects)
            assertUnavailable { managed.unary(REQUEST) }
            assertUnavailable { managed.waitUntilReady() }
        }

    @Test
    fun `reconnect failures use deterministic capped exponential backoff and jitter`() =
        runTest {
            val generation = TestGeneration()
            var connects = 0
            val managed =
                ReconnectingRpcTransport(
                    backgroundScope,
                    connector = {
                        connects++
                        if (connects < 4) error("connect $connects failed")
                        generation.connection()
                    },
                    backoff =
                        ReconnectBackoff(
                            initialDelay = 100.milliseconds,
                            maxDelay = 250.milliseconds,
                            multiplier = 2.0,
                            jitterRatio = 0.0,
                        ),
                )

            runCurrent()
            assertEquals(1, connects)
            assertRetry(1, 100, managed)

            advanceTimeBy(100.milliseconds)
            runCurrent()
            assertEquals(2, connects)
            assertRetry(2, 200, managed)

            advanceTimeBy(200.milliseconds)
            runCurrent()
            assertEquals(3, connects)
            assertRetry(3, 250, managed)

            advanceTimeBy(249.milliseconds)
            runCurrent()
            assertEquals(3, connects)
            advanceTimeBy(1.milliseconds)
            runCurrent()
            assertEquals(4, connects)
            assertEquals(1, managed.waitUntilReady())

            val jittered =
                ReconnectBackoff(
                    initialDelay = 100.milliseconds,
                    maxDelay = 1.seconds,
                    jitterRatio = 0.2,
                    random = { 1.0 },
                ).delayForAttempt(1)
            assertEquals(120.milliseconds, jittered)
            managed.close()
        }

    private fun assertRetry(
        attempts: Int,
        delayMillis: Int,
        managed: ReconnectingRpcTransport,
    ) {
        val state = assertInstanceOf(ReconnectingRpcTransportState.Connecting::class.java, managed.state.value)
        assertEquals(attempts, state.failedAttempts)
        assertEquals(delayMillis.milliseconds, state.retryDelay)
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

    private fun noJitter(): ReconnectBackoff =
        ReconnectBackoff(
            initialDelay = 1.milliseconds,
            maxDelay = 1.milliseconds,
            jitterRatio = 0.0,
        )

    companion object {
        private val REQUEST = RpcRequest("test.Service", "Call")
    }
}

private class TestGeneration(
    response: String = "ok",
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

            override suspend fun openStream(
                request: RpcRequest,
                requestBody: Flow<ByteArray>,
            ): RpcTransportStream =
                object : RpcTransportStream {
                    override suspend fun receive(): RpcStreamFrame? = null

                    override suspend fun close(cause: Throwable?) = Unit
                }
        }

    fun connection(): RpcTransportConnection =
        RpcTransportConnection(
            transport = transport,
            lifecycle = RpcTransportLifecycle { closed.await() },
            closeTransport = {
                closeCalls.incrementAndGet()
                closed.complete(Unit)
            },
        )
}
