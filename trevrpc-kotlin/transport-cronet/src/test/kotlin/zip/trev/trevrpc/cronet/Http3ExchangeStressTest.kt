package zip.trev.trevrpc.cronet

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.RpcClientStream
import zip.trev.trevrpc.RpcKind
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.TrevRpcException
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random
import kotlin.time.Duration.Companion.seconds

/**
 * Bounded multi-threaded races of a real [Http3Exchange] against a fake Cronet stream. The
 * interleavings cannot be pinned by a hook, because the state machines make the harmful ones
 * impossible by construction (see [ExchangeStateTest] for the deterministic checks); these runs
 * confirm that nothing outside them reintroduces one.
 */
class Http3ExchangeStressTest {
    /** A thread-safe fake stream: Cronet's callbacks and the exchange's calls arrive on different threads. */
    private class RacingStream : DuplexStream {
        lateinit var callback: DuplexCallback
        val cancelCalls = AtomicInteger()
        val writes = ConcurrentLinkedQueue<Pair<ByteBuffer, Boolean>>()
        val startEntered = CountDownLatch(1)

        @Volatile var startGate: CountDownLatch? = null

        override fun start() {
            startEntered.countDown()
            startGate?.let { check(it.await(5, TimeUnit.SECONDS)) { "start gate timed out" } }
        }

        override fun read(buffer: ByteBuffer) = Unit

        override fun write(
            buffer: ByteBuffer,
            endOfStream: Boolean,
        ) {
            writes.add(buffer to endOfStream)
        }

        override fun flush() = Unit

        override fun cancel() {
            cancelCalls.incrementAndGet()
            callback.onCanceled(this)
        }
    }

    private class RacingFactory : DuplexStreamFactory {
        val stream = RacingStream()
        val opened = CountDownLatch(1)

        override fun open(callback: DuplexCallback): DuplexStream {
            stream.callback = callback
            opened.countDown()
            return stream
        }
    }

    private fun streamRequest() = RpcRequest("/example.Service/Stream", kindValue = RpcKind.BIDIRECTIONAL_STREAMING.value)

    private fun RacingStream.awaitWrite(): Pair<ByteBuffer, Boolean> {
        val deadline = System.nanoTime() + 5_000_000_000L
        while (System.nanoTime() < deadline) {
            writes.poll()?.let { return it }
            Thread.onSpinWait()
        }
        error("the exchange issued no write")
    }

    private fun RacingStream.completeWrite(write: Pair<ByteBuffer, Boolean>) {
        write.first.position(write.first.limit())
        callback.onWrite(this, write.first, write.second)
    }

    /**
     * A caller cancels a send while Cronet completes its write. Cancellation must reach `receive()`
     * as cancellation, never as the INTERNAL "unexpected request write" a completion that outran the
     * cancel handler used to produce, before the call state settled in one step.
     */
    @Test
    fun `cancelling a send while Cronet completes its write never surfaces as an internal error`() =
        runBlocking {
            val iterations = 1500
            val internal = AtomicInteger()
            val cancelled = AtomicInteger()
            val completedFirst = AtomicInteger()
            val unexpected = ConcurrentLinkedQueue<Throwable>()
            RaceThreads(2).use { threads ->
                repeat(iterations) {
                    val factory = RacingFactory()
                    val transport =
                        Http3RpcTransport(
                            factory,
                            CronetTransportOptions(closeTimeout = 3.seconds),
                            Dispatchers.Default,
                            shutdownContext = Dispatchers.Default,
                        )
                    val opening = async(Dispatchers.Default) { transport.openStream(streamRequest()) }
                    assertTrue(factory.opened.await(5, TimeUnit.SECONDS))
                    assertTrue(factory.stream.startEntered.await(5, TimeUnit.SECONDS))
                    factory.stream.callback.onReady(factory.stream)
                    factory.stream.completeWrite(factory.stream.awaitWrite())
                    val stream: RpcClientStream = opening.await()

                    // A send cancelled after its write completed may still finish normally (as cancelled),
                    // leaving the exchange open; only a send that threw cancellation must fail the call.
                    val sendThrewCancellation = AtomicBoolean(false)
                    val send =
                        launch(Dispatchers.Default) {
                            try {
                                stream.send(byteArrayOf(1))
                            } catch (error: CancellationException) {
                                sendThrewCancellation.set(true)
                                throw error
                            }
                        }
                    val write = factory.stream.awaitWrite()
                    threads.round({ factory.stream.completeWrite(write) }, { send.cancel() })
                    send.join()

                    if (sendThrewCancellation.get()) {
                        val failure = runCatching { withTimeout(5_000) { stream.receive() } }.exceptionOrNull()
                        when {
                            failure is TrevRpcException && failure.status.code == Code.INTERNAL -> internal.incrementAndGet()
                            failure is CancellationException && failure !is TimeoutCancellationException -> cancelled.incrementAndGet()
                            else -> unexpected.add(failure ?: AssertionError("receive() returned after a canceled send"))
                        }
                    } else {
                        completedFirst.incrementAndGet()
                    }
                    runCatching { stream.close(null) }
                    transport.close()
                }
            }
            assertEquals(0, internal.get(), "canceled sends that surfaced INTERNAL")
            assertTrue(unexpected.isEmpty(), "unexpected outcomes: $unexpected")
            assertEquals(iterations, cancelled.get() + completedFirst.get())
        }

    /**
     * Close, a terminal callback during `start()` and the end of `start()` race. Every lease settles
     * so close completes, the native stream is canceled at most once, and never after it reported a
     * terminal callback during start; a stream that did not terminate is canceled exactly once.
     */
    @Test
    fun `start racing close and a terminal callback settles every lease and cancels at most once`() =
        runBlocking {
            val random = Random(42)
            RaceThreads(3).use { threads ->
                repeat(300) { iteration ->
                    val factory = RacingFactory()
                    val gate = CountDownLatch(1)
                    factory.stream.startGate = gate
                    val transport =
                        Http3RpcTransport(
                            factory,
                            CronetTransportOptions(closeTimeout = 2.seconds),
                            Dispatchers.Default,
                            shutdownContext = Dispatchers.Default,
                        )
                    val opening = async(Dispatchers.Default) { runCatching { transport.openStream(streamRequest()) } }
                    assertTrue(factory.stream.startEntered.await(5, TimeUnit.SECONDS))

                    val terminalDuringStart = random.nextBoolean()
                    val delayGate = random.nextBoolean()
                    val terminatorDone = CountDownLatch(1)
                    threads.round(
                        { runBlocking { runCatching { transport.close() } } },
                        {
                            try {
                                if (terminalDuringStart) factory.stream.callback.onFailed(factory.stream, RuntimeException("net::ERR"))
                            } finally {
                                terminatorDone.countDown()
                            }
                        },
                        {
                            // start() returns only after the terminal callback, if any, was delivered.
                            check(terminatorDone.await(5, TimeUnit.SECONDS)) { "the terminal callback did not return" }
                            if (delayGate) Thread.sleep(2)
                            gate.countDown()
                        },
                    )
                    val result = withTimeout(10_000) { opening.await() }
                    assertTrue(result.isFailure, "iteration $iteration: the call opened on a closed channel")

                    withTimeout(10_000) { transport.close() }
                    val context = "iteration $iteration terminalDuringStart=$terminalDuringStart"
                    assertEquals(0, transport.activeExchangeCount, "leases left behind: $context")
                    assertEquals(if (terminalDuringStart) 0 else 1, factory.stream.cancelCalls.get(), "native cancels: $context")
                }
            }
        }
}
