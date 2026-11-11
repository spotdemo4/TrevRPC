package zip.trev.trevrpc.cronet

import kotlinx.coroutines.CancellableContinuation
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.suspendCancellableCoroutine
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import java.nio.ByteBuffer
import java.util.concurrent.CountDownLatch
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.random.Random

/** The call and native-stream state machines, without Cronet or a coroutine dispatcher in the way. */
class ExchangeStateTest {
    /** Runs [block] with an active continuation, the way a suspended send holds one. */
    private fun withContinuation(block: (CancellableContinuation<Unit>) -> Unit) =
        runBlocking {
            launch(start = CoroutineStart.UNDISPATCHED) {
                suspendCancellableCoroutine<Unit> { block(it) }
            }.cancel()
        }

    private fun pendingWrite(
        continuation: CancellableContinuation<Unit>,
        endOfStream: Boolean = false,
    ) = PendingWrite(ByteBuffer.allocateDirect(1), endOfStream, continuation)

    @Test
    fun `settling because a write was canceled takes the write and leaves no open call behind`() =
        withContinuation { continuation ->
            val call = CallStateMachine()
            val pending = pendingWrite(continuation)
            assertNull(call.installWrite(pending))

            val cause = RuntimeException("canceled")
            val transition = call.settleWrite(pending, cause)!!

            assertSame(pending, transition.pendingWrite)
            assertSame(cause, transition.state.cause)
            assertSame(transition.state, call.state)
            // The write completion that lands next, as in the race this guards, finds the write gone
            // and the call already settled, so it has nothing to fail.
            assertNull(call.takeCompletedWrite(pending.buffer, pending.endOfStream))
            assertNull(call.settleIfOpen(RuntimeException("unexpected write")))
        }

    @Test
    fun `a write completion never finds its write gone while the call is still open`() =
        withContinuation { continuation ->
            val violations = AtomicInteger()
            repeat(3_000) {
                val call = CallStateMachine()
                val pending = pendingWrite(continuation)
                assertNull(call.installWrite(pending))
                val barrier = CyclicBarrier(2)
                val completer =
                    Thread {
                        barrier.await()
                        if (call.takeCompletedWrite(pending.buffer, pending.endOfStream) == null && call.state is CallState.Open) {
                            violations.incrementAndGet()
                        }
                    }
                completer.start()
                barrier.await()
                call.settleWrite(pending, RuntimeException("canceled"))
                completer.join()
            }
            assertEquals(0, violations.get())
        }

    @Test
    fun `a write completion takes only the write it names and leaves nothing to settle`() =
        withContinuation { continuation ->
            val call = CallStateMachine()
            val pending = pendingWrite(continuation)
            assertNull(call.installWrite(pending))

            assertNull(call.takeCompletedWrite(ByteBuffer.allocateDirect(1), false), "another buffer is not this write")
            assertNull(call.takeCompletedWrite(pending.buffer, true), "another half-close flag is not this write")
            assertSame(pending, call.takeCompletedWrite(pending.buffer, false))

            assertFalse(call.isPending(pending))
            assertNull(call.settleWrite(pending, RuntimeException("flush failed")), "the write completed first")
            assertSame(CallState.Open, call.state)
        }

    @Test
    fun `an ended call refuses a write, a second write is a bug and a canceled send is not installed`() {
        withContinuation { continuation ->
            val settled = CallStateMachine().also { it.settle(null) }
            assertInstanceOf(CallState.Settled::class.java, settled.installWrite(pendingWrite(continuation)))

            val remote = CallStateMachine().also { it.beginRemoteTerminal(Status.ok()) }
            assertInstanceOf(CallState.RemoteTerminal::class.java, remote.installWrite(pendingWrite(continuation)))

            val open = CallStateMachine()
            assertNull(open.installWrite(pendingWrite(continuation)))
            assertThrows(IllegalStateException::class.java) { open.installWrite(pendingWrite(continuation)) }
        }
        runBlocking {
            launch(start = CoroutineStart.UNDISPATCHED) {
                suspendCancellableCoroutine<Unit> { continuation ->
                    continuation.cancel()
                    val call = CallStateMachine()
                    val pending = pendingWrite(continuation)
                    assertSame(CallState.Open, call.installWrite(pending), "the send is gone, the call is open")
                    assertFalse(call.isPending(pending))
                }
            }
        }
    }

    @Test
    fun `the server's status takes the write in flight and survives into the settled state`() =
        withContinuation { continuation ->
            val call = CallStateMachine()
            val pending = pendingWrite(continuation)
            assertNull(call.installWrite(pending))

            val remote = call.beginRemoteTerminal(Status.invalidArgument("bad"))!!
            assertSame(pending, remote.pendingWrite)
            assertEquals(
                Code.INVALID_ARGUMENT,
                remote.state
                    .sendError()
                    .status.code,
            )
            assertNull(call.beginRemoteTerminal(Status.ok()), "the first status wins")
            assertNull(call.settleIfOpen(RuntimeException("late failure")), "a remote terminal call is no longer open")
            assertNull(call.takeCompletedWrite(pending.buffer, false))

            val settled = call.settle(null)!!
            assertNull(settled.pendingWrite)
            assertEquals(Code.INVALID_ARGUMENT, settled.state.remoteStatus?.code)
            assertEquals(
                Code.INVALID_ARGUMENT,
                settled.state
                    .sendError()
                    .status.code,
            )
            assertNull(call.settle(RuntimeException("again")), "a call settles once")
            assertNull(call.beginRemoteTerminal(Status.ok()), "a settled call takes no status")
        }

    @Test
    fun `sends fail with why the call ended`() {
        val ok = CallStateMachine().also { it.beginRemoteTerminal(Status.ok()) }.state as CallState.Ended
        assertEquals(Code.CANCELLED, ok.sendError().status.code)

        val closed = CallStateMachine().also { it.settle(null) }.state as CallState.Ended
        assertEquals(Code.CANCELLED, closed.sendError().status.code)
        assertNull(closed.remoteStatus)

        val cause = TrevRpcException(Status.unavailable("down"))
        val failed = CallStateMachine().also { it.settle(cause) }.state as CallState.Ended
        assertEquals(Code.UNAVAILABLE, failed.sendError().status.code)
        assertSame(cause, failed.sendError().cause)
    }

    @Test
    fun `a cancel before the stream is opened abandons it unstarted`() {
        val native = NativeStreamLifecycle()
        assertFalse(native.requestCancel(), "nothing to cancel yet")
        assertFalse(native.beginStart(), "the stream is abandoned, never started")
        assertFalse(native.requestCancel())
        assertFalse(native.terminated())
    }

    @Test
    fun `a cancel during start is issued once start returns`() {
        val native = NativeStreamLifecycle()
        assertTrue(native.beginStart())
        assertFalse(native.requestCancel(), "a stream cannot be canceled while it starts")
        assertFalse(native.requestCancel())
        assertEquals(NativeStreamLifecycle.AfterStart.CANCEL, native.startReturned())
        assertFalse(native.requestCancel(), "the cancel was issued already")
        assertTrue(native.terminated(), "the terminal callback releases the lease")
        assertFalse(native.terminated())
    }

    @Test
    fun `a terminal callback during start defers the release and suppresses a cancel`() {
        val native = NativeStreamLifecycle()
        assertTrue(native.beginStart())
        assertFalse(native.terminated(), "start is still running")
        assertFalse(native.requestCancel())
        assertEquals(NativeStreamLifecycle.AfterStart.FINISHED, native.startReturned())
        assertFalse(native.requestCancel(), "no cancel after a terminal callback")
        assertFalse(native.terminated(), "released once")
    }

    @Test
    fun `a started stream is canceled at most once and never after its terminal callback`() {
        val native = NativeStreamLifecycle()
        assertTrue(native.beginStart())
        assertEquals(NativeStreamLifecycle.AfterStart.RUNNING, native.startReturned())
        assertTrue(native.requestCancel())
        assertFalse(native.requestCancel())
        assertTrue(native.terminated())

        val finished = NativeStreamLifecycle()
        assertTrue(finished.beginStart())
        assertEquals(NativeStreamLifecycle.AfterStart.RUNNING, finished.startReturned())
        assertTrue(finished.terminated())
        assertFalse(finished.requestCancel())
    }

    @Test
    fun `a failed start has nothing to cancel or release again`() {
        val native = NativeStreamLifecycle()
        assertTrue(native.beginStart())
        native.startFailed()
        assertFalse(native.requestCancel())
        assertFalse(native.terminated())
    }

    /**
     * Start, cancel and the terminal callback race. Whatever the interleaving, the stream is canceled
     * at most once, a cancel is never lost and never follows a terminal callback seen during start,
     * and the native side is reported done exactly once.
     */
    @Test
    fun `start cancel and terminal callbacks race without losing or repeating either`() {
        val random = Random(7)
        repeat(3_000) { iteration ->
            val native = NativeStreamLifecycle()
            val cancels = AtomicInteger()
            val releases = AtomicInteger()
            val cancelRequested = random.nextBoolean()
            val cancelBeforeOpen = cancelRequested && random.nextBoolean()
            val terminalFires = random.nextBoolean()
            val startDecided = CountDownLatch(1)

            val startedFlag = AtomicBoolean(false)
            val finishedFlag = AtomicBoolean(false)
            if (cancelBeforeOpen) assertFalse(native.requestCancel())

            val barrier = CyclicBarrier(3)
            val starter =
                Thread {
                    barrier.await()
                    startedFlag.set(native.beginStart())
                    startDecided.countDown()
                    if (!startedFlag.get()) {
                        releases.incrementAndGet()
                        return@Thread
                    }
                    Thread.onSpinWait()
                    val afterStart = native.startReturned()
                    if (afterStart == NativeStreamLifecycle.AfterStart.FINISHED) {
                        finishedFlag.set(true)
                        releases.incrementAndGet()
                    } else if (afterStart == NativeStreamLifecycle.AfterStart.CANCEL) {
                        cancels.incrementAndGet()
                    }
                }
            val canceller =
                Thread {
                    barrier.await()
                    if (cancelRequested && !cancelBeforeOpen) repeat(2) { if (native.requestCancel()) cancels.incrementAndGet() }
                }
            val terminator =
                Thread {
                    barrier.await()
                    // Cronet reports a terminal callback only for a stream whose start() was called.
                    startDecided.await()
                    if (terminalFires && startedFlag.get() && native.terminated()) releases.incrementAndGet()
                }
            val threads = listOf(starter, canceller, terminator)
            threads.forEach(Thread::start)
            threads.forEach(Thread::join)

            val context = "iteration $iteration cancelRequested=$cancelRequested beforeOpen=$cancelBeforeOpen terminal=$terminalFires"
            assertTrue(cancels.get() <= 1, "canceled more than once: $context")
            val started = startedFlag.get()
            if (!started) {
                assertEquals(0, cancels.get(), "abandoned stream was canceled: $context")
                assertEquals(1, releases.get(), "abandoned stream not released once: $context")
            } else {
                assertEquals(if (terminalFires) 1 else 0, releases.get(), "release count: $context")
                if (finishedFlag.get()) assertEquals(0, cancels.get(), "canceled after a terminal callback during start: $context")
                if (!terminalFires && cancelRequested) assertEquals(1, cancels.get(), "cancel was lost: $context")
            }
        }
    }
}
