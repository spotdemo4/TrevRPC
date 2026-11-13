package zip.trev.trevrpc.cronet

import java.util.concurrent.ExecutionException
import java.util.concurrent.Executors
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.AtomicInteger
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

/**
 * Long-lived threads that run rounds of racing actions. A race test repeats its round thousands of
 * times so that the rare interleaving comes up; starting fresh threads for every round costs
 * seconds and adds nothing to the race, so the threads are kept and only the start is per round.
 *
 * Close it (`use { }`) so the threads end with the test.
 */
internal class RaceThreads(
    private val parties: Int,
    private val roundTimeout: Duration = 10.seconds,
) : AutoCloseable {
    private val names = AtomicInteger()
    private val executor =
        Executors.newFixedThreadPool(parties) { task ->
            // Daemon, so a thread stuck in a deadlock the round timed out on cannot keep the test JVM alive.
            Thread(task, "race-${names.incrementAndGet()}").apply { isDaemon = true }
        }

    /**
     * Runs one round: each of [actions] on its own thread, all released at once, and returns once
     * every one has finished. An action's exception is rethrown here, so it fails the test, and a
     * round that does not finish within the timeout fails it too.
     */
    fun round(vararg actions: () -> Unit) {
        require(actions.size == parties) { "expected $parties actions, got ${actions.size}" }
        val deadline = System.nanoTime() + roundTimeout.inWholeNanoseconds
        val arrived = AtomicInteger()
        val running =
            actions.map { action ->
                executor.submit {
                    awaitOthers(arrived, deadline)
                    action()
                }
            }
        for (future in running) {
            try {
                future.get(maxOf(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS)
            } catch (error: ExecutionException) {
                throw error.cause ?: error
            } catch (error: TimeoutException) {
                throw AssertionError("a race round did not finish within $roundTimeout (deadlock?)", error)
            }
        }
    }

    /**
     * Spins until every party of the round has arrived, then waits a few random spins more. A
     * blocking barrier lets the last party run at once while the others are still being unparked,
     * which mostly serializes critical sections as short as the ones these tests race; spinning
     * parties leave together, and the jitter varies which one gets ahead and by how much, so the
     * windows between two locked steps are hit from both sides. With a method of
     * [CallStateMachine] or [NativeStreamLifecycle] split in two, this start caught the split at
     * least as often as fresh threads behind a barrier, and far more often for most of them.
     */
    private fun awaitOthers(
        arrived: AtomicInteger,
        deadline: Long,
    ) {
        arrived.incrementAndGet()
        var spins = 0
        while (arrived.get() < parties) {
            if (System.nanoTime() > deadline) throw TimeoutException("not every race thread arrived within $roundTimeout")
            // Let a party that is not running yet have the CPU on a machine with fewer cores than parties.
            if (++spins % 1_024 == 0) Thread.yield() else Thread.onSpinWait()
        }
        repeat(ThreadLocalRandom.current().nextInt(JITTER_SPINS)) { Thread.onSpinWait() }
    }

    override fun close() {
        executor.shutdownNow()
        check(executor.awaitTermination(roundTimeout.inWholeMilliseconds, TimeUnit.MILLISECONDS)) {
            "race threads did not stop within $roundTimeout"
        }
    }

    private companion object {
        /**
         * Spin hints, a microsecond or so: wider jitter caught the splits no more often, at several
         * times the cost per round.
         */
        const val JITTER_SPINS = 64
    }
}
