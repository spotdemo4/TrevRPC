package zip.trev.trevrpc.netty

import io.netty.channel.EventLoopGroup
import io.netty.handler.codec.quic.QuicStreamChannel
import io.netty.util.AttributeKey
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.time.Duration

private val STREAM_JOB = AttributeKey.valueOf<Job>("trevrpc.netty.stream-job")
private val STREAM_CLOSE_EXPECTED = AttributeKey.valueOf<Boolean>("trevrpc.netty.stream-close-expected")

internal class NettyAdmissionGate {
    private val lock = Any()
    private var accepting = true

    fun <T> admit(block: () -> T): T? =
        synchronized(lock) {
            if (!accepting) return@synchronized null
            block()
        }

    fun stopAdmission() {
        synchronized(lock) { accepting = false }
    }
}

internal class NettyCallRegistry(
    private val admission: NettyAdmissionGate = NettyAdmissionGate(),
) {
    private val lock = Any()
    private val calls = linkedMapOf<Job, QuicStreamChannel>()
    private var drained = CompletableDeferred(Unit)

    fun stopAdmission() {
        admission.stopAdmission()
    }

    fun launch(
        scope: CoroutineScope,
        channel: QuicStreamChannel,
        onComplete: () -> Unit = {},
        block: suspend () -> Unit,
    ): Job? {
        lateinit var job: Job
        admission.admit {
            synchronized(lock) {
                if (calls.isEmpty()) drained = CompletableDeferred()
                job = scope.launch(start = CoroutineStart.LAZY, block = { block() })
                calls[job] = channel
            }
        } ?: return null

        job.invokeOnCompletion {
            if (channel.attr(STREAM_JOB).get() === job) channel.attr(STREAM_JOB).set(null)
            runCatching(onComplete)
            synchronized(lock) {
                calls.remove(job)
                if (calls.isEmpty()) drained.complete(Unit)
            }
        }
        channel.attr(STREAM_JOB).set(job)
        channel.closeFuture().addListener {
            if (channel.attr(STREAM_CLOSE_EXPECTED).get() != true) {
                job.cancel(CancellationException("RPC stream closed"))
            }
        }
        job.start()
        return job
    }

    fun expectClose(channel: QuicStreamChannel) {
        channel.attr(STREAM_CLOSE_EXPECTED).set(true)
    }

    fun cancel(
        channel: QuicStreamChannel,
        message: String,
    ) {
        if (channel.attr(STREAM_CLOSE_EXPECTED).get() != true) {
            channel.attr(STREAM_JOB).get()?.cancel(CancellationException(message))
        }
    }

    suspend fun awaitDrained() {
        synchronized(lock) { drained }.await()
    }

    suspend fun awaitDrained(timeout: Duration): Boolean = withTimeoutOrNull(timeout) { awaitDrained() } != null

    fun cancelConnection(
        connection: io.netty.handler.codec.quic.QuicChannel,
        message: String,
    ) {
        val jobs = synchronized(lock) { calls.filterValues { it.parent() === connection }.keys.toList() }
        jobs.forEach { it.cancel(CancellationException(message)) }
    }

    fun cancelAndCloseAll(message: String) {
        val snapshot = synchronized(lock) { calls.toList() }
        snapshot.forEach { (job, channel) ->
            job.cancel(CancellationException(message))
            channel.cancelBoth()
            channel.close()
        }
    }

    fun activeCount(): Int = synchronized(lock) { calls.size }
}

internal class NettyConnectionAdmission<T>(
    private val admission: NettyAdmissionGate = NettyAdmissionGate(),
) {
    private val lock = Any()
    private val connections = linkedSetOf<T>()

    fun admit(
        connection: T,
        limit: Int?,
        afterAdmissionCheck: () -> Unit = {},
    ): Boolean =
        admission.admit {
            synchronized(lock) {
                afterAdmissionCheck()
                if (limit != null && connections.size >= limit) return@synchronized false
                connections.add(connection)
            }
        } ?: false

    fun stopAdmission() {
        admission.stopAdmission()
    }

    fun remove(connection: T): Boolean = synchronized(lock) { connections.remove(connection) }

    fun snapshot(): List<T> = synchronized(lock) { connections.toList() }

    fun activeCount(): Int = synchronized(lock) { connections.size }
}

internal class NettyShutdownCoordinator(
    private val cleanup: suspend () -> Unit,
) {
    private val started = AtomicBoolean(false)
    private val complete = CompletableDeferred<Result<Unit>>()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    suspend fun shutdown() {
        if (started.compareAndSet(false, true)) {
            scope.launch {
                val result = runCatching { withContext(NonCancellable) { cleanup() } }
                scope.cancel()
                complete.complete(result)
            }
        }
        complete.await().getOrThrow()
    }
}

internal suspend fun shutdownOwnedNettyResources(
    group: EventLoopGroup,
    timeout: Duration,
    description: String,
    cancelScope: () -> Unit = {},
    forceClose: () -> Unit,
    gracefulClose: suspend () -> Unit,
) {
    var failure: Throwable? = null
    try {
        val completed =
            withTimeoutOrNull(timeout) {
                gracefulClose()
                true
            } == true
        if (!completed) failure = IllegalStateException("$description shutdown timed out")
    } catch (error: Throwable) {
        failure = error
    }

    failure = failure.combineFailure(runCatching(forceClose).exceptionOrNull())
    failure = failure.combineFailure(runCatching(cancelScope).exceptionOrNull())

    val eventLoopFailure =
        runCatching {
            val terminated =
                withTimeoutOrNull(timeout) {
                    group.shutdownNow(timeout)
                    true
                } == true
            check(terminated) { "$description event-loop shutdown timed out" }
        }.exceptionOrNull()
    failure = failure.combineFailure(eventLoopFailure)
    failure?.let { throw it }
}

internal suspend fun runBoundedNonCancellableCleanup(
    timeout: Duration,
    description: String,
    cleanup: suspend () -> Unit,
    forceCleanup: () -> Unit,
) {
    withContext(NonCancellable) {
        var failure: Throwable? = null
        val completed =
            try {
                withTimeoutOrNull(timeout) {
                    cleanup()
                    true
                } == true
            } catch (error: Throwable) {
                failure = error
                false
            }
        if (!completed && failure == null) failure = IllegalStateException("$description cleanup timed out")
        if (!completed) failure = failure.combineFailure(runCatching(forceCleanup).exceptionOrNull())
        failure?.let { throw it }
    }
}

internal fun Throwable?.combineFailure(other: Throwable?): Throwable? =
    when {
        this == null -> other
        other == null || other === this -> this
        else -> this.apply { addSuppressed(other) }
    }

internal fun EventLoopGroup.isOwnedEventLoopThread(): Boolean = any { it.inEventLoop() }

internal suspend fun EventLoopGroup.shutdownNow(timeout: Duration) {
    shutdownGracefully(0, timeout.inWholeMilliseconds, TimeUnit.MILLISECONDS).awaitValue()
}
