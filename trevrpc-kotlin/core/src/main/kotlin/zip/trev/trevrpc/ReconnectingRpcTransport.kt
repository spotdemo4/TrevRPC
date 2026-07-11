package zip.trev.trevrpc

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.SharedFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asSharedFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.pow
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** A persistent transport's terminal lifecycle signal. */
fun interface RpcTransportLifecycle {
    suspend fun awaitClosed()
}

/** One transport generation and the operations needed to observe and release it. */
class RpcTransportConnection(
    val transport: RpcTransport,
    val lifecycle: RpcTransportLifecycle,
    private val closeTransport: suspend () -> Unit,
) {
    private val closed = AtomicBoolean(false)

    suspend fun close() {
        if (!closed.compareAndSet(false, true)) return
        withContext(NonCancellable) { closeTransport() }
    }
}

class ReconnectBackoff(
    val initialDelay: Duration = 100.milliseconds,
    val maxDelay: Duration = 30.seconds,
    val multiplier: Double = 2.0,
    val jitterRatio: Double = 0.2,
    private val random: () -> Double = { Random.nextDouble() },
) {
    init {
        require(initialDelay.isFinite() && initialDelay.isPositive()) {
            "initialDelay must be positive and finite"
        }
        require(maxDelay.isFinite() && maxDelay >= initialDelay) {
            "maxDelay must be finite and at least initialDelay"
        }
        require(multiplier.isFinite() && multiplier >= 1.0) { "multiplier must be finite and at least 1" }
        require(jitterRatio in 0.0..1.0) { "jitterRatio must be between 0 and 1" }
    }

    internal fun delayForAttempt(attempt: Int): Duration {
        require(attempt > 0) { "attempt must be positive" }
        val exponential = initialDelay * multiplier.pow((attempt - 1).toDouble())
        val bounded = minOf(exponential, maxDelay)
        val sample = random().coerceIn(0.0, 1.0)
        val jittered = bounded * (1.0 + ((sample * 2.0) - 1.0) * jitterRatio)
        return minOf(jittered, maxDelay)
    }
}

sealed interface ReconnectingRpcTransportState {
    val generation: Long

    data class Connecting(
        override val generation: Long = 0,
        val failedAttempts: Int = 0,
        val retryDelay: Duration? = null,
    ) : ReconnectingRpcTransportState

    data class Ready(
        override val generation: Long,
    ) : ReconnectingRpcTransportState

    data class Reconnecting(
        override val generation: Long,
        val failedAttempts: Int = 0,
        val retryDelay: Duration? = null,
    ) : ReconnectingRpcTransportState

    data class Closed(
        override val generation: Long,
        val cause: Throwable? = null,
    ) : ReconnectingRpcTransportState
}

sealed interface ReconnectingRpcTransportEvent {
    data class Ready(
        val generation: Long,
    ) : ReconnectingRpcTransportEvent

    data class Disconnected(
        val generation: Long,
        val cause: Throwable?,
    ) : ReconnectingRpcTransportEvent

    data class ConnectFailed(
        val generation: Long,
        val attempt: Int,
        val cause: Throwable,
        val retryDelay: Duration,
    ) : ReconnectingRpcTransportEvent

    data class Closed(
        val generation: Long,
        val cause: Throwable?,
    ) : ReconnectingRpcTransportEvent
}

/**
 * Maintains one live transport generation. Calls never wait for or replay across a reconnect.
 */
class ReconnectingRpcTransport(
    scope: CoroutineScope,
    private val connector: suspend () -> RpcTransportConnection,
    private val backoff: ReconnectBackoff = ReconnectBackoff(),
) : RpcTransport {
    private data class Generation(
        val number: Long,
        val connection: RpcTransportConnection,
    )

    private val terminal = AtomicBoolean(false)
    private val transitionLock = Any()
    private val current = AtomicReference<Generation?>(null)
    private val closeComplete = CompletableDeferred<Unit>()
    private val mutableState = MutableStateFlow<ReconnectingRpcTransportState>(ReconnectingRpcTransportState.Connecting())
    private val mutableEvents = MutableSharedFlow<ReconnectingRpcTransportEvent>(extraBufferCapacity = 64)
    private val manager: Job = scope.launch { manageConnections() }

    val state: StateFlow<ReconnectingRpcTransportState> = mutableState.asStateFlow()
    val events: SharedFlow<ReconnectingRpcTransportEvent> = mutableEvents.asSharedFlow()

    val generation: Long
        get() = state.value.generation

    override suspend fun unary(request: RpcRequest): RpcResponse = snapshot().unary(request)

    override suspend fun openStream(
        request: RpcRequest,
        requestBody: Flow<ByteArray>,
    ): RpcTransportStream = snapshot().openStream(request, requestBody)

    suspend fun waitUntilReady(): Long =
        when (val ready = state.first { it is ReconnectingRpcTransportState.Ready || it is ReconnectingRpcTransportState.Closed }) {
            is ReconnectingRpcTransportState.Ready -> ready.generation
            is ReconnectingRpcTransportState.Closed -> throw unavailable("managed RPC transport is closed")
            else -> error("unreachable transport state")
        }

    suspend fun close() {
        val first = markClosed(null)
        withContext(NonCancellable) {
            if (first) manager.cancel()
            manager.join()
            closeCurrent()
            closeComplete.complete(Unit)
            closeComplete.await()
        }
    }

    private fun snapshot(): RpcTransport {
        val ready =
            state.value as? ReconnectingRpcTransportState.Ready
                ?: throw unavailable(
                    if (terminal.get()) "managed RPC transport is closed" else "managed RPC transport is reconnecting",
                )
        val generation = current.get()
        if (generation == null || generation.number != ready.generation) {
            throw unavailable("managed RPC transport is reconnecting")
        }
        return generation.connection.transport
    }

    private suspend fun manageConnections() {
        var owned: RpcTransportConnection? = null
        var failedAttempts = 0
        try {
            while (!terminal.get()) {
                try {
                    owned = connector()
                    currentCoroutineContext().ensureActive()
                } catch (error: CancellationException) {
                    throw error
                } catch (error: Throwable) {
                    failedAttempts++
                    val retryDelay = backoff.delayForAttempt(failedAttempts)
                    updateConnecting(failedAttempts, retryDelay)
                    emitConnectFailed(failedAttempts, error, retryDelay)
                    delay(retryDelay)
                    continue
                }

                val connection = checkNotNull(owned)
                val next =
                    synchronized(transitionLock) {
                        if (terminal.get()) {
                            null
                        } else {
                            Generation(mutableState.value.generation + 1, connection).also {
                                current.set(it)
                                mutableState.value = ReconnectingRpcTransportState.Ready(it.number)
                                mutableEvents.tryEmit(ReconnectingRpcTransportEvent.Ready(it.number))
                            }
                        }
                    } ?: break
                owned = null
                failedAttempts = 0

                val cause =
                    try {
                        connection.lifecycle.awaitClosed()
                        null
                    } catch (error: CancellationException) {
                        throw error
                    } catch (error: Throwable) {
                        error
                    }
                if (terminal.get()) break

                val disconnected =
                    synchronized(transitionLock) {
                        if (terminal.get()) {
                            false
                        } else {
                            mutableState.value = ReconnectingRpcTransportState.Reconnecting(next.number)
                            current.compareAndSet(next, null)
                            mutableEvents.tryEmit(ReconnectingRpcTransportEvent.Disconnected(next.number, cause))
                            true
                        }
                    }
                if (!disconnected) break
                closeQuietly(connection)
            }
        } finally {
            withContext(NonCancellable) {
                owned?.let { closeQuietly(it) }
                closeCurrent()
            }
            markClosed(null)
            closeComplete.complete(Unit)
        }
    }

    private fun updateConnecting(
        failedAttempts: Int,
        retryDelay: Duration,
    ) {
        synchronized(transitionLock) {
            if (terminal.get()) return
            val generation = mutableState.value.generation
            mutableState.value =
                if (generation == 0L) {
                    ReconnectingRpcTransportState.Connecting(generation, failedAttempts, retryDelay)
                } else {
                    ReconnectingRpcTransportState.Reconnecting(generation, failedAttempts, retryDelay)
                }
        }
    }

    private fun emitConnectFailed(
        attempt: Int,
        cause: Throwable,
        retryDelay: Duration,
    ) {
        synchronized(transitionLock) {
            if (!terminal.get()) {
                mutableEvents.tryEmit(
                    ReconnectingRpcTransportEvent.ConnectFailed(
                        generation = mutableState.value.generation,
                        attempt = attempt,
                        cause = cause,
                        retryDelay = retryDelay,
                    ),
                )
            }
        }
    }

    private suspend fun closeCurrent() {
        current.getAndSet(null)?.connection?.let { closeQuietly(it) }
    }

    private suspend fun closeQuietly(connection: RpcTransportConnection) {
        try {
            connection.close()
        } catch (_: Throwable) {
            // The connection is already terminal; cleanup failure must not restart or strand the manager.
        }
    }

    private fun markClosed(cause: Throwable?): Boolean {
        synchronized(transitionLock) {
            if (!terminal.compareAndSet(false, true)) return false
            val currentGeneration = mutableState.value.generation
            mutableState.value = ReconnectingRpcTransportState.Closed(currentGeneration, cause)
            mutableEvents.tryEmit(ReconnectingRpcTransportEvent.Closed(currentGeneration, cause))
            return true
        }
    }

    private fun unavailable(message: String): TrevRpcException = TrevRpcException(Status.unavailable(message))
}
