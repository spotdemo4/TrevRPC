package zip.trev.trevrpc.netty

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import zip.trev.trevrpc.RpcChannel
import zip.trev.trevrpc.RpcChannelState
import zip.trev.trevrpc.RpcClientStream
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcResponse
import zip.trev.trevrpc.RpcTransport
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.netty.advanced.RawNettyHttp3RpcTransport
import zip.trev.trevrpc.netty.advanced.RawNettyQuicRpcTransport
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.math.pow
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

/** Application channel factories for TrevRPC's native QUIC and HTTP/3 protocols. */
object NettyRpcChannel {
    fun nativeQuic(config: NettyQuicClientConfig): RpcChannel =
        createNettyRpcChannel {
            val transport = RawNettyQuicRpcTransport.connect(config)
            NettyChannelConnection(transport, transport::awaitClosed, transport::shutdown)
        }

    fun http3(config: NettyQuicClientConfig): RpcChannel =
        createNettyRpcChannel {
            val transport = RawNettyHttp3RpcTransport.connect(config)
            NettyChannelConnection(transport, transport::awaitClosed, transport::shutdown)
        }
}

internal class NettyChannelConnection(
    val transport: RpcTransport,
    val awaitClosed: suspend () -> Unit,
    private val closeTransport: suspend () -> Unit,
) {
    private val closed = AtomicBoolean(false)

    suspend fun close() {
        if (!closed.compareAndSet(false, true)) return
        withContext(NonCancellable) { closeTransport() }
    }
}

internal class NettyChannelBackoff(
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

    fun delayForAttempt(attempt: Int): Duration {
        require(attempt > 0) { "attempt must be positive" }
        val exponential = initialDelay * multiplier.pow((attempt - 1).toDouble())
        val bounded = minOf(exponential, maxDelay)
        val sample = random().coerceIn(0.0, 1.0)
        val jittered = bounded * (1.0 + ((sample * 2.0) - 1.0) * jitterRatio)
        return minOf(jittered, maxDelay)
    }
}

internal fun createNettyRpcChannel(
    scope: CoroutineScope = CoroutineScope(SupervisorJob() + Dispatchers.IO),
    backoff: NettyChannelBackoff = NettyChannelBackoff(),
    connector: suspend () -> NettyChannelConnection,
): RpcChannel = DefaultNettyRpcChannel(scope, connector, backoff)

private class DefaultNettyRpcChannel(
    scope: CoroutineScope,
    private val connector: suspend () -> NettyChannelConnection,
    private val backoff: NettyChannelBackoff,
) : RpcChannel {
    private val terminal = AtomicBoolean(false)
    private val transitionLock = Any()
    private val current = AtomicReference<NettyChannelConnection?>(null)
    private val terminalCleanupStarted = AtomicBoolean(false)
    private val closeComplete = CompletableDeferred<Result<Unit>>()
    private val mutableState = MutableStateFlow(RpcChannelState.CONNECTING)
    private val manager: Job = scope.launch { manageConnections() }

    override val state: StateFlow<RpcChannelState> = mutableState.asStateFlow()

    override suspend fun unary(request: RpcRequest): RpcResponse = snapshot().unary(request)

    override suspend fun openStream(request: RpcRequest): RpcClientStream = snapshot().openStream(request)

    override suspend fun awaitReady() {
        when (state.first { it == RpcChannelState.READY || it == RpcChannelState.CLOSED }) {
            RpcChannelState.READY -> Unit
            RpcChannelState.CLOSED -> throw unavailable("RPC channel is closed")
            RpcChannelState.CONNECTING -> error("unreachable channel state")
        }
    }

    override suspend fun close() {
        val first = markClosed()
        withContext(NonCancellable) {
            if (first) manager.cancel()
            manager.join()
            completeTerminalCleanup()
            closeComplete.await().getOrThrow()
        }
    }

    private fun snapshot(): RpcTransport {
        if (state.value != RpcChannelState.READY) {
            throw unavailable(if (terminal.get()) "RPC channel is closed" else "RPC channel is connecting")
        }
        return current.get()?.transport ?: throw unavailable("RPC channel is connecting")
    }

    private suspend fun manageConnections() {
        var owned: NettyChannelConnection? = null
        var failedAttempts = 0
        try {
            while (!terminal.get()) {
                try {
                    owned = connector()
                    currentCoroutineContext().ensureActive()
                } catch (error: CancellationException) {
                    throw error
                } catch (_: Throwable) {
                    failedAttempts++
                    delay(backoff.delayForAttempt(failedAttempts))
                    continue
                }

                val connection = checkNotNull(owned)
                val accepted =
                    synchronized(transitionLock) {
                        if (terminal.get()) {
                            false
                        } else {
                            current.set(connection)
                            mutableState.value = RpcChannelState.READY
                            true
                        }
                    }
                if (!accepted) break
                owned = null
                failedAttempts = 0

                try {
                    connection.awaitClosed()
                } catch (error: CancellationException) {
                    throw error
                } catch (_: Throwable) {
                    // A terminal connection failure has the same reconnect behavior as an orderly close.
                }
                if (terminal.get()) break

                val disconnected =
                    synchronized(transitionLock) {
                        if (terminal.get()) {
                            false
                        } else {
                            mutableState.value = RpcChannelState.CONNECTING
                            current.compareAndSet(connection, null)
                            true
                        }
                    }
                if (!disconnected) break
                closeQuietly(connection)
                failedAttempts = 1
                delay(backoff.delayForAttempt(failedAttempts))
            }
        } finally {
            withContext(NonCancellable) { completeTerminalCleanup(owned) }
            markClosed()
        }
    }

    private suspend fun completeTerminalCleanup(owned: NettyChannelConnection? = null) {
        if (!terminalCleanupStarted.compareAndSet(false, true)) return
        var failure = runCatching { owned?.close() }.exceptionOrNull()
        failure = failure.combineFailure(runCatching { current.getAndSet(null)?.close() }.exceptionOrNull())
        closeComplete.complete(
            if (failure == null) {
                Result.success(Unit)
            } else {
                Result.failure(failure)
            },
        )
    }

    private suspend fun closeQuietly(connection: NettyChannelConnection) {
        try {
            connection.close()
        } catch (_: Throwable) {
            // Cleanup failure must not restart or strand the channel manager.
        }
    }

    private fun markClosed(): Boolean =
        synchronized(transitionLock) {
            if (!terminal.compareAndSet(false, true)) return@synchronized false
            mutableState.value = RpcChannelState.CLOSED
            true
        }

    private fun unavailable(message: String): TrevRpcException = TrevRpcException(Status.unavailable(message))
}
