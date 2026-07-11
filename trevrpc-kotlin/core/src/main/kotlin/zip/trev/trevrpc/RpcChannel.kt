package zip.trev.trevrpc

import kotlinx.coroutines.flow.StateFlow

/** Application-facing RPC connectivity state. */
enum class RpcChannelState {
    CONNECTING,
    READY,
    CLOSED,
}

/**
 * A long-lived application channel. Implementations reconnect future calls after a connection loss.
 * Calls made while connecting fail immediately. Calls are never retried or replayed, and channel
 * implementations do not use 0-RTT.
 */
interface RpcChannel : RpcTransport {
    val state: StateFlow<RpcChannelState>

    /** Waits for this channel to become ready, or fails if it is closed. */
    suspend fun awaitReady()

    /** Permanently closes this channel and releases resources owned by it. */
    suspend fun close()
}
