package zip.trev.trevrpc

/**
 * A client transport that owns resources. Calls are never retried or replayed, and implementations
 * do not use 0-RTT.
 */
interface RpcChannel : RpcTransport {
    /** Permanently closes this channel and releases resources owned by it. */
    suspend fun close()
}
