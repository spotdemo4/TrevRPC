package zip.trev.trevrpc

/**
 * A client transport that owns resources. Calls are never retried or replayed, and implementations
 * do not use 0-RTT.
 */
interface RpcChannel : RpcTransport {
    /**
     * Replaces the channel's connection metadata, request metadata sent with every call, such as a
     * bearer token; empty metadata removes it. Calls opened afterwards use the new set, a call's own
     * metadata wins over the set, and calls in flight keep the set they opened with.
     */
    fun setConnectionMetadata(metadata: Metadata)

    /** Permanently closes this channel and releases resources owned by it. */
    suspend fun close()
}
