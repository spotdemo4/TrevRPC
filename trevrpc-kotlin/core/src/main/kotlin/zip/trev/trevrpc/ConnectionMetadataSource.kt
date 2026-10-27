package zip.trev.trevrpc

import java.util.concurrent.atomic.AtomicReference

/**
 * A channel's connection metadata: request metadata set once for all its calls, such as a bearer
 * token, that [Client] offers on each call and the server merges into the call's own metadata. Each
 * set gets the next ID of the channel, starting at 1, so a server can tell versions apart; empty
 * metadata removes the set. [Metadata] is validated when it is built, so a set is always valid.
 */
class ConnectionMetadataSource(
    initial: Metadata = Metadata.EMPTY,
) {
    private val current = AtomicReference<ConnectionMetadata?>(null)
    private var lastId = 0u

    init {
        set(initial)
    }

    /** The set to offer on a call, or null for none. */
    fun current(): ConnectionMetadata? = current.get()

    /** Replaces the connection metadata; calls opened afterwards offer the new set. */
    @Synchronized
    fun set(metadata: Metadata) {
        if (metadata.isEmpty) {
            current.set(null)
            return
        }
        // IDs wrap after 2^32 sets, past 0, which means none.
        if (++lastId == 0u) ++lastId
        current.set(ConnectionMetadata(lastId, metadata))
    }
}
