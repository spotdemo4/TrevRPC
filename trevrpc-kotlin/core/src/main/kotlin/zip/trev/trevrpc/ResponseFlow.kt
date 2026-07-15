package zip.trev.trevrpc

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.FlowCollector
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map

internal data class ResponseBatching(
    val maxMessages: Int,
    val maxBytes: Int,
)

private interface ReadyResponseFlow<out T> : Flow<T> {
    val batching: ResponseBatching
}

private class ReadyResponseFlowImpl<T>(
    delegate: Flow<T>,
    override val batching: ResponseBatching,
) : ReadyResponseFlow<T>,
    Flow<T> by delegate

/**
 * Creates a finite stream whose responses are immediately available and may be written in bounded batches.
 * Do not use this for interactive or delayed response streams. Per-message idle timeouts are not applied.
 */
fun <T> readyResponseFlow(
    maxBatchMessages: Int = 32,
    maxBatchBytes: Int = 16 * 1024,
    block: suspend FlowCollector<T>.() -> Unit,
): Flow<T> {
    require(maxBatchMessages > 0) { "maxBatchMessages must be positive" }
    require(maxBatchBytes > 0) { "maxBatchBytes must be positive" }
    return ReadyResponseFlowImpl(flow(block), ResponseBatching(maxBatchMessages, maxBatchBytes))
}

/** Preserves [readyResponseFlow] batching through generated response encoding. */
fun <T, R> Flow<T>.mapReadyResponses(transform: suspend (T) -> R): Flow<R> {
    val mapped = map(transform)
    val ready = this as? ReadyResponseFlow ?: return mapped
    return ReadyResponseFlowImpl(mapped, ready.batching)
}

internal fun Flow<*>.responseBatching(): ResponseBatching? = (this as? ReadyResponseFlow)?.batching
