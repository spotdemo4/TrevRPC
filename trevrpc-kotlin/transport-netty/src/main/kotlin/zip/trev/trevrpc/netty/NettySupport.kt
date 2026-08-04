package zip.trev.trevrpc.netty

import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelFuture
import io.netty.handler.codec.quic.QuicChannel
import io.netty.handler.codec.quic.QuicStreamChannel
import io.netty.util.concurrent.Future
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException
import kotlin.time.Duration

internal const val CANCELLED_STREAM_CODE = 1

internal suspend fun ChannelFuture.awaitChannel(): Channel =
    suspendCancellableCoroutine<Channel> { continuation ->
        val ownedChannel = channel()
        addListener { completed ->
            if (completed.isSuccess) {
                continuation.resume(ownedChannel) { _, rejectedChannel, _ ->
                    rejectedChannel.close()
                }
            } else {
                continuation.resumeWithException(completed.cause() ?: IllegalStateException("Netty operation failed"))
            }
        }
        continuation.invokeOnCancellation { cancel(false) }
    }

internal suspend fun <T> Future<T>.awaitValue(onCancellation: (T) -> Unit = {}): T =
    suspendCancellableCoroutine { continuation ->
        addListener { completed ->
            if (completed.isSuccess) {
                @Suppress("UNCHECKED_CAST")
                continuation.resume(completed.getNow() as T) { _, rejectedValue, _ ->
                    onCancellation(rejectedValue)
                }
            } else {
                continuation.resumeWithException(completed.cause() ?: IllegalStateException("Netty operation failed"))
            }
        }
        continuation.invokeOnCancellation { cancel(false) }
    }

internal suspend fun ChannelFuture.awaitCompletion() {
    suspendCancellableCoroutine<Unit> { continuation ->
        addListener { completed ->
            if (completed.isSuccess) {
                continuation.resume(Unit)
            } else {
                continuation.resumeWithException(completed.cause() ?: IllegalStateException("Netty operation failed"))
            }
        }
        continuation.invokeOnCancellation { cancel(false) }
    }
}

internal fun QuicStreamChannel.cancelBoth() {
    val combined = runCatching { shutdown(CANCELLED_STREAM_CODE) }
    if (combined.isFailure) {
        runCatching { shutdownInput(CANCELLED_STREAM_CODE) }
        runCatching { shutdownOutput(CANCELLED_STREAM_CODE) }
    }
}

internal suspend fun QuicStreamChannel.cancelAndClose() {
    withContext(NonCancellable) {
        cancelBoth()
        close()
    }
}

internal suspend fun awaitCancellationResetFutures(
    issueReset: () -> ChannelFuture?,
    issueClose: () -> ChannelFuture?,
) {
    val reset = runCatching(issueReset).getOrNull()
    runCatching { reset?.awaitCompletion() }
    val closeFuture = runCatching(issueClose).getOrNull()
    runCatching { closeFuture?.awaitCompletion() }
}

internal suspend fun QuicStreamChannel.cancelAndAwaitReset(timeout: Duration) {
    withContext(NonCancellable) {
        if (!isActive || !parent().isActive || eventLoop().isShuttingDown) {
            cancelBoth()
            close()
            return@withContext
        }
        val completed =
            withTimeoutOrNull(timeout) {
                awaitCancellationResetFutures(
                    issueReset = { shutdown(CANCELLED_STREAM_CODE) },
                    issueClose = ::close,
                )
                true
            } == true
        if (!completed) {
            cancelBoth()
            close()
        }
    }
}

/**
 * Completes post-FIN cleanup without allowing a stalled Netty future to replace the remote RPC
 * result. When [outputFinSubmitted] is true, both directions have already reached FIN and close is
 * only scheduled: awaiting that bookkeeping future can otherwise delay an already received terminal
 * result. Caller cancellation still wins; timeout or cleanup failure initiates an idempotent reset and
 * is otherwise suppressed because the peer result has already been received.
 */
internal suspend fun QuicStreamChannel.finishAndClose(
    timeout: Duration,
    outputFinSubmitted: Boolean = false,
) {
    if (outputFinSubmitted) {
        runCatching { close() }
        return
    }
    try {
        val completed =
            withTimeoutOrNull(timeout) {
                if (!isOutputShutdown) shutdownOutput().awaitCompletion()
                close().awaitCompletion()
                true
            } == true
        if (!completed) cancelAndClose()
    } catch (error: CancellationException) {
        cancelAndClose()
        throw error
    } catch (_: Throwable) {
        cancelAndClose()
    }
}

internal fun QuicChannel.closeApplication(
    error: Int,
    reason: String,
) {
    QuicClose.applicationClose(this, error, reason)
}

internal fun transportException(
    message: String,
    cause: Throwable? = null,
): TrevRpcException =
    when (cause) {
        is TrevRpcException -> cause
        is CancellationException -> TrevRpcException(Status.cancelled(message), cause)
        else -> TrevRpcException(Status.unavailable(message), cause)
    }

internal fun ByteBuf.copyReadableBytes(): ByteArray = ByteArray(readableBytes()).also(::readBytes)
