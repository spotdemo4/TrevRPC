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
internal const val HTTP3_CANCELLED_STREAM_CODE = 268 // Http3ErrorCode.H3_REQUEST_CANCELLED

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

internal fun QuicStreamChannel.cancelBoth(code: Int = CANCELLED_STREAM_CODE) {
    val combined = runCatching { shutdown(code) }
    if (combined.isFailure) {
        runCatching { shutdownInput(code) }
        runCatching { shutdownOutput(code) }
    }
}

internal fun QuicStreamChannel.cancelBothHttp3() {
    cancelBoth(HTTP3_CANCELLED_STREAM_CODE)
}

internal suspend fun QuicStreamChannel.cancelAndClose(code: Int = CANCELLED_STREAM_CODE) {
    withContext(NonCancellable) {
        cancelBoth(code)
        close()
    }
}

internal suspend fun QuicStreamChannel.cancelAndCloseHttp3() {
    cancelAndClose(HTTP3_CANCELLED_STREAM_CODE)
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

internal suspend fun QuicStreamChannel.cancelAndAwaitReset(
    timeout: Duration,
    code: Int = CANCELLED_STREAM_CODE,
) {
    withContext(NonCancellable) {
        if (!isActive || !parent().isActive || eventLoop().isShuttingDown) {
            cancelBoth(code)
            close()
            return@withContext
        }
        val completed =
            withTimeoutOrNull(timeout) {
                awaitCancellationResetFutures(
                    issueReset = { shutdown(code) },
                    issueClose = ::close,
                )
                true
            } == true
        if (!completed) {
            cancelBoth(code)
            close()
        }
    }
}

internal suspend fun QuicStreamChannel.cancelAndAwaitResetHttp3(timeout: Duration) {
    cancelAndAwaitReset(timeout, HTTP3_CANCELLED_STREAM_CODE)
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
    cancelCode: Int = CANCELLED_STREAM_CODE,
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
        if (!completed) cancelAndClose(cancelCode)
    } catch (error: CancellationException) {
        cancelAndClose(cancelCode)
        throw error
    } catch (_: Throwable) {
        cancelAndClose(cancelCode)
    }
}

internal suspend fun QuicStreamChannel.finishAndCloseHttp3(timeout: Duration) {
    finishAndClose(timeout, cancelCode = HTTP3_CANCELLED_STREAM_CODE)
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
