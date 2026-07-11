package zip.trev.trevrpc.netty

import io.netty.buffer.ByteBuf
import io.netty.channel.Channel
import io.netty.channel.ChannelFuture
import io.netty.handler.codec.quic.QuicChannel
import io.netty.handler.codec.quic.QuicStreamChannel
import io.netty.util.concurrent.Future
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.suspendCancellableCoroutine
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException

internal const val CANCELLED_STREAM_CODE = 1

internal suspend fun ChannelFuture.awaitChannel(): Channel {
    awaitCompletion()
    return channel()
}

internal suspend fun <T> Future<T>.awaitValue(): T =
    suspendCancellableCoroutine { continuation ->
        addListener { completed ->
            if (completed.isSuccess) {
                @Suppress("UNCHECKED_CAST")
                continuation.resume(completed.getNow() as T)
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
    shutdownInput(CANCELLED_STREAM_CODE)
    shutdownOutput(CANCELLED_STREAM_CODE)
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
