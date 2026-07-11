package zip.trev.trevrpc.examples

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.ResponseEnvelope
import zip.trev.trevrpc.Server
import zip.trev.trevrpc.Status
import zip.trev.trevrpc.TrevRpcException

private const val LIFECYCLE_SERVICE = "browser.lifecycle.Lifecycle"

internal fun registerBrowserLifecycle(
    server: Server,
    shutdown: CompletableDeferred<Unit>,
) {
    server.routeClientStreaming(LIFECYCLE_SERVICE, "EarlyOk") { _, _ ->
        ResponseEnvelope(encodeValue("early ok"))
    }
    server.routeBidirectionalStreaming(LIFECYCLE_SERVICE, "EarlyError") { _, _ ->
        throw TrevRpcException(Status(Code.PERMISSION_DENIED, "remote rejected upload"))
    }
    server.routeServerStreaming(LIFECYCLE_SERVICE, "Pending") { _, _ ->
        ResponseEnvelope(pendingFlow("EVENT pending_cancelled"))
    }
    server.routeServerStreaming(LIFECYCLE_SERVICE, "FirstThenPending") { _, _ ->
        ResponseEnvelope(
            flow {
                try {
                    emit(encodeValue("first"))
                    awaitCancellation()
                } finally {
                    println("EVENT response_stream_closed")
                }
            },
        )
    }
    server.routeServerStreaming(LIFECYCLE_SERVICE, "LongReplies") { _, _ ->
        ResponseEnvelope(sequence("reply", 256))
    }
    server.routeBidirectionalStreaming(LIFECYCLE_SERVICE, "BidiEchoMany") { _, requests ->
        ResponseEnvelope(requests.map(ByteArray::copyOf))
    }
    server.routeServerStreaming(LIFECYCLE_SERVICE, "ErrorAfterMessages") { _, _ ->
        ResponseEnvelope(
            flow {
                repeat(32) { emit(encodeValue("before-error-${it.toString().padStart(3, '0')}")) }
                throw TrevRpcException(Status(Code.PERMISSION_DENIED, "stream failed after messages"))
            },
        )
    }
    server.routeServerStreaming(LIFECYCLE_SERVICE, "ShutdownAfterFirst") { _, _ ->
        ResponseEnvelope(
            flow {
                emit(encodeValue("first"))
                println("EVENT server_shutdown_mid_stream")
                delay(100)
                shutdown.complete(Unit)
                throw TrevRpcException(Status.cancelled("server shutdown"))
            },
        )
    }
}

private fun pendingFlow(event: String): Flow<ByteArray> =
    flow {
        try {
            awaitCancellation()
        } finally {
            println(event)
        }
    }

private fun sequence(
    prefix: String,
    count: Int,
): Flow<ByteArray> = flow { repeat(count) { emit(encodeValue("$prefix-${it.toString().padStart(3, '0')}")) } }

private fun encodeValue(value: String): ByteArray {
    require(value.toByteArray().size <= 127) { "lifecycle value is too long" }
    return byteArrayOf(0x0a, value.toByteArray().size.toByte()) + value.toByteArray()
}
