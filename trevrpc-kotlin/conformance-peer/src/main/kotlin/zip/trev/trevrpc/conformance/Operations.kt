package zip.trev.trevrpc.conformance

import com.google.protobuf.UnknownFieldSet
import kotlinx.coroutines.runBlocking
import zip.trev.trevrpc.CallOptions
import zip.trev.trevrpc.Client
import zip.trev.trevrpc.Code
import zip.trev.trevrpc.DEFAULT_MAX_FRAME_SIZE
import zip.trev.trevrpc.FrameDecoder
import zip.trev.trevrpc.MessageCodec
import zip.trev.trevrpc.Metadata
import zip.trev.trevrpc.RpcClientStream
import zip.trev.trevrpc.RpcKind
import zip.trev.trevrpc.RpcRequest
import zip.trev.trevrpc.RpcResponse
import zip.trev.trevrpc.RpcStreamFrame
import zip.trev.trevrpc.RpcStreamFrameKind
import zip.trev.trevrpc.RpcTransport
import zip.trev.trevrpc.TrevRpcException
import zip.trev.trevrpc.WIRE_VERSION
import zip.trev.trevrpc.WireCodec
import zip.trev.trevrpc.decodeProtobuf
import zip.trev.trevrpc.frame
import java.util.concurrent.atomic.AtomicBoolean

private val statePayloadCodec =
    MessageCodec<StatePayload>(
        encode = StatePayload::toByteArray,
        decode = ::decodeStatePayload,
    )

private fun decodeStatePayload(body: ByteArray): StatePayload =
    decodeProtobuf(body, StatePayload.getDescriptor(), StatePayload::parseFrom)
        .toBuilder()
        .setUnknownFields(UnknownFieldSet.getDefaultInstance())
        .build()

internal data class OperationResult(
    val payload: Map<String, Any?> = emptyMap(),
    val error: ConformanceError? = null,
)

internal data class ConformanceError(
    val category: String,
    val statusCode: UInt,
    val native: Throwable,
)

internal fun dispatch(command: RunCommand): OperationResult =
    when (command.operation) {
        "codec.encode" -> codecEncode(checkNotNull(command.message), DEFAULT_MAX_FRAME_SIZE)
        "codec.decode" -> codecDecode(checkNotNull(command.messageType), checkNotNull(command.body))
        "framing.encode" -> codecEncode(checkNotNull(command.message), checkNotNull(command.maxFrameSize))
        "framing.decode_stream" -> framingDecode(checkNotNull(command.maxFrameSize), command.chunks)
        "state.server_stream" -> runBlocking { serverState(command.frames) }
        "state.client_stream" -> runBlocking { clientState(command.frames) }
        else -> OperationResult(error = malformedError(command.operation, IllegalArgumentException("unknown operation")))
    }

private fun codecEncode(
    normalized: NormalizedMessage,
    maxFrameSize: Int,
): OperationResult =
    try {
        val message = normalized.toWireMessage()
        validateMessage(message)
        val body = encodeMessage(message)
        val framed = frame(body, maxFrameSize)
        OperationResult(
            linkedMapOf(
                "body_hex" to body.lowerHex(),
                "frame_hex" to framed.lowerHex(),
            ),
        )
    } catch (error: Throwable) {
        OperationResult(error = classifyCodecError(normalized.messageType, error))
    }

private fun codecDecode(
    messageType: String,
    body: ByteArray,
): OperationResult =
    try {
        val message = decodeMessage(messageType, body)
        validateMessage(message)
        val canonical = encodeMessage(message)
        OperationResult(
            linkedMapOf(
                "message" to normalizeMessage(message),
                "canonical_body_hex" to canonical.lowerHex(),
            ),
        )
    } catch (error: Throwable) {
        OperationResult(error = classifyCodecError(messageType, error))
    }

private fun framingDecode(
    maxFrameSize: Int,
    chunks: List<ByteArray>,
): OperationResult =
    try {
        val decoder = FrameDecoder(maxFrameSize)
        val bodies = chunks.flatMap(decoder::feed)
        decoder.finish()
        OperationResult(
            linkedMapOf(
                "bodies_hex" to bodies.map(ByteArray::lowerHex),
                "eof" to true,
            ),
        )
    } catch (error: Throwable) {
        val category =
            if (error is TrevRpcException && error.status.code == Code.RESOURCE_EXHAUSTED) {
                "frame_too_large"
            } else {
                "incomplete_frame"
            }
        val status = if (category == "frame_too_large") Code.RESOURCE_EXHAUSTED else Code.INTERNAL
        OperationResult(error = ConformanceError(category, status.value, error))
    }

private suspend fun serverState(frames: List<ByteArray>): OperationResult {
    val transport = ScriptedTransport(frames)
    return try {
        val call =
            Client(transport).serverStreaming(
                "conformance",
                "server",
                StatePayload.getDefaultInstance(),
                statePayloadCodec,
                statePayloadCodec,
                stateOptions(),
            )
        val events = mutableListOf<Map<String, Any?>>()
        while (true) {
            val message = call.receive()
            if (message == null) {
                events += linkedMapOf("event" to "eof")
                check(call.receive() == null) { "terminal receive was not stable EOF" }
                events += linkedMapOf("event" to "eof")
                break
            }
            events += linkedMapOf("event" to "message", "body_hex" to message.toByteArray().lowerHex())
        }
        val result =
            linkedMapOf<String, Any?>(
                "events" to events,
                "transport_close_count" to transport.closeCount.toString(),
            )
        transport.terminalFrame?.let { result["terminal_status"] = normalizeStatus(it) }
        OperationResult(result)
    } catch (error: Throwable) {
        OperationResult(
            payload = linkedMapOf("transport_close_count" to transport.closeCount.toString()),
            error = classifyStateError(error, transport, client = false),
        )
    }
}

private suspend fun clientState(frames: List<ByteArray>): OperationResult {
    val transport = ScriptedTransport(frames)
    return try {
        val response =
            Client(transport)
                .clientStreaming(
                    "conformance",
                    "client",
                    statePayloadCodec,
                    statePayloadCodec,
                    stateOptions(),
                ).receive()
        OperationResult(linkedMapOf("response_body_hex" to response.message.toByteArray().lowerHex()))
    } catch (error: Throwable) {
        OperationResult(error = classifyStateError(error, transport, client = true))
    }
}

private fun stateOptions(): CallOptions =
    CallOptions(
        maxResponseMessages = null,
        maxResponseStreamBodySize = null,
        streamIdleTimeout = null,
    )

private fun Throwable.isInvalidMetadata(): Boolean = this is TrevRpcException && status.message.startsWith("invalid metadata:")

private fun Throwable.isTypedDecodeFailure(): Boolean = this is TrevRpcException && status.message == "failed to decode response body"

private fun classifyStateError(
    error: Throwable,
    transport: ScriptedTransport,
    client: Boolean,
): ConformanceError {
    if (transport.trailingFrameRead) return ConformanceError("trailing_frame", Code.INTERNAL.value, error)
    if (error.isInvalidMetadata()) return ConformanceError("invalid_metadata", Code.INTERNAL.value, error)
    if (error.isTypedDecodeFailure() || transport.decodeFailure != null) {
        return ConformanceError("malformed_protobuf", Code.INTERNAL.value, error)
    }
    if (transport.unsupportedFrameSeen) {
        return ConformanceError("unsupported_frame_kind", Code.INVALID_ARGUMENT.value, error)
    }
    val terminal = transport.terminalFrame
    if (terminal != null && transport.cleanFinObserved && !terminal.status.isOk) {
        return ConformanceError("remote_status", terminal.status.code.value, error)
    }
    if (terminal == null && transport.cleanFinObserved) {
        return ConformanceError("missing_terminal_status", Code.INTERNAL.value, error)
    }
    return if (client) {
        ConformanceError("response_cardinality", Code.INTERNAL.value, error)
    } else {
        ConformanceError("missing_terminal_status", Code.INTERNAL.value, error)
    }
}

private class ScriptedTransport(
    private val frameBodies: List<ByteArray>,
) : RpcTransport {
    private var stream: ScriptedStream? = null

    val closeCount: Int
        get() = stream?.closeCount ?: 0

    val terminalFrame: RpcStreamFrame?
        get() = stream?.terminalFrame

    val cleanFinObserved: Boolean
        get() = stream?.cleanFinObserved == true

    val trailingFrameRead: Boolean
        get() = stream?.trailingFrameRead == true

    val unsupportedFrameSeen: Boolean
        get() = stream?.unsupportedFrameSeen == true

    val decodeFailure: Throwable?
        get() = stream?.decodeFailure

    override suspend fun unary(request: RpcRequest): RpcResponse = error("unexpected unary call")

    override suspend fun openStream(request: RpcRequest): RpcClientStream = ScriptedStream(frameBodies).also { stream = it }
}

private class ScriptedStream(
    private val frameBodies: List<ByteArray>,
) : RpcClientStream {
    private val closed = AtomicBoolean(false)
    private var next = 0
    private var terminalSeen = false

    var closeCount: Int = 0
        private set
    var terminalFrame: RpcStreamFrame? = null
        private set
    var cleanFinObserved: Boolean = false
        private set
    var trailingFrameRead: Boolean = false
        private set
    var unsupportedFrameSeen: Boolean = false
        private set
    var decodeFailure: Throwable? = null
        private set

    override suspend fun send(body: ByteArray) = Unit

    override suspend fun finishSend() = Unit

    override suspend fun receive(): RpcStreamFrame? {
        if (next == frameBodies.size) {
            cleanFinObserved = true
            return null
        }
        if (terminalSeen) trailingFrameRead = true
        val body = frameBodies[next++]
        val decoded =
            try {
                WireCodec.decodeStreamFrame(body)
            } catch (error: Throwable) {
                decodeFailure = error
                throw error
            }
        if (decoded.kind == null) unsupportedFrameSeen = true
        if (decoded.kind == RpcStreamFrameKind.STATUS) {
            terminalSeen = true
            terminalFrame = decoded
        }
        return decoded
    }

    override suspend fun close(cause: Throwable?) {
        if (closed.compareAndSet(false, true)) closeCount++
    }
}

private val NormalizedMessage.messageType: String
    get() =
        when (this) {
            is NormalizedMessage.Request -> "rpc_request"
            is NormalizedMessage.Response -> "rpc_response"
            is NormalizedMessage.StreamFrame -> "rpc_stream_frame"
        }

private fun NormalizedMessage.toWireMessage(): Any =
    when (this) {
        is NormalizedMessage.Request -> {
            RpcRequest(
                service = service.strictUtf8("request service"),
                method = method.strictUtf8("request method"),
                body = body,
                metadata = metadata.toMetadata(),
                kindValue =
                    when (kind) {
                        "unary" -> RpcKind.UNARY.value
                        "client_stream" -> RpcKind.CLIENT_STREAMING.value
                        "server_stream" -> RpcKind.SERVER_STREAMING.value
                        "bidi" -> RpcKind.BIDIRECTIONAL_STREAMING.value
                        else -> error("unknown normalized RPC kind")
                    },
                version = version,
                timeoutNanos = timeoutNanos,
            )
        }

        is NormalizedMessage.Response -> {
            RpcResponse(
                statusValue = statusRaw,
                message = message.strictUtf8("response message"),
                body = body,
                metadata = metadata.toMetadata(),
            )
        }

        is NormalizedMessage.StreamFrame -> {
            RpcStreamFrame(
                kindValue = kindRaw.toInt(),
                statusValue = statusRaw,
                message = message.strictUtf8("stream status message"),
                body = body,
                metadata = metadata.toMetadata(),
            )
        }
    }

private fun List<MetadataEntry>.toMetadata(): Metadata =
    Metadata.from(
        associate { entry -> entry.key.strictUtf8("metadata key") to entry.value },
    )

private fun validateMessage(message: Any) {
    when (message) {
        is RpcRequest -> {
            try {
                message.validateProtocol()
            } catch (error: TrevRpcException) {
                if (message.version != WIRE_VERSION) throw UnsupportedWireVersionException(error)
                if (message.kind == null) throw UnsupportedRpcKindException(error)
                throw error
            }
        }

        is RpcStreamFrame -> {
            if (message.kind == null) throw UnsupportedFrameKindException()
        }
    }
}

private fun encodeMessage(message: Any): ByteArray =
    when (message) {
        is RpcRequest -> WireCodec.encode(message)
        is RpcResponse -> WireCodec.encode(message)
        is RpcStreamFrame -> WireCodec.encode(message)
        else -> error("unknown wire message")
    }

private fun decodeMessage(
    messageType: String,
    body: ByteArray,
): Any =
    when (messageType) {
        "rpc_request" -> WireCodec.decodeRequest(body)
        "rpc_response" -> WireCodec.decodeResponse(body)
        "rpc_stream_frame" -> WireCodec.decodeStreamFrame(body)
        else -> error("unknown wire message type")
    }

private fun normalizeMessage(message: Any): Map<String, Any?> =
    when (message) {
        is RpcRequest -> {
            linkedMapOf(
                "type" to "rpc_request",
                "service_hex" to message.service.encodeToByteArray().lowerHex(),
                "method_hex" to message.method.encodeToByteArray().lowerHex(),
                "body_hex" to message.body.lowerHex(),
                "metadata" to normalizeMetadata(message.metadata),
                "kind" to rpcKindToken(checkNotNull(message.kind)),
                "version" to message.version.toString(),
                "timeout_nanos" to message.timeoutNanos.toString(),
            )
        }

        is RpcResponse -> {
            linkedMapOf(
                "type" to "rpc_response",
                "status_raw" to message.statusValue.toString(),
                "status_code" to message.status.code.value,
                "message_hex" to message.message.encodeToByteArray().lowerHex(),
                "body_hex" to message.body.lowerHex(),
                "metadata" to normalizeMetadata(message.metadata),
            )
        }

        is RpcStreamFrame -> {
            linkedMapOf(
                "type" to "rpc_stream_frame",
                "kind" to frameKindToken(checkNotNull(message.kind)),
                "kind_raw" to message.kindValue.toUInt().toString(),
                "status_raw" to message.statusValue.toString(),
                "status_code" to message.status.code.value,
                "message_hex" to message.message.encodeToByteArray().lowerHex(),
                "body_hex" to message.body.lowerHex(),
                "metadata" to normalizeMetadata(message.metadata),
            )
        }

        else -> {
            error("unknown wire message")
        }
    }

private fun normalizeStatus(frame: RpcStreamFrame): Map<String, Any?> =
    linkedMapOf(
        "status_raw" to frame.statusValue.toString(),
        "status_code" to frame.status.code.value,
        "message_hex" to frame.message.encodeToByteArray().lowerHex(),
        "metadata" to normalizeMetadata(frame.metadata),
    )

private fun normalizeMetadata(metadata: Metadata): List<Map<String, Any?>> =
    metadata
        .map { entry -> MetadataEntry(entry.key.encodeToByteArray(), entry.value) }
        .sortedWith { left, right -> compareUnsignedBytes(left.key, right.key) }
        .map { entry ->
            linkedMapOf(
                "key_hex" to entry.key.lowerHex(),
                "value_hex" to entry.value.lowerHex(),
            )
        }

private fun compareUnsignedBytes(
    left: ByteArray,
    right: ByteArray,
): Int {
    for (index in 0 until minOf(left.size, right.size)) {
        val comparison = (left[index].toInt() and 0xff).compareTo(right[index].toInt() and 0xff)
        if (comparison != 0) return comparison
    }
    return left.size.compareTo(right.size)
}

private fun rpcKindToken(kind: RpcKind): String =
    when (kind) {
        RpcKind.UNARY -> "unary"
        RpcKind.CLIENT_STREAMING -> "client_stream"
        RpcKind.SERVER_STREAMING -> "server_stream"
        RpcKind.BIDIRECTIONAL_STREAMING -> "bidi"
    }

private fun frameKindToken(kind: RpcStreamFrameKind): String =
    when (kind) {
        RpcStreamFrameKind.MESSAGE -> "message"
        RpcStreamFrameKind.STATUS -> "status"
    }

private fun classifyCodecError(
    messageType: String,
    error: Throwable,
): ConformanceError {
    val directionalCode = if (messageType == "rpc_request") Code.INVALID_ARGUMENT else Code.INTERNAL
    if (error.isInvalidMetadata()) return ConformanceError("invalid_metadata", directionalCode.value, error)
    if (error is UnsupportedWireVersionException) {
        return ConformanceError("unsupported_wire_version", Code.FAILED_PRECONDITION.value, error)
    }
    if (error is UnsupportedRpcKindException) {
        return ConformanceError("unsupported_rpc_kind", Code.INVALID_ARGUMENT.value, error)
    }
    if (error is UnsupportedFrameKindException) {
        return ConformanceError("unsupported_frame_kind", Code.INVALID_ARGUMENT.value, error)
    }
    if (error is TrevRpcException) {
        return if (error.status.code == Code.RESOURCE_EXHAUSTED) {
            ConformanceError("frame_too_large", Code.RESOURCE_EXHAUSTED.value, error)
        } else {
            ConformanceError("malformed_protobuf", directionalCode.value, error)
        }
    }
    return malformedError(messageType, error)
}

private fun malformedError(
    messageType: String,
    error: Throwable,
): ConformanceError =
    ConformanceError(
        "malformed_protobuf",
        if (messageType == "rpc_request") Code.INVALID_ARGUMENT.value else Code.INTERNAL.value,
        error,
    )

private class UnsupportedWireVersionException(
    cause: Throwable,
) : Exception(cause)

private class UnsupportedRpcKindException(
    cause: Throwable,
) : Exception(cause)

private class UnsupportedFrameKindException : Exception()
