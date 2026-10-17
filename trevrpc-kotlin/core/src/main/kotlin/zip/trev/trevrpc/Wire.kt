package zip.trev.trevrpc

enum class RpcKind(
    val value: Int,
) {
    UNARY(0),
    CLIENT_STREAMING(1),
    SERVER_STREAMING(2),
    BIDIRECTIONAL_STREAMING(3),
    ;

    companion object {
        fun fromValue(value: Int): RpcKind? = entries.firstOrNull { it.value == value }
    }
}

/** REQUEST frame payload. The request message travels separately in MESSAGE frames. */
data class RpcRequest(
    val service: String,
    val method: String,
    val metadata: Metadata = Metadata.EMPTY,
    val kindValue: Int = RpcKind.UNARY.value,
    val version: UInt = WIRE_VERSION,
    val timeoutNanos: ULong = 0uL,
    val extensions: List<ULong> = emptyList(),
) {
    val kind: RpcKind?
        get() = RpcKind.fromValue(kindValue)
}

/** HEADERS frame payload. */
data class RpcHeaders(
    val metadata: Metadata = Metadata.EMPTY,
    val extensions: List<ULong> = emptyList(),
)

/** STATUS frame payload. */
data class RpcStatus(
    val statusValue: UInt = Code.OK.value,
    val message: String = "",
    val metadata: Metadata = Metadata.EMPTY,
) {
    val status: Status
        get() = Status(Code.fromValue(statusValue), message, metadata)

    companion object {
        fun fromStatus(status: Status): RpcStatus = RpcStatus(status.code.value, status.message, status.metadata)
    }
}

/**
 * A decoded response-stream frame. Unknown frames never reach this type, and extension lists are
 * dropped because the client negotiates none.
 */
sealed interface ResponseFrame {
    /** Response headers. Absent headers are equivalent to empty ones. */
    class Headers(
        val metadata: Metadata = Metadata.EMPTY,
    ) : ResponseFrame

    /** One raw protobuf-encoded application message. */
    class Message(
        val body: ByteArray,
    ) : ResponseFrame

    /** The terminal status, carrying trailing metadata. */
    class Status(
        val status: zip.trev.trevrpc.Status,
    ) : ResponseFrame
}
