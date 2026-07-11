package zip.trev.trevrpc

enum class Code(
    val value: UInt,
) {
    OK(0u),
    CANCELLED(1u),
    UNKNOWN(2u),
    INVALID_ARGUMENT(3u),
    DEADLINE_EXCEEDED(4u),
    NOT_FOUND(5u),
    ALREADY_EXISTS(6u),
    PERMISSION_DENIED(7u),
    RESOURCE_EXHAUSTED(8u),
    FAILED_PRECONDITION(9u),
    ABORTED(10u),
    OUT_OF_RANGE(11u),
    UNIMPLEMENTED(12u),
    INTERNAL(13u),
    UNAVAILABLE(14u),
    DATA_LOSS(15u),
    UNAUTHENTICATED(16u),
    ;

    companion object {
        fun fromValue(value: UInt): Code = entries.firstOrNull { it.value == value } ?: UNKNOWN
    }
}

data class Status(
    val code: Code,
    val message: String = "",
    val metadata: Metadata = Metadata.EMPTY,
) {
    val isOk: Boolean
        get() = code == Code.OK

    fun withMetadata(metadata: Metadata): Status = copy(metadata = metadata)

    companion object {
        fun ok(metadata: Metadata = Metadata.EMPTY): Status = Status(Code.OK, metadata = metadata)

        fun cancelled(message: String): Status = Status(Code.CANCELLED, message)

        fun invalidArgument(message: String): Status = Status(Code.INVALID_ARGUMENT, message)

        fun deadlineExceeded(message: String): Status = Status(Code.DEADLINE_EXCEEDED, message)

        fun notFound(message: String): Status = Status(Code.NOT_FOUND, message)

        fun resourceExhausted(message: String): Status = Status(Code.RESOURCE_EXHAUSTED, message)

        fun failedPrecondition(message: String): Status = Status(Code.FAILED_PRECONDITION, message)

        fun unimplemented(message: String): Status = Status(Code.UNIMPLEMENTED, message)

        fun internal(message: String): Status = Status(Code.INTERNAL, message)

        fun unavailable(message: String): Status = Status(Code.UNAVAILABLE, message)

        fun unauthenticated(message: String): Status = Status(Code.UNAUTHENTICATED, message)
    }
}

class TrevRpcException(
    val status: Status,
    cause: Throwable? = null,
) : Exception(status.toString(), cause)

internal fun Throwable.toStatus(): Status =
    when (this) {
        is TrevRpcException -> status
        else -> Status.internal(message ?: "RPC handler failed")
    }
