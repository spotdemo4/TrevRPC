package zip.trev.trevrpc

import com.google.protobuf.Message
import com.google.protobuf.MessageLite
import com.google.protobuf.Parser

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

/**
 * An RPC status. [details] are protobuf messages that describe a failure in more depth, such as the
 * `buf.validate.Violations` of a request that broke its validation rules; [findDetails] decodes
 * those of one message type.
 */
data class Status(
    val code: Code,
    val message: String = "",
    val metadata: Metadata = Metadata.EMPTY,
    val details: List<StatusDetail> = emptyList(),
) {
    val isOk: Boolean
        get() = code == Code.OK

    fun withMetadata(metadata: Metadata): Status = copy(metadata = metadata)

    fun withDetails(details: List<StatusDetail>): Status = copy(details = details.toList())

    /** Decodes the details whose type is [typeName], a full name such as `buf.validate.Violations`. */
    fun <T : MessageLite> findDetails(
        typeName: String,
        parser: Parser<T>,
    ): List<T> = details.filter { it.type == typeName }.map { parser.parseFrom(it.value) }

    /** Decodes the details of the message type of [defaultInstance]. */
    fun <T : Message> findDetails(defaultInstance: T): List<T> {
        @Suppress("UNCHECKED_CAST")
        val parser = defaultInstance.parserForType as Parser<T>
        return findDetails(defaultInstance.descriptorForType.fullName, parser)
    }

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

/**
 * One detail of a [Status]: a protobuf message, carried as its full name [type] and its encoding so
 * that a receiver decodes only the details it reads.
 */
class StatusDetail(
    val type: String,
    value: ByteArray,
) {
    private val valueBytes = value.copyOf()

    /** The protobuf encoding of the message. */
    val value: ByteArray
        get() = valueBytes.copyOf()

    override fun equals(other: Any?): Boolean = other is StatusDetail && type == other.type && valueBytes.contentEquals(other.valueBytes)

    override fun hashCode(): Int = 31 * type.hashCode() + valueBytes.contentHashCode()

    override fun toString(): String = "StatusDetail(type=$type, value=${valueBytes.joinToString("") { "%02x".format(it) }})"

    companion object {
        /** Encodes [message] as a detail. */
        fun of(message: Message): StatusDetail = StatusDetail(message.descriptorForType.fullName, message.toByteArray())
    }
}
