package zip.trev.trevrpc

import com.google.protobuf.CodedInputStream
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.Message
import com.google.protobuf.MessageLite
import com.google.protobuf.Parser
import com.google.protobuf.WireFormat
import kotlin.time.Duration
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds

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

    /**
     * How long the server told the client to wait before it retries, from the first well-formed
     * `google.rpc.RetryInfo` detail, such as that of a call a server's rate limit rejected; null when
     * the status has none. A detail that does not decode, or whose delay is negative or not a valid
     * `google.protobuf.Duration`, is ignored.
     */
    val retryDelay: Duration?
        get() = details.firstNotNullOfOrNull { if (it.type == RETRY_INFO_TYPE) decodeRetryInfo(it.bytes) else null }

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
) : Exception(status.toString(), cause) {
    /** How long the server told the client to wait before it retries; see [Status.retryDelay]. */
    val retryDelay: Duration?
        get() = status.retryDelay
}

/** The full name of `google.rpc.RetryInfo`, the standard detail that tells a client when to retry. */
private const val RETRY_INFO_TYPE = "google.rpc.RetryInfo"

/**
 * Decodes a `google.rpc.RetryInfo` into its delay. TrevRPC does this itself, so that it does not
 * depend on googleapis. Returns null when the message is malformed, has no `retry_delay`, or holds a
 * delay that is not a valid non-negative `Duration`: its nanos must be below a second. Fields it
 * does not know are skipped, and a repeated field takes its last value, as protobuf decoders do.
 */
internal fun decodeRetryInfo(value: ByteArray): Duration? {
    try {
        var duration: ByteArray? = null
        val message = CodedInputStream.newInstance(value)
        while (true) {
            val tag = message.readTag()
            if (tag == 0) break
            if (WireFormat.getTagFieldNumber(tag) != 1) {
                if (!message.skipField(tag)) return null
            } else if (WireFormat.getTagWireType(tag) != WireFormat.WIRETYPE_LENGTH_DELIMITED) {
                return null
            } else {
                duration = message.readByteArray()
            }
        }
        if (duration == null) return null

        var seconds = 0L
        var nanos = 0
        val fields = CodedInputStream.newInstance(duration)
        while (true) {
            val tag = fields.readTag()
            if (tag == 0) break
            val field = WireFormat.getTagFieldNumber(tag)
            if (field != 1 && field != 2) {
                if (!fields.skipField(tag)) return null
            } else if (WireFormat.getTagWireType(tag) != WireFormat.WIRETYPE_VARINT) {
                return null
            } else if (field == 1) {
                seconds = fields.readInt64()
            } else {
                nanos = fields.readInt32()
            }
        }
        if (seconds < 0 || nanos < 0 || nanos >= 1_000_000_000) return null
        return seconds.seconds + nanos.nanoseconds
    } catch (_: InvalidProtocolBufferException) {
        return null
    }
}

/**
 * One detail of a [Status]: a protobuf message, carried as its full name [type] and its encoding so
 * that a receiver decodes only the details it reads.
 */
class StatusDetail(
    val type: String,
    value: ByteArray,
) {
    private val valueBytes = value.copyOf()

    /** The encoding without a copy, for decoding inside the module. */
    internal val bytes: ByteArray
        get() = valueBytes

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
