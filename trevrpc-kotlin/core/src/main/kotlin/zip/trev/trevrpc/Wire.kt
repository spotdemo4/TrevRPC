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

/**
 * REQUEST frame payload. The request message travels separately in MESSAGE frames. [method] is the
 * method path `/package.Service/Method`. Frames carry no wire version: the transport fixes it.
 */
data class RpcRequest(
    val method: String,
    val metadata: Metadata = Metadata.EMPTY,
    val kindValue: Int = RpcKind.UNARY.value,
    val timeoutNanos: ULong = 0uL,
    val extensions: List<ULong> = emptyList(),
    /**
     * The datagram flow ID the client allocated for the call, or zero for none. Cronet exposes no
     * datagram API, so this client never sets it and its calls stay reliable on the stream. The
     * field exists so the codec round-trips what other runtimes send.
     */
    val datagramFlow: ULong = 0uL,
    /** The dictionaries the client accepts on this call's response messages. */
    val acceptDictionaries: List<UInt> = emptyList(),
    /** The dictionaries the client asks the server about, with their fingerprints. */
    val dictionaryQuery: List<DictionaryInfo> = emptyList(),
) {
    val kind: RpcKind?
        get() = RpcKind.fromValue(kindValue)
}

/** HEADERS frame payload. */
data class RpcHeaders(
    val metadata: Metadata = Metadata.EMPTY,
    val extensions: List<ULong> = emptyList(),
    /** The server's answer to the request's `dictionaryQuery`: the queried dictionaries it holds. */
    val dictionaries: List<DictionaryInfo> = emptyList(),
)

/** A compression dictionary in negotiation fields: its Dictionary_ID and fingerprint. */
class DictionaryInfo(
    val id: UInt,
    fingerprint: ByteArray,
) {
    private val fingerprintBytes = fingerprint.copyOf()

    /** The first 8 bytes of the SHA-256 digest of the dictionary. */
    val fingerprint: ByteArray
        get() = fingerprintBytes.copyOf()

    override fun equals(other: Any?): Boolean =
        other is DictionaryInfo && id == other.id && fingerprintBytes.contentEquals(other.fingerprintBytes)

    override fun hashCode(): Int = 31 * id.hashCode() + fingerprintBytes.contentHashCode()

    override fun toString(): String = "DictionaryInfo(id=$id, fingerprint=${fingerprintBytes.joinToString("") { "%02x".format(it) }})"
}

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
    /**
     * Response headers. Absent headers are equivalent to empty ones. [dictionaries] is the server's
     * answer to the call's dictionary query.
     */
    class Headers(
        val metadata: Metadata = Metadata.EMPTY,
        val dictionaries: List<DictionaryInfo> = emptyList(),
    ) : ResponseFrame

    /**
     * One application message. A message read from a ZSTD_MESSAGE frame is still compressed: [body]
     * is the zstd payload and [compressed] is set until [Client] decompresses it.
     */
    class Message internal constructor(
        val body: ByteArray,
        internal val zstd: ZstdHeader?,
    ) : ResponseFrame {
        /** One raw protobuf-encoded message. */
        constructor(body: ByteArray) : this(body, null)

        val compressed: Boolean
            get() = zstd != null
    }

    /** The terminal status, carrying trailing metadata. */
    class Status(
        val status: zip.trev.trevrpc.Status,
    ) : ResponseFrame
}
