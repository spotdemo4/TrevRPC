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
    /**
     * The client's part in stream compression: with [StreamCompression.ENABLED] it decodes
     * stream-compressed responses, and with [StreamCompression.OPTIMISTIC] it also compresses
     * requests without waiting for the answer.
     */
    val streamCompression: StreamCompression = StreamCompression.NONE,
    /**
     * The channel's connection metadata, which the server merges into [metadata] before it
     * validates and authorizes the call, the call's own entries winning. This client offers the set
     * whole on every call: Cronet hides which calls share a connection, and a server confirms a set
     * only on connections that keep one, which the ordinary HTTP mapping does not.
     */
    val connectionMetadata: ConnectionMetadata? = null,
) {
    val kind: RpcKind?
        get() = RpcKind.fromValue(kindValue)
}

/** HEADERS frame payload: the response header metadata. */
data class RpcHeaders(
    val metadata: Metadata = Metadata.EMPTY,
)

/**
 * ANSWER frame payload: the server's answer to the REQUEST's negotiation fields, which it sends
 * before any HEADERS. A field it leaves out means no, or none.
 */
data class RpcAnswer(
    /** The answer to the request's `dictionaryQuery`: the queried dictionaries the server holds. */
    val dictionaries: List<DictionaryInfo> = emptyList(),
    /** The server decodes the call's stream-compressed requests. */
    val streamCompression: Boolean = false,
    /** The ID of the offered connection metadata the server stored for the connection, or zero. */
    val connectionMetadataId: UInt = 0u,
)

/**
 * Request metadata a channel sets once for all its calls, such as a bearer token. [id] names this
 * version of the set, which changes with the set. A REQUEST carries it as an offer, with [metadata]
 * non-empty, or as a reference to the set the server confirmed, with [metadata] empty; this client
 * only offers.
 */
data class ConnectionMetadata(
    val id: UInt,
    val metadata: Metadata = Metadata.EMPTY,
)

/**
 * A client's part in stream compression, whose zstd windows are at most 64 KiB. [ENABLED] says the
 * client decodes stream-compressed responses and would compress requests once the server answers;
 * [OPTIMISTIC] says it compresses requests from the start of the call, without waiting for the
 * answer, and the server accepts the call or refuses it before any application code runs.
 */
enum class StreamCompression(
    val value: Int,
) {
    NONE(0),
    ENABLED(1),
    OPTIMISTIC(2),
    ;

    companion object {
        fun fromValue(value: Int): StreamCompression? = entries.firstOrNull { it.value == value }
    }
}

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

/**
 * STATUS frame payload. [refusedStreamCompression] is set on the lone STATUS of an optimistic call
 * the server refused before any application code ran, which the client replays.
 */
data class RpcStatus(
    val statusValue: UInt = Code.OK.value,
    val message: String = "",
    val metadata: Metadata = Metadata.EMPTY,
    val refusedStreamCompression: Boolean = false,
) {
    val status: Status
        get() = Status(Code.fromValue(statusValue), message, metadata)

    companion object {
        fun fromStatus(status: Status): RpcStatus = RpcStatus(status.code.value, status.message, status.metadata)
    }
}

/** A decoded response-stream frame. Unknown frames never reach this type. */
sealed interface ResponseFrame {
    /**
     * The server's answer to the call's negotiation: [dictionaries] answers its dictionary query,
     * and [streamCompression] says the server decodes its stream-compressed requests. It is not
     * header metadata; [Client] consumes it before the headers.
     */
    class Answer(
        val dictionaries: List<DictionaryInfo> = emptyList(),
        val streamCompression: Boolean = false,
    ) : ResponseFrame

    /** Response headers. Absent headers are equivalent to empty ones. */
    class Headers(
        val metadata: Metadata = Metadata.EMPTY,
    ) : ResponseFrame

    /**
     * One application message. A message read from a ZSTD_MESSAGE or ZSTD_STREAM_MESSAGE frame is
     * still compressed: [body] is the zstd data and [compressed] is set until [Client] decompresses
     * it. [streamSize] is the declared size of a ZSTD_STREAM_MESSAGE.
     */
    class Message internal constructor(
        val body: ByteArray,
        internal val zstd: ZstdHeader?,
        internal val streamSize: Int? = null,
    ) : ResponseFrame {
        /** One raw protobuf-encoded message. */
        constructor(body: ByteArray) : this(body, null)

        val compressed: Boolean
            get() = zstd != null || streamSize != null
    }

    /**
     * The terminal status, carrying trailing metadata. [refusedStreamCompression] marks a server's
     * refusal of an optimistic call, which [Client] replays instead of reporting.
     */
    class Status(
        val status: zip.trev.trevrpc.Status,
        val refusedStreamCompression: Boolean = false,
    ) : ResponseFrame
}
