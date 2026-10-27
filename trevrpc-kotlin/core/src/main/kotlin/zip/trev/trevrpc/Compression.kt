package zip.trev.trevrpc

/**
 * Compressed messages: messages that travel as zstd frames. A channel configured with it compresses
 * in three ways, which combine:
 *
 * - With dictionaries both peers hold. The channel asks the server on every call which of the
 *   call's dictionaries it holds, since Cronet does not say which calls share a connection, so a
 *   message whose dictionary the server lacks travels uncompressed.
 * - Without a dictionary on methods whose schema sets `COMPRESSION_MESSAGE` or
 *   `COMPRESSION_STREAM`: messages of [PLAIN_MIN_SIZE] bytes or more that no dictionary applies to,
 *   negotiated on every call as the dictionary with ID 0 (see [CompressMessages]).
 * - With stream compression on methods whose schema sets `COMPRESSION_STREAM`: one zstd context per
 *   streamed direction of a call.
 *
 * The last two need no dictionaries, so an empty [dictionaries] list enables only them.
 *
 * Compression calls zstd-jni, which the application adds: `com.github.luben:zstd-jni` as an `aar`
 * on Android, or as a JAR on the JVM. Constructing a [Compression] without it fails.
 *
 * [level] is the zstd compression level of outgoing messages.
 */
class Compression(
    dictionaries: List<Dictionary>,
    val level: Int = 1,
) {
    /** The dictionaries this client holds. */
    val dictionaries: List<Dictionary> = dictionaries.toList()

    private val byType: Map<String, ConfiguredDictionary>

    /** Compresses and decompresses messages without a dictionary. */
    private val plain: ZstdCodec

    init {
        try {
            loadZstd()
        } catch (error: LinkageError) {
            throw IllegalStateException("compression needs zstd-jni (com.github.luben:zstd-jni)", error)
        }
        val byId = mutableMapOf<UInt, ConfiguredDictionary>()
        val types = mutableMapOf<String, ConfiguredDictionary>()
        this.dictionaries.forEachIndexed { index, dictionary ->
            val configured =
                try {
                    ConfiguredDictionary(dictionary.data, level)
                } catch (error: IllegalArgumentException) {
                    throw IllegalArgumentException("compression dictionary $index: ${error.message}", error)
                }
            require(byId.put(configured.info.id, configured) == null) {
                "compression dictionaries share ID ${configured.info.id}"
            }
            dictionary.messages.forEach { type ->
                require(MESSAGE_TYPE.matches(type)) { "compression dictionary $index: invalid message type \"$type\"" }
                require(types.put(type, configured) == null) { "message type $type is in two compression dictionaries" }
            }
        }
        byType = types
        plain = zstdCodec(null, level)
    }

    /**
     * The compression of one call that sends messages of [requestType] and receives messages of
     * [responseType], or null if it has none: neither type has a dictionary, and [plain] allows no
     * direction to compress without one. Request messages can be compressed only once the server's
     * answer arrives, so [streamsRequest] says whether any are sent after it.
     */
    internal fun call(
        requestType: String?,
        responseType: String?,
        streamsRequest: Boolean,
        plain: CompressMessages = CompressMessages.NONE,
    ): CallCompression? {
        val request = if (streamsRequest) requestType?.let(byType::get) else null
        val response = responseType?.let(byType::get)
        val plainRequests = this.plain.takeIf { streamsRequest && plain.requests }
        val plainResponses = this.plain.takeIf { plain.responses }
        if (request == null && response == null && plainRequests == null && plainResponses == null) return null
        return CallCompression(request, response, plainRequests, plainResponses)
    }

    private companion object {
        val MESSAGE_TYPE = Regex("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*")
    }
}

/**
 * A zstd dictionary and the protobuf message types it compresses.
 *
 * [data] is the dictionary in zstd's format, as written by `zstd --train`; its header carries the
 * Dictionary_ID that identifies it to peers, which must never name different dictionaries.
 * [messages] are the full names of the message types it compresses, such as `example.v1.Position`.
 * A message type may appear in one dictionary only. A dictionary without messages is still offered
 * to the server, but this client never asks to use it.
 */
class Dictionary(
    data: ByteArray,
    messages: List<String> = emptyList(),
) {
    private val bytes = data.copyOf()

    val data: ByteArray
        get() = bytes.copyOf()

    val messages: List<String> = messages.toList()
}

/**
 * The directions of a method whose messages may be compressed without a dictionary (a ZSTD_MESSAGE
 * without a Dictionary_ID) where no dictionary applies to their type. Generated clients pass it for
 * methods whose schema sets `COMPRESSION_MESSAGE` or `COMPRESSION_STREAM`, leaving out directions
 * whose message type has a field marked `debug_redact`. Messages of [PLAIN_MIN_SIZE] bytes or more
 * are compressed when that makes them shorter. Responses may be compressed from the first call;
 * requests only on calls that stream them, once the call's answer confirms that the server decodes
 * them.
 */
data class CompressMessages(
    val requests: Boolean = false,
    val responses: Boolean = false,
) {
    companion object {
        val NONE = CompressMessages()
        val REQUESTS = CompressMessages(requests = true)
        val RESPONSES = CompressMessages(responses = true)
        val BOTH = CompressMessages(requests = true, responses = true)
    }
}

/** The smallest message compressed without a dictionary: smaller ones rarely shrink on their own. */
const val PLAIN_MIN_SIZE: Int = 256

/** Compression without a dictionary, as the negotiation names it: the dictionary with ID 0. */
internal val PLAIN_DICTIONARY = DictionaryInfo(0u, ByteArray(0))

/** One configured dictionary, digested for both directions. */
internal class ConfiguredDictionary(
    data: ByteArray,
    level: Int,
) {
    val info = DictionaryInfo(zstdDictionaryId(data), zstdFingerprint(data))
    val codec: ZstdCodec = zstdCodec(data, level)
}

/** A [ZstdCodec] for [dictionary], or for none, with the errors a configuration reports. */
private fun zstdCodec(
    dictionary: ByteArray?,
    level: Int,
): ZstdCodec =
    try {
        ZstdCodec(dictionary, level)
    } catch (error: LinkageError) {
        throw IllegalStateException("compression needs zstd-jni (com.github.luben:zstd-jni)", error)
    } catch (error: RuntimeException) {
        throw IllegalArgumentException("cannot load the dictionary", error)
    }

/**
 * The negotiation of one call: it queries the dictionaries of its request and response types,
 * accepts responses compressed with the response type's, and compresses request messages with the
 * request type's once the server's answer confirmed it. [plainRequests] and [plainResponses], the
 * codec without a dictionary when that direction may use it, join the negotiation as ID 0.
 */
internal class CallCompression(
    private val request: ConfiguredDictionary?,
    private val response: ConfiguredDictionary?,
    private val plainRequests: ZstdCodec? = null,
    private val plainResponses: ZstdCodec? = null,
) {
    /** The call's `dictionary_query`. */
    val query: List<DictionaryInfo> =
        listOfNotNull(request, response).distinct().map { it.info } + listOfNotNull(PLAIN_DICTIONARY.takeIf { plainRequests != null })

    /** The call's `accept_dictionaries`. */
    val accept: List<UInt> = listOfNotNull(response?.info?.id, PLAIN_DICTIONARY.id.takeIf { plainResponses != null })

    @Volatile
    private var requestConfirmed = false

    @Volatile
    private var plainConfirmed = false

    /** The request type's dictionary once the server's answer confirmed it, or null. */
    val confirmedRequest: ConfiguredDictionary?
        get() = request?.takeIf { requestConfirmed }

    /** The dictionary the call accepts on its responses, or null. */
    val acceptedResponse: ConfiguredDictionary?
        get() = response

    /** Records the server's answer to the dictionary query from the call's ANSWER. */
    fun learn(answer: List<DictionaryInfo>) {
        if (request != null && request.info in answer) requestConfirmed = true
        if (plainRequests != null && PLAIN_DICTIONARY in answer) plainConfirmed = true
    }

    /**
     * The ZSTD_MESSAGE payload of a request message, or null if it travels as a MESSAGE: with the
     * request type's dictionary once confirmed, and otherwise, from [PLAIN_MIN_SIZE] bytes, without
     * one once that is confirmed.
     */
    fun compressRequest(message: ByteArray): ByteArray? {
        if (requestConfirmed && request != null) return request.codec.compress(message)
        if (plainConfirmed && message.size >= PLAIN_MIN_SIZE) return plainRequests?.compress(message)
        return null
    }

    /** Decompresses a response message, or fails as [inflateResponse] says. */
    fun inflate(
        message: ResponseFrame.Message,
        header: ZstdHeader,
        maxSize: Int,
    ): ResponseFrame.Message {
        val codec =
            if (header.dictionaryId == PLAIN_DICTIONARY.id) {
                plainResponses ?: throw invalidCompressed("compressed message without a dictionary, which is not accepted")
            } else {
                response?.takeIf { it.info.id == header.dictionaryId }?.codec
                    ?: throw invalidCompressed("compressed message uses dictionary ${header.dictionaryId}, which is not accepted")
            }
        checkContentSize(header, maxSize)
        val body = codec.decompress(message.body, header) ?: throw invalidCompressed("invalid ZSTD_MESSAGE payload")
        return ResponseFrame.Message(body)
    }
}

/**
 * Decompresses a response message read from a ZSTD_MESSAGE frame; other messages are returned as
 * they are. A message of a dictionary the call does not accept, or one that fails to decompress, is
 * `Internal`; one above [maxSize] is `ResourceExhausted`, found before decompressing.
 */
internal fun inflateResponse(
    message: ResponseFrame.Message,
    compression: CallCompression?,
    maxSize: Int,
): ResponseFrame.Message {
    val header = message.zstd ?: return message
    if (compression == null) throw invalidCompressed("compressed message where no dictionary is accepted")
    return compression.inflate(message, header, maxSize)
}

/**
 * Validates a ZSTD_MESSAGE payload read from a response stream whose frames are limited to
 * [maxFrameSize], which limits the message it carries too.
 */
internal fun compressedResponseHeader(
    payload: ByteArray,
    maxFrameSize: Int,
): ZstdHeader {
    val header = parseZstdHeader(payload) ?: throw invalidCompressed("invalid ZSTD_MESSAGE payload")
    if (header.contentSize > maxFrameSize) {
        throw TrevRpcException(
            Status.resourceExhausted("compressed message is ${header.contentSize} bytes, maximum is $maxFrameSize"),
        )
    }
    return header
}

private fun checkContentSize(
    header: ZstdHeader,
    maxSize: Int,
) {
    if (header.contentSize > maxSize) {
        throw TrevRpcException(Status.resourceExhausted("response message is ${header.contentSize} bytes, maximum is $maxSize"))
    }
}

private fun invalidCompressed(message: String) = TrevRpcException(Status.internal(message))
