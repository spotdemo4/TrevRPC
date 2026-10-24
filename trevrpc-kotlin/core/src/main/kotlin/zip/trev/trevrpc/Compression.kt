package zip.trev.trevrpc

/**
 * Compressed messages: messages that travel as zstd frames. A channel configured with it compresses
 * in two ways, which combine:
 *
 * - With dictionaries both peers hold. The channel asks the server on every call which of the
 *   call's dictionaries it holds, since Cronet does not say which calls share a connection, so a
 *   message whose dictionary the server lacks travels uncompressed.
 * - With stream compression on methods whose schema sets `compress_stream`: one zstd context per
 *   streamed direction of a call, which needs no dictionaries, so an empty [dictionaries] list
 *   enables only this.
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
    }

    /**
     * The compression of one call that sends messages of [requestType] and receives messages of
     * [responseType], or null if neither has a dictionary. Request messages can be compressed only
     * once the server's answer arrives, so [streamsRequest] says whether any are sent after it.
     */
    internal fun call(
        requestType: String?,
        responseType: String?,
        streamsRequest: Boolean,
    ): CallCompression? {
        val request = if (streamsRequest) requestType?.let(byType::get) else null
        val response = responseType?.let(byType::get)
        if (request == null && response == null) return null
        return CallCompression(request, response)
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

/** One configured dictionary, digested for both directions. */
internal class ConfiguredDictionary(
    data: ByteArray,
    level: Int,
) {
    val info = DictionaryInfo(zstdDictionaryId(data), zstdFingerprint(data))
    val codec: ZstdCodec =
        try {
            ZstdCodec(data, level)
        } catch (error: LinkageError) {
            throw IllegalStateException("compression needs zstd-jni (com.github.luben:zstd-jni)", error)
        } catch (error: RuntimeException) {
            throw IllegalArgumentException("cannot load the dictionary", error)
        }
}

/**
 * The negotiation of one call: it queries the dictionaries of its request and response types,
 * accepts responses compressed with the response type's, and compresses request messages with the
 * request type's once the server's answer confirmed it.
 */
internal class CallCompression(
    private val request: ConfiguredDictionary?,
    private val response: ConfiguredDictionary?,
) {
    /** The call's `dictionary_query`. */
    val query: List<DictionaryInfo> = listOfNotNull(request, response).distinct().map { it.info }

    /** The call's `accept_dictionaries`. */
    val accept: List<UInt> = listOfNotNull(response?.info?.id)

    @Volatile
    private var requestConfirmed = false

    /** The request type's dictionary once the server's answer confirmed it, or null. */
    val confirmedRequest: ConfiguredDictionary?
        get() = request?.takeIf { requestConfirmed }

    /** The dictionary the call accepts on its responses, or null. */
    val acceptedResponse: ConfiguredDictionary?
        get() = response

    /** Records the server's answer from the call's HEADERS. */
    fun learn(answer: List<DictionaryInfo>) {
        if (request != null && request.info in answer) requestConfirmed = true
    }

    /** The ZSTD_MESSAGE payload of a request message, or null if it travels as a MESSAGE. */
    fun compressRequest(message: ByteArray): ByteArray? = if (requestConfirmed) request?.codec?.compress(message) else null

    /** Decompresses a response message, or fails as [inflateResponse] says. */
    fun inflate(
        message: ResponseFrame.Message,
        header: ZstdHeader,
        maxSize: Int,
    ): ResponseFrame.Message {
        val dictionary =
            response?.takeIf { it.info.id == header.dictionaryId }
                ?: throw invalidCompressed("compressed message uses dictionary ${header.dictionaryId}, which is not accepted")
        checkContentSize(header, maxSize)
        val body = dictionary.codec.decompress(message.body, header) ?: throw invalidCompressed("invalid ZSTD_MESSAGE payload")
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
