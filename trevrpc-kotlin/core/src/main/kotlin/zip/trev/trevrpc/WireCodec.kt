package zip.trev.trevrpc

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction

object WireCodec {
    fun encode(request: RpcRequest): ByteArray =
        Writer()
            .apply {
                string(1, request.method)
                metadata(2, request.metadata)
                int32(3, request.kindValue)
                uint64(4, request.timeoutNanos)
                uint64(5, request.datagramFlow)
                packedUint32(6, request.acceptDictionaries)
                dictionaries(7, request.dictionaryQuery)
                int32(8, request.streamCompression.value)
                connectionMetadata(9, request.connectionMetadata)
            }.toByteArray()

    fun decodeRequest(bytes: ByteArray): RpcRequest {
        val reader = Reader(bytes)
        var method = ""
        val metadata = linkedMapOf<String, ByteArray>()
        var kind = 0
        var timeoutNanos = 0uL
        var datagramFlow = 0uL
        val acceptDictionaries = mutableListOf<UInt>()
        val dictionaryQuery = mutableListOf<DictionaryInfo>()
        var streamCompression = StreamCompression.NONE
        var connectionMetadata: ConnectionMetadata? = null
        while (reader.hasRemaining) {
            val tag = reader.tag()
            when (tag.field) {
                1 -> method = reader.string(tag, "request method")
                2 -> reader.mapEntry(tag, metadata)
                3 -> kind = reader.int32(tag)
                4 -> timeoutNanos = reader.uint64(tag)
                5 -> datagramFlow = reader.uint64(tag)
                6 -> reader.uint32Elements(tag, acceptDictionaries)
                7 -> dictionaryQuery += reader.dictionaryInfo(tag)
                8 -> streamCompression = reader.streamCompression(tag)
                9 -> connectionMetadata = reader.connectionMetadata(tag)
                else -> reader.skip(tag)
            }
        }
        return RpcRequest(
            method,
            Metadata.fromWire(metadata),
            kind,
            timeoutNanos,
            datagramFlow,
            acceptDictionaries,
            dictionaryQuery,
            streamCompression,
            connectionMetadata,
        )
    }

    fun encode(headers: RpcHeaders): ByteArray = Writer().apply { metadata(1, headers.metadata) }.toByteArray()

    fun decodeHeaders(bytes: ByteArray): RpcHeaders {
        val reader = Reader(bytes)
        val metadata = linkedMapOf<String, ByteArray>()
        while (reader.hasRemaining) {
            val tag = reader.tag()
            when (tag.field) {
                1 -> reader.mapEntry(tag, metadata)
                else -> reader.skip(tag)
            }
        }
        return RpcHeaders(Metadata.fromWire(metadata))
    }

    fun encode(answer: RpcAnswer): ByteArray =
        Writer()
            .apply {
                dictionaries(1, answer.dictionaries)
                bool(2, answer.streamCompression)
                uint32(3, answer.connectionMetadataId)
            }.toByteArray()

    fun decodeAnswer(bytes: ByteArray): RpcAnswer {
        val reader = Reader(bytes)
        val dictionaries = mutableListOf<DictionaryInfo>()
        var streamCompression = false
        var connectionMetadataId = 0u
        while (reader.hasRemaining) {
            val tag = reader.tag()
            when (tag.field) {
                1 -> dictionaries += reader.dictionaryInfo(tag)
                2 -> streamCompression = reader.bool(tag)
                3 -> connectionMetadataId = reader.uint32(tag)
                else -> reader.skip(tag)
            }
        }
        return RpcAnswer(dictionaries, streamCompression, connectionMetadataId)
    }

    fun encode(status: RpcStatus): ByteArray =
        Writer()
            .apply {
                uint32(1, status.statusValue)
                string(2, status.message)
                metadata(3, status.metadata)
                bool(4, status.refusedStreamCompression)
            }.toByteArray()

    fun decodeStatus(bytes: ByteArray): RpcStatus {
        val reader = Reader(bytes)
        var code = 0u
        var message = ""
        val metadata = linkedMapOf<String, ByteArray>()
        var refusedStreamCompression = false
        while (reader.hasRemaining) {
            val tag = reader.tag()
            when (tag.field) {
                1 -> code = reader.uint32(tag)
                2 -> message = reader.string(tag, "status message")
                3 -> reader.mapEntry(tag, metadata)
                4 -> refusedStreamCompression = reader.bool(tag)
                else -> reader.skip(tag)
            }
        }
        return RpcStatus(code, message, Metadata.fromWire(metadata), refusedStreamCompression)
    }

    /**
     * Decodes one known frame read from a response stream exactly once. A REQUEST frame is a known
     * frame the response grammar does not allow, which is an `Internal` protocol error. A
     * ZSTD_MESSAGE is validated but stays compressed; the message it carries is limited by
     * [maxFrameSize], like the frames of the stream. A ZSTD_STREAM_MESSAGE is checked only for its
     * declared size here: the rest depends on the call's earlier messages.
     */
    fun decodeResponseFrame(
        frame: Frame,
        maxFrameSize: Int = DEFAULT_MAX_FRAME_SIZE,
    ): ResponseFrame =
        when (frame.type) {
            FrameType.MESSAGE -> {
                ResponseFrame.Message(frame.payload)
            }

            FrameType.ZSTD_MESSAGE -> {
                ResponseFrame.Message(frame.payload, compressedResponseHeader(frame.payload, maxFrameSize))
            }

            FrameType.ZSTD_STREAM_MESSAGE -> {
                streamMessage(frame.payload, maxFrameSize)
            }

            FrameType.ANSWER -> {
                decodeAnswer(frame.payload).let { ResponseFrame.Answer(it.dictionaries, it.streamCompression) }
            }

            FrameType.HEADERS -> {
                ResponseFrame.Headers(decodeHeaders(frame.payload).metadata)
            }

            FrameType.STATUS -> {
                decodeStatus(frame.payload).let { ResponseFrame.Status(it.status, it.refusedStreamCompression) }
            }

            else -> {
                throw TrevRpcException(Status.internal("response stream contained an unexpected frame type ${frame.type}"))
            }
        }
}

/**
 * Splits a ZSTD_STREAM_MESSAGE payload into its declared size and its chunk, rejecting a size above
 * [maxFrameSize] before anything is decompressed.
 */
private fun streamMessage(
    payload: ByteArray,
    maxFrameSize: Int,
): ResponseFrame.Message {
    val size =
        try {
            QuicVarint.decode(payload)
        } catch (error: IllegalArgumentException) {
            throw TrevRpcException(Status.internal("invalid ZSTD_STREAM_MESSAGE payload"), error)
        }
    if (size > maxFrameSize) {
        throw TrevRpcException(Status.resourceExhausted("compressed message is $size bytes, maximum is $maxFrameSize"))
    }
    val chunk = payload.copyOfRange(QuicVarint.encodedLength(payload[0]), payload.size)
    return ResponseFrame.Message(chunk, null, size.toInt())
}

private data class Tag(
    val field: Int,
    val wireType: Int,
)

private class Writer {
    private val output = ByteArrayOutputStream()

    fun uint32(
        field: Int,
        value: UInt,
    ) {
        if (value != 0u) {
            tag(field, 0)
            varint(value.toULong())
        }
    }

    fun bool(
        field: Int,
        value: Boolean,
    ) {
        if (value) {
            tag(field, 0)
            varint(1u)
        }
    }

    fun int32(
        field: Int,
        value: Int,
    ) {
        if (value != 0) {
            tag(field, 0)
            varint(value.toLong().toULong())
        }
    }

    fun uint64(
        field: Int,
        value: ULong,
    ) {
        if (value != 0uL) {
            tag(field, 0)
            varint(value)
        }
    }

    fun packedUint32(
        field: Int,
        values: List<UInt>,
    ) {
        if (values.isEmpty()) return
        val packed = Writer().apply { values.forEach { varint(it.toULong()) } }.toByteArray()
        tag(field, 2)
        varint(packed.size.toULong())
        output.write(packed)
    }

    fun dictionaries(
        field: Int,
        dictionaries: List<DictionaryInfo>,
    ) {
        dictionaries.forEach { info ->
            val entry =
                Writer()
                    .apply {
                        uint32(1, info.id)
                        bytes(2, info.fingerprint)
                    }.toByteArray()
            tag(field, 2)
            varint(entry.size.toULong())
            output.write(entry)
        }
    }

    /** Writes a present `ConnectionMetadata` message, even one whose fields are all zero. */
    fun connectionMetadata(
        field: Int,
        value: ConnectionMetadata?,
    ) {
        if (value == null) return
        val entry =
            Writer()
                .apply {
                    uint32(1, value.id)
                    metadata(2, value.metadata)
                }.toByteArray()
        tag(field, 2)
        varint(entry.size.toULong())
        output.write(entry)
    }

    fun string(
        field: Int,
        value: String,
    ) {
        if (value.isNotEmpty()) bytes(field, value.toByteArray(Charsets.UTF_8))
    }

    fun bytes(
        field: Int,
        value: ByteArray,
    ) {
        if (value.isNotEmpty()) {
            tag(field, 2)
            varint(value.size.toULong())
            output.write(value)
        }
    }

    fun metadata(
        field: Int,
        metadata: Metadata,
    ) {
        metadata.wireEntries().forEach { (key, value) ->
            val entry =
                Writer()
                    .apply {
                        string(1, key)
                        bytes(2, value)
                    }.toByteArray()
            tag(field, 2)
            varint(entry.size.toULong())
            output.write(entry)
        }
    }

    fun toByteArray(): ByteArray = output.toByteArray()

    private fun tag(
        field: Int,
        wireType: Int,
    ) = varint(((field shl 3) or wireType).toULong())

    private fun varint(initial: ULong) {
        var value = initial
        while (value >= 0x80u) {
            output.write(((value and 0x7fu).toInt() or 0x80))
            value = value shr 7
        }
        output.write(value.toInt())
    }
}

private class Reader(
    private val bytes: ByteArray,
) {
    private var offset = 0

    val hasRemaining: Boolean
        get() = offset < bytes.size

    fun tag(): Tag {
        val raw = varint()
        if (raw == 0uL || raw > UInt.MAX_VALUE.toULong()) malformed("invalid protobuf field tag")
        val field = (raw shr 3).toInt()
        val wireType = (raw and 7u).toInt()
        if (field == 0) malformed("invalid protobuf field number")
        return Tag(field, wireType)
    }

    fun uint32(tag: Tag): UInt {
        requireWireType(tag, 0)
        val value = varint()
        if (value > UInt.MAX_VALUE.toULong()) malformed("protobuf uint32 exceeded 32 bits")
        return value.toUInt()
    }

    fun int32(tag: Tag): Int {
        requireWireType(tag, 0)
        return varint().toInt()
    }

    fun uint64(tag: Tag): ULong {
        requireWireType(tag, 0)
        return varint()
    }

    fun bool(tag: Tag): Boolean {
        requireWireType(tag, 0)
        return varint() != 0uL
    }

    /** Reads a repeated uint64 element: either one unpacked varint or a packed run of them. */
    fun uint64Elements(
        tag: Tag,
        target: MutableList<ULong>,
    ) {
        when (tag.wireType) {
            0 -> {
                target += varint()
            }

            2 -> {
                val packed = Reader(bytes(tag))
                while (packed.hasRemaining) target += packed.varint()
            }

            else -> {
                malformed("field ${tag.field} used wire type ${tag.wireType}, expected 0 or 2")
            }
        }
    }

    /** Reads a repeated uint32 element: either one unpacked varint or a packed run of them. */
    fun uint32Elements(
        tag: Tag,
        target: MutableList<UInt>,
    ) {
        val values = mutableListOf<ULong>()
        uint64Elements(tag, values)
        values.forEach { value ->
            if (value > UInt.MAX_VALUE.toULong()) malformed("protobuf uint32 exceeded 32 bits")
            target += value.toUInt()
        }
    }

    fun dictionaryInfo(tag: Tag): DictionaryInfo {
        val entry = Reader(bytes(tag))
        var id = 0u
        var fingerprint = byteArrayOf()
        while (entry.hasRemaining) {
            val entryTag = entry.tag()
            when (entryTag.field) {
                1 -> id = entry.uint32(entryTag)
                2 -> fingerprint = entry.bytes(entryTag)
                else -> entry.skip(entryTag)
            }
        }
        return DictionaryInfo(id, fingerprint)
    }

    fun streamCompression(tag: Tag): StreamCompression {
        val value = int32(tag)
        return StreamCompression.fromValue(value) ?: malformed("unknown stream compression value $value")
    }

    fun connectionMetadata(tag: Tag): ConnectionMetadata {
        val entry = Reader(bytes(tag))
        var id = 0u
        val metadata = linkedMapOf<String, ByteArray>()
        while (entry.hasRemaining) {
            val entryTag = entry.tag()
            when (entryTag.field) {
                1 -> id = entry.uint32(entryTag)
                2 -> entry.mapEntry(entryTag, metadata)
                else -> entry.skip(entryTag)
            }
        }
        return ConnectionMetadata(id, Metadata.fromWire(metadata))
    }

    fun bytes(tag: Tag): ByteArray {
        requireWireType(tag, 2)
        val length = length()
        val result = bytes.copyOfRange(offset, offset + length)
        offset += length
        return result
    }

    fun string(
        tag: Tag,
        fieldName: String,
    ): String {
        val value = bytes(tag)
        return try {
            Charsets.UTF_8
                .newDecoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(value))
                .toString()
        } catch (error: Exception) {
            throw TrevRpcException(Status.invalidArgument("$fieldName was not valid UTF-8"), error)
        }
    }

    fun mapEntry(
        tag: Tag,
        target: MutableMap<String, ByteArray>,
    ) {
        val entry = Reader(bytes(tag))
        var key = ""
        var value = byteArrayOf()
        while (entry.hasRemaining) {
            val entryTag = entry.tag()
            when (entryTag.field) {
                1 -> key = entry.string(entryTag, "metadata key")
                2 -> value = entry.bytes(entryTag)
                else -> entry.skip(entryTag)
            }
        }
        target[key] = value
    }

    fun skip(tag: Tag) {
        when (tag.wireType) {
            0 -> varint()
            1 -> advance(8)
            2 -> advance(length())
            5 -> advance(4)
            else -> malformed("unsupported protobuf wire type ${tag.wireType}")
        }
    }

    private fun requireWireType(
        tag: Tag,
        expected: Int,
    ) {
        if (tag.wireType != expected) {
            malformed("field ${tag.field} used wire type ${tag.wireType}, expected $expected")
        }
    }

    private fun length(): Int {
        val value = varint()
        if (value > Int.MAX_VALUE.toULong()) malformed("protobuf field length exceeded supported range")
        val length = value.toInt()
        if (length > bytes.size - offset) malformed("truncated length-delimited protobuf field")
        return length
    }

    private fun advance(count: Int) {
        if (count > bytes.size - offset) malformed("truncated protobuf field")
        offset += count
    }

    private fun varint(): ULong {
        var result = 0uL
        for (index in 0 until 10) {
            if (offset >= bytes.size) malformed("truncated protobuf varint")
            val byte = bytes[offset++].toInt() and 0xff
            if (index == 9 && byte > 1) malformed("protobuf varint exceeded 64 bits")
            result = result or ((byte and 0x7f).toULong() shl (index * 7))
            if (byte and 0x80 == 0) return result
        }
        malformed("protobuf varint exceeded 64 bits")
    }

    private fun malformed(message: String): Nothing = throw TrevRpcException(Status.invalidArgument(message))
}
