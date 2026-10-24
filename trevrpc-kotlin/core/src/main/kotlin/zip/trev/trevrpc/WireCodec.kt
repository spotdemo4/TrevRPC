package zip.trev.trevrpc

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction

object WireCodec {
    fun encode(request: RpcRequest): ByteArray =
        Writer()
            .apply {
                string(1, request.method)
                metadata(4, request.metadata)
                int32(5, request.kindValue)
                uint64(7, request.timeoutNanos)
                packedUint64(8, request.extensions)
                uint64(9, request.datagramFlow)
                packedUint32(10, request.acceptDictionaries)
                dictionaries(11, request.dictionaryQuery)
            }.toByteArray()

    fun decodeRequest(bytes: ByteArray): RpcRequest {
        val reader = Reader(bytes)
        var method = ""
        val metadata = linkedMapOf<String, ByteArray>()
        var kind = 0
        var timeoutNanos = 0uL
        val extensions = mutableListOf<ULong>()
        var datagramFlow = 0uL
        val acceptDictionaries = mutableListOf<UInt>()
        val dictionaryQuery = mutableListOf<DictionaryInfo>()
        while (reader.hasRemaining) {
            val tag = reader.tag()
            when (tag.field) {
                1 -> method = reader.string(tag, "request method")
                4 -> reader.mapEntry(tag, metadata)
                5 -> kind = reader.int32(tag)
                7 -> timeoutNanos = reader.uint64(tag)
                8 -> reader.uint64Elements(tag, extensions)
                9 -> datagramFlow = reader.uint64(tag)
                10 -> reader.uint32Elements(tag, acceptDictionaries)
                11 -> dictionaryQuery += reader.dictionaryInfo(tag)
                else -> reader.skip(tag)
            }
        }
        return RpcRequest(
            method,
            Metadata.fromWire(metadata),
            kind,
            timeoutNanos,
            extensions,
            datagramFlow,
            acceptDictionaries,
            dictionaryQuery,
        )
    }

    fun encode(headers: RpcHeaders): ByteArray =
        Writer()
            .apply {
                metadata(1, headers.metadata)
                packedUint64(2, headers.extensions)
                dictionaries(3, headers.dictionaries)
            }.toByteArray()

    fun decodeHeaders(bytes: ByteArray): RpcHeaders {
        val reader = Reader(bytes)
        val metadata = linkedMapOf<String, ByteArray>()
        val extensions = mutableListOf<ULong>()
        val dictionaries = mutableListOf<DictionaryInfo>()
        while (reader.hasRemaining) {
            val tag = reader.tag()
            when (tag.field) {
                1 -> reader.mapEntry(tag, metadata)
                2 -> reader.uint64Elements(tag, extensions)
                3 -> dictionaries += reader.dictionaryInfo(tag)
                else -> reader.skip(tag)
            }
        }
        return RpcHeaders(Metadata.fromWire(metadata), extensions, dictionaries)
    }

    fun encode(status: RpcStatus): ByteArray =
        Writer()
            .apply {
                uint32(1, status.statusValue)
                string(2, status.message)
                metadata(3, status.metadata)
            }.toByteArray()

    fun decodeStatus(bytes: ByteArray): RpcStatus {
        val reader = Reader(bytes)
        var code = 0u
        var message = ""
        val metadata = linkedMapOf<String, ByteArray>()
        while (reader.hasRemaining) {
            val tag = reader.tag()
            when (tag.field) {
                1 -> code = reader.uint32(tag)
                2 -> message = reader.string(tag, "status message")
                3 -> reader.mapEntry(tag, metadata)
                else -> reader.skip(tag)
            }
        }
        return RpcStatus(code, message, Metadata.fromWire(metadata))
    }

    /**
     * Decodes one known frame read from a response stream exactly once. A REQUEST frame is a known
     * frame the response grammar does not allow, which is an `Internal` protocol error. A
     * ZSTD_MESSAGE is validated but stays compressed; the message it carries is limited by
     * [maxFrameSize], like the frames of the stream.
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

            FrameType.HEADERS -> {
                decodeHeaders(frame.payload).let { ResponseFrame.Headers(it.metadata, it.dictionaries) }
            }

            FrameType.STATUS -> {
                ResponseFrame.Status(decodeStatus(frame.payload).status)
            }

            else -> {
                throw TrevRpcException(Status.internal("response stream contained an unexpected frame type ${frame.type}"))
            }
        }
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

    fun packedUint64(
        field: Int,
        values: List<ULong>,
    ) {
        if (values.isEmpty()) return
        val packed = Writer().apply { values.forEach { varint(it) } }.toByteArray()
        tag(field, 2)
        varint(packed.size.toULong())
        output.write(packed)
    }

    fun packedUint32(
        field: Int,
        values: List<UInt>,
    ) = packedUint64(field, values.map(UInt::toULong))

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
