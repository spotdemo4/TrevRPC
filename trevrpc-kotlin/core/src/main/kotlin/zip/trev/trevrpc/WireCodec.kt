package zip.trev.trevrpc

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction

object WireCodec {
    fun encode(request: RpcRequest): ByteArray =
        Writer()
            .apply {
                string(1, request.service)
                string(2, request.method)
                bytes(3, request.body)
                metadata(4, request.metadata)
                int32(5, request.kindValue)
                uint32(6, request.version)
                uint64(7, request.timeoutNanos)
            }.toByteArray()

    fun decodeRequest(bytes: ByteArray): RpcRequest {
        val reader = Reader(bytes)
        var service = ""
        var method = ""
        var body = byteArrayOf()
        val metadata = linkedMapOf<String, ByteArray>()
        var kind = 0
        var version = 0u
        var timeoutNanos = 0uL
        while (reader.hasRemaining) {
            val tag = reader.tag()
            when (tag.field) {
                1 -> service = reader.string(tag, "request service")
                2 -> method = reader.string(tag, "request method")
                3 -> body = reader.bytes(tag)
                4 -> reader.mapEntry(tag, metadata)
                5 -> kind = reader.int32(tag)
                6 -> version = reader.uint32(tag)
                7 -> timeoutNanos = reader.uint64(tag)
                else -> reader.skip(tag)
            }
        }
        return RpcRequest(service, method, body, Metadata.fromWire(metadata), kind, version, timeoutNanos)
    }

    fun encode(response: RpcResponse): ByteArray =
        Writer()
            .apply {
                uint32(1, response.statusValue)
                string(2, response.message)
                bytes(3, response.body)
                metadata(4, response.metadata)
            }.toByteArray()

    fun decodeResponse(bytes: ByteArray): RpcResponse {
        val reader = Reader(bytes)
        var status = 0u
        var message = ""
        var body = byteArrayOf()
        val metadata = linkedMapOf<String, ByteArray>()
        while (reader.hasRemaining) {
            val tag = reader.tag()
            when (tag.field) {
                1 -> status = reader.uint32(tag)
                2 -> message = reader.string(tag, "response message")
                3 -> body = reader.bytes(tag)
                4 -> reader.mapEntry(tag, metadata)
                else -> reader.skip(tag)
            }
        }
        return RpcResponse(status, message, body, Metadata.fromWire(metadata))
    }

    fun encode(frame: RpcStreamFrame): ByteArray =
        if (
            frame.kindValue == RpcStreamFrameKind.MESSAGE.value &&
            frame.statusValue == Code.OK.value &&
            frame.message.isEmpty() &&
            frame.metadata.isEmpty
        ) {
            encodeMessageFrame(frame.body)
        } else {
            Writer()
                .apply {
                    int32(1, frame.kindValue)
                    uint32(2, frame.statusValue)
                    string(3, frame.message)
                    bytes(4, frame.body)
                    metadata(5, frame.metadata)
                }.toByteArray()
        }

    fun encodeMessageFrame(body: ByteArray): ByteArray {
        if (body.isEmpty()) return byteArrayOf()
        val lengthBytes = varintSize(body.size)
        return ByteArray(1 + lengthBytes + body.size).also { encoded ->
            encoded[0] = MESSAGE_BODY_TAG
            val bodyOffset = writeVarint(encoded, 1, body.size)
            body.copyInto(encoded, bodyOffset)
        }
    }

    fun decodeStreamFrame(bytes: ByteArray): RpcStreamFrame {
        decodeMessageFrame(bytes)?.let { return it }
        val reader = Reader(bytes)
        var kind = 0
        var status = 0u
        var message = ""
        var body = byteArrayOf()
        val metadata = linkedMapOf<String, ByteArray>()
        while (reader.hasRemaining) {
            val tag = reader.tag()
            when (tag.field) {
                1 -> kind = reader.int32(tag)
                2 -> status = reader.uint32(tag)
                3 -> message = reader.string(tag, "stream status message")
                4 -> body = reader.bytes(tag)
                5 -> reader.mapEntry(tag, metadata)
                else -> reader.skip(tag)
            }
        }
        return RpcStreamFrame(kind, status, message, body, Metadata.fromWire(metadata))
    }

    private fun decodeMessageFrame(bytes: ByteArray): RpcStreamFrame? {
        if (bytes.isEmpty()) return RpcStreamFrame()
        if (bytes[0] != MESSAGE_BODY_TAG) return null
        var length = 0uL
        var offset = 1
        for (index in 0 until 10) {
            if (offset >= bytes.size) return null
            val value = bytes[offset].toInt() and 0xff
            offset++
            if (index == 9 && value > 1) return null
            length = length or ((value and 0x7f).toULong() shl (index * 7))
            if (value and 0x80 == 0) {
                if (length > Int.MAX_VALUE.toULong() || bytes.size - offset != length.toInt()) return null
                return RpcStreamFrame(body = bytes.copyOfRange(offset, bytes.size))
            }
        }
        return null
    }
}

private const val MESSAGE_BODY_TAG: Byte = (4 shl 3 or 2).toByte()

private fun varintSize(value: Int): Int {
    var remaining = value
    var size = 1
    while (remaining >= 0x80) {
        remaining = remaining ushr 7
        size++
    }
    return size
}

private fun writeVarint(
    output: ByteArray,
    initialOffset: Int,
    initialValue: Int,
): Int {
    var offset = initialOffset
    var value = initialValue
    while (value >= 0x80) {
        output[offset++] = ((value and 0x7f) or 0x80).toByte()
        value = value ushr 7
    }
    output[offset++] = value.toByte()
    return offset
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
