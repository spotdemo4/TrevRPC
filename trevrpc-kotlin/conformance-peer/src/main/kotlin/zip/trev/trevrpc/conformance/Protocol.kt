package zip.trev.trevrpc.conformance

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.nio.ByteBuffer
import java.nio.charset.CodingErrorAction

internal const val PROTOCOL_VERSION = 1
internal const val MAX_COMMAND_BYTES = 262_144
internal const val MAX_EVENT_BYTES = 65_536
internal const val PEER_ID = "kotlin"

internal val capabilities =
    listOf(
        "codec.decode",
        "codec.encode",
        "framing.decode_stream",
        "framing.encode",
        "state.client_stream",
        "state.server_stream",
    )

internal data class MetadataEntry(
    val key: ByteArray,
    val value: ByteArray,
)

internal sealed interface NormalizedMessage {
    data class Request(
        val service: ByteArray,
        val method: ByteArray,
        val body: ByteArray,
        val metadata: List<MetadataEntry>,
        val kind: String,
        val version: UInt,
        val timeoutNanos: ULong,
    ) : NormalizedMessage

    data class Response(
        val statusRaw: UInt,
        val message: ByteArray,
        val body: ByteArray,
        val metadata: List<MetadataEntry>,
    ) : NormalizedMessage

    data class StreamFrame(
        val kindRaw: UInt,
        val statusRaw: UInt,
        val message: ByteArray,
        val body: ByteArray,
        val metadata: List<MetadataEntry>,
    ) : NormalizedMessage
}

internal data class RunCommand(
    val sequence: String,
    val caseId: String,
    val operation: String,
    val messageType: String? = null,
    val message: NormalizedMessage? = null,
    val body: ByteArray? = null,
    val maxFrameSize: Int? = null,
    val chunks: List<ByteArray> = emptyList(),
    val frames: List<ByteArray> = emptyList(),
)

internal sealed interface ParsedCommand {
    data object Stop : ParsedCommand

    data class Run(
        val command: RunCommand,
    ) : ParsedCommand
}

internal fun parseCommand(line: ByteArray): ParsedCommand {
    if (line.any { byte -> byte.toInt() < 0 || byte.toInt() == '\r'.code }) {
        protocolFailure("command must be LF-terminated tab-delimited ASCII")
    }
    val text = line.toString(Charsets.US_ASCII)
    if (text == "STOP") return ParsedCommand.Stop
    val fields = text.split('\t')
    if (fields.size < 4 || fields[0] != "RUN") protocolFailure("expected RUN command")
    parseUnsigned(fields[1], ULong.MAX_VALUE, "sequence")
    if (!validId(fields[2])) protocolFailure("invalid case ID")

    val parser = FieldParser(fields, 4)
    val base = RunCommand(sequence = fields[1], caseId = fields[2], operation = fields[3])
    val command =
        when (base.operation) {
            "codec.encode" -> {
                val messageType = parser.messageType()
                base.copy(messageType = messageType, message = parser.message(messageType))
            }

            "codec.decode" -> {
                base.copy(
                    messageType = parser.messageType(),
                    body = parser.hexBytes(),
                )
            }

            "framing.encode" -> {
                val messageType = parser.messageType()
                base.copy(
                    messageType = messageType,
                    maxFrameSize = parser.nonnegativeInt(),
                    message = parser.message(messageType),
                )
            }

            "framing.decode_stream" -> {
                base.copy(
                    messageType = parser.messageType(),
                    maxFrameSize = parser.nonnegativeInt(),
                    chunks = parser.hexList(),
                )
            }

            "state.server_stream", "state.client_stream" -> {
                base.copy(frames = parser.hexList())
            }

            else -> {
                protocolFailure("unknown operation ${base.operation}")
            }
        }
    if (!parser.exhausted) protocolFailure("unexpected command fields")
    return ParsedCommand.Run(command)
}

internal class StrictLineReader(
    private val input: InputStream,
    private val maximum: Int = MAX_COMMAND_BYTES,
) {
    fun readLine(): ByteArray {
        val output = ByteArrayOutputStream()
        while (true) {
            val value = input.read()
            if (value == -1) {
                if (output.size() == 0) protocolFailure("controller input ended without STOP")
                protocolFailure("command was not LF-terminated")
            }
            if (value == '\n'.code) return output.toByteArray()
            if (output.size() == maximum) protocolFailure("command line exceeded limit")
            output.write(value)
        }
    }
}

internal fun ByteArray.strictUtf8(fieldName: String): String =
    try {
        Charsets.UTF_8
            .newDecoder()
            .onMalformedInput(CodingErrorAction.REPORT)
            .onUnmappableCharacter(CodingErrorAction.REPORT)
            .decode(ByteBuffer.wrap(this))
            .toString()
    } catch (error: Exception) {
        throw IllegalArgumentException("$fieldName was not valid UTF-8", error)
    }

internal fun ByteArray.lowerHex(): String {
    val alphabet = "0123456789abcdef"
    return CharArray(size * 2)
        .also { encoded ->
            forEachIndexed { index, byte ->
                val value = byte.toInt() and 0xff
                encoded[index * 2] = alphabet[value ushr 4]
                encoded[index * 2 + 1] = alphabet[value and 0x0f]
            }
        }.concatToString()
}

private class FieldParser(
    private val fields: List<String>,
    private var next: Int,
) {
    val exhausted: Boolean
        get() = next == fields.size

    private val remaining: Int
        get() = fields.size - next

    fun token(): String {
        if (next >= fields.size) protocolFailure("missing command field")
        return fields[next++]
    }

    fun messageType(): String {
        val value = token()
        if (value !in setOf("rpc_request", "rpc_response", "rpc_stream_frame")) {
            protocolFailure("unknown message type $value")
        }
        return value
    }

    fun hexBytes(): ByteArray = decodeLowerHex(token())

    fun nonnegativeInt(): Int = parseUnsigned(token(), Int.MAX_VALUE.toULong(), "integer").toInt()

    fun hexList(): List<ByteArray> {
        val count = nonnegativeInt()
        if (count > remaining) protocolFailure("list count exceeded remaining command fields")
        return buildList(count) {
            repeat(count) { add(hexBytes()) }
        }
    }

    fun message(messageType: String): NormalizedMessage =
        when (messageType) {
            "rpc_request" -> {
                NormalizedMessage.Request(
                    service = hexBytes(),
                    method = hexBytes(),
                    body = hexBytes(),
                    metadata = metadata(),
                    kind = rpcKind(),
                    version = uint32(),
                    timeoutNanos = uint64(),
                )
            }

            "rpc_response" -> {
                NormalizedMessage.Response(
                    statusRaw = uint32(),
                    message = hexBytes(),
                    body = hexBytes(),
                    metadata = metadata(),
                )
            }

            "rpc_stream_frame" -> {
                NormalizedMessage.StreamFrame(
                    kindRaw = uint32(),
                    statusRaw = uint32(),
                    message = hexBytes(),
                    body = hexBytes(),
                    metadata = metadata(),
                )
            }

            else -> {
                protocolFailure("unknown message type $messageType")
            }
        }

    private fun metadata(): List<MetadataEntry> {
        val count = nonnegativeInt()
        if (count > remaining / 2) protocolFailure("metadata count exceeded remaining command fields")
        val entries =
            buildList(count) {
                repeat(count) { add(MetadataEntry(hexBytes(), hexBytes())) }
            }
        entries.zipWithNext().forEach { (left, right) ->
            if (compareBytes(left.key, right.key) >= 0) {
                protocolFailure("metadata entries must be key-sorted and unique")
            }
        }
        return entries
    }

    private fun rpcKind(): String {
        val value = token()
        if (value !in setOf("unary", "client_stream", "server_stream", "bidi")) {
            protocolFailure("unknown RPC kind $value")
        }
        return value
    }

    private fun uint32(): UInt = parseUnsigned(token(), UInt.MAX_VALUE.toULong(), "uint32").toUInt()

    private fun uint64(): ULong = parseUnsigned(token(), ULong.MAX_VALUE, "uint64")
}

private fun decodeLowerHex(value: String): ByteArray {
    if (value.length % 2 != 0) protocolFailure("hex value has odd length")
    if (value.any { it !in '0'..'9' && it !in 'a'..'f' }) protocolFailure("hex value must be lowercase")
    return ByteArray(value.length / 2) { index ->
        value.substring(index * 2, index * 2 + 2).toInt(16).toByte()
    }
}

private fun parseUnsigned(
    value: String,
    maximum: ULong,
    fieldName: String,
): ULong {
    if (value.isEmpty() || (value.length > 1 && value[0] == '0')) protocolFailure("non-canonical $fieldName decimal")
    if (value.any { it !in '0'..'9' }) protocolFailure("non-decimal $fieldName value")
    val parsed = value.toULongOrNull() ?: protocolFailure("$fieldName exceeded supported range")
    if (parsed > maximum) protocolFailure("$fieldName exceeded supported range")
    return parsed
}

private fun validId(value: String): Boolean =
    value.isNotEmpty() &&
        value.all { character ->
            character in 'a'..'z' || character in '0'..'9' || character in "._-"
        }

private fun compareBytes(
    left: ByteArray,
    right: ByteArray,
): Int {
    for (index in 0 until minOf(left.size, right.size)) {
        val comparison = (left[index].toInt() and 0xff).compareTo(right[index].toInt() and 0xff)
        if (comparison != 0) return comparison
    }
    return left.size.compareTo(right.size)
}

internal fun protocolFailure(message: String): Nothing = throw ProtocolException(message)

internal class ProtocolException(
    message: String,
) : Exception(message)
