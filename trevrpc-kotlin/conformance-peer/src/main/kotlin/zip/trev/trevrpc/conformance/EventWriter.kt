package zip.trev.trevrpc.conformance

import java.io.OutputStream

internal class EventWriter(
    private val output: OutputStream,
) {
    fun emit(event: Map<String, Any?>) {
        val encoded = buildString { appendJson(event) }.encodeToByteArray()
        if (encoded.size + 1 > MAX_EVENT_BYTES) protocolFailure("event exceeded output limit")
        output.write(encoded)
        output.write('\n'.code)
        output.flush()
    }
}

private fun StringBuilder.appendJson(value: Any?) {
    when (value) {
        null -> {
            append("null")
        }

        is String -> {
            appendJsonString(value)
        }

        is Boolean, is Byte, is Short, is Int, is Long, is UInt, is ULong -> {
            append(value.toString())
        }

        is Map<*, *> -> {
            append('{')
            value.entries.forEachIndexed { index, entry ->
                if (index != 0) append(',')
                appendJsonString(entry.key as String)
                append(':')
                appendJson(entry.value)
            }
            append('}')
        }

        is Iterable<*> -> {
            append('[')
            value.forEachIndexed { index, item ->
                if (index != 0) append(',')
                appendJson(item)
            }
            append(']')
        }

        else -> {
            error("unsupported JSON value ${value::class.qualifiedName}")
        }
    }
}

private fun StringBuilder.appendJsonString(value: String) {
    append('"')
    value.forEach { character ->
        when (character) {
            '"' -> {
                append("\\\"")
            }

            '\\' -> {
                append("\\\\")
            }

            '\b' -> {
                append("\\b")
            }

            '\n' -> {
                append("\\n")
            }

            '\r' -> {
                append("\\r")
            }

            '\t' -> {
                append("\\t")
            }

            else -> {
                if (character.code < 0x20) {
                    append("\\u")
                    append(character.code.toString(16).padStart(4, '0'))
                } else {
                    append(character)
                }
            }
        }
    }
    append('"')
}
