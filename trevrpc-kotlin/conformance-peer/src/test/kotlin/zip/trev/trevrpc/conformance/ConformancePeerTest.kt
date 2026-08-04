package zip.trev.trevrpc.conformance

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.nio.file.Path
import java.util.concurrent.TimeUnit

class ConformancePeerTest {
    @Test
    fun `strict parser accepts canonical commands and rejects protocol deviations`() {
        val parsed =
            parseCommand(
                "RUN\t7\trpc_request.unary\tcodec.decode\trpc_request\t3001".encodeToByteArray(),
            ) as ParsedCommand.Run
        assertEquals("7", parsed.command.sequence)
        assertEquals("rpc_request", parsed.command.messageType)
        assertEquals("3001", parsed.command.body?.lowerHex())
        assertEquals(ParsedCommand.Stop, parseCommand("STOP".encodeToByteArray()))

        listOf(
            "RUN\t01\tcase\tcodec.decode\trpc_request\t3001",
            "RUN\t1\tBad\tcodec.decode\trpc_request\t3001",
            "RUN\t1\tcase\tcodec.decode\trpc_request\t0A",
            "RUN\t1\tcase\tcodec.decode\trpc_request\t0",
            "RUN\t1\tcase\tunknown",
            "RUN\t1\tcase\tcodec.encode\trpc_request\t\t\t\t2\t62\t\t61\t\tunary\t1\t0",
            "RUN\t1\tcase\tcodec.decode\trpc_request\t3001\textra",
            "RUN\t1\tcase\tcodec.decode\trpc_request\t3001\r",
        ).forEach { line ->
            assertThrows(ProtocolException::class.java) { parseCommand(line.encodeToByteArray()) }
        }
        assertThrows(ProtocolException::class.java) {
            parseCommand(byteArrayOf(*"RUN\t1\tcase\tcodec.decode\trpc_request\t".encodeToByteArray(), 0xff.toByte()))
        }
    }

    @Test
    fun `strict reader enforces lf and the 262144 byte boundary`() {
        val exact = ByteArray(MAX_COMMAND_BYTES) { 'a'.code.toByte() }
        val exactInput = ByteArrayInputStream(exact + byteArrayOf('\n'.code.toByte()))
        assertEquals(MAX_COMMAND_BYTES, StrictLineReader(exactInput).readLine().size)

        val oversized = ByteArray(MAX_COMMAND_BYTES + 1) { 'a'.code.toByte() }
        assertThrows(ProtocolException::class.java) {
            StrictLineReader(ByteArrayInputStream(oversized + byteArrayOf('\n'.code.toByte()))).readLine()
        }
        assertThrows(ProtocolException::class.java) {
            StrictLineReader(ByteArrayInputStream("STOP".encodeToByteArray())).readLine()
        }
        assertThrows(ProtocolException::class.java) {
            StrictLineReader(ByteArrayInputStream(byteArrayOf())).readLine()
        }
    }

    @Test
    fun `codec normalization preserves raw status and maps directional failures`() {
        val unknown = run("RUN\t1\tunknown\tcodec.decode\trpc_response\t08e70712036f6464")
        assertEquals(null, unknown.error)
        val message = unknown.payload.getValue("message") as Map<*, *>
        assertEquals("999", message["status_raw"])
        assertEquals(2u, message["status_code"])
        assertEquals("08e70712036f6464", unknown.payload["canonical_body_hex"])

        val streamUnknown =
            run("RUN\t2\tunknown\tcodec.decode\trpc_stream_frame\t080110e7071a036f6464")
        assertEquals(null, streamUnknown.error)
        val streamMessage = streamUnknown.payload.getValue("message") as Map<*, *>
        assertEquals("999", streamMessage["status_raw"])
        assertEquals(2u, streamMessage["status_code"])
        assertEquals("080110e7071a036f6464", streamUnknown.payload["canonical_body_hex"])

        val responseMalformed = run("RUN\t3\tmalformed\tcodec.decode\trpc_response\t80")
        assertError(responseMalformed, "malformed_protobuf", 13u)
        assertError(
            run("RUN\t4\tmalformed\tcodec.decode\trpc_response\t0a00"),
            "malformed_protobuf",
            13u,
        )
        assertError(
            run("RUN\t5\tmalformed\tcodec.decode\trpc_response\t1201ff"),
            "malformed_protobuf",
            13u,
        )
        val invalidMetadata =
            run(
                "RUN\t3\tmetadata\tcodec.decode\trpc_response\t" +
                    "22130a0d417574686f72697a6174696f6e12026f6b",
            )
        assertError(invalidMetadata, "invalid_metadata", 13u)
        val requestMetadata =
            run(
                "RUN\t4\tmetadata\tcodec.decode\trpc_request\t" +
                    "22130a0d417574686f72697a6174696f6e12026f6b3001",
            )
        assertError(requestMetadata, "invalid_metadata", 3u)
        val streamMetadata =
            run(
                "RUN\t6\tmetadata\tcodec.decode\trpc_stream_frame\t" +
                    "2a130a0d417574686f72697a6174696f6e12026f6b",
            )
        assertError(streamMetadata, "invalid_metadata", 13u)
        val malformedMetadataUtf8 = run("RUN\t7\tmetadata\tcodec.decode\trpc_response\t22030a01ff")
        assertError(malformedMetadataUtf8, "malformed_protobuf", 13u)
    }

    @Test
    fun `resource limit vectors use production metadata validation`() {
        val tooMany = requestWithMetadata((0..64).map { index -> "k%02d".format(index) to byteArrayOf() })
        assertError(decodeRequest(tooMany), "invalid_metadata", 3u)

        val key128 = requestWithMetadata(listOf("a".repeat(128) to byteArrayOf(0)))
        val key128Result = decodeRequest(key128)
        assertEquals(null, key128Result.error)
        assertEquals(key128.lowerHex(), key128Result.payload["canonical_body_hex"])

        assertError(
            decodeRequest(requestWithMetadata(listOf("a".repeat(129) to byteArrayOf()))),
            "invalid_metadata",
            3u,
        )

        val value8192 = requestWithMetadata(listOf("a" to ByteArray(8192)))
        val value8192Result = decodeRequest(value8192)
        assertEquals(null, value8192Result.error)
        assertEquals(value8192.lowerHex(), value8192Result.payload["canonical_body_hex"])

        assertError(
            decodeRequest(requestWithMetadata(listOf("a" to ByteArray(8193)))),
            "invalid_metadata",
            3u,
        )
        val overTotal =
            requestWithMetadata(
                ('a'..'h').map { key -> key.toString() to ByteArray(8191) } +
                    ("i" to byteArrayOf()),
            )
        assertError(decodeRequest(overTotal), "invalid_metadata", 3u)
    }

    @Test
    fun `framing and production client state enforce terminal precedence`() {
        assertError(
            run("RUN\t1\tpartial\tframing.decode_stream\trpc_stream_frame\t16\t1\t000000"),
            "incomplete_frame",
            13u,
        )
        assertError(
            run("RUN\t2\ttrailing\tstate.server_stream\t2\t0801\t22031a0161"),
            "trailing_frame",
            13u,
        )
        assertError(
            run("RUN\t3\ttrailing\tstate.client_stream\t3\t22031a0161\t0801\t22031a0162"),
            "trailing_frame",
            13u,
        )
        val remoteUnknown = run("RUN\t4\tremote\tstate.server_stream\t1\t080110e7071a036f6464")
        assertError(remoteUnknown, "remote_status", 2u)
        assertEquals("1", remoteUnknown.payload["transport_close_count"])
        assertError(
            run("RUN\t5\tremote\tstate.client_stream\t1\t080110e7071a036f6464"),
            "remote_status",
            2u,
        )
        assertError(
            run("RUN\t6\tempty\tstate.client_stream\t0"),
            "missing_terminal_status",
            13u,
        )
        assertError(
            run("RUN\t5\tmessage\tstate.client_stream\t1\t22031a0161"),
            "missing_terminal_status",
            13u,
        )
        assertError(
            run("RUN\t6\tremote\tstate.client_stream\t2\t22031a0161\t0801100e1a04646f776e"),
            "remote_status",
            14u,
        )
        val success = run("RUN\t7\tsuccess\tstate.client_stream\t2\t22031a0161\t0801")
        assertEquals(null, success.error)
        assertEquals("1a0161", success.payload["response_body_hex"])
        assertError(
            run("RUN\t8\tmalformed\tstate.server_stream\t2\t220108\t0801"),
            "malformed_protobuf",
            13u,
        )
        assertError(
            run("RUN\t9\tmalformed\tstate.client_stream\t2\t220108\t0801"),
            "malformed_protobuf",
            13u,
        )
        assertError(
            run("RUN\t10\tknown_wrong_wire\tstate.client_stream\t2\t22021801\t0801"),
            "malformed_protobuf",
            13u,
        )
        assertError(
            run("RUN\t11\tknown_wrong_wire\tstate.server_stream\t2\t22021801\t0801"),
            "malformed_protobuf",
            13u,
        )
        val unknownField = run("RUN\t12\tunknown_field\tstate.client_stream\t2\t22070a01781a026f6b\t0801")
        assertEquals(null, unknownField.error)
        assertEquals("1a026f6b", unknownField.payload["response_body_hex"])
        val unknownServer = run("RUN\t13\tunknown_field\tstate.server_stream\t2\t22070a01781a026f6b\t0801")
        assertEquals(null, unknownServer.error)
        assertEquals(
            listOf(
                linkedMapOf("event" to "message", "body_hex" to "1a026f6b"),
                linkedMapOf("event" to "eof"),
                linkedMapOf("event" to "eof"),
            ),
            unknownServer.payload["events"],
        )
        assertError(
            run("RUN\t12\tmissing_precedence\tstate.client_stream\t2\t2200\t2200"),
            "missing_terminal_status",
            13u,
        )
        assertError(
            run(
                "RUN\t13\tremote_precedence\tstate.client_stream\t3\t" +
                    "2200\t2200\t0801100e1a04646f776e",
            ),
            "remote_status",
            14u,
        )
        assertError(
            run("RUN\t14\ttrailing_precedence\tstate.client_stream\t4\t2200\t2200\t0801\t2200"),
            "trailing_frame",
            13u,
        )
    }

    @Test
    fun `installed command emits strict ready result fatal and clean stop`() {
        val executable = Path.of(checkNotNull(System.getProperty("trevrpc.kotlin.conformance.peer")))
        val clean = ProcessBuilder(executable.toString(), "--protocol", "1").start()
        val cleanOutput = clean.inputStream.bufferedReader()
        val ready = cleanOutput.readLine()
        assertTrue(ready.startsWith("{\"schema_version\":1,\"event\":\"ready\",\"peer\":\"kotlin\",\"pid\":"))
        assertTrue(
            ready.endsWith(
                ",\"capabilities\":[\"codec.decode\",\"codec.encode\",\"framing.decode_stream\",\"framing.encode\",\"state.client_stream\",\"state.server_stream\"]}",
            ),
        )
        clean.outputStream.write(
            (
                "RUN\t1\trequest\tcodec.decode\trpc_request\t3001\n" +
                    "RUN\t2\tknown_wrong_wire\tstate.client_stream\t2\t22021801\t0801\n" +
                    "RUN\t3\tunknown_field\tstate.client_stream\t2\t22070a01781a026f6b\t0801\n" +
                    "RUN\t4\tmissing_precedence\tstate.client_stream\t2\t2200\t2200\n" +
                    "RUN\t5\tremote_precedence\tstate.client_stream\t3\t" +
                    "2200\t2200\t0801100e1a04646f776e\n" +
                    "RUN\t6\ttrailing_precedence\tstate.client_stream\t4\t2200\t2200\t0801\t2200\n" +
                    "STOP\n"
            ).encodeToByteArray(),
        )
        clean.outputStream.flush()
        val result = cleanOutput.readLine()
        assertTrue(result.contains("\"event\":\"result\""))
        assertTrue(result.contains("\"outcome\":\"success\""))
        assertEquals(
            "{\"schema_version\":1,\"event\":\"result\",\"peer\":\"kotlin\",\"sequence\":\"2\"," +
                "\"case_id\":\"known_wrong_wire\",\"operation\":\"state.client_stream\"," +
                "\"outcome\":\"error\",\"category\":\"malformed_protobuf\",\"status_code\":13}",
            cleanOutput.readLine(),
        )
        assertEquals(
            "{\"schema_version\":1,\"event\":\"result\",\"peer\":\"kotlin\",\"sequence\":\"3\"," +
                "\"case_id\":\"unknown_field\",\"operation\":\"state.client_stream\"," +
                "\"response_body_hex\":\"1a026f6b\",\"outcome\":\"success\"}",
            cleanOutput.readLine(),
        )
        assertInstalledError(cleanOutput.readLine(), "4", "missing_precedence", "missing_terminal_status", 13u)
        assertInstalledError(cleanOutput.readLine(), "5", "remote_precedence", "remote_status", 14u)
        assertInstalledError(cleanOutput.readLine(), "6", "trailing_precedence", "trailing_frame", 13u)
        assertFalse(cleanOutput.readLine() != null)
        assertTrue(clean.waitFor(10, TimeUnit.SECONDS))
        assertEquals(0, clean.exitValue())

        val fatal = ProcessBuilder(executable.toString(), "--protocol", "1").start()
        val fatalOutput = fatal.inputStream.bufferedReader()
        fatalOutput.readLine()
        fatal.outputStream.write("BAD\n".encodeToByteArray())
        fatal.outputStream.flush()
        assertEquals(
            "{\"schema_version\":1,\"event\":\"fatal\",\"peer\":\"kotlin\",\"message\":\"expected RUN command\"}",
            fatalOutput.readLine(),
        )
        assertTrue(fatal.waitFor(10, TimeUnit.SECONDS))
        assertEquals(2, fatal.exitValue())

        val huge = ProcessBuilder(executable.toString(), "--protocol", "1").start()
        val hugeOutput = huge.inputStream.bufferedReader()
        hugeOutput.readLine()
        huge.outputStream.write(
            "RUN\t1\thuge\tstate.server_stream\t2147483647\n".encodeToByteArray(),
        )
        huge.outputStream.flush()
        assertTrue(hugeOutput.readLine().contains("\"event\":\"fatal\""))
        assertTrue(huge.waitFor(10, TimeUnit.SECONDS))
        assertEquals(2, huge.exitValue())
    }

    private fun run(line: String): OperationResult {
        val command = (parseCommand(line.encodeToByteArray()) as ParsedCommand.Run).command
        return dispatch(command)
    }

    private fun decodeRequest(body: ByteArray): OperationResult = run("RUN\t1\tresource\tcodec.decode\trpc_request\t${body.lowerHex()}")

    private fun requestWithMetadata(entries: List<Pair<String, ByteArray>>): ByteArray =
        entries.fold(byteArrayOf()) { request, (key, value) ->
            val keyBytes = key.encodeToByteArray()
            val entry =
                byteArrayOf(0x0a) +
                    protobufLength(keyBytes.size) +
                    keyBytes +
                    byteArrayOf(0x12) +
                    protobufLength(value.size) +
                    value
            request + byteArrayOf(0x22) + protobufLength(entry.size) + entry
        } + byteArrayOf(0x30, 0x01)

    private fun protobufLength(initialValue: Int): ByteArray {
        var value = initialValue
        val encoded = mutableListOf<Byte>()
        while (value >= 0x80) {
            encoded += ((value and 0x7f) or 0x80).toByte()
            value = value ushr 7
        }
        encoded += value.toByte()
        return encoded.toByteArray()
    }

    private fun assertInstalledError(
        result: String,
        sequence: String,
        caseId: String,
        category: String,
        statusCode: UInt,
    ) {
        assertEquals(
            "{\"schema_version\":1,\"event\":\"result\",\"peer\":\"kotlin\"," +
                "\"sequence\":\"$sequence\",\"case_id\":\"$caseId\"," +
                "\"operation\":\"state.client_stream\",\"outcome\":\"error\"," +
                "\"category\":\"$category\",\"status_code\":$statusCode}",
            result,
        )
    }

    private fun assertError(
        result: OperationResult,
        category: String,
        statusCode: UInt,
    ) {
        assertEquals(category, result.error?.category)
        assertEquals(statusCode, result.error?.statusCode)
    }
}
