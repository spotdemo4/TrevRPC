package zip.trev.trevrpc.bench

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.io.ByteArrayOutputStream
import java.io.PrintStream

class ProtocolTest {
    @Test
    fun `client command parses every required setting`() {
        val command =
            parseCommand(
                arrayOf(
                    "client",
                    "--stack",
                    "trevrpc_native_quic",
                    "--address",
                    "127.0.0.1:7443",
                    "--cert",
                    "certificate.pem",
                    "--rpc",
                    "bidi",
                    "--concurrency",
                    "8",
                    "--warmup-ms",
                    "250",
                    "--measurement-ms",
                    "1000",
                    "--request-bytes",
                    "32",
                    "--response-bytes",
                    "64",
                    "--messages-per-stream",
                    "7",
                ),
            ) as PeerCommand.Client

        assertEquals("127.0.0.1:7443", command.address)
        assertEquals(BenchmarkStack.TREVRPC_NATIVE_QUIC, command.stack)
        assertEquals(BenchmarkRpcKind.BIDI, command.rpcKind)
        assertEquals(8, command.concurrency)
        assertEquals(250, command.warmupMilliseconds)
        assertEquals(1_000_000_000, command.admissionNanoseconds)
        assertEquals(32, command.requestBytes)
        assertEquals(64, command.responseBytes)
        assertEquals(7, command.messagesPerStream)
    }

    @Test
    fun `commands reject unknown duplicate and invalid settings`() {
        assertThrows(IllegalArgumentException::class.java) {
            parseCommand(
                arrayOf(
                    "server",
                    "--stack",
                    "trevrpc_native_quic",
                    "--listen",
                    "127.0.0.1:0",
                    "--cert",
                    "cert",
                    "--key",
                    "key",
                    "--x",
                    "1",
                ),
            )
        }
        assertThrows(IllegalArgumentException::class.java) {
            parseCommand(
                arrayOf(
                    "server",
                    "--stack",
                    "trevrpc_native_quic",
                    "--listen",
                    "a",
                    "--listen",
                    "b",
                    "--cert",
                    "cert",
                    "--key",
                    "key",
                ),
            )
        }
        assertThrows(IllegalArgumentException::class.java) {
            clientArgs("--concurrency", "0")
        }
        assertThrows(IllegalArgumentException::class.java) {
            clientArgs("--measurement-ms", "0")
        }
        assertThrows(IllegalArgumentException::class.java) {
            clientArgs("--messages-per-stream", "0")
        }
    }

    @Test
    fun `schema 5 capabilities advertise role-specific stacks and commands require one`() {
        val output = ByteArrayOutputStream()
        EventWriter(PrintStream(output, true, Charsets.UTF_8)).capabilities()

        val rpcKinds = """["unary","client_stream","server_stream","bidi"]"""
        val expectedCapabilities =
            """{"schema_version":5,"event":"capabilities","peer":"kotlin","roles":{""" +
                """"client":["trevrpc_native_quic"],""" +
                """"server":["trevrpc_native_quic","trevrpc_http3","trevrpc_webtransport"]},""" +
                """"rpc_kinds":$rpcKinds,"histogram":"log_linear_v1"}""" +
                System.lineSeparator()
        assertEquals(
            expectedCapabilities,
            output.toString(Charsets.UTF_8),
        )
        assertThrows(IllegalArgumentException::class.java) {
            parseCommand(arrayOf("server", "--listen", "127.0.0.1:0", "--cert", "cert", "--key", "key"))
        }
        assertThrows(IllegalArgumentException::class.java) {
            clientArgs("--stack", "unknown")
        }
    }

    @Test
    fun `ordinary HTTP3 is server-only and does not require an origin`() {
        val server =
            parseCommand(
                arrayOf(
                    "server",
                    "--stack",
                    "trevrpc_http3",
                    "--listen",
                    "127.0.0.1:0",
                    "--cert",
                    "cert",
                    "--key",
                    "key",
                ),
            ) as PeerCommand.Server
        assertEquals(BenchmarkStack.TREVRPC_HTTP3, server.stack)
        assertNull(server.webTransportOrigin)
        assertThrows(IllegalArgumentException::class.java) { clientArgs("--stack", "trevrpc_http3") }
        assertThrows(IllegalArgumentException::class.java) {
            parseCommand(
                arrayOf(
                    "server",
                    "--stack",
                    "trevrpc_http3",
                    "--listen",
                    "127.0.0.1:0",
                    "--cert",
                    "cert",
                    "--key",
                    "key",
                    "--webtransport-origin",
                    "https://benchmark.example",
                ),
            )
        }
    }

    @Test
    fun `WebTransport is server-only and requires an origin`() {
        assertThrows(IllegalArgumentException::class.java) {
            parseCommand(
                arrayOf(
                    "server",
                    "--stack",
                    "trevrpc_webtransport",
                    "--listen",
                    "127.0.0.1:0",
                    "--cert",
                    "cert",
                    "--key",
                    "key",
                ),
            )
        }
        val server =
            parseCommand(
                arrayOf(
                    "server",
                    "--stack",
                    "trevrpc_webtransport",
                    "--listen",
                    "127.0.0.1:0",
                    "--cert",
                    "cert",
                    "--key",
                    "key",
                    "--webtransport-origin",
                    "https://benchmark.example",
                ),
            ) as PeerCommand.Server
        assertEquals("https://benchmark.example", server.webTransportOrigin)
        assertThrows(IllegalArgumentException::class.java) {
            clientArgs("--stack", "trevrpc_webtransport")
        }
        assertThrows(IllegalArgumentException::class.java) {
            parseCommand(arrayOf("client", "--stack", "trevrpc_webtransport"))
        }
    }

    @Test
    fun `log linear histogram has exact small buckets and sorted sparse output`() {
        assertEquals(1, LogLinearHistogram.upperBound(1))
        assertEquals(1023, LogLinearHistogram.upperBound(1023))
        assertEquals(1025, LogLinearHistogram.upperBound(1024))
        assertEquals(2041, LogLinearHistogram.upperBound(2040))

        val histogram = LogLinearHistogram()
        histogram.record(2040)
        histogram.record(1)
        histogram.record(1024)
        histogram.record(1024)
        assertEquals(
            listOf(
                HistogramBucket(1, 1),
                HistogramBucket(1025, 2),
                HistogramBucket(2041, 1),
            ),
            histogram.buckets(),
        )
        assertEquals(4, histogram.count)
    }

    @Test
    fun `events are one-line JSON with decimal string counters`() {
        val output = ByteArrayOutputStream()
        val events = EventWriter(PrintStream(output, true, Charsets.UTF_8))
        val histogram = LogLinearHistogram().also { it.record(100) }
        events.ready("127.0.0.1:7", 12)
        events.sample(
            SampleResult(
                BenchmarkRpcKind.UNARY,
                1_000_000,
                1_000_100,
                1,
                0,
                1,
                1,
                histogram,
            ),
        )

        assertEquals(
            listOf(
                """{"schema_version":5,"event":"ready","peer":"kotlin","address":"127.0.0.1:7","pid":12}""",
                """{"schema_version":5,"event":"sample","peer":"kotlin","rpc_kind":"unary","admission_ns":"1000000","elapsed_ns":"1000100","drain_ns":"100","completed":"1","failed":"0","request_messages":"1","response_messages":"1","histogram":[{"upper_bound_ns":"100","count":"1"}]}""",
            ),
            output
                .toString(Charsets.UTF_8)
                .lineSequence()
                .filter(String::isNotEmpty)
                .toList(),
        )
    }

    private fun clientArgs(
        option: String,
        value: String,
    ) {
        val values =
            linkedMapOf(
                "--stack" to "trevrpc_native_quic",
                "--address" to "127.0.0.1:7443",
                "--cert" to "cert",
                "--rpc" to "unary",
                "--concurrency" to "1",
                "--warmup-ms" to "0",
                "--measurement-ms" to "1",
                "--request-bytes" to "0",
                "--response-bytes" to "0",
                "--messages-per-stream" to "1",
            )
        values[option] = value
        parseCommand(arrayOf("client", *values.flatMap { listOf(it.key, it.value) }.toTypedArray()))
    }
}
